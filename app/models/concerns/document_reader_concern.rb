require 'csv'

# Allows to analyze and parse documents
# Requires model to follow the constrants:
#
# It should have fields:
# "active storage": source
# "datetime": parse_analyzed_at, parse_started_at, parse_finished_at
# "integer": parse_percents
# "text": parse_first_rows, parse_definition, parse_errors
# "string": parse_status
#
# It should have methods:
# self.parse_definitions method to set hash of supported columns
# required_columns to set list of required columns
# 
# And finally, it should have parse method to do all the job!

module DocumentReaderConcern
  extend ActiveSupport::Concern

  included do

    @@parse_definitions = {}
    @@required_columns = []
    @@default_parse_definition = nil

    serialize :parse_definition, coder: JSON
    serialize :parse_first_rows, coder: JSON
    serialize :parse_errors, coder: JSON

    before_validation :normalize_parse_definition
    before_validation :set_parse_status

    after_create_commit :analyze, if: Proc.new { |document| document.parse_definition.nil? && document.source.attached? }
    after_commit :schedule_parse, if: Proc.new { |document| document.parse_status == "analyzed" && document.parse_definition_enough? }
  
    #has_many :parse_errors, class_name: DocumentReaderError, as: :documentable, dependent: :delete_all
 
    enum :parse_status, {
      unparseable: "unparseable",
      pending: "pending", 
      waiting_analyze: "waiting_analyze", 
      analyzing: "analyzing", 
      needs_manual_analyze: "needs_manual_analyze",
      analyzed: "analyzed",
      waiting_parse: "waiting_parse", 
      parsing: "parsing", 
      parsed: "parsed",
      error: "error"
    }, suffix: true
  
    scope :parseed, -> { where.not(:parse_finished_at => nil) }
  end

  def encodings
    ["windows-1251","UTF-8"]
  end

  def source_path
    ActiveStorage::Blob.service.path_for(source.key)
  end

  def parse_status_i18n
    return nil if self.parse_status.blank?
    return I18n.t("document_reader.statuses.#{self.parse_status}")
  end

  def default_parse_definition
    nil
  end

  def normalize_parse_definition
    return true if self.parse_definition.nil? || self.parse_definition.empty?
    return true if self.parse_definition["columns"].nil? || self.parse_definition["columns"].empty?

    self.parse_definition["columns"].each do |k, d|
      next if k == "head"
      self.parse_definition["columns"][k] = d.to_i
    end
    self.parse_definition["columns"] = self.parse_definition["columns"].symbolize_keys
  end

  def get_csv_options

    case parse_definition["parse_csv_format"]
    when "csv"
      return {encoding: "windows-1251:UTF-8", col_sep: ",", quote_char: '"', force_quotes: false}
    when "dsv"
      return {encoding: "windows-1251:UTF-8", col_sep: ";", quote_char: '"', force_quotes: false}
    when "ssv"
      return {encoding: "windows-1251:UTF-8", col_sep: " ", quote_char: '"', force_quotes: false}
    when "tsv"
      return {encoding: "windows-1251:UTF-8", col_sep: "\t", quote_char: '"', force_quotes: false}
    when "nqcsv"
      return {encoding: "windows-1251:UTF-8", col_sep: ",", quote_char: "\x00", force_quotes: false}
    end

    return {}

  end

  def read_sheets

    file = source_csv_file

    detect_csv_format if parse_definition["parse_csv_format"].nil?

    @rows_count = `wc -l "#{file}"`.strip.split(' ')[0].to_i

  end

  # Detects file type by its extension
  def detect_csv_format

    file = source_csv_file

    if [".xlsx", ".xls"].include? File.extname(source.filename.to_s)
      parse_definition["parse_csv_format"] = "csv"
    else

      begin
        lines = []
        File.open(file, "r").each_line do |line|
          lines.push line
          break if lines.length >= 3
        end

        # TODO: replace with line-by-line checks!
        if lines.join(" ").count("\t") >= 6
          parse_definition["parse_csv_format"] = "tsv"
        elsif lines.join(" ").count(";") >= 6
          parse_definition["parse_csv_format"] = "dsv"
        else
          parse_definition["parse_csv_format"] = "csv" 
        end
      rescue Exception => e
        self.parse_errors = [{reason: "unreadable_file"}]
        self.parse_status = "error"
      end
    end

    save!

  end

  # Checks if first rows of table are column-consistent

  def check_cols_enough sheets
    t = nil
    1.upto [sheets.count, 5].min do |i|
      row = sheets.row(i+1).compact
      return false if (!t.nil?) && row.count!=t
      t = row.count if t.nil?
    end
    return (t > 1)
  end

  # Main enumerator while parsing rows
  # Use it within the parse method at your model
  # Returns only filtered, validated rows
  # Does some checks

  def each_row

    #self.parse_percents = 0
    self.parse_started_at = Time.now
    self.parse_finished_at = nil
    self.parse_errors = []
    save!

    if parse_definition.nil?
      self.update(parse_status: "error", parse_errors: [{reason: "no_parse_definition"}])
      return false
    end

    if !parse_definition_enough?
      self.update(parse_status: "error", parse_errors: [{reason: "not_ready_for_parse"}])
      return false
    end

    @start_row = nil

    rows do |data, line|

      data = data.values if data.is_a?(Hash)

      row = parse_row(data, line)

      #percents = ((line.to_f/rows_count.to_f)*100).ceil
      #self.update_column(:parse_percents, percents) if self.parse_percents!=percents

      yield(row, line) if !row.nil?

      if self.parse_errors.length > 100 || (line >= 100 && self.parse_errors.length*5 >= line)
        # 20% or <100 allowed
        self.parse_status = "error"
        save!
        break
      end

    end

  end


  # This one to avild race conditions
  
  def finish_parse
    #self.parse_percents = 100
    self.parse_finished_at = Time.now
    self.parse_status = "parsed" if self.parse_status == "parsing"
    save!
  end

  # parsees row according parse_definition

  def parse_row row, line
    out = {:raw_data => row.to_a, :line => line}

    # Checking for empty rows without any filtering or searching
    # Rows should be absolutely empty

    empty_row = true
    parse_definition["columns"].each do |field, index|
      if !row[index.to_i].to_s.strip.blank?
        empty_row = false
        break
      end
    end

    return nil if empty_row

    if @start_row.nil?
      if parse_definition["head"]
        @start_row = line+1
        return nil
      else
        @start_row = line
      end
    end

    parse_definition["columns"].each do |field, index|

      # Checking for floats with .0 at the end
      if row[index].class == Float
        row[index] = row[index].to_i if row[index] == row[index].to_i
      end

      defs = self.parse_definitions[field.to_sym]

      if defs.nil?
        error_row(out, reason: "unknown_field", field: field)
        return nil
      end

      cleared = row[index].to_s.strip
      cleared = I18n.transliterate(cleared) if defs[:transliterate]==true
      cleared = cleared.gsub(/[[:cntrl:]]/, '')

      defs = shortcut_defs(defs)

      out[field] = cleared if !(cleared =~ defs[:s]).nil? || defs[:s].nil?

      if !defs[:gsub].nil? && !out[field].blank?
        gsub_params = defs[:gsub]
        out[field] = out[field].gsub(/^(\"*)(.*)\1$/, '\2')
        if gsub_params.class == Array
          out[field] = out[field].gsub(gsub_params[0], gsub_params[1])
        else
          out[field] = out[field].gsub(gsub_params, '')
        end
      end

    end

    # Check if row is impossible to use because it lacks some required fields
    # Add errors if these rows are not the first ones

    required_columns.each do |c|
      if out[c.to_s].blank?
        error_row(out, reason: "not_all_required_fields_set", field: c.to_s) if @start_row < line
        return nil
      end
    end

    return out
  end

  def shortcut_defs defs
    # Shortcut for float values
    if defs[:s] == :float
      defs[:s] = /\A(?:(?:0|[1-9][0-9]*)(?:(\.|,)[0-9]+)?|(\.|,)[0-9]+)\Z/i
      defs[:gsub] = [/,/, "."]
    end

    # Shortcut for integer values
    if defs[:s] == :integer
      defs[:s] = /\A(?:(?:0|[1-9][0-9]*)(?:)?)\Z/i
    end

    defs
  end

  # Reason - translated short text
  # Details - long description
  def error_row(row, reason: nil, field: nil, details: nil)
    self.parse_errors.push({line: row[:line]+1, row: row[:raw_data], reason: reason, field: field, details: details})
  end

  def rows_count
    read_sheets if @rows_count.nil?
    return @rows_count
  end

  def sheets
    read_sheets if @sheets.nil?
    @sheets
  end

  def schedule_parse
    return false if !self.source.attached?
    return false if !parse_definition_enough?

    update_columns(
      parse_finished_at: nil, parse_started_at: Time.now,
      parse_errors: nil, parse_status: "waiting_parse"
    )

    ParseJob.perform_later(self.class, self.id)
  end

  # Can be overriden if parser class is not the same as file holder
  def parser_class
    self.class
  end

  # This is for case when user submits inverted column definition hash
  # during parse_definition parse
  def parse_columns= (cols)

    a = {}
    cols.each_with_index do |col, i|
      next if col.blank?
      a[col.to_sym] = i
    end
    self.parse_definition["columns"] = a

    if self.parse_definition["columns"].present? && self.parse_definition_enough?
      self.parse_analyzed_at = Time.now
    else
      self.parse_analyzed_at = nil
    end

    if parse_definition_enough?
      self.parse_status = "analyzed"
    else
      self.parse_status = "needs_manual_analyze"
    end

  end

  def parse_head= (val)
    self.parse_definition["head"] = ActiveRecord::Type::Boolean.new.cast(val)
  end

  # Allows to have first N rows cached in a variable

  def first_rows

    # Gets cached rows if presented
    return parse_first_rows if !parse_first_rows.nil?

    self.parse_first_rows = []

    rows do |row, i|

      row = row.values if row.is_a?(Hash)
 
      row = row.map{|v| v.nil? ? nil : v.strip}

      self.parse_first_rows.push row
      break if i>10
    end

    save!

    return parse_first_rows
  end

  def parse_sql_csv_options
    s = []
    if parse_definition["parse_csv_format"] == "tsv"
      s.push "FIELDS TERMINATED BY '\t'" 
    elsif parse_definition["parse_csv_format"] == "dsv"
      s.push "FIELDS TERMINATED BY ';'"
    else
      s.push "FIELDS TERMINATED BY ','"
    end
    s.push "OPTIONALLY ENCLOSED BY '\"'"
    return s.join(" ")
  end

  def extract_archive

    if (File.extname(source.filename.to_s) == ".zip")

      @tempfile = "/tmp/uploaded-file-#{self.model_name.plural}-#{self.id}.txt"
      # puts "Extracting archive to #{@tempfile}"

      if (File.exist?(@tempfile))
        puts "Already extracted file found in #{@tempfile}"
        return @tempfile 
      end

      Zip::File.open(source_path) do |zipfile|

        entry = zipfile.glob('*.txt').first
        entry = zipfile.glob('*.csv').first if entry.nil?

        if !entry.nil?
          zipfile.extract(entry, @tempfile) 
          return @tempfile
        end

      end

    end

    if (File.extname(source.filename.to_s) == ".gz")

      @tempfile = "/tmp/uploaded-file-#{self.model_name.plural}-#{self.id}.txt"
      # puts "Extracting archive to #{@tempfile}"

      if (File.exist?(@tempfile))
        puts "Already extracted file found in #{@tempfile}"
        return @tempfile 
      end

      puts "zcat #{source_path} > #{@tempfile}"

      `zcat #{source_path} > #{@tempfile}`

      return @tempfile

    end


  end

  def convert_to_csv
    
    @tempfile = "/tmp/uploaded-file-#{self.model_name.plural}-#{self.id}.csv"
    return @tempfile if File.exist?(@tempfile)

    if ([".xlsx"].include? File.extname(source.filename.to_s))
      # puts "Extracting archive to #{@tempfile}"
      system("xlsx2csv -q nonnumeric -a -p '' #{source_path} > #{@tempfile}")
    end

    if ([".xls"].include? File.extname(source.filename.to_s))
      # puts "Extracting archive to #{@tempfile}"
      system("xls2csv #{source_path} > #{@tempfile}")
    end

    return @tempfile if File.exist?(@tempfile)

  end


  # Reads CSV file providing useful YIELD 
  def rows

    file = source_csv_file

    detect_csv_format if parse_definition["parse_csv_format"].nil?

    # Cannot read file
    return nil if parse_definition["parse_csv_format"].nil?

    # puts "!!!!!! #{get_csv_options()} !!!!!!"

    begin

      i = 0

      logger.info "Reading CSV file: #{file}"

      CSV.foreach(file, **get_csv_options()) do |data|
        yield(data, i)
        i+=1
        logger.info "[=====] parseed document #{self.id} at row #{i}/#{@rows_count}..." if i % 10000 == 0
      end

    rescue Exception => e
      logger.error e.message
      self.parse_errors = [{reason: "cannot_parse_csv", details: e.message}]
      if i>0
        self.parse_errors[0][:details] = "Cannot parse CSV row at line #{i}: #{e.message}"
      end
      save!
      raise ArgumentError, "Cannot parse CSV #{file} at row #{i}: #{e.message}"
    end

  end

  # Returns path to source file converted or extracted to CSV format
  def source_csv_file

    return nil if source.filename.nil?

    if ([".zip", ".gz"].include? File.extname(source.filename.to_s))
      file = extract_archive()
    elsif ([".xlsx", ".xls"].include? File.extname(source.filename.to_s))
      file = convert_to_csv()
    else
      file = source_path
    end

    return file
  end

  def is_csv?
    detect_type if !self.source_type?
    return !["xls", "xlsx"].include?(self.source_type.to_s)
  end

  def parsing?
    self.parse_started_at? && !self.parse_finished_at?
  end

  # Rewrite this method to add some logic to it
  def parse_definition_enough? columns_definition=nil
    columns_definition = parse_definition["columns"] if columns_definition.nil? && !parse_definition.nil?
    return false if columns_definition.nil? || columns_definition.empty?
    columns_definition = columns_definition.symbolize_keys
    return false if columns_definition.nil?
    required_columns.each do |c|
      return false if columns_definition.symbolize_keys[c.to_sym].blank?
    end
    return true
  end

  def analyze_not_fired?
    return false if @analyze_fired
    return true
  end

  # Overwrite to point to default document parse_definition storage
  def default_parse_definition
    nil
  end

  def schedule_analyze
    @analyze_fired = true # For after_commit fix
    return true if self.parse_definition?
    return true if self.source.nil?

    if self.source.byte_size < 200000
      analyze
    else
      update_columns(parse_status: "waiting_analyze")
      AnalyzeJob.perform_later(self.class, self.id)
    end

  end

  def analyze

    if parse_definitions.nil?
      self.parse_status = "unparseable"
      save!
      return
    end

    self.parse_definition = {}
    self.parse_errors = nil
    self.parse_analyzed_at = nil
    self.parse_first_rows = nil
    self.parse_status = "analyzing"
    save!

    cols = nil; prev_cols = nil
    #parse_errors = []

    return if first_rows.blank?

    # Saving default parse definition
    if !default_parse_definition.nil?
      update_columns(parse_definition: default_parse_definition, parse_status: "analyzed")
      return self.parse_definition
    end

    first_rows.each_with_index do |row, i|
      # Looking for head
      break if i>9
      row = row.values if row.is_a?(Hash)
      prev_cols = cols if !cols.nil?
      cols = {}
      head = {}

      # Passing through cells to find data
      # To pass all iterations there should be at least 2 equal rows with the same data
      row.each_with_index do |cell, col|

        cell = cell.to_s.strip



        self.parse_definitions.each do |name, definition|
          definition = shortcut_defs(definition)
          head[name] = col if !cell.to_s[definition[:head]].nil? && head[name].nil?
          # We are only search required columns outside head
          cols[name] = col if required_columns.include?(name.to_sym) && !definition[:s].nil? && !cell.to_s[definition[:s]].nil? && cols[name].nil? && !cols.values.include?(col)
        end

      end

      # found heading which contains at least all required attributes
      if parse_definition_enough? head
        self.parse_definition["columns"] = head
        self.parse_definition["head"] = true
        break
      end

      next if prev_cols.nil?

      # Just rows with part number and (price/availablility) are enough
      next if !parse_definition_enough? cols

      # found 2 similar rows
      identical = true
      required_columns.each do |c|
        identical = false if prev_cols[c] != cols[c]
      end

      puts required_columns

      if identical
        self.parse_definition["columns"] = cols
        break
      end

    end

    self.parse_definition["columns"] = {} if self.parse_definition["columns"].nil?

    logger.info "Parse Definition: #{self.parse_definition}"

    self.parse_definition = {} if self.parse_definition.nil?
    self.parse_status = "needs_manual_analyze"

    self.save!
    return self.parse_definition

  end

  def set_parse_status
    self.parse_status = "pending" if parse_status.blank?
  end

  def required_columns
    rc = self.required_columns
    return rc if rc.kind_of?(Array)
    method(rc).call if rc.kind_of?(Symbol)
  end

  # Proxy to class method. Can be overritten to get dynamic definitions
  def parse_definitions
    return self.class.parse_definitions
  end

  # Proxy to class method. Can be overritten to get dynamic definitions
  def required_columns
    return self.class.required_columns
  end

  # def default_parse_definition
  #   da = self.class.default_parse_definition
  #   return da if da.kind_of?(Hash)
  #   method(da).call if da.kind_of?(Symbol)
  # end

  module ClassMethods

    def document_definitions defs, options=nil
      @parse_definitions = defs
      @required_columns = options[:required_columns] if !options[:required_columns].nil?
      @default_parse_definition = options[:default_parse_definition] if !options[:default_parse_definition].nil?
    end

    def parse_definitions
      return @parse_definitions if !@parse_definitions.nil?
      return superclass.parse_definitions rescue nil
    end

    def required_columns
      return @required_columns if !@required_columns.nil?
      return superclass.required_columns rescue nil
    end

    def default_parse_definition
      @default_parse_definition
    end

  end


end