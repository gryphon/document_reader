require 'csv'

# Allows to analyze and parse documents
# Requires model to follow the constrants:
#
# It should have fields:
# "active storage": source
# "datetime": parse_analyzed_at, parse_started_at, parse_finished_at
# "integer": parse_percents
# "text": parse_first_rows, parse_definition
# "string": document_error
#
# It should have methods:
# self.definitions method to set hash of supported columns
# required_columns to set list of required columns
# 
# And finally, it should have process method to do all the job!

module DocumentReaderConcern
  extend ActiveSupport::Concern

  included do

    @@definitions = {}
    @@required_columns = []
    @@default_parse_definition = nil

    serialize :parse_definition, JSON
    serialize :parse_first_rows, JSON
    serialize :parse_errors, JSON

    before_validation :normalize_parse_definition
    before_validation :set_parse_status

    #has_many :parse_errors, class_name: DocumentReaderError, as: :documentable, dependent: :delete_all

    enum :source_type, {:xls => 0, :xlsx => 1, :csv => 2, :dsv => 3, :ssv => 4, :tsv => 5, nqcsv: 6}

    enum :parse_status, {
      pending: "pending", 
      waiting_analyze: "waiting_analyze", 
      analyzing: "analyzing", 
      needs_manual_analyze: "needs_manual_analyze",
      analyzed: "analyzed",
      waiting_parse: "waiting_parse", 
      parsing: "parsing", 
      parsed: "parsed"
    }, suffix: true
  
    scope :processed, -> { where.not(:parse_finished_at => nil) }
  end

  def encodings
    ["windows-1251","UTF-8"]
  end

  def source_path
    ActiveStorage::Blob.service.path_for(source.key)
  end

  def default_parse_definition
    nil
  end

  def normalize_parse_definition
    return true if self.parse_definition.nil?
    return true if self.parse_definition.empty?
    self.parse_definition.each do |k, d|
      next if k == "head"
      self.parse_definition[k] = d.to_i
    end
    self.parse_definition = self.parse_definition.symbolize_keys
  end

  def get_csv_options

    case self.source_type
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

    detect_type if !self.source_type?

    case self.source_type.to_s

    when "csv", "ssv", "dsv", "tsv", "nqcsv", "xls", "xlsx"

      csv_options = get_csv_options

      @sheets = []

      file = source_csv_file

      @rows_count = `wc -l "#{file}"`.strip.split(' ')[0].to_i

    else
      self.parse_errors = "cannot_detect_type"
      save!
      raise ArgumentError, "Cannot detect file type"
    end


  end

  # Detects file type by its extension

  def detect_type

    ext = File.extname source.filename.to_s
    case ext.downcase
    when '.xls'
      self.source_type = :xls
    when '.xlsx'
      self.source_type = :xlsx
    when '.csv'
      self.source_type = :csv
    when '.txt'
      self.source_type = :csv
    when '.zip'
      # Assuming that zipped file can only contain CSV data
      self.source_type = :csv
    end
    save if self.source_type?
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

  # Main enumerator while processing rows
  # Use it within the process method at your model
  # Returns only filtered, validated rows
  # Does some checks

  def each_row

    #self.parse_percents = 0
    self.parse_started_at = Time.now
    self.parse_finished_at = nil
    #self.parse_errors.delete_all
    save!

    if parse_definition.nil?
      logger.warn "Empty parse definition"
      return
    end

    @start_row = nil

    rows do |data, line|

      data = data.values if data.is_a?(Hash)

      logger.info "Got raw data row: #{data.inspect}"

      row = parse_row(data, line)

      #percents = ((line.to_f/rows_count.to_f)*100).ceil
      #self.update_column(:parse_percents, percents) if self.parse_percents!=percents

      yield(row, line) if !row.nil?

      #if (line>=100) && ((self.parse_errors.length*5) >= line)
        # 20% allowed
      #  self.document_error = "max_errors_reached"
      #  save!
      #  break
      #end

    end

  end


  # This one to avild race conditions
  
  def finish_process
    #self.parse_percents = 100
    self.parse_finished_at = Time.now
    save!
  end

  # Processes row according parse_definition

  def parse_row row, line
    out = {:raw_data => row.to_a, :line => line}

    # Checking for empty rows without any filtering or searching
    # Rows should be absolutely empty

    empty_row = true
    parse_definition.each do |field, index|
      if !row[index.to_i].to_s.strip.blank?
        empty_row = false
        break
      end
    end

    return nil if empty_row

    @start_row = line if @start_row.nil?

    parse_definition.each do |field, index|

      # Checking for floats with .0 at the end
      if row[index].class == Float
        row[index] = row[index].to_i if row[index] == row[index].to_i
      end

      defs = self.class.definitions[field]

      if defs.nil?
        error_row(out, "unknown_field", field)
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
      if out[c].blank?
        error_row(out, "not_all_required_fields_set", c.to_s) if @start_row < line
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

  def error_row(row, reason = nil, field = nil)
    #self.parse_errors.create!(line: row[:line]+1, row: row[:raw_data], reason: reason, field: field)
    #return false if self.parse_errors.reload.count >= 5
    return true
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
    return true if self.source.nil?
    #self.parse_percents = nil
    self.parse_finished_at = nil
    self.parse_started_at = Time.now
    self.parse_errors = nil
    #self.document_error_details = nil
    self.parse_status = "waiting_parse"
    save!
    ParseJob.perform_later(self.class, self.id)
  end

  # This is for case when user submits inverted column definition hash
  # during parse_definition process

  def columns= (cols)
    a = {}
    cols.each_with_index do |col, i|
      next if col.blank?
      a[col.to_sym] = i
    end
    self.parse_definition = a

    if self.parse_definition? && self.parse_definition_enough?
      self.parse_analyzed_at = Time.now
    else
      self.parse_analyzed_at = nil
    end

    save!
  end

  # Allows to have first N rows cached in a variable

  def first_rows

    return parse_first_rows if !parse_first_rows.nil?

    self.parse_first_rows = []

    rows do |row, i|

      row = row.values if row.is_a?(Hash)
      row = row.map(&:strip)

      self.parse_first_rows.push row
      break if i>10
    end

    save!

    return parse_first_rows
  end

  def extract_archive
    if (File.extname(source.filename.to_s) == ".zip")

      @tempfile = "/tmp/uploaded-file-#{self.id}.txt"

      puts "Extracting archive to #{@tempfile}"

      return @tempfile if (File.exist?(@tempfile))

      Zip::File.open(source_path) do |zipfile|

        entry = zipfile.glob('*.txt').first
        entry = zipfile.glob('*.csv').first if entry.nil?

        if !entry.nil?
          zipfile.extract(entry, @tempfile) 
          return @tempfile
        end

      end

    end

  end

  def convert_to_csv
    
    if (File.extname(source.filename.to_s) == ".xlsx")

      @tempfile = "/tmp/uploaded-file-#{self.id}.csv"

      puts "Extracting archive to #{@tempfile}"

      return @tempfile if File.exist?(@tempfile)

      system("xlsx2csv #{source_path} #{@tempfile}")

      return @tempfile if File.exist?(@tempfile)

    end

  end


  def rows


    file = source_csv_file

    begin

      i = 0

      logger.info "Reading CSV file: #{file}"

      CSV.foreach(file, **get_csv_options()) do |data|
        yield(data, i)
        i+=1
        logger.info "[=====] Processed document #{self.id} at row #{i}/#{@rows_count}..." if i % 10000 == 0
      end

    rescue Exception => e
      logger.error e.message
      self.parse_errors = {"cannot_parse_csv": nil}
      if i>0
        self.parse_errors["cannot_parse_csv"] = "Cannot parse CSV row at line #{i}: #{e.message}"
      end
      save!
      raise ArgumentError, "Cannot parse CSV at row #{i}"
    end

  end

  def source_csv_file
    if (File.extname(source.filename.to_s) == ".zip")
      file = extract_archive()
    elsif (File.extname(source.filename.to_s) == ".xlsx")
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

  def ready_for_process?
    (!parse_definition.nil?) && parse_analyzed_at? && parse_started_at.nil?
  end

  def processing?
    self.parse_started_at? && !self.parse_finished_at?
  end

  # Rewrite this method to add some logic to it
  def parse_definition_enough? parse_definition=nil
    parse_definition = self.parse_definition if parse_definition.nil?
    parse_definition = parse_definition.symbolize_keys
    return false if parse_definition.nil?
    required_columns.each do |c|
      return false if parse_definition.symbolize_keys[c.to_sym].blank?
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
    if !default_parse_definition.nil?
      self.parse_definition = default_parse_definition
      schedule_parse
    else
      if self.source.byte_size < 200000
        analyze
      else
        update_columns(parse_status: "waiting_analyze")
        AnalyzeJob.perform_later(self.class, self.id)
      end
    end
  end

  def analyze

    self.parse_definition = nil
    self.parse_analyzed_at = nil
    self.parse_first_rows = nil
    self.parse_status = "analyzing"
    save!

    cols = nil; prev_cols = nil
    #parse_errors = []

    return if first_rows.blank?

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

        self.class.definitions.each do |name, definition|
          definition = shortcut_defs(definition)
          head[name] = col if !cell.to_s[definition[:head]].nil? && head[name].nil?
          # We are only search required columns outside head
          cols[name] = col if required_columns.include?(name.to_sym) && !definition[:s].nil? && !cell.to_s[definition[:s]].nil? && cols[name].nil?
        end

      end

      # found heading which contains at least all required attributes
      if parse_definition_enough? head
        self.parse_definition = head
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
        self.parse_definition = cols
        break
      end

    end

    self.parse_definition = {} if self.parse_definition.nil?
    self.parse_status = "needs_manual_analyze"

    self.save!
    return self.parse_definition

  end

  def set_parse_status
    self.parse_status = "pending" if parse_status.blank?
  end

  def required_columns
    rc = self.class.required_columns
    return rc if rc.kind_of?(Array)
    method(rc).call if rc.kind_of?(Symbol)
  end

  # def default_parse_definition
  #   da = self.class.default_parse_definition
  #   return da if da.kind_of?(Hash)
  #   method(da).call if da.kind_of?(Symbol)
  # end

  module ClassMethods

    def document_definitions defs, options=nil
      @definitions = defs
      @required_columns = options[:required_columns] if !options[:required_columns].nil?
      @default_parse_definition = options[:default_parse_definition] if !options[:default_parse_definition].nil?
    end

    def definitions
      @definitions
    end

    def required_columns
      @required_columns
    end

    def default_parse_definition
      @default_parse_definition
    end

  end


end