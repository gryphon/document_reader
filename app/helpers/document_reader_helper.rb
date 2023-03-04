module DocumentReaderHelper

  def col_select_tag obj, i, val

    cls = obj.class

    options = []
    cls.definitions.each do |i, d|
      loc = d[:name]
      loc = translate_document_column cls, i if loc.blank?
      rc = obj.required_columns.include?(i)
      options.push [loc+(rc ? " *" : ""), i.to_s, {:rc => rc ? "1" : "0"}]
    end

    obj_name = cls.model_name.to_s.tableize.singularize

    select_tag(
      "#{obj_name}[columns][]", 
      options_for_select(options, val), 
      :prompt => t("document_reader.select_column_type"), :class => "form-control document-reader-select"
    )
  end

  def translate_document_column cls, i
    return "" if i.blank?
    return t("document_reader."+cls.name.underscore+"."+i.to_s)
  end

  def document_error_message cls, err
    if err.reason.blank?
      out = t("document_reader.errors.error")
    else
      out = t("document_reader.errors."+err.reason)
    end
    out += " ("+translate_document_column(cls, err.field)+")" if !err.field.nil?
    out
  end

  # Returns updateable progress

  def document_processing_status document, params = {}

    p = {}
    # Use update path to let JS update status periodically
    p["update-url"] = params[:update].blank? ? "" : params[:update]

    # Use refresh param to refresh page after progress is finished
    p["refresh"] = true if !params[:refresh].blank? && params[:refresh]

    return content_tag(:span, processing_status(document, params), class: "document-status-update", data: p)
  end

  # Returns status icon only

  def processing_status document, params = {}

    # document-status-update adds javascript command to update status!

    out = ""
    if document.process_finished_at?
      # Finished
      if document.document_error?
        # Fatal error
        return content_tag(:i, "", :class => "fa danger fa-warning")
      elsif document.process_errors.count > 0
        # Some errors
        return content_tag(:i, "", :class => "fa warning fa-warning")
      else
        # Everything is nice!
        return content_tag(:i, "", :class => "fa fa-lg fa-check success", data: {refresh: true})
      end
    elsif document.process_started_at?
      # Processing
      return content_tag(:span, progress_bar(document.process_percents), data: {update: true})
    elsif !document.analytics.nil?
      if document.checked_at?
        # Ready for processing
        # Not really meaningful
        return t("documents.ready_for_processing")
      else
        if document.analytics_enough?
          # Requires passing
          return link_to t("documents.pass_analytics"), params[:analyze_path]
        else
          return link_to params[:analyze_path] do
            content_tag(:i, "", :class=> "fa fa-warning")
            t("documents.pass_analytics")
          end
        end
      end
    else
      # Analyzing
      content_tag(:span, data: {update: true}) do
        concat content_tag(:i, "", :class=> "fa fa-spin fa-spinner")
        concat " "
        concat t("documents.reading_file")
      end
    end

  end




end
