class ParseJob < ApplicationJob
  queue_as :parse
  sidekiq_options retry: false

  def perform(cls, id)
    doc = cls.where(:id => id).first
    doc.update_columns(parse_status: "parsing")
    return true if doc.nil?
    if doc.parser_class == cls
      doc.parse
    else
      doc.parser_class.parse(doc)
    end
  end
end
