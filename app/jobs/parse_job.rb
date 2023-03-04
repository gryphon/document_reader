class ParseJob < ApplicationJob
  queue_as :parse
  sidekiq_options retry: false

  def perform(cls, id)
    doc = cls.where(:id => id).first
    return true if doc.nil?
    doc.parse if !doc.nil?
  end
end
