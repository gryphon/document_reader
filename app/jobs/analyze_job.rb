class AnalyzeJob < ApplicationJob
  queue_as :analyze
  sidekiq_options retry: false

  def perform(cls, id)
    doc = cls.where(:id => id).first
    return true if doc.nil?
    doc.analyze if !doc.nil?
  end
end