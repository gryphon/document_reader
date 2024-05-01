module DocumentReader
  module ApplicationHelper

    def document_reader_errors document
      render "document_reader/parse_errors", obj: document
    end

    def document_reader_analyze document
      render "document_reader/analyze", obj: @document do
        yield
      end
    end

  end
  
end
