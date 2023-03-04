require 'rubygems'
require 'rails'

module DocumentReader
  module Rails
    class Engine < ::Rails::Engine
      initializer 'document_reader.assets' do |app|
        %w(stylesheets javascripts).each do |sub|
          app.config.assets.paths << root.join('assets', sub).to_s
        end
      end
    end
  end
end
