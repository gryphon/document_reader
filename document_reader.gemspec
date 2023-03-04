$:.push File.expand_path("../lib", __FILE__)
require "document_reader/version"

Gem::Specification.new do |s|
  s.name        = "document_reader"
  s.version     = DocumentReader::VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = ["Grigory"]
  s.email       = ["mail@grigor.io"]
  s.homepage    = "http://github.com/gryphon/document_reader"
  s.summary     = %q{Tabular documents parser}
  s.description = %q{Tabular documents parser}
  
  #s.files         = `git ls-files`.split("\n")
  #s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.files = Dir['assets/**/*']
  
  s.require_paths = ["lib"]

  s.add_dependency "rails", "~> 7.0"

end
