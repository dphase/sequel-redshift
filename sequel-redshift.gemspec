# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'sequel/redshift/version'

Gem::Specification.new do |spec|
  spec.name          = "sequel-redshift"
  spec.version       = Sequel::Redshift::VERSION
  spec.authors       = ["Sergey Varaksin"]
  spec.email         = ["varaksin86@gmail.com"]
  spec.description   = %q{Amazon Redshift adapter for Sequel}
  spec.summary       = %q{Amazon Redshift adapter for Sequel}
  spec.homepage      = ""
  spec.license       = "MIT"

  spec.files         = `git ls-files`.split($/)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_dependency "pg"
  spec.add_dependency "sequel", '~> 5'

  spec.add_development_dependency "bundler", "~> 1.3"
  spec.add_development_dependency "rake"
  spec.add_development_dependency "rspec"
end
