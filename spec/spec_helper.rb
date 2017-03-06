require 'rubygems'
require 'bundler/setup'

Bundler.require :default

require 'sequel'
require 'logger'
require 'yaml'

$: << "."
require 'sequel/redshift'

config_path = File.join(File.dirname(__FILE__), "database.yml")

unless File.exists?(config_path)
  warn "spec/database.yml does not exist."
  warn "Create it based on spec/database.yml.example\nto conenct to a redshift cluster."
  exit 1
end

options = YAML.load(File.read(config_path))
options.merge(logger: Logger.new(STDOUT))

DB = Sequel.connect(options)
# Run all the tests in a specific test schema
DB.run "set search_path to 'sequel_redshift_adapter_test'"

