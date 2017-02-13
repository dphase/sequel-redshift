require 'rubygems'
require 'bundler/setup'

Bundler.require :default

require 'sequel'
require 'logger'

options = {
  logger: Logger.new(STDOUT)
}
DB = Sequel.connect('redshift://remind101:wvpfSzw2TT8COE@r101-dw-production.ck1sr65ao7pg.us-east-1.redshift.amazonaws.com:5439/test1', options)
