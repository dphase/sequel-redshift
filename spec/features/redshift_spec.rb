require 'spec_helper'

describe "Redshift" do
  around do |example|
    DB.transaction do
      example.run
      raise Sequel::Rollback
    end
  end

  it "inserts new record" do
    DB.drop_table? :items
    DB.create_table :items do
      primary_key :id
      column :name, 'varchar(255)'
      column :description, :text
      column :created_at, :timestamp
      column :price, :decimal
    end

    expect { DB[:items].insert name: 'test' }.to change { DB[:items].count }.by(1)
  end
end
