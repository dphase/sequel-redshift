require 'spec_helper'

describe 'Extensions' do
  describe 'string_agg' do
    before { DB.extension :string_agg }

    it 'generates correct sql for Redshift' do
      allow(DB).to receive(:adapter_scheme).and_return(:redshift)
      expect(
        DB[:test].
          select(
            Sequel.
              string_agg(Sequel[:revenue].cast_string).
              order(Sequel.asc(:period_start), Sequel.asc(:sub_period_start)).
              as(:relative)
          ).sql).to eq(
            "SELECT listagg(CAST(\"revenue\" AS varchar(255)), ',') WITHIN GROUP (ORDER BY \"period_start\" ASC, \"sub_period_start\" ASC) AS \"relative\" FROM \"test\""
          )
    end

    it 'generates correct sql for Postgresql' do
      allow(DB).to receive(:adapter_scheme).and_return(:postgres)
      expect(
        DB[:test].
          select(
            Sequel.
              string_agg(Sequel[:revenue].cast_string).
              order(Sequel.asc(:period_start), Sequel.asc(:sub_period_start)).
              as(:relative)
          ).sql).to eq(
            "SELECT string_agg(CAST(\"revenue\" AS varchar(255)), ',' ORDER BY \"period_start\" ASC, \"sub_period_start\" ASC) AS \"relative\" FROM \"test\""
          )
    end
  end

  describe 'median' do
    before { DB.extension :median }

    it 'generates correct sql for Redshift' do
      allow(DB).to receive(:adapter_scheme).and_return(:redshift)
      expect(
        DB[:test].
          select(
            Sequel.
              median(:revenue)
          ).sql).to eq(
            "SELECT median(\"revenue\") FROM \"test\""
          )
    end

    it 'generates correct sql for Postgresql' do
      allow(DB).to receive(:adapter_scheme).and_return(:postgres)
      expect(
        DB[:test].
          select(
            Sequel.
              median(:revenue)
          ).sql).to eq(
            "SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY \"revenue\") FROM \"test\""
          )
    end
  end
end
