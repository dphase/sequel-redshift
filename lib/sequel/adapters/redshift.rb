require 'sequel/adapters/postgres'
require 'sequel/adapters/shared/redshift'

module Sequel
  module Redshift
    include Postgres

    class Database < Postgres::Database
      include Sequel::Redshift::DatabaseMethods

      set_adapter_scheme :redshift

      DIST_KEY = ' DISTKEY'.freeze
      SORT_KEY = ' SORTKEY'.freeze

      # The order of column modifiers to use when defining a column.
      COLUMN_DEFINITION_ORDER = [:collate, :default, :primary_key, :dist_key, :sort_key, :null, :unique, :auto_increment, :references]

      def dataset_class_default
        Sequel::Redshift::Dataset
      end

      # We need to change these default settings because they correspond to
      # Postgres configuration variables which do not exist in Redshift
      def adapter_initialize
        @opts.merge!(
          force_standard_strings: false,
          client_min_messages:    false
        )
        super
      end

      def table_exists?(name)
        sql = <<~SQL
          SELECT EXISTS (
            SELECT * FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE  n.nspname = 'public'
            AND  c.relname = '#{name}'
            AND  c.relkind = 'r'
          );
        SQL
        fetch(sql).first.fetch(:"?column?")
      end

      def column_definition_primary_key_sql(sql, column)
        if column[:primary_key] && column[:type] == Integer
          sql << ' IDENTITY'
        end
      end

      # Add DISTKEY SQL fragment to column creation SQL.
      def column_definition_dist_key_sql(sql, column)
        if column[:dist_key]
          sql << DIST_KEY
        end
      end

      # Add SORTKEY SQL fragment to column creation SQL.
      def column_definition_sort_key_sql(sql, column)
        if column[:sort_key]
          sql << SORT_KEY
        end
      end

      def serial_primary_key_options
        # redshift doesn't support serial type
        super.merge(serial: false)
      end

      # DROP TABLE IF EXISTS is now supported by Redshift
      def supports_drop_table_if_exists?
        true
      end

      # None of the alter table operation are combinable.
      def combinable_alter_table_op?(op)
        false
      end

      def supports_transactional_ddl?
        false
      end

      def supports_savepoints?
        false
      end
    end

    class Dataset < Postgres::Dataset
      Database::DatasetClass = self

      Dataset.def_sql_method(self, :select, [['if opts[:values]', %w'values order limit'], ['elsif server_version >= 80400', %w'with select distinct columns from join where group having window compounds order limit lock'], ['else', %w'with select distinct columns from join where group having compounds order limit lock']])

      def initialize(db)
        @db = db
        @opts = {disable_insert_returning: true}.freeze
        @cache = {}
        freeze
      end

      def supports_cte?(type = :select)
        true if type == :select
      end

      def supports_insert_select?
        false
      end

      def supports_returning?(type)
        false
      end

      def supports_window_functions?
        true
      end
    end
  end
end
