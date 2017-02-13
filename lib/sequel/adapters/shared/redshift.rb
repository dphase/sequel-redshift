module Sequel
  module Redshift
    module DatabaseMethods

      REDSHIFT_STRING_MAXIMUM_SIZE = 255

      # Redshift does not support arrays (type of pg_index.indkey is INT2VECTOR),
      # and because of that we can't determine the primary key - so we set it to false.
      #
      # The workaround for now is to use `set_primary_key` inside the sequel model.
      def schema_parse_table(table_name, opts)
        m  = output_identifier_meth(opts[:dataset])

        ds = metadata_dataset.select(:pg_attribute__attname___name,
            SQL::Cast.new(:pg_attribute__atttypid, :integer).as(:oid),
            SQL::Cast.new(:basetype__oid, :integer).as(:base_oid),
            SQL::Function.new(:format_type, :basetype__oid, :pg_type__typtypmod).as(:db_base_type),
            SQL::Function.new(:format_type, :pg_type__oid, :pg_attribute__atttypmod).as(:db_type),
            SQL::Function.new(:pg_get_expr, :pg_attrdef__adbin, :pg_class__oid).as(:default),
            SQL::BooleanExpression.new(:NOT, :pg_attribute__attnotnull).as(:allow_null)).
          from(:pg_class).
          join(:pg_attribute, :attrelid=>:oid).
          join(:pg_type, :oid=>:atttypid).
          left_outer_join(:pg_type___basetype, :oid=>:typbasetype).
          left_outer_join(:pg_attrdef, :adrelid=>:pg_class__oid, :adnum=>:pg_attribute__attnum).
          filter(:pg_attribute__attisdropped=>false).
          filter{|o| o.pg_attribute__attnum > 0}.
          filter(:pg_class__oid=>regclass_oid(table_name, opts)).
          order(:pg_attribute__attnum)

        ds.map do |row|
          row[:default] = nil if blank_object?(row[:default])
          if row[:base_oid]
            row[:domain_oid] = row[:oid]
            row[:oid] = row.delete(:base_oid)
            row[:db_domain_type] = row[:db_type]
            row[:db_type] = row.delete(:db_base_type)
          else
            row.delete(:base_oid)
            row.delete(:db_base_type)
          end
          row[:type] = schema_column_type(row[:db_type])
          row[:primary_key] = false
          [m.call(row.delete(:name)), row]
        end
      end

      # Redshift changes text to varchar with maximum size of 256, and it complains if you will give text column
      def type_literal_generic_string(column)
        "#{ column[:fixed] ? 'char' : 'varchar' }(#{ column[:size] || REDSHIFT_STRING_MAXIMUM_SIZE })"
      end
    end
  end
end
