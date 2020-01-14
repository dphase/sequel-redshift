# frozen-string-literal: true
#
module Sequel
  module SQL
    module Builders
      # Return a median expression.
      def median(*a)
        Median.new(*a)
      end
    end

    # The Median class represents a median function for Redshift and Postgresql databases.
    class Median < GenericExpression
      include OrderMethods

      module DatasetMethods
        # Append the SQL fragment for the Median expression to the SQL query.
        def median_sql_append(sql, sa)
          if defined?(super)
            return super
          end

          expr = sa.expr

          case db_type = db.adapter_scheme
          when :postgres

            literal_append(sql, Function.new(:percentile_disc, 0.5))
            sql << " WITHIN GROUP (ORDER BY "
            identifier_append(sql, expr)
            sql << ")"

          when :redshift

            literal_append(sql, Function.new(:median, expr))

          else
            raise Error, "median is not implemented on #{db.database_type}"
          end
        end
      end

      attr_reader :expr

      # Set the expression and separator
      def initialize(expr)
        @expr = expr
      end

      to_s_method :median_sql
    end
  end

  Dataset.register_extension(:median, SQL::Median::DatasetMethods)
end
