# frozen-string-literal: true
#
# The redshift_string_agg extension adds the ability to perform database-independent
# aggregate string concatentation on Amazon Redshift.

# Related module: Sequel::SQL::RedshiftStringAgg
module Sequel
  module SQL
    module Builders
      # Return a RedshiftStringAgg expression for an aggregate string concatentation.
      def redshift_string_agg(*a)
        RedshiftStringAgg.new(*a)
      end
    end

    # The RedshiftStringAgg class represents an aggregate string concatentation.
    class RedshiftStringAgg < GenericExpression
      include StringMethods
      include StringConcatenationMethods
      include InequalityMethods
      include AliasMethods
      include CastMethods
      include OrderMethods
      include PatternMatchMethods
      include SubscriptMethods

      # These methods are added to datasets using the redshift_string_agg
      # extension, for the purposes of correctly literalizing RedshiftStringAgg
      # expressions for the appropriate database type.
      module DatasetMethods
        # Append the SQL fragment for the RedshiftStringAgg expression to the SQL query.
        def redshift_string_agg_sql_append(sql, sa)
          unless db.adapter_scheme == :redshift
            raise Error, "redshift_string_agg is not implemented on #{db.adapter_scheme}"
          end

          expr = sa.expr
          separator = sa.separator || ","
          order = sa.order_expr
          distinct = sa.is_distinct?

          if distinct
            raise Error, "redshift_string_agg with distinct is not implemented on #{db.database_type}"
          end
          literal_append(sql, Function.new(:listagg, expr, separator))
          if order
            sql << " WITHIN GROUP (ORDER BY "
            expression_list_append(sql, order)
            sql << ")"
          else
            sql << " WITHIN GROUP (ORDER BY 1)"
          end
        end
      end

      # The string expression for each row that will concatenated to the output.
      attr_reader :expr

      # The separator between each string expression.
      attr_reader :separator

      # The expression that the aggregation is ordered by.
      attr_reader :order_expr

      # Set the expression and separator
      def initialize(expr, separator=nil)
        @expr = expr
        @separator = separator
      end

      # Whether the current expression uses distinct expressions 
      def is_distinct?
        @distinct == true
      end

      # Return a modified RedshiftStringAgg that uses distinct expressions
      def distinct
        sa = dup
        sa.instance_variable_set(:@distinct, true)
        sa
      end

      # Return a modified RedshiftStringAgg with the given order
      def order(*o)
        sa = dup
        sa.instance_variable_set(:@order_expr, o.empty? ? nil : o)
        sa
      end

      to_s_method :redshift_string_agg_sql
    end
  end

  Dataset.register_extension(:redshift_string_agg, SQL::RedshiftStringAgg::DatasetMethods)
end
