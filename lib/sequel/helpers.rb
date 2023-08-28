module Sequel
  class Database
    def redshift?
      adapter_scheme == :redshift
    end
  end
end
