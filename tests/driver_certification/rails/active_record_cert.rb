# frozen_string_literal: true

require "active_record"
require "logger"

dsn = ARGV.fetch(0) do
  warn "usage: active_record_cert.rb DATABASE_URL"
  exit 2
end

def assert_equal(actual, expected, context)
  return if actual == expected

  raise "#{context}: expected #{expected.inspect}, got #{actual.inspect}"
end

ActiveRecord::Base.establish_connection(
  "url" => dsn,
  "adapter" => "postgresql",
  "prepared_statements" => true
)

if ENV["ULTRASQL_DRIVER_CERT_DEBUG"]
  ActiveRecord::Base.logger = Logger.new($stderr)
end

class RailsCert < ActiveRecord::Base
  self.table_name = "rails_cert"
end

begin
  rows = ActiveRecord::Base.connection.select_rows("SELECT id, name FROM users WHERE id = 1")
  assert_equal(rows, [[1, "Ada"]], "Rails ActiveRecord connection SELECT")

  ActiveRecord::Base.connection.create_table(:rails_cert, id: false) do |table|
    table.integer :id, null: false, primary_key: true
    table.text :label, null: false
  end

  RailsCert.create!(id: 1, label: "alpha")
  RailsCert.create!(id: 2, label: "beta")
  rows = RailsCert.where(id: 1).pluck(:id, :label)
  assert_equal(rows, [[1, "alpha"]], "Rails ActiveRecord model parameterized SELECT")
  rows = RailsCert.order(:id).pluck(:id, :label)
  assert_equal(rows, [[1, "alpha"], [2, "beta"]], "Rails ActiveRecord create/query")

  ActiveRecord::Base.transaction do
    RailsCert.create!(id: 3, label: "rollback")
    raise ActiveRecord::Rollback
  end
  assert_equal(RailsCert.count, 2, "Rails ActiveRecord transaction rollback")

  begin
    ActiveRecord::Base.transaction do
      ActiveRecord::Base.connection.execute("SELECT missing_column FROM rails_cert")
    end
  rescue ActiveRecord::StatementInvalid
    # Expected failure path; subsequent query proves connection recovery.
  else
    raise "Rails ActiveRecord expected missing-column failure"
  end
  rows = RailsCert.order(:id).pluck(:id)
  assert_equal(rows, [1, 2], "Rails ActiveRecord recovery after error")
ensure
  ActiveRecord::Base.connection_pool.disconnect!
end
