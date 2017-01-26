defmodule CassandraEctoMigrationCQLSpec do
  import Cassandra.Ecto.Migration.CQL
  alias Ecto.Migration.{Table, Index}
  use ESpec, async: true
  import Ecto.Query

  describe "Cassandra.Ecto.Migration.CQL" do
    describe "to_cql/1" do
      context "with :create" do
        it "generates cql to create table" do
          to_cql({:create, %Table{name: :test}, [{:add, :id, :uuid, [primary_key: true]}, {:add, :value, :integer, []}]})
          |> to(eq "CREATE TABLE \"test\" (\"id\" uuid, \"value\" int, PRIMARY KEY (\"id\"))")
        end
        it "generates cql to create index" do
          to_cql({:create, %Index{table: :test, columns: ["a", "b"]}})
          |> to(eq "CREATE INDEX \"nil\" ON \"test\" (\"a\", \"b\")")
        end
        it "generates cql to create type" do
          to_cql({:create, %Table{name: :test, options: [type: :type]}, [{:add, :a, :integer, []}, {:add, :b, :integer, []}]})
          |> to(eq "CREATE TYPE \"test\" (\"a\" int, \"b\" int)")
        end
        it "generates cql to create materialized view" do
          to_cql({:create, %Table{name: :test_view, options: [type: :materialized_view, as: (from p in "test", select: {p.a, p.b}, where: not(is_nil(p.a)) and not(is_nil(p.b))), primary_key: {:a, :b}, comment: "test"]}, []})
          |> to(eq "CREATE MATERIALIZED VIEW \"test_view\" AS SELECT \"a\", \"b\" FROM \"test\" WHERE \"a\" IS NOT NULL AND \"b\" IS NOT NULL PRIMARY KEY (\"a\", \"b\") WITH COMMENT = 'test'")
        end
        it "generates cql to create user function in :java" do
          to_cql({:create, %Table{name: :fLog, prefix: :cycling, options: [type: :function, vars: [input: :double], returns: :double, language: :java, on_null_input: :called, as: "return Double.valueOf(Math.log(input.doubleValue()));"]}, []})
          |> to(eq "CREATE FUNCTION \"cycling\".\"fLog\" (\"input\" double) CALLED ON NULL INPUT RETURNS double LANGUAGE java AS $$return Double.valueOf(Math.log(input.doubleValue()));$$")
        end
        it "generates cql to create user function in :javascript" do
          to_cql({:create_if_not_exists, %Table{name: :left, prefix: :cycling, options: [type: :function, vars: [column: :text, num: :int], returns: :text, language: :javascript, on_null_input: :returns_null, as: "column.substring(0,num)"]}, []})
          |> to(eq "CREATE FUNCTION IF NOT EXISTS \"cycling\".\"left\" (\"column\" text, \"num\" int) RETURNS NULL ON NULL INPUT RETURNS text LANGUAGE javascript AS $$column.substring(0,num)$$")
        end
        it "generates cql to create aggregate", focus: true do
          to_cql({:create, %Table{name: :average, prefix: :cycling, options: [type: :aggregate, var: :int, sfunc: %Table{name: :avgState, options: [type: :function]}, stype: {:tuple, {:int, :bigint}}, finalfunc: %Table{name: :avgFinal, options: [type: :function]}, initcond: {0, 0}]}, []})
          |> to(eq "CREATE AGGREGATE \"cycling\".\"average\"(int) SFUNC \"avgState\" STYPE tuple<int, bigint> FINALFUNC \"avgFinal\" INITCOND (0, 0)")
        end
      end
      context "with :alter" do
        it "generates cql to alter table" do
          to_cql({:alter, %Table{name: :test, options: [comment: "test"]}, [{:add, :id, :uuid, []}, {:remove, :value}, {:modify, :value, :blob, []}]})
          |> to(eq "ALTER TABLE \"test\" ADD \"id\" uuid, DROP \"value\", ALTER \"value\" TYPE blob WITH COMMENT = 'test'")
        end
        it "generates cql to alter type" do
          to_cql({:alter, %Table{name: :test, options: [type: :type]}, [{:add, :id, :uuid, []}, {:remove, :value}, {:modify, :value, :blob, []}]})
          |> to(eq "ALTER TYPE \"test\" ADD \"id\" uuid, DROP \"value\", ALTER \"value\" TYPE blob")
        end
        it "generates cql to alter materialized view" do
          to_cql({:alter, %Table{name: :test, options: [type: :materialized_view, comment: "test"]}, []})
          |> to(eq "ALTER MATERIALIZED VIEW \"test\" WITH COMMENT = 'test'")
        end
      end
      context "with :drop" do
        it "generates cql to drop table" do
          to_cql({:drop, %Table{name: :test}})
          |> to(eq "DROP TABLE \"test\"")
        end
        it "generates cql to drop index" do
          to_cql({:drop, %Index{name: :test}})
          |> to(eq "DROP INDEX \"test\"")
        end
        it "generates cql to drop type" do
          to_cql({:drop, %Table{name: :test, options: [type: :type]}})
          |> to(eq "DROP TYPE \"test\"")
        end
        it "generates cql to drop materialized view" do
          to_cql({:drop, %Table{name: :test, options: [type: :materialized_view]}})
          |> to(eq "DROP MATERIALIZED VIEW \"test\"")
        end
        it "generates cql to drop user function" do
          to_cql({:drop, %Table{name: :test, options: [type: :function]}})
          |> to(eq "DROP FUNCTION \"test\"")
        end
        it "generates cql to drop user aggregation" do
          to_cql({:drop, %Table{name: :test, options: [type: :aggregate]}})
          |> to(eq "DROP AGGREGATE \"test\"")
        end
      end
    end
  end
end
