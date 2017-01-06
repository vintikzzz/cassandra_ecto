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
      end
    end
  end
end
