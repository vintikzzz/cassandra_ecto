defmodule CassandraEctoAdapterCQLSpec do
  import Cassandra.Ecto.Adapter.CQL
  import Ecto.Query
  alias Cassandra.Ecto.Spec.Support.Schemas.Post
  use ESpec, async: true

  defp normalize(query, operation \\ :all, counter \\ 0) do
    {query, _params, _key} = Ecto.Query.Planner.prepare(query, operation, Cassandra.Ecto, counter)
    Ecto.Query.Planner.normalize(query, operation, Cassandra.Ecto, counter)
  end
  describe "Cassandra.Ecto.Adapter.CQL" do
    describe "to_cql/1" do
      context "with :all" do
        context "with :select" do
          it "generates cql with steriks if no select provided" do
            query = (from p in "posts") |> normalize
            expect(to_cql(:all, query))
            |> to(eq "SELECT * FROM \"posts\"")
          end
          it "generates cql with specified fields" do
            query = (from p in "posts", select: {p.id, p.title}) |> normalize
            expect(to_cql(:all, query))
            |> to(eq "SELECT \"id\", \"title\" FROM \"posts\"")
          end
          it "generates cql with all schema fields" do
            query = (from p in Post, select: p) |> normalize
            expect(to_cql(:all, query))
            |> to(eq "SELECT \"id\", \"title\", \"text\", \"public\", \"tags\", \"location\", \"links\", \"comments\", \"author_id\", \"inserted_at\", \"updated_at\" FROM \"posts\"")
          end
        end
        context "with :order_by" do
          it "generates cql" do
            query = (from p in "posts", order_by: p.title) |> normalize
            expect(to_cql(:all, query))
            |> to(eq "SELECT * FROM \"posts\" ORDER BY \"title\"")
          end
          it "generates cql with specified order" do
            query = (from p in "posts", order_by: [desc: p.title]) |> normalize
            expect(to_cql(:all, query))
            |> to(eq "SELECT * FROM \"posts\" ORDER BY \"title\" DESC")
          end
          it "generates cql with multiple order fields" do
            query = (from p in "posts", order_by: [desc: p.title, asc: p.id]) |> normalize
            expect(to_cql(:all, query))
            |> to(eq "SELECT * FROM \"posts\" ORDER BY \"title\" DESC, \"id\"")
          end
        end
        context "with :limit" do
          it "generates cql" do
            query = (from p in "posts", limit: 1) |> normalize
            expect(to_cql(:all, query))
            |> to(eq "SELECT * FROM \"posts\" LIMIT 1")
          end
        end
        context "with :distinct" do
          it "generates cql" do
            query = (from p in "posts", select: p.id, distinct: true) |> normalize
            expect(to_cql(:all, query))
            |> to(eq "SELECT DISTINCT \"id\" FROM \"posts\"")
          end
        end
        context "with :where" do
          it "generates cql with binary clauses" do
            query = (from p in "posts", where: p.id >= 1 and p.title == "abra") |> normalize
            expect(to_cql(:all, query))
            |> to(eq "SELECT * FROM \"posts\" WHERE ((\"id\" >= 1) AND (\"title\" = 'abra'))")
          end
          it "generates cql with in clauses" do
            query = (from p in "posts", where: p.id in [1, 2]) |> normalize
            expect(to_cql(:all, query))
            |> to(eq "SELECT * FROM \"posts\" WHERE (\"id\" IN (1, 2))")
          end
          it "alters :in to :contains in array field search" do
            query = (from p in "posts", where: "abra" in p.tags) |> normalize
            expect(to_cql(:all, query))
            |> to(eq "SELECT * FROM \"posts\" WHERE (\"tags\" CONTAINS 'abra')")
          end
          it "supports fragments" do
            query = (from p in "posts", where: p.id > fragment("token(?)", 1)) |> normalize
            expect(to_cql(:all, query))
            |> to(eq "SELECT * FROM \"posts\" WHERE (\"id\" > token(1))")
          end
        end
        context "with :offset" do
          it "fails to generate cql"  do
            query = (from p in "posts", offset: 1) |> normalize
            expect(fn -> to_cql(:all, query) end)
            |> to(raise_exception())
          end
        end
        context "with :group_by" do
          it "fails to generate cql"  do
            query = (from p in "posts", group_by: p.id) |> normalize
            expect(fn -> to_cql(:all, query) end)
            |> to(raise_exception())
          end
        end
        context "with :having" do
          it "fails to generate cql"  do
            query = (from p in "posts", having: p.id > 10) |> normalize
            expect(fn -> to_cql(:all, query) end)
            |> to(raise_exception())
          end
        end
        context "with :join" do
          it "fails to generate cql"  do
            query = (from p in "posts",
                    join: u in "users", on: p.author_id == u.user_id) |> normalize
            expect(fn -> to_cql(:all, query) end)
            |> to(raise_exception())
          end
        end
        context "with :lock" do
          it "fails to generate cql"  do
            query = (from p in "posts", lock: "FOR SHARE NOWAIT") |> normalize
            expect(fn -> to_cql(:all, query) end)
            |> to(raise_exception())
          end
        end
        context "with :allow_filtering" do
          it "generates cql"  do
            query = (from p in "posts", where: p.id == 1) |> normalize
            expect(to_cql(:all, query, allow_filtering: true))
            |> to(eq "SELECT * FROM \"posts\" WHERE (\"id\" = 1) ALLOW FILTERING")
          end
        end
        context "with :per_partition_limit" do
          it "generates cql"  do
            query = (from p in "posts") |> normalize
            expect(to_cql(:all, query, per_partition_limit: 2))
            |> to(eq "SELECT * FROM \"posts\" PER PARTITION LIMIT 2")
          end
        end
      end
      context "with :insert" do
        context "with :autogenerate_id" do
          it "generates cql with now() for option binary_id: :now" do
            expect(to_cql(:insert, %{autogenerate_id: {:id, :binary_id}, source: {nil, "posts"}}, [id: nil, title: "a", text: "b"], {:raise, [], []}, [binary_id: :now]))
            |> to(eq "INSERT INTO \"posts\" (\"id\", \"title\", \"text\") VALUES (now(), ?, ?) IF NOT EXISTS")
          end
          it "generates cql with uuid() for option binary_id: :uuid" do
            expect(to_cql(:insert, %{autogenerate_id: {:id, :binary_id}, source: {nil, "posts"}}, [id: nil, title: "a", text: "b"], {:raise, [], []}, [binary_id: :uuid]))
            |> to(eq "INSERT INTO \"posts\" (\"id\", \"title\", \"text\") VALUES (uuid(), ?, ?) IF NOT EXISTS")
          end
          it "fails with autogenerate_id: {_, :id}" do
            expect(fn -> to_cql(:insert, %{autogenerate_id: {:id, :id}, source: {nil, "posts"}}, [id: nil, title: "a", text: "b"], {:raise, [], []}, []) end)
            |> to(raise_exception(ArgumentError))
          end
        end
        context "with on_conflict :raise" do
          it "generates cql with \"IF NOT EXISTS\"" do
            expect(to_cql(:insert, %{autogenerate_id: nil, source: {nil, "posts"}}, [title: "a", text: "b"], {:raise, [], []}, []))
            |> to(eq "INSERT INTO \"posts\" (\"title\", \"text\") VALUES (?, ?) IF NOT EXISTS")
          end
        end
        context "with on_conflict :nothing" do
          it "generates cql for Cassandra upsert" do
            expect(to_cql(:insert, %{autogenerate_id: nil, source: {nil, "posts"}}, [title: "a", text: "b"], {:nothing, [], []}, []))
            |> to(eq "INSERT INTO \"posts\" (\"title\", \"text\") VALUES (?, ?)")
          end
        end
        context "with on_conflict :nothing and if: :not_exists" do
          it "generates cql with \"IF NOT EXISTS\"" do
            expect(to_cql(:insert, %{autogenerate_id: nil, source: {nil, "posts"}}, [title: "a", text: "b"], {:nothing, [], []}, if: :not_exists))
            |> to(eq "INSERT INTO \"posts\" (\"title\", \"text\") VALUES (?, ?) IF NOT EXISTS")
          end
        end
        context "with :prefix" do
          it "generates cql with Cassandra keyspace" do
            expect(to_cql(:insert, %{autogenerate_id: nil, source: {"test", "posts"}}, [title: "a", text: "b"], {:nothing, [], []}, []))
            |> to(eq "INSERT INTO \"test\".\"posts\" (\"title\", \"text\") VALUES (?, ?)")
          end
        end
        context "with :timestamp and :ttl" do
          it "generates cql" do
            expect(to_cql(:insert, %{autogenerate_id: nil, source: {nil, "posts"}}, [title: "a", text: "b"], {:raise, [], []}, ttl: 86400, timestamp: 123456789))
            |> to(eq "INSERT INTO \"posts\" (\"title\", \"text\") VALUES (?, ?) IF NOT EXISTS USING TTL 86400 AND TIMESTAMP 123456789")
          end
        end
      end
      context "with :update" do
        context "without :prefix" do
          it "generates cql" do
            expect(to_cql(:update, %{source: {nil, "posts"}}, [title: "a", text: "b"], [id: 1], []))
            |> to(eq "UPDATE \"posts\" SET \"title\" = ?, \"text\" = ? WHERE \"id\" = ?")
          end
        end
        context "with :prefix" do
          it "generates cql with Cassandra keyspace" do
            expect(to_cql(:update, %{source: {"test", "posts"}}, [title: "a", text: "b"], [id: 1], []))
            |> to(eq "UPDATE \"test\".\"posts\" SET \"title\" = ?, \"text\" = ? WHERE \"id\" = ?")
          end
        end
        context "with multiple filters" do
          it "generates cql" do
            expect(to_cql(:update, %{source: {nil, "posts"}}, [title: "a", text: "b"], [id: 1, title: "c"], []))
            |> to(eq "UPDATE \"posts\" SET \"title\" = ?, \"text\" = ? WHERE \"id\" = ? AND \"title\" = ?")
          end
        end
        context "with if: :exists" do
          it "generates cql with \"IF EXISTS\"" do
            expect(to_cql(:update, %{source: {nil, "posts"}}, [title: "a", text: "b"], [id: 1, title: "c"], if: :exists))
            |> to(eq "UPDATE \"posts\" SET \"title\" = ?, \"text\" = ? WHERE \"id\" = ? AND \"title\" = ? IF EXISTS")
          end
        end
        context "with if: :not_exists" do
          it "generates cql with \"IF NOT EXISTS\"" do
            expect(to_cql(:update, %{source: {nil, "posts"}}, [title: "a", text: "b"], [id: 1, title: "c"], if: :not_exists))
            |> to(eq "UPDATE \"posts\" SET \"title\" = ?, \"text\" = ? WHERE \"id\" = ? AND \"title\" = ? IF NOT EXISTS")
          end
        end
        context "with conditions in :if" do
          it "generates cql" do
            expect(to_cql(:update, %{source: {nil, "posts"}}, [title: "a", text: "b"], [id: 1, title: "c"], if: [title: "c", text: "d"]))
            |> to(eq "UPDATE \"posts\" SET \"title\" = ?, \"text\" = ? WHERE \"id\" = ? AND \"title\" = ? IF \"title\" = ? AND \"text\" = ?")
          end
        end
        context "with :update_all" do
          context "with :inc" do
            it "generates cql" do
              query = (from p in "posts", where: p.id == 1, update: [inc: [visits: 5]]) |> normalize(:update_all)
              expect(to_cql(:update_all, query))
              |> to(eq "UPDATE \"posts\" SET \"visits\" = \"visits\" + 5 WHERE (\"id\" = 1)")
            end
          end
          context "with :set" do
            it "generates cql" do
              query = (from p in "posts", where: p.id == 1, update: [set: [title: "a", text: "b"]]) |> normalize(:update_all)
              expect(to_cql(:update_all, query))
              |> to(eq "UPDATE \"posts\" SET \"title\" = 'a', \"text\" = 'b' WHERE (\"id\" = 1)")
            end
          end
          # context "with :push" do
          #   it "generates cql", focus: true do
          #     id = Ecto.UUID.bingenerate()
          #     query = (from p in Post, where: p.id == ^id, update: [push: [tags: "a"]]) |> normalize(:update_all)
          #     expect(to_cql(:update_all, query))
          #     |> to(eq "UPDATE \"posts\" SET \"tags\" + {'a'} WHERE (\"id\" = ?)")
          #   end
          # end
          context "with if: :exists" do
            it "generates cql" do
              query = (from p in "posts", where: p.id == 1, update: [set: [title: "a", text: "b"]]) |> normalize(:update_all)
              expect(to_cql(:update_all, query, if: :exists))
              |> to(eq "UPDATE \"posts\" SET \"title\" = 'a', \"text\" = 'b' WHERE (\"id\" = 1) IF EXISTS")
            end
          end
        end
        context "with :delete_all" do
          it "generates cql" do
            query = (from p in "posts", where: p.id == 1) |> normalize(:delete_all)
            expect(to_cql(:delete_all, query))
            |> to(eq "DELETE FROM \"posts\" WHERE (\"id\" = 1)")
          end
        end
      end
    end
  end
end
