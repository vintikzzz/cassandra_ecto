defmodule CassandraEctoAdapterSpec do
  import Ecto.Query
  use ESpec, async: false
  alias Cassandra.Ecto.Spec.Support.Schemas.{Post, PostStats}
  import Cassandra.Ecto.Spec.Support.Factories

  describe "Cassandra.Ecto", context_tag: :db do
    describe "Adapter behaviour" do
      before do
        case Ecto.Migrator.up(TestRepo, 0, Cassandra.Ecto.Spec.Support.Migrations.PostsMigration, log: false) do
          :already_up -> :ok
          :ok         -> :ok
        end
      end
      describe "execute/6" do
        context "with :all" do
          it "returns empty list if no data present in database" do
            expect(TestRepo.all(Post)) |> to(eq [])
            expect(TestRepo.all(from p in Post)) |> to(eq [])
          end
          it "returns inserted data" do
            post = factory(:post)
            post = TestRepo.insert!(post)
            fetched_post = TestRepo.all(Post) |> List.first
            expect(fetched_post) |> to(eq post)
          end
          it "fails with wrong schema" do
            expect(fn -> TestRepo.all("posts", prefix: "oops") end)
            |> to(raise_exception())
          end
          context "with in clause" do
            let :id, do: Ecto.UUID.bingenerate()
            let :random_ids, do:
              Enum.map((1..3), fn _ -> Ecto.UUID.bingenerate() end)
              |> Enum.to_list
            before do: TestRepo.insert!(factory(:post, %{id: id}, with: [:tags]))

            it "raises error with empty arguments" do
              expect(fn -> TestRepo.all from p in Post, where: p.id in [] end)
              |> to(raise_exception())
            end
            it "fetches nothing with absent ids" do
              [id1, id2, id3] = random_ids
              expect(TestRepo.all from p in Post, where: p.id in [^id1, ^id2, ^id3])
              |> to(eq [])
            end
            it "fetches record with proper id" do
              expect((TestRepo.all from p in Post, where: p.id in [^id]) |> List.first |> Map.get(:id))
              |> to(eq id)
            end
            it "fetches record with proper id and random ids" do
              [id1, id2, _id3] = random_ids
              expect((TestRepo.all from p in Post, where: p.id in [^id, ^id1, ^id2]) |> List.first |> Map.get(:id))
              |> to(eq id)
            end
            it "fetches record by searching value in array field" do
              expect(TestRepo.all((from p in Post, where: "abra" in p.tags), allow_filtering: true) |> List.first |> Map.get(:id))
              |> to(eq id)
            end
            it "fetches record with multiple where clauses" do
              expect(TestRepo.all((from p in Post, where: "abra" in p.tags and p.text == "test" and p.title == "test"), allow_filtering: true) |> List.first |> Map.get(:id))
              |> to(eq id)
            end
            it "writes log to io in :cyan when logging enabled" do
              message = capture_log(fn ->
                TestRepo.all((from p in Post), log: true)
              end)
              expect(message) |> to(start_with "\e[36")
            end
          end
        end
        context "with :update_all" do
          it "updates all records that matches query" do
            TestRepo.insert_all(Post, factory(:posts), on_conflict: :nothing)
            post = TestRepo.one(from p in Post, limit: 1)
            TestRepo.update_all((from p in Post, where: p.id == ^post.id), set: [title: "x", text: "y", tags: ["oh", "my", "god"]])
            post = TestRepo.one(from p in Post, where:  p.id == ^post.id)
            expect(post.title) |> to(eq "x")
            expect(post.text)  |> to(eq "y")
            expect(post.tags)  |> to(eq ["god", "my", "oh"])
          end
          it "increments counters with :inc" do
            post = factory(:post)
            post = TestRepo.insert!(post)
            TestRepo.update_all((from p in PostStats, where: p.id == ^post.id), [inc: [visits: 5]], if: nil)
            stats = TestRepo.one(from p in PostStats, where: p.id == ^post.id)
            expect(stats.visits) |> to(eq 5)
          end
        end
        context "with :delete_all" do
          it "deletes all records that matches query" do
            TestRepo.insert_all(Post, factory(:posts), on_conflict: :nothing)
            post = TestRepo.one(from p in Post, limit: 1)
            TestRepo.delete_all(from p in Post, where: p.id == ^post.id)
            expect(TestRepo.all(from p in Post, where: p.id == ^post.id)) |> to(eq [])
          end
        end
      end
      describe "insert/6" do
        it "inserts record" do
          post = factory(:post)
          post = TestRepo.insert!(post)
          fetched_post = TestRepo.one(Post)
          expect(fetched_post) |> to(eq post)
        end
        it "inserts record with now() if has option binary_id: :now" do
          post = factory(:post)
          post = TestNowRepo.insert!(post, binary_id: :now)
          fetched_post = TestNowRepo.one(Post)
          expect(fetched_post) |> to_not(eq post)
        end
        it "inserts record with now() if repo has option binary_id: :now" do
          post = factory(:post)
          post = TestNowRepo.insert!(post)
          fetched_post = TestNowRepo.one(Post)
          expect(fetched_post) |> to_not(eq post)
        end
        it "inserts record with ttl and timestamp" do
          post = factory(:post)
          post = TestRepo.insert!(post, on_conflict: :nothing, ttl: 1000, timestamp: :os.system_time(:micro_seconds))
          fetched_post = TestRepo.one(Post)
          expect(fetched_post) |> to(eq post)
        end
        it "inserts tuples, maps and sets" do
          post = factory(:post, %{}, with: [:location, :tags, :links])
          post = TestRepo.insert!(post)
          fetched_post = TestRepo.one(Post)
          expect(fetched_post) |> to(eq post)
        end
        it "raises error when record already exists with on_conflict: :raise" do
          post = factory(:post)
          post = TestRepo.insert!(post)
          post = factory(:updated_post, post)
          expect(fn -> TestRepo.insert!(post) end) |> to(raise_exception(ArgumentError))
        end
        it "upserts data with on_conflict: :nothing" do
          init_post = factory(:post)
          init_post = TestRepo.insert!(init_post)
          updated_post = factory(:updated_post, init_post)
          updated_post = TestRepo.insert!(updated_post, on_conflict: :nothing, if: :not_exists)
          fetched_post = TestRepo.one(Post)
          expect(updated_post) |> to(eq fetched_post)
        end
        it "returns existing record with on_conflict: :nothing and if: :not_exists" do
          init_post = factory(:post)
          init_post = TestRepo.insert!(init_post)
          updated_post = factory(:updated_post, init_post)
          updated_post = TestRepo.insert!(updated_post, on_conflict: :nothing, if: :not_exists)
          fetched_post = TestRepo.one(Post)
          expect(fetched_post) |> to(eq init_post)
          expect(updated_post) |> to(eq init_post)
        end
        it "doesn't raise error and upserts data with repo option upsert: true" do
          init_post = factory(:post)
          init_post = TestUpsertRepo.insert!(init_post)
          updated_post = factory(:updated_post, init_post)
          updated_post = TestUpsertRepo.insert!(updated_post)
          fetched_post = TestUpsertRepo.one(Post)
          expect(updated_post) |> to(eq fetched_post)
        end
      end
      describe "update/6" do
        it "updates record" do
          post = factory(:post)
          post = TestRepo.insert!(post)
          post = Ecto.Changeset.change post, text: "updated text"
          post = TestRepo.update!(post)
          fetched_post = TestRepo.one(Post)
          expect(fetched_post) |> to(eq post)
        end
        it "doesn't upsert data by default" do
          post = %Post{id: Ecto.UUID.bingenerate()}
          |> Ecto.Changeset.cast(%{title: "test", text: "upserted text"}, [:title, :text])
          |> TestRepo.update!
          fetched_post = TestRepo.one(Post)
          expect(fetched_post) |> to_not(eq post)
        end
        it "upserts data with repo option upsert: true" do
          post = %Post{id: Ecto.UUID.bingenerate()}
          |> Ecto.Changeset.cast(%{title: "test", text: "upserted text"}, [:title, :text])
          |> TestUpsertRepo.update!
          fetched_post = TestUpsertRepo.one(Post)
          expect(fetched_post) |> to(eq post)
        end
        it "upserts data with if: nil" do
          post = %Post{id: Ecto.UUID.bingenerate()}
          |> Ecto.Changeset.cast(%{title: "test", text: "upserted text"}, [:title, :text])
          |> TestRepo.update!(if: nil)
          fetched_post = TestRepo.one(Post)
          expect(fetched_post) |> to(eq post)
        end
        it "doesnt't update data with failed if condition" do
          post = factory(:post)
          post = TestRepo.insert!(post)
          updated_post = Ecto.Changeset.change post, text: "updated text", title: "updated title"
          updated_post = TestRepo.update!(updated_post)
          updated_post2 = Ecto.Changeset.change updated_post, text: "another updated text", title: "another updated title"
          updated_post2 = TestRepo.update!(updated_post2, if: [title: "test"])
          expect(updated_post2.title) |> to(eq "updated title")
        end
      end
      describe "delete/4" do
        it "deletes record" do
          post = factory(:post)
          post = TestRepo.insert!(post)
          TestRepo.delete!(post)
          expect(TestRepo.all(Post) |> Enum.count) |> to(eq 0)
        end
      end
      describe "insert_all/7" do
        it "inserts multiple records in a batch" do
          expect(TestRepo.insert_all(Post, factory(:posts), on_conflict: :nothing) |> elem(0))
          |> to(eq 10)
          expect(TestRepo.one(from p in Post, select: count(p.id))) |> to(eq 10)
        end
      end
    end
  end
end
