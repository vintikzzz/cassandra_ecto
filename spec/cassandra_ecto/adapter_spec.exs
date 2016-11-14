defmodule CassandraEctoAdapterSpec do
  import Ecto.Query
  use ESpec, async: false
  alias Cassandra.Ecto, as: C
  alias Ecto.Integration.TestRepo
  alias Cassandra.Ecto.Spec.Support.Schemas.{Post}

  context "Adapter behaviour" do
    before do
      case Ecto.Migrator.up(TestRepo, 0, Cassandra.Ecto.Spec.Support.Migrations.PostsMigration, log: false) do
        :already_up -> :ok
        :ok         -> :ok
      end
    end
    context "all/1" do
      context "with empty data" do
        it "returns empty list" do
          expect(TestRepo.all(Post)) |> to(eq [])
          expect(TestRepo.all(from p in Post)) |> to(eq [])
        end
      end
      context "with no clauses" do
        it "returns inserted data" do
          id = Ecto.UUID.bingenerate()
          post = TestRepo.insert!(%Post{
            id: id, title: "hello", tags: ["abra", "cadabra"]
          })
          fetched_post = TestRepo.all(Post) |> List.first
          expect(fetched_post) |> to(eq post)
        end
      end
      context "with in clause" do
        let :id, do: Ecto.UUID.bingenerate()
        let :random_ids, do:
          Enum.map((1..3), fn _ -> Ecto.UUID.bingenerate() end)
          |> Enum.to_list
        before do: TestRepo.insert!(%Post{id: id, title: "hello", tags: ["abra", "cadabra"]})

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
      end
      context "with schema" do
        it "fetches data without schema" do
          %Post{} = TestRepo.insert!(%Post{title: "title1"})
          %Post{} = TestRepo.insert!(%Post{title: "title2"})
          expect(TestRepo.all(from(p in "posts", select: p.title)) |> Enum.sort)
          |> to(eq ["title1", "title2"])
          expect(TestRepo.all(from(p in "posts", where: p.title == "title1", select: p.title), allow_filtering: true) |> List.first)
          |> to(eq "title1")
        end
        it "fails with wrong schema" do
          expect(fn -> TestRepo.all("posts", prefix: "oops") end)
          |> to(raise_exception())
        end
      end
    end
    context "with basic actions" do
      it "inserts, updates and deletes" do
        post = %Post{title: "insert, update, delete", text: "fetch empty"}
        meta = post.__meta__

        deleted_meta = put_in meta.state, :deleted
        assert %Post{} = to_be_deleted = TestRepo.insert!(post)
        assert %Post{__meta__: ^deleted_meta} = TestRepo.delete!(to_be_deleted)

        loaded_meta = put_in meta.state, :loaded
        assert %Post{__meta__: ^loaded_meta} = TestRepo.insert!(post)

        post = TestRepo.one(Post)
        assert post.__meta__.state == :loaded
        assert post.inserted_at
      end
    end
    pending "when insert/6"
    pending "when insert/5"
  end
end
