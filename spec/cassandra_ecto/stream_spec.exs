defmodule CassandraEctoStreamSpec do
  import Ecto.Query
  use ESpec, async: false
  alias Ecto.Integration.TestRepo
  alias Cassandra.Ecto.Spec.Support.Schemas.Post
  import Cassandra.Ecto.Spec.Support.Factories

  describe "Cassandra.Ecto" do
    before do
      case Ecto.Migrator.up(TestRepo, 0, Cassandra.Ecto.Spec.Support.Migrations.PostsMigration, log: false) do
        :already_up -> :ok
        :ok         -> :ok
      end
    end
    describe "stream/6" do
      it "streams nothing if no data in DB" do
        posts = TestRepo.stream(from p in Post)
        |> Enum.to_list()
        expect(posts) |> to(eq [])
      end
      it "streams data from DB" do
        TestRepo.insert_all(Post, factory(:posts, %{}, num: 100), on_conflict: :nothing)
        posts = TestRepo.stream(from p in Post)
        |> Enum.to_list()
        expect(Enum.count(posts)) |> to(eq 100)
      end
    end
  end
end
