defmodule CassandraEctoAssocSpec do
  use ESpec, async: false
  alias Ecto.Integration.TestRepo
  alias Cassandra.Ecto.Spec.Support.Schemas.{Post, PostStats, User, PersonalInfo}
  import Cassandra.Ecto.Spec.Support.Factories
  import Ecto.Query
  import Ecto
  describe "Associations" do
    before do
      case Ecto.Migrator.up(TestRepo, 0, Cassandra.Ecto.Spec.Support.Migrations.PostsMigration, log: false) do
        :already_up -> :ok
        :ok         -> :ok
      end
    end
    it "supports :embeds_one" do
      user = factory(:user)
      user = TestRepo.insert!(user)
      changeset = Ecto.Changeset.change(user)
      info = factory(:personal_info)
      changeset = Ecto.Changeset.put_embed(changeset, :personal_info, info)
      changeset = TestRepo.update!(changeset)
      user = TestRepo.get!(User, user.id)
      expect(user.personal_info.first_name) |> to(eq info.first_name)
      expect(user.personal_info.last_name)  |> to(eq info.last_name)
      expect(user.personal_info.birthdate)  |> to(eq info.birthdate)
    end
    it "supports :embeds_many" do
      user = factory(:user)
      user = TestRepo.insert!(user)
      post = factory(:post)
      post = TestRepo.insert!(post)
      comments = factory(:comments, %{user_id: user.id})
      changeset = Ecto.Changeset.change(post)
      changeset = Ecto.Changeset.put_embed(changeset, :comments, comments)
      changeset = TestRepo.update!(changeset)
      post = TestRepo.get!(Post, post.id)
      expect(Enum.count(post.comments)) |> to(eq 10)
    end
    it "supports one to many relation", focus: true do
      user = factory(:user)
      user = TestRepo.insert!(user)
      posts = factory(:posts, %{author_id: user.id})
      posts = TestRepo.insert_all(Post, posts, on_conflict: :nothing)
      user = TestRepo.get!(User, user.id)
      post = TestRepo.one(from p in Post, limit: 1)
      assoc_author = TestRepo.one(assoc(post, :author))
      expect(assoc_author) |> to(eq user)
      assoc_posts = TestRepo.all(assoc(user, :posts), allow_filtering: true)
      expect(Enum.count(assoc_posts)) |> to(eq 10)
    end
  end
end
