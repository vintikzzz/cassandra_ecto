defmodule CassandraEctoAssocSpec do
  use ESpec, async: false
  alias Ecto.Integration.TestRepo
  alias Cassandra.Ecto.Spec.Support.Schemas.{Post, PostStats, User, PersonalInfo}
  import Cassandra.Ecto.Spec.Support.Factories
  describe "Associations" do
    before do
      case Ecto.Migrator.up(TestRepo, 0, Cassandra.Ecto.Spec.Support.Migrations.PostsMigration, log: false) do
        :already_up -> :ok
        :ok         -> :ok
      end
    end
    it "embeds one" do
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
    it "embeds many" do
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
  end
end
