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
    context "with embeds_one" do
      it "embeds data" do
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
    end
  end
end
