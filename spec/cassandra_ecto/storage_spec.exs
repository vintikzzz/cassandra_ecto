defmodule CassandraEctoStorageSpec do
  alias Cassandra.Ecto, as: C
  alias Ecto.Integration.TestRepo
  use ESpec, async: false
  describe "Storage behaviour" do
    context "when storage_up/1" do
      it "creates new keyspace" do
        assert :ok = C.storage_up(TestRepo.config)
      end
    end
    context "when storage_down/1" do
      it "removes keyspace" do
        assert :ok = C.storage_down(TestRepo.config)
      end
    end
  end
end
