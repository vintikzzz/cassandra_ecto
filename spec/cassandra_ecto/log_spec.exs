defmodule CassandraEctoLogSpec do
  alias Cassandra.Ecto.Log

  use ESpec, async: true

  describe "Cassandra.Log" do
    describe "log/4" do
      it "writes log to io in cyan color for select queries" do
        entry = %{connection_time: 0, decode_time: nil,
          pool_time: nil, result: {:ok, []}, query: "SELECT something"}
        message = capture_log(fn ->
          Log.log(TestRepo, [], entry, [])
        end)
        expect(message) |> to(start_with "\e[36m\n")
        expect(message) |> to(end_with "[debug] QUERY OK db=0.0ms\nSELECT something []\n\e[0m")
      end
    end
  end
end
