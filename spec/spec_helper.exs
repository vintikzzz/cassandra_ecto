Code.require_file "../deps/ecto/integration_test/support/repo.exs", __DIR__
Code.require_file "support/migrations.exs", __DIR__

alias Ecto.Integration.TestRepo

Application.put_env(:ecto, TestRepo, adapter: Cassandra.Ecto)

defmodule Ecto.Integration.TestRepo do
  use Ecto.Integration.Repo, otp_app: :ecto
end

_   = Cassandra.Ecto.storage_down(TestRepo.config)
:ok = Cassandra.Ecto.storage_up(TestRepo.config)

{:ok, _pid} = TestRepo.start_link
