Logger.configure(level: :info)
ExUnit.start exclude: [:uses_usec, :id_type, :read_after_writes, :sql_fragments, :decimal_type, :invalid_prefix, :transaction, :foreign_key_constraint]

Application.put_env(:ecto, :primary_key_type, :binary_id)
Application.put_env(:ecto, :async_integration_tests, false)

Code.require_file "../deps/ecto/integration_test/support/repo.exs", __DIR__
Code.require_file "../deps/ecto/integration_test/support/types.exs", __DIR__
Code.require_file "../deps/ecto/integration_test/support/schemas.exs", __DIR__
Code.require_file "../deps/ecto/integration_test/support/migration.exs", __DIR__

alias Ecto.Integration.TestRepo

Application.put_env(:ecto, TestRepo, adapter: Cassandra.Ecto)

defmodule Ecto.Integration.TestRepo do
  use Ecto.Integration.Repo, otp_app: :ecto
end

{:ok, _pid} = TestRepo.start_link

_   = Cassandra.Ecto.storage_down(TestRepo.config)
:ok = Cassandra.Ecto.storage_up(TestRepo.config)

# # We capture_io, because of warnings on references
# ExUnit.CaptureIO.capture_io fn ->
:ok = Ecto.Migrator.up(TestRepo, 0, Ecto.Integration.Migration, log: false)
# end
