Code.require_file "../deps/ecto/integration_test/support/repo.exs", __DIR__
Code.require_file "support/migrations.exs", __DIR__
Code.require_file "support/schemas.exs", __DIR__

alias Ecto.Integration.TestRepo

Application.put_env(:ecto, TestRepo, adapter: Cassandra.Ecto)

defmodule Ecto.Integration.TestRepo do
  use Ecto.Integration.Repo, otp_app: :ecto
end

ESpec.configure fn(config) ->
  config.before fn(_tags) ->
    :ok = Cassandra.Ecto.storage_up(TestRepo.config)
  end
  config.finally fn(_shared) ->
    :ok = Cassandra.Ecto.storage_down(TestRepo.config)
  end
end

:ok = Cassandra.Ecto.storage_up(TestRepo.config)

{:ok, _pid} = TestRepo.start_link
