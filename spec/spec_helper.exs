Code.require_file "support/migrations.exs", __DIR__
Code.require_file "support/schemas.exs", __DIR__
Code.require_file "support/factories.exs", __DIR__

Application.put_env(:ecto, TestRepo, adapter: Cassandra.Ecto)
Application.put_env(:ecto, TestUpsertRepo, adapter: Cassandra.Ecto, upsert: true)
Application.put_env(:ecto, TestNowRepo, adapter: Cassandra.Ecto, binary_id: :now)

defmodule TestRepo, do:
  use Ecto.Repo, otp_app: :ecto

defmodule TestUpsertRepo, do:
  use Ecto.Repo, otp_app: :ecto

defmodule TestNowRepo, do:
  use Ecto.Repo, otp_app: :ecto

ESpec.configure fn(config) ->
  config.before fn
    %{context_tag: :db} ->
      Cassandra.Ecto.storage_up(TestRepo.config)
    _ -> :ok
    end
  config.finally fn
    %{context_tag: :db} ->
      Cassandra.Ecto.storage_down(TestRepo.config)
    _ -> :ok
    end
end

:ok = Cassandra.Ecto.storage_up(TestRepo.config)

{:ok, _pid} = TestRepo.start_link
{:ok, _pid} = TestUpsertRepo.start_link
{:ok, _pid} = TestNowRepo.start_link
