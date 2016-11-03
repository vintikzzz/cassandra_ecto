defmodule Cassandra.Ecto.Storage do
  alias Cassandra.Ecto.Connection

  def storage_up(opts) do
    {repo, opts} = start(opts, "Up")
    replication = opts[:replication] || {"SimpleStrategy",
      replication_factor: 1
    }
    {repl_class, repl_opts} = replication
    durable_writes = opts[:durable_writes] || true
    cql = """
          CREATE KEYSPACE #{opts[:keyspace]}
          WITH REPLICATION = {
            'class' : '#{repl_class}',
            'replication_factor' : #{repl_opts[:replication_factor]}
          }
          AND DURABLE_WRITES = #{durable_writes}
          """
    res = case Connection.query(repo, cql, [], opts) do
      {:ok, _} -> :ok
      {:error, %{code: 9216}} -> :ok
      error -> error
    end
  end

  def storage_down(opts) do
    {repo, opts} = start(opts, "Down")
    cql = "DROP KEYSPACE #{opts[:keyspace]}"
    res = case Connection.query(repo, cql, [], opts) do
      {:ok, _} -> :ok
      {:error, %{code: 8960}} -> :ok
      error -> error
    end
  end

  defp start(opts, suffix) do
    repo = opts[:repo]
    name = Module.concat(repo.__pool_name__, suffix)
    opts = opts
    |> Keyword.put(:use_keyspace, false)
    |> Keyword.put(:pool_name, name)
    |> Keyword.put_new(:keyspace, Mix.env)
    Connection.start_link(repo, opts)
    {repo, opts}
  end
end
