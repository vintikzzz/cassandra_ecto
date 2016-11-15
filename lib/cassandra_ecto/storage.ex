defmodule Cassandra.Ecto.Storage do
  alias Cassandra.Ecto.Connection
  import __MODULE__.CQL, only: [to_cql: 2]

  def storage_up(opts) do
    {repo, opts} = start(opts, "Up")
    cql = to_cql(:up, opts)
    res = case Connection.query(repo, cql, [], opts) do
      {:ok, _} -> :ok
      {:error, %{code: 9216}} -> :ok
      error -> error
    end
  end

  def storage_down(opts) do
    {repo, opts} = start(opts, "Down")
    cql = to_cql(:down, opts)
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
