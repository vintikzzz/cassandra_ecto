defmodule Cassandra.Ecto.Connection do
  @moduledoc """
  Manages connection to Cassandra DB.
  """

  use GenServer
  import Supervisor.Spec
  alias Cassandrex, as: C
  alias Cassandra.Ecto.Log

  @conn_opts [:keyspace, :auth, :ssl, :protocol_version,
    :pool_max_size, :pool_min_size, :pool_cull_interval]

  @default_host "127.0.0.1"
  @default_port 9042
  @default_opts [timeout: 5000, consistency: :one,
                 batch_mode: :logged, log: false, batched: false]

  def init({repo, opts}) do
    repo.__adapter__.ensure_all_started(repo, :temporary)
    config = repo.__pool__
    name = pool_name(repo, opts)
    nodes = Keyword.get(config, :nodes, [{@default_host, @default_port}])
    conn_opts = config
    |> Keyword.put_new(:keyspace, Mix.env)
    conn_opts = case opts[:use_keyspace] do
      false -> Keyword.delete(conn_opts, :keyspace)
      _     -> conn_opts
    end
    C.add_nodes(name, nodes, conn_opts)
    {:ok, c} = C.get_client(name)
    {:ok, c}
  end

  defp pool_name(repo, opts), do:
    Keyword.get(opts, :pool_name, repo.__pool_name__)

  def handle_call({:query, statement, values, opts}, _from, c) do
    res = C.query(c, statement, values, opts)
    {:reply, res, c}
  end
  def handle_call({:batch, queries, opts}, _from, c) do
    res = C.batch(c, queries, opts)
    {:reply, res, c}
  end

  def terminate(_reason, c), do: C.close_client(c)

  def start_link(repo, opts) do
    name = pool_name(repo, opts)
    GenServer.start_link(__MODULE__, {repo, opts}, name: name)
  end

  def stop(repo, opts) do
    name = pool_name(repo, opts)
    GenServer.stop(name)
  end

  def query(repo, statement, values \\ [], opts \\ []) do
    opts = prepare_opts(opts)
    name = pool_name(repo, opts)
    fn -> GenServer.call(name, {:query, statement, values, opts}, opts[:timeout]) end
    |> with_log(repo, statement, values, opts)
  end

  def batch(repo, queries, opts \\ []) do
    opts = prepare_opts(opts)
    name = pool_name(repo, opts)
    fn -> GenServer.call(name, {:batch, queries, opts}, opts[:timeout]) end
    |> with_log(repo, queries, opts)
  end

  def child_spec(repo, opts), do: worker(__MODULE__, [repo, opts])

  defp prepare_opts(opts), do:
    Keyword.merge(@default_opts, opts)

  defp with_log(res, repo, statement, values, opts) do
    case opts[:log] do
      true  ->
        log(repo, statement, values, opts, res)
      false -> res.()
    end
  end
  defp with_log(res, repo, queries, opts) do
    case opts[:log] do
      true ->
        statement = "BEGIN BATCH\n" <> Enum.map_join(queries, "\n", fn
          {statement, values} -> statement <> " " <> inspect(values)
        end) <> "\nAPPLY BATCH;"
        log(repo, statement, [], opts, res)
      false -> res.()
    end
  end
  defp log(repo, statement, values, opts, res) do
    start = :os.system_time(:nano_seconds)
    res = res.()
    connection_time = :os.system_time(:nano_seconds) - start
    entry = %{connection_time: connection_time, decode_time: nil,
      pool_time: nil, result: res, query: statement}
    Log.log(repo, values, entry, opts)
    res
  end
end
