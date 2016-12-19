defmodule Cassandra.Ecto.Adapter do
  @moduledoc """
  Implements `Ecto.Adapter` behaviour.

  ## Queries

  Cassandra repo supports only following keywords for `Ecto.Query.from/2`:

      :where
      :order_by
      :limit
      :select
      :preload

  > NOTE: don't try to find `:offset`. It's not supported by Cassandra.

  > NOTE: you should remember that by default in Cassandra it is not possible
  > to filter and order by non primary key columns. To override this behaviour
  > you could pass additional option `allow_filtering: true`. But it is not
  > recommended, because of strong performance penalty. So it is very common
  > to add extra tables that better fits specific queries, also you are free to
  > create additional seconary indexes.
  >
  >     Repo.all((from p in Post, where: "abra" in p.tags), allow_filtering: true)

  ## Upserts and Lightweight transactions

  By default in Apache Cassandra `insert` and `update` both are equivalent to `upsert`.
  But `Ecto` by default expects that if record already exists it will raise error
  on insert and it should not create new record on update.
  So when `Cassandra.Ecto` executes `insert` it makes it conditional with
  `IF NOT EXISTS` and when executes `update` - `IF EXISTS`.

  To perform `upsert` with `insert` just use option `on_conflict: :nothing`.
  To perform `upsert` with `update` just use option `if: nil`.

  If you wish to perform upserts by default you need to specify `upsert: true`
  option in you repo. It might be very usefull for CQL version prior 3.2.

  By the way, you can set `:if` option for your `update` and `insert` queries.
  Here is available types for `:if` option:

      :exists
      :not_exists
      [field1: value1, field2: value2, ...]

  > HINT: please take a look at last option with keyword list. You can use it
  > for optimistic locking like so:
  >
  >     Repo.update!(post, if: [version: 2])

  ### Comparision table

      Ecto function       :on_conflict     :if           Cassandra
      -------------       ------------     --            ---------
      insert/2            :raise           (no option)   insert with 'IF NOT EXISTS'
      insert/2            :nothing         (no option)   insert/upsert
      insert/2            :nothing         :not_exists   insert with 'IF NOT EXISTS'
      update/2            (no option)      (no option)   updates with 'IF EXISTS'
      update/2            (no option)      nil           insert/upsert
      update/2            (no option)      :exists       updates with 'IF EXISTS'

  > NOTE: this comparision table works only for repo without `upsert: true` option.

  ## Batched queries

  Batched queries are done by `Ecto.Repo.insert_all/3` with option `batched: true`.
  By default all batched queries runs in `:logged` mode. But it is possible to
  override this on two levels:

  1. Repo level, by setting `:batch_mode` repo option

          config :my_app, Repo,
            adapter: Cassandra.Ecto
            batch_mode: :unlogged

  2. Query level, by setting `:batch_mode` query option

          Repo.insert_all(Post, posts, on_conflict: :nothing, batched: true, batch_mode: :unlogged)

  Available types:

      :logged
      :unlogged
      :serial

  Default type is `:logged`.

  > NOTE: don't forget to set proper `:on_conflict` option.

  Learn more about using batching in
  [Using and misusing batches](https://docs.datastax.com/en/cql/3.3/cql/cql_using/useBatch.html)

  ## Consistency

  There are to options to configure consistency:

  1. Tunable consistency.

    Sets by `:consistency` option. Available types:

          :any
          :one
          :two
          :three
          :quorum
          :all
          :local_quorum
          :each_quorum

    Default type is `:one`.

  2. Linearizable consistency.

    Sets by `:serial_consistency` option. Available types:

          :serial
          :local_serial

    Default type is `:undefined`.

  Every type of consistency can be set at repo and query level deparately.

  Please see [Consistency](https://docs.datastax.com/en/cassandra/3.x/cassandra/dml/dmlAboutDataConsistency.html)
  for more information.

  ## TIMESTAMP and TTL

  You can specify TTL and TIMESTAMP with `:ttl` and `:timestamp` respectively
  on query level.

  ## Transactions

  CASSANDRA DOESN'T SUPPORT TRANSACTIONS!

  """

  alias Cassandra.Ecto.Connection
  import __MODULE__.CQL, only: [to_cql: 3, to_cql: 4, to_cql: 5]
  import Cassandra.Ecto.Helper

  @doc """
  See `c:Ecto.Adapter.execute/6`
  """
  def execute(repo, %{fields: fields}, {_cache, {func, query}}, params, process, opts) do
    opts = prepare_opts(func, repo, opts)
    cql = to_cql(func, query, opts)
    names = get_names(query)
    params = Enum.zip(names, params) ++ if_fields(opts)
    case Connection.query(repo, cql, params, opts) do
      {:ok, %{rows: rows, num_rows: num, command: :select}} -> {num, rows |> Enum.map(&process_row(&1, process, fields))}
      {:ok, %{num_rows: num}} -> {num, []}
      {:error, err} -> raise err
    end
  end

  @doc """
  See `c:Ecto.Adapter.delete/4`
  """
  def delete(repo, meta, fields, opts) do
    cql = to_cql(:delete, meta, fields, opts)

    {:ok, _res} = Connection.query(repo, cql, fields, opts)
    {:ok, []}
  end

  @doc """
  See `c:Ecto.Adapter.insert/6`
  """
  def insert(_repo, meta, _params, _on_conflict, [_|_] = returning, _opts), do:
    read_write_error!(meta, returning)
  def insert(repo, meta, fields, on_conflict, [], opts) do
    on_conflict = prepare_on_conflict(repo, on_conflict)
    cql = to_cql(:insert, meta, fields, on_conflict, opts)
    fields = fields ++ if_fields(opts)
    {:ok, res} = Connection.query(repo, cql, fields, opts)
    action = elem(on_conflict, 0)
    prepare_result(res, action, fields)
  end

  @doc """
  See `c:Ecto.Adapter.insert_all/7`
  """
  def insert_all(_repo, meta, _header, _rows, _on_conflict, [_|_] = returning, _opts), do:
    read_write_error!(meta, returning)
  def insert_all(repo, meta, _header, rows, on_conflict, [], opts) do
    on_conflict = prepare_on_conflict(repo, on_conflict)
    queries = rows
    |> Enum.map(fn
      row -> {to_cql(:insert, meta, row, on_conflict, opts), row}
    end)
    case Keyword.get(opts, :batched, false) do
      false -> for {cql, row} <- queries, do: Connection.query(repo, cql, row, opts)
      true -> Connection.batch(repo, queries, opts)
    end
    {Enum.count(rows), []}
  end

  @doc """
  See `c:Ecto.Adapter.update/6`
  """
  def update(_repo, meta, _fields, _filters, [_|_] = returning, _opts), do:
    read_write_error!(meta, returning)
  def update(repo, meta, fields, filters, [], opts) do
    opts = prepare_opts(:update, repo, opts)
    cql = to_cql(:update, meta, fields, filters, opts)
    fields = fields ++ filters ++ if_fields(opts)
    {:ok, res} = Connection.query(repo, cql, fields, opts)
    prepare_result(res, :nothing, fields)
  end

  defp read_write_error!(meta, returning), do:
    error! nil,
      "Cassandra adapter does not support :read_after_writes in models. " <>
      "The following fields in #{inspect meta.schema} are tagged as such: #{inspect returning}"

  @doc """
  See `c:Ecto.Adapter.autogenerate/1`
  """
  def autogenerate(:id), do:
    error! nil,
      "Cassandra adapter does not support autogenerated :id field type in schema."
  def autogenerate(:embed_id),  do: Ecto.UUID.bingenerate()
  def autogenerate(:binary_id), do: Ecto.UUID.bingenerate()

  @datetimes [:datetime, :utc_datetime, :naive_datetime]

  @doc """
  See `c:Ecto.Adapter.loaders/2`
  """
  def loaders(datetime, type) when datetime in @datetimes, do: [&timestamp_decode/1, type]
  def loaders({:embed, _} = type, _), do: [&load_embed(type, &1)]
  def loaders(_primitive, type), do: [type]

  @doc """
  See `c:Ecto.Adapter.dumpers/2`
  """
  def dumpers(datetime, type) when datetime in @datetimes, do: [type, &timestamp_encode/1]
  def dumpers(_primitive, type), do: [type]

  defp load_embed({:embed, %{cardinality: :one, related: schema}} = type, value) do
    value = struct(schema, value)
    Ecto.Type.cast(type, value)
  end
  defp load_embed({:embed, %{cardinality: :many, related: schema}} = type, value) do
    value = value |> Enum.map(&struct(schema, &1))
    Ecto.Type.cast(type, value)
  end

  defp timestamp_decode(timestamp) do
    usec = timestamp |> rem(1_000_000)
    timestamp = timestamp |> div(1_000_000)
    {date, time} = :calendar.gregorian_seconds_to_datetime(timestamp)
    time = time |> Tuple.append(usec)
    {:ok, {date, time}}
  end

  defp timestamp_encode({{y, m, d}, {h, i, s, usec}}), do:
    {:ok, :calendar.datetime_to_gregorian_seconds({{y, m, d}, {h, i, s}}) * 1_000_000 + usec}

  @doc """
  See `c:Ecto.Adapter.prepare/2`
  """
  def prepare(func, query), do: {:nocache, {func, query}}

  defp prepare_result(res, action, fields) do
    row  = res.rows |> List.first
    cols = res.columns
    case {row, cols, action} do
      {nil,            _cols,      :nothing} -> {:ok, []}
      {[true  | []],   _cols,      _}        -> {:ok, []}
      {[false | row],  [_ | cols], :nothing} -> {:ok, Enum.zip(cols, row)}
      {[false | data], _cols,      :raise}   -> error! nil,
        "Unable to insert #{inspect(fields)}. Record #{inspect(Enum.zip(Keyword.keys(fields), data))} " <>
        "already exists. Use :insert_or_update for default upsert behaviour."
    end
  end

  defp prepare_on_conflict(repo, on_conflict) do
    case Keyword.get(repo.__pool__, :upsert, false) do
       true  -> put_elem(on_conflict, 0, :nothing)
       false -> on_conflict
    end
  end

  defp if_fields(opts) do
    case Keyword.get(opts, :if, nil) do
      wheres when is_list(wheres) -> wheres
      _ -> []
    end
  end

  defp prepare_opts(func, repo, opts) when func in [:update, :update_all] do
    case Keyword.get(repo.__pool__, :upsert, false)  do
      true  -> opts
      false -> Keyword.put_new(opts, :if, :exists)
    end
  end
  defp prepare_opts(_, _, opts), do: opts
end
