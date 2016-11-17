defmodule Cassandra.Ecto do

  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Storage
  @behaviour Ecto.Adapter.Migration

  alias Cassandra.Ecto.Connection

  defmacro __before_compile__(env) do
    config = Module.get_attribute(env.module, :config)
    name = Keyword.get(config, :pool_name, Module.concat(env.module, "Pool"))
    config = config |> Keyword.delete(:pool_name)
    quote do
      def __pool__, do: unquote(Macro.escape(config))
      def __pool_name__, do: unquote(name)

      defoverridable [__pool__: 0]
    end
  end

  def query(repo, statement, values \\ [], opts \\ []), do:
    Connection.query(repo, statement, values, opts)

  def child_spec(repo, opts), do: Connection.child_spec(repo, opts)

  ## Adapter
  alias Cassandra.Ecto.Adapter
  defdelegate prepare(func, query), to: Adapter
  defdelegate execute(repo, meta, query, params, preprocess, opts), to: Adapter
  defdelegate insert(repo, meta, fields, on_conflict, returning, opts), to: Adapter
  defdelegate update(repo, meta, fields, filters, returning, opts), to: Adapter
  defdelegate delete(repo, meta, fields, opts), to: Adapter
  defdelegate insert_all(repo, meta, header, rows, on_conflict, returning, opts), to: Adapter
  defdelegate autogenerate(type), to: Adapter
  defdelegate loaders(primitive, type), to: Adapter
  defdelegate dumpers(primitive, type), to: Adapter

  # def insert_all(repo, meta, _header, fields, returning, _opts) do
  # end
  #
  # def update(repo, meta, fields, filters, returning, _opts) do
  # end

  ## Storage
  alias Cassandra.Ecto.Storage
  defdelegate storage_up(opts), to: Storage
  defdelegate storage_down(opts), to: Storage

  ## Migration
  alias Cassandra.Ecto.Migration
  defdelegate execute_ddl(repo, command, opts), to: Migration
  def supports_ddl_transaction?, do: false
end
