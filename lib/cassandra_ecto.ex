defmodule Cassandra.Ecto do
  @moduledoc """
  Ecto integration for Apache Cassandra.

  Cassandra adapter implements 3 behaviours:

  * `Ecto.Adapter`
  * `Ecto.Adapter.Storage`
  * `Ecto.Adapter.Migration`

  Every behaviour implementation stays in separate file with appropriate docs.
  Please view for more information:

  * `Cassandra.Ecto.Adapter`
  * `Cassandra.Ecto.Storage`
  * `Cassandra.Ecto.Migration`

  ## Usage example

      # In your config/config.exs file
      config :my_app, Repo,
        keyspace: "my_keyspace"

      # In your application code
      defmodule Repo do
        use Ecto.Repo,
          otp_app: :my_app,
          adapter: Cassandra.Ecto
      end

      defmodule Post do
        use Ecto.Model

        @primary_key {:id, :binary_id, autogenerate: true}
        schema "posts" do
          field :title,    :string
          field :text,     :string
          field :tags,     {:array, :string}
          timestamps()
        end
      end

      defmodule Simple do
        import Ecto.Query

        def sample_query do
          query = from p in Post, where: "elixir" in p.tags
          Repo.all(query, allow_filtering: true)
        end
      end

  ## Available connection options

      :nodes
      :keyspace
      :auth
      :ssl
      :protocol_version
      :pool_max_size
      :pool_min_size
      :pool_cull_interval

  By default `nodes: [{"127.0.0.1", 9042}]`

  Please see [CQErl connecting](https://github.com/matehat/cqerl#connecting) for
  other options information.

  """

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

  def ensure_all_started(_repo, _type), do: {:ok, []}

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
  # defdelegate transaction(repo, opts, fun), to: Adapter
  # defdelegate in_transaction?(repo), to: Adapter
  # defdelegate rollback(repo, value), to: Adapter

  ## Storage
  alias Cassandra.Ecto.Storage
  defdelegate storage_up(opts), to: Storage
  defdelegate storage_down(opts), to: Storage

  ## Migration
  alias Cassandra.Ecto.Migration
  defdelegate execute_ddl(repo, command, opts), to: Migration
  def supports_ddl_transaction?, do: false

  ## Stream
  alias Cassandra.Ecto.Stream
  defdelegate stream(repo, meta, prepared, params, preprocess, opts), to: Stream
end
