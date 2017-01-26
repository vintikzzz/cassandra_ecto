defmodule Cassandra.Ecto.Migration do
  @moduledoc """
  Implements `Ecto.Adapter.Migration` behaviour.

  ## Defining Cassandra migrations

  Your migration module should use `Cassandra.Ecto.Migration` instead of
  `Ecto.Migration` to be able to use additional features.

  Any table must have option `primary_key: false` because Cassandra doesn't
  have `serial` type.

      defmodule TestMigration do
        use Cassandra.Ecto.Migration

        def up do
          create table(:test, primary_key: false) do
            add :id, :uuid, primary_key: true
            add :value, :integer
          end
        end
      end

  ## Primary keys

  There are two different methods to define primary keys.

  1. With `:primary_key` column option

          create table(:test, primary_key: false) do
            add :id, :uuid, primary_key: true
            add :id2, :uuid, primary_key: true
            add :id3, :uuid, primary_key: true
          end

    In this case `id` column will be partition key and rest, `id2` and `id3`,
    will be clustering columns.

  2. With `:partition_key` and `:clustering_column` options

          create table(:test, primary_key: false) do
            add :id, :uuid, partition_key: true
            add :id2, :uuid, paritition_key: true
            add :id3, :uuid, clustering_column: true
          end

    In this case we have defined composite partition key and one clustering column.
    More info about composite keys in [Using a composite partition key](https://docs.datastax.com/en/cql/3.3/cql/cql_reference/refCompositePk.html).

  > NOTE: It is not possible to use both methods together. The rule of thumb is:
  > if you don't use compound partition key just stay with `:primary_key`.

  ## Static columns

  To define `static` column use column option `static: true`

      create table(:test, primary_key: false) do
        add :id, :uuid, primary_key: true
        add :clustering_id, :uuid, primary_key: true
        add :value, :integer, static: true
      end

  More info about static columns in [Sharing a static column](https://docs.datastax.com/en/cql/3.3/cql/cql_reference/refStaticCol.html).

  ## Setting table options

  Use `:options` option to define additional settings for table:

      create table(:test, primary_key: false, options: [
        clustering_order_by: [value: :desc, value2: :asc],
        id: "5a1c395e-b41f-11e5-9f22-ba0be0483c18", compact_storage: true,
        comment: "Test", read_repair_chance: 1.0,
        compression: [sstable_compression: "DeflateCompressor", chunk_length_kb: 64]]) do
        add :id, :uuid, partition_key: true
        add :id2, :uuid, partition_key: true
        add :value, :integer, clustering_column: true
        add :value2, :integer, clustering_column: true
      end

  For full list of properties please see
  [Table properties](https://docs.datastax.com/en/cql/3.3/cql/cql_reference/tabProp.html).

  ## Data type mapping

      Migration type                  Cassandra type
      --------------                  --------------
      :id                             int
      :integer                        int
      :datetime                       timestamp
      :naive_datetime                 timestamp
      :utc_datetime                   timestamp
      :binary_id                      uuid
      :uuid                           uuid
      :binary                         blob
      :string                         text
      :counter                        counter
      :map                            map<varchar, blob>
      {:map, :integer}                map<varchar, int>
      {:map, :integer, :string}       map<int, text>
      {:array, :integer}              list<int>
      {:list, :integer}               list<int>
      {:set, :integer}                set<int>
      {:tuple, {:integer, :integer}}  tuple<int, int>
      {:frozen, :integer}             frozen<int>
      :udt_type                       udt_type

  It is possible to nest types like so:

      {:map, {:integer, {:frozen, {:map, {:integer, :integer}}}}}

  ## User defined types (UDT's)

  It is possible to define Cassandra UDT's and use as column type in table
  definitions.

      defmodule PostsMigration do
        use Cassandra.Ecto.Migration
        def change do
          create type(:comment) do
            add :id,        :uuid
            add :text,      :text
            add :posted_at, :utc_datetime
          end
          create table(:posts, primary_key: false) do
            add :id,        :uuid,   primary_key: true
            add :title,     :string
            add :text,      :text
            add :comments,  {:array, {:frozen, :comment}}
          end
        end
      end

  Later you can use it in your schema definition:

      defmodule Comment do
        use Schema
        embedded_schema do
          field :text, :string
          field :posted_at, :utc_datetime
        end
      end

      defmodule Post do
        use Schema
        alias Cassandra.Ecto.Spec.Support.Schemas.Comment
        schema "posts" do
          @primary_key {:id, :binary_id, autogenerate: true}
          field :title,    :string
          field :text,     :string
          embeds_many :comments, Comment
        end
      end

  More info about UDT's in [User-defined type](https://docs.datastax.com/en/cql/3.3/cql/cql_reference/cqlRefUDType.html).

  ## Custom indexes

  You can define any custom index like so:

      create index(:test, [:value], using: "org.apache.cassandra.index.sasi.SASIIndex",
        options: [mode: :contains, case_sensitive: false,
        analyzer_class: "org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer"])

  More info about custom indexes in [CREATE CUSTOM INDEX (SASI)](https://docs.datastax.com/en/cql/3.3/cql/cql_reference/refCreateSASIIndex.html).

  """
  import __MODULE__.CQL, only: [to_cql: 1]
  alias Ecto.Migration.Table
  alias Cassandra.Ecto.Connection

  defmacro __using__(_) do
    quote do
      use Ecto.Migration
      import Cassandra.Ecto.Migration, only: [type: 1, materialized_view: 1, materialized_view: 2, function: 1, function: 3]
    end
  end

  @doc """
  Defines Cassandra UDT

  ### Example

      create type(:comment) do
        add :id,        :uuid
        add :text,      :text
        add :posted_at, :utc_datetime
      end
  """
  def type(name) do
    struct(%Table{name: name, primary_key: false, options: [type: :type]})
  end

  @doc """
  Defines Cassandra materialized view

  ### Example

      create materialized_view(:cyclist_by_age,
          as: (from c in "cyclist_mv", select: {c.age, c.birthday, c.name, c.country})),
          primary_key: {:age, :cid}
  """
  def materialized_view(name, options \\ []) do
    options = Keyword.put(options, :type, :materialized_view)
    prefix = Keyword.get(options, :prefix)
    struct(%Table{name: name, primary_key: false, options: options, prefix: prefix})
  end

  @doc """
  Defines Cassandra user defined function (UDF)

  ### Example

      create function(:left, [column: :text, num: :int],
          returns: :text, language: :javascript,
          on_null_input: :returns_null,
          as: "column.substring(0,num)")
  """
  def function(name, vars \\ [], options \\ []) do
    options = options
    |> Keyword.put(:type, :function)
    |> Keyword.put(:vars, vars)
    |> Keyword.put_new(:language, :java)
    |> Keyword.put_new(:on_null_input, :returns_null)
    prefix = Keyword.get(options, :prefix)
    struct(%Table{name: name, prefix: prefix, options: options})
  end

  @doc """
  Defines Cassandra user defined aggregate (UDA)

  ### Example

      create aggregate(:average, :int,
          sfunc: function(:avgState),
          stype: {:tuple, {:int, :bigint}},
          finalfunc: function(:avgFinal),
          initcond: {0, 0})
  """
  def aggregate(name, var, options \\ []) do
    options = options
    |> Keyword.put(:type, :aggregate)
    |> Keyword.put(:var, var)
    prefix = Keyword.get(options, :prefix)
    struct(%Table{name: name, prefix: prefix, options: options})
  end

  @doc """
  See `Ecto.Adapter.Migration.execute_ddl/3`
  """
  def execute_ddl(repo, command, opts) do
    cql = to_cql(command)
    {:ok, _} = Connection.query(repo, cql, [], opts)
  end
end
