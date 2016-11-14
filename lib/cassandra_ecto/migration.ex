defmodule Cassandra.Ecto.Migration do
  import __MODULE__.CQL, only: [to_cql: 1]
  alias Ecto.Migration.Table
  alias Cassandra.Ecto.Connection

  defmacro __using__(_) do
    quote do
      use Ecto.Migration
      import Cassandra.Ecto.Migration, only: [type: 1]
    end
  end

  defmodule Type do
    defstruct name: nil,
              prefix: nil,
              columns: []
  end

  def type(name) when is_atom(name) do
    struct(%Table{name: name, primary_key: false, options: [as: :type]})
  end

  def execute_ddl(repo, command, opts) do
    cql = to_cql(command)
    IO.inspect opts
    IO.inspect cql
    {:ok, _} = Connection.query(repo, cql, [], opts)
  end
end
