defmodule Cassandra.Ecto.Stream do
  alias Cassandra.Ecto.Connection
  import Cassandra.Ecto.Adapter.CQL, only: [to_cql: 3]
  import Cassandra.Ecto.Helper,      only: [get_names: 1]

  defstruct [:result, :process, :fields]

  def stream(repo, %{fields: fields}, {_cache, {func, query}}, params, process, opts) do
    cql = to_cql(func, query, opts)
    names = get_names(query)
    params = Enum.zip(names, params)
    case Connection.query(repo, cql, params, opts) do
      {:ok, res} -> %__MODULE__{result: res, process: process, fields: fields}
      {:error, err} -> raise err
    end
  end
  defimpl Enumerable do
    import Cassandra.Ecto.Helper, only: [process_row: 3]
    def count(_), do: {:error, __MODULE__}

    def member?(_, _), do: {:error, __MODULE__}

    def reduce(stream, {:cont, acc}, fun) do
      Enumerable.reduce(stream.result, {:cont, acc}, fn
        row, _ ->
          res = process_row(row, stream.process, stream.fields)
          fun.({1, [res]}, acc)
      end)
    end
    def reduce(res, {:suspend, acc}, fun), do:
      {:suspended, acc, &reduce(res, &1, fun)}
    def reduce(_res, {:halt, acc}, _fun), do: {:halted, acc}
  end
end
