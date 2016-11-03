defmodule Cassandra.Ecto.Helper do
  alias Ecto.Migration.{Table, Index}
  def quote_name(name)
  def quote_name(name) when is_atom(name),
    do: quote_name(Atom.to_string(name))
  def quote_name(name) do
    if String.contains?(name, "\"") do
      error!(nil, "bad field name #{inspect name}")
    end
    <<?", name::binary, ?">>
  end
  def quote_index(%Index{} = index), do: quote_table(index.prefix, index.name)

  def quote_table(%Table{} = table), do: quote_table(table.prefix, table.name)
  def quote_table(%Index{} = index), do: quote_index(index)
  def quote_table(nil, name),        do: quote_table(name)
  def quote_table(prefix, name),     do: quote_table(prefix) <> "." <> quote_table(name)
  def quote_table(name) when is_atom(name),
    do: quote_table(Atom.to_string(name))
  def quote_table(name) do
    if String.contains?(name, "\"") do
      error!(nil, "bad table name #{inspect name}")
    end
    <<?", name::binary, ?">>
  end

  def error!(nil, message) do
    raise ArgumentError, message
  end
  def error!(query, message) do
    raise Ecto.QueryError, query: query, message: message
  end

  def assemble(list), do: assemble(list, " ")
  def assemble(list, joiner) do
    list
    |> List.flatten
    |> Enum.reject(fn(v)-> v == "" end)
    |> Enum.join(joiner)
  end
end
