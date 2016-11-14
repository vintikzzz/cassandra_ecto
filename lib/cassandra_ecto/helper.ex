defmodule Cassandra.Ecto.Helper do
  alias Ecto.Migration.{Table, Index}
  alias Ecto.Query
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

  def get_names(%Query{wheres: []}), do: []
  def get_names(%Query{wheres: wheres} = query) do
    Enum.map(wheres, fn
      %{expr: expr} -> get_names(expr)
    end)
    |> List.flatten
    |> Enum.sort(&(elem(&1, 0) > elem(&2, 0)))
    |> Enum.unzip
    |> elem(1)
  end
  def get_names({fun, _, [{{:., _, [{:&, _, [idx]}, field]}, _, []}, {:^, [], [ix]}]}), do:
    {ix, field}
  def get_names({fun, _, [{:^, [], [ix]}, {{:., _, [{:&, _, [idx]}, field]}, _, []}]}), do:
    {ix, field}
  def get_names({fun, _, [{{:., [], [{:&, [], [idx]}, field]}, [], []}, [{:^, [], [ix]}]]}), do:
    {ix, field}
  def get_names({fun, _meta, [{{:., [], [{:&, [], [idx]}, field]}, [], []}, [head | tail]]}), do:
    [
      get_names({fun, _meta, [{{:., [], [{:&, [], [idx]}, field]}, [], []}, head]}),
      get_names({fun, _meta, [{{:., [], [{:&, [], [idx]}, field]}, [], []}, tail]}),
    ]
  def get_names({fun, _, [left, right]}), do: [get_names(left), get_names(right)]
  def get_names({{:., [], [{:&, [], [ix]}, field]}, [], []}), do:
    {ix, field}
  def get_names(_), do: []
end
