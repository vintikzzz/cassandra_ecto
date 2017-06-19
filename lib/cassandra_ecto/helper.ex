defmodule Cassandra.Ecto.Helper do
  @moduledoc false

  alias Ecto.Migration.{Table, Index}
  alias Ecto.Query

  def db_value(value, {:tuple, types}) when is_tuple(value) and is_tuple(types) do
    Enum.zip(Tuple.to_list(value), Tuple.to_list(types))
    |> Enum.map_join(", ", &db_value(elem(&1, 0), elem(&1, 1)))
    |> fn e -> "(" <> e  <> ")" end.()
  end
  def db_value(value, {:list, type}) when is_list(value) do
    value
    |> Enum.map_join(", ", &db_value(&1, type))
    |> fn e -> "[" <> e  <> "]" end.()
  end
  def db_value(value, {:set, type}) when is_list(value) do
    value
    |> Enum.map_join(", ", &db_value(&1, type))
    |> fn e -> "{" <> e  <> "}" end.()
  end
  def db_value(value, {:map, type}) when is_map(value), do:
    db_value(value, {:map, :string, type})
  def db_value(value, {:map, key_type, value_type}) when is_map(value) do
    Map.to_list(value)
    |> Enum.map_join(", ", &db_value(&1, {key_type, value_type}))
    |> fn e -> "{" <> e  <> "}" end.()
  end
  def db_value({key, value}, {key_type, value_type}), do:
    db_value(key, key_type) <> ": " <> db_value(value, value_type)
  def db_value(value, _) when is_binary(value), do: "$$" <> value <> "$$"
  def db_value(value, _), do: to_string(value)

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
  def quote_table(name) when is_atom(name),
    do: quote_table(Atom.to_string(name))
  def quote_table(name) do
    if String.contains?(name, "\"") do
      error!(nil, "bad table name #{inspect name}")
    end
    <<?", name::binary, ?">>
  end
  def quote_table(nil, name),        do: quote_table(name)
  def quote_table(prefix, name),     do: quote_table(prefix) <> "." <> quote_table(name)

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
    |> Enum.reject(fn(v) -> v == "" end)
    |> Enum.join(joiner)
  end

  def get_names(query), do: get_names(query, [])
  def get_names(query, opts) do
    update_names = get_update_names(query)
    where_names =
      case Keyword.get(opts, :where_names) do
         names when is_list(names) ->
           names |> Enum.with_index(length(update_names)) |> Enum.map(fn {k, v} -> {v, k} end)
         _ ->
           get_where_names(query)
      end
    update_names ++ where_names
    |> List.flatten
    |> Enum.sort(&(elem(&1, 0) < elem(&2, 0)))
    |> Enum.unzip
    |> elem(1)
  end

  def process_row(row, process, fields) do
    Enum.map_reduce(fields, row, fn
      {:&, _, [_, _, _counter]} = field, acc ->
        {process.(field, acc, nil), []}
      field, [h|t] ->
        {process.(field, h, nil), t}
    end) |> elem(0)
  end

  defp get_update_names(%Query{updates: []}), do: []
  defp get_update_names(%Query{updates: updates}) do
    for(%{expr: expr} <- updates,
      {op, kw} <- expr,
      {key, value} <- kw,
      do: get_update_names(op, key, value))
  end
  defp get_update_names(_), do: []
  defp get_update_names(_op, key, {:^, [], [ix]}), do: {ix, key}

  defp get_where_names(%Query{wheres: wheres}), do:
    Enum.map(wheres, fn
      %{expr: expr} -> get_where_names(expr)
    end)
  defp get_where_names({_fun, _, [{{:., _, [{:&, _, [_idx]}, field]}, _, []}, {:^, [], [ix]}]}), do:
    {ix, field}
  defp get_where_names({_fun, _, [{:^, [], [ix]}, {{:., _, [{:&, _, [_idx]}, field]}, _, []}]}), do:
    {ix, field}
  defp get_where_names({_fun, _, [{{:., [], [{:&, [], [_idx]}, field]}, [], []}, [{:^, [], [ix]}]]}), do:
    {ix, field}
  defp get_where_names({fun, meta, [{{:., [], [{:&, [], [idx]}, field]}, [], []}, [head | tail]]}), do:
    [
      get_where_names({fun, meta, [{{:., [], [{:&, [], [idx]}, field]}, [], []}, head]}),
      get_where_names({fun, meta, [{{:., [], [{:&, [], [idx]}, field]}, [], []}, tail]}),
    ]
  defp get_where_names({_fun, _, [left, right]}), do: [get_where_names(left), get_where_names(right)]
  defp get_where_names({{:., [], [{:&, [], [ix]}, field]}, [], []}), do:
    {ix, field}
  defp get_where_names(_), do: []
end
