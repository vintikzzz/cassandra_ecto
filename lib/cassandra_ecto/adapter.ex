defmodule Cassandra.Ecto.Adapter do
  alias Ecto.Query
  alias Ecto.Query.{BooleanExpr, QueryExpr}
  alias Cassandra.Ecto.Connection
  import Cassandra.Ecto.Helper

  def execute(repo, meta, {_cache, {func, query}}, params, preprocess, opts) do
    cql = apply(__MODULE__, func, [query])
    IO.inspect opts
    IO.inspect cql
    fields = Enum.zip(names(query), params)
    IO.inspect fields
    case Connection.query(repo, cql, fields, opts) do
      {:ok, %{rows: rows, num_rows: num}} -> {num, rows}
      {:error, err} -> raise err
    end
  end

  defp names(%Query{wheres: []}), do: []
  defp names(%Query{wheres: wheres} = query) do
    Enum.map(wheres, fn
      %{expr: expr} -> names(expr)
    end)
    |> List.flatten
    |> Enum.sort(&(elem(&1, 0) > elem(&2, 0)))
    |> Enum.unzip
    |> elem(1)
  end
  defp names({fun, _, [{{:., _, [{:&, _, [idx]}, field]}, _, []}, {:^, [], [ix]}]}), do:
    {ix, field}
  defp names({fun, _, [{:^, [], [ix]}, {{:., _, [{:&, _, [idx]}, field]}, _, []}]}), do:
    {ix, field}
  defp names({fun, _, [left, right]}), do: [names(left), names(right)]
  defp names(_), do: []

  def all(%Query{} = query) do
    from   = from(query)
    select = select(query)
    where  = where(query)
    assemble([select, from, where])
  end

  def delete_all(%Query{} = query) do
    from  = from(query)
    where = where(query)
    assemble(["DELETE", from, where])
  end

  def insert(_repo, meta, _params, _on_conflict, [_|_] = returning, _opts), do:
    error! nil,
      "Cassandra adapter does not support :read_after_writes in models. " <>
      "The following fields in #{inspect meta.schema} are tagged as such: #{inspect returning}"

  def insert(repo, %{source: {prefix, table}}, fields, _on_conflict, [], opts) do
    header = fields |> Keyword.keys
    values = "(" <> Enum.map_join(header, ",", &quote_name/1) <> ") " <>
      "VALUES " <> "(" <> Enum.map_join(header, ",", fn _arg -> "?" end) <> ")"
    cql = assemble(["INSERT INTO", quote_table(prefix, table), values])
    {:ok, res} = Connection.query(repo, cql, fields, opts)
    {:ok, []}
  end

  defp from(%{from: {from, _name}, prefix: prefix}), do: from(prefix, from)
  defp from(%{from: {from, _name}}), do: from(nil, from)
  defp from(prefix, from), do: "FROM #{quote_table(prefix, from)}"

  defp select(%Query{select: %{fields: fields}}), do:
    "SELECT " <> select_fields(fields)

  defp where(%Query{wheres: wheres}) do
    boolean("WHERE", wheres)
  end

  defp select_fields([]), do: "TRUE"
  defp select_fields(fields) do
    Enum.map_join(fields, ", ", fn
      {key, value} ->
        expr(value)
      value ->
        expr(value)
    end)
  end

  defp boolean(_name, []), do: []
  defp boolean(name, [%{expr: expr} | query_exprs]) do
    name <> " " <>
      Enum.reduce(query_exprs, paren_expr(expr), fn
        %BooleanExpr{expr: expr, op: :and}, acc ->
          acc <> " AND " <> paren_expr(expr)
        %BooleanExpr{expr: expr, op: :or}, acc ->
          acc <> " OR " <> paren_expr(expr)
      end)
  end

  defp paren_expr(expr) do
    "(" <> expr(expr) <> ")"
  end

  defp expr({{:., _, [{:&, _, [idx]}, field]}, _, []}) when is_atom(field), do:
    quote_name(field)

  defp expr(nil),   do: "NULL"
  defp expr(true),  do: "TRUE"
  defp expr(false), do: "FALSE"

  defp expr(literal) when is_binary(literal) do
    "'#{escape_string(literal)}'"
  end

  defp expr(literal) when is_integer(literal) do
    String.Chars.Integer.to_string(literal)
  end

  defp expr({:^, [], [ix]}), do: "?"

  binary_ops =
    [==: "=", !=: "!=", <=: "<=", >=: ">=",
      <:  "<", >:  ">", and: "AND", or: "OR", like: "LIKE"]

  @binary_ops Keyword.keys(binary_ops)

  Enum.map(binary_ops, fn {op, str} ->
    defp handle_call(unquote(op), 2), do: {:binary_op, unquote(str)}
  end)

  defp handle_call(fun, _arity), do: {:fun, Atom.to_string(fun)}

  defp expr({fun, _, args}) when is_atom(fun) and is_list(args) do
    case handle_call(fun, length(args)) do
      {:binary_op, op} ->
        [left, right] = args
        op_to_binary(left)
        <> " #{op} "
        <> op_to_binary(right)

      {:fun, fun} ->
        "#{fun}(" <> Enum.map_join(args, ", ", &expr/1) <> ")"
    end
  end

  defp op_to_binary({op, _, [_, _]} = expr) when op in @binary_ops do
    paren_expr(expr)
  end

  defp op_to_binary(expr) do
    expr(expr)
  end

  defp escape_string(value) when is_binary(value) do
    :binary.replace(value, "'", "''", [:global])
  end
end
