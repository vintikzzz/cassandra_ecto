defmodule Cassandra.Ecto.Adapter.CQL do
  alias Ecto.Query
  alias Ecto.Query.{BooleanExpr, QueryExpr}
  import Cassandra.Ecto.Helper

  def to_cql(:all, query, opts) do
    from     = from(query)
    select   = select(query)
    where    = where(query)
    order_by = order_by(query)
    limit    = limit(query)
    allow_filtering = allow_filtering(opts)
    assemble([select, from, where, order_by, limit, allow_filtering])
  end
  def to_cql(:insert, %{source: {prefix, source}}, fields, opts) do
    header = fields |> Keyword.keys
    values = "(" <> Enum.map_join(header, ",", &quote_name/1) <> ") " <>
      "VALUES " <> "(" <> Enum.map_join(header, ",", fn _arg -> "?" end) <> ")"
    assemble(["INSERT INTO", quote_table(prefix, source), values, using(opts)])
  end
  def to_cql(:update, %{source: {prefix, source}}, fields, filters, opts) do
    header = fields |> Keyword.keys
    values = "(" <> Enum.map_join(header, ",", &quote_name/1) <> ") " <>
      "VALUES " <> "(" <> Enum.map_join(header, ",", fn _arg -> "?" end) <> ")"
    assemble(["UPDATE", quote_table(prefix, source), using(opts), "SET", assigment(fields),
      "WHERE", filter(filters)])
  end
  def to_cql(:delete, %{source: {prefix, source}}, fields, opts) do
    assemble(["DELETE FROM", quote_table(prefix, source), using(opts), "WHERE", filter(fields)])
  end
  def to_cql(:delete_all, query, opts) do
    from  = from(query)
    where = where(query)
    assemble(["DELETE", from, where])
  end

  defp using(opts) do
    using_definitions = case using_definitions(opts) do
      "" -> ""
      w   -> "USING #{w}"
    end
  end

  defp using_definitions(opts) do
    opts
    |> Enum.map(&using_definition/1)
    |> Enum.reject(fn o -> o == nil  end)
    |> Enum.uniq
    |> Enum.join(" AND ")
  end

  @using_definitions [:ttl, :timestamp]
  defp using_definition({key, val}) when key in @using_definitions, do:
    assemble([String.upcase(Atom.to_string(key)), val])
  defp using_definition(_), do: nil

  defp assigment(fields), do: filter(fields, ", ")

  defp filter(filter, delimiter \\ " AND "), do:
    filter
    |> Keyword.keys
    |> Enum.map_join(delimiter, &"#{quote_name(&1)} = ?")

  defp allow_filtering([allow_filtering: true]), do: "ALLOW FILTERING"
  defp allow_filtering(_), do: []

  defp from(%{from: {from, _name}, prefix: prefix}), do: from(prefix, from)
  defp from(%{from: {from, _name}}), do: from(nil, from)
  defp from(prefix, from), do: "FROM #{quote_table(prefix, from)}"

  defp select(%Query{select: %{fields: fields}} = query), do:
    "SELECT " <> select_fields(fields, query)

  defp where(%Query{wheres: wheres} = query), do:
    boolean("WHERE", wheres, query)

  defp order_by(%Query{order_bys: []}), do: []
  defp order_by(%Query{order_bys: order_bys} = query) do
    order_bys = Enum.flat_map(order_bys, & &1.expr)
    exprs = Enum.map_join(order_bys, ", ", &order_by_expr(&1, query))
    "ORDER BY " <> exprs
  end

  defp limit(%Query{limit: nil}), do: []
  defp limit(%Query{limit: %QueryExpr{expr: expr}} = query), do:
    "LIMIT " <> expr(expr, query)

  defp select_fields([], _query), do: "TRUE"
  defp select_fields(fields, query) do
    Enum.map_join(fields, ", ", fn
      {key, value} ->
        expr(value, query)
      value ->
        expr(value, query)
    end)
  end

  defp boolean(_name, [], _query), do: []
  defp boolean(name, [%{expr: expr} | query_exprs], query) do
    name <> " " <>
      Enum.reduce(query_exprs, paren_expr(expr, query), fn
        %BooleanExpr{expr: expr, op: :and}, acc ->
          acc <> " AND " <> paren_expr(expr, query)
        %BooleanExpr{expr: expr, op: :or}, acc ->
          acc <> " OR " <> paren_expr(expr, query)
      end)
  end

  defp paren_expr(expr, query), do:
    "(" <> expr(expr, query) <> ")"

  binary_ops =
    [==: "=", !=: "!=", <=: "<=", >=: ">=",
      <:  "<", >:  ">", and: "AND", or: "OR", like: "LIKE"]

  @binary_ops Keyword.keys(binary_ops)

  Enum.map(binary_ops, fn {op, str} ->
    defp handle_call(unquote(op), 2), do: {:binary_op, unquote(str)}
  end)

  defp handle_call(fun, _arity), do: {:fun, Atom.to_string(fun)}

  defp op_to_binary({op, _, [_, _]} = expr, query) when op in @binary_ops, do:
    paren_expr(expr, query)
  defp op_to_binary(expr, query), do: expr(expr, query)

  defp expr({{:., _, [{:&, _, [idx]}, field]}, _, []}, _query) when is_atom(field), do:
    quote_name(field)
  defp expr({:&, _, [idx, nil, _counter]}, query), do:
    error!(query, "Cassandra adapter requires a schema module when using selector")
  defp expr({:&, _, [idx, fields, _counter]}, query), do:
    Enum.map_join(fields, ", ", &quote_name(&1))
  defp expr({:in, _, [_left, []]}, query), do:
    error! query,
      "Cassandra adapter does not support queries with empty :in clauses"
  defp expr({:in, _, [left, right]}, query) when is_list(right) do
    args = Enum.map_join right, ",", &expr(&1, query)
    expr(left, query) <> " IN (" <> args <> ")"
  end
  defp expr({:in, _, [left, {:^, _, [ix, _]}]}, query), do:
    error! query,
      "Cassandra adapter does not support queries with array in :in clauses"
  defp expr({:in, _, [left, right]}, query), do:
    expr(right, query) <> " CONTAINS " <> expr(left, query)
  defp expr({:is_nil, _, [arg]}, query), do:
    "#{expr(arg, query)} IS NULL"
  defp expr({:not, _, [expr]}, query), do:
    "NOT (" <> expr(expr, query) <> ")"
  defp expr(nil, _query),   do: "NULL"
  defp expr(true, _query),  do: "TRUE"
  defp expr(false, _query), do: "FALSE"
  defp expr(literal, _query) when is_binary(literal), do:
    "'#{escape_string(literal)}'"
  defp expr(literal, query) when is_integer(literal), do:
    String.Chars.Integer.to_string(literal)
  defp expr({:^, [], [ix]}, _query), do: "?"
  defp expr({fun, _, args}, query) when is_atom(fun) and is_list(args) do
    case handle_call(fun, length(args)) do
      {:binary_op, op} ->
        [left, right] = args
        op_to_binary(left, query)
        <> " #{op} "
        <> op_to_binary(right, query)

      {:fun, fun} ->
        "#{fun}(" <> Enum.map_join(args, ", ", &expr(&1, query)) <> ")"
    end
  end

  defp order_by_expr({dir, expr}, query) do
    str = expr(expr, query)
    case dir do
      :asc  -> str
      :desc -> str <> " DESC"
    end
  end

  defp escape_string(value) when is_binary(value) do
    :binary.replace(value, "'", "''", [:global])
  end

end
