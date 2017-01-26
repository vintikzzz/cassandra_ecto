defmodule Cassandra.Ecto.Migration.CQL do
  @moduledoc """
  Generates CQL-queries for Cassandra DDL statements.
  """

  alias Ecto.Migration.{Table, Index, Reference, Constraint}
  import Cassandra.Ecto.Helper

  @creates [:create, :create_if_not_exists]
  @drops   [:drop,   :drop_if_exists]

  @allowed_column_opts [:primary_key, :partition_key, :null,
                        :clustering_column, :frozen, :static]

  def to_cql({command, %Table{} = table, columns}) when command in @creates, do:
    table
    |> get_type
    |> fn t -> to_cql({t, command, table, columns}) end.()
  def to_cql({command, %Table{} = table}) when command in @drops, do:
    table
    |> get_type
    |> get_type_cql_name
    |> drop(table, command)
  def to_cql({:alter, %Table{} = table, changes}), do:
    table
    |> get_type
    |> fn t -> to_cql({t, :alter, table, changes}) end.()
  def to_cql({command, %Index{} = index}) when command in @creates do
    fields = Enum.map_join(index.columns, ", ", &quote_name/1)
    validate_index!(index)
    assemble(["CREATE", index_custom(index), "INDEX", quote_index(index), "ON",
      quote_table(index.prefix, index.table), "(#{fields})", index_using(index),
      with_definitions_any(index)])
  end
  def to_cql({_command, %Constraint{} = _constraint}), do:
    error! nil, "Cassandra adapter does not support constraints"
  def to_cql({:table, :alter, %Table{} = table, changes}), do:
    assemble([alter(table), column_changes(table, changes), with_definitions_any(table)])
  def to_cql({:type, :alter, %Table{} = table, changes}), do:
    assemble([alter(table), column_changes(table, changes)])
  def to_cql({:materialized_view, :alter, %Table{options: options} = table, _changes}) do
    def_options = case options |> Enum.reject &(elem(&1, 0) in [:type]) do
      [] -> nil
      any -> any
    end
    assemble([alter(table), with_definitions_any(%Table{options: def_options})])
  end
  def to_cql({:rename, %Table{} = table, old, new}), do:
    assemble([alter(table), "RENAME", quote_name(old), "TO", quote_name(new)])
  def to_cql({:rename, %Table{} = _old, %Table{} = _new}), do:
    error! nil, "Cassandra adapter does not support table renaming"
  def to_cql(string) when is_binary(string), do: string
  def to_cql({command, %Index{} = index}) when command in @drops, do:
    drop("INDEX", index, command)
  def to_cql({:type, command, %Table{} = type, columns}) when command in @creates do
    assemble(["CREATE TYPE", if_not_exists(command), quote_table(type),
      "(#{column_definitions(type, columns)})"])
  end
  def to_cql({:table, command, %Table{} = table, columns}) when command in @creates do
    assemble(["CREATE TABLE", if_not_exists(command), quote_table(table),
      "(#{column_definitions(table, columns)}, #{pk_definition(columns)})",
      with_definitions_any(table)])
  end
  def to_cql({:materialized_view, command, %Table{options: options} = view, _columns}) when command in @creates do
    def_options = case options |> Enum.reject &(elem(&1, 0) in [:type, :as, :primary_key]) do
      [] -> nil
      any -> any
    end
    assemble(["CREATE MATERIALIZED VIEW", if_not_exists(command), quote_table(view),
             as(Keyword.get(options, :as)), pk_definition(Keyword.get(options, :primary_key)),
             with_definitions_any(%Table{options: def_options})])
  end
  def to_cql({:function, command, %Table{options: options} = function, _columns}) when command in @creates do
    assemble(["CREATE FUNCTION", if_not_exists(command),
              quote_table(function), function_vars(function),
              on_null_input(Keyword.fetch!(options, :on_null_input)), "ON NULL INPUT",
              "RETURNS", ecto_to_db(Keyword.fetch!(options, :returns)),
              "LANGUAGE", Atom.to_string(Keyword.fetch!(options, :language)),
              "AS", "$$" <> Keyword.fetch!(options, :as) <> "$$"])
  end
  def to_cql({:aggregate, command, %Table{options: options} = aggregate, _columns}) when command in @creates do
    assemble(["CREATE AGGREGATE", if_not_exists(command),
              quote_table(aggregate) <> "(" <> ecto_to_db(Keyword.fetch!(options, :var)) <> ")",
              "SFUNC", quote_table(Keyword.fetch!(options, :sfunc)),
              "STYPE", ecto_to_db(Keyword.fetch!(options, :stype)),
              "FINALFUNC", quote_table(Keyword.fetch!(options, :finalfunc)),
              "INITCOND", initcond(aggregate)])
  end

  defp as(%Ecto.Query{} = query) do
    {query, _params, _key} = Ecto.Query.Planner.prepare(query, :all, Cassandra.Ecto, 0)
    query = Ecto.Query.Planner.normalize(query, :all, Cassandra.Ecto, 0)
    cql = Cassandra.Ecto.Adapter.CQL.to_cql(:all, query)
    ["AS", cql]
  end

  defp function_vars(%Table{options: options}) do
    {:ok, vars} = Keyword.fetch(options, :vars)
    vars
    |> Enum.map_join(", ", fn {name, type} -> quote_name(name) <> " " <> ecto_to_db(type) end)
    |> (fn e -> "(" <> e <> ")" end).()
  end

  defp on_null_input(:called), do: "CALLED"
  defp on_null_input(:returns_null), do: "RETURNS NULL"

  defp initcond(%Table{options: options}) do
    type = Keyword.fetch!(options, :stype)
    value = Keyword.fetch!(options, :initcond)
    db_value(value, type)
  end

  defp index_using(%Index{using: nil}), do: ""
  defp index_using(%Index{using: using}), do: "USING '#{using}'"

  defp index_custom(%Index{using: nil}), do: ""
  defp index_custom(%Index{using: _}), do: "CUSTOM"

  defp validate_index!(%Index{unique: unique, where: where})
    when unique or not is_nil(where), do:
      error! nil,
        "Cassandra adapter doesn't support unique indexes and where clauses"
  defp validate_index!(%Index{}), do: :ok

  defp if_not_exists(command), do: if command == :create_if_not_exists, do: "IF NOT EXISTS", else: ""
  defp if_exists(command), do: if command == :drop_if_exists, do: "IF EXISTS", else: ""

  defp alter(%Table{} = table), do: assemble(["ALTER", table |> get_type |> get_type_cql_name, quote_table(table)])

  defp drop(name, table, command), do:
    assemble(["DROP", name, if_exists(command), quote_table(table)])

  defp column_changes(table, columns) do
    Enum.map_join(columns, ", ", &column_change(table, &1))
  end
  defp column_change(_table, {_command, _name, %Reference{}, _opts}), do:
    error! nil, "Cassandra adapter does not support references"
  defp column_change(_table, {:add, name, type, opts}), do:
    assemble(["ADD", quote_name(name), column_type(type, opts)])
  defp column_change(_table, {:modify, name, type, opts}), do:
    assemble(["ALTER", quote_name(name), "TYPE", column_type(type, opts)])
  defp column_change(_table, {:remove, name}), do: assemble(["DROP", quote_name(name)])

  defp column_definition(_table, {:add, _name, %Reference{} = _ref, _opts}), do:
    error! nil, "Cassandra adapter does not support references"

  defp column_definition(_table, {:add, name, type, opts}), do:
    assemble([quote_name(name), column_type(type, opts)])

  defp column_type(type, opts) do
    validate_column_opts!(opts)

    ecto_to_db(type)
    |> frozen(opts)
    |> static(opts)
  end

  defp validate_column_opts!([null: false]), do:
    error! nil, "Cassandra adapter doesn't allow non-nullable fields"

  defp validate_column_opts!(opts) do
    case Keyword.keys(opts) -- @allowed_column_opts do
      []  -> :ok
      res -> error! nil, "Cassandra adapter doesn't allow #{inspect(res)} opts"
    end
  end

  defp frozen(type, opts) do
    case opts[:frozen] do
      true -> "frozen <#{type}>"
      _ -> type
    end
  end

  defp static(type, opts) do
    case opts[:static] do
      true -> "#{type} STATIC"
      _ -> type
    end
  end

  defp ecto_to_db(:id),              do: ecto_to_db(:integer)
  defp ecto_to_db(:serial),          do: error! nil,
                                       "Cassandra adapter does not support :serial type"
  defp ecto_to_db(:integer),         do: "int"
  defp ecto_to_db(:datetime),        do: "timestamp"
  defp ecto_to_db(:naive_datetime),  do: "timestamp"
  defp ecto_to_db(:utc_datetime),    do: "timestamp"
  defp ecto_to_db(:binary_id),       do: "uuid"
  defp ecto_to_db(:binary),          do: "blob"
  defp ecto_to_db(:string),          do: "text"
  defp ecto_to_db(:map),             do: ecto_to_db({:map, :binary})
  defp ecto_to_db({:map, {t1, t2}}), do: "map<#{ecto_to_db(t1)}, #{ecto_to_db(t2)}>"
  defp ecto_to_db({:map, t1, t2}),   do: "map<#{ecto_to_db(t1)}, #{ecto_to_db(t2)}>"
  defp ecto_to_db({:map, t}),        do: ecto_to_db({:map, {:varchar, t}})
  defp ecto_to_db({:array, t}),      do: "list<#{ecto_to_db(t)}>"
  defp ecto_to_db({:list, t}),       do: "list<#{ecto_to_db(t)}>"
  defp ecto_to_db({:set, t}),        do: "set<#{ecto_to_db(t)}>"
  defp ecto_to_db({:tuple, type}) when is_atom(type), do: ecto_to_db({:tuple, {type}})
  defp ecto_to_db({:tuple, types}) when is_tuple(types) do
    types_defintion = types
    |> Tuple.to_list
    |> Enum.map_join(", ", &ecto_to_db/1)
    "tuple<#{types_defintion}>"
  end
  defp ecto_to_db({:frozen, type}),  do: "frozen<#{ecto_to_db(type)}>"
  defp ecto_to_db(other),            do: Atom.to_string(other)

  defp column_definitions(table, columns), do:
    Enum.map_join(columns, ", ", &column_definition(table, &1))

  defp with_definitions_any(%{options: _opts} = any) do
    case with_definitions(any) do
      nil -> ""
      w   -> "WITH #{w}"
    end
  end
  defp with_definitions(%{options: nil}), do: nil
  defp with_definitions(%Table{options: opts}), do:
    Enum.map_join(opts, " AND ", &with_definiton/1)
  defp with_definitions(%Index{options: opts}), do:
    with_definiton({:options, opts})

  defp with_definiton({:compact_storage, true}), do: "COMPACT STORAGE"
  defp with_definiton({:clustering_order_by, val}), do:
    "CLUSTERING ORDER BY (#{Enum.map_join(val, ", ", &clustering/1)})"
  defp with_definiton({key, val}), do:
    "#{String.upcase(Atom.to_string(key))} = #{with_val(val)}"

  defp clustering({col, :desc}), do: "#{col} DESC"
  defp clustering({col, :asc}),  do: "#{col} ASC"

  defp with_val({key, val}), do:
    "'#{ Atom.to_string(key)}' : #{with_val(val)}"
  defp with_val(val) when is_list(val), do:
    "{ #{Enum.map_join(val, ", ", &with_val/1)} }"
  defp with_val(val) when is_binary(val) or is_atom(val), do: "'#{val}'"
  defp with_val(val), do: val

  defp pk_definition(columns) when is_binary(columns), do:
    assemble(["PRIMARY KEY", columns])
  defp pk_definition(columns) when is_tuple(columns), do:
    pk_tuple_definition(columns) |> pk_definition
  defp pk_definition(columns) do
    pks             = get_col_names(columns, :primary_key)
    partition_keys  = get_col_names(columns, :partition_key)
    clustering_cols = get_col_names(columns, :clustering_column)

    {partition_keys, clustering_cols} = case {pks, partition_keys, clustering_cols} do
      {[], [], _} -> error! nil,
        "Cassandra adapter requires any of :primary_key or :partition_key"
      {[], partition_keys, clustering_cols} -> {partition_keys, clustering_cols}
      {[head | tail], [], []} -> {[head], tail}
      _ -> error! nil,
        """
        Cassandra adapter doesn't allow to mix :primary_key
        with :partition_key or :clustering_column
        """
    end

    pk_definition("(#{partition_part(partition_keys)}#{clustering_part(clustering_cols)})")
  end

  defp partition_part(partition_keys) do
    case partition_keys do
      [p] -> quote_name(p)
      _  -> "(" <> Enum.map_join(partition_keys, ", ", &quote_name/1) <> ")"
    end
  end

  defp clustering_part(clustering_cols) do
    case clustering_cols do
      [] -> ""
      _  -> ", " <> Enum.map_join(clustering_cols, ", ", &quote_name/1)
    end
  end

  defp get_col_names(columns, type), do:
    for {_, name, _, opts} <- columns,
      opts[type],
      do: name

  defp pk_tuple_definition(columns) when is_tuple(columns), do:
    Tuple.to_list(columns)
    |> Enum.map_join(", ", &pk_tuple_definition/1)
    |> fn arg -> "(" <> arg <> ")" end.()
  defp pk_tuple_definition(column) when is_binary(column), do: quote_name(column)
  defp pk_tuple_definition(column) when is_atom(column), do:
    pk_tuple_definition(Atom.to_string(column))

  defp get_type(%Table{options: nil}), do: :table
  defp get_type(%Table{} = table), do: Keyword.get(table.options, :type, :table)

  defp get_type_cql_name(type) do
    case type do
      :type              -> "TYPE"
      :materialized_view -> "MATERIALIZED VIEW"
      :table             -> "TABLE"
      :function          -> "FUNCTION"
      :aggregate         -> "AGGREGATE"
    end
  end
end
