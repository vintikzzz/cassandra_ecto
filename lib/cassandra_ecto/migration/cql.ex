defmodule Cassandra.Ecto.Migration.CQL do
  alias Ecto.Migration.{Table, Index, Reference, Constraint}
  import Cassandra.Ecto.Helper

  @creates [:create, :create_if_not_exists]
  @drops   [:drop,   :drop_if_exists]

  @allowed_column_opts [:primary_key, :partition_key, :null,
                        :clustering_column, :frozen, :static]

  def to_cql({command, %Table{options: [as: :type]} = table, columns}) when command in @creates do
    assemble(["CREATE TYPE", if_not_exists(command), quote_table(table),
      "(#{column_definitions(table, columns)})"])
  end
  def to_cql({command, %Table{} = table, columns}) when command in @creates do
    assemble(["CREATE TABLE", if_not_exists(command), quote_table(table),
      "(#{column_definitions(table, columns)}, #{pk_definition(columns)})",
      with_definitions_any(table)])
  end
  def to_cql({command, %Index{} = index}) when command in @creates do
    fields = Enum.map_join(index.columns, ", ", &quote_name/1)
    validate_index!(index)
    assemble(["CREATE", index_custom(index), "INDEX", quote_index(index), "ON",
      quote_table(index.prefix, index.table), "(#{fields})", index_using(index),
      with_definitions_any(index)])
  end
  def to_cql({_command, %Constraint{} = constraint}), do:
    error! nil, "Cassandra adapter does not support constraints"
  def to_cql({:alter, %Table{} = table, changes}), do:
    assemble([alter(table), column_changes(table, changes)])
  def to_cql({:rename, %Table{} = table, old, new}), do:
    assemble([alter(table), "RENAME", quote_name(old), "TO", quote_name(new)])
  def to_cql({:rename, %Table{} = old, %Table{} = new}), do:
    error! nil, "Cassandra adapter does not support table renaming"
  def to_cql(string) when is_binary(string), do: string
  def to_cql({command, %Table{options: [as: :type]} = table}) when command in @drops, do:
    drop("TYPE", table, command)
  def to_cql({command, %Table{} = table}) when command in @drops, do:
    drop("TABLE", table, command)
  def to_cql({command, %Index{} = index}) when command in @drops, do:
    drop("INDEX", index, command)

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

  defp alter(%Table{options: [as: :type]} = table), do: assemble(["ALTER TYPE", quote_table(table)])
  defp alter(%Table{} = table), do: assemble(["ALTER TABLE", quote_table(table)])

  defp drop(name, table, command), do:
    assemble(["DROP", name, if_exists(command), quote_table(table)])

  defp column_changes(table, columns) do
    Enum.map_join(columns, ", ", &column_change(table, &1))
  end
  defp column_change(_table, {_command, _name, %Reference{}, _opts}), do:
    error! nil, "Cassandra adapter does not support references"
  defp column_change(_table, {:add, name, type, opts}), do:
    assemble(["ADD ", quote_name(name), column_type(type, opts)])
  defp column_change(_table, {:modify, name, type, opts}), do:
    assemble(["ALTER ", quote_name(name), "TYPE", column_type(type, opts)])
  defp column_change(_table, {:remove, name}), do: "DROP #{quote_name(name)}"

  defp reference_column_type(type, opts), do:
    column_type(type, opts)

  defp column_definition(_table, {:add, name, %Reference{} = ref, opts}), do:
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
  defp ecto_to_db({:map, t}),        do: ecto_to_db({:map, {:varchar, t}})
  defp ecto_to_db({:array, t}),      do: "list<#{ecto_to_db(t)}>"
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

  defp with_definitions_any(%{options: opts} = any) do
    with_definitions = case with_definitions(any) do
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

  defp pk_definition(columns) do
    pks =
      for {_, name, _, opts} <- columns,
          opts[:primary_key],
          do: name

    partition_keys =
      for {_, name, _, opts} <- columns,
          opts[:partition_key],
          do: name

    clustering_cols =
      for {_, name, _, opts} <- columns,
          opts[:clustering_column],
          do: name

    if length(pks) > 0 && length(partition_keys ++ clustering_cols) > 0, do:
      error! nil,
        """
        Cassandra adapter doesn't allow to mix :primary_key
        with :partition_key or :clustering_column
        """

    if length(pks) > 0 do
      [head | tail] = pks
      {partition_keys, clustering_cols} = {[head], tail}
    end

    if partition_keys == [], do:
      error! nil,
        "Cassandra adapter requires any of :primary_key or :partition_key"

    partition_part = case partition_keys do
      [p] -> quote_name(p)
      _  -> "(" <> Enum.map_join(partition_keys, ", ", &quote_name/1) <> ")"
    end

    clustering_part = case clustering_cols do
      [] -> ""
      _  -> ", " <> Enum.map_join(clustering_cols, ", ", &quote_name/1)
    end

    "PRIMARY KEY (#{partition_part}#{clustering_part})"
  end
end
