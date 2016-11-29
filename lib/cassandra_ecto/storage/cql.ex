defmodule Cassandra.Ecto.Storage.CQL do
  @default_replication_factor 1
  @default_durable_writes     true
  def to_cql(:up, opts) do
    opts = opts
    |> Keyword.put_new(:replication, {"SimpleStrategy",
      replication_factor: @default_replication_factor
    })
    |> Keyword.put_new(:durable_writes, true)
    {repl_class, repl_opts} = opts[:replication]
    durable_writes = opts[:durable_writes] || true
    """
    CREATE KEYSPACE #{opts[:keyspace]}
    WITH REPLICATION = {
      'class' : '#{repl_class}',
      #{replication_options(opts[:replication])}
    }
    AND DURABLE_WRITES = #{opts[:durable_writes]}
    """
  end
  def to_cql(:down, opts), do: "DROP KEYSPACE #{opts[:keyspace]}"

  defp replication_options({_, opts}), do:
    Enum.map_join(opts, ", ", &"'#{elem(&1, 0)}' : #{replication_option(elem(&1, 1))}")

  defp replication_option(opt) when is_integer(opt) or is_float(opt), do: opt
  defp replication_option(opt), do: "'#{opt}'"
end
