defmodule Cassandra.Ecto.Storage.CQL do
  def to_cql(:up, opts) do
    replication = opts[:replication] || {"SimpleStrategy",
      replication_factor: 1
    }
    {repl_class, repl_opts} = replication
    durable_writes = opts[:durable_writes] || true
    """
    CREATE KEYSPACE #{opts[:keyspace]}
    WITH REPLICATION = {
      'class' : '#{repl_class}',
      'replication_factor' : #{repl_opts[:replication_factor]}
    }
    AND DURABLE_WRITES = #{durable_writes}
    """
  end
  def to_cql(:down, opts), do: "DROP KEYSPACE #{opts[:keyspace]}"
end
