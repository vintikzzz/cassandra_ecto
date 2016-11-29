defmodule CassandroEctoStorageCQLSpec do
  import Cassandra.Ecto.Storage.CQL
  use ESpec, async: true
  describe "Cassandra.Ecto.Storage.CQL" do
    describe "to_cql/1" do
      context "with :up" do
        context "with keyspace name only" do
          it "generates cql to create keyspace with SimpleStrategy and replication factor = 1" do
              expect(to_cql(:up, keyspace: :test))
              |> to(eq """
                       CREATE KEYSPACE test
                       WITH REPLICATION = {
                         'class' : 'SimpleStrategy',
                         'replication_factor' : 1
                       }
                       AND DURABLE_WRITES = true
                       """)
          end
        end
        context "with :durable_writes" do
          it "generates cql with provided :durable_writes" do
              expect(to_cql(:up, keyspace: :test, durable_writes: false))
              |> to(eq """
                       CREATE KEYSPACE test
                       WITH REPLICATION = {
                         'class' : 'SimpleStrategy',
                         'replication_factor' : 1
                       }
                       AND DURABLE_WRITES = false
                       """)
          end
        end
        context "with :replication" do
          it "generates cql for SimpleStrategy" do
            expect(to_cql(:up, keyspace: :test,
                          replication: {"SimpleStrategy",
                                        replication_factor: 2}))
            |> to(eq """
                     CREATE KEYSPACE test
                     WITH REPLICATION = {
                       'class' : 'SimpleStrategy',
                       'replication_factor' : 2
                     }
                     AND DURABLE_WRITES = true
                     """)
          end
          it "generates cql for NetworkTopologyStrategy" do
            expect(to_cql(:up, keyspace: :test,
                          replication: {"NetworkTopologyStrategy",
                                        dc1: 1, dc2: 2, dc3: 3}))
            |> to(eq """
                     CREATE KEYSPACE test
                     WITH REPLICATION = {
                       'class' : 'NetworkTopologyStrategy',
                       'dc1' : 1, 'dc2' : 2, 'dc3' : 3
                     }
                     AND DURABLE_WRITES = true
                     """)
          end
        end
      end
      context "with :down" do
        it "generates cql to drop keyspace" do
          expect(to_cql(:down, keyspace: :test))
          |> to(eq "DROP KEYSPACE test")
        end
      end
    end
  end
end
