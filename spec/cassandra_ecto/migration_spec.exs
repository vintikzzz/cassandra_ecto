defmodule CassandraEctoMigrationSpec do
  import Ecto.Migrator, only: [up: 4, down: 4]
  import Ecto.Query
  use ESpec, async: false
  alias Cassandra.Ecto, as: C
  alias Ecto.Integration.TestRepo
  alias Cassandra.Ecto.Spec.Support.Migrations.{CreateMigration,
    AddColumnMigration, DropColumnMigration, ChangeColumnMigration,
    RenameColumnMigration, RenameTableMigration, ConstraintMigration,
    IndexMigration, CreateWithCompoundPrimaryKeyAndPropertiesMigration,
    CreateWithStaticColumnMigration, CreateWithFrozenTypeMigration,
    CreateWithDifferentTypesMigration, CustomIndexMigration,
    CustomIndexWithOptsMigration, CreateUserTypeMigration,
    AlterTypeMigration, CreateCounterMigration, CreateWithPrimaryAndPartitionKeys,
    CreateWithWithoutPrimaryAndPartitionKeys
  }
  describe "Cassandra.Ecto" do
    describe "Migration behaviour" do
      before do
        :ok = C.storage_up(TestRepo.config)
      end
      describe "execute_ddl" do
        context "when :create" do
          context "with %Table" do
            it "creates table" do
              assert :ok = up(TestRepo, 20050906120000, CreateMigration, log: false)
              down(TestRepo, 20050906120000, CreateMigration, log: false)
            end
            it "creates table with different types" do
              assert :ok = up(TestRepo, 23050906120000, CreateWithDifferentTypesMigration, log: false)
              down(TestRepo, 23050906120000, CreateWithDifferentTypesMigration, log: false)
            end
            it "creates table with compound primary key and properties" do
              assert :ok = up(TestRepo, 24050906120000, CreateWithCompoundPrimaryKeyAndPropertiesMigration, log: false)
              down(TestRepo, 24050906120000, CreateWithCompoundPrimaryKeyAndPropertiesMigration, log: false)
            end
            it "fails to create table with primary and partition keys" do
              expect(fn -> up(TestRepo, 84050906120000, CreateWithPrimaryAndPartitionKeys, log: false) end)
              |> to(raise_exception())
            end
            it "fails to create table without primary and partition keys", focus: true do
              expect(fn -> up(TestRepo, 85050906120000, CreateWithWithoutPrimaryAndPartitionKeys, log: false) end)
              |> to(raise_exception())
            end
            it "creates table with static column" do
              assert :ok = up(TestRepo, 14050906120000, CreateWithStaticColumnMigration, log: false)
              down(TestRepo, 14050906120000, CreateWithStaticColumnMigration, log: false)
            end
            it "creates table with frozen type" do
              assert :ok = up(TestRepo, 15050906120000, CreateWithFrozenTypeMigration, log: false)
              down(TestRepo, 15050906120000, CreateWithFrozenTypeMigration, log: false)
            end
            it "creates table with user types" do
              assert :ok = up(TestRepo, 16050906120000, CreateUserTypeMigration, log: false)
              assert [[[value1: 1, value2: [value: 1]], [[value1: 2, value2: [value: 2]], [value1: 3, value2: [value: 3]]]]] =
                TestRepo.all from p in "create_user_type_migration", select: [p.value1, p.value2]
              down(TestRepo, 16050906120000, CreateUserTypeMigration, log: false)
            end
            it "creates table with counter" do
              assert :ok = up(TestRepo, 12050906120000, CreateCounterMigration, log: false)
              assert [1] = TestRepo.all from p in "create_counter_migration", select: p.counter
              down(TestRepo, 12050906120000, CreateCounterMigration, log: false)
            end
          end
          context "with %Index" do
            it "creates index" do
              assert :ok = up(TestRepo, 20550906120000, IndexMigration, log: false)
              down(TestRepo, 20550906120000, IndexMigration, log: false)
            end
            it "creates custom index" do
              assert :ok = up(TestRepo, 30550906120000, CustomIndexMigration, log: false)
              assert ["test"] = TestRepo.all from p in "custom_index_migration", select: p.value, where: like(p.value, "t%")
              down(TestRepo, 30550906120000, CustomIndexMigration, log: false)
            end
            it "creates custom index with options" do
              assert :ok = up(TestRepo, 40550906120000, CustomIndexWithOptsMigration, log: false)
              assert ["test"] = TestRepo.all from p in "custom_index_with_opts_migration", select: p.value, where: like(p.value, "%ES%")
              down(TestRepo, 40550906120000, CustomIndexWithOptsMigration, log: false)
            end
          end
          context "with %Constraint" do
            it "fails to add constraint" do
              expect(fn -> up(TestRepo, 25010906120000, ConstraintMigration, log: false) end)
              |> to(raise_exception())
            end
          end
        end
        context "when :create_if_not_exists" do
          pending "creates table"
        end
        context "when :rename" do
          it "renames column" do
            assert :ok = up(TestRepo, 20010906120000, RenameColumnMigration, log: false)
            assert [2] = TestRepo.all from p in "rename_col_migration", select: p.renamed
            down(TestRepo, 20010906120000, RenameColumnMigration, log: false)
          end
          it "fails to rename table" do
            expect(fn -> up(TestRepo, 23010906120000, RenameTableMigration, log: false) end)
            |> to(raise_exception())
          end
        end
        context "when :alter" do
          it "adds column" do
            assert :ok = up(TestRepo, 20070906120000, AddColumnMigration, log: false)
            assert [2] = TestRepo.all from p in "add_col_migration", select: p.to_be_added
            down(TestRepo, 20070906120000, AddColumnMigration, log: false)
          end
          it "drops column" do
            assert :ok = up(TestRepo, 20090906120000, DropColumnMigration, log: false)
            try do
              TestRepo.all from p in "drop_col_migration", select: p.to_be_removed
            rescue error in [Cassandrex.Error] ->
              expect(error.code) |> to(eq 8704)
            end
            down(TestRepo, 20090906120000, DropColumnMigration, log: false)
          end
          it "changes column type" do
            assert :ok = up(TestRepo, 20100906120000, ChangeColumnMigration, log: false)
            down(TestRepo, 20100906120000, ChangeColumnMigration, log: false)
          end
          it "also alters types" do
            assert :ok = up(TestRepo, 19100906120000, AlterTypeMigration, log: false)
            down(TestRepo, 19100906120000, AlterTypeMigration, log: false)
          end
        end
        context "when :drop" do
          pending "drops table"
        end
        context "when :drop_if_exists" do
          pending "drops table"
        end
      end
    end
  end
end
