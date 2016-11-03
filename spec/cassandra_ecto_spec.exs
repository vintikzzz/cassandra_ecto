defmodule CassandraEctoSpec do
  import Ecto.Migrator, only: [up: 4, down: 4]
  import Ecto.Query, only: [from: 2]
  use ESpec, async: false
  alias Cassandra.Ecto, as: C
  alias Ecto.Integration.TestRepo
  alias Cassandra.Spec.Support.Migrations.{CreateMigration,
    AddColumnMigration, DropColumnMigration, ChangeColumnMigration,
    RenameColumnMigration, RenameTableMigration, ConstraintMigration,
    IndexMigration, CreateWithCompoundPrimaryKeyAndPropertiesMigration,
    CreateWithStaticColumnMigration, CreateWithFrozenTypeMigration,
    CreateWithDifferentTypesMigration, CustomIndexMigration,
    CustomIndexWithOptsMigration, CreateUserTypeMigration,
    AlterTypeMigration
  }

  context "Storage behaviour" do
    context "when storage_up/1" do
      it "creates new keyspace" do
        :ok = C.storage_up(TestRepo.config)
      end
    end
    context "when storage_down/1" do
      it "removes keyspace" do
        :ok = C.storage_down(TestRepo.config)
      end
    end
  end
  context "Migration behaviour" do
    before do
      :ok = C.storage_up(TestRepo.config)
    end
    context "when execute_ddl" do
      context "when :create" do
        context "with %Table" do
          it "creates table" do
            :ok == up(TestRepo, 20050906120000, CreateMigration, log: false)
            :ok == down(TestRepo, 20050906120000, CreateMigration, log: false)
          end
          it "creates table with different types" do
            :ok == up(TestRepo, 23050906120000, CreateWithDifferentTypesMigration, log: false)
            :ok == down(TestRepo, 23050906120000, CreateWithDifferentTypesMigration, log: false)
          end
          it "creates table with compound primary key and properties" do
            :ok == up(TestRepo, 24050906120000, CreateWithCompoundPrimaryKeyAndPropertiesMigration, log: false)
            :ok == down(TestRepo, 24050906120000, CreateWithCompoundPrimaryKeyAndPropertiesMigration, log: false)
          end
          it "creates table with static column" do
            :ok == up(TestRepo, 14050906120000, CreateWithStaticColumnMigration, log: false)
            :ok == down(TestRepo, 14050906120000, CreateWithStaticColumnMigration, log: false)
          end
          it "creates table with frozen type" do
            :ok == up(TestRepo, 15050906120000, CreateWithFrozenTypeMigration, log: false)
            :ok == down(TestRepo, 15050906120000, CreateWithFrozenTypeMigration, log: false)
          end
          it "creates table with user types" do
            :ok == up(TestRepo, 16050906120000, CreateUserTypeMigration, log: false)
            expect(TestRepo.all from p in "create_user_type_migration", select: [p.value1, p.value2])
            |> to(eq [[[value1: 1, value2: [value: 1]], [[value1: 2, value2: [value: 2]], [value1: 3, value2: [value: 3]]]]])
            :ok == down(TestRepo, 16050906120000, CreateUserTypeMigration, log: false)
          end
        end
        context "with %Index" do
          it "creates index" do
            :ok == up(TestRepo, 20550906120000, IndexMigration, log: false)
            :ok == down(TestRepo, 20550906120000, IndexMigration, log: false)
          end
          it "creates custom index" do
            :ok == up(TestRepo, 30550906120000, CustomIndexMigration, log: false)
            expect(TestRepo.all from p in "custom_index_migration", select: p.value, where: like(p.value, "t%")) |> to(eq ["test"])
            :ok == down(TestRepo, 30550906120000, CustomIndexMigration, log: false)
          end
          it "creates custom index with options" do
            :ok == up(TestRepo, 40550906120000, CustomIndexWithOptsMigration, log: false)
            expect(TestRepo.all from p in "custom_index_with_opts_migration", select: p.value, where: like(p.value, "%ES%")) |> to(eq ["test"])
            :ok == down(TestRepo, 40550906120000, CustomIndexWithOptsMigration, log: false)
          end
        end
        context "with %Constraint" do
          it "fails to add constraint" do
            try do
              up(TestRepo, 25010906120000, ConstraintMigration, log: false)
            rescue error in [ArgumentError] ->
              expect(error.message) |> to(have "does not support")
            end
          end
        end
      end
      context "when :create_if_not_exists" do
        pending "creates table"
      end
      context "when :rename" do
        it "renames column" do
          :ok == up(TestRepo, 20010906120000, RenameColumnMigration, log: false)
          expect(TestRepo.all from p in "rename_col_migration", select: p.renamed) |> to(eq [2])
          :ok = down(TestRepo, 20010906120000, RenameColumnMigration, log: false)
        end
        it "fails to rename table" do
          try do
            up(TestRepo, 23010906120000, RenameTableMigration, log: false)
          rescue error in [ArgumentError] ->
            expect(error.message) |> to(have "does not support")
          end
        end
      end
      context "when :alter" do
        it "adds column" do
          :ok == up(TestRepo, 20070906120000, AddColumnMigration, log: false)
          expect(TestRepo.all from p in "add_col_migration", select: p.to_be_added) |> to(eq [2])
          :ok = down(TestRepo, 20070906120000, AddColumnMigration, log: false)
        end
        it "drops column" do
          :ok == up(TestRepo, 20090906120000, DropColumnMigration, log: false)
          try do
            TestRepo.all from p in "drop_col_migration", select: p.to_be_removed
          rescue error in [Cassandrex.Error] ->
            expect(error.code) |> to(eq 8704)
          end
          :ok = down(TestRepo, 20090906120000, DropColumnMigration, log: false)
        end
        it "changes column type" do
          :ok == up(TestRepo, 20100906120000, ChangeColumnMigration, log: false)
          :ok = down(TestRepo, 20100906120000, ChangeColumnMigration, log: false)
        end
        it "also alters types" do
          :ok == up(TestRepo, 19100906120000, AlterTypeMigration, log: false)
          :ok = down(TestRepo, 19100906120000, AlterTypeMigration, log: false)
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
