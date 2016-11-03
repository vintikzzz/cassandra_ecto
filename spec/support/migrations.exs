defmodule Cassandra.Spec.Support.Migrations do
  defmodule CreateMigration do
    use Cassandra.Ecto.Migration

    @table :create_table_migration

    def up do
      create table(@table, primary_key: false) do
        add :id,    :uuid,     primary_key: true
        add :value, :integer
      end
    end

    def down do
      drop table(@table)
    end
  end

  defmodule CreateWithDifferentTypesMigration do
    use Cassandra.Ecto.Migration

    @table :create_table_with_different_types_migration

    def up do
      create table(@table, primary_key: false) do
        add :id,      :uuid,              primary_key: true
        add :value1,  :binary_id
        add :value2,  :integer
        add :value3,  :float
        add :value4,  :boolean
        add :value5,  :string
        add :value6,  :binary
        add :value7,  {:array, :integer}
        add :value8,  :decimal
        add :value9,  :map
        add :value10, {:map, :integer}
        add :value11, :date
        add :value12, :datetime
        add :value13, :time
        add :value14, {:set, :integer}
      end
    end

    def down do
      drop table(@table)
    end
  end

  defmodule CreateUserTypeMigration do
    use Cassandra.Ecto.Migration

    @table :create_user_type_migration

    def up do
      create type(:my_nested_type) do
        add :value,   :integer
      end
      create type(:mytype) do
        add :value1,  :integer
        add :value2,  :my_nested_type,    frozen: true
      end
      create table(@table, primary_key: false) do
        add :id,      :uuid,              primary_key: true
        add :value1,  :mytype,            frozen: true
        add :value2,  {:array, :mytype},  frozen: true
      end
      execute "INSERT INTO #{@table} (id, value1, value2) VALUES (now(),
        {value1: 1, value2: {value: 1}}, [{value1: 2, value2: {value: 2}},
        {value1: 3, value2: {value: 3}}])"
    end

    def down do
      drop table(@table)
      drop type(:mytype)
      drop type(:my_nested_type)
    end
  end

  defmodule CreateWithCompoundPrimaryKeyAndPropertiesMigration do
    use Cassandra.Ecto.Migration

    @table :create_table_with_compound_primary_key_migration

    def up do
      create table(@table, primary_key: false, options: [
        clustering_order_by: [value: :desc, value2: :asc],
        id: "5a1c395e-b41f-11e5-9f22-ba0be0483c18", compact_storage: true,
        comment: "Test", read_repair_chance: 1.0,
        compression: [sstable_compression: "DeflateCompressor", chunk_length_kb: 64]]) do
        add :id, :uuid, partition_key: true
        add :id2, :uuid, partition_key: true
        add :value, :integer, clustering_column: true
        add :value2, :integer, clustering_column: true
      end
    end

    def down do
      drop table(@table)
    end
  end

  defmodule CreateWithStaticColumnMigration do
    use Cassandra.Ecto.Migration

    @table :create_table_with_static_columns_migration

    def up do
      create table(@table, primary_key: false) do
        add :id, :uuid, primary_key: true
        add :clustering_id, :uuid, primary_key: true
        add :value, :integer, static: true
      end
    end

    def down do
      drop table(@table)
    end
  end

  defmodule CreateWithFrozenTypeMigration do
    use Cassandra.Ecto.Migration

    @table :create_table_with_frozen_type_migration

    def up do
      create table(@table, primary_key: false) do
        add :id, :uuid, primary_key: true
        add :value, :map, frozen: true
      end
    end

    def down do
      drop table(@table)
    end
  end

  defmodule AddColumnMigration do
    use Cassandra.Ecto.Migration

    @table :add_col_migration

    def up do
      create table(@table, primary_key: false) do
        add :id, :uuid, primary_key: true
        add :value, :integer
      end

      alter table(@table) do
        add :to_be_added, :integer
      end

      execute "INSERT INTO #{@table} (id, value, to_be_added) VALUES (now(), 1, 2)"
    end

    def down do
      drop table(@table)
    end
  end

  defmodule AlterTypeMigration do
    use Cassandra.Ecto.Migration

    @table :alter_type_migration

    def up do
      create type(@table) do
        add :value, :integer
      end

      alter type(@table) do
        add :to_be_added, :integer
      end
    end

    def down do
      drop type(@table)
    end
  end

  defmodule DropColumnMigration do
    use Cassandra.Ecto.Migration

    @table :drop_col_migration

    def up do
      create table(@table, primary_key: false) do
        add :id, :uuid, primary_key: true
        add :value, :integer
        add :to_be_removed, :integer
      end

      execute "INSERT INTO #{@table} (id, value, to_be_removed) VALUES (now(), 1, 2)"

      alter table(@table) do
        remove :to_be_removed
      end
    end

    def down do
      drop table(@table)
    end
  end

  defmodule ChangeColumnMigration do
    use Cassandra.Ecto.Migration

    @table :change_col_migration

    def up do
      create table(@table, primary_key: false) do
        add :id, :uuid, primary_key: true
        add :value, :integer
        add :to_be_changed, :text
      end

      alter table(@table) do
        modify :to_be_changed, :blob
      end
    end

    def down do
      drop table(@table)
    end
  end

  defmodule RenameColumnMigration do
    use Cassandra.Ecto.Migration

    @table :rename_col_migration

    def up do
      create table(@table, primary_key: false) do
        add :id, :uuid, primary_key: true
        add :to_be_renamed, :integer, primary_key: true
      end
      rename table(@table), :to_be_renamed, to: :renamed
      execute "INSERT INTO #{@table} (id, renamed) VALUES (now(), 2)"
    end

    def down do
      drop table(@table)
    end
  end

  defmodule RenameTableMigration do
    use Cassandra.Ecto.Migration

    @table :to_be_renamed_table_migration
    @renamed_table :renamed_table_migration

    def up do
      create table(@table, primary_key: false) do
        add :id, :uuid, primary_key: true
        add :value, :integer
      end
      rename table(@table), to: table(@renamed_table)
      execute "INSERT INTO #{@renamed_table} (id, value) VALUES (now(), 2)"
    end

    def down do
      drop table(@renamed_table)
    end
  end

  defmodule ConstraintMigration do
    use Cassandra.Ecto.Migration

    @table :constraint_table_migration

    def up do
      create table(@table, primary_key: false) do
        add :id, :uuid, primary_key: true
        add :value, :integer
      end
      create constraint(:value, :value_must_be_positive, check: "price > 0")
    end

    def down do
      drop table(@table)
    end
  end

  defmodule IndexMigration do
    use Cassandra.Ecto.Migration

    @table :index_migration

    def up do
      create table(@table, primary_key: false) do
        add :id, :uuid, primary_key: true
        add :value, :integer
      end
      create index(@table, [:value])
    end

    def down do
      drop index(@table, [:value])
      drop table(@table)
    end
  end
  defmodule CustomIndexMigration do
    use Cassandra.Ecto.Migration

    @table :custom_index_migration

    def up do
      create table(@table, primary_key: false) do
        add :id, :uuid, primary_key: true
        add :value, :string
      end
      create index(@table, [:value], using: "org.apache.cassandra.index.sasi.SASIIndex")
      execute "INSERT INTO #{@table} (id, value) VALUES (now(), 'test')"
    end

    def down do
      drop index(@table, [:value])
      drop table(@table)
    end
  end
  defmodule CustomIndexWithOptsMigration do
    use Cassandra.Ecto.Migration

    @table :custom_index_with_opts_migration

    def up do
      create table(@table, primary_key: false) do
        add :id, :uuid, primary_key: true
        add :value, :string
      end
      create index(@table, [:value], using: "org.apache.cassandra.index.sasi.SASIIndex",
        options: [mode: :contains, case_sensitive: false,
        analyzer_class: "org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer"])
      execute "INSERT INTO #{@table} (id, value) VALUES (now(), 'test')"
    end

    def down do
      drop index(@table, [:value])
      drop table(@table)
    end
  end
end
