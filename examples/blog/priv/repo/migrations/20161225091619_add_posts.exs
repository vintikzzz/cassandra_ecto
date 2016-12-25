defmodule Blog.Repo.Migrations.AddPosts do
  use Ecto.Migration

  def change do
    create table(:posts, primary_key: false) do
      add :id,        :uuid,   primary_key: true
      add :title,     :string
      add :text,      :text
      add :tags,      {:set,   :string}
      timestamps null: true
    end
  end
end
