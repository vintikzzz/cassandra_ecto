defmodule Blog.Post do
  use Ecto.Schema

  @primary_key {:id, :binary_id, autogenerate: true}
  schema "posts" do
    field :title,    :string
    field :text,     :string
    field :tags,     {:array, :string}
    timestamps()
  end
end
