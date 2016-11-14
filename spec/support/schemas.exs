defmodule Cassandra.Ecto.Spec.Support.Schemas do
  defmodule Schema do
    defmacro __using__(_) do
      quote do
        use Ecto.Schema
        @primary_key {:id, :binary_id, autogenerate: true}
        @foreign_key_type :binary_id
      end
    end
  end
  defmodule User do
    use Schema
    schema "users" do
      @primary_key {:id, :binary_id, autogenerate: true}
      field :name,     :string
      has_many :posts, Post
      timestamps()
    end
  end
  defmodule Post do
    use Schema
    alias Cassandra.Ecto.Spec.Support.Schemas.{Comment, User}
    schema "posts" do
      @primary_key {:id, :binary_id, autogenerate: true}
      field :title,    :string
      field :text,     :string
      field :public,   :boolean
      field :tags,     {:array, :string}
      field :location, {:array, :float}
      embeds_many :comments, Comment
      belongs_to  :author, User
      timestamps()
    end
  end
  defmodule Comment do
    use Schema
    embedded_schema do
      field :text, :string
      field :posted_at, :date
      belongs_to :user, User
    end
  end
end
