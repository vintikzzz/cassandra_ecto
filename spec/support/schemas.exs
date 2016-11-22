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
    alias Cassandra.Ecto.Spec.Support.Schemas.{Post, PersonalInfo}
    schema "users" do
      @primary_key {:id, :binary_id, autogenerate: true}
      field      :name,  :string
      has_many   :posts, Post
      embeds_one :personal_info, PersonalInfo
      timestamps()
    end
  end
  defmodule PersonalInfo do
    use Schema
    embedded_schema do
      field :first_name, :string
      field :last_name,  :string
      field :birthdate,  Cassandra.Types.Tuple
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
      field :location, Cassandra.Types.Tuple
      field :links,    {:map, :string}
      embeds_many :comments, Comment
      belongs_to  :author,   User
      timestamps()
    end
  end
  defmodule PostStats do
    use Schema
    schema "post_stats" do
      @primary_key {:id, :binary_id}
      field :visits,   :integer
    end
  end
  defmodule Comment do
    use Schema
    alias Cassandra.Ecto.Spec.Support.Schemas.User
    embedded_schema do
      field :text, :string
      field :posted_at, :date
      belongs_to :user, User
    end
  end
end
