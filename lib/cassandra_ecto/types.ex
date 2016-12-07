defmodule Cassandra.Types do
  @moduledoc """
  Represents custom Cassandra types.

  ## Example

      schema "posts" do
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

  > TODO: current implementation is quite useless. Next step is to implement
  > custom composite types on `Ecto` level.
  """

  defmodule Any do
    @moduledoc """
    Represents any type. Passes data to Cassandra as is.
    """
    @behaviour Ecto.Type
    def type, do: :custom
    def cast(value), do: {:ok, value}
    def load(value), do: {:ok, value}
    def dump(value), do: {:ok, value}
  end
  defmodule Tuple do
    @moduledoc """
    Represents Cassandra tuple type.
    """
    @behaviour Ecto.Type
    def type, do: :custom
    def cast(value), do: {:ok, value}
    def load(value), do: {:ok, value}
    def dump(value) when is_tuple(value), do: {:ok, value}
    def dump(_), do: :error
  end
  defmodule Map do
    @moduledoc """
    Represents Cassandra map type.
    """
    @behaviour Ecto.Type
    def type, do: :custom
    def cast(value), do: {:ok, value}
    def load(value), do: {:ok, value}
    def dump(value) when is_map(value), do: {:ok, value}
    def dump(_), do: :error
  end
  defmodule List do
    @moduledoc """
    Represents Cassandra list type.
    """
    @behaviour Ecto.Type
    def type, do: :custom
    def cast(value), do: {:ok, value}
    def load(value), do: {:ok, value}
    def dump(value) when is_list(value), do: {:ok, value}
    def dump(_), do: :error
  end
  defmodule Set do
    @moduledoc """
    Represents Cassandra set type.
    """
    @behaviour Ecto.Type
    def type, do: :custom
    def cast(value), do: {:ok, value}
    def load(value), do: {:ok, value}
    def dump(value) when is_list(value), do: {:ok, value}
    def dump(_), do: :error
  end
end
