defmodule Cassandra.Types do
  defmodule Any do
    @behaviour Ecto.Type
    def type, do: :custom
    def cast(value), do: {:ok, value}
    def load(value), do: {:ok, value}
    def dump(value), do: {:ok, value}
    def dump(_), do: :error
  end
  defmodule Tuple do
    @behaviour Ecto.Type
    def type, do: :tuple
    def cast(value), do: {:ok, value}
    def load(value), do: {:ok, value}
    def dump(value) when is_tuple(value), do: {:ok, value}
    def dump(_), do: :error
  end
  defmodule Map do
    @behaviour Ecto.Type
    def type, do: :custom
    def cast(value), do: {:ok, value}
    def load(value), do: {:ok, value}
    def dump(value) when is_map(value), do: {:ok, value}
    def dump(_), do: :error
  end
  defmodule List do
    @behaviour Ecto.Type
    def type, do: :custom
    def cast(value), do: {:ok, value}
    def load(value), do: {:ok, value}
    def dump(value) when is_list(value), do: {:ok, value}
    def dump(_), do: :error
  end
  defmodule Set do
    @behaviour Ecto.Type
    def type, do: :custom
    def cast(value), do: {:ok, value}
    def load(value), do: {:ok, value}
    def dump(value) when is_list(value), do: {:ok, value}
    def dump(_), do: :error
  end
end
