defmodule Cassandra.Ecto.Spec.Support.Factories do
  alias Cassandra.Ecto.Spec.Support.Schemas.{Post, User, PersonalInfo}
  def factory(type, args \\ %{}, opts \\ [])
  def factory(:posts, args, _opts), do:
    Enum.map((1..10), fn
      arg -> Map.merge(args, %{title: "title #{arg}", text: "text #{arg}"})
    end)
  def factory(:comments, args, _opts), do:
    Enum.map((1..10), fn
      arg -> Map.merge(args, %{text: "text #{arg}", posted_at: Ecto.DateTime.utc(:usec)})
    end)
  def factory(type, args, with: items) when is_list(items), do:
    Enum.reduce(items, factory(type, args, []), fn
      (p, acc) -> Map.merge(acc, factory({type, p}, %{}, []))
    end)
  def factory({:post, :location}, _args, _opts), do: %{location: {10, 10}}
  def factory({:post, :links}, _args, _opts), do: %{links: %{
    abra:   "http://cadabra",
    crabli: "http://booms!"
  }}
  def factory({:post, :tags}, _args, _opts), do: %{tags: ["abra", "cadabra"]}
  def factory(:post, args, _opts), do:
    Map.merge(%Post{title: "test", text: "test"}, args)
  def factory(:updated_post, post, _opts), do:
    factory(:post, %{id: post.id, title: "new title", text: "updated text"}, [])
  def factory(:user, args, _opts), do:
    Map.merge(%User{name: "anonymous"}, args)
  def factory(:personal_info, args, _opts), do:
    Map.merge(%PersonalInfo{first_name: "Ivan", last_name: "Ivanov", birthdate: {1984, 9, 28}}, args)
end
