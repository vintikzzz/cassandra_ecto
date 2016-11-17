defmodule Cassandra.Ecto.Spec.Support.Factories do
  alias Cassandra.Ecto.Spec.Support.Schemas.{Post}
  def factory(type, args \\ %{})
  def factory(:posts, args), do:
    Enum.map((1..10), fn
      arg -> Map.merge(args, %{title: "title #{arg}", text: "text #{arg}"})
    end)
  def factory(:post, args), do:
    Map.merge(%Post{title: "test", text: "test", tags: ["abra", "cadabra"]}, args)
  def factory(:updated_post, post), do:
    factory(:post, %{id: post.id, title: "new title", text: "updated text"})
end
