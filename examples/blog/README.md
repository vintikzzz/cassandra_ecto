# Blog

Example project

```
$ mix do deps.get, ecto.create, ecto.migrate
$ iex -S mix
iex(1)> post = %Blog.Post{title: "some awesome title", text: "great content", tags: ~w(cool awesome great)}
%Blog.Post{__meta__: #Ecto.Schema.Metadata<:built, "posts">, id: nil,
 inserted_at: nil, tags: ["cool", "awesome", "great"], text: "great content",
 title: "some awesome title", updated_at: nil}
iex(2)> Blog.Repo.insert!(post)
%Blog.Post{__meta__: #Ecto.Schema.Metadata<:loaded, "posts">,
 id: <<210, 232, 26, 16, 140, 186, 69, 83, 166, 84, 109, 132, 39, 245, 19,
   217>>, inserted_at: ~N[2016-12-25 09:21:15.569503],
 tags: ["cool", "awesome", "great"], text: "great content",
 title: "some awesome title", updated_at: ~N[2016-12-25 09:21:15.577037]}
iex(3)> Blog.Repo.all(Blog.Post)
[%Blog.Post{__meta__: #Ecto.Schema.Metadata<:loaded, "posts">,
  id: <<210, 232, 26, 16, 140, 186, 69, 83, 166, 84, 109, 132, 39, 245, 19,
    217>>, inserted_at: ~N[2016-12-25 09:21:15.569503],
  tags: ["awesome", "cool", "great"], text: "great content",
  title: "some awesome title", updated_at: ~N[2016-12-25 09:21:15.577037]}]
```

> Note: by default it connects to localhost.
