# Cassandra.Ecto

[![Build Status](https://secure.travis-ci.org/vintikzzz/cassandra_ecto.svg?branch=master "Build Status")](http://travis-ci.org/vintikzzz/cassandra_ecto) [![Coverage Status](https://coveralls.io/repos/vintikzzz/cassandra_ecto/badge.svg?branch=master)](https://coveralls.io/r/vintikzzz/cassandra_ecto?branch=master) [![hex.pm version](https://img.shields.io/hexpm/v/cassandra_ecto.svg)](https://hex.pm/packages/cassandra_ecto) [![hex.pm downloads](https://img.shields.io/hexpm/dt/cassandra_ecto.svg)](https://hex.pm/packages/cassandra_ecto) [![Deps Status](https://beta.hexfaktor.org/badge/all/github/vintikzzz/cassandra_ecto.svg)](https://beta.hexfaktor.org/github/vintikzzz/cassandra_ecto)
[![Inline docs](http://inch-ci.org/github/vintikzzz/cassandra_ecto.svg?branch=master&style=flat)](http://inch-ci.org/github/vintikzzz/cassandra_ecto)
[![Ebert](https://ebertapp.io/github/vintikzzz/cassandra_ecto.svg)](https://ebertapp.io/github/vintikzzz/cassandra_ecto)

Ecto integration with Cassandra.

Documentation: http://hexdocs.pm/cassandra_ecto/

## Example

```elixir
# In your config/config.exs file
config :my_app, Repo,
  keyspace: "my_keyspace"

# In your application code
defmodule Repo do
  use Ecto.Repo,
    otp_app: :my_app,
    adapter: Cassandra.Ecto
end

defmodule Post do
  use Ecto.Model

  @primary_key {:id, :binary_id, autogenerate: true}
  schema "posts" do
    field :title,    :string
    field :text,     :string
    field :tags,     {:array, :string}
    timestamps()
  end
end

defmodule Simple do
  import Ecto.Query

  def sample_query do
    query = from p in Post, where: "elixir" in p.tags
    Repo.all(query, allow_filtering: true)
  end
end
```

## Supported Cassandra version

Tested against 3.7+.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed as:

  1. Add `cassandra_ecto` and `cqerl` to your list of dependencies in `mix.exs`:

    ```elixir
    def deps do
      [{:cqerl, github: "matehat/cqerl", tag: "v1.0.2", only: :test},
      {:cassandra_ecto, "~> 0.1.0"}]
    end
    ```

  2. Ensure `cassandra_ecto` and `cqerl` is started before your application:

    ```elixir
    def application do
      [applications: [:cassandra_ecto, :cqerl]]
    end
    ```

## Contributing

To contribute you need to compile `cassandra_ecto` from source and test it:

```
$ git clone https://github.com/vintikzzz/cassandra_ecto.git
$ cd cassandra_ecto
$ mix deps.get
$ mix espec
```

## License

Copyright 2016 Pavel Tatarskiy

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
