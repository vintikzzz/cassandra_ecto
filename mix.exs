defmodule CassandraEcto.Mixfile do
  use Mix.Project

  @version "0.2.1"

  def project do
    [app: :cassandra_ecto,
     version: @version,
     elixir: "~> 1.3",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     test_coverage: [tool: ExCoveralls, test_task: "espec"],
     preferred_cli_env: ["coveralls": :test, "coveralls.detail": :test, "coveralls.post": :test, "coveralls.html": :test, espec: :test],
     deps: deps(),

     # Hex
     description: description(),
     package: package(),

     # Docs
     name: "Cassandra.Ecto",
     docs: [source_ref: "v#{@version}", main: "Cassandra.Ecto",
            canonical: "http://hexdocs.pm/cassandra_ecto",
            source_url: "https://github.com/vintikzzz/cassandra_ecto"]]
  end

  # Configuration for the OTP application
  #
  # Type "mix help compile.app" for more information
  def application do
    [applications: [:ecto, :cassandrex, :cqerl]]
  end

  # Dependencies can be Hex packages:
  #
  #   {:mydep, "~> 0.3.0"}
  #
  # Or git/path repositories:
  #
  #   {:mydep, git: "https://github.com/elixir-lang/mydep.git", tag: "0.1.0"}
  #
  # Type "mix help deps" for more examples and options
  defp deps do
    [
      {:cqerl, github: "matehat/cqerl", tag: "v1.0.2", only: :test},
      {:cassandrex, "~> 0.1.0"},
      {:ecto, "~> 2.1.0"},
      {:espec, "~> 1.2.0", only: :test},
      {:excoveralls, "~> 0.5", only: :test},
      {:credo, "~> 0.5", only: [:dev, :test]},
      {:ex_doc, "~> 0.14", only: :dev},
      {:inch_ex, "~> 0.5.5", only: [:dev, :test]}
    ]
  end

  defp description do
    """
    Ecto integration for Apache Cassandra.
    """
  end

  defp package do
    [maintainers: ["Pavel Tatarskiy"],
     licenses: ["Apache 2.0"],
     links: %{"GitHub" => "https://github.com/vintikzzz/cassandra_ecto"},
     files: ~w(mix.exs README.md lib)]
  end
end
