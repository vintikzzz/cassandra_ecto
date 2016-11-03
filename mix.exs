defmodule CassandraEcto.Mixfile do
  use Mix.Project

  def project do
    [app: :cassandra_ecto,
     version: "0.1.0",
     elixir: "~> 1.3",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     preferred_cli_env: [espec: :test],
     test_coverage: [tool: Coverex.Task],
     deps: deps()]
  end

  # Configuration for the OTP application
  #
  # Type "mix help compile.app" for more information
  def application do
    [applications: [:ecto, :cassandrex]]
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
      {:cassandrex, path: "../cassandrex"},
      {:ecto, path: "../ecto"},
      {:espec, "~> 1.0.1", only: :test},
      {:coverex, "~> 1.4.10", only: :test},
    ]
  end
end
