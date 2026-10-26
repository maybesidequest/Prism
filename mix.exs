defmodule Prism.MixProject do
  use Mix.Project

  def project do
    [
      app: :prism,
      version: "1.3.5",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {Prism.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:broadway, "~> 1.1"},
      {:off_broadway_redis_stream, "~> 0.7"},
      {:finch, "~> 0.18"},
      {:dotenvy, "~> 1.1"},
      {:jason, "~> 1.4"},
      {:redix, "~> 1.4"},
      {:libcluster, "~> 3.4"},
      {:opentelemetry_api, "~> 1.3"},
      {:opentelemetry_exporter, "~> 1.6"},
      {:opentelemetry, "~> 1.4"},
      {:bandit, "~> 1.5"},
      {:plug, "~> 1.6"},
      {:brod, "~> 4.5"},
      {:broadway_kafka, "~> 0.4.4"},
      {:protox, "~> 2.0"},
      {:ezstd, "~> 1.1"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:mix_audit, "~> 2.1", only: [:dev, :test], runtime: false}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
end
