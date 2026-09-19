defmodule BinanceElixir.MixProject do
  use Mix.Project

  def project do
    [
      app: :binance_elixir,
      version: "0.2.1",
      source_url: "https://github.com/ugurbay/binance-elixir-connector",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "Unofficial Binance Spot REST and WebSocket connector for Elixir",
      docs: [main: "readme", extras: ["README.md"]],
      package: [
        licenses: ["MIT"],
        links: %{
          "GitHub" => "https://github.com/ugurbay/binance-elixir-connector",
          "Binance Spot API" =>
            "https://developers.binance.com/en/docs/binance-spot-api-docs/rest-api"
        },
        files: ["lib", "mix.exs", "README.md", "CHANGELOG.md", "LICENSE", ".formatter.exs"]
      ]
    ]
  end

  def application do
    [extra_applications: [:logger, :inets, :ssl, :crypto, :public_key]]
  end

  defp deps do
    [
      {:jason, "~> 1.4"},
      {:decimal, "~> 3.1"},
      {:websockex, "~> 0.5.1"},
      {:ex_doc, "~> 0.38", only: :dev, runtime: false}
    ]
  end
end
