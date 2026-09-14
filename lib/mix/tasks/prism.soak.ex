defmodule Mix.Tasks.Prism.Soak do
  @shortdoc "Drives synthetic load and records the node's memory/throughput curve"

  @moduledoc """
  Runs a soak against the started application and writes one JSON sample per
  line, then prints a summary.

      mix prism.soak --duration 3h --rate 20 --targets 8 --output /tmp/soak.jsonl

  Requires `DISCORD_BASE_URL` to point at a local address, because the app builds
  its Finch pool around that value at startup and the harness serves a stand-in
  webhook endpoint there. Redis must be reachable for the callback path.

  ## Options

    * `--duration` — how long to run, e.g. `90s`, `30m`, `3h` (default `1h`)
    * `--rate` — batches started per second (default 10)
    * `--targets` — webhook targets per batch (default 5)
    * `--sample-interval-ms` — sampling period (default 1000)
    * `--max-inflight` — in-flight batch ceiling (default `max_async_batches`)
    * `--output` — JSONL output path (default a timestamped file in the temp dir)
    * `--no-sink` — do not start the local webhook sink
    * `--no-otel-sink` — do not swallow OTLP exports locally
  """

  use Mix.Task

  @switches [
    duration: :string,
    rate: :integer,
    targets: :integer,
    sample_interval_ms: :integer,
    max_inflight: :integer,
    output: :string,
    sink: :boolean,
    otel_sink: :boolean
  ]

  @impl true
  def run(argv) do
    Mix.Task.run("app.start")

    {opts, _args} =
      OptionParser.parse!(argv,
        strict: @switches,
        aliases: [r: :rate, t: :targets, o: :output]
      )

    options =
      [
        duration_ms: parse_duration(Keyword.get(opts, :duration, "1h")),
        rate_per_second: Keyword.get(opts, :rate, 10),
        targets_per_batch: Keyword.get(opts, :targets, 5),
        sample_interval_ms: Keyword.get(opts, :sample_interval_ms, 1_000),
        max_inflight: Keyword.get(opts, :max_inflight),
        output_path: Keyword.get(opts, :output),
        sink: Keyword.get(opts, :sink, true),
        otel_sink: Keyword.get(opts, :otel_sink, true)
      ]
      |> Enum.reject(fn {_key, value} -> is_nil(value) end)

    summary = Prism.SoakHarness.run(options)

    IO.puts(Jason.encode!(summary, pretty: true))
  end

  @doc false
  def parse_duration(value) when is_binary(value) do
    case Regex.run(~r/^(\d+\.?\d*)\s*(ms|s|m|h)?$/, String.trim(value)) do
      [_, amount, unit] -> round(to_number(amount) * multiplier(unit))
      [_, amount] -> round(to_number(amount) * 1_000)
      _ -> raise ArgumentError, "invalid duration: #{inspect(value)}"
    end
  end

  defp to_number(amount) do
    case Float.parse(amount) do
      {number, ""} -> number
      _ -> raise ArgumentError, "invalid duration amount: #{inspect(amount)}"
    end
  end

  defp multiplier("ms"), do: 1
  defp multiplier("s"), do: 1_000
  defp multiplier("m"), do: 60_000
  defp multiplier("h"), do: 3_600_000
end
