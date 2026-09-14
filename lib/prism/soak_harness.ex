defmodule Prism.SoakHarness.Sink do
  @moduledoc """
  Local stand-in for Discord's webhook API.

  Keeps a soak free of credentials and upstream latency: the harness points
  `discord_base_url` here, so the batch path under load is Prism's, and only the
  remote endpoint is replaced.
  """

  use Plug.Router

  plug(:match)
  plug(:dispatch)

  post "/api/webhooks/:webhook_id/:webhook_token" do
    body =
      Jason.encode!(%{
        "id" => "synthetic-#{System.unique_integer([:positive])}",
        "channel_id" => webhook_id
      })

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, body)
  end

  match _ do
    send_resp(conn, 204, "")
  end
end

defmodule Prism.SoakHarness.TelemetrySink do
  @moduledoc """
  Accepts OTLP trace exports for the duration of a soak.

  The batch span processor retries failed exports and holds the spans until they
  succeed. With no collector listening, that backlog grows for as long as the
  soak runs, and its growth would be indistinguishable from a leak in Prism. This
  sink keeps the exporter healthy so the curve measures Prism and nothing else.
  """

  use Plug.Router

  plug(:match)
  plug(:dispatch)

  match _ do
    send_resp(conn, 200, "")
  end
end

defmodule Prism.SoakHarness do
  @moduledoc """
  Drives sustained synthetic load through the real async batch path and records
  the node's memory and throughput curve.

  Batches are spawned exactly the way `Prism.FanoutBroadway` spawns them on the
  Redis lane — `Prism.FanoutBroadway.Batch.spawn_async_batch/10` onto
  `Prism.TaskSup`, which increments the same `Prism.AsyncBatchCounter` — so the
  working set under test is the production one: real batch fan-out, real Finch
  pools, real per-target worker. Egress is aimed at `Prism.SoakHarness.Sink`
  instead of Discord, so a soak needs no credentials and no upstream.

  In-flight batches are held at `:max_inflight` (default
  `Prism.Config.max_async_batches()`). Without that cap the harness would build
  an unbounded queue and produce a memory slope that describes the harness rather
  than Prism — the same backpressure the Broadway lane applies.

  Each sample is one JSON object per line in `:output_path`, which is what the
  soak evidence is read from.
  """

  require Logger

  alias Prism.FanoutBroadway.Batch
  alias Prism.MetricsSink

  @doc """
  Runs a soak and returns its summary.

  Options:

    * `:duration_ms` — how long to drive load (default 60_000)
    * `:rate_per_second` — batches started per second (default 10)
    * `:targets_per_batch` — webhook targets in each batch (default 5)
    * `:sample_interval_ms` — sampling period (default 1_000)
    * `:max_inflight` — in-flight batch ceiling (default `max_async_batches`)
    * `:output_path` — JSONL sample file
    * `:sink` — start the local webhook sink (default true)
    * `:otel_sink` — start the local OTLP sink (default true)
  """
  def run(opts \\ []) do
    duration_ms = Keyword.get(opts, :duration_ms, 60_000)
    rate = Keyword.get(opts, :rate_per_second, 10)
    targets_per_batch = Keyword.get(opts, :targets_per_batch, 5)
    sample_interval_ms = Keyword.get(opts, :sample_interval_ms, 1_000)
    output_path = Keyword.get(opts, :output_path, default_output_path())
    max_inflight = Keyword.get(opts, :max_inflight) || Prism.Config.max_async_batches()

    sink = start_sink(opts)
    otel_sink = start_otel_sink(opts)
    File.write!(output_path, "")

    started_at = System.monotonic_time(:millisecond)

    state = %{
      started_at: started_at,
      # Monotonic time is negative on Linux, so seeding this with 0 would leave
      # the first sample permanently in the future. Seeding one interval back
      # makes the baseline sample land on the first tick.
      last_sample_at: started_at - sample_interval_ms,
      duration_ms: duration_ms,
      tick_interval: max(div(1_000, max(rate, 1)), 1),
      targets_per_batch: targets_per_batch,
      sample_interval_ms: sample_interval_ms,
      max_inflight: max_inflight,
      output_path: output_path,
      spawned: 0,
      deferred: 0,
      samples: 0
    }

    Logger.info(
      "[Soak] duration=#{duration_ms}ms rate=#{rate}/s targets=#{targets_per_batch} " <>
        "max_inflight=#{max_inflight} output=#{output_path}"
    )

    final = run_loop(state)
    summary = summarise(final)

    Logger.info("[Soak] " <> Jason.encode!(summary))
    stop_sink(sink)
    stop_sink(otel_sink)

    summary
  end

  defp run_loop(state) do
    now = System.monotonic_time(:millisecond)
    state = maybe_sample(state, now)

    if now - state.started_at >= state.duration_ms do
      state
    else
      state =
        if Prism.AsyncBatchCounter.count() >= state.max_inflight do
          %{state | deferred: state.deferred + 1}
        else
          state |> spawn_batch() |> Map.update!(:spawned, &(&1 + 1))
        end

      Process.sleep(state.tick_interval)
      run_loop(state)
    end
  end

  defp spawn_batch(state) do
    now = :os.system_time(:millisecond)
    index = state.spawned

    targets =
      Enum.map(1..state.targets_per_batch, fn target_index ->
        %{
          "webhook_id" => "synthetic-webhook-#{rem(index + target_index, 500)}",
          "webhook_token" => "synthetic-token"
        }
      end)

    Batch.spawn_async_batch(
      "execute",
      "soak-#{index}-#{System.unique_integer([:positive])}",
      %{"content" => "soak message #{index}"},
      targets,
      now,
      now,
      nil,
      %{},
      nil,
      0
    )

    state
  end

  defp maybe_sample(state, now) do
    if now - state.last_sample_at >= state.sample_interval_ms do
      write_sample(state)
      %{state | last_sample_at: now, samples: state.samples + 1}
    else
      state
    end
  end

  defp write_sample(state) do
    sample = %{
      "elapsed_ms" => System.monotonic_time(:millisecond) - state.started_at,
      "mem_available_bytes" => MetricsSink.host_memory(:available),
      "mem_total_bytes" => MetricsSink.host_memory(:total),
      "beam_rss_bytes" => MetricsSink.beam_rss(),
      "beam_memory_bytes" => :erlang.memory(:total),
      "beam_process_memory_bytes" => :erlang.memory(:processes),
      "beam_binary_memory_bytes" => :erlang.memory(:binary),
      "erlang_processes" => length(:erlang.processes()),
      "erlang_ports" => length(:erlang.ports()),
      "run_queue" => :erlang.statistics(:run_queue),
      "inflight_batches" => Prism.AsyncBatchCounter.count(),
      "processed_batches" => Prism.AsyncBatchCounter.get_processed_batches(),
      "processed_targets" => Prism.AsyncBatchCounter.get_processed_targets(),
      "spawned_batches" => state.spawned,
      "deferred_ticks" => state.deferred
    }

    File.write!(state.output_path, Jason.encode!(sample) <> "\n", [:append])
  end

  defp summarise(state) do
    samples = read_samples(state.output_path)

    available = Enum.map(samples, & &1["mem_available_bytes"])
    rss = Enum.map(samples, & &1["beam_rss_bytes"])
    elapsed_s = max((System.monotonic_time(:millisecond) - state.started_at) / 1000.0, 0.001)

    %{
      output_path: state.output_path,
      duration_s: Float.round(elapsed_s, 1),
      samples: state.samples,
      spawned_batches: state.spawned,
      deferred_ticks: state.deferred,
      max_inflight_observed:
        samples |> Enum.map(& &1["inflight_batches"]) |> Enum.max(fn -> 0 end),
      processed_targets: Prism.AsyncBatchCounter.get_processed_targets(),
      targets_per_second: Float.round(Prism.AsyncBatchCounter.get_processed_targets() / elapsed_s, 1),
      mem_available_min_bytes: Enum.min(available, fn -> 0 end),
      mem_available_max_bytes: Enum.max(available, fn -> 0 end),
      beam_rss_min_bytes: Enum.min(rss, fn -> 0 end),
      beam_rss_max_bytes: Enum.max(rss, fn -> 0 end)
    }
  end

  defp read_samples(path) do
    path
    |> File.stream!()
    |> Enum.flat_map(fn line ->
      case Jason.decode(line) do
        {:ok, sample} -> [sample]
        _ -> []
      end
    end)
  end

  defp start_sink(opts) do
    if Keyword.get(opts, :sink, true) do
      uri = URI.parse(Prism.Config.discord_base_url())

      {:ok, pid} =
        Bandit.start_link(
          plug: Prism.SoakHarness.Sink,
          scheme: :http,
          port: uri.port || 4002,
          ip: {127, 0, 0, 1}
        )

      Logger.info("[Soak] local webhook sink listening on port #{uri.port || 4002}")
      pid
    end
  end

  defp start_otel_sink(opts) do
    if Keyword.get(opts, :otel_sink, true) do
      uri = URI.parse(Application.get_env(:opentelemetry_exporter, :otlp_endpoint, "http://localhost:4318"))

      {:ok, pid} =
        Bandit.start_link(
          plug: Prism.SoakHarness.TelemetrySink,
          scheme: :http,
          port: uri.port || 4318,
          ip: {127, 0, 0, 1}
        )

      Logger.info("[Soak] local OTLP sink listening on port #{uri.port || 4318}")
      pid
    end
  end

  defp stop_sink(nil), do: :ok

  defp stop_sink(pid) do
    if Process.alive?(pid), do: Supervisor.stop(pid, :normal, 5_000)
  catch
    :exit, _ -> :ok
  end

  defp default_output_path do
    stamp = DateTime.utc_now() |> DateTime.to_iso8601() |> String.replace(~r/[:.]/, "-")
    Path.join(System.tmp_dir!(), "prism-soak-#{stamp}.jsonl")
  end
end
