defmodule Prism.MetricsSink do
  @moduledoc """
  Turns the telemetry Prism already emits into a scrapeable surface.

  Publish, consume, retry, DLQ and delivery each already called
  `:telemetry.execute/3`, but nothing ever attached a handler, so every one of
  those events was discarded and the only view of Prism's state was a log line
  every ten seconds. This module attaches one handler for all of them and keeps
  the aggregates in an ETS table.

  Counters are pushed by events. Gauges that describe other processes — BEAM
  memory, the async batch counter, the congestion window, host memory — are read
  when `render/0` is called instead of being cached, so a gauge cannot drift from
  the thing it names, and a restart of this process cannot lose a live reading.
  """

  use GenServer

  @table :prism_metrics_sink
  @meminfo "/proc/meminfo"
  @statm "/proc/self/statm"

  # Labels are **allowlisted per event**, not taken from whatever the metadata
  # happens to carry. A Prometheus label set identifies a series, so any
  # per-message field becomes an unbounded number of new series: `batch_id`
  # alone put one series on the heap for every batch ever sent, which is the
  # same class of unbounded growth this project exists to remove. Cardinality is
  # a property of the metric, so the metric has to declare it.
  @families %{
    [:prism, :event_bus, :published] => %{
      kind: :counter,
      name: "prism_event_bus_published_total",
      labels: ["stream", "type"]
    },
    [:prism, :event_bus, :consumed] => %{
      kind: :counter,
      name: "prism_event_bus_consumed_total",
      labels: ["stream", "consumer_group", "type"]
    },
    [:prism, :event_bus, :retries] => %{
      kind: :counter,
      name: "prism_event_bus_retries_total",
      labels: ["stream", "consumer_group", "type", "attempt"]
    },
    # `error` is deliberately not a label: it is free text from a failed
    # handler and can carry identifiers, so it belongs in the log line, not in a
    # series key.
    [:prism, :event_bus, :dlq] => %{
      kind: :counter,
      name: "prism_event_bus_dlq_total",
      labels: ["type", "consumer_group"]
    },
    [:prism, :event_bus, :dlq_depth] => %{
      kind: :gauge,
      name: "prism_event_bus_dlq_depth",
      labels: []
    },
    [:prism, :discord_worker, :bad_requests_dlq_depth] => %{
      kind: :gauge,
      name: "prism_bad_requests_dlq_depth",
      labels: []
    },
    [:prism, :delivery, :consumed] => %{
      kind: :summary,
      name: "prism_delivery_consumed",
      labels: []
    },
    # `action` is a handful of values; `batch_id` and `webhook_id` are not
    # labels, for the reason above.
    [:prism, :delivery, :completed] => %{
      kind: :summary,
      name: "prism_delivery_completed",
      labels: ["action"]
    }
  }

  @max_series_per_family 256

  @events Map.keys(@families)

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  @doc """
  Every metric this node knows about, in Prometheus text exposition format.
  """
  def render do
    rows = table_rows()

    [
      render_counters(rows),
      render_event_gauges(rows),
      render_summaries(rows),
      render_gauges(rows)
    ]
    |> IO.iodata_to_binary()
  end

  @doc """
  Bytes of host memory for `:total` or `:available`, read from `/proc/meminfo`.

  Returns 0 where `/proc` is unavailable, so a non-Linux host reports nothing
  rather than failing the scrape.
  """
  def host_memory(kind) when kind in [:total, :available] do
    key = if kind == :total, do: "MemTotal", else: "MemAvailable"

    case File.read(@meminfo) do
      {:ok, contents} ->
        case Regex.run(~r/^#{key}:\s+(\d+) kB$/m, contents) do
          [_, kib] -> String.to_integer(kib) * 1024
          _ -> 0
        end

      _ ->
        0
    end
  end

  @doc """
  Resident set size of the BEAM process in bytes, read from `/proc/self/statm`.

  Returns 0 where `/proc` is unavailable.
  """
  def beam_rss do
    case File.read(@statm) do
      {:ok, contents} ->
        case Regex.run(~r/^\d+\s+(\d+)/, contents) do
          [_, resident] -> String.to_integer(resident) * page_size()
          _ -> 0
        end

      _ ->
        0
    end
  end

  @impl true
  def init(_opts) do
    # :public because the telemetry handler runs in whichever process emitted the
    # event, not in this one — routing every metric through a GenServer call would
    # put the metrics path in front of the work it measures.
    :ets.new(@table, [:named_table, :public, :set, read_concurrency: true])

    # A restart re-attaches with the same handler id; telemetry keeps the
    # original registration, which already points at this module, so that is not
    # an error worth stopping for.
    case :telemetry.attach_many(__MODULE__, @events, &__MODULE__.handle_event/4, nil) do
      :ok -> {:ok, %{}}
      {:error, :already_exists} -> {:ok, %{}}
    end
  end

  @doc false
  def handle_event(event, measurements, metadata, _config) do
    %{kind: kind, name: name, labels: allowed} = Map.fetch!(@families, event)
    labels = labels(metadata, allowed)

    case kind do
      :counter ->
        increment(name, labels)

      :gauge ->
        set({:gauge, name, []}, Map.get(measurements, :length, 0))

      :summary ->
        Enum.each(measurements, fn
          {key, value} when is_number(value) ->
            # Event measurement keys already carry their unit (`http_ms`,
            # `origin_to_consume_ms`), so the family name must not add another.
            observe("#{name}_#{key}", labels, value)

          _ ->
            :ok
        end)
    end

    :ok
  end

  defp render_counters(rows) do
    rows
    |> Enum.flat_map(fn
      {{:counter, name, labels}, value} -> [{name, labels, value}]
      _ -> []
    end)
    |> Enum.group_by(&elem(&1, 0))
    |> Enum.map(fn {name, samples} ->
      family("counter", name, Enum.map(samples, fn {_, labels, value} -> {labels, value} end))
    end)
  end

  defp render_event_gauges(rows) do
    rows
    |> Enum.flat_map(fn
      {{:gauge, name, _labels}, value} -> [{name, value}]
      _ -> []
    end)
    |> Enum.group_by(&elem(&1, 0))
    |> Enum.map(fn {name, samples} ->
      family("gauge", name, Enum.map(samples, fn {_, value} -> {[], value} end))
    end)
  end

  defp render_summaries(rows) do
    rows
    |> Enum.flat_map(fn
      {{:summary, name, labels}, count, sum} -> [{name, labels, count, sum}]
      _ -> []
    end)
    |> Enum.group_by(&elem(&1, 0))
    |> Enum.map(fn {name, samples} -> summary_family(name, samples) end)
  end

  defp render_gauges(rows) do
    memory = :erlang.memory()

    samples =
      [
        {"prism_beam_memory_bytes", [{"kind", "total"}], memory[:total]},
        {"prism_beam_memory_bytes", [{"kind", "processes"}], memory[:processes]},
        {"prism_beam_memory_bytes", [{"kind", "binary"}], memory[:binary]},
        {"prism_beam_memory_bytes", [{"kind", "ets"}], memory[:ets]},
        {"prism_beam_process_count", [], length(:erlang.processes())},
        {"prism_beam_port_count", [], length(:erlang.ports())},
        {"prism_run_queue", [], :erlang.statistics(:run_queue)},
        {"prism_beam_rss_bytes", [], beam_rss()},
        {"prism_host_memory_bytes", [{"kind", "total"}], host_memory(:total)},
        {"prism_host_memory_bytes", [{"kind", "available"}], host_memory(:available)},
        {"prism_inflight_batches", [], Prism.AsyncBatchCounter.count()},
        {"prism_processed_batches_total", [], Prism.AsyncBatchCounter.get_processed_batches()},
        {"prism_processed_targets_total", [], Prism.AsyncBatchCounter.get_processed_targets()},
        {"prism_uptime_seconds", [],
         safe_value(fn -> Prism.MetricsAPI.get_metrics()[:uptime] end, 0)},
        {"prism_metrics_series_total", [], Enum.sum(series_counts(rows))},
        {"prism_metrics_series_dropped_total", [], dropped_series(rows)}
      ] ++ congestion_gauges()

    samples
    |> Enum.group_by(&elem(&1, 0))
    |> Enum.map(fn {name, grouped} ->
      family("gauge", name, Enum.map(grouped, fn {_, labels, value} -> {labels, value} end))
    end)
  end

  defp series_counts(rows) do
    for {{:series, _name}, count} <- rows, do: count
  end

  defp dropped_series(rows) do
    Enum.find_value(rows, 0, fn
      {{:dropped, :series}, count} -> count
      _ -> nil
    end)
  end

  defp congestion_gauges do
    if Prism.Config.congestion_control_enabled?() do
      [
        {"prism_cwnd_window", [], safe_value(&Prism.CongestionWindow.window_size/0, 0)},
        {"prism_cwnd_cubic", [], safe_value(&Prism.CongestionWindow.cubic_window/0, 0)},
        {"prism_cwnd_safety", [], safe_value(&Prism.CongestionWindow.safety_window/0, 0)},
        {"prism_cwnd_in_flight", [], safe_value(&Prism.CongestionWindow.in_flight/0, 0)},
        {"prism_cwnd_w_max", [], safe_value(&Prism.CongestionWindow.w_max/0, 0)},
        {"prism_cwnd_4xx_budget_used", [], safe_value(&Prism.CongestionWindow.budget_count/0, 0)},
        {"prism_cwnd_estimated_rtt_ms", [],
         safe_value(&Prism.CongestionWindow.estimated_rtt/0, 0)}
      ]
    else
      []
    end
  end

  # A scrape must not fail because a process it reads from happens to be
  # restarting; the reading is simply absent for that scrape.
  defp safe_value(fun, default) do
    fun.()
  rescue
    _ -> default
  catch
    :exit, _ -> default
  end

  defp family(type, name, samples) do
    [
      "# TYPE ",
      name,
      " ",
      type,
      "\n",
      Enum.map(samples, fn {labels, value} ->
        [name, render_labels(labels), " ", format(value), "\n"]
      end)
    ]
  end

  defp summary_family(name, samples) do
    [
      "# TYPE ",
      name,
      " summary\n",
      Enum.map(samples, fn {_, labels, count, sum} ->
        [
          name,
          "_count",
          render_labels(labels),
          " ",
          format(count),
          "\n",
          name,
          "_sum",
          render_labels(labels),
          " ",
          format(sum / 1_000),
          "\n"
        ]
      end)
    ]
  end

  defp render_labels([]), do: []

  defp render_labels(labels) do
    [
      "{",
      Enum.map_join(labels, ",", fn {key, value} ->
        [key, "=\"", escape(value), "\""]
      end),
      "}"
    ]
  end

  defp escape(value) do
    value
    |> String.replace("\\", "\\\\")
    |> String.replace("\"", "\\\"")
    |> String.replace("\n", "\\n")
  end

  defp format(value) when is_integer(value), do: Integer.to_string(value)
  defp format(value) when is_float(value), do: :erlang.float_to_binary(value, decimals: 2)
  defp format(value), do: to_string(value)

  defp labels(metadata, allowed) do
    metadata
    |> Enum.flat_map(fn
      {key, value} ->
        name = to_string(key)

        if name in allowed do
          case render_label_value(value) do
            nil -> []
            rendered -> [{name, rendered}]
          end
        else
          []
        end
      _ ->
        []
    end)
    |> Enum.sort()
  end

  defp render_label_value(value) when is_binary(value), do: value
  defp render_label_value(value) when is_number(value), do: to_string(value)

  defp render_label_value(value) when is_atom(value) and not is_nil(value) and not is_boolean(value),
    do: to_string(value)

  defp render_label_value(_value), do: nil

  defp table_rows do
    if :ets.whereis(@table) == :undefined, do: [], else: :ets.tab2list(@table)
  end

  defp increment(name, labels) do
    key = {:counter, name, labels}

    if admitted?(key, name) do
      :ets.update_counter(@table, key, {2, 1}, {key, 0})
    end
  rescue
    ArgumentError -> :ok
  end

  # Measurements are millisecond floats, but :ets.update_counter/3 accepts
  # integer increments only, so sums accumulate in microseconds and are scaled
  # back to milliseconds when rendered.
  defp observe(name, labels, value) do
    key = {:summary, name, labels}

    if admitted?(key, name) do
      :ets.insert_new(@table, {key, 0, 0})
      :ets.update_counter(@table, key, [{2, 1}, {3, round(value * 1_000)}])
    end
  rescue
    ArgumentError -> :ok
  end

  # The allowlists above are the primary cardinality guard; this is the backstop.
  # A family that somehow meets a high-cardinality field stops growing and starts
  # counting what it dropped, so the failure is visible on the scrape instead of
  # in the node's memory.
  defp admitted?(key, name) do
    cond do
      :ets.member(@table, key) ->
        true

      :ets.update_counter(@table, {:series, name}, {2, 1}, {{:series, name}, 0}) <=
          @max_series_per_family ->
        true

      true ->
        :ets.update_counter(@table, {:dropped, :series}, {2, 1}, {{:dropped, :series}, 0})
        false
    end
  end

  defp set(key, value) do
    :ets.insert(@table, {key, value})
  rescue
    ArgumentError -> :ok
  end

  defp page_size do
    :erlang.system_info(:page_size)
  rescue
    _ -> 4096
  end
end
