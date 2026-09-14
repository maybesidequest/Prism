defmodule Prism.MetricsSinkTest do
  use ExUnit.Case, async: false

  import Plug.Test

  setup do
    :ets.delete_all_objects(:prism_metrics_sink)
    :ok
  end

  test "counts emitted events, keyed by their metadata" do
    :telemetry.execute([:prism, :event_bus, :published], %{count: 1}, %{
      stream: "prism.stream.jobs",
      type: "batch"
    })

    :telemetry.execute([:prism, :event_bus, :published], %{count: 1}, %{
      stream: "prism.stream.jobs",
      type: "batch"
    })

    assert Prism.MetricsSink.render() =~
             ~s(prism_event_bus_published_total{stream="prism.stream.jobs",type="batch"} 2)
  end

  test "accumulates latency measurements as count and sum in milliseconds" do
    :telemetry.execute([:prism, :delivery, :completed], %{http_ms: 12.5}, %{})
    :telemetry.execute([:prism, :delivery, :completed], %{http_ms: 7.5}, %{})

    rendered = Prism.MetricsSink.render()

    assert rendered =~ "prism_delivery_completed_http_ms_count 2"
    assert rendered =~ "prism_delivery_completed_http_ms_sum 20.00"
  end

  test "ignores non-numeric measurements" do
    :telemetry.execute(
      [:prism, :delivery, :consumed],
      %{origin_to_ingress_ms: 3.0, timing_complete: true},
      %{}
    )

    rendered = Prism.MetricsSink.render()

    assert rendered =~ "prism_delivery_consumed_origin_to_ingress_ms_count 1"
    refute rendered =~ "timing_complete"
  end

  test "reports depth events as gauges" do
    :telemetry.execute([:prism, :event_bus, :dlq_depth], %{length: 7}, %{})
    :telemetry.execute([:prism, :discord_worker, :bad_requests_dlq_depth], %{length: 3}, %{})

    rendered = Prism.MetricsSink.render()

    assert rendered =~ "prism_event_bus_dlq_depth 7"
    assert rendered =~ "prism_bad_requests_dlq_depth 3"
  end

  test "reads live gauges at render time instead of caching them" do
    rendered = Prism.MetricsSink.render()

    assert rendered =~ ~s(prism_beam_memory_bytes{kind="total"})
    assert rendered =~ ~s(prism_host_memory_bytes{kind="available"})
    assert rendered =~ "prism_beam_rss_bytes"
    assert rendered =~ "prism_uptime_seconds"
    assert rendered =~ "prism_inflight_batches"
  end

  test "escapes label values" do
    :telemetry.execute([:prism, :event_bus, :dlq], %{count: 1}, %{
      consumer_group: "g",
      type: ~s(quote " and \\ backslash)
    })

    assert Prism.MetricsSink.render() =~ ~s(type="quote \\" and \\\\ backslash")
  end

  test "only allowlisted metadata becomes a label" do
    # `error` is free text from a failed handler and can carry identifiers, so it
    # must not become part of a series key.
    :telemetry.execute([:prism, :event_bus, :dlq], %{count: 1}, %{
      type: "batch",
      consumer_group: "g",
      error: "handler failed for batch 0123456789abcdef"
    })

    rendered = Prism.MetricsSink.render()

    assert rendered =~ ~s(prism_event_bus_dlq_total{consumer_group="g",type="batch"} 1)
    refute rendered =~ "0123456789abcdef"
  end

  test "a per-message identifier cannot create an unbounded number of series" do
    # Regression: `delivery:completed` metadata carries `batch_id`, which is
    # unique per batch. Turning every metadata field into a label put one series
    # per batch on the heap forever — the leak a soak found.
    for index <- 1..500 do
      :telemetry.execute([:prism, :delivery, :completed], %{http_ms: 5.0}, %{
        action: "execute",
        batch_id: "batch-#{index}-#{System.unique_integer([:positive])}",
        webhook_id: "wh-#{index}"
      })
    end

    rendered = Prism.MetricsSink.render()

    assert rendered =~ ~s(prism_delivery_completed_http_ms_count{action="execute"} 500)
    refute rendered =~ "batch-1-", "a batch id must never reach a series key"

    series = Regex.run(~r/prism_metrics_series_total (\d+)/, rendered) |> List.last() |> String.to_integer()
    assert series <= 64, "series must stay bounded by the declared labels"
    assert rendered =~ "prism_metrics_series_dropped_total 0"
  end

  test "is served on /metrics" do
    conn =
      conn(:get, "/metrics")
      |> Prism.Health.call([])

    assert conn.status == 200
    assert conn.resp_body =~ "prism_beam_memory_bytes"
    assert Plug.Conn.get_resp_header(conn, "content-type") |> hd() =~ "text/plain"
  end
end
