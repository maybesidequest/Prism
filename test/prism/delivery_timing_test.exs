defmodule Prism.DeliveryTimingTest do
  use ExUnit.Case, async: true

  test "derives every delivery stage from Polarizer CloudEvent headers" do
    metadata = %{
      headers: [
        {"interchat-origin-time-ms", "1000"},
        {"interchat-ingress-time-ms", "1050"},
        {"interchat-decision-time-ms", "1100"},
        {"ce_time", "1970-01-01T00:00:01.200Z"},
        {"interchat-published-at-ms", "1300"}
      ]
    }

    timing = Prism.DeliveryTiming.from_metadata(metadata, 900, 1500)
    measurements = Prism.DeliveryTiming.record_consumed(timing)

    assert timing.complete?
    assert timing.origin_at_ms == 1000

    assert measurements == %{
             origin_to_ingress_ms: 50,
             ingress_to_decision_ms: 50,
             decision_to_outbox_ms: 100,
             outbox_to_publish_ms: 100,
             publish_to_consume_ms: 200,
             origin_to_consume_ms: 500
           }
  end

  test "old jobs fall back to the transport enqueue time" do
    timing = Prism.DeliveryTiming.from_metadata(%{headers: []}, 700, 900)

    refute timing.complete?
    assert timing.origin_at_ms == 700
    assert Prism.DeliveryTiming.record_consumed(timing) == %{origin_to_consume_ms: 200}
  end

  test "delivery completion records queue, preparation, HTTP, and total latency" do
    assert Prism.DeliveryTiming.record_completed(1000, 1500, 1520, 1600, 80, %{}) == %{
             queue_ms: 500,
             preparation_ms: 20,
             http_ms: 80,
             origin_to_delivery_ms: 600
           }
  end
end
