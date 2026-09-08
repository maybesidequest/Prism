defmodule Prism.DeliveryTiming do
  @moduledoc """
  Turns the delivery pipeline's CloudEvent timestamps into stable latency
  measurements. Missing headers degrade to the transport enqueue timestamp so
  rolling upgrades do not reject or mis-handle older jobs.
  """

  require OpenTelemetry.Tracer

  defstruct [
    :origin_at_ms,
    :ingress_at_ms,
    :decision_at_ms,
    :outbox_at_ms,
    :published_at_ms,
    :consumed_at_ms,
    complete?: false
  ]

  @type t :: %__MODULE__{
          origin_at_ms: integer(),
          ingress_at_ms: integer() | nil,
          decision_at_ms: integer() | nil,
          outbox_at_ms: integer() | nil,
          published_at_ms: integer() | nil,
          consumed_at_ms: integer(),
          complete?: boolean()
        }

  @doc false
  @spec from_metadata(map(), integer(), integer()) :: t()
  def from_metadata(metadata, fallback_enqueued_at_ms, consumed_at_ms) do
    headers = normalized_headers(metadata)
    origin_at_ms = integer_header(headers, "interchat-origin-time-ms")
    ingress_at_ms = integer_header(headers, "interchat-ingress-time-ms")
    decision_at_ms = integer_header(headers, "interchat-decision-time-ms")
    outbox_at_ms = iso8601_header(headers, "ce_time")
    published_at_ms = integer_header(headers, "interchat-published-at-ms")

    %__MODULE__{
      origin_at_ms: origin_at_ms || outbox_at_ms || fallback_enqueued_at_ms,
      ingress_at_ms: ingress_at_ms,
      decision_at_ms: decision_at_ms,
      outbox_at_ms: outbox_at_ms,
      published_at_ms: published_at_ms,
      consumed_at_ms: consumed_at_ms,
      complete?:
        Enum.all?(
          [origin_at_ms, ingress_at_ms, decision_at_ms, outbox_at_ms, published_at_ms],
          &is_integer/1
        )
    }
  end

  @doc false
  @spec record_consumed(t()) :: map()
  def record_consumed(%__MODULE__{} = timing) do
    measurements =
      %{}
      |> put_duration(:origin_to_ingress_ms, timing.origin_at_ms, timing.ingress_at_ms)
      |> put_duration(:ingress_to_decision_ms, timing.ingress_at_ms, timing.decision_at_ms)
      |> put_duration(:decision_to_outbox_ms, timing.decision_at_ms, timing.outbox_at_ms)
      |> put_duration(:outbox_to_publish_ms, timing.outbox_at_ms, timing.published_at_ms)
      |> put_duration(:publish_to_consume_ms, timing.published_at_ms, timing.consumed_at_ms)
      |> put_duration(:origin_to_consume_ms, timing.origin_at_ms, timing.consumed_at_ms)

    :telemetry.execute(
      [:prism, :delivery, :consumed],
      measurements,
      %{timing_complete: timing.complete?}
    )

    OpenTelemetry.Tracer.set_attributes(
      Enum.map(measurements, fn {name, value} -> {"interchat.#{name}", value} end)
    )

    measurements
  end

  @doc false
  @spec record_completed(integer(), integer(), integer(), integer(), integer(), map()) :: map()
  def record_completed(
        origin_at_ms,
        polled_at_ms,
        request_started_at_ms,
        request_finished_at_ms,
        http_time_ms,
        metadata
      ) do
    measurements = %{
      queue_ms: non_negative(polled_at_ms - origin_at_ms),
      preparation_ms: non_negative(request_started_at_ms - polled_at_ms),
      http_ms: non_negative(http_time_ms),
      origin_to_delivery_ms: non_negative(request_finished_at_ms - origin_at_ms)
    }

    :telemetry.execute([:prism, :delivery, :completed], measurements, metadata)

    OpenTelemetry.Tracer.set_attributes(
      Enum.map(measurements, fn {name, value} -> {"interchat.#{name}", value} end)
    )

    measurements
  end

  defp normalized_headers(metadata) do
    metadata
    |> Map.get(:headers, [])
    |> Enum.into(%{}, fn {key, value} -> {to_string(key), to_string(value)} end)
  end

  defp integer_header(headers, name) do
    case Integer.parse(Map.get(headers, name, "")) do
      {value, ""} when value >= 0 -> value
      _ -> nil
    end
  end

  defp iso8601_header(headers, name) do
    with value when is_binary(value) <- Map.get(headers, name),
         {:ok, datetime, _offset} <- DateTime.from_iso8601(value) do
      DateTime.to_unix(datetime, :millisecond)
    else
      _ -> nil
    end
  end

  defp put_duration(measurements, _name, nil, _finish), do: measurements
  defp put_duration(measurements, _name, _start, nil), do: measurements

  defp put_duration(measurements, name, start, finish),
    do: Map.put(measurements, name, non_negative(finish - start))

  defp non_negative(value), do: max(value, 0)
end
