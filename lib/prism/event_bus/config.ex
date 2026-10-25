defmodule Prism.EventBus.Config do
  @moduledoc """
  Configuration for the EventBus adapter.
  """

  @doc "Shared event stream key"
  def events_stream, do: Application.get_env(:prism, :events_stream, "events.bus")

  @doc "Dead-letter queue stream key"
  def events_dlq_stream,
    do: Application.get_env(:prism, :events_dlq_stream, "events.bus.dlq")

  @doc "Stream MAXLEN cap (~ approximate)"
  def events_stream_maxlen,
    do: Application.get_env(:prism, :events_stream_maxlen, 100_000)

  @doc "Event source identifier for this service"
  def event_source, do: Application.get_env(:prism, :event_source, "/prism")

  @doc "Max delivery attempts before DLQ"
  def max_retries, do: Application.get_env(:prism, :event_bus_max_retries, 3)

  @doc "Base backoff in ms for retries (doubles each attempt)"
  def retry_backoff_base_ms,
    do: Application.get_env(:prism, :event_bus_retry_backoff_base_ms, 1000)

  @doc "Maximum backoff cap in ms"
  def retry_backoff_max_ms,
    do: Application.get_env(:prism, :event_bus_retry_backoff_max_ms, 30_000)

  @doc "Messages per XREADGROUP batch"
  def consumer_batch_size,
    do: Application.get_env(:prism, :event_bus_consumer_batch_size, 10)

  @doc "XREADGROUP block timeout in ms"
  def consumer_block_ms,
    do: Application.get_env(:prism, :event_bus_consumer_block_ms, 3000)

  @doc "XAUTOCLAIM idle threshold in ms"
  def stale_claim_idle_ms,
    do: Application.get_env(:prism, :event_bus_stale_claim_idle_ms, 30_000)

  @doc "Interval between XAUTOCLAIM runs in ms"
  def stale_claim_interval_ms,
    do: Application.get_env(:prism, :event_bus_stale_claim_interval_ms, 60_000)

  @doc "CloudEvent type for broadcast completion"
  def broadcast_event_type,
    do: Application.get_env(:prism, :event_bus_broadcast_type, "prism.broadcast.completed")

  @doc "CloudEvent type for batch callbacks"
  def callback_event_type,
    do: Application.get_env(:prism, :event_bus_callback_type, "prism.callback")

  @doc "Binary Protobuf topic for authoritative Polarizer delivery receipts"
  def delivery_topic,
    do: Application.get_env(:prism, :delivery_topic, "events.prism.delivery.v2")

  @doc "Required CloudEvents source on authoritative Prism jobs"
  def prism_job_source, do: Application.get_env(:prism, :prism_job_source, "/polarizer")

  @doc "Required CloudEvents type on authoritative Prism jobs"
  def prism_job_event_type,
    do: Application.get_env(:prism, :prism_job_event_type, "fun.interchat.prism.job")

  @doc "Restricted Kafka topic for invalid Prism jobs"
  def prism_jobs_dlq_topic,
    do: Application.get_env(:prism, :prism_jobs_dlq_topic, "prism.stream.jobs.dlq")

  @doc "Durable Kafka topic for retryable Prism jobs"
  def prism_jobs_retry_topic,
    do: Application.get_env(:prism, :prism_jobs_retry_topic, "prism.stream.jobs.retry")

  @doc "Initial backoff for mandatory Kafka retry/DLQ handoffs"
  def prism_handoff_retry_base_ms,
    do: Application.get_env(:prism, :prism_handoff_retry_base_ms, 100)

  @doc "How long a mandatory Kafka handoff retries before falling back"
  def prism_handoff_deadline_ms,
    do: Application.get_env(:prism, :prism_handoff_deadline_ms, 60_000)

  @doc "Transport backend module (production default: Prism.EventBus.Transport.Kafka)"
  def transport_backend,
    do: Application.get_env(:prism, :event_bus_transport_backend, Prism.EventBus.Transport.Kafka)

  @doc "Kafka brokers list for brod"
  def kafka_brokers, do: Application.get_env(:prism, :kafka_brokers, [{"localhost", 9092}])

  @doc "TLS/SASL and topic-creation settings for Kafka clients"
  def kafka_client_config, do: Application.get_env(:prism, :kafka_client_config, [])

  @doc "Full brod client settings, including the no-auto-create guard"
  def kafka_brod_client_config, do: Application.get_env(:prism, :kafka_brod_client_config, [])
end
