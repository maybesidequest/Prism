defmodule Prism.EventBus.Consumer do
  @moduledoc """
  GenServer consumer that reads CloudEvents from a Redis Stream via a
  consumer group, invokes a handler for each event, and performs retries
  with dead-letter queue on exhaustion.

  ## Starting

      {:ok, pid} = EventBus.Consumer.start_link(
        stream: "events.bus",
        consumer_group: "my-consumer-group",
        handler: &MyHandler.handle/1
      )

  Or via the convenience function:

      {:ok, pid} = EventBus.subscribe("events.bus", "my-group", &MyHandler.handle/1)
  """

  use GenServer

  require Logger

  alias Prism.EventBus.{Config, DLQ, Message, Retry, Telemetry, Transport}

  defstruct [
    :stream,
    :consumer_group,
    :consumer_name,
    :handler,
    :handler_opts,
    :stale_claim_timer_ref,
    :poll_timer_ref,
    state: :init,
    max_retries: 3,
    retry_backoff_base_ms: 1000,
    retry_backoff_max_ms: 30_000,
    batch_size: 10,
    block_ms: 3000,
    stale_claim_idle_ms: 30_000,
    stale_claim_interval_ms: 60_000
  ]

  # ── Client API ──────────────────────────────────────────────────────────

  @doc """
  Starts a consumer GenServer linked to the calling process.

  ## Options
    - `:stream` (required) — Redis stream key
    - `:consumer_group` (required) — consumer group name
    - `:handler` (required) — function receiving `cloud_event` map, returning `:ok | {:error, reason}`
    - `:handler_opts` — arbitrary data passed to handler callbacks (optional)
    - `:max_retries` — max delivery attempts before DLQ (default from config)
    - `:retry_backoff_base_ms` — base backoff in ms (default from config)
    - `:retry_backoff_max_ms` — max backoff cap in ms (default from config)
    - `:consumer_batch_size` — messages per XREADGROUP batch (default from config)
    - `:consumer_block_ms` — XREADGROUP block timeout in ms (default from config)
    - `:stale_claim_idle_ms` — XAUTOCLAIM idle threshold in ms (default from config)
    - `:stale_claim_interval_ms` — interval between XAUTOCLAIM runs (default from config)
  """
  def start_link(opts) do
    stream = Keyword.fetch!(opts, :stream)
    consumer_group = Keyword.fetch!(opts, :consumer_group)
    handler = Keyword.fetch!(opts, :handler)

    consumer_name =
      "#{consumer_group}-#{:erlang.phash2(System.unique_integer(), 1_000_000)}"

    state = %__MODULE__{
      stream: stream,
      consumer_group: consumer_group,
      consumer_name: consumer_name,
      handler: handler,
      handler_opts: Keyword.get(opts, :handler_opts),
      max_retries: Keyword.get(opts, :max_retries, Config.max_retries()),
      retry_backoff_base_ms:
        Keyword.get(opts, :retry_backoff_base_ms, Config.retry_backoff_base_ms()),
      retry_backoff_max_ms:
        Keyword.get(opts, :retry_backoff_max_ms, Config.retry_backoff_max_ms()),
      batch_size: Keyword.get(opts, :consumer_batch_size, Config.consumer_batch_size()),
      block_ms: Keyword.get(opts, :consumer_block_ms, Config.consumer_block_ms()),
      stale_claim_idle_ms: Keyword.get(opts, :stale_claim_idle_ms, Config.stale_claim_idle_ms()),
      stale_claim_interval_ms:
        Keyword.get(opts, :stale_claim_interval_ms, Config.stale_claim_interval_ms())
    }

    GenServer.start_link(__MODULE__, state, name: opts[:name])
  end

  def child_spec(opts) do
    %{
      id: opts[:id] || __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :worker
    }
  end

  # ── GenServer Callbacks ─────────────────────────────────────────────────

  @impl true
  def init(state) do
    Logger.info(
      "[EventBus.Consumer] Starting consumer '#{state.consumer_name}' " <>
        "on stream '#{state.stream}' (group: #{state.consumer_group})"
    )

    # Create consumer group (ignore BUSYGROUP)
    create_consumer_group(state)

    # Schedule initial poll
    ref = Process.send_after(self(), :poll, 0)
    stale_ref = Process.send_after(self(), :stale_claim, state.stale_claim_interval_ms)

    {:ok,
     %{
       state
       | state: :running,
         poll_timer_ref: ref,
         stale_claim_timer_ref: stale_ref
     }}
  end

  @impl true
  def handle_info(:poll, state) do
    case xreadgroup_batch(state) do
      {:ok, []} ->
        {:noreply, schedule_next_poll(state)}

      {:ok, messages} ->
        process_batch_messages(messages, state)
        {:noreply, schedule_next_poll(state)}

      {:error, reason} ->
        Logger.error(
          "[EventBus.Consumer] XREADGROUP error on '#{state.stream}': #{inspect(reason)}"
        )

        Process.sleep(5000)
        {:noreply, schedule_next_poll(state)}
    end
  end

  def handle_info(:stale_claim, state) do
    run_stale_claim(state)
    ref = Process.send_after(self(), :stale_claim, state.stale_claim_interval_ms)

    {:noreply, %{state | stale_claim_timer_ref: ref}}
  end

  # ── Private: Consumer group initialization ──────────────────────────────

  defp create_consumer_group(state) do
    case Transport.create_consumer_group(state.stream, state.consumer_group) do
      {:ok, _} ->
        Logger.info(
          "[EventBus.Consumer] Created consumer group '#{state.consumer_group}' on '#{state.stream}'"
        )

      {:error, %Redix.Error{message: "BUSYGROUP" <> _}} ->
        Logger.debug(
          "[EventBus.Consumer] Consumer group '#{state.consumer_group}' already exists on '#{state.stream}'"
        )

      {:error, reason} ->
        Logger.warning(
          "[EventBus.Consumer] Could not create consumer group '#{state.consumer_group}' " <>
            "on '#{state.stream}': #{inspect(reason)}"
        )
    end
  end

  # ── Private: Message polling ────────────────────────────────────────────

  defp xreadgroup_batch(state) do
    Transport.read_batch(
      state.stream,
      state.consumer_group,
      state.consumer_name,
      state.block_ms,
      state.batch_size
    )
  end

  # ── Private: Batch message processing ───────────────────────────────────

  defp process_batch_messages(messages, state) do
    Enum.each(messages, fn %Message{id: id} = message ->
      case decode_cloud_event(message) do
        {:ok, cloud_event} ->
          process_message(id, cloud_event, state)

        {:error, :invalid_data} ->
          Logger.warning(
            "[EventBus.Consumer] Invalid CloudEvent data for message #{id}. ACKing and sending to DLQ."
          )

          DLQ.publish(
            %{"id" => id, "type" => "unknown"},
            "invalid cloud event envelope",
            0,
            state.consumer_group
          )

          ack_message(state, [id])

        {:error, {:missing_header, header}} ->
          Logger.warning(
            "[EventBus.Consumer] Missing '#{header}' CloudEvents header for message #{id}. ACKing and skipping."
          )

          ack_message(state, [id])

        {:error, {:json_decode, reason}} ->
          Logger.error(
            "[EventBus.Consumer] Failed to parse payload for message #{id}: #{inspect(reason)}"
          )

          DLQ.publish(
            %{"id" => id, "type" => "unknown"},
            "json decode error: #{inspect(reason)}",
            0,
            state.consumer_group
          )

          ack_message(state, [id])
      end
    end)
  end

  # CloudEvents attributes belong in broker headers; the payload contains only
  # event data. Reconstruct the application-facing event at this boundary so
  # handlers remain independent of Redis/Kafka wire framing.
  defp decode_cloud_event(%Message{data: payload, headers: headers}) do
    with {:ok, data} when is_map(data) <- Jason.decode(payload),
         normalized <- normalize_cloud_event_headers(headers),
         :ok <- require_header(normalized, "specversion"),
         :ok <- require_header(normalized, "type"),
         :ok <- require_header(normalized, "source"),
         :ok <- require_header(normalized, "id") do
      {:ok, Map.put(normalized, "data", data)}
    else
      {:ok, _other} -> {:error, :invalid_data}
      {:error, %Jason.DecodeError{} = reason} -> {:error, {:json_decode, reason}}
      {:error, {:missing_header, _header}} = error -> error
    end
  end

  defp normalize_cloud_event_headers(headers) do
    Enum.reduce(headers, %{}, fn
      {"ce_" <> key, value}, event -> Map.put(event, key, value)
      _, event -> event
    end)
  end

  defp require_header(headers, key) do
    if is_binary(headers[key]) and headers[key] != "" do
      :ok
    else
      {:error, {:missing_header, key}}
    end
  end

  defp process_message(id, cloud_event, state) do
    type = cloud_event["type"]
    stream = state.stream
    consumer_group = state.consumer_group

    {_parent_ctx, span_ctx} = Telemetry.span_consume(cloud_event, stream, consumer_group)

    Telemetry.emit_consumed(stream, consumer_group, type)

    # Invoke handler with retry
    result = invoke_with_retry(cloud_event, id, state)

    case result do
      :ok ->
        ack_message(state, [id])

      {:error, error_msg} ->
        s_ctx = Telemetry.span_dlq(cloud_event, stream, consumer_group, error_msg)
        DLQ.publish(cloud_event, error_msg, state.max_retries, consumer_group)
        if s_ctx, do: OpenTelemetry.Span.end_span(s_ctx)
        ack_message(state, [id])
    end

    if span_ctx do
      OpenTelemetry.Span.end_span(span_ctx)
    end
  end

  defp invoke_with_retry(cloud_event, _message_id, state) do
    stream = state.stream
    consumer_group = state.consumer_group

    Enum.reduce_while(1..state.max_retries, nil, fn attempt, _acc ->
      result =
        if attempt > 1 do
          Retry.sleep_for_retry(attempt, state.retry_backoff_base_ms, state.retry_backoff_max_ms)

          Telemetry.emit_retry(stream, consumer_group, cloud_event["type"], attempt)

          Logger.info(
            "[EventBus.Consumer] Retrying event #{cloud_event["id"]} " <>
              "(attempt #{attempt}/#{state.max_retries}, stream=#{stream})"
          )

          s_ctx = Telemetry.span_retry(cloud_event, stream, consumer_group, attempt)
          invoke_result = invoke_handler(cloud_event, state)

          if s_ctx do
            case invoke_result do
              :ok ->
                OpenTelemetry.Span.end_span(s_ctx)

              {:error, _reason} ->
                OpenTelemetry.Span.set_status(s_ctx, :error)
                OpenTelemetry.Span.end_span(s_ctx)
            end
          end

          invoke_result
        else
          invoke_handler(cloud_event, state)
        end

      case result do
        :ok ->
          {:halt, :ok}

        {:error, _reason} = error when attempt < state.max_retries ->
          {:cont, error}

        {:error, _reason} = error ->
          Logger.warning(
            "[EventBus.Consumer] Handler exhausted #{state.max_retries} attempts " <>
              "for event #{cloud_event["id"]} (type=#{cloud_event["type"]})"
          )

          {:halt, error}
      end
    end)
  end

  defp invoke_handler(cloud_event, state) do
    try do
      case state.handler.(cloud_event, state.handler_opts) do
        :ok ->
          :ok

        {:ok, _} ->
          :ok

        {:error, reason} ->
          {:error, reason}

        other ->
          Logger.warning(
            "[EventBus.Consumer] Unexpected handler return for event #{cloud_event["id"]}: #{inspect(other)}"
          )

          :ok
      end
    rescue
      e ->
        Logger.error(
          "[EventBus.Consumer] Handler crashed for event #{cloud_event["id"]}: #{Exception.message(e)}"
        )

        {:error, Exception.message(e)}
    catch
      kind, reason ->
        Logger.error(
          "[EventBus.Consumer] Handler #{kind} for event #{cloud_event["id"]}: #{inspect(reason)}"
        )

        {:error, inspect(reason)}
    end
  end

  # ── Private: Acknowledgment ─────────────────────────────────────────────

  defp ack_message(state, ids) do
    Transport.ack(state.stream, state.consumer_group, ids)
  end

  # ── Private: Stale claim recovery ───────────────────────────────────────

  defp run_stale_claim(state) do
    messages =
      Transport.claim_stale(
        state.stream,
        state.consumer_group,
        state.consumer_name,
        state.stale_claim_idle_ms,
        state.batch_size
      )

    if messages != [] do
      Logger.info(
        "[EventBus.Consumer] XAUTOCLAIM recovered #{length(messages)} stale messages " <>
          "on '#{state.stream}'"
      )

      process_batch_messages(messages, state)
    end
  end

  # ── Private: Scheduling ─────────────────────────────────────────────────

  defp schedule_next_poll(state) do
    ref = Process.send_after(self(), :poll, 0)
    %{state | poll_timer_ref: ref}
  end
end
