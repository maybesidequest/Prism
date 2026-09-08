defmodule Prism.FanoutBroadway do
  use Broadway

  require Logger
  require OpenTelemetry.Tracer

  alias Broadway.Message
  alias Prism.Helpers

  def start_link(opts) do
    lane = Keyword.fetch!(opts, :lane)
    name = Keyword.get(opts, :name, __MODULE__)

    stream_key = Prism.Config.stream_jobs()
    receive_interval = Prism.Config.jobs_receive_interval()
    consumer_group = Prism.Config.consumer_group()
    broadway_concurrency = Prism.Config.broadway_concurrency()

    transport_backend = Prism.EventBus.Config.transport_backend()

    producer =
      if transport_backend == Prism.EventBus.Transport.Kafka do
        [
          module: {
            BroadwayKafka.Producer,
            [
              hosts: Prism.EventBus.Config.kafka_brokers(),
              group_id: consumer_group,
              topics: [stream_key, Prism.EventBus.Config.prism_jobs_retry_topic()],
              receive_interval: receive_interval,
              offset_commit_on_ack: true,
              client_config: Prism.EventBus.Config.kafka_client_config(),
              group_config: [
                session_timeout_seconds: 30,
                rebalance_timeout_seconds: 30
              ],
              fetch_config: [max_wait_time: 500],
              offset_reset_policy: :latest
            ]
          },
          concurrency: 5
        ]
      else
        [
          module: {
            OffBroadwayRedisStream.Producer,
            [
              client: Prism.RedisClient,
              redis_client_opts: Prism.Config.redis_opts(),
              stream: stream_key,
              group: consumer_group,
              consumer_name:
                "broadcast_worker_#{lane}_" <> Integer.to_string(:os.system_time(:microsecond)),
              make_stream: true,
              receive_interval: receive_interval
            ]
          }
        ]
      end

    Broadway.start_link(__MODULE__,
      name: name,
      producer: producer,
      processors: [
        default: [
          concurrency: broadway_concurrency,
          max_demand: Prism.Config.broadway_max_demand(),
          min_demand: Prism.Config.broadway_min_demand()
        ]
      ]
    )
  end

  defp extract_payload_and_time(data) do
    case data do
      [id, fields] ->
        enqueued_at = Helpers.extract_enqueued_at(id)
        payload_json = Helpers.get_payload_from_redis_data(fields)
        {payload_json, enqueued_at}

      binary when is_binary(binary) ->
        enqueued_at = :os.system_time(:millisecond)
        {binary, enqueued_at}

      _ ->
        {"", :os.system_time(:millisecond)}
    end
  end

  @doc false
  def validate_job_contract(%Message{data: data, metadata: metadata}) do
    with :ok <- validate_transport_metadata(metadata),
         {payload_binary, _enqueued_at} <- extract_payload_and_time(data),
         {:ok, payload} <- decode_raw_payload(payload_binary),
         :ok <- validate_payload_identity(payload) do
      {:ok, payload}
    end
  end

  defp validate_transport_metadata(metadata) do
    if Prism.EventBus.Config.transport_backend() == Prism.EventBus.Transport.Kafka do
      headers =
        metadata
        |> Map.get(:headers, [])
        |> Enum.into(%{}, fn {key, value} -> {to_string(key), to_string(value)} end)

      expected = %{
        "ce_specversion" => "1.0",
        "ce_source" => Prism.EventBus.Config.prism_job_source(),
        "ce_type" => Prism.EventBus.Config.prism_job_event_type(),
        "ce_datacontenttype" => "application/protobuf",
        "content-type" => "application/protobuf"
      }

      cond do
        Map.get(metadata, :topic) not in [
          Prism.Config.stream_jobs(),
          Prism.EventBus.Config.prism_jobs_retry_topic()
        ] ->
          {:error, :unexpected_topic}

        blank?(Map.get(headers, "ce_id")) or blank?(Map.get(headers, "ce_time")) or
            Enum.any?(expected, fn {key, value} -> Map.get(headers, key) != value end) ->
          {:error, :invalid_cloud_event_headers}

        true ->
          :ok
      end
    else
      :ok
    end
  end

  defp decode_raw_payload(payload_binary) when is_binary(payload_binary) do
    case Prism.PrismStreamPayload.decode(payload_binary) do
      {:ok, payload} -> {:ok, payload}
      {:error, _reason} -> {:error, :invalid_protobuf}
    end
  rescue
    _ -> {:error, :invalid_protobuf}
  end

  defp validate_payload_identity(payload) do
    cond do
      blank?(payload.action_id) -> {:error, :missing_action_id}
      not uuid_v7?(payload.action_id) -> {:error, :invalid_action_id}
      blank?(payload.batch_id) -> {:error, :missing_batch_id}
      blank?(payload.message_id) -> {:error, :missing_message_id}
      payload.targets == [] -> {:error, :missing_targets}
      true -> :ok
    end
  end

  defp uuid_v7?(value) when is_binary(value) do
    Regex.match?(
      ~r/\A[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-7[0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}\z/,
      value
    )
  end

  defp uuid_v7?(_value), do: false
  defp blank?(value), do: is_nil(value) or value == ""

  @impl true
  def handle_message(_, %Message{data: data} = message, _) do
    contract = validate_job_contract(message)

    if match?({:error, _}, contract) do
      {:error, reason} = contract
      Logger.error("Rejected Prism job that violated the Polarizer contract: #{reason}")
      Message.failed(message, {:invalid_contract, reason})
    else
      handle_valid_message(data, message)
    end
  end

  defp handle_valid_message(data, message) do
    wait_for_retry_deadline(message.metadata)

    if Prism.Config.backpressure_enabled?() and Prism.RateLimit.unhealthy?() do
      delay_ms = Prism.RateLimit.backoff_ms()
      publish_retry_job!(message, delay_ms, :backpressure)
      message
    else
      polled_at = :os.system_time(:millisecond)
      {payload_binary, fallback_enqueued_at} = extract_payload_and_time(data)

      timing =
        Prism.DeliveryTiming.from_metadata(message.metadata, fallback_enqueued_at, polled_at)

      OpenTelemetry.Tracer.with_span "prism.delivery.consume" do
        Prism.DeliveryTiming.record_consumed(timing)
        enqueued_at = timing.origin_at_ms

        try do
          payload = Prism.PrismStreamPayload.decode!(payload_binary)

          batch_id = payload.batch_id

          targets =
            Enum.map(payload.targets, fn t ->
              # Parse overrides from JSON string
              overrides =
                if is_nil(t.overrides) or t.overrides == "" do
                  nil
                else
                  Jason.decode!(t.overrides)
                end

              %{
                "channel_id" => t.channel_id,
                "webhook_id" => t.webhook_id,
                "webhook_token" => t.webhook_token,
                "guild_id" => t.guild_id,
                "hub_id" => t.hub_id,
                "thread_id" =>
                  if(is_nil(t.thread_id) or t.thread_id == "", do: nil, else: t.thread_id),
                "message_id" =>
                  if(is_nil(t.message_id) or t.message_id == "", do: nil, else: t.message_id),
                "overrides" => overrides,
                "polarizer_action_id" => payload.action_id
              }
            end)

          action = if payload.action == "", do: "execute", else: payload.action

          # Payload is now a JSON string
          discord_payload =
            if is_nil(payload.payload) or payload.payload == "",
              do: %{},
              else: Jason.decode!(payload.payload)

          parent_message_id = payload.message_id

          metadata =
            if payload.metadata do
              %{
                "author_id" => payload.metadata.author_id,
                "guild_id" => payload.metadata.guild_id,
                "guild_name" => payload.metadata.guild_name,
                "badges" => payload.metadata.badges
              }
            else
              %{}
            end

          hub_id = payload.hub_id

          # The cancel flag means "the source message is gone, stop delivering it".
          # A delete batch is how those copies get removed, so it must never be
          # gated by it — the producer sets the flag on the same message_id right
          # after enqueueing the delete, which would cancel every delete.
          if action != "delete" and parent_message_id != nil and parent_message_id != "" and
               Prism.CancelChecker.cancelled?(parent_message_id) do
            Logger.info(
              "FanoutBroadway: skipping cancelled batch batch_id=#{batch_id} message_id=#{parent_message_id}"
            )

            publish_terminal_failure!(
              payload.action_id,
              parent_message_id,
              "CANCELLED_BEFORE_DELIVERY"
            )

            message
          else
            shard_index = payload.shard_index || 0

            OpenTelemetry.Tracer.set_attributes([
              {:batch_id, batch_id},
              {:action, action},
              {:target_count, length(targets)},
              {:shard_index, shard_index}
            ])

            if action == "execute" and Helpers.empty_discord_payload?(discord_payload) do
              Logger.warning(
                "FanoutBroadway: skipping empty execute batch batch_id=#{batch_id} — no content, embeds, or components"
              )

              publish_terminal_failure!(payload.action_id, parent_message_id, "EMPTY_PAYLOAD")
            else
              if Prism.EventBus.Config.transport_backend() == Prism.EventBus.Transport.Kafka do
                Prism.FanoutBroadway.Batch.process_batch(
                  action,
                  batch_id,
                  discord_payload,
                  targets,
                  polled_at,
                  enqueued_at,
                  parent_message_id,
                  metadata,
                  hub_id,
                  shard_index
                )
              else
                max_async = Prism.Config.max_async_batches()
                current = Prism.AsyncBatchCounter.count()

                if current < max_async do
                  Prism.FanoutBroadway.Batch.spawn_async_batch(
                    action,
                    batch_id,
                    discord_payload,
                    targets,
                    polled_at,
                    enqueued_at,
                    parent_message_id,
                    metadata,
                    hub_id,
                    shard_index
                  )
                else
                  Logger.warning(
                    "Async batch cap reached (#{current}/#{max_async}). " <>
                      "Re-enqueueing batch #{batch_id} to delayed queue (200ms)."
                  )

                  payload_map = retry_payload(payload_binary, message.metadata)

                  case Prism.DelayedQueue.enqueue(payload_map, 200) do
                    :ok ->
                      :ok

                    {:error, reason} ->
                      raise "failed to durably re-enqueue batch: #{inspect(reason)}"
                  end
                end
              end
            end

            message
          end
        rescue
          e ->
            Logger.error("Failed to process approved Prism job: #{Exception.message(e)}")
            Message.failed(message, {:processing_failed, Exception.message(e)})
        end
      end
    end
  end

  @impl true
  def handle_failed(messages, _context) do
    Enum.map(messages, fn message ->
      case message.status do
        {:failed, {:invalid_contract, reason}} ->
          publish_invalid_job!(message, reason)

        {:failed, _reason} ->
          attempts = message.metadata |> normalized_headers() |> retry_attempt()

          if attempts >= Prism.EventBus.Config.max_retries() do
            Logger.error(
              "Prism job exhausted #{attempts} whole-batch retries; sending it to the DLQ"
            )

            publish_invalid_job!(message, :retry_exhausted)
          else
            publish_retry_job!(message, 1_000, :processing_failed)
          end

        _ ->
          :ok
      end

      message
    end)
  end

  @doc false
  def retry_payload(payload_binary, metadata) do
    headers =
      metadata
      |> Map.get(:headers, [])
      |> Enum.into(%{}, fn {key, value} -> {to_string(key), to_string(value)} end)

    %{
      "type" => "protobuf_batch",
      "bytes" => Base.encode64(payload_binary),
      "partition_key" => effective_partition_key(metadata, payload_binary),
      "headers" => headers
    }
  end

  @doc false
  def effective_partition_key(metadata, payload_binary) do
    case metadata |> Map.get(:key, "") |> to_string() do
      "" ->
        case decode_raw_payload(payload_binary) do
          {:ok, payload} -> payload.action_id || payload.batch_id || ""
          {:error, _reason} -> ""
        end

      key ->
        key
    end
  end

  @doc false
  def retry_delay_ms(metadata, now_ms \\ System.system_time(:millisecond)) do
    headers = normalized_headers(metadata)

    case Integer.parse(Map.get(headers, "prism-not-before-ms", "0")) do
      {not_before, ""} -> max(not_before - now_ms, 0)
      _ -> 0
    end
  end

  defp wait_for_retry_deadline(metadata) do
    case retry_delay_ms(metadata) do
      0 ->
        :ok

      delay_ms ->
        Process.sleep(min(delay_ms, 30_000))
        wait_for_retry_deadline(metadata)
    end
  end

  defp publish_retry_job!(message, delay_ms, reason) do
    {payload_binary, _} = extract_payload_and_time(message.data)
    headers = normalized_headers(message.metadata)
    attempt = retry_attempt(headers) + 1
    not_before_ms = System.system_time(:millisecond) + max(delay_ms, 0)
    partition_key = effective_partition_key(message.metadata, payload_binary)

    headers =
      headers
      |> Map.put("partition-key", partition_key)
      |> Map.put_new(
        "prism-original-topic",
        Map.get(message.metadata, :topic, Prism.Config.stream_jobs())
      )
      |> Map.put("prism-retry-attempt", Integer.to_string(attempt))
      |> Map.put("prism-not-before-ms", Integer.to_string(not_before_ms))
      |> Map.put("prism-retry-reason", to_string(reason))
      |> Map.put("interchat-published-at-ms", Integer.to_string(System.system_time(:millisecond)))

    publish_until_ack(
      fn ->
        Prism.EventBus.Transport.publish(
          Prism.EventBus.Config.prism_jobs_retry_topic(),
          payload_binary,
          Prism.EventBus.Config.events_stream_maxlen(),
          headers
        )
      end,
      "retry"
    )
  end

  defp retry_attempt(headers) do
    case Integer.parse(Map.get(headers, "prism-retry-attempt", "0")) do
      {attempt, ""} -> max(attempt, 0)
      _ -> 0
    end
  end

  defp normalized_headers(metadata) do
    metadata
    |> Map.get(:headers, [])
    |> Enum.into(%{}, fn {key, value} -> {to_string(key), to_string(value)} end)
  end

  defp publish_invalid_job!(message, reason) do
    {payload_binary, _} = extract_payload_and_time(message.data)
    retry = retry_payload(payload_binary, message.metadata)

    headers =
      retry["headers"]
      |> Map.put("prism-error-code", to_string(reason))
      |> Map.put("partition-key", retry["partition_key"])

    publish_until_ack(
      fn ->
        Prism.EventBus.Transport.publish(
          Prism.EventBus.Config.prism_jobs_dlq_topic(),
          payload_binary,
          Prism.EventBus.Config.events_stream_maxlen(),
          headers
        )
      end,
      "DLQ"
    )
  end

  defp publish_until_ack(publish, label, attempt \\ 1) do
    result =
      try do
        publish.()
      rescue
        error -> {:error, {:exception, Exception.message(error)}}
      catch
        kind, reason -> {:error, {kind, reason}}
      end

    case result do
      :ok ->
        :ok

      {:ok, _id} ->
        :ok

      {:error, reason} ->
        base = Prism.EventBus.Config.prism_handoff_retry_base_ms()
        delay_ms = min(base * Integer.pow(2, min(attempt - 1, 6)), 5_000)

        Logger.error(
          "Mandatory Kafka #{label} handoff failed; retrying after #{delay_ms}ms: #{inspect(reason)}"
        )

        Process.sleep(delay_ms)
        publish_until_ack(publish, label, attempt + 1)

      other ->
        base = Prism.EventBus.Config.prism_handoff_retry_base_ms()
        delay_ms = min(base * Integer.pow(2, min(attempt - 1, 6)), 5_000)

        Logger.error(
          "Mandatory Kafka #{label} handoff returned an unexpected result; retrying after #{delay_ms}ms: #{inspect(other)}"
        )

        Process.sleep(delay_ms)
        publish_until_ack(publish, label, attempt + 1)
    end
  end

  defp publish_terminal_failure!(action_id, message_id, failure_code) do
    case Helpers.publish_delivery_callback(
           action_id,
           message_id,
           :MESSAGE_STATE_DELIVERY_FAILED,
           failure_code
         ) do
      :ok -> :ok
      {:error, reason} -> raise "authoritative delivery callback failed: #{inspect(reason)}"
    end
  end
end
