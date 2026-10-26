defmodule Prism.RetryBroadway do
  @moduledoc """
  Broadway pipeline that reads the Redis retries stream and replays each
  delivery once its retry is due.
  """

  use Broadway

  require Logger

  alias Broadway.Message
  alias Prism.Helpers

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)

    redis_opts = Prism.Config.redis_opts()
    stream_key = Prism.Config.stream_retries()
    consumer_group = Prism.Config.consumer_group() <> "_retries"

    broadway_concurrency = Prism.Config.retry_broadway_concurrency()
    receive_interval = Prism.Config.retry_receive_interval()

    Broadway.start_link(__MODULE__,
      name: name,
      producer: [
        module: {
          OffBroadwayRedisStream.Producer,
          [
            client: Prism.RedisClient,
            redis_client_opts: redis_opts,
            stream: stream_key,
            group: consumer_group,
            consumer_name:
              "broadcast_worker_retry_" <> Integer.to_string(:os.system_time(:microsecond)),
            make_stream: true,
            receive_interval: receive_interval
          ]
        }
      ],
      processors: [
        default: [
          concurrency: broadway_concurrency,
          max_demand: 10,
          min_demand: 0
        ]
      ]
    )
  end

  @impl true
  def handle_message(_, %Message{data: data} = message, _) do
    if Prism.Config.backpressure_enabled?() and Prism.RateLimit.unhealthy?() do
      delay_ms = Prism.RateLimit.backoff_ms()
      [_id, fields] = data
      payload_json = Helpers.get_payload_from_redis_data(fields)

      result =
        with {:ok, raw} <- Jason.decode(payload_json),
             :ok <- Helpers.re_enqueue_on_backpressure(raw, "-Retry", delay_ms) do
          message
        else
          {:error, reason} ->
            Logger.error("Failed to durably re-enqueue retry: #{inspect(reason)}")
            Message.failed(message, {:retry_enqueue_failed, reason})

          _ ->
            Logger.error("Failed to parse retry payload during backpressure")
            Message.failed(message, :invalid_payload_during_backpressure)
        end

      result
    else
      polled_at = :os.system_time(:millisecond)
      [id, fields] = data
      enqueued_at = Helpers.extract_enqueued_at(id)

      payload_json = Helpers.get_payload_from_redis_data(fields)

      case Jason.decode(payload_json) do
        {:ok, payload} ->
          if Map.has_key?(payload, "targets") or Map.get(payload, "type") == "protobuf_batch" do
            case route_batch_to_fanout(payload, payload_json) do
              :ok -> message
              {:error, reason} -> Message.failed(message, {:fanout_republish_failed, reason})
            end
          else
            Prism.DiscordWorker.process_retry(payload, polled_at, enqueued_at)
            message
          end

        _ ->
          Logger.error("Failed to parse retry payload: #{inspect(payload_json)}")
          Message.failed(message, "invalid payload")
      end
    end
  end

  defp route_batch_to_fanout(payload, _payload_json) do
    if Map.get(payload, "type") == "protobuf_batch" do
      stream_key = Prism.Config.stream_jobs()
      Logger.info("[RetryBroadway] Routing protobuf batch back to jobs stream #{stream_key}")

      bytes = Base.decode64!(Map.fetch!(payload, "bytes"))
      headers = Map.get(payload, "headers", %{})
      partition_key = Map.get(payload, "partition_key", "")

      case Prism.EventBus.Publisher.publish_raw(stream_key, bytes,
             headers: headers,
             key: partition_key
           ) do
        :ok ->
          :ok

        {:error, reason} ->
          Logger.error(
            "[RetryBroadway] Failed to publish protobuf batch to #{stream_key}: #{inspect(reason)}"
          )

          {:error, reason}
      end
    else
      targets = Map.get(payload, "targets", [])
      target_count = length(targets)
      batch_id = Map.get(payload, "batch_id", "unknown")

      stream_key = Prism.Config.stream_jobs()

      Logger.info(
        "[RetryBroadway] Routing batch #{batch_id} (#{target_count} targets) back to jobs stream #{stream_key}"
      )

      case Prism.EventBus.Publisher.publish(stream_key, payload, type: "prism.job.fanout") do
        :ok ->
          :ok

        {:error, reason} ->
          Logger.error(
            "[RetryBroadway] Failed to publish batch #{batch_id} to #{stream_key}: #{inspect(reason)}"
          )

          {:error, reason}
      end
    end
  end
end
