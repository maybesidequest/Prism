defmodule Prism.DelayedQueue do
  @moduledoc """
  Manages the pure-Redis delayed retry queue using a ZSET.
  """
  require Logger

  alias Prism.Helpers

  @migrate_batch_size 500

  @doc """
  Enqueues a payload for delayed execution.
  `delay_ms` is the number of milliseconds to wait.
  """
  def enqueue(payload, delay_ms) when is_map(payload) and is_integer(delay_ms) do
    retry_id = :crypto.strong_rand_bytes(16) |> Base.encode16()
    payload_with_id = Map.put_new(payload, "retry_id", retry_id)
    json_payload = Jason.encode!(payload_with_id)
    execute_at_ms = :os.system_time(:millisecond) + delay_ms

    zset_key = Prism.Config.delayed_zset_key()
    pubsub_channel = Prism.Config.pubsub_channel()

    script = """
    local zset_key = KEYS[1]
    local pubsub_channel = KEYS[2]
    local score = tonumber(ARGV[1])
    local member = ARGV[2]

    redis.call("ZADD", zset_key, score, member)

    -- Check if it's the earliest item
    local earliest = redis.call("ZRANGE", zset_key, 0, 0, "WITHSCORES")
    if tonumber(earliest[2]) == tonumber(ARGV[1]) then
      redis.call("PUBLISH", pubsub_channel, "new_earliest:" .. score)
      return 1
    end
    return 0
    """

    case Helpers.redix_command([
           "EVAL",
           script,
           "2",
           zset_key,
           pubsub_channel,
           to_string(execute_at_ms),
           json_payload
         ]) do
      {:ok, 1} ->
        :ok

      {:ok, 0} ->
        :ok

      {:error, reason} ->
        Logger.error("Failed to enqueue retry: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @doc """
  Migrates items whose execute_at timestamp is <= now_ms from the ZSET to the retry stream.
  Returns the timestamp of the NEXT earliest item, or nil if empty.
  """
  def migrate_due_items(now_ms) do
    zset_key = Prism.Config.delayed_zset_key()
    stream_key = Prism.Config.stream_retries()

    script = """
    local zset_key = KEYS[1]
    local stream_key = KEYS[2]
    local now_ms = tonumber(ARGV[1])
    local batch_size = tonumber(ARGV[2])

    -- Move due items in bounded batches. An unbounded unpack() fails past
    -- Lua's stack limit (~8000 items), which would wedge the whole backlog.
    repeat
      local due_items = redis.call("ZRANGEBYSCORE", zset_key, "-inf", now_ms, "LIMIT", 0, batch_size)

      for i, item in ipairs(due_items) do
        redis.call("XADD", stream_key, "*", "payload", item)
      end

      if #due_items > 0 then
        redis.call("ZREM", zset_key, unpack(due_items))
      end
    until #due_items < batch_size

    local next_earliest = redis.call("ZRANGE", zset_key, 0, 0, "WITHSCORES")
    if #next_earliest > 0 then
      return next_earliest[2]
    else
      return nil
    end
    """

    case Helpers.redix_command([
           "EVAL",
           script,
           "2",
           zset_key,
           stream_key,
           to_string(now_ms),
           to_string(@migrate_batch_size)
         ]) do
      {:ok, nil} ->
        {:ok, nil}

      {:ok, next_score_str} when is_binary(next_score_str) ->
        case Float.parse(next_score_str) do
          {score, _} -> {:ok, trunc(score)}
          :error -> {:ok, nil}
        end

      {:ok, next_score} when is_integer(next_score) ->
        {:ok, next_score}

      {:error, reason} ->
        Logger.error("Failed to migrate due items: #{inspect(reason)}")
        {:error, reason}
    end
  end
end
