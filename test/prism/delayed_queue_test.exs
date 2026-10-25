defmodule Prism.DelayedQueueTest do
  use ExUnit.Case, async: false

  alias Prism.DelayedQueue

  setup do
    zset_key = Prism.Config.delayed_zset_key()
    stream_key = Prism.Config.stream_retries()
    pubsub_channel = Prism.Config.pubsub_channel()

    # Ensure Redis is clean before each test to prevent cross-test contamination
    {:ok, redix_conn} = Redix.start_link(Prism.TestRedis.uri(), sync_connect: true)
    Redix.command!(redix_conn, ["DEL", zset_key, stream_key])

    # Subscribe to pubsub for assertions
    Redix.PubSub.subscribe(Prism.PubSub, pubsub_channel, self())
    assert_receive {:redix_pubsub, _, _, :subscribed, %{channel: ^pubsub_channel}}, 1000

    %{
      pubsub_channel: pubsub_channel,
      redix_conn: redix_conn,
      stream_key: stream_key,
      zset_key: zset_key
    }
  end

  describe "enqueue/2" do
    test "enqueues payload into zset with correct timestamp score", %{
      redix_conn: redix_conn,
      zset_key: zset_key
    } do
      payload = %{"action" => "test"}
      delay_ms = 5000
      now = :os.system_time(:millisecond)

      assert :ok = DelayedQueue.enqueue(payload, delay_ms)

      # Check ZSET length
      assert 1 = Redix.command!(redix_conn, ["ZCARD", zset_key])

      # Check item score
      [item_json, score_str] =
        Redix.command!(redix_conn, ["ZRANGE", zset_key, "0", "-1", "WITHSCORES"])

      score = String.to_integer(score_str)
      assert score >= now + delay_ms
      # Add 100ms tolerance for execution time
      assert score <= now + delay_ms + 100

      # Check payload structure
      decoded = Jason.decode!(item_json)
      assert decoded["action"] == "test"
      assert is_binary(decoded["retry_id"])
    end

    test "publishes a wakeup event if the enqueued item is the earliest", %{
      pubsub_channel: pubsub_channel
    } do
      # Enqueue an item 10 seconds in the future
      assert :ok = DelayedQueue.enqueue(%{"id" => 1}, 10_000)

      # We should receive a wakeup event
      assert_receive {:redix_pubsub, _, _, :message, %{channel: ^pubsub_channel, payload: msg1}},
                     1000

      assert String.starts_with?(msg1, "new_earliest:")

      # Enqueue an item 20 seconds in the future
      assert :ok = DelayedQueue.enqueue(%{"id" => 2}, 20_000)

      # We should NOT receive a wakeup event because it's not the earliest
      refute_receive {:redix_pubsub, _, _, :message, _}, 500

      # Enqueue an item 5 seconds in the future (earliest!)
      assert :ok = DelayedQueue.enqueue(%{"id" => 3}, 5_000)

      # We SHOULD receive a new wakeup event
      assert_receive {:redix_pubsub, _, _, :message, %{channel: ^pubsub_channel, payload: msg3}},
                     1000

      assert String.starts_with?(msg3, "new_earliest:")
    end
  end

  describe "migrate_due_items/1" do
    test "migrates items whose score is <= now to the stream", %{
      redix_conn: redix_conn,
      stream_key: stream_key,
      zset_key: zset_key
    } do
      now = :os.system_time(:millisecond)

      # Item 1: Already due (score = now - 1000)
      Redix.command!(redix_conn, ["ZADD", zset_key, to_string(now - 1000), "{\"id\": 1}"])
      # Item 2: Exactly due (score = now)
      Redix.command!(redix_conn, ["ZADD", zset_key, to_string(now), "{\"id\": 2}"])
      # Item 3: Not due yet (score = now + 5000)
      Redix.command!(redix_conn, ["ZADD", zset_key, to_string(now + 5000), "{\"id\": 3}"])

      assert 3 = Redix.command!(redix_conn, ["ZCARD", zset_key])

      # Migrate
      assert {:ok, next_score} = DelayedQueue.migrate_due_items(now)

      # The next score should be item 3's score
      assert next_score == now + 5000

      # ZSET should now only have 1 item left
      assert 1 = Redix.command!(redix_conn, ["ZCARD", zset_key])

      # The Stream should have 2 items
      assert stream_count = Redix.command!(redix_conn, ["XLEN", stream_key])
      assert stream_count == 2

      # Check Stream contents
      stream_items = Redix.command!(redix_conn, ["XRANGE", stream_key, "-", "+"])

      # stream_items is a list of entries like: [["id1", ["payload", "val1"]], ["id2", ["payload", "val2"]]]

      payloads = Enum.map(stream_items, fn [_id, ["payload", val]] -> val end)
      assert "{\"id\": 1}" in payloads
      assert "{\"id\": 2}" in payloads
      refute "{\"id\": 3}" in payloads
    end

    test "returns nil when there are no items left" do
      # Item 1: Due
      assert :ok = DelayedQueue.enqueue(%{"id" => 1}, 0)

      # Allow time to pass
      Process.sleep(10)

      assert {:ok, nil} = DelayedQueue.migrate_due_items(:os.system_time(:millisecond))
    end
  end
end
