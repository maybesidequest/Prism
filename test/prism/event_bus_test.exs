defmodule Prism.EventBusTest do
  use ExUnit.Case, async: false

  alias Prism.EventBus
  alias Prism.EventBus.{Publisher, Retry, DLQ}

  @consumer_group "test-consumer-group"

  setup do
    {:ok, redix_conn} = Redix.start_link(Prism.TestRedis.uri(), sync_connect: true)

    # Use a unique stream key per test to prevent cross-contamination
    test_id = System.unique_integer([:positive, :monotonic])
    stream = "events.bus:test:#{test_id}"
    dlq_stream = "#{stream}:dlq"

    # Clean up any leftover keys from previous runs
    try do
      Redix.command!(redix_conn, ["DEL", stream, dlq_stream])
    rescue
      _ -> :ok
    end

    # Override config for test streams
    Application.put_env(:prism, :events_stream, stream)
    Application.put_env(:prism, :events_dlq_stream, dlq_stream)
    Application.put_env(:prism, :events_stream_maxlen, 100_000)
    Application.put_env(:prism, :event_source, "/prism-test")

    on_exit(fn ->
      Application.delete_env(:prism, :events_stream)
      Application.delete_env(:prism, :events_dlq_stream)
      Application.delete_env(:prism, :events_stream_maxlen)
      Application.delete_env(:prism, :event_source)
    end)

    %{redix_conn: redix_conn, stream: stream, dlq_stream: dlq_stream}
  end

  # ── Publisher Tests ─────────────────────────────────────────────────────

  describe "publish/2" do
    test "publishes a CloudEvent to the stream", %{redix_conn: redix_conn, stream: stream} do
      data = %{"test" => true, "value" => 42}

      assert :ok =
               EventBus.publish(stream,
                 type: "fun.interchat.test.event",
                 data: data
               )

      assert 1 = Redix.command!(redix_conn, ["XLEN", stream])

      [[_id, fields]] = Redix.command!(redix_conn, ["XRANGE", stream, "-", "+"])

      stored = stream_fields(fields)
      payload = stored["payload"]

      assert payload != nil
      data = Jason.decode!(payload)

      assert stored["ce_specversion"] == "1.0"
      assert stored["ce_type"] == "fun.interchat.test.event"
      assert stored["ce_source"] == "/prism-test"
      assert stored["ce_datacontenttype"] == "application/json"
      assert data == %{"test" => true, "value" => 42}
      assert String.starts_with?(stored["ce_id"], "evt_")
      assert is_binary(stored["ce_time"])
    end

    test "builds envelope with correct ID format" do
      cloud_event =
        Publisher.build_envelope("fun.interchat.test", "/prism-test", %{"key" => "val"})

      assert String.starts_with?(cloud_event["id"], "evt_")
      assert String.length(cloud_event["id"]) == 36
      assert cloud_event["specversion"] == "1.0"
      assert cloud_event["type"] == "fun.interchat.test"
      assert cloud_event["source"] == "/prism-test"
      assert cloud_event["data"] == %{"key" => "val"}
    end

    test "publish_cloud_event/3 publishes a pre-built envelope", %{
      redix_conn: redix_conn,
      stream: stream
    } do
      cloud_event = %{
        "specversion" => "1.0",
        "type" => "fun.interchat.test.forwarded",
        "source" => "/some-service",
        "id" => "evt_prebuilt123",
        "time" => "2026-06-24T00:00:00Z",
        "datacontenttype" => "application/json",
        "data" => %{"forwarded" => true}
      }

      assert :ok = EventBus.publish_cloud_event(stream, cloud_event)

      assert 1 = Redix.command!(redix_conn, ["XLEN", stream])

      [[_id, fields]] = Redix.command!(redix_conn, ["XRANGE", stream, "-", "+"])

      stored = stream_fields(fields)
      assert stored["ce_type"] == "fun.interchat.test.forwarded"
      assert Jason.decode!(stored["payload"]) == %{"forwarded" => true}
    end
  end

  # ── Retry Tests ────────────────────────────────────────────────────────

  describe "Retry.backoff_ms/3" do
    test "calculates exponential backoff" do
      assert Retry.backoff_ms(1, 1000) == 1000
      assert Retry.backoff_ms(2, 1000) == 2000
      assert Retry.backoff_ms(3, 1000) == 4000
      assert Retry.backoff_ms(4, 1000) == 8000
    end

    test "caps at max_ms" do
      assert Retry.backoff_ms(10, 1000, 5000) == 5000
    end

    test "should_retry?/2 returns correct values" do
      assert Retry.should_retry?(1, 3) == true
      assert Retry.should_retry?(2, 3) == true
      assert Retry.should_retry?(3, 3) == true
      assert Retry.should_retry?(4, 3) == false
    end
  end

  # ── DLQ Tests ──────────────────────────────────────────────────────────

  describe "DLQ.publish/5" do
    test "publishes failed event to DLQ stream", %{redix_conn: redix_conn, dlq_stream: dlq_stream} do
      cloud_event = %{
        "specversion" => "1.0",
        "type" => "fun.interchat.test.failing",
        "source" => "/prism-test",
        "id" => "evt_fail123",
        "time" => "2026-06-24T00:00:00Z",
        "data" => %{"will_fail" => true}
      }

      assert :ok =
               DLQ.publish(cloud_event, "handler timed out", 3, "test-consumer",
                 dlq_stream: dlq_stream
               )

      assert 1 = Redix.command!(redix_conn, ["XLEN", dlq_stream])

      [[_id, fields]] = Redix.command!(redix_conn, ["XRANGE", dlq_stream, "-", "+"])

      payload =
        fields
        |> Enum.chunk_every(2)
        |> Enum.find_value(fn
          ["payload", val] -> val
          _ -> nil
        end)

      dlq_env = Jason.decode!(payload)
      assert dlq_env["original_event"]["id"] == "evt_fail123"
      assert dlq_env["error"] == "handler timed out"
      assert dlq_env["attempts"] == 3
      assert dlq_env["consumer_group"] == "test-consumer"
      assert is_binary(dlq_env["failed_at"])
    end
  end

  # ── Consumer Tests ─────────────────────────────────────────────────────

  describe "Consumer (subscribe)" do
    test "receives and processes events from the stream", %{stream: stream} do
      test_pid = self()

      {:ok, consumer} =
        EventBus.subscribe(
          stream: stream,
          consumer_group: @consumer_group,
          handler: fn cloud_event, _opts ->
            send(test_pid, {:event_received, cloud_event})
            :ok
          end,
          consumer_block_ms: 500,
          consumer_batch_size: 5
        )

      EventBus.publish(stream,
        type: "fun.interchat.test.hello",
        data: %{msg: "hello world"}
      )

      assert_receive {:event_received, cloud_event}, 3000
      assert cloud_event["type"] == "fun.interchat.test.hello"
      assert cloud_event["data"]["msg"] == "hello world"

      Process.exit(consumer, :normal)
    end

    test "retries on handler failure then DLQ", %{
      redix_conn: redix_conn,
      stream: stream,
      dlq_stream: dlq_stream
    } do
      test_pid = self()

      {:ok, consumer} =
        EventBus.subscribe(
          stream: stream,
          consumer_group: "#{@consumer_group}-retry-test",
          handler: fn cloud_event, _opts ->
            send(test_pid, {:event_attempt, cloud_event["id"]})
            {:error, "simulated handler failure"}
          end,
          max_retries: 3,
          retry_backoff_base_ms: 50,
          consumer_block_ms: 500,
          consumer_batch_size: 1
        )

      EventBus.publish(stream,
        type: "fun.interchat.test.failing",
        data: %{will_fail: true}
      )

      for _ <- 1..3 do
        assert_receive {:event_attempt, _event_id}, 3000
      end

      refute_receive {:event_attempt, _}, 1000

      dlq_len = Redix.command!(redix_conn, ["XLEN", dlq_stream])
      assert dlq_len >= 1

      Process.exit(consumer, :normal)
    end

    test "successful handler acks the message", %{redix_conn: redix_conn, stream: stream} do
      test_pid = self()

      {:ok, consumer} =
        EventBus.subscribe(
          stream: stream,
          consumer_group: "#{@consumer_group}-ack-test",
          handler: fn _cloud_event, _opts ->
            send(test_pid, :handler_called)
            :ok
          end,
          consumer_block_ms: 500,
          consumer_batch_size: 5
        )

      EventBus.publish(stream,
        type: "fun.interchat.test.ack",
        data: %{should_ack: true}
      )

      assert_receive :handler_called, 3000

      # The consumer acks after the handler returns.
      Prism.TestWait.eventually(fn ->
        pending =
          try do
            Redix.command!(redix_conn, [
              "XPENDING",
              stream,
              "#{@consumer_group}-ack-test",
              "-",
              "+",
              "100"
            ])
          rescue
            _ -> []
          end

        assert pending == []
      end)

      Process.exit(consumer, :normal)
    end

    test "handler_opts are passed to the handler", %{stream: stream} do
      test_pid = self()

      {:ok, consumer} =
        EventBus.subscribe(
          stream: stream,
          consumer_group: "#{@consumer_group}-opts-test",
          handler: fn _cloud_event, opts ->
            send(test_pid, {:opts, opts})
            :ok
          end,
          handler_opts: %{custom: "value", num: 42},
          consumer_block_ms: 500,
          consumer_batch_size: 1
        )

      EventBus.publish(stream,
        type: "fun.interchat.test.opts",
        data: %{}
      )

      assert_receive {:opts, %{custom: "value", num: 42}}, 3000

      Process.exit(consumer, :normal)
    end
  end

  describe "Redis 7 stale recovery" do
    test "accepts the three-element XAUTOCLAIM response", %{stream: stream} do
      group = "#{@consumer_group}-autoclaim"
      transport = Prism.EventBus.Transport.Redis

      assert {:ok, _} = transport.create_consumer_group(stream, group)

      assert :ok =
               EventBus.publish(stream,
                 type: "fun.interchat.test.claim",
                 data: %{recover: true}
               )

      assert {:ok, [original]} = transport.read_batch(stream, group, "consumer-a", 1, 1)
      assert [claimed] = transport.claim_stale(stream, group, "consumer-b", 0, 1)
      assert claimed.id == original.id
      assert claimed.data == original.data
      assert claimed.headers["ce_type"] == "fun.interchat.test.claim"
    end
  end

  # ── Broadcast Completed Event Data Match ───────────────────────────────

  describe "broadcast completed event format" do
    test "event data matches the contract schema" do
      data = %{
        "batch_id" => "b_a1b2c3d4",
        "action" => "execute",
        "ok_count" => 15,
        "fail_count" => 2,
        "parent_message_id" => "1234567890",
        "hub_id" => "hub_abc123",
        "timestamp" => 1_719_202_374_000
      }

      cloud_event = Publisher.build_envelope("fun.interchat.broadcast.completed", "/prism", data)

      assert cloud_event["type"] == "fun.interchat.broadcast.completed"
      assert cloud_event["data"]["batch_id"] == "b_a1b2c3d4"
      assert cloud_event["data"]["action"] == "execute"
      assert cloud_event["data"]["ok_count"] == 15
      assert cloud_event["data"]["fail_count"] == 2
      assert cloud_event["data"]["parent_message_id"] == "1234567890"
      assert cloud_event["data"]["hub_id"] == "hub_abc123"
      assert cloud_event["data"]["timestamp"] == 1_719_202_374_000
    end
  end

  defp stream_fields(fields) do
    fields
    |> Enum.chunk_every(2)
    |> Map.new(fn [key, value] -> {key, value} end)
  end
end
