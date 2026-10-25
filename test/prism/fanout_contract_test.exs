defmodule Prism.FanoutContractTest do
  use ExUnit.Case, async: false

  alias Broadway.Message
  alias Prism.{FanoutBroadway, PrismStreamPayload, PrismTarget}

  @action_id "019f603b-227b-78b2-909e-47717b9b5765"

  setup do
    previous_backend = Application.get_env(:prism, :event_bus_transport_backend)
    previous_stream = Application.get_env(:prism, :stream_jobs)

    Application.put_env(
      :prism,
      :event_bus_transport_backend,
      Prism.EventBus.Transport.Kafka
    )

    Application.put_env(:prism, :stream_jobs, "prism.stream.jobs")

    on_exit(fn ->
      restore_env(:event_bus_transport_backend, previous_backend)
      restore_env(:stream_jobs, previous_stream)
    end)

    :ok
  end

  test "accepts the raw Polarizer PrismStreamPayload with the required envelope metadata" do
    assert {:ok, decoded} =
             valid_payload() |> kafka_message() |> FanoutBroadway.validate_job_contract()

    assert decoded.action_id == @action_id
    assert decoded.batch_id == "batch-1"
    assert decoded.message_id == "message-1"
  end

  test "accepts trusted Polarizer jobs without a Kafka key and derives one for retries" do
    message = kafka_message(valid_payload())
    message = %{message | metadata: Map.put(message.metadata, :key, "")}

    assert {:ok, decoded} = FanoutBroadway.validate_job_contract(message)
    assert decoded.action_id == @action_id

    retry = FanoutBroadway.retry_payload(message.data, message.metadata)
    assert retry["partition_key"] == @action_id
  end

  test "rejects a spoofed or incomplete CloudEvents envelope" do
    message = kafka_message(valid_payload(), [{"ce_source", "/another-service"}])

    assert {:error, :invalid_cloud_event_headers} =
             FanoutBroadway.validate_job_contract(message)
  end

  test "rejects Confluent framing because prism.stream.jobs is raw binary Protobuf" do
    raw = encode(valid_payload())
    message = kafka_message(<<0, 0, 0, 0, 1, raw::binary>>)

    assert {:error, :invalid_protobuf} = FanoutBroadway.validate_job_contract(message)
  end

  test "requires action, batch, message, and target identity" do
    cases = [
      {%{valid_payload() | action_id: nil}, :missing_action_id},
      {%{valid_payload() | action_id: "not-a-uuid"}, :invalid_action_id},
      {%{valid_payload() | batch_id: ""}, :missing_batch_id},
      {%{valid_payload() | message_id: nil}, :missing_message_id},
      {%{valid_payload() | targets: []}, :missing_targets}
    ]

    for {payload, expected_error} <- cases do
      assert {:error, ^expected_error} =
               payload |> kafka_message() |> FanoutBroadway.validate_job_contract()
    end
  end

  test "durable retry projection preserves raw bytes, partition key, and contract headers" do
    message = kafka_message(valid_payload())
    retry = FanoutBroadway.retry_payload(message.data, message.metadata)

    assert Base.decode64!(retry["bytes"]) == message.data
    assert retry["partition_key"] == "HUB:hub-1"
    assert retry["headers"]["ce_source"] == "/polarizer"
    assert retry["headers"]["ce_type"] == "fun.interchat.prism.job"
    assert retry["headers"]["content-type"] == "application/protobuf"
  end

  test "delivery checkpoints include action, batch, and target identity" do
    first = Prism.Helpers.checkpoint_key("execute", "batch-1", "webhook-1", @action_id)
    duplicate = Prism.Helpers.checkpoint_key("execute", "batch-1", "webhook-1", @action_id)

    another_action =
      Prism.Helpers.checkpoint_key(
        "execute",
        "batch-1",
        "webhook-1",
        "019f603b-227c-74c2-909e-47717b9b5765"
      )

    assert first == duplicate
    assert first != another_action
    assert first == "prism:ck:#{@action_id}:batch-1:webhook-1"
  end

  test "a job whose retry deadline is far off is parked in the delayed queue, not slept on" do
    {:ok, redis} = Redix.start_link(Prism.TestRedis.uri(), sync_connect: true)
    zset_key = Prism.Config.delayed_zset_key()
    Redix.command!(redis, ["DEL", zset_key])

    not_before = System.system_time(:millisecond) + 60_000

    message =
      kafka_message(valid_payload(), [{"prism-not-before-ms", Integer.to_string(not_before)}])

    {elapsed_us, result} =
      :timer.tc(fn -> FanoutBroadway.handle_message(:default, message, %{}) end)

    assert result == message
    assert elapsed_us < 1_000_000
    assert [parked, score] = Redix.command!(redis, ["ZRANGE", zset_key, 0, -1, "WITHSCORES"])
    assert_in_delta String.to_integer(score), not_before, 1_000

    parked = Jason.decode!(parked)
    assert parked["type"] == "protobuf_batch"
    assert Base.decode64!(parked["bytes"]) == message.data
    assert parked["headers"]["prism-not-before-ms"] == Integer.to_string(not_before)

    Redix.command!(redis, ["DEL", zset_key])
  end

  defp valid_payload do
    %PrismStreamPayload{
      action_id: @action_id,
      batch_id: "batch-1",
      action: "execute",
      message_id: "message-1",
      payload: ~s({"content":"hello"}),
      targets: [
        %PrismTarget{
          channel_id: "channel-1",
          webhook_id: "webhook-1",
          webhook_token: "secret"
        }
      ]
    }
  end

  defp kafka_message(payload, overrides \\ [])

  defp kafka_message(%PrismStreamPayload{} = payload, overrides),
    do: kafka_message(encode(payload), overrides)

  defp kafka_message(binary, overrides) when is_binary(binary) do
    headers =
      [
        {"ce_specversion", "1.0"},
        {"ce_source", "/polarizer"},
        {"ce_type", "fun.interchat.prism.job"},
        {"ce_datacontenttype", "application/protobuf"},
        {"content-type", "application/protobuf"},
        {"ce_id", "event-1"},
        {"ce_time", "2026-07-17T00:00:00Z"}
      ]
      |> Map.new()
      |> Map.merge(Map.new(overrides))
      |> Map.to_list()

    %Message{
      data: binary,
      acknowledger: {Broadway.NoopAcknowledger, nil, nil},
      metadata: %{
        topic: "prism.stream.jobs",
        key: "HUB:hub-1",
        headers: headers
      }
    }
  end

  defp encode(payload) do
    {iodata, _size} = PrismStreamPayload.encode!(payload)
    IO.iodata_to_binary(iodata)
  end

  defp restore_env(key, nil), do: Application.delete_env(:prism, key)
  defp restore_env(key, value), do: Application.put_env(:prism, key, value)
end
