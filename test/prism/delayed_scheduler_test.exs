defmodule Prism.DelayedSchedulerTest do
  use ExUnit.Case, async: false

  alias Prism.DelayedScheduler

  import Prism.TestWait

  setup do
    zset_key = Prism.Config.delayed_zset_key()
    stream_key = Prism.Config.stream_retries()
    pubsub_channel = Prism.Config.pubsub_channel()

    {:ok, redix_conn} = Redix.start_link(Prism.TestRedis.uri(), sync_connect: true)
    Redix.command!(redix_conn, ["DEL", zset_key, stream_key])

    # Start a test-specific scheduler (the global one was stopped in test_helper)
    {:ok, pid} = GenServer.start_link(DelayedScheduler, [])

    %{
      pid: pid,
      pubsub_channel: pubsub_channel,
      redix_conn: redix_conn,
      stream_key: stream_key,
      zset_key: zset_key
    }
  end

  test "scheduler sleeps until the earliest item is due", %{
    pid: pid,
    redix_conn: redix_conn,
    stream_key: stream_key,
    zset_key: zset_key
  } do
    now = :os.system_time(:millisecond)
    item = ~s({"id": 1})
    # Due in 1 s: wide enough that a slow Redis round trip under a loaded test
    # run still checks it before it is due.
    Redix.command!(redix_conn, ["ZADD", zset_key, to_string(now + 1_000), item])

    send(pid, :tick)
    flush(pid)

    assert delayed?(redix_conn, zset_key, item)
    refute item in stream_payloads(redix_conn, stream_key)

    eventually(
      fn ->
        refute delayed?(redix_conn, zset_key, item)
        assert item in stream_payloads(redix_conn, stream_key)
      end,
      3_000
    )
  end

  test "scheduler wakes up early when a PubSub event arrives", %{
    pid: pid,
    pubsub_channel: pubsub_channel,
    redix_conn: redix_conn,
    stream_key: stream_key,
    zset_key: zset_key
  } do
    now = :os.system_time(:millisecond)

    late = ~s({"id": 2})
    early = ~s({"id": 3})
    Redix.command!(redix_conn, ["ZADD", zset_key, to_string(now + 5000), late])
    send(pid, :tick)
    flush(pid)

    # The scheduler subscribes in init; the wake-up is lost if Redis has not
    # registered that subscription before the PUBLISH.
    flush(Prism.PubSub)

    eventually(fn ->
      assert [_, subscribers] = Redix.command!(redix_conn, ["PUBSUB", "NUMSUB", pubsub_channel])
      assert subscribers >= 1
    end)

    Redix.command!(redix_conn, ["ZADD", zset_key, to_string(now + 100), early])
    Redix.command!(redix_conn, ["PUBLISH", pubsub_channel, "new_earliest:#{now + 100}"])

    # The 5 s item stays put, so the move can only come from the early wake-up.
    eventually(fn ->
      assert delayed?(redix_conn, zset_key, late)
      refute delayed?(redix_conn, zset_key, early)
      assert early in stream_payloads(redix_conn, stream_key)
    end)
  end

  # Retry tasks left by other test files also write to these keys, so the tests
  # look for their own items instead of counting entries.
  defp delayed?(conn, zset_key, item),
    do: Redix.command!(conn, ["ZSCORE", zset_key, item]) != nil

  defp stream_payloads(conn, stream_key) do
    for [_id, fields] <- Redix.command!(conn, ["XRANGE", stream_key, "-", "+"]),
        {"payload", payload} <- Enum.chunk_every(fields, 2) |> Enum.map(&List.to_tuple/1),
        do: payload
  end
end
