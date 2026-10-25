defmodule Prism.DelayedSchedulerTest do
  use ExUnit.Case, async: false

  alias Prism.DelayedScheduler

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
    Redix.command!(redix_conn, ["ZADD", zset_key, to_string(now + 200), "{\"id\": 1}"])

    send(pid, :tick)
    Process.sleep(50)

    assert 1 = Redix.command!(redix_conn, ["ZCARD", zset_key])
    assert 0 = Redix.command!(redix_conn, ["XLEN", stream_key])

    Process.sleep(250)

    assert 0 = Redix.command!(redix_conn, ["ZCARD", zset_key])
    assert 1 = Redix.command!(redix_conn, ["XLEN", stream_key])
  end

  test "scheduler wakes up early when a PubSub event arrives", %{
    pid: pid,
    pubsub_channel: pubsub_channel,
    redix_conn: redix_conn,
    stream_key: stream_key,
    zset_key: zset_key
  } do
    now = :os.system_time(:millisecond)

    Redix.command!(redix_conn, ["ZADD", zset_key, to_string(now + 5000), "{\"id\": 2}"])
    send(pid, :tick)

    Process.sleep(50)

    Redix.command!(redix_conn, ["ZADD", zset_key, to_string(now + 100), "{\"id\": 3}"])
    Redix.command!(redix_conn, ["PUBLISH", pubsub_channel, "new_earliest:#{now + 100}"])

    Process.sleep(500)

    assert 1 = Redix.command!(redix_conn, ["ZCARD", zset_key])
    assert 1 = Redix.command!(redix_conn, ["XLEN", stream_key])
  end
end
