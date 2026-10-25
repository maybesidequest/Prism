defmodule Prism.RateLimit.BucketTest do
  use ExUnit.Case, async: false

  alias Prism.RateLimit.Bucket

  setup do
    # Ensure the Redix pool is available for rate_limit_bucket calls.
    for i <- 0..4 do
      case Redix.start_link(Prism.TestRedis.uri(), name: :"my_redix_#{i}") do
        {:ok, _} -> :ok
        {:error, {:already_started, _}} -> :ok
      end
    end

    # Build a standalone connection for direct Redis inspection.
    {:ok, redix} = Redix.start_link(Prism.TestRedis.uri())

    # Clean up rate-limit bucket keys from prior runs.
    Prism.TestRedis.delete_matching(redix, "prism:rl:*")

    %{redix: redix}
  end

  describe "bucket_key/2" do
    test "produces keys with correct prefix, webhook_id, and method" do
      key = Bucket.bucket_key("abc123", "post")
      assert String.match?(key, ~r/^prism:rl:[a-f0-9]+:abc123:post$/)

      key2 = Bucket.bucket_key("abc123", "patch")
      assert String.match?(key2, ~r/^prism:rl:[a-f0-9]+:abc123:patch$/)

      key3 = Bucket.bucket_key("abc123", "delete")
      assert String.match?(key3, ~r/^prism:rl:[a-f0-9]+:abc123:delete$/)
    end
  end

  describe "acquire/2" do
    test "allows the first request (no bucket state yet)" do
      assert {:ok, -1} = Bucket.acquire("acq_first", "post")
    end

    test "decrements remaining across sequential calls", %{redix: redix} do
      webhook = "acq_decr"
      key = Bucket.bucket_key(webhook, "post")

      # Simulate a prior update setting limit=3, remaining=3, window far ahead.
      now_ms = System.monotonic_time(:millisecond)

      Redix.command!(redix, [
        "HSET",
        key,
        "limit",
        "3",
        "remaining",
        "3",
        "reset_at",
        to_string(now_ms + 60000)
      ])

      assert {:ok, 2} = Bucket.acquire(webhook, "post")
      assert {:ok, 1} = Bucket.acquire(webhook, "post")
      assert {:ok, 0} = Bucket.acquire(webhook, "post")

      # Fourth call: bucket exhausted.
      {:blocked, ttl} = Bucket.acquire(webhook, "post")
      assert ttl > 0
    end

    test "blocks when remaining=0 and window is still active", %{redix: redix} do
      webhook = "acq_block"
      key = Bucket.bucket_key(webhook, "post")
      now_ms = System.monotonic_time(:millisecond)

      Redix.command!(redix, [
        "HSET",
        key,
        "limit",
        "5",
        "remaining",
        "0",
        "reset_at",
        to_string(now_ms + 5000)
      ])

      assert {:blocked, ttl} = Bucket.acquire(webhook, "post")
      assert ttl > 0
      assert ttl <= 5000 + 100
    end

    test "resets and allows when the rate-limit window has expired", %{redix: redix} do
      webhook = "acq_expire"
      key = Bucket.bucket_key(webhook, "post")
      now_ms = System.monotonic_time(:millisecond)

      # Window expired 1 second ago.
      Redix.command!(redix, [
        "HSET",
        key,
        "limit",
        "5",
        "remaining",
        "0",
        "reset_at",
        to_string(now_ms - 1000)
      ])

      assert {:ok, -1} = Bucket.acquire(webhook, "post")

      # Key should be deleted after expired-state acquire.
      refute Redix.command!(redix, ["EXISTS", key]) == 1
    end

    test "treats nil remaining as exhausted (defensive)", %{redix: redix} do
      webhook = "acq_nil"
      key = Bucket.bucket_key(webhook, "post")
      now_ms = System.monotonic_time(:millisecond)

      # Missing "remaining" field — should be treated as 0.
      Redix.command!(redix, ["HSET", key, "limit", "5", "reset_at", to_string(now_ms + 10000)])

      assert {:blocked, _} = Bucket.acquire(webhook, "post")
    end

    test "blocks when global rate limit is active", %{redix: redix} do
      webhook = "acq_global"
      global_key = Bucket.global_key()
      now_ms = System.monotonic_time(:millisecond)

      Redix.command!(redix, [
        "HSET",
        global_key,
        "limit",
        "50",
        "remaining",
        "0",
        "reset_at",
        to_string(now_ms + 4000)
      ])

      assert {:blocked, ttl} = Bucket.acquire(webhook, "post")
      assert ttl > 0
      assert ttl <= 4000
    end

    test "clears expired global rate limit and allows request", %{redix: redix} do
      webhook = "acq_global_expired"
      global_key = Bucket.global_key()
      now_ms = System.monotonic_time(:millisecond)

      Redix.command!(redix, [
        "HSET",
        global_key,
        "limit",
        "50",
        "remaining",
        "0",
        "reset_at",
        to_string(now_ms - 1000)
      ])

      assert {:ok, -1} = Bucket.acquire(webhook, "post")
      refute Redix.command!(redix, ["EXISTS", global_key]) == 1
    end
  end

  describe "update/5" do
    test "sets all hash fields", %{redix: redix} do
      webhook = "upd_fields"
      key = Bucket.bucket_key(webhook, "post")
      reset_at = System.monotonic_time(:millisecond) + 2000

      Bucket.update(webhook, "post", 5, 3, reset_at)

      assert Redix.command!(redix, ["HGET", key, "limit"]) == "5"
      assert Redix.command!(redix, ["HGET", key, "remaining"]) == "3"
      assert Redix.command!(redix, ["HGET", key, "reset_at"]) == to_string(reset_at)
      assert Redix.command!(redix, ["HGET", key, "bucket"]) == ""
    end

    test "sets a TTL on the hash key", %{redix: redix} do
      webhook = "upd_ttl"
      key = Bucket.bucket_key(webhook, "post")
      reset_at = System.monotonic_time(:millisecond) + 2000

      Bucket.update(webhook, "post", 5, 1, reset_at)

      ttl = Redix.command!(redix, ["TTL", key])
      assert is_integer(ttl) and ttl > 0
    end
  end

  describe "update_global/3" do
    test "updates the global rate-limit key", %{redix: redix} do
      global_key = Bucket.global_key()
      reset_at = System.monotonic_time(:millisecond) + 3000

      Bucket.update_global(50, 0, reset_at)

      assert Redix.command!(redix, ["HGET", global_key, "limit"]) == "50"
      assert Redix.command!(redix, ["HGET", global_key, "remaining"]) == "0"
      assert Redix.command!(redix, ["HGET", global_key, "reset_at"]) == to_string(reset_at)
    end
  end

  describe "concurrent acquires" do
    test "serialise through Redis: last caller is blocked", %{redix: redix} do
      webhook = "acq_conc"
      key = Bucket.bucket_key(webhook, "post")
      now_ms = System.monotonic_time(:millisecond)

      Redix.command!(redix, [
        "HSET",
        key,
        "limit",
        "3",
        "remaining",
        "3",
        "reset_at",
        to_string(now_ms + 60000)
      ])

      results =
        1..5
        |> Enum.map(fn _ ->
          Task.async(fn ->
            Bucket.acquire(webhook, "post")
          end)
        end)
        |> Enum.map(&Task.await/1)

      ok_count = Enum.count(results, &match?({:ok, _}, &1))
      blocked_count = Enum.count(results, &match?({:blocked, _}, &1))

      assert ok_count == 3
      assert blocked_count == 2
    end
  end
end
