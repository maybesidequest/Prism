defmodule Prism.RateLimit.BackpressureTest do
  use ExUnit.Case, async: false

  alias Prism.RateLimit.Backpressure

  import Prism.TestWait, only: [flush: 1]

  @moduletag :capture_log

  setup do
    :persistent_term.put(:prism_backoff_until, 0)
    :persistent_term.put(:prism_blocked_at, 0)
    flush(Backpressure)
  end

  describe "backoff_ms/0" do
    test "returns 0 when there is no active block" do
      assert Backpressure.backoff_ms() == 0
    end

    test "returns remaining backoff when a block is active" do
      now = System.monotonic_time(:millisecond)
      :persistent_term.put(:prism_backoff_until, now + 5000)
      :persistent_term.put(:prism_blocked_at, now)

      ms = Backpressure.backoff_ms()
      # Should be approximately 5000ms remaining
      assert ms > 0
      assert ms <= 5100
    end

    test "returns 0 when backoff has fully expired" do
      past = System.monotonic_time(:millisecond) - 5000
      :persistent_term.put(:prism_backoff_until, past)
      :persistent_term.put(:prism_blocked_at, past - 5000)

      assert Backpressure.backoff_ms() == 0
    end
  end

  describe "unhealthy?/0" do
    test "returns false with no block" do
      refute Backpressure.unhealthy?()
    end

    test "returns true during active block" do
      now = System.monotonic_time(:millisecond)
      :persistent_term.put(:prism_backoff_until, now + 5000)
      :persistent_term.put(:prism_blocked_at, now)

      assert Backpressure.unhealthy?()
    end
  end

  describe "record_cloudflare_block/1" do
    test "sets backoff via GenServer cast" do
      Backpressure.record_cloudflare_block(5000)
      flush(Backpressure)
      assert Backpressure.unhealthy?()
    end

    test "caps backoff at max (600s)" do
      Backpressure.record_cloudflare_block(1_000_000)
      flush(Backpressure)
      ms = Backpressure.backoff_ms()
      assert ms > 0
      # Capped at 600_000 ± tolerance
      assert ms <= 602_000
    end

    test "ignores non-positive retry_after values" do
      Backpressure.record_cloudflare_block(0)
      Backpressure.record_cloudflare_block(-1)
      flush(Backpressure)
      refute Backpressure.unhealthy?()
    end
  end

  describe "record_success/0" do
    test "clears backpressure after it has expired" do
      past = System.monotonic_time(:millisecond) - 5000
      :persistent_term.put(:prism_blocked_at, past - 5000)
      :persistent_term.put(:prism_backoff_until, past)

      Backpressure.record_success()
      flush(Backpressure)
      refute Backpressure.unhealthy?()
    end
  end
end
