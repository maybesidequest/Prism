defmodule Prism.RateLimit.InvalidRequestTrackerTest do
  use ExUnit.Case, async: false

  alias Prism.RateLimit.InvalidRequestTracker

  import Prism.TestWait, only: [flush: 1]

  @table :prism_invalid_tracker

  setup do
    # Let casts from earlier tests land before clearing the table.
    flush(InvalidRequestTracker)
    :ets.delete_all_objects(@table)
    :ok
  end

  describe "count_in_window/0" do
    test "starts at 0 after cleanup" do
      assert InvalidRequestTracker.count_in_window() == 0
    end
  end

  describe "record_invalid/0 via GenServer cast" do
    test "single record_invalid is reflected in count" do
      InvalidRequestTracker.record_invalid()
      flush(InvalidRequestTracker)
      assert InvalidRequestTracker.count_in_window() >= 1
    end
  end

  describe "approaching_limit?/0" do
    test "returns false when count is well below threshold" do
      assert InvalidRequestTracker.approaching_limit?() == false
    end
  end

  describe "ETS-based counting (bypass GenServer for timing)" do
    test "multiple recent entries are all counted in window" do
      now = System.monotonic_time(:millisecond)
      for i <- 1..5, do: :ets.insert(@table, {now + i})
      assert InvalidRequestTracker.count_in_window() == 5
    end

    test "old entries are pruned leaving only recent ones" do
      old_time = System.monotonic_time(:millisecond) - 700_000
      for i <- 1..5, do: :ets.insert(@table, {old_time + i})
      :ets.insert(@table, {System.monotonic_time(:millisecond)})

      pid = Process.whereis(InvalidRequestTracker)
      assert pid != nil
      send(pid, :prune)
      flush(pid)

      assert InvalidRequestTracker.count_in_window() == 1
    end

    test "approaching_limit returns true at threshold" do
      now = System.monotonic_time(:millisecond)
      for i <- 1..9500, do: :ets.insert(@table, {now + i})
      assert InvalidRequestTracker.approaching_limit?() == true
    end
  end
end
