defmodule Prism.StressHelpers do
  @moduledoc """
  Shared helpers for the rate-limit stress test suite.
  """

  alias Prism.RateLimit.{Backpressure, InvalidRequestTracker}

  @doc "Builds a minimal valid batch payload map compatible with FanoutBroadway."
  def build_payload(webhook_ids, action \\ "execute") when is_list(webhook_ids) do
    targets =
      Enum.map(webhook_ids, fn id ->
        %{"webhook_id" => id, "webhook_token" => "tok_#{id}"}
      end)

    %{
      "action" => action,
      "batch_id" => "batch_#{System.unique_integer([:positive])}",
      "payload" => %{"content" => "batch test message"},
      "targets" => targets
    }
  end

  @doc "Builds a single target map for a given webhook ID."
  def build_target(webhook_id, opts \\ []) do
    %{
      "webhook_id" => webhook_id,
      "webhook_token" => Keyword.get(opts, :webhook_token, "tok_#{webhook_id}"),
      "channel_id" => Keyword.get(opts, :channel_id, "chan_#{webhook_id}"),
      "guild_id" => Keyword.get(opts, :guild_id, "guild_#{webhook_id}"),
      "thread_id" => Keyword.get(opts, :thread_id)
    }
    |> Map.reject(fn {_k, v} -> is_nil(v) end)
  end

  @doc """
  Calls `DiscordWorker.process_target/7` with sensible defaults for stress testing.

  ## Options
    - `:content` — the Discord payload map (default: `%{"content" => "stress test message"}`)
    - `:batch_id` — batch identifier (default: `nil`, skips checkpointing)
    - `:action` — "execute", "edit", or "delete" (default: "execute")
    - `:polled_at` — timestamp (default: `nil`)
    - `:enqueued_at` — timestamp (default: `nil`)
  """
  def call_process_target(webhook_id, opts \\ []) do
    action = Keyword.get(opts, :action, "execute")
    target = build_target(webhook_id, opts)
    content = Keyword.get(opts, :content, %{"content" => "stress test message"})
    batch_id = Keyword.get(opts, :batch_id)
    polled_at = Keyword.get(opts, :polled_at)
    enqueued_at = Keyword.get(opts, :enqueued_at)

    Prism.DiscordWorker.process_target(
      action,
      target,
      content,
      batch_id,
      polled_at,
      enqueued_at,
      nil
    )
  end

  @doc "Spawns a task under `Prism.TaskSup` that sleeps for `sleep_ms` then returns `:done`."
  def spawn_sleep_task(sleep_ms \\ 100) do
    Task.Supervisor.start_child(Prism.TaskSup, fn ->
      Process.sleep(sleep_ms)
      :done
    end)
  end

  @doc "Returns the count of active children under `Prism.TaskSup`."
  def active_count do
    Supervisor.count_children(Prism.TaskSup).active
  end

  @doc """
  Polls `active_count/0` until it matches `expected` or `timeout_ms` elapses.
  Returns the final count.
  """
  def wait_for_active_count(expected, timeout_ms \\ 1000) when is_integer(expected) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    Stream.repeatedly(fn ->
      Process.sleep(10)
      active_count()
    end)
    |> Enum.find(fn count ->
      count == expected or System.monotonic_time(:millisecond) > deadline
    end)
    |> then(fn count ->
      if is_nil(count), do: active_count(), else: count
    end)
  end

  @doc """
  Injects a Cloudflare block at the backpressure level.

  `Backpressure.record_cloudflare_block/1` is a `GenServer.cast`, so this
  waits for Backpressure to handle it.
  """
  def inject_cloudflare_block(retry_after_ms \\ 120_000) do
    Backpressure.record_cloudflare_block(retry_after_ms)
    Prism.TestWait.flush(Backpressure)
  end

  @doc """
  Waits until Backpressure and InvalidRequestTracker have handled the casts a
  call in this process made. Bucket updates are synchronous and need no wait.
  """
  def flush_rate_limit_casts do
    Prism.TestWait.flush(Backpressure)
    Prism.TestWait.flush(InvalidRequestTracker)
  end

  @doc """
  Clears rate-limit ETS state between test scenarios.
  Does NOT clear persistent_term (backpressure) — call
  `clear_backpressure/0` separately if needed.
  """
  def reset_invalid_tracker do
    :ets.delete_all_objects(:prism_invalid_tracker)
  end

  @doc "Clears backpressure persistent_term state."
  def clear_backpressure do
    :persistent_term.put(:prism_backoff_until, 0)
    :persistent_term.put(:prism_blocked_at, 0)
    :ok
  end

  @doc "Full rate-limit state reset for clean test scenarios."
  def reset_rate_limit_state do
    reset_invalid_tracker()
    clear_backpressure()
    :ok
  end

  @doc "Convenience: returns `unhealthy?/0` value."
  def unhealthy?, do: Prism.RateLimit.unhealthy?()

  @doc "Convenience: returns `count_in_window/0` value."
  def invalid_count, do: InvalidRequestTracker.count_in_window()
end
