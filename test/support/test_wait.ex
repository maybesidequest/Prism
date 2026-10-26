defmodule Prism.TestWait do
  @moduledoc """
  Waits for asynchronous effects in tests without guessing a sleep.
  """

  @doc """
  Runs `assertion` until it stops raising `ExUnit.AssertionError` or
  `timeout_ms` passes, then returns its result or re-raises the last failure.
  """
  def eventually(assertion, timeout_ms \\ 1_000) when is_function(assertion, 0) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    retry(assertion, deadline)
  end

  defp retry(assertion, deadline) do
    assertion.()
  rescue
    error in ExUnit.AssertionError ->
      if System.monotonic_time(:millisecond) >= deadline do
        reraise error, __STACKTRACE__
      else
        Process.sleep(10)
        retry(assertion, deadline)
      end
  end

  @doc """
  Returns once `server` has handled every message sent to it before this call.
  A cast from the calling process is therefore applied when this returns.
  """
  def flush(server) do
    _ = :sys.get_state(server)
    :ok
  end
end
