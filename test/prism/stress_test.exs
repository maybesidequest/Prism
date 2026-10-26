defmodule Prism.StressTest do
  use ExUnit.Case, async: false

  import Prism.StressHelpers

  alias Prism.RateLimit.{Bucket, Backpressure, InvalidRequestTracker}

  unless Application.compile_env(:prism, :show_test_logs, false) do
    @moduletag :capture_log
  end

  setup_all do
    MockDiscordServer.ensure_tables()
    {:ok, bandit} = MockDiscordServer.start_bandit(4002)

    on_exit(fn ->
      if :ets.whereis(:mock_discord_stubs) != :undefined do
        :ets.delete(:mock_discord_stubs)
      end

      if :ets.whereis(:mock_discord_requests) != :undefined do
        :ets.delete(:mock_discord_requests)
      end

      ThousandIsland.stop(bandit)
      Process.sleep(50)
    end)

    :ok
  end

  setup do
    MockDiscordServer.reset()

    MockDiscordServer.stub_default(%{
      status: 200,
      headers: [],
      body: Jason.encode!(%{id: "fallback"})
    })

    reset_rate_limit_state()
    :ok
  end

  # ---------------------------------------------------------------------------
  # Scenario A: Normal flow — 2xx response updates bucket
  # ---------------------------------------------------------------------------

  describe "Scenario A: 2xx response" do
    test "returns message ID and leaves system healthy", %{} do
      webhook = "sA_2xx_#{System.unique_integer([:positive])}"

      MockDiscordServer.stub_ok(webhook, "msg_A_001")

      assert {:ok, "msg_A_001"} = call_process_target(webhook)

      refute unhealthy?()
    end

    test "pre-flight acquire decrements remaining after a prior update", %{} do
      webhook = "sA_dec_#{System.unique_integer([:positive])}"

      MockDiscordServer.stub_ok(webhook, "msg_A_002")

      call_process_target(webhook)

      {:ok, remaining} = Bucket.acquire(webhook, "post")
      assert remaining == 48
    end
  end

  # ---------------------------------------------------------------------------
  # Scenario B: Cloudflare 429 triggers full backpressure
  # ---------------------------------------------------------------------------

  describe "Scenario B: Cloudflare 429" do
    test "triggers backpressure and defers subsequent calls", %{} do
      webhook = "sB_cf_#{System.unique_integer([:positive])}"

      MockDiscordServer.stub_cloudflare_429(webhook, 5.0)

      assert {:error, {:rate_limited, _}} = call_process_target(webhook)

      flush_rate_limit_casts()
      assert Backpressure.unhealthy?()

      MockDiscordServer.clear_requests()

      assert {:error, {:rate_limited, _}} = call_process_target(webhook)

      assert MockDiscordServer.request_count() == 0

      flush_rate_limit_casts()
      count = InvalidRequestTracker.count_in_window()
      assert count >= 1
    end
  end

  # ---------------------------------------------------------------------------
  # Scenario C: Discord global 429 updates global bucket
  # ---------------------------------------------------------------------------

  describe "Scenario C: Discord global 429" do
    test "global bucket is exhausted, blocking other webhooks", %{} do
      webhook = "sC_glob_#{System.unique_integer([:positive])}"
      other_webhook = "sC_other_#{System.unique_integer([:positive])}"

      MockDiscordServer.stub_discord_global_429(webhook, 1.5)

      assert {:error, {:rate_limited, _}} = call_process_target(webhook)

      assert {:blocked, _ttl} = Bucket.acquire(other_webhook, "post")
    end

    test "global bucket TTL is non-zero", %{} do
      webhook = "sC_ttl_#{System.unique_integer([:positive])}"

      MockDiscordServer.stub_discord_global_429(webhook, 3.0)

      call_process_target(webhook)

      {:blocked, ttl} =
        Bucket.acquire("any_webhook_#{System.unique_integer([:positive])}", "post")

      assert ttl > 0
      assert ttl <= 3500
    end
  end

  # ---------------------------------------------------------------------------
  # Scenario D: Per-webhook 429 with shared scope does NOT increment tracker
  # ---------------------------------------------------------------------------

  describe "Scenario D: shared scope" do
    test "shared scope does not count as an invalid request", %{} do
      webhook = "sD_shared_#{System.unique_integer([:positive])}"

      MockDiscordServer.stub_discord_per_webhook_429(webhook, scope: "shared")

      before_count = InvalidRequestTracker.count_in_window()

      assert {:error, {:rate_limited, _}} = call_process_target(webhook)

      flush_rate_limit_casts()

      assert InvalidRequestTracker.count_in_window() == before_count
    end
  end

  # ---------------------------------------------------------------------------
  # Scenario E: Per-webhook 429 with user scope DOES increment tracker
  # ---------------------------------------------------------------------------

  describe "Scenario E: user scope" do
    test "user scope increments invalid request tracker", %{} do
      webhook = "sE_user_#{System.unique_integer([:positive])}"

      MockDiscordServer.stub_discord_per_webhook_429(webhook, scope: "user")

      before_count = InvalidRequestTracker.count_in_window()

      assert {:error, {:rate_limited, _}} = call_process_target(webhook)

      flush_rate_limit_casts()

      assert InvalidRequestTracker.count_in_window() >= before_count + 1
    end
  end

  # ---------------------------------------------------------------------------
  # Scenario F: Supervisor.count_children stays accurate on task exit
  # ---------------------------------------------------------------------------

  describe "Scenario F: TaskSup active count" do
    test "active count decrements when tasks are killed", %{} do
      assert active_count() == 0

      {:ok, pid1} = spawn_sleep_task(5000)
      {:ok, pid2} = spawn_sleep_task(5000)
      {:ok, pid3} = spawn_sleep_task(5000)

      assert active_count() == 3

      Process.exit(pid1, :kill)
      Process.exit(pid2, :kill)

      assert wait_for_active_count(1) == 1

      Process.exit(pid3, :kill)

      assert wait_for_active_count(0) == 0
    end
  end

  # ---------------------------------------------------------------------------
  # Scenario G: Cap check does not permanently block after task crashes
  # ---------------------------------------------------------------------------

  describe "Scenario G: Cap check after task crash" do
    test "cap recovers after tasks exit", %{} do
      assert active_count() == 0

      max_async = Application.get_env(:prism, :max_async_batches, 10)

      pids =
        for _ <- 1..max_async do
          {:ok, pid} = spawn_sleep_task(5000)
          pid
        end

      assert active_count() == max_async

      Enum.each(pids, fn pid -> Process.exit(pid, :kill) end)

      assert wait_for_active_count(0) == 0

      {:ok, _pid} = spawn_sleep_task(50)

      assert wait_for_active_count(0) == 0
    end
  end

  # ---------------------------------------------------------------------------
  # Scenario H: Backpressure gate prevents HTTP calls and recovers
  # ---------------------------------------------------------------------------

  describe "Scenario H: Backpressure gate" do
    test "calls are deferred during backpressure, processed after clearing", %{} do
      webhook = "sH_bp_#{System.unique_integer([:positive])}"

      inject_cloudflare_block(120_000)
      assert unhealthy?()

      MockDiscordServer.stub_ok(webhook, "msg_H_001")

      assert {:error, {:rate_limited, _}} = call_process_target(webhook)

      assert MockDiscordServer.request_count() == 0

      clear_backpressure()
      refute unhealthy?()

      MockDiscordServer.stub_ok(webhook, "msg_H_002")
      assert {:ok, "msg_H_002"} = call_process_target(webhook)

      assert MockDiscordServer.request_count() >= 1
    end
  end

  # ---------------------------------------------------------------------------
  # Scenario I: Mixed rate limits across targets in a single batch
  # ---------------------------------------------------------------------------

  describe "Scenario I: Mixed rate limits" do
    test "each target receives its configured response independently", %{} do
      webhook_1 = "sI_ok_#{System.unique_integer([:positive])}"
      webhook_2 = "sI_per_#{System.unique_integer([:positive])}"
      webhook_3 = "sI_cf_#{System.unique_integer([:positive])}"

      MockDiscordServer.stub_ok(webhook_1, "msg_I_ok")
      MockDiscordServer.stub_discord_per_webhook_429(webhook_2, scope: "user")
      MockDiscordServer.stub_cloudflare_429(webhook_3, 5.0)

      result_1 = call_process_target(webhook_1)
      result_2 = call_process_target(webhook_2)
      result_3 = call_process_target(webhook_3)

      assert {:ok, "msg_I_ok"} = result_1

      assert {:error, {:rate_limited, _}} = result_2
      assert {:error, {:rate_limited, _}} = result_3

      flush_rate_limit_casts()
      assert unhealthy?()
      assert InvalidRequestTracker.count_in_window() >= 2
    end
  end
end
