defmodule Prism.CongestionWindow do
  @moduledoc """
  Dual-controller congestion control for Prism webhook delivery.

  Controller 1: Cubic throughput (RFC 9438). Discovers aggregate throughput limits
  empirically via global and Cloudflare 429s. Includes TCP-friendly (Reno) fallback
  and fast convergence.

  Controller 2: 4xx safety budget. Primary protection against Cloudflare IP bans,
  particularly the ~200 404s/minute restriction for dead webhooks. Progressively
  throttles as 4xx errors accumulate.

  effective_cwnd = min(cubic_cwnd, safety_cwnd)
  """
  use GenServer
  require Logger

  @table_name :prism_4xx_budget
  @cwnd_key :prism_cwnd_effective
  @atomics_key :prism_cwnd_atomics
  @in_flight_idx 1

  # EWMA alpha for RTT estimation (RFC 6298)
  @ewma_alpha 0.125

  # ── Public API (Lock-free Hot Path) ──────────────────────────────────────────

  @doc """
  Acquires a concurrency token from the congestion window.
  Returns `:ok` or `{:backoff, wait_ms}`.
  """
  @spec acquire() :: :ok | {:backoff, non_neg_integer()}
  def acquire do
    atomics_ref =
      Prism.Config.congestion_control_enabled?() and :persistent_term.get(@atomics_key, nil)

    if atomics_ref, do: take_token(atomics_ref), else: :ok
  end

  defp take_token(atomics_ref) do
    new_val = :atomics.add_get(atomics_ref, @in_flight_idx, 1)
    cwnd = :persistent_term.get(@cwnd_key, Prism.Config.cwnd_min())

    if new_val <= cwnd do
      :ok
    else
      :atomics.sub(atomics_ref, @in_flight_idx, 1)
      {:backoff, estimate_backoff_ms(new_val, cwnd)}
    end
  end

  @doc """
  Releases a concurrency token back to the congestion window.
  """
  @spec release() :: :ok
  def release do
    if Prism.Config.congestion_control_enabled?() do
      atomics_ref = :persistent_term.get(@atomics_key, nil)

      if atomics_ref do
        :atomics.sub(atomics_ref, @in_flight_idx, 1)
      end
    end

    :ok
  end

  defp estimate_backoff_ms(in_flight, cwnd) do
    overflow = max(in_flight - cwnd, 1)
    rtt = :persistent_term.get(:prism_cwnd_estimated_rtt, 100.0)
    min(trunc(overflow * rtt / max(cwnd, 1)), 5_000)
  end

  # ── Public API (Signals - Async) ─────────────────────────────────────────────

  @doc "Records a global 429 to trigger a Cubic decrease"
  def record_global_429 do
    GenServer.cast(__MODULE__, {:decrease, Prism.Config.cwnd_beta_global()})
  end

  @doc "Records a Cloudflare 429 to trigger a severe Cubic decrease"
  def record_cloudflare_429 do
    GenServer.cast(__MODULE__, {:decrease, Prism.Config.cwnd_beta_cloudflare()})
  end

  @doc "Records a successful response to update RTT EWMA and W_est"
  def record_success(rtt_ms) when is_integer(rtt_ms) do
    GenServer.cast(__MODULE__, {:record_success, rtt_ms})
  end

  @doc "Records a 4xx error for the safety budget"
  def record_4xx do
    now = System.monotonic_time(:millisecond)
    unique = System.unique_integer()
    :ets.insert(@table_name, {{now, unique}, true})
    :ok
  end

  # ── Public API (Observability - Zero Cost Reads) ────────────────────────────

  def window_size, do: :persistent_term.get(@cwnd_key, Prism.Config.cwnd_initial())
  def cubic_window, do: :persistent_term.get(:prism_cwnd_cubic, float_cwnd_initial())
  def safety_window, do: :persistent_term.get(:prism_cwnd_safety, float_cwnd_max())
  def phase, do: :persistent_term.get(:prism_cwnd_phase, :slow_start)
  def w_max, do: :persistent_term.get(:prism_cwnd_w_max, float_cwnd_initial())
  def budget_count, do: :persistent_term.get(:prism_cwnd_4xx_count, 0)
  def estimated_rtt, do: :persistent_term.get(:prism_cwnd_estimated_rtt, 100.0)

  def in_flight do
    atomics_ref = :persistent_term.get(@atomics_key, nil)
    if atomics_ref, do: :atomics.get(atomics_ref, @in_flight_idx), else: 0
  end

  def budget_utilization do
    budget = max(Prism.Config.cwnd_4xx_budget(), 1)
    budget_count() / budget
  end

  defp float_cwnd_initial, do: Prism.Config.cwnd_initial() * 1.0
  defp float_cwnd_max, do: Prism.Config.cwnd_max() * 1.0

  # ── GenServer Lifecycle ──────────────────────────────────────────────────────

  def start_link(_) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_) do
    if Prism.Config.congestion_control_enabled?(), do: start_window(), else: :ignore
  end

  defp start_window do
    # Initialize ETS for 4xx tracking
    :ets.new(@table_name, [:ordered_set, :public, :named_table, read_concurrency: true])

    # Initialize Atomics for hot path
    atomics_ref = :atomics.new(1, signed: false)
    :persistent_term.put(@atomics_key, atomics_ref)
    :persistent_term.put(@cwnd_key, Prism.Config.cwnd_initial())

    now = System.monotonic_time(:millisecond)

    cubic_state = %{
      cwnd: float_cwnd_initial(),
      w_max: float_cwnd_initial(),
      ssthresh: Prism.Config.ssthresh_initial() * 1.0,
      cwnd_epoch: float_cwnd_initial(),
      epoch_start: now,
      k: 0.0,
      beta_last: Prism.Config.cwnd_beta_global(),
      w_est: float_cwnd_initial(),
      alpha_cubic:
        3.0 * (1.0 - Prism.Config.cwnd_beta_global()) / (1.0 + Prism.Config.cwnd_beta_global()),
      last_decrease_at: now - Prism.Config.cwnd_decrease_cooldown_ms(),
      estimated_rtt: 100.0,
      phase: :slow_start
    }

    state = %{
      cubic: cubic_state,
      probe_timer: nil,
      successes_since_probe: 0
    }

    schedule_probe()
    schedule_prune()

    Logger.info("[CongestionWindow] Started with CWND=#{cubic_state.cwnd}")
    {:ok, state}
  end

  # ── Signal Handling (Casts) ──────────────────────────────────────────────────

  @impl true
  def handle_cast({:record_success, rtt_ms}, state) do
    cubic = state.cubic
    new_rtt = (1 - @ewma_alpha) * cubic.estimated_rtt + @ewma_alpha * rtt_ms
    new_cubic = %{cubic | estimated_rtt: new_rtt}

    :persistent_term.put(:prism_cwnd_estimated_rtt, new_rtt)

    {:noreply,
     %{state | cubic: new_cubic, successes_since_probe: state.successes_since_probe + 1}}
  end

  @impl true
  def handle_cast({:decrease, beta}, state) do
    now = System.monotonic_time(:millisecond)
    cubic = state.cubic

    if now - cubic.last_decrease_at < Prism.Config.cwnd_decrease_cooldown_ms() do
      {:noreply, state}
    else
      current_cwnd = cubic.cwnd

      # Fast Convergence (RFC 9438 §4.7)
      new_w_max =
        if current_cwnd < cubic.w_max do
          current_cwnd * (1.0 + beta) / 2.0
        else
          current_cwnd
        end

      new_cwnd = max(current_cwnd * beta, Prism.Config.cwnd_min() * 1.0)
      new_cwnd_epoch = new_cwnd

      c = Prism.Config.cubic_c()
      # RFC 9438 §4.2: K = cbrt((W_max - cwnd_epoch) / C)
      new_k =
        if new_w_max > new_cwnd_epoch do
          :math.pow((new_w_max - new_cwnd_epoch) / c, 1.0 / 3.0)
        else
          0.0
        end

      # RFC 9438 §4.3: alpha_cubic = 3 * (1 - beta) / (1 + beta)
      alpha_cubic = 3.0 * (1.0 - beta) / (1.0 + beta)

      new_cubic = %{
        cubic
        | cwnd: new_cwnd,
          w_max: new_w_max,
          ssthresh: new_cwnd,
          cwnd_epoch: new_cwnd_epoch,
          epoch_start: now,
          k: new_k,
          beta_last: beta,
          w_est: new_cwnd,
          alpha_cubic: alpha_cubic,
          last_decrease_at: now,
          phase: :concave
      }

      # Immediately recompute and apply
      safety_cwnd = compute_safety_cwnd(count_4xx_in_window())
      effective = max(trunc(min(new_cwnd, safety_cwnd)), Prism.Config.cwnd_min())

      :persistent_term.put(@cwnd_key, effective)
      :persistent_term.put(:prism_cwnd_cubic, new_cwnd)
      :persistent_term.put(:prism_cwnd_safety, safety_cwnd)
      :persistent_term.put(:prism_cwnd_phase, :concave)
      :persistent_term.put(:prism_cwnd_w_max, new_w_max)

      Logger.info(
        "[CongestionWindow] Decrease (beta=#{beta}). New cwnd=#{trunc(new_cwnd)}, W_max=#{trunc(new_w_max)}"
      )

      {:noreply, %{state | cubic: new_cubic, successes_since_probe: 0}}
    end
  end

  # ── Probe Loop (Info) ────────────────────────────────────────────────────────

  @impl true
  def handle_info(:probe, state) do
    now = System.monotonic_time(:millisecond)
    c_config = Prism.Config.cubic_c()
    min_config = Prism.Config.cwnd_min() * 1.0
    max_config = Prism.Config.cwnd_max() * 1.0

    # 1. Update W_est based on successes
    w_est_updated =
      state.cubic.w_est +
        state.cubic.alpha_cubic * (state.successes_since_probe / max(state.cubic.w_est, 1.0))

    cubic = %{state.cubic | w_est: w_est_updated}

    # 2. Compute Cubic target
    {cubic_cwnd, new_cubic} =
      case cubic.phase do
        :slow_start ->
          new_cwnd = min(cubic.cwnd * 2.0, cubic.ssthresh)
          phase = if new_cwnd >= cubic.ssthresh, do: :concave, else: :slow_start
          {new_cwnd, %{cubic | cwnd: new_cwnd, phase: phase, w_est: new_cwnd}}

        _ ->
          elapsed_s = (now - cubic.epoch_start) / 1_000.0
          cubic_target = c_config * :math.pow(elapsed_s - cubic.k, 3) + cubic.w_max

          target = max(cubic_target, w_est_updated)
          target = max(min_config, min(target, max_config))

          new_phase =
            cond do
              w_est_updated > cubic_target -> :tcp_friendly
              elapsed_s < cubic.k -> :concave
              true -> :convex
            end

          {target, %{cubic | cwnd: target, phase: new_phase}}
      end

    # 3. Compute Safety Budget
    count_4xx = count_4xx_in_window()
    safety_cwnd = compute_safety_cwnd(count_4xx)

    # 4. Apply min() and publish
    effective = max(trunc(min(cubic_cwnd, safety_cwnd)), Prism.Config.cwnd_min())

    :persistent_term.put(@cwnd_key, effective)
    :persistent_term.put(:prism_cwnd_cubic, cubic_cwnd)
    :persistent_term.put(:prism_cwnd_safety, safety_cwnd)
    :persistent_term.put(:prism_cwnd_phase, new_cubic.phase)
    :persistent_term.put(:prism_cwnd_w_max, new_cubic.w_max)
    :persistent_term.put(:prism_cwnd_4xx_count, count_4xx)

    schedule_probe()
    {:noreply, %{state | cubic: new_cubic, successes_since_probe: 0}}
  end

  @impl true
  def handle_info(:prune_4xx, state) do
    cutoff = System.monotonic_time(:millisecond) - Prism.Config.cwnd_4xx_window_ms()
    :ets.select_delete(@table_name, [{{{:"$1", :_}, :_}, [{:<, :"$1", cutoff}], [true]}])
    schedule_prune()
    {:noreply, state}
  end

  # ── Internals ────────────────────────────────────────────────────────────────

  defp schedule_probe do
    Process.send_after(self(), :probe, Prism.Config.cwnd_probe_interval_ms())
  end

  defp schedule_prune do
    Process.send_after(self(), :prune_4xx, Prism.Config.cwnd_4xx_prune_interval_ms())
  end

  defp count_4xx_in_window do
    cutoff = System.monotonic_time(:millisecond) - Prism.Config.cwnd_4xx_window_ms()
    :ets.select_count(@table_name, [{{{:"$1", :_}, :_}, [{:>=, :"$1", cutoff}], [true]}])
  end

  defp compute_safety_cwnd(count_4xx) do
    budget = Prism.Config.cwnd_4xx_budget()
    safe_pct = Prism.Config.cwnd_4xx_safe_pct()
    crit_pct = Prism.Config.cwnd_4xx_critical_pct()
    cwnd_max = Prism.Config.cwnd_max() * 1.0
    cwnd_min = Prism.Config.cwnd_min() * 1.0

    utilization = count_4xx / max(budget, 1)

    cond do
      utilization < safe_pct ->
        cwnd_max

      utilization >= crit_pct ->
        cwnd_min

      true ->
        t = (utilization - safe_pct) / (crit_pct - safe_pct)
        cwnd_max * (1.0 - t) + cwnd_min * t
    end
  end
end
