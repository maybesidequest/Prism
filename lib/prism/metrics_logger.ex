defmodule Prism.MetricsLogger do
  @moduledoc """
  Logs throughput, queue lengths and VM counters every ten seconds.
  """

  use GenServer
  require Logger

  alias Prism.Helpers

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    :timer.send_interval(10_000, :log_metrics)

    {:ok,
     %{
       last_batches: Prism.AsyncBatchCounter.get_processed_batches(),
       last_targets: Prism.AsyncBatchCounter.get_processed_targets(),
       last_time: :os.system_time(:millisecond)
     }}
  end

  @impl true
  def handle_info(:log_metrics, state) do
    batch_count = Prism.AsyncBatchCounter.count()
    process_count = length(:erlang.processes())
    run_queue = :erlang.statistics(:run_queue)
    port_count = length(:erlang.ports())

    is_kafka = Prism.EventBus.Config.transport_backend() == Prism.EventBus.Transport.Kafka

    jobs_len_str =
      if is_kafka do
        "N/A(Kafka)"
      else
        "#{stream_length(Prism.Config.stream_jobs())}"
      end

    dlq_len = stream_length(Prism.EventBus.Config.events_dlq_stream())
    bad_req_dlq_len = stream_length(Prism.Config.stream_bad_requests_dlq())

    :telemetry.execute([:prism, :event_bus, :dlq_depth], %{length: dlq_len}, %{})

    :telemetry.execute(
      [:prism, :discord_worker, :bad_requests_dlq_depth],
      %{length: bad_req_dlq_len},
      %{}
    )

    current_batches = Prism.AsyncBatchCounter.get_processed_batches()
    current_targets = Prism.AsyncBatchCounter.get_processed_targets()
    current_time = :os.system_time(:millisecond)

    delta_time_s = max((current_time - state.last_time) / 1000.0, 0.001)
    delta_batches = current_batches - state.last_batches
    delta_targets = current_targets - state.last_targets

    avg_targets_per_sec = Float.round(delta_targets / delta_time_s, 1)
    avg_batches_per_sec = Float.round(delta_batches / delta_time_s, 1)

    cwnd_info =
      if Prism.Config.congestion_control_enabled?() do
        cwnd = Prism.CongestionWindow.window_size()
        cubic = Prism.CongestionWindow.cubic_window()
        safety = Prism.CongestionWindow.safety_window()
        inflight = Prism.CongestionWindow.in_flight()
        phase = Prism.CongestionWindow.phase()
        w_max = Prism.CongestionWindow.w_max()
        count_4xx = Prism.CongestionWindow.budget_count()
        budget = Prism.Config.cwnd_4xx_budget()
        util = Float.round(Prism.CongestionWindow.budget_utilization() * 100, 1)
        rtt = Float.round(Prism.CongestionWindow.estimated_rtt(), 1)
        binding = if safety < cubic, do: "safety", else: "cubic"

        " | CWND: #{cwnd} [#{binding}] (Cubic: #{trunc(cubic)} Safety: #{trunc(safety)} " <>
          "W_max: #{trunc(w_max)} Phase: #{phase}) " <>
          "In-flight: #{inflight} | 4xx: #{count_4xx}/#{budget} (#{util}%) | RTT: #{rtt}ms"
      else
        ""
      end

    Logger.info(
      "[Metrics] Active Batches: #{batch_count} | Erlang Procs: #{process_count} | Run Queue: #{run_queue} | Ports/Sockets: #{port_count} | Jobs Stream: #{jobs_len_str} | DLQ: #{dlq_len} | BadReq DLQ: #{bad_req_dlq_len} | Throughput: #{avg_targets_per_sec} msg/s (#{avg_batches_per_sec} batch/s)#{cwnd_info}"
    )

    new_state = %{
      last_batches: current_batches,
      last_targets: current_targets,
      last_time: current_time
    }

    {:noreply, new_state}
  end

  defp stream_length(stream_key) do
    case Helpers.redix_command(["XLEN", stream_key]) do
      {:ok, len} -> len
      _ -> -1
    end
  end
end
