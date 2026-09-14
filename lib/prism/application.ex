defmodule Prism.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    require Logger

    unless Node.alive?() do
      case Node.start(:prism, :shortnames) do
        {:ok, _pid} ->
          :ok

        {:error, reason} ->
          Logger.error("""
          [Prism] Failed to start Erlang distribution! 
          Reason: #{inspect(reason)}

          This usually happens because 'epmd' (Erlang Port Mapper Daemon) is not running.
          Try running 'epmd -daemon' in your terminal before starting the application,
          or start the app with 'iex --sname prism -S mix'.
          """)
      end
    end

    finch_pool_count = Prism.Config.finch_pool_count()
    finch_protocols = Prism.Config.finch_protocols()
    discord_base_url_val = Prism.Config.discord_base_url()
    pool_size = Prism.Config.redix_pool_size()

    Logger.info(
      "[Prism] Starting up! Initializing Redix pool (#{pool_size} conns) and Finch pool (#{finch_pool_count} conns against #{discord_base_url_val})."
    )

    worker_id =
      System.get_env("PRISM_WORKER_ID") ||
        :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)

    :persistent_term.put(:prism_worker_id, worker_id)

    redis_opts = Prism.Config.redis_opts()

    redix_children =
      for i <- 0..(pool_size - 1) do
        Supervisor.child_spec({Redix, Keyword.put(redis_opts, :name, :"my_redix_#{i}")},
          id: :"my_redix_#{i}"
        )
      end

    cluster_name = Prism.Config.cluster_topology()

    topologies = [
      {cluster_name, [strategy: Cluster.Strategy.LocalEpmd]}
    ]

    transport_backend = Prism.EventBus.Config.transport_backend()

    kafka_client_child =
      if transport_backend == Prism.EventBus.Transport.Kafka do
        [
          %{
            id: :kafka_client,
            start:
              {:brod, :start_link_client,
               [
                 Prism.EventBus.Config.kafka_brokers(),
                 :kafka_client,
                 Prism.EventBus.Config.kafka_brod_client_config()
               ]}
          }
        ]
      else
        []
      end

    # Initialise the lock-free async batch counter before any Broadway starts
    Prism.AsyncBatchCounter.init()

    children =
      if Node.alive?() do
        [{Cluster.Supervisor, [topologies, [name: Prism.ClusterSupervisor]]}]
      else
        Logger.warning(
          "[Prism] Node is not alive (Erlang distribution offline). Skipping Cluster.Supervisor to avoid {:error, :address} crashes."
        )

        []
      end ++
        kafka_client_child ++
        [
          {Bandit,
           plug: Prism.Health,
           scheme: :http,
           port: Prism.Config.health_port(),
           ip: Prism.Config.health_host()},
          {Finch,
           name: DiscordFinch,
           pools: %{
             discord_base_url_val => [
               protocols: finch_protocols,
               count: finch_pool_count,
               conn_max_idle_time: Prism.Config.finch_idle_timeout_ms(),
               conn_opts: [
                 transport_opts: [keepalive: Prism.Config.finch_keepalive_ms()]
               ]
             ]
           }},
          {Task.Supervisor, name: Prism.TaskSup},
          # :pg ships as a kernel module with no application of its own, so the
          # default scope has to be started here. Starting it from inside
          # MetricsAPI.init/1 — as it used to be — left it unsupervised and linked
          # to that GenServer, and re-ran on every restart of it.
          %{id: :pg, start: {:pg, :start_link, []}},
          {Prism.MetricsSink, []},
          {Prism.MetricsAPI, []}
        ] ++
        redix_children ++
        [
          %{
            id: Prism.PubSub,
            start: {Redix.PubSub, :start_link, [Keyword.put(redis_opts, :name, Prism.PubSub)]}
          },
          {Prism.RateLimit.Backpressure, []},
          {Prism.RateLimit.InvalidRequestTracker, []},
          {Prism.CongestionWindow, []},
          {Prism.DelayedScheduler, []},
          {Prism.SchemaRegistry, []},
          {Prism.StreamTrimmer, []}
          # Spawn N parallel FanoutBroadway producers to parallelize XREADGROUP fetches.
          # Each instance owns its own Redix connection; they share the consumer group
          # so Redis distributes messages across them.
        ] ++
        for i <- 0..(Prism.Config.fanout_producer_count() - 1) do
          name = Module.concat(Prism.FanoutBroadway, :"Jobs_#{i}")

          Supervisor.child_spec(
            {Prism.FanoutBroadway, [name: name, lane: :jobs]},
            id: :"fanout_broadway_jobs_#{i}"
          )
        end ++
        [
          Supervisor.child_spec(
            {Prism.RetryBroadway, [name: Prism.RetryBroadway]},
            id: :retry_broadway
          ),
          {Prism.MetricsLogger, []}
        ]

    opts = [strategy: :one_for_one, name: Prism.Supervisor]

    Logger.info("[Prism] Application supervisor starting children...")
    Supervisor.start_link(children, opts)
  end
end
