import Config
import Dotenvy

dotenv_sources =
  if config_env() == :test do
    [System.get_env()]
  else
    [".env", ".env.local", System.get_env()]
  end

Dotenvy.source!(dotenv_sources)

log_level =
  case String.downcase(env!("LOG_LEVEL", :string, "info")) do
    "debug" -> :debug
    "info" -> :info
    "warning" -> :warning
    "warn" -> :warning
    "error" -> :error
    _ -> :info
  end

stream_jobs = env!("PRISM_STREAM_JOBS", :string, "prism.stream.jobs")

config :logger, level: log_level

if config_env() != :test do
  redis_socket_opts = [
    {:keepalive, true},
    {:nodelay, true},
    # Linux TCP keepalive tuning: idle 10s, interval 5s, 3 probes
    # Prevents Cilium conntrack GC from evicting idle Redis connections.
    {:raw, 6, 4, <<10::32-native>>},
    {:raw, 6, 5, <<5::32-native>>},
    {:raw, 6, 6, <<3::32-native>>}
  ]

  redis_opts =
    case System.get_env("REDIS_URI") do
      uri when is_binary(uri) and uri != "" ->
        uri |> Redix.URI.to_start_options() |> Keyword.put(:socket_opts, redis_socket_opts)

      _ when config_env() == :prod ->
        raise "REDIS_URI is required in production"

      _ ->
        [
          host: env!("REDIS_HOST", :string, "localhost"),
          port: env!("REDIS_PORT", :integer, 6379),
          database: env!("REDIS_DB", :integer, 0),
          socket_opts: redis_socket_opts
        ]
    end

  config :prism, redis_opts: redis_opts
end

if config_env() != :test do
  kafka_config = Prism.KafkaConfig.from_env!()

  health_host =
    case env!("PRISM_HEALTH_HOST", :string, "0.0.0.0")
         |> String.to_charlist()
         |> :inet.parse_address() do
      {:ok, address} -> address
      {:error, reason} -> raise "invalid PRISM_HEALTH_HOST: #{inspect(reason)}"
    end

  config :prism,
    health_host: health_host,
    health_port: env!("PRISM_HEALTH_PORT", :integer, 9090),
    redix_pool_size: env!("PRISM_REDIX_POOL_SIZE", :integer, 5),
    finch_pool_count: env!("PRISM_FINCH_POOL_COUNT", :integer, 50),
    finch_receive_timeout_ms: env!("PRISM_FINCH_RECEIVE_TIMEOUT_MS", :integer, 30000),
    finch_pool_timeout_ms: env!("PRISM_FINCH_POOL_TIMEOUT_MS", :integer, 10000),
    finch_idle_timeout_ms: env!("PRISM_FINCH_IDLE_TIMEOUT_MS", :integer, 60000),
    finch_keepalive_ms: env!("PRISM_FINCH_KEEPALIVE_MS", :integer, 30000),
    discord_base_url: env!("PRISM_DISCORD_BASE_URL", :string, "https://discord.com"),
    stream_jobs: stream_jobs,
    redis_retry_stream: env!("REDIS_RETRY_STREAM", :string, "prism.stream.retries"),
    consumer_group: env!("PRISM_CONSUMER_GROUP", :string, "prism:cg:fanout"),
    delayed_zset_key: env!("PRISM_DELAYED_ZSET_KEY", :string, "prism:delayed"),
    pubsub_channel: env!("PRISM_PUBSUB_CHANNEL", :string, "prism:wakeup"),
    delayed_scheduler_error_retry_ms:
      env!("PRISM_DELAYED_SCHEDULER_ERROR_RETRY_MS", :integer, 5000),
    broadway_concurrency: env!("PRISM_BROADWAY_CONCURRENCY", :integer, 50),
    broadway_max_demand: env!("PRISM_BROADWAY_MAX_DEMAND", :integer, 50),
    broadway_min_demand: env!("PRISM_BROADWAY_MIN_DEMAND", :integer, 5),
    fanout_producer_count: env!("PRISM_FANOUT_PRODUCER_COUNT", :integer, 3),
    batch_max_concurrency: env!("PRISM_BATCH_MAX_CONCURRENCY", :integer, 80),
    retry_broadway_concurrency: env!("PRISM_RETRY_BROADWAY_CONCURRENCY", :integer, 10),
    jobs_receive_interval: env!("PRISM_JOBS_RECEIVE_INTERVAL", :integer, 5),
    retry_receive_interval: env!("PRISM_RETRY_RECEIVE_INTERVAL", :integer, 100),
    queue_time_warn_ms: env!("PRISM_QUEUE_TIME_WARN_MS", :integer, 2000),
    task_timeout_ms: env!("PRISM_TASK_TIMEOUT_MS", :integer, 60000),
    max_async_batches: env!("PRISM_MAX_ASYNC_BATCHES", :integer, 300),
    backpressure_enabled: env!("PRISM_BACKPRESSURE_ENABLED", :boolean, true),
    backpressure_max_backoff_ms: env!("PRISM_BACKPRESSURE_MAX_BACKOFF_MS", :integer, 600_000),
    backpressure_min_cooldown_ms: env!("PRISM_BACKPRESSURE_MIN_COOLDOWN_MS", :integer, 60000),
    invalid_request_window_ms: env!("PRISM_INVALID_REQUEST_WINDOW_MS", :integer, 600_000),
    invalid_request_backpressure_threshold:
      env!("PRISM_INVALID_REQUEST_BACKPRESSURE_THRESHOLD", :integer, 9500),
    invalid_request_critical_threshold:
      env!("PRISM_INVALID_REQUEST_CRITICAL_THRESHOLD", :integer, 10000),
    bucket_hash_ttl_seconds: env!("PRISM_BUCKET_HASH_TTL_SECONDS", :integer, 3600),
    server_error_base_delay_ms: env!("PRISM_SERVER_ERROR_BASE_DELAY_MS", :integer, 2000),
    server_error_max_retries: env!("PRISM_SERVER_ERROR_MAX_RETRIES", :integer, 3),
    network_error_base_delay_ms: env!("PRISM_NETWORK_ERROR_BASE_DELAY_MS", :integer, 1000),
    network_error_max_retries: env!("PRISM_NETWORK_ERROR_MAX_RETRIES", :integer, 5),
    message_not_found_max_retries: env!("PRISM_MESSAGE_NOT_FOUND_MAX_RETRIES", :integer, 5),
    rate_limit_defer_threshold_ms: env!("PRISM_RATE_LIMIT_DEFER_THRESHOLD_MS", :integer, 10000),
    checkpoint_ttl_seconds: env!("PRISM_CHECKPOINT_TTL_SECONDS", :integer, 86400),
    dead_message_cache_enabled: env!("PRISM_DEAD_MESSAGE_CACHE_ENABLED", :boolean, true),
    key_expansion_enabled: env!("PRISM_KEY_EXPANSION_ENABLED", :boolean, true),
    cancel_checker_enabled: env!("PRISM_CANCEL_CHECKER_ENABLED", :boolean, true),
    stream_trimmer_enabled: env!("PRISM_STREAM_TRIMMER_ENABLED", :boolean, true),
    dead_message_cache_prefix: env!("PRISM_DEAD_MESSAGE_CACHE_PREFIX", :string, "prism:dead:"),
    dead_message_cache_ttl: env!("PRISM_DEAD_MESSAGE_CACHE_TTL", :integer, 1800),
    cancel_prefix: env!("PRISM_CANCEL_PREFIX", :string, "prism:cancel:"),
    stream_trim_interval_ms: env!("PRISM_STREAM_TRIM_INTERVAL_MS", :integer, 30000),
    callback_include_parent_message_id: env!("PRISM_INCLUDE_PARENT_MESSAGE_ID", :boolean, false),
    reply_index_enabled: env!("PRISM_REPLY_INDEX_ENABLED", :boolean, false),
    prism_prefix: env!("PRISM_PREFIX", :string, "prism"),
    # 72h: mirrors the bot's `prism_reply_index_ttl_seconds`. Both services
    # write `prism:targets:{id}`, so these defaults must stay in lockstep
    # (adopted mapping-retention policy, D03 scope resolved 2026-09-12).
    reply_index_ttl_seconds: env!("PRISM_REPLY_INDEX_TTL_SECONDS", :integer, 259_200),
    cancel_ttl: env!("PRISM_CANCEL_TTL", :integer, 300),
    cluster_topology: env!("PRISM_CLUSTER_TOPOLOGY", :string, "prism_cluster"),
    events_stream: env!("EVENTS_STREAM", :string, "events.bus"),
    events_dlq_stream: env!("EVENTS_DLQ_STREAM", :string, "events.bus.dlq"),
    events_stream_maxlen: env!("EVENTS_STREAM_MAXLEN", :integer, 100_000),
    event_source: env!("EVENT_SOURCE", :string, "/prism"),
    event_bus_max_retries: env!("EVENT_BUS_MAX_RETRIES", :integer, 3),
    event_bus_retry_backoff_base_ms: env!("EVENT_BUS_RETRY_BACKOFF_BASE_MS", :integer, 1000),
    event_bus_retry_backoff_max_ms: env!("EVENT_BUS_RETRY_BACKOFF_MAX_MS", :integer, 30000),
    event_bus_consumer_batch_size: env!("EVENT_BUS_CONSUMER_BATCH_SIZE", :integer, 10),
    event_bus_consumer_block_ms: env!("EVENT_BUS_CONSUMER_BLOCK_MS", :integer, 3000),
    event_bus_stale_claim_idle_ms: env!("EVENT_BUS_STALE_CLAIM_IDLE_MS", :integer, 30000),
    event_bus_stale_claim_interval_ms: env!("EVENT_BUS_STALE_CLAIM_INTERVAL_MS", :integer, 60000),
    event_bus_broadcast_type:
      env!("EVENT_BUS_BROADCAST_TYPE", :string, "prism.broadcast.completed"),
    event_bus_callback_type: env!("EVENT_BUS_CALLBACK_TYPE", :string, "prism.callback"),
    delivery_topic: env!("PRISM_DELIVERY_TOPIC", :string, "events.prism.delivery.v2"),
    prism_job_source: env!("PRISM_JOB_SOURCE", :string, "/polarizer"),
    prism_job_event_type: env!("PRISM_JOB_EVENT_TYPE", :string, "fun.interchat.prism.job"),
    prism_jobs_dlq_topic: env!("PRISM_JOBS_DLQ_TOPIC", :string, "#{stream_jobs}.dlq"),
    prism_jobs_retry_topic: env!("PRISM_JOBS_RETRY_TOPIC", :string, "#{stream_jobs}.retry"),
    prism_handoff_retry_base_ms: env!("PRISM_HANDOFF_RETRY_BASE_MS", :integer, 100),
    # Congestion control (Cubic + 4xx safety budget). The 4xx budget is the primary
    # protection against Cloudflare IP bans, so it defaults on — shipping it disabled
    # left nothing throttling a 4xx cascade. Set the env var to false to opt out.
    congestion_control_enabled: env!("PRISM_CONGESTION_CONTROL_ENABLED", :boolean, true),
    cwnd_initial: env!("PRISM_CWND_INITIAL", :integer, 100),
    cwnd_min: env!("PRISM_CWND_MIN", :integer, 10),
    cwnd_max: env!("PRISM_CWND_MAX", :integer, 2000),
    ssthresh_initial: env!("PRISM_SSTHRESH_INITIAL", :integer, 500),
    cubic_c: env!("PRISM_CUBIC_C", :float, 0.4),
    cwnd_beta_global: env!("PRISM_CWND_BETA_GLOBAL", :float, 0.7),
    cwnd_beta_cloudflare: env!("PRISM_CWND_BETA_CLOUDFLARE", :float, 0.3),
    cwnd_probe_interval_ms: env!("PRISM_CWND_PROBE_INTERVAL_MS", :integer, 1000),
    cwnd_decrease_cooldown_ms: env!("PRISM_CWND_DECREASE_COOLDOWN_MS", :integer, 5000),
    cwnd_4xx_budget: env!("PRISM_CWND_4XX_BUDGET", :integer, 200),
    cwnd_4xx_window_ms: env!("PRISM_CWND_4XX_WINDOW_MS", :integer, 60000),
    cwnd_4xx_safe_pct: env!("PRISM_CWND_4XX_SAFE_PCT", :float, 0.3),
    cwnd_4xx_critical_pct: env!("PRISM_CWND_4XX_CRITICAL_PCT", :float, 0.8),
    cwnd_4xx_prune_interval_ms: env!("PRISM_CWND_4XX_PRUNE_INTERVAL_MS", :integer, 10000),
    schema_registry_url: env!("SCHEMA_REGISTRY_URL", :string, "http://localhost:8081"),
    kafka_brokers: kafka_config.brokers,
    kafka_client_config: kafka_config.client_config,
    kafka_brod_client_config: kafka_config.brod_client_config

  event_bus_transport =
    case String.downcase(env!("EVENT_BUS_TRANSPORT", :string, "kafka")) do
      "kafka" ->
        Module.concat(["Prism.EventBus.Transport.Kafka"])

      other ->
        raise "EVENT_BUS_TRANSPORT must be kafka in production, got: #{inspect(other)}"
    end

  config :prism, event_bus_transport_backend: event_bus_transport
end

config :opentelemetry,
  span_processor: :batch,
  traces_exporter: :otlp

config :opentelemetry_exporter,
  otlp_protocol: :http_protobuf,
  otlp_endpoint: env!("OTEL_EXPORTER_OTLP_ENDPOINT", :string, "http://localhost:4318")
