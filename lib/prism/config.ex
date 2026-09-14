defmodule Prism.Config do
  @moduledoc """
  Centralized configuration module for all configurable Prism values.

  Every getter reads from Application env with a sensible default,
  providing a single source of truth for all runtime configuration.
  """

  # ── Service probes ────────────────────────────────────────────────────

  @doc "IP address for the health probe listener"
  def health_host, do: Application.get_env(:prism, :health_host, {0, 0, 0, 0})

  @doc "Port for liveness and readiness probes"
  def health_port, do: Application.get_env(:prism, :health_port, 9090)

  # ── Redis ──────────────────────────────────────────────────────────────

  @doc "Redis connection options"
  def redis_opts, do: Application.get_env(:prism, :redis_opts, host: "localhost", port: 6379)

  @doc "Number of Redix connections in the pool"
  def redix_pool_size, do: Application.get_env(:prism, :redix_pool_size, 10)

  # ── Stream keys ────────────────────────────────────────────────────────

  @doc "Jobs lane stream key"
  def stream_jobs,
    do: Application.get_env(:prism, :stream_jobs, "prism.stream.jobs")

  @doc "Retry stream key"
  def stream_retries,
    do: Application.get_env(:prism, :redis_retry_stream, "prism.stream.retries")

  @doc "Bad requests DLQ stream key"
  def stream_bad_requests_dlq,
    do: Application.get_env(:prism, :stream_bad_requests_dlq, "prism.dlq.bad_requests")

  @doc "Consumer group name"
  def consumer_group, do: Application.get_env(:prism, :consumer_group, "prism:cg:fanout")

  # ── Delayed queue ──────────────────────────────────────────────────────

  @doc "Delayed queue ZSET key"
  def delayed_zset_key,
    do: Application.get_env(:prism, :delayed_zset_key, "prism:delayed")

  @doc "PubSub wakeup channel"
  def pubsub_channel, do: Application.get_env(:prism, :pubsub_channel, "prism:wakeup")

  @doc "Error retry delay in ms when delayed scheduler Redis call fails"
  def delayed_scheduler_error_retry_ms,
    do: Application.get_env(:prism, :delayed_scheduler_error_retry_ms, 5_000)

  # ── Discord / HTTP ─────────────────────────────────────────────────────

  @doc "Discord API base URL"
  def discord_base_url,
    do: Application.get_env(:prism, :discord_base_url, "https://discord.com")

  @doc "Number of Finch HTTP connections in the pool"
  def finch_pool_count, do: Application.get_env(:prism, :finch_pool_count, 50)

  @doc "Finch HTTP protocols"
  def finch_protocols, do: Application.get_env(:prism, :finch_protocols, [:http2])

  @doc "Finch receive timeout in ms"
  def finch_receive_timeout_ms,
    do: Application.get_env(:prism, :finch_receive_timeout_ms, 30_000)

  @doc "Finch pool timeout in ms"
  def finch_pool_timeout_ms,
    do: Application.get_env(:prism, :finch_pool_timeout_ms, 10_000)

  @doc "Finch max idle time in ms"
  def finch_idle_timeout_ms,
    do: Application.get_env(:prism, :finch_idle_timeout_ms, 60_000)

  @doc "Finch keepalive in ms"
  def finch_keepalive_ms, do: Application.get_env(:prism, :finch_keepalive_ms, 30_000)

  # ── Rate limiting ──────────────────────────────────────────────────────

  @doc "Enable/disable backpressure"
  def backpressure_enabled?,
    do: Application.get_env(:prism, :backpressure_enabled, true)

  @doc "Maximum backoff for Cloudflare blocks (ms)"
  def backpressure_max_backoff_ms,
    do: Application.get_env(:prism, :backpressure_max_backoff_ms, 600_000)

  @doc "Minimum cooldown window after a Cloudflare block (ms)"
  def backpressure_min_cooldown_ms,
    do: Application.get_env(:prism, :backpressure_min_cooldown_ms, 60_000)

  @doc "Invalid request tracking window (ms)"
  def invalid_request_window_ms,
    do: Application.get_env(:prism, :invalid_request_window_ms, 600_000)

  @doc "Threshold for invalid request backpressure"
  def invalid_request_backpressure_threshold,
    do: Application.get_env(:prism, :invalid_request_backpressure_threshold, 9_500)

  @doc "Critical threshold for invalid requests"
  def invalid_request_critical_threshold,
    do: Application.get_env(:prism, :invalid_request_critical_threshold, 10_000)

  @doc "Bucket hash TTL in seconds"
  def bucket_hash_ttl_seconds,
    do: Application.get_env(:prism, :bucket_hash_ttl_seconds, 3600)

  # ── Retry parameters ──────────────────────────────────────────────────

  @doc "Base delay for server error retries (ms)"
  def server_error_base_delay_ms,
    do: Application.get_env(:prism, :server_error_base_delay_ms, 2000)

  @doc "Max retry attempts for server errors"
  def server_error_max_retries,
    do: Application.get_env(:prism, :server_error_max_retries, 3)

  @doc "Base delay for network error retries (ms)"
  def network_error_base_delay_ms,
    do: Application.get_env(:prism, :network_error_base_delay_ms, 1000)

  @doc "Max retry attempts for network errors"
  def network_error_max_retries,
    do: Application.get_env(:prism, :network_error_max_retries, 5)

  @doc "Max retry attempts for message_not_found_transient"
  def message_not_found_max_retries,
    do: Application.get_env(:prism, :message_not_found_max_retries, 5)

  @doc "Threshold above which rate-limit defer triggers (ms)"
  def rate_limit_defer_threshold_ms,
    do: Application.get_env(:prism, :rate_limit_defer_threshold_ms, 10_000)

  @doc "Checkpoint TTL in seconds"
  def checkpoint_ttl_seconds,
    do: Application.get_env(:prism, :checkpoint_ttl_seconds, 86_400)

  # ── Feature gates ──────────────────────────────────────────────────────

  @doc "Enable/disable dead message cache"
  def dead_message_cache_enabled?,
    do: Application.get_env(:prism, :dead_message_cache_enabled, true)

  @doc "Enable/disable key expansion"
  def key_expansion_enabled?,
    do: Application.get_env(:prism, :key_expansion_enabled, true)

  @doc "Enable/disable cancel checker"
  def cancel_checker_enabled?,
    do: Application.get_env(:prism, :cancel_checker_enabled, true)

  @doc "Enable/disable stream trimmer"
  def stream_trimmer_enabled?,
    do: Application.get_env(:prism, :stream_trimmer_enabled, true)

  # ── Dead message cache ─────────────────────────────────────────────────

  @doc "Redis key prefix for dead message cache"
  def dead_message_cache_prefix,
    do: Application.get_env(:prism, :dead_message_cache_prefix, "prism:dead:")

  @doc "TTL for dead message cache entries (seconds)"
  def dead_message_cache_ttl,
    do: Application.get_env(:prism, :dead_message_cache_ttl, 1800)

  # ── Cancel checker ─────────────────────────────────────────────────────

  @doc "Redis key prefix for cancel checker"
  def cancel_prefix, do: Application.get_env(:prism, :cancel_prefix, "prism:cancel:")

  # ── Stream trimmer ─────────────────────────────────────────────────────

  @doc "Stream trim interval in ms"
  def stream_trim_interval_ms,
    do: Application.get_env(:prism, :stream_trim_interval_ms, 30_000)

  # ── Broadway tuning ────────────────────────────────────────────────────

  @doc "Broadway processor concurrency for fanout lanes"
  def broadway_concurrency, do: Application.get_env(:prism, :broadway_concurrency, 50)

  @doc "Broadway processor max_demand — messages fetched per XREADGROUP round-trip"
  def broadway_max_demand, do: Application.get_env(:prism, :broadway_max_demand, 50)

  @doc "Broadway processor min_demand — triggers refill before processors drain fully"
  def broadway_min_demand, do: Application.get_env(:prism, :broadway_min_demand, 5)

  @doc "Number of parallel FanoutBroadway producer instances (each owns its own Redis connection)"
  def fanout_producer_count, do: Application.get_env(:prism, :fanout_producer_count, 3)

  @doc "Broadway processor concurrency for retry lane"
  def retry_broadway_concurrency,
    do: Application.get_env(:prism, :retry_broadway_concurrency, 10)

  @doc "Jobs lane receive interval (ms)"
  def jobs_receive_interval, do: Application.get_env(:prism, :jobs_receive_interval, 5)

  @doc "Retry lane receive interval (ms)"
  def retry_receive_interval, do: Application.get_env(:prism, :retry_receive_interval, 100)

  @doc "Queue time warning threshold (ms)"
  def queue_time_warn_ms, do: Application.get_env(:prism, :queue_time_warn_ms, 2000)

  @doc "Task async timeout (ms)"
  def task_timeout_ms, do: Application.get_env(:prism, :task_timeout_ms, 60_000)

  @doc "Max concurrent async batches"
  def max_async_batches, do: Application.get_env(:prism, :max_async_batches, 300)

  @doc "Batch max concurrency"
  def batch_max_concurrency, do: Application.get_env(:prism, :batch_max_concurrency, 80)

  @doc "Enable/disable preflight Redis batching (pipelines checkpoint + rate-limit reads before fan-out)"
  def preflight_batching_enabled?,
    do: Application.get_env(:prism, :preflight_batching_enabled, true)

  # ── Reply index ────────────────────────────────────────────────────────

  @doc "Enable/disable reply index storage"
  def reply_index_enabled?, do: Application.get_env(:prism, :reply_index_enabled, true)

  @doc "Include parent_message_id in callback payloads"
  def callback_include_parent_message_id?,
    do: Application.get_env(:prism, :callback_include_parent_message_id, true)

  @doc "Reply index Redis key prefix"
  def prism_prefix, do: Application.get_env(:prism, :prism_prefix, "prism")

  @doc "Reply index TTL in seconds"
  def reply_index_ttl_seconds,
    do: Application.get_env(:prism, :reply_index_ttl_seconds, 259_200)

  # ── Cluster ────────────────────────────────────────────────────────────

  @doc "Cluster topology name (atom)"
  def cluster_topology,
    do: Application.get_env(:prism, :cluster_topology, :prism_cluster)

  # ── Worker ─────────────────────────────────────────────────────────────

  @doc "Unique worker ID for Redis key scoping"
  def worker_id do
    :persistent_term.get(:prism_worker_id, "default")
  end

  @doc "Cancel TTL seconds"
  def cancel_ttl, do: Application.get_env(:prism, :cancel_ttl, 300)

  # ── Congestion control ─────────────────────────────────────────────────

  @doc "Enable/disable Cubic congestion control with 4xx safety budget"
  def congestion_control_enabled?,
    do: Application.get_env(:prism, :congestion_control_enabled, false)

  # -- Cubic parameters --
  @doc "Initial congestion window size"
  def cwnd_initial, do: Application.get_env(:prism, :cwnd_initial, 100)

  @doc "Minimum congestion window (floor)"
  def cwnd_min, do: Application.get_env(:prism, :cwnd_min, 10)

  @doc "Maximum congestion window (ceiling)"
  def cwnd_max, do: Application.get_env(:prism, :cwnd_max, 2000)

  @doc "Initial slow-start threshold"
  def ssthresh_initial, do: Application.get_env(:prism, :ssthresh_initial, 500)

  @doc "Cubic scaling constant C (RFC 9438 default: 0.4)"
  def cubic_c, do: Application.get_env(:prism, :cubic_c, 0.4)

  @doc "Multiplicative decrease factor for global 429s"
  def cwnd_beta_global, do: Application.get_env(:prism, :cwnd_beta_global, 0.7)

  @doc "Multiplicative decrease factor for Cloudflare 429s"
  def cwnd_beta_cloudflare, do: Application.get_env(:prism, :cwnd_beta_cloudflare, 0.3)

  @doc "Probe interval (ms)"
  def cwnd_probe_interval_ms, do: Application.get_env(:prism, :cwnd_probe_interval_ms, 1_000)

  @doc "Minimum time between consecutive Cubic decreases (ms)"
  def cwnd_decrease_cooldown_ms,
    do: Application.get_env(:prism, :cwnd_decrease_cooldown_ms, 5_000)

  # -- 4xx Safety Budget --
  @doc "Max 4xx responses before full throttle (Cloudflare 404 restriction ~200)"
  def cwnd_4xx_budget, do: Application.get_env(:prism, :cwnd_4xx_budget, 200)

  @doc "4xx sliding window duration (ms)"
  def cwnd_4xx_window_ms, do: Application.get_env(:prism, :cwnd_4xx_window_ms, 60_000)

  @doc "Utilization below which no throttling occurs (0.0–1.0)"
  def cwnd_4xx_safe_pct, do: Application.get_env(:prism, :cwnd_4xx_safe_pct, 0.3)

  @doc "Utilization above which cwnd_min is enforced (0.0–1.0)"
  def cwnd_4xx_critical_pct, do: Application.get_env(:prism, :cwnd_4xx_critical_pct, 0.8)

  @doc "ETS prune interval for 4xx budget (ms)"
  def cwnd_4xx_prune_interval_ms,
    do: Application.get_env(:prism, :cwnd_4xx_prune_interval_ms, 10_000)
end
