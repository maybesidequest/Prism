# Elixir Prism Service — Agent Context (`prism/`)

This document is the specialized guide for agents working on the Elixir-based webhook broadcast service `Prism`. It covers Stream lane consumers, key expansion functions, rate limit tracking, and the atomic delayed queue system.

## Search and repository boundaries

For any file search or grep in the current git-indexed directory, use the Prism `fff_prism` MCP server. Use `find_files` for file names, `grep` for one content identifier, and `multi_grep` for several alternatives. FFF is scoped to one repository; use the FFF server configured for the target repository when a task crosses into a sibling repo. If that server is unavailable, use regular search scoped to that repository.

---

## Service Architecture

Prism is an OTP application built in Elixir that consumes Polarizer-approved binary Protobuf payloads from Kafka and dispatches them concurrently via Finch HTTP clients to Discord's webhooks. Redis remains an internal retry/rate-limit/cache dependency.

```
prism/
├── lib/
│   ├── prism/
│   │   ├── discord_worker/           # Discord HTTP delivery (split into sub-modules)
│   │   │   ├── callbacks.ex          # Partial callback publishing
│   │   │   ├── dead_message.ex       # Dead message cache (prevents retrying deleted messages)
│   │   │   ├── http.ex               # HTTP request building & execution
│   │   │   └── retry.ex              # Retry spawning & delayed queue enqueue
│   │   ├── fanout_broadway/          # Jobs-lane fanout pipeline (split into sub-modules)
│   │   │   ├── batch.ex              # Batch fan-out, result aggregation, reply index
│   │   │   ├── key_expansion.ex      # Short→long JSON key mapping
│   │   │   └── preflight.ex           # Batched checkpoint and rate-limit checks
│   │   ├── rate_limit/               # Rate limit tracking & Cloudflare backpressure
│   │   │   ├── backpressure.ex       # IP-level Cloudflare block tracking
│   │   │   ├── bucket.ex             # Redis-backed token bucket
│   │   │   ├── headers.ex            # Discord/Cloudflare HTTP header parsing
│   │   │   └── invalid_request_tracker.ex  # ETS sliding-window invalid request counter
│   │   ├── application.ex            # Supervision tree configuration
│   │   ├── async_batch_counter.ex     # In-flight async batch cap
│   │   ├── cancel_checker.ex         # Source message cancel detection (gated)
│   │   ├── congestion_window.ex      # Optional Cubic-style concurrency control
│   │   ├── config.ex                 # Centralized configuration module
│   │   ├── delayed_queue.ex          # Atomic Redis ZSET enqueue/pop handlers
│   │   ├── delayed_scheduler.ex      # Event-driven queue scheduler
│   │   ├── discord_worker.ex         # Core orchestration (process_target, process_retry)
│   │   ├── fanout_broadway.ex        # Broadway pipeline for approved jobs
│   │   ├── health.ex                 # Liveness and readiness probes
│   │   ├── helpers.ex                # Shared utilities (redix_command, payload extraction, etc.)
│   │   ├── metrics_api.ex            # Telemetry/metrics HTTP API
│   │   ├── metrics_logger.ex         # Periodic server metrics logging
│   │   ├── rate_limit.ex             # Public facade for all rate-limit operations
│   │   ├── redis_client.ex           # OffBroadwayRedisStream client adapter
│   │   ├── retry_broadway.ex         # Retry stream consumer
│   │   ├── schema_registry.ex        # Runtime event/schema validation
│   │   └── stream_trimmer.ex         # Periodic stream XTRIM (gated)
│   └── prism.ex
├── config/                           # Elixir compile-time configurations
│   ├── runtime.exs                   # Runtime env var → config mapping
│   └── test.exs                      # Test-specific config overrides
├── mix.exs                           # Package manifest & deps
├── .env.example                      # All environment variables with defaults
└── CONTRACT.md                       # Data structures & serialization rules
```

---

## Configuration (`Prism.Config`)

All runtime configuration is centralized in `lib/prism/config.ex`. Every configurable value has a getter function that reads from `Application.get_env/3` with sensible defaults. See `.env.example` for the full list of environment variables.

Key config categories:
- **EventBus**: transport backend (`Redis` or `Kafka`), stream topics, Kafka brokers
- **Redis / Streams**: pool size, consumer group, delayed queue keys, retry stream keys
- **HTTP / Finch**: pool count, timeouts, keepalive
- **Rate limiting**: backpressure thresholds, invalid request tracker windows, bucket TTLs
- **Retry parameters**: base delays, max attempts per error type, checkpoints
- **Feature gates**: dead message cache, key expansion, cancel checker, stream trimmer
- **Broadway tuning**: concurrency, receive intervals, task timeouts

- **Cluster**: topology name

---

## Consumer Lanes (`Prism.FanoutBroadway`)

Prism runs Broadway pipelines to consume batches concurrently:

1. **Jobs Lane (default `prism.stream.jobs`):** The authoritative production topic written by Polarizer after policy approval. The configured EventBus backend can be swapped for local/test use, but production is Kafka.
2. **Retry Lane (default `prism.stream.retries`):** Internal Redis Stream fed by the delayed scheduler for per-target and backpressure retries.

Whole approved-job retries use the durable Kafka topic `prism.stream.jobs.retry`; the Redis retry lane is limited to per-target scheduling and is never the only retained copy of an acknowledged Kafka job.

All lane parameters (concurrency, receive intervals) are configurable via `Prism.Config`.

---

## Key Expansion (`Prism.FanoutBroadway.KeyExpansion`)

To optimize payload size, publishers may minify JSON object keys before publishing a batch. Prism automatically expands these keys to their full names using a compile-time `@key_map`, regardless of whether the batch arrived through Kafka or the Redis compatibility backend.

```elixir
# Detects format: if the first key is inside @key_map, recursively maps keys
def expand_keys(map) when is_map(map) do ... end
```

- **Backward Compatibility:** If the incoming payload is already in long-key format (e.g., the key `"action"` is present and not a known short key), it passes through unchanged.
- **Disableable:** Set `PRISM_KEY_EXPANSION_ENABLED=false` to skip key expansion entirely. The key map is opinionated; users not using key minification should disable it.

---

## Atomic Delayed Queue System

Failed webhook targets (rate-limited, server errors, network dropouts) are enqueued in Redis for delayed execution using a **ZSET** (default key `prism:delayed`).

### Enqueueing (`Prism.DelayedQueue.enqueue/2`)
- Adds a unique `retry_id` to the payload to prevent duplicates in the ZSET.
- Scores the payload by epoch timestamp `execute_at_ms`.
- Uses a Lua script to add the item. If the item has the *lowest score* (soonest tick), the script publishes a wakeup command to the Redis PubSub channel (default `prism:wakeup`).

### Event-Driven Scheduling (`Prism.DelayedScheduler`)
- A zero-polling GenServer subscribes to the PubSub channel.
- It executes `migrate_due_items/1` using an atomic Lua script:
  1. Queries all items whose execution timestamp is ≤ `now`.
  2. Removes them from the ZSET and adds them to the retry stream.
  3. Returns the score of the *new* earliest item in the ZSET.
- The scheduler calculates the delay to the next earliest item and registers a `Process.send_after/3` timer to sleep until that tick.
- If a `"new_earliest"` PubSub wakeup event is received, the current timer is immediately cancelled, and the process ticks.
- On Redis failure, the scheduler retries after a configurable delay (default 5s).

---

## Rate Limiting & Backpressure

### Rate Limit Facade (`Prism.RateLimit`)
- Public API that aggregates `Bucket`, `Backpressure`, `Headers`, and `InvalidRequestTracker`.
- Callers import one module for `.check/2`, `.handle_response/5`, `.unhealthy?/0`, `.backoff_ms/0`, `.record_success/0`.

### Local Rate-Limit Buckets (`Prism.RateLimit.Bucket`)
- Keeps Redis-backed counters of remaining requests per webhook ID.
- Pre-flight checks (`acquire/2`) atomically check-and-decrement *before* making network calls.
- Updates bucket state on HTTP responses (2xx extracts `x-ratelimit-*` headers; 429 locks the bucket).
- All thresholds and TTLs are configurable via `Prism.Config`.

### Cloudflare IP-Level Blocks (`Prism.RateLimit.Backpressure`)
- If a 429 response is identified as coming from Cloudflare (IP-level blocking):
  1. Backpressure is triggered via `record_cloudflare_block/1`.
  2. The backoff target is saved to `:persistent_term` to survive GenServer crashes.
  3. Outbound requests are blocked (`unhealthy?() == true`).
  4. Once a worker records a successful 2xx request, `record_success/0` clears the backpressure blocks.

### Invalid Request Tracker (`Prism.RateLimit.InvalidRequestTracker`)
- ETS-based sliding window counter for invalid HTTP responses (401, 403, non-shared 429).
- When the count approaches Discord's Cloudflare ban threshold, outbound HTTP is paused.
- All window sizes and thresholds are configurable via `Prism.Config`.

---

## Helpers (`Prism.Helpers`)

Shared utilities consumed across the codebase:
- `redix_command/1` — Redis command dispatch with pool-aware connection selection
- `redix_pipeline/1` — Redis pipeline dispatch
- `get_payload_from_redis_data/1` — Extracts the `payload` field from OffBroadwayRedisStream data
- `empty_discord_payload?/1` — Checks whether a webhook payload has no content, embeds, or components
- `action_to_method_string/1` — Converts Prism action names to HTTP method strings
- `safe_method_atom/1` — Converts method strings to atoms with fallback

---

## Discord Worker Sub-Modules

### `Prism.DiscordWorker` (core)
- `process_target/7` — Main entry point for processing individual webhook targets
- `process_retry/3` — Retry processing from the retry pipeline

### `Prism.DiscordWorker.HTTP`
- `build_request/4` — URL/method construction for execute/edit/delete
- `do_http_request/7` — Finch HTTP call wrapper with OpenTelemetry tracing

### `Prism.DiscordWorker.Retry`
- `spawn_retry/13` — Encode retry payload and enqueue to `DelayedQueue`

### `Prism.DiscordWorker.Callbacks`
- `publish_partial/6` — Publish individual retry results to the callback stream

### `Prism.DiscordWorker.DeadMessage`
- `dead_message_cached?/2` — Check both scoped and unscoped dead message cache keys
- `cache_dead_message/3` — Store dead message with configurable TTL (gated by `Prism.Config.dead_message_cache_enabled?/0`)

---

## Fanout Broadway Sub-Modules

### `Prism.FanoutBroadway` (core)
- `start_link/1` — Broadway initialization with configurable stream keys and receive intervals
- `handle_message/3` — Parse JSON, expand keys, gate checks, spawn async batch

### `Prism.FanoutBroadway.KeyExpansion`
- `@key_map` — The 23-entry short→long key mapping (compile-time constant)
- `expand_keys/1` — Recursive key expansion (gated by `Prism.Config.key_expansion_enabled?/0`)

### `Prism.FanoutBroadway.Batch`
- `process_batch/10` — `Task.async_stream` fan-out, success/failure aggregation
- `spawn_async_batch/10` — `Task.Supervisor` async spawning with crash recovery
- `store_reply_index/2` — Redis reply index storage
