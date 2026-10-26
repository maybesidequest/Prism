# Prism — A High-Performance Discord Webhook Router

Prism is a high-performance, asynchronous worker pool written in Elixir. Polarizer is its only production message producer; Prism delivers approved binary-Protobuf jobs to Discord at scale.

Polarizer publishes raw `PrismStreamPayload` values to Kafka topic `prism.stream.jobs`, with CloudEvents metadata in Kafka headers. Prism validates the topic, partition key, producer source, event type, content type, and action/batch/message identity before delivery. It publishes typed delivery receipts to `events.prism.delivery.v2`; JSON summaries are observational only. Redis is used internally for delayed retries, rate limits, checkpoints, and caches, never as the production ingress authority.

Prism is entirely payload-agnostic. It does not enforce any specific Discord formatting rules, making it a general-purpose tool for any Discord bot or application that needs to broadcast messages to hundreds or thousands of webhooks quickly.

## Architecture

This worker uses **Broadway Kafka** to process approved jobs concurrently, with **Finch** providing high-performance HTTP pooling.

### Key Features
- **Unbounded Fan-out:** Group requests into batches; Prism concurrently dispatches up to 80 targets per batch (configurable).
- **Smart Retries & Backpressure:** Automatically intercepts Discord's `429` rate limit responses, reads the `retry_after` header, and spawns background tasks to complete the request without blocking the main pipeline.
- **Target Overrides:** Efficiently send the exact same base payload to most targets, while providing a target-specific `overrides` dictionary that Prism will merge on the fly.
- **Callback System:** Emits typed delivery state to Polarizer and separate observational batch summaries.
- **Configurable Everything:** All thresholds, delays, stream keys, pool sizes, and feature gates are configurable via environment variables. See `.env.example` for the full list.
- **Feature Gates:** Dead message cache, key expansion, cancel checker, and stream trimmer can all be disabled independently.

## Integration Contract

Prism consumes and produces raw binary Protobuf over Kafka. CloudEvents metadata is carried in Kafka headers; Discord request bodies remain JSON inside the Protobuf payload. See [CONTRACT.md](CONTRACT.md).

## Configuration

All runtime configuration is done via environment variables with sensible defaults. Copy `.env.example` to `.env` and adjust as needed:

```bash
cp .env.example .env
```

### Essential Variables

| Variable | Description | Default |
|---|---|---|
| `REDIS_HOST` | Redis hostname | `localhost` |
| `REDIS_PORT` | Redis port | `6379` |
| `PRISM_STREAM_JOBS` | Jobs lane stream topic/key | `prism.stream.jobs` |
| `EVENT_BUS_TRANSPORT` | Production transport (must be `kafka`) | `kafka` |
| `KAFKA_BROKERS` | Comma-separated Kafka brokers | `localhost:9092` |
| `PRISM_CONSUMER_GROUP` | Consumer group name | `prism:cg:fanout` |
| `PRISM_JOB_SOURCE` | Required `ce_source` job header | `/polarizer` |
| `PRISM_JOB_EVENT_TYPE` | Required `ce_type` job header | `fun.interchat.prism.job` |
| `PRISM_JOBS_DLQ_TOPIC` | Restricted invalid-job topic | `prism.stream.jobs.dlq` |
| `PRISM_JOBS_RETRY_TOPIC` | Durable approved-job retry topic | `prism.stream.jobs.retry` |
| `PRISM_HANDOFF_RETRY_BASE_MS` | Initial mandatory Kafka-handoff retry delay | `100` |
| `PRISM_HEALTH_PORT` | `/live` and `/ready` probe port | `9090` |

`/live` proves the BEAM can respond. `/ready` fails closed until the Prism
supervisor, Kafka client, Redis PubSub connection, and every configured fanout
consumer and retry consumer are running.

The Redis event-bus transport (`Prism.EventBus.Transport.Redis`, with
`Prism.EventBus.Consumer` and `Prism.EventBus.DLQ`) is for dev and tests only,
so the test suite runs without a Kafka broker. Production refuses any
`EVENT_BUS_TRANSPORT` other than `kafka`. The delayed queue, the retries stream,
rate limits and caches use Redis through separate code, in every environment.

### Performance Tuning

| Variable | Description | Default |
|---|---|---|
| `PRISM_REDIX_POOL_SIZE` | Redix connection pool size | `5` |
| `PRISM_FINCH_POOL_COUNT` | Finch HTTP connection pool size | `50` |
| `PRISM_BROADWAY_CONCURRENCY` | Max concurrent batches per fanout lane | `50` |
| `PRISM_BATCH_MAX_CONCURRENCY` | Max concurrent HTTP requests per batch | `80` |
| `PRISM_RETRY_BROADWAY_CONCURRENCY` | Max concurrent batches for retry lane | `10` |
| `PRISM_MAX_ASYNC_BATCHES` | Max in-flight async batches before re-enqueue | `300` |

See `.env.example` for the complete list of all configurable variables including retry parameters, rate limit thresholds, feature gates, timeouts, and cluster settings.

## Getting Started

1. Ensure you have [Elixir installed](https://elixir-lang.org/install.html).
2. Clone the repository.
3. Install dependencies:
   ```bash
   mix deps.get
   ```
4. Copy the `.env.example` file and set your environment variables:
   ```bash
   cp .env.example .env
   ```
5. Source your environment and start the application:
   ```bash
   source .env && mix run --no-halt
   ```

## Docker

```bash
# Build
docker build -t prism .

# Run
docker run -d \
  -e REDIS_HOST=host.docker.internal \
  -e REDIS_PORT=6379 \
  prism
```

## Project Structure

```
lib/
├── prism.ex                                  # Application entrypoint
├── prism/
│   ├── application.ex                        # Supervision tree
│   ├── config.ex                             # Centralized configuration module
│   ├── helpers.ex                            # Shared utility functions
│   ├── discord_worker.ex                     # Core orchestration (process_target, process_retry)
│   ├── discord_worker/
│   │   ├── http.ex                           # HTTP request building & execution
│   │   ├── retry.ex                          # Retry spawning & delayed queue enqueue
│   │   ├── callbacks.ex                      # Partial callback publishing
│   │   └── dead_message.ex                   # Dead message cache
│   ├── fanout_broadway.ex                    # Broadway pipeline (Jobs lane)
│   ├── fanout_broadway/
│   │   ├── key_expansion.ex                  # Short→long JSON key mapping
│   │   ├── batch.ex                          # Batch fan-out & result aggregation

│   ├── rate_limit.ex                         # Public facade for rate-limit operations
│   ├── rate_limit/
│   │   ├── backpressure.ex                   # Cloudflare IP-level block tracking
│   │   ├── bucket.ex                         # Redis-backed token bucket
│   │   ├── headers.ex                        # HTTP header parsing
│   │   └── invalid_request_tracker.ex        # Sliding-window invalid request counter
│   ├── delayed_queue.ex                      # Atomic Redis ZSET enqueue/pop
│   ├── delayed_scheduler.ex                  # Event-driven queue scheduler
│   ├── retry_broadway.ex                     # Retry stream consumer
│   ├── cancel_checker.ex                     # Source message cancel detection
│   ├── stream_trimmer.ex                     # Periodic stream XTRIM
│   ├── redis_client.ex                       # OffBroadwayRedisStream adapter
│   ├── metrics_api.ex                        # Telemetry/metrics HTTP API
│   └── metrics_logger.ex                     # Periodic server metrics logging
config/
├── runtime.exs                               # Env var → config mapping
└── test.exs                                  # Test-specific config overrides
test/                                         # Unit and integration tests
```

## License

This project is licensed under the GNU Affero General Public License v3.0 (AGPL-3.0). See the [LICENSE](LICENSE) file for details.
