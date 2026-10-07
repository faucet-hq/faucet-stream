# faucet-source-kafka

[![Crates.io](https://img.shields.io/crates/v/faucet-source-kafka.svg)](https://crates.io/crates/faucet-source-kafka)
[![Docs.rs](https://docs.rs/faucet-source-kafka/badge.svg)](https://docs.rs/faucet-source-kafka)
[![MSRV](https://img.shields.io/crates/msrv/faucet-source-kafka.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-source-kafka.svg)](https://github.com/faucet-hq/faucet-stream#license)

Apache **Kafka** consumer source for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. Subscribes to one or more topics, drains messages until a `max_messages` count or an `idle_timeout` window fires, and emits each Kafka message as a structured JSON record. Built on [`rdkafka`](https://crates.io/crates/rdkafka) (librdkafka bindings) — one of the fastest Kafka clients available.

Reach for it when you want to land a Kafka topic into any faucet-stream sink — a file, a database, a warehouse, object storage — with one declarative config, durable offset resume, and no glue code. Offsets are tracked through any `faucet-core` `StateStore`, so a pipeline resumes exactly where the last run stopped, without re-reading or skipping records.

## Feature highlights

- **Native streaming** — overrides `Source::stream_pages`, draining the consumer into `batch_size`-sized pages so memory stays `O(batch_size)` no matter the topic volume; the sink writes (and the bookmark advances) incrementally rather than once at the end of the run.
- **Durable offset resume** — persists a per-partition offset bookmark through any `StateStore` (file, memory, Redis, Postgres). On restart the bookmark seeds the partition assignment *before* the first poll, so a resume never produces a duplicate or skips a record.
- **Two stop conditions** — `max_messages` and/or `idle_timeout`; the loop exits on whichever fires first (at least one is required). `Ctrl+C` exits cleanly, persisting everything consumed so far.
- **Five authentication modes** — plaintext, SASL/PLAIN, SASL/SCRAM (SHA-256 / SHA-512), SSL client certificates, and SASL+SSL — via the shared `KafkaAuth` enum from [`faucet-common-kafka`](https://crates.io/crates/faucet-common-kafka).
- **Six value formats** — JSON, raw string, raw bytes (base64), plus Confluent Avro / Protobuf / JSON Schema behind the `schema-registry` feature.
- **Structured records** — each message becomes a JSON object with `key`, `value`, `topic`, `partition`, `offset`, `timestamp`, and `headers`.
- **Per-message decode policy** — `on_decode_error: fail | skip` chooses between aborting the batch and dropping a bad message with a warning.
- **Escape hatch** — `extra_client_config` passes any raw librdkafka property straight through to the consumer.

## Installation

```bash
# As a library:
cargo add faucet-source-kafka

# With Confluent Schema Registry support:
cargo add faucet-source-kafka --features schema-registry

# In the CLI (opt-in connector feature):
cargo install faucet-cli --features source-kafka
```

The Kafka source is **not** in the CLI default build — enable `source-kafka` (or `full`). Schema-Registry-backed formats additionally require `kafka-schema-registry` on the CLI / umbrella.

## Quick start

```yaml
# pipeline.yaml — faucet run pipeline.yaml
version: 1
pipeline:
  source:
    type: kafka
    config:
      brokers: "localhost:9092"
      topics: ["orders"]
      group_id: faucet-orders-consumer
      value_format: { type: json }
      auto_offset_reset: earliest
      idle_timeout: 30      # stop after 30 s of no new messages
      max_messages: 10000   # or after 10 000 messages, whichever comes first
  sink:
    type: file
    config:
      path: ./orders.jsonl
```

```bash
faucet run pipeline.yaml
```

To resume from where the last run stopped, add a state store:

```yaml
  state:
    type: file
    config:
      path: ./.faucet-state
```

## Configuration reference

All fields are keys under `source.config`.

### Core

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `brokers` | string | — *(required)* | Comma-separated bootstrap broker list, e.g. `"broker1:9092,broker2:9092"`. |
| `topics` | string[] | — *(required)* | One or more topic names to subscribe to. Must contain at least one entry. |
| `group_id` | string | — *(required)* | Kafka consumer group ID. Drives partition assignment and forms part of the state-store key. |
| `auth` | `KafkaAuth` | `{ type: none }` | Authentication mode — see [Authentication](#authentication). |
| `value_format` | `KafkaValueFormat` | `{ type: json }` | How message **value** bytes are decoded — see [Value formats](#value-formats). |
| `key_format` | `KafkaValueFormat` \| null | `null` | How message **key** bytes are decoded. When unset, key bytes are decoded as UTF-8 (or `null` if the message carried no key). |

### Termination & polling

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_messages` | int \| null | `null` | Stop after this many messages. At least one of `max_messages` / `idle_timeout` is required. |
| `idle_timeout` | int (seconds) \| null | `null` | Stop after this many seconds with no new message. At least one of `max_messages` / `idle_timeout` is required. |
| `poll_timeout` | int (seconds) | `1` | Max time to block on a single `consumer.recv()` before re-checking termination. Advisory — rarely needs tuning. |
| `session_timeout` | int (seconds) | `30` | Kafka `session.timeout.ms` (in seconds). Increase for slow brokers or long GC pauses. |

### Offsets & reliability

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `auto_offset_reset` | `earliest` \| `latest` | `latest` | Where to start a partition that has **no** bookmarked offset — i.e. a first-ever run or a newly-added partition. Resumed partitions always start from their bookmark. |
| `on_decode_error` | `fail` \| `skip` | `fail` | What to do when one message fails to decode. `fail` aborts the batch; `skip` drops the message and logs a `WARN`. |
| `extra_client_config` | object | `{}` | Raw librdkafka client properties passed straight to the consumer. These can override anything set by `auth` or the typed fields above — use with care. |

### Batching

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `batch_size` | int | `1000` | Messages per emitted `StreamPage`. **`0` = drain the entire run window** into one page (tests / one-shot drains only — see [Streaming & batching](#streaming--batching)). Capped at `MAX_BATCH_SIZE` (1,000,000). |

## Authentication

`auth` uses the shared `KafkaAuth` enum (the project-wide `{ type, config }` shape). The full reference — all fields and edge cases — lives in the [`faucet-common-kafka`](https://crates.io/crates/faucet-common-kafka) README.

| `type` | `config` | Use when |
|--------|----------|----------|
| `none` | *(none)* | Plaintext brokers (default). |
| `sasl_plain` | `{ username, password }` | Confluent Cloud, MSK with SASL/PLAIN. |
| `sasl_scram` | `{ mechanism: sha256\|sha512, username, password }` | Brokers configured for SCRAM. |
| `ssl` | `{ ca_path, cert_path, key_path, key_password? }` | Mutual-TLS client certificates. |
| `sasl_ssl` | `{ sasl: {…}, ssl: {…} }` | SASL over a TLS transport. |

```yaml
# SASL/PLAIN — env indirection keeps secrets out of the YAML
auth:
  type: sasl_plain
  config:
    username: ${env:KAFKA_USERNAME}
    password: ${env:KAFKA_PASSWORD}
```

```yaml
# SASL/SCRAM-SHA-512
auth:
  type: sasl_scram
  config:
    mechanism: sha512
    username: ${env:KAFKA_USERNAME}
    password: ${env:KAFKA_PASSWORD}
```

```yaml
# Mutual TLS
auth:
  type: ssl
  config:
    ca_path: /etc/kafka/certs/ca.pem
    cert_path: /etc/kafka/certs/client.pem
    key_path: /etc/kafka/certs/client.key
```

## Value formats

Configured via `value_format` (and optionally `key_format`); all use a `type` discriminator.

| `type` | Description | Feature |
|--------|-------------|---------|
| `json` | Parse value bytes as a JSON document. **Default.** | base |
| `raw_string` | Decode value bytes as a UTF-8 string into `value`. | base |
| `bytes` | Pass bytes through as a **base64-encoded string** in `value`; no parsing. | base |
| `confluent_avro` | Confluent wire-format Avro: `[0x00][schema_id 4B][Avro binary]`. Logical types map like the Avro file format: `decimal` → exact decimal string, `bytes`/`fixed` → hex, `date`/`time`/`timestamp` → ISO 8601 strings. | `schema-registry` |
| `confluent_protobuf` | Confluent wire-format Protobuf (single-message schemas). Every field is emitted, proto3 defaults (`0`, `false`, `""`) included, under its `.proto` field name (`order_id`, not `orderId`); 64-bit integers follow the proto3 JSON mapping (strings). | `schema-registry` |
| `confluent_json_schema` | Confluent wire-format JSON: `[0x00][schema_id 4B][JSON bytes]`; optional validation. | `schema-registry` |

The three Confluent formats take a `schema_registry` block (URL, optional basic auth, cache capacity, request timeout) — see the [`faucet-common-kafka`](https://crates.io/crates/faucet-common-kafka) README for the full `SchemaRegistryConfig`.

```yaml
value_format:
  type: confluent_avro
  schema_registry:
    url: http://localhost:8081
    auth:                  # optional basic auth (flat username/password)
      username: ${env:SR_USERNAME}
      password: ${env:SR_PASSWORD}
    cache_capacity: 1024   # default 1024
    request_timeout: 10    # seconds, default 10
```

## Record shape

Each Kafka message becomes one JSON object:

```json
{
  "key": "order-42",
  "value": { "id": 42, "status": "shipped", "amount": 99.95 },
  "topic": "orders",
  "partition": 2,
  "offset": 10483,
  "timestamp": 1747483200000,
  "headers": { "content-type": "application/json", "trace-id": "abc123" }
}
```

- `key` — the key decoded as UTF-8, or per `key_format` if set. `null` when the message carried no key.
- `value` — the decoded payload; shape depends on `value_format`.
- `topic` / `partition` / `offset` — provenance for the message within its partition.
- `timestamp` — milliseconds since the Unix epoch; `0` when the message had no timestamp.
- `headers` — a flat string→string object; non-UTF-8 values are base64-encoded; `{}` when none were set.

## Examples

### Confluent Cloud (SASL/PLAIN + JSON)

```yaml
source:
  type: kafka
  config:
    brokers: "pkc-xxxx.us-east-1.aws.confluent.cloud:9092"
    topics: ["payments"]
    group_id: faucet-payments
    auth:
      type: sasl_plain
      config:
        username: ${env:CC_API_KEY}
        password: ${env:CC_API_SECRET}
    value_format: { type: json }
    auto_offset_reset: earliest
    idle_timeout: 60
```

### Confluent Avro via Schema Registry

```yaml
source:
  type: kafka
  config:
    brokers: "localhost:9092"
    topics: ["users-avro"]
    group_id: faucet-users
    value_format:
      type: confluent_avro
      schema_registry:
        url: http://localhost:8081
    max_messages: 5000
    idle_timeout: 15
```

### Resumable continuous drain into Postgres

```yaml
pipeline:
  source:
    type: kafka
    config:
      brokers: "localhost:9092"
      topics: ["events", "audit"]   # joined into one stable state key
      group_id: faucet-warehouse
      value_format: { type: json }
      auto_offset_reset: earliest
      idle_timeout: 30
      batch_size: 5000
  sink:
    type: postgres
    config:
      connection_url: ${env:DATABASE_URL}
      table: kafka_events
  state:
    type: file
    config:
      path: ./.faucet-state
```

### Raw-bytes passthrough, skip undecodable messages

```yaml
source:
  type: kafka
  config:
    brokers: "localhost:9092"
    topics: ["raw-feed"]
    group_id: faucet-raw
    value_format: { type: bytes }   # base64 string in `value`
    on_decode_error: skip
    max_messages: 100000
    idle_timeout: 10
```

## Streaming & batching

The source overrides `Source::stream_pages`. Messages drained from the `StreamConsumer` are accumulated into an in-memory buffer and emitted as a `StreamPage` whenever:

1. **The buffer reaches `batch_size`** — yield a full page, reset the buffer, keep polling.
2. **The idle window flushes a partial buffer** — when the `idle_timeout` deadline fires with a non-empty buffer, emit it as a trailing page and continue.
3. **`max_messages` is reached or `Ctrl+C` is received** — emit the final partial page (if any) and exit.

Each emitted page carries a snapshot of the cumulative `(topic, partition) → next_offset` bookmark. The pipeline persists it through the configured `StateStore` **after** the sink confirms the write, so memory is bounded at one page and a crash between pages re-reads only the uncommitted page on resume.

**`batch_size = 0` — drain the entire run window.** The source accumulates **every** message produced by the run (until `max_messages` / `idle_timeout` fires) into a single page before yielding. This negates the streaming benefit and is intended only for tests or one-shot drains; production pipelines should use a finite `batch_size` so the state store advances with each successful sink write.

## Resume & state store

When a `StateStore` is wired in (via `state:` in YAML, or `Pipeline::with_state_store` in Rust) the source tracks durable offsets:

1. **Before the run**, the pipeline reads the stored bookmark and calls `apply_start_bookmark`. It is buffered in memory — no seeking happens yet.
2. **On partition assignment** (the rebalance callback, before any fetch), each assigned `(topic, partition)`'s bookmarked offset is injected *into* the assignment. Setting the offset as part of the assignment — rather than seeking after the first poll — means no pre-bookmark message is ever delivered, so a resume never duplicates.
3. **After the sink confirms a batch**, the pipeline persists the new bookmark — one `{topic, partition, offset}` entry per assigned partition, recording one past the highest committed offset.

The bookmark records an offset for **every assigned partition**, not just those that produced a message this run. An empty-this-run partition is recorded at the consumer's current position; if it were omitted, the next resume would fall back to `auto_offset_reset` (default `latest`) and silently **skip** records that arrived meanwhile. A partition that has *never* been assigned (e.g. added to the topic after the last run) honours `auto_offset_reset` on first encounter. A bookmarked partition that has not delivered yet in this run keeps its bookmarked offset — the watermark the assignment resolved only seeds partitions nothing else knows about.

**A bookmark that fell out of retention.** When a bookmarked offset is below the partition's log start (retention deleted those records while the pipeline was down), the source resumes at the log start and logs a `WARN` naming how many bookmarked records were deleted — it never lets librdkafka reset the partition to `auto_offset_reset` (default `latest`), which would also skip the still-retained backlog.

**Rebalances during a run.** Outside cluster member mode nothing is committed to the consumer group, so a rebalance after the first one (another consumer joining the same `group_id`, a session-timeout rejoin) re-seeks each re-assigned partition from the start bookmark advanced by what this run already delivered, instead of restarting it at `auto_offset_reset`.

**Transient broker errors.** Broker transport failures, "all brokers down", coordinator moves, request timeouts and a `max.poll.interval.ms` overrun are logged and the consumer keeps polling (librdkafka reconnects / rejoins on its own); any other consumer error fails the run. A page write plus flush that regularly outlasts `max.poll.interval.ms` (librdkafka default 300 s) costs a rejoin each time — raise it through `extra_client_config` (`max.poll.interval.ms: "900000"`) for slow sinks.

**State key format:**

```
kafka:{group_id}:{topic1}:{topic2}...
```

Topics are sorted alphabetically before joining, so the key is stable regardless of config order. They are joined with `:` (not `.`) because a topic name may legally contain `.`. So `group_id = "my-group"`, `topics = ["beta", "alpha"]` yields `kafka:my-group:alpha:beta`.

**Delivery semantics:** offsets are persisted only after the sink confirms, and on restart the consumer seeds the assignment with the bookmark before the first fetch. End-to-end this is **at-least-once** by default; the source also qualifies for effectively-once — see below.

## Effectively-once delivery

The Kafka source supports faucet-stream's `delivery: exactly_once` mode (the
**atomic-watermark** mechanism), because it satisfies the mechanism's source
requirement: partitions are immutable, ordered logs and **every** emitted page
carries a complete per-partition next-offset bookmark, so resuming from any
bookmark continues the record stream at exactly that position.

One subtlety is handled by the pipeline rather than the source: page
*boundaries* on replay can differ (an `idle_timeout` cut is timing-dependent),
so counting pages is not enough. The commit token an idempotent sink stores
embeds the page's offsets bookmark; on resume the pipeline recovers that exact
position from the sink's watermark and re-anchors the consumer there — nothing
is re-written, nothing is skipped.

Pair with any idempotent sink (`postgres`, `mysql`, `mssql`, `sqlite`,
`bigquery`, `iceberg`, `kafka`, `snowflake`, `redis`, `mongodb`) and a durable
state store:

```yaml
version: 1
name: kafka_to_postgres_eo
delivery: exactly_once

pipeline:
  source:
    type: kafka
    config:
      brokers: localhost:9092
      topics: [orders]
      group_id: faucet-orders
      idle_timeout: 30
  sink:
    type: postgres
    config:
      connection_url: postgres://writer:pass@localhost:5432/warehouse
      table_name: orders
      column_mapping: auto_map
  state:
    type: file
    config: { path: ./state }
```

In plain (non-member) mode the source never commits consumer-group offsets, so
the broker's group position can never run ahead of the durable bookmark. See
the [state cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/state.html#effectively-once-delivery)
for the full mechanism.

## Clustered consumption (Mode B, native consumer groups)

Under `faucet serve --cluster`, a top-level `shard: { count: N }` block distributes one Kafka pipeline across N cluster workers using **Kafka's native consumer-group assignment** ([#261](https://github.com/faucet-hq/faucet-stream/issues/261)) — each shard is a *membership slot* (one more consumer sharing the config's `group_id`), not a data slice. The broker assigns the topic's partitions across the members and rebalances onto survivors when a worker dies; the requested member count is capped at the subscription's total partition count.

In member mode (i.e. only when a cluster coordinator applies a shard — a plain `faucet run` is unchanged) the source additionally:

- **seeds the group with its starting position** for every assigned partition the group has no committed offset for (the bookmarked offset, or where `auto_offset_reset` starts), so a partition that migrates before the first durable commit resumes there rather than at `auto_offset_reset`;
- **commits offsets to the consumer group at durable page boundaries** — after the pipeline has written a page to the sink and persisted its bookmark, plus a synchronous commit at stream end — so a partition that migrates to another member resumes from the last durable position instead of `auto_offset_reset`;
- **defers bookmark seeks to the group's committed offsets** whenever those are ahead (another member may have durably advanced a partition past this member's bookmark); a bookmark *ahead* of the committed offset — the durable-write→commit crash window — still wins.

The boundary on membership change is **at-least-once**: a crash between a durable page and its commit makes the partition's next owner re-read that page. Pair with an upsert-mode or otherwise idempotent sink. Note `max_messages` applies per member (N members consume up to N × `max_messages` total); `idle_timeout` is the natural terminator for shared consumption. See the [cluster cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/cluster.html) for the full Mode B walkthrough.

## Config loading & schema introspection

Load from YAML/JSON or environment. Inspect the full JSON Schema with:

```bash
faucet schema source kafka
faucet validate pipeline.yaml
faucet preview pipeline.yaml --limit 5   # consume a few messages and print to stdout
```

A complete working example ships at [`cli/examples/kafka_to_jsonl.yaml`](https://github.com/faucet-hq/faucet-stream/blob/main/cli/examples/kafka_to_jsonl.yaml).

## Library usage

```rust
use faucet_core::Source;
use faucet_source_kafka::{KafkaSource, KafkaSourceConfig, KafkaValueFormat, OffsetReset};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let cfg = KafkaSourceConfig {
    brokers: "localhost:9092".into(),
    topics: vec!["orders".into()],
    group_id: "faucet-orders".into(),
    auth: Default::default(),                 // KafkaAuth::None
    value_format: KafkaValueFormat::Json,
    key_format: None,
    auto_offset_reset: OffsetReset::Earliest,
    max_messages: Some(10_000),
    idle_timeout: Some(std::time::Duration::from_secs(30)),
    poll_timeout: std::time::Duration::from_secs(1),
    session_timeout: std::time::Duration::from_secs(30),
    on_decode_error: Default::default(),      // fail
    extra_client_config: Default::default(),
    batch_size: 1000,
};
cfg.validate()?;

let records = KafkaSource::new(cfg).await?.fetch_all().await?;
println!("consumed {} messages", records.len());
# Ok(())
# }
```

For durable resume and incremental sink writes, drive it through `faucet_core::Pipeline` (or `run_stream`) with a `StateStore` rather than `fetch_all`.

## How it works

1. `new()` validates the config, builds the state key, and constructs the `StreamConsumer` **once** with the resolved librdkafka client config (auth + typed fields + `extra_client_config` overrides).
2. A rebalance callback seeds the partition assignment with bookmarked offsets before the first poll.
3. The consume loop polls with `poll_timeout`, decodes each message per `value_format` / `key_format`, and buffers it; it exits on `max_messages`, `idle_timeout`, or SIGINT.
4. Decoded messages are framed into `batch_size` pages and streamed to the pipeline, each page carrying the cumulative offset bookmark.

The v1 consume loop is single-threaded — one task polls one `StreamConsumer`. For higher throughput, partition the topic and run multiple `faucet` instances with the same `group_id`; Kafka assigns disjoint partition sets and they scale linearly. The downstream sink (database writes, object-store uploads) is usually the bottleneck, not the consume loop.

## Lineage dataset URI

`kafka://<first_broker>?topic=<topic1>,<topic2>` — e.g. `kafka://kafka.example.com:9092?topic=orders` (the first broker in `brokers`, all topics comma-joined).

## Feature flags

| Feature | Default | Effect |
|---------|---------|--------|
| `schema-registry` | off | Enables the Confluent Avro / Protobuf / JSON Schema value formats and `SchemaRegistryConfig` (pulls `reqwest`, `apache-avro`, `prost-reflect`, `jsonschema`, …). |

In the CLI / umbrella, enable the connector with `source-kafka`, and the registry formats with `kafka-schema-registry`.

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| `Config: at least one of max_messages or idle_timeout must be set` | A Kafka source has no stop condition. Set `max_messages`, `idle_timeout`, or both. |
| Run consumes nothing and exits immediately | `auto_offset_reset` defaults to `latest`, so a fresh group skips existing messages. Set `auto_offset_reset: earliest` to read from the start. |
| Run hangs until `idle_timeout` on an empty topic | Expected — the consumer waits `idle_timeout` seconds for new messages before exiting. Lower `idle_timeout` for faster turnaround. |
| Resume re-reads or skips records | Ensure a non-`memory` `state:` block is configured and the `group_id` + `topics` are unchanged (the state key is derived from both). A changed `group_id` is a new bookmark. |
| `Source` error / connection refused / timeout | Broker unreachable or wrong `brokers`. `faucet doctor` runs a non-consuming metadata probe to validate connectivity + auth without reading messages. |
| SASL / SSL handshake failure | Wrong `auth` type or credentials, or a `key_path` / `cert_path` / `ca_path` that doesn't exist (paths are validated at config time). Confirm the broker's `security.protocol` matches. |
| Messages fail to decode | The `value_format` doesn't match the wire data (e.g. `json` against Avro). Match the producer's format; use `on_decode_error: skip` to drop bad messages instead of aborting. |
| `confluent_protobuf` fails with "only supports single-message schemas" | The record's `message_indexes` names a message other than the first in the `.proto`. Only single-message schemas are supported; use `bytes` and decode downstream. |
| Confluent format rejected as unknown `type` | Build with the `schema-registry` feature (CLI: `kafka-schema-registry`). |
| Throughput lower than expected | Partition the topic and run multiple instances with the same `group_id`, and/or tune `fetch.max.bytes` / `max.partition.fetch.bytes` via `extra_client_config`. |

## See also

- [Kafka source reference](https://faucet-hq.github.io/faucet-stream/reference/connectors.html) — capability matrix.
- [State & resume cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/state.html) — bookmarks and delivery semantics.
- [`faucet-sink-kafka`](https://crates.io/crates/faucet-sink-kafka) — produce records to Kafka topics.
- [`faucet-common-kafka`](https://crates.io/crates/faucet-common-kafka) — shared auth modes, value formats, Schema Registry client, and policy enums.

## Source lag

`Source::lag` (#733) reports **events**: each partition's high watermark minus the next offset to read (the bookmark; with no position, where `auto_offset_reset` starts), summed over the subscribed topics. The pipeline polls it on the first page, at most every
15 s after, and when the run ends, exporting `faucet_source_lag_*{pipeline,row,connector}`;
`faucet status --probe` and `faucet doctor` ask it from the stored bookmark,
and `sla.max_lag_*` thresholds turn it into an SLA. A failing lag query is
logged once and reported as no lag — it never fails a run.

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.
