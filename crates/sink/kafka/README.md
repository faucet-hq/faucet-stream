# faucet-sink-kafka

[![Crates.io](https://img.shields.io/crates/v/faucet-sink-kafka.svg)](https://crates.io/crates/faucet-sink-kafka)
[![Docs.rs](https://docs.rs/faucet-sink-kafka/badge.svg)](https://docs.rs/faucet-sink-kafka)
[![MSRV](https://img.shields.io/crates/msrv/faucet-sink-kafka.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-sink-kafka.svg)](https://github.com/faucet-hq/faucet-stream#license)

Apache **Kafka** producer sink for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. Publishes each record to one or more Kafka topics over [`rdkafka`](https://crates.io/crates/rdkafka)'s `FutureProducer`, with an idempotent producer, configurable compression, per-record topic/key/partition/header routing, and `QueueFull` retry.

Reach for it when you want to land any faucet-stream source — a REST API, a database, a CDC stream, a file — onto Kafka with one declarative config and no glue code. Sends inside each batch fly concurrently through a `FuturesUnordered` window, so a single pipeline saturates the producer instead of round-tripping one message at a time. This connector reports metrics under the observability label `connector="kafka"`.

## Feature highlights

- **Concurrent batched sends** — each `write_batch` enqueues its records into a `FuturesUnordered` of `send_result` futures so multiple produce requests are in flight at once, bounded by `min(max_in_flight, batch_size)`.
- **Idempotent producer by default** — `idempotent: true` sets `enable.idempotence`, so retried produce calls within a session never duplicate. Combined with `acks: all`, that's no-loss, no-duplicate delivery per produce call.
- **`QueueFull` retry** — when librdkafka's send queue is full, the sink backs off (`queue_full_backoff`) and retries up to `queue_full_max_retries` before surfacing the error.
- **Multi-topic routing** — send everything to one `fixed` topic, or extract the destination topic from each record with a JSONPath (`from_path`).
- **Per-record key / partition / headers** — JSONPath-driven `key_path`, `partition_path` and `headers_path`, with a configurable `on_key_error` policy (`fail` / `skip` / `round_robin`).
- **Producer compression** — `none` / `gzip` / `snappy` / `lz4` / `zstd`.
- **Value & key encoding** — JSON, raw UTF-8 string, or base64 bytes out of the box; Confluent **Avro / Protobuf / JSON Schema** behind the `schema-registry` feature.
- **SASL / SSL auth** — `none`, `sasl_plain`, `sasl_scram` (SHA-256/512), `ssl` (mTLS), `tls` (server-authenticated TLS), and `sasl_ssl`, shared verbatim with the Kafka **source** via [`faucet-common-kafka`](https://crates.io/crates/faucet-common-kafka).
- **Producer built once** — the authenticated `FutureProducer` is constructed in `new()` and reused for every batch.

## Installation

```bash
# As a library:
cargo add faucet-sink-kafka

# In the CLI (opt-in connector feature):
cargo install faucet-cli --features sink-kafka

# With Confluent Schema Registry encoding (Avro / JSON Schema):
cargo install faucet-cli --features sink-kafka,kafka-schema-registry
```

## Quick start

```yaml
# pipeline.yaml — faucet run pipeline.yaml
version: 1
pipeline:
  source:
    type: rest
    config:
      base_url: https://jsonplaceholder.typicode.com
      path: /users
      pagination: { type: none }
  sink:
    type: kafka
    config:
      brokers: localhost:9092
      topic: { type: fixed, name: users }
      value_format: { type: json }
      key_path: $.id          # use the record's "id" field as the message key
      on_key_error: round_robin
      compression: zstd
      acks: all
      idempotent: true
```

```bash
faucet run pipeline.yaml
```

## Configuration reference

All fields are keys under `sink.config`.

### Core

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `brokers` | string | — *(required)* | Comma-separated bootstrap broker addresses, e.g. `"broker1:9092,broker2:9092"`. |
| `topic` | `KafkaSinkTopic` | — *(required)* | Topic routing strategy. See [Topic routing](#topic-routing). |
| `auth` | `KafkaAuth` | `{ type: none }` | Authentication / transport security. See [Authentication](#authentication). |

### Value & key encoding

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `value_format` | `KafkaValueFormat` | `{ type: json }` | How each record is encoded into the message value bytes. See [Encoding](#encoding). |
| `key_format` | `KafkaValueFormat` | *(unset)* | Encoding applied to the extracted key value. When unset, the key is written as UTF-8 bytes. |
| `value_schema` | string | *(unset)* | Schema text (Avro `.avsc` JSON / `.proto` / JSON Schema) for the value, **required** when `value_format` is a Confluent format. Registered under the `{topic}-value` subject on first use; ignored otherwise. |
| `key_schema` | string | *(unset)* | Schema text for the key, **required** when `key_format` is a Confluent format. Registered under `{topic}-key`; ignored otherwise. |

### Routing

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `key_path` | string | *(unset)* | JSONPath into each record to extract the message key. Absent → no key (round-robin partitioning). |
| `partition_path` | string | *(unset)* | JSONPath to extract the target partition number (integer). Absent → librdkafka chooses. |
| `headers_path` | string | *(unset)* | JSONPath to a flat object whose entries become Kafka message headers (values stringified). Applied on both the at-least-once and transactional produce paths. A JSON `null` becomes a **valueless** header rather than the string `"null"`. A path resolving to nothing yields no headers; one resolving to a non-object fails the batch (same strictness as `partition_path`). The exactly-once commit-token record never carries these. |
| `on_key_error` | `"fail" \| "skip" \| "round_robin"` | `"fail"` | What to do when `key_path` / `partition_path` extraction fails. See [Key error policy](#key-error-policy). |

### Reliability

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `acks` | `"none" \| "leader" \| "all"` | `"all"` | Broker acknowledgment level. Must be `"all"` when `idempotent: true`. |
| `idempotent` | bool | `true` | Enable the idempotent producer (`enable.idempotence = true`). Requires `acks: all`. |
| `message_timeout` | int (seconds) | `30` | Per-message delivery timeout. Raise for high-latency brokers. |
| `max_in_flight` | int | `100` | Max concurrent produce requests. Must be ≥ 1. Set to `1` (with `idempotent: true`) for strict per-partition ordering. |
| `queue_full_backoff` | int (seconds) | `0` *(0.1s)* | Pause between retries on `QueueFull`. The default is **100 ms** (sub-second); the config field is whole-seconds, so use `extra_client_config` only if you need finer control elsewhere. |
| `queue_full_max_retries` | int | `3` | Max `QueueFull` retries before the error surfaces. |

### Batching & throughput

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `batch_size` | int | `1000` | In-flight `FuturesUnordered` send-window cap per `write_batch`, and the seed for librdkafka's `queue.buffering.max.messages`. **`0` = no cap** (bounded only by `max_in_flight`). See [Streaming & batching](#streaming--batching). |
| `linger` | int (seconds) | `0` *(5ms)* | Time the producer waits to accumulate a batch before flushing. The default is **5 ms**; for other sub-second values set `linger.ms` via `extra_client_config`. |
| `compression` | `"none" \| "gzip" \| "snappy" \| "lz4" \| "zstd"` | `"none"` | Producer-side compression codec. See [Compression](#compression). |
| `extra_client_config` | object (string→string) | `{}` | Raw librdkafka client properties. **Overrides** anything set by `auth` or the typed fields above. |

### Effectively-once

These fields apply only under `delivery: exactly_once` (ignored otherwise). See [Effectively-once delivery](#effectively-once-delivery).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `exactly_once` | block | *(unset)* | Exactly-once knobs, grouped — see below. Only consulted under `delivery: exactly_once`. |
| `exactly_once.transactional_id_prefix` | string | `"faucet"` | Namespace prefix for the producer's auto-derived `transactional.id` (`"{prefix}.{sanitized_scope}"`). Set it to isolate transactional ids across clusters/environments that share a pipeline-scope namespace; with an explicit prefix the commit token is also keyed `"{prefix}.{scope}"` on the side-topic, so environments never read each other's watermark (a pre-prefix bare-scope token is read until the first prefixed write). |
| `exactly_once.commit_token_topic` | string | `"__faucet_commit_token"` | Compacted side-topic that holds one commit-token record per pipeline scope. Auto-created with `cleanup.policy=compact` if absent. |
| `exactly_once.commit_token_topic_partitions` | int | `1` | Partition count used when auto-creating the commit-token topic. Must be ≥ 1. |
| `exactly_once.commit_token_topic_replication` | int | `-1` | Replication factor used when auto-creating the commit-token topic. `-1` means "use the broker default". |

The same four keys are still accepted flat at the config top level (`transactional_id_prefix`, `commit_token_topic`, …) — **deprecated** since #654, and superseded wholesale when an `exactly_once:` block is present.

## Topic routing

`topic` is a `{ type, … }` discriminated value with two strategies:

```yaml
# fixed — every record goes to the same topic
topic:
  type: fixed
  name: orders
```

```yaml
# from_path — topic extracted per record via JSONPath
topic:
  type: from_path
  path: $.dest
```

For `from_path`, the path must resolve to a non-empty string for **every** record — a missing or non-string value is always fatal. `on_key_error` does **not** apply to topic routing.

## Encoding

`value_format` (and the optional `key_format`) is a `{ type, … }` value:

| `type` | Feature | Description |
|--------|---------|-------------|
| `json` | — | Serialize the record as a JSON document. **Default.** |
| `raw_string` | — | Write the record's string representation as UTF-8 bytes. |
| `bytes` | — | Expect a base64-encoded string; decode and write the raw bytes. |
| `confluent_avro` | `schema-registry` | Confluent wire-format Avro (`[0x00][be u32 schema_id][Avro]`). Subject `{topic}-value`. |
| `confluent_protobuf` | `schema-registry` | Confluent wire-format Protobuf against the subject's registered schema (single-message schemas only; schema references are not resolved). |
| `confluent_json_schema` | `schema-registry` | Confluent wire-format JSON (`[0x00][be u32 schema_id][JSON]`). |

The three Confluent formats need the `schema-registry` feature (CLI: `kafka-schema-registry`) and a `schema_registry` block. On the **sink** side you must also supply the schema text to register and encode against — `value_schema` for the value, `key_schema` for the key — or the config is rejected at load time. See the [`faucet-common-kafka` README](https://crates.io/crates/faucet-common-kafka) for `SchemaRegistryConfig` options.

```yaml
sink:
  type: kafka
  config:
    brokers: localhost:9092
    topic: { type: fixed, name: events }
    value_format:
      type: confluent_avro
      schema_registry: { url: http://localhost:8081 }
    value_schema: '{"type":"record","name":"Event","fields":[{"name":"id","type":"long"}]}'
```

### Avro `bytes` / `fixed` fields

A JSON string written to an Avro `bytes` or `fixed` field is read as **standard base64** by default (`"yv4="` → the two bytes `0xca 0xfe`), the canonical binary rendering every faucet source emits — the Kafka source decodes those fields to it — so records read from one topic re-encode unchanged; a string that is not base64 fails the record. Set `bytes_encoding: hex` to read the lowercase hex the source emitted before #885 (`"cafe"` → `0xca 0xfe`), or `bytes_encoding: utf8` on the `confluent_avro` format to write a string's UTF-8 bytes instead (`"cafe"` → four bytes) — the behaviour of releases before faucet-common-kafka 1.1.1, which a pipeline feeding text into a `bytes` field relied on. A JSON array of byte numbers (`[202, 254]`) is accepted under either setting.

```yaml
    value_format:
      type: confluent_avro
      schema_registry: { url: http://localhost:8081 }
      bytes_encoding: utf8
```

## Authentication

`auth` uses the shared `KafkaAuth` enum (the project-wide `{ type, config }` discriminated shape):

| `type` | `config` | Use when |
|--------|----------|----------|
| `none` | — | Plaintext brokers (default; `security.protocol = PLAINTEXT`). |
| `sasl_plain` | `username`, `password` | SASL/PLAIN over plaintext (e.g. dev clusters). |
| `sasl_scram` | `mechanism` (`sha256`/`sha512`), `username`, `password` | SASL/SCRAM over plaintext. |
| `ssl` | `ca_path`, `cert_path`, `key_path` (+ optional `key_password`) | Mutual TLS. Paths are validated to exist at config time. |
| `tls` | `ca_path?` | TLS with server authentication only (no client certificate); the system trust store unless `ca_path` is set. |
| `sasl_ssl` | `sasl` (a `sasl_plain`/`sasl_scram` block), `ssl` (a `tls`, `ssl` or `none` block) | SASL over TLS — Confluent Cloud, Amazon MSK. |

```yaml
# SASL/PLAIN
auth:
  type: sasl_plain
  config:
    username: ${env:KAFKA_USERNAME}
    password: ${env:KAFKA_PASSWORD}
```

```yaml
# SASL/SCRAM-SHA-512 over TLS without client certificates (managed clusters)
auth:
  type: sasl_ssl
  config:
    sasl:
      type: sasl_scram
      config:
        mechanism: sha512
        username: ${env:KAFKA_USERNAME}
        password: ${env:KAFKA_PASSWORD}
    ssl:
      type: tls
      config: {}          # or { ca_path: /etc/kafka/certs/ca.pem }
```

```yaml
# SASL over mutual TLS
auth:
  type: sasl_ssl
  config:
    sasl:
      type: sasl_plain
      config: { username: ${env:KAFKA_USERNAME}, password: ${env:KAFKA_PASSWORD} }
    ssl:
      type: ssl
      config:
        ca_path: /etc/kafka/certs/ca.pem
        cert_path: /etc/kafka/certs/client.pem
        key_path: /etc/kafka/certs/client.key
```

Use `${env:VAR}` / `${secret:…}` interpolation so credentials never land in the YAML file. The full auth reference lives in the [`faucet-common-kafka` README](https://crates.io/crates/faucet-common-kafka).

## Key error policy

`on_key_error` controls what happens when `key_path` or `partition_path` extraction fails (path absent, resolving to JSON `null`, type mismatch, or invalid partition). A `null` key is never sent as the literal key `"null"`, which would put every such record on one partition and, on a compacted topic, keep only the last:

| Value | Behaviour |
|-------|-----------|
| `fail` *(default)* | Abort the batch with `FaucetError::Sink`. |
| `skip` | Drop the record, log a `WARN`, continue. The dropped records are reported as *skipped* to the pipeline's row accounting — on the exactly-once path too (`write_batch_idempotent_reported`) — so the run stays balanced. |
| `round_robin` | Send the record with no key; librdkafka assigns the partition. Record is kept. |

It does **not** apply to `from_path` topic failures (including a path resolving to `null`) — those are always fatal.

## Compression

| Value | Notes |
|-------|-------|
| `none` *(default)* | No compression. |
| `gzip` | Good ratio, higher CPU. |
| `snappy` | Balanced speed and ratio. |
| `lz4` | Fast encode/decode — recommended for throughput-sensitive pipelines. |
| `zstd` | Best ratio at moderate CPU. Requires broker ≥ 2.1. |

## Examples

### Fan out to many topics by a record field

```yaml
sink:
  type: kafka
  config:
    brokers: broker1:9092,broker2:9092
    topic:
      type: from_path
      path: $.event_type      # each record routed to its own topic
    value_format: { type: json }
    key_path: $.entity_id
    compression: lz4
```

### Strict per-partition ordering

```yaml
sink:
  type: kafka
  config:
    brokers: localhost:9092
    topic: { type: fixed, name: ledger }
    key_path: $.account_id
    idempotent: true
    acks: all
    max_in_flight: 1          # at most one send on the wire
    batch_size: 1
```

### Throughput over durability (fire-and-forget)

```yaml
sink:
  type: kafka
  config:
    brokers: localhost:9092
    topic: { type: fixed, name: telemetry }
    value_format: { type: json }
    idempotent: false
    acks: leader              # or "none" for no broker ack
    compression: zstd
    batch_size: 5000          # wider send window
    max_in_flight: 200
    linger: 0                 # 0s field → 5ms default; raise linger.ms via extra_client_config
```

### Key and partition from the record

```yaml
sink:
  type: kafka
  config:
    brokers: localhost:9092
    topic: { type: fixed, name: orders }
    key_path: $.order_id        # string → message key
    partition_path: $.shard     # integer → target partition
    on_key_error: skip
```

## Streaming & batching

The sink is driven from the streaming pipeline via `Sink::write_batch`, once per `StreamPage` the source emits. Within a single call, records are produced through a `FuturesUnordered` of `send_result` futures so multiple sends fly concurrently. `batch_size` bounds how many futures are in flight at any moment:

- **Effective in-flight cap = `min(max_in_flight, batch_size)`** when `batch_size > 0`.
- **Effective in-flight cap = `max_in_flight`** when `batch_size = 0` (the "no batching" sentinel — every record is enqueued immediately).

When `batch_size > 0`, `KafkaSink::new` also sets librdkafka's `queue.buffering.max.messages` to `batch_size` so the broker-side buffer can hold one full send window (avoiding spurious `QueueFull` rejections). `extra_client_config` takes precedence — set a smaller `queue.buffering.max.messages` there to force backpressure. `QueueFull` retry semantics (`queue_full_backoff` / `queue_full_max_retries`) are independent of `batch_size`.

Reach for `batch_size: 0` when a source emits its whole result set as a single page (small lookup tables, one-shot drains) and you want the entire write to fire in parallel without the extra cap.

> **Two delivery tiers.** `idempotent: true` (the default) de-duplicates retried produce calls *within a producer session* — fast, but it does not survive a faucet-stream crash/resume. For faucet-stream's cross-run **effectively-once** delivery (`delivery: exactly_once`), the sink runs a *transactional* producer that writes each page's records and a commit-token record into a compacted side-topic in one Kafka transaction — see [Effectively-once delivery](#effectively-once-delivery) below. The sink is **append-only** either way: there is no `write_mode` / upsert / delete (Kafka has no row identity to update).

## Effectively-once delivery

`KafkaSink` implements `Sink::supports_idempotent_writes` (returns `true`) and the two companion hooks:

- `write_batch_idempotent(records, scope, token)` — a **transactional** producer writes the page's records **and** a commit-token record for `scope` into the compacted side-topic (`commit_token_topic`) inside a single Kafka transaction, so the data and the watermark commit atomically or not at all.
- `last_committed_token(scope)` — reads the latest token for `scope` back from the side-topic so the pipeline skips already-committed pages on resume.

The producer's `transactional.id` is auto-derived from the pipeline scope — stable across restarts and unique per matrix row — so a restart fences any prior producer. The side-topic is **auto-created** with `cleanup.policy=compact` if it does not exist (so only the latest token per scope is retained); its metadata is read first, so a principal without `CREATE` permission can use a pre-created side-topic.

A transaction enqueues the whole page plus its token before awaiting anything, so the transactional producer's queue is not capped at `batch_size` (librdkafka's 100 000-message default, or `batch_size + 1` when larger; `extra_client_config` still wins), and a full queue waits for room for up to `message_timeout` instead of `queue_full_max_retries`.

Scopes share the side-topic, so another pipeline's open transaction holds its Last Stable Offset below this scope's newer tokens. `last_committed_token` waits until every transaction open when it started has committed or aborted (bounded by `max(message_timeout, 60 s) + 10 s`, then the run fails) rather than resuming from an older token. Its own scope's transaction left open by a crash is fenced and aborted first (the transactional producer is initialised before the read), so it never holds the wait. A peer's transaction lasts up to the peer's transaction timeout, so give every pipeline sharing a commit-token topic the same or a lower `message_timeout` — or its own `commit_token_topic`. Downstream consumers of the destination topic should set `isolation.level=read_committed` so they only ever observe committed transactions.

To use effectively-once delivery, set `delivery: exactly_once` and pair this sink with a CDC source (`postgres-cdc`, `mysql-cdc`, `mongodb-cdc`) plus a `state:` block. A DLQ is not permitted in effectively-once mode. All four requirements are validated at config-load time (`faucet validate`) before any run starts.

```yaml
version: 1
name: pg_cdc_to_kafka_eo
delivery: exactly_once
pipeline:
  source:
    type: postgres-cdc
    config:
      connection_url: ${env:SOURCE_PG_URL}
      slot_name: faucet_kafka_eo
      publication_name: faucet_pub
      create_slot_if_missing: true
  sink:
    type: kafka
    config:
      brokers: localhost:9092
      topic: { type: fixed, name: orders_cdc }
      value_format: { type: json }
      # effectively-once: transactional producer + compacted watermark side-topic
      # (auto-created). transactional.id is auto-derived from the pipeline scope.
      exactly_once:
        commit_token_topic: __faucet_commit_token
  state:
    type: file
    config:
      path: ./.faucet-state/pg_cdc_to_kafka_eo
```

The `exactly_once:` block (`transactional_id_prefix`, `commit_token_topic`, `commit_token_topic_partitions`, `commit_token_topic_replication`) tunes the transactional id namespace and the side-topic. See the [effectively-once delivery cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/state.html#effectively-once-delivery) for the cross-connector picture, and the runnable [`cli/examples/postgres_cdc_to_kafka_exactly_once.yaml`](https://github.com/faucet-hq/faucet-stream/blob/main/cli/examples/postgres_cdc_to_kafka_exactly_once.yaml).

## Config loading & schema

Load from YAML/JSON or environment. Inspect the full JSON Schema with:

```bash
faucet schema sink kafka
```

Validate and dry-run a config before sending anything:

```bash
faucet validate pipeline.yaml
faucet preview pipeline.yaml --limit 5
```

## Library usage

```rust
use faucet_core::Sink;
use faucet_sink_kafka::{KafkaSink, KafkaSinkConfig, KafkaSinkTopic};
use faucet_common_kafka::{KafkaAuth, KafkaValueFormat};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let cfg = KafkaSinkConfig {
    brokers: "localhost:9092".into(),
    topic: KafkaSinkTopic::Fixed { name: "orders".into() },
    auth: KafkaAuth::None,
    value_format: KafkaValueFormat::Json,
    key_path: Some("$.order_id".into()),
    ..serde_json::from_value(serde_json::json!({
        "brokers": "localhost:9092",
        "topic": { "type": "fixed", "name": "orders" }
    }))?
};
cfg.validate()?;

let sink = KafkaSink::new(cfg).await?;
let n = sink.write_batch(&[serde_json::json!({ "order_id": "A1", "total": 42 })]).await?;
sink.flush().await?;
println!("produced {n} records");
# Ok(())
# }
```

In a full pipeline, wire the sink into `faucet_core::Pipeline` (or `run_stream`) with any `Source`.

## How it works

1. `new()` resolves `KafkaAuth`, applies compression / acks / idempotence / timeout / `extra_client_config`, and builds the `FutureProducer` **once**.
2. Each `write_batch` resolves the topic, key, partition and headers per record (JSONPath), encoding the value via `value_format`.
3. Records are enqueued into a `FuturesUnordered` up to the effective in-flight cap; `QueueFull` rejections back off and retry.
4. `flush()` blocks until all in-flight deliveries are acknowledged (or `message_timeout` elapses) before the pipeline advances the bookmark.

### How it is built

The sink runs on the core sink driver (RFC 0014): `KafkaDriver` implements
`faucet_core::driver::SinkDriver` — produce a page, flush the producer — and
`CommitTokenDriver` — the page and its commit-token record in one producer
transaction, and the token read back from the compacted side-topic — and
`DriverSink` derives the exactly-once capability from that group. It is not
on the messaging family's `MessageDriver`: encoding is asynchronous (a schema
registry lookup), a record dropped by `on_key_error: skip` is reported as
skipped rather than failed, and a record that cannot be encoded fails the
whole page and aborts its transaction — none of which the per-message
`MessageSinkDriver` expresses. The public type and config are unchanged.

## Lineage dataset URI

`kafka://<brokers>?topic=<topic>` for a fixed topic, or `kafka://<brokers>?topic=(from_path:<path>)` for dynamic routing — e.g. `kafka://kafka.example.com:9092?topic=orders`.

## Feature flags

| Feature | Default | Enables |
|---------|---------|---------|
| `schema-registry` | off | Confluent Avro / Protobuf / JSON Schema value/key formats + `SchemaRegistryConfig` (via `faucet-common-kafka/schema-registry`). In the CLI / umbrella this is the `kafka-schema-registry` feature. |

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| `kafka sink: idempotent=true requires acks=all` | `idempotent` defaults to `true`, which forces `acks: all`. Either keep `acks: all` or set `idempotent: false` to use `acks: leader` / `none`. |
| `kafka sink: max_in_flight must be at least 1` | `max_in_flight` is `0`. Set it to ≥ 1. |
| `kafka sink: value_schema is not set` | A Confluent `value_format` was selected without `value_schema`. Supply the schema text (and likewise `key_schema` for a Confluent `key_format`). |
| Confluent format compiles but won't build | The `schema-registry` feature isn't enabled. Install with `--features sink-kafka,kafka-schema-registry`. |
| `confluent_protobuf` encodes against the wrong message, or fails to compile the schema | Only single-message schemas are supported: the first `message` in the subject's `.proto` is used, and a schema that imports others through Schema Registry references does not compile (references are not resolved). Use `confluent_avro`, `confluent_json_schema`, or `bytes` with an external encoder. |
| `QueueFull` errors surface despite retries | The producer queue is saturating faster than the broker drains. Raise `queue_full_max_retries` / `queue_full_backoff`, lower `batch_size`/`max_in_flight`, or enable `compression`. |
| `from_path` routing aborts mid-batch | A record's topic path was missing or non-string. `from_path` is always fatal — ensure every record carries the field, or use a `fixed` topic. |
| Records dropped unexpectedly | `on_key_error: skip` silently drops records whose key/partition extraction fails. Use `fail` to surface the error or `round_robin` to keep them keyless. |
| Auth handshake fails / `SSL` errors | Verify `brokers` matches the listener's advertised name and that `ca_path`/`cert_path`/`key_path` exist and match the cluster. For managed clusters use `sasl_ssl`. |
| Messages produced out of order | Concurrent sends reorder under retries. For strict order set `max_in_flight: 1`, `batch_size: 1`, `idempotent: true`. |

## See also

- [`faucet-source-kafka`](https://crates.io/crates/faucet-source-kafka) — consume records from Kafka topics.
- [`faucet-common-kafka`](https://crates.io/crates/faucet-common-kafka) — shared auth modes, value formats, schema-registry client, and policy enums.
- [Connectors reference](https://faucet-hq.github.io/faucet-stream/reference/connectors.html) — capability matrix.
- [Configuration reference](https://faucet-hq.github.io/faucet-stream/reference/config.html) — the full pipeline-config grammar.
- A complete working example: [`cli/examples/rest_to_kafka.yaml`](https://github.com/faucet-hq/faucet-stream/blob/main/cli/examples/rest_to_kafka.yaml).

## Batch atomicity

What a failed write leaves behind (#737): **best-effort** — each record is its own send. `on_batch_error: dlq_all`
is refused on a best-effort configuration unless the `dlq:` block sets
`allow_duplicates_on_dlq_all: true` (a DLQ replay would write the rows that
already landed a second time). See
[batch atomicity](https://faucet-hq.github.io/faucet-stream/cookbook/dlq.html#batch-atomicity-and-dlq_all).

## Capabilities

Declares `write_mode: append` and idempotent (exactly-once) writes. Under `value_format: confluent_avro` (the `schema-registry` feature) it declares that it drops record fields the Avro schema lacks, so `on_drift: fail` is refused at load time. `faucet list --json` prints the declaration.

## Timeouts, cancellation and retries

Every produced record's delivery, the `QueueFull` backoff, the transaction calls (`init`, `commit`) and the commit-token read run through the pipeline's `CallContext` (#880): the run's deadline ends a delivery that an unreachable or stalled cluster never confirms, and a `QueueFull` wait, promptly; a commit cut short is aborted. Each counts as one round trip (`produce`, `init`, `commit`, `metadata`, `read_token`) in `faucet_sink_roundtrips_total`, and Schema Registry requests run under the same context (`schema`, `register`). The sink never re-sends a delivered-or-unknown record itself.

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.
