# faucet-lineage

[![Crates.io](https://img.shields.io/crates/v/faucet-lineage.svg)](https://crates.io/crates/faucet-lineage)
[![Docs.rs](https://docs.rs/faucet-lineage/badge.svg)](https://docs.rs/faucet-lineage)
[![MSRV](https://img.shields.io/crates/msrv/faucet-lineage.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-lineage.svg)](https://github.com/faucet-hq/faucet-stream#license)

[OpenLineage](https://openlineage.io/) event emission for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. Every pipeline run emits OpenLineage `RunEvent`s describing the job, the run, its input/output datasets, their inferred schemas, and column-level lineage — to an HTTP endpoint (e.g. [Marquez](https://marquezproject.ai/)), a local JSON Lines file, or a Kafka topic.

Reach for it when you want your faucet-stream pipelines to show up in a lineage / data-catalog tool automatically — no per-connector instrumentation, no code in your data path. The `faucet` CLI wires it from a single top-level `lineage:` block; the types here are also usable directly by library callers via [`LineageEmitter`].

**OpenLineage spec version: 2.0.2.** Every event carries the pinned schema URL `https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent`.

## Feature highlights

- **Automatic lifecycle events** — `START` / `RUNNING` / `COMPLETE` / `ABORT` / `FAIL` emitted around every pipeline run, with per-event toggles.
- **Three transports** — HTTP (with optional bearer auth), local JSON Lines file, or Kafka — all using the project-wide `{ type, config }` shape.
- **Schema facets** — inferred `(field, type)` dataset schemas attached to inputs/outputs on terminal events, derived from a bounded record sample.
- **Deterministic column-level lineage** — per-output-field input edges folded over the declared transform chain, emitted only when the whole chain is mappable (never fabricated).
- **Parent-job linkage** — tie runs to an orchestrator job (Airflow, Dagster, …) via `parent_job`.
- **Emission never fails a run** — a broken lineage backend is logged and counted, then dropped; it can never abort or stall a pipeline.
- **Cheap to enable** — emission work happens in the CLI layer; the hot `run_stream` loop is untouched.

## Installation

```bash
# As a library:
cargo add faucet-lineage

# In the CLI (opt-in feature — enables the top-level `lineage:` block):
cargo install faucet-cli --features lineage

# Add the Kafka transport:
cargo install faucet-cli --features lineage-kafka
```

`lineage` is a **CLI-only** feature and is **not** in the CLI `default` build — it is included in `full`. The `lineage` CLI feature enables HTTP + file transports; `lineage-kafka` additionally enables the Kafka transport (pulls in `rdkafka`).

## Quick start

Add a top-level `lineage:` block to any pipeline config. The pipeline below pulls from PostgreSQL, shapes records, and lands them in BigQuery, emitting OpenLineage events to a Marquez endpoint on the way:

```yaml
# pipeline.yaml — faucet run pipeline.yaml
version: 1
name: orders_load

lineage:
  namespace: prod.warehouse
  transport:
    type: http
    config:
      url: ${env:MARQUEZ_URL}   # e.g. http://localhost:5000/api/v1/lineage
  include_schema_facet: true
  include_column_lineage: true

pipeline:
  source:
    type: postgres
    config:
      connection_url: postgres://user:pass@localhost/app
      query: SELECT id, created_at, customer_email FROM orders
  transforms:
    - type: rename_field
      config:
        fields:
          customer_email: contact_email
  sink:
    type: file
    config:
      path: ./orders.jsonl
```

```bash
faucet run pipeline.yaml
```

A full end-to-end example lives at [`cli/examples/postgres_to_bigquery_with_lineage.yaml`](https://github.com/faucet-hq/faucet-stream/blob/main/cli/examples/postgres_to_bigquery_with_lineage.yaml).

## OpenLineage event types

faucet-lineage emits standard OpenLineage `RunEvent`s. The pipeline's lifecycle maps to event types as follows; each can be toggled independently via `emit_on`:

| Event | When | Default | Carries terminal facets? |
|-------|------|---------|--------------------------|
| `START` | Just before the run begins. | on | no |
| `RUNNING` | Periodic heartbeat while the run is in flight (every `heartbeat_interval`). | **off** | no |
| `COMPLETE` | The run finished successfully. | on | yes |
| `ABORT` | The run was cancelled. | on | yes |
| `FAIL` | The run errored. | on | yes |

"Terminal facets" (schema + column-lineage) are attached only to the terminal events (`COMPLETE` / `ABORT` / `FAIL`), because they depend on the record sample observed during the run.

## Configuration reference

The `lineage:` block deserializes into [`LineageConfig`]. Unknown top-level fields are rejected.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | enum | `openlineage` | Lineage format. Only `openlineage` exists in v1. |
| `namespace` | string | — *(required)* | OpenLineage namespace for the emitted job and datasets. |
| `transport` | `Transport` | — *(required)* | Where events are sent — see [Transports](#transports). |
| `job_name` | string | `"${name}::${row_id}"` | Job-name template; `${name}` / `${row_id}` / `${now.*}` resolve per matrix row at run time. |
| `parent_job` | `ParentJob` | *(unset)* | Optional orchestrator linkage — see [Parent job](#parent-job). |
| `include_schema_facet` | bool | `false` | Emit inferred `(field, type)` dataset schema facets on input/output datasets. |
| `include_column_lineage` | bool | `false` | Emit column-level lineage facets when the transform chain is mappable. |
| `include_source_code_facet` | bool | `false` | Emit the resolved config body as a `SourceCode` job facet. **Off by default — the resolved config may contain secrets; enabling logs a warning.** |
| `emit_on` | `EmitOn` | see below | Which lifecycle events to emit — see [Emit toggles](#emit-toggles-emit_on). |
| `sample_records` | int | `100` | Max records sampled for schema / column-lineage facets. |
| `heartbeat_interval` | int (seconds) | `30` | Interval between `RUNNING` heartbeat events; only used when `emit_on.running` is `true`. |

### Transports

`transport` uses the project-wide `{ type, config: { … } }` shape (adjacently tagged):

| `type`  | Backend                                  | `config` fields |
|---------|------------------------------------------|-----------------|
| `http`  | OpenLineage HTTP endpoint (e.g. Marquez) | `url` *(required)*, `timeout_secs` (default `10`), `auth` (optional — see below). `POST`s one event per call; a non-2xx response is logged and dropped. |
| `file`  | Local JSON Lines file                    | `path` *(required)*. Appends one JSON object per line; parent directories are created. |
| `kafka` | Kafka topic                              | `brokers` *(required)*, `topic` *(required)*. One JSON message per event; delivery is bounded at 10 s per event. **Requires the `transport-kafka` feature.** |

```yaml
# HTTP — POST to an OpenLineage endpoint
transport:
  type: http
  config:
    url: https://marquez/api/v1/lineage
    timeout_secs: 10
    auth:
      type: bearer
      config:
        token: ${env:MARQUEZ_TOKEN}
```

```yaml
# File — append JSON Lines locally (great for testing)
transport:
  type: file
  config:
    path: ./out/lineage.jsonl
```

```yaml
# Kafka — requires the transport-kafka feature
transport:
  type: kafka
  config:
    brokers: localhost:9092
    topic: openlineage.events
```

#### HTTP transport auth

The nested `auth` uses the same `{ type, config }` shape as connector auth. Only bearer is supported:

| `type`  | `config`          | Effect |
|---------|-------------------|--------|
| `bearer`| `{ token: <str> }`| Sends `Authorization: Bearer <token>` on each POST. |

### Parent job

`parent_job` links each run to an upstream orchestrator job, populating OpenLineage's `parent` run facet:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `namespace` | string | — *(required)* | Orchestrator namespace (e.g. `airflow`). |
| `name` | string | — *(required)* | Parent job name (e.g. `warehouse_dag.load_orders`). |
| `run_id` | string | *(unset)* | Parent run id. When omitted, the pipeline's own run id is used as the parent run reference. |

```yaml
parent_job:
  namespace: airflow
  name: warehouse_dag.load_orders
  run_id: 0190-abc...   # optional
```

### Emit toggles (`emit_on`)

Per-event booleans. Defaults: `start`, `complete`, `fail`, `abort` are `true`; `running` is `false`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `start` | bool | `true` | Emit a `START` event. |
| `running` | bool | `false` | Emit periodic `RUNNING` heartbeats every `heartbeat_interval`. |
| `complete` | bool | `true` | Emit a `COMPLETE` event on success. |
| `fail` | bool | `true` | Emit a `FAIL` event on error. |
| `abort` | bool | `true` | Emit an `ABORT` event on cancellation. |

```yaml
emit_on:
  start: true
  running: true        # opt into heartbeats
  complete: true
  fail: true
  abort: true
heartbeat_interval: 15
```

## Schema facets

When `include_schema_facet: true`, the run wraps its source and sink in sampling wrappers ([`SamplingSource`] / [`SamplingSink`]) that clone up to `sample_records` records, infer an ordered `(field, type)` schema, and attach it to the input/output datasets on the terminal event.

Inferred OpenLineage types: `null`, `boolean`, `integer`, `number`, `string`, `array`, `object`.

## Column-level lineage

When `include_column_lineage: true`, the declared transform chain is folded over the input field set ([`derive_column_lineage`]) to produce per-output-field input edges. Lineage is **only** emitted when *every* transform in the chain is mappable; if any transform is opaque, no column-lineage facet is attached for that run — it is never fabricated.

| Transform | Column lineage |
|-----------|----------------|
| `rename_field`, `select`, `drop`, `set` | **Mapped** — explicit key relationships. |
| `cast`, `redact`, `value_case`, `spell_symbols`, `filter` | **Mapped** — identity (keys unchanged). |
| `flatten`, `explode`, `keys_case`, `rename_keys`, custom Rust closures | **Opaque** — facet suppressed for the whole run. |

A `set` field with no upstream column produces a literal output field with no input edge.

## Examples

### File transport for local development

```yaml
lineage:
  namespace: dev.local
  transport:
    type: file
    config:
      path: ./out/lineage.jsonl
  include_schema_facet: true
```

### HTTP with bearer auth and an orchestrator parent

```yaml
lineage:
  namespace: prod.warehouse
  job_name: ${name}::${row_id}
  transport:
    type: http
    config:
      url: ${env:MARQUEZ_URL}
      timeout_secs: 15
      auth:
        type: bearer
        config:
          token: ${env:MARQUEZ_TOKEN}
  parent_job:
    namespace: airflow
    name: warehouse_dag.load_orders
  include_schema_facet: true
  include_column_lineage: true
```

### Heartbeats for long-running pipelines

```yaml
lineage:
  namespace: prod.warehouse
  transport:
    type: http
    config:
      url: ${env:MARQUEZ_URL}
  emit_on:
    running: true
  heartbeat_interval: 30
```

## Metrics

Emitted via the [`metrics`](https://docs.rs/metrics) facade (scraped through the standard faucet-stream observability exporter):

- `faucet_lineage_events_total{event_type, outcome}` — events sent; `outcome` ∈ `ok | err`.
- `faucet_lineage_dropped_total{reason}` — dropped events; `reason` ∈ `disabled | transport_error`.
- `faucet_lineage_emit_duration_seconds{event_type}` — per-event send-latency histogram.

## Emission never fails a run

Lineage is observability, not a data path. [`LineageEmitter::emit`] returns no error: a transport failure (HTTP non-2xx, unreachable file, Kafka send error), a serialization failure, or a disabled event is logged and counted (`faucet_lineage_dropped_total`), then dropped. A broken lineage backend can never abort or stall a pipeline.

## Library usage

Build a [`LineageEmitter`] from a [`LineageConfig`] and emit lifecycle events around your own run loop:

```rust
use faucet_lineage::{LineageConfig, LineageEmitter};
use faucet_lineage::event::EventType;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let cfg: LineageConfig = serde_json::from_value(serde_json::json!({
    "namespace": "prod.warehouse",
    "transport": {
        "type": "file",
        "config": { "path": "./out/lineage.jsonl" }
    }
}))?;

let emitter = LineageEmitter::new(cfg)?;   // Arc<LineageEmitter>

// Build a RunLifecycle for the run (job/run ids, input/output datasets, …),
// then emit around the run — emit() never returns an error.
// emitter.emit(EventType::Start, &lifecycle).await;
// emitter.emit(EventType::Complete, &lifecycle).await;
# Ok(())
# }
```

The CLI bridge (`cli/src/lineage_glue.rs`) does exactly this: it builds the emitter, wraps the source/sink in the sampling wrappers, maps resolved transform specs onto `ColumnOp`s, and emits around `run_stream`.

## How it works

- The only `faucet-core` touch-points are `dataset_uri()` on the `Source` / `Sink` traits (so each connector names its dataset) and `redact_uri_credentials` in `faucet_core::util` (so credentials never leak into a dataset URI). The hot `run_stream` loop is untouched.
- `event.rs` is a serde-faithful subset of the OpenLineage 2.0.2 `RunEvent` object model; `emitter.rs` builds events from a `RunLifecycle` and dispatches them through the configured transport.
- Each transport (`transport/{http,file,kafka}.rs`) implements a single `send(Vec<u8>)` method that serializes and ships one event; errors are returned to the emitter, which logs and drops them.
- Column lineage (`column.rs`) is a pure, deterministic fold over `ColumnOp`s with no I/O — fully unit-tested.

## Feature flags

| Feature | Default | Effect |
|---------|---------|--------|
| *(none)* | — | HTTP and file transports are always available. |
| `transport-kafka` | off | Adds the Kafka transport (pulls in `rdkafka`). |

In the CLI these surface as the `lineage` (HTTP + file) and `lineage-kafka` (adds Kafka) features.

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| No events appear in Marquez | Confirm `transport.url` points at the `/api/v1/lineage` endpoint and is reachable. Emission failures are silent by design — set `FAUCET_LOG=warn` to see drop warnings, or check `faucet_lineage_dropped_total{reason="transport_error"}`. |
| 401 / 403 from the HTTP endpoint | The endpoint needs auth. Add `auth: { type: bearer, config: { token: ${env:MARQUEZ_TOKEN} } }`. |
| No column-lineage facet emitted | The transform chain contains an opaque transform (`flatten`, `explode`, `keys_case`, `rename_keys`, or a custom closure). Lineage is suppressed for the whole run rather than fabricated — see the [matrix](#column-level-lineage). Also ensure `include_column_lineage: true`. |
| No schema facet emitted | Set `include_schema_facet: true`. Schemas attach only to terminal events (`COMPLETE` / `ABORT` / `FAIL`), not `START`. |
| `RUNNING` heartbeats never fire | `emit_on.running` defaults to `false`. Set it `true` (and tune `heartbeat_interval`). |
| `transport: kafka` rejected as an unknown variant | The Kafka transport is gated behind the `transport-kafka` feature (`lineage-kafka` in the CLI). Rebuild with it enabled. |
| Config rejected with an unknown-field error | `LineageConfig` is `deny_unknown_fields`. Check spelling against the [configuration reference](#configuration-reference). |
| Worried about secrets in lineage events | Keep `include_source_code_facet: false` (the default). Enabling it emits the resolved config — which may contain resolved secrets — and logs a warning. |

## See also

- [Lineage cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/lineage.html) — end-to-end walkthrough.
- [Config reference](https://faucet-hq.github.io/faucet-stream/reference/config.html) — the full `lineage:` grammar.
- [OpenLineage](https://openlineage.io/) · [Marquez](https://marquezproject.ai/) — the spec and a reference backend.

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.
