# Lineage (OpenLineage)

faucet-stream can emit [OpenLineage](https://openlineage.io/) `RunEvent`s for every pipeline run —
START, RUNNING, COMPLETE, ABORT, and FAIL — carrying job identity, input/output dataset URIs,
inferred dataset schemas, and column-level lineage derived from the transform chain.

Events are emitted asynchronously after each lifecycle transition and **never fail a run**: if the
transport is unreachable or returns an error, faucet logs a warning, increments the
`faucet_lineage_dropped_total` counter, and continues. The pipeline result is unaffected.

## What is OpenLineage?

[OpenLineage](https://openlineage.io/) is a vendor-neutral open standard for data lineage metadata.
It defines a common event format (JSON) that tools like
[Marquez](https://marquezproject.ai/), [Apache Atlas](https://atlas.apache.org/), and
[OpenMetadata](https://open-metadata.org/) consume to build data-lineage graphs.

faucet-stream emits the **OpenLineage spec version 2.0.2** (`RunEvent` schema).

## Quick start with Marquez

```bash
# Start a local Marquez instance
docker run -p 5000:5000 -p 5001:5001 \
  -e MARQUEZ_CONFIG=/etc/marquez/marquez.yml \
  marquezproject/marquez:latest

# Run the bundled example (requires lineage + postgres + bigquery CLI features)
export MARQUEZ_URL=http://localhost:5000/api/v1/lineage
export GCP_KEY_JSON=$(cat service-account.json)
faucet run cli/examples/postgres_to_bigquery_with_lineage.yaml
```

Then open the Marquez UI at `http://localhost:3000` to explore the emitted lineage graph.

## The `lineage:` block

Add a `lineage:` block at the top level of your pipeline config:

```yaml
version: 1
name: my_pipeline

lineage:
  namespace: prod.warehouse      # REQUIRED. Logical namespace for all jobs/datasets.
  transport:                     # REQUIRED. Where to send events.
    type: http
    config:
      url: http://marquez:5000/api/v1/lineage

pipeline:
  source: { type: postgres, config: { … } }
  sink:   { type: bigquery, config: { … } }
```

### Full field reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | `openlineage` | `openlineage` | Lineage format. Only `openlineage` is supported in v1. |
| `namespace` | string | **required** | OpenLineage namespace used for all jobs and datasets emitted by this config. |
| `transport` | Transport | **required** | How events are delivered (see [Transports](#transports)). |
| `job_name` | string | `"${name}::${row_id}"` | Job-name template. `${name}` and `${row_id}` are resolved per matrix row at run time; `${now.*}` tokens are also supported. |
| `parent_job` | ParentJob | `null` | Optional parent-job linkage for orchestration tools (Airflow, Dagster). |
| `include_schema_facet` | bool | `false` | Emit dataset schema facets (inferred from a sample of records). Input schema from the pre-transform sample; output schema always inferred from the transformed sample. |
| `include_column_lineage` | bool | `false` | Emit column-level lineage facets where the transform chain is deterministically mappable (see [Column lineage](#column-lineage)). |
| `include_source_code_facet` | bool | `false` | Emit the resolved config body as a `sourceCode` job facet. **Off by default** — the resolved config may contain secrets; enabling this field logs a one-time warning. |
| `emit_on` | EmitOn | start+complete+fail+abort | Which lifecycle events to emit (see below). |
| `sample_records` | integer | `100` | Maximum records sampled to infer schemas and column lineage. |
| `heartbeat_interval` | integer (seconds) | `30` | RUNNING heartbeat interval; only relevant when `emit_on.running: true`. |

#### `emit_on` toggles

```yaml
lineage:
  emit_on:
    start: true      # Emit START before the pipeline begins. Default true.
    running: false   # Emit periodic RUNNING heartbeats. Default false.
    complete: true   # Emit COMPLETE on success. Default true.
    fail: true       # Emit FAIL on pipeline error. Default true.
    abort: true      # Emit ABORT on cooperative cancellation / timeout. Default true.
```

#### `parent_job` — orchestrator linkage

```yaml
lineage:
  parent_job:
    namespace: airflow.prod
    name: dag.etl_daily.extract_orders
    run_id: ${env:AIRFLOW_RUN_ID}   # optional; set by orchestrators
```

## Transports

### HTTP (Marquez / any OpenLineage-compatible endpoint)

```yaml
lineage:
  namespace: prod
  transport:
    type: http
    config:
      url: https://lineage.example.com/api/v1/lineage
      timeout_secs: 10        # request timeout. Default 10.
      auth:                   # optional bearer auth
        type: bearer
        config:
          token: ${env:LINEAGE_TOKEN}
```

### File (local JSON Lines)

Events are appended one-per-line to a local file. Parent directories are created automatically.

```yaml
lineage:
  namespace: dev
  transport:
    type: file
    config:
      path: ./out/lineage.jsonl
```

### Kafka (gated on `lineage-kafka` feature)

Each event is produced as a JSON message to a Kafka topic. Requires building with
`--features lineage-kafka`. Delivery of one event is bounded at 10 seconds
(`message.timeout.ms`), so an unreachable broker delays a run by seconds per event
and the event is dropped, rather than waiting out Kafka's 5-minute default.

```yaml
lineage:
  namespace: prod
  transport:
    type: kafka
    config:
      brokers: kafka.example.com:9092
      topic: openlineage.events
```

## Schema facets

When `include_schema_facet: true`, faucet attaches `DatasetFacets.schema` to both the input
and output datasets:

- **Output schema** is always available — inferred from the post-transform sample written to
  the sink (up to `sample_records` records).
- **Input schema** is inferred from the pre-transform sample (before any transforms run),
  so it reflects what the source actually produced.

Field types follow OpenLineage naming conventions (e.g. `string`, `integer`, `number`, `boolean`,
`object`, `array`, `null`).

## Column lineage

When `include_column_lineage: true`, faucet derives per-field upstream→downstream mappings from
the declared transform chain. If the chain contains any transform that cannot be statically
analyzed, **no column-lineage facet is emitted** (never fabricated).

### Supported transforms

These transforms produce exact column-lineage edges:

| Transform | Effect on lineage |
|-----------|------------------|
| `rename_field` | Renames an output column; preserves the source field as the upstream edge. |
| `select` | Retains only the listed fields; unlisted fields are removed from the lineage map. |
| `drop` | Removes listed fields from the lineage map. |
| `set` | Adds new literal fields with no upstream edge (empty input list). |
| `cast` | Key unchanged; treated as identity (no rename). |
| `redact` | Key unchanged; treated as identity. |
| `value_case` | Key unchanged; treated as identity. |
| `spell_symbols` | Key unchanged; treated as identity. |

### Omitted transforms (column-lineage facet suppressed)

If the chain includes any of these, the column-lineage facet is **not emitted** for that run:

| Transform | Why |
|-----------|-----|
| `flatten` | Restructures keys — source-to-output mapping is not deterministic. |
| `explode` | Expands arrays — 1:N relationship cannot be expressed as a column graph. |
| `keys_case` | Rewrites all key names — rename map is not declared, only computed. |
| `rename_keys` | Regex-based key renaming — not statically analyzable per-field. |
| Custom Rust closures | Unknown at config-parse time. |

## Example: PostgreSQL → BigQuery with lineage

```yaml
# cli/examples/postgres_to_bigquery_with_lineage.yaml
version: 1
name: postgres_to_bigquery_with_lineage

lineage:
  namespace: prod.warehouse
  job_name: ${name}::${row_id}
  include_schema_facet: true
  include_column_lineage: true
  transport:
    type: http
    config:
      url: ${env:MARQUEZ_URL}

pipeline:
  source:
    type: postgres
    config:
      connection_url: postgres://user:pass@localhost/app
      query: SELECT id, created_at, customer_email, payload FROM orders WHERE created_at > $1 AND status = $2
      params:
        - "2026-01-01T00:00:00Z"
        - completed
      max_connections: 16
      batch_size: 1000

  transforms:
    - type: rename_field
      config:
        fields:
          customer_email: contact_email
    - type: select
      config:
        fields:
          - id
          - created_at
          - contact_email

  sink:
    type: bigquery
    config:
      project_id: my-gcp-project
      dataset_id: warehouse
      table_id: orders
      auth:
        type: service_account_key
        config:
          json: ${env:GCP_KEY_JSON}
      batch_size: 1000
```

This config emits:
- START before the first page is fetched.
- COMPLETE after the sink flushes, with schema facets for both input (`postgres://localhost/app?query=…`)
  and output (`bigquery://my-gcp-project.warehouse.orders`).
- Column-lineage facet: `contact_email ← customer_email` (rename_field), `id ← id`, `created_at ← created_at` (identity via select).
- FAIL / ABORT on error or cancellation.

## Metrics

All lineage metrics are automatically registered when `lineage:` is configured:

| Metric | Labels | Description |
|--------|--------|-------------|
| `faucet_lineage_events_total` | `event_type`, `outcome` | Total events emitted (`outcome` = `ok` or `err`). |
| `faucet_lineage_emit_duration_seconds` | `event_type` | Histogram of emission latency per event type. |
| `faucet_lineage_dropped_total` | `reason` | Events dropped due to transport errors or serialization failures. |

`event_type` values: `START`, `RUNNING`, `COMPLETE`, `FAIL`, `ABORT`.

## `faucet validate` and `faucet doctor`

`faucet validate` checks the `lineage:` block at parse time — bad transport config, unreachable
file paths, and schema errors all surface as config errors before any run starts.

`faucet doctor` probes the configured transport for reachability:
- **HTTP** — issues a `HEAD` request to the configured URL.
- **File** — verifies the parent directory exists or can be created.
- **Kafka** — reports the brokers as configured (not probed; requires a live broker).

## `faucet schema lineage`

```bash
faucet schema lineage
```

Prints the full JSON Schema for the `lineage:` block — the same schema used by `faucet validate`
and `faucet init`.
