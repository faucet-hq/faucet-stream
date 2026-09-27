# Schema drift

Source schemas change. A team adds a column to a table, an API starts returning
a new field, an `integer` becomes a `bigint`. In a naive ELT pipeline those
changes break the destination write — a new field has no column to land in, a
widened type overflows — and the pipeline either errors out or silently drops
data. faucet's `schema:` block turns that into one declarative policy: detect
when an incoming page's shape diverges from the sink's live destination schema
and apply a single, uniform action across every sink.

## The `schema:` block

`schema:` is a pipeline-level block (a sibling of `source`, `sink`,
`transforms`, and `state`). It is fully opt-in — with no block, sinks keep their
existing per-connector behaviour.

```yaml
pipeline:
  schema:
    on_drift: warn                     # warn | evolve | ignore | quarantine | fail
    allow_type_widening: true          # default true; only consulted by `evolve`
    on_incompatible: fail              # fail | quarantine — `evolve` only (default fail)
    relax_nullability_on_missing: false # default false; `evolve` only
  source: { ... }
  sink: { ... }
```

| Field | Default | Purpose |
|-------|---------|---------|
| `on_drift` | `warn` | The policy applied when drift is detected. |
| `allow_type_widening` | `true` | Whether a lossless type widening (e.g. `integer → number`, or gaining nullability) counts as evolvable rather than incompatible. Only consulted by `evolve`. |
| `on_incompatible` | `fail` | `evolve` only — what to do with a residue that cannot be auto-applied (a narrowing / incompatible type swap): `fail` aborts, `quarantine` routes the offending rows to the DLQ. |
| `relax_nullability_on_missing` | `false` | `evolve` only — whether a `NOT NULL` destination column that is merely **absent** from a page may have its `NOT NULL` constraint dropped. Default `false`: a transiently-omitted column is not evidence the column is optional, so the constraint is left untouched. Set `true` only when you deliberately want column omission to relax nullability. *Nullability relaxation driven by an observed null value (a widening) is unaffected by this flag.* |

### How detection works

On each page, faucet infers the page's top-level shape and diffs it against the
sink's **live** destination schema (read once per run, refreshed after an
`evolve`). The diff is **top-level only**: a nested object counts as one column,
so a change *inside* a nested object is invisible. Each top-level column is
bucketed as an **addition** (in the page, not in the destination), a **widening**
(an existing column whose type widened losslessly), an **incompatible** change (a
narrowing or unrelated type swap), or a **droppable-required** column (a NOT NULL
destination column the page never provides).

## The five modes

### `warn` (default)

Detect, emit a metric and a one-shot log line, and write the page **unchanged**.
The safest default — nothing about the destination or the data changes; you just
get visibility that drift is happening.

```yaml
schema:
  on_drift: warn
```

### `ignore`

Drop every field that is not present in the destination schema, then write the
trimmed records. Use this when the destination is the source of truth and new
upstream fields should simply be discarded.

```yaml
schema:
  on_drift: ignore
```

### `fail`

Raise a `SchemaDrift` error and abort the run the moment drift is detected. Use
this when any divergence is a real incident that a human must look at before more
data flows.

```yaml
schema:
  on_drift: fail
```

### `quarantine`

Route the records that exhibit the drift to the [dead-letter
queue](./dlq.md) and write the rest of the page normally. Requires a `dlq:`
block. Quarantined rows carry a `schema_drift` reason in their DLQ envelope.

```yaml
schema:
  on_drift: quarantine
pipeline:
  # ...
  dlq:
    sink: { type: jsonl, config: { path: ./drift.jsonl } }
    on_batch_error: dlq_all
```

### `evolve`

Apply additive/widening DDL to the destination — `ADD COLUMN` for additions,
type widening for widenings — then write the page through. Any incompatible
residue is handled by `on_incompatible`. This is the mode that keeps a mirror in
lockstep with a changing source without manual `ALTER TABLE`s.

```yaml
schema:
  on_drift: evolve
  allow_type_widening: true
  on_incompatible: fail
  relax_nullability_on_missing: false
```

> **A `NOT NULL` column missing from a page does not relax by default.** A column
> the page simply doesn't carry (a `droppable-required` column) is *not* treated
> as evidence that the column became optional — a partial/transient page omits it
> just as readily as a real schema change, and auto-dropping the constraint would
> silently and irreversibly weaken the destination. With the default
> `relax_nullability_on_missing: false`, an omitted required column is left
> untouched (a page that genuinely lacks a required value then fails loudly at
> write time). Set `relax_nullability_on_missing: true` only when you deliberately
> want omission to relax the constraint. Relaxation driven by an *observed* null
> value in a present column (a widening) still happens regardless of this flag.

## Sink support

Not every sink can evolve, and a schemaless sink has no schema to diverge from.

| Sink | Detection (`warn`/`ignore`/`fail`/`quarantine`) | `evolve` |
|------|:---:|:---:|
| `postgres`, `mysql`, `mssql`, `sqlite` | ✅ | ✅ |
| `bigquery` | ✅ | ✅ |
| `elasticsearch` | ✅ | ✅ (add fields only) |
| `spanner` | ✅ | ✅ (add + NOT NULL relax; no base-type widening) |
| `iceberg` | ✅ | ✅ (add columns only) |
| `oracle` | ✅ | ✅ (add, integer→decimal widening, NOT NULL relax) |
| `databricks` | ✅ | ✅ (add columns, numeric widening to `DOUBLE`, NOT NULL relax) |
| `jsonl`, `csv`, `stdout`, `mongodb`, `redis`, `http`, `kafka`, `s3`, `gcs`, `snowflake`, `parquet`, `dynamodb` | — (inert) | — |

- **Evolvable** (ten sinks): postgres, mysql, mssql, sqlite, bigquery,
  elasticsearch, spanner, iceberg, databricks, oracle. They implement in-place additive DDL.
- **Iceberg** adds new columns via iceberg-rust 0.10.0's `update_schema`
  action (issue #255). It is **additive-only**: `update_schema` exposes
  `add_column` but no in-place type promotion or nullability relaxation, so a
  drift that needs a *widening* (base-type change) or nullability relaxation is
  rejected with a typed error rather than silently ignored.
- **Schemaless sinks** report no destination schema, so *any* `schema:` policy is
  **inert** against them (a one-shot log notes this). `on_drift: evolve` against
  a schemaless sink is rejected at config-load (there is nothing to evolve).

### Per-sink `evolve` nuances

- **SQLite** — widening and NOT NULL relaxation are no-ops because SQLite is
  dynamically typed; only `ADD COLUMN` does real work.
- **MySQL / MSSQL** — relaxing a NOT NULL column re-emits the column at its
  (lossless) widened base type to drop the constraint.
- **Elasticsearch** — can only **add** fields. Changing the type of an existing
  field is impossible in Elasticsearch mappings, so an existing-field type change
  is always treated as incompatible (routed by `on_incompatible`).
- **Cloud Spanner** — adds columns and relaxes NOT NULL (by re-emitting the
  column without the constraint), but Spanner cannot change a column's base
  type (e.g. INT64→FLOAT64), so a base-type widening fails with guidance to set
  `allow_type_widening: false` (classifying it incompatible instead). DDL runs
  as a bounded long-running operation via the admin API.
- **Databricks** — `ALTER TABLE … ADD COLUMNS` for new columns and `ALTER COLUMN
  … DROP NOT NULL` for relaxation. A numeric widening from
  `tinyint`/`smallint`/`int`/`float` to `DOUBLE` enables Delta type widening
  (`delta.enableTypeWidening`) and alters the column in place; any other
  widening fails with a typed error naming the column.

## Composition rules

- **`quarantine` requires a `dlq:` block** (`on_drift: quarantine`, or `evolve`
  with `on_incompatible: quarantine`). Validated at config-load.
- **`quarantine` is incompatible with `delivery: exactly_once`** — effectively-once
  forbids a DLQ, so a quarantine policy cannot run alongside it.
- **`evolve` / `ignore` / `fail` / `warn` compose with everything** — including
  [`delivery: exactly_once`](./state.md#effectively-once-delivery) and
  [`write_mode: upsert`](./upsert.md). Under `evolve` + effectively-once the additive
  DDL runs first, then the records and the commit token land in one transaction.

## Worked example: CDC mirror that evolves with the source

The shipped example
[`cli/examples/postgres_cdc_to_postgres_evolve.yaml`](https://github.com/faucet-hq/faucet-stream/blob/main/cli/examples/postgres_cdc_to_postgres_evolve.yaml)
mirrors a Postgres table via CDC and evolves the destination as the source
schema changes — effectively-once, upsert, drift-aware:

```yaml
version: 1
name: pg_cdc_mirror_evolve
delivery: exactly_once

pipeline:
  schema:
    on_drift: evolve
    allow_type_widening: true
    on_incompatible: fail

  source:
    type: postgres-cdc
    config:
      connection_url: ${env:SOURCE_PG_URL}
      slot_name: faucet_mirror_evolve
      publication_name: faucet_pub
      create_slot_if_missing: true
      idle_timeout: 30

  transforms:
    - type: cdc_unwrap

  sink:
    type: postgres
    config:
      connection_url: ${env:DEST_PG_URL}
      table_name: users_mirror
      column_mapping: auto_map
      write_mode: upsert
      key: [id]
      delete_marker: { field: __op, values: [d] }

  state:
    type: file
    config:
      path: ./state
```

`ALTER TABLE users ADD COLUMN email text;` on the source, then INSERT a row with
`email` set — faucet adds `email` to `users_mirror` on the next fetch cycle and
writes the row. Validate it offline (no database connection required):

```bash
faucet validate cli/examples/postgres_cdc_to_postgres_evolve.yaml
```

## Metric

Every detected drift increments
`faucet_schema_drift_total{pipeline,row,connector,mode,kind}`, where `mode` is
the `on_drift` policy (`warn` / `ignore` / `quarantine` / `fail` / `evolve`) and
`kind` is the drift bucket (`added` / `widened` / `narrowed` / `dropped`). Alert
on it (or just chart it) to see drift before it surprises you — even under
`warn`, where nothing else changes.
