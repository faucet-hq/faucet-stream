# faucet-sink-mssql

[![Crates.io](https://img.shields.io/crates/v/faucet-sink-mssql.svg)](https://crates.io/crates/faucet-sink-mssql)
[![Docs.rs](https://docs.rs/faucet-sink-mssql/badge.svg)](https://docs.rs/faucet-sink-mssql)
[![MSRV](https://img.shields.io/crates/msrv/faucet-sink-mssql.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-sink-mssql.svg)](https://github.com/faucet-hq/faucet-stream#license)

Microsoft **SQL Server** sink for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. Writes records via parameterized multi-row `INSERT`s — either auto-mapped to same-named table columns or serialized into a single JSON column — built on [`tiberius`](https://crates.io/crates/tiberius) + [`bb8-tiberius`](https://crates.io/crates/bb8-tiberius) with a pooled, statement-timeout-aware connection.

Reach for it when you want to land records from any faucet-stream source into SQL Server or Azure SQL with one declarative config: multi-row INSERTs auto-split to stay under SQL Server's 2100-parameter ceiling, batches commit atomically inside a transaction, per-row failures are isolated for dead-letter routing, and `upsert`/`delete` write modes plus effectively-once delivery are available for keyed mirrors.

## Feature highlights

- **Two write shapes** — `auto_columns` maps top-level JSON keys to same-named table columns (the column set is the union across the batch); `json_column` serializes each record into a single `NVARCHAR(MAX)` / native `JSON` column. Schema-agnostic vs. typed-column landing.
- **2100-parameter auto-split** — a multi-row `INSERT` binds `rows × columns` parameters. SQL Server caps a request at 2100 parameters (and `tiberius` spends 2 on its `sp_executesql` wrapper, leaving 2098) and at 1000 row expressions in a `VALUES` clause. The sink splits each batch into `min(2098 / columns, 1000)`-row statements automatically, all inside one transaction.
- **Write modes (upsert / delete)** — beyond the default append, merge-by-key (`upsert`) or delete-by-key (`delete`) via a single T-SQL `MERGE`, including composite keys and a `delete_marker` for CDC-style streams. Requires `auto_columns`.
- **Effectively-once delivery** — atomic record + commit-token write into a `_faucet_commit_token` watermark table, so a CDC mirror never double-applies a page on resume.
- **Row-isolation DLQ** — on a batch failure the sink rolls back and replays the batch one row at a time so good rows still land and only the offender is dead-lettered. Transient errors (deadlock, lock-timeout, dropped connection) retry with backoff.
- **Connection pooling** — a `bb8` pool (default 5 connections) created once and reused; each statement runs under a configurable server-side timeout.
- **Flexible connection** — a `mssql://` URL parsed by faucet, or an ADO.NET-style connection string handed straight to `tiberius`, with TLS/encryption governed by a `tls` block in either case.

## Installation

```bash
# As a library:
cargo add faucet-sink-mssql

# In the CLI (opt-in connector feature):
cargo install faucet-cli --features sink-mssql
```

`sink-mssql` is opt-in — it is not in the CLI/umbrella default build.

## Quick start

```yaml
# pipeline.yaml — faucet run pipeline.yaml
version: 1
pipeline:
  source:
    type: csv
    config:
      path: ./events.csv
  sink:
    type: mssql
    config:
      connection_url: "mssql://sa:Str0ng%40Pass@localhost:1433/sales"
      table: "dbo.events"
      column_mapping:
        type: auto_columns
```

```bash
faucet run pipeline.yaml
```

> The password in `connection_url` is percent-encoded (`@` → `%40`). Prefer secrets-manager / env indirection over inline credentials — see [Security](#security).

## Configuration reference

### Core

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `connection_url` | string | — | `mssql://user:pass@host:1433/database` URL (parsed by faucet; credentials percent-decoded). Mutually exclusive with `connection_string`; exactly one is required. |
| `connection_string` | string | — | ADO.NET-style string handed straight to `tiberius`, e.g. `Server=tcp:host,1433;Database=db;User Id=sa;Password=...;`. Mutually exclusive with `connection_url`. |
| `table` | string | — *(required)* | Target table, optionally schema-qualified (e.g. `dbo.events`). |
| `column_mapping` | `MssqlColumnMapping` | `{ type: json_column, column: "data" }` | How records map onto columns — see [Column mapping](#column-mapping). |

### Batching & reliability

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `batch_size` | int | `500` | Rows per multi-row `INSERT`. Auto-split further so `rows × columns` stays within the 2100-parameter limit. **`0` = send the whole page** in one logical write (still param-split internally). |
| `max_connections` | int | `5` | Maximum pooled connections (`bb8`). |
| `transaction_per_batch` | bool | `true` | Wrap each batch's `INSERT`s in `BEGIN TRAN` / `COMMIT TRAN`. Upsert/delete are always transaction-wrapped regardless. |
| `isolate_row_failures` | bool | `true` | On a batch failure, roll back and replay row-by-row so good rows land and only the bad row is DLQ-routed. `false` fails the whole batch on the first bad row (fewer round-trips). |
| `statement_timeout_secs` | int (seconds) | `300` | Per-statement server-side timeout. `0` disables. |
| `create_table` | bool | `false` | **`json_column` mode only** — create the table if absent as `(id BIGINT IDENTITY PRIMARY KEY, <column> NVARCHAR(MAX))`. Rejected with `auto_columns` (schema inference for MSSQL types is unsafe). |

### Write mode

These fields come from the shared `WriteSpec` (flattened into the sink config), so they sit at the config top level:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `write_mode` | `append` \| `upsert` \| `delete` | `append` | Append (plain INSERT), merge-by-key, or delete-by-key. `upsert`/`delete` require `column_mapping: auto_columns`. |
| `key` | array of string | `[]` | Key column(s) for upsert/delete. Required & non-empty for those modes; composite keys supported. Ignored for append. |
| `delete_marker` | object | *(unset)* | **Upsert only.** `{ field: <name>, values: [<str>, …] }` — rows whose `field` matches a value become deletes; the marker field is stripped from upserted rows. |

### TLS

The `tls` block governs encryption for both connection forms:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `tls.type` | `prefer` \| `require` \| `trust_server_certificate` \| `disable` | `prefer` | Encryption mode — see [TLS modes](#tls-modes). |
| `tls.ca_cert_path` | path | *(unset)* | CA certificate (PEM/DER) to trust for server validation. Ignored when `tls.type` is `disable`. |

## Column mapping

`column_mapping` is the project-wide `{ type, … }` tagged shape:

| `type` | Fields | Behavior |
|--------|--------|----------|
| `auto_columns` | `on_unknown_field: warn \| drop \| error` | Top-level JSON keys map to same-named table columns. The column set is the **union** of keys across the batch (a field present only in a later record is still written; rows missing a column → SQL `NULL`). `IDENTITY` columns are skipped automatically. Keys with no matching column are handled by `on_unknown_field` (default `warn`). |
| `json_column` | `column: <name>` | Each record is serialized to a JSON string inserted into a single `NVARCHAR(MAX)` (or native Azure SQL `JSON`) column. Schema-agnostic. Pair with `create_table: true` to create the table if absent. |

```yaml
# Typed columns
column_mapping:
  type: auto_columns
  on_unknown_field: warn      # warn | drop | error
```

```yaml
# Single JSON column
column_mapping:
  type: json_column
  column: payload
```

> `IDENTITY` columns are server-generated — do **not** put identity values in your records unless you've run `SET IDENTITY_INSERT <table> ON` yourself. `auto_columns` + `create_table` is rejected at construction: create the table yourself first so its types are correct.

## TLS modes

| `tls.type` | Encryption | Use when |
|-----------|------------|----------|
| `prefer` | Encrypt if the server supports it (the safe modern default). | General use — the recommended default. |
| `require` | Require encryption; fail if the server doesn't offer it. | Production where encryption is mandatory. |
| `trust_server_certificate` | Encrypt but accept the server certificate without validating its chain. | **Dev/test only** — insecure against MITM. |
| `disable` | No transport encryption. | Trusted local networks only. |

## Examples

### Append into typed columns

```yaml
sink:
  type: mssql
  config:
    connection_url: "mssql://sa:Str0ng%40Pass@localhost:1433/sales"
    table: "dbo.events"
    column_mapping:
      type: auto_columns
      on_unknown_field: drop
    batch_size: 1000
    max_connections: 8
```

### JSON-column landing with auto-created table

```yaml
sink:
  type: mssql
  config:
    connection_string: "Server=tcp:db.example.com,1433;Database=lake;User Id=ingest;Password=${env:MSSQL_PASSWORD};"
    table: "dbo.raw_payloads"
    column_mapping:
      type: json_column
      column: payload
    create_table: true
    tls:
      type: require
```

### Upsert by key (keyed mirror)

```yaml
sink:
  type: mssql
  config:
    connection_url: "mssql://sa:Str0ng%40Pass@localhost:1433/sales"
    table: "dbo.users"
    column_mapping:
      type: auto_columns
    write_mode: upsert            # append (default) | upsert | delete
    key: [id]                     # composite keys: [tenant_id, id]
    delete_marker:                # upsert only: route flagged rows to deletes
      field: __op
      values: [d, delete]
```

### Azure SQL with self-signed dev certificate

```yaml
sink:
  type: mssql
  config:
    connection_url: "mssql://sa:Str0ng%40Pass@localhost:1433/sales"
    table: "dbo.events"
    column_mapping: { type: json_column, column: data }
    tls:
      type: trust_server_certificate   # dev only — never in production
```

## Streaming & batching

The pipeline streams pages from the source and calls `Sink::write_batch` once per page. `batch_size` re-chunks each page into multi-row `INSERT` statements; every statement is then auto-split so `rows × columns ≤ 2098` and `rows ≤ 1000`, keeping a single request within SQL Server's caps. With `transaction_per_batch: true` (the default) all statements for a page commit atomically inside one `BEGIN TRAN` / `COMMIT TRAN`.

`batch_size: 0` is the "no batching" sentinel: the whole upstream page is handed to the sink as one logical write (still param-split internally into compliant statements). Bulk-copy (`BCP`) is out of scope.

## Staged bulk load (`staging`)

With the `staging` feature and a `staging:` block, each page is uploaded to Azure
Blob / ADLS and SQL Server / Synapse pulls it with `COPY INTO … FROM '<url>'`
instead of parameterized `INSERT`s — far faster for large batches.

```yaml
sink:
  type: mssql
  config:
    connection_url: "..."
    table: dbo.events
    staging:
      location: "az://my-container/mssql-stage/"  # Azure only (COPY INTO reads Azure Blob/ADLS)
      format: csv                                  # COPY INTO file type; csv only
      cleanup: always
      storage_account: mystorageacct              # builds the COPY INTO https URL
      # endpoint: azurite:10000/devacct           # Azurite / sovereign-cloud override
      sas_token: "${env:MSSQL_STAGE_SAS}"          # SAS the server uses to READ the stage
```

Build with the crate's `staging` feature (CLI: `--features sink-mssql-staging`,
in `full`). **Azure only** (`az://…`), **CSV only**. `COPY INTO` maps CSV columns
by **position**, so the target table's column order must match the staged CSV —
align the table (or stage a matching column subset). The load SQL/URL generation
is unit-tested; the server-side `COPY INTO` requires a live SQL Server/Synapse +
Azure storage and is not exercised in CI (same as the other staged-load sinks).

## Write modes (upsert / delete)

In addition to the default append, the sink can **upsert** (insert-or-update by key) or **delete** by key. Both require `column_mapping: auto_columns` — the key columns must be real table columns, not buried inside a JSON column (using `json_column` with `upsert`/`delete` is rejected at construction). The key columns should have a `UNIQUE` / `PRIMARY KEY` constraint.

- **`upsert`** — each record is merged via a single T-SQL [`MERGE`](https://learn.microsoft.com/sql/t-sql/statements/merge-transact-sql): matching rows (by `key`) have their non-key columns updated; non-matching rows are inserted. When every column is a key column there's nothing to update, so the `WHEN MATCHED` clause is omitted. Within a batch, records sharing a key are deduplicated **last-write-wins** before the `MERGE` runs (MERGE rejects a source targeting the same key twice).
- **`delete`** — every record's `key` is collected and deleted via `MERGE … WHEN MATCHED THEN DELETE` (T-SQL has no row-constructor `IN ((a,b), …)`), so single- and multi-column keys share one code path.
- **`delete_marker`** (upsert mode only) — rows whose `field` equals one of `values` are routed to a delete instead of an upsert; the marker field is stripped from the upserted record. This lets a CDC stream carrying an operation flag drive inserts, updates, and deletes from one pipeline.

A row missing or null in a key column fails with a clear `mssql upsert: …` error. With a `dlq:` block configured, the good rows still apply (upserts + deletes) and only the missing/null-key rows are routed to the DLQ per-row; without a DLQ the whole batch fails. Upserts and deletes for a batch always run inside a single `BEGIN TRAN` / `COMMIT TRAN`.

See the [upsert cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/upsert.html) for the full write-mode model.

## Dead-letter queue (partial failures)

With `isolate_row_failures: true` (default), a batch that fails is rolled back and retried one row at a time: the good rows land and only the offending row is returned as an error for dead-letter routing under the pipeline's `dlq:` block. Transient errors (deadlock, lock-timeout, connection drops) are retried with backoff and otherwise propagated so the pipeline's `on_batch_error` policy decides. Set `isolate_row_failures: false` to fail the whole batch on the first bad row (fewer round-trips, no row isolation).

## Effectively-once delivery

`MssqlSink` implements `Sink::supports_idempotent_writes` (returns `true`) and the two companion hooks:

- `write_batch_idempotent(records, scope, token)` — writes `records` and UPSERTs the `token` into a `_faucet_commit_token(scope NVARCHAR, token NVARCHAR)` watermark table inside the **same transaction** (respecting `transaction_per_batch`), so both commit together or neither does.
- `last_committed_token(scope)` — reads the current watermark so the pipeline can skip already-committed pages on resume.

To use effectively-once delivery, set `delivery: exactly_once` in the pipeline config and pair this sink with one of the CDC sources (`postgres-cdc`, `mysql-cdc`, `mongodb-cdc`) plus a `state:` block. A DLQ is not permitted in effectively-once mode. All four requirements are validated at config-load time (`faucet validate`) before any run starts. Effectively-once composes with `write_mode: upsert`.

```yaml
pipeline:
  source:
    type: mongodb-cdc
    config:
      connection_uri: "mongodb://localhost:27017/?replicaSet=rs0"
      scope:
        type: collection
        database: appdb
        collection: orders
  sink:
    type: mssql
    config:
      connection_url: "mssql://sa:Str0ng%40Pass@localhost:1433/warehouse"
      table: "dbo.change_events"
      column_mapping:
        type: auto_columns
  state:
    type: file
    config:
      path: ./state
delivery: exactly_once
```

See the [effectively-once delivery cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/state.html#effectively-once-delivery) for the full rationale and supported source/sink set.

## Schema evolution

`MssqlSink` reports its live destination schema via `current_schema()` (read from `sys.columns`, including nullability), so the pipeline-level `schema:` policy can detect drift between an incoming page's top-level shape and the real table. All five `on_drift` modes (`warn` / `ignore` / `quarantine` / `fail` / `evolve`) work against this sink.

Under `on_drift: evolve`, `MssqlSink::evolve_schema()` applies additive DDL:

- **New columns** → `ALTER TABLE … ADD`, guarded with `IF NOT EXISTS (SELECT 1 FROM sys.columns …)` (idempotent).
- **Lossless widenings** (e.g. integer → number) → `ALTER COLUMN` to the wider type — gated on `allow_type_widening`.
- **Nullability relaxations** → `ALTER COLUMN … NULL`. MSSQL's `ALTER COLUMN` requires the full type spec, so the column is re-emitted at its widened base type keyword (e.g. `INT` → `BIGINT`). This is a minor, always-lossless type canonicalization — the column ends up nullable at the same or a wider type.

After an evolution the cached AutoColumns set is dropped so the next write re-discovers any newly-added column. Incompatible changes (narrowing / type swaps) are never auto-applied — they are routed by `on_incompatible` (`fail` or `quarantine`). See the [schema-drift cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/schema-drift.html).

## Config loading & schema

Load from YAML/JSON or environment. Inspect the full JSON Schema with:

```bash
faucet schema sink mssql
```

## Library usage

```rust
use faucet_core::{Pipeline, Sink};
use faucet_sink_mssql::{MssqlColumnMapping, MssqlSink, MssqlSinkConfig, OnUnknownField};

# async fn run() -> Result<(), faucet_core::FaucetError> {
let mut cfg = MssqlSinkConfig::new(
    "mssql://sa:Str0ng%40Pass@localhost:1433/sales",
    "dbo.events",
);
cfg.column_mapping = MssqlColumnMapping::AutoColumns {
    on_unknown_field: OnUnknownField::Warn,
};
cfg.batch_size = 1000;

let sink = MssqlSink::new(cfg).await?;
// Drive it from any source via `Pipeline` / `run_stream`.
# let _ = sink;
# Ok(())
# }
```

The shared connection/TLS types (`MssqlConnectionConfig`, `MssqlTls`, `MssqlTlsMode`) are re-exported from this crate, so you configure the sink without depending on [`faucet-common-mssql`](https://crates.io/crates/faucet-common-mssql) directly.

## How it works

1. `new()` validates the config, builds a `bb8`+`tiberius` connection pool **once**, and reuses it for every write.
2. Each page is re-chunked to `batch_size` rows, then each chunk is split so `rows × columns ≤ 2098` and `rows ≤ 1000` — one or more multi-row `INSERT` (or `MERGE`) statements per chunk.
3. With `transaction_per_batch`, all statements for a page run inside one `BEGIN TRAN` / `COMMIT TRAN`; upsert/delete are always transaction-wrapped.
4. Identifiers are bracket-quoted (`[name]`, doubling interior `]`) via `quote_ident_mssql`; values are bound as parameters (never string-interpolated), so SQL injection isn't possible.
5. On a batch failure with `isolate_row_failures`, the transaction rolls back and the batch is replayed one row at a time to surface the single offending row.

## Lineage dataset URI

`<connection_url_or_string>?table=<table>` (password redacted) — e.g. `Server=tcp:host,1433;Database=db;User Id=sa;Password=***;?table=orders`.

## Authentication

SQL Server authentication (username + password) only in v1, via the `connection_url` / `connection_string`. Windows / Integrated authentication and Azure AD / Managed Identity are out of scope. See [`faucet-common-mssql`](https://crates.io/crates/faucet-common-mssql) for the full connection/TLS reference.

## Security

`tls.type: trust_server_certificate` (and `TrustServerCertificate=true` in a connection string) disable certificate-chain validation — they accept **any** server certificate and are vulnerable to man-in-the-middle attacks. Use them only against trusted dev servers. In production use `prefer` / `require` with a proper certificate, or pin a CA via `tls.ca_cert_path`. Never hard-code credentials — supply `connection_url` / `connection_string` via secrets-manager interpolation (`${vault:…}`, `${aws-sm:…}`, …) or environment variables (`${env:…}`).

## Feature flags

This crate has no optional Cargo features of its own; enable it in the CLI / umbrella via the `sink-mssql` feature.

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| `MSSQL config requires either connection_url or connection_string` | Neither connection form set. Provide exactly one. |
| `MSSQL config sets both connection_url and connection_string` | Both forms set. Keep exactly one. |
| `MSSQL connection_url scheme must be mssql://` | Wrong scheme. Use `mssql://` (or `sqlserver://`). |
| Login fails with a parsed password | Special characters in the password aren't percent-encoded in the URL. Encode them (`@` → `%40`, `:` → `%3A`, `/` → `%2F`), or use `connection_string`. |
| TLS handshake fails against a self-signed dev server | Set `tls.type: trust_server_certificate` (dev only) or pin the CA with `tls.ca_cert_path`. |
| `create_table` rejected | `create_table` only works with `json_column`. For `auto_columns`, create the table yourself first (type inference is unsafe). |
| Unknown-key rows error out | `on_unknown_field` is `error`. Switch to `warn` / `drop`, or add the column to the table. |
| `IDENTITY` insert error | You included an identity column's value in a record. Drop it (the server generates identities) or run `SET IDENTITY_INSERT <table> ON`. |
| `mssql upsert: …` missing/null key | An upsert/delete row has a null or absent `key` column. Configure a `dlq:` to isolate those rows, or fix the data. |
| `json_column` with `upsert`/`delete` rejected | Key columns must be real table columns. Use `column_mapping: auto_columns` for keyed modes. |
| Frequent deadlock / lock-timeout retries | Reduce `batch_size`, lower write concurrency, or ensure the upsert `key` matches an index so `MERGE` doesn't escalate locks. |
| Effectively-once config rejected at `faucet validate` | All four gates must hold: a CDC source, this sink, a `state:` block, and **no** `dlq:`. The error names the offending row. |

## See also

- [Sinks reference & capability matrix](https://faucet-hq.github.io/faucet-stream/reference/connectors.html)
- [Upsert / write modes cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/upsert.html)
- [State & effectively-once cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/state.html)
- [Dead-letter queue cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/dlq.html)
- [`faucet-common-mssql`](https://crates.io/crates/faucet-common-mssql) — shared connection/TLS config
- [`faucet-source-mssql`](https://crates.io/crates/faucet-source-mssql) — the matching SQL Server source
- [`faucet-sink-postgres`](https://crates.io/crates/faucet-sink-postgres) · [`faucet-sink-mysql`](https://crates.io/crates/faucet-sink-mysql) · [`faucet-sink-sqlite`](https://crates.io/crates/faucet-sink-sqlite) — sibling SQL sinks


## Auto-create (`create_table`)

`create_table` (**default `true`**, #580) creates the target table from the
first written page's inferred columns when it does not exist — a first-ever
sync cannot assume the destination is already there. Every inferred column is
created **nullable**: a column present in page 1 is not required forever, and a
`NOT NULL` inferred from one page fails page 2 the first time a record omits
the field (narrowing later is the `schema:` drift policy's job). In `json_column` mode the shape is fixed; in `auto_columns` mode the columns come from the first page.

Set `create_table: false` to require a pre-existing target; a missing one then
fails fast with the same error every table sink raises, naming both ways out.

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.

## Overwrite (`write_mode: overwrite`)

Full-refresh: each run atomically **replaces** the whole table. Writes are
staged into a `SELECT … INTO … WHERE 1=0` clone (`{table}__faucet_ovw`) and
swapped in one transaction (`DELETE` + `INSERT` over the explicit non-IDENTITY
column list + `DROP`) only after the run succeeds, so a mid-run failure leaves
the previous rows intact. No `key` is needed; the target table must already
exist.
