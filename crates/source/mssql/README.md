# faucet-source-mssql

[![Crates.io](https://img.shields.io/crates/v/faucet-source-mssql.svg)](https://crates.io/crates/faucet-source-mssql)
[![Docs.rs](https://docs.rs/faucet-source-mssql/badge.svg)](https://docs.rs/faucet-source-mssql)
[![MSRV](https://img.shields.io/crates/msrv/faucet-source-mssql.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-source-mssql.svg)](https://github.com/faucet-hq/faucet-stream#license)

Microsoft **SQL Server** query source for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. Runs a parameterized T-SQL statement over a pooled [`tiberius`](https://crates.io/crates/tiberius) connection, decodes each row into a typed `serde_json::Value`, and streams the result set back page-by-page so memory stays bounded regardless of how many rows the query returns.

Reach for it when you want to pull tables, views, or ad-hoc query results out of SQL Server (or Azure SQL) and land them in any faucet-stream sink — a file, another database, a warehouse, a queue — with one declarative config and no glue code. Built-in incremental replication lets repeat runs pick up only the rows that changed since last time.

## Feature highlights

- **Native row streaming** — overrides `Source::stream_pages` to walk `tiberius`'s `QueryStream` row-by-row, buffering into pages of `batch_size` and yielding each page as it fills. Peak memory is `O(batch_size)`, never the full result set.
- **Connection pooling** — a [`bb8`](https://crates.io/crates/bb8) + [`bb8-tiberius`](https://crates.io/crates/bb8-tiberius) pool is built once in `new()` and reused for every query; size it with `max_connections` (default 10).
- **Incremental / bookmark replication** — track a monotonic column (e.g. `updated_at`) and emit only rows strictly greater than the stored bookmark on each run. The cursor pushes down to the server via the `@bookmark` token *and* is enforced client-side as a correctness backstop. If you configure `incremental` replication but omit the `@bookmark` token from the query, the run still produces correct rows (client-side filtering), but the server returns the **entire table every run** — so `faucet validate` / load warns you to add `@bookmark` to the `WHERE` clause for server-side pushdown.
- **Positional bind parameters** — `params` are sent as `@P1`, `@P2`, … bind values, and `${parent.path}` matrix-context placeholders resolve to additional bind markers at runtime — no string interpolation into SQL.
- **Type-aware row decoding** — integers, floats, `BIT`, decimals (precision-preserving strings), dates/times, `DATETIMEOFFSET`, `UNIQUEIDENTIFIER`, and binary (base64) all map to sensible JSON.
- **Four TLS modes** — `prefer` (default), `require`, `trust_server_certificate`, `disable` — with an optional CA cert path, shared with the MSSQL sink via [`faucet-common-mssql`](https://crates.io/crates/faucet-common-mssql).
- **Per-query timeout** — `statement_timeout_secs` bounds each query (default 300s; `0` disables).
- **Credentials never logged** — the `Debug` impl masks the connection config, and the lineage URI strips credentials.

## Installation

```bash
# As a library:
cargo add faucet-source-mssql

# In the CLI (opt-in connector feature):
cargo install faucet-cli --features source-mssql
```

This is an opt-in connector — it is not in the CLI default build. Enable it with the `source-mssql` feature (or the `source` / `full` aggregates).

## Quick start

```yaml
# pipeline.yaml — faucet run pipeline.yaml
version: 1
pipeline:
  source:
    type: mssql
    config:
      connection_url: "mssql://sa:Str0ng%40Pass@localhost:1433/sales"
      query: "SELECT id, email, updated_at FROM dbo.users"
  sink:
    type: file
    config:
      path: ./users.jsonl
```

```bash
faucet run pipeline.yaml
```

> URL credentials with special characters must be percent-encoded (`@` → `%40`, `:` → `%3A`, `/` → `%2F`).

## Configuration reference

### Core

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `connection_url` | string | — | `mssql://user:pass@host:1433/database` URL form. **Mutually exclusive** with `connection_string`; set exactly one. Host required; port defaults to `1433`; database optional. Credentials are percent-decoded. |
| `connection_string` | string | — | ADO.NET-style string handed straight to `tiberius`, e.g. `Server=tcp:host,1433;Database=db;User Id=sa;Password=...;`. **Mutually exclusive** with `connection_url`. The `tls` block is applied on top. |
| `query` | string | — *(required)* | T-SQL to run. Use `@P1`, `@P2`, … for `params`, and the literal `@bookmark` token to bind the incremental cursor server-side. |
| `params` | array | `[]` | Positional bind parameters (`@P1`…`@Pn`), typed from each JSON value. Sent in declaration order, before any matrix-context values. |

### Batching & reliability

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_connections` | int | `10` | Maximum pooled connections. |
| `batch_size` | int | `1000` | Records per emitted `StreamPage`. **`0` = no batching**: the entire result set is emitted as a single page (good for small lookup tables, or sinks that prefer one large request). |
| `shard` | object | *(unset)* | Optional [Mode B sharding](#sharded-execution-cluster-mode-b): `{ key: <integer column> }`. Opts the source into primary-key range splitting under `faucet serve --cluster`; no effect on a plain `faucet run`. |
| `statement_timeout_secs` | int (seconds) | `300` | Bounds the query's first response and every wait for the next row. **`0` disables** (wait indefinitely). |
| `state_key` | string | *(derived)* | Explicit state-store key for the incremental bookmark. When unset, a stable key is derived from the connection host plus a fingerprint of the query. |

### Replication

`replication` is a tagged enum (`{ type: full | incremental, … }`):

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `replication.type` | `full` \| `incremental` | `full` | `full` re-fetches the whole result set every run; `incremental` emits only rows past the bookmark. |
| `replication.column` | string | — *(required for `incremental`)* | The column whose value is the replication cursor (e.g. `updated_at`). Must be non-empty. |
| `replication.initial_value` | any JSON | — *(required for `incremental`)* | Lower bound used on the first run, before any bookmark is stored. |

### TLS

`tls` matches the YAML shape `tls: { type: <mode>, ca_cert_path: <path> }`:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `tls.type` | enum | `prefer` | Encryption mode — see the table below. |
| `tls.ca_cert_path` | path | *(unset)* | Optional CA certificate (PEM/DER) to trust for server validation. Ignored when `tls.type` is `disable`. |

| `tls.type` | Behavior |
|------------|----------|
| `prefer` | Encrypt; fails against a server that offers no TLS (same as `require`). The default. |
| `require` | Require encryption; fail if the server does not offer it. |
| `trust_server_certificate` | Encrypt and accept the server certificate **without validating its chain** (self-signed dev servers). **Insecure against MITM — never use in production.** |
| `disable` | No transport encryption. |

## Authentication

SQL Server authentication (username + password) only, supplied inline in the `connection_url` or `connection_string`. Windows / Integrated authentication and Azure AD / Managed Identity are out of scope in v1.

```yaml
# URL form — percent-encode special characters in the password
config:
  connection_url: "mssql://sa:Str0ng%40Pass@localhost:1433/sales"
```

```yaml
# ADO.NET connection-string form
config:
  connection_string: "Server=tcp:db.example.com,1433;Database=sales;User Id=sa;Password=${env:MSSQL_PASSWORD};"
```

Keep secrets out of the YAML by referencing the environment or a secrets manager — e.g. `Password=${env:MSSQL_PASSWORD}` or `${vault:secret/mssql#password}`.

## Examples

### Full table export to JSONL

```yaml
version: 1
pipeline:
  source:
    type: mssql
    config:
      connection_url: "mssql://sa:Str0ng%40Pass@localhost:1433/sales"
      query: "SELECT id, email, created_at FROM dbo.users"
      batch_size: 5000
  sink:
    type: file
    config:
      path: ./users.jsonl
```

### Incremental replication with a bookmark

```yaml
version: 1
name: mssql_to_jsonl
pipeline:
  source:
    type: mssql
    config:
      connection_url: "mssql://sa:Str0ng%40Pass@localhost:1433/sales"
      query: "SELECT id, email, updated_at FROM dbo.users WHERE updated_at > @bookmark ORDER BY updated_at"
      batch_size: 1000
      tls:
        type: trust_server_certificate   # self-signed dev cert; use prefer/require in prod
      replication:
        type: incremental
        column: updated_at
        initial_value: "1970-01-01T00:00:00Z"
  sink:
    type: file
    config:
      path: ./out/users.jsonl
      if_exists: append
  state:
    type: file
    config:
      path: ./.faucet-state
```

### Parameterized query with bind values

```yaml
version: 1
pipeline:
  source:
    type: mssql
    config:
      connection_url: "mssql://sa:Str0ng%40Pass@localhost:1433/sales"
      query: "SELECT id, total FROM dbo.orders WHERE region = @P1 AND total > @P2"
      params:
        - "EMEA"
        - 100
  sink:
    type: stdout
    config:
      format: jsonl
```

### Hardened production connection

```yaml
version: 1
pipeline:
  source:
    type: mssql
    config:
      connection_url: "mssql://etl_user:${env:MSSQL_PASSWORD}@sql.internal:1433/warehouse"
      query: "SELECT * FROM dbo.fact_sales"
      max_connections: 20
      statement_timeout_secs: 120
      tls:
        type: require
        ca_cert_path: /etc/ssl/certs/corp-ca.pem
  sink:
    type: file
    config:
      path: ./fact_sales.parquet
```

## Streaming & batching

The source overrides `Source::stream_pages`: it checks out a pooled connection, runs the query, and walks `tiberius`'s row stream. Rows are decoded and buffered until the buffer reaches `batch_size`, at which point a `StreamPage` is yielded; the final page carries the incremental bookmark (when replication is enabled) so the pipeline persists state only after every prior page has been written by the sink. With `batch_size: 0` the whole result set is buffered and emitted as one page.

The `batch_size` argument the pipeline passes to `stream_pages` is informational — the config field is authoritative, so a pipeline-supplied hint never overrides an explicit value.

## Resume & state

When `replication.type: incremental`, the source is resumable:

- `state_key()` returns the configured `state_key`, or a key derived from the connection host plus a query fingerprint (e.g. `mssql:db.example.com:<hex>`).
- Before fetching, the pipeline loads the stored bookmark and hands it to `apply_start_bookmark()`, which overrides the configured `initial_value` for that run.
- Each page is filtered to rows strictly greater than the bookmark on `replication.column`, and the running maximum is persisted on the final page **after** the sink confirms the batch — so a crash mid-run never advances the bookmark past unwritten rows.

**Server-side pushdown:** put the literal `@bookmark` token in your `WHERE` clause (e.g. `WHERE updated_at > @bookmark ORDER BY updated_at`). The source binds the cursor there as a parameter so SQL Server does the filtering. **Backstop:** whether or not `@bookmark` appears, the source *also* filters client-side, so correctness never depends on the query text — though without `@bookmark` the full result set is fetched and filtered in memory (fine for small tables, prefer `@bookmark` for large ones). `@bookmark` is a reserved token; don't use it as an identifier.

To use resume from the CLI, add a `state:` block (`file`, `memory`, `redis`, or `postgres`) to the pipeline as shown in the incremental example above.

## Dataset discovery

The source supports live introspection via `Source::discover()`: it enumerates every base table visible to the connection (from `INFORMATION_SCHEMA.COLUMNS` / `INFORMATION_SCHEMA.TABLES`) and returns one dataset descriptor per table with:

- `name` — `schema.table` (e.g. `dbo.orders`), `kind: table`
- `schema` — the column shape as a JSON-Schema object (types mapped per the table below; nullable columns become `["T", "null"]`)
- `estimated_rows` — a cheap row estimate from `sys.partitions` (heap/clustered-index partitions only). If the `sys.*` views aren't readable for your principal, discovery still succeeds — the estimate is simply omitted.
- `config_patch` — `{ "query": "SELECT * FROM [schema].[table]" }`, bracket-quoted (interior `]` doubled), ready to deep-merge over the connection config as a matrix row

Discovery reads catalog metadata only — it never scans table data.

## Type mapping

| MSSQL | JSON |
|-------|------|
| TINYINT / SMALLINT / INT / BIGINT | number |
| REAL / FLOAT | number |
| BIT | bool |
| DECIMAL / NUMERIC / MONEY | string (precision-preserving; MONEY/SMALLMONEY as exact 4-decimal text — the driver delivers MONEY as a float, which is exact below about ±900 billion, the range we render without loss) |
| CHAR / VARCHAR / NCHAR / NVARCHAR / TEXT / NTEXT / XML | string |
| DATE | `YYYY-MM-DD` |
| TIME | ISO time |
| DATETIME / SMALLDATETIME | ISO 8601, milliseconds (`2024-03-01T12:30:45.007`, as SQL Server displays it), so the value round-trips as a `@bookmark` |
| DATETIME2 | ISO 8601 (no offset), full fractional precision |
| DATETIMEOFFSET | RFC 3339 (offset preserved) |
| UNIQUEIDENTIFIER | hyphenated string |
| BINARY / VARBINARY / IMAGE | base64 string |
| SQL NULL | `null` |

## Config loading & schema

Load from YAML/JSON or environment. Inspect the full JSON Schema with:

```bash
faucet schema source mssql
```

## Library usage

```rust
use faucet_core::Source;
use faucet_source_mssql::{MssqlSource, MssqlSourceConfig};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let cfg = MssqlSourceConfig::new(
    "mssql://sa:Str0ng%40Pass@localhost:1433/sales",
    "SELECT id, email, updated_at FROM dbo.users",
);

let source = MssqlSource::new(cfg).await?;
let rows = source.fetch_all().await?;
println!("rows: {rows:?}");
# Ok(())
# }
```

For incremental replication, set `cfg.replication` to `MssqlReplication::Incremental { column, initial_value }` and drive the source through a `Pipeline` with a `StateStore` so the bookmark persists across runs. The shared connection/TLS types (`MssqlConnectionConfig`, `MssqlTls`, `MssqlTlsMode`) are re-exported from this crate, so you don't need to depend on `faucet-common-mssql` directly.

## How it works

1. `new()` validates the config, then builds the `bb8` + `bb8-tiberius` connection pool **once** via `faucet-common-mssql::build_pool` (TLS mode applied).
2. Each query checks out a pooled connection, binds `params` (and the bookmark, when `@bookmark` is present) as typed `@Pn` parameters, and runs under `statement_timeout_secs`.
3. `tiberius`'s `QueryStream` is walked row-by-row; each row is decoded by its column type into JSON and buffered into `batch_size` pages.
4. For incremental runs, each page is filtered against the bookmark and the running maximum is tracked; the bookmark is emitted on the final page so the pipeline persists it only after the sink confirms.
5. `connector_name()` reports `"mssql"` for metrics labels; `check()` probes a pooled connection for `faucet doctor`.

## Lineage dataset URI

`<connection>?query=<sql>` with credentials stripped — e.g. `mssql://db.example.com:1433/sales?query=SELECT id FROM orders`. Emitted automatically when lineage is enabled in the CLI.

## Feature flags

This crate has no optional features of its own; enable it in the CLI/umbrella via the `source-mssql` feature (included in the `source` and `full` aggregates, but not in the CLI default build).

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| `Config: MSSQL config requires either connection_url or connection_string` | Neither was set. Provide exactly one. |
| `Config: MSSQL config sets both connection_url and connection_string` | Both were set. Remove one — they are mutually exclusive. |
| `Config: invalid MSSQL connection_url` / scheme error | The URL is malformed or uses the wrong scheme. Use `mssql://` (or `sqlserver://`) and percent-encode special characters in the password (`@` → `%40`, `:` → `%3A`, `/` → `%2F`). |
| `Source: MSSQL pool checkout failed` / connect timeout | Server unreachable, wrong host/port, bad credentials, or a TLS mismatch. Verify the server is listening on `1433`, the login works, and the `tls` mode matches what the server offers. |
| TLS handshake fails against a self-signed dev server | Set `tls.type: trust_server_certificate` for local dev (never in production), or point `tls.ca_cert_path` at the server's CA. |
| `Source: MSSQL query timed out` | The query ran longer than `statement_timeout_secs`. Raise the timeout (or set `0` to disable), and add the right indexes / `WHERE` clause. |
| Incremental run re-emits old rows | No bookmark is being persisted — add a `state:` block, and make sure `replication.column` is the same monotonic column you filter on. |
| Incremental run is slow on a large table | Add `@bookmark` to the `WHERE` clause so filtering pushes down to the server instead of fetching every row and filtering in memory. |
| `Config: MSSQL incremental replication requires a non-empty column` | `replication.type` is `incremental` but `column` is empty. Set it to the cursor column. |
| A `DECIMAL`/`MONEY` column arrives as a string | Intentional — decimals are decoded as strings to preserve full precision. Cast in a downstream transform if you need a JSON number. |
| Windows / Azure AD login required | Not supported in v1 — only SQL Server username/password authentication. |

## See also

- [SQL Server connector docs](https://faucet-hq.github.io/faucet-stream/reference/connectors.html)
- [State & incremental replication cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/state.html)
- [Configuration grammar reference](https://faucet-hq.github.io/faucet-stream/reference/config.html)
- [`faucet-common-mssql`](https://crates.io/crates/faucet-common-mssql) — shared connection/TLS/pool types.
- [`faucet-sink-mssql`](https://crates.io/crates/faucet-sink-mssql) — the matching SQL Server sink.

## Sharded execution (cluster Mode B)

Under [`faucet serve --cluster`](https://faucet-hq.github.io/faucet-stream/cookbook/cluster.html),
a top-level `shard: { count: N }` block splits this source into contiguous
primary-key ranges that different cluster workers process concurrently. Opt in
by naming an integer-typed key column:

```yaml
shard:
  count: 8
pipeline:
  source:
    type: mssql
    config:
      connection_url: ${env:MSSQL_URL}
      query: "SELECT * FROM events"
      shard: { key: id }   # integer column to range-partition on
```

The coordinator computes `MIN(key)` / `MAX(key)` once and splits that range
into half-open slices (`[key]` in the generated predicate, injection-safe).
The boundary shards stay open-ended so rows inserted outside the captured
range during the run are still read, and exactly one shard additionally
matches `key IS NULL` so nullable keys are never silently dropped. Each shard
keeps its own state key (`{run}::{shard}`), so a reassigned shard resumes
where its previous owner left off.

Outside the cluster coordinator the `shard` config has **no effect** — a plain
`faucet run` streams the whole query.

> A sharded query must not end in a top-level `ORDER BY` — T-SQL forbids it
> inside the derived table the shard predicate wraps (ordering across
> concurrently-executing shards is meaningless anyway). With incremental
> replication, shard bounds are computed over the not-yet-synced slice (the
> `@bookmark` binding is honoured during enumeration).

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.
