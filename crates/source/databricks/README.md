# faucet-source-databricks

Databricks **SQL query source** for the [`faucet-stream`](https://crates.io/crates/faucet-stream)
ecosystem. Runs a SQL statement against a Databricks **SQL Warehouse** via the
[Statement Execution API](https://docs.databricks.com/api/workspace/statementexecution)
(plain REST — no JDBC/ODBC driver, no Python) and streams the result rows as
typed JSON objects.

This is the **query-results** read path for Databricks — joins, aggregates,
filtered extracts. For full-table lakehouse scans and the **write** path, use
the Delta Lake connectors ([`faucet-source-delta`](https://crates.io/crates/faucet-source-delta)
/ [`faucet-sink-delta`](https://crates.io/crates/faucet-sink-delta)). To load
*into* a Databricks SQL warehouse (Unity Catalog permissions, `COPY INTO`,
`MERGE`, exactly-once), use [`faucet-sink-databricks`](https://crates.io/crates/faucet-sink-databricks).

## Highlights

- **Async statement lifecycle** — submit → poll until terminal → stream result
  chunks (`EXTERNAL_LINKS` + `JSON_ARRAY`: each chunk is downloaded from its
  presigned URL without the workspace token), following the chunk links, so
  results of any size stream — `INLINE` results are capped at 25 MiB by
  Databricks.
- **Type-aware decode** from the response `manifest` column schema (every
  `JSON_ARRAY` cell is a string; decoded per `type_name` to typed JSON, with
  `DECIMAL`/large `LONG` preserved losslessly as strings). A result with two
  columns of the same name (a join's `id`) fails with an error asking you to
  alias them, rather than one silently replacing the other.
- **Incremental replication** — reads only rows not yet delivered, in cursor
  order, without skipping rows that share a cursor value; a crash replays at
  most one page (see [Incremental replication](#incremental-replication)).
- **Bearer auth** (PAT / OAuth M2M), inline or via the shared `auth:` catalog.
  The auth and Statement Execution API client live in
  [`faucet-common-databricks`](https://crates.io/crates/faucet-common-databricks)
  (shared with the sink). This crate keeps its own `DatabricksAuth` (same wire
  shape, converted with `From`) so its public API is unchanged.
- **Warm-up tolerant** — a submit refused with `429`/`503` (a serverless
  warehouse starting) is retried with backoff honouring `Retry-After`, polls
  retry transient `5xx`, and a `401` makes a shared provider mint a fresh token
  once.
- **Arrow-native fetch** — behind the `arrow` feature, `arrow_native: true`
  fetches results as `EXTERNAL_LINKS` + `ARROW_STREAM` and decodes each chunk
  as an Arrow IPC stream, enabling the columnar fast path and skipping the
  per-cell JSON decode on the row path. See
  [Arrow columnar (Parquet) mode](#arrow-columnar-parquet-mode).

## Configuration

| Field | Type | Default | Notes |
|---|---|---|---|
| `workspace_url` | string | — (required) | `https://<host>.cloud.databricks.com` |
| `warehouse_id` | string | — (required) | target SQL Warehouse id |
| `sql` | string | — (required) | the query; supports `:name` params and a `${bookmark}` token |
| `auth` | `{ type, config }` / `{ ref }` | — (required) | `pat` or `token` bearer, or a shared provider |
| `catalog` / `schema` | string? | — | default Unity Catalog catalog / schema |
| `parameters` | `[{name, value, type?}]` | `[]` | named `:name` SQL parameters |
| `wait_timeout_secs` | int | `50` | server wait before async (`0` or `5`–`50`) |
| `poll_interval_secs` | int | `1` | client poll cadence while running |
| `statement_timeout_secs` | int | `3600` | client deadline for the statement to finish; on expiry it is cancelled and the read fails (`0` = wait indefinitely) |
| `batch_size` | int | `1000` | rows per emitted page; `0` is the "no batching" sentinel — the whole result set is emitted in a single page |
| `result_disposition` | `external_links` \| `inline` | `external_links` | how the row path receives results. `external_links` downloads each chunk from a presigned cloud-storage URL (no size cap; the client must reach the workspace's storage); `inline` keeps results in the API response, which Databricks fails above 25 MiB. Ignored under `arrow_native`. |
| `arrow_native` | bool | `false` | fetch as `EXTERNAL_LINKS` + `ARROW_STREAM` and decode Arrow IPC; enables the columnar fast path. Requires the `arrow` feature and `replication: full`. See [Arrow columnar (Parquet) mode](#arrow-columnar-parquet-mode). |
| `replication` | `{ type: full \| incremental, column, initial_value }` | `full` | incremental cursor |
| `state_key` | string? | derived | explicit bookmark key. The query fingerprint changed in #839 (a fixed FNV-1a hash replaced Rust's `DefaultHasher`, which is not stable across releases), so a library pipeline relying on the derived key starts once from its initial position after upgrading; set `state_key` to pin it. Runs through the `faucet` CLI are unaffected — the CLI keys state by pipeline and row. |

```yaml
pipeline:
  source:
    type: databricks
    config:
      workspace_url: https://dbc-xxxx.cloud.databricks.com
      warehouse_id: 0123456789abcdef
      sql: SELECT id, ts FROM events
      auth: { type: pat, config: { token: "${env:DATABRICKS_TOKEN}" } }
      replication: { type: incremental, column: ts, initial_value: "2026-01-01" }
```

## Incremental replication

`replication: { type: incremental, column, initial_value }` wraps the SQL as
``SELECT * FROM (<sql>) AS q WHERE q.`<column>` >= :_faucet_bookmark ORDER BY q.`<column>` ``,
so the warehouse filters and orders by the cursor; `initial_value` is the
inclusive lower bound for the first run. To apply the cursor further inside
the SQL, write `${bookmark}` there with `>=` (it binds the same named
parameter); a `> ${bookmark}` is refused at load time because it would skip
rows that share the bookmark value.

**Rows that share a cursor value are never skipped.** The bookmark stores the
last cursor value **and** a 128-bit fingerprint of every row already emitted at
it — `{"value": "...", "boundary": ["<fingerprint>", ...]}`. The next read
re-reads that value with `>=` and drops only the rows it already emitted, so a
row committed later with the last `updated_at` is delivered. Above 10,000 rows
at one value the set is dropped and the value is re-read in full: duplicates,
never loss. Every page carries the bookmark after its last row, so a crash
replays at most one page. A bare bookmark stored by an earlier release reads
as that value with an empty boundary (its rows are re-delivered once). Rows
whose cursor is `NULL` are never read.

## Arrow columnar (Parquet) mode

Behind the crate-local `arrow` Cargo feature, setting `arrow_native: true`
submits the statement with `EXTERNAL_LINKS` disposition and `ARROW_STREAM`
format; each result chunk's presigned link is fetched and decoded as an Arrow
IPC stream rather than a `JSON_ARRAY`. This has two effects:

- the opt-in **columnar fast path** (RFC 0002 / #375) — when the sink is also
  Arrow-native (the [Parquet](https://crates.io/crates/faucet-sink-parquet) or
  [Delta Lake](https://crates.io/crates/faucet-sink-delta) sink) and no
  `Value`-shaped transform is configured, records move end-to-end as Arrow
  `RecordBatch`es with no `serde_json::Value` materialization; and
- a faster **row path** — even into a JSON sink, decoding Arrow batches skips
  the per-cell string→JSON decode the default `JSON_ARRAY` path pays.

`arrow_native: true` requires the `arrow` feature and only works with
`replication: full` — config validation rejects `arrow_native` combined with
incremental replication, because the columnar path does not track the
per-row cursor bookmark. The default (`arrow_native: false`) keeps the
`JSON_ARRAY` row path.

```yaml
# databricks(arrow) → parquet — runs Arrow end-to-end
pipeline:
  source:
    type: databricks
    config:
      workspace_url: https://dbc-xxxx.cloud.databricks.com
      warehouse_id: 0123456789abcdef
      sql: SELECT id, ts, payload FROM events
      auth: { type: pat, config: { token: "${env:DATABRICKS_TOKEN}" } }
      arrow_native: true   # requires the `arrow` feature; replication must be `full`
  sink:
    type: file
    config:
      path: ./out/
      format: parquet
```

Enable it with `cargo add faucet-source-databricks --features arrow` (library)
or `cargo install faucet-cli --features "source-databricks,arrow"` (CLI).

## Out of scope (v1)

`discover()` via `INFORMATION_SCHEMA` and reading from Unity Catalog Volumes.
To write to a SQL warehouse, use the
[`faucet-sink-databricks`](https://crates.io/crates/faucet-sink-databricks) sink.

License: MIT OR Apache-2.0.

## Capabilities

Declares columnar (Arrow) output only when the crate is built with the `arrow` feature and the config sets `arrow_native: true`; otherwise it declares nothing. `faucet list --json` prints these declared capabilities.

## Timeouts, cancellation and retries

Every request — statement submit and polls, result chunks, presigned
external links — runs through the pipeline's `CallContext` (#880): the run's
cancel token stops an in-flight request or poll (an abandoned statement is
cancelled on the warehouse), any caller deadline ends it, and each is metered as
a round trip (`submit`, `poll`, `chunk`). The HTTP client's connect / idle-read
timeouts, `statement_timeout_secs` and the `429` / `503` retry are unchanged.
