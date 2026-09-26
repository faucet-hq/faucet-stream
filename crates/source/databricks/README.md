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
  chunks (INLINE + `JSON_ARRAY`), following `next_chunk_internal_link`.
- **Type-aware decode** from the response `manifest` column schema (every
  `JSON_ARRAY` cell is a string; decoded per `type_name` to typed JSON, with
  `DECIMAL`/large `LONG` preserved losslessly as strings).
- **Incremental replication** — a bookmark column + a `${bookmark}` token bound
  as a server-side named parameter, plus a client-side filter backstop.
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
| `batch_size` | int | `1000` | rows per emitted page; `0` is the "no batching" sentinel — the whole result set is emitted in a single page |
| `arrow_native` | bool | `false` | fetch as `EXTERNAL_LINKS` + `ARROW_STREAM` and decode Arrow IPC; enables the columnar fast path. Requires the `arrow` feature and `replication: full`. See [Arrow columnar (Parquet) mode](#arrow-columnar-parquet-mode). |
| `replication` | `{ type: full \| incremental, column, initial_value }` | `full` | incremental cursor |
| `state_key` | string? | derived | explicit bookmark key |

```yaml
pipeline:
  source:
    type: databricks
    config:
      workspace_url: https://dbc-xxxx.cloud.databricks.com
      warehouse_id: 0123456789abcdef
      sql: SELECT id, ts FROM events WHERE ts > ${bookmark}
      auth: { type: pat, config: { token: "${env:DATABRICKS_TOKEN}" } }
      replication: { type: incremental, column: ts, initial_value: "2026-01-01" }
```

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
  the per-cell string→JSON decode the default `INLINE` + `JSON_ARRAY` path pays.

`arrow_native: true` requires the `arrow` feature and only works with
`replication: full` — config validation rejects `arrow_native` combined with
incremental replication, because the columnar path does not run the per-row
client-side incremental filter. The default (`arrow_native: false`) keeps the
unchanged `INLINE` + `JSON_ARRAY` row path.

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
    type: parquet
    config:
      path: ./out/
```

Enable it with `cargo add faucet-source-databricks --features arrow` (library)
or `cargo install faucet-cli --features "source-databricks,arrow"` (CLI).

## Out of scope (v1)

`discover()` via `INFORMATION_SCHEMA`, Unity Catalog Volumes, and a Databricks
SQL sink (use the Delta sink). (The `EXTERNAL_LINKS` large-result disposition is
available via `arrow_native` — see above.)

License: MIT OR Apache-2.0.
