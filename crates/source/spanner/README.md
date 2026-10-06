# faucet-source-spanner

Google Cloud Spanner query source connector for [faucet-stream](https://crates.io/crates/faucet-stream).

Runs a GoogleSQL query against a Spanner database via the streaming read API
(`ExecuteStreamingSql`) and emits rows as JSON with bounded memory. Supports
named bind parameters, stale reads, incremental replication via a monotonic
column bookmark, live dataset discovery (`faucet discover`), and PK-range
sharding for clustered (Mode B) execution.

```yaml
source:
  kind: spanner
  config:
    project_id: my-project
    instance: my-instance
    database: my-db
    query: "SELECT * FROM orders WHERE updated_at > TIMESTAMP(@bookmark)"
    replication:
      type: incremental
      column: updated_at
      initial_value: "1970-01-01T00:00:00Z"
sink:
  kind: jsonl
  config:
    path: ./orders.jsonl
```

## Configuration

| Field | Required | Default | Description |
|---|---|---|---|
| `project_id` | ✔ | — | GCP project that owns the Spanner instance. |
| `instance` | ✔ | — | Spanner instance ID. |
| `database` | ✔ | — | Database name within the instance. |
| `query` | ✔ | — | GoogleSQL query. Named parameters (`@name`) bind from `params`; `@bookmark` binds the incremental cursor. |
| `auth` | | `application_default` | Credentials: `{type: application_default}`, `{type: service_account_key_path, config: {path}}`, or `{type: service_account_key, config: {json}}`. |
| `params` | | `{}` | Named bind parameters. Values must be scalars (string / integer / float / boolean) — Spanner parameters are typed. |
| `batch_size` | | `1000` | Records per emitted page. `0` emits the whole result set as one page. |
| `exact_staleness_secs` | | — | Read at a timestamp this many seconds in the past (stale read, served by any replica) instead of a strong read. |
| `replication` | | `{type: full}` | `full` or `{type: incremental, column, initial_value}` — see below. |
| `state_key` | | derived | Explicit state-store key for the bookmark. Defaults to `spanner:<project>.<instance>.<database>:<query-fingerprint>`. |
| `max_sessions` | | `100` | Upper bound on pooled Spanner sessions (gRPC channels are sized at one per 100 sessions). |
| `emulator_host` | | — | Spanner emulator endpoint (`host:port`); plaintext + unauthenticated. See [Emulator](#emulator). |
| `shard` | | — | `{key: <INT64 column>}` — opt in to PK-range sharding for clustered execution. |

## Record shape

Each row becomes one JSON object keyed by column name:

| Spanner type | JSON |
|---|---|
| `INT64` | integer (lossless — Spanner string-encodes it on the wire) |
| `FLOAT64` / `FLOAT32` | number (`NaN` / `Infinity` become strings) |
| `BOOL` | boolean |
| `STRING` / `TIMESTAMP` (RFC 3339) / `DATE` / `UUID` / `INTERVAL` | string |
| `BYTES` / `PROTO` | base64 string |
| `NUMERIC` | string (precision preserved) |
| `JSON` | the parsed value (object / array / scalar) |
| `ARRAY<T>` | array |
| `STRUCT` | object keyed by field name |

## Incremental replication

```yaml
query: "SELECT * FROM orders WHERE updated_at > TIMESTAMP(@bookmark)"
replication:
  type: incremental
  column: updated_at
  initial_value: "1970-01-01T00:00:00Z"
```

Only rows whose `column` is strictly greater than the stored bookmark (or
`initial_value` on the first run) are emitted; the new maximum is persisted on
the final page via the pipeline's `state:` store. When the query contains the
named parameter `@bookmark`, the cursor is bound server-side (efficient);
without it the source still filters client-side but the server re-scans the
whole table every run (a warning is logged at config load).

**Type casts:** Spanner does not implicitly coerce parameter types. A string
bookmark compared against a `TIMESTAMP` column needs an explicit cast in the
query — `updated_at > TIMESTAMP(@bookmark)` — and likewise `DATE(@bookmark)`
for `DATE` columns. Integer bookmarks bind as `INT64` and compare directly.

**Cursor column type:** `INT64`, `TIMESTAMP`, `DATE` and `NUMERIC` all work.
Cursors are compared by value, not as text: `TIMESTAMP` values as instants
(Spanner trims trailing fractional zeros, so `…:00Z` sorts after `…:00.5Z` as
text) and `NUMERIC` strings as exact decimals (`"9" < "10"`). Bind a `NUMERIC`
cursor with `amount > CAST(@bookmark AS NUMERIC)`.

## Stale reads

Set `exact_staleness_secs: 15` to read at a bounded-staleness timestamp.
Stale reads can be served by any replica (offloading the leader) but will not
see rows committed within the window — for incremental replication this can
delay (never lose) rows, since the bookmark only advances over rows actually
read.

## Dataset discovery

`faucet discover` enumerates every base table in the database's default
schema from `INFORMATION_SCHEMA` (system schemas excluded) with column types
mapped to JSON Schema, and emits one matrix row per table with a
``{"query": "SELECT * FROM `table`"}`` config patch. Spanner exposes no cheap
row-count estimate, so descriptors carry none.

## Sharding (clustered Mode B)

```yaml
shard:
  key: id   # INT64 column present in the query's output
```

The cluster coordinator computes `MIN`/`MAX` of `key` over the query and
splits the range into contiguous half-open slices; each worker streams
`SELECT * FROM (<query>) AS _faucet_shard WHERE ...` for its slice. Boundary
shards are open-ended and exactly one shard also matches `key IS NULL`, so
concurrent inserts outside the enumerated range and NULL-key rows are read by
exactly one shard. Has no effect outside the cluster coordinator.

## Emulator

Point the source at the [Cloud Spanner emulator](https://cloud.google.com/spanner/docs/emulator)
(`gcr.io/cloud-spanner-emulator/emulator`, gRPC port 9010) with:

```yaml
emulator_host: "localhost:9010"
```

The connection is plaintext and unauthenticated. The process-global
`SPANNER_EMULATOR_HOST` env var is also honored when `emulator_host` is
unset. This crate's integration tests provision instance + database
programmatically against the emulator via testcontainers.

## License

MIT OR Apache-2.0
