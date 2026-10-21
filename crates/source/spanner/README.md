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
    query: "SELECT * FROM orders WHERE updated_at >= TIMESTAMP(@bookmark)"
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
| `state_key` | | derived | Explicit state-store key for the bookmark. Defaults to `spanner:<project>.<instance>.<database>:<query-fingerprint>`. The query fingerprint changed in #839 (a fixed FNV-1a hash replaced Rust's `DefaultHasher`, which is not stable across releases), so a library pipeline relying on the derived key starts once from its initial position after upgrading; set `state_key` to pin it. Runs through the `faucet` CLI are unaffected — the CLI keys state by pipeline and row. |
| `max_sessions` | | `100` | Upper bound on pooled Spanner sessions (gRPC channels are sized at one per 100 sessions). |
| `emulator_host` | | — | Spanner emulator endpoint (`host:port`); plaintext + unauthenticated. See [Emulator](#emulator). |
| `shard` | | — | `{key: <INT64 column>}` — opt in to PK-range sharding for clustered execution. |

## Record shape

Each row becomes one JSON object keyed by column name:

| Spanner type | JSON |
|---|---|
| `INT64` | integer (lossless — Spanner string-encodes it on the wire) |
| `FLOAT64` / `FLOAT32` | number (`NaN` / `Infinity` become a `null` counted in the run summary (`non_finite`)) |
| `BOOL` | boolean |
| `STRING` / `TIMESTAMP` (RFC 3339) / `DATE` / `UUID` / `INTERVAL` | string |
| `BYTES` / `PROTO` | base64 string |
| `NUMERIC` | string (precision preserved) |
| `JSON` | the parsed value (object / array / scalar) |
| `ARRAY<T>` | array |
| `STRUCT` | object keyed by field name |

## Incremental replication

```yaml
query: "SELECT * FROM orders WHERE updated_at >= TIMESTAMP(@bookmark)"
replication:
  type: incremental
  column: updated_at
  initial_value: "1970-01-01T00:00:00Z"
```

Each run reads the rows not yet read, in `column` order, from the stored
bookmark (or `initial_value`, an inclusive lower bound, on the first run). The
source wraps the query as

```sql
SELECT * FROM (<query>) AS q
WHERE q.`updated_at` >= CAST(@_faucet_bookmark AS TIMESTAMP)
ORDER BY q.`updated_at`
```

(the cast type is the cursor column's type, read from the result metadata of
a `LIMIT 0` probe query at the start of each run) and drops the rows it already
emitted at the bookmark value. So rows that share a cursor value — ties, or a
row committed late at the boundary — are never skipped. Every page carries a
bookmark `{value, boundary}` (the boundary lists fingerprints of the rows read
at `value`), so a crash replays at most one page. Above 10,000 rows at one
value the boundary is dropped and the next run re-reads that value
(duplicates, never loss). Rows whose `column` is NULL are never read.

Writing `@bookmark` in the query is optional; use it to apply the cursor
inside the query (e.g. before a join), with `>=`. A strict comparison
(`> @bookmark`, `> TIMESTAMP(@bookmark)`, `@bookmark < c`) is refused at load
time, because it skips rows that share the bookmark value.

**Upgrading:** a bookmark stored by an earlier release (a bare value) reads as
a bookmark with an empty boundary, so the first run after upgrading
re-delivers the rows at that value once. Queries written with `>` must switch
to `>=`.

**Type casts:** Spanner does not implicitly coerce parameter types. Inside
your query, `@bookmark` binds by its JSON type, so a string bookmark compared
against a `TIMESTAMP` column needs an explicit cast — `updated_at >=
TIMESTAMP(@bookmark)` — and likewise `DATE(@bookmark)` for `DATE` columns or
`CAST(@bookmark AS NUMERIC)` for `NUMERIC`. Integer bookmarks bind as `INT64`
and compare directly. The outer filter casts for you.

**Cursor column type:** `INT64`, `FLOAT64`, `FLOAT32`, `NUMERIC`, `STRING`,
`BOOL`, `DATE`, `TIMESTAMP` and `BYTES` work; `JSON`, `ARRAY` and `STRUCT`
columns are refused when the run starts.

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

## Capabilities

Declares `discover`, and `shardable` when the config has a `shard:` block. `faucet list --json` prints these declared capabilities.

## Timeouts, cancellation and retries

The transaction begin, the query and every page of rows run through the
pipeline's `CallContext` (#880): the run's cancel token stops a stalled read,
any caller deadline ends it, and the begin and query are metered as round trips
(`begin`, `query`). Rows are read a page at a time per call, so the per-row cost
is unchanged; nothing is retried by faucet.

## License

MIT OR Apache-2.0
