# faucet-source-redshift

Amazon Redshift query source connector for the
[`faucet-stream`](https://crates.io/crates/faucet-stream) ecosystem.

Redshift speaks the PostgreSQL wire protocol, so this source connects through
`sqlx`'s Postgres driver, runs a configurable SQL query, and streams the result
rows as JSON objects with `O(batch_size)` memory. It supports **full** and
**incremental** (bookmark-based) replication.

## Configuration

| Field | Required | Description |
|-------|----------|-------------|
| `host` | yes | Cluster endpoint host. |
| `port` | no | Port (default `5439`). |
| `database` | yes | Database name. |
| `user` | yes | Database user. |
| `credentials` | yes | `{ type: password, config: { password: … } }`. `iam` / `redshift_data_api` are reserved (not yet supported). |
| `tls` | no | Require TLS (default `true`, certificate not verified; `false` → `sslmode=prefer`). |
| `tls_mode` | no | `disable` \| `prefer` \| `require` \| `verify_ca` \| `verify_full`; overrides `tls`. |
| `ssl_root_cert` | no | CA PEM path for `verify_ca` / `verify_full`. |
| `query` | yes | SQL query. May contain `${field.path}` context tokens and, for incremental mode, `${bookmark}`. |
| `params` | no | Positional bind values (`$1, $2, …`) applied before context/bookmark values. |
| `max_connections` | no | Pool size (default `10`). |
| `batch_size` | no | Rows per emitted page (default `1000`; `0` = one page). |
| `replication` | no | `{ type: full }` (default) or `{ type: incremental, column, initial_value }`. |
| `state_key` | no | Explicit bookmark key; otherwise derived from host/database/query. The query fingerprint changed in #839 (a fixed FNV-1a hash replaced Rust's `DefaultHasher`, which is not stable across releases), so a library pipeline relying on the derived key starts once from its initial position after upgrading; set `state_key` to pin it. Runs through the `faucet` CLI are unaffected — the CLI keys state by pipeline and row. |

## Incremental replication

With `replication.type = incremental` the source reads only the rows not yet
delivered, in cursor order. It wraps the query as
`SELECT * FROM (<query>) AS q WHERE q."<column>" >= CAST($n AS <type>) ORDER BY q."<column>"`,
where `<type>` is the cursor column's type as the server describes the query
(so a string bookmark compares as a `TIMESTAMP`, a number as an `INT8`, …);
`initial_value` is the inclusive lower bound for the first run. To apply the
cursor further inside the query, write `${bookmark}` there with `>=`; it binds
the same way, and a `> ${bookmark}` is refused at load time because it would
skip rows that share the bookmark value.

**Rows that share a cursor value are never skipped.** The bookmark stores the
last cursor value **and** a 128-bit fingerprint of every row already emitted at
it — `{"value": "...", "boundary": ["<fingerprint>", ...]}`. The next read
re-reads that value with `>=` and drops only the rows it already emitted, so a
second COPY batch stamped with the same load time, or any row committed later
with the last `updated_at`, is delivered. Above 10,000 rows at one value the
set is dropped and the value is re-read in full: duplicates, never loss. Every
page carries the bookmark after its last row, so a crash replays at most one
page, and the bookmark never moves backwards (a run that reads nothing keeps
it). A bare bookmark stored by an earlier release reads as that value with an
empty boundary (its rows are re-delivered once). Rows whose cursor is `NULL`
are never read.

## Type decoding

Integers, floats, booleans, JSON and text map to JSON natively; `NUMERIC` is an
exact decimal string, timestamps/dates/times are ISO strings, `TIMETZ` keeps its
offset, `INTERVAL` is an ISO 8601 duration (`P1Y2DT3H1.5S`) and `VARBYTE` is
base64, and `oid` / `reg*` values are numbers. Any other type is emitted as
its text form when the server sends it as text, or when its wire form *is* its
text (`SUPER`, `XML`, character types); a binary-encoded value of any other
type (`GEOMETRY`'s EWKB, an `int[]`) is refused rather than garbled. A
non-NULL value the source cannot decode fails the read naming the column — cast
it in the query (`col::varchar`) — rather than becoming `null`.

```yaml
host: my-cluster.abc123.us-east-1.redshift.amazonaws.com
database: dev
user: admin
credentials:
  type: password
  config:
    password: ${env:REDSHIFT_PASSWORD}
query: "SELECT * FROM events"
replication:
  type: incremental
  column: updated_at
  initial_value: "2026-01-01T00:00:00Z"
```

## Testing

Redshift has no local container image, but it speaks the PostgreSQL wire
protocol, so `tests/integration.rs` runs the read path against a Postgres
container (Docker required).

License: MIT OR Apache-2.0

## Capabilities

A plain query source: no exactly-once, discovery or sharding. `faucet list --json` prints the declaration.

## Timeouts, cancellation and retries

The query and every page of rows read from its cursor run through the
pipeline's `CallContext` (#880): the run's cancel token stops a stalled read,
any caller deadline ends it, and the query is metered as a `query` round trip.
Rows are read a page at a time per call, so the per-row cost is unchanged; the
pool's connect timeout still applies and nothing is retried.
