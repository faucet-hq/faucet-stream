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

With `replication.type = incremental`, only rows whose `column` is strictly
greater than the stored bookmark (or `initial_value` on the first run) are
emitted. If the query contains the literal `${bookmark}` token it is replaced
with a positional bind so Redshift filters server-side; the source also filters
client-side as a correctness backstop. The new maximum of `column` is persisted
on the final page; it starts from the stored bookmark, so it never moves
backwards even when the row that held the old maximum is gone.

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
query: "SELECT * FROM events WHERE updated_at > ${bookmark} ORDER BY updated_at"
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
