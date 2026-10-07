# faucet-source-clickhouse

ClickHouse query **source** for the
[`faucet-stream`](https://crates.io/crates/faucet-stream) ecosystem.

Talks to ClickHouse over its
[HTTP interface](https://clickhouse.com/docs/en/interfaces/http) using
[`reqwest`](https://crates.io/crates/reqwest): runs a SQL `SELECT`, requests the
`JSONCompactEachRowWithNamesAndTypes` output format, and streams the response
body straight into `StreamPage`s. Response bytes are line-buffered and decoded incrementally, so
memory stays bounded (`batch_size` records per page) regardless of how large the
result set is.

## Configuration

```yaml
source:
  kind: clickhouse
  config:
    # Endpoint — either `url` OR `host` (+ optional `http_port` / `tls`).
    url: http://localhost:8123
    # host: localhost
    # http_port: 8123
    # tls: false
    database: default
    user: default          # optional; sent as X-ClickHouse-User
    password: ${env:CH_PASSWORD}   # optional; sent as X-ClickHouse-Key
    query: SELECT id, email, updated_at FROM events
    batch_size: 1000       # records per StreamPage; 0 = whole result as one page
```

Do **not** append a `FORMAT` clause to `query` — the source sets the output
format via the request settings.

### Exact numbers

The source asks the server to quote every 64-bit-and-wider integer and every
decimal (`output_format_json_quote_64bit_integers=1`,
`output_format_json_quote_decimals=1`) and decodes each cell by its column
type, so nothing is rounded through a float and the result does not depend on
the server's defaults:

| ClickHouse type | JSON |
|---|---|
| `Int8` … `Int64`, `UInt8` … `UInt64` (incl. `Nullable`/`LowCardinality`) | exact number |
| `Int128`, `Int256`, `UInt128`, `UInt256`, `Decimal(P, S)` | exact decimal string |
| everything else | as ClickHouse renders it |

An integer incremental cursor therefore orders numerically, and a decimal or
wide-integer cursor (a string) is compared by value, not as text.

### Authentication

Username + password (ClickHouse native HTTP auth), sent as the
`X-ClickHouse-User` / `X-ClickHouse-Key` headers (never URL query parameters, so
credentials do not leak into request logs).

## Incremental replication

Set `replication` to track a monotonically increasing column across runs. Only
rows whose column value is strictly greater than the stored bookmark (or
`initial_value` on the first run) are emitted, and the new maximum is persisted
on the final page.

```yaml
    replication:
      type: incremental
      column: updated_at
      initial_value: "1970-01-01T00:00:00Z"
    query: SELECT * FROM events WHERE updated_at > parseDateTime64BestEffort(@bookmark)
```

`DateTime` / `DateTime64` values are emitted as RFC 3339 in UTC
(`2024-01-01T04:30:00Z`, the `date_time_output_format=iso` setting), so a
non-UTC column zone never shifts them. ClickHouse will not compare a
`DateTime` column with an RFC 3339 string literal directly, so wrap the
bookmark in `parseDateTime64BestEffort(@bookmark)` for a `DateTime` cursor, and
write `initial_value` in the same RFC 3339 form so the client-side filter
orders it correctly. Float `NaN` / `±Inf` are emitted as the strings `"nan"`,
`"inf"` and `"-inf"` rather than `null`.

Put the literal `@bookmark` token in the `WHERE` clause to push the cursor down
to the server (efficient); it is substituted as an injection-safe SQL literal.
The source *also* filters client-side as a correctness backstop. If `@bookmark`
is omitted the cursor is applied client-side only, so the server returns the
whole result set on every run (correctness is preserved, but it is a full
re-scan — a warning is logged).

In a matrix child, reference parent-record values with `${parent.field}`
tokens; they are substituted as injection-safe SQL literals. Library callers
pass the same values as `{key}` tokens through `fetch_with_context`.

## Dataset discovery / sharding

Not supported in v1.

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your
option.
