# faucet-source-clickhouse

ClickHouse query **source** for the
[`faucet-stream`](https://crates.io/crates/faucet-stream) ecosystem.

Talks to ClickHouse over its
[HTTP interface](https://clickhouse.com/docs/en/interfaces/http) using
[`reqwest`](https://crates.io/crates/reqwest): runs a SQL `SELECT`, requests the
`JSONEachRow` output format, and streams the response body straight into
`StreamPage`s. Response bytes are line-buffered and decoded incrementally, so
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
format to `JSONEachRow` via the request settings.

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
      initial_value: "1970-01-01 00:00:00"
    query: SELECT * FROM events WHERE updated_at > @bookmark
```

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
