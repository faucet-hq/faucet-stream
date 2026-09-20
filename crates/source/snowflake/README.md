# faucet-source-snowflake

[![Crates.io](https://img.shields.io/crates/v/faucet-source-snowflake.svg)](https://crates.io/crates/faucet-source-snowflake)
[![Docs.rs](https://docs.rs/faucet-source-snowflake/badge.svg)](https://docs.rs/faucet-source-snowflake)
[![MSRV](https://img.shields.io/crates/msrv/faucet-source-snowflake.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-source-snowflake.svg)](https://github.com/faucet-hq/faucet-stream#license)

**Snowflake** query source for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. Executes a SQL statement against Snowflake's [SQL REST API](https://docs.snowflake.com/en/developer-guide/sql-api/intro) (`POST /api/v2/statements`), decodes each row into a typed `serde_json::Value`, and streams the result set back partition-by-partition so peak memory stays bounded no matter how large the result is.

Reach for it when you want to pull tables, aggregates, or ad-hoc query results out of Snowflake and land them in any faucet-stream sink — a file, a database, a warehouse, a queue — with one declarative config and no glue code. It talks to Snowflake over HTTPS only, so there is no driver, no ODBC, and no native dependency to build.

## Feature highlights

- **JWT key-pair and OAuth bearer authentication** — sign requests with an RSA key pair (RS256 JWT minted per request) or hand Snowflake an OAuth bearer token from an external IdP. The shared `SnowflakeAuth` enum is re-exported from [`faucet-common-snowflake`](https://crates.io/crates/faucet-common-snowflake) so it matches the Snowflake **sink** byte-for-byte, and credentials are masked in `Debug` output.
- **Server-side partition pagination** — large result sets are paged via `GET /api/v2/statements/{handle}?partition=N`; one HTTP round-trip per partition, with no client-side buffering of partitions you haven't reached yet.
- **Native streaming** — overrides `Source::stream_pages`: partitions are re-framed into `batch_size`-sized pages on the fly, so peak memory is `O(batch_size)` regardless of total row count.
- **Async-statement aware** — when the initial `POST` returns `202 Accepted` (the statement is still running), the source polls the statement handle until the result is ready, bounded by `poll_timeout` so a stuck statement fails cleanly instead of hanging forever.
- **Typed positional bind parameters** — `params` from config plus any `${parent.path}` matrix-context values are sent as Snowflake bind variables, typed from the JSON value (`FIXED` for integers, `REAL` for floats, `BOOLEAN` for bools, `TEXT` otherwise) so a numeric or boolean bind compares correctly against a typed column.
- **Type-aware row decoding** — `FIXED`, `REAL`, `BOOLEAN`, and `VARIANT`/`OBJECT`/`ARRAY` columns are parsed into native JSON shapes; everything else (timestamps, dates, binary) passes through as strings. **Full precision is preserved for `NUMBER`/`DECIMAL`/`NUMERIC`:** a fractional column (any `NUMBER(p,s)` with scale `s > 0`, including all monetary/decimal columns) is decoded as a **string** carrying the exact decimal text, since a JSON number is an `f64` and would silently lose precision — matching the BigQuery source's `NUMERIC`/`BIGNUMERIC` handling. Scale-0 (`NUMBER(p,0)`) values are JSON integers, except a value beyond `u64` which is likewise kept as a string.
- **Shared auth catalog** — OAuth tokens can come from the CLI's top-level `auth:` catalog via `auth: { ref: <name> }`, so many matrix rows hitting one IdP share a single token with single-flight refresh.

## Installation

```bash
# As a library:
cargo add faucet-source-snowflake

# In the CLI (opt-in connector feature):
cargo install faucet-cli --features source-snowflake
```

The connector is opt-in — it is not in the CLI's default feature set. Enable `source-snowflake` (or the `source` / `full` aggregates) to compile it in.

## Quick start

```yaml
# pipeline.yaml — faucet run pipeline.yaml
version: 1
pipeline:
  source:
    type: snowflake
    config:
      account: xy12345.us-east-1
      warehouse: COMPUTE_WH
      database: ANALYTICS
      schema: PUBLIC
      auth:
        type: oauth
        config:
          token: ${env:SNOWFLAKE_OAUTH_TOKEN}
      query: |
        SELECT id, name, created_at
        FROM events
        WHERE created_at >= ?
      params:
        - "2026-01-01"
  sink:
    type: jsonl
    config:
      path: ./events.jsonl
```

```bash
faucet run pipeline.yaml
```

## Configuration reference

### Core

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `account` | string | — *(required)* | Snowflake account identifier (e.g. `"xy12345.us-east-1"`). Used to build the API hostname and to sign the key-pair JWT. |
| `warehouse` | string | — *(required)* | Virtual warehouse to run the statement on. |
| `database` | string | — *(required)* | Database for the session. |
| `schema` | string | — *(required)* | Schema for the session. |
| `role` | string | *(unset)* | Optional role to assume for the session. When unset, the user's default role is used. |
| `auth` | `AuthSpec<SnowflakeAuth>` | — *(required)* | Authentication — see [Authentication](#authentication). Accepts an inline `{ type, config }` block or `{ ref: <name> }` pointing at a shared provider. |
| `query` | string | — *(required)* | SQL to execute. May contain positional `?` bind markers (bound from `params` + matrix context) and `${field.path}` placeholders resolved against the parent-record context at runtime. |
| `params` | array | `[]` | Positional bind parameters, applied in order **before** any context-derived values. Each entry is typed from its JSON value. |

### Reliability & timeouts

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `statement_timeout` | int (seconds) | `60` | Per-statement server-side timeout, passed as the `timeout` field on the `POST /api/v2/statements` body. Independent of the HTTP-level request timeout. |
| `poll_timeout` | int (seconds) | `300` | Max wall-clock time the source spends polling an asynchronous statement (one whose initial `POST` returned HTTP 202) before failing with `FaucetError::Source`. **`0` disables the cap** (poll forever). |

### Batching

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `batch_size` | int | `1000` | Records per emitted `StreamPage`. Snowflake returns the result in *partitions*; the source re-frames them into `batch_size`-sized pages. **`0` = no batching**: the entire result set is buffered and emitted in a single page (good for small lookup tables or sinks that prefer one large request). Capped at `MAX_BATCH_SIZE` (1,000,000); validated at config-load time. |

## Authentication

`auth` uses the shared `SnowflakeAuth` enum (the project-wide `{ type, config }` shape):

| `type` | `config` | Use when |
|--------|----------|----------|
| `oauth` | `{ token: <string> }` | You have an OAuth bearer token from Snowflake or an external IdP. Sent as `Authorization: Snowflake Token="..."` with `X-Snowflake-Authorization-Token-Type: OAUTH`. |
| `key_pair` | `{ user: <string>, private_key_pem: <pem> }` | You authenticate with an RSA key pair registered on the Snowflake user. A fresh RS256 JWT (1-hour expiry) is minted per request and sent as `Authorization: Bearer <jwt>` with `X-Snowflake-Authorization-Token-Type: KEYPAIR_JWT`. |

### OAuth bearer token

```yaml
auth:
  type: oauth
  config:
    token: ${env:SNOWFLAKE_OAUTH_TOKEN}
```

### Key-pair JWT

```yaml
auth:
  type: key_pair
  config:
    user: SVC_LOAD
    private_key_pem: ${file:/etc/secrets/snowflake.pem}
```

### Shared provider (CLI `auth:` catalog)

A shared provider must yield a `Bearer` or `Token` credential, which maps onto `SnowflakeAuth::OAuth`. Key-pair JWT is always inline (it cannot be shared via the catalog).

```yaml
auth:
  my_snowflake_idp:
    type: oauth2
    config:
      token_url: https://idp.example.com/oauth/token
      client_id: ${env:SF_CLIENT_ID}
      client_secret: ${secret:SF_CLIENT_SECRET}
pipeline:
  source:
    type: snowflake
    config:
      account: xy12345.us-east-1
      warehouse: COMPUTE_WH
      database: ANALYTICS
      schema: PUBLIC
      auth: { ref: my_snowflake_idp }
      query: SELECT 1
```

## Examples

### Incremental pull with a typed bind parameter

```yaml
source:
  type: snowflake
  config:
    account: xy12345.us-east-1
    warehouse: COMPUTE_WH
    database: ANALYTICS
    schema: PUBLIC
    auth:
      type: oauth
      config: { token: ${env:SNOWFLAKE_OAUTH_TOKEN} }
    query: |
      SELECT id, name, score
      FROM users
      WHERE score > ?
    params:
      - 0.75          # bound as REAL, compares correctly against a numeric column
```

### Large export with no re-chunking

```yaml
source:
  type: snowflake
  config:
    account: xy12345.us-east-1
    warehouse: COMPUTE_WH
    database: WAREHOUSE
    schema: PUBLIC
    auth:
      type: key_pair
      config:
        user: SVC_LOAD
        private_key_pem: ${file:/etc/secrets/snowflake.pem}
    query: SELECT * FROM daily_snapshot
    batch_size: 0      # emit the whole result set as one page
```

### Daily window driven by a `${now.*}` token

```yaml
source:
  type: snowflake
  config:
    account: xy12345.us-east-1
    warehouse: COMPUTE_WH
    database: ANALYTICS
    schema: PUBLIC
    role: ANALYST
    auth:
      type: oauth
      config: { token: ${env:SNOWFLAKE_OAUTH_TOKEN} }
    query: |
      SELECT country, COUNT(*) AS n
      FROM events
      WHERE event_date = ?
      GROUP BY country
    params:
      - "${now.date}"           # resolved per run, e.g. 2026-06-17
    statement_timeout: 30       # return fast; if not done, poll…
    poll_timeout: 600           # …for up to 10 minutes before failing
```

## Streaming & batching

The source overrides `Source::stream_pages`. It submits the statement, then walks the result partitions reported by Snowflake (the first partition arrives inline in the `POST` response; subsequent partitions are fetched via `GET /api/v2/statements/{handle}?partition=N`). Decoded rows accumulate in a buffer and are yielded as a `StreamPage` every time the buffer reaches `batch_size`, so peak memory is one page. With `batch_size: 0` the entire result set is buffered and emitted in a single page.

This is a one-shot query source — it has no incremental bookmark / resume support, so each run re-executes the query. For incremental loads, encode the watermark in the query (e.g. `WHERE created_at >= ?`) and drive it from a `params` entry, a matrix context, or a `${now.*}` token.

## Dataset discovery

The source supports live introspection via `Source::discover()`: it runs one catalog statement (through the same SQL REST API path as ordinary queries, async-202 polling and partition paging included) that enumerates every base table in the configured database from `information_schema.columns` / `information_schema.tables` (the `INFORMATION_SCHEMA` schema itself is excluded), and returns one dataset descriptor per table with:

- `name` — `SCHEMA.TABLE` (e.g. `PUBLIC.ORDERS`), `kind: table`
- `schema` — the column shape as a JSON-Schema object (`TEXT`/`DATE`/`TIMESTAMP_*`/`BINARY` → string; `NUMBER`/`FLOAT` → number; `BOOLEAN` → boolean; `VARIANT`/`OBJECT` → object; `ARRAY` → array; nullable columns become `["T", "null"]`). Note `NUMBER(38,0)` maps to the conservative `number`, not `integer`.
- `estimated_rows` — the table's `row_count` from `information_schema.tables` (omitted when NULL)
- `config_patch` — `{ "query": "SELECT * FROM \"SCHEMA\".\"TABLE\"" }`, double-quote-quoted (interior quotes doubled), ready to deep-merge over the connection config as a matrix row

Discovery reads catalog metadata only — it never scans table data.

## Config loading & schema introspection

Configs load from YAML/JSON files, environment variables, or `.env` files via the helpers in `faucet_core::config`. Inspect the full JSON Schema with:

```bash
faucet schema source snowflake
```

## Library usage

```rust
use faucet_core::Source;
use faucet_source_snowflake::{SnowflakeAuth, SnowflakeSource, SnowflakeSourceConfig};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let cfg = SnowflakeSourceConfig::new(
    "xy12345.us-east-1",
    "COMPUTE_WH",
    "ANALYTICS",
    "PUBLIC",
    SnowflakeAuth::OAuth { token: std::env::var("SNOWFLAKE_OAUTH_TOKEN")? },
    "SELECT id, name FROM events LIMIT 10",
)
.with_role("ANALYST")
.with_batch_size(0);

let rows = SnowflakeSource::new(cfg).fetch_all().await?;
println!("got {} rows", rows.len());
# Ok(())
# }
```

## How it works

1. `new()` builds the reusable HTTPS client and derives the API base URL from `account`.
2. `POST /api/v2/statements` submits the SQL with `statement_timeout` as the body `timeout`, the session `warehouse` / `database` / `schema` / `role`, and any positional bindings (typed from their JSON values). The `Authorization` header is minted per request — a fresh RS256 JWT for key-pair auth, or the OAuth bearer token wrapped as `Snowflake Token="..."`.
3. If the statement is still running, Snowflake returns `202 Accepted` with a handle; the source polls the handle until the result is ready or `poll_timeout` elapses.
4. Result partitions are walked via `?partition=N`; each cell is decoded by its column metadata into a typed JSON value (full precision preserved for large `NUMBER`s).
5. Rows are re-framed into `batch_size` pages and streamed to the pipeline.

## Lineage dataset URI

`snowflake://<account>/<database>/<schema>?query=<sql>` — e.g. `snowflake://xy12345.us-east-1/DB/PUBLIC?query=SELECT id FROM orders`.

## Feature flags

This crate has no optional features of its own; enable it in the CLI/umbrella via the `source-snowflake` feature (included in the `source` and `full` aggregates).

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| `Auth` error / 401 | OAuth token expired or invalid, or the key-pair public key isn't registered on the Snowflake user. Refresh the token, or confirm `ALTER USER … SET RSA_PUBLIC_KEY` matches your `private_key_pem`. |
| `403` / "insufficient privileges" | The session role can't read the queried objects, or `warehouse` access is denied. Set `role` to one with `USAGE` on the warehouse and `SELECT` on the objects. |
| Statement hangs, then fails with `FaucetError::Source: poll timeout` | The statement ran longer than `poll_timeout` (default 300 s). Raise `poll_timeout` (or set `0` to poll indefinitely), and/or resize the warehouse so the query finishes sooner. |
| `390201` / "no active warehouse" | `warehouse` is misspelled, suspended, or the role lacks `USAGE` on it. Verify the warehouse name and grants. |
| Account / host not found (DNS or 404) | `account` is wrong. Use the full account identifier including region/cloud (e.g. `xy12345.us-east-1`, or `orgname-accountname`), not the login URL. |
| A `NUMBER`/`DECIMAL` column arrives as a string | Intentional — a fractional `NUMBER(p,s)` (scale `s > 0`) and a scale-0 value beyond `u64` are decoded as strings to preserve full precision (a JSON number is an `f64`). Cast in a downstream transform if you need a JSON number and can accept the precision loss. |
| Bind parameter compared as text unexpectedly | Each `params` entry is typed from its JSON value. Pass `0.75` (number) not `"0.75"` (string) to bind a `REAL`. |
| `auth: { ref }` rejected for key-pair | Key-pair JWT can't come from the shared `auth:` catalog — only OAuth (`Bearer`/`Token`) credentials map to a shared provider. Inline the `key_pair` block instead. |

## See also

- [Connector reference & capability matrix](https://faucet-hq.github.io/faucet-stream/reference/connectors.html)
- [Authentication cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/auth.html) — shared `auth:` catalog and the `{ type, config }` shape
- [Secrets interpolation](https://faucet-hq.github.io/faucet-stream/cookbook/secrets.html) — `${env:…}` / `${file:…}` / `${secret:…}`
- [`faucet-common-snowflake`](https://crates.io/crates/faucet-common-snowflake) — the shared `SnowflakeAuth` enum and header helpers
- [`faucet-sink-snowflake`](https://crates.io/crates/faucet-sink-snowflake) — write rows back to Snowflake


## Partition read throughput (`partition_concurrency`)

Snowflake splits a large result into server-chosen partitions, each its own
`GET …/partition={n}`. Those used to be fetched one at a time, making the read
latency-bound on the round trip rather than throughput-bound on the data — and
the partition count is the *server's* choice, not something `batch_size` can
influence.

`partition_concurrency` (default `4`; `0`/`1` mean sequential) fetches them
with a bounded look-ahead. Partitions are consumed **in order**, so the emitted
row order is unchanged — a result set with an `ORDER BY` is not silently
reordered by a throughput knob — and a failure is still attributed to the
partition that caused it. The look-ahead holds up to this many partitions'
decoded rows, so raise it for throughput and lower it for memory.

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.
