# faucet-sink-snowflake

[![Crates.io](https://img.shields.io/crates/v/faucet-sink-snowflake.svg)](https://crates.io/crates/faucet-sink-snowflake)
[![Docs.rs](https://docs.rs/faucet-sink-snowflake/badge.svg)](https://docs.rs/faucet-sink-snowflake)
[![MSRV](https://img.shields.io/crates/msrv/faucet-sink-snowflake.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-sink-snowflake.svg)](https://github.com/faucet-hq/faucet-stream#license)

**Snowflake** sink for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. Writes JSON records to a Snowflake table over the [Snowflake SQL REST API](https://docs.snowflake.com/en/developer-guide/sql-api/index) — no driver, no ODBC, no client library, just HTTPS.

Reach for it when you want to land records from any faucet-stream source — a REST API, a database, a file, a queue — into Snowflake with one declarative config. Each batch is parsed and inserted in a single `PARSE_JSON` + `FLATTEN` statement, and the JSON array travels as a bound `TEXT` parameter (`PARSE_JSON(?)`) rather than interpolated into SQL, so quote characters in your data are safe and cannot inject SQL.

## Feature highlights

- **Driverless SQL REST API** — pure HTTPS against `https://{account}.snowflakecomputing.com/api/v2/statements`. The HTTP client is built once in `new()` and reused for every request.
- **Two auth methods** — JWT **key-pair** (RS256, minted locally from your RSA private key with the public-key SHA-256 fingerprint in the `iss` claim) or **OAuth** bearer tokens from an external identity provider.
- **Shared auth providers** — the `auth` field also accepts `{ ref: <name> }`, pointing at a provider in the CLI's top-level `auth:` catalog (OAuth/bearer tokens only; key-pair JWT is always inline). N matrix rows hitting one IdP then share a single token.
- **Set-based batch inserts** — one `INSERT … SELECT … FROM TABLE(FLATTEN(input => PARSE_JSON(?)))` per chunk parses and inserts the whole batch in a single statement.
- **Configurable batching** — `batch_size` controls rows per request (default `1000`, the documented sweet spot); `batch_size: 0` forwards each upstream page untouched.
- **Async-execution aware** — when Snowflake answers an `INSERT` with HTTP 202 (queued, not yet run), the sink polls the statement handle until it succeeds before counting rows as written, bounded by `poll_timeout`.
- **Effectively-once delivery** — with `delivery: exactly_once`, each page's INSERT and a commit-token `MERGE` land in one multi-statement transaction, so a crash/resume never duplicates a page.
- **SQL-injection-safe** — data is bound, never interpolated; all table/schema/database identifiers are quoted.
- **Preflight probe** — `faucet doctor` runs a read-only `SELECT 1` to confirm credentials and warehouse access without writing rows.

## Installation

```bash
# As a library:
cargo add faucet-sink-snowflake

# Via the umbrella crate:
cargo add faucet-stream --features sink-snowflake

# In the CLI (opt-in connector feature):
cargo install faucet-cli --features sink-snowflake
```

## Quick start

```yaml
# pipeline.yaml — faucet run pipeline.yaml
version: 1
pipeline:
  source:
    type: s3
    config:
      bucket: my-data-lake
      prefix: raw/events/
      region: us-east-1
      file_format: json_lines
  sink:
    type: snowflake
    config:
      account: xy12345.us-east-1
      warehouse: LOAD_WH
      database: ANALYTICS
      schema: RAW
      table: EVENTS
      auth:
        type: key_pair
        config:
          user: LOADER
          private_key_pem: ${file:./snowflake_key.pem}
      batch_size: 1000
```

```bash
faucet run pipeline.yaml
```

## Configuration reference

### Core

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `account` | string | — *(required)* | Snowflake account identifier (e.g. `"xy12345.us-east-1"`). Used to build the API host `https://{account}.snowflakecomputing.com`. |
| `warehouse` | string | — *(required)* | Warehouse used for the session executing the inserts. |
| `database` | string | — *(required)* | Target database name. |
| `schema` | string | — *(required)* | Target schema name. |
| `table` | string | — *(required)* | Target table name. |
| `auth` | `AuthSpec<SnowflakeAuth>` | — *(required)* | Authentication — inline `{ type, config }` or `{ ref: <name> }`. See [Authentication](#authentication). |

### Batching & reliability

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `batch_size` | int | `1000` | Maximum records per SQL REST API request. The sink re-chunks each incoming slice into `batch_size` slices and issues one `INSERT` per chunk. **`0` = no batching**: the whole slice is sent in one `INSERT`, no matter how large. Values above `MAX_BATCH_SIZE` (1,000,000) are rejected by `faucet_core::validate_batch_size`. |
| `poll_timeout` | int (seconds) | `300` | Max wall-clock time to wait for an asynchronously-executed `INSERT` (HTTP 202) to finish before failing with `FaucetError::Sink`. **`0` = poll forever.** See [Asynchronous execution](#asynchronous-execution). |

## Authentication

`auth` uses the shared `SnowflakeAuth` enum (re-exported from [`faucet-common-snowflake`](https://crates.io/crates/faucet-common-snowflake), so it matches the Snowflake **source** byte-for-byte) in the project-wide `{ type, config }` shape. The `Debug` impl masks `private_key_pem` and `token` as `***`.

| `type` | `config` | Use when |
|--------|----------|----------|
| `key_pair` | `{ user: <string>, private_key_pem: <PEM string> }` | You have an RSA key pair registered on the Snowflake user. The sink mints an RS256 JWT locally (1-hour expiry, public-key SHA-256 fingerprint in the `iss` claim). |
| `oauth` | `{ token: <string> }` | You have an OAuth2 bearer token from an external IdP. Sent as `Snowflake Token="…"`. |

```yaml
# Key-pair JWT (PEM inlined from disk via the file: directive)
auth:
  type: key_pair
  config:
    user: INGEST_USER
    private_key_pem: ${file:./snowflake_key.pem}
```

```yaml
# OAuth bearer token via env indirection
auth:
  type: oauth
  config:
    token: ${env:SNOWFLAKE_TOKEN}
```

```yaml
# Shared provider from the top-level auth: catalog (OAuth/bearer only)
auth:
  ref: snowflake_idp
```

> **Note:** a shared `auth: { ref }` provider must yield a `Bearer` or `Token` credential, which maps onto `SnowflakeAuth::OAuth`. Key-pair JWT is stateless (minted locally from the RSA key) and therefore must always be supplied inline.

## Examples

### Postgres → Snowflake with key-pair auth

```yaml
version: 1
name: postgres_to_snowflake
pipeline:
  source:
    type: postgres
    config:
      connection_url: postgres://user:pass@localhost/app
      query: SELECT id, email, created_at FROM users WHERE tenant_id = $1
      params:
        - acme
  sink:
    type: snowflake
    config:
      account: xy12345.us-east-1
      warehouse: INGEST_WH
      database: ANALYTICS
      schema: RAW
      table: USERS
      auth:
        type: key_pair
        config:
          user: INGEST_USER
          private_key_pem: ${file:./snowflake_key.pem}
      batch_size: 500
```

### Latency-sensitive stream with small batches

```yaml
sink:
  type: snowflake
  config:
    account: xy12345.us-east-1
    warehouse: COMPUTE_WH
    database: MY_DB
    schema: PUBLIC
    table: realtime_events
    auth:
      type: oauth
      config: { token: ${env:SNOWFLAKE_TOKEN} }
    batch_size: 50      # smaller chunks → lower per-row latency
```

### Large load with no re-chunking

```yaml
sink:
  type: snowflake
  config:
    account: xy12345.us-east-1
    warehouse: LOAD_WH
    database: ANALYTICS
    schema: RAW
    table: EVENTS
    auth:
      type: key_pair
      config:
        user: LOADER
        private_key_pem: ${file:./snowflake_key.pem}
    batch_size: 0          # forward each upstream page as one INSERT
    poll_timeout: 900      # allow up to 15 min for a queued statement to run
```

## Streaming & batching

`SnowflakeSink::write_batch` re-chunks the incoming records slice into `batch_size` slices and issues one SQL REST API `INSERT` per chunk. The default of `1000` matches Snowflake's documented sweet spot for the SQL API — enough rows to amortize the round-trip and statement-parse cost without bloating the request body. Tune **up** for narrow rows where round-trip latency dominates, **down** for wide rows where request-body size dominates.

`batch_size = 0` is the **"no batching" sentinel**: `write_batch` forwards the entire slice as a single `INSERT`, so the upstream source's `StreamPage` framing flows through untouched. Use it when the source already emits pages sized for the target (e.g. one large page per write).

### Asynchronous execution

Snowflake's SQL REST API may answer a submitted `INSERT` with **HTTP 202 Accepted** — the statement was queued but has not yet executed. The sink does **not** count those rows as written at that point (that would report success before the data is durable). Instead it polls `GET /api/v2/statements/{handle}` until the statement reports success (code `090001`), then returns. The poll loop is bounded by `poll_timeout` (default 300 s): if the statement is still running after that budget, the write fails with `FaucetError::Sink` rather than hanging forever. Set `poll_timeout: 0` to poll indefinitely.

## Effectively-once delivery

`SnowflakeSink` implements `Sink::supports_idempotent_writes` (returns `true`) and the two companion hooks:

- `write_batch_idempotent(records, scope, token)` — writes the page's records and MERGEs the `token` into a `_faucet_commit_token("scope" STRING PRIMARY KEY, "token" STRING, "updated_at" TIMESTAMP_NTZ)` watermark table in the target database/schema, all inside **one multi-statement transaction** (`BEGIN; INSERT; MERGE; COMMIT;` with `MULTI_STATEMENT_COUNT` set on the request), so both either commit together or neither does. The watermark table is created (`CREATE TABLE IF NOT EXISTS`) as its own request once per sink instance — Snowflake DDL auto-commits, so it can never ride inside the transaction.
- `last_committed_token(scope)` — reads the current watermark so the pipeline skips already-committed pages on resume.

**The whole page is one atomic unit on this path** — `batch_size` re-chunking does not apply to `write_batch_idempotent` (core issues exactly one token per page; splitting the page across transactions would break the rows-plus-token atomicity). Size the *source's* `batch_size` down if a page's JSON payload grows too large for a single SQL REST API request. An empty page still advances the watermark via a commit-only `BEGIN; MERGE; COMMIT;` transaction.

To use effectively-once delivery, set `delivery: exactly_once` and pair this sink with a CDC source (`postgres-cdc`, `mysql-cdc`, `mongodb-cdc`) plus a `state:` block. A DLQ is not permitted in effectively-once mode. All four requirements are validated at config-load time (`faucet validate`) before any run starts.

```yaml
version: 1
pipeline:
  source:
    type: postgres-cdc
    config:
      connection_url: postgres://faucet:faucet@localhost:5432/appdb
      slot_name: faucet_slot
      publication_name: faucet_pub
  sink:
    type: snowflake
    config:
      account: xy12345.us-east-1
      warehouse: COMPUTE_WH
      database: ANALYTICS_DB
      schema: PUBLIC
      table: change_events
      auth:
        type: oauth
        config:
          token: ${env:SNOWFLAKE_TOKEN}
  state:
    type: file
    config:
      path: ./state
delivery: exactly_once
```

See the [effectively-once delivery cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/state.html#effectively-once-delivery).

## Config loading & schema

Load from YAML/JSON or environment, and inspect the full JSON Schema:

```bash
faucet schema sink snowflake
```

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_sink_snowflake::SnowflakeSinkConfig;

// From a JSON file
let config: SnowflakeSinkConfig = load_json("config.json")?;
// From an .env file with a prefix
let config: SnowflakeSinkConfig = load_env_file(".env", "SNOWFLAKE")?;
```

```env
SNOWFLAKE_ACCOUNT=xy12345.us-east-1
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=ANALYTICS_DB
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_TABLE=events
SNOWFLAKE_AUTH='{"type":"oauth","config":{"token":"eyJhbGciOiJSUzI1NiIs..."}}'
SNOWFLAKE_BATCH_SIZE=1000
```

## Library usage

```rust
use faucet_core::{Pipeline, Sink};
use faucet_sink_snowflake::{SnowflakeAuth, SnowflakeSink, SnowflakeSinkConfig};
use serde_json::json;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let config = SnowflakeSinkConfig::new(
    "xy12345.us-east-1",
    "COMPUTE_WH",
    "ANALYTICS_DB",
    "PUBLIC",
    "events",
    SnowflakeAuth::OAuth { token: std::env::var("SNOWFLAKE_TOKEN")? },
)
.with_batch_size(1000);

let sink = SnowflakeSink::new(config)?;

let records = vec![
    json!({"user_id": "u123", "event": "page_view", "ts": "2026-04-02T10:00:00Z"}),
    json!({"user_id": "u456", "event": "click",     "ts": "2026-04-02T10:01:00Z"}),
];
let rows_written = sink.write_batch(&records).await?;
println!("Wrote {rows_written} rows to Snowflake");
# Ok(())
# }
```

To drive it end-to-end, pair it with any source via `Pipeline::new(source, sink).run().await?`.

## How it works

1. `SnowflakeSink::new()` builds an HTTP client (reused across all requests). No network call happens at construction.
2. `write_batch()` splits records into `batch_size` chunks (or one chunk when `batch_size = 0`). For each chunk it builds an `INSERT` using `PARSE_JSON(?)` + `FLATTEN` and sends the chunk's JSON array as a **bound `TEXT` parameter**, parsing and inserting every row in one statement without interpolating data into the SQL text.
3. **Field-to-column mapping:** each record's top-level keys project into matching table columns — `INSERT INTO "db"."schema"."tbl" ("col1","col2") SELECT value:"col1"::string, value:"col2"::string FROM TABLE(FLATTEN(input => PARSE_JSON(?)))`. The `::string` cast strips the VARIANT's JSON quotes so Snowflake coerces each scalar into the destination column's type on insert (text → number / boolean / timestamp, etc.). The column set comes from the **first record**; a key absent from a later record is inserted as `NULL`. Target columns should be **scalar** — a key targeting a `VARIANT`/`OBJECT`/`ARRAY` column is stringified, not stored as structured JSON. Both column identifiers and JSON path keys are quote-escaped, so record keys cannot inject SQL.
4. The statement targets the fully-qualified `"database"."schema"."table"` with quoted identifiers.
5. Auth headers are generated per request: an RS256 JWT (1-hour expiry, public-key fingerprint in `iss`) for `key_pair`, or `Snowflake Token="…"` for `oauth`.
6. Success is confirmed by the `090001` response code; an HTTP 202 enters the [async-execution poll loop](#asynchronous-execution).

## Lineage dataset URI

`snowflake://<account>/<database>/<schema>?table=<table>` — e.g. `snowflake://xy12345.us-east-1/ANALYTICS_DB/PUBLIC?table=events`.

This connector reports metrics under the label `connector="snowflake"`.

## Feature flags

This crate has no optional features of its own; enable it in the CLI/umbrella via the `sink-snowflake` feature.

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| `Auth` error / 401 | Credentials rejected. For `key_pair`, confirm the **public** key is registered on the Snowflake user (`ALTER USER … SET RSA_PUBLIC_KEY=…`) and the PEM is a valid RSA private key (PKCS#8 `BEGIN PRIVATE KEY` or PKCS#1 `BEGIN RSA PRIVATE KEY`). For `oauth`, check the token isn't expired and matches the account's configured OAuth integration. Run `faucet doctor` for a one-shot `SELECT 1` probe. |
| `invalid RSA private key` | The `private_key_pem` body isn't a parseable RSA key — check the `${file:…}` path resolved and the PEM wasn't truncated/escaped. |
| `Snowflake auth provider must yield a bearer/token credential` | You pointed `auth: { ref }` at a non-bearer shared provider, or tried key-pair via a provider. Key-pair JWT must be inline; shared providers only supply OAuth/bearer tokens. |
| Rows silently land as `NULL` | A record key doesn't match a table column name (case-sensitive — Snowflake folds unquoted identifiers to uppercase). Ensure record keys match the **quoted** column names exactly. |
| Structured field stored as a JSON string | The target column isn't scalar. The `::string` cast stringifies `VARIANT`/`OBJECT`/`ARRAY` targets — land those into a `VARIANT` column with a transform, or split the field. |
| Write fails with `FaucetError::Sink: … timed out` | The queued statement didn't finish within `poll_timeout`. Raise `poll_timeout` (or set `0`), and make sure the `warehouse` is not suspended / can resume. |
| `batch_size` rejected | Values above `MAX_BATCH_SIZE` (1,000,000) are invalid. Use `0` for "no batching", or a value ≤ 1,000,000. |
| First record's columns don't cover all rows | The column set is taken from the first record. Put a record with the full key set first, or normalize keys with an upstream transform so every row shares the same shape. |

## See also

- [Snowflake source](https://crates.io/crates/faucet-source-snowflake) — query Snowflake via the same SQL REST API.
- [faucet-common-snowflake](https://crates.io/crates/faucet-common-snowflake) — the shared `SnowflakeAuth` enum and auth helpers.
- [Sinks reference](https://faucet-hq.github.io/faucet-stream/reference/connectors.html) — capability matrix across all connectors.
- [Authentication cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/auth.html) — the shared `auth:` provider catalog.
- [Secrets cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/secrets.html) — injecting the PEM / token from a secrets manager.


## Auto-create (`create_table`)

`create_table` (**default `true`**, #580) creates the target table from the
first written page's inferred columns when it does not exist — a first-ever
sync cannot assume the destination is already there. Every inferred column is
created **nullable**: a column present in page 1 is not required forever, and a
`NOT NULL` inferred from one page fails page 2 the first time a record omits
the field (narrowing later is the `schema:` drift policy's job). Columns are created as `STRING`: the insert path projects every value with `::string`, so a typed column would reject its own writer's cast.

Set `create_table: false` to require a pre-existing target; a missing one then
fails fast with the same error every table sink raises, naming both ways out.


## Commit accumulation (`commit_rows` / `commit_bytes`)

Records **accumulate across `write_batch` calls** and commit once per
threshold, plus once at `flush` (#617). Before this the commit unit was the
page unit, and `batch_size` could only ever *split* an oversized page — it
could never merge two undersized ones, so a small source page meant one
expensive warehouse operation per small page. Each `INSERT … SELECT` is a full warehouse query, so the per-query overhead — not the data — dominated a small-page run. For true bulk volume, configure `bulk_load:` to stage and `COPY` instead.

- `commit_rows` — records per commit. `None` (the default) accumulates the
  **whole run** into one commit.
- `commit_bytes` — estimated-bytes counterpart, bounding how much is buffered.

`batch_size` still bounds an individual request inside a commit group, so a
very large group is split into reasonably-sized requests.

**Only the append path accumulates.** `delivery: exactly_once` and the DLQ
path commit per page, because a commit token must land atomically with its own
page, and a DLQ must report which rows of *this* page failed — neither is
expressible once pages are merged.

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.

## Arrow columnar bulk load (opt-in, `arrow` feature)

With a binary built `--features arrow`, add a `bulk_load` block to switch the
sink onto the Arrow columnar fast path: incoming Arrow `RecordBatch`es are
written to Parquet, uploaded to an **external** stage's backing cloud storage,
and loaded with `COPY INTO <table> FROM @stage FILE_FORMAT=(TYPE=PARQUET)`. This
runs end-to-end without per-row JSON when the source is also columnar (e.g.
`parquet → snowflake`). It is **append-only** and independent of the row-path
`INSERT`/exactly-once code, which is unchanged.

```yaml
sink:
  type: snowflake
  config:
    account: ${env:SNOWFLAKE_ACCOUNT}
    warehouse: LOAD_WH
    database: ANALYTICS
    schema: PUBLIC
    table: EVENTS
    auth: { type: oauth, config: { token: ${env:SNOWFLAKE_TOKEN} } }
    bulk_load:
      stage: ANALYTICS.PUBLIC.EVENTS_STAGE   # existing external stage
      url: s3://my-bucket/snowflake-stage/   # the stage's backing location
      storage_options:                       # object_store keys for the upload
        aws_access_key_id: ${env:AWS_ACCESS_KEY_ID}
        aws_secret_access_key: ${env:AWS_SECRET_ACCESS_KEY}
        aws_region: us-east-1
      match_by_column_name: CASE_INSENSITIVE  # default
      purge: false                            # default; PURGE=TRUE removes staged files
```

Only external stages are supported — internal-stage `PUT` is a driver command
the SQL REST API cannot run. The Snowflake **source** has no Arrow path (the v2
SQL API is jsonv2-only). `bulk_load` on an `arrow`-off binary is rejected at
construction.
