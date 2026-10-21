# faucet-source-oracle

[![Crates.io](https://img.shields.io/crates/v/faucet-source-oracle.svg)](https://crates.io/crates/faucet-source-oracle)
[![Docs.rs](https://docs.rs/faucet-source-oracle/badge.svg)](https://docs.rs/faucet-source-oracle)
[![MSRV](https://img.shields.io/crates/msrv/faucet-source-oracle.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-source-oracle.svg)](https://github.com/faucet-hq/faucet-stream#license)

Oracle Database **query source** for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. Runs parameterized SQL and streams rows as JSON pages, with incremental replication, PK-range sharding for clustered runs and catalog discovery. Built on the [`oracle`](https://crates.io/crates/oracle) driver (ODPI-C).

> **Runtime requirement:** Oracle Instant Client must be installed where ODPI-C can load it — see [`faucet-common-oracle`](https://crates.io/crates/faucet-common-oracle#runtime-requirement-oracle-instant-client).

## Highlights

- **Streaming fetch** — the fetch array size is the `batch_size`; rows are decoded on a blocking thread and handed over one page at a time, so memory stays at `O(batch_size)`.
- **Exact types** — `NUMBER` never loses precision (big values arrive as decimal strings); dates, timestamps, intervals, RAW/BLOB and CLOB are mapped explicitly ([type table](https://crates.io/crates/faucet-common-oracle#type-mapping)).
- **Incremental replication** — reads only rows not yet delivered, in cursor order, without skipping rows that share a cursor value; a crash replays at most one page.
- **PK-range sharding** — `shard: { key }` splits the query across cluster workers.
- **Discovery** — `faucet discover` lists tables from `ALL_TAB_COLUMNS` / `ALL_TABLES`.

## Configuration

Connection fields (`connect_string` or `host` + `service_name`/`sid`, `username`, `password`, `tls`, …) are documented in [`faucet-common-oracle`](https://crates.io/crates/faucet-common-oracle#connection-settings).

| Field | Default | Description |
|---|---|---|
| `query` | — | SQL to run. A trailing `;` is ignored. |
| `params` | `[]` | Values bound to `:1`, `:2`, … |
| `batch_size` | `1000` | Records per page and fetch array size. `0` = one page. |
| `max_connections` | `10` | Pooled sessions. |
| `statement_timeout_secs` | `300` | Per-round-trip call timeout (`0` disables). |
| `replication` | `{ type: full }` | Or `{ type: incremental, column, initial_value }`. |
| `state_key` | derived | Bookmark key; defaults to `oracle:<service>:<query hash>`. The query fingerprint changed in #839 (a fixed FNV-1a hash replaced Rust's `DefaultHasher`, which is not stable across releases), so a library pipeline relying on the derived key starts once from its initial position after upgrading; set `state_key` to pin it. Runs through the `faucet` CLI are unaffected — the CLI keys state by pipeline and row. |
| `shard` | — | `{ key: ID }` — integer output column for PK-range sharding. |
| `json_columns` | `[]` | Output columns whose text is JSON, emitted as parsed values. |

Oracle upper-cases unquoted identifiers, so output keys, `replication.column`, `shard.key` and `json_columns` use the names Oracle reports (e.g. `UPDATED_AT`).

```yaml
source:
  type: oracle
  config:
    host: db.example.com
    service_name: ORCLPDB1
    username: app
    password: ${secret:ORACLE_PASSWORD}
    query: >
      SELECT ID, STATUS, UPDATED_AT FROM APP.ORDERS
      WHERE UPDATED_AT >= :bookmark AND REGION = :1
    params: ["EMEA"]
    replication: { type: incremental, column: UPDATED_AT, initial_value: "1970-01-01T00:00:00" }
```

### Incremental replication

The source wraps the query as `SELECT * FROM (<query>) q WHERE q."<column>" >= :bookmark ORDER BY q."<column>"`, so Oracle filters and orders by the cursor (index it); `initial_value` is the inclusive lower bound for the first run. To apply the cursor further inside the query, write `:bookmark` there with `>=`; a `> :bookmark` is refused at load time because it would skip rows that share the bookmark value.

**Rows that share a cursor value are never skipped.** The bookmark stores the last cursor value **and** a 128-bit fingerprint of every row already emitted at it — `{"value": "...", "boundary": ["<fingerprint>", ...]}`. The next read re-reads that value with `>=` and drops only the rows it already emitted, so a row committed later with the same cursor value (common with `DATE`'s one-second resolution) is delivered, and a boundary row updated in place is emitted again. Above 10,000 rows at one value the set is dropped and the value is re-read in full: duplicates, never loss. Every page carries the bookmark after its last row, so a crash replays at most one page. A bare bookmark stored by an earlier release reads as that value with an empty boundary (its rows are re-delivered once). Rows whose cursor is `NULL` are never read, and a row that commits with a cursor *below* the stored bookmark is not seen — use the CDC source when that matters.

A bookmark from a `DATE` / `TIMESTAMP` / `TIMESTAMP WITH TIME ZONE` column (`YYYY-MM-DDTHH:MI:SS[.f][offset]`, the form this source emits) is bound as a timestamp, so the comparison does not depend on the session's `NLS_DATE_FORMAT`; any other bookmark binds as its JSON type. A text (`VARCHAR2`) cursor holding ISO-8601 strings produces the same shape: Oracle then converts the column to a timestamp and refuses the `T` before returning any row, and the source re-runs the query with the bookmark bound as text. A session whose `NLS_TIMESTAMP_FORMAT` accepts ISO strings compares such a column as timestamps instead; wrap the column (`TO_CHAR(col)`) or the bind (`TO_CHAR(:bookmark)`) to force a text comparison.

### Sharding

With `shard: { key: ID }` the cluster coordinator computes `MIN`/`MAX` of the key over the query and splits the range; each worker runs `SELECT * FROM (<query>) WHERE "ID" >= lo AND "ID" < hi`. The outermost shards are open-ended and exactly one also matches `ID IS NULL`, so every row is read exactly once. No effect outside clustered execution.

### JSON columns

Native `JSON` columns (21c+) cannot be fetched directly by the driver. Select `JSON_SERIALIZE(DOC RETURNING CLOB) AS DOC` and list `DOC` under `json_columns`; discovery generates exactly that query for tables with `JSON` columns.

## Dataset discovery

`discover()` enumerates tables in non-Oracle-maintained schemas (`ALL_USERS.ORACLE_MAINTAINED = 'N'`), with column types from `ALL_TAB_COLUMNS` and row estimates from optimizer statistics (`ALL_TABLES.NUM_ROWS`). Each dataset's `config_patch` is a `query` selecting it (plus `json_columns` when needed). Metadata only — no data scan.

## Preflight

`faucet doctor` checks out a session and runs `SELECT 1 FROM DUAL`.

## Library usage

```rust,no_run
use faucet_source_oracle::{OracleConnectionConfig, OracleSource, OracleSourceConfig};
use faucet_core::Source;

# async fn run() -> Result<(), faucet_core::FaucetError> {
let conn = OracleConnectionConfig::new("localhost", 1521, "FREEPDB1", "app", "secret");
let source = OracleSource::new(OracleSourceConfig::new(conn, "SELECT * FROM ORDERS")).await?;
let rows = source.fetch_all().await?;
# let _ = rows;
# Ok(())
# }
```

## Capabilities

Declares `discover`, and `shardable` when a `shard:` block is set. `faucet list --json` prints the declaration.

## Timeouts, cancellation and retries

Every query (and the discovery and shard-bounds reads) runs through the pipeline's `CallContext` (#880): the run's deadline and cancel token end the wait promptly, while `statement_timeout_secs` still bounds the blocking call on the server. Each query counts as one round trip (`query`, `discover`) in `faucet_source_roundtrips_total`. Reads are not retried.

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.
