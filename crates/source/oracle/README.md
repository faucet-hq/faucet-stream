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
- **Incremental replication** — a bookmark column, pushed down with `:bookmark`.
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
| `state_key` | derived | Bookmark key; defaults to `oracle:<service>:<query hash>`. |
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
      WHERE UPDATED_AT > :bookmark AND REGION = :1
    params: ["EMEA"]
    replication: { type: incremental, column: UPDATED_AT, initial_value: "1970-01-01T00:00:00" }
```

### Incremental replication

Rows whose `column` exceeds the stored bookmark (or `initial_value` on the first run) are emitted; the new maximum is persisted with the final page, after everything before it was written. Put `:bookmark` in the `WHERE` clause to filter server-side — without it the source still filters client-side but re-reads the whole result set (a warning is logged).

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

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.
