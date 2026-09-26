# faucet-source-oracle-cdc

[![Crates.io](https://img.shields.io/crates/v/faucet-source-oracle-cdc.svg)](https://crates.io/crates/faucet-source-oracle-cdc)
[![Docs.rs](https://docs.rs/faucet-source-oracle-cdc/badge.svg)](https://docs.rs/faucet-source-oracle-cdc)
[![MSRV](https://img.shields.io/crates/msrv/faucet-source-oracle-cdc.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-source-oracle-cdc.svg)](https://github.com/faucet-hq/faucet-stream#license)

Oracle Database **change data capture** for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem, built on LogMiner (`DBMS_LOGMNR`) over the online and archived redo logs. Emits every committed insert, update and delete on the captured tables as a change envelope, resumable by SCN with no gap and no duplicate.

> **Runtime requirement:** Oracle Instant Client must be installed where ODPI-C can load it — see [`faucet-common-oracle`](https://crates.io/crates/faucet-common-oracle#runtime-requirement-oracle-instant-client).

## Highlights

- **Transactions emitted on commit only** — changes are buffered per transaction and released when its `COMMIT` is mined; rolled-back work and savepoint-rolled-back statements never appear. Each committed transaction is one page with its own bookmark.
- **SCN bookmarks** — resume reaches back to the start of any transaction still open at the bookmark, so long transactions spanning log switches are captured whole.
- **Exactly-once** — `supports_exactly_once() = true`; pair with an idempotent sink.
- **Snapshot handoff** — `capture_resume_position()` returns the current SCN for `faucet mirror`.
- **LOBs** — `CLOB`/`BLOB` values LogMiner reports in pieces (`LOB_WRITE`) are reassembled into the row.
- **Never skips silently** — missing redo fails the run naming the SCN range; changes LogMiner cannot render fail by default.

## Database prerequisites

1. **ARCHIVELOG mode**, with archived logs kept until faucet has mined them.
2. **Supplemental logging** — minimal logging at the database (SYSDBA, in the CDB root), and all-column logging on each captured table for full before/after images:

   ```sql
   ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
   ALTER TABLE APP.ORDERS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
   ```

   With only primary-key logging, updates carry the key and changed columns. A table that logs no key columns is rejected at startup.
3. **Privileges** for the capture user:

   ```sql
   GRANT CREATE SESSION, CREATE TABLE, LOGMINING, SELECT ANY TRANSACTION,
         SELECT_CATALOG_ROLE, EXECUTE_CATALOG_ROLE TO cdc_user;
   GRANT SELECT ON APP.ORDERS TO cdc_user;
   ```

   `CREATE TABLE` is for the flush table (below).
4. **Where to connect** — to the PDB that owns the tables (Oracle 21c+ mines per PDB and locates logs itself), or to a non-CDB / the CDB root (faucet registers the log files).

`faucet doctor` checks supplemental logging, full-row images and redo-log visibility, and prints the exact `ALTER` statement to run.

## Configuration

Connection fields are documented in [`faucet-common-oracle`](https://crates.io/crates/faucet-common-oracle#connection-settings).

| Field | Default | Description |
|---|---|---|
| `tables` | — | `OWNER.TABLE` list, in dictionary case. Required. |
| `start_position` | `{ type: current }` | Fresh-run start: `current` or `earliest` (oldest available redo). |
| `poll_interval` | `1` | Seconds between polls that find nothing. |
| `idle_timeout` | `30` | End the fetch cycle after this many quiet seconds. |
| `max_scn_window` | `500000` | Largest SCN range per LogMiner session. |
| `max_staged_records` | unbounded | Abort when one open transaction buffers more changes. |
| `batch_size` | `1000` | `0` = one trailing page with everything; otherwise a page per transaction. |
| `max_connections` | `2` | Pooled sessions. |
| `statement_timeout_secs` | `600` | Per-round-trip call timeout (`0` disables). |
| `flush_table` | `FAUCET_LOGMNR_FLUSH` | Table committed to before each window to force the log writer to flush redo up to the window's end. Created if missing. |
| `on_unsupported` | `fail` | `fail` or `skip` for changes LogMiner cannot render. |
| `state_key` | `oracle-cdc:<service>:<table>` | Bookmark key (a digest for several tables). |

```yaml
source:
  type: oracle-cdc
  config:
    host: db.example.com
    service_name: ORCLPDB1
    username: cdc_user
    password: ${secret:ORACLE_PASSWORD}
    tables: [APP.ORDERS, APP.ORDER_LINES]
```

## Change envelope

```json
{ "op": "u", "schema": "APP", "table": "ORDERS",
  "before": {"ID": 1, "STATUS": "new"}, "after": {"ID": 1, "STATUS": "paid"},
  "scn": 2146947, "commit_scn": 2146951, "xid": "07000C00F3010000",
  "ts": "2024-01-02T03:04:05" }
```

`op` is `i` / `u` / `d`; DDL on a captured table arrives as `ddl` (with `sql`) and `TRUNCATE` as `truncate`, both dropped by the `cdc_unwrap` transform. Values are typed from the column's data type exactly as the query source types them. `before` is the logged before image (`where` clause) and never includes LOB columns.

## Bookmark

```json
{ "commit_scn": 2146951, "restart_scn": 2146790, "committed_xids": ["07000C00F3010000"] }
```

Transactions committed below `commit_scn`, or at it and listed in `committed_xids`, were emitted. Mining resumes at `restart_scn`, the first SCN of the oldest transaction open at the bookmark.

## Failure behaviour

- **Missing redo** — if the logs covering the resume range were recycled or deleted, the run fails with the missing SCN range. Re-snapshot the tables, then restart capture.
- **Dictionary mismatch** — mining uses the current data dictionary (`DICT_FROM_ONLINE_CATALOG`). Redo written *before* a DDL on a captured table cannot be rendered after it; if a restart has to re-mine such redo the run fails (or skips, with `on_unsupported: skip`). Mining near real time avoids this; after DDL during downtime, re-snapshot the table.
- **Unsupported changes** (`UNSUPPORTED` operations, types LogMiner cannot render) fail by default.

## Mirroring

Pair with `cdc_unwrap` and an upsert-capable sink (`write_mode: upsert`, `delete_marker: { field: __op, values: [d] }`). For an initial load, `faucet mirror` captures `capture_resume_position()` before bulk-copying with the `oracle` query source, then streams changes from that SCN.

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.
