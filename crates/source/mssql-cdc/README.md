# faucet-source-mssql-cdc

Microsoft SQL Server **CDC (change data capture)** source for the
[`faucet-stream`](https://crates.io/crates/faucet-stream) ecosystem.

It polls native SQL Server change data capture — `sys.fn_cdc_get_max_lsn()` for
the high-water LSN, then `cdc.fn_cdc_get_all_changes_<capture_instance>()` per
configured capture instance — and emits per-row change events as CDC envelopes.
Progress is a durable, per-capture-instance **LSN bookmark**, so a resumed run
never re-reads an already-committed change.

## Highlights

- **Native CDC, LSN-driven.** Advances by Log Sequence Number using the built-in
  `cdc.fn_cdc_get_all_changes_*` table-valued functions — no triggers, no
  polling of the base table.
- **Per-transaction durability.** Change rows are buffered by commit LSN
  (`__$start_lsn`); each committed transaction is emitted as its own
  `StreamPage` with a bookmark, so nothing partially-visible ever leaks.
- **Resumable & exactly-once capable.** The bookmark is a monotonic LSN; on
  resume the next poll starts at `increment(bookmark)`. `supports_exactly_once()`
  is `true`, and `capture_resume_position()` anchors CDC before a bulk snapshot
  for `faucet replicate`.
- **Preflight checks.** `new()` (and `faucet doctor`) verify CDC is enabled on
  the database and that every configured capture instance exists, with
  actionable errors.

## Configuration

```yaml
pipeline:
  source:
    type: mssql-cdc
    config:
      # faucet-common-mssql connection block (connection_url OR connection_string + tls)
      connection_url: "mssql://faucet:${env:MSSQL_PASSWORD}@sqlserver:1433/sales"
      tls: { type: trust_server_certificate }

      capture_instances: ["dbo_Orders", "dbo_Items"]  # required, non-empty
      start_position: { type: current }                # current (default) | earliest
      poll_interval: 1                                 # seconds between empty polls
      idle_timeout: 30                                 # end the fetch cycle after this much quiet
      batch_size: 1000                                 # 0 = one aggregate page (test/snapshot)
      max_staged_records: 500000                       # cap one in-progress transaction
      max_connections: 5
      statement_timeout_secs: 300
      # state_key: "mssql-cdc:sales:dbo_Orders"        # optional override
```

| Field | Default | Notes |
|-------|---------|-------|
| `capture_instances` | — (required) | Capture-instance names (`sys.sp_cdc_enable_table` defaults to `<schema>_<table>`). Only `[A-Za-z0-9_]` accepted (injected into the function name). |
| `start_position` | `current` | `current` skips existing history; `earliest` replays whatever the CDC cleanup job still retains. Ignored once a bookmark exists. |
| `poll_interval` | `1s` | Wait between empty polls. |
| `idle_timeout` | `30s` | End the fetch cycle after this much continuous quiet. A long-running runtime (`faucet schedule` / `faucet serve`) re-invokes to keep tailing. |
| `batch_size` | `1000` | `0` accumulates every change into a single trailing page. |
| `max_staged_records` | unbounded | Abort (typed error) if one in-progress transaction buffers more than this. |
| `state_key` | derived | `mssql-cdc:<db>:<capture_instance>` for one instance, else `mssql-cdc:<db>:<digest>`. |

### Prerequisites

Change data capture must be enabled on the server (requires sysadmin, and SQL
Server Agent running for the capture job to populate the change tables):

```sql
EXEC sys.sp_cdc_enable_db;
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name   = N'Orders',
    @role_name     = NULL,
    @capture_instance = N'dbo_Orders';
```

## Change envelope

Each row becomes a CDC envelope with a normalized `op` marker mapped from
`__$operation`:

| `__$operation` | meaning | `op` |
|----------------|---------|------|
| 1 | delete | `d` |
| 2 | insert | `i` |
| 3 | update (before image) | *skipped* |
| 4 | update (after image) | `u` |

```json
{
  "op": "i",
  "schema": "dbo",
  "table": "Orders",
  "before": null,
  "after": { "id": 1, "amount": "9.99" },
  "lsn": "0000002a000000550003",
  "seqval": "0000002a000000550003"
}
```

Deletes carry the removed row in `before`; inserts and updates carry the current
image in `after` (the source queries in `N'all'` mode, so an update's pre-image
is not emitted).

## Mirroring to an upsert sink

Pair the envelope with the `cdc_unwrap` transform stage (which normalizes `op`
into `__op: "u"`/`"d"` and flattens `after`/`before`) and an upsert-capable sink
to build a live mirror:

```yaml
pipeline:
  transforms:
    - type: cdc_unwrap
  source:
    type: mssql-cdc
    config: { connection_url: "...", capture_instances: ["dbo_Orders"] }
  sink:
    type: postgres
    config:
      connection_url: "..."
      table: orders
      write_mode: upsert
      key: ["id"]
      delete_marker: { field: "__op", values: ["d"] }
```

## Source lag

`Source::lag` (#733) reports **events** and **seconds**: the change transactions after the slowest capture instance's committed LSN, and the age of the oldest of them, from `cdc.lsn_time_mapping` (needs `SELECT` on it). The pipeline polls it on the first page, at most every
15 s after, and when the run ends, exporting `faucet_source_lag_*{pipeline,row,connector}`;
`faucet status --probe` and `faucet doctor` ask it from the stored bookmark,
and `sla.max_lag_*` thresholds turn it into an SLA. A failing lag query is
logged once and reported as no lag — it never fails a run.

## License

Licensed under either of Apache-2.0 or MIT at your option.
