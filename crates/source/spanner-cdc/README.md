# faucet-source-spanner-cdc

[![Crates.io](https://img.shields.io/crates/v/faucet-source-spanner-cdc.svg)](https://crates.io/crates/faucet-source-spanner-cdc)
[![Docs.rs](https://docs.rs/faucet-source-spanner-cdc/badge.svg)](https://docs.rs/faucet-source-spanner-cdc)
[![License](https://img.shields.io/crates/l/faucet-source-spanner-cdc.svg)](https://github.com/faucet-hq/faucet-stream#license)

Google Cloud Spanner **change streams** (change data capture) source for the
[faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. It reads
a change stream's partition tree and emits every `INSERT` / `UPDATE` / `DELETE`
as a CDC envelope, with a durable, resumable bookmark.

Use it to stream live mutations out of Spanner into a warehouse, queue or lake
without polling or touching the application. To run an arbitrary query against
Spanner instead, use [`faucet-source-spanner`](../spanner).

## Feature highlights

- **Real CDC** — reads the change stream, not the tables: no `updated_at`
  columns and no query load on the base tables.
- **Transactions are never split** — the records of one source transaction land
  in the same page, so a sink never commits half of one.
- **Resumable** — the bookmark holds the per-partition read position and the
  partition parent/child ordering, and survives restarts.
- **Partition-tree aware** — Spanner splits and merges partitions as load
  changes. A child partition is never read before its parents are finished, so
  per-key order is preserved across splits and merges.
- **Typed values** — the wire format (INT64 as string, JSON as text, NUMERIC as
  string, BYTES as base64) is re-typed from the record's column types.
- **Bounded memory** — pages are cut at `batch_size`; `max_staged_records`
  bounds a single run.
- **Compatible with `cdc_unwrap`** — the envelope uses the same `op` /
  `before` / `after` / `document_key` contract as the other CDC sources.

## Installation

```bash
cargo add faucet-source-spanner-cdc
# or in the CLI:
cargo install faucet-cli --features source-spanner-cdc
```

## Spanner setup (one-time)

```sql
CREATE CHANGE STREAM orders_stream FOR orders
  OPTIONS (value_capture_type = 'NEW_ROW', retention_period = '7d');
```

faucet never creates change streams. The caller needs read access to the
database and permission to run the change stream query.

> **Prefer `NEW_ROW` (or `NEW_ROW_AND_OLD_VALUES`).** With the default
> `OLD_AND_NEW_VALUES`, Spanner only sends the columns that were modified, so
> `after` holds just the key and the changed columns. A mirror into a keyed sink
> wants the whole row.

## Quick start

```yaml
version: 1
pipeline:
  source:
    type: spanner-cdc
    config:
      project_id: my-project
      instance: my-instance
      database: my-db
      change_stream: orders_stream
  transforms:
    - type: cdc_unwrap
  sink:
    type: jsonl
    config:
      path: ./orders.jsonl
```

## Configuration

The connection block (`project_id`, `instance`, `database`, `auth`,
`max_sessions`, `emulator_host`) is shared with the Spanner query source and
sink.

| Field | Required | Default | Description |
|---|---|---|---|
| `change_stream` | ✔ | — | Name of the change stream (an identifier). |
| `start_from` | | `{type: now}` | Fresh-run start: `{type: now}` (no history replay) or `{type: timestamp, timestamp: <RFC 3339>}`. Ignored once a bookmark exists. |
| `heartbeat_ms` | | `10000` | Heartbeat interval on quiet partitions (1000–300000). A bounded run ends about one heartbeat after its end time. |
| `idle_timeout` | | `60` (s) | Give up on a partition read that yields nothing for this long. Must exceed `heartbeat_ms`. |
| `batch_size` | | `1000` | Records per page (`0` = whole run in one page). Pages are cut only between transactions. |
| `max_concurrent_partitions` | | `4` | Partitions read in parallel (1–64, at most `max_sessions`). |
| `max_staged_records` | | — | Abort the run with a typed error past this many buffered records. |
| `state_key` | | derived | Defaults to `spanner-cdc:<project>.<instance>.<database>:<change_stream>`. |

## Record shape

One envelope per modified row:

```json
{
  "op": "u",
  "ts_ms": 1791792315459,
  "table": "orders",
  "document_key": {"id": 7},
  "before": {"id": 7, "status": "new"},
  "after": {"id": 7, "status": "paid"},
  "commit_timestamp": "2026-10-12T08:05:15.459123Z",
  "server_transaction_id": "...",
  "record_sequence": "00000001",
  "transaction_tag": "",
  "value_capture_type": "NEW_ROW",
  "is_system_transaction": false
}
```

`op` is `c`, `u` or `d`. `before` is null for inserts and `after` is null for
deletes. `before` / `after` contain the key columns merged with the old / new
values the stream captured.

## Delivery semantics

- **At-least-once.** Resume is inclusive, so a restart can re-deliver the last
  few records. Pair it with an idempotent keyed sink (`write_mode: upsert`).
  There is no exactly-once mode and no `faucet mirror` snapshot handoff yet.
- Each run is **bounded**: it reads until the local clock time at the start of
  the run, then ends. Run it on a schedule; each run resumes from the bookmark.
  The run end uses the local clock, so a badly skewed clock delays records to
  the next run rather than losing them.

## Limits

- `MUTABLE_KEY_RANGE` change streams (`partition_mode`) are refused.
- `start_from` must be inside the change stream's retention period and not
  before the stream was created; otherwise the error mentions the retention
  period.

## PostgreSQL dialect

For a PostgreSQL-dialect database set `dialect: postgresql` next to the other
connection fields:

```yaml
source:
  type: spanner-cdc
  config:
    project_id: my-project
    instance: my-instance
    database: my-pg-db
    dialect: postgresql
    change_stream: orders_stream
```

The source reads the stream through `spanner.read_json_<stream>`, which returns
one JSONB record per row, and produces the same envelopes, bookmarks and
ordering as on GoogleSQL. A bookmark taken on one dialect is not portable to a
database of the other.

faucet checks the database's dialect at startup and fails with a clear message
if `dialect` does not match it. In the PostgreSQL dialect the stream name is
folded to lower case, so create it with a lower-case name.

## Emulator

Set `emulator_host` (for example `localhost:9010`) to talk to the Cloud Spanner
emulator. Change streams work on emulator 1.5.x; the emulator splits partitions
after about 20 seconds, which is useful for testing the partition tree.

## License

Dual licensed under MIT or Apache-2.0.
