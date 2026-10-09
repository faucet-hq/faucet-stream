# Mirror (snapshot → CDC)

A [CDC pipeline](./upsert.md#cdc--mirror-with-cdc_unwrap) keeps a destination in
sync with a source from the moment it starts streaming — but it knows nothing
about the rows that already existed before it connected. To get a *complete*
mirror you have to back-fill the existing rows first, then stream changes. Doing
that by hand is fiddly: start CDC too late and you miss changes that happened
during the back-fill (a **gap**); start it too early and the back-fill replays
rows the stream already delivered (**duplicates**).

`faucet mirror` (formerly `faucet replicate`, still accepted) does the coordination for you. It bulk-snapshots the table
and then hands off to CDC from a position captured *before* the snapshot — so the
result is a true mirror with **no gap and no duplicate rows** when paired with
[`write_mode: upsert`](./upsert.md).

## How the handoff stays correct

The ordering is the whole trick:

1. **Capture the CDC position `P` first.** Before reading a single row,
   `faucet mirror` asks the CDC source for its current replication position —
   the WAL LSN (postgres), binlog file+pos (mysql), or change-stream resume token
   (mongodb) — and ensures any server-side resource needed to resume from it
   (e.g. the postgres replication slot) exists, so the log from `P` onward is
   retained.
2. **Bulk-snapshot the table.** A plain query source (`SELECT * FROM …`) reads
   the current state, which is at-or-after `P`.
3. **Stream CDC from `P`.** Every change committed after `P` is replayed over the
   snapshot baseline.

Why this leaves no gap and no duplicate **under `write_mode: upsert`**:

- **No gap** — every change with position > `P` is in the CDC stream. A row whose
  last change was at or before `P` is read by the snapshot at its current
  (unchanged-since-`P`) value; a row changed after `P` is delivered by CDC.
- **No duplicate** — a change in the overlap window (between `P` and the moment
  the snapshot reads that row) appears in *both* the snapshot and the CDC stream,
  but `upsert` is last-write-wins by key, so re-applying it is idempotent.
  Inserts and updates upsert; a delete of an already-absent row is a no-op. The
  destination converges to the source's current state.

This is the standard Debezium-style "snapshot then stream" model. The snapshot
does **not** need a consistent (repeatable-read) transaction — correctness rests
only on capturing `P` before the snapshot starts, plus upsert idempotency.

> **Append mode can produce boundary duplicates.** With `write_mode: append`,
> rows that fall in the overlap window are written twice (once by the snapshot,
> once by CDC). `upsert` is the recommended — and expected — pairing. If you run
> the mirror with an append sink, `faucet mirror` warns at validation
> time; see [no primary key](#tables-without-a-primary-key) below.

## Config shape

The main `pipeline` *is* the CDC pipeline (its `source` is a CDC connector, its
`sink` the destination). A top-level `mirror:` block adds the one-time
snapshot source. Both source specs point at the **same upstream database** — the
query connector for the bulk read, the `-cdc` connector for the stream — and they
share the destination `sink` and the pipeline-level `transforms`.

This is the shipped example
[`cli/examples/postgres_replicate_snapshot_cdc.yaml`](https://github.com/faucet-hq/faucet-stream/blob/main/cli/examples/postgres_replicate_snapshot_cdc.yaml):

```yaml
# Mirror public.orders → public.orders_mirror: bulk snapshot, then CDC.
version: 1
name: orders_mirror

pipeline:
  source:
    type: postgres-cdc
    config:
      connection_url: ${env:SOURCE_PG_URL}
      slot_name: orders_repl_slot
      publication_name: orders_pub      # CREATE PUBLICATION orders_pub FOR TABLE public.orders;
  transforms:
    - type: cdc_unwrap                   # {op,before,after} → flat row + __op marker
      config: {}
  sink:
    type: postgres
    config:
      connection_url: ${env:DEST_PG_URL}
      table_name: orders_mirror
      column_mapping: auto_map
      write_mode: upsert
      key: [id]
      delete_marker: { field: __op, values: [d] }
  state:
    type: file
    config: { path: ./.faucet-state }

mirror:
  mode: snapshot_then_cdc
  continuous: true                       # keep streaming after the snapshot
  snapshot:
    source:
      type: postgres
      config:
        connection_url: ${env:SOURCE_PG_URL}
        query: "SELECT * FROM public.orders"
```

A few things to note:

- The CDC source emits change-event **envelopes** (`{op, before, after, …}`), so a
  [`cdc_unwrap`](./transforms.md#cdc_unwrap--normalize-cdc-change-events-into-flat-rows)
  transform flattens them into rows and stamps an `__op` marker that the sink's
  `delete_marker` routes to deletes. The snapshot source instead produces flat
  table rows directly (no envelope), so **`faucet mirror` automatically strips
  `cdc_unwrap` from the snapshot phase** — running it there would drop every
  snapshot row (no `after`/`op` image). Any *other* pipeline-level transforms are
  kept for both phases, so write your snapshot `query` to yield rows in the
  destination's shape (the same shape `cdc_unwrap` produces for the CDC phase).
- The destination table needs a UNIQUE/PRIMARY KEY on the `key` columns before
  the first run (the same requirement as any [upsert sink](./upsert.md#supported-sinks-and-their-native-primitives)):

  ```sql
  CREATE TABLE IF NOT EXISTS orders_mirror (id int4 PRIMARY KEY, ...);
  ```

Validate it offline (no database connection required):

```bash
faucet validate cli/examples/postgres_replicate_snapshot_cdc.yaml
```

## Running it

```bash
faucet mirror cli/examples/postgres_replicate_snapshot_cdc.yaml
```

`faucet mirror` runs two phases in order: the **bulk snapshot**, then the
**CDC handoff**. `faucet run` ignores the `mirror:` block entirely (exactly
as it ignores `schedule:`), so use `faucet mirror` for a mirror config.

### `continuous`

The `continuous` flag (default `true`) controls what happens after the snapshot
completes:

- **`continuous: true`** — keep streaming CDC indefinitely as a long-running
  foreground process. Stop it with Ctrl-C or SIGTERM; the in-flight page flushes
  at the next page boundary before the process exits. A **transient** CDC-phase
  failure (a dropped connection, a slow upstream, a momentary network blip) no
  longer crash-exits the process: faucet logs the error, backs off (the delay
  grows on repeated failures, capped, and resets after a successful cycle), and
  resumes the CDC stream from the persisted bookmark. The long-running mirror
  rides out brief outages on its own.
- **`continuous: false`** — drain CDC once (until the source's idle timeout) and
  exit. Handy for tests, batch back-fills, or a one-shot container invocation.

## Resume behaviour

`faucet mirror` records its phase in a durable marker, so an interrupted run
picks up where it left off:

- **Crash during the snapshot** — the next run redoes the *whole* snapshot. This
  is safe because the snapshot is idempotent under `write_mode: upsert` (re-reading
  and re-upserting the same rows converges to the same state). The captured CDC
  position `P` is preserved across the redo, so no changes are lost.
- **Crash during CDC** — the next run resumes CDC from the persisted bookmark (the
  CDC source's own per-transaction position, which started at `P`). No snapshot
  redo, no gap.

Under `continuous: true`, a **transient** CDC-phase error does not even require a
restart: the process logs it, backs off, and resumes from the persisted bookmark
in place (see [`continuous`](#continuous) above). A **one-shot** run
(`continuous: false`) instead surfaces the error and exits non-zero, so a batch
back-fill or CI invocation still fails loudly on a real problem.

On a fresh run the marker is absent, so `faucet mirror` captures `P`, seeds
the CDC bookmark, and starts the snapshot. On any later run the marker tells it
whether to redo the snapshot or go straight to CDC.

## Requirements & caveats

### Durable state is required

The snapshot↔CDC handoff and the resume logic both depend on the `state:` store:
it holds the captured position, the phase marker, and the advancing CDC bookmark.
`faucet mirror` therefore **requires a durable backend** — `file`, `redis`, or
`postgres` — and rejects `memory` at validation time (a `memory` store is
per-process and would lose the marker on restart, breaking resume). See the
[state cookbook](./state.md#state-stores) for the backend table.

### `pipeline.source` must be CDC, `pipeline.sink` should upsert

The main pipeline source must be one of the capture-capable CDC connectors —
`postgres-cdc`, `mysql-cdc`, `mssql-cdc`, `mongodb-cdc`, `oracle-cdc`, or
`dynamodb` in `mode: streams` — and the snapshot source must be a **non-CDC**
bulk reader (e.g. `postgres` / `mysql` / `mongodb` / `oracle` running a query,
or `dynamodb` in `mode: scan`). For `oracle-cdc` the captured position is the
current SCN; the database must be in ARCHIVELOG mode with supplemental logging. Both are checked at config-load time. The sink should use
`write_mode: upsert` for a true mirror; an append sink validates with a warning
(see above).

### DynamoDB requires a keyed sink

DynamoDB Streams has no replayable position to capture: `faucet mirror` anchors
the CDC phase at every shard's trim horizon, so the change stream replays its
whole retained window (up to 24 hours) over the snapshot. That converges only
through keyed writes, so a `dynamodb` streams mirror **requires** the sink to use
`write_mode: upsert` with a non-empty `key` (an append sink is rejected, not
warned). Enable a `NEW_AND_OLD_IMAGES` stream on the table before
the snapshot starts, and pair the source with a `cdc_unwrap` transform so the
`__op` marker reaches the sink's `delete_marker`.

### Postgres requires a permanent slot

For `postgres-cdc`, position capture uses a **permanent** replication slot
(`slot_type: permanent`, the default), which retains WAL across the snapshot.
`slot_type: temporary` is refused at config load: a temporary slot is dropped
with the session that creates it, before replication could start.

### Log retention must outlast the snapshot

The captured position is only useful while the source still has the log from `P`
onward. A permanent postgres slot pins WAL until it is consumed, but MySQL binlog
and MongoDB oplog retention are **time-bounded**:

- If the snapshot takes longer than the source's binlog/oplog retention window,
  the captured position may be purged before CDC starts, and the CDC source will
  error that its start position is unavailable.
- Keep your retention window comfortably larger than the expected snapshot
  duration, and decommission an unused postgres pipeline by dropping its slot so
  it stops pinning WAL (`PostgresCdcSource::drop_slot()`).

### Tables without a primary key

`upsert` needs a `key`, and the destination needs a UNIQUE/PK on it. A record
that is missing or has a `null` key column [cannot be keyed](./upsert.md#missing-or-null-keys):
without a DLQ the batch fails; with one the offending rows are routed aside. If
the source table has no natural key you cannot mirror it with upsert — either
supply a synthetic `key` the snapshot and CDC both produce, or accept
append-mode semantics (and the boundary duplicates that come with them).

## Composing with effectively-once delivery

`faucet mirror` composes with [`delivery: exactly_once`](./state.md#effectively-once-delivery)
on the CDC phase: set `delivery: exactly_once` at the top level and pair it with
one of the four idempotent SQL sinks (`postgres`, `mysql`, `mssql`, `sqlite`) in
`upsert` mode. The snapshot phase always runs at-least-once (the query source is
not effectively-once-capable), but that is harmless — re-running the snapshot is
idempotent under upsert. The standard effectively-once hard requirements still apply
to the CDC pipeline (CDC source, idempotent SQL sink, a `state:` block, and no
`dlq:` block).

## Mirroring a set of tables

A 40-table database does not need 40 configs, 40 replication slots and 40
snapshot phases. Add a `tables:` block and one `faucet mirror` replicates every
table that matches, over **one change stream** (one Postgres slot, one MySQL
binlog reader, one MongoDB change stream, one SQL Server / Oracle capture
connection). The complete example is
[`cli/examples/postgres_mirror_tables.yaml`](https://github.com/faucet-hq/faucet-stream/blob/main/cli/examples/postgres_mirror_tables.yaml):

```yaml
name: shop_mirror
pipeline:
  source:                                  # ONE CDC connection for every table
    type: postgres-cdc
    config: { connection_url: "postgres://…/shop", slot_name: shop_mirror_slot, publication_name: shop_pub }
  transforms:
    - { type: cdc_unwrap, config: {} }
  sink:                                    # template: the table is filled in per table
    type: postgres
    config: { connection_url: "postgres://…/analytics", schema: shop_mirror, column_mapping: auto_map,
              delete_marker: { field: __op, values: [d] } }
  state: { type: file, config: { path: ./.faucet-state } }
mirror:
  mode: snapshot_then_cdc
  snapshot:
    source:                                # discovery runs here; each table's selection is merged over it
      type: postgres
      config: { connection_url: "postgres://…/shop", query: "SELECT 1" }
    concurrency: 4                         # tables snapshotted in parallel
    shards: 8                              # primary-key ranges per table snapshot
  tables:
    include: ["shop.*"]
    exclude: ["shop.audit_*"]
    new_tables: follow                     # snapshot tables created later, then stream them
    destination: { table_name: "{table_name}" }
  per_table:
    shop.orders: { schema_drift: { on_drift: evolve } }
```

**The table set** comes from the snapshot source's
[`discover()`](./discover.md), filtered by `include` / `exclude` globs. Each
table is keyed on its discovered **primary key** (override with
`per_table.<table>.key`). A table with no primary key is **refused** — reported
in status, never mirrored without a key — unless you set
`without_primary_key: append` (updates and deletes then append rows).
`destination` is merged over the sink config per table; `{table}`,
`{table_name}` and `{schema}` are filled in. SQL sinks, BigQuery, MongoDB and
Elasticsearch have a default (`table_name` / `table` / `table_id` /
`collection` / `index` = `{table_name}`); file sinks need one.

**How one stream serves many tables.** Every table runs as its own pipeline —
its own sink, write mode, drift policy, DLQ and **its own state key**
`{name}::{table}` — fed by a demultiplexer that reads the change stream once and
routes each record by the table it belongs to. A table commits a stream
position only after its own sink has flushed. The stream resumes from the
**earliest** position any table has committed (so a slot never releases WAL a
table still needs), and each table skips the changes it already applied. The
skip is decided per change, by the change's own stream position: a replay can
cut the stream into pages at different points than the run that committed
them, so a page may straddle a table's position. Under
`delivery: exactly_once` each table's watermark is scoped to its own state key,
so exactly-once composes per table. A one-shot run (`continuous: false`) exits
with an error naming every table whose snapshot or last stream cycle failed,
even under `on_table_error: pause`.

**Per-table handoff.** Each table's snapshot starts from a stream position
captured just before it and recorded as that table's join point; the stream
keeps that position until the table joins, and replays everything after it over
the snapshot (keyed upsert makes the overlap idempotent). On sinks that support
[overwrite](./upsert.md#overwrite-full-refresh) a re-snapshot replaces the
destination atomically, so a redo never leaves rows the source no longer has.
On other sinks a re-snapshot (a redo, a paused table retried, a dropped table
returning) writes over the existing destination, so rows the source deleted in
between can remain: the table is flagged `resync required` in status (and
logged) until you empty the destination and snapshot it again.

| Event | What happens |
|---|---|
| Crash mid-snapshot of one table | Only that table redoes its snapshot on restart; finished tables keep streaming from their positions. |
| Crash mid-stream | Every table resumes from its own committed position — no gap, no duplicate. |
| Table created at the source (`new_tables: follow`) | A change record for an unknown matching table, or the periodic discovery (`discover_interval_secs`, default 300), adds it: it snapshots at the current position and joins the stream. |
| Table dropped at the source | It is marked `dropped` and no longer routed. Its destination table is **never** dropped. |
| A table's sink keeps failing | After `max_table_failures` failed cycles (default 3) it is `paused` with its error in status; the rest of the stream continues and is not held back. After `retry_paused_secs` (default 300) it is re-snapshotted and rejoins. `on_table_error: fail` stops the whole mirror instead. |
| A table lags | A table whose last applied change is older than `lag_warning_secs` is flagged `lagging` in status and logged — it is holding the stream's resume position (and the slot's WAL) back. |
| DDL on a mirrored table | Routed through that table's [schema-drift](./schema-drift.md) policy (`per_table.<table>.schema_drift` overrides the top-level one). |

**Status.** `faucet mirror status <config>` (or `--json`, or
[`GET /v1/mirror/{name}`](../reference/http-api.md#mirror-status)) reads the
mirror's state store and shows, per table: phase (`pending` / `snapshotting` /
`active` / `paused` / `dropped` / `refused`), snapshot progress, change records
routed, committed position, lag, last error and a `resync required` note when
a re-snapshot could not replace the destination:

```text
mirror shop_mirror (41 tables: 38 active, 1 paused, 1 refused, 1 snapshotting)
  TABLE            PHASE          SNAPSHOT     CHANGES     LAG  NOTE
  shop.orders      active             100%   1,204,511      3s
  shop.events      snapshotting        62%           0       -
  shop.logs        refused               -           0       -  table 'shop.logs' has no primary key — …
  shop.payments    paused             100%      88,120     14m  Sink error: …
```

Per-source notes:

- **Postgres** — the publication decides what the slot streams; use
  `FOR TABLES IN SCHEMA …` or `FOR ALL TABLES` so `new_tables: follow` sees new
  tables.
- **MySQL** — discovery names tables without the database; the binlog's
  `database.table` names are matched to the snapshot connection's database.
  Scope the binlog reader with `include_tables` when the server hosts others.
- **MongoDB** — `scope: { type: database, database: <name> }`, naming the
  database the `mongodb` snapshot source reads. The config is refused with a
  collection or cluster scope (the default), because their change records do
  not name tables the way the snapshot's discovery does; keys default to `_id`.
- **SQL Server** — list the tables' capture instances in `capture_instances`;
  left empty, they are derived as `{schema}_{table}` (SQL Server's default
  capture-instance name).
- **Oracle** — the LogMiner `tables:` list is set to the mirrored set.
- **DynamoDB** — each table has its own stream, so each table gets its own
  stream reader (DynamoDB has no connection-wide stream); keyed upsert is
  required.

