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
`postgres-cdc`, `mysql-cdc`, `mssql-cdc`, `mongodb-cdc`, or `dynamodb` in
`mode: streams` — and the snapshot source must be a **non-CDC** bulk reader
(e.g. `postgres` / `mysql` / `mongodb` running a query, or `dynamodb` in
`mode: scan`). Both are checked at config-load time. The sink should use
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

For `postgres-cdc`, position capture requires a **permanent** replication slot
(`slot_type: permanent`, the default). A temporary slot is dropped when the
short-lived capture connection closes, so it cannot retain WAL across the
snapshot — `faucet mirror` rejects a temporary slot with a typed error.

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
