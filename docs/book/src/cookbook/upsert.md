# Upsert / mirror tables

By default every sink **appends** — each record becomes a new row. That is the
right behaviour for event logs and immutable history, but it is wrong for a
*mirror*: a destination table that should stay an exact, up-to-date replica of a
source table, where an updated source row updates the mirror in place and a
deleted source row disappears from the mirror.

Upsert-capable sinks add two more write modes — `upsert` and `delete` — keyed by
a configurable `key`, so faucet can keep a destination in sync with a changing
source instead of only ever growing it.

## Write modes

Each upsert-capable sink config carries three flattened fields (they appear at
the top level of the sink's `config`, alongside `table_name` etc.):

| Field | Default | Purpose |
|-------|---------|---------|
| `write_mode` | `append` | `append`, `upsert`, or `delete` |
| `key` | `[]` | Key columns. **Required and non-empty** for `upsert`/`delete`; ignored for `append` |
| `delete_marker` | (none) | `upsert` only — `{ field: <name>, values: [<str>, …] }`; rows whose `field` matches one of `values` become deletes instead of upserts |

- **`append`** — insert every record (the default; today's behaviour).
- **`upsert`** — insert-or-update by `key`. If `delete_marker` is set, rows whose
  marker field matches are routed to deletes instead; the marker field is
  stripped from the upserted row before writing.
- **`delete`** — delete by `key` for every record in the batch.

## Supported sinks and their native primitives

Eleven sinks support `upsert`/`delete`; every other sink is append-only.

| Sink | Requires | Native primitive |
|------|----------|------------------|
| `postgres` | `column_mapping: auto_map` + UNIQUE/PK on `key` (created for you) | `INSERT … ON CONFLICT … DO UPDATE` |
| `sqlite` | `column_mapping: auto_map` + UNIQUE/PK on `key` (created for you) | `INSERT … ON CONFLICT … DO UPDATE` |
| `mysql` | `column_mapping: auto_map` + a PRIMARY/UNIQUE index whose columns **exactly match** `key` (created for you) | `INSERT … ON DUPLICATE KEY UPDATE` |
| `mssql` | `column_mapping: auto_columns` + UNIQUE/PK on `key` (created for you) | `MERGE` |
| `oracle` | `column_mapping: auto_columns` + a PRIMARY KEY/UNIQUE on `key` (created for you) | array-bound `MERGE INTO … USING (SELECT :1 … FROM DUAL)` / `DELETE … WHERE key = :n` |
| `mongodb` | — (schemaless) | `replace_one(upsert)` / `delete_one`, `key` → match filter |
| `elasticsearch` | — (schemaless) | `_bulk` `index` / `delete`, `key` → `_id` |
| `bigquery` | a defined table schema + `key` columns | in-place `MERGE … USING UNNEST(@payload)` (no staging table) |
| `spanner` | `key` must equal the table's primary-key columns | `InsertOrUpdate` / `Delete` mutations (mutations always address the PK) |
| `dynamodb` | `key` must name the table's partition key (+ sort key) | `BatchWriteItem` `PutRequest` / `DeleteRequest`, last-write-wins per key within a page |
| `databricks` | a Delta table (created for you with `create_table: true`) + `key` columns | `MERGE INTO … USING (VALUES …)` (or a staged `COPY INTO` temp table for large pages) |

The **SQL sinks require column-mapping mode** — `column_mapping: auto_map`
(postgres/mysql/sqlite) or `auto_columns` (mssql). The single-JSONB-column blob
mode cannot upsert because there is no per-column conflict target. They also require a
**UNIQUE or PRIMARY KEY constraint on the `key` columns** — that constraint is
what the database's `ON CONFLICT` / `ON DUPLICATE KEY` / `MERGE` matches against;
without it the upsert silently degrades to plain inserts. faucet does not create
the constraint for you; create it on the destination table first.

**"Created for you":** when the table does not exist and `create_table: true`
(the default), the SQL sinks create it with `PRIMARY KEY (<key…>)` on the key
columns, so the very first run upserts correctly. On MySQL and SQL Server, text
key columns are created as `VARCHAR(191)` / `NVARCHAR(450)`, because neither can
index an unbounded text type. A table you created yourself must carry that
constraint already; the sink checks, and does not alter your table.

> **MySQL validates the index match at startup.** MySQL's `ON DUPLICATE KEY
> UPDATE` resolves against *whichever* unique index a row collides with — not the
> columns you name in `key`. So a `key` that doesn't correspond to a real
> PRIMARY/UNIQUE index would silently upsert on the wrong index. The MySQL sink
> therefore checks at construction that the configured `key` **exactly matches**
> (order-insensitively) the columns of some PRIMARY or UNIQUE index on the target
> table, and fails fast with a typed error if it does not — catching the
> mismatch before any data is written rather than corrupting rows.

The **schemaless sinks** (MongoDB, Elasticsearch) have no such requirement: the
`key` columns are joined into a document filter / `_id`, so the same record both
inserts and replaces.

> **Not yet supported:** Iceberg is append-only today — Iceberg upsert is blocked
> on equality-delete writer support in `iceberg-rust` (#225).

## Last-write-wins within a batch

A single batch may contain several changes to the same key (common with CDC — an
insert and three updates of one row in one transaction). faucet **deduplicates by
`key` within the batch, last-write-wins**: only the final action for each key is
applied. If the last action is a delete, the row is deleted; if it is an upsert,
the row is upserted — regardless of what came before it in the batch. This keeps
the write minimal and the result deterministic.

## Missing or null keys

`upsert`/`delete` need a key value for every row. A record that is not a JSON
object, is missing a `key` column, or has a `null` value in a `key` column cannot
be keyed:

- **With a DLQ configured**, the offending rows are routed to the dead-letter
  queue per-row (the rest of the batch still writes).
- **Without a DLQ**, the whole batch fails with a typed error so the bad data is
  never silently dropped.

## CDC → mirror with `cdc_unwrap`

The most common use of upsert is mirroring a database table via change-data
capture. CDC sources emit change-event **envelopes** (`{op, before, after, …}`),
not bare rows, so a [`cdc_unwrap`](./transforms.md#cdc_unwrap--normalize-cdc-change-events-into-flat-rows)
transform sits between the source and the sink: it flattens the envelope into a
single row and stamps an `__op` marker (`"u"` for insert/update, `"d"` for
delete). The sink's `delete_marker` then routes the `"d"` rows to deletes.

This is the shipped example
[`cli/examples/postgres_cdc_to_postgres_upsert.yaml`](https://github.com/faucet-hq/faucet-stream/blob/main/cli/examples/postgres_cdc_to_postgres_upsert.yaml):

```yaml
version: 1
name: pg_cdc_mirror
delivery: exactly_once

pipeline:
  source:
    type: postgres-cdc
    config:
      connection_url: ${env:SOURCE_PG_URL}
      slot_name: faucet_mirror
      publication_name: faucet_pub
      create_slot_if_missing: true
      idle_timeout: 30

  transforms:
    - type: cdc_unwrap

  sink:
    type: postgres
    config:
      connection_url: ${env:DEST_PG_URL}
      table_name: users_mirror
      column_mapping: auto_map
      write_mode: upsert
      key: [id]
      delete_marker: { field: __op, values: [d] }

  state:
    type: file
    config:
      path: ./state
```

The destination table needs a UNIQUE/PRIMARY KEY on the `key` columns before the
first run:

```sql
CREATE TABLE IF NOT EXISTS users_mirror (id int4 PRIMARY KEY, name text);
```

Validate it offline (no database connection required):

```bash
faucet validate cli/examples/postgres_cdc_to_postgres_upsert.yaml
```

## Composing with effectively-once delivery

A keyed upsert **is** an effectively-once mechanism in its own right: any
source feeding an upsert-capable sink with `write_mode: upsert` + `key` is
accepted under [`delivery: exactly_once`](./state.md#effectively-once-delivery)
and reported by `faucet validate` as `effectively-once (keyed upsert)` — the
replayed records converge on the same keyed rows instead of duplicating. No
state store or watermark is required for this mechanism (state is still
recommended so re-runs are incremental).

The **atomic-watermark** mechanism additionally composes with upsert on the
four SQL sinks (`postgres`, `mysql`, `mssql`, `sqlite`), **BigQuery**,
**Oracle**, **Databricks** and **MongoDB** (replica set required): the sink commits the upserted/deleted rows
**and** the monotonic commit token in a single transaction, so a crash-and-resume
never re-applies or skips a batch — the mirror stays exactly consistent with
the source even across restarts. Its requirements, checked at config-load time:

1. a positional-replay source (`postgres-cdc` / `mysql-cdc` / `mongodb-cdc` / `oracle-cdc` / `kafka`),
2. an idempotent sink (`postgres` / `mysql` / `mssql` / `sqlite` / `oracle` / `bigquery` / `databricks` / `mongodb`),
3. a **durable** `state:` block (not `memory`), and
4. **no** `dlq:` block (incompatible with the atomic-watermark path in this version —
   a missing/null-key row therefore fails the batch rather than being routed aside).

For BigQuery, the whole page is merged as one `jobs.query` request (~10 MB limit);
keep the CDC source's `batch_size` modest (the default 1 000 rows is fine for most
schemas; lower it for very wide rows that approach the limit).

Elasticsearch supports upsert but not the atomic watermark (`_bulk` cannot
commit a watermark atomically) — an upsert mirror into Elasticsearch reaches
effectively-once via the keyed-upsert mechanism instead.

## Removing records deleted at the source (scoped cleanup)

An upsert mirror keeps rows **fresh and additive**, but on its own it can never
remove a record that was deleted upstream. An incremental source returns "what
changed since X"; a deleted record simply stops appearing, so there is nothing to
act on. The destination looks healthy, the run reports success, and stale rows
accumulate forever.

If your source emits deletions — a CDC stream, or a soft-delete field — use
[`delete_marker`](#delete-marker) and stop here. **Scoped cleanup** is for the
common case where it does not: a REST API with an updated-since filter and no
tombstones.

### The idea

Some fetches are *complete for a scope*. Fetching one contact's associations
returns every association that contact currently has. That is a claim only the
**source** can make — a sink sees a page of records and cannot tell a complete
set from page 1 of 3.

Declare the claim on the source and opt the sink in:

```yaml
matrix:
  - id: associations
    parent: contacts
    source:
      type: rest
      config:
        url: "https://api.example.com/contacts/${contacts.id}/associations"
      complete_for:
        scope:
          contact_id: "${contacts.id}"   # destination column names
        on_missing: delete               # omit (or `ignore`) = claim is inert
    sink:
      ref: assoc
      write_mode: upsert
      key: [association_id]
```

After the run writes every page, faucet deletes the rows matching
`contact_id = <that contact>` whose `association_id` it did not write. Other
contacts are untouched.

**Deleting is an explicit opt-in.** `on_missing` defaults to `ignore`, so adding
a claim documents the scope without ever deleting anything; only
`on_missing: delete` acts on it. An empty `scope` is rejected at load time —
unbounded, it would match every row in the destination.

### Scope keys are destination column names

`complete_for.scope` is written in **destination** terms, because the `DELETE` runs
against destination columns. If a transform renames `contactId` → `contact_id`
between source and sink, the scope uses `contact_id`. Values may carry
`${parent.*}` / `${now.*}` tokens and are resolved per invocation, exactly like
the connector config around them.

### The empty case is the point

A contact whose associations were *all* removed produces a fetch returning
**zero records**. An upsert alone writes nothing and every stale row survives.
Cleanup still fires and empties the scope, because the claim comes from the
parent record rather than from the records observed.

### When it does not run

Cleanup deletes data, so it only runs when the written set is trustworthy:

| Situation | Behaviour |
|---|---|
| Run failed | Skipped — the run never wrote the authoritative set |
| Run cancelled | Skipped — a partial read would delete rows that never arrived |
| `--dry-run` / `--limit` | Skipped — the sink is a counter or a dropper, so the written set is synthetic |
| Sharded run | Skipped — a shard reads a fraction, so the difference is other shards' rows |
| Written rows exceed the key ceiling | **Run fails**, deleting nothing |
| A record was quarantined by a quality / contract / drift policy | Rejected at load time — a quarantined record never reaches the sink, so cleanup could not tell it from a deleted one |

That last one is deliberate. Above the ceiling the written-key set is incomplete,
so a delete would remove rows the run wrote — but skipping quietly would leave
the stale rows this feature exists to remove. Neither is safe to do silently, so
the run fails and you narrow the scope.

Rows routed to the DLQ or quarantined by a quality/contract check **count as
written**. They are real source records — the source claimed them present — so
they are never deleted even though they did not reach the destination.

### Supported sinks

Eight of the eleven upsert-capable sinks: `postgres`, `mysql`, `mssql`, `sqlite`,
`mongodb`, `elasticsearch`, `bigquery`, `spanner` (not `dynamodb` or
`databricks` or `oracle`). The SQL sinks require
column-mapping mode (`auto_map` / `auto_columns`) — a single JSON payload column
has no columns to predicate on.

`on_missing: delete` requires `write_mode: upsert` and a non-empty `key`, and is
incompatible with `delivery: exactly_once` (the scoped delete happens outside the commit-token
transaction, so it cannot be replayed idempotently).

### Metrics

| Metric | Meaning |
|---|---|
| `faucet_cleanup_deleted_total{pipeline,row,connector}` | Rows deleted. Emitted even at zero — zero is the steady state a healthy mirror shows. |
| `faucet_cleanup_runs_total{pipeline,row,outcome}` | `applied` / `skipped_cancelled` / `refused_overflow`. A non-zero `refused_overflow` means stale rows were left behind — worth alerting on. |

## Overwrite (full refresh)

`write_mode: overwrite` replaces the **entire** destination with the current
run's records — a truncate-and-load / full refresh. Use it for reference and
dimension tables, or any source you re-fetch in full each run and where a plain
`upsert` would leave behind rows that were deleted at the source.

```yaml
pipeline:
  source:
    type: csv
    config: { path: ./data/contacts.csv }
  sink:
    type: sqlite
    config:
      database_url: "sqlite://./out/warehouse.db"
      table_name: contacts
      column_mapping: auto_map   # overwrite replaces real columns, not a JSON blob
      write_mode: overwrite      # no `key` needed — it is a whole-table replace
```

**Safety — the old data survives a failed run.** Overwrite never truncates the
destination up front. The run's writes are staged into a temporary target and
only swapped into place **after the run finishes successfully and
uncancelled**. If the run fails or is cancelled part-way, the staging target is
discarded and the previous destination is left exactly as it was. There is no
window where the table is empty because a load died halfway.

**A first run creates the target.** With `create_table: true` (the default) a
destination that does not exist yet is created by the first run: the staging
table is built from the first page's inferred columns, and the commit renames it
into place. Until that commit the target does not exist, and a failed first run
leaves no table behind. From the second run on, overwrite replaces the
destination's *rows*, not its definition — so indexes, partitioning, or column
types you add after the first run survive every refresh. With
`create_table: false`, a missing target is an error.

### Supported sinks & mechanism

| Sink | Atomic swap |
|---|---|
| `postgres` | one transaction: `TRUNCATE` + `INSERT … SELECT` from a `LIKE` staging clone + `DROP` |
| `sqlite` | one transaction: `DELETE` + `INSERT … SELECT` from a `SELECT … WHERE 0` clone + `DROP` |
| `mysql` | `CREATE TABLE staging LIKE target`, then an atomic `RENAME TABLE` swap (MySQL auto-commits DDL, so a transaction can't span it) |
| `mssql` | one transaction: `DELETE` + `INSERT` (explicit non-IDENTITY column list) from a `SELECT … INTO … WHERE 1=0` clone + `DROP` |
| `mongodb` | load a `{collection}__faucet_ovw` staging collection, then atomic `renameCollection(dropTarget: true)` (needs the rename privilege; unsupported on sharded collections) |
| `bigquery` | **bucket-free** — load a `LIKE` temp table via the query API, then `BEGIN TRANSACTION; TRUNCATE; INSERT … SELECT; COMMIT` (preserves the target's partitioning/clustering); no GCS staging bucket required |
| `elasticsearch` | index into a fresh physical index `{index}-faucet-ovw-…` (mappings copied from the current target), then an atomic `POST /_aliases` swap repoints the read alias and the old index is dropped |
| `oracle` | load a `CREATE TABLE … AS SELECT * FROM target WHERE 1 = 0` staging table, then one transaction: `DELETE` + `INSERT … SELECT` over the insertable columns + `DROP` (a first run renames staging into place) |
| `databricks` | load a `CREATE TABLE … LIKE` staging Delta table, then one atomic `INSERT OVERWRITE target SELECT * FROM staging` + `DROP` (a first run renames staging into place) |

**Elasticsearch requires `index` to be an alias** (not a concrete index): the
overwrite swaps the alias atomically, so a reader never sees a half-replaced
dataset. Point `index` at an alias (or a not-yet-existing name — the first run
creates the alias); a concrete index of that name is rejected at `begin`.

### Incompatibilities (rejected at `faucet validate`)

- `delivery: exactly_once` — a full replace has no per-page watermark to resume from.
- `schema.on_drift: evolve` — the staging target is a pre-run clone, so evolving the live target mid-run would leave the staged data a column short at swap time.
- Scoped cleanup (`complete_for`) — cleanup requires `write_mode: upsert`; a full overwrite already removes source-deleted rows wholesale.

## Scoped / windowed overwrite (#518)

Replace only the destination rows in a **scope** (a date window) instead of the
whole table — the declarative equivalent of "delete a rolling window, then
re-insert" (period-report loads: QuickBooks / Xero / Zoho Books). Add a `scope:`
block alongside `write_mode: overwrite`:

```yaml
sink:
  type: bigquery            # or postgres
  config:
    write_mode: overwrite
    scope:
      window: { column: posting_date, from: "${now.month_start}", to: "${now.month_end}" }
```

At commit, the same begin→stage→swap machinery runs, but the swap is a single
transaction of `DELETE FROM target WHERE <scope>; INSERT INTO target SELECT *
FROM staging;` — only the in-window rows are replaced; everything outside the
window is preserved. The window is half-open `[from, to)`.

**Supported sinks (v1):** `postgres`, `bigquery` (`SCOPED_OVERWRITE_SINK_KINDS`).
The other overwrite sinks still support **full** overwrite; scoped overwrite on
them (and key-set scopes) is a follow-up. `scope` requires `write_mode:
overwrite` and inherits the overwrite incompatibilities above.
