# faucet-sink-duckdb

DuckDB sink connector for the [faucet-stream](https://github.com/faucet-hq/faucet-stream)
data-movement platform. Writes JSON records to a DuckDB table using either a
single JSON text column or dynamic column mapping. Each batch is one
`BEGIN`/`COMMIT` transaction of `batch_size`-row multi-row `INSERT`s, rolled
back on error.

DuckDB is a synchronous embedded engine, so writes run on a blocking thread.
A missing target table is created from the first page (`create_table: true`,
the default); with `create_table: false` it must already exist. An unqualified
`table_name` resolves to the connection's current database and schema, exactly
as the `INSERT` does; `schema.table` and `catalog.schema.table` are honoured.
The sink is **append-only**; keyed upsert and an Arrow-native columnar fast path
are tracked as follow-ups.

In `auto_map` mode a field matches its column exactly or, failing that,
ignoring ASCII case (DuckDB identifiers are case-insensitive), so `ID` lands in
column `id` instead of failing the write. A record with no field matching a column of the table is
refused rather than skipped: `write_batch` fails the page, and with a `dlq:`
block the record fails on its own row (routed to the DLQ) while the rest of the
page is written.

## Config

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `database` | string | — | Path to the `.duckdb` file, or `:memory:`. A `duckdb://` / `duckdb:` prefix is accepted and stripped. |
| `table_name` | string | — | Target table, optionally `schema.table` or `catalog.schema.table`. |
| `column_mapping` | enum | `{json: {column: "data"}}` | `json` stores each record as one JSON text column; `auto_map` maps top-level keys onto matching columns. |
| `batch_size` | integer | `1000` | Rows per multi-row INSERT. `0` = one INSERT for the whole slice. |
| `sandbox` | bool | `false` | No file or network access, no extension install or autoload, configuration locked. The setting holds for the whole database instance, which sinks on one file in one process share. `faucet serve` forces it on for every config except those in its own `--triggers` file. |

## Example

```yaml
version: 1
pipeline:
  source:
    type: file
    config:
      path: events.jsonl
  sink:
    type: duckdb
    config:
      database: warehouse.duckdb
      table_name: events
      column_mapping: auto_map
      batch_size: 5000
```

## Conformance

Wires the [`faucet-conformance`](https://docs.rs/faucet-conformance) battery in
`tests/conformance.rs` — config-schema validity and truthful capabilities
(append works; the sink honestly advertises no idempotency mechanism).

## Auto-create (`create_table`)

`create_table` (**default `true`**, #580) creates the target table from the
first written page's inferred columns when it does not exist — a first-ever
sync cannot assume the destination is already there. Every inferred column is
created **nullable**: a column present in page 1 is not required forever, and a
`NOT NULL` inferred from one page fails page 2 the first time a record omits
the field. With `auto_map`, a field first seen on a later page (or missing
from a pre-existing table) is added with `ALTER TABLE … ADD COLUMN IF NOT
EXISTS`, typed the same way, in the page's transaction — it is never dropped.

Set `create_table: false` to require a pre-existing target; a missing one then
fails fast with the same error every table sink raises, naming both ways out,
and with `auto_map` a record field the table has no column for fails the write
naming the field.

With `auto_map`, an ISO 8601 string carrying an offset (`…-08:00`, `…Z`) bound
for a tz-less `TIMESTAMP` column is converted to UTC first; DuckDB's text cast
would otherwise drop the offset. Offset-less strings are stored as given.

## Batch atomicity

What a failed write leaves behind (#737): **atomic** — every chunk of a batch runs in one transaction. `on_batch_error: dlq_all`
is refused on a best-effort configuration unless the `dlq:` block sets
`allow_duplicates_on_dlq_all: true` (a DLQ replay would write the rows that
already landed a second time). See
[batch atomicity](https://faucet-hq.github.io/faucet-stream/cookbook/dlq.html#batch-atomicity-and-dlq_all).

## Capabilities

Append only. `faucet list --json` prints the declaration.

## Timeouts, cancellation and retries

Every page write runs through the pipeline's `CallContext` (#880): the run's deadline ends it promptly, and a write cut short is interrupted inside DuckDB so its transaction rolls back. Each write counts as one `write` round trip in `faucet_sink_roundtrips_total`. The sink never retries a write itself.

## Shared SQL layer

This sink takes its row planning — the engine's column mapping (exact first, else ignoring ASCII case), union column set and `INSERT` statements — and its quoting, types and DDL from the `Duckdb` dialect of
[`faucet-common-sql`](../../common/sql/README.md) (RFC 0013, #884), the same
place every SQL sink gets them. The driver is blocking, so the planned statements run on `spawn_blocking` here rather than through the async executor; adding columns for new fields stays sink-specific.

## How it is built

The sink runs on the core sink driver (RFC 0014): `DuckdbDriver` implements
`faucet_core::driver::SinkDriver` — the one-transaction blocking page write
(interrupted when the call context ends it) and the per-row split of the
dead-letter path — and `DuckdbSink` wraps it in a `DriverSink`, which serves
the plain, reported and per-row write paths with row accounting. It is not on
the SQL family driver (`faucet_common_sql::SqlDriver`): that adapter runs
statements through an async executor, and DuckDB's connection is blocking and
writes a whole page in one task. The sink has no overwrite, exactly-once,
cleanup or schema lifecycle, so the driver declares no lifecycle group. The
public type and config are unchanged.

## Value fidelity

A non-integer JSON number is bound as its shortest decimal text, which DuckDB
casts exactly into a `DECIMAL` or `DOUBLE` column. See [value fidelity](https://faucet-hq.github.io/faucet-stream/reference/value-fidelity.html).
