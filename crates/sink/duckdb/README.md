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

In `auto_map` mode a record with no field matching a column of the table is
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
