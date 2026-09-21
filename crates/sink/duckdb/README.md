# faucet-sink-duckdb

DuckDB sink connector for the [faucet-stream](https://github.com/faucet-hq/faucet-stream)
data-movement platform. Writes JSON records to a DuckDB table using either a
single JSON text column or dynamic column mapping. Each batch is one
`BEGIN`/`COMMIT` transaction of `batch_size`-row multi-row `INSERT`s, rolled
back on error.

DuckDB is a synchronous embedded engine, so writes run on a blocking thread.
The target table must already exist. The sink is **append-only**; keyed upsert
and an Arrow-native columnar fast path are tracked as follow-ups.

## Config

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `database` | string | — | Path to the `.duckdb` file, or `:memory:`. A `duckdb://` / `duckdb:` prefix is accepted and stripped. |
| `table_name` | string | — | Target table (must already exist). |
| `column_mapping` | enum | `{json: {column: "data"}}` | `json` stores each record as one JSON text column; `auto_map` maps top-level keys onto matching columns. |
| `batch_size` | integer | `1000` | Rows per multi-row INSERT. `0` = one INSERT for the whole slice. |

## Example

```yaml
version: 1
pipeline:
  source:
    type: jsonl
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
the field (narrowing later is the `schema:` drift policy's job).

Set `create_table: false` to require a pre-existing target; a missing one then
fails fast with the same error every table sink raises, naming both ways out.
