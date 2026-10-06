# faucet-source-duckdb

DuckDB query source connector for the [faucet-stream](https://github.com/faucet-hq/faucet-stream)
data-movement platform. Opens a DuckDB database (a file, or in-memory), runs a
configured SQL query, and streams rows as JSON with bounded memory.

DuckDB is a synchronous embedded engine, so every database call runs on a
blocking thread; streaming hands bounded pages to the async pipeline over a
small channel rather than buffering the whole result set.

## Config

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `database` | string | — | Path to the `.duckdb` file, or `:memory:`. A `duckdb://` / `duckdb:` prefix is accepted and stripped. |
| `query` | string | — | SQL query to execute. In a parent/child matrix run, `${parent.field}` tokens are bound as parameters (safe against injection); library callers pass `{key}` placeholders through `fetch_with_context`. |
| `read_only` | bool | `false` | Open read-only. DuckDB allows many read-only connections to one file but only a single read-write connection. |
| `batch_size` | integer | `1000` | Rows per emitted page. `0` = emit the entire result set as one page. Validated at config load: an empty `database` / `query`, or a `batch_size` above `MAX_BATCH_SIZE` (1,000,000), is rejected with `FaucetError::Config`. |

## Example

```yaml
version: 1
pipeline:
  source:
    type: duckdb
    config:
      database: analytics.duckdb
      query: "SELECT id, name, amount FROM sales WHERE amount > 0 ORDER BY id"
      batch_size: 5000
  sink:
    type: file
    config:
      path: sales.jsonl
```

## Type mapping

Scalar DuckDB types map exactly to JSON (integers → number, `DOUBLE`/`FLOAT` →
number, `BOOLEAN` → bool, `VARCHAR` → string). `BLOB` is base64-encoded so
binary survives the JSON round-trip. Temporal (`TIMESTAMP`/`DATE`/`TIME`),
`DECIMAL`, and nested (`LIST`/`STRUCT`/`MAP`) values are best-effort: temporal
types surface their raw integer representation and the rest fall back to a
stable string. A future Arrow-native columnar fast path is tracked separately.

## Conformance

This crate wires the reusable [`faucet-conformance`](https://docs.rs/faucet-conformance)
battery in `tests/conformance.rs` — config-schema validity, bounded-memory
streaming (seeded temp database), and errors-not-panics.
