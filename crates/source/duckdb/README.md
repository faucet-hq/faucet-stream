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

Results are read through DuckDB's **streaming** Arrow interface, one chunk
(about 2,048 rows) at a time, so memory is bounded by `batch_size` rather than
the result size and the first page arrives before the query finishes. (A
statement that cannot be wrapped in a subquery — `PRAGMA`, `SHOW`, … — is read
as a materialized result.)

| DuckDB type | JSON |
|-------------|------|
| integer types, `UBIGINT` | number |
| `HUGEINT` | number when it fits `i64`, else its exact decimal string |
| `UHUGEINT`, `BIGNUM` | exact decimal string |
| `DECIMAL` | exact decimal string (a bare literal such as `2.5` is a `DECIMAL`, so it arrives as `"2.5"`; `CAST` to `DOUBLE` for a JSON number) |
| `DOUBLE` / `REAL` | number (`REAL` through its shortest decimal form, so `0.1` stays `0.1`); NaN / ±Inf as `"NaN"` / `"Infinity"` / `"-Infinity"` |
| `BOOLEAN` | bool |
| `VARCHAR`, `UUID`, `ENUM` | string |
| `BLOB` | base64 string |
| `BIT` | its bit string (`"101"`) |
| `DATE` | `"2024-01-02"` |
| `TIME` | `"10:11:12.500"` |
| `TIMETZ` | `"10:00:00+05"` |
| `TIMESTAMP` / `TIMESTAMP_S` / `_MS` / `_NS` | ISO-8601 at the column's precision, `"2024-01-01T10:00:00.123456789"` |
| `TIMESTAMPTZ` | ISO-8601 UTC, `"2024-01-01T04:30:00Z"` |
| `infinity` / `-infinity` dates and timestamps | `"infinity"` / `"-infinity"` |
| `INTERVAL` | `{"months": …, "days": …, "nanos": …}` |
| `LIST` / `ARRAY` | array |
| `STRUCT` | object |
| `MAP` | object when the keys are strings, else `[{"key": …, "value": …}]` |
| `UNION` | the value of its active member |

A query whose result has two columns with the same name (`SELECT * FROM a JOIN
b`) is refused with an error naming the column — a JSON row holds one value per
name, so one of them would be lost. Alias the columns.

## Conformance

This crate wires the reusable [`faucet-conformance`](https://docs.rs/faucet-conformance)
battery in `tests/conformance.rs` — config-schema validity, bounded-memory
streaming (seeded temp database), and errors-not-panics.
