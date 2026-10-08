# faucet-transform-sql

[![Crates.io](https://img.shields.io/crates/v/faucet-transform-sql.svg)](https://crates.io/crates/faucet-transform-sql)
[![Docs.rs](https://docs.rs/faucet-transform-sql/badge.svg)](https://docs.rs/faucet-transform-sql)
[![MSRV](https://img.shields.io/crates/msrv/faucet-transform-sql.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-transform-sql.svg)](https://github.com/faucet-hq/faucet-stream#license)

SQL-as-transform for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem — run [DuckDB](https://duckdb.org/) SQL over each pipeline page. The page's records are exposed as the relation `batch`; the query's result set replaces the page.

Reach for it when you need to filter, reshape, aggregate, or join your in-flight data with the full power of an analytical SQL engine, inline in the pipeline, without standing up a separate warehouse step. DuckDB is embedded (bundled at build time) and vectorized, so the transform is fast and self-contained — no external database, no network hop.

## Feature highlights

- **Full DuckDB SQL** — `SELECT`, `WHERE`, `GROUP BY`, window functions, CTEs, `json_extract`, casts, string/date functions — the page is just a table named `batch`.
- **Reference relations** — pre-load static lookup data from CSV, JSONL, inline `values`, or a small REST endpoint (`http`) and `JOIN` against it by name. Optional `reload_on_change` re-reads a file when its mtime changes; `http` is fetched once and cached for the whole run.
- **Vectorized JSON ↔ Arrow shovel** — records are moved into and out of DuckDB through Arrow batches (`arrow` / `arrow-json`), not row-by-row, so throughput stays high.
- **Compile-time query validation** — the query is parse/bind-checked inside DuckDB and missing relation files are caught at `faucet validate` / start of `faucet run`, never mid-stream.
- **Embedded engine** — DuckDB is bundled (`bundled` feature), so there are no system dependencies and the connection is built once per transform and reused for every page.
- **Tunable** — optional `memory_limit` and `threads` DuckDB pragmas keep resource use predictable in high-fan-out matrix runs.

## Installation

```bash
# As a library:
cargo add faucet-transform-sql

# In the umbrella crate (opt-in transform feature):
cargo add faucet-stream --features transform-sql

# In the CLI:
cargo install faucet-cli --features transform-sql
```

The `transform-sql` feature is **not** in the default build; it is included in `full`.

## Quick start

The `sql` transform is a pipeline-level (or matrix-row-level) transform — it goes under `transforms:`, between a source and a sink:

```yaml
# pipeline.yaml — faucet run pipeline.yaml
version: 1
pipeline:
  source:
    type: file
    config:
      path: data/users.csv
      has_header: true
  transforms:
    - type: sql
      config:
        query: "SELECT id, upper(name) AS name FROM batch WHERE active"
  sink:
    type: file
    config:
      path: ./active_users.jsonl
```

```bash
faucet run pipeline.yaml
```

Each result row becomes one output JSON record: column name → JSON key; `NULL` → JSON `null`; DuckDB `STRUCT` / `LIST` / `MAP` → nested JSON.

**Sparse records.** Arrow needs one column per key across the page, so a key some records lack is `NULL` for them inside DuckDB. When the query keeps `batch`'s columns (`SELECT *`, `SELECT * EXCLUDE (…)`, `SELECT *, … FROM batch WHERE …`) the transform remembers which keys each record did not carry and leaves them out of the output again, so an upsert sink does not overwrite those destination columns with `NULL`; an explicit `null` in the input stays `null`. A query that names its columns gets exactly those columns, `null` where a record had no value. A key that is a number in one record and a string in another becomes a string column (`1` → `"1"`).

**Sandbox.** The DuckDB connection has no host access once the reference relations are loaded: `enable_external_access` is off (no `read_text`, `read_csv`, `COPY … TO`, `ATTACH` or network reads; only the files of `reload_on_change` relations stay readable), extensions cannot be installed or autoloaded, and the configuration is locked, so a query cannot `SET` its own `memory_limit` or `threads`.

## Configuration reference

Wire shape: `{ type: sql, config: { query, relations?, memory_limit?, threads? } }`.

| Field | Type | Default | Description |
|---|---|---|---|
| `query` | string | — *(required)* | The SQL statement. The page's records are the relation `batch`. Must produce a result set; each result row becomes one output record. |
| `relations` | list of [`RelationSpec`](#reference-relations) | `[]` | Reference relations loaded once at compile time and joinable by name. |
| `memory_limit` | string | *(DuckDB's own)* | DuckDB `memory_limit` pragma (e.g. `"1GB"`). |
| `threads` | integer | *(DuckDB's own)* | DuckDB `threads` pragma. Set to `1`–`2` in high-fan-out matrices to avoid CPU over-subscription across rows. |

### Reference-relation fields (`RelationSpec`)

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | — *(required)* | Relation name as referenced in the query. Must be a safe SQL identifier and must not be `batch` (reserved for the page). |
| `source` | [`RelationSource`](#relation-source-types) | — *(required)* | Where the relation's data comes from. |
| `reload_on_change` | bool | `false` | Re-stat the file's mtime before each page; rebuild and atomically swap the relation if it changed. Ignored for `values` and `http` (both loaded once for the whole run). |

### Relation source types (`source.type`)

| `type` | Fields | Description |
|---|---|---|
| `csv` | `path` (string, required), `has_header` (bool, default `true`) | Delimited file loaded via DuckDB `read_csv_auto`. |
| `jsonl` | `path` (string, required) | Newline-delimited JSON loaded via DuckDB `read_json_auto`. |
| `values` | `columns` (list of strings, required), `rows` (list of lists, required) | Inline rows materialized into a table; no file I/O. Each inner row must have the same length as `columns`. |
| `http` | `url` (string, required), `method` (`GET`\|`POST`, default `GET`), `headers` (map of string→string, optional), `records_path` (JSONPath string, optional) | Rows fetched from a small REST endpoint **once** at compile time and cached for the whole run. `records_path` selects the row array in the response body (e.g. `$.items[*]`); if omitted the whole body must be a JSON array. Every selected element must be a JSON object (one table row). |

## Reference relations

Pre-load static lookup data (CSV, JSONL, inline `values`, or an `http` endpoint) that your query can `JOIN` against. Relations are loaded **once at compile time** (when `faucet validate` / `faucet run` first reads the config) and remain resident for the lifetime of the transform.

```yaml
- type: sql
  config:
    query: |
      SELECT b.id, c.country
      FROM batch b
      LEFT JOIN countries c ON b.code = c.code
    relations:
      - name: countries
        source:
          type: csv
          path: data/countries.csv
          has_header: true        # default true
```

Inline `values` need no file I/O:

```yaml
relations:
  - name: tiers
    source:
      type: values
      columns: [id, label]
      rows:
        - [1, gold]
        - [2, silver]
        - [3, bronze]
```

An `http` relation fetches its rows from a small REST endpoint **once** at compile time and caches them for the whole run (every page joins against the same in-memory table — the endpoint is never re-hit):

```yaml
relations:
  - name: named_lists
    source:
      type: http
      url: https://api.example.com/v1/company/named-lists
      method: GET                 # default GET; POST also supported
      headers:                    # optional static headers
        Authorization: "Bearer ${env:HIBOB_TOKEN}"
      records_path: "$.items[*]"  # JSONPath to the row array; omit if the body is already an array
```

`records_path` is a JSONPath selecting the row array in the response body; each selected element must be a JSON object (one table row). If `records_path` is omitted, the whole response body must be a JSON array. A non-array body without a `records_path`, or a path that selects non-object values, is a clear compile-time configuration error. Keep auth simple: pass a bearer token via a static `headers` entry (interpolated at the CLI layer with `${env:...}` / `${vault:...}`); the shared `auth:` catalog is not wired here.

### `reload_on_change`

```yaml
relations:
  - name: prices
    source:
      type: csv
      path: data/prices.csv
    reload_on_change: true   # re-read when the file's mtime changes
```

When `true`, faucet stats the file before each page and rebuilds the relation atomically if the mtime changed. Defaults to `false`. Ignored for `values` and `http` (both loaded once for the whole run).

The name `batch` is reserved for the page relation. Using it as a relation name is a compile-time error.

## Per-page semantics and `batch_size: 0`

**This is the most important thing to understand about the SQL transform.**

The transform runs once **per page**, not once across the whole stream. With the default `batch_size` of 1000, `GROUP BY` and window functions aggregate within a single 1000-row page — not across all pages.

```yaml
# BAD: GROUP BY runs per-page, giving partial aggregates.
pipeline:
  source:
    type: file
    config:
      path: data/orders.csv
  transforms:
    - type: sql
      config:
        query: "SELECT country, SUM(amount) AS total FROM batch GROUP BY country"
```

**To aggregate across the whole dataset**, set the source's `batch_size: 0` so the entire result set arrives as one page:

```yaml
# CORRECT: batch_size: 0 loads the whole file as one page → global GROUP BY.
pipeline:
  source:
    type: file
    config:
      path: data/orders.csv
      batch_size: 0
  transforms:
    - type: sql
      config:
        query: "SELECT country, SUM(amount) AS total FROM batch GROUP BY country"
```

`batch_size: 0` means "no batching" — the source emits the entire result set as a single `StreamPage`. All sources support it; it is appropriate for small lookup tables and for aggregating transforms like this one. Be mindful of memory: `batch_size: 0` buffers the whole result set in RAM, so use it for datasets that comfortably fit in memory.

When an aggregating query receives a second page (i.e. `batch_size` was not set to `0`), faucet emits a one-time warning:

```
WARN faucet::transform::sql: sql transform with aggregation received multiple pages;
aggregation is per-page — set batch_size: 0 for global aggregation
```

## Error handling and validation

**At config load time** (`faucet validate` / start of `faucet run`):

- The query is parse/bind-checked inside DuckDB. Syntax errors report line and column number.
- Reference-relation files that do not exist cause an immediate error (before any page is processed).
- An `http` relation is fetched here: a request failure, a non-2xx status, an undecodable body, a non-array body without a `records_path`, or a path selecting non-object rows all cause an immediate error (before any page is processed).
- A relation named `batch` is rejected.

**At runtime** (per page):

- A query that fails mid-run aborts the pipeline immediately with `FaucetError::Transform`.
- Runtime query errors are **not** routed to the dead-letter queue — they follow the same fail-fast policy as every other built-in transform.

Empty result sets are valid: a query that matches zero rows produces zero output records for that page.

## Examples

### Filter and reshape (per-page, no aggregation)

```yaml
transforms:
  - type: sql
    config:
      query: |
        SELECT id,
               lower(email)        AS email,
               coalesce(plan, 'free') AS plan
        FROM batch
        WHERE deleted_at IS NULL
```

### Global aggregation joined to a reference CSV

This mirrors `cli/examples/csv_to_jsonl_sql.yaml` — group order data by country and join to a reference countries CSV:

```yaml
version: 1
name: csv_to_jsonl_sql

pipeline:
  source:
    type: file
    config:
      path: cli/examples/data/orders.csv
      has_header: true
      batch_size: 0          # whole file as one page → global GROUP BY

  transforms:
    - type: sql
      config:
        query: |
          SELECT c.country,
                 COUNT(*)                      AS order_count,
                 SUM(CAST(o.amount AS DOUBLE)) AS total_amount
          FROM   batch o
          LEFT JOIN countries c ON o.country_code = c.code
          GROUP BY c.country
          ORDER BY c.country
        relations:
          - name: countries
            source:
              type: csv
              path: cli/examples/data/countries.csv
              has_header: true

  sink:
    type: file
    config:
      path: /tmp/faucet_sql_demo.jsonl
```

Run it (requires the `transform-sql`, `source-csv`, and `sink-jsonl` features):

```bash
faucet run cli/examples/csv_to_jsonl_sql.yaml
```

Output rows look like:

```json
{"country":"Germany","order_count":1,"total_amount":3.0}
{"country":"India","order_count":1,"total_amount":7.0}
{"country":"United States","order_count":2,"total_amount":15.5}
```

### Enrich with an inline lookup table

```yaml
transforms:
  - type: sql
    config:
      query: |
        SELECT b.id, b.tier_id, t.label AS tier
        FROM batch b
        LEFT JOIN tiers t ON b.tier_id = t.id
      relations:
        - name: tiers
          source:
            type: values
            columns: [id, label]
            rows:
              - [1, gold]
              - [2, silver]
              - [3, bronze]
```

## Working with JSON columns

DuckDB's `json_extract` works on string or JSON columns. If a field is a JSON string, use it directly:

```sql
SELECT json_extract(payload, '$.user.id') AS user_id FROM batch
```

Or cast it first:

```sql
SELECT json_extract(payload::JSON, '$.user.id') AS user_id FROM batch
```

## Timestamp / timezone note

DuckDB's `TIMESTAMP` type is timezone-naive. faucet JSON timestamps are RFC 3339 strings (e.g. `"2026-01-01T12:00:00Z"`). To compare or cast them:

```sql
-- Parse an RFC 3339 string into a DuckDB TIMESTAMP (drops the offset)
SELECT CAST(created_at AS TIMESTAMP) AS ts FROM batch
WHERE CAST(created_at AS TIMESTAMP) > '2026-01-01'::TIMESTAMP

-- Keep the string form, compare lexicographically (safe for UTC-only data)
SELECT * FROM batch WHERE created_at > '2026-01-01T00:00:00Z'
```

If your timestamps include non-UTC offsets, normalise them to UTC with `cast` before passing to the SQL transform, or parse with `strptime`.

## Config loading & schema

Configs load from YAML/JSON. Inspect the full JSON Schema for the transform with:

```bash
faucet schema transform sql
```

## Library usage

Library callers build the compiled transform and attach it to any `Source` via a page stage and `faucet_core::TransformingSource`:

```rust
use faucet_transform_sql::{SqlTransform, SqlTransformConfig};
use faucet_core::stage::{compile_stage, apply_stages_to_page};

# fn run(records: Vec<serde_json::Value>) -> Result<(), faucet_core::FaucetError> {
let cfg = SqlTransformConfig {
    query: "SELECT id, upper(name) AS name FROM batch".into(),
    relations: vec![],
    memory_limit: None,
    threads: None,
};

let t = SqlTransform::compile(&cfg)?;          // parse/bind-checks the query now
let stage = compile_stage(&t.into_page_stage())?;

let output = apply_stages_to_page(records, &[stage])?;
# let _ = output;
# Ok(())
# }
```

To wrap a whole source, hand the same stage to `faucet_core::TransformingSource::new(source, vec![stage])` and run it through `Pipeline` / `run_stream` like any other source. `SqlTransform::compile` is the canonical entry point; `into_page_stage()` produces a `TransformStage::PageFn` (a page-level, whole-batch stage).

## How it works

1. `SqlTransform::compile(&cfg)` opens an in-memory DuckDB connection, applies the `memory_limit` / `threads` pragmas, materializes every reference relation, and parse/bind-checks the query — all once.
2. For each page, the records are converted to an Arrow batch and registered as the `batch` relation (a vectorized JSON ↔ Arrow shovel, not row-by-row).
3. The query runs; the result set is converted back from Arrow to JSON records, which replace the page.
4. With `reload_on_change`, a relation's file mtime is checked before each page and the relation is rebuilt and atomically swapped if it changed.

The DuckDB connection and all relations are owned by the compiled `SqlTransform` and reused for every page — there is no per-page connection setup.

## Feature flags

| Feature | Enables |
|---|---|
| `transform-sql` (CLI / umbrella) | The `sql` transform type. Pulls in this crate (`duckdb` with `bundled` + `vtab-arrow`, `arrow`, `arrow-json`). Not in `default`; included in `full`. |

This crate itself has no optional features of its own; the `bundled` and `vtab-arrow` DuckDB features are always on.

```bash
# Enable only the SQL transform
cargo add faucet-stream --features transform-sql

# Enable together with specific connectors
cargo add faucet-stream --features source-csv,sink-jsonl,transform-sql

# Enable everything
cargo add faucet-stream --features full
```

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| `GROUP BY` / window results look partial or per-chunk | The transform runs **per page**. Set the source's `batch_size: 0` so the whole dataset arrives as one page (see [Per-page semantics](#per-page-semantics-and-batch_size-0)). |
| `WARN ... aggregation is per-page` | An aggregating query saw a second page. Set the source's `batch_size: 0` for global aggregation, or accept per-page aggregates. |
| Out-of-memory with `batch_size: 0` | The whole result set is buffered in RAM. Cap DuckDB with `memory_limit`, or aggregate in stages, or keep `batch_size` bounded if global aggregation isn't required. |
| `FaucetError::Config` at validate time, "syntax error" | The query failed DuckDB parse/bind. The error reports line/column — fix the SQL. |
| Validate fails: relation file not found | A `csv` / `jsonl` relation `path` doesn't exist. Paths are relative to the working directory; use an absolute path or run from the right directory. |
| Compile error: relation named `batch` | `batch` is reserved for the page relation. Rename the reference relation. |
| `FaucetError::Transform` aborts the run mid-stream | A runtime query error (e.g. a cast that fails on real data). These are **not** sent to the DLQ — fix the query or pre-clean the data with an earlier transform. |
| A JSON column won't parse | Cast it: `json_extract(payload::JSON, '$.path')` (see [Working with JSON columns](#working-with-json-columns)). |
| Timestamp comparisons behave oddly | DuckDB `TIMESTAMP` is timezone-naive; RFC 3339 strings carry offsets. Normalise to UTC first (see [Timestamp / timezone note](#timestamp--timezone-note)). |
| High CPU in a large matrix run | Each row's transform spins up DuckDB threads. Set `threads: 1` or `2` on the SQL config to avoid over-subscription. |

## See also

- [SQL transform cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/transforms.html) — the transforms model and worked examples.
- [Transforms reference](https://faucet-hq.github.io/faucet-stream/reference/config.html) — the full `transforms:` grammar and layering rules.
- [`faucet-core`](https://crates.io/crates/faucet-core) — the `TransformStage` / `TransformingSource` types this transform plugs into.
- [`faucet-stream`](https://crates.io/crates/faucet-stream) — the umbrella crate that exposes the `transform-sql` feature.
- [DuckDB SQL documentation](https://duckdb.org/docs/sql/introduction) — the dialect available inside the `query`.

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.
