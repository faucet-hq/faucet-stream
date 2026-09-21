# SQL transform

Run embedded DuckDB SQL over each pipeline page. Each page's records are exposed as
the relation `batch`; the query result replaces the page. Column name becomes JSON
key; `NULL` becomes JSON `null`; `STRUCT`/`LIST`/`MAP` become nested JSON.

Requires the `transform-sql` Cargo feature (CLI + umbrella; not in defaults; in `full`).

## Overview

The `sql` transform embeds DuckDB in-process — no external database, no network
round-trip. Every time a page of records arrives from the source, faucet registers
that page as a temporary Arrow-backed relation named `batch` and executes your
query. The result set is the new page forwarded to the next transform or to the sink.

Config shape:

```yaml
transforms:
  - type: sql
    config:
      query: "SELECT id, upper(name) AS name FROM batch WHERE active"
```

All standard DuckDB SQL is available: filtering, projection, type casting,
aggregation, window functions, `regexp_replace`, `json_extract`, date/time
arithmetic, and `JOIN` to reference relations (see below).

## The `batch` relation

When your query runs, `batch` contains the current page's records as a table.
Column types are inferred from the JSON values in each record:

| JSON type | DuckDB type |
|---|---|
| integer | `BIGINT` |
| float | `DOUBLE` |
| string | `VARCHAR` |
| boolean | `BOOLEAN` |
| null | nullable column |
| array | `LIST` |
| object | `STRUCT` |

You can `SELECT *`, project individual columns, rename with `AS`, cast types, add
computed columns — anything DuckDB supports as a `SELECT` statement.

`batch` is reserved. Using it as a reference relation name is a compile-time error.

## Per-page semantics and `batch_size: 0`

**This is the most important thing to know about the SQL transform.**

The query runs **once per page**, not once across the whole stream. `GROUP BY`,
`COUNT(*)`, window functions, and any other aggregation operate within a single
page only.

With the default `batch_size` of 1000, a GROUP BY across 10,000 records runs on
10 separate pages of 1000 rows each — giving 10 sets of partial results rather than
one global result.

```yaml
# WRONG for global aggregation — GROUP BY sees only one page at a time.
transforms:
  - type: sql
    config:
      query: "SELECT country, COUNT(*) AS n FROM batch GROUP BY country"
```

**To aggregate globally, set `batch_size: 0` on the source.** This is the sentinel
value meaning "no batching" — the source emits the entire result set as a single
page, so the SQL transform sees all rows at once.

```yaml
pipeline:
  source:
    type: csv
    config:
      path: data/orders.csv
      batch_size: 0          # ← load everything as one page
  transforms:
    - type: sql
      config:
        query: "SELECT country, COUNT(*) AS n FROM batch GROUP BY country"
```

`batch_size: 0` is supported by every source. It is appropriate when the full
dataset fits in memory and you need global semantics.

When an aggregating query receives a second page without `batch_size: 0`, faucet
logs a one-time warning to help you catch the footgun:

```
WARN faucet::transform::sql: sql transform with aggregation received multiple pages;
aggregation is per-page — set batch_size: 0 for global aggregation
```

## Reference relations

Join pre-loaded lookup data against `batch`:

```yaml
transforms:
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
            has_header: true   # default true
```

Reference relations are loaded **once at compile time** (the moment `faucet validate`
or `faucet run` reads the config) and remain resident for the run. Missing files
are caught at load time — not mid-run.

### Source types

| `type` | Required fields | Notes |
|---|---|---|
| `csv` | `path` | `has_header` defaults to `true` |
| `jsonl` | `path` | Loaded via DuckDB `read_json_auto` |
| `values` | `columns`, `rows` | Inline; no file I/O |

Inline values:

```yaml
relations:
  - name: tiers
    source:
      type: values
      columns: [id, label]
      rows:
        - [1, gold]
        - [2, silver]
```

### `reload_on_change`

```yaml
relations:
  - name: prices
    source:
      type: csv
      path: data/prices.csv
    reload_on_change: true
```

When `true`, faucet stats the file's mtime before each page and rebuilds the
relation if it changed. Useful for reference files that are updated while the
pipeline is running (e.g. a nightly price list). Default `false`. Ignored for
`values`.

## JSON columns

Use `json_extract` on string fields that contain JSON:

```sql
-- Extract a nested field
SELECT json_extract(payload, '$.user.id') AS user_id,
       json_extract(payload, '$.event.name') AS event_name
FROM batch
```

For explicit typing:

```sql
SELECT CAST(json_extract(payload, '$.amount') AS DOUBLE) AS amount
FROM batch
```

If the field is typed as `JSON` rather than `VARCHAR`, omit the cast:

```sql
SELECT payload.user.id AS user_id FROM batch
```

## Timestamp and timezone

DuckDB's `TIMESTAMP` type is timezone-naive. faucet JSON timestamps are RFC 3339
strings (e.g. `"2026-01-01T12:00:00Z"`).

**UTC-only data** — compare lexicographically or cast:

```sql
SELECT * FROM batch
WHERE created_at > '2026-01-01T00:00:00Z'
-- or
WHERE CAST(created_at AS TIMESTAMP) > '2026-01-01'::TIMESTAMP
```

**Data with non-UTC offsets** — normalise upstream with the `cast` transform or
`TIMESTAMPTZ`:

```sql
SELECT TIMESTAMPTZ created_at AT TIME ZONE 'UTC' AS created_utc FROM batch
```

The safest approach is to normalise timestamps to UTC strings before they reach the
SQL transform, using the `cast` built-in transform upstream.

## Validation with `faucet validate`

```bash
faucet validate pipeline.yaml
```

`faucet validate` runs the SQL transform's compile step: DuckDB parse/bind-checks
the query and reports syntax errors with line and column number before any data is
touched. Reference-relation files that do not exist are also caught here.

Example error output:

```
error: sql transform: invalid query: Parser Error: syntax error at or near "SELEKT"
  --> line 1, col 1
```

Runtime errors (e.g. type mismatches that only appear with real data) abort the
run and are reported as `FaucetError::Transform`.

## Full example — GROUP BY and JOIN

The runnable file is `cli/examples/csv_to_jsonl_sql.yaml`.

Data:

```
# cli/examples/data/orders.csv
order_id,country_code,amount
1,US,10.0
2,US,5.5
3,IN,7.0
4,DE,3.0
```

```
# cli/examples/data/countries.csv
code,country
US,United States
IN,India
DE,Germany
```

Config:

```yaml
version: 1
name: csv_to_jsonl_sql

pipeline:
  source:
    type: csv
    config:
      path: cli/examples/data/orders.csv
      has_headers: true
      batch_size: 0          # whole file as one page → global GROUP BY

  transforms:
    - type: sql
      config:
        query: |
          SELECT c.country,
                 COUNT(*)                     AS order_count,
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
    type: jsonl
    config:
      path: /tmp/faucet_sql_demo.jsonl
```

Run it:

```bash
faucet validate cli/examples/csv_to_jsonl_sql.yaml
faucet run     cli/examples/csv_to_jsonl_sql.yaml
```

Output (`/tmp/faucet_sql_demo.jsonl`):

```json
{"country":"Germany","order_count":1,"total_amount":3.0}
{"country":"India","order_count":1,"total_amount":7.0}
{"country":"United States","order_count":2,"total_amount":15.5}
```

## SQL vs. built-in transforms

| Situation | Recommended approach |
|---|---|
| Rename, drop, select, cast a few fields | Built-in `rename_field` / `drop` / `select` / `cast` — lighter, no DuckDB overhead |
| PII redaction | Built-in `redact` |
| Re-case keys | Built-in `keys_case` |
| Complex reshape, JOIN, computed columns | `sql` |
| Global aggregation / GROUP BY | `sql` with `batch_size: 0` |
| Window functions | `sql` with `batch_size: 0` if global; `sql` as-is if per-page windowing is what you want |
| Live-updating lookup join | `sql` with `reload_on_change: true` on the reference relation |

Use the built-in transforms for simple field-level operations — they are
always-on, have no external dependencies, and carry zero extra compile weight.
Reach for `sql` when you need expressive SQL semantics: multi-table joins,
aggregation, window functions, or any computation the built-ins cannot express.
