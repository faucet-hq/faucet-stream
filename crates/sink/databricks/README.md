# faucet-sink-databricks

Databricks **SQL warehouse sink** for the
[`faucet-stream`](https://crates.io/crates/faucet-stream) ecosystem. Loads pages
into a Delta table through a SQL warehouse's
[Statement Execution API](https://docs.databricks.com/api/workspace/statementexecution)
(plain REST — no JDBC/ODBC driver), under the warehouse's Unity Catalog
permissions. Completes the pair with
[`faucet-source-databricks`](https://crates.io/crates/faucet-source-databricks);
both share connection and auth types through
[`faucet-common-databricks`](https://crates.io/crates/faucet-common-databricks).

For landing files in object storage without a warehouse, use
[`faucet-sink-delta`](https://crates.io/crates/faucet-sink-delta).

## Highlights

- **Two load paths** — a multi-row `INSERT … SELECT … FROM VALUES` for small
  pages, or a staged Parquet file loaded with `COPY INTO` for large ones.
  Staging goes to a Unity Catalog **volume** (uploaded through the Files API
  with the sink's own token) or to S3 / GCS / ADLS behind the `staging` feature.
- **One typing rule on both paths** — every value travels as a string cell and
  is cast to the column's *declared* type on the server (`CAST(… AS <type>)`,
  `from_json` for `STRUCT`/`ARRAY`/`MAP`, `parse_json` for `VARIANT`), so the
  insert and staged paths can never land a value differently.
- **Write modes** — `append`, `upsert` / `delete` via `MERGE` (with
  `delete_marker`), and `overwrite` via a staging table and an atomic swap.
- **Exactly-once** — a `_faucet_commit_token` watermark plus a data write that
  is idempotent per page (see below).
- **Schema drift** — `current_schema` from `information_schema.columns`;
  `evolve` adds columns and widens where Delta allows. Tables are created from
  the first page when `create_table: true`.
- **Warm-up tolerant** — submits refused with `429`/`503` while a serverless
  warehouse starts are retried with backoff; long statements are polled up to
  `statement_timeout_secs` and then cancelled; Delta concurrent-write conflicts
  are retried.

## Configuration

| Field | Type | Default | Notes |
|---|---|---|---|
| `workspace_url` | string | — (required) | `https://<host>.cloud.databricks.com` |
| `warehouse_id` | string | — (required) | target SQL warehouse id |
| `auth` | `{ type, config }` / `{ ref }` | — (required) | `pat` or `token` bearer, or a shared provider (OAuth M2M — see below) |
| `catalog` | string? | warehouse default | Unity Catalog catalog |
| `schema` | string | — (required) | target schema (must exist) |
| `table` | string | — (required) | target table |
| `create_table` | bool | `true` | create the table from the first page's inferred schema |
| `load_method` | `auto` \| `insert` \| `copy_into` | `auto` | `auto` stages when `staging` is set and the page is ≥ `copy_threshold_bytes` |
| `staging.location` | string | — | `/Volumes/<catalog>/<schema>/<volume>[/prefix]`, `s3://…`, `gs://…`, `abfss://<container>@<account>.dfs.core.windows.net/…` |
| `staging.cleanup` | `always` \| `on_success` \| `never` | `always` | when staged files are deleted (best-effort, logged) |
| `staging.copy_options` | string? | — | extra `COPY_OPTIONS` entries, verbatim |
| `copy_threshold_bytes` | int | `1048576` | `auto` staging threshold |
| `batch_size` | int | `1000` | rows per `INSERT` / `MERGE` statement on the insert path; `0` = bounded by bytes only |
| `max_statement_bytes` | int | `8388608` | statement-text bound on the insert path (API limit 16 MiB) |
| `wait_timeout_secs` | int | `50` | server wait before async (`0` or `5`–`50`) |
| `poll_interval_ms` | int | `1000` | poll cadence while queued / running |
| `statement_timeout_secs` | int | `3600` | client deadline per statement, then cancel (`0` = none) |
| `max_retries` | int | `5` | `429`/`503` and Delta conflict retries |
| `retry_backoff_ms` | int | `1000` | exponential backoff base |
| `write_mode` | `append` \| `upsert` \| `delete` \| `overwrite` | `append` | |
| `key` | `[string]` | `[]` | required for `upsert` / `delete` |
| `delete_marker` | `{ field, values }` | — | `upsert` only: matching rows become deletes |

```yaml
pipeline:
  sink:
    type: databricks
    config:
      workspace_url: https://dbc-xxxx.cloud.databricks.com
      warehouse_id: 0123456789abcdef
      catalog: main
      schema: sales
      table: orders
      auth: { type: pat, config: { token: "${env:DATABRICKS_TOKEN}" } }
      staging:
        location: /Volumes/main/sales/faucet_stage
      write_mode: upsert
      key: [order_id]
```

Columns in a record that the table does not have are an error (never silently
dropped); add a `schema: { on_drift: evolve }` (or `ignore`) policy to handle
them.

### OAuth M2M (service principal)

Configure a client-credentials provider in the top-level `auth:` catalog and
reference it — the token is refreshed before expiry and shared by every
connector that references it:

```yaml
auth:
  databricks_sp:
    type: oauth2
    config:
      token_url: https://dbc-xxxx.cloud.databricks.com/oidc/v1/token
      client_id: "${env:DATABRICKS_CLIENT_ID}"
      client_secret: "${env:DATABRICKS_CLIENT_SECRET}"
      scopes: [all-apis]
# …then in the sink config:  auth: { ref: databricks_sp }
```

## Load paths

| Path | Append | Upsert / delete | Exactly-once append |
|---|---|---|---|
| insert | `INSERT INTO t (cols) SELECT <casts> FROM VALUES …` (split by `batch_size` / `max_statement_bytes`) | one `MERGE` per chunk | first chunk `INSERT INTO t REPLACE WHERE …`, later chunks `INSERT` |
| staged | `COPY INTO t FROM (SELECT <casts> FROM '<dir>') FILEFORMAT = PARQUET FILES = ('<file>')` | `MERGE … USING (SELECT <casts> FROM read_files('<file>'))` | `INSERT INTO t REPLACE WHERE … SELECT … FROM read_files('<file>')` |

Staged files are all-`STRING` Parquet (typing happens in the `SELECT`). File
names:

- **exactly-once pages** — `_faucet/<table>/<scope-hash>/<seq>.parquet`,
  derived from the page token only, so every attempt of a page writes and reads
  the same file;
- **other pages** — `_faucet/<table>/<run>/part-<n>-<content-hash>.parquet`.
  `COPY INTO` records every file it loaded into a table and skips it on a
  repeat, so re-submitting the load after an ambiguous failure cannot load it
  twice, while two identical pages still land as two files.

The warehouse must be able to read the staging location: a volume it has
`READ VOLUME` on, or a cloud path covered by a Unity Catalog external location.
Cloud uploads use ambient credentials (the `object_store` default chains) and
need the crate's `staging` feature.

## Write modes

- **append** — the load paths above.
- **upsert / delete** — one `MERGE INTO t USING (…) AS s ON t.k = s.k` per
  statement: `WHEN MATCHED AND op = 'd' THEN DELETE`, `WHEN MATCHED THEN UPDATE`,
  `WHEN NOT MATCHED AND op = 'u' THEN INSERT`. Rows are deduplicated per page
  (last write wins) before the `MERGE`, so it never sees two source rows for one
  key. Rows missing a key fail the batch, or go to the DLQ per row via
  `write_batch_partial`.
- **overwrite** — `begin` drops any leftover `<table>__faucet_ovw` and creates
  it `LIKE` the target; every page lands in it; `commit` runs one
  `INSERT OVERWRITE TABLE t SELECT * FROM <staging>` (a single Delta commit —
  readers see the old or the new contents, never a mix) and drops the staging
  table; `abort` drops it. On a first run (no target yet, `create_table: true`)
  the first page creates the staging table and `commit` renames it into place.
  Every step derives state from the warehouse, not the sink instance.

## Exactly-once delivery

Databricks SQL has no multi-table transaction, so the data and the watermark
cannot commit in one statement. The sink makes the **data write idempotent per
page token** instead, then advances the watermark:

1. **append** — the table carries two bookkeeping columns, `_faucet_scope`
   (the pipeline state key) and `_faucet_seq` (the page sequence), added
   automatically (`ALTER TABLE ADD COLUMNS`) the first time exactly-once runs.
   The page is written with one atomic Delta commit,
   `INSERT INTO t REPLACE WHERE _faucet_scope = '<scope>' AND _faucet_seq >= <seq> …`,
   which removes whatever an earlier attempt of this page (or any later one)
   left under the scope and inserts the page. An empty page runs the matching
   `DELETE`.
2. **upsert / delete** — the `MERGE` is keyed, so re-applying it converges.
3. The watermark row `(scope, token)` is upserted into
   `<catalog>.<schema>._faucet_commit_token` with a `MERGE`.

A crash between steps 1–2 and step 3 leaves the watermark one page behind; on
resume the pipeline re-sends that page under the same sequence and step 1
replaces the earlier attempt's rows — regardless of whether the replayed page
has the same boundaries. The guarantee is **effectively-once per scope**; the
bookkeeping columns are visible in the table (they are hidden from
`current_schema`). `write_mode: overwrite` cannot be combined with
exactly-once. `REPLACE WHERE` takes no column list, so every table column is
projected (missing ones as typed `NULL`); tables with generated or identity
columns are not supported on the exactly-once append path.

## Schema drift

`current_schema()` reads `information_schema.columns` (`full_data_type`,
`is_nullable`). `evolve_schema` is idempotent (existing columns are skipped):

| Change | DDL |
|---|---|
| new column | `ALTER TABLE t ADD COLUMNS (…)` (`BIGINT` / `DOUBLE` / `BOOLEAN` / `STRING`) |
| `tinyint`/`smallint`/`int`/`float` → number | `SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')` + `ALTER COLUMN … TYPE DOUBLE` |
| gained nullability | `ALTER COLUMN … DROP NOT NULL` |

Delta cannot widen `BIGINT` (the type auto-created integer columns get) to
`DOUBLE` in place, so that change fails `evolve` with an explanatory error —
cast the field upstream (e.g. a `cast` transform) or migrate the table.
Auto-created tables use `BIGINT` / `DOUBLE` / `BOOLEAN` / `STRING` (nested values
as JSON text) with every column nullable.

## Preflight (`faucet doctor`)

`check()` is read-only: a `SELECT 1` on the warehouse, then a column listing of
the target (`Skip` when it is missing and will be created).

## Features

| Feature | Enables |
|---|---|
| `staging` | S3 / GCS / ADLS staging locations via `object_store` (volume staging needs no feature) |
