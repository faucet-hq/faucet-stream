# faucet-sink-redshift

Amazon Redshift sink connector for the
[`faucet-stream`](https://crates.io/crates/faucet-stream) ecosystem.

Redshift speaks the PostgreSQL wire protocol, so the sink connects through
`sqlx`'s Postgres driver. Two load paths:

- **`copy`** (default) — stage each page to S3 as JSONL or CSV, then bulk-load
  it with `COPY <table> FROM 's3://…' IAM_ROLE '<arn>' FORMAT …`, and delete the
  staged object (best-effort). This is Redshift's recommended, fastest bulk-load
  path.
- **`insert`** — multi-row `INSERT INTO … VALUES (…), (…)`. Portable and needs
  no S3, but slower for bulk data. Sub-chunked to respect Redshift's bind-param
  limit. Columns are the union of table columns present across the page; a
  record that shares *no* column with the table is skipped (with a warning)
  rather than inserted as an all-NULL row — on both strategies, reported as
  skipped in the page's row accounting, and failed on its own (so it reaches
  the DLQ) on the per-row path.

Append-only: Redshift has no `ON CONFLICT`, and `COPY` cannot upsert, so
`supported_write_modes()` is `[Append]`.

## Configuration

| Field | Required | Description |
|-------|----------|-------------|
| `host` / `port` / `database` / `user` / `credentials` / `tls` / `tls_mode` / `ssl_root_cert` | — | Connection block (see `faucet-common-redshift`); `tls_mode: verify_full` + `ssl_root_cert` verifies the server certificate. |
| `table_name` | yes | Target table (matched case-insensitively, as Redshift folds identifiers). |
| `schema` | no | Namespace qualifying the table. Unset = the session's `current_schema()`; same-named tables in other schemas are ignored. |
| `write_strategy` | no | `copy` (default) or `insert`. |
| `copy.format` | no | `jsonl` (default, `FORMAT AS JSON 'auto ignorecase'`) or `csv` (`FORMAT AS CSV NULL AS '__FAUCET_NULL__'`: a NULL is written as that marker and an empty string as `""`, so NULL text values do not load as empty strings). Both add `TIMEFORMAT 'auto' DATEFORMAT 'auto'`, so RFC 3339 instants (`2024-05-01T10:00:00Z`) and ISO dates load into `TIMESTAMP` / `TIMESTAMPTZ` / `DATE` columns. |
| `copy.staging_bucket` | copy only | S3 bucket for staged files. |
| `copy.staging_prefix` | no | Key prefix for staged objects. |
| `copy.iam_role` | copy only | IAM role ARN Redshift assumes to read the staged file. |
| `copy.region` | no | AWS region (S3 client + `COPY … REGION`). |
| `copy.endpoint_url` | no | S3-compatible endpoint override (testing). |
| `batch_size` | no | Rows per load unit (default `1000`; `0` = whole page). |
| `max_connections` | no | Pool size (default `5`). |

```yaml
host: my-cluster.abc123.us-east-1.redshift.amazonaws.com
database: dev
user: admin
credentials:
  type: password
  config:
    password: ${env:REDSHIFT_PASSWORD}
table_name: events
write_strategy: copy
copy:
  format: jsonl
  staging_bucket: my-redshift-staging
  staging_prefix: faucet/
  iam_role: arn:aws:iam::123456789012:role/redshift-copy
  region: us-east-1
```

The six `copy.*` keys are also still accepted flat at the config top level
(`copy_format`, `staging_bucket`, `staging_prefix`, `iam_role`, `region`,
`endpoint_url`) — **deprecated** since #654, and superseded wholesale when a
`copy:` block is present. They only ever applied to `write_strategy: copy`,
which is what the block now makes visible.

## How it is built

The sink runs on the family-neutral sink driver (RFC 0014): `RedshiftDriver`
implements `faucet_core::driver::SinkDriver` — the cross-page commit groups,
the `COPY`-from-S3 and multi-row `INSERT` load paths, table and column
creation — and `RedshiftSink` wraps it in a `DriverSink`, which serves the
plain, reported and per-row writes from that one primitive and derives the
capabilities from it. It does not use the SQL family driver
(`faucet_common_sql::SqlSinkDriver`): that adapter writes each page in its
own transaction, while this sink accumulates pages into commit groups and
loads them through S3. The public type and config are unchanged.

## Testing

Redshift has no local container image, and the `copy` path also needs a real S3
bucket + IAM role, so live load tests live in `tests/integration.rs` and are
`#[ignore]`d — they run only when `REDSHIFT_*` environment variables are set.

License: MIT OR Apache-2.0

## Auto-create (`create_table`)

`create_table` (**default `true`**, #580) creates the target table from the
first written page's inferred columns when it does not exist — a first-ever
sync cannot assume the destination is already there. Every inferred column is
created **nullable**: a column present in page 1 is not required forever, and a
`NOT NULL` inferred from one page fails page 2 the first time a record omits
the field. No DISTKEY or SORTKEY is chosen — faucet has no basis to pick either, and a wrong one is baked into the table. A field first seen on a later page (or missing from a pre-existing table) is added with `ALTER TABLE … ADD COLUMN`, typed the same way, before the page is buffered — the loads drop fields with no column, so it would otherwise be lost on every page.

Set `create_table: false` to require a pre-existing target; a missing one then
fails fast with the same error every table sink raises, naming both ways out,
and a record field the table has no column for fails the write naming the
field (drop or rename it with a transform).

## Commit accumulation (`commit_rows` / `commit_bytes`)

Records **accumulate across `write_batch` calls** and commit once per
threshold, plus once at `flush` (#617). Before this the commit unit was the
page unit, and `batch_size` could only ever *split* an oversized page — it
could never merge two undersized ones, so a small source page meant one
expensive warehouse operation per small page. `COPY` wants millions of rows per load; one `COPY` per 1000-row page produced many small commits, small unsorted blocks, and VACUUM pressure.

- `commit_rows` — records per commit. `0` removes the row limit.
- `commit_bytes` — estimated-bytes counterpart, bounding how much is buffered.
  `0` removes the byte limit.

With neither set, a group commits at **100,000 rows or ~256 MiB**, whichever
comes first, so a large load never holds the whole run in memory. A group
whose commit fails keeps its uncommitted rows, so a retried `flush` (for
example under a `resilience:` policy) commits them rather than reporting
success with nothing written.

`batch_size` still bounds an individual request inside a commit group, so a
very large group is split into reasonably-sized requests.

**Only the append path accumulates.** `delivery: exactly_once` and the DLQ
path commit per page, because a commit token must land atomically with its own
page, and a DLQ must report which rows of *this* page failed — neither is
expressible once pages are merged. With a `dlq:` block, earlier buffered pages are
committed first and a failed write sends exactly the rows of that page that did
not land to the DLQ; rows from chunks that committed before the failure are not
repeated.

## Batch atomicity

What a failed write leaves behind (#737): **atomic** for `write_strategy: copy` with `batch_size: 0`, otherwise **best-effort** — a commit group is one COPY only when unchunked; INSERT splits by bind parameters. `on_batch_error: dlq_all`
is refused on a best-effort configuration unless the `dlq:` block sets
`allow_duplicates_on_dlq_all: true` (a DLQ replay would write the rows that
already landed a second time). See
[batch atomicity](https://faucet-hq.github.io/faucet-stream/cookbook/dlq.html#batch-atomicity-and-dlq_all).

## Capabilities

Append only, with a staged load (`write_strategy: copy` bulk-loads through S3). `faucet list --json` prints the declaration.

## Timeouts, cancellation and retries

Every statement (`INSERT`, `COPY`, DDL, the catalog lookups) and every S3
staging request runs through the pipeline's `CallContext` (#880): each is sent
once, any caller deadline ends it, and each is metered as a round trip
(`execute`, `query`, `stage`). The pool's connect timeout still applies.

## Shared SQL layer

This sink takes its quoting, literal escaping, `$n` placeholders and 32767-parameter chunking, column types and the create-table, add-column and `INSERT` text from the `Redshift` dialect of
[`faucet-common-sql`](../../common/sql/README.md) (RFC 0013, #884), the same
place every SQL sink gets them. Row handling stays here: an append skips (with a warning) a record that shares no column rather than failing it, field names fold with Unicode lower-casing as Redshift does, and pages are buffered into `COPY`-from-S3 commit groups — semantics the engine's mapping does not have.

## Value fidelity

A column with an exact-decimal type (`NUMERIC(p,s)` / `DECIMAL`) is bound as
text cast to that type (`$n::numeric(38,9)`), so an exact decimal string from a
source (`"12345678901234567890.123456789"`) and a non-integer JSON number both
land with every digit — the server parses them, never a double. See
[value fidelity](https://faucet-hq.github.io/faucet-stream/reference/value-fidelity.html).
