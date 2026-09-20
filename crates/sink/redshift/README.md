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
  rather than inserted as an all-NULL row.

Append-only: Redshift has no `ON CONFLICT`, and `COPY` cannot upsert, so
`supported_write_modes()` is `[Append]`.

## Configuration

| Field | Required | Description |
|-------|----------|-------------|
| `host` / `port` / `database` / `user` / `credentials` / `tls` | — | Connection block (see `faucet-common-redshift`). |
| `table_name` | yes | Target table. |
| `schema` | no | Namespace qualifying the table. |
| `write_strategy` | no | `copy` (default) or `insert`. |
| `copy.format` | no | `jsonl` (default, `FORMAT AS JSON 'auto'`) or `csv` (`FORMAT AS CSV`). |
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
the field (narrowing later is the `schema:` drift policy's job). No DISTKEY or SORTKEY is chosen — faucet has no basis to pick either, and a wrong one is baked into the table.

Set `create_table: false` to require a pre-existing target; a missing one then
fails fast with the same error every table sink raises, naming both ways out.

## Commit accumulation (`commit_rows` / `commit_bytes`)

Records **accumulate across `write_batch` calls** and commit once per
threshold, plus once at `flush` (#617). Before this the commit unit was the
page unit, and `batch_size` could only ever *split* an oversized page — it
could never merge two undersized ones, so a small source page meant one
expensive warehouse operation per small page. `COPY` wants millions of rows per load; one `COPY` per 1000-row page produced many small commits, small unsorted blocks, and VACUUM pressure.

- `commit_rows` — records per commit. `None` (the default) accumulates the
  **whole run** into one commit.
- `commit_bytes` — estimated-bytes counterpart, bounding how much is buffered.

`batch_size` still bounds an individual request inside a commit group, so a
very large group is split into reasonably-sized requests.

**Only the append path accumulates.** `delivery: exactly_once` and the DLQ
path commit per page, because a commit token must land atomically with its own
page, and a DLQ must report which rows of *this* page failed — neither is
expressible once pages are merged.
