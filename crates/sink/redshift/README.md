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
