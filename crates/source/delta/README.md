# faucet-source-delta

Apache **Delta Lake** source for the [`faucet-stream`](https://crates.io/crates/faucet-stream)
ecosystem. Streams the rows of a Delta table on the local filesystem or cloud
object storage (S3 / Azure / GCS) via the Rust
[`deltalake`](https://crates.io/crates/deltalake) crate (delta-rs) — reading the
same open table format **Databricks** (and Spark, Trino, DuckDB, Microsoft
Fabric) write.

## Highlights

- **Bounded streaming** — the active file set is read from the Delta log and
  each parquet data file is streamed through the async Arrow reader; memory
  stays at one `batch_size`, no whole-table buffering, no datafusion.
- **Time travel** — read a pinned `version` or as-of `timestamp`.
- **Projection pushdown** — `columns` limits the columns read.
- **Partition-aware** — partition-column values (stored in the Hive-style path,
  not the data files) are reconstructed and merged into every row, typed
  against the table schema.
- **Deletion vectors are refused** — a table whose active files carry deletion
  vectors (Delta 3.x `DELETE` / `UPDATE` / `MERGE` with
  `delta.enableDeletionVectors`) fails the read naming the first such file,
  because reading those files whole would return deleted rows. Purge them
  (`REORG TABLE … APPLY (PURGE)`) or disable the table property first.

## Record shape

Every column is present in every row (a null is `null`); a decimal keeps every digit as a string.

## Configuration

| Field | Type | Default | Notes |
|---|---|---|---|
| `table_uri` | string | — (required) | `file:///…`, a bare local path, `s3://…`, `abfss://…`, `gs://…` |
| `credentials` | tagged enum | `{ type: default }` | `default` / `aws` / `azure` / `gcp` |
| `storage_options` | map | `{}` | Passed verbatim to delta-rs; explicit keys win over `credentials` |
| `version` | int? | — | Time travel: read as of this version (mutually exclusive with `timestamp`) |
| `timestamp` | string? | — | Time travel: read as of this RFC 3339 timestamp |
| `columns` | string[] | `[]` (all) | Projection pushdown |
| `batch_size` | int | `1000` | Page-size hint; `0` = one page per file |

Cloud backends require the matching crate feature: `s3`, `azure`, `gcs`.

The **`arrow`** feature opts this source into the columnar fast path (#375): when
the sink is also Arrow-capable (e.g. `faucet-sink-parquet` /
`faucet-sink-delta`), it emits Arrow `RecordBatch` pages directly (Hive
partition columns re-appended as constant columns) with no `serde_json::Value`
materialization.

```yaml
pipeline:
  source:
    type: delta
    config:
      table_uri: s3://lake/events
      credentials: { type: aws, config: { region: us-east-1 } }
      version: 42
      columns: ["id", "ts", "region"]
```

License: MIT OR Apache-2.0.
