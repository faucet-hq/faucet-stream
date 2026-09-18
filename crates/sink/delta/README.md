# faucet-sink-delta

Apache **Delta Lake** sink for the [`faucet-stream`](https://crates.io/crates/faucet-stream)
ecosystem. Appends JSON records to a Delta table on the local filesystem or
cloud object storage (S3 / Azure / GCS) via the Rust
[`deltalake`](https://crates.io/crates/deltalake) crate (delta-rs) — the
idiomatic, high-throughput way to land data for **Databricks** (and Spark,
Trino, DuckDB, Microsoft Fabric) at the open table-format level, with no
running/billed compute and no Python.

## Highlights

- **Lazy create** — on first write the table is created from the inferred
  schema when `create_if_not_missing` (default), honouring `partition_by`.
- **Atomic commits** — each `flush()` is one Delta transaction, so a
  bookmark-carrying page commits (and becomes visible) atomically.
- **Append-only in v1** — `write_mode` is `append`. MERGE/upsert is a
  version-gated follow-up.
- **No datafusion** — appends via delta-rs's low-level `RecordBatchWriter`, so
  the dependency tree (and compile time) stays slim.

## Configuration

| Field | Type | Default | Notes |
|---|---|---|---|
| `table_uri` | string | — (required) | `file:///…`, a bare local path, `s3://…`, `abfss://…`, `gs://…` |
| `credentials` | tagged enum | `{ type: default }` | `default` / `aws` / `azure` / `gcp` |
| `storage_options` | map | `{}` | Passed verbatim to delta-rs; explicit keys win over `credentials` |
| `create_if_not_missing` | bool | `true` | Create the table + schema on first write |
| `partition_by` | string[] | `[]` | Partition columns (applied only on create) |
| `schema_sample_size` | int | `100` | Records sampled to infer the schema on create |
| `batch_size` | int | `1000` | Arrow record-batch write size; `0` = no re-chunk |
| `target_file_size` | int? | *(unset)* | Commit early once the in-memory parquet buffer reaches this many bytes, which both caps output data-file size and bounds peak memory to roughly this value. Unset means one commit per run and a buffer that grows with the whole dataset — fine for small loads, an OOM risk on a large table, since a bulk source emits no bookmarks and so triggers no intermediate flush. Expect a few commits per run when set: that is the trade the knob exists to let you make. |

Cloud backends require the matching crate feature: `s3`, `azure`, `gcs`.

The **`arrow`** feature opts this sink into the columnar fast path (#375): when
the source is also Arrow-capable (e.g. `faucet-source-parquet` /
`faucet-source-delta`), batches are written straight through delta-rs's
`RecordBatchWriter` with no `serde_json::Value` materialization.

```yaml
pipeline:
  sink:
    type: delta
    config:
      table_uri: s3://lake/events
      credentials:
        type: aws
        config:
          region: us-east-1
      partition_by: ["region"]
```

## Contract

Like the Parquet sink, the writer **must be `flush`ed before drop** — a
dropped, un-flushed writer loses the buffered, uncommitted batch. The
`faucet` pipeline flushes after every bookmark-carrying page.

License: MIT OR Apache-2.0.
