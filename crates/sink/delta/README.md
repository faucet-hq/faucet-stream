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
| `schema_sample_size` | int | `100` | Records sampled to infer the schema on create (a field null in every sampled record becomes a nullable string column); an existing table's own schema is always used |
| `batch_size` | int | `1000` | Arrow record-batch write size; `0` = no re-chunk |
| `target_file_size` | int? | *(unset)* | Commit early once the in-memory parquet buffer reaches this many bytes, which both caps output data-file size and bounds peak memory to roughly this value. Unset means one commit per run and a buffer that grows with the whole dataset — fine for small loads, an OOM risk on a large table, since a bulk source emits no bookmarks and so triggers no intermediate flush. Expect a few commits per run when set: that is the trade the knob exists to let you make. |

Cloud backends require the matching crate feature: `s3`, `azure`, `gcs`.

The **`arrow`** feature opts this sink into the columnar fast path (#375): when
the source is also Arrow-capable (e.g. `faucet-source-parquet` /
`faucet-source-delta`), batches are written straight through delta-rs's
`RecordBatchWriter` with no `serde_json::Value` materialization. delta-rs uses a
newer Arrow major than the rest of faucet-stream, so each incoming batch is
handed across through the Arrow C Data Interface on its way in, sharing its buffers rather than copying them.

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

## Batch atomicity

What a failed write leaves behind (#737): **atomic** — every `batch_size` chunk of a page is decoded before any is buffered, so a record that does not fit fails the page with none of it written — unless `target_file_size` is set and `batch_size` splits a page (then **best-effort**) — a page is one Delta commit unless target_file_size commits part of a chunked page early. A write the buffered writer itself rejects drops everything buffered since the last commit and fails every later write and the flush of the run, so a fragment of the failed page is never committed; the dropped rows are replayed on resume. `on_batch_error: dlq_all`
is refused on a best-effort configuration unless the `dlq:` block sets
`allow_duplicates_on_dlq_all: true` (a DLQ replay would write the rows that
already landed a second time). See
[batch atomicity](https://faucet-hq.github.io/faucet-stream/cookbook/dlq.html#batch-atomicity-and-dlq_all).

## Integer columns

A fractional number bound for an integer column (`10.5` into a `long`) fails the write naming the column instead of being truncated.

## Capabilities

Declares `append` only and `schema_report: drops_unknown_fields` (fields the table lacks are warned about and dropped, so `on_drift: fail` is refused), plus columnar (Arrow) input when built with the `arrow` feature. `faucet list --json` prints these declared capabilities.

## Timeouts, cancellation and retries

The table open, every buffered write and the commit run through the
pipeline's `CallContext` (#880): each is made once, any caller deadline ends it,
and each is metered as a round trip (`open`, `write`, `commit`). A failed or
cut-short commit poisons the sink for the rest of the run — the buffered rows
are gone, so the run fails and the page is replayed rather than reported
written.

## How it is built

The sink runs on the family-neutral sink driver (RFC 0014): `DeltaDriver`
implements `faucet_core::driver::SinkDriver` (plus `ColumnarDriver`, so Arrow
batches go straight to the `RecordBatchWriter`) — open or create the table,
buffer a page, commit on flush or at `target_file_size` — and `DeltaSink`
wraps it in a `DriverSink`, which derives the capabilities (append only) and
serves the plain, reported and per-row write paths with row accounting. Every
table open, write and commit is a call through the run's call context (op
labels `open`, `write`, `commit`). The public type and config are unchanged.
