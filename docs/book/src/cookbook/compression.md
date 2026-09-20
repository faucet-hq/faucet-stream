# Compression

The file-shaped connectors can read and write gzip / zstd transparently. Enable
the `compression` feature, then set a `compression:` field on the connector.

## Enable the feature

```bash
# CLI
cargo install faucet-cli --features compression

# Library (umbrella) — activates compression on whichever file connectors you've enabled
faucet-stream = { version = "1.0", features = ["sink-jsonl", "source-csv", "compression"] }
```

The `compression` aggregate feature forwards to whichever of the supported
connectors you've already opted into; it doesn't pull in connectors by itself.
`full` includes compression.

## Connectors that support it

`source-csv`, `source-s3`, `source-gcs`, `source-azure-blob`, `sink-jsonl`,
`sink-csv`, `sink-s3`, `sink-gcs`, `sink-azure-blob`.

Compression is independent of [file format](./file-formats.md): pick the format,
pick the codec.

## Config

```yaml
sink:
  type: jsonl
  config:
    path: ./out/records.jsonl.gz
    compression: auto      # none | gzip | zstd | auto (default)
```

- `auto` chooses from the filename suffix: `.gz` → gzip, `.zst` → zstd, anything
  else → none.
- Explicit `gzip` / `zstd` / `none` override the suffix.

Auto-detection runs per file at I/O time, so one matrix run can read a mix of
`.jsonl`, `.jsonl.gz`, and `.jsonl.zst` objects.

## Notes

- File sinks finalize the encoder on `flush()`; later writes reopen in append
  mode, producing a multi-member compressed file that gzip/zstd decoders read
  back transparently.
- S3 and GCS sinks do **not** set a `Content-Encoding` header — consumers must
  decompress explicitly.
- Parquet, Kafka, HTTP, stdout, and the database sinks are intentionally out of
  scope: Parquet has internal columnar compression and the others have native
  protocol-level options.

## See also

- [Throughput tuning](./tuning.md) — compression trades CPU for I/O; tune it
  alongside batch sizes.
- [Connector capability matrix](../reference/capability-matrix.md) — which
  connectors expose the `compression` field.
