# faucet-common-s3

[![Crates.io](https://img.shields.io/crates/v/faucet-common-s3.svg)](https://crates.io/crates/faucet-common-s3)
[![Docs.rs](https://docs.rs/faucet-common-s3/badge.svg)](https://docs.rs/faucet-common-s3)

Shared S3 pieces for the [faucet-stream](https://github.com/faucet-hq/faucet-stream)
connectors that write to S3. This crate is a building block, not a connector
itself. It provides:

- `build_client(region, endpoint_url)` — an S3 client from the default AWS
  credential chain, optionally against an S3-compatible endpoint (MinIO,
  LocalStack, R2, …).
- `S3Backend` — the `StorageBackend` the shared file writer in
  [`faucet-common-file`](https://crates.io/crates/faucet-common-file)
  publishes objects through. The
  [`faucet-sink-s3`](https://crates.io/crates/faucet-sink-s3) sink and the S3
  destination of the deprecated
  [`faucet-sink-parquet`](https://crates.io/crates/faucet-sink-parquet) sink
  are both a `FileSink<S3Backend>`, so they write every format, compression,
  encryption, rollover and overwrite mode the local file sink does with one
  implementation (RFC 0013, #886).

## How `S3Backend` writes

- A file up to 8 MiB (`PART_BYTES`) is one `PutObject`; a larger one is a
  multipart upload of 8 MiB parts, completed only after every part landed and
  aborted on any failure, so an object is either wholly published or not at
  all. Line formats (JSON Lines, raw text) go up part by part while they are
  written, so neither memory nor scratch disk grows with the object.
- S3 allows 10,000 parts per upload (`MAX_PARTS`); a streamed object that
  would need more fails with an error naming `max_bytes_per_file`.
- A move into place (`write_mode: overwrite`) is a server-side copy and a
  delete: one `CopyObject` up to 5 GiB (`MAX_COPY_BYTES`), `UploadPartCopy`
  above it. Deletes are batched 1,000 keys per `DeleteObjects`.
- Up to `concurrency` uploads (objects and parts) run at once, while the
  writer encodes the next file.
- Every request runs through the run's call context, so a hung request is
  bounded by the pipeline's call deadline, and reports to the run's
  round-trip recorder (`put`, `get`, `head`, `list`, `copy`, `delete`).
- A 429 / 5xx failure (`SlowDown`, `ServiceUnavailable`, …) is a typed
  `FaucetError::HttpStatus`, so the pipeline's resilience policy retries it.
- The preflight probe (`faucet doctor`) is a non-mutating `HeadBucket`.

```rust,ignore
use faucet_common_file::write::{FileSink, WriteConfig};
use faucet_common_s3::{S3Backend, build_client};
use std::sync::Arc;

let client = build_client(Some("us-east-1"), None).await;
let write = WriteConfig { /* the sink config's write fields */ ..Default::default() };
let settings = write.settings()?;
let (base, template) = write.object_layout(&settings)?;
let backend = S3Backend::builder(client, "my-bucket")
    .concurrency(8)
    .build(base, &template)?;
let sink = FileSink::build(settings, template, Arc::new(backend), my_identity)?;
```

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your
option.
