# faucet-common-delta

Shared configuration and helpers for the [`faucet-stream`](https://crates.io/crates/faucet-stream)
Delta Lake connectors — [`faucet-source-delta`](https://crates.io/crates/faucet-source-delta)
and [`faucet-sink-delta`](https://crates.io/crates/faucet-sink-delta). Built on
the Rust [`deltalake`](https://crates.io/crates/deltalake) crate (delta-rs).

Provides:

- **`DeltaCredentials`** — object-store credentials (`default` chain / `aws` /
  `azure` / `gcp`), each mapping to the `storage_options` keys delta-rs expects.
- **`DeltaConnection`** — the shared `table_uri` / `credentials` /
  `storage_options` block (flattened into both connector configs) plus the
  open-table, time-travel, storage-option, and handler-registration helpers.
- **`convert`** — Arrow ⇆ JSON conversion (`record_batch_to_json`,
  `infer_arrow_schema`, and `infer_delta_schema` for the Arrow version
  `deltalake` writes with) reused by the source (read) and sink (write).
- **`arrow_bridge`** — `deltalake` 1.x is built on a newer Arrow major than the
  rest of faucet-stream. `schema_from_delta`, `schema_to_delta` and
  `batch_to_delta` carry schemas and batches across through the Arrow IPC
  format, which preserves nested types, dictionaries, timezones, metadata and
  nulls. A batch crossing costs one buffer copy.

Cloud object-store backends are opt-in cargo features (`s3`, `azure`, `gcs`)
that forward to the matching delta-rs feature; the default build supports the
local filesystem only.

Connector authors normally depend on `faucet-source-delta` /
`faucet-sink-delta`, which re-export the types they need from here.

License: MIT OR Apache-2.0.
