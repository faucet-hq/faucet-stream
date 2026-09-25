# faucet-common-gcs

[![Crates.io](https://img.shields.io/crates/v/faucet-common-gcs.svg)](https://crates.io/crates/faucet-common-gcs)
[![Docs.rs](https://docs.rs/faucet-common-gcs/badge.svg)](https://docs.rs/faucet-common-gcs)
[![MSRV](https://img.shields.io/crates/msrv/faucet-common-gcs.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-common-gcs.svg)](https://github.com/faucet-hq/faucet-stream#license)

Shared Google Cloud Storage credentials and client construction for the GCS source and sink connectors. Part of the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem.

This is a small **internal/shared** crate: it holds the one credential enum and the async client builders that both [`faucet-source-gcs`](https://crates.io/crates/faucet-source-gcs) and [`faucet-sink-gcs`](https://crates.io/crates/faucet-sink-gcs) need, so the two crates accept exactly the same `credentials:` shape and build their GCS clients the same way. It is built on the official [`google-cloud-storage`](https://crates.io/crates/google-cloud-storage) SDK and [`google-cloud-auth`](https://crates.io/crates/google-cloud-auth).

## What it provides

### `GcsCredentials`

A tagged enum that serializes as the project-wide `{ type, config }` auth wire shape — adjacent tagging (`#[serde(tag = "type", content = "config")]`) with `snake_case` discriminators — so YAML/JSON configs read consistently across every faucet connector. Derives `Serialize + Deserialize + JsonSchema + Clone + Default`.

| Variant | YAML | Notes |
|---|---|---|
| `application_default` *(default)* | `{ type: application_default }` | Application Default Credentials — honours `GOOGLE_APPLICATION_CREDENTIALS`, `gcloud auth application-default login` user creds, and the GCE/GKE metadata server, in that order. |
| `service_account_json_file` | `{ type: service_account_json_file, config: { path: /run/secrets/sa.json } }` | Reads a service-account JSON key file from disk. |
| `service_account_json_inline` | `{ type: service_account_json_inline, config: { json: "${env:GCP_SA_JSON}" } }` | Service-account JSON key as an inline string. Pairs with `${env:…}` / `${secret:…}` interpolation in CLI configs. |
| `anonymous` | `{ type: anonymous }` | Unauthenticated. Use **only** with emulators (e.g. `fake-gcs-server`) that don't validate bearer tokens — never in production. |

### Client builders

All three are `async` and map every failure to [`FaucetError::Auth`](https://docs.rs/faucet-core/latest/faucet_core/enum.FaucetError.html) with a message that includes the underlying SDK / I/O error.

```rust
pub async fn build_credentials(
    creds: &GcsCredentials,
) -> Result<google_cloud_auth::credentials::Credentials, FaucetError>;

pub async fn build_storage(
    creds: &GcsCredentials,
    storage_host: Option<&str>,
) -> Result<google_cloud_storage::client::Storage, FaucetError>;

pub async fn build_storage_control(
    creds: &GcsCredentials,
    storage_host: Option<&str>,
) -> Result<google_cloud_storage::client::StorageControl, FaucetError>;
```

- `build_credentials` resolves a `GcsCredentials` spec into a `google-cloud-auth` credential handle (the shared step both client builders call first).
- `build_storage` returns the **data-plane** `Storage` client used by source reads and sink writes (`read_object`, `write_object`).
- `build_storage_control` returns the **control-plane** `StorageControl` client used by source object listings (`list_objects`).
- `storage_host` is an integration-test escape hatch — pass `None` in production. Tests target `fake-gcs-server` with `Some("http://127.0.0.1:4443")`, which sets the client endpoint. For a plaintext (`http://`) host, `build_storage_control` returns a client whose `list_objects` and `get_object` go over the GCS JSON API: the SDK's control client speaks gRPC, which needs HTTP/2, and emulators serve HTTP/2 only behind TLS. Every other host uses the SDK's gRPC client unchanged.

## Who depends on this

You usually **don't** depend on this crate directly. End users configure GCS through `faucet-source-gcs` / `faucet-sink-gcs` (both re-export `GcsCredentials`), or through the `faucet` CLI via the `source-gcs` / `sink-gcs` features. Depend on `faucet-common-gcs` only if you are a **third-party connector author** building your own GCS source/sink for the faucet ecosystem and want to stay interchangeable with the first-party connectors' credential shape and client construction.

## Installation

```bash
cargo add faucet-common-gcs
```

## Usage

```rust
use faucet_common_gcs::{build_storage, build_storage_control, GcsCredentials};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let creds = GcsCredentials::ServiceAccountJsonFile {
        path: "/run/secrets/sa.json".into(),
    };

    // Data-plane client for read/write object operations.
    let storage = build_storage(&creds, None).await?;

    // Control-plane client for listing objects.
    let control = build_storage_control(&creds, None).await?;

    let _ = (storage, control);
    Ok(())
}
```

In a faucet config, the same enum is what the GCS source/sink accept under `credentials:`:

```yaml
credentials:
  type: service_account_json_inline
  config:
    json: "${env:GCP_SA_JSON}"
```

## Troubleshooting / FAQ

| Symptom | Likely cause / fix |
|---|---|
| `FaucetError::Auth: GCS auth (ADC): …` | Application Default Credentials could not be resolved. Set `GOOGLE_APPLICATION_CREDENTIALS`, run `gcloud auth application-default login`, or run on a GCE/GKE node with a metadata server. |
| `GCS auth: could not read service-account key from '…'` | The `path` for `service_account_json_file` is wrong or unreadable by the process. Check the path and file permissions. |
| `GCS auth: … is not valid JSON` | The service-account key (file or inline) isn't valid JSON. For `service_account_json_inline`, confirm the env var / secret holds the full JSON, not a path. |
| Requests fail against an emulator | The SDK tries to fetch ADC tokens by default. Use `type: anonymous` with emulators like `fake-gcs-server`, and pass the emulator endpoint via `storage_host`. |

**Why two clients?** GCS splits object I/O (`Storage`, data plane) from bucket/object metadata operations like listing (`StorageControl`, control plane). The source needs both; the sink needs only `Storage`.

## See also

- [`faucet-source-gcs`](https://crates.io/crates/faucet-source-gcs) and [`faucet-sink-gcs`](https://crates.io/crates/faucet-sink-gcs) — the connectors that consume these types.
- [faucet-stream documentation](https://faucet-hq.github.io/faucet-stream/) — connector reference and cookbook.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](../../../LICENSE-APACHE) or <https://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](../../../LICENSE-MIT) or <https://opensource.org/licenses/MIT>)

at your option.
