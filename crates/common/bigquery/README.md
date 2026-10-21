# faucet-common-bigquery

[![Crates.io](https://img.shields.io/crates/v/faucet-common-bigquery.svg)](https://crates.io/crates/faucet-common-bigquery)
[![Docs.rs](https://docs.rs/faucet-common-bigquery/badge.svg)](https://docs.rs/faucet-common-bigquery)
[![MSRV](https://img.shields.io/crates/msrv/faucet-common-bigquery.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-common-bigquery.svg)](https://github.com/faucet-hq/faucet-stream#license)

Shared Google BigQuery credentials and client construction for the BigQuery source and sink connectors. Part of the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem.

This is a small **internal/shared** crate: it holds the one credential enum and the one async client builder that both [`faucet-source-bigquery`](https://crates.io/crates/faucet-source-bigquery) and [`faucet-sink-bigquery`](https://crates.io/crates/faucet-sink-bigquery) need, so the two crates accept exactly the same auth shape and build their `gcp_bigquery_client::Client` the same way.

## What it provides

| Item | Kind | Description |
|---|---|---|
| `BigQueryCredentials` | `enum` | How to authenticate with BigQuery. Derives `Serialize`, `Deserialize`, `JsonSchema` so it round-trips through YAML/JSON configs and CLI schema introspection. Its `Debug` impl masks inline service-account JSON as `***` while leaving the key path visible. |
| `build_client(&BigQueryCredentials)` | `async fn` | Turns a `BigQueryCredentials` into a ready-to-use [`gcp_bigquery_client::Client`]. Returns `FaucetError::Auth` on authentication failure or unparseable inline JSON. |
| `build_client_with_tokens(&BigQueryCredentials)` | `async fn` | Like `build_client`, also returning the client's `TokenProvider` so requests the typed client cannot make reuse its cached token. |
| `token_provider(&BigQueryCredentials)` | `async fn` | A `TokenProvider` (`Arc<dyn gcp_bigquery_client::auth::Authenticator>`) built once; it caches the access token and refreshes it before expiry. |
| `access_token(&BigQueryCredentials)` | `async fn` | An access token for a request the typed client cannot make. The token provider is built once per set of credentials (and per runtime) and reused for an hour, so repeated calls share one cached token; building and minting are bounded by `TOKEN_MINT_TIMEOUT` (60 s). The `raw::tolerant_*` fallbacks use it and share one HTTP client. |
| `raw::QueryResultsReader` | `struct` | `jobs.getQueryResults` with `formatOptions.useInt64Timestamp=true` on every page (integer-microsecond TIMESTAMP cells), one HTTP client, a cached token. |

### `BigQueryCredentials` variants

Credentials serialize as `{ type: <method>, config: { … } }` (adjacent tagging, `snake_case` discriminators) — the consistent auth wire shape shared by every faucet connector.

| `type` | `config` fields | Description |
|---|---|---|
| `service_account_key_path` | `path: String` | Path to a service-account JSON key file on disk. |
| `service_account_key` | `json: String` | Inline service-account JSON key content (e.g. injected from a secrets manager). |
| `application_default` | *(none)* | Use Application Default Credentials — workload identity, `gcloud auth application-default login`, or the `GOOGLE_APPLICATION_CREDENTIALS` env var. |

Example YAML, as it appears inside a source/sink `credentials:` block:

```yaml
# Key file on disk
credentials:
  type: service_account_key_path
  config:
    path: /etc/faucet/bq-key.json

# Inline JSON (often via a secret reference)
credentials:
  type: service_account_key
  config:
    json: ${secret:BQ_SA_JSON}

# Application Default Credentials — no config
credentials:
  type: application_default
```

## Who should depend on this

- **End users / pipeline authors** — you almost certainly **do not** depend on this crate directly. Install [`faucet-source-bigquery`](https://crates.io/crates/faucet-source-bigquery) or [`faucet-sink-bigquery`](https://crates.io/crates/faucet-sink-bigquery) (or the `faucet` CLI with the `source-bigquery` / `sink-bigquery` features); both re-export `BigQueryCredentials`, so your imports don't change.
- **Connector authors** — depend on this crate if you are building a third-party `faucet-source-*` / `faucet-sink-*` crate that talks to BigQuery and want to accept the same credentials shape and reuse the same client builder.

## Installation

```bash
cargo add faucet-common-bigquery
```

The only required faucet dependency is `faucet-core` (pulled in transitively); see the [Third-Party Connector Friendliness](https://github.com/faucet-hq/faucet-stream#readme) guidance.

## Usage

```rust
use faucet_common_bigquery::{build_client, BigQueryCredentials};

#[tokio::main]
async fn main() -> Result<(), faucet_core::FaucetError> {
    // Built from your connector's deserialized config.
    let creds = BigQueryCredentials::ServiceAccountKeyPath {
        path: "/etc/faucet/bq-key.json".to_string(),
    };

    let client = build_client(&creds).await?;
    // `client` is a `gcp_bigquery_client::Client` — drive jobs / streaming inserts with it.
    let _ = client;
    Ok(())
}
```

Embed `BigQueryCredentials` in your own connector config so it deserializes from the standard wire shape:

```rust
use faucet_common_bigquery::BigQueryCredentials;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct MyBigQueryConfig {
    pub project_id: String,
    pub credentials: BigQueryCredentials,
}
```

## Feature flags

None. The crate has no optional features; all functionality is always compiled.

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `FaucetError::Auth("invalid service account JSON: …")` | The `service_account_key` `json` string is not valid service-account JSON. Verify the value (and that any secret reference resolved to the full key body, not a path). |
| `FaucetError::Auth("BigQuery auth failed: …")` | The key file path is wrong/unreadable, the service account lacks BigQuery permissions, or ADC could not be resolved. Check the `path`, the SA's IAM roles, and run `gcloud auth application-default login` for the `application_default` variant. |
| Inline key shows as `ServiceAccountKey(***)` in logs | Intended — the `Debug` impl masks inline JSON so it never leaks into tracing output. Key paths are not masked. |
| `application_default` can't find credentials | Set `GOOGLE_APPLICATION_CREDENTIALS`, run `gcloud auth application-default login`, or run on a GCP runtime with an attached service account / workload identity. |

## See also

- [faucet-source-bigquery](https://crates.io/crates/faucet-source-bigquery) — BigQuery query source.
- [faucet-sink-bigquery](https://crates.io/crates/faucet-sink-bigquery) — BigQuery streaming-insert / `MERGE` upsert sink.
- [faucet-stream documentation](https://faucet-hq.github.io/faucet-stream/) — full project docs.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.
