# faucet-common-elasticsearch

[![Crates.io](https://img.shields.io/crates/v/faucet-common-elasticsearch.svg)](https://crates.io/crates/faucet-common-elasticsearch)
[![Docs.rs](https://docs.rs/faucet-common-elasticsearch/badge.svg)](https://docs.rs/faucet-common-elasticsearch)
[![MSRV](https://img.shields.io/badge/dynamic/toml?url=https%3A%2F%2Fraw.githubusercontent.com%2Ffaucet-hq%2Ffaucet-stream%2Fmain%2FCargo.toml&query=%24.workspace.package%5B%27rust-version%27%5D&label=MSRV&color=blue)](../../../Cargo.toml)
[![License](https://img.shields.io/crates/l/faucet-common-elasticsearch.svg)](https://github.com/faucet-hq/faucet-stream#license)

Shared configuration types for the Elasticsearch source and sink connectors. Part of the
[faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem.

This crate exists so that `faucet-source-elasticsearch` and `faucet-sink-elasticsearch` describe
authentication exactly the same way — one `ElasticsearchAuth` enum, one wire shape, one
secret-masking `Debug` impl — instead of each duplicating its own copy. If you only run pipelines,
you never touch this crate directly; it's pulled in (and re-exported) by the connectors.

## What it provides

- **`ElasticsearchAuth`** — the authentication mode for an Elasticsearch endpoint. Derives
  `Serialize`, `Deserialize`, and `JsonSchema` so it round-trips through YAML/JSON configs and
  shows up in `faucet schema`. Its `Debug` impl masks credentials as `"***"`, so accidentally
  logging a config value never leaks a password, token, or key.
- **`credential_to_auth`** — maps a [`Credential`](https://docs.rs/faucet-core) yielded by a shared
  `faucet_core::AuthProvider` onto an `ElasticsearchAuth` variant (used when a connector references a
  provider from the top-level `auth:` catalog).

- **`ElasticsearchTls`** — the `tls:` block (an alias of `faucet_core::TlsOptions`):
  `ca_cert_path` / `ca_cert_pem` (the only trust anchor when set),
  `client_cert_path` + `client_key_path` (mutual TLS) and
  `insecure_skip_verify` (default `false`, logs a warning).
- **`build_client(connector, connect_timeout, request_timeout, &ElasticsearchTls)`** —
  the HTTP client both connectors use, built through
  `faucet_core::http::ClientBuilder::tls`. A bad certificate file is a
  `FaucetError::Config` prefixed with `connector`.

### `ElasticsearchAuth` variants

Serializes as `{ type: <method>, config: { … } }` (adjacent tagging, snake_case discriminators) —
the consistent auth wire shape shared by every faucet connector.

| `type`     | `config` fields        | Description                                                            |
|------------|------------------------|------------------------------------------------------------------------|
| `none`     | *(none)*               | No authentication.                                                     |
| `basic`    | `username`, `password` | HTTP Basic authentication.                                             |
| `bearer`   | `token`                | Bearer token in the `Authorization` header.                            |
| `api_key`  | `key`                  | API key sent as `ApiKey <key>` in the `Authorization` header.          |

Example config snippets (as they appear inside a connector's `auth:` block):

```yaml
auth: { type: none }
auth: { type: basic, config: { username: elastic, password: changeme } }
auth: { type: bearer, config: { token: my-token } }
auth: { type: api_key, config: { key: base64-encoded-id-and-key } }
```

## Who should depend on this

- **End users:** you don't. Use the connectors directly —
  [`faucet-source-elasticsearch`](https://crates.io/crates/faucet-source-elasticsearch) and
  [`faucet-sink-elasticsearch`](https://crates.io/crates/faucet-sink-elasticsearch) re-export
  `ElasticsearchAuth`, so the type is already in scope from those crates.
- **Third-party connector authors:** depend on this crate if you're building another Elasticsearch
  connector and want to accept the identical auth shape in your config (so users get one consistent
  experience across the ecosystem).

## Installation

```bash
cargo add faucet-common-elasticsearch
```

## Usage

```rust
use faucet_common_elasticsearch::{ElasticsearchAuth, credential_to_auth};
use faucet_core::Credential;

// Embed the auth enum in your connector's config struct.
#[derive(serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
struct MyConfig {
    url: String,
    auth: ElasticsearchAuth,
}

// Parse the `{ type, config }` wire shape straight from JSON/YAML.
let cfg: MyConfig = serde_json::from_str(
    r#"{ "url": "https://es:9200", "auth": { "type": "bearer", "config": { "token": "t" } } }"#,
)?;

// Map a shared AuthProvider credential onto an ElasticsearchAuth variant.
let auth = credential_to_auth(Credential::Bearer("token".into()))?;
assert!(matches!(auth, ElasticsearchAuth::Bearer { .. }));
# Ok::<(), Box<dyn std::error::Error>>(())
```

## Troubleshooting / FAQ

| Question / symptom | Answer |
|---|---|
| `credential_to_auth` returns `FaucetError::Auth`. | The shared provider yielded a `Header` or `Token` credential, which has no Elasticsearch equivalent. Elasticsearch accepts only `Bearer` and `Basic` credentials from a shared `auth:` provider — use a `bearer`/`basic`-yielding provider, or set the connector's `auth:` inline instead. |
| My password / token showed up in a log line. | It shouldn't — `ElasticsearchAuth`'s `Debug` impl masks `password`, `token`, and `key` as `"***"`. If you see a raw secret, it came from somewhere other than this type's `Debug` output. |
| `unknown variant` / `missing field` when parsing config. | The auth block must use the `{ type, config }` shape (e.g. `{ type: basic, config: { username, password } }`), not flat fields. `type: none` takes no `config`. |

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-Apache-2.0](../../../LICENSE-Apache-2.0) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](../../../LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.
