# faucet-common-snowflake

[![Crates.io](https://img.shields.io/crates/v/faucet-common-snowflake.svg)](https://crates.io/crates/faucet-common-snowflake)
[![Docs.rs](https://docs.rs/faucet-common-snowflake/badge.svg)](https://docs.rs/faucet-common-snowflake)
[![MSRV](https://img.shields.io/crates/msrv/faucet-common-snowflake.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-common-snowflake.svg)](https://github.com/faucet-hq/faucet-stream#license)

Shared Snowflake authentication types and SQL REST API header helpers. Part of the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem.

This is an internal building block: it holds the one piece of configuration the Snowflake **source** and **sink** must agree on — how a request authenticates against the Snowflake SQL REST API — so both connectors mint identical `Authorization` / `X-Snowflake-Authorization-Token-Type` headers from one place instead of duplicating the JWT logic.

## What it provides

### `SnowflakeAuth`

The authentication method for a Snowflake connector. It serializes as the project-wide adjacently-tagged shape — `{ type: <method>, config: { … } }`, snake_case discriminators — and derives `Serialize`, `Deserialize`, and `JsonSchema` so it round-trips through YAML/JSON configs and CLI schema introspection. Its `Debug` impl masks every credential as `"***"`.

| `type` | `config` fields | Notes |
|---|---|---|
| `key_pair` | `user: String`, `private_key_pem: String` | RSA key-pair JWT auth. A fresh RS256 JWT is minted locally per request (1-hour expiry); the issuer carries the SHA-256 fingerprint of the derived public key, which Snowflake requires. Accepts PKCS#8 (`BEGIN PRIVATE KEY`) or PKCS#1 (`BEGIN RSA PRIVATE KEY`) PEM. Stateless — cannot be sourced from a shared `auth: { ref }` provider. |
| `oauth` | `token: String` | OAuth2 bearer token from an external IdP. Can be supplied via a shared `auth: { ref }` provider. |

### Helper functions

| Function | Returns | Purpose |
|---|---|---|
| `authorization_header(auth, account) -> Result<String, FaucetError>` | `Bearer {jwt}` for `KeyPair`; `Bearer {token}` for `OAuth` | Builds the `Authorization` header value for a SQL REST API request. `account` (e.g. `"xy12345.us-east-1"`) names the host; its locator (region and `.global` suffix removed, see `jwt_account`) is uppercased into the JWT `iss`/`sub` claims. |
| `snowflake_token_type(auth) -> &'static str` | `KEYPAIR_JWT` or `OAUTH` | The matching `X-Snowflake-Authorization-Token-Type` header value. |
| `jwt_account(account) -> String` | e.g. `XY12345` | The account part of the key-pair JWT claims: the locator without its region / `.global` suffix, upper-cased. |
| `credential_to_auth(cred) -> Result<SnowflakeAuth, FaucetError>` | `SnowflakeAuth::OAuth { token }` | Maps a `faucet_core::Credential` from a shared `AuthProvider` onto `SnowflakeAuth`. `Bearer`/`Token` → OAuth; `Basic`/`Header` → `FaucetError::Auth` (key-pair JWT can't come from a provider). |

## Who should depend on this

Most users **don't** — it is re-exported by the connectors:

- [`faucet-source-snowflake`](https://crates.io/crates/faucet-source-snowflake)
- [`faucet-sink-snowflake`](https://crates.io/crates/faucet-sink-snowflake)

Depend on it directly only if you are building a third-party Snowflake connector and want to accept the same `auth` config shape and produce correct request headers.

## Installation

```bash
cargo add faucet-common-snowflake
```

## Usage

In a config (the wire shape `SnowflakeAuth` deserializes from):

```yaml
# Key-pair JWT
auth:
  type: key_pair
  config:
    user: SVC_FAUCET
    private_key_pem: ${file:./rsa_key.p8}

# OAuth bearer
auth:
  type: oauth
  config:
    token: ${env:SNOWFLAKE_OAUTH_TOKEN}
```

In Rust, build the two headers a SQL REST API call needs:

```rust
use faucet_common_snowflake::{SnowflakeAuth, authorization_header, snowflake_token_type};

let auth = SnowflakeAuth::OAuth { token: "ey...".into() };
let account = "xy12345.us-east-1";

let authorization = authorization_header(&auth, account)?;      // Bearer ey...
let token_type = snowflake_token_type(&auth);                   // "OAUTH"

let resp = client
    .post(format!("https://{account}.snowflakecomputing.com/api/v2/statements"))
    .header("Authorization", authorization)
    .header("X-Snowflake-Authorization-Token-Type", token_type)
    .json(&body)
    .send()
    .await?;
# Ok::<(), faucet_core::FaucetError>(())
```

## Troubleshooting / FAQ

| Symptom | Likely cause / fix |
|---|---|
| `401` from Snowflake with a key-pair JWT | The registered public key must match the private key in `private_key_pem`. The `iss` claim is `{ACCOUNT}.{USER}.SHA256:<fingerprint>`; verify the fingerprint matches `openssl rsa -pubout … \| openssl dgst -sha256 -binary \| base64`. |
| `FaucetError::Auth("invalid RSA private key: …")` | `private_key_pem` isn't valid PKCS#8 or PKCS#1 PEM. Generate an RSA key in PEM form (encrypted keys are not supported — decrypt first). |
| `FaucetError::Auth("Snowflake auth provider must yield a bearer/token credential")` | A shared `auth: { ref }` provider returned a `Basic`/`Header` credential. Only OAuth bearer/token credentials work; use inline `key_pair` for JWT auth. |
| A private key showed up in logs | It shouldn't — `SnowflakeAuth`'s `Debug` masks all credentials as `"***"`. File an issue if you see one leak. |

## See also

- [Snowflake source crate](https://crates.io/crates/faucet-source-snowflake) · [Snowflake sink crate](https://crates.io/crates/faucet-sink-snowflake)
- [Authentication cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/auth.html)
- [Configuration reference](https://faucet-hq.github.io/faucet-stream/reference/config.html)

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](../../../LICENSE-APACHE) or <https://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](../../../LICENSE-MIT) or <https://opensource.org/licenses/MIT>)

at your option.
