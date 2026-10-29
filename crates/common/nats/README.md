# faucet-common-nats

Shared configuration types for the [`faucet-stream`](https://crates.io/crates/faucet-stream)
NATS **source** ([`faucet-source-nats`](https://crates.io/crates/faucet-source-nats))
and **sink** ([`faucet-sink-nats`](https://crates.io/crates/faucet-sink-nats)).

Both connectors depend on this crate and re-export its types, so end-user
imports do not change.

## Types

- **`NatsAuth`** — authentication mode, serialized as the standard faucet
  `{ type, config }` adjacent tag:
  - `none` (default) — anonymous
  - `token` — `{ token }`
  - `user_password` — `{ username, password }`
  - `nkey` — `{ nkey }` (Ed25519 seed)
  - `creds_file` — `{ path }` (an `nsc`-produced `.creds` bundle)

  Its `Debug` implementation is secret-safe: `token`, `password`, and `nkey`
  are never printed.

- **`NatsConnectionConfig`** — the connection surface both connectors
  `#[serde(flatten)]` into their config:

  | field     | type            | default                      | description                              |
  |-----------|-----------------|------------------------------|------------------------------------------|
  | `servers` | `Vec<String>`   | `["nats://127.0.0.1:4222"]`  | server URLs (first reachable wins)       |
  | `auth`    | `NatsAuth`      | `none`                       | authentication mode                      |
  | `tls`     | `bool` or block | `false`                      | `true` requires TLS with the platform roots; a block (below) also requires TLS |
  | `name`    | `Option<String>`| —                            | client connection name (server monitor)  |

  In Rust the block lands in `NatsConnectionConfig::tls_options`
  (`NatsTls`, an alias of `faucet_core::TlsOptions`, #973):

  | field | type | default | description |
  |-------|------|---------|-------------|
  | `ca_cert_path` | path | — | PEM CA certificate(s) that signed the server certificate; the only trust anchor when set. |
  | `ca_cert_pem` | string | — | The same CA inline; mutually exclusive with `ca_cert_path`. |
  | `client_cert_path` | path | — | PEM client certificate for mutual TLS; set with `client_key_path`. |
  | `client_key_path` | path | — | PEM private key for the client certificate. |
  | `insecure_skip_verify` | bool | `false` | Accept any server certificate. Logs a warning. |

  Without a CA the platform roots are used. The hostname is always checked
  unless `insecure_skip_verify` is on.

- **`connect(&NatsConnectionConfig) -> Result<async_nats::Client, FaucetError>`**
  — the single client builder both connectors use. Retry-on-initial-connect is
  left off, so an unreachable server fails immediately with a typed error.

## Example (embedded in a connector config)

```yaml
servers: ["nats://127.0.0.1:4222"]
auth:
  type: token
  config:
    token: ${env:NATS_TOKEN}
tls: false
name: faucet
```

With a private CA and mutual TLS:

```yaml
servers: ["tls://nats.internal:4222"]
tls:
  ca_cert_path: /etc/faucet/nats-ca.pem
  client_cert_path: /etc/faucet/client.pem
  client_key_path: /etc/faucet/client.key
```

## License

Licensed under either of Apache-2.0 or MIT at your option.
