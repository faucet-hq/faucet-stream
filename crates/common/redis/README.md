# faucet-common-redis

Shared Redis client building for the [`faucet-stream`](https://crates.io/crates/faucet-stream)
Redis **source** ([`faucet-source-redis`](https://crates.io/crates/faucet-source-redis)),
**sink** ([`faucet-sink-redis`](https://crates.io/crates/faucet-sink-redis)) and
**state store** ([`faucet-state-redis`](https://crates.io/crates/faucet-state-redis)).

## Types

- **`RedisTls`** — the `tls:` block (an alias of `faucet_core::TlsOptions`).
  It applies to `rediss://` URLs only; setting it on a `redis://` URL is a
  config error.

  | field                  | type   | default | description |
  |------------------------|--------|---------|-------------|
  | `ca_cert_path`         | path   | —       | PEM CA certificate(s) that signed the server certificate. Replaces the bundled web PKI roots. |
  | `ca_cert_pem`          | string | —       | The same CA inline (e.g. `${secret:…}`). Mutually exclusive with `ca_cert_path`. |
  | `client_cert_path`     | path   | —       | PEM client certificate for mutual TLS. Set with `client_key_path`. |
  | `client_key_path`      | path   | —       | PEM private key for the client certificate. |
  | `insecure_skip_verify` | bool   | `false` | Accept any server certificate. Logs a warning; never use in production. |

  Without a CA, the server certificate is verified against the bundled
  Mozilla (web PKI) roots. The URL fragment `#insecure` also disables
  verification and logs a warning.

## Functions

- **`build_client(url, &RedisTls) -> Result<redis::Client, FaucetError>`** —
  parses the URL, applies the TLS settings and returns a client (no I/O).
  An invalid URL, `tls` on a non-TLS URL, or an unreadable / malformed
  certificate file is a `FaucetError::Config`.

```yaml
state:
  type: redis
  config:
    url: rediss://redis.internal:6380/0
    tls:
      ca_cert_path: /etc/faucet/redis-ca.pem
      client_cert_path: /etc/faucet/client.pem
      client_key_path: /etc/faucet/client.key
```
