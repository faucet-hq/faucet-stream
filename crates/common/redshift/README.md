# faucet-common-redshift

Shared connection, credentials, and connection-pool types for the
[`faucet-stream`](https://crates.io/crates/faucet-stream) Amazon Redshift
**source** (`faucet-source-redshift`) and **sink** (`faucet-sink-redshift`)
connectors.

Redshift speaks the PostgreSQL wire protocol, so both connectors connect through
`sqlx`'s Postgres driver. This crate centralises that so TLS, auth, and pooling
behave identically on both sides.

## Types

- **`RedshiftCredentials`** — adjacently-tagged `{ type, config }` enum:
  - `password` — username/password auth (the user comes from the connection
    block). **The only mechanism implemented in v1.**
  - `iam` / `redshift_data_api` — reserved for a future release; building a
    client with either currently returns a typed
    `FaucetError::Config`.
- **`RedshiftConnection`** — `host`, `port` (default `5439`), `database`,
  `user`, `credentials`, a `tls` toggle (default `true`), and the optional
  `tls_mode` (`disable` | `prefer` | `require` | `verify_ca` | `verify_full`,
  overrides `tls`) + `ssl_root_cert` (CA PEM path for the verifying modes).
  Flattened into both end configs.

## Helpers

- `build_connect_options(&RedshiftConnection)` — pure `PgConnectOptions` builder.
  `tls: true` → `sslmode=require` (encrypted, certificate **not** verified);
  `tls: false` → `sslmode=prefer`. Set `tls_mode: verify_full` with
  `ssl_root_cert` (the Redshift CA bundle) to verify the server certificate and
  host name; `ssl_root_cert` with a non-verifying mode is a config error.
- `build_pool_lazy(conn, max)` — lazily-connected pool (no I/O at construction).
- `build_pool(conn, max)` — eagerly validated pool (fails fast on bad creds).
- `resolve_password(&RedshiftCredentials)` — extracts the password.

## Example

```yaml
host: my-cluster.abc123.us-east-1.redshift.amazonaws.com
port: 5439
database: dev
user: admin
credentials:
  type: password
  config:
    password: ${env:REDSHIFT_PASSWORD}
tls_mode: verify_full
ssl_root_cert: /etc/ssl/redshift-ca-bundle.crt
```

License: MIT OR Apache-2.0
