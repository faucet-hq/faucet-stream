# faucet-common-mssql

[![Crates.io](https://img.shields.io/crates/v/faucet-common-mssql.svg)](https://crates.io/crates/faucet-common-mssql)
[![Docs.rs](https://docs.rs/faucet-common-mssql/badge.svg)](https://docs.rs/faucet-common-mssql)
[![MSRV](https://img.shields.io/crates/msrv/faucet-common-mssql.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-common-mssql.svg)](https://github.com/faucet-hq/faucet-stream#license)

Shared connection, TLS, and pooling types for the Microsoft SQL Server **source** and **sink** connectors. Part of the [faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem.

This is an internal building block. It holds the configuration the MSSQL [source](https://crates.io/crates/faucet-source-mssql) and [sink](https://crates.io/crates/faucet-sink-mssql) must agree on — how to parse a connection target, apply TLS, and build a [`tiberius`](https://crates.io/crates/tiberius) + [`bb8`](https://crates.io/crates/bb8) connection pool — so both connectors behave identically instead of duplicating the logic. End users configure MSSQL through the source/sink crates, which re-export these types; you only depend on this crate directly if you are building your own MSSQL connector.

## What it provides

| Item | Kind | Purpose |
|------|------|---------|
| `MssqlConnectionConfig` | struct | `connection_url` **or** `connection_string` (exactly one) plus a `tls` block. `#[serde(flatten)]`'d into both end configs. |
| `MssqlTls` / `MssqlTlsMode` | struct / enum | Encryption mode + optional `ca_cert_path`. |
| `build_config` | fn | `MssqlConnectionConfig` → `tiberius::Config` (URL parse / ADO passthrough + TLS). |
| `build_pool` | async fn | Builds a `bb8` pool and eagerly validates one connection (fail-fast). |
| `with_statement_timeout` | async fn | Wraps a query future in a `tokio::time::timeout`. |
| `quote_ident_mssql` | fn | `[bracket]` identifier quoting (doubles interior `]`). |
| `PARAM_LIMIT` | const | `2100` — MSSQL's per-request bind-parameter ceiling. |
| `MssqlPool` / `MssqlPooledConnection<'a>` | type alias | The `bb8` pool and a checked-out connection (derefs to `tiberius::Client`). |

### `MssqlConnectionConfig` fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `connection_url` | `Option<String>` | `None` | `mssql://user:pass@host:1433/database` URL form. Mutually exclusive with `connection_string`. |
| `connection_string` | `Option<String>` | `None` | ADO.NET-style string handed straight to `tiberius` (`Server=tcp:host,1433;Database=db;User Id=sa;Password=...;`). Mutually exclusive with `connection_url`. |
| `tls` | `MssqlTls` | `{ type: prefer }` | TLS / encryption settings (see below). |

`validate()` enforces that exactly one of `connection_url` / `connection_string` is set. `max_connections` and `statement_timeout_secs` are deliberately **not** here — they default differently per end (source 10 / 300s, sink 5 / 300s), so each connector owns them and passes them to `build_pool` / `with_statement_timeout`.

### `MssqlTls` modes

`MssqlTls { type: <mode>, ca_cert_path: <path?> }`. `ca_cert_path` trusts a specific CA for server validation and is ignored when `mode` is `disable`.

| Mode | `tiberius` encryption | Notes |
|------|-----------------------|-------|
| `prefer` (default) | `Required` | Encrypt the connection; fails against a server that offers no TLS (same as `require` — tiberius cannot fall back to plaintext without panicking). |
| `require` | `Required` | Fail if the server does not offer TLS. |
| `trust_server_certificate` | `Required` + `trust_cert()` | Accepts self-signed certs. **Insecure against MITM — dev only.** Cannot be combined with `ca_cert_path` (a config error). |
| `disable` | `NotSupported` | No transport encryption. |

Pooled connections set TCP keepalive (first probe after 60 s idle, then every 15 s), so a server that vanishes without a reset fails the read instead of hanging a query, a row stream or a control statement.

### Authentication

SQL Server authentication (username + password) only in v1. Windows / Integrated authentication and Azure AD / Managed Identity are out of scope.

## Who should depend on this

- **Connector authors** building an alternative MSSQL source/sink who want the same connection/TLS/pool behaviour.
- The first-party `faucet-source-mssql` and `faucet-sink-mssql` crates (they re-export everything here).

If you are running pipelines, install the connectors instead — you never add this crate to a YAML config:

```bash
cargo install faucet-cli --features "source-mssql,sink-mssql"
```

## Usage

The connection block in a pipeline config flattens straight onto each end config:

```yaml
# URL form — faucet parses host/port/database/credentials; tls governs encryption.
connection_url: "mssql://sa:Str0ng%40Pass@localhost:1433/sales"
tls:
  type: prefer          # prefer | require | trust_server_certificate | disable
  ca_cert_path: null

# …or an ADO.NET-style string handed straight to tiberius:
connection_string: "Server=tcp:localhost,1433;Database=sales;User Id=sa;Password=...;"
```

In Rust, build a pool and run a quoted query:

```rust,no_run
use faucet_common_mssql::{MssqlConnectionConfig, build_pool, quote_ident_mssql, with_statement_timeout};
use std::time::Duration;
use faucet_core::FaucetError;

# async fn demo() -> Result<(), FaucetError> {
let cfg = MssqlConnectionConfig {
    connection_url: Some("mssql://sa:pw@localhost:1433/sales".into()),
    ..Default::default()
};

// Eagerly validates one connection — bad creds / unreachable host fail here.
let pool = build_pool(&cfg, 10).await?;

let table = quote_ident_mssql("orders")?; // -> "[orders]"
let mut conn = pool.get().await.map_err(|e| FaucetError::Source(e.to_string()))?;

let query = conn.simple_query(format!("SELECT TOP 100 * FROM {table}"));
let _stream = with_statement_timeout(
    Duration::from_secs(300),
    async { query.await.map_err(|e| FaucetError::Source(e.to_string())) },
    || FaucetError::Source("query timed out".into()),
)
.await?;
# Ok(())
# }
```

`pool.rs` is the only place in the workspace that constructs a `tiberius::Config` or an MSSQL pool, so TLS, auth, and pooling stay consistent across the source and sink.

## Feature flags

None. This crate has no optional features — it is pure config + pool plumbing always compiled into the MSSQL source/sink.

## Troubleshooting / FAQ

| Symptom | Cause / fix |
|---------|-------------|
| `MSSQL config requires either connection_url or connection_string` | Neither field set. Provide exactly one. |
| `MSSQL config sets both ... set exactly one` | Both fields set. Keep only one. |
| `MSSQL connection_url scheme must be mssql://` | Use the `mssql://` (or `sqlserver://`) scheme, not `postgres://` etc. |
| `MSSQL connection_url is missing a host` | The URL has no host segment — check for a stray `@` or empty authority. |
| Password / username rejected, special chars mangled | Percent-encode special characters in URL credentials: `@` → `%40`, `:` → `%3A`, `/` → `%2F`. |
| `MSSQL connection failed` at startup | `build_pool` validates one connection eagerly. Verify host/port reachability, credentials, and that the TLS mode matches the server (try `require` vs `prefer`). |
| Self-signed cert handshake fails | Use `tls.type: trust_server_certificate` (dev only) or point `ca_cert_path` at the server's CA. |
| `... binds more than 2100 parameters` style errors | That is `PARAM_LIMIT`; the sink auto-splits batches, so this should not surface — if it does, reduce `batch_size`. |

## See also

- [`faucet-source-mssql`](https://crates.io/crates/faucet-source-mssql) — the MSSQL query source.
- [`faucet-sink-mssql`](https://crates.io/crates/faucet-sink-mssql) — the MSSQL sink.
- [`faucet-core`](https://crates.io/crates/faucet-core) — traits, `FaucetError`, and the pipeline runtime.
- [faucet-stream documentation](https://faucet-hq.github.io/faucet-stream/) — connector reference and cookbook.

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](../../../LICENSE-APACHE) or <https://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](../../../LICENSE-MIT) or <https://opensource.org/licenses/MIT>)

at your option.
