# faucet-common-oracle

[![Crates.io](https://img.shields.io/crates/v/faucet-common-oracle.svg)](https://crates.io/crates/faucet-common-oracle)
[![Docs.rs](https://docs.rs/faucet-common-oracle/badge.svg)](https://docs.rs/faucet-common-oracle)
[![MSRV](https://img.shields.io/crates/msrv/faucet-common-oracle.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-common-oracle.svg)](https://github.com/faucet-hq/faucet-stream#license)

Shared connection, TLS and type-mapping code for the [faucet-stream](https://github.com/faucet-hq/faucet-stream) Oracle connectors — [`faucet-source-oracle`](https://crates.io/crates/faucet-source-oracle), [`faucet-source-oracle-cdc`](https://crates.io/crates/faucet-source-oracle-cdc) and [`faucet-sink-oracle`](https://crates.io/crates/faucet-sink-oracle). You normally configure it through one of those crates; they re-export the connection types.

## Runtime requirement: Oracle Instant Client

The connectors use the [`oracle`](https://crates.io/crates/oracle) driver (ODPI-C), which loads **Oracle Instant Client** at runtime. The crates build without it; connecting without it fails with a typed error naming the fix. Install the *Basic* or *Basic Light* package and make it loadable:

- Linux: unzip it and add the directory to `LD_LIBRARY_PATH` (or run `ldconfig`).
- macOS: put the directory on `DYLD_LIBRARY_PATH`, or symlink `libclntsh.dylib` into `~/lib`.
- Windows: add the directory to `PATH`.

## Connection settings

These fields are flattened into every Oracle connector config.

| Field | Default | Description |
|---|---|---|
| `connect_string` | — | Easy Connect (`host:1521/FREEPDB1`), a TNS alias, or a full connect descriptor. Mutually exclusive with `host`. |
| `host` | — | Database host. |
| `port` | `1521` (`2484` with TLS) | Listener port. |
| `service_name` | — | Service name (e.g. a PDB). Exactly one of `service_name` / `sid` with `host`. |
| `sid` | — | Legacy SID. |
| `username` / `password` | — | Database credentials. Required unless `external_auth`. |
| `external_auth` | `false` | Use OS authentication or wallet-held credentials instead. |
| `tls.enabled` | `false` | Connect over TCPS (`host` form). |
| `tls.wallet_location` | — | Wallet directory holding the trusted CA (and client certificate for mutual TLS). |
| `tls.server_dn_match` | `true` | Check the server certificate's DN against the host. |
| `tls.server_cert_dn` | — | Expected server DN when it differs from the host. |

```yaml
source:
  type: oracle
  config:
    host: db.example.com
    service_name: ORCLPDB1
    username: app
    password: ${secret:ORACLE_PASSWORD}
    tls: { enabled: true, wallet_location: /etc/oracle/wallet }
    query: SELECT * FROM ORDERS
```

With the `host` form, faucet renders a connect descriptor, rejecting values containing `(`, `)` or `=` so a host name cannot inject descriptor clauses. For TLS with `connect_string`, use a `tcps://` Easy Connect string or a descriptor with `PROTOCOL=TCPS`.

## Type mapping

| Oracle | JSON |
|---|---|
| `NUMBER`, `INTEGER`, `FLOAT` | number when exact (fits `i64`/`u64`, or the decimal survives `f64`), otherwise the exact decimal **string** — a `NUMBER(38)` key never loses digits |
| `BINARY_FLOAT` / `BINARY_DOUBLE` | number (`Inf`/`NaN` as text) |
| `VARCHAR2`, `CHAR`, `NVARCHAR2`, `CLOB`, `NCLOB`, `LONG`, `ROWID` | string |
| `RAW`, `LONG RAW`, `BLOB` | base64 string |
| `DATE` | `2024-01-02T03:04:05` |
| `TIMESTAMP` | `2024-01-02T03:04:05.500` (fraction in 3, 6 or 9 digits) |
| `TIMESTAMP WITH [LOCAL] TIME ZONE` | `2024-01-02T03:04:05+05:30` |
| `INTERVAL DAY TO SECOND` / `YEAR TO MONTH` | ISO-8601 duration (`P1DT2H3M4.5S`, `P1Y2M`) |
| `BOOLEAN` (23ai) | boolean |
| `JSON` (21c+) | select `JSON_SERIALIZE(col RETURNING CLOB) AS col` and list it under `json_columns` |

Sessions pin `NLS_DATE_FORMAT`, `NLS_TIMESTAMP[_TZ]_FORMAT` and `NLS_NUMERIC_CHARACTERS`, so text conversions behave the same on every database.

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your option.
