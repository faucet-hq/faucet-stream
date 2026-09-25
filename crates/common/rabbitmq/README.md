# faucet-common-rabbitmq

Shared configuration types for the [`faucet-stream`](https://crates.io/crates/faucet-stream)
RabbitMQ (AMQP 0.9.1) **source** ([`faucet-source-rabbitmq`](https://crates.io/crates/faucet-source-rabbitmq))
and **sink** ([`faucet-sink-rabbitmq`](https://crates.io/crates/faucet-sink-rabbitmq)),
built on the pure-Rust [`lapin`](https://crates.io/crates/lapin) client (no C
bindings).

Both connectors depend on this crate and re-export its types, so end-user
imports do not change.

## Connection (`RabbitMqConnectionConfig`)

Flattened into both connectors' configs. Give **either** a full `url` **or** the
discrete `host` / `port` / `vhost` fields — not both.

| field                  | type                | default                  | description |
|------------------------|---------------------|--------------------------|-------------|
| `url`                  | `Option<String>`    | —                        | `amqp://user:pass@host:5672/%2f` (vhost URL-encoded; `amqps://` selects TLS) |
| `host`                 | `Option<String>`    | `127.0.0.1`              | broker host |
| `port`                 | `Option<u16>`       | `5672` (`5671` with TLS) | broker port |
| `vhost`                | `Option<String>`    | `/`                      | virtual host |
| `auth`                 | `RabbitMqAuth`      | `none`                   | see below |
| `tls`                  | `RabbitMqTls`       | disabled                 | see below |
| `connection_name`      | `Option<String>`    | —                        | name shown in the management UI |
| `heartbeat_secs`       | `Option<u16>`       | broker's proposal        | heartbeat interval (`0` disables) |
| `connect_timeout_secs` | `u64`               | `30`                     | bound on TCP + AMQP handshake (incl. retries) |
| `connect_retries`      | `u32`               | `0`                      | extra TCP connect attempts with exponential backoff |

Automatic topology recovery is deliberately not enabled: a recovered channel
restarts delivery tags, so acknowledging a pre-recovery tag would be wrong. A
connection lost mid-run fails the run loudly and the broker requeues every
unacknowledged delivery.

## Auth (`RabbitMqAuth`)

The standard faucet `{ type, config }` adjacent tag:

- `none` (default) — the userinfo in `url`, or the AMQP default `guest`/`guest`
  (which RabbitMQ only accepts from loopback).
- `plain` — `{ username, password }` (SASL PLAIN). Overrides `url` userinfo.
- `external` — SASL EXTERNAL: the broker authenticates the TLS client
  certificate (requires `tls.client_cert_path` / `client_key_path`).

`Debug` never prints the password or `url` credentials.

## TLS (`RabbitMqTls`) — `tls` feature

| field              | type              | description |
|--------------------|-------------------|-------------|
| `enabled`          | `bool`            | connect over TLS (also implied by an `amqps://` url) |
| `ca_cert_path`     | `Option<PathBuf>` | extra PEM CA certificates to trust (private CA) |
| `client_cert_path` | `Option<PathBuf>` | PEM client certificate (mutual TLS) |
| `client_key_path`  | `Option<PathBuf>` | PEM PKCS#8 private key for the client certificate |

Server certificates are always verified against the platform's native root
store plus `ca_cert_path` — there is no insecure mode. TLS needs the `tls`
Cargo feature (rustls + ring; CLI / umbrella: `rabbitmq-tls`); requesting TLS
without it is a config-load error.

## Value formats (`RabbitMqValueFormat`)

`json` (default), `string` (UTF-8 text ↔ JSON string), `bytes` (opaque body ↔
base64 JSON string). `decode_payload` / `encode_payload` never panic on
non-UTF-8 input.

## Other types

- `RabbitMqExchangeKind` — `direct` / `fanout` / `topic` / `headers`, with
  `declare_exchange` (durable, idempotent) and `check_exchange` (passive).
- `field_table_to_json` — renders AMQP headers as JSON.
- `amqp_error` — maps lapin errors to `FaucetError`, turning broker
  `PRECONDITION_FAILED` (redeclaring with different properties) into a config
  error with a hint.

## License

Licensed under either of Apache-2.0 or MIT at your option.
