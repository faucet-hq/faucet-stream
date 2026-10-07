# faucet-common-kafka

[![Crates.io](https://img.shields.io/crates/v/faucet-common-kafka.svg)](https://crates.io/crates/faucet-common-kafka)
[![Docs.rs](https://docs.rs/faucet-common-kafka/badge.svg)](https://docs.rs/faucet-common-kafka)
[![MSRV](https://img.shields.io/crates/msrv/faucet-common-kafka.svg)](https://github.com/faucet-hq/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-common-kafka.svg)](https://github.com/faucet-hq/faucet-stream#license)

Shared configuration types for the Kafka source and sink connectors. Part of the
[faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem.

This crate holds the wire-format config that `faucet-source-kafka` and `faucet-sink-kafka`
have in common — authentication, value formats, compression, and the Confluent Schema
Registry client — so the two connectors stay byte-for-byte interchangeable. All types derive
`Serialize`, `Deserialize`, and `JsonSchema`, so they round-trip through YAML/JSON configs
and `faucet schema` introspection.

---

## Who should depend on this

- **End users:** you almost never depend on this crate directly. `faucet-source-kafka` and
  `faucet-sink-kafka` re-export every type below, so importing from those connectors is enough.
- **Third-party connector authors:** if you build your own Kafka source/sink for the faucet
  ecosystem, depend on `faucet-common-kafka` and reuse these types so your connector accepts
  the same `auth:` / `value_format:` config as the first-party ones.

---

## Types it provides

| Type | Module | Purpose |
|------|--------|---------|
| `KafkaAuth` | `auth` | Broker authentication mode (adjacently-tagged enum) |
| `ScramMechanism` | `auth` | `sha256` / `sha512` selector for SASL/SCRAM |
| `BasicAuth` | `auth` | Optional username/password for the Schema Registry HTTP client |
| `KafkaValueFormat` | `format` | Message value serialization format (adjacently-tagged enum) |
| `OnDecodeError` | `format` | Source policy when a message fails to decode (`fail` / `skip`) |
| `OnKeyError` | `format` | Sink policy when key/partition extraction fails (`fail` / `skip` / `round_robin`) |
| `CompressionType` | `format` | Producer-side compression (`none` / `gzip` / `snappy` / `lz4` / `zstd`) |
| `SchemaRegistryConfig` | `schema_registry` | Confluent Schema Registry client settings (feature `schema-registry`) |

The `schema_registry` module also exposes `SchemaRegistryClient` (an `Arc`-cloneable HTTP
client with an LRU schema cache) and per-format codecs (`avro`, `protobuf`, `json_schema`,
`envelope`) — all behind the `schema-registry` feature.

### `KafkaAuth` modes

Configured as a `{ type, config }`-style **adjacently-tagged** enum — the `type`
discriminator (`snake_case`) selects the variant and its fields sit alongside it, matching the
project-wide auth convention (not a flat shape).

| `type` | Fields | Effect |
|--------|--------|--------|
| `none` (default) | — | `security.protocol = PLAINTEXT` |
| `sasl_plain` | `username`, `password` | `SASL_PLAINTEXT` + `sasl.mechanism = PLAIN`; both fields must be non-empty |
| `sasl_scram` | `mechanism` (`sha256`/`sha512`), `username`, `password` | `SASL_PLAINTEXT` + `SCRAM-SHA-256`/`SCRAM-SHA-512` |
| `ssl` | `ca_path`, `cert_path`, `key_path`, `key_password?` | `SSL`; all three paths validated to exist at config time |
| `tls` | `ca_path?` | `SSL` with server authentication only (no client certificate); the system trust store unless `ca_path` is set |
| `sasl_ssl` | `sasl` (a `sasl_plain`/`sasl_scram`), `ssl` (a `tls`, `ssl` or `none`) | applies `ssl` then `sasl`, then forces `security.protocol = SASL_SSL` |

`sasl_plain` and `sasl_scram` on their own send credentials over a plaintext
connection. For a TLS listener without client certificates — the usual shape of
a managed cluster — wrap them in `sasl_ssl` with a `tls` layer:

```yaml
auth:
  type: sasl_ssl
  config:
    sasl:
      type: sasl_scram
      config:
        mechanism: sha512
        username: my-user
        password: my-secret
    ssl:
      type: tls
      config: {}          # or { ca_path: /etc/kafka/ca.pem }
```

### `KafkaValueFormat` formats

Same adjacently-tagged shape. The first three are always available; the Confluent variants
require the `schema-registry` feature.

| `type` | Feature | Behaviour |
|--------|---------|-----------|
| `json` (default) | — | Parse bytes as a JSON document |
| `raw_string` | — | UTF-8 string → `value` field |
| `bytes` | — | Raw bytes passed through as a base64 string |
| `confluent_avro` | `schema-registry` | Confluent wire envelope; writer schema fetched by id and cached; logical types map like the Avro file format (`decimal` → exact string, `bytes`/`fixed` → hex, temporals → ISO 8601) |
| `confluent_protobuf` | `schema-registry` | Confluent wire envelope + single-message `.proto` compiled in-process; every field is emitted (proto3 defaults included) under its `.proto` name |
| `confluent_json_schema` | `schema-registry` | Confluent wire envelope; optional `validate: true` checks decoded JSON against the schema |

```yaml
value_format:
  type: confluent_avro
  schema_registry:
    url: http://localhost:8081
    auth:                  # optional BasicAuth
      username: sr-user
      password: sr-secret
    cache_capacity: 1024   # default 1024
    request_timeout: 10    # seconds, default 10
```

The Confluent wire envelope is `[0x00][schema_id: 4-byte big-endian][payload bytes…]`. The
client caches schema fetches (keyed by id) and registrations (keyed by subject/type/text) in
two LRUs of `cache_capacity`, so a stream sharing one schema issues a single registry call.
On the sink side the subject is `{topic}-value` (Confluent TopicNameStrategy).

---

## Usage

Library callers normally pull these in transitively. To depend on them directly:

```bash
cargo add faucet-common-kafka
cargo add faucet-common-kafka --features schema-registry   # for the Confluent variants
```

```rust
use faucet_common_kafka::{KafkaAuth, KafkaValueFormat, CompressionType};

// Built straight from a YAML/JSON config, or constructed in code:
let auth: KafkaAuth = serde_yaml::from_str(
    "type: sasl_plain\nusername: u\npassword: p",
)?;
let format = KafkaValueFormat::default();      // KafkaValueFormat::Json
let compression = CompressionType::default();  // CompressionType::None
# Ok::<(), Box<dyn std::error::Error>>(())
```

End users configure these through the Kafka connectors rather than this crate; see the
connector READMEs for full pipeline examples.

---

## Feature flags

| Feature | Default | Adds |
|---------|---------|------|
| `schema-registry` | off | The Confluent `KafkaValueFormat` variants, `SchemaRegistryConfig`, `SchemaRegistryClient`, and the `avro`/`protobuf`/`json_schema`/`envelope` codec modules. Pulls in `reqwest`, `lru`, `bytes`, `apache-avro`, `prost-reflect`, `prost`, `jsonschema`, `tokio`, `url`, `urlencoding`. |

> In the umbrella `faucet-stream` crate and the `faucet-cli`, this is enabled via the
> `kafka-schema-registry` feature, which forwards to this crate's `schema-registry` feature.

### Platform support

librdkafka is built with TLS plus its built-in `PLAIN`, `SCRAM-SHA-256/512` and
`OAUTHBEARER` mechanisms on every platform. On Unix it also links Cyrus SASL, so
Kerberos (`sasl.mechanism: GSSAPI` through the connectors' `extra_client_config`)
works there; Cyrus SASL does not build on Windows, so GSSAPI is unavailable in
Windows builds.

---

## Troubleshooting / FAQ

| Symptom | Cause & fix |
|---------|-------------|
| `confluent_avro` / `confluent_json_schema` rejected as an unknown variant | The `schema-registry` feature is off. Enable it on this crate (or `kafka-schema-registry` on the umbrella / CLI). |
| `sasl_plain` config rejected at load | `username` and `password` must both be non-empty. |
| `ssl` config errors on a path | `ca_path`, `cert_path`, and `key_path` are validated to exist on the filesystem at config time. Check the paths and the process's read permissions. |
| `confluent_protobuf` returns `FaucetError::Config` | Protobuf decode/encode is not yet implemented in v1 (#44). Use `confluent_avro` or `confluent_json_schema`, or decode protobuf upstream. |
| Schema Registry calls time out | Raise `request_timeout` (seconds, default 10) and verify `url` / `auth` reachability. |
| Encrypted SSL key fails to load | Supply `key_password`; omit it only for unencrypted keys. |

---

## See also

- [faucet-source-kafka](https://crates.io/crates/faucet-source-kafka) — Kafka consumer source
- [faucet-sink-kafka](https://crates.io/crates/faucet-sink-kafka) — Kafka producer sink
- [Connectors reference](https://faucet-hq.github.io/faucet-stream/reference/connectors.html)
- [Authentication cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/auth.html)

---

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)
or [MIT license](https://opensource.org/licenses/MIT) at your option.
