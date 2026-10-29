# faucet-registry

[![crates.io](https://img.shields.io/crates/v/faucet-registry.svg)](https://crates.io/crates/faucet-registry)
[![docs.rs](https://docs.rs/faucet-registry/badge.svg)](https://docs.rs/faucet-registry)

Build any [faucet-stream](https://github.com/faucet-hq/faucet-stream) source
or sink from its `type:` name and a JSON config — the same name → connector
map the `faucet` CLI runs pipelines with — without depending on the CLI.

This is the crate for tools that **embed** faucet's connectors: an
orchestrator, a bridge to another data tool, a language binding. It carries no
CLI, server, scheduler or UI dependencies, and it follows semver (it is checked
by the workspace's `cargo-semver-checks` gate).

## Install

Every connector is behind its own feature; none is on by default.

```toml
[dependencies]
faucet-registry = { version = "1", features = ["source-postgres", "sink-jsonl"] }
faucet-core = "1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
serde_json = "1"
```

| Feature | Enables |
|---|---|
| `source-<kind>` / `sink-<kind>` | One connector (`source-postgres`, `sink-jsonl`, `source-kafka`, …) |
| `source` / `sink` | Every first-party source / sink except the Oracle ones (they load Oracle Instant Client at run time) |
| `full` | Everything in this manifest, Oracle included |
| `compression`, `encryption`, `arrow`, `file-formats` / `file-format-*`, `mtls`, `rabbitmq-tls`, `oauth1`, `google-sa`, `kafka-schema-registry`, `delta-s3` / `delta-azure` / `delta-gcs`, `source-iceberg-*` / `sink-iceberg-*`, `*-staging`, `source-rest-excel` / `source-xml-excel` | The matching option of the connector crates |

A connector whose feature is off is reported as unknown
(`RegistryError::UnknownConnector`).

## Embedding example

Build a source and a sink by name, move rows, and keep a bookmark between runs:

```rust,no_run
use std::sync::Arc;

use faucet_core::{FileStateStore, Pipeline};
use faucet_registry::{auth_catalog, build_sink, build_source};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // A config's `auth:` block (none here); connectors refer to its providers
    // with `auth: { ref: <name> }` and share one token cache.
    let auth = auth_catalog::build_auth_catalog(None)?;

    let source = build_source(
        "file",
        json!({ "path": "inbox/", "incremental": { "by": "name" } }),
        &auth,
        None,
    )
    .await?;
    let sink = build_sink("jsonl", json!({ "path": "out.jsonl", "append": true }), &auth).await?;

    // The bookmark is read before the run and written only after the sink
    // confirms each page, so the next run reads only new files.
    let result = Pipeline::new(source.as_ref(), sink.as_ref())
        .with_state_store(Arc::new(FileStateStore::new("state")))
        .run()
        .await?;
    println!("{} records", result.records_written);
    Ok(())
}
```

This needs the `source-file` and `sink-jsonl` features. The same example runs
as an integration test (`tests/embed.rs`).

Other lookups, all by name and without building anything:

| Function | Answers |
|---|---|
| `source_kinds()` / `sink_kinds()`, `source_exists` / `sink_exists` | Which connectors this build has |
| `source_descriptions()` / `sink_descriptions()` | One-line descriptions |
| `source_schema(kind)` / `sink_schema(kind)` | The config's JSON Schema |
| `validate_source_config` / `validate_sink_config` | Whether a config deserializes and passes the connector's own checks |
| `source_capabilities(kind, &config)` / `sink_capabilities(kind, &config)` and `*_kind_capabilities`, `*_kinds_where` | What a connector can do (write modes, exactly-once, discover, …) |
| `source_resumes_from_bookmark`, `source_reads_a_delta`, `source_position_fields`, `source_state_schema`, `migrate_source_state`, `state_codec_for` | How a source uses state |

## What the registry does not do

**It does not resolve references.** `build_source` / `build_sink` take an
already resolved config: `${env:…}`, `${file:…}` and secret-manager
references are not expanded, and no escape is undone. Resolve references
yourself, with the trust policy your host needs, before you call them.

**It does not run programs unless told to.** A connector whose config names a
program to run on this host — the Singer tap and target bridges, or a plugin
whose schema has an `x-faucet-resource: command` field — is refused with
`RegistryError::SubprocessRefused`. Allow it explicitly:

```rust,no_run
# async fn f(auth: &faucet_registry::AuthCatalog, cfg: serde_json::Value) -> Result<(), faucet_registry::RegistryError> {
use faucet_registry::{BuildPolicy, build_source_with};
let policy = BuildPolicy::default().allow_subprocess(true);
let tap = build_source_with("singer", cfg, auth, None, &policy).await?;
# Ok(()) }
```

## Secrets found while building

Some connectors surface credentials while they are built: a
`google_service_account` auth provider's inline key, a Singer target's
`target_config` / `env`. Install a registrar once to be told about each, so you
can scrub them from your logs:

```rust
faucet_registry::secrets::install(Box::new(|secret: &str| {
    let _ = secret; // add to your redaction set
}));
```

Without one, the values are not reported anywhere.

## Your own connectors

Register a connector under a `type:` name of your own; every lookup above then
finds it like a built-in:

```rust,no_run
# use faucet_core::{Source, async_trait, serde_json::Value};
# use std::collections::HashMap;
# struct MySource;
# #[async_trait]
# impl Source for MySource {
#     async fn fetch_with_context(&self, _: &HashMap<String, Value>) -> Result<Vec<Value>, faucet_core::FaucetError> { Ok(vec![]) }
#     fn config_schema(&self) -> Value { Value::Null }
# }
faucet_registry::PluginRegistry::with_builtins()
    .register_source("my-api", |_config| Ok(Box::new(MySource) as Box<dyn Source>))
    .install()?;
# Ok::<(), faucet_registry::RegistryError>(())
```

Use `register_source_described` / `register_sink_described` with your crate's
`ConnectorDescriptor` so capability lookups see what it declares.

## Errors

`RegistryError` (`#[non_exhaustive]`): `UnknownConnector`,
`InvalidConnectorConfig` (with the `path` in the config where deserializing
stopped, when known; values are redacted from the message), `UnknownAuthProvider`,
`AuthProviderBuild`, `SubprocessRefused`, `Config`, `Faucet` (a connector's
constructor failed) and `Other` (a plugin factory's own error).

## Stability

Everything above follows semver. The hidden `internal` module holds helpers the
`faucet` CLI shares with this crate and is outside that contract. The CLI keeps
`faucet_cli::registry` and `faucet_cli::auth_catalog` as re-exports of this
crate.

See also [Embedding faucet in another tool](https://faucet-hq.github.io/faucet-stream/extending/embedding.html).
