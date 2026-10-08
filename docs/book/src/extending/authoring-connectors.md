# Authoring a connector

faucet-stream is designed as an ecosystem: third parties can publish their own
`faucet-source-*` / `faucet-sink-*` crates with minimal friction. **`faucet-core`
is the only required dependency** — it re-exports everything a connector author
needs (`async_trait`, `serde_json`, `schemars`).

## Scaffold it in one command

Don't hand-assemble the crate — generate one that already follows every
convention below:

```bash
faucet new connector acme --kind source        # → faucet-source-acme/
faucet new connector acme --kind sink --common  # also emit faucet-common-acme/
```

The generated crate has the standard module layout (`config.rs`, `stream.rs` /
`sink.rs`), a `JsonSchema`-deriving config, the `config_schema()` /
`connector_name()` overrides, the `#![cfg_attr(docsrs, feature(doc_cfg))]`
crate-root line, the `[package.metadata.docs.rs]` block, system-name-first
crates.io keywords, a README, and a passing unit test — so `cargo test` is green
immediately with a trivial passthrough. Replace the `TODO`s with your real
config fields and I/O, then publish. The rest of this page explains what the
scaffold sets up.

To make your published connector usable from a `faucet.yaml` config (not just
from Rust), see
[Custom binaries with third-party connectors](https://github.com/faucet-hq/faucet-stream/blob/main/cli/README.md#custom-binaries-with-third-party-connectors).

## The traits

Implement `Source` or `Sink`. Both are object-safe (`Box<dyn Source>` works) and
all newer methods have defaults, so a minimal connector is small.

```rust,ignore
use std::collections::HashMap;

use faucet_core::{async_trait, Source, Sink, FaucetError, Value};

struct MySource { /* reusable client/pool created in new() */ }

#[async_trait]
impl Source for MySource {
    // Primary entry point. (`fetch_all()` is a provided convenience.)
    // `context` carries parent-record values for `${parent.path}` placeholders.
    async fn fetch_with_context(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        todo!("fetch records from your system")
    }
}

struct MySink { /* reusable client/pool */ }

#[async_trait]
impl Sink for MySink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        todo!("write records to your system")
    }
}
```

Your connector now works with the `Pipeline` and every other connector:
`Pipeline::new(&MySource { .. }, &MySink { .. }).run().await?`.

## Crate layout

Follow the same module layout as the built-in connectors:

- **`lib.rs`** — re-export the config + the `Source`/`Sink` type. First line:
  `#![cfg_attr(docsrs, feature(doc_cfg))]` (see below).
- **`config.rs`** — the config struct + sub-enums, deriving
  `Serialize + Deserialize + JsonSchema`. **No I/O here.**
- **`stream.rs`** (source) / **`sink.rs`** (sink) — the one place that performs
  I/O. Create reusable clients/pools in `new()` and store them; never reconnect
  per call.

## Make it fast

Performance is the project's first principle. Reuse clients and connections,
pool database connections, use multi-row inserts and bulk APIs, and prefer
parallel I/O. Where it makes sense, override `stream_pages` to stream natively
from your source's paging primitive so memory stays bounded.

## Config schema introspection

Implement `config_schema()` so `faucet schema` and `faucet init` work:

```rust,ignore
fn config_schema(&self) -> Value {
    faucet_core::schema_for!(MyConfig).into()
}
```

Derive `JsonSchema` on the config struct and all sub-types, and add
`#[schemars(with = "String")]` for any custom-serde fields.

## Errors

Map every failure to a `FaucetError` variant. Third-party error types wrap into
`FaucetError::Custom(Box<dyn Error + Send + Sync>)` without losing the chain.
Never `.unwrap()` on anything that can fail at runtime.

## Self-certify with the conformance battery

A connector becomes **Tier-1 / conformant** by adding a `tests/conformance.rs`
that invokes the reusable `faucet-conformance` battery against the *real*
connector and passing it in CI. That battery **is** the tiering mechanism —
there is no separate scheme. Anything not yet wired into it is Tier-2 (still
useful, usually with its own integration tests — Tier-2 does not mean low
quality).

Add the battery as a dev-dependency (it is a path-only workspace crate, so it
does not need to be published first):

```toml
[dev-dependencies]
faucet-conformance.workspace = true
```

For a **source**, drive the checks against a live connector:

```rust,ignore
// crates/source/foo/tests/conformance.rs
use faucet_source_foo::{FooSource, FooSourceConfig};

#[test]
fn conformance_config_schema_valid() {
    let source = FooSource::new(FooSourceConfig::new(/* … */));
    faucet_conformance::assert_config_schema_valid(&source);
}

#[tokio::test]
async fn conformance_bounded_memory() {
    // drive a source that yields `total` records in pages of `batch`
    faucet_conformance::assert_bounded_memory(&source, batch, total).await;
}

#[tokio::test]
async fn conformance_errors_not_panics() {
    // a source configured to fail must return Err, not panic
    faucet_conformance::assert_errors_not_panics(&broken_source).await;
}
```

Resumable sources also add `assert_bookmark_roundtrip` (persist a bookmark,
re-run, confirm the stream resumes at exactly that position). For a **sink**,
use `assert_idempotent_replay` and `assert_capabilities_truthful` — both take a
`distinct_count` closure that returns the destination's current row count (for a
real sink, a `SELECT count(*)` against the target table).

A **discoverable source** (one that overrides `supports_discover`) adds
`assert_discover_roundtrips`, an *integration-level* check that proves every
dataset `discover()` reports is genuinely selectable — it deep-merges each
descriptor's `config_patch` onto the base config (the `merge_config_patch`
helper does this), rebuilds the source, and reads it. Run it against the same
live/seeded backend your other checks use:

```rust,ignore
faucet_conformance::assert_discover_roundtrips(&source, |patch| {
    let base = /* the connection config as a serde_json::Value */;
    let merged = faucet_conformance::merge_config_patch(base, &patch);
    let cfg = serde_json::from_value(merged).unwrap();
    async move { Box::new(FooSource::new(cfg).await.unwrap()) as Box<dyn faucet_core::Source> }
})
.await;
```

`assert_cancellation_flushes` covers the flush-completing cancellation contract
(a mid-run `CancellationToken` stops at a page boundary and still flushes) by
driving the real pipeline — most useful for a buffered sink (Parquet footer, S3
multipart) whose output only commits on `flush()`.

**Assert the honest branch.** Where a connector legitimately can't satisfy a
check — an append-only sink has no idempotency mechanism, for instance — don't
skip it: assert the honest behaviour instead. The capability method returns
`false` and the pipeline refuses `delivery: exactly_once`. A passing conformance
run that documents what a connector *cannot* do is exactly the point.

The full contract is the
[Faucet Connector Protocol (FCP v0)](../spec/faucet-connector-spec-v0.md).

## docs.rs setup

So docs.rs renders your full API with per-feature badges, add to `Cargo.toml`:

```toml
[package.metadata.docs.rs]
all-features = true
rustdoc-args = ["--cfg", "docsrs"]
```

and make the first line of `lib.rs` `#![cfg_attr(docsrs, feature(doc_cfg))]`.

## Naming & publishing

Name crates `faucet-source-<name>` / `faucet-sink-<name>`. If you ship both a
source and a sink for the same system, put shared types (auth, formats) in a
`faucet-common-<name>` crate that both depend on and re-export.

> See any built-in connector (e.g. `faucet-source-rest`) for a reference
> implementation.
