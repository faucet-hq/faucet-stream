# Authoring a connector

*How to build a source or sink that is fast, resumable, and ships cleanly — and every place you must wire it in.*

This is the contributor-facing companion to the user-facing
[authoring guide](../book/src/extending/authoring-connectors.md) and the
[connector protocol spec](../spec/faucet-connector-spec-v0.md). Read those for
the external contract; read this for the *internal* conventions and the wiring
checklist that CI enforces.

A connector is a crate that implements `faucet_core::Source` or
`faucet_core::Sink`. The traits are object-safe on purpose (see the
[builder-pattern ADR](../adr/0003-builder-pattern.md)) — no associated types, no
generic methods — so `Box<dyn Source>` and `Box<dyn Sink>` work and third-party
crates need nothing but `faucet-core`.

## Crate layout

Every connector follows the same module split. Stick to it — reviewers expect it
and it keeps the "pure config vs. I/O" boundary clean:

```
crates/source/foo/
├── Cargo.toml        # version = "1.0.0"; per-crate keywords; docs.rs metadata
└── src/
    ├── lib.rs        # re-exports; first line is the docsrs attribute
    ├── config.rs     # config struct(s) — NO I/O, NO protocol logic
    └── stream.rs     # the ONE place that performs I/O (sink.rs for sinks)
```

- **`lib.rs`** — re-export the config + the `Source`/`Sink` type. **The very
  first line must be** `#![cfg_attr(docsrs, feature(doc_cfg))]` so docs.rs
  renders per-item feature badges (see
  `.claude/rules/publishing.md`).
- **`config.rs`** — the config struct and its sub-enums (auth, format,
  pagination). Derive `Serialize + Deserialize + JsonSchema`. **No I/O and no
  protocol logic here** — this file must be trivially unit-testable and is what
  `faucet schema` / `faucet init` introspect.
- **`stream.rs` / `sink.rs`** — the only file that touches the network or disk.
  Create reusable clients/pools in `new()` and store them on the struct; never
  build a client per call (see [performance](./performance.md)).

Optional helper modules (`auth/`, `pagination/`, `extract/`, `convert.rs`,
`schema.rs`, `state.rs`, `serde_helpers.rs`) hold pure logic that you can unit
test without a network.

New crates start at **`version = "1.0.0"`** — never `0.x` (a hard workspace
rule). Set per-crate `keywords` with the system name first; keep
`categories.workspace = true`; add the `[package.metadata.docs.rs]` block.

## Implement the trait

Minimum viable source:

```rust
#[async_trait]
impl Source for FooSource {
    async fn fetch_with_context(&self, ctx: &HashMap<String, String>)
        -> Result<Vec<Value>, FaucetError> { /* … */ }

    fn config_schema(&self) -> Value { schema_for!(FooConfig) }
    fn connector_name(&self) -> &'static str { "foo" }  // friendly, non-empty
}
```

`fetch_with_context` is the required method. The default `stream_pages`
chunks its result — which materializes everything first. **If the underlying
system has a native paging primitive (cursor, offset, scroll, log position),
override `stream_pages`** so memory stays O(batch_size). See
[stream-pages](../architecture/stream-pages.md) for the contract and
[batching](../architecture/batching.md) for `batch_size` semantics.

`connector_name()` must return a non-empty `&'static str` — it becomes the
`connector` metric label. Empty strings fall back to `"unknown"`.

### Mark the fields that name a resource

A config field that names a local path, a URL or a program the connector runs
carries an `x-faucet-resource` annotation:

```rust
#[schemars(extend("x-faucet-resource" = "path"))]   // or "url" / "command"
pub path: String,
```

`faucet serve` reads it from the schema and checks the value against the
operator's [reference policy](../book/src/reference/http-api.md#reference-policy)
before running a config a caller sent, so your connector is confined with no
serve code. A field that only looks like a resource (an object key inside a
configured bucket, a URL path appended to a checked `base_url`) goes in
`scripts/resource-annotation-allow.txt` with a reason instead.
`cli/tests/resource_annotations.rs` fails on an unannotated field whose name
looks like a resource, and on an allow-list entry that no longer matches one.
After adding an annotation, regenerate `schemas/faucet.schema.json`.

A connector whose own input can reach past its declared resources — a SQL
engine whose query can `ATTACH` a file or `read_csv` a path — needs a boolean
switch that confines it, marked so the server can find it:

```rust
#[schemars(extend("x-faucet-sandbox" = true))]
pub sandbox: bool,
```

`faucet serve` forces every field marked this way to `true` in a config it did
not load from its own triggers file, wherever the connector appears (template,
row override, topology node, fan-out source, DLQ sink, …).

### Resumability

If your source can resume, implement two methods:

- `state_key()` → a stable, non-empty key (validated by `validate_state_key`).
- `apply_start_bookmark(bookmark)` → seek to the position encoded in the
  bookmark the pipeline read from the [state store](../architecture/state-management.md).

The pipeline handles the rest: it persists the bookmark **only after** the sink
durably wrote and flushed the page. You never call `StateStore::put` yourself.

### Capability opt-ins (all defaulted — implement only what applies)

| Capability | Trait methods | Notes |
|---|---|---|
| Exactly-once (source) | `supports_exactly_once`, `replay_guarantee`, `capture_resume_position` | Only for deterministic-replay sources (CDC, Kafka). See [idempotency](../architecture/recovery.md). |
| Exactly-once (sink) | `supports_idempotent_writes`, `write_batch_idempotent`, `last_committed_token` | Commit records + the commit token **atomically**. |
| Upsert / delete | `supported_write_modes`, and use `faucet_core::write_mode::plan_writes` | See the [write-mode](../book/src/cookbook/upsert.md) cookbook. |
| Discovery | `supports_discover`, `discover` | Read-only, cheap catalog introspection only. |
| Sharding | `is_shardable`, `enumerate_shards`, `apply_shard` | PK-range or hash-of-key. |
| Schema evolution (sink) | `current_schema`, `supports_schema_evolution`, `evolve_schema` | Additive/widening DDL. |

Every one of these has a default implementation, so existing connectors never
break when a new capability lands. That is the whole point of the
[progressive-trait design](../adr/0003-builder-pattern.md).

### Backend calls

Route every backend call through a `faucet_core::io::CallSlot` /
`CallContext::call` and build HTTP clients with `faucet_core::http::Client`;
the workspace `clippy.toml` refuses `reqwest::Client::new` / `builder` and
`tokio::time::sleep` outside sanctioned sites. See
[Backend calls](../book/src/extending/authoring-connectors.md#backend-calls-callcontext-httpclient-batching).

## Wiring checklist (CI enforces most of this)

Implementing the trait is half the job. A connector isn't shipped until it's
wired into every surface. Miss one and a CI job — or a downstream user — breaks:

- [ ] **Umbrella feature** — add `source-foo` / `sink-foo` to `faucet-stream/Cargo.toml`.
- [ ] **CLI feature** — add the matching feature to `cli/Cargo.toml`.
- [ ] **CLI dispatch** — add the arm to `build_source`/`build_sink` (and the
      `*_schema` / `*_descriptions` helpers) in [`cli/src/registry.rs`](../../cli/src/registry.rs).
- [ ] **Registry index** — add a verified entry to
      [`cli/connectors/registry.json`](../../cli/connectors/registry.json), or the CLI
      `registry_index` test panics under `--all-features`.
- [ ] **CI matrix** — add the connector to the `feature-check` matrix in
      `.github/workflows/ci.yml` (feature-isolation builds each connector alone).
- [ ] **Own dep-features** — the crate must enable in *its own* `Cargo.toml`
      every feature of a dependency it uses. Feature unification hides this
      locally but the isolation matrix catches it (see [common-mistakes](./common-mistakes.md)).
- [ ] **README** — crate `README.md` with config fields, auth, defaults.
- [ ] **Docs site** — the capability matrix in
      `docs/book/src/reference/connectors.md`, plus a `cli/examples/*.yaml`.
- [ ] **Shipped example validates** — the example must pass the
      `cli_end_to_end` validate test (use literal hosts, not `${env:VAR}`
      unless the var is in the allowlist).

The full docs-sync trigger table is in
`.claude/rules/maintenance.md`.

## Related

- [Connector SDK](../architecture/connector-sdk.md)
- [Stream pages](../architecture/stream-pages.md)
- [State management](../architecture/state-management.md)
- [Performance](./performance.md)
- [Testing](./testing.md)
- [Common mistakes](./common-mistakes.md)
- [Connector spec (FCP v0)](../spec/faucet-connector-spec-v0.md)
