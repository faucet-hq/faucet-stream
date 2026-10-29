# API Design Standard

*Conventions every public type, trait, and connector API in faucet-stream must follow so the ecosystem stays object-safe, additive, and stable.*

This standard governs anything reachable from a crate's public surface — most importantly `faucet_core`, which third-party connector authors depend on directly. The overriding constraint is that faucet-stream is a **marketplace**: a change that forces every `faucet-source-*` / `faucet-sink-*` crate to be edited is a design failure, not a routine bump.

Related decisions: [ADR 0003 — Builder Pattern](../adr/0003-builder-pattern.md), [ADR 0004 — JSON Record Model](../adr/0004-json-record-model.md).

## Traits

- **MUST keep `Source` and `Sink` object-safe.** They are consumed as `Box<dyn Source>` / `Box<dyn Sink>` throughout the pipeline and the CLI registry (`crates/core/src/traits.rs`, `crates/registry/src/connectors.rs`). No generic type parameters on trait methods, no associated types, no `Self`-returning methods, no `impl Trait` in return position that would break `dyn`.
- **MUST give every new trait method a default implementation.** A new method without a default is a breaking change for every existing connector. All capability methods added over time — `stream_pages`, `state_key`, `capture_resume_position`, `supports_exactly_once`, `is_shardable`/`enumerate_shards`/`apply_shard`, `supports_discover`/`discover`, `current_schema`/`supports_schema_evolution`/`evolve_schema`, `check` — ship with a safe default so a connector written against an older core still compiles.

  ```rust
  // GOOD — additive, defaulted, opt-in.
  fn supports_discover(&self) -> bool { false }
  async fn discover(&self) -> Result<Vec<DatasetDescriptor>, FaucetError> {
      Err(FaucetError::Source("discover not supported".into()))
  }

  // BAD — no default; breaks every connector on upgrade.
  async fn discover(&self) -> Result<Vec<DatasetDescriptor>, FaucetError>;
  ```

- **SHOULD express a new capability as a boolean probe + a defaulted worker method**, mirroring `supports_exactly_once()` + `write_batch_idempotent()`. The probe lets the runtime and `faucet validate`/`doctor` reason about topology *before* a run; the worker stays inert until overridden.
- **MUST NOT couple a trait to a concrete connector or a driver type.** Method signatures use `serde_json::Value`, `&str`, and core types only. Driver-specific types (a `sqlx::Pool`, an `aws_sdk_s3::Client`) never appear on a trait boundary.

## Builders

- **MUST expose runtime wiring through progressive `with_*` builders**, not wide constructors. `Pipeline::new(&source, &sink)` returns a working pipeline; every optional capability is a chained, order-independent `.with_state_store(..)`, `.with_dlq(..)`, `.with_quality(..)`, `.with_cancel(..)`, `.with_delivery(..)` (see `crates/core/src/pipeline.rs`). The same shape backs `RunStreamOptions`.
- **Rationale:** a builder is inherently backward-compatible (a new knob is a new method, never a new positional argument) and it keeps the zero-config path a one-liner. See [ADR 0003](../adr/0003-builder-pattern.md).

## Connector dependency surface

- **`faucet-core` MUST remain the only required dependency for a connector author.** It re-exports the crates authors would otherwise pin themselves: `async_trait`, `serde_json` (with `Value`, `json!`), `schemars` (`JsonSchema`, `schema_for!`), `async_stream`, and `tokio_util::sync::CancellationToken`.
- **MUST re-export a new shared dependency from `faucet-core`** rather than requiring authors to add it to their own `Cargo.toml`.
- **MUST NOT add a mandatory heavy dependency to `faucet-core`** (DB drivers, cloud SDKs). Core stays lightweight; connector-specific deps belong in the connector crate. See [ADR 0006](../adr/0006-state-management.md) for the same principle applied to state backends.

## Config types

- **Every config struct MUST derive `Serialize + Deserialize + JsonSchema`** and every sub-enum with it. This is what powers `faucet schema`, `faucet init`, and editor autocomplete for free.
- **Config structs MUST contain no I/O or protocol logic.** They live in `config.rs`; the single I/O module is `stream.rs`/`sink.rs`. A config type is pure data.
- **Auth/credentials MUST use the adjacently-tagged `{ type, config }` shape** with snake_case discriminators, e.g. `auth: { type: bearer, config: { token: … } }`. This one wire shape is consistent across every connector and the shared `auth:` catalog; it also accepts `auth: { ref: <name> }` for shared providers. Introducing a differently-shaped auth block is a defect.

  ```yaml
  # GOOD — the canonical adjacently-tagged shape.
  auth: { type: oauth2, config: { client_id: …, client_secret: … } }
  # BAD — flat/internally-tagged; inconsistent with the rest of the ecosystem.
  auth: { type: oauth2, client_id: …, client_secret: … }
  ```

- **Custom-serde fields MUST carry `#[schemars(with = "...")]`** so the generated schema stays valid (e.g. `reqwest::Method` → `String`).

## Versioning

- **Every crate we ship starts at `1.0.0` or higher — never `0.x`.** A new crate is scaffolded at `version = "1.0.0"` in both its `Cargo.toml` and the `[workspace.dependencies]` path entry.
- **MUST treat public-API changes under semver discipline.** The `cargo-semver-checks` gate in CI is the backstop; a breaking change requires a major bump and an entry in the change surface described by [docs/stability.md](../stability.md).
- **SHOULD prefer an additive path over a breaking one.** A defaulted trait method, a new builder method, or a new enum variant behind `#[non_exhaustive]` is almost always available and avoids an ecosystem-wide edit.

## Related

- [ADR 0003 — Builder Pattern](../adr/0003-builder-pattern.md)
- [ADR 0004 — JSON Record Model](../adr/0004-json-record-model.md)
- [Public API Stability](../stability.md)
- [Connector SDK](../architecture/connector-sdk.md)
- [Extensibility](../architecture/extensibility.md)
- [Error Handling Standard](./error-handling.md)
