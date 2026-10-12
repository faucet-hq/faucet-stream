# RFC 0001 — Capability descriptors for connectors

*Replace the scattered set of defaulted `Source`/`Sink` capability methods with one uniform, introspectable capability model.*

| | |
|---|---|
| **RFC** | 0001 |
| **Title** | Capability descriptors for connectors |
| **Status** | Superseded by [RFC 0013](./0013-structural-core-guarantees.md) (#876) |
| **Authors** | faucet-stream maintainers |
| **Related issues** | epic #38 |
| **Related ADRs** | [0003 builder pattern](../docs/adr/0003-builder-pattern.md), [0010 pipeline runtime](../docs/adr/0010-pipeline-runtime.md) |

## Summary

Connector capabilities — exactly-once support, shardability, discoverability,
schema evolution, supported write modes, keyed dedup — are currently expressed
as a handful of independent, defaulted trait methods discovered one at a time by
call sites. This RFC proposes consolidating them behind a single introspectable
capability surface so that validation, `faucet doctor`, and the various planners
query capabilities uniformly, and so third-party connectors advertise what they
support in one obvious place.

## Motivation

Capabilities today are a growing set of boolean/enum methods with defaults, spread
across `crates/core/src/traits.rs`:

- `Source::supports_exactly_once()` / `replay_guarantee()`
- `Source::is_shardable()` / `enumerate_shards()` / `apply_shard()`
- `Source::supports_discover()` / `discover()`
- `Source::capture_resume_position()`
- `Sink::supports_idempotent_writes()` / `sink_guarantee()` / `dedups_by_key()`
- `Sink::supported_write_modes()`
- `Sink::supports_schema_evolution()` / `current_schema()` / `evolve_schema()`

This has worked — defaulted methods keep the traits object-safe and additive
(see [ADR 0003](../docs/adr/0003-builder-pattern.md)) — but three problems are
emerging as the set grows:

1. **Duplication of the capability truth.** The CLI re-encodes several of these
   as static allowlists so it can validate a config *before* building a
   connector — e.g. `registry::source_supports_exactly_once`,
   `sink_supports_idempotent_writes`, `sink_supported_write_modes`,
   `sink_supports_schema_evolution` in `cli/src/registry.rs`. The trait method
   and the allowlist can drift, and every new capability requires editing both.
2. **No uniform introspection.** `faucet validate` and `faucet doctor` reason
   about capabilities ad hoc. A reader (or a UI) cannot ask a connector "what
   can you do?" and get a structured answer; they must know which methods to
   probe.
3. **Discoverability for authors.** A connector author has to find each method
   by reading the trait. There is no single "capabilities" concept to implement.

Doing nothing means the allowlist duplication and per-capability call-site
plumbing keeps growing linearly with each new capability.

## Guide-level explanation

A connector reports a single `Capabilities` value. Planners and diagnostics read
that value instead of probing individual methods:

```rust
let caps = source.capabilities();
if caps.replay == ReplayGuarantee::Deterministic { /* eligible for atomic watermark */ }
```

For connector authors, the capability set becomes the one place to declare
support:

```rust
fn capabilities(&self) -> SinkCapabilities {
    SinkCapabilities {
        idempotent_writes: true,
        write_modes: &[WriteMode::Append, WriteMode::Upsert, WriteMode::Delete],
        schema_evolution: true,
        dedups_by_key: self.config.write.dedups_by_key(),
        ..SinkCapabilities::default()
    }
}
```

`faucet validate` and `faucet doctor` print the derived
[delivery guarantee](../docs/architecture/invariants.md) and every capability
from this single source of truth, and the CLI's static allowlists are generated
from — or replaced by — the same descriptors.

## Reference-level explanation

Introduce two plain-data structs in `faucet-core` (e.g.
`crates/core/src/capability.rs`):

```rust
pub struct SourceCapabilities {
    pub replay: ReplayGuarantee,      // already exists as an enum
    pub shardable: bool,
    pub discoverable: bool,
    pub captures_resume_position: bool,
}

pub struct SinkCapabilities {
    pub guarantee: SinkGuarantee,     // already exists as an enum
    pub idempotent_writes: bool,
    pub dedups_by_key: bool,
    pub write_modes: &'static [WriteMode],
    pub schema_evolution: bool,
}
```

Add `fn capabilities(&self) -> …Capabilities` to each trait with a **default
implementation that derives the struct from the existing methods**. This keeps
the change additive and object-safe: existing connectors compile unchanged and
report correct capabilities via the default derivation; connectors may override
`capabilities()` directly once the individual methods are deprecated.

Migration is staged:

1. Add the structs and defaulted `capabilities()` deriving from today's methods.
2. Move CLI validation (`cli/src/registry.rs`, `cli/src/expand.rs`) to read the
   descriptor instead of hand-maintained allowlists.
3. Optionally deprecate the fine-grained boolean methods in a later release once
   all call sites read the descriptor.

Object-safety is preserved because `capabilities()` returns an owned/`'static`
value and takes `&self` — no generics, no associated types (see the
[extensibility](../docs/architecture/extensibility.md) constraints).

The delivery-guarantee derivation (`derive_delivery_guarantee` in
`crates/core/src/idempotency.rs`) already consumes a `GuaranteeInputs` bundle;
that function becomes the model for how planners consume capability bundles.

## Drawbacks

- **Two representations during migration.** Until the individual methods are
  deprecated, both the methods and the descriptor exist; the defaulted
  derivation must stay in sync with the methods (mechanically, but still surface).
- **Coarser overrides.** A connector overriding `capabilities()` wholesale must
  restate every field; partial overrides rely on `..Default::default()`
  discipline.
- **`&'static` write-mode slices** constrain how dynamic a connector's advertised
  write modes can be; `dedups_by_key` already shows a capability that depends on
  *live config*, so some fields must be computed per-instance, not `const`.

## Rationale and alternatives

- **Keep the status quo (defaulted methods + CLI allowlists).** Rejected: the
  duplication and drift risk grow with every capability, and there is no uniform
  introspection for tooling/UI.
- **A bitflags-style capability set.** Rejected for the richer fields
  (`write_modes`, the two guarantee enums) that are not booleans; a struct models
  them faithfully.
- **A trait-object registry keyed by capability name (string-typed).** Rejected:
  loses compile-time checking and invites typo bugs, contrary to the
  [error-handling standard](../docs/standards/error-handling.md).

## Prior art

Kafka Connect exposes connector capabilities/config via `ConfigDef` + validation
endpoints. Arrow's `DataType`/`Schema` model shows structured capability data
consumed by planners. dbt's adapter capabilities registry is the closest analog:
adapters declare a capability map the core queries uniformly.

## Unresolved questions

- Must resolve before Accepted: whether to deprecate the individual methods or
  keep them indefinitely as the ergonomic API with the descriptor purely derived.
- During implementation: exact field set, and whether capabilities should be
  serializable (`JsonSchema`) for the web console / `faucet doctor --json`.

## Future possibilities

- A `faucet capabilities <connector>` command and a web-console capability matrix
  generated from the descriptors rather than hand-maintained docs.
- Version negotiation in a future plugin system (see
  [RFC 0003](./0003-plugin-system.md)) built on the same descriptor.

## Related

- [RFC process](./README.md) · [RFC 0003 plugin system](./0003-plugin-system.md)
- [Connector SDK](../docs/architecture/connector-sdk.md) · [Extensibility](../docs/architecture/extensibility.md)
- [Design invariants](../docs/architecture/invariants.md)
