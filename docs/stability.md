# API Stability Policy

*What is stable, what is experimental, what is internal, and the compatibility you can rely on across releases.*

faucet-stream is a Cargo workspace of independently versioned crates (63 as of
this writing) plus the `faucet` CLI. This document defines which parts of that
surface carry a stability promise, and what that promise means in practice.

The policy exists to serve two audiences with opposite needs: **connector
authors** and **library embedders**, who need a stable foundation to build on;
and **maintainers**, who need room to evolve the runtime. The categories below
draw the line between them.

---

## Semver expectations

- **Every crate is versioned `1.0.0` or higher — never `0.x`.** A `1.x` release
  follows semantic versioning: breaking changes to a crate's public API require a
  major-version bump.
- **The public API is gated by tooling, not vigilance.** `cargo-semver-checks`
  runs in CI against each crate's public surface, so an accidental breaking change
  fails the build rather than shipping.
- **Object-safe defaulted trait methods are the compatibility workhorse.** When
  `Source` or `Sink` grows a new capability method, it ships with a default
  implementation. Existing connectors — including third-party crates compiled
  against an older `faucet-core` — keep compiling and behave exactly as before.
  This is why trait growth is *not* a breaking change here. See
  [ADR 0003](./adr/0003-builder-pattern.md) and
  [Engineering principles](./engineering-principles.md).

### The feature-flag caveat

Cargo features are part of the compatibility surface but are governed
separately from semver on types. Enabling a new *optional* feature is additive
and non-breaking. However, feature *unification* across a workspace can change
observable behaviour (for example, `serde_json`'s `preserve_order` flips `Map`
from `BTreeMap` to `IndexMap` under `--all-features`). Code — and especially
tests — must not depend on behaviour that only holds under a particular feature
set. Treat feature-gated behaviour as covered by the same stability tier as the
type it gates.

---

## Stable

Stable APIs carry the full semver promise: they will not break without a major
version bump, and breaking them requires a deprecation cycle (below). The Stable
set is grounded in what the crate roots actually re-export.

**`faucet-core` (the connector-facing contract):**

- The `Source` and `Sink` traits, including their defaulted capability methods.
  New defaulted methods may be *added* (non-breaking); existing signatures are
  stable.
- `Pipeline` and its `with_*` builder methods; the free function `run_stream` and
  `RunStreamOptions`.
- `StreamPage`, `PipelineResult`, and the batch-size contract
  (`DEFAULT_BATCH_SIZE`, `MAX_BATCH_SIZE`, `validate_batch_size`, the `0`
  sentinel).
- `StateStore` and the built-in `MemoryStateStore` / `FileStateStore`;
  `validate_state_key`.
- `FaucetError` and its variants, including `Custom(Box<dyn Error>)` for
  third-party errors. New variants may be added (the enum is not exhaustively
  matched by well-behaved external code).
- The re-exported ecosystem crates connector authors depend on: `async_trait`,
  `serde_json` (`Value`, `json!`), `schemars` (`JsonSchema`, `schema_for!`).

**CLI:**

- The **config-file grammar `version: 1`** — the top-level `pipeline` / `matrix`
  / `execution` / `auth` shape, the `{ type, config }` connector shape, and the
  documented transform/state/dlq blocks. Grammar changes are additive within
  `version: 1`; an incompatible grammar would be a new `version`.
- The stable CLI verbs and their documented flags: `run`, `validate`, `schema`,
  `list`, `preview`, `init`, `doctor`, `test`.

## Experimental

Experimental APIs are shipped, supported, and safe to use, but their surface may
change in a minor release as the design settles. They are opt-in — a user who
does not enable them is unaffected. Pin the crate version if you depend on the
exact shape.

- **Clustered execution** (`--cluster`, Mode A/B) and its `RunHistory` cluster
  methods.
- **Event-driven triggers** (`--triggers`, the `triggers*` feature family) — the
  trigger config schema and `${trigger.*}` tokens.
- **Data Movement Catalog** (`catalog` feature) — the `catalog:` block, the
  `/v1/catalog/*` endpoints, and the catalog storage schema.
- **Adaptive batching** (`with_adaptive`, `crates/core/src/adaptive.rs`) — the
  AIMD controller knobs.
- **OTLP export** (`otel` feature) — the `observability.otel:` block. The OTel
  crate stack is pinned to the 0.31 line, so this surface tracks an
  upstream-in-flux dependency.
- **The HTTP control plane** (`faucet serve`) request/response shapes beyond the
  documented `docs/openapi.yaml` core — the OpenAPI file is the source of truth,
  and endpoints outside it should be treated as experimental.

## Internal

Internal surface has no stability promise and may change at any time. Do not
build against it. It is internal precisely because it is not re-exported from a
crate root.

- Everything under `cli/src/serve/` (the axum server, history backends, RBAC,
  cluster loops) except the documented HTTP API.
- The `crates/core/src/observability/` decorator internals (`InstrumentedSource`,
  `InstrumentedSink`, the metric-emission plumbing). The *metric names and labels*
  they emit are a stable operational contract; the Rust types are not.
- Pure helper modules not re-exported from `lib.rs` (compile passes, expand,
  merge, interpolate, etc. in the CLI).
- The exactly-once token format (`format_token`, `wrap_state`) — sinks store
  tokens opaquely and must never parse them; the format is an internal
  implementation detail of the runtime.

## Deprecated

There are **no deprecated public APIs at this time.** When something is
deprecated in the future, it will follow this cycle:

1. **Announce** in the crate's `CHANGELOG.md` and mark the item
   `#[deprecated(since = "...", note = "use X instead")]` so it warns at compile
   time.
2. **Coexist** — the deprecated item keeps working for at least one minor release,
   with the replacement documented alongside it.
3. **Remove** only in a major version bump, never in a minor or patch release.

This mirrors how trait growth stays additive: users get a compiler-visible signal
and a migration window before anything they depend on disappears.

---

## Reporting a compatibility break

If a minor or patch release breaks a Stable API, that is a bug — please open an
issue. The `cargo-semver-checks` gate is meant to make this impossible, so a real
break is worth investigating as a CI-coverage gap, not just a fix.

---

## Related

- [Engineering principles](./engineering-principles.md) — backward compatibility as a feature
- [ADR 0003 — builder pattern & object-safe traits](./adr/0003-builder-pattern.md)
- [Standards: API design](./standards/api-design.md)
- [Architectural roadmap](./roadmap.md)
- [Connector protocol (FCP v0)](./spec/faucet-connector-spec-v0.md)
- [Connector conformance & tiers](./site/src/content/docs/reference/conformance.md) — the
  per-connector maturity tier (`Stable` / `Experimental` / `Beta` / `Draft`)
  from `faucet conformance`, complementing the crate-level policy above.
