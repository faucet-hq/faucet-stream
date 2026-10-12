# Architectural Roadmap

*The direction of faucet-stream's architecture — near, medium, and long term. This is direction, not a dated commitment, and is subject to change.*

This roadmap describes where the *architecture* is heading, not which connectors
ship next. Feature-level and connector-level planning lives in the
[community roadmap](./community/roadmap.md) and the tracking epic
[#38](https://github.com/faucet-hq/faucet-stream/issues/38). Deeper,
subsystem-scoped direction is in
[docs/architecture/roadmap.md](./architecture/roadmap.md); concrete design
proposals live in [rfcs/](../rfcs/README.md).

Each item links to the RFC or ADR that carries its design rationale. Items are
grouped by architectural horizon, not by date — an item moves forward when its
dependencies land and its RFC is accepted, not on a calendar.

---

## Guiding constraints

Any change on this roadmap is held to three non-negotiable constraints, in order:

1. **Correctness is never traded for capability.** No item ships if it can
   silently corrupt downstream data. See
   [engineering principles](./engineering-principles.md).
2. **Backward compatibility.** Trait and API growth stays additive via defaulted
   methods; crates remain semver-clean from a 1.0.0 floor. See
   [stability](./stability.md).
3. **`faucet-core` stays lean.** New orchestration and runtime surface belongs in
   the CLI layer or its own crate, not in the connector-facing core. See
   [ADR 0010](./adr/0010-pipeline-runtime.md).

---

## Near term — consolidate what exists

Work that hardens and documents the current architecture without adding new
runtime surface.

- **Documentation maturity.** The architecture docs, ADRs, standards, and RFC
  process introduced alongside this roadmap. The goal is that a new contributor
  can understand *why* each subsystem is shaped as it is without reading the
  5,471-line `crates/core/src/pipeline.rs` cold. Tracked here and across
  [docs/architecture/](./architecture/README.md).
- **Capability-trait cleanup.** The `Source`/`Sink` traits have accreted a dozen
  defaulted capability methods (`supports_exactly_once`, `enumerate_shards`,
  `supports_discover`, `current_schema`, …). Grouping these into coherent,
  documented capability sets reduces the cognitive load without breaking object
  safety. Design in [RFC 0013](../rfcs/0013-structural-core-guarantees.md)
  (#876), which supersedes RFC 0001.
- **Structural core guarantees.** Governance assembled by core for every entry
  point, mandatory I/O primitives, row accounting, one trust resolver, typed
  state keys and per-family engines (SQL, remote files, value fidelity), so a
  connector cannot bypass masking, policy, budgets or exactly-once by omission.
  Design and phasing in [RFC 0013](../rfcs/0013-structural-core-guarantees.md).
- **Connector SDK ergonomics.** Reduce the boilerplate checklist a new connector
  must satisfy (feature flags in three places, `cli/connectors/registry.json`, the CI
  matrix, the docs capability table). Candidate direction: a proc-macro or code
  generator that derives the wiring. See
  [Extensibility](./architecture/extensibility.md) and
  [Connector authoring](./contributing/connector-authoring.md).

## Medium term — grow the platform surface

Work that extends the framework's reach, each gated on an accepted RFC.

- **Plugin-system maturity.** The CLI already supports compile-time plugin
  registration via `PluginRegistry` (`cli/src/registry.rs`). The next step is a
  documented, stable SDK contract for out-of-tree connectors — and, longer term,
  the question of dynamic loading. Design in
  [RFC 0003](../rfcs/0003-plugin-system.md).
- **Arrow / columnar record path.** The `serde_json::Value` record model
  ([ADR 0004](./adr/0004-json-record-model.md)) is ergonomic but allocation-heavy.
  An optional Arrow-backed batch path would cut allocation and unlock zero-copy
  hand-off to columnar sinks (Parquet, Iceberg, BigQuery) without replacing the
  JSON model for the connectors that prefer it. Design in
  [RFC 0002](../rfcs/0002-arrow-support.md).
- **Benchmark-suite hardening.** `BENCHMARKS.md` and the scenario scripts exist;
  the direction is a repeatable, CI-adjacent suite that guards against throughput
  regressions on the hot `run_stream` path. See
  [Performance](./architecture/performance.md).
- **Connector certification.** A conformance test kit — driven by the connector
  spec ([FCP v0](./spec/faucet-connector-spec-v0.md)) — that any first- or
  third-party connector can run to prove it honours the streaming, checkpoint,
  and error contracts. This is the quality gate that makes a marketplace
  trustworthy.

## Long term — architectural bets

Directional bets that depend on medium-term work landing first. These are
explicitly speculative.

- **Zero-copy execution.** Building on the Arrow path, a batch representation that
  flows from a columnar source to a columnar sink without per-record
  materialization. Depends on [RFC 0002](../rfcs/0002-arrow-support.md).
- **Dynamic plugins.** Loading connectors that were not compiled into the binary
  (e.g. via a stable ABI or a WASM boundary). This is a hard problem in Rust and
  is deliberately downstream of a stable compile-time SDK. Open question in
  [RFC 0003](../rfcs/0003-plugin-system.md).
- **Richer streaming runtime.** Windowing, ordering guarantees beyond per-page,
  and multi-source joins push against the current single-source → single-sink
  page model. Whether this belongs in `faucet-core` or a separate runtime crate
  is itself a design question. Exploration in
  [RFC 0004](../rfcs/0004-streaming-improvements.md); the shared runtime
  primitives first proposed in RFC 0005 are now part of
  [RFC 0013](../rfcs/0013-structural-core-guarantees.md).

---

## What is deliberately *not* on the roadmap

- **Replacing `serde_json`** as the default record model. Arrow is additive, not a
  replacement — the JSON model stays for the many connectors and transforms that
  are natural over `Value`.
- **Moving orchestration into `faucet-core`.** Scheduling, the HTTP control plane,
  clustering, and DAG execution stay in the CLI layer by design
  ([ADR 0010](./adr/0010-pipeline-runtime.md)).
- **New source/sink connectors as an architectural priority.** Connector breadth
  is tracked in the epic, not here; this roadmap is about the *framework*.

---

## Related

- [docs/architecture/roadmap.md](./architecture/roadmap.md) — subsystem-level direction
- [Community roadmap](./community/roadmap.md) — feature/connector planning
- [RFC index](../rfcs/README.md)
- [Architecture review](./architecture-review.md) — the risks this roadmap addresses
- [Stability policy](./stability.md)
- [Engineering principles](./engineering-principles.md)
