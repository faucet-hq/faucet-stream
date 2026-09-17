# Architecture Review

*An objective senior-maintainer review of faucet-stream. Every observation is backed by evidence from the implementation; strengths and weaknesses are reported honestly.*

> **Point-in-time snapshot.** Last reviewed: **2026-07-17**. Suggested cadence:
> re-review each **minor release** (or ~quarterly). This document is a dated
> assessment, not a permanent statement of fact — treat any finding older than
> the last review date with suspicion and verify against the code.
>
> **This review drives work, it is not a graveyard.** Every concrete weakness
> below should map to a tracked GitHub issue or a [roadmap](./roadmap.md) item;
> when a finding is resolved, mark it here on the next review and link the PR. If
> a finding has no issue, filing one is the first action.

This review is written for maintainers deciding where to invest and for
prospective adopters assessing risk. It is deliberately balanced: the project has
unusual strengths for its age, and it also carries real debt. Nothing below is
fabricated — each point cites the code, the workspace shape, or a documented
limitation. Where a risk already has a mitigation planned, the relevant
[ADR](./adr/0001-stream-pages.md) or [RFC](../rfcs/README.md) is linked.

---

## Strengths

- **A single, correct data-integrity protocol, applied uniformly.** The
  write → flush → checkpoint ordering holds identically across all three write
  paths (default, DLQ, exactly-once) in `crates/core/src/pipeline.rs`. This is the
  hardest thing to get right in a data-movement tool, and it is centralized in one
  place rather than reimplemented per connector. See
  [ADR 0002](./adr/0002-checkpoint-ordering.md).
- **Retry safety is treated as a correctness property, not a convenience.** The
  `with_retry_write!` macro refuses to retry a non-idempotent `write_batch` unless
  the sink advertises idempotence — directly preventing the silent-duplication
  bug class that plagues naive ETL tools. See
  [ADR 0007](./adr/0007-retries.md).
- **Bounded memory by construction.** The `StreamPage` model keeps memory at
  O(batch_size) regardless of dataset size (`stream_pages`,
  `DEFAULT_BATCH_SIZE`/`MAX_BATCH_SIZE`). This is a structural property, not a
  tuning parameter. See [ADR 0001](./adr/0001-stream-pages.md).
- **Zero-cost observability.** Automatic instrumentation via the
  `observability/` decorators means every connector is observable without writing
  metrics code, and third-party connectors inherit it for free. See
  [ADR 0008](./adr/0008-observability.md).
- **A genuinely extensible core.** `faucet-core` is object-safe, has one required
  dependency, re-exports what authors need, and grows via defaulted trait methods.
  The compile-time `PluginRegistry` (`cli/src/registry.rs`) lets a third party
  ship a custom CLI. See [Extensibility](./architecture/extensibility.md).
- **Operational maturity beyond its version.** A control plane (`faucet serve`),
  clustering, RBAC + audit log, run history with instance-fenced orphan recovery,
  OpenLineage emission, a data catalog, and an offline fixture test harness
  (`faucet test`) are all present. Few frameworks at this stage have this breadth.
- **High enforced test bar.** The `codecov/patch` gate is a *required* merge check
  at 95% patch coverage, so new code cannot merge under-tested.

---

## Weaknesses

- **`crates/core/src/pipeline.rs` is a 5,471-line file** with a deeply nested
  `run_stream` loop that folds masking, quality, contract, drift, DLQ routing,
  exactly-once, adaptive batching, resilience, and cancellation into one function.
  It is correct and heavily commented, but it is the single hardest file in the
  repo to modify safely, and its size is a barrier to new contributors. The
  layered-pass ordering is load-bearing and only partially guarded by types.
- **The record model allocates.** `serde_json::Value` per record is ergonomic but
  costs allocation and blocks zero-copy hand-off to columnar sinks. This is an
  informed trade-off ([ADR 0004](./adr/0004-json-record-model.md)), not an
  oversight, but it is the dominant performance ceiling on the hot path.
- **Exactly-once and DLQ are mutually exclusive.** The atomic-watermark mechanism
  rejects a configured DLQ (`run_stream` returns `FaucetError::Config`). This is a
  deliberate safety choice for this version, but it is a real functional gap:
  users who want both must fall back to keyed-upsert.
- **Capability discovery is implicit.** A connector's capabilities are a scatter
  of boolean/defaulted methods (`supports_exactly_once`, `is_shardable`,
  `supports_discover`, `supports_schema_evolution`, …). There is no single typed
  capability descriptor, so tooling and docs must enumerate them by hand. See
  [RFC 0001](../rfcs/0001-capability-traits.md).

## Future risks

- **Upstream dependency pins constrain evolution.** `sqlx` is pinned at 0.8 by
  `pgwire-replication`; the OpenTelemetry stack is pinned to the 0.31 line by
  `metrics-exporter-opentelemetry`; Iceberg schema-evolution is blocked on
  `iceberg-rust` 0.9.1. Each pin is a place where the project cannot move until an
  upstream does. These are documented in `CLAUDE.md`, which reduces the surprise
  but not the constraint.
- **Feature-flag combinatorics.** With per-connector features plus aggregate
  features (`full`, `source`, `sink`, `secrets`, `triggers*`, serve/history
  variants), the number of buildable configurations is large. The CI
  feature-isolation matrix guards against feature-unification bugs, but the matrix
  itself is a maintenance surface that must grow with every connector.

## Scalability

- **Vertical (single run):** bounded by O(batch_size) memory and by connector
  throughput; the streaming model scales to datasets larger than RAM cleanly.
- **Horizontal (many runs / one big source):** addressed by cluster Mode A
  (whole-run balancing) and Mode B (source sharding), both riding a SQL-backed
  `RunHistory` with lease-based claiming. This is a real distributed system with
  the attendant complexity (leases, orphan recovery, poison handling) —
  well-implemented but a large surface to keep correct.
- **The `serde_json::Value` allocation cost** is the scaling ceiling for very
  high-throughput single pipelines; the Arrow path
  ([RFC 0002](../rfcs/0002-arrow-support.md)) is the planned relief.

## Technical debt

- The monolithic `run_stream` (above) is the primary technical debt. Extracting
  the per-page pass pipeline (masking → quality → contract → drift) into a typed,
  independently testable stage chain would reduce risk without changing behaviour.
- The exactly-once token format is stringly-typed (`format_token` /
  `format_token_with_bookmark` / `parse_token_parts`); it works and is
  well-tested, but a typed token would be harder to misuse.

## Architectural debt

- **All orchestration lives in the CLI layer** (expand, executor, serve,
  schedule, replicate, backfill, cluster). This keeps `faucet-core` lean
  ([ADR 0010](./adr/0010-pipeline-runtime.md)) — a good decision — but it means a
  library embedder who wants matrix/DAG execution must either reimplement it or
  depend on the CLI crate. If embedding-with-orchestration becomes a real use
  case, some of this may need to move into a reusable runtime crate.
- **Capability model** (above) is architectural debt as much as a weakness:
  formalizing it is [RFC 0001](../rfcs/0001-capability-traits.md).

## Maintenance risks

- **Solo-owner bus factor.** The repository is maintained by a single owner (the
  `faucet-hq` account self-merges under branch protection with
  `enforce_admins: false`). The documentation effort this review is part of
  directly mitigates the knowledge-concentration risk.
- **Docs-sync burden.** Adding a connector requires touching features in three
  places, `cli/connectors/registry.json`, the CI matrix, the docs capability table,
  and the crate README. This is documented as a trigger→update table, but it is
  manual and easy to get partially wrong. SDK-ergonomics work (near-term roadmap)
  targets this.

## Contributor experience

- **Strong onboarding surface:** `CONTRIBUTING.md`, the mdBook, `faucet init` /
  `doctor` / `test`, and now this architecture documentation.
- **Sharp edges:** the size of `pipeline.rs`, the feature-declaration rule (a
  crate must declare every dep-feature it uses or feature-isolation CI fails), and
  the `serde_json::Map` key-order gotcha under `--all-features` are all real
  traps. These are now catalogued in
  [common mistakes](./contributing/common-mistakes.md).

## Testing

- **Bar is high and enforced:** 95% required patch coverage, unit + `wiremock` /
  `testcontainers` integration tests, an offline fixture harness, and property
  tests for resume/checkpoint. The main caveat is that Docker-dependent
  integration tests do not count toward patch coverage, so changed lines must be
  unit-testable — which pushes good design (extract pure logic from I/O).

## Documentation

- **User-facing docs are strong** (the mdBook covers connectors, cookbook,
  reference, operations). The historical gap — now being closed — was
  *internal/architectural* documentation explaining *why*. This review and its
  sibling ADRs/standards are that closure.

## Performance

- The connector-level performance disciplines (client reuse, pooling, multi-row
  INSERT, `buffer_unordered`, bulk APIs, buffered I/O) are consistently applied
  and documented as rules ([Performance standard](./standards/performance.md)).
- Benchmarks exist (`BENCHMARKS.md`) with real scale findings. The gap is a
  CI-adjacent regression guard on the hot path (medium-term roadmap).

## Extensibility

- **Best-in-class for a project this size.** One required dep, object-safe traits,
  defaulted growth, a plugin registry, a connector spec, and a naming convention.
  The remaining step is a conformance/certification kit so third-party connectors
  can *prove* they honour the contracts — planned, not yet present.

---

## Summary

faucet-stream's architecture is unusually disciplined about correctness and
memory, and unusually broad in operational capability for its stage. Its principal
risks are concentration risk (a single monolithic hot-path function, a single
maintainer) and an allocation-bound record model. Both have identified,
non-breaking mitigations already scoped in the ADRs and RFCs referenced above. The
project is in a strong position to grow, provided the near-term consolidation work
(documentation, capability formalization, SDK ergonomics) lands before the
platform surface widens further.

---

## Related

- [Architecture overview](./architecture/overview.md)
- [Design invariants](./architecture/invariants.md)
- [Architectural roadmap](./roadmap.md)
- [RFC index](../rfcs/README.md)
- [Architecture Decision Records](./adr/0001-stream-pages.md)
- [Engineering principles](./engineering-principles.md)
