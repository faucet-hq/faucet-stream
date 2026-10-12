# RFC 0013 — Structural core guarantees

*Move the generic machinery connectors keep re-implementing into `faucet-core`, and assemble every guarantee in one place, so that a connector or a new entry point cannot skip masking, policy, budgets, exactly-once, retries, timeouts or redaction by omission.*

| | |
|---|---|
| **RFC** | 0013 |
| **Title** | Structural core guarantees |
| **Status** | Discussion |
| **Authors** | faucet-stream maintainers |
| **Related issues** | Phase 1: #875 · #876 · #877 · #878 · #879 · #880 · #881 · #882 · #883 — Phase 2: #884 · #885 · #886 · #887 — Phase 3: #888 — open gaps: #870 · #871 · #872 · #873 · #874 — evidence: #78 · #146 · #264 · #321 · #456 · #466 · #654 · #783 · #789 · #862 — epic #38 |
| **Related ADRs** | [0002 checkpoint ordering](../docs/adr/0002-checkpoint-ordering.md), [0004 JSON record model](../docs/adr/0004-json-record-model.md), [0007 retries](../docs/adr/0007-retries.md), [0010 pipeline runtime](../docs/adr/0010-pipeline-runtime.md) |
| **Supersedes** | [RFC 0001](./0001-capability-traits.md) (draft), [RFC 0005](./0005-async-connector-runtime.md) (draft) |

## Summary

faucet-stream was designed so that the heavy lifting lives in core and a
problem in the field is at most a connector *configuration* issue. The audit
record says otherwise: of 1,451 findings across ten audit and review issues,
46% lived in connector crates, and most of those were generic machinery
(retries, timeouts, value conversion, SQL planning, pagination, state keys)
written again per connector, not protocol semantics. About 180 findings were
a core guarantee being bypassed. This RFC proposes three phases:

1. **Phase 1 (additive, minor releases).** Core assembles governance; capabilities are declared
   by connectors, not by CLI allowlists. Wrappers forward hooks by construction,
   and a declared capability requires its conformance test. Core accounts for
   every row; connectors get mandatory I/O primitives. The server has one
   allow-list trust resolver, state keys are typed and migrated, and one table
   says which features compose with which write paths.
2. **Phase 2 (additive, per family).** Shared engines for SQL sinks, remote
   file sinks and value fidelity, plus one runtime API that serve, MCP and the
   CLI all call.
3. **Phase 3 (next major).** Make an un-governed write unrepresentable: a sealed
   lifecycle trait, a `GovernedPage` typestate, required capabilities, and
   family driver traits over a typed record model.

## Motivation

### Where the defects were

The analysis classified every finding of #78, #146, #264, #321, #456, #466,
#654, #783 (+#670, #324), #789, #862 and the #868 re-review by the layer the
defect lived in:

| Layer | Findings | Share |
|---|---|---|
| connector crates | 665 | 46% |
| CLI orchestration | 257 | 18% |
| `faucet-core` | 216 | 15% |
| serve (control plane) | 130 | 9% |
| `faucet-common-*` | 115 | 8% |
| CI / release / docs | 68 | 5% |

Connector findings are not connector-specific. A finding counts as generic when the same mechanism appears in at least two
connectors with no connector-specific semantics. By that test:
- earlier audits: 58% overall and 71% of critical and high;
- #789: 67% (255 of 381 connector findings fall into 25 recurring patterns);
- #862: 63% of MEDIUM.

What remains is protocol quirks (Oracle RESETLOGS, Kinesis resharding, Delta
deletion vectors), and those are mostly LOW.

### Why the machinery leaked out of core

1. **Core helpers are optional, so adoption is 10–40%.**
   - Retry loops exist in 29 crates; 11 use core.
   - Identifier quoting exists in 27; 9 use `quote_ident`.
   - Decimal and JSON-number handling is hand-written in about 14 places, while `faucet_core::json_numbers` is used by 2–3.
   - Per-call timeouts appear in 77 crates with no core primitive.
   - Re-chunking (23 crates) and concurrency limiting (24) also have none.

   Where a core gate *requires* the helper (shard planning, `create_table`,
   `plan_writes`), adoption is near total.
2. **"Fixed in one backend, not its siblings" is the dominant remediation
   failure.** 40 of the 63 incomplete #789 fixes; the same shape again in the
   #868 re-review. Fixing copied machinery one copy at a time does not
   converge.
3. **Each entry point assembles its own pipeline.** `faucet run` / schedule /
   serve (`cli/src/executor.rs`, the decorator stack around l.2620–2900),
   topology (`crates/core/src/topology.rs`, `cli/src/topology.rs`), DLQ replay
   (`cli/src/dlq_replay/mod.rs`), `verify --repair` (`cli/src/verify/mod.rs`),
   and preview / plan `--sample` / MCP (`preview_sample::mask` at five call
   sites). About 25 #789 findings and most of #456 are a pass one of them forgot.

### How core guarantees get bypassed

Four mechanisms account for nearly all of the ~180 bypass findings:

| Mechanism | Examples |
|---|---|
| **Fail-open defaulted trait hooks**: a connector must implement a contract correctly and nothing checks it | `write_batch_partial` per-row truth, idempotent watermark storage, `batch_atomicity`, `write_batch_is_replay_safe`, `record_covered`; `current_schema() == None` makes the drift pass inert while delta, deprecated parquet and `on_unknown_field: warn` drop columns themselves |
| **Wrappers that don't forward new defaulted methods** | `TransformingSource`, `InstrumentedSink`, `MetadataSink`, `SamplingSink`, `LimitedSink`, `BindContextSource` (today: `capture_resume_position`, `record_table`, `range_digest`, shard hooks) |
| **Parallel assembly paths** | topology masking, preview masking at five sites, replay and repair with budget / cancel / resilience set to `None` (#871) |
| **Trust boundaries enforced per caller with deny-lists** | serve credential guard H-02…H-05 (#862), re-review F1–F4, tenant egress |

Still open today, filed as issues: the DLQ is not a governed destination (#870);
replay and repair run unbudgeted and uncancellable (#871); panic isolation
covers only the row path (#872); policy quarantine is per-row or whole-page
depending on the write path (#873); run errors are persisted unredacted (#874).

### If we do nothing

Every new connector, trait method or entry point adds another place where a
guarantee has to be remembered. The audit cadence shows the cost: each audit
finds the previous fix applied to some copies and not others.

## Guide-level explanation

After Phase 1 a connector author writes I/O and nothing else:

```rust
#[async_trait]
impl Sink for AcmeSink {
    fn capabilities(&self) -> Capabilities {
        Capabilities::builder()
            .write_modes([WriteMode::Append, WriteMode::Upsert])
            .partial_outcomes(true)
            .build()
    }

    fn set_call_context(&self, ctx: CallContext) { self.ctx.install(ctx); } // like set_roundtrip_recorder

    async fn write_batch_partial(&self, rows: &[Value]) -> Result<Vec<RowOutcome>, FaucetError> {
        // deadline, retry, Retry-After, cancel, metering and redaction all come from ctx
        let resp = self.ctx.call("insert", || self.http.post(&self.url).json(rows).send()).await?;
        Ok(decode_outcomes(resp, rows.len()))
    }
}
```

The author does not wrap anything in masking, policy, budget, metering or the DLQ. They don't
add the kind to a CLI allowlist. They don't write a retry loop, a timeout or a forwarding impl. The
crate's `tests/conformance.rs` must call the assertion for every capability
it declared, or CI fails.

A caller (CLI, serve, MCP, a library user) writes:

```rust
let governance = Governance::from_config(&node, &caller)?; // one function, every entry point
Pipeline::new(&source, &sink)
    .with_governance(governance)   // masking … policy … budget … DLQ … in core's order
    .run()
    .await?;
```

A replay path that must not re-mask says so explicitly:
`governance.without(Pass::Masking, "rows are already masked")`. It does not
just leave the field unset.

## Reference-level explanation

Every item below is an issue with a full design, affected files and acceptance
criteria; this section states the shape and the invariant each one buys.

### Phase 1 — additive (minor releases)

| # | Item | Shape | Invariant it buys |
|---|---|---|---|
| #875 | Pipeline-owned governance | `Governance` + one builder that wraps the sink (and the DLQ sink) in the canonical order; `Pipeline::with_governance`; one CLI `governance_for(node, caller)` | every entry point gets every pass, or opts out by name with a reason |
| #876 | Declared capabilities | `Capabilities` (non-exhaustive, builder) via defaulted `Source/Sink::capabilities()` derived from today's methods; a static `ConnectorDescriptor` registered with each factory for load-time gates; delete `UPSERT_SINK_KINDS`, `OVERWRITE_SINK_KINDS`, `CLEANUP_SINK_KINDS`, `ROLLBACK_SINK_KINDS`, `RANGE_READ_SOURCE_KINDS` and the `registry::*_supports_*` lists | gates read what the connector declares; plugins are first-class |
| #877 | Forwarding by construction | `forward_source!` / `forward_sink!`; public forwarding harnesses in `faucet-conformance` for both traits, checked against a canonical hook list | adding a trait method cannot silently break a wrapper |
| #878 | Capability ⇒ conformance | a required-assertion table + `scripts/conformance-coverage.py` in CI | a declared capability is a tested capability |
| #879 | Row accounting | per page: offered = accepted + failed + quarantined + filtered; `FaucetError::RowsUnaccounted`; `write_batch_reported` for sinks that coalesce | "silent green" becomes a failed run |
| #880 | Mandatory I/O primitives | `CallContext::call` (deadline, classified retry, `Retry-After`, cancel, metering, redaction), `faucet_core::http::Client`, `faucet_core::batching`; `clippy::disallowed_methods` in connector crates | one implementation of retry and timeouts, enforced by lint |
| #881 | One trust resolver | `ReferencePolicy` per `BodyOrigin` and tenant; config fields that name files, URLs or commands carry `x-faucet-resource`, checked generically | a new connector field is closed by default for caller-supplied configs |
| #882 | Typed state keys | `StateKey` built from components, one rendering, `KEY_SCHEME_VERSION`, key migration at run start (generalising `legacy_state_ids`); frozen fixtures | a changed key never looks like a first run |
| #883 | Compatibility table | `faucet_core::compat`: (feature, mode) × (path, capability) → supported / disables path / refuse; read by path selection and `expand`; generates the docs page | the same config gives the same guarantee on every path |

Compatibility with the stability contract:
- Every Phase 1 item is a new type, a new defaulted method, or a CLI change,
  all minor bumps under the API-evolution policy.
- `RunStreamOptions` stays frozen (any added field is a major break), so new
  options go on `Pipeline`, as `with_cleanup` already does.
- No existing method changes meaning.

Design invariants ([invariants.md](../docs/architecture/invariants.md)):
- I1–I4 (write → flush → checkpoint) are untouched; #879 adds a check before the checkpoint, not a reordering.
- I5–I6 (masking first, fixed pass order) move from a convention the CLI follows to a property the #875 builder enforces.
- I9 (gates before any run) gains #883.

### Phase 2 — additive, per family

| # | Item | Shape |
|---|---|---|
| #884 | SQL sink engine | `faucet-common-sql`: `SqlDialect` (quoting, escaping, type names, `MERGE` / `ON CONFLICT`, widening, journal SQL, param limit) + `SqlSinkEngine` owning column mapping, `plan_writes`, create-table, drift, overwrite staging, cleanup, rollback, idempotent tokens. postgres / mysql / sqlite / mssql first |
| #885 | Value fidelity | canonical rendering rules (exact decimals and big integers as strings, RFC 3339 timestamps at native precision, base64 binary, NaN/Inf as counted nulls); shared per-family converters; the `faucet-conformance::fidelity` corpus required per connector |
| #886 | Remote file sinks | `StorageBackend` for S3 / GCS / Azure / SFTP; each remote sink becomes `FileSink<Backend>` over the existing `faucet-common-file::FileWriter` |
| #887 | Runtime API | a `runtime` facade (`plan`, `submit`, `run`, `cancel`, `replay_dlq`, `verify`, `rollback`, `trigger_template`) taking a `Principal`; serve handlers, MCP, triggers and change requests become adapters |

Public config types are unchanged; deprecated fields stay as aliases.

### Phase 3 — next major (`faucet-core` 2.0), #888

- **Sealed lifecycle.** `Source` (34 methods today) and `Sink` (39) split into a
  small connector-facing trait of I/O primitives plus a sealed `SourceExt` /
  `SinkExt` only core implements. Core drives overwrite, idempotent commit,
  cleanup, rollback and partial outcomes from the primitives.
- **Typestate.** `GovernedPage` is constructible only by `apply_governance`; the
  sink write methods and the DLQ writer take it, so an unmasked write is a
  compile error.
- **Required capabilities.** `capabilities()` loses its default.
- **Family drivers over a typed model.** `SqlDriver`, `ObjectStoreDriver`,
  `MessageDriver`, `HttpDriver` over an Arrow-typed internal record, building on
  [RFC 0002](./0002-arrow-support.md) and the Phase 2 engines.

Phase 3 needs an explicit, owner-approved major bump and a shim crate adapting
1.x connectors for one major version. It is tracked, not scheduled.

## Drawbacks

- **Migration cost.** #880, #884 and #885 touch most connector crates. Done
  family by family, this is months of work, and each migration risks behaviour
  changes the existing integration suites must catch.
- **Core grows.** An HTTP client, a call wrapper, batching, a compatibility
  table and a trust resolver add surface and dependencies to `faucet-core`. Every
  addition must stay feature-gated or dependency-light to honour "no mandatory
  heavy dependencies in core" (the HTTP client sits behind a feature; the SQL
  engine lives in a common crate, not core).
- **Less freedom for connector authors.** Mandatory primitives and the lint
  constrain how a connector does I/O. A backend with unusual retry semantics
  needs an escape hatch (`#[allow]` with a reason, reviewed).
- **Row accounting can false-positive.** Sinks that legitimately coalesce rows
  must report it; until every sink does, #879 needs a per-sink opt-in period.
- **Two models during Phases 1–2.** Old and new paths coexist until the
  migrations finish, and the allowlists and capabilities must agree in the meantime.

## Rationale and alternatives

- **Keep fixing findings individually (status quo).** Rejected: the audit
  history shows fixes do not converge. A pattern fixed in one copy reappears
  in the next audit in its siblings.
- **Rewrite from scratch around the target design.** A clean-slate design was
  worked through in full:
  - family driver traits and an Arrow-typed model;
  - one plan/graph engine and a plan-time compatibility table;
  - capabilities as types and a governed-batch type;
  - no ambient I/O, a typed resource resolver and core-owned versioned state keys;
  - row conservation, conformance plus deterministic simulation;
  - serve as a client of the runtime;
  - families before breadth.

  The retrofit in this RFC converges on the same design, phased so that
  third-party connectors and the additive semver policy survive. The clean
  slate's four ideas the earlier proposals lacked were added here: the
  compatibility table (#883), typed state keys (#882), row conservation (#879)
  and serve behind a runtime API (#887). The rewrite itself is rejected: it
  would break every third-party connector and discard the battle-tested
  connector code at once.
- **Go straight to Phase 3.** Rejected for now. A sealed trait and typestate are
  the only way to make bypasses unrepresentable, but they need the Phase 1–2
  engines to exist first. Otherwise every connector has to be rewritten against
  an engine that isn't there yet.
- **Enforce with lints and conformance only, no core engines.** Partially
  adopted (#878, #880's lint), but insufficient alone. A lint can forbid
  `reqwest::Client::new`. It cannot make a hand-written `MERGE` builder handle
  case-folded columns the way the other eight do.

## Prior art

- **Kafka Connect:** connectors supply tasks; the framework owns offsets,
  converters, transforms, the DLQ and retries (`errors.tolerance`), so a
  connector cannot skip them. This is close to #875 + #880.
- **Singer / Meltano:** taps and targets own all I/O and state, and the record
  shows the cost: behaviour differs per tap. faucet's Singer bridge exists
  precisely because of that.
- **Airbyte CDK:** a declarative low-code framework plus a shared HTTP stream base
  class, so most API connectors are configuration. That is the direction of #880
  and of REST discovery recipes.
- **Arrow DataFusion `TableProvider`:** a small capability-declaring trait
  (`supports_filters_pushdown`) the planner reads instead of a central allowlist.
  This is the shape of #876.
- **Diesel / sqlx dialect backends:** one query builder over per-backend
  dialects; the shape of #884.

## Unresolved questions

Must resolve before Accepted:
- The static `ConnectorDescriptor` (#876) needs capabilities before a connector
  is built. Is a pure `fn(&Value) -> Capabilities` enough for config-dependent
  capabilities (a SQL sink's upsert depends on `auto_map`), or does it need the
  parsed config type?
- Does `faucet_core::http` (#880) live in core behind a feature, or in a new
  `faucet-common-http` crate? It hinges on the "keep core lightweight" rule.

Can resolve during implementation:
- The order of family migrations for #880 and #885.
- Whether #879's opt-in period is per sink or global.
- The exact required-assertion table for #878.

## Future possibilities

- A connector SDK generator or proc-macro (roadmap: *Connector SDK
  ergonomics*). It becomes much smaller once capabilities, forwarding and I/O
  are core types.
- Deterministic simulation of whole pipelines (fault injection at every
  `CallContext::call`) once all I/O goes through one wrapper.
- A connector certification badge driven by #878's table.

## Related

- [RFC process](./README.md)
- [Design invariants](../docs/architecture/invariants.md)
- [Roadmap](../docs/roadmap.md)
