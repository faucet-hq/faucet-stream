# Connector conformance & tiers

Every faucet connector — built-in or third-party — can be graded against the
faucet connector contract and its capabilities. The grade is a **conformance
score** (0–100) that maps to a **maturity tier**, so you can answer *"can I bet a
pipeline on this connector?"* at a glance, and connector authors know exactly
what to improve to level up.

Compute it any time with [`faucet conformance`](./cli.md#conformance).

## Tiers

| Tier | Badge | Score | Meaning |
|------|:-----:|:-----:|---------|
| **Stable** | 🟢 | ≥ 70 | Production-ready: registered, a complete config schema, documented. |
| **Experimental** | 🟡 | 45–69 | Works, but missing some of the contract (e.g. no config schema yet). |
| **Beta** | 🟠 | 20–44 | Early — only partial contract coverage. |
| **Draft** | ⚪ | < 20 | Scaffolded / incomplete. |

The tier is **advisory** — a legitimately-early connector can still ship. It is
not a merge blocker unless you opt in with `faucet conformance --min-tier <tier>`
in CI.

## How the score is computed

The score is deterministic and **instantiation-free** — it reads authoritative
signals the CLI already tracks (the registry index and each connector's
trait-reported capabilities), so it can never drift from the code.

| Dimension | Points | What it checks |
|-----------|:------:|----------------|
| Registered & verified | 40 | a `verified` entry in `cli/connectors/registry.json` |
| Config schema | 30 | `config_schema()` returns a real, non-empty object schema |
| Documented | 10 | a one-line description in the connector catalog |
| Exactly-once delivery | 10 | source: deterministic replay from a bookmark · sink: atomic-watermark idempotent writes |
| Dataset discovery *(source)* | 10 | `faucet discover` introspects the catalog |
| Upsert / mirror *(sink)* | 6 | `write_mode: upsert \| delete` |
| Schema evolution *(sink)* | 4 | evolves the destination schema on drift |

The **core contract** — a verified registry entry (40) + a real config schema
(30) = 70 — is the `Stable` gate. Everything else is a bonus that lifts the score
and adds a capability badge without gating the tier. So every conforming built-in
lands at `Stable`, while an incomplete third-party connector (no verified entry /
no schema) drops to `Experimental` / `Beta`.

## Where the tier surfaces

- **`faucet conformance`** — the full per-connector scorecard (dimension
  pass/fail + points, total, tier, capability badges). `--json` for CI.
- **`faucet list`** — a tier badge next to every compiled-in source and sink.
- **`cli/connectors/registry.json`** — a per-connector `tier` field, validated in
  CI against the computed score so the published catalog stays honest.
- **`faucet new connector`** — the scaffold wires `tests/conformance.rs` (the
  `faucet-conformance` battery, run with `cargo test --test conformance`) and
  names the checks to add as the connector grows. A third-party crate is
  scored by that test; `faucet conformance` scores compiled-in connectors.

## A badge for your crate README

`faucet conformance <name>` prints a shields.io badge URL you can drop into a
third-party `faucet-source-*` / `faucet-sink-*` crate README:

```markdown
![faucet](https://img.shields.io/badge/faucet-stable-brightgreen)
```

## Third-party connectors

Out-of-repo connectors are scored from their **trait-reported capabilities
only** — the repo-scan dimensions (verified registry entry, catalog docs) don't
apply, so a community connector is typically graded on its config schema and the
capabilities it advertises. Publish a PR adding a `verified` registry entry to
have it scored like a built-in.

## The behavioral conformance battery

The score above is a **static** grade (registry entry, config schema, advertised
capabilities). It is complemented by the
[`faucet-conformance`](https://crates.io/crates/faucet-conformance) crate — a
**runtime** test battery a connector calls from its own `tests/` to prove it
actually upholds the contract, not just advertises it. Every check ships with a
passing *and* a `#[should_panic]` failing test in the battery, so no check can
be vacuous.

Checks 1–11 run against synthetic in-memory doubles or a single connector
instance (config-schema validity, bounded-memory paging, bookmark resume,
idempotent replay, truthful capabilities/write-modes, effective schema
evolution, `batch_size = 0` single-page, non-empty `connector_name`, well-formed
`check()` probes). Two more are **integration-level** — they need a live backend
or the real pipeline, so they live in a connector's testcontainers/tempfile
test:

- **`assert_discover_roundtrips`** *(discoverable sources)* — every dataset
  `discover()` reports is genuinely selectable: deep-merge its `config_patch`,
  rebuild the source, and read it. Adopted by all 11 catalog-backed sources
  (postgres, mysql, mssql, sqlite, mongodb, elasticsearch, bigquery, snowflake,
  spanner, s3, gcs).
- **`assert_cancellation_flushes`** — a mid-run `CancellationToken` stops at a
  page boundary and flushes the sink, so buffered output (a Parquet footer, an
  S3 multipart) survives cancellation rather than being orphaned
  ([ADR 0011](https://github.com/faucet-hq/faucet-stream/blob/main/docs/adr/0011-cooperative-cancellation.md)).

One more check pins what the CLI checks a config against:

- **`assert_descriptor_matches_source` / `_sink`** — the connector's static
  `ConnectorDescriptor` answers, from a config alone, exactly what the connector
  built from that config declares through `capabilities()`. Every built-in calls
  it from its `tests/conformance.rs`; the few that connect inside `new()` pin
  their descriptor in unit tests instead (listed in the `faucet-conformance`
  README). See [Declaring capabilities](../extending/authoring-connectors.md#declaring-capabilities).

Five more prove a sink behaves as a capability it declares says it does
(RFC 0013):

| Check | Proves |
|---|---|
| `assert_partial_outcomes_truthful` | `write_batch_partial` returns one outcome per row and every `Ok` row is in the destination |
| `assert_reported_rows_truthful` | `write_batch_reported` accounts for every row offered and `accepted` equals what landed |
| `assert_replay_safe` | a plain write the sink calls replay-safe, retried after a lost response, does not duplicate rows |
| `assert_overwrite_lifecycle` | `write_mode: overwrite` keeps staged rows out of the destination until commit, and an abort leaves it as it was |
| `assert_batch_atomicity_truthful` | a failed write of a batch declared `atomic` lands no row; a `per_row` batch lands exactly its `Ok` rows |

Two more prove a sink's declarations match its lifecycle hooks (RFC 0014). A
sink built on a [family driver](../extending/authoring-connectors.md#start-from-a-family-driver)
passes them by construction; they run on any sink:

| Check | Proves |
|---|---|
| `assert_driver_declarations_truthful` | every lifecycle the sink does not declare (overwrite, exactly-once, cleanup, schema evolution, rollback) is refused by its hook; the declared ones' non-destructive probes answer; keyed dedup comes with a keyed write mode |
| `assert_driver_row_paths_agree` | the plain, reported, per-row and governed (`write_governed`) writes of a valid page each account for every row |

### A declared capability requires its check

`faucet_conformance::required::REQUIREMENTS` maps each declared capability to
the assertions that prove it:

| Requirement | Applies when the connector… | Satisfied by |
|---|---|---|
| `descriptor` | (every source and sink) | `assert_descriptor_matches_source` / `_sink` |
| `idempotent_writes` | declares `idempotent_writes` | `assert_idempotent_replay` or `assert_capabilities_truthful` |
| `keyed_write_modes` | lists `upsert` or `delete` | `assert_write_modes_truthful` |
| `replay_safe` | declares `keyed_upsert`, or overrides `write_batch_is_replay_safe` | `assert_replay_safe` |
| `overwrite` | lists `overwrite` | `assert_overwrite_lifecycle` |
| `schema_evolution` | declares `schema_evolution` | `assert_schema_evolution_effective` |
| `buffered_flush` | overrides `flush` with a body that does work, or uses the shared file writer | `assert_cancellation_flushes` |
| `partial_outcomes` | overrides `write_batch_partial` | `assert_partial_outcomes_truthful` |
| `row_accounting` | overrides `write_batch_reported` | `assert_reported_rows_truthful` |
| `atomic_batches` | declares `BatchAtomicity::Atomic` for some config | `assert_batch_atomicity_truthful` |
| `driver_declarations` | is built on a family driver (holds a `DriverSink<`, or implements `SinkDriver`, `SqlDriver`, `MessageDriver` or `HttpDriver`) | `assert_driver_declarations_truthful` |
| `driver_row_paths` | is built on a family driver | `assert_driver_row_paths_agree` |
| `call_deadline` | (every source and sink) | `assert_source_call_deadline` / `assert_sink_call_deadline` |
| `exactly_once` *(source)* | declares `exactly_once` | `assert_bookmark_roundtrip` |
| `discover` *(source)* | declares `discover` | `assert_discover_roundtrips` |

CI's `Doc counts` job runs `scripts/conformance-coverage.py`, which fails when a
built-in connector declares a capability and none of its conformance tests
(`tests/conformance.rs`, or `crates/interop-tests/tests/<kind>_conformance.rs`
for a check that needs a second connector) calls a matching assertion. It
builds no Rust: the table and every connector's kind-level capabilities come
from the committed `crates/conformance/declared-capabilities.json`, which a
test in the `Test` job keeps in step with the code. Regenerate it after
changing a connector's declared capabilities:

```bash
cargo test -p faucet-cli --features full --test declared_capabilities -- --ignored regenerate
```

A check that cannot run in CI — no emulator exists for the backend, or the
backend's own semantics would only be simulated — is listed in
`crates/conformance/conformance-coverage-exempt.txt` as
`<crate> <requirement>: <reason>` (call-deadline exemptions stay in
`call-deadline-exempt.txt`). An exemption for a requirement that does not apply,
or that the tests already meet, fails the run, so the list cannot go stale.

See [Authoring connectors](../extending/authoring-connectors.md#self-certify-with-the-conformance-battery)
for how to wire the battery into a new connector.

A connector that **wraps** another one (a decorator holding an inner source or
sink) also uses the forwarding harness,
`faucet_conformance::forwarding::assert_source_forwards_every_hook` /
`assert_sink_forwards_every_hook`. It proves every defaulted hook reaches the
wrapped connector. See
[Wrapping another connector](../extending/authoring-connectors.md#wrapping-another-connector).

## Related

- [`faucet conformance`](./cli.md#conformance) — the command
- [Connector catalog](./connectors.md) — per-connector capability matrix
- [API stability policy](https://github.com/faucet-hq/faucet-stream/blob/main/docs/stability.md)
- [Authoring connectors](../extending/authoring-connectors.md)
