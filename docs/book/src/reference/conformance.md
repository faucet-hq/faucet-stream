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
actually upholds the contract, not just advertises it. There are 13 checks; each
ships with a passing *and* a `#[should_panic]` failing test in the battery, so no
check can be vacuous.

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

See [Authoring connectors](../extending/authoring-connectors.md#self-certify-with-the-conformance-battery)
for how to wire the battery into a new connector.

## Related

- [`faucet conformance`](./cli.md#conformance) — the command
- [Connector catalog](./connectors.md) — per-connector capability matrix
- [API stability policy](https://github.com/faucet-hq/faucet-stream/blob/main/docs/stability.md)
- [Authoring connectors](../extending/authoring-connectors.md)
