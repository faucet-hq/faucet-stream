# FAQ (draft)

> **Draft for a GitHub Discussion / docs page.** Pasteable seed for a `❓ Q&A`
> discussion — not published yet, Discussions not enabled. Edit before posting.

## How does this compare to Meltano / Airbyte?

Different layer, honest trade-offs:

- **Breadth:** Meltano (600+ Singer taps) and Airbyte (350+) have far larger
  connector catalogs today, and will for a long time. If you need a connector
  faucet doesn't ship and can't quickly write, use them — faucet even
  [bridges to Singer taps](https://faucet-hq.github.io/faucet-stream/) (v0,
  experimental) so you can run an existing tap through faucet.
- **Throughput & footprint:** on single-machine batch throughput faucet is
  roughly **10–80×** faster than a Python Singer runtime with a
  fraction of the memory (see [BENCHMARKS.md](../../BENCHMARKS.md) — CSV→JSONL is
  a best case; sink-bound DB→DB narrows the gap). It's a compiled Rust binary, not
  a Python + subprocess pipeline.
- **Shape:** faucet is a CLI *and* an embeddable Rust library — no mandatory
  server, database, or UI. Airbyte is a platform; Meltano is a Python project
  runner. Reach for faucet when you want a fast, self-contained mover you can
  drop into a service or a container.

## Is it production-ready?

The core (pipeline, streaming, state/bookmarks, DLQ, effectively-once) is stable
and every crate is `1.0.0`+. Maturity varies **by connector**, and we make that
explicit: **Tier-1 / conformant** connectors pass the
[`faucet-conformance`](../../crates/conformance) battery in CI; **Tier-2**
connectors have their own integration tests but aren't conformance-certified yet
(see the [connector matrix](https://faucet-hq.github.io/faucet-stream/reference/connectors.html)).
Pick the tier that matches your risk tolerance, pin versions, and — please —
[reproduce the benchmark](../../BENCHMARKS.md) on your hardware before relying on
the numbers.

## What exactly is the delivery guarantee?

**Effectively-once**, spelled `delivery: exactly_once` in config. Concretely:
*each record is observably applied once* across retries and resumes — this is
**idempotent at-least-once**, achieved two ways:

- **Atomic watermark** — a deterministic (CDC-style) source + a sink that commits
  the page's records *and* a monotonic commit token in one transaction; on resume
  the pipeline recovers the exact position or skips already-committed pages.
- **Keyed upsert** — any source + an upsert-capable sink with a non-empty `key`;
  re-applying a record converges instead of duplicating.

It is **not** distributed-consensus exactly-once — there is no cross-system
two-phase commit or consensus protocol. `faucet validate` prints which mechanism
each pipeline row actually gets. Full detail:
[delivery guarantees](https://faucet-hq.github.io/faucet-stream/cookbook/state.html).

## Can I write my own connector?

Yes — that's the point of the marketplace design. `faucet-core` is the only
required dependency. Read the [FCP spec](../spec/faucet-connector-spec-v0.md) and
the [authoring guide](https://faucet-hq.github.io/faucet-stream/extending/authoring-connectors.html),
scaffold with `faucet new connector`, and add a `tests/conformance.rs` to reach
Tier-1. There's an "add a connector" issue template to claim one.
