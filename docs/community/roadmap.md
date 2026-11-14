# Roadmap & vision (draft)

> **Draft for a GitHub Discussion.** This is a pasteable seed for a
> `📣 Announcements` / `🗺️ Roadmap` discussion — it is *not* published yet and
> Discussions is not enabled. Edit freely before posting.

## What faucet-stream is

The fastest, most reliable way to move data between two endpoints on a single
machine in Rust — usable **two ways from one codebase**:

1. a config-driven `faucet` CLI that runs YAML/JSON pipelines with no Rust code, and
2. an embeddable Rust library (`Source` / `Sink` / `Pipeline`) you drop into your
   own service.

The number-one design goal for every connector is throughput and reliability
(see [BENCHMARKS.md](../../BENCHMARKS.md) and the
[delivery-guarantee docs](https://faucet-hq.github.io/faucet-stream/cookbook/state/)).

## How we grow — conformance-tiered, not connector-count-racing

Breadth is **not** the goal; a trustworthy core is. Every connector's support
level is defined by one executable contract — the
[`faucet-conformance`](../../crates/conformance) battery
([FCP spec](../spec/faucet-connector-spec-v0.md)):

- **Tier-1 / conformant** — invokes and passes the battery in CI. This set grows
  as connectors wire it in; growing it is more valuable than adding a new Tier-2
  connector.
- **Tier-2** — not yet wired into the battery (most have their own integration
  tests). Not "low quality" — just not conformance-certified yet.

**Priorities, in order:** (1) move more existing connectors to Tier-1; (2)
reliability/correctness on the hot path; (3) runtime/CLI/observability/UX; (4)
new connectors (community-driven — see the "add a connector" issue template).

## Explicit non-goals

- **Not distributed-consensus exactly-once.** faucet delivers *effectively-once*
  (idempotent at-least-once — no duplicates or loss across retries/resumes). There
  is no cross-system two-phase commit. If you need consensus semantics, faucet is
  the wrong layer.
- **Not a general workflow orchestrator.** `faucet schedule` / `serve` cover cron
  and a control-plane, but faucet is not trying to replace Airflow/Dagster/Temporal
  for arbitrary DAGs.
- **Not the broadest connector catalog.** Meltano (600+ Singer taps) and Airbyte
  (350+) win on breadth today, and will for the foreseeable future. faucet bridges
  to Singer taps (experimental) precisely so we don't have to chase that count.
- **Not a heavyweight platform.** No mandatory server, database, or UI. The CLI
  and library stay the primary surfaces; `serve`/UI are opt-in.

## Where help matters most

1. **Independent benchmark reproduction** on your hardware (`make bench`) — one
   confirmation is worth more than a new connector.
2. **Moving a Tier-2 connector to Tier-1** by adding `tests/conformance.rs`.
3. **New connectors** via the FCP spec + authoring guide.
