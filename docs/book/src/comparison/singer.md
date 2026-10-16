# faucet-stream vs. Singer

*Native connectors vs. the tap/target spec. What you gain, and what you give up.*

> Reflects the ecosystem as of **2026-07**. [Singer](https://www.singer.io/) is an open spec with many runtimes (Meltano is the most common — see [that comparison](./meltano.md) too).

## The short version

**Singer** isn't a tool — it's an open **specification**: *taps* (extractors) and *targets* (loaders) exchange `SCHEMA` / `RECORD` / `STATE` messages as JSON over stdout. Its strength is a **huge, language-agnostic ecosystem** of taps and near-universal recognition.

**faucet-stream** takes the opposite approach: **native connectors compiled into one binary**, exchanging typed records in-process — no per-tap subprocess, no JSON serialization between stages, no Python. Third parties extend it through faucet's own connector protocol (**FCP**) and SDK, not the Singer spec.

**Be clear on one thing:** faucet does **not** run Singer taps directly. This is *native connectors vs. the tap model* — you use faucet's built-in connectors (or write an FCP one), not an existing Singer tap.

## Where faucet-stream is different

- **No inter-process serialization tax.** Singer pipes JSON between a tap process and a target process; faucet moves typed records inside one binary. That, plus native Rust and no Python, is why a 1M-row move runs at **701k rows/s in 20 MiB** ([benchmarks](https://github.com/faucet-hq/faucet-stream/blob/main/BENCHMARKS.md)).
- **One artifact, not a pipeline of processes.** A single static binary vs. a tap + target (+ a runner + Python envs).
- **Governance & delivery guarantees in-path.** Quality, contracts, masking, drift, lineage, SLAs, and effectively-once delivery are part of the engine — the Singer spec covers extract/load messaging, not these.
- **A typed connector contract.** faucet's [FCP protocol](../spec/faucet-connector-spec-v0.md) + SDK give connector authors a documented, versioned surface.

## Where Singer is the better choice

- **Ecosystem breadth.** Hundreds of taps across many vendors and languages. If a specific long-tail source only exists as a Singer tap, that's a real reason to use Singer (via Meltano or another runner).
- **A known, open, language-agnostic spec.** Write a tap in any language; huge prior art and community familiarity.
- **You already run Singer taps** and they work — inertia is a legitimate cost to weigh.

## Side-by-side

| | **faucet-stream** | Singer |
|---|---|---|
| What it is | a runtime + native connectors | a message spec (taps/targets) |
| Process model | one binary, in-process records | tap process → JSON → target process |
| Language | Rust (connectors compiled in) | any (commonly Python) |
| Extensibility | FCP protocol + Rust SDK | the Singer spec |
| Runs existing Singer taps | ✗ (native connectors instead) | ✓ (that's the point) |
| Governance / effectively-once | ✓ native | out of scope for the spec |

## When to choose which

- **Choose faucet-stream** when performance, a single artifact, and in-flight governance matter, and your sources/sinks are covered by native connectors (or worth writing as an FCP connector).
- **Stay with Singer** (via [Meltano](./meltano.md) or another runner) when you depend on a tap that only exists in the Singer ecosystem, or breadth trumps everything.

## See for yourself

- **[faucet-stream vs. Meltano](./meltano.md)** — the concrete runtime comparison.
- **[Connector catalog](../reference/connectors.md)** — what ships natively today.
- **[Authoring a connector](../extending/authoring-connectors.md)** — the FCP + SDK path.
