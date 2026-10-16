# faucet-stream vs. Airbyte

*Binary-and-library vs. a platform you operate. Here's the honest trade-off.*

> Reflects each tool as of **2026-07**. Airbyte evolves quickly (OSS + Cloud); check [airbyte.com](https://airbyte.com/) for current details.

## The short version

**Airbyte** is a data-integration **platform** with a **350+** connector catalog, a web UI, an API, a scheduler, and a managed Cloud option. Each connector runs as its own container; you operate the platform (Docker/Kubernetes) or pay for Cloud. It's a strong fit when **non-engineers need a no-code UI** and **connector breadth** is paramount.

**faucet-stream** is the opposite shape: **a single binary (or an embeddable library)** you run to completion — no platform to stand up, no per-connector containers, no daemon to babysit — with **governance built into the movement path**.

## Where faucet-stream is different

- **Nothing to operate.** `brew install`, run a YAML file, done. No control-plane deployment, no container registry per connector, no orchestrator to keep alive. A pipeline is a process that starts, moves data, and exits.
- **Footprint & throughput.** A native Rust binary streams with bounded memory (a 1M-row move in **20 MiB**, flat as the row count grows); there's no container-per-connector overhead or JSON hand-off between processes. See the [benchmarks](https://github.com/faucet-hq/faucet-stream/blob/main/BENCHMARKS.md).
- **Governance in-path.** Quality checks, versioned contracts, **PII masking before any sink sees a row**, schema-drift policy, OpenLineage lineage + catalog, and SLAs are native — not a separate enterprise tier.
- **Embeddable.** Compile the engine into your own Rust service via typed traits; Airbyte is a platform you call, not a library you link.

## Where Airbyte is the better choice

- **Connector catalog.** 350+ connectors, plus a low-code connector builder. faucet has **58** first-party connectors.
- **A no-code UI for non-engineers.** Airbyte's web app lets analysts add sources, configure syncs, and browse schemas entirely by point-and-click, with a low-code connector builder for new APIs. faucet ships a web console too (`faucet serve` with the `serve-ui` feature — submit and monitor runs, browse connector schemas, and view the data catalog + lineage), but it's an **operations control plane for engineers**, not a no-code sync builder: pipelines are still authored as config.
- **Managed Cloud.** If you'd rather not run anything yourself, Airbyte Cloud is a turnkey option. faucet is self-hosted by design.
- **Maturity & normalization.** A large user base and built-in normalization patterns.

## Side-by-side

| | **faucet-stream** | Airbyte |
|---|---|---|
| Shape | single binary + library | platform (Docker/K8s) or Cloud |
| To run one pipeline | a process that exits | a deployed control plane |
| Connectors | 58, growing | 350+ |
| Per-connector runtime | compiled in | a container each |
| Web console | ✓ ops control plane (`serve`) | ✓ |
| No-code sync builder for non-engineers | ✗ (pipelines are config) | ✓ |
| Governance in-path | ✓ native | partial / paywalled |
| Embeddable as a library | ✓ (Rust) | ✗ |
| License | MIT / Apache-2.0 | ELv2 + MIT |

## When to choose which

- **Choose faucet-stream** for engineer-owned pipelines where performance, a tiny footprint, self-hosting simplicity, embedding, or in-flight governance matter — and your sources/sinks are covered.
- **Choose Airbyte** when many non-engineers need a no-code UI to self-serve syncs, you need the long-tail connector catalog, or you want a managed Cloud.

## See for yourself

- **[Try it in 60 seconds](../getting-started/try-it-locally.md)** — a no-infrastructure local demo (no Docker).
- **[Connector catalog](../reference/connectors.md)** — check coverage first.
- **[Benchmarks](https://github.com/faucet-hq/faucet-stream/blob/main/BENCHMARKS.md)** — methodology and honest caveats.
