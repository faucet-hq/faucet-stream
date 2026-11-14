# faucet-stream documentation

*The documentation home — how the project's docs are organized and where to start.*

faucet-stream keeps two complementary bodies of documentation, aimed at two
different readers:

| Body | Audience | Location |
|------|----------|----------|
| **User guide (docs site)** | *Users* running pipelines — installation, tutorials, cookbook, connector catalog, CLI/config reference. | [`site/`](./site/) → published at <https://faucet-hq.github.io/faucet-stream/> |
| **Engineering documentation** | *Maintainers and contributors* — why the system is built the way it is. | this tree (`architecture/`, `adr/`, `contributing/`, `standards/`, `../rfcs/`) |

If you want to *use* faucet-stream, start with the [docs site](./site/src/content/docs/index.md).
If you want to *change* or *extend* it, start here.

## Engineering documentation map

```mermaid
%%{init: {'theme':'base','flowchart':{'curve':'basis','nodeSpacing':50,'rankSpacing':72,'padding':14},'themeVariables':{'fontFamily':'-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif','fontSize':'14px','lineColor':'#a5b4c4','clusterBkg':'#f8fafc','clusterBorder':'#e2e8f0'}}}%%
flowchart TD
    Hub["docs/README.md<br/>(this file)"]
    Hub --> Arch["architecture/<br/>how & why each subsystem works"]
    Hub --> ADR["adr/<br/>the decisions, with rationale"]
    Hub --> Contrib["contributing/<br/>how to work in the code"]
    Hub --> Std["standards/<br/>repo-wide conventions"]
    Hub --> RFC["../rfcs/<br/>forward-looking proposals"]
    Hub --> Principles["engineering-principles.md"]
    Hub --> Review["architecture-review.md"]
    Hub --> Stability["stability.md"]
    Hub --> Roadmap["roadmap.md"]

    Arch -. grounds .-> ADR
    ADR -. evolves into .-> RFC
    Contrib -. enforces .-> Std
    classDef src fill:#e0f2f1,stroke:#26a69a,stroke-width:1.5px,color:#00695c
    classDef proc fill:#eceff8,stroke:#7986cb,stroke-width:1.5px,color:#303f9f
    class Hub src
    class Arch,ADR,Contrib,Std,RFC,Principles,Review,Stability,Roadmap proc
```

### [Architecture](./architecture/README.md)

**New here? Read the [🎓 Beginner's guide](./architecture/learn.md) first** — it
tells the whole architecture as a story, building it up one idea at a time
(connectors → fetch → incremental → streaming → the production extras), and links
into the reference pages as you go. When you have the mental model, switch to the
🏛 architect reference below.

How each subsystem works and the trade-offs behind it. Start with the
[overview](./architecture/overview.md), then the execution spine:
[execution](./architecture/execution.md) →
[pipeline](./architecture/pipeline.md) →
[stream-pages](./architecture/stream-pages.md) →
[state](./architecture/state-management.md) →
[recovery](./architecture/recovery.md) →
[retries](./architecture/retries.md) →
[observability](./architecture/observability.md).
The [design invariants](./architecture/invariants.md) are the load-bearing
rules the whole system depends on.

### [Architecture Decision Records](./adr/)

One record per major decision — Context, Problem, Decision, Alternatives,
Trade-offs, Consequences, Future work. These explain *why* the architecture is
what it is. See especially
[ADR 0002 — checkpoint ordering](./adr/0002-checkpoint-ordering.md), the
data-integrity keystone.

### [Contributing](./contributing/architecture.md)

Practical, code-level guidance:
[a map of the codebase](./contributing/architecture.md),
[authoring a connector](./contributing/connector-authoring.md),
[testing](./contributing/testing.md),
[performance](./contributing/performance.md),
[debugging](./contributing/debugging.md), and a
[common-mistakes catalogue](./contributing/common-mistakes.md). The
build/test/PR mechanics live in the top-level
[`CONTRIBUTING.md`](../CONTRIBUTING.md).

### [Standards](./standards/api-design.md)

Enforceable repo-wide conventions a reviewer can point at:
[API design](./standards/api-design.md),
[error handling](./standards/error-handling.md),
[logging & metrics](./standards/logging.md),
[testing](./standards/testing.md),
[performance](./standards/performance.md),
[state & durability](./standards/state.md).

### [RFCs](../rfcs/README.md)

Forward-looking design proposals for cross-cutting or breaking changes.

### Cross-cutting documents

- [Glossary](./glossary.md) — the canonical vocabulary (bookmark, page, watermark, effectively-once, …).
- [Security model](./architecture/security.md) — how faucet handles credentials and data; the redaction boundary and hardening checklist.
- [Engineering principles](./engineering-principles.md) — the values the code embodies.
- [Architecture review](./architecture-review.md) — a dated, objective critique of the current state.
- [Stability policy](./stability.md) — what's Stable / Experimental / Internal.
- [Roadmap](./roadmap.md) — architectural direction.
- [Releasing](../RELEASING.md) — the maintainer release runbook.

## Relationship to the always-on maintainer rules

The operational rules a maintainer must follow (release flow, coverage gate,
branch protection, docs-sync triggers) live in `CLAUDE.md` and
`.claude/rules/*.md` at the repo root. This tree explains the *architecture*
those rules protect; the two are complementary and should stay consistent.
