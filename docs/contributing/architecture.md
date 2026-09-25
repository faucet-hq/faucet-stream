# Contributor's map of the codebase

*Where everything lives, and the order to read it in when you're tracing a record from config to sink.*

This page orients a new contributor inside the workspace. It is deliberately a
*map*, not a spec — for the "why" behind each subsystem, follow the links into
[`docs/architecture/`](../architecture/README.md). For build/test/PR mechanics,
see the top-level [`CONTRIBUTING.md`](../../CONTRIBUTING.md).

## Workspace shape

faucet-stream is a Cargo workspace of <!--COUNT:crates-->95<!--/COUNT--> crates. They fall into four layers,
and the dependency direction only ever flows downward:

```mermaid
%%{init: {'theme':'base','flowchart':{'curve':'basis','nodeSpacing':50,'rankSpacing':72,'padding':14},'themeVariables':{'fontFamily':'-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif','fontSize':'14px','lineColor':'#a5b4c4','clusterBkg':'#f8fafc','clusterBorder':'#e2e8f0'}}}%%
flowchart TD
    CLI["faucet-cli (cli/)<br/>config · expand · executor · serve · schedule · replicate · backfill"]
    UMB["faucet-stream (faucet-stream/)<br/>umbrella: feature-gated re-exports"]
    CONN["connector crates<br/>crates/source/* · crates/sink/* · crates/common/* · crates/state/* · crates/transform-sql · crates/lineage · crates/auth"]
    CORE["faucet-core (crates/core/)<br/>traits · pipeline · state · error · transforms · observability"]

    CLI --> UMB
    CLI --> CONN
    UMB --> CONN
    CONN --> CORE
    CLI --> CORE
    classDef src fill:#e0f2f1,stroke:#26a69a,stroke-width:1.5px,color:#00695c
    classDef proc fill:#eceff8,stroke:#7986cb,stroke-width:1.5px,color:#303f9f
    classDef store fill:#f3e5f5,stroke:#ab47bc,stroke-width:1.5px,color:#6a1b9a
    class CLI src
    class UMB,CONN proc
    class CORE store
```

The invariant that keeps the ecosystem healthy: **every connector crate depends
only on `faucet-core`.** A connector never depends on another connector, on the
umbrella, or on the CLI. This is what lets a third party publish a
`faucet-source-foo` crate against `faucet-core` alone. See
[extensibility](../architecture/extensibility.md) and the
[connector spec](../spec/faucet-connector-spec-v0.md).

## Which crate holds what

| You're looking for… | It lives in… |
|---|---|
| The `Source` / `Sink` / `StateStore` traits, `FaucetError`, `Pipeline`, `run_stream` | `crates/core/` — see [`traits.rs`](../../crates/core/src/traits.rs), [`pipeline.rs`](../../crates/core/src/pipeline.rs), [`state.rs`](../../crates/core/src/state.rs), [`error.rs`](../../crates/core/src/error.rs) |
| Record transforms, quality, contracts, masking, drift, resilience, idempotency | `crates/core/src/{transform,stage,quality,contract,masking,drift,resilience,idempotency}.rs` (and their sub-modules) |
| Automatic metrics/tracing decorators | `crates/core/src/observability/` |
| A specific connector's I/O | `crates/source/<name>/src/stream.rs` or `crates/sink/<name>/src/sink.rs` |
| A connector's config shape | `crates/{source,sink}/<name>/src/config.rs` |
| Shared config for a source+sink pair | `crates/common/<name>/` (e.g. `faucet-common-kafka`) |
| Config parsing, matrix expansion, execution | `cli/src/{config,expand,executor}.rs` |
| Connector dispatch + plugin registry | `cli/src/registry.rs` |
| The CLI verbs (`run`, `validate`, `doctor`, `test`, …) | `cli/src/commands/` |
| The HTTP control plane | `cli/src/serve/` |
| The connector inventory used by the CLI's `registry_index` test | [`cli/connectors/registry.json`](../../cli/connectors/registry.json) |

The authoritative, always-current crate table is in
`.claude/rules/architecture.md`.

## Read-order: tracing one record end to end

When you need to understand how a record actually moves, read the layers in the
order the data does. Each hop below has a "start here" file:

```mermaid
%%{init: {'theme':'base','flowchart':{'curve':'basis','nodeSpacing':50,'rankSpacing':72,'padding':14},'themeVariables':{'fontFamily':'-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif','fontSize':'14px','lineColor':'#a5b4c4','clusterBkg':'#f8fafc','clusterBorder':'#e2e8f0'}}}%%
flowchart LR
    A["YAML/JSON config"] --> B["cli/src/config.rs<br/>parse + interpolate + secrets"]
    B --> C["cli/src/expand.rs<br/>matrix → ExpandedNode[]"]
    C --> D["cli/src/executor.rs<br/>build source/sink, wire options"]
    D --> E["Pipeline::run<br/>crates/core/src/pipeline.rs"]
    E --> F["run_stream loop<br/>mask → quality → contract → drift → write → flush → checkpoint"]
    F --> G["Source::stream_pages / Sink::write_batch<br/>crates/{source,sink}/*"]
    classDef src fill:#e0f2f1,stroke:#26a69a,stroke-width:1.5px,color:#00695c
    classDef proc fill:#eceff8,stroke:#7986cb,stroke-width:1.5px,color:#303f9f
    classDef sink fill:#e3f2fd,stroke:#42a5f5,stroke-width:1.5px,color:#1565c0
    class A src
    class B,C,D,E,F proc
    class G sink
```

1. **Config** — `cli/src/config.rs` defines `PipelineConfig`; `interpolate.rs`,
   `compose.rs`, and `secrets/` resolve `${…}` references at load time.
2. **Expansion** — `cli/src/expand.rs` turns the config (plus any `matrix:`) into
   a list of `ExpandedNode`s and runs every config-load gate (exactly-once,
   write-mode, quarantine-requires-DLQ). This is where a bad config is rejected
   *before* any I/O.
3. **Execution** — `cli/src/executor.rs` builds the concrete source/sink via
   `cli/src/registry.rs`, wires the `RunStreamOptions`, and drives each node
   under a bounded semaphore.
4. **Core pipeline** — `Pipeline::run` (in [`pipeline.rs`](../../crates/core/src/pipeline.rs))
   resolves the bookmark, then calls `run_stream`. This is the heart of the
   system; the [pipeline](../architecture/pipeline.md) and
   [stream-pages](../architecture/stream-pages.md) docs explain it in full.
5. **Connector I/O** — the source's `stream_pages` yields
   [`StreamPage`](../architecture/stream-pages.md)s; the sink's `write_batch`
   commits them.

The single most important thing to internalize before touching the write path:
**a page's bookmark is persisted only *after* the sink has durably written and
flushed that page** (write → flush → `StateStore::put`, in `run_stream`). This is
the ordering that makes crashes safe. See
[checkpoint-ordering ADR](../adr/0002-checkpoint-ordering.md).

## Where to go next

- Adding a connector → [connector-authoring](./connector-authoring.md)
- Writing tests → [testing](./testing.md)
- Making it fast → [performance](./performance.md)
- Something's broken → [debugging](./debugging.md)
- Avoiding the classic traps → [common-mistakes](./common-mistakes.md)

## Related

- [Architecture overview](../architecture/overview.md)
- [Execution model](../architecture/execution.md)
- [Pipeline runtime](../architecture/pipeline.md)
- [Extensibility & plugins](../architecture/extensibility.md)
- [Engineering principles](../engineering-principles.md)
- [Top-level CONTRIBUTING.md](../../CONTRIBUTING.md)
