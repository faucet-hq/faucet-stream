# Architecture roadmap

*The architectural direction of the runtime and SDK — direction, not dated promises.*

This page tracks where the *architecture* is heading. It is deliberately narrow:
connector coverage and product features live in the roadmap epic
([#38](https://github.com/faucet-hq/faucet-stream/issues/38)) and the
[community roadmap](../community/roadmap.md); this page is about the shape of the
core, the SDK, and the record model. The repository-wide phased view is in
[docs/roadmap.md](../roadmap.md).

## Direction

### Capability traits
Today, connector capabilities (exactly-once, sharding, discovery, schema
evolution) are defaulted boolean/methods on the `Source`/`Sink` traits (see
[extensibility](./extensibility.md)). The direction is to formalize these as
first-class, composable capability traits with a conformance story, so the
runtime can reason about a connector's guarantees uniformly. The first step has
landed: connectors declare a `Capabilities` value and a static
`ConnectorDescriptor`, and the CLI gates read those instead of allowlists
([RFC 0013](../../rfcs/0013-structural-core-guarantees.md), which supersedes
RFC 0001).

### Arrow-native record model / zero-copy execution
The record model is `serde_json::Value` ([ADR 0004](../adr/0004-json-record-model.md)),
which is ideal for SDK simplicity and JSON-shaped sources but allocates per field.
The direction is an optional columnar (Arrow) page representation so
Arrow-speaking connectors (Parquet, DuckDB, Arrow Flight) move data zero-copy and
CPU-bound transforms stop paying JSON allocation. This is an additive path, not a
replacement — see [performance](./performance.md) and
[RFC 0002](../../rfcs/0002-arrow-support.md).

### Plugin-system maturity
Custom connectors are registered by compiling them into a custom CLI via
`PluginRegistry`. The direction is a dynamic-loading / stable-ABI story so
connectors can ship independently of a rebuild, without sacrificing the
object-safe, defaulted-method contract that keeps the ecosystem compatible.
Tracked in [RFC 0003](../../rfcs/0003-plugin-system.md).

### Streaming-runtime improvements
The paging model bounds memory but couples checkpoint granularity to page size.
The direction is finer-grained and byte-aware paging plus richer backpressure
signalling between source and sink. See
[RFC 0004](../../rfcs/0004-streaming-improvements.md) and
[RFC 0005](../../rfcs/0005-async-connector-runtime.md).

### Connector certification
As the ecosystem grows, "does this third-party connector honour the contract?"
becomes the key trust question. The direction is a published conformance suite
(paging, checkpoint ordering, cancellation, observability) that a connector can
run to earn a certified badge.

### SDK ergonomics
Continued investment in making the [connector SDK](./connector-sdk.md) the
easiest correct path: better `check()`/doctor scaffolding, sharper compile-time
config validation, and templates that bake in the [performance
disciplines](./performance.md) so authors start fast.

## Non-goals

- No `0.x` crates — everything ships `1.0.0`+.
- No breaking changes to the object-safe `Source`/`Sink` contract; capabilities
  are added additively.
- No coupling of `faucet-core` to specific connectors or heavy dependencies.

## Related

- [Extensibility](./extensibility.md) · [Performance](./performance.md) · [Overview](./overview.md)
- [docs/roadmap.md](../roadmap.md) · [Stability](../stability.md)
- RFCs: [0001](../../rfcs/0001-capability-traits.md) · [0002](../../rfcs/0002-arrow-support.md) · [0003](../../rfcs/0003-plugin-system.md) · [0004](../../rfcs/0004-streaming-improvements.md) · [0005](../../rfcs/0005-async-connector-runtime.md)
