# Authoring a connector

faucet-stream is designed as an ecosystem: third parties can publish their own
`faucet-source-*` / `faucet-sink-*` crates with minimal friction. **`faucet-core`
is the only required dependency** — it re-exports everything a connector author
needs (`async_trait`, `serde_json`, `schemars`).

## Scaffold it in one command

Don't hand-assemble the crate — generate one that already follows every
convention below:

```bash
faucet new connector acme --kind source        # → faucet-source-acme/
faucet new connector acme --kind sink --common  # also emit faucet-common-acme/
```

The generated crate has the standard module layout (`config.rs`, `stream.rs` /
`sink.rs`), a `JsonSchema`-deriving config, the `config_schema()` /
`connector_name()` overrides, the `#![cfg_attr(docsrs, feature(doc_cfg))]`
crate-root line, the `[package.metadata.docs.rs]` block, system-name-first
crates.io keywords, a README, a passing unit test, and `tests/conformance.rs`
wired to the [`faucet-conformance`](../reference/conformance.md) battery (a
`faucet-conformance` dev-dependency) — so `cargo test` is green immediately with
a trivial passthrough, and `cargo test --test conformance` runs the SDK-contract
checks. Add checks to that file as the connector grows (bounded memory and
bookmark round-trips for a source; truthful capabilities, write modes and
idempotent replay for a sink). `faucet conformance` scores only connectors
compiled into a faucet binary, so it does not apply to a standalone crate. Replace the `TODO`s with your real
config fields and I/O, then publish. The rest of this page explains what the
scaffold sets up.

To make your published connector usable from a `faucet.yaml` config (not just
from Rust), see
[Custom binaries with third-party connectors](https://github.com/faucet-hq/faucet-stream/blob/main/cli/README.md#custom-binaries-with-third-party-connectors).

## The traits

Implement `Source` or `Sink`. Both are object-safe (`Box<dyn Source>` works) and
all newer methods have defaults, so a minimal connector is small.

```rust,ignore
use std::collections::HashMap;

use faucet_core::{async_trait, Source, Sink, FaucetError, Value};

struct MySource { /* reusable client/pool created in new() */ }

#[async_trait]
impl Source for MySource {
    // Primary entry point. (`fetch_all()` is a provided convenience.)
    // `context` carries parent-record values for `${parent.path}` placeholders.
    async fn fetch_with_context(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        todo!("fetch records from your system")
    }
}

struct MySink { /* reusable client/pool */ }

#[async_trait]
impl Sink for MySink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        todo!("write records to your system")
    }
}
```

Your connector now works with the `Pipeline` and every other connector:
`Pipeline::new(&MySource { .. }, &MySink { .. }).run().await?`.

The trait signatures, every defaulted method and every re-export are on
[docs.rs/faucet-core](https://docs.rs/faucet-core) — open the version your
crate's `Cargo.lock` resolves (`cargo tree -p faucet-core --depth 0`). The
sections below are the contract those methods carry.

### Object safety

The pipeline and the CLI hold connectors as `Box<dyn Source>` /
`Box<dyn Sink>`. Implement only the trait's own methods with their exact
signatures: no generic trait methods, associated types or driver types in a
signature. Generics belong on private helpers; pools and SDK clients live
inside your struct. Keep a constructor that returns `Result<Self, FaucetError>`
and does **no network I/O** — a custom binary registers connectors through a
synchronous factory, and `faucet validate` builds them without contacting
anything. Connect lazily and probe connectivity in `check()`.

Always override `config_schema`, `connector_name` (a short, stable, non-empty
name — it is the `connector` metric label) and `dataset_uri` (the lineage
identity; credential-free — `faucet_core::redact_uri_credentials` strips
userinfo from a URL).

## Crate layout

Follow the same module layout as the built-in connectors:

- **`lib.rs`** — re-export the config + the `Source`/`Sink` type. First line:
  `#![cfg_attr(docsrs, feature(doc_cfg))]` (see below).
- **`config.rs`** — the config struct + sub-enums, deriving
  `Serialize + Deserialize + JsonSchema`. **No I/O here.**
- **`stream.rs`** (source) / **`sink.rs`** (sink) — the one place that performs
  I/O. Create reusable clients/pools in `new()` and store them; never reconnect
  per call.

## Streaming pages and bookmarks

`Pipeline::run` drives `Source::stream_pages` and writes each `StreamPage`
(`records` + optional `bookmark`) to the sink as it arrives, one page at a time,
in order. The default `stream_pages` loads the whole result and chunks it —
correct but unbounded — so a source with a paging primitive (cursor, keyset,
scroll, offset, a consumer) overrides it and yields as it reads, keeping memory
at O(page size). Then have `fetch_with_context` drain that stream, so every
entry point returns the same records.

- The `batch_size` argument is a hint; a page-size field in your config wins
  when set. `batch_size == 0` means "one page with everything" — handle it
  explicitly. Clamp to the backend's request limit; pages smaller than the hint
  are fine, larger are not.
- Decide end-of-data from the backend's own signal (short page, null
  next-cursor), and let an empty first page end cleanly.
- Never emit page N+1 before page N; bookmarks are persisted in emit order.
- Don't hold a `std::sync::MutexGuard` across an `.await` inside the stream (it
  makes the stream `!Send`).
- A record missing its cursor field is an error, never a silent skip — skipping
  would move the bookmark past data.

**Resumable sources** return a valid key from `state_key()` (the CLI may
substitute its own `{pipeline}::{row}` key, so never read it back), receive the
last persisted bookmark in `apply_start_bookmark` before streaming starts, and
attach bookmarks to pages: the final page only for a query whose high-water mark
is known at the end, or every page when replay from a bookmark is deterministic
(keyset on a unique increasing key, CDC) — each page then becomes a checkpoint.
Keep the bookmark a small JSON object, never a record. A stored bookmark of the
wrong shape is `FaucetError::State`, never a silent restart from zero. When the
shape changes in a later release, bump `state_schema()` and teach
`migrate_state()` the step.

## Write, flush, checkpoint

For every page the pipeline writes (`write_batch`, `write_batch_partial` or
`write_batch_idempotent`), then calls `flush()`, then persists the page's
bookmark. The bookmark never moves ahead of durable data, which asks of you:

- **Sources** never persist, ack, commit offsets or delete source data
  themselves. Emit the position on a page and let the pipeline persist it after
  the flush. A backend that needs an ack acks when the next page is polled (the
  pipeline polls only after the previous page is durable) and reports
  `consumes_destructively() -> true`, so previews and dry runs refuse it.
- **Sinks** make everything passed to `write_batch` so far durable and readable
  when `flush()` returns `Ok`. A write-through sink keeps the no-op default; a
  buffering sink (file writer, multipart upload, client-side batch) commits in
  `flush`. The pipeline also flushes on cancellation, so `flush` must be safe to
  call at any time, including with nothing buffered.
- The crash window between flush and checkpoint is deliberate: the page is
  re-read on the next run. Default delivery is at-least-once, so every sink must
  tolerate seeing a page twice. An empty slice is a cheap `Ok(0)`.

**Per-row outcomes.** Override `write_batch_partial` when the backend reports
per-row results or a row is invalid before sending (a missing key): one outcome
per input row, in order; an outer `Err` means the whole call failed. Declare
`batch_atomicity()` truthfully — `Atomic` only if a failed write commits
nothing — and if `write_batch` splits a page into several requests to respect
a backend limit, it is partially committed on failure, so it stays best-effort.

### Effectively-once

Two mechanisms remove duplicates across retries and resumes
([Exactly-once delivery](../cookbook/state.md#effectively-once-delivery) is the user's view):

| Mechanism | Sink | Source |
|---|---|---|
| Keyed upsert | flatten `faucet_core::WriteSpec` into the config, list `Upsert`/`Delete` in `supported_write_modes()`, return `self.config.write.dedups_by_key()` from `dedups_by_key()`, route rows through `faucet_core::plan_writes`, and genuinely converge on the key | nothing |
| Atomic watermark | `supports_idempotent_writes() -> true`; `write_batch_idempotent` commits the rows **and** the token under `scope` in one atomic unit (one transaction, one object commit, one producer transaction); `last_committed_token` reads it back | deterministic replay (`supports_exactly_once() -> true`) |

Store the token verbatim and never parse it — it may carry a bookmark only the
pipeline decodes. Two separate writes do not qualify. If you can't do all of
this, leave the capability `false`: an honest `false` makes the CLI refuse
`delivery: exactly_once` at load time, a false `true` corrupts data. The same
goes for every `supports_*` probe and for `supported_write_modes()` — the CLI
gates configs on them. A sink that lists `Upsert` but can't apply a
`delete_marker` rejects it as a `Config` error rather than ignoring it.

## Retries

- **Reads and other idempotent calls** inside your connector retry transient
  failures with `faucet_core::execute_with_retry`, which retries only errors
  whose `is_retriable()` is true, with capped exponential backoff and jitter.
  Expose the retry count in config.
- **Sink writes** are retried by the pipeline (under a `resilience:` policy),
  and a plain `write_batch` only when `write_batch_is_replay_safe()` is true —
  by default that follows `dedups_by_key()`. `supports_idempotent_writes()` does
  not make a plain `write_batch` retryable. Override `write_batch_is_replay_safe`
  only when a replayed write converges by construction (every write a keyed
  `PUT`).
- Never retry a non-idempotent write inside your own `write_batch`: if the
  server committed and the response was lost, the retry duplicates every row.
  Let the error propagate; the page is replayed from the last checkpoint.

## Make it fast

Performance is the project's first principle, and none of this may trade away
the ordering rules above:

- Build HTTP clients, pools and producers once in `new()` and store them; never
  per call, page or record. Bound database pools with a `max_connections`
  field.
- Use bulk APIs: multi-row `INSERT … VALUES (…), (…)` or `COPY` per batch inside
  a transaction, a bulk HTTP endpoint per chunk, pipelining for key-value
  stores. One statement per record is a defect. Make the per-request limit a
  config field with a safe default.
- Bound parallel I/O (`buffer_unordered(concurrency)` or a semaphore) and expose
  `concurrency`; concurrency must never reorder emitted pages.
- Put blocking or CPU-heavy work (synchronous drivers, encoding, compression)
  on `tokio::task::spawn_blocking`; buffer file and socket writers.
- Don't log or meter per record. The pipeline already records metrics and spans
  for every call; never use record ids, URLs or cursors as labels.

## Config and schema

Implement `config_schema()` so `faucet schema` and `faucet init` work:

```rust,ignore
fn config_schema(&self) -> Value {
    faucet_core::schema_for!(MyConfig).into()
}
```

- Derive `Serialize + Deserialize + JsonSchema` on the config and every nested
  type; doc comments become the schema's descriptions, so write them for users.
- Give optional fields `#[serde(default)]` with safe, bounded defaults, and add
  `#[schemars(with = "String")]` (or the matching type) to custom-serde fields.
- `#[serde(deny_unknown_fields)]` catches typos at `faucet validate`; serde
  can't combine it with `#[serde(flatten)]`, so drop it on a sink that flattens
  `WriteSpec`.
- Auth uses the adjacently tagged shape every built-in uses
  (`auth: { type: bearer, config: { … } }`).
- Validate in the constructor (empty names, zero sizes, bad URLs, unsupported
  modes, `WriteSpec::validate`) and return `FaucetError::Config`, so
  `faucet validate` fails before data moves.
- Depend on `faucet-core` at the major only (`faucet-core = "1"`); a minor floor
  forces a release of your crate whenever core releases. Use its re-exports
  (`async_trait`, `serde_json`, `schemars`, `async_stream`, `Stream`) rather
  than adding them yourself; `serde` and `schemars` are direct dependencies only
  because their derive macros need them in scope.

**Secrets.** Never hardcode a credential, host or URL. Users write `${env:…}`
or a secrets-manager reference and the CLI resolves it before your constructor
runs. Don't derive `Debug` on a struct holding a secret (print `<redacted>`),
and never log a config, auth header or connection string.

## Errors

Map every failure to a `FaucetError` variant. Third-party error types wrap into
`FaucetError::Custom(Box<dyn Error + Send + Sync>)` without losing the chain.
Never `.unwrap()` on anything that can fail at runtime.

The variant decides whether the pipeline retries: `is_retriable()` is true for
transport errors, 5xx and 429 statuses, and rate limits, and false for
everything else. So type a transient failure as one of those (a 503 wrapped in
a plain sink-error string is never retried) and a permanent one as a config,
auth, JSON, state or source/sink error. `FaucetError::sink_status` picks the
right one from an HTTP status. Truncate server error bodies — they can echo
request data — and keep secrets out of every message. `FaucetError` is
`#[non_exhaustive]`; match it with a `_` arm.

## Tests

Unit tests live in a `#[cfg(test)]` module at the bottom of each file and cover
the pure logic: config validation branches, request building, cursor and
bookmark handling (including a missing cursor and a malformed bookmark), error
mapping, capability probes per config, and a `Debug` that hides secrets.
Integration tests in `tests/` use `wiremock` for HTTP backends (a `Respond` impl
can model pagination or a keyed store) and `testcontainers` for databases and
queues: a multi-page read, a resume, a retried 5xx, a non-retried 4xx, a
malformed response, per-row sink failures and every write mode you advertise.
Assert exact outcomes — records, bookmark values, error variants.

## Versioning

Start at `1.0.0`. Adding an optional config field, a defaulted method or a
variant of a `#[non_exhaustive]` enum is a minor release; construct config
types through serde, `new()`, `Default` or builders so struct-literal
construction is not part of your API. Renaming or removing a field, changing a
default's meaning, or changing the bookmark shape without
`state_schema`/`migrate_state` is breaking — keep compatibility
(`#[serde(alias = "…")]`, a bookmark migration) or release a new major. The
scaffold's `[package.metadata.cargo-semver-checks.lints]` block encodes this.

## Self-certify with the conformance battery

A connector becomes **Tier-1 / conformant** by adding a `tests/conformance.rs`
that invokes the reusable `faucet-conformance` battery against the *real*
connector and passing it in CI. That battery **is** the tiering mechanism —
there is no separate scheme. Anything not yet wired into it is Tier-2 (still
useful, usually with its own integration tests — Tier-2 does not mean low
quality).

Add the battery as a dev-dependency. A third-party crate takes it from
crates.io at the same major as `faucet-core`; a connector inside this workspace
uses the workspace entry instead (`faucet-conformance.workspace = true`):

```toml
[dev-dependencies]
faucet-conformance = "1"
```

For a **source**, drive the checks against a live connector:

```rust,ignore
// crates/source/foo/tests/conformance.rs
use faucet_source_foo::{FooSource, FooSourceConfig};

#[test]
fn conformance_config_schema_valid() {
    let source = FooSource::new(FooSourceConfig::new(/* … */));
    faucet_conformance::assert_config_schema_valid(&source);
}

#[tokio::test]
async fn conformance_bounded_memory() {
    // drive a source that yields `total` records in pages of `batch`
    faucet_conformance::assert_bounded_memory(&source, batch, total).await;
}

#[tokio::test]
async fn conformance_errors_not_panics() {
    // a source configured to fail must return Err, not panic
    faucet_conformance::assert_errors_not_panics(&broken_source).await;
}
```

Resumable sources also add `assert_bookmark_roundtrip` (persist a bookmark,
re-run, confirm the stream resumes at exactly that position). For a **sink**,
use `assert_idempotent_replay` and `assert_capabilities_truthful` — both take a
`distinct_count` closure that returns the destination's current row count (for a
real sink, a `SELECT count(*)` against the target table).

A **discoverable source** (one that overrides `supports_discover`) adds
`assert_discover_roundtrips`, an *integration-level* check that proves every
dataset `discover()` reports is genuinely selectable — it deep-merges each
descriptor's `config_patch` onto the base config (the `merge_config_patch`
helper does this), rebuilds the source, and reads it. Run it against the same
live/seeded backend your other checks use:

```rust,ignore
faucet_conformance::assert_discover_roundtrips(&source, |patch| {
    let base = /* the connection config as a serde_json::Value */;
    let merged = faucet_conformance::merge_config_patch(base, &patch);
    let cfg = serde_json::from_value(merged).unwrap();
    async move { Box::new(FooSource::new(cfg).await.unwrap()) as Box<dyn faucet_core::Source> }
})
.await;
```

`assert_cancellation_flushes` covers the flush-completing cancellation contract
(a mid-run `CancellationToken` stops at a page boundary and still flushes) by
driving the real pipeline — most useful for a buffered sink (Parquet footer, S3
multipart) whose output only commits on `flush()`.

**Assert the honest branch.** Where a connector legitimately can't satisfy a
check — an append-only sink has no idempotency mechanism, for instance — don't
skip it: assert the honest behaviour instead. The capability method returns
`false` and the pipeline refuses `delivery: exactly_once`. A passing conformance
run that documents what a connector *cannot* do is exactly the point.

Which checks apply (the [crate docs](https://docs.rs/faucet-conformance) list
each one's exact signature for your version):

| Applies to | Checks |
|---|---|
| every source | config schema valid, connector name non-empty, errors not panics (pass one configured to fail), well-formed `check()` probe |
| a pageable source | bounded memory (`total > batch`), `batch_size = 0` gives a single page |
| a resumable source | bookmark round-trip |
| a discoverable source | discover round-trips (integration-level) |
| every sink | config schema valid, connector name non-empty, capabilities truthful, well-formed `check()` probe |
| a keyed or idempotent sink | idempotent replay, write modes truthful |
| a sink with schema evolution / a declared batch atomicity | schema evolution effective / batch atomicity declared |
| a buffering sink | cancellation flushes (integration-level) |

The sink checks write rows keyed on `"id"` with a `"v"` column, so configure
the sink under test `write_mode: upsert`, `key: ["id"]` for the keyed checks.
They measure row-count deltas through your `distinct_count` closure, so give
each check its own fresh destination (a new mock server, table or container).
`faucet_conformance::doubles` has a counting source and a test sink for testing
your own wrappers.

The full contract is the
[Faucet Connector Protocol (FCP v0)](../spec/faucet-connector-spec-v0.md).

## docs.rs setup

So docs.rs renders your full API with per-feature badges, add to `Cargo.toml`:

```toml
[package.metadata.docs.rs]
all-features = true
rustdoc-args = ["--cfg", "docsrs"]
```

and make the first line of `lib.rs` `#![cfg_attr(docsrs, feature(doc_cfg))]`.

## Naming & publishing

Name crates `faucet-source-<name>` / `faucet-sink-<name>`. If you ship both a
source and a sink for the same system, put shared types (auth, formats) in a
`faucet-common-<name>` crate that both depend on and re-export.

> See any built-in connector (e.g. `faucet-source-rest`) for a reference
> implementation.
