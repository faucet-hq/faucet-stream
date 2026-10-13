# faucet-conformance

A reusable connector **conformance test battery** for the
[faucet-stream](https://github.com/faucet-hq/faucet-stream) ecosystem. Any
`faucet-source-*` / `faucet-sink-*` crate calls it from its own `tests/` to
prove it upholds the connector contract.

**Passing this battery is the Tier-1 (supported) criterion for a connector.**
There is no separate tiering scheme — a connector is "supported" exactly when it
invokes and passes these checks in CI; connectors that don't are Tier-2
(experimental).

## Checks

Every check ships with both a passing and a `#[should_panic]` failing test in
this crate — a check that cannot fail is worthless.

| # | Function | What it proves |
|---|---|---|
| 1 | `assert_config_schema_valid` | `config_schema()` is a valid, round-tripping JSON Schema |
| 2 | `assert_bounded_memory` | `stream_pages` pages instead of buffering the whole set |
| 3 | `assert_bookmark_roundtrip` | a resumed run picks up *after* the emitted bookmark; strictly fewer records reappear |
| 4 | `assert_idempotent_replay` | atomic-watermark replay (or keyed-upsert) converges to one row per key, no double-writes |
| 5 | `assert_capabilities_truthful` | advertised `supports_*` capabilities match actual behavior (idempotency, schema evolution) |
| 6 | `assert_errors_not_panics` | a failing source surfaces a typed `FaucetError` without unwinding |
| 7 | `assert_write_modes_truthful` | a sink advertising `Upsert`/`Delete` converges by key and removes on delete; missing/null keys are reported as failed |
| 8 | `assert_schema_evolution_effective` | an evolvable sink's `evolve_schema` makes the added column appear in a fresh `current_schema()` |
| 9 | `assert_batch_size_zero_single_page` | a source built with `batch_size = 0` yields the whole result set as one page |
| 10 | `assert_connector_name_nonempty` | `connector_name()` is non-empty (an empty name becomes the `"unknown"` metric label) |
| 11 | `assert_preflight_check_wellformed` | `check()` returns `Ok(CheckReport)` with well-formed probes; a probe failure is a `Fail` probe, not an `Err` |
| 14 | `assert_descriptor_matches_source` / `_sink` | the connector's static `ConnectorDescriptor` answers, from a config alone, exactly what the connector built from that config declares through `capabilities()` (RFC 0013) — the CLI's load-time gates trust the first, the pipeline the second |

### Declared-capability checks (RFC 0013)

Each proves a sink behaves as a capability it declares says it does:

| # | Function | What it proves |
|---|---|---|
| 15 | `assert_partial_outcomes_truthful` | `write_batch_partial` returns one outcome per row and an `Ok` row is in the destination |
| 16 | `assert_reported_rows_truthful` | `write_batch_reported` accounts for every row offered and `accepted` equals what landed |
| 17 | `assert_replay_safe` | a replay-safe plain write, retried after a lost response, does not duplicate rows |
| 18 | `assert_overwrite_lifecycle` | `write_mode: overwrite` stages until commit; an abort leaves the destination as it was |
| 19 | `assert_batch_atomicity_truthful` | a failed write of a batch declared atomic lands no row; a per-row batch lands exactly its `Ok` rows |

### Which checks a connector must run

`faucet_conformance::required::REQUIREMENTS` maps each declared capability (a
descriptor flag, a write mode, a trait-method override, a buffering `flush`, a
declared `BatchAtomicity`) to the assertions that prove it. For the built-in
connectors `scripts/conformance-coverage.py` (CI `Doc counts` job) enforces the
table from the committed `declared-capabilities.json` snapshot, each crate's
`src/` and its `tests/conformance.rs` (or
`crates/interop-tests/tests/<kind>_conformance.rs`). Checks that cannot run in
CI are listed with a reason in `conformance-coverage-exempt.txt` (call-deadline
ones in `call-deadline-exempt.txt`). Regenerate the snapshot after changing a
connector's declared capabilities:

```bash
cargo test -p faucet-cli --features full --test declared_capabilities -- --ignored regenerate
```

A test driving a cloud client whose futures are deeper than the default test
stack runs its body through `block_on_large_stack`.

### Descriptor checks the built-in connectors cannot run offline

Every built-in connector calls check 14 from its `tests/conformance.rs` for
sample configs covering each config-dependent capability. These connectors open
a connection inside `new()`, so they cannot be built without a backend; their
`src/descriptor.rs` unit tests pin every capability for every config branch
instead (and `faucet-cli`'s `registry_capability_parity` test probes them
whenever they do build):

| Connector | Why |
|---|---|
| `faucet-source-postgres`, `faucet-source-mysql`, `faucet-source-mssql` | eager connection pool |
| `faucet-source-oracle`, `faucet-sink-oracle` | connect at construction; need Oracle Instant Client |
| `faucet-source-mysql-cdc`, `faucet-source-mssql-cdc`, `faucet-source-oracle-cdc` | connection preflight in `new()` |
| `faucet-source-mongodb-cdc` | `hello` round trip in `new()` |
| `faucet-sink-postgres`, `faucet-sink-mysql`, `faucet-sink-mssql` | eager connection pool |
| `faucet-sink-redis` | connection manager opens in `new()` |

The Spanner source and sink run check 14 against the emulator container, like
their other conformance checks.

### Integration-level checks

These drive a **live backend** or the **real pipeline**, so they belong in a
connector's testcontainers/tempfile conformance test rather than against the
synthetic doubles:

| # | Function | What it proves |
|---|---|---|
| 12 | `assert_discover_roundtrips` | every `discover()` descriptor is genuinely selectable — deep-merge its `config_patch` (via `merge_config_patch`), rebuild, and read |
| 13 | `assert_cancellation_flushes` | a mid-run cancel stops at a page boundary and flushes the sink, so buffered output survives (#146 H16) |

## Usage

```rust
use faucet_conformance as conf;

#[test]
fn config_schema_is_valid() {
    let source = MySource::new(/* … */);
    conf::assert_config_schema_valid(&source);
}

#[tokio::test]
async fn streams_with_bounded_memory() {
    let source = MySource::over_n_rows(10_000);
    conf::assert_bounded_memory(&source, 500, 10_000).await;
}
```

Sinks use the value form of check 1:
`conf::assert_config_schema_valid_value(&sink.config_schema(), sink.connector_name())`.

## Reliability suites

`tests/reliability_*.rs` run the real `Pipeline::run` against scripted doubles
to pin the engine's guarantees — bookmark ordering, exactly-once, lifecycle,
cleanup safety, the DLQ boundary, panic isolation, state migration and
(`reliability_governance`) the governed DLQ, row accounting and per-row policy
quarantine on every write path (RFC 0013). See the
[reliability testing guide](../../docs/book/src/operations/reliability-testing.md).

## Forwarding harnesses (for wrappers)

A wrapper (a source or sink that holds another one) must forward every
defaulted hook, or the feature behind it turns off for everything it wraps.
Write wrappers with `faucet_core::forward_source!` / `forward_sink!`, then prove
the result with `faucet_conformance::forwarding`:

```rust
use faucet_conformance::forwarding::{HookSink, assert_sink_forwards_every_hook};

#[tokio::test]
async fn my_wrapper_forwards_every_hook() {
    let probe = HookSink::default();
    let wrapped = MyWrapper::new(Box::new(probe.clone()));
    // List the hooks the wrapper deliberately does not forward.
    assert_sink_forwards_every_hook(&wrapped, &probe, &["load_native"]).await;
}
```

`HookSource` / `HookSink` answer every hook differently from the trait default
and record which hooks were reached; `assert_source_forwards_every_hook` /
`assert_sink_forwards_every_hook` call every hook through the wrapper and name
the ones that never reached the inner connector. A name in the skip list that
is not a hook fails the assertion, so the list cannot go stale. `SOURCE_HOOKS`
and `SINK_HOOKS` are the canonical hook lists; a test in this crate checks them
against the trait definitions, so a hook added to either trait without being
added to the harness fails CI.

## Call deadlines and cancellation

A connector that runs its backend calls through `faucet_core::io::CallContext`
(#880) proves it with `faucet_conformance::calls`: point the connector at a
backend that never answers (a wiremock server with a long `set_delay`) and
call `assert_source_call_deadline(|| make_source())` or
`assert_sink_call_deadline(|| make_sink(), &records)`. Each installs a context
with a short deadline, then one with a cancel token, and fails if the read or
write does not end within `CALL_BOUND`.

`faucet_conformance::hung::StallingBodyServer` answers HTTP requests through a
route closure and can stop a response body mid-stream
(`Reply::stall_body`): the same assertions then prove the connector reads the
body under the context too (`CallContext::reader`), not only the request.

## Doubles

`faucet_conformance::doubles` provides a `CountingSource` (lazily emits N
synthetic records in pages — genuinely bounded) and a `TestSink` (append or
keyed/upsert recording sink) for use in your own tests.

## Starting test containers

With the `containers` feature, `faucet_conformance::containers` starts a
`testcontainers` backend the same way in every integration test: a 180 s
start-up budget, up to three attempts for start-up failures only (logged, with
backoff, the failed container dropped first), an optional readiness probe
(TCP, HTTP or a custom check) and the `FAUCET_REQUIRE_BACKENDS` rule — skip
locally, fail where the backends are required.

```toml
[dev-dependencies]
faucet-conformance = { version = "1", features = ["containers"] }
```

```rust,ignore
use faucet_conformance::containers::{self, ReadyProbe, StartOptions};

let opts = StartOptions::default().ready(ReadyProbe::tcp(6379));
let Some(redis) = containers::start_or_skip(Redis::default, &opts).await else {
    return;
};
```

## License

Licensed under either of Apache-2.0 or MIT at your option.
