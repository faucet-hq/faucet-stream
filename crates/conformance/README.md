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
