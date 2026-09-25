#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-conformance
//!
//! A reusable test battery that any faucet connector can call from its own
//! `tests/` to prove it upholds the connector contract. Passing this battery is
//! the **Tier-1** criterion for a connector — there is no separate tiering
//! scheme; a connector is "supported" exactly when it invokes and passes these
//! checks in CI.
//!
//! ```no_run
//! # async fn ex() {
//! use faucet_conformance as conf;
//! let source = /* your Source */
//! #     conf::doubles::CountingSource::new(1000, 100);
//! conf::assert_config_schema_valid(&source);
//! conf::assert_bounded_memory(&source, 100, 1000).await;
//! # }
//! ```
//!
//! The core contract checks:
//! 1. [`assert_config_schema_valid`] — the config schema is a valid JSON Schema.
//! 2. [`assert_bounded_memory`] — the source pages instead of buffering.
//! 3. [`assert_bookmark_roundtrip`] — an incremental source resumes from its
//!    bookmark rather than restarting.
//! 4. [`assert_idempotent_replay`] — re-delivering committed rows leaves no
//!    duplicates (atomic-watermark or keyed-upsert mechanism).
//! 5. [`assert_capabilities_truthful`] — advertised capabilities match real
//!    behaviour.
//! 6. [`assert_errors_not_panics`] — failures surface as a typed
//!    [`faucet_core::FaucetError`], never a panic.
//!
//! Capability-demonstration checks — each proves a connector that *advertises* a
//! capability actually *demonstrates* it:
//! 7. [`assert_write_modes_truthful`] — a sink advertising `Upsert`/`Delete`
//!    genuinely converges by key and removes on delete, and missing/null keys
//!    are reported as failed rather than silently written.
//! 8. [`assert_schema_evolution_effective`] — an evolvable sink's `evolve_schema`
//!    makes the added column appear in a fresh `current_schema()`.
//! 9. [`assert_batch_size_zero_single_page`] — a source built with `batch_size =
//!    0` yields the whole result set as one page.
//! 10. [`assert_connector_name_nonempty`] — `connector_name()` is non-empty (an
//!     empty name becomes the `"unknown"` metric label).
//! 11. [`assert_preflight_check_wellformed`] — `check()` returns `Ok(CheckReport)`
//!     with well-formed probes; a probe failure is a `Fail` probe, not an `Err`.
//!
//! Integration-level checks — these drive a live backend / the real pipeline,
//! so they belong in a connector's testcontainers/tempfile conformance test
//! (not the synthetic unit doubles):
//! 12. [`assert_discover_roundtrips`] — a source's `discover()` descriptors are
//!     genuinely selectable: deep-merge each `config_patch`, rebuild, and read.
//! 13. [`assert_cancellation_flushes`] — a mid-run cancel stops at a page
//!     boundary and flushes the sink, so buffered output survives (#146 H16).
//!
//! Each check has both a passing and a `#[should_panic]` failing test in this
//! crate — a check that cannot fail is worthless.
//!
//! # Reliability suites
//!
//! Alongside the per-connector battery above, two modules support the
//! engine-level reliability program (#651):
//!
//! - [`scripted`] — boundary-precise failure injection plus a shared ordered
//!   [`EventLog`](scripted::EventLog), for asserting the *ordering* guarantees
//!   (bookmark-after-confirm, swap-only-on-success, cancel-still-flushes) that
//!   a pure-logic unit test cannot observe.
//! - [`fidelity`] — the shared typed round-trip corpus and assertion helper, so
//!   every source↔sink pair inherits a type-exactness test instead of
//!   hand-rolling one.

pub mod doubles;
pub mod fidelity;
pub mod scripted;

use std::collections::HashMap;

use faucet_core::{Sink, Source, Value};
use futures::StreamExt;

// ── Check 1: config schema validity ─────────────────────────────────────────

/// Anything that can expose a config JSON Schema + a label — blanket-implemented
/// for every [`Source`] so [`assert_config_schema_valid`] accepts a source
/// directly. (Sinks can be checked via [`assert_config_schema_valid_value`].)
pub trait HasConfigSchema {
    /// The connector's advertised config schema.
    fn conformance_schema(&self) -> Value;
    /// A human label for assertion messages.
    fn conformance_label(&self) -> String;
}

impl<T: Source + ?Sized> HasConfigSchema for T {
    fn conformance_schema(&self) -> Value {
        self.config_schema()
    }
    fn conformance_label(&self) -> String {
        self.connector_name().to_string()
    }
}

/// **Check 1.** Assert the connector's `config_schema()` is a structurally valid
/// JSON Schema that round-trips through `serde_json`.
///
/// Panics (fails the test) on: a non-object schema, a schema with no recognized
/// schema shape, a non-object `properties`, or a serialize→parse→serialize that
/// is not stable.
pub fn assert_config_schema_valid<C: HasConfigSchema + ?Sized>(connector: &C) {
    assert_config_schema_valid_value(
        &connector.conformance_schema(),
        &connector.conformance_label(),
    );
}

/// The value-level core of [`assert_config_schema_valid`] — usable for sinks:
/// `assert_config_schema_valid_value(&sink.config_schema(), sink.connector_name())`.
pub fn assert_config_schema_valid_value(schema: &Value, label: &str) {
    let obj = schema.as_object().unwrap_or_else(|| {
        panic!("[{label}] config_schema() must be a JSON object, got: {schema}")
    });

    // Recognized as *some* JSON Schema shape.
    let recognized = [
        "type",
        "properties",
        "$ref",
        "oneOf",
        "allOf",
        "anyOf",
        "$schema",
        "enum",
    ]
    .iter()
    .any(|k| obj.contains_key(*k));
    assert!(
        recognized,
        "[{label}] config_schema() has no recognizable JSON Schema keyword: {schema}"
    );

    if let Some(props) = obj.get("properties") {
        assert!(
            props.is_object(),
            "[{label}] config_schema().properties must be an object, got: {props}"
        );
    }
    if let Some(ty) = obj.get("type") {
        assert!(
            ty.is_string() || ty.is_array(),
            "[{label}] config_schema().type must be a string or array, got: {ty}"
        );
    }

    // Round-trip: serialize → parse → serialize must be stable.
    let text = serde_json::to_string(schema).expect("schema serializes");
    let reparsed: Value = serde_json::from_str(&text).expect("schema re-parses");
    assert_eq!(
        &reparsed, schema,
        "[{label}] config_schema() does not round-trip through serde_json"
    );
}

// ── Check 2: bounded memory ──────────────────────────────────────────────────

/// **Check 2.** Drive `stream_pages` over a source that yields `total` records
/// and assert the consumer never holds more than ~`batch_size` records live at
/// once (i.e. the source pages instead of buffering everything).
///
/// Requires `batch_size > 0` and `total > batch_size` for a meaningful result.
/// Asserts: every record is streamed (`sum == total`), the largest single page
/// is `<= batch_size`, and strictly `< total` (proving the source did not emit
/// the whole set as one page).
pub async fn assert_bounded_memory<S: Source + ?Sized>(
    source: &S,
    batch_size: usize,
    total: usize,
) {
    assert!(
        batch_size > 0,
        "batch_size must be > 0 for a bounded-memory check"
    );
    assert!(
        total > batch_size,
        "total ({total}) must exceed batch_size ({batch_size}) for a meaningful check"
    );
    let label = source.connector_name();

    let ctx: HashMap<String, Value> = HashMap::new();
    let mut stream = source.stream_pages(&ctx, batch_size);
    let mut seen = 0usize;
    let mut peak = 0usize;
    while let Some(page) = stream.next().await {
        let page = page.unwrap_or_else(|e| panic!("[{label}] stream_pages errored: {e}"));
        peak = peak.max(page.records.len());
        seen += page.records.len();
        // `page` is dropped here — the consumer only ever holds one page.
    }

    assert_eq!(
        seen, total,
        "[{label}] streamed {seen} records, expected {total}"
    );
    assert!(
        peak <= batch_size,
        "[{label}] peak page {peak} exceeds batch_size {batch_size} (not bounded)"
    );
    assert!(
        peak < total,
        "[{label}] peak page {peak} == total: source buffered the whole set into one page"
    );
}

// ── Check 3: bookmark round-trip (resumable sources) ─────────────────────────

/// **Check 3.** Drive an incremental source to completion, capture the bookmark
/// it emits, feed it back via
/// [`apply_start_bookmark`](Source::apply_start_bookmark), and assert the second
/// run resumes *after* that point — strictly fewer records reappear (zero for a
/// fully-consumed static source).
///
/// Only meaningful for a source that actually emits a bookmark and honours it.
/// Panics if the source produces no bookmark (nothing to round-trip), or if the
/// resumed run replays the same volume (the bookmark was ignored).
pub async fn assert_bookmark_roundtrip<S: Source + ?Sized>(source: &S) {
    let label = source.connector_name();
    let ctx: HashMap<String, Value> = HashMap::new();

    // First run: consume every page, remembering how many records we saw and the
    // last non-null bookmark.
    let (first_records, bookmark) = drain(source, &ctx, label).await;
    assert!(
        first_records > 0,
        "[{label}] produced no records — cannot exercise bookmark round-trip"
    );
    let bookmark = bookmark.unwrap_or_else(|| {
        panic!("[{label}] produced no bookmark to round-trip (stream_pages never set one)")
    });

    // Resume from the captured bookmark.
    source
        .apply_start_bookmark(bookmark.clone())
        .await
        .unwrap_or_else(|e| panic!("[{label}] apply_start_bookmark errored: {e}"));

    let (second_records, _) = drain(source, &ctx, label).await;
    assert!(
        second_records < first_records,
        "[{label}] resumed run replayed {second_records} records (first run: {first_records}); \
         the bookmark {bookmark} was ignored — no incremental resume"
    );
}

/// Drive `stream_pages` to completion, returning `(record_count, last_bookmark)`.
async fn drain<S: Source + ?Sized>(
    source: &S,
    ctx: &HashMap<String, Value>,
    label: &str,
) -> (usize, Option<Value>) {
    let mut stream = source.stream_pages(ctx, 100);
    let mut count = 0usize;
    let mut last_bookmark = None;
    while let Some(page) = stream.next().await {
        let page = page.unwrap_or_else(|e| panic!("[{label}] stream_pages errored: {e}"));
        count += page.records.len();
        if page.bookmark.is_some() {
            last_bookmark = page.bookmark;
        }
    }
    (count, last_bookmark)
}

// ── Check 4: idempotent replay (no duplicates on re-delivery) ─────────────────

/// **Check 4.** Assert re-delivering already-committed rows leaves no
/// duplicates in the destination — the trust-critical effectively-once check.
///
/// `distinct_count` returns the number of distinct rows the destination
/// currently holds (for a double, `|| async { sink.len() }`; for a real sink, a
/// `SELECT count(*)`). Records are keyed on the field `"id"`, so a real sink
/// under test must be configured `write_mode: upsert` with `key: ["id"]`.
///
/// Dispatches on the mechanism the sink advertises:
/// - `supports_idempotent_writes()` → the **atomic-watermark** path: writing a
///   page durably records a commit token; a crash-replay (guarded by
///   `last_committed_token`, exactly as the pipeline guards it) does not
///   re-write, and forward progress still advances.
/// - else `dedups_by_key()` → the **keyed-upsert** path: overlapping keys across
///   pages converge to one row each.
/// - neither → panics (the sink advertises no idempotency mechanism to test).
pub async fn assert_idempotent_replay<S, F, Fut>(sink: &S, distinct_count: F)
where
    S: Sink + ?Sized,
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = usize>,
{
    let label = sink.connector_name();
    if sink.supports_idempotent_writes() {
        assert_watermark_idempotent(sink, &distinct_count, label).await;
    } else if sink.dedups_by_key() {
        assert_keyed_convergence(sink, &distinct_count, label).await;
    } else {
        panic!(
            "[{label}] advertises no idempotency mechanism \
             (supports_idempotent_writes=false, dedups_by_key=false) — nothing to verify"
        );
    }
}

/// Build test records keyed on `"id"` with a non-key `"v"` column, so a SQL
/// upsert (`ON CONFLICT(id) DO UPDATE SET v = …`) has something to set — a
/// single key-only column would produce an empty SET clause.
fn rows(ids: &[i64]) -> Vec<Value> {
    ids.iter()
        .map(|i| serde_json::json!({ "id": i, "v": format!("v{i}") }))
        .collect()
}

async fn assert_watermark_idempotent<S, F, Fut>(sink: &S, count: &F, label: &str)
where
    S: Sink + ?Sized,
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = usize>,
{
    let scope = "conformance::idem";
    let before = count().await;

    // Page 1 with the first commit token.
    let t1 = faucet_core::format_token(1);
    let p1 = rows(&[1, 2, 3]);
    sink.write_batch_idempotent(&p1, scope, &t1)
        .await
        .unwrap_or_else(|e| panic!("[{label}] write_batch_idempotent(page 1) errored: {e}"));
    let after_first = count().await;
    assert_eq!(
        after_first - before,
        3,
        "[{label}] first idempotent write did not add all 3 rows"
    );

    // The token must be durably recorded — this is what lets the pipeline skip a
    // replay. A sink that claims idempotency but never persists a token fails here.
    let committed = sink
        .last_committed_token(scope)
        .await
        .unwrap_or_else(|e| panic!("[{label}] last_committed_token errored: {e}"));
    assert_eq!(
        committed.as_deref(),
        Some(t1.as_str()),
        "[{label}] did not durably record its commit token — cannot skip a replay"
    );

    // Crash-replay of page 1: the pipeline compares the page token against the
    // committed token and *skips* the page when already committed. Assert that
    // decision resolves to "skip" — i.e. the sink's recorded token parses and is
    // ≥ the page token. (We deliberately do NOT re-invoke the sink and assert the
    // row count is unchanged: the no-duplication guarantee lives in the pipeline's
    // skip, not in the sink, so an append-mode idempotent sink re-delivered the
    // same committed page legitimately *would* grow. Testing that here would fail
    // correct sinks. This is the vacuous assertion #466 L4 removed.)
    let committed_seq = faucet_core::parse_token(committed.as_deref().unwrap_or_default())
        .unwrap_or_else(|| panic!("[{label}] committed token {committed:?} does not parse"));
    assert!(
        committed_seq >= faucet_core::parse_token(&t1).unwrap_or(0),
        "[{label}] committed token did not reach the written page's token — \
         run_stream could not skip the replay and would re-write the page"
    );

    // Forward progress with a new token still writes.
    let t2 = faucet_core::format_token(2);
    let p2 = rows(&[4, 5]);
    sink.write_batch_idempotent(&p2, scope, &t2)
        .await
        .unwrap_or_else(|e| panic!("[{label}] write_batch_idempotent(page 2) errored: {e}"));
    let after_second = count().await;
    assert_eq!(
        after_second - after_first,
        2,
        "[{label}] forward progress after a new token did not add the new rows"
    );
}

async fn assert_keyed_convergence<S, F, Fut>(sink: &S, count: &F, label: &str)
where
    S: Sink + ?Sized,
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = usize>,
{
    let before = count().await;
    write_and_flush(sink, &rows(&[1, 2, 3]), "page 1").await;
    // Overlapping page: ids 2 and 3 are re-delivered.
    write_and_flush(sink, &rows(&[2, 3, 4]), "overlapping page").await;
    let after = count().await;
    assert_eq!(
        after - before,
        4,
        "[{label}] overlapping keys did not converge: expected 4 distinct rows (ids 1-4), \
         got {}",
        after - before
    );
}

// ── Check 5: capabilities are truthful ───────────────────────────────────────

/// **Check 5.** Assert a sink's advertised capabilities match real behaviour:
/// - `Append` (always supported) actually adds rows;
/// - a declared idempotent/keyed mechanism actually dedups (reuses check 4);
/// - `supports_schema_evolution()` implies `evolve_schema` is callable (not the
///   default "unsupported" error);
/// - the honest-false branch: a non-idempotent sink's `write_batch_idempotent`
///   delegates to `write_batch` and records no token.
///
/// Write a page and make it visible.
///
/// A buffering sink (object-store JSONL since #618, Parquet always) only makes
/// rows readable at `flush` — the trait's contract — so a harness that counts
/// straight after `write_batch` measures nothing on those sinks. `flush` is a
/// no-op for the immediate writers, so this is correct for every sink rather
/// than a special case for some.
async fn write_and_flush<S: Sink + ?Sized>(sink: &S, page: &[Value], what: &str) {
    let label = sink.connector_name();
    sink.write_batch(page)
        .await
        .unwrap_or_else(|e| panic!("[{label}] write_batch({what}) errored: {e}"));
    sink.flush()
        .await
        .unwrap_or_else(|e| panic!("[{label}] flush after {what} errored: {e}"));
}

/// `distinct_count` reports the destination's current distinct-row count.
pub async fn assert_capabilities_truthful<S, F, Fut>(sink: &S, distinct_count: F)
where
    S: Sink + ?Sized,
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = usize>,
{
    let label = sink.connector_name();

    // Every sink must accept Append.
    assert!(
        sink.supported_write_modes()
            .contains(&faucet_core::write_mode::WriteMode::Append),
        "[{label}] does not advertise Append — every sink must support append"
    );

    if sink.supports_idempotent_writes() || sink.dedups_by_key() {
        // The advertised idempotency mechanism must actually work.
        assert_idempotent_replay(sink, &distinct_count).await;
    } else {
        // Honest-false: the default idempotent path must delegate, not pretend.
        let before = distinct_count().await;
        write_and_flush(sink, &rows(&[100]), "append probe").await;
        assert_eq!(
            distinct_count().await - before,
            1,
            "[{label}] Append is advertised but write_batch did not add a row"
        );
        assert_eq!(
            sink.last_committed_token("conformance::honest")
                .await
                .unwrap_or_else(|e| panic!("[{label}] last_committed_token errored: {e}")),
            None,
            "[{label}] is not idempotent yet reports a committed token"
        );
    }

    if sink.supports_schema_evolution() {
        // A no-op evolution must be accepted (idempotent, `ADD … IF NOT EXISTS`
        // semantics) — not the default "does not support" error.
        let empty = faucet_core::drift::SchemaEvolution::default();
        sink.evolve_schema(&empty).await.unwrap_or_else(|e| {
            panic!("[{label}] advertises schema evolution but evolve_schema(no-op) errored: {e}")
        });
    }
}

// ── Check 6: errors, not panics ──────────────────────────────────────────────

/// **Check 6.** Drive a source configured to fail (unreachable endpoint / bad
/// config) and assert it surfaces a typed [`faucet_core::FaucetError`] **without
/// unwinding**. Catches any panic and re-raises it as a check failure, so a
/// connector that `unwrap()`s on bad input is caught rather than crashing the
/// test process silently.
///
/// Pass a source that is *expected to fail*. Panics if the source succeeds (the
/// failure path was not exercised) or if it panics instead of returning `Err`.
pub async fn assert_errors_not_panics<S: Source + ?Sized>(source: &S) {
    use futures::FutureExt;
    let label = source.connector_name();

    // `fetch_all` path.
    let outcome = std::panic::AssertUnwindSafe(source.fetch_all())
        .catch_unwind()
        .await;
    match outcome {
        Err(_) => panic!("[{label}] panicked instead of returning Err from fetch_all"),
        Ok(Ok(_)) => panic!("[{label}] expected a failure but fetch_all succeeded"),
        Ok(Err(_e)) => { /* typed FaucetError, no unwind — good */ }
    }

    // `stream_pages` path — the first poll must also error (typed), not panic.
    let ctx: HashMap<String, Value> = HashMap::new();
    let stream_outcome = std::panic::AssertUnwindSafe(async {
        let mut s = source.stream_pages(&ctx, 100);
        s.next().await
    })
    .catch_unwind()
    .await;
    match stream_outcome {
        Err(_) => panic!("[{label}] panicked instead of returning Err from stream_pages"),
        Ok(Some(Err(_e))) => { /* typed FaucetError on first page — good */ }
        Ok(None) => panic!("[{label}] stream_pages yielded no pages (expected an error)"),
        Ok(Some(Ok(_))) => {
            panic!("[{label}] expected a failure but stream_pages produced a page")
        }
    }
}

// ── Check 7: write modes are truthful ────────────────────────────────────────

/// **Check 7.** Assert a sink advertising `Upsert`/`Delete` in
/// [`supported_write_modes`](Sink::supported_write_modes) genuinely upholds
/// those modes:
/// - **Upsert** — re-writing a record with an existing key converges to one row
///   (last-write-wins), it does not append a duplicate.
/// - **Delete** — a record carrying the standard delete marker
///   ([`DELETE_MARKER_FIELD`](doubles::DELETE_MARKER_FIELD) =
///   [`DELETE_MARKER_VALUE`](doubles::DELETE_MARKER_VALUE), matching the
///   `cdc_unwrap` convention) removes its keyed row. Only run when the sink
///   advertises `Delete`; the sink under test must be configured with a
///   matching `delete_marker`.
/// - **Missing/null key** — [`plan_writes`](faucet_core::write_mode::plan_writes),
///   the shared planner every upsert sink routes through, reports such rows as
///   `failed` (destined for a DLQ) rather than writing them.
///
/// Records are keyed on `"id"`, so the sink under test must be configured
/// `write_mode: upsert` with `key: ["id"]`. A sink advertising only `Append`
/// has nothing to prove and the body is skipped. `distinct_count` reports the
/// destination's current distinct-row count.
pub async fn assert_write_modes_truthful<S, F, Fut>(sink: &S, distinct_count: F)
where
    S: Sink + ?Sized,
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = usize>,
{
    use faucet_core::write_mode::{WriteMode, WriteSpec, plan_writes};

    let label = sink.connector_name();
    let modes = sink.supported_write_modes();
    let has_upsert = modes.contains(&WriteMode::Upsert);
    let has_delete = modes.contains(&WriteMode::Delete);

    if !has_upsert && !has_delete {
        // Append-only sink: nothing to demonstrate.
        return;
    }

    // The instance under test must be *configured* for keyed writes, or there is
    // no upsert/delete behaviour to exercise through the trait.
    assert!(
        sink.dedups_by_key(),
        "[{label}] advertises {modes:?} but dedups_by_key()=false — pass a sink \
         configured `write_mode: upsert` with `key: [\"id\"]` so the mode can be exercised"
    );

    // ── Upsert: same key twice → one row (last-write-wins), not a duplicate. ──
    if has_upsert {
        let before = distinct_count().await;
        // Two distinct keys, then re-write one of them.
        write_and_flush(sink, &rows(&[1, 2]), "upsert seed").await;
        write_and_flush(
            sink,
            &[serde_json::json!({ "id": 1, "v": "updated" })],
            "upsert overwrite",
        )
        .await;
        let after = distinct_count().await;
        assert_eq!(
            after - before,
            2,
            "[{label}] upsert did not converge: re-writing key id=1 left {} distinct rows \
             (expected 2: ids 1 and 2) — it appended a duplicate instead of updating",
            after - before
        );
    }

    // ── Delete: a delete-marked record removes its keyed row. ──
    if has_delete {
        let before = distinct_count().await;
        write_and_flush(
            sink,
            &[serde_json::json!({ "id": 777, "v": "doomed" })],
            "delete seed",
        )
        .await;
        let seeded = distinct_count().await;
        assert_eq!(
            seeded - before,
            1,
            "[{label}] delete precondition failed: the row to delete was not written"
        );
        let mut del = serde_json::Map::new();
        del.insert("id".to_string(), serde_json::json!(777));
        del.insert(
            doubles::DELETE_MARKER_FIELD.to_string(),
            Value::String(doubles::DELETE_MARKER_VALUE.to_string()),
        );
        write_and_flush(sink, &[Value::Object(del)], "delete").await;
        let after = distinct_count().await;
        assert_eq!(
            after, before,
            "[{label}] a delete-marked record did not remove the row: {after} rows remain \
             (expected {before}) — the delete was ignored"
        );
    }

    // ── Missing / null key must be reported as failed, never silently written. ──
    let spec = WriteSpec {
        write_mode: WriteMode::Upsert,
        key: vec!["id".to_string()],
        delete_marker: None,
        rollback: None,
    };
    let plan = plan_writes(
        &[
            serde_json::json!({ "id": 9, "v": "ok" }),
            serde_json::json!({ "no_key": 1 }),
            serde_json::json!({ "id": null }),
        ],
        &spec,
    );
    assert_eq!(
        plan.upserts.len(),
        1,
        "[{label}] the one keyed row should be planned as an upsert"
    );
    assert_eq!(
        plan.failed.len(),
        2,
        "[{label}] plan_writes did not report the missing-key and null-key rows as failed \
         (they would be silently dropped or written): {:?}",
        plan.failed
    );
}

// ── Check 8: schema evolution is effective ────────────────────────────────────

/// **Check 8.** For a sink advertising
/// [`supports_schema_evolution`](Sink::supports_schema_evolution): read
/// [`current_schema`](Sink::current_schema), apply a real add-column
/// [`SchemaEvolution`](faucet_core::drift::SchemaEvolution) via
/// [`evolve_schema`](Sink::evolve_schema), and assert the new column appears in
/// a *fresh* `current_schema()`. Stronger than
/// [`assert_capabilities_truthful`], which only checks a no-op evolve does not
/// error.
///
/// Panics if the sink does not advertise evolution (call it only on an evolvable
/// sink), if `current_schema()` is `None` (nothing to diff against), or if the
/// added column never surfaces (the evolution was a silent no-op).
pub async fn assert_schema_evolution_effective<S: Sink + ?Sized>(sink: &S) {
    use faucet_core::drift::{ColumnChange, SchemaEvolution};

    let label = sink.connector_name();
    assert!(
        sink.supports_schema_evolution(),
        "[{label}] does not advertise schema evolution — call this only on an evolvable sink \
         (assert_capabilities_truthful covers the no-op case)"
    );

    let before = sink
        .current_schema()
        .await
        .unwrap_or_else(|e| panic!("[{label}] current_schema() errored: {e}"))
        .unwrap_or_else(|| {
            panic!(
                "[{label}] advertises schema evolution but current_schema() is None — \
                 cannot verify an added column appears"
            )
        });

    // Must be a valid identifier on every evolvable backend: Spanner rejects a
    // leading underscore ("Column name not valid"), so no `__…__` fencing here.
    let new_col = "faucet_conformance_evolved";
    let already = before
        .get("properties")
        .and_then(|p| p.as_object())
        .is_some_and(|p| p.contains_key(new_col));
    assert!(
        !already,
        "[{label}] test column `{new_col}` already exists in current_schema() — \
         cannot prove evolution added it"
    );

    let evolution = SchemaEvolution {
        additions: vec![ColumnChange {
            name: new_col.to_string(),
            from: None,
            to: serde_json::json!({ "type": "string" }),
        }],
        widenings: Vec::new(),
        relax_nullability: Vec::new(),
    };
    sink.evolve_schema(&evolution)
        .await
        .unwrap_or_else(|e| panic!("[{label}] evolve_schema(add `{new_col}`) errored: {e}"));

    let after = sink
        .current_schema()
        .await
        .unwrap_or_else(|e| panic!("[{label}] current_schema() errored after evolve: {e}"))
        .unwrap_or_else(|| panic!("[{label}] current_schema() became None after evolve_schema"));
    let after_props = after
        .get("properties")
        .and_then(|p| p.as_object())
        .unwrap_or_else(|| {
            panic!("[{label}] current_schema() has no `properties` object after evolve: {after}")
        });
    assert!(
        after_props.contains_key(new_col),
        "[{label}] evolve_schema reported success but the added column `{new_col}` does not \
         appear in a fresh current_schema() — the evolution was not effective: {after}"
    );
}

// ── Check 9: batch_size=0 emits a single page ─────────────────────────────────

/// **Check 9.** Drive a source **built with `batch_size = 0`** and assert it
/// yields the entire result set as a single [`StreamPage`](faucet_core::StreamPage)
/// — the documented "no batching" sentinel (small lookup tables, sinks that
/// prefer one large request).
///
/// Asserts the source produced at least one record and that exactly one page
/// carried records (a trailing empty terminal page carrying only a bookmark is
/// tolerated). Panics if the data is split across multiple non-empty pages.
pub async fn assert_batch_size_zero_single_page<S: Source + ?Sized>(source: &S) {
    let label = source.connector_name();
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut stream = source.stream_pages(&ctx, 0);
    let mut pages = 0usize;
    let mut non_empty = 0usize;
    let mut records = 0usize;
    while let Some(page) = stream.next().await {
        let page = page.unwrap_or_else(|e| panic!("[{label}] stream_pages errored: {e}"));
        pages += 1;
        if !page.records.is_empty() {
            non_empty += 1;
        }
        records += page.records.len();
    }
    assert!(
        records > 0,
        "[{label}] produced no records under batch_size=0 — cannot verify single-page batching"
    );
    assert_eq!(
        non_empty, 1,
        "[{label}] batch_size=0 must yield the entire result set as a single page, but \
         {non_empty} non-empty pages were emitted ({pages} pages total, {records} records)"
    );
}

// ── Check 10: connector_name is non-empty ─────────────────────────────────────

/// **Check 10.** Assert a source's [`connector_name`](Source::connector_name)
/// is a non-empty string. An empty name is a cardinality-rule violation — the
/// observability layer falls back to the `"unknown"` metric label, silently
/// merging distinct connectors' metrics.
///
/// For sinks (which expose the same method), use
/// [`assert_connector_name_nonempty_value`]:
/// `assert_connector_name_nonempty_value(sink.connector_name(), sink.connector_name())`.
pub fn assert_connector_name_nonempty<S: Source + ?Sized>(source: &S) {
    assert_connector_name_nonempty_value(source.connector_name(), source.connector_name());
}

/// The value-level core of [`assert_connector_name_nonempty`] — usable for sinks.
pub fn assert_connector_name_nonempty_value(name: &str, label: &str) {
    assert!(
        !name.is_empty(),
        "[{label}] connector_name() returned an empty string — it would surface as the \
         \"unknown\" metric label (a cardinality-rule violation)"
    );
    assert!(
        !name.trim().is_empty(),
        "[{label}] connector_name() is whitespace-only ({name:?}) — same effect as empty"
    );
}

// ── Check 11: preflight check() is well-formed ────────────────────────────────

/// **Check 11.** Assert a source's [`check`](Source::check) returns
/// `Ok(CheckReport)` with at least one well-formed probe (non-empty name; a
/// `Fail`/`Skip` probe carries a non-empty reason). A connector must surface a
/// probe failure as a [`ProbeStatus::Fail`](faucet_core::check::ProbeStatus)
/// *inside* `Ok(report)`, never as an `Err` from `check()` (an `Err` means "no
/// probe could run at all", which `faucet doctor` renders differently).
///
/// For sinks, use [`assert_sink_preflight_check_wellformed`].
pub async fn assert_preflight_check_wellformed<S: Source + ?Sized>(
    source: &S,
    ctx: &faucet_core::check::CheckContext,
) {
    assert_report_wellformed(source.check(ctx).await, source.connector_name());
}

/// The sink counterpart of [`assert_preflight_check_wellformed`].
pub async fn assert_sink_preflight_check_wellformed<S: Sink + ?Sized>(
    sink: &S,
    ctx: &faucet_core::check::CheckContext,
) {
    assert_report_wellformed(sink.check(ctx).await, sink.connector_name());
}

/// Shared assertion over a `check()` outcome: `Ok(report)` with well-formed
/// probes, never `Err`.
fn assert_report_wellformed(
    outcome: Result<faucet_core::check::CheckReport, faucet_core::FaucetError>,
    label: &str,
) {
    use faucet_core::check::ProbeStatus;

    let report = outcome.unwrap_or_else(|e| {
        panic!(
            "[{label}] check() returned Err({e}) — a probe failure must surface as a Fail \
             probe inside Ok(report), not as an Err from check()"
        )
    });
    assert!(
        !report.probes.is_empty(),
        "[{label}] check() returned an empty report — a well-formed report carries at least \
         one probe"
    );
    for probe in &report.probes {
        assert!(
            !probe.name.is_empty(),
            "[{label}] check() returned a probe with an empty name"
        );
        match &probe.status {
            ProbeStatus::Pass => {}
            ProbeStatus::Fail { reason } => assert!(
                !reason.trim().is_empty(),
                "[{label}] Fail probe `{}` has an empty reason",
                probe.name
            ),
            ProbeStatus::Skip { reason } => assert!(
                !reason.trim().is_empty(),
                "[{label}] Skip probe `{}` has an empty reason",
                probe.name
            ),
        }
    }
}

// ── Check 12: discovery round-trips (discoverable sources) ────────────────────

/// Deep-merge a discovery `config_patch` onto a base config `Value` and return
/// the merged config — objects merge recursively, scalars and arrays replace
/// wholesale. This mirrors the deep-merge the CLI applies when it turns a
/// [`DatasetDescriptor`](faucet_core::DatasetDescriptor) into a matrix row, so a
/// `rebuild` closure for [`assert_discover_roundtrips`] can compose the patch
/// onto the real base config exactly as production does:
///
/// ```
/// use faucet_conformance::merge_config_patch;
/// use serde_json::json;
/// let base = json!({ "url": "…", "query": "SELECT 1" });
/// let merged = merge_config_patch(base, &json!({ "query": "SELECT * FROM t" }));
/// assert_eq!(merged["query"], "SELECT * FROM t");
/// assert_eq!(merged["url"], "…");
/// ```
pub fn merge_config_patch(mut base: Value, patch: &Value) -> Value {
    merge_into(&mut base, patch);
    base
}

fn merge_into(base: &mut Value, patch: &Value) {
    match (base, patch) {
        (Value::Object(base_map), Value::Object(patch_map)) => {
            for (k, v) in patch_map {
                merge_into(base_map.entry(k.clone()).or_insert(Value::Null), v);
            }
        }
        (base_slot, patch) => *base_slot = patch.clone(),
    }
}

/// **Check 12.** For a source that advertises
/// [`supports_discover`](Source::supports_discover): enumerate its datasets via
/// [`discover`](Source::discover), then for each descriptor deep-merge its
/// [`config_patch`](faucet_core::DatasetDescriptor::config_patch) onto the base
/// config (via the caller's `rebuild` closure) and assert the rebuilt source is
/// actually readable — the dataset the catalog advertised is genuinely
/// selectable end-to-end, not just a name in a list.
///
/// This is an **integration-level** check: it needs a live, seeded backend so
/// `discover()` returns real descriptors and the rebuilt source reads real (or
/// legitimately empty) data. Run it in a connector's testcontainers/tempfile
/// conformance test, reusing the already-seeded backend. `rebuild` receives the
/// descriptor's `config_patch` and returns the source built from
/// `base config ⊕ patch` — [`merge_config_patch`] writes that closure in one
/// line.
///
/// Asserts: the source advertises discovery; `discover()` returns at least one
/// descriptor (a seeded backend must expose something — an empty catalog makes
/// the round-trip vacuous); and every rebuilt source drains through
/// `stream_pages` **without error** (≥ 0 rows — the selected dataset may be
/// legitimately empty, but selecting it must not fail).
pub async fn assert_discover_roundtrips<S, F, Fut>(source: &S, rebuild: F)
where
    S: Source + ?Sized,
    F: Fn(Value) -> Fut,
    Fut: std::future::Future<Output = Box<dyn Source>>,
{
    let label = source.connector_name();
    assert!(
        source.supports_discover(),
        "[{label}] does not advertise discovery (supports_discover()=false) — call this only \
         on a discoverable source"
    );

    let descriptors = source
        .discover()
        .await
        .unwrap_or_else(|e| panic!("[{label}] discover() errored: {e}"));
    assert!(
        !descriptors.is_empty(),
        "[{label}] discover() returned no datasets — seed the backend before the round-trip so \
         there is a real dataset to re-select (an empty catalog makes the check vacuous)"
    );

    for descriptor in &descriptors {
        let patch = descriptor.config_patch.clone();
        let rebuilt = rebuild(patch.clone()).await;
        // Drive the real read path of the rebuilt source. The dataset selected
        // by the patch must be readable; it may legitimately hold zero rows.
        let ctx: HashMap<String, Value> = HashMap::new();
        let mut stream = rebuilt.stream_pages(&ctx, 100);
        while let Some(page) = stream.next().await {
            page.unwrap_or_else(|e| {
                panic!(
                    "[{label}] rebuilt source for dataset `{}` (config_patch {patch}) errored on \
                     read: {e} — the descriptor the catalog advertised is not actually selectable",
                    descriptor.name
                )
            });
        }
    }
}

// ── Check 13: cancellation flushes buffered output ────────────────────────────

/// **Check 13.** Assert the flush-completing cancellation contract
/// ([ADR 0011](https://github.com/faucet-hq/faucet-stream/blob/main/docs/adr/0011-cooperative-cancellation.md),
/// #146 H16): a [`CancellationToken`](faucet_core::CancellationToken) fired
/// mid-run stops [`run_stream`](faucet_core::run_stream) at the next page
/// boundary, **flushes the sink** so buffered output is made durable, and
/// returns `Ok` with the partial result — as opposed to dropping the run
/// future, which would flush nothing and orphan a buffered sink's output (a
/// Parquet footer, an S3 multipart).
///
/// Drives the **real** `faucet_core::run_stream` with a synthetic source that
/// yields one page and then cancels the token (deterministic — no timers, no
/// flakiness). `durable_count` reports the number of rows the sink has made
/// **durable** (for [`BufferedSink`](doubles::BufferedSink),
/// `|| async { sink.durable_len() }`; for a real buffered sink, whatever a
/// reader observes *after* the run). Asserts `run_stream` returned `Ok`, the
/// partial result counts the written page, and every written row is durable —
/// i.e. the cancel path flushed. A sink whose `flush` does not commit fails
/// here, as does a pipeline that skips the on-cancel flush.
pub async fn assert_cancellation_flushes<S, F, Fut>(sink: &S, durable_count: F)
where
    S: Sink + ?Sized,
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = usize>,
{
    use faucet_core::{CancellationToken, RunStreamOptions, StreamPage, run_stream};

    let label = sink.connector_name();
    let before = durable_count().await;

    let page = rows(&[1, 2, 3]);
    let n = page.len();

    let token = CancellationToken::new();
    let stream_token = token.clone();
    // Yield one page, then fire the token and block. The only way `run_stream`
    // exits is the cooperative cancel at the page boundary — so the flush it
    // performs there is exactly the #146 H16 behaviour under test.
    let stream = Box::pin(async_stream::stream! {
        yield Ok(StreamPage { records: page, bookmark: None });
        stream_token.cancel();
        futures::future::pending::<()>().await;
    });

    let result = run_stream(stream, sink, RunStreamOptions::new().with_cancel(token))
        .await
        .unwrap_or_else(|e| {
            panic!(
                "[{label}] a cooperative cancel must return Ok with the partial result, got \
                 Err({e})"
            )
        });
    assert_eq!(
        result.records_written, n,
        "[{label}] the page written before cancellation is not counted in the partial result"
    );

    let durable = durable_count().await - before;
    assert_eq!(
        durable, n,
        "[{label}] the sink was not flushed on the cancel path: {durable} of {n} written rows \
         are durable — buffered output would be lost when a run is cancelled"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use doubles::{
        BufferedSink, CountingSource, DiscoverableSource, EmptyNameSource, ErringCheckSink,
        ErringCheckSource, EvolvingSink, FailingSource, LyingIdempotentSink, LyingKeyedSink,
        MultiPageZeroSource, NoOpEvolvingSink, PanickingSource, TestSink,
    };

    #[test]
    fn check1_accepts_a_valid_source_schema() {
        let s = CountingSource::new(10, 2);
        assert_config_schema_valid(&s);
    }

    #[test]
    fn check1_value_form_works_for_a_sink() {
        let sink = TestSink::new();
        assert_config_schema_valid_value(&sink.config_schema(), sink.connector_name());
    }

    #[test]
    #[should_panic(expected = "no recognizable JSON Schema keyword")]
    fn check1_rejects_a_non_schema() {
        assert_config_schema_valid_value(&serde_json::json!({"nope": 1}), "bogus");
    }

    #[tokio::test]
    async fn check2_passes_for_a_paging_source() {
        let s = CountingSource::new(1000, 100);
        assert_bounded_memory(&s, 100, 1000).await;
    }

    #[tokio::test]
    #[should_panic(expected = "not bounded")]
    async fn check2_fails_when_source_emits_one_big_page() {
        // batch 0 => single page of `total`, which must trip the bounded check.
        let s = CountingSource::new(500, 0);
        assert_bounded_memory(&s, 100, 500).await;
    }

    // ── Check 3: bookmark round-trip ─────────────────────────────────────────

    #[tokio::test]
    async fn check3_passes_for_a_resumable_source() {
        let s = CountingSource::new(500, 100);
        assert_bookmark_roundtrip(&s).await;
    }

    #[tokio::test]
    #[should_panic(expected = "was ignored")]
    async fn check3_fails_when_source_ignores_the_bookmark() {
        let s = CountingSource::non_resumable(500, 100);
        assert_bookmark_roundtrip(&s).await;
    }

    // ── Check 4: idempotent replay ───────────────────────────────────────────

    #[tokio::test]
    async fn check4_passes_for_a_watermark_sink() {
        let sink = TestSink::idempotent("id");
        let s = sink.clone();
        assert_idempotent_replay(&sink, || {
            let s = s.clone();
            async move { s.len() }
        })
        .await;
    }

    #[tokio::test]
    async fn check4_passes_for_a_keyed_upsert_sink() {
        let sink = TestSink::keyed("id");
        let s = sink.clone();
        assert_idempotent_replay(&sink, || {
            let s = s.clone();
            async move { s.len() }
        })
        .await;
    }

    #[tokio::test]
    #[should_panic(expected = "did not durably record its commit token")]
    async fn check4_fails_for_a_lying_idempotent_sink() {
        let sink = LyingIdempotentSink::new();
        let s = sink.clone();
        assert_idempotent_replay(&sink, || {
            let s = s.clone();
            async move { s.len() }
        })
        .await;
    }

    #[tokio::test]
    #[should_panic(expected = "did not converge")]
    async fn check4_fails_for_a_lying_keyed_sink() {
        let sink = LyingKeyedSink::new();
        let s = sink.clone();
        assert_idempotent_replay(&sink, || {
            let s = s.clone();
            async move { s.len() }
        })
        .await;
    }

    #[tokio::test]
    #[should_panic(expected = "no idempotency mechanism")]
    async fn check4_fails_for_an_append_only_sink() {
        let sink = TestSink::new();
        let s = sink.clone();
        assert_idempotent_replay(&sink, || {
            let s = s.clone();
            async move { s.len() }
        })
        .await;
    }

    // ── Check 5: capabilities truthful ───────────────────────────────────────

    #[tokio::test]
    async fn check5_passes_for_an_honest_append_sink() {
        let sink = TestSink::new();
        let s = sink.clone();
        assert_capabilities_truthful(&sink, || {
            let s = s.clone();
            async move { s.len() }
        })
        .await;
    }

    #[tokio::test]
    async fn check5_passes_for_an_honest_idempotent_sink() {
        let sink = TestSink::idempotent("id");
        let s = sink.clone();
        assert_capabilities_truthful(&sink, || {
            let s = s.clone();
            async move { s.len() }
        })
        .await;
    }

    #[tokio::test]
    #[should_panic(expected = "did not durably record its commit token")]
    async fn check5_fails_for_a_lying_idempotent_sink() {
        let sink = LyingIdempotentSink::new();
        let s = sink.clone();
        assert_capabilities_truthful(&sink, || {
            let s = s.clone();
            async move { s.len() }
        })
        .await;
    }

    // ── Check 6: errors, not panics ──────────────────────────────────────────

    #[tokio::test]
    async fn check6_passes_for_a_source_that_returns_err() {
        assert_errors_not_panics(&FailingSource).await;
    }

    #[tokio::test]
    #[should_panic(expected = "panicked instead of returning Err")]
    async fn check6_fails_for_a_source_that_panics() {
        assert_errors_not_panics(&PanickingSource).await;
    }

    #[tokio::test]
    #[should_panic(expected = "expected a failure but fetch_all succeeded")]
    async fn check6_fails_for_a_source_that_succeeds() {
        // A healthy source fed to the failure check must be flagged — the check
        // is only meaningful against a source expected to fail.
        assert_errors_not_panics(&CountingSource::new(3, 1)).await;
    }

    // ── Check 7: write modes truthful ────────────────────────────────────────

    #[tokio::test]
    async fn check7_passes_for_an_upsert_delete_sink() {
        let sink = TestSink::keyed_upsert("id");
        let s = sink.clone();
        assert_write_modes_truthful(&sink, || {
            let s = s.clone();
            async move { s.len() }
        })
        .await;
    }

    #[tokio::test]
    async fn check7_skips_an_append_only_sink() {
        // Append-only: the body is skipped (nothing to prove), so the check
        // passes without ever touching the sink's write path.
        let sink = TestSink::new();
        let s = sink.clone();
        assert_write_modes_truthful(&sink, || {
            let s = s.clone();
            async move { s.len() }
        })
        .await;
        assert!(sink.is_empty(), "append-only skip must not write anything");
    }

    #[tokio::test]
    #[should_panic(expected = "did not converge")]
    async fn check7_fails_for_a_lying_keyed_sink() {
        let sink = LyingKeyedSink::new();
        let s = sink.clone();
        assert_write_modes_truthful(&sink, || {
            let s = s.clone();
            async move { s.len() }
        })
        .await;
    }

    // ── Check 8: schema evolution effective ──────────────────────────────────

    #[tokio::test]
    async fn check8_passes_for_an_evolving_sink() {
        let sink = EvolvingSink::new();
        assert_schema_evolution_effective(&sink).await;
        assert_eq!(sink.column_count(), 2, "evolve must have added a column");
    }

    #[tokio::test]
    #[should_panic(expected = "was not effective")]
    async fn check8_fails_for_a_noop_evolving_sink() {
        assert_schema_evolution_effective(&NoOpEvolvingSink).await;
    }

    // ── Check 9: batch_size=0 single page ────────────────────────────────────

    #[tokio::test]
    async fn check9_passes_for_a_single_page_source() {
        let s = CountingSource::new(6, 0);
        assert_batch_size_zero_single_page(&s).await;
    }

    #[tokio::test]
    #[should_panic(expected = "single page")]
    async fn check9_fails_for_a_multi_page_source() {
        let s = MultiPageZeroSource::new(6);
        assert_batch_size_zero_single_page(&s).await;
    }

    // ── Check 10: connector_name non-empty ───────────────────────────────────

    #[test]
    fn check10_passes_for_a_named_source() {
        assert_connector_name_nonempty(&CountingSource::new(1, 1));
    }

    #[test]
    fn check10_value_form_works_for_a_sink() {
        let sink = TestSink::new();
        assert_connector_name_nonempty_value(sink.connector_name(), sink.connector_name());
    }

    #[test]
    #[should_panic(expected = "empty string")]
    fn check10_fails_for_an_empty_name_source() {
        assert_connector_name_nonempty(&EmptyNameSource);
    }

    #[test]
    #[should_panic(expected = "empty string")]
    fn check10_value_form_rejects_empty() {
        assert_connector_name_nonempty_value("", "bogus");
    }

    // ── Check 11: preflight check() well-formed ──────────────────────────────

    #[tokio::test]
    async fn check11_passes_for_a_source_with_a_fail_probe() {
        // A failing source surfaces its failure as a Fail probe inside
        // Ok(report) — exactly what the check requires.
        let ctx = faucet_core::check::CheckContext::default();
        assert_preflight_check_wellformed(&FailingSource, &ctx).await;
    }

    #[tokio::test]
    async fn check11_passes_for_a_healthy_source() {
        let ctx = faucet_core::check::CheckContext::default();
        assert_preflight_check_wellformed(&CountingSource::new(3, 1), &ctx).await;
    }

    #[tokio::test]
    async fn check11_passes_for_a_sink() {
        let ctx = faucet_core::check::CheckContext::default();
        assert_sink_preflight_check_wellformed(&TestSink::new(), &ctx).await;
    }

    #[tokio::test]
    #[should_panic(expected = "returned Err")]
    async fn check11_fails_when_source_check_returns_err() {
        let ctx = faucet_core::check::CheckContext::default();
        assert_preflight_check_wellformed(&ErringCheckSource, &ctx).await;
    }

    #[tokio::test]
    #[should_panic(expected = "returned Err")]
    async fn check11_fails_when_sink_check_returns_err() {
        let ctx = faucet_core::check::CheckContext::default();
        assert_sink_preflight_check_wellformed(&ErringCheckSink, &ctx).await;
    }

    // ── merge_config_patch ────────────────────────────────────────────────────

    #[test]
    fn merge_config_patch_is_recursive_with_scalar_and_array_replace() {
        let base = serde_json::json!({
            "url": "keep",
            "query": "SELECT 1",
            "opts": { "a": 1, "b": 2 },
            "keys": [1, 2, 3],
        });
        let merged = merge_config_patch(
            base,
            &serde_json::json!({
                "query": "SELECT * FROM t",   // scalar replace
                "opts": { "b": 9, "c": 3 },   // recursive object merge
                "keys": [7],                    // array replaces wholesale
            }),
        );
        assert_eq!(merged["url"], "keep");
        assert_eq!(merged["query"], "SELECT * FROM t");
        assert_eq!(
            merged["opts"],
            serde_json::json!({ "a": 1, "b": 9, "c": 3 })
        );
        assert_eq!(merged["keys"], serde_json::json!([7]));
    }

    // ── Check 12: discovery round-trips ──────────────────────────────────────

    #[tokio::test]
    async fn check12_passes_when_every_dataset_rebuilds_and_reads() {
        let source = DiscoverableSource::new();
        assert_discover_roundtrips(&source, |patch| async move {
            // The patch selects a dataset; a real adopter would deep-merge it
            // onto the base config and rebuild. Here the rebuilt source just
            // reads a small, healthy set.
            let name = patch["dataset"].as_str().unwrap_or("");
            assert!(!name.is_empty(), "config_patch must carry the dataset");
            Box::new(CountingSource::new(3, 1)) as Box<dyn Source>
        })
        .await;
    }

    #[tokio::test]
    #[should_panic(expected = "does not advertise discovery")]
    async fn check12_fails_for_a_non_discoverable_source() {
        let source = CountingSource::new(3, 1);
        assert_discover_roundtrips(&source, |_patch| async {
            Box::new(CountingSource::new(3, 1)) as Box<dyn Source>
        })
        .await;
    }

    #[tokio::test]
    #[should_panic(expected = "returned no datasets")]
    async fn check12_fails_when_catalog_is_empty() {
        let source = DiscoverableSource::empty();
        assert_discover_roundtrips(&source, |_patch| async {
            Box::new(CountingSource::new(3, 1)) as Box<dyn Source>
        })
        .await;
    }

    #[tokio::test]
    #[should_panic(expected = "errored on read")]
    async fn check12_fails_when_rebuilt_source_is_unreadable() {
        let source = DiscoverableSource::new();
        assert_discover_roundtrips(&source, |_patch| async {
            // The catalog advertised a dataset the rebuilt source can't read.
            Box::new(FailingSource) as Box<dyn Source>
        })
        .await;
    }

    // ── Check 13: cancellation flushes ───────────────────────────────────────

    #[tokio::test]
    async fn check13_passes_for_a_sink_that_flushes_on_cancel() {
        let sink = BufferedSink::new();
        let s = sink.clone();
        assert_cancellation_flushes(&sink, || {
            let s = s.clone();
            async move { s.durable_len() }
        })
        .await;
        // The written page was flushed to durable storage on the cancel path.
        assert_eq!(sink.durable_len(), 3);
        assert_eq!(sink.staged_len(), 0);
    }

    #[tokio::test]
    #[should_panic(expected = "was not flushed on the cancel path")]
    async fn check13_fails_for_a_sink_whose_flush_drops_the_buffer() {
        let sink = BufferedSink::broken();
        let s = sink.clone();
        assert_cancellation_flushes(&sink, || {
            let s = s.clone();
            async move { s.durable_len() }
        })
        .await;
    }
}
