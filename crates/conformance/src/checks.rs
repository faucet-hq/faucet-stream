//! Capability checks 15–19 (RFC 0013, #878): each proves a sink that declares a
//! capability behaves as declared. [`crate::required`] names which of them a
//! connector must run, and `scripts/conformance-coverage.py` enforces it.
//!
//! Every check writes rows keyed on `"id"` with a non-key `"v"` column, from an
//! id range no other check uses, so several checks can share one destination.
//! `distinct_count` reports the destination's current row count (distinct by
//! key for a keyed sink).

use faucet_core::{BatchAtomicity, Sink, Value};
use std::future::Future;

fn rows(ids: std::ops::RangeInclusive<i64>) -> Vec<Value> {
    ids.map(|i| serde_json::json!({ "id": i, "v": format!("v{i}") }))
        .collect()
}

async fn flush<S: Sink + ?Sized>(sink: &S, what: &str) {
    let label = sink.connector_name();
    sink.flush()
        .await
        .unwrap_or_else(|e| panic!("[{label}] flush after {what} errored: {e}"));
}

/// **Check 15.** A sink that reports per-row outcomes
/// ([`Sink::write_batch_partial`]) reports them truthfully: one outcome per
/// row offered, every row of a valid page `Ok`, and exactly the `Ok` rows
/// land in the destination. A sink that answers `Ok` for a row it dropped
/// would let the pipeline advance a bookmark past a lost row.
pub async fn assert_partial_outcomes_truthful<S, F, Fut>(sink: &S, distinct_count: F)
where
    S: Sink + ?Sized,
    F: Fn() -> Fut,
    Fut: Future<Output = usize>,
{
    let label = sink.connector_name();
    let page = rows(901..=905);
    let before = distinct_count().await;
    let outcomes = sink
        .write_batch_partial(&page)
        .await
        .unwrap_or_else(|e| panic!("[{label}] write_batch_partial of a valid page errored: {e}"));
    flush(sink, "write_batch_partial").await;
    assert_eq!(
        outcomes.len(),
        page.len(),
        "[{label}] write_batch_partial returned {} outcomes for {} rows — one per row offered",
        outcomes.len(),
        page.len()
    );
    let ok = outcomes.iter().filter(|o| o.is_ok()).count();
    let landed = distinct_count().await - before;
    assert_eq!(
        landed, ok,
        "[{label}] write_batch_partial reported {ok} rows Ok but {landed} landed — an Ok \
         outcome must mean the row is in the destination"
    );
    assert_eq!(
        ok,
        page.len(),
        "[{label}] a valid page reported {} failed rows",
        page.len() - ok
    );
}

/// **Check 16.** A sink's [`Sink::write_batch_reported`] accounts for every
/// row offered (`accepted + coalesced + skipped` equals the page) and its
/// `accepted` count is the number of rows that landed (#879).
pub async fn assert_reported_rows_truthful<S, F, Fut>(sink: &S, distinct_count: F)
where
    S: Sink + ?Sized,
    F: Fn() -> Fut,
    Fut: Future<Output = usize>,
{
    let label = sink.connector_name();
    let page = rows(911..=915);
    let before = distinct_count().await;
    let report = sink
        .write_batch_reported(&page)
        .await
        .unwrap_or_else(|e| panic!("[{label}] write_batch_reported of a valid page errored: {e}"));
    flush(sink, "write_batch_reported").await;
    let accounted = report.accepted + report.coalesced + report.skipped;
    assert_eq!(
        accounted,
        page.len(),
        "[{label}] write_batch_reported accounts for {accounted} of {} rows offered ({report:?})",
        page.len()
    );
    let landed = distinct_count().await - before;
    assert_eq!(
        report.accepted, landed,
        "[{label}] write_batch_reported says {} rows were accepted but {landed} landed",
        report.accepted
    );
}

/// **Check 17.** A sink whose [`Sink::write_batch_is_replay_safe`] is `true`
/// converges when the same page is written twice — the resilience layer
/// retries such a write after a lost response, so a duplicate here is a
/// duplicate in production. Configure the sink as it declares replay safety
/// (for most sinks: `write_mode: upsert` with `key: ["id"]`).
pub async fn assert_replay_safe<S, F, Fut>(sink: &S, distinct_count: F)
where
    S: Sink + ?Sized,
    F: Fn() -> Fut,
    Fut: Future<Output = usize>,
{
    let label = sink.connector_name();
    assert!(
        sink.write_batch_is_replay_safe(),
        "[{label}] declares its plain write replay-unsafe — configure it as it declares \
         replay safety (e.g. a keyed upsert) before calling this check"
    );
    let page = rows(921..=923);
    let before = distinct_count().await;
    for attempt in ["first write", "replayed write"] {
        sink.write_batch(&page)
            .await
            .unwrap_or_else(|e| panic!("[{label}] {attempt} errored: {e}"));
        flush(sink, attempt).await;
    }
    let landed = distinct_count().await - before;
    assert_eq!(
        landed,
        page.len(),
        "[{label}] replaying a page duplicated rows: {landed} landed for {} distinct keys — \
         the sink is not replay-safe",
        page.len()
    );
}

/// **Check 18.** A sink configured for `write_mode: overwrite` keeps the
/// begin → stage → commit lifecycle: staged rows are invisible until
/// [`commit_overwrite`](Sink::commit_overwrite), an
/// [`abort_overwrite`](Sink::abort_overwrite) leaves the destination as it
/// was, and a commit replaces the destination with exactly the run's rows.
///
/// Seed the destination with a few rows first so "untouched" is observable.
pub async fn assert_overwrite_lifecycle<S, F, Fut>(sink: &S, distinct_count: F)
where
    S: Sink + ?Sized,
    F: Fn() -> Fut,
    Fut: Future<Output = usize>,
{
    let label = sink.connector_name();
    assert!(
        sink.is_overwrite(),
        "[{label}] is not configured for write_mode: overwrite — nothing to verify"
    );
    let before = distinct_count().await;

    sink.begin_overwrite()
        .await
        .unwrap_or_else(|e| panic!("[{label}] begin_overwrite (aborted run) errored: {e}"));
    sink.write_batch(&rows(931..=933))
        .await
        .unwrap_or_else(|e| panic!("[{label}] write in the aborted run errored: {e}"));
    flush(sink, "the aborted run's write").await;
    assert_eq!(
        distinct_count().await,
        before,
        "[{label}] staged rows are visible in the destination before commit"
    );
    sink.abort_overwrite()
        .await
        .unwrap_or_else(|e| panic!("[{label}] abort_overwrite errored: {e}"));
    assert_eq!(
        distinct_count().await,
        before,
        "[{label}] abort_overwrite changed the destination — a failed run must leave it as it was"
    );

    sink.begin_overwrite()
        .await
        .unwrap_or_else(|e| panic!("[{label}] begin_overwrite (committed run) errored: {e}"));
    let page = rows(941..=942);
    sink.write_batch(&page)
        .await
        .unwrap_or_else(|e| panic!("[{label}] write in the committed run errored: {e}"));
    flush(sink, "the committed run's write").await;
    assert_eq!(
        distinct_count().await,
        before,
        "[{label}] staged rows are visible in the destination before commit"
    );
    sink.commit_overwrite()
        .await
        .unwrap_or_else(|e| panic!("[{label}] commit_overwrite errored: {e}"));
    assert_eq!(
        distinct_count().await,
        page.len(),
        "[{label}] commit_overwrite did not replace the destination with exactly the run's rows"
    );
}

/// **Check 19.** A sink's declared [`BatchAtomicity`] holds when a write
/// fails. `poison_page` must hold at least one row the sink accepts and at
/// least one it rejects (a missing required column, a value of the wrong
/// type).
///
/// - `Atomic`: the write fails and no row of the page lands.
/// - `PerRow`: an outer `Err` lands no row; otherwise the poison row is
///   reported `Err` and exactly the `Ok` rows land.
/// - `BestEffort` declares nothing to verify, so the check fails rather than
///   pass vacuously: configure the sink as it declares atomicity.
pub async fn assert_batch_atomicity_truthful<S, F, Fut>(
    sink: &S,
    distinct_count: F,
    poison_page: &[Value],
) where
    S: Sink + ?Sized,
    F: Fn() -> Fut,
    Fut: Future<Output = usize>,
{
    let label = sink.connector_name();
    let declared = crate::assert_batch_atomicity_declared(sink);
    let before = distinct_count().await;
    match declared {
        BatchAtomicity::Atomic => {
            // A buffering sink reports the failure when it flushes.
            let outcome = match sink.write_batch(poison_page).await {
                Ok(_) => sink.flush().await,
                Err(e) => {
                    let _ = sink.flush().await;
                    Err(e)
                }
            };
            assert!(
                outcome.is_err(),
                "[{label}] the poison page was written without error — it must hold a row \
                 the sink rejects"
            );
            let landed = distinct_count().await - before;
            assert_eq!(
                landed, 0,
                "[{label}] declares atomic batches but {landed} rows of a failed write landed"
            );
        }
        BatchAtomicity::PerRow => match sink.write_batch_partial(poison_page).await {
            Err(_) => {
                let _ = sink.flush().await;
                let landed = distinct_count().await - before;
                assert_eq!(
                    landed, 0,
                    "[{label}] declares per-row outcomes but an outer Err left {landed} rows \
                     behind — an outer Err must mean no row committed"
                );
            }
            Ok(outcomes) => {
                flush(sink, "the poison page").await;
                assert_eq!(
                    outcomes.len(),
                    poison_page.len(),
                    "[{label}] write_batch_partial returned {} outcomes for {} rows",
                    outcomes.len(),
                    poison_page.len()
                );
                let ok = outcomes.iter().filter(|o| o.is_ok()).count();
                assert!(
                    ok < poison_page.len(),
                    "[{label}] every row of the poison page was reported Ok — it must hold a \
                     row the sink rejects"
                );
                let landed = distinct_count().await - before;
                assert_eq!(
                    landed, ok,
                    "[{label}] reported {ok} rows Ok but {landed} landed"
                );
            }
        },
        _ => panic!(
            "[{label}] declares {declared} batches — nothing to verify; configure the sink as \
             it declares atomic or per-row batches"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::doubles::{
        AtomicitySink, LyingKeyedSink, MisreportingSink, OverwriteSink, PartialOutcomeSink,
        TestSink,
    };
    use serde_json::json;

    fn counter<T: Clone + Send + 'static>(
        sink: &T,
        len: fn(&T) -> usize,
    ) -> impl Fn() -> std::future::Ready<usize> {
        let s = sink.clone();
        move || std::future::ready(len(&s))
    }

    fn poison() -> Vec<Value> {
        vec![json!({ "id": 1, "v": "a" }), json!({ "v": "no id" })]
    }

    #[tokio::test]
    async fn check15_passes_for_an_honest_per_row_sink() {
        let sink = PartialOutcomeSink::honest();
        assert_partial_outcomes_truthful(&sink, counter(&sink, PartialOutcomeSink::len)).await;
        assert_eq!(sink.len(), 5);
    }

    #[tokio::test]
    async fn check15_passes_for_the_default_impl() {
        let sink = TestSink::new();
        assert_partial_outcomes_truthful(&sink, counter(&sink, TestSink::len)).await;
    }

    #[tokio::test]
    #[should_panic(expected = "an Ok outcome must mean the row is in the destination")]
    async fn check15_fails_when_ok_rows_are_dropped() {
        let sink = PartialOutcomeSink::drops_silently();
        assert_partial_outcomes_truthful(&sink, counter(&sink, PartialOutcomeSink::len)).await;
    }

    #[tokio::test]
    #[should_panic(expected = "one per row offered")]
    async fn check15_fails_on_a_short_outcome_vector() {
        let sink = PartialOutcomeSink::short();
        assert_partial_outcomes_truthful(&sink, counter(&sink, PartialOutcomeSink::len)).await;
    }

    #[tokio::test]
    #[should_panic(expected = "a valid page reported")]
    async fn check15_fails_when_a_valid_page_reports_failures() {
        let sink = PartialOutcomeSink::rejects_all();
        assert_partial_outcomes_truthful(&sink, counter(&sink, PartialOutcomeSink::len)).await;
    }

    #[tokio::test]
    async fn check16_passes_for_the_default_report() {
        let sink = TestSink::new();
        assert_reported_rows_truthful(&sink, counter(&sink, TestSink::len)).await;
    }

    #[tokio::test]
    async fn check16_passes_for_an_honest_skipping_sink() {
        let sink = MisreportingSink::honest_skip();
        assert_reported_rows_truthful(&sink, counter(&sink, MisreportingSink::len)).await;
    }

    #[tokio::test]
    #[should_panic(expected = "rows were accepted but")]
    async fn check16_fails_when_accepted_overstates_what_landed() {
        let sink = MisreportingSink::over_reports();
        assert_reported_rows_truthful(&sink, counter(&sink, MisreportingSink::len)).await;
    }

    #[tokio::test]
    #[should_panic(expected = "accounts for 4 of 5 rows offered")]
    async fn check16_fails_when_rows_go_unaccounted() {
        let sink = MisreportingSink::under_reports();
        assert_reported_rows_truthful(&sink, counter(&sink, MisreportingSink::len)).await;
    }

    #[tokio::test]
    async fn check17_passes_for_a_keyed_sink() {
        let sink = TestSink::keyed("id");
        assert_replay_safe(&sink, counter(&sink, TestSink::len)).await;
    }

    #[tokio::test]
    #[should_panic(expected = "replaying a page duplicated rows")]
    async fn check17_fails_for_a_sink_that_lies_about_dedup() {
        let sink = LyingKeyedSink::new();
        assert_replay_safe(&sink, counter(&sink, LyingKeyedSink::len)).await;
    }

    #[tokio::test]
    #[should_panic(expected = "declares its plain write replay-unsafe")]
    async fn check17_fails_for_an_append_sink() {
        let sink = TestSink::new();
        assert_replay_safe(&sink, counter(&sink, TestSink::len)).await;
    }

    #[tokio::test]
    async fn check18_passes_for_an_honest_overwrite_sink() {
        let sink = OverwriteSink::honest();
        sink.seed(4);
        assert_overwrite_lifecycle(&sink, counter(&sink, OverwriteSink::len)).await;
        assert_eq!(sink.len(), 2);
    }

    #[tokio::test]
    #[should_panic(expected = "visible in the destination before commit")]
    async fn check18_fails_when_writes_bypass_staging() {
        let sink = OverwriteSink::eager();
        sink.seed(4);
        assert_overwrite_lifecycle(&sink, counter(&sink, OverwriteSink::len)).await;
    }

    #[tokio::test]
    #[should_panic(expected = "abort_overwrite changed the destination")]
    async fn check18_fails_when_abort_truncates() {
        let sink = OverwriteSink::truncating_abort();
        sink.seed(4);
        assert_overwrite_lifecycle(&sink, counter(&sink, OverwriteSink::len)).await;
    }

    #[tokio::test]
    #[should_panic(expected = "did not replace the destination")]
    async fn check18_fails_when_commit_appends() {
        let sink = OverwriteSink::appending_commit();
        sink.seed(4);
        assert_overwrite_lifecycle(&sink, counter(&sink, OverwriteSink::len)).await;
    }

    #[tokio::test]
    #[should_panic(expected = "is not configured for write_mode: overwrite")]
    async fn check18_fails_for_a_sink_not_in_overwrite_mode() {
        let sink = TestSink::new();
        assert_overwrite_lifecycle(&sink, counter(&sink, TestSink::len)).await;
    }

    #[tokio::test]
    async fn check19_passes_for_an_atomic_sink() {
        let sink = AtomicitySink::atomic();
        assert_batch_atomicity_truthful(&sink, counter(&sink, AtomicitySink::len), &poison()).await;
    }

    #[tokio::test]
    #[should_panic(expected = "declares atomic batches but 1 rows of a failed write landed")]
    async fn check19_fails_for_a_torn_atomic_sink() {
        let sink = AtomicitySink::torn();
        assert_batch_atomicity_truthful(&sink, counter(&sink, AtomicitySink::len), &poison()).await;
    }

    #[tokio::test]
    #[should_panic(expected = "the poison page was written without error")]
    async fn check19_fails_for_a_harmless_poison_page() {
        let sink = AtomicitySink::atomic();
        let page = vec![json!({ "id": 1, "v": "a" })];
        assert_batch_atomicity_truthful(&sink, counter(&sink, AtomicitySink::len), &page).await;
    }

    #[tokio::test]
    async fn check19_passes_for_a_per_row_sink() {
        let sink = AtomicitySink::per_row();
        assert_batch_atomicity_truthful(&sink, counter(&sink, AtomicitySink::len), &poison()).await;
        assert_eq!(sink.len(), 1);
    }

    #[tokio::test]
    async fn check19_passes_for_a_per_row_sink_failing_the_whole_call() {
        let sink = AtomicitySink::per_row_outer_err();
        assert_batch_atomicity_truthful(&sink, counter(&sink, AtomicitySink::len), &poison()).await;
    }

    #[tokio::test]
    #[should_panic(expected = "an outer Err left 1 rows behind")]
    async fn check19_fails_for_a_per_row_sink_whose_outer_err_committed() {
        let sink = AtomicitySink::per_row_torn();
        assert_batch_atomicity_truthful(&sink, counter(&sink, AtomicitySink::len), &poison()).await;
    }

    #[tokio::test]
    #[should_panic(expected = "every row of the poison page was reported Ok")]
    async fn check19_fails_for_a_harmless_per_row_poison_page() {
        let sink = AtomicitySink::per_row();
        let page = vec![json!({ "id": 1, "v": "a" })];
        assert_batch_atomicity_truthful(&sink, counter(&sink, AtomicitySink::len), &page).await;
    }

    #[tokio::test]
    #[should_panic(expected = "declares best_effort batches")]
    async fn check19_fails_for_a_best_effort_sink() {
        let sink = TestSink::new();
        assert_batch_atomicity_truthful(&sink, counter(&sink, TestSink::len), &poison()).await;
    }
}
