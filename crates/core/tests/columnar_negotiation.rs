//! Proves the pipeline actually negotiates the Arrow columnar fast path
//! (feature `arrow`, RFC 0002 / #375) — not just that it compiles.
//!
//! The mock source/sink are **columnar-only**: their `Value` methods
//! (`stream_pages` / `write_batch`) return errors, while their columnar methods
//! (`stream_batches` / `write_batch_columnar`) work. So a successful
//! `Pipeline::run` is only possible if the pipeline drove the columnar path;
//! if it fell back to the `Value` path the run would error.
#![cfg(feature = "arrow")]

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use faucet_core::columnar::{ColumnarPage, values_to_record_batch_inferred};
use faucet_core::{FaucetError, Pipeline, Sink, Source, Stream, StreamPage, async_trait};
use serde_json::{Value, json};

/// A source that ONLY works columnar: `stream_pages` errors, `stream_batches`
/// yields one batch built from `rows`.
struct ColumnarOnlySource {
    rows: Vec<Value>,
}

#[async_trait]
impl Source for ColumnarOnlySource {
    async fn fetch_with_context(
        &self,
        _context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        Err(FaucetError::Source("value path must not be used".into()))
    }

    fn stream_pages<'a>(
        &'a self,
        _context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        Box::pin(futures::stream::once(async {
            Err(FaucetError::Source(
                "stream_pages must not be called on the columnar path".into(),
            ))
        }))
    }

    fn connector_name(&self) -> &'static str {
        "columnar-only-source"
    }

    fn state_key(&self) -> Option<String> {
        Some("columnar-test".to_string())
    }

    fn supports_columnar(&self) -> bool {
        true
    }

    fn stream_batches<'a>(
        &'a self,
        _context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<ColumnarPage, FaucetError>> + Send + 'a>> {
        let batch = values_to_record_batch_inferred(&self.rows).unwrap();
        Box::pin(futures::stream::once(async move {
            Ok(ColumnarPage::new(batch, Some(json!({"done": true}))))
        }))
    }
}

/// A sink that ONLY works columnar: `write_batch` errors, `write_batch_columnar`
/// records the row count.
struct ColumnarOnlySink {
    rows: Arc<AtomicUsize>,
    columnar_calls: Arc<AtomicUsize>,
    /// Records the sink actually received, materialized back to `Value` so a
    /// test can assert on the *content* the columnar path delivered.
    seen: Arc<std::sync::Mutex<Vec<Value>>>,
}

impl ColumnarOnlySink {
    fn new(rows: Arc<AtomicUsize>, columnar_calls: Arc<AtomicUsize>) -> Self {
        Self {
            rows,
            columnar_calls,
            seen: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }
}

#[async_trait]
impl Sink for ColumnarOnlySink {
    async fn write_batch(&self, _records: &[Value]) -> Result<usize, FaucetError> {
        Err(FaucetError::Sink("value path must not be used".into()))
    }

    fn connector_name(&self) -> &'static str {
        "columnar-only-sink"
    }

    fn supports_columnar(&self) -> bool {
        true
    }

    async fn write_batch_columnar(
        &self,
        batch: &arrow::array::RecordBatch,
    ) -> Result<usize, FaucetError> {
        self.columnar_calls.fetch_add(1, Ordering::SeqCst);
        let n = batch.num_rows();
        self.rows.fetch_add(n, Ordering::SeqCst);
        if let Ok(vals) = faucet_core::columnar::record_batch_to_values(batch) {
            self.seen.lock().expect("seen lock").extend(vals);
        }
        Ok(n)
    }
}

#[tokio::test]
async fn pipeline_negotiates_columnar_path_when_both_sides_support_it() {
    let source = ColumnarOnlySource {
        rows: vec![
            json!({"id": 1, "region": "NA"}),
            json!({"id": 2, "region": "EU"}),
            json!({"id": 3, "region": "APAC"}),
        ],
    };
    let rows = Arc::new(AtomicUsize::new(0));
    let columnar_calls = Arc::new(AtomicUsize::new(0));
    let sink = ColumnarOnlySink::new(Arc::clone(&rows), Arc::clone(&columnar_calls));

    // If the pipeline used the Value path, stream_pages / write_batch would
    // error and this would be `Err`.
    let result = Pipeline::new(&source, &sink)
        .run()
        .await
        .expect("columnar path should succeed");

    assert_eq!(
        result.records_written, 3,
        "all rows written via columnar path"
    );
    assert_eq!(rows.load(Ordering::SeqCst), 3);
    assert_eq!(
        columnar_calls.load(Ordering::SeqCst),
        1,
        "exactly one columnar write for the single batch"
    );
    assert_eq!(
        result.bookmark,
        Some(json!({"done": true})),
        "the columnar page's bookmark propagates to the result"
    );
}

/// When the sink does NOT support columnar, the pipeline must fall back to the
/// `Value` path — verified here by a columnar-only *source* (whose `stream_pages`
/// errors) paired with a value-only sink, so the run fails rather than silently
/// mis-routing. This pins the negotiation predicate (both sides required).
#[tokio::test]
async fn pipeline_falls_back_when_sink_lacks_columnar() {
    struct ValueOnlySink;
    #[async_trait]
    impl Sink for ValueOnlySink {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            Ok(records.len())
        }
        fn connector_name(&self) -> &'static str {
            "value-only-sink"
        }
        // supports_columnar() defaults to false.
    }

    let source = ColumnarOnlySource {
        rows: vec![json!({"id": 1})],
    };
    let sink = ValueOnlySink;

    // Sink is not columnar → Value path → source.stream_pages errors.
    let result = Pipeline::new(&source, &sink).run().await;
    assert!(
        result.is_err(),
        "with a non-columnar sink the pipeline must use the Value path (which this source rejects)"
    );
}

/// The columnar loop honors the checkpoint contract: a page carrying a bookmark
/// flushes and persists it to the state store (same write→flush→persist ordering
/// as the `Value` path).
#[tokio::test]
async fn columnar_path_persists_bookmark_to_state_store() {
    use faucet_core::{MemoryStateStore, StateStore};

    let source = ColumnarOnlySource {
        rows: vec![json!({"id": 1}), json!({"id": 2})],
    };
    let sink = ColumnarOnlySink::new(Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());

    let result = Pipeline::new(&source, &sink)
        .with_state_store(Arc::clone(&store))
        .run()
        .await
        .expect("columnar path with a state store should succeed");

    assert_eq!(result.records_written, 2);
    let persisted = store
        .get("columnar-test")
        .await
        .unwrap()
        .map(|v| faucet_core::state_version::peel_versioned(&v));
    assert_eq!(
        persisted,
        Some(json!({"done": true})),
        "the columnar page's bookmark is persisted under the source's state key"
    );
}

/// A source that advertises `supports_columnar` but never overrides
/// `stream_batches` hits the defaulted error stream — proving the default fails
/// loudly rather than silently emitting nothing.
#[tokio::test]
async fn columnar_source_without_stream_batches_override_errors() {
    struct BadColumnarSource;
    #[async_trait]
    impl Source for BadColumnarSource {
        async fn fetch_with_context(
            &self,
            _context: &HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(Vec::new())
        }
        fn connector_name(&self) -> &'static str {
            "bad-columnar-source"
        }
        fn supports_columnar(&self) -> bool {
            true
        }
        // stream_batches intentionally NOT overridden → defaulted error stream.
    }

    let sink = ColumnarOnlySink::new(Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let result = Pipeline::new(&BadColumnarSource, &sink).run().await;
    match result {
        Err(FaucetError::Source(msg)) => {
            assert!(
                msg.contains("does not support columnar streaming"),
                "expected the defaulted unsupported-error, got: {msg}"
            );
        }
        other => panic!("expected a Source error from the default stream_batches, got: {other:?}"),
    }
}

// ── Governance on the columnar path (#636) ──────────────────────────────────
//
// The mocks are columnar-only, so a run that *succeeds* proves the columnar
// path was taken. That is what makes these assertions meaningful: governance is
// not merely "configured", it ran without the pipeline falling back to `Value`.

#[cfg(feature = "masking")]
#[tokio::test]
async fn masking_runs_on_the_columnar_path_and_the_sink_sees_masked_values() {
    use faucet_core::masking::{CompiledMasking, MaskAction, MaskRule, MaskingSpec, MatchSpec};

    let source = ColumnarOnlySource {
        rows: vec![
            json!({"id": 1, "email": "ada@example.com"}),
            json!({"id": 2, "email": "grace@example.com"}),
        ],
    };
    let rows = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let sink = ColumnarOnlySink::new(Arc::clone(&rows), Arc::clone(&calls));
    let seen = Arc::clone(&sink.seen);

    let spec = MaskingSpec {
        description: None,
        key: None,
        rules: vec![MaskRule {
            name: Some("email".into()),
            matcher: MatchSpec {
                fields: vec!["email".into()],
                ..Default::default()
            },
            action: MaskAction::Redact { mask: json!("***") },
            applies_to: vec![],
        }],
    };
    let masking = Arc::new(CompiledMasking::compile(&spec).expect("compile masking"));

    let result = Pipeline::new(&source, &sink)
        .with_masking(masking)
        .run()
        .await
        .expect("masking must not disqualify the columnar path");

    assert_eq!(result.records_written, 2);
    let got = seen.lock().unwrap().clone();
    assert_eq!(got.len(), 2);
    for r in &got {
        assert_eq!(
            r["email"],
            json!("***"),
            "PII must be masked before the sink: {r}"
        );
    }
    assert_eq!(got[0]["id"], json!(1), "untouched columns survive");
}

/// Masking a numeric column turns its values into strings; the columnar path
/// widens that column to Utf8 instead of failing to re-encode the page.
#[cfg(feature = "masking")]
#[tokio::test]
async fn masking_a_numeric_column_on_the_columnar_path_widens_it() {
    use faucet_core::masking::{CompiledMasking, MaskAction, MaskRule, MaskingSpec, MatchSpec};

    let source = ColumnarOnlySource {
        rows: vec![
            json!({"id": 1, "acct": 4111111111111111u64, "n": 7}),
            json!({"id": 2, "acct": 5500000000000004u64, "n": 8}),
        ],
    };
    let sink = ColumnarOnlySink::new(Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let seen = Arc::clone(&sink.seen);
    let rule = |matcher: MatchSpec| MaskRule {
        name: None,
        matcher,
        action: MaskAction::Hash,
        applies_to: vec![],
    };
    let spec = MaskingSpec {
        description: None,
        key: None,
        rules: vec![
            rule(MatchSpec {
                fields: vec!["id".into()],
                ..Default::default()
            }),
            rule(MatchSpec {
                field_pattern: Some("^acct$".into()),
                ..Default::default()
            }),
        ],
    };
    let result = Pipeline::new(&source, &sink)
        .with_masking(Arc::new(CompiledMasking::compile(&spec).unwrap()))
        .run()
        .await
        .expect("a masked numeric column must not fail the page");
    assert_eq!(result.records_written, 2);
    let got = seen.lock().unwrap().clone();
    for r in &got {
        assert!(r["id"].is_string(), "{r}");
        assert!(r["acct"].is_string(), "{r}");
        assert!(r["n"].is_number(), "an unmasked column keeps its type: {r}");
    }
}

/// A string-only value detector cannot re-type a column, so the run stays on
/// the columnar path (the columnar-only mocks would fail otherwise).
#[cfg(feature = "masking")]
#[tokio::test]
async fn a_string_only_value_detector_keeps_the_columnar_path() {
    use faucet_core::masking::{
        CompiledMasking, Detector, MaskAction, MaskRule, MaskingSpec, MatchSpec,
    };

    let source = ColumnarOnlySource {
        rows: vec![json!({"id": 1, "contact": "ada@example.com"})],
    };
    let sink = ColumnarOnlySink::new(Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let seen = Arc::clone(&sink.seen);
    let spec = MaskingSpec {
        description: None,
        key: None,
        rules: vec![MaskRule {
            name: None,
            matcher: MatchSpec {
                value_detector: Some(Detector::Email),
                ..Default::default()
            },
            action: MaskAction::Hash,
            applies_to: vec![],
        }],
    };
    Pipeline::new(&source, &sink)
        .with_masking(Arc::new(CompiledMasking::compile(&spec).unwrap()))
        .run()
        .await
        .expect("an email detector must not disqualify the columnar path");
    let got = seen.lock().unwrap().clone();
    assert_eq!(got[0]["id"], json!(1));
    assert_ne!(got[0]["contact"], json!("ada@example.com"));
}

/// A value detector that matches integers (`credit_card`) would widen a
/// numeric column only from the first page holding a card number, so the
/// sink would see Int64 on page 1 and Utf8 on page 2 (#862 M-016). Such a run
/// takes the row path; both columnar entry points stay unused.
#[cfg(feature = "masking")]
#[tokio::test]
async fn a_number_matching_value_detector_takes_the_row_path() {
    use faucet_core::masking::{
        CompiledMasking, Detector, MaskAction, MaskRule, MaskingSpec, MatchSpec,
    };

    struct DualSource {
        batch_calls: Arc<AtomicUsize>,
    }
    fn pages() -> Vec<Vec<Value>> {
        vec![
            vec![json!({"id": 1, "acct": 12345u64})],
            vec![json!({"id": 2, "acct": 4111111111111111u64})],
        ]
    }
    #[async_trait]
    impl Source for DualSource {
        async fn fetch_with_context(
            &self,
            _c: &HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(pages().concat())
        }
        fn stream_pages<'a>(
            &'a self,
            _c: &'a HashMap<String, Value>,
            _b: usize,
        ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
            let n = pages().len();
            Box::pin(futures::stream::iter(pages().into_iter().enumerate().map(
                move |(i, records)| {
                    Ok(StreamPage {
                        records,
                        bookmark: (i + 1 == n).then(|| json!({"done": true})),
                    })
                },
            )))
        }
        fn supports_columnar(&self) -> bool {
            true
        }
        fn stream_batches<'a>(
            &'a self,
            _c: &'a HashMap<String, Value>,
            _b: usize,
        ) -> Pin<Box<dyn Stream<Item = Result<ColumnarPage, FaucetError>> + Send + 'a>> {
            self.batch_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(futures::stream::iter(pages().into_iter().map(|rows| {
                Ok(ColumnarPage::new(
                    values_to_record_batch_inferred(&rows).unwrap(),
                    None,
                ))
            })))
        }
    }
    struct DualSink {
        columnar_calls: Arc<AtomicUsize>,
        seen: Arc<std::sync::Mutex<Vec<Value>>>,
    }
    #[async_trait]
    impl Sink for DualSink {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            self.seen.lock().unwrap().extend_from_slice(records);
            Ok(records.len())
        }
        fn supports_columnar(&self) -> bool {
            true
        }
        async fn write_batch_columnar(
            &self,
            batch: &arrow::array::RecordBatch,
        ) -> Result<usize, FaucetError> {
            self.columnar_calls.fetch_add(1, Ordering::SeqCst);
            Ok(batch.num_rows())
        }
    }

    let batch_calls = Arc::new(AtomicUsize::new(0));
    let columnar_calls = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let source = DualSource {
        batch_calls: Arc::clone(&batch_calls),
    };
    let sink = DualSink {
        columnar_calls: Arc::clone(&columnar_calls),
        seen: Arc::clone(&seen),
    };
    let spec = MaskingSpec {
        description: None,
        key: None,
        rules: vec![MaskRule {
            name: None,
            matcher: MatchSpec {
                value_detector: Some(Detector::CreditCard),
                ..Default::default()
            },
            action: MaskAction::Hash,
            applies_to: vec![],
        }],
    };
    let result = Pipeline::new(&source, &sink)
        .with_masking(Arc::new(CompiledMasking::compile(&spec).unwrap()))
        .run()
        .await
        .expect("row-path run");

    assert_eq!(result.records_written, 2);
    assert_eq!(
        batch_calls.load(Ordering::SeqCst),
        0,
        "stream_batches unused"
    );
    assert_eq!(
        columnar_calls.load(Ordering::SeqCst),
        0,
        "write_batch_columnar unused"
    );
    let got = seen.lock().unwrap().clone();
    for column in ["id", "acct"] {
        assert!(
            got.iter().all(|r| r[column].is_string()),
            "`{column}` keeps one JSON type on every page: {got:?}"
        );
    }
    assert_eq!(
        got[0]["acct"],
        json!("12345"),
        "a non-card value keeps its digits"
    );
    assert_ne!(
        got[1]["acct"],
        json!("4111111111111111"),
        "the card number is masked"
    );
}

/// A non-quarantining quality policy (`abort`) must run *on* the columnar path
/// and stop the run — proving the pass executed rather than being skipped.
#[cfg(feature = "quality")]
#[tokio::test]
async fn a_quality_abort_fires_on_the_columnar_path() {
    use faucet_core::quality::{CompiledQuality, OnFailure, QualitySpec, RecordCheck};

    let source = ColumnarOnlySource {
        rows: vec![json!({"id": 1, "email": null})],
    };
    let rows = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let sink = ColumnarOnlySink::new(Arc::clone(&rows), Arc::clone(&calls));

    let spec = QualitySpec {
        record: vec![RecordCheck::NotNull {
            field: "email".into(),
            treat_missing_as_null: true,
            on_failure: OnFailure::Abort,
        }],
        batch: vec![],
    };
    let quality = Arc::new(CompiledQuality::compile(&spec).expect("compile quality"));

    let err = Pipeline::new(&source, &sink)
        .with_quality(quality)
        .run()
        .await
        .expect_err("the abort check must fail the run");
    assert!(
        matches!(err, FaucetError::QualityFailure { .. }),
        "quality must run on the columnar path; got {err:?}"
    );
    assert_eq!(
        rows.load(Ordering::SeqCst),
        0,
        "an abort writes nothing from the page"
    );
}

/// Quarantine now runs **on** the columnar path: the offending rows go to the
/// DLQ, the survivors are written, and the run stays green. With columnar-only
/// mocks a successful run is itself proof the fast path was taken.
#[cfg(feature = "quality")]
#[tokio::test]
async fn a_quarantining_quality_policy_routes_to_the_dlq_on_the_columnar_path() {
    use faucet_core::dlq::DlqConfig;
    use faucet_core::quality::{CompiledQuality, OnFailure, QualitySpec, RecordCheck};

    struct CapturingDlq {
        seen: Arc<std::sync::Mutex<Vec<Value>>>,
    }
    #[async_trait]
    impl Sink for CapturingDlq {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            self.seen.lock().unwrap().extend_from_slice(records);
            Ok(records.len())
        }
        fn connector_name(&self) -> &'static str {
            "capturing-dlq"
        }
    }

    let source = ColumnarOnlySource {
        rows: vec![
            json!({"id": 1, "email": "ada@example.com"}),
            json!({"id": 2, "email": null}),
            json!({"id": 3, "email": "hopper@example.com"}),
        ],
    };
    let rows = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let sink = ColumnarOnlySink::new(Arc::clone(&rows), Arc::clone(&calls));
    let seen = Arc::clone(&sink.seen);
    let dlq_seen = Arc::new(std::sync::Mutex::new(Vec::new()));

    let spec = QualitySpec {
        record: vec![RecordCheck::NotNull {
            field: "email".into(),
            treat_missing_as_null: true,
            on_failure: OnFailure::Quarantine,
        }],
        batch: vec![],
    };
    let quality = Arc::new(CompiledQuality::compile(&spec).expect("compile quality"));

    let result = Pipeline::new(&source, &sink)
        .with_quality(quality)
        .with_dlq(DlqConfig::new(Arc::new(CapturingDlq {
            seen: Arc::clone(&dlq_seen),
        })))
        .run()
        .await
        .expect("quarantine must not fail the run on the columnar path");

    // Two survivors written columnar, one row dead-lettered.
    assert_eq!(result.records_written, 2);
    let written = seen.lock().unwrap().clone();
    assert_eq!(written.len(), 2);
    assert!(
        written.iter().all(|r| r["email"] != json!(null)),
        "the offending row must not reach the sink: {written:?}"
    );
    let envelopes = dlq_seen.lock().unwrap().clone();
    assert_eq!(envelopes.len(), 1, "exactly one envelope: {envelopes:?}");
    assert_eq!(
        result.dlq.map(|d| d.records_dlq),
        Some(1),
        "the run result reports the DLQ count"
    );
}

/// The per-page DLQ budget is enforced on the columnar path too — and the
/// overshoot is still **written** before the run aborts, so quarantined rows
/// are never dropped on the way out.
#[cfg(feature = "quality")]
#[tokio::test]
async fn the_dlq_budget_aborts_on_the_columnar_path_after_writing_the_envelopes() {
    use faucet_core::dlq::DlqConfig;
    use faucet_core::quality::{CompiledQuality, OnFailure, QualitySpec, RecordCheck};

    struct CapturingDlq {
        seen: Arc<std::sync::Mutex<Vec<Value>>>,
    }
    #[async_trait]
    impl Sink for CapturingDlq {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            self.seen.lock().unwrap().extend_from_slice(records);
            Ok(records.len())
        }
        fn connector_name(&self) -> &'static str {
            "capturing-dlq"
        }
    }

    let source = ColumnarOnlySource {
        rows: vec![
            json!({"id": 1, "email": null}),
            json!({"id": 2, "email": null}),
        ],
    };
    let sink = ColumnarOnlySink::new(Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let dlq_seen = Arc::new(std::sync::Mutex::new(Vec::new()));

    let spec = QualitySpec {
        record: vec![RecordCheck::NotNull {
            field: "email".into(),
            treat_missing_as_null: true,
            on_failure: OnFailure::Quarantine,
        }],
        batch: vec![],
    };
    let mut dlq = DlqConfig::new(Arc::new(CapturingDlq {
        seen: Arc::clone(&dlq_seen),
    }));
    dlq.max_failures_per_page = Some(1); // 2 quarantined > 1 → trips

    let err = Pipeline::new(&source, &sink)
        .with_quality(Arc::new(CompiledQuality::compile(&spec).unwrap()))
        .with_dlq(dlq)
        .run()
        .await
        .expect_err("exceeding the budget must abort");
    assert!(err.to_string().contains("budget exceeded"), "{err}");
    assert_eq!(
        dlq_seen.lock().unwrap().len(),
        2,
        "the overshoot is written before aborting — losing those rows would be worse"
    );
}

/// The governance result must be identical whichever path ran it. Same records,
/// same masking policy, through the columnar mocks and through `Value`-capable
/// ones — the outputs must match exactly.
#[cfg(feature = "masking")]
#[tokio::test]
async fn columnar_governance_matches_the_value_path() {
    use faucet_core::masking::{CompiledMasking, MaskAction, MaskRule, MaskingSpec, MatchSpec};

    /// A source/sink pair that works on the `Value` path only.
    struct ValueSource {
        rows: Vec<Value>,
    }
    #[async_trait]
    impl Source for ValueSource {
        async fn fetch_with_context(
            &self,
            _c: &HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(self.rows.clone())
        }
    }
    struct ValueSink {
        seen: Arc<std::sync::Mutex<Vec<Value>>>,
    }
    #[async_trait]
    impl Sink for ValueSink {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            self.seen.lock().unwrap().extend_from_slice(records);
            Ok(records.len())
        }
        fn connector_name(&self) -> &'static str {
            "value-sink"
        }
    }

    let records = vec![
        json!({"id": 1, "email": "ada@example.com", "note": "keep"}),
        json!({"id": 2, "email": "grace@example.com", "note": "keep"}),
    ];
    let spec = MaskingSpec {
        description: None,
        key: None,
        rules: vec![MaskRule {
            name: Some("email".into()),
            matcher: MatchSpec {
                fields: vec!["email".into()],
                ..Default::default()
            },
            action: MaskAction::Redact { mask: json!("***") },
            applies_to: vec![],
        }],
    };

    // Columnar path.
    let c_sink =
        ColumnarOnlySink::new(Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let c_seen = Arc::clone(&c_sink.seen);
    Pipeline::new(
        &ColumnarOnlySource {
            rows: records.clone(),
        },
        &c_sink,
    )
    .with_masking(Arc::new(CompiledMasking::compile(&spec).unwrap()))
    .run()
    .await
    .expect("columnar run");

    // Value path.
    let v_seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    Pipeline::new(
        &ValueSource {
            rows: records.clone(),
        },
        &ValueSink {
            seen: Arc::clone(&v_seen),
        },
    )
    .with_masking(Arc::new(CompiledMasking::compile(&spec).unwrap()))
    .run()
    .await
    .expect("value run");

    assert_eq!(
        c_seen.lock().unwrap().clone(),
        v_seen.lock().unwrap().clone(),
        "governance output must be identical across paths"
    );
}

/// A DLQ in front of a sink that reports per-row outcomes keeps the run on the
/// row path: the one rejected row goes to the DLQ and the rest are written,
/// exactly as when the pair could not speak Arrow.
#[tokio::test]
async fn a_dlq_before_a_per_row_sink_keeps_rejected_rows_out_of_the_columnar_path() {
    use faucet_core::dlq::{BatchAtomicity, DlqConfig};

    struct PerRowSink {
        columnar_calls: AtomicUsize,
        written: std::sync::Mutex<Vec<Value>>,
    }
    #[async_trait]
    impl Sink for PerRowSink {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            self.written.lock().unwrap().extend_from_slice(records);
            Ok(records.len())
        }
        async fn write_batch_partial(
            &self,
            records: &[Value],
        ) -> Result<Vec<faucet_core::RowOutcome>, FaucetError> {
            let mut out = Vec::new();
            for r in records {
                if r["id"] == json!(2) {
                    out.push(Err(FaucetError::Sink("rejected".into())));
                } else {
                    self.written.lock().unwrap().push(r.clone());
                    out.push(Ok(()));
                }
            }
            Ok(out)
        }
        fn batch_atomicity(&self) -> BatchAtomicity {
            BatchAtomicity::PerRow
        }
        fn supports_columnar(&self) -> bool {
            true
        }
        async fn write_batch_columnar(
            &self,
            _batch: &arrow::array::RecordBatch,
        ) -> Result<usize, FaucetError> {
            self.columnar_calls.fetch_add(1, Ordering::SeqCst);
            Err(FaucetError::Sink(
                "one row rejected; the whole load fails".into(),
            ))
        }
    }
    struct Both(Vec<Value>);
    #[async_trait]
    impl Source for Both {
        async fn fetch_with_context(
            &self,
            _c: &HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(self.0.clone())
        }
        fn supports_columnar(&self) -> bool {
            true
        }
        fn stream_batches<'a>(
            &'a self,
            _context: &'a HashMap<String, Value>,
            _batch_size: usize,
        ) -> Pin<Box<dyn Stream<Item = Result<ColumnarPage, FaucetError>> + Send + 'a>> {
            let batch = values_to_record_batch_inferred(&self.0).unwrap();
            Box::pin(futures::stream::once(async move {
                Ok(ColumnarPage::new(batch, None))
            }))
        }
    }
    struct Dlq(std::sync::Mutex<Vec<Value>>);
    #[async_trait]
    impl Sink for Dlq {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            self.0.lock().unwrap().extend_from_slice(records);
            Ok(records.len())
        }
    }

    let source = Both(vec![json!({"id": 1}), json!({"id": 2}), json!({"id": 3})]);
    let sink = PerRowSink {
        columnar_calls: AtomicUsize::new(0),
        written: std::sync::Mutex::new(Vec::new()),
    };
    let dlq = Arc::new(Dlq(std::sync::Mutex::new(Vec::new())));
    let result = Pipeline::new(&source, &sink)
        .with_dlq(DlqConfig::new(Arc::clone(&dlq) as Arc<dyn Sink>))
        .run()
        .await
        .expect("the rejected row is dead-lettered, not fatal");
    assert_eq!(sink.columnar_calls.load(Ordering::SeqCst), 0);
    assert_eq!(result.records_written, 2);
    assert_eq!(
        *sink.written.lock().unwrap(),
        vec![json!({"id": 1}), json!({"id": 3})]
    );
    let envelopes = dlq.0.lock().unwrap().clone();
    assert_eq!(envelopes.len(), 1);
    assert_eq!(envelopes[0]["payload"], json!({"id": 2}));
}
