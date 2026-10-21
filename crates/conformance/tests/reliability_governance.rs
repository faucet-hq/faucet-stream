//! RFC 0013 — the governance the engine assembles itself.
//!
//! - #870: the DLQ is a governed destination. A row quarantined by *any* pass
//!   is policy-checked against the DLQ's own attributes, counted against the
//!   run budget, metered, and its write retried under `resilience:`.
//! - #879: every row offered to a sink is accounted for. A sink that silently
//!   drops a row fails the run; one that merges rows by key, or declines rows
//!   by design and says so, does not.
//! - #873: a `policy:` quarantine gives the same quarantined set whichever
//!   write path the connectors could take — the run takes the row path.
//!
//! Each test runs the real `Pipeline::run` with governance attached through
//! `Pipeline::with_governance` and a destination wrapped by
//! `Governance::sink_builder`, exactly as every CLI entry point does.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::RecordBatch;
use faucet_core::columnar::ColumnarPage;
use faucet_core::dlq::DlqConfig;
use faucet_core::{
    BudgetSpec, CompiledPolicy, CompiledQuality, DlqGovernance, FaucetError, Governance, Pipeline,
    PolicyGovernance, PolicyScope, RowAccounts, RowOutcome, Sink, SinkFacts, Source, StreamPage,
    UsageMeter, Value, WriteReport, async_trait, json,
};

/// A source of fixed rows; optionally columnar.
struct Rows {
    rows: Vec<Value>,
    columnar: bool,
}

impl Rows {
    fn new(rows: Vec<Value>) -> Self {
        Self {
            rows,
            columnar: false,
        }
    }
}

#[async_trait]
impl Source for Rows {
    async fn fetch_with_context(
        &self,
        _ctx: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        Ok(self.rows.clone())
    }

    fn stream_pages<'a>(
        &'a self,
        _ctx: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> std::pin::Pin<
        Box<dyn faucet_core::Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>,
    > {
        let page = StreamPage {
            records: self.rows.clone(),
            bookmark: Some(json!({"done": true})),
        };
        Box::pin(futures::stream::iter(vec![Ok(page)]))
    }

    fn supports_columnar(&self) -> bool {
        self.columnar
    }

    fn stream_batches<'a>(
        &'a self,
        _ctx: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> std::pin::Pin<
        Box<dyn faucet_core::Stream<Item = Result<ColumnarPage, FaucetError>> + Send + 'a>,
    > {
        let batch =
            faucet_core::columnar::values_to_record_batch_inferred(&self.rows).map(|batch| {
                ColumnarPage {
                    batch,
                    bookmark: Some(json!({"done": true})),
                }
            });
        Box::pin(futures::stream::iter(vec![batch]))
    }
}

/// What a test sink does with each row it is offered.
#[derive(Clone, Copy, PartialEq)]
enum Mode {
    /// Write every row.
    All,
    /// Fail every row per row (`write_batch_partial`).
    RejectRows,
    /// Report one row fewer than offered, with no per-row outcome.
    DropOne,
    /// Merge rows by key: report half the rows written (keyed upsert).
    Keyed,
    /// Decline one row by design and report it as skipped.
    SkipOne,
}

struct Capture {
    mode: Mode,
    rows: Mutex<Vec<Value>>,
    columnar_calls: AtomicUsize,
    columnar: bool,
}

impl Capture {
    fn new(mode: Mode) -> Arc<Self> {
        Arc::new(Self {
            mode,
            rows: Mutex::new(Vec::new()),
            columnar_calls: AtomicUsize::new(0),
            columnar: false,
        })
    }

    fn columnar(mode: Mode) -> Arc<Self> {
        Arc::new(Self {
            mode,
            rows: Mutex::new(Vec::new()),
            columnar_calls: AtomicUsize::new(0),
            columnar: true,
        })
    }

    fn rows(&self) -> Vec<Value> {
        self.rows.lock().unwrap().clone()
    }
}

#[async_trait]
impl Sink for Capture {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        self.rows.lock().unwrap().extend(records.iter().cloned());
        Ok(match self.mode {
            Mode::DropOne | Mode::SkipOne => records.len().saturating_sub(1),
            Mode::Keyed => records.len().div_ceil(2),
            _ => records.len(),
        })
    }

    async fn write_batch_reported(&self, records: &[Value]) -> Result<WriteReport, FaucetError> {
        let n = self.write_batch(records).await?;
        Ok(match self.mode {
            Mode::SkipOne => WriteReport::accepted(n).with_skipped(records.len() - n),
            Mode::Keyed => WriteReport::keyed(records.len(), n),
            _ => WriteReport::accepted(n),
        })
    }

    async fn write_batch_partial(&self, records: &[Value]) -> Result<Vec<RowOutcome>, FaucetError> {
        if self.mode == Mode::RejectRows {
            return Ok(records
                .iter()
                .map(|_| Err(FaucetError::Sink("row rejected".into())))
                .collect());
        }
        self.write_batch_reported(records)
            .await?
            .check(records.len())?;
        Ok(records.iter().map(|_| Ok(())).collect())
    }

    fn dedups_by_key(&self) -> bool {
        self.mode == Mode::Keyed
    }

    fn supports_columnar(&self) -> bool {
        self.columnar
    }

    async fn write_batch_columnar(&self, batch: &RecordBatch) -> Result<usize, FaucetError> {
        self.columnar_calls.fetch_add(1, Ordering::Relaxed);
        let rows = faucet_core::columnar::record_batch_to_values(batch)?;
        self.rows.lock().unwrap().extend(rows);
        Ok(batch.num_rows())
    }
}

/// A shared sink as an owned one, so the test keeps a handle to read back.
struct Shared(Arc<Capture>);

#[async_trait]
impl Sink for Shared {
    faucet_core::forward_sink!(0);
}

/// A DLQ that fails its first write with a retriable timeout.
struct FlakyDlq {
    failed: AtomicBool,
    attempts: AtomicUsize,
    replay_safe: bool,
    rows: Mutex<Vec<Value>>,
}

#[async_trait]
impl Sink for FlakyDlq {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        self.attempts.fetch_add(1, Ordering::Relaxed);
        if !self.failed.swap(true, Ordering::Relaxed) {
            return Err(FaucetError::Timeout {
                operation: "dlq append".into(),
                after: std::time::Duration::from_millis(1),
            });
        }
        self.rows.lock().unwrap().extend(records.iter().cloned());
        Ok(records.len())
    }

    fn write_batch_is_replay_safe(&self) -> bool {
        self.replay_safe
    }
}

fn policy() -> Arc<CompiledPolicy> {
    let spec = serde_json::from_value(json!({
        "classifications": [{"label": "pii", "value_detector": "email"}],
        "rules": [{"name": "pii-eu", "when": {"label": "pii"},
                   "require": {"residency": ["eu"]}, "on_runtime": "quarantine"}]
    }))
    .unwrap();
    Arc::new(CompiledPolicy::compile(&spec).unwrap())
}

fn facts(id: &str, residency: &str) -> SinkFacts {
    SinkFacts {
        id: id.into(),
        kind: "test".into(),
        attributes: [("residency".to_string(), residency.to_string())].into(),
    }
}

fn scope() -> PolicyScope {
    PolicyScope {
        pipeline: "p".into(),
        row: "r".into(),
    }
}

fn quality_quarantine_null_id() -> Arc<CompiledQuality> {
    let spec = serde_json::from_value(json!({
        "record": [{"type": "not_null", "field": "id", "on_failure": "quarantine"}]
    }))
    .unwrap();
    Arc::new(CompiledQuality::compile(&spec).unwrap())
}

fn dlq_of(sink: Arc<Capture>, residency: Option<&str>) -> DlqGovernance {
    let d = DlqGovernance::new(DlqConfig::new(Arc::new(Shared(sink))));
    match residency {
        Some(r) => d.with_facts(facts("dlq", r)),
        None => d,
    }
}

/// #870: a row the *quality* pass quarantines carries an email the policy
/// confines to the EU. A DLQ in the US must not receive it: the run fails and
/// the DLQ stays empty. The same run against an EU DLQ succeeds.
#[tokio::test]
async fn a_quality_quarantine_never_lands_a_label_the_dlq_may_not_hold() {
    for (dlq_residency, refused) in [("us", true), ("eu", false)] {
        let source = Rows::new(vec![
            json!({"id": null, "email": "a@b.co"}),
            json!({"id": 1, "note": "plain"}),
        ]);
        let main = Capture::new(Mode::All);
        let dlq = Capture::new(Mode::All);
        let g = Governance::new()
            .with_quality(quality_quarantine_null_id())
            .with_policy(PolicyGovernance::new(
                policy(),
                facts("main", "eu"),
                scope(),
            ))
            .with_dlq(dlq_of(Arc::clone(&dlq), Some(dlq_residency)));
        let governed = g.sink_builder().build(Box::new(Shared(Arc::clone(&main))));
        let result = Pipeline::new(&source, &governed)
            .with_governance(g)
            .run()
            .await;
        if refused {
            let err = result.unwrap_err();
            assert!(
                matches!(err, FaucetError::PolicyViolation { ref rule, .. } if rule == "pii-eu"),
                "{err}"
            );
            assert!(dlq.rows().is_empty(), "nothing reached the US DLQ");
        } else {
            result.unwrap();
            assert_eq!(dlq.rows().len(), 1);
            assert_eq!(dlq.rows()[0]["payload"]["email"], "a@b.co");
        }
    }
}

/// #870: a row the sink rejects per row goes to the DLQ through the same
/// check — the path never asked about before.
#[tokio::test]
async fn a_per_row_sink_failure_is_policy_checked_at_the_dlq() {
    let source = Rows::new(vec![json!({"id": 1, "email": "a@b.co"})]);
    let main = Capture::new(Mode::RejectRows);
    let dlq = Capture::new(Mode::All);
    let g = Governance::new()
        .with_policy(PolicyGovernance::new(
            policy(),
            facts("main", "eu"),
            scope(),
        ))
        .with_dlq(dlq_of(Arc::clone(&dlq), Some("us")));
    let governed = g.sink_builder().build(Box::new(Shared(main)));
    let err = Pipeline::new(&source, &governed)
        .with_governance(g)
        .run()
        .await
        .unwrap_err();
    assert!(matches!(err, FaucetError::PolicyViolation { .. }), "{err}");
    assert!(dlq.rows().is_empty());
}

/// #870: DLQ envelopes are metered into the usage snapshot and count against
/// the same budget as the main sink.
#[tokio::test]
async fn dlq_records_and_bytes_are_metered_and_budgeted() {
    let source = Rows::new(vec![json!({"id": 1}), json!({"id": 2})]);
    let main = Capture::new(Mode::RejectRows);
    let dlq = Capture::new(Mode::All);
    let meter = Arc::new(UsageMeter::new());
    let g = Governance::new()
        .with_usage_meter(Arc::clone(&meter))
        .with_budget(BudgetSpec {
            max_records: Some(10),
            ..Default::default()
        })
        .with_dlq(dlq_of(Arc::clone(&dlq), None));
    let governed = g.sink_builder().build(Box::new(Shared(main)));
    let state = Arc::clone(governed.budget_state().unwrap());
    let r = Pipeline::new(&source, &governed)
        .with_governance(g)
        .run()
        .await
        .unwrap();
    assert_eq!(r.dlq.unwrap().records_dlq, 2);
    let usage = meter.snapshot();
    assert_eq!(usage.dlq_records_written, 2);
    assert!(usage.dlq_bytes_written > 0);
    assert_eq!(
        state.records(),
        2,
        "the DLQ envelopes count against the budget"
    );
}

/// #870: a budget the DLQ alone crosses stops the run like a main-sink
/// crossing.
#[tokio::test]
async fn a_budget_crossed_by_the_dlq_alone_stops_the_run() {
    let source = Rows::new(vec![json!({"id": 1}), json!({"id": 2}), json!({"id": 3})]);
    let main = Capture::new(Mode::RejectRows);
    let dlq = Capture::new(Mode::All);
    let g = Governance::new()
        .with_budget(BudgetSpec {
            max_records: Some(2),
            ..Default::default()
        })
        .with_dlq(dlq_of(Arc::clone(&dlq), None));
    let governed = g.sink_builder().build(Box::new(Shared(main)));
    let err = Pipeline::new(&source, &governed)
        .with_governance(g)
        .run()
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            FaucetError::BudgetExceeded {
                limit: 2,
                actual: 3,
                ..
            }
        ),
        "{err}"
    );
    assert!(dlq.rows().is_empty(), "the crossing page is refused whole");
}

/// #870: a transient DLQ write failure is retried under `resilience:` when
/// the DLQ's write is replay-safe, and fails the run when it is not.
#[tokio::test]
async fn a_transient_dlq_failure_is_retried_under_resilience() {
    for replay_safe in [true, false] {
        let source = Rows::new(vec![json!({"id": 1})]);
        let main = Capture::new(Mode::RejectRows);
        let dlq = Arc::new(FlakyDlq {
            failed: AtomicBool::new(false),
            attempts: AtomicUsize::new(0),
            replay_safe,
            rows: Mutex::new(Vec::new()),
        });
        let retry = faucet_core::resilience::RetryPolicy {
            max_attempts: 3,
            base: std::time::Duration::from_millis(1),
            max: std::time::Duration::from_millis(2),
            jitter: false,
            ..Default::default()
        };
        let g = Governance::new()
            .with_resilience(faucet_core::resilience::ResiliencePolicy {
                retry,
                ..Default::default()
            })
            .with_dlq(DlqGovernance::new(DlqConfig::new(
                Arc::clone(&dlq) as Arc<dyn Sink>
            )));
        let governed = g.sink_builder().build(Box::new(Shared(main)));
        let result = Pipeline::new(&source, &governed)
            .with_governance(g)
            .run()
            .await;
        if replay_safe {
            result.unwrap();
            assert_eq!(dlq.attempts.load(Ordering::Relaxed), 2);
            assert_eq!(dlq.rows.lock().unwrap().len(), 1);
        } else {
            assert!(
                result.is_err(),
                "a non-replay-safe DLQ write is not retried"
            );
            assert_eq!(dlq.attempts.load(Ordering::Relaxed), 1);
        }
    }
}

/// #879: a sink that reports fewer rows than offered — no per-row outcome, no
/// coalescing — fails the run before the bookmark advances, on the plain
/// write path and on the per-row (DLQ) path.
#[tokio::test]
async fn a_sink_that_silently_drops_a_row_fails_the_run() {
    for with_dlq in [false, true] {
        let source = Rows::new(vec![json!({"id": 1}), json!({"id": 2})]);
        let main = Capture::new(Mode::DropOne);
        let accounts = Arc::new(RowAccounts::new());
        let mut g = Governance::new();
        if with_dlq {
            g = g.with_dlq(dlq_of(Capture::new(Mode::All), None));
        }
        let governed = g.sink_builder().build(Box::new(Shared(main)));
        let store = Arc::new(faucet_core::MemoryStateStore::new());
        let err = Pipeline::new(&source, &governed)
            .with_governance(g)
            .with_row_accounts(Arc::clone(&accounts))
            .with_state_store(store)
            .run()
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                FaucetError::RowsUnaccounted {
                    offered: 2,
                    accepted: 1
                }
            ),
            "{err}"
        );
        if !with_dlq {
            let s = accounts.snapshot();
            assert_eq!((s.offered, s.accepted, s.unaccounted), (2, 1, 1));
            assert!(!s.balanced());
        }
    }
}

/// #879: no false positives — a sink that merges rows by key, or declines a
/// row by design and reports it, balances.
#[tokio::test]
async fn coalesced_and_skipped_rows_balance() {
    for (mode, coalesced, skipped) in [(Mode::Keyed, 2, 0), (Mode::SkipOne, 0, 1)] {
        let source = Rows::new((0..4).map(|i| json!({"id": i})).collect());
        let accounts = Arc::new(RowAccounts::new());
        let g = Governance::new();
        let governed = g.sink_builder().build(Box::new(Shared(Capture::new(mode))));
        Pipeline::new(&source, &governed)
            .with_governance(g)
            .with_row_accounts(Arc::clone(&accounts))
            .run()
            .await
            .unwrap();
        let s = accounts.snapshot();
        assert_eq!(s.offered, 4);
        assert_eq!((s.coalesced, s.skipped), (coalesced, skipped));
        assert!(s.balanced(), "{s:?}");
    }
}

/// #879: quarantined and DLQ'd rows count, so a page with both balances.
#[tokio::test]
async fn quarantined_and_failed_rows_are_accounted() {
    let source = Rows::new(vec![
        json!({"id": null}),
        json!({"id": 1}),
        json!({"id": 2}),
    ]);
    let accounts = Arc::new(RowAccounts::new());
    let g = Governance::new()
        .with_quality(quality_quarantine_null_id())
        .with_dlq(dlq_of(Capture::new(Mode::All), None));
    let governed = g
        .sink_builder()
        .build(Box::new(Shared(Capture::new(Mode::RejectRows))));
    Pipeline::new(&source, &governed)
        .with_governance(g)
        .with_row_accounts(Arc::clone(&accounts))
        .run()
        .await
        .unwrap();
    let s = accounts.snapshot();
    assert_eq!(
        (s.offered, s.quarantined, s.failed, s.accepted),
        (3, 1, 2, 0)
    );
    assert!(s.balanced());
}

/// #873: the same input gives the same quarantined set whether the
/// connectors could take the columnar path or not — a policy quarantine makes
/// the run take the row path, where quarantine is per row.
#[tokio::test]
async fn policy_quarantine_is_per_row_on_a_columnar_capable_pair() {
    let rows = vec![
        json!({"id": 1, "contact": "a@b.co"}),
        json!({"id": 2, "contact": "none"}),
        json!({"id": 3, "contact": "c@d.co"}),
    ];
    let mut outcomes = Vec::new();
    for columnar in [true, false] {
        let source = Rows {
            rows: rows.clone(),
            columnar,
        };
        let main = Capture::columnar(Mode::All);
        let dlq = Capture::new(Mode::All);
        let g = Governance::new()
            .with_policy(PolicyGovernance::new(
                policy(),
                facts("main", "us"),
                scope(),
            ))
            .with_dlq(dlq_of(Arc::clone(&dlq), Some("eu")));
        assert!(g.policy_quarantines());
        let governed = g.sink_builder().build(Box::new(Shared(Arc::clone(&main))));
        Pipeline::new(&source, &governed)
            .with_governance(g)
            .run()
            .await
            .unwrap();
        assert_eq!(
            main.columnar_calls.load(Ordering::Relaxed),
            0,
            "a policy quarantine keeps the run off the columnar path"
        );
        let quarantined: Vec<Value> = dlq
            .rows()
            .iter()
            .map(|e| e["payload"]["id"].clone())
            .collect();
        outcomes.push((main.rows(), quarantined));
    }
    assert_eq!(outcomes[0], outcomes[1]);
    assert_eq!(outcomes[0].1, vec![json!(1), json!(3)]);
    assert_eq!(outcomes[0].0, vec![json!({"id": 2, "contact": "none"})]);
}

/// A DLQ-only masking rule rewrites the quarantined payload before the DLQ's
/// policy check, so a `mask:` rule it satisfies passes at run time exactly as
/// the static pass reports — the run does not fail on the first quarantine.
#[tokio::test]
async fn dlq_only_masking_satisfies_a_mask_rule_at_the_dlq() {
    let policy: faucet_core::PolicySpec = serde_json::from_value(json!({
        "classifications": [{"label": "pii", "fields": ["email"]}],
        "rules": [{"name": "pii-hashed", "when": {"label": "pii"}, "mask": ["hash"],
                   "on_runtime": "fail"}]
    }))
    .unwrap();
    let policy = Arc::new(CompiledPolicy::compile(&policy).unwrap());
    let masking: faucet_core::MaskingSpec = serde_json::from_value(json!({
        "key": "k",
        "rules": [{"match": {"fields": ["email"]}, "action": {"type": "hash"}}]
    }))
    .unwrap();
    let masking = Arc::new(faucet_core::CompiledMasking::compile(&masking).unwrap());
    for dlq_masked in [true, false] {
        let source = Rows::new(vec![
            json!({"id": null, "email": "a@b.co"}),
            json!({"id": 1, "note": "plain"}),
        ]);
        let main = Capture::new(Mode::All);
        let dlq = Capture::new(Mode::All);
        let mut d = dlq_of(Arc::clone(&dlq), Some("eu"));
        if dlq_masked {
            d = d.with_masking(Arc::clone(&masking));
        }
        let g = Governance::new()
            .with_quality(quality_quarantine_null_id())
            .with_policy(PolicyGovernance::new(
                Arc::clone(&policy),
                facts("main", "eu"),
                scope(),
            ))
            .with_dlq(d);
        let governed = g.sink_builder().build(Box::new(Shared(Arc::clone(&main))));
        let result = Pipeline::new(&source, &governed)
            .with_governance(g)
            .run()
            .await;
        if dlq_masked {
            result.unwrap();
            assert_eq!(dlq.rows().len(), 1);
            assert_ne!(dlq.rows()[0]["payload"]["email"], "a@b.co");
        } else {
            let err = result.unwrap_err();
            assert!(matches!(err, FaucetError::PolicyViolation { .. }), "{err}");
            assert!(dlq.rows().is_empty());
        }
    }
}
