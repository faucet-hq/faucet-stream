//! Dead-letter queue (DLQ) wiring shared by the pipeline runner.
//!
//! The types defined here are config-shaped: they describe *what* the
//! pipeline should do with row-level failures, not *how* the routing is
//! executed. The execution lives in [`run_stream`](crate::run_stream).
//!
//! See `docs/superpowers/specs/2026-05-24-dlq-design.md`.

use crate::FaucetError;
use crate::traits::Sink;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::fmt;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Policy applied when a sink reports an outer failure (the whole batch
/// failed, no per-row info).
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OnBatchError {
    /// Surface the underlying [`FaucetError`] and fail the pipeline (default).
    #[default]
    Propagate,
    /// Treat every row in the failed page as a DLQ candidate. Unsafe with
    /// best-effort APIs that haven't overridden
    /// [`Sink::write_batch_partial`] — already-committed rows would land in
    /// the DLQ as duplicates. Use with atomic sinks (single-statement
    /// INSERT, file writes) where the failure mode is "nothing landed".
    DlqAll,
}

/// Pipeline-level DLQ wiring.
#[derive(Clone)]
pub struct DlqConfig {
    /// Sink that receives DLQ envelopes.
    pub sink: Arc<dyn Sink>,
    /// What to do when the main sink fails wholesale.
    pub on_batch_error: OnBatchError,
    /// Per-page failure budget. `None` = unlimited.
    ///
    /// This budget is **shared across both sink-side row failures and
    /// quality-check quarantines**: a record routed to the DLQ by a
    /// `quarantine` quality check counts against it just as a sink-side
    /// row failure does.
    pub max_failures_per_page: Option<usize>,
    /// Cumulative failure budget across the run. `None` = unlimited.
    ///
    /// This budget is **shared across both sink-side row failures and
    /// quality-check quarantines**: records quarantined by the quality pass
    /// accumulate in this counter alongside sink-side failures.
    pub max_failures_total: Option<usize>,
    /// Always `true` in v1. Reserved for a future "headers-only" mode.
    pub include_original_payload: bool,
}

impl DlqConfig {
    /// Convenience constructor: `propagate` policy, no budgets, payload
    /// included.
    pub fn new(sink: Arc<dyn Sink>) -> Self {
        Self {
            sink,
            on_batch_error: OnBatchError::Propagate,
            max_failures_per_page: None,
            max_failures_total: None,
            include_original_payload: true,
        }
    }
}

impl fmt::Debug for DlqConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DlqConfig")
            .field("sink", &self.sink.connector_name())
            .field("on_batch_error", &self.on_batch_error)
            .field("max_failures_per_page", &self.max_failures_per_page)
            .field("max_failures_total", &self.max_failures_total)
            .field("include_original_payload", &self.include_original_payload)
            .finish()
    }
}

/// Counters returned alongside [`PipelineResult`](crate::PipelineResult)
/// when a DLQ is wired.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DlqStats {
    /// Total rows routed to the DLQ across the run.
    pub records_dlq: usize,
    /// Pages that produced at least one DLQ record.
    pub pages_with_failures: usize,
}

/// Reason a page produced DLQ traffic. Used as a metric label and span
/// attribute; closed-set enum so cardinality stays bounded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DlqReason {
    /// At least one per-row outcome was `Err`, surfaced by an overriding
    /// [`Sink::write_batch_partial`].
    Partial,
    /// The whole batch failed and the configured policy was
    /// [`OnBatchError::DlqAll`].
    DlqAll,
    /// A record was quarantined (or batch-quarantined) by a data-quality check.
    Quality,
    /// A record was routed to the DLQ by an `on_drift`/`on_incompatible`
    /// quarantine policy.
    SchemaDrift,
    /// A record was routed to the DLQ by a data-contract `on_breach:
    /// quarantine` policy.
    Contract,
}

impl DlqReason {
    /// Returns the stable Prometheus label value for this reason.
    /// Closed-set values: `"partial"`, `"dlq_all"`, or `"quality"`.
    pub fn as_str(self) -> &'static str {
        match self {
            DlqReason::Partial => "partial",
            DlqReason::DlqAll => "dlq_all",
            DlqReason::Quality => "quality",
            DlqReason::SchemaDrift => "schema_drift",
            DlqReason::Contract => "contract",
        }
    }

    /// Every closed-set reason value, for validating a user-supplied
    /// `--reason` filter against the exact serde strings.
    pub const ALL: [DlqReason; 5] = [
        DlqReason::Partial,
        DlqReason::DlqAll,
        DlqReason::Quality,
        DlqReason::SchemaDrift,
        DlqReason::Contract,
    ];

    /// Parse a reason from its stable serde string (the inverse of
    /// [`as_str`](Self::as_str)). Returns `None` for an unknown value.
    pub fn from_serde_str(s: &str) -> Option<DlqReason> {
        DlqReason::ALL.into_iter().find(|r| r.as_str() == s)
    }
}

/// Build a single DLQ envelope.
///
/// The schema is fixed; see the design spec for the rationale. `payload`
/// is included verbatim — no truncation, no transformation. `reason`
/// records *which stage* quarantined the row (as the closed-set
/// [`DlqReason`] serde value) so tools like `faucet dlq inspect` /
/// `faucet dlq replay` can group and filter without re-deriving it from
/// the free-form error message. It is written as a top-level `reason`
/// field alongside the structured `error`.
pub fn build_envelope(
    payload: &Value,
    error: &FaucetError,
    reason: DlqReason,
    sink_name: &str,
    pipeline_name: &str,
    row: &str,
    record_index: usize,
) -> Value {
    let kind = crate::observability::decorator::error_kind(error);
    // The envelope is written to a file / object store, so it leaves the process:
    // scrub any resolved secret the error text picked up (a `reqwest` error
    // embeds the request URL, which may carry an API key in a query parameter).
    // No-op unless the host installed a redactor (#456 H5).
    let message = crate::redact::redact(&error.to_string());
    // `as_millis()` returns u128. Convert via TryFrom so we saturate at
    // i64::MAX instead of silently wrapping to a negative number. The
    // saturation ceiling (year ~292,000,000) is impossible in practice,
    // so this only ever fires on a corrupt clock. `unwrap_or(0)` covers
    // the (also impossible on modern systems) clock-before-epoch case.
    let ts_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0);
    json!({
        "error": { "kind": kind, "message": message },
        "reason": reason.as_str(),
        "payload": payload,
        "ts_ms": ts_ms,
        "sink": sink_name,
        "pipeline": pipeline_name,
        "row": row,
        "record_index": record_index,
    })
}

/// A DLQ envelope parsed back into its original payload plus the metadata
/// needed to inspect and replay it. Produced by [`unwrap_envelope`].
#[derive(Debug, Clone, PartialEq)]
pub struct UnwrappedEnvelope {
    /// The original record that was quarantined — replayed verbatim.
    pub payload: Value,
    /// The stage that quarantined the row (`build_envelope`'s `reason`
    /// field). `None` for envelopes written before the field existed.
    pub reason: Option<String>,
    /// The [`FaucetError`] variant name (`error.kind`), e.g. `"Sink"`,
    /// `"QualityFailure"`. `None` if the envelope omits it.
    pub error_kind: Option<String>,
    /// Human-readable failure message (`error.message`), if present.
    pub error_message: Option<String>,
    /// Position of the record within its original page.
    pub record_index: Option<u64>,
    /// Pipeline name that produced the envelope, if present.
    pub pipeline: Option<String>,
    /// Matrix row id that produced the envelope, if present.
    pub row: Option<String>,
    /// Sink name the record was destined for, if present.
    pub sink: Option<String>,
    /// Epoch-millis timestamp the envelope was written, if present.
    pub ts_ms: Option<i64>,
}

/// Error returned by [`unwrap_envelope`] when a value is not a usable DLQ
/// envelope. Only the *payload* is mandatory — every other field is
/// optional so envelopes written by older versions still replay.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EnvelopeError {
    /// The value was not a JSON object.
    #[error("DLQ envelope is not a JSON object")]
    NotObject,
    /// The mandatory `payload` field was absent — nothing to replay.
    #[error("DLQ envelope has no `payload` field")]
    MissingPayload,
}

/// Parse a DLQ envelope produced by [`build_envelope`] back into its
/// original payload plus metadata.
///
/// Only `payload` is required; all other fields are optional so envelopes
/// written before a field existed still round-trip (forward-compatible
/// read). Callers reading a DLQ location back (e.g. `faucet dlq inspect`)
/// should treat an [`EnvelopeError`] as "skip + count", never as fatal —
/// a DLQ file may legitimately contain arbitrary lines.
pub fn unwrap_envelope(value: &Value) -> Result<UnwrappedEnvelope, EnvelopeError> {
    let obj = value.as_object().ok_or(EnvelopeError::NotObject)?;
    let payload = obj.get("payload").ok_or(EnvelopeError::MissingPayload)?;
    let error = obj.get("error").and_then(|e| e.as_object());
    let str_field = |k: &str| obj.get(k).and_then(|v| v.as_str()).map(str::to_owned);
    Ok(UnwrappedEnvelope {
        payload: payload.clone(),
        reason: str_field("reason"),
        error_kind: error
            .and_then(|e| e.get("kind"))
            .and_then(|v| v.as_str())
            .map(str::to_owned),
        error_message: error
            .and_then(|e| e.get("message"))
            .and_then(|v| v.as_str())
            .map(str::to_owned),
        record_index: obj.get("record_index").and_then(Value::as_u64),
        pipeline: str_field("pipeline"),
        row: str_field("row"),
        sink: str_field("sink"),
        ts_ms: obj.get("ts_ms").and_then(Value::as_i64),
    })
}

/// What a sink promises about one failed batch write (#737): whether rows of a
/// batch whose write failed may already have landed.
///
/// It decides whether [`OnBatchError::DlqAll`] is safe: routing a failed batch
/// to the DLQ only avoids duplicates when nothing of it committed, because a
/// DLQ replay writes every routed row again.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum BatchAtomicity {
    /// All or nothing: a failed write lands no row (one statement, a
    /// transaction around every chunk, one object upload, one table commit).
    Atomic,
    /// Per-row outcomes through [`Sink::write_batch_partial`]; an outer `Err`
    /// from it means no row of that call committed.
    PerRow,
    /// A failed write may have committed some rows and reports no per-row
    /// detail. The default, because it is the only safe assumption.
    #[default]
    BestEffort,
}

impl BatchAtomicity {
    /// Stable label (`atomic` / `per_row` / `best_effort`).
    pub fn as_str(self) -> &'static str {
        match self {
            BatchAtomicity::Atomic => "atomic",
            BatchAtomicity::PerRow => "per_row",
            BatchAtomicity::BestEffort => "best_effort",
        }
    }
}

impl fmt::Display for BatchAtomicity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Whether [`OnBatchError::DlqAll`] can route a failed batch without
/// duplicating rows downstream: the sink lands nothing on failure, or writes
/// by key so a replayed row overwrites itself.
pub fn dlq_all_is_safe(atomicity: BatchAtomicity, dedups_by_key: bool) -> bool {
    dedups_by_key || !matches!(atomicity, BatchAtomicity::BestEffort)
}

/// The refusal for `dlq_all` on a sink that may commit part of a failed batch.
pub fn dlq_all_refusal(sink: &str, atomicity: BatchAtomicity) -> FaucetError {
    FaucetError::Config(format!(
        "dlq: on_batch_error 'dlq_all' is unsafe with sink '{sink}' (batch atomicity \
         '{atomicity}'): a failed batch may already have written some rows, and replaying \
         the DLQ would write them again. Use on_batch_error 'propagate', configure the sink \
         with write_mode 'upsert' and a key so a replay overwrites instead of duplicating, or \
         set allow_duplicates_on_dlq_all: true to accept the duplicates"
    ))
}

/// Refuse [`OnBatchError::DlqAll`] against a sink that may commit part of a
/// failed batch, unless the caller explicitly accepts duplicates.
pub fn check_dlq_all_policy(
    sink: &dyn Sink,
    on_batch_error: OnBatchError,
    allow_duplicates: bool,
) -> Result<(), FaucetError> {
    if on_batch_error != OnBatchError::DlqAll || allow_duplicates {
        return Ok(());
    }
    let atomicity = sink.batch_atomicity();
    if dlq_all_is_safe(atomicity, sink.dedups_by_key()) {
        Ok(())
    } else {
        Err(dlq_all_refusal(sink.connector_name(), atomicity))
    }
}

/// How one sink write (a page, or an adaptive sub-batch of one) ended (#737).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum BatchOutcome {
    /// Every row committed.
    Committed,
    /// Some rows committed; the sink reported the others per row and they went
    /// to the DLQ.
    DlqPartial,
    /// The whole write failed and `on_batch_error: dlq_all` sent every row to
    /// the DLQ.
    DlqAll,
    /// The write failed and the error propagated (or was retried).
    Failed,
}

impl BatchOutcome {
    /// Stable label (`committed` / `dlq_partial` / `dlq_all` / `failed`).
    pub fn as_str(self) -> &'static str {
        match self {
            BatchOutcome::Committed => "committed",
            BatchOutcome::DlqPartial => "dlq_partial",
            BatchOutcome::DlqAll => "dlq_all",
            BatchOutcome::Failed => "failed",
        }
    }
}

/// Per-run batch outcome counts, as reported on a run (#737). `attempted` is
/// the sum of the other four.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct BatchOutcomes {
    /// Sink writes attempted.
    pub attempted: u64,
    /// Writes where every row committed.
    pub committed: u64,
    /// Writes where some rows failed per row and went to the DLQ.
    pub dlq_partial: u64,
    /// Failed writes routed whole to the DLQ by `on_batch_error: dlq_all`.
    pub dlq_all: u64,
    /// Failed writes whose error propagated.
    pub failed: u64,
}

impl BatchOutcomes {
    /// Whether no write was attempted.
    pub fn is_empty(&self) -> bool {
        self.attempted == 0
    }

    /// Writes that did not fully commit.
    pub fn unclean(&self) -> u64 {
        self.dlq_partial + self.dlq_all + self.failed
    }
}

/// Shared counters a pipeline run fills in as it writes (#737). Attach with
/// [`Pipeline::with_batch_outcomes`](crate::Pipeline::with_batch_outcomes) and
/// read [`snapshot`](Self::snapshot) afterwards — on failure too.
#[derive(Debug, Default)]
pub struct BatchOutcomeCounters {
    committed: std::sync::atomic::AtomicU64,
    dlq_partial: std::sync::atomic::AtomicU64,
    dlq_all: std::sync::atomic::AtomicU64,
    failed: std::sync::atomic::AtomicU64,
}

impl BatchOutcomeCounters {
    /// Empty counters.
    pub fn new() -> Self {
        Self::default()
    }

    /// Count one write.
    pub fn record(&self, outcome: BatchOutcome) {
        use std::sync::atomic::Ordering::Relaxed;
        let cell = match outcome {
            BatchOutcome::Committed => &self.committed,
            BatchOutcome::DlqPartial => &self.dlq_partial,
            BatchOutcome::DlqAll => &self.dlq_all,
            BatchOutcome::Failed => &self.failed,
        };
        cell.fetch_add(1, Relaxed);
    }

    /// The counts so far.
    pub fn snapshot(&self) -> BatchOutcomes {
        use std::sync::atomic::Ordering::Relaxed;
        let committed = self.committed.load(Relaxed);
        let dlq_partial = self.dlq_partial.load(Relaxed);
        let dlq_all = self.dlq_all.load(Relaxed);
        let failed = self.failed.load(Relaxed);
        BatchOutcomes {
            attempted: committed + dlq_partial + dlq_all + failed,
            committed,
            dlq_partial,
            dlq_all,
            failed,
        }
    }
}

/// Where one pipeline run reports its batch outcomes: the
/// `faucet_batch_outcomes_total` counter and, when attached, the caller's
/// [`BatchOutcomeCounters`].
#[derive(Debug, Clone)]
pub(crate) struct BatchOutcomeSink {
    labels: Vec<metrics::Label>,
    counters: Option<Arc<BatchOutcomeCounters>>,
}

impl BatchOutcomeSink {
    pub(crate) fn new(
        pipeline: &str,
        row: &str,
        sink: &str,
        counters: Option<Arc<BatchOutcomeCounters>>,
    ) -> Self {
        use metrics::{Label, SharedString};
        Self {
            labels: vec![
                Label::new("pipeline", SharedString::from(pipeline.to_string())),
                Label::new("row", SharedString::from(row.to_string())),
                Label::new("sink", SharedString::from(sink.to_string())),
            ],
            counters,
        }
    }

    pub(crate) fn record(&self, outcome: BatchOutcome) {
        let mut labels = self.labels.clone();
        labels.push(metrics::Label::new(
            "outcome",
            metrics::SharedString::const_str(outcome.as_str()),
        ));
        metrics::counter!("faucet_batch_outcomes_total", labels).increment(1);
        if let Some(c) = &self.counters {
            c.record(outcome);
        }
    }

    /// Record the outcome of a plain (non-partial) write and pass it through.
    pub(crate) fn observe<T>(&self, result: Result<T, FaucetError>) -> Result<T, FaucetError> {
        self.record(if result.is_ok() {
            BatchOutcome::Committed
        } else {
            BatchOutcome::Failed
        });
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct AtomSink {
        atomicity: BatchAtomicity,
        keyed: bool,
    }

    #[async_trait::async_trait]
    impl Sink for AtomSink {
        async fn write_batch(&self, r: &[Value]) -> Result<usize, FaucetError> {
            Ok(r.len())
        }
        fn batch_atomicity(&self) -> BatchAtomicity {
            self.atomicity
        }
        fn dedups_by_key(&self) -> bool {
            self.keyed
        }
        fn connector_name(&self) -> &'static str {
            "atom"
        }
    }

    #[test]
    fn batch_atomicity_labels_and_default() {
        assert_eq!(BatchAtomicity::default(), BatchAtomicity::BestEffort);
        assert_eq!(BatchAtomicity::Atomic.as_str(), "atomic");
        assert_eq!(BatchAtomicity::PerRow.to_string(), "per_row");
        assert_eq!(BatchAtomicity::BestEffort.as_str(), "best_effort");
        assert_eq!(
            serde_json::to_value(BatchAtomicity::PerRow).unwrap(),
            json!("per_row")
        );
    }

    #[test]
    fn dlq_all_is_safe_unless_best_effort_and_unkeyed() {
        assert!(dlq_all_is_safe(BatchAtomicity::Atomic, false));
        assert!(dlq_all_is_safe(BatchAtomicity::PerRow, false));
        assert!(dlq_all_is_safe(BatchAtomicity::BestEffort, true));
        assert!(!dlq_all_is_safe(BatchAtomicity::BestEffort, false));
    }

    #[test]
    fn check_dlq_all_policy_decision_paths() {
        let best = AtomSink {
            atomicity: BatchAtomicity::BestEffort,
            keyed: false,
        };
        assert!(check_dlq_all_policy(&best, OnBatchError::Propagate, false).is_ok());
        assert!(check_dlq_all_policy(&best, OnBatchError::DlqAll, true).is_ok());
        let err = check_dlq_all_policy(&best, OnBatchError::DlqAll, false).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("'atom'") && msg.contains("best_effort"),
            "{msg}"
        );
        assert!(msg.contains("allow_duplicates_on_dlq_all"), "{msg}");
        let keyed = AtomSink {
            atomicity: BatchAtomicity::BestEffort,
            keyed: true,
        };
        assert!(check_dlq_all_policy(&keyed, OnBatchError::DlqAll, false).is_ok());
        let atomic = AtomSink {
            atomicity: BatchAtomicity::Atomic,
            keyed: false,
        };
        assert!(check_dlq_all_policy(&atomic, OnBatchError::DlqAll, false).is_ok());
    }

    #[tokio::test]
    async fn atom_sink_writes_every_record() {
        let sink = AtomSink {
            atomicity: BatchAtomicity::Atomic,
            keyed: false,
        };
        assert_eq!(sink.write_batch(&[json!(1), json!(2)]).await.unwrap(), 2);
    }

    #[test]
    fn batch_outcome_counters_snapshot() {
        let c = BatchOutcomeCounters::new();
        assert!(c.snapshot().is_empty());
        for o in [
            BatchOutcome::Committed,
            BatchOutcome::Committed,
            BatchOutcome::DlqPartial,
            BatchOutcome::DlqAll,
            BatchOutcome::Failed,
        ] {
            c.record(o);
        }
        let s = c.snapshot();
        assert_eq!(
            s,
            BatchOutcomes {
                attempted: 5,
                committed: 2,
                dlq_partial: 1,
                dlq_all: 1,
                failed: 1,
            }
        );
        assert_eq!(s.unclean(), 3);
        assert!(!s.is_empty());
        assert_eq!(BatchOutcome::DlqPartial.as_str(), "dlq_partial");
        assert_eq!(BatchOutcome::DlqAll.as_str(), "dlq_all");
        assert_eq!(BatchOutcome::Failed.as_str(), "failed");
        assert_eq!(BatchOutcome::Committed.as_str(), "committed");
    }

    #[test]
    fn batch_outcome_sink_observes_and_counts() {
        let counters = Arc::new(BatchOutcomeCounters::new());
        let sink = BatchOutcomeSink::new("p", "r", "s", Some(Arc::clone(&counters)));
        assert_eq!(sink.observe(Ok::<usize, FaucetError>(3)).unwrap(), 3);
        assert!(
            sink.observe(Err::<usize, _>(FaucetError::Sink("x".into())))
                .is_err()
        );
        sink.record(BatchOutcome::DlqAll);
        let snap = counters.snapshot();
        assert_eq!((snap.committed, snap.failed, snap.dlq_all), (1, 1, 1));
        BatchOutcomeSink::new("p", "r", "s", None).record(BatchOutcome::Committed);
    }

    #[test]
    fn envelope_has_all_required_fields() {
        let payload = json!({"user_id": 7, "name": "Alice"});
        let err = FaucetError::Sink("row rejected: bad timestamp".into());
        let env = build_envelope(
            &payload,
            &err,
            DlqReason::Partial,
            "bigquery",
            "users_etl",
            "us",
            3,
        );

        assert_eq!(env["error"]["kind"], "Sink");
        assert_eq!(env["reason"], "partial");
        assert!(
            env["error"]["message"]
                .as_str()
                .unwrap()
                .contains("row rejected")
        );
        assert_eq!(env["payload"], payload);
        assert!(env["ts_ms"].as_i64().unwrap() > 0);
        assert_eq!(env["sink"], "bigquery");
        assert_eq!(env["pipeline"], "users_etl");
        assert_eq!(env["row"], "us");
        assert_eq!(env["record_index"], 3);
    }

    #[test]
    fn envelope_preserves_payload_byte_for_byte() {
        let payload = json!({
            "nested": { "a": [1, 2, 3], "b": null, "c": true },
            "unicode": "café — résumé"
        });
        let env = build_envelope(
            &payload,
            &FaucetError::Sink("x".into()),
            DlqReason::Quality,
            "s",
            "p",
            "",
            0,
        );
        assert_eq!(env["payload"], payload);
    }

    #[test]
    fn envelope_empty_row_serializes_as_empty_string() {
        let env = build_envelope(
            &json!({}),
            &FaucetError::Sink("x".into()),
            DlqReason::DlqAll,
            "s",
            "",
            "",
            0,
        );
        assert_eq!(env["row"], "");
        assert_eq!(env["pipeline"], "");
    }

    #[test]
    fn dlq_reason_from_serde_str_round_trips() {
        for r in DlqReason::ALL {
            assert_eq!(DlqReason::from_serde_str(r.as_str()), Some(r));
        }
        assert_eq!(DlqReason::from_serde_str("nope"), None);
        assert_eq!(DlqReason::from_serde_str("sink_error"), None);
    }

    #[test]
    fn unwrap_envelope_round_trips_build_envelope() {
        let payload = json!({"id": 42, "name": "Zoe"});
        let err = FaucetError::QualityFailure {
            check: "not_null(email)".into(),
            message: "email is null".into(),
        };
        let env = build_envelope(&payload, &err, DlqReason::Quality, "pg", "etl", "eu", 5);
        let u = unwrap_envelope(&env).expect("valid envelope");
        assert_eq!(u.payload, payload);
        assert_eq!(u.reason.as_deref(), Some("quality"));
        assert_eq!(u.error_kind.as_deref(), Some("QualityFailure"));
        assert!(u.error_message.unwrap().contains("email is null"));
        assert_eq!(u.record_index, Some(5));
        assert_eq!(u.pipeline.as_deref(), Some("etl"));
        assert_eq!(u.row.as_deref(), Some("eu"));
        assert_eq!(u.sink.as_deref(), Some("pg"));
        assert!(u.ts_ms.unwrap() > 0);
    }

    #[test]
    fn unwrap_envelope_tolerates_legacy_envelope_without_reason() {
        // An envelope written before `reason`/`error` existed still yields its
        // payload; the missing metadata comes back as `None`, never a panic.
        let legacy = json!({ "payload": { "x": 1 } });
        let u = unwrap_envelope(&legacy).expect("payload present");
        assert_eq!(u.payload, json!({ "x": 1 }));
        assert_eq!(u.reason, None);
        assert_eq!(u.error_kind, None);
        assert_eq!(u.record_index, None);
    }

    #[test]
    fn unwrap_envelope_errors_on_non_object_and_missing_payload() {
        assert_eq!(
            unwrap_envelope(&json!("just a string")),
            Err(EnvelopeError::NotObject)
        );
        assert_eq!(
            unwrap_envelope(&json!([1, 2, 3])),
            Err(EnvelopeError::NotObject)
        );
        assert_eq!(
            unwrap_envelope(&json!({ "error": { "kind": "Sink" } })),
            Err(EnvelopeError::MissingPayload)
        );
    }

    #[test]
    fn on_batch_error_defaults_to_propagate() {
        assert_eq!(OnBatchError::default(), OnBatchError::Propagate);
    }

    #[test]
    fn on_batch_error_serializes_snake_case() {
        let prop = serde_json::to_string(&OnBatchError::Propagate).unwrap();
        let all = serde_json::to_string(&OnBatchError::DlqAll).unwrap();
        assert_eq!(prop, "\"propagate\"");
        assert_eq!(all, "\"dlq_all\"");
    }

    #[test]
    fn on_batch_error_deserializes_snake_case() {
        let prop: OnBatchError = serde_json::from_str("\"propagate\"").unwrap();
        let all: OnBatchError = serde_json::from_str("\"dlq_all\"").unwrap();
        assert_eq!(prop, OnBatchError::Propagate);
        assert_eq!(all, OnBatchError::DlqAll);
    }

    #[test]
    fn dlq_reason_strings() {
        assert_eq!(DlqReason::Partial.as_str(), "partial");
        assert_eq!(DlqReason::DlqAll.as_str(), "dlq_all");
    }

    #[test]
    fn dlq_reason_quality_string() {
        assert_eq!(DlqReason::Quality.as_str(), "quality");
    }

    #[test]
    fn dlq_reason_schema_drift_string() {
        assert_eq!(DlqReason::SchemaDrift.as_str(), "schema_drift");
    }

    #[test]
    fn dlq_reason_contract_string() {
        assert_eq!(DlqReason::Contract.as_str(), "contract");
    }
}
