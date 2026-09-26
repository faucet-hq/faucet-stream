//! Run budgets (#703): hard ceilings on what one invocation may move, so an
//! approved change (or any run) cannot exceed what was agreed.
//!
//! A [`BudgetSpec`] is a top-level `budget:` block or the budget on a change
//! request: `max_records`, `max_bytes` (estimated serialized bytes, see
//! [`crate::usage`]), `max_duration_secs`, and `allowed_sinks`. The first
//! three are enforced by [`BudgetSink`], a decorator the CLI executor wraps
//! **innermost** around the real sink, so it counts exactly what the
//! destination accepts:
//!
//! - **Records / bytes** are checked **before** a page is written. A page
//!   that would cross the ceiling is refused whole with
//!   [`FaucetError::BudgetExceeded`]: nothing of it lands and the pipeline
//!   never advances the bookmark past it, so a resumed run picks the page up
//!   again. Refusing rather than truncating is what keeps the bookmark honest
//!   (a partial write with a full-page bookmark would silently drop rows).
//! - **Duration** cancels the run's cooperative token when the deadline
//!   passes: the pipeline stops at its next page boundary and flushes (an
//!   overwrite aborts cleanly, nothing half-written), and the
//!   [`BudgetState`] records the verdict so the caller turns the partial
//!   `Ok` into a `budget_exceeded` failure.
//!
//! `allowed_sinks` is a plan-time check (the executor refuses a row whose
//! sink template or kind is not listed) — no decorator needed.

use crate::error::FaucetError;
use crate::traits::{RowOutcome, Sink};
use crate::usage::estimate_page_bytes;
use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

/// A run budget.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BudgetSpec {
    /// Most records the sink may accept. A page that would cross it is
    /// refused whole and the run fails with `budget_exceeded`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_records: Option<u64>,
    /// Most estimated serialized bytes the sink may accept (same estimate as
    /// usage accounting). Refused whole at the page that would cross it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<u64>,
    /// Longest an invocation may run. Past it the run is cancelled
    /// cooperatively (stops at the next page boundary, flushes) and fails
    /// with `budget_exceeded`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_duration_secs: Option<u64>,
    /// Sink template names (`pipeline.sinks.*` keys) and/or connector kinds a
    /// row may write to. Empty = any. Checked before anything runs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_sinks: Vec<String>,
}

impl BudgetSpec {
    /// No ceiling set at all.
    pub fn is_empty(&self) -> bool {
        self.max_records.is_none()
            && self.max_bytes.is_none()
            && self.max_duration_secs.is_none()
            && self.allowed_sinks.is_empty()
    }

    /// Every ceiling must be positive; an empty `allowed_sinks` entry is a
    /// typo.
    pub fn validate(&self) -> Result<(), String> {
        for (name, v) in [
            ("max_records", self.max_records),
            ("max_bytes", self.max_bytes),
            ("max_duration_secs", self.max_duration_secs),
        ] {
            if v == Some(0) {
                return Err(format!(
                    "budget.{name} must be greater than 0 (omit it for no ceiling)"
                ));
            }
        }
        if self.allowed_sinks.iter().any(|s| s.trim().is_empty()) {
            return Err("budget.allowed_sinks contains an empty entry".to_string());
        }
        Ok(())
    }

    /// The stricter of two budgets: the lower of each ceiling, and the
    /// intersection of the allowed-sink lists (either side's list alone when
    /// only one names any).
    pub fn merge(&self, other: &BudgetSpec) -> BudgetSpec {
        fn min_opt(a: Option<u64>, b: Option<u64>) -> Option<u64> {
            match (a, b) {
                (Some(x), Some(y)) => Some(x.min(y)),
                (x, None) => x,
                (None, y) => y,
            }
        }
        let allowed_sinks = match (
            self.allowed_sinks.is_empty(),
            other.allowed_sinks.is_empty(),
        ) {
            (true, true) => Vec::new(),
            (false, true) => self.allowed_sinks.clone(),
            (true, false) => other.allowed_sinks.clone(),
            (false, false) => self
                .allowed_sinks
                .iter()
                .filter(|s| other.allowed_sinks.contains(s))
                .cloned()
                .collect(),
        };
        BudgetSpec {
            max_records: min_opt(self.max_records, other.max_records),
            max_bytes: min_opt(self.max_bytes, other.max_bytes),
            max_duration_secs: min_opt(self.max_duration_secs, other.max_duration_secs),
            allowed_sinks,
        }
    }

    /// Whether a row writing to sink template `sink_ref` of connector `kind`
    /// is allowed.
    pub fn sink_allowed(&self, sink_ref: &str, kind: &str) -> bool {
        self.allowed_sinks.is_empty()
            || self
                .allowed_sinks
                .iter()
                .any(|s| s == sink_ref || s == kind)
    }
}

/// Which ceiling was crossed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum BudgetKind {
    Records,
    Bytes,
    Duration,
}

impl BudgetKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Records => "max_records",
            Self::Bytes => "max_bytes",
            Self::Duration => "max_duration_secs",
        }
    }
}

/// The verdict a [`BudgetSink`] reached.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct BudgetVerdict {
    pub kind: BudgetKind,
    pub limit: u64,
    /// What the run would have reached (records / bytes including the refused
    /// page; elapsed seconds for duration).
    pub actual: u64,
}

impl BudgetVerdict {
    pub fn error(&self) -> FaucetError {
        FaucetError::BudgetExceeded {
            budget: self.kind.as_str().to_string(),
            limit: self.limit,
            actual: self.actual,
        }
    }
}

/// The shared state of one invocation's budget, readable after the run.
#[derive(Debug, Default)]
pub struct BudgetState {
    records: AtomicU64,
    bytes: AtomicU64,
    verdict: Mutex<Option<BudgetVerdict>>,
}

impl BudgetState {
    /// The verdict, if a ceiling was crossed.
    pub fn verdict(&self) -> Option<BudgetVerdict> {
        self.verdict
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn set_verdict(&self, v: BudgetVerdict) {
        let mut g = self.verdict.lock().unwrap_or_else(|e| e.into_inner());
        if g.is_none() {
            *g = Some(v);
        }
    }

    /// Records the sink has accepted so far.
    pub fn records(&self) -> u64 {
        self.records.load(Ordering::Relaxed)
    }

    /// Estimated bytes the sink has accepted so far.
    pub fn bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }
}

/// The enforcing decorator. Build with [`BudgetSink::wrap`].
pub struct BudgetSink {
    inner: Box<dyn Sink>,
    spec: BudgetSpec,
    state: Arc<BudgetState>,
    cancel: CancellationToken,
}

impl BudgetSink {
    /// Wrap `inner`. `cancel` is the run's cooperative token; the duration
    /// ceiling (when set) cancels it from a timer task that is aborted when
    /// the returned guard drops. Returns the sink, the readable state, and
    /// the guard the caller keeps alive for the run.
    pub fn wrap(
        inner: Box<dyn Sink>,
        spec: BudgetSpec,
        cancel: CancellationToken,
    ) -> (Self, Arc<BudgetState>, BudgetTimer) {
        let state = Arc::new(BudgetState::default());
        let timer = BudgetTimer::start(spec.max_duration_secs, Arc::clone(&state), cancel.clone());
        (
            Self {
                inner,
                spec,
                state: Arc::clone(&state),
                cancel,
            },
            state,
            timer,
        )
    }

    /// Check a page against the records / bytes ceilings before it is
    /// written; on a crossing, record the verdict and refuse the page.
    fn admit(&self, records: &[Value]) -> Result<u64, FaucetError> {
        let n = records.len() as u64;
        let bytes = if self.spec.max_bytes.is_some() {
            estimate_page_bytes(records)
        } else {
            0
        };
        if let Some(v) = self.state.verdict() {
            // Already over: refuse everything after the verdict, so a page
            // can't slip in between the cancel and the flush.
            return Err(v.error());
        }
        if let Some(max) = self.spec.max_records {
            let would = self.state.records() + n;
            if would > max {
                let v = BudgetVerdict {
                    kind: BudgetKind::Records,
                    limit: max,
                    actual: would,
                };
                self.state.set_verdict(v.clone());
                self.cancel.cancel();
                return Err(v.error());
            }
        }
        if let Some(max) = self.spec.max_bytes {
            let would = self.state.bytes() + bytes;
            if would > max {
                let v = BudgetVerdict {
                    kind: BudgetKind::Bytes,
                    limit: max,
                    actual: would,
                };
                self.state.set_verdict(v.clone());
                self.cancel.cancel();
                return Err(v.error());
            }
        }
        Ok(bytes)
    }

    fn account(&self, accepted: u64, bytes: u64) {
        self.state.records.fetch_add(accepted, Ordering::Relaxed);
        self.state.bytes.fetch_add(bytes, Ordering::Relaxed);
    }
}

/// Aborts the duration timer on drop.
pub struct BudgetTimer(Option<tokio::task::JoinHandle<()>>);

impl BudgetTimer {
    fn start(
        max_duration_secs: Option<u64>,
        state: Arc<BudgetState>,
        cancel: CancellationToken,
    ) -> Self {
        let Some(secs) = max_duration_secs else {
            return Self(None);
        };
        let handle = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(secs)).await;
            state.set_verdict(BudgetVerdict {
                kind: BudgetKind::Duration,
                limit: secs,
                actual: secs,
            });
            cancel.cancel();
        });
        Self(Some(handle))
    }
}

impl Drop for BudgetTimer {
    fn drop(&mut self) {
        if let Some(h) = self.0.take() {
            h.abort();
        }
    }
}

#[async_trait]
impl Sink for BudgetSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        let bytes = self.admit(records)?;
        let n = self.inner.write_batch(records).await?;
        self.account(n as u64, bytes);
        Ok(n)
    }
    async fn write_batch_partial(&self, records: &[Value]) -> Result<Vec<RowOutcome>, FaucetError> {
        let bytes = self.admit(records)?;
        let outcomes = self.inner.write_batch_partial(records).await?;
        let ok = outcomes.iter().filter(|o| o.is_ok()).count() as u64;
        self.account(ok, bytes);
        Ok(outcomes)
    }
    async fn write_batch_idempotent(
        &self,
        records: &[Value],
        scope: &str,
        token: &str,
    ) -> Result<usize, FaucetError> {
        let bytes = self.admit(records)?;
        let n = self
            .inner
            .write_batch_idempotent(records, scope, token)
            .await?;
        self.account(n as u64, bytes);
        Ok(n)
    }
    async fn flush(&self) -> Result<(), FaucetError> {
        self.inner.flush().await
    }
    fn connector_name(&self) -> &'static str {
        self.inner.connector_name()
    }
    fn dataset_uri(&self) -> String {
        self.inner.dataset_uri()
    }
    async fn local_outputs(&self) -> Vec<crate::local_outputs::LocalOutput> {
        self.inner.local_outputs().await
    }
    fn supports_idempotent_writes(&self) -> bool {
        self.inner.supports_idempotent_writes()
    }
    fn sink_guarantee(&self) -> crate::idempotency::SinkGuarantee {
        self.inner.sink_guarantee()
    }
    fn write_batch_is_replay_safe(&self) -> bool {
        self.inner.write_batch_is_replay_safe()
    }
    fn dedups_by_key(&self) -> bool {
        self.inner.dedups_by_key()
    }
    fn supported_write_modes(&self) -> &'static [crate::write_mode::WriteMode] {
        self.inner.supported_write_modes()
    }
    async fn last_committed_token(&self, scope: &str) -> Result<Option<String>, FaucetError> {
        self.inner.last_committed_token(scope).await
    }
    async fn current_schema(&self) -> Result<Option<Value>, FaucetError> {
        self.inner.current_schema().await
    }
    fn supports_schema_evolution(&self) -> bool {
        self.inner.supports_schema_evolution()
    }
    async fn evolve_schema(
        &self,
        evolution: &crate::drift::SchemaEvolution,
    ) -> Result<(), FaucetError> {
        self.inner.evolve_schema(evolution).await
    }
    fn supports_cleanup(&self) -> bool {
        self.inner.supports_cleanup()
    }
    fn supports_staged_load(&self) -> bool {
        self.inner.supports_staged_load()
    }
    async fn cleanup_scope(
        &self,
        scope: &BTreeMap<String, Value>,
        seen: &crate::cleanup::SeenKeys,
    ) -> Result<u64, FaucetError> {
        self.inner.cleanup_scope(scope, seen).await
    }
    fn is_overwrite(&self) -> bool {
        self.inner.is_overwrite()
    }
    async fn begin_overwrite(&self) -> Result<(), FaucetError> {
        self.inner.begin_overwrite().await
    }
    async fn commit_overwrite(&self) -> Result<(), FaucetError> {
        self.inner.commit_overwrite().await
    }
    async fn abort_overwrite(&self) -> Result<(), FaucetError> {
        self.inner.abort_overwrite().await
    }
    fn supports_rollback(&self) -> bool {
        self.inner.supports_rollback()
    }
    async fn rollback_run(
        &self,
        run_id: &str,
        opts: &crate::rollback::RollbackOptions,
    ) -> Result<crate::rollback::RollbackOutcome, FaucetError> {
        self.inner.rollback_run(run_id, opts).await
    }
    async fn forget_run(&self, run_id: &str) -> Result<(), FaucetError> {
        self.inner.forget_run(run_id).await
    }
    async fn rewind_commit_token(
        &self,
        scope: &str,
        token: Option<&str>,
    ) -> Result<(), FaucetError> {
        self.inner.rewind_commit_token(scope, token).await
    }
    fn readback_source(&self) -> Option<(String, Value)> {
        self.inner.readback_source()
    }
    fn set_roundtrip_recorder(&self, recorder: Arc<crate::observability::RoundtripRecorder>) {
        self.inner.set_roundtrip_recorder(recorder)
    }
    async fn check(
        &self,
        ctx: &crate::check::CheckContext,
    ) -> Result<crate::check::CheckReport, FaucetError> {
        self.inner.check(ctx).await
    }
    // Columnar and native pages are admitted by row count only (their byte
    // size is the batch's, not a JSON estimate); a `max_bytes` budget makes
    // the executor take the row path, so this is the records ceiling.
    #[cfg(feature = "arrow")]
    fn supports_columnar(&self) -> bool {
        self.inner.supports_columnar()
    }
    #[cfg(feature = "arrow")]
    async fn write_batch_columnar(
        &self,
        batch: &arrow::array::RecordBatch,
    ) -> Result<usize, FaucetError> {
        let n = batch.num_rows() as u64;
        if let Some(v) = self.state.verdict() {
            return Err(v.error());
        }
        if let Some(max) = self.spec.max_records {
            let would = self.state.records() + n;
            if would > max {
                let v = BudgetVerdict {
                    kind: BudgetKind::Records,
                    limit: max,
                    actual: would,
                };
                self.state.set_verdict(v.clone());
                self.cancel.cancel();
                return Err(v.error());
            }
        }
        let written = self.inner.write_batch_columnar(batch).await?;
        self.account(written as u64, batch.get_array_memory_size() as u64);
        Ok(written)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::atomic::AtomicUsize;

    struct CountingSink(AtomicUsize);
    #[async_trait]
    impl Sink for CountingSink {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            self.0.fetch_add(records.len(), Ordering::Relaxed);
            Ok(records.len())
        }
        async fn write_batch_partial(
            &self,
            records: &[Value],
        ) -> Result<Vec<RowOutcome>, FaucetError> {
            self.0.fetch_add(records.len(), Ordering::Relaxed);
            Ok(records.iter().map(|_| Ok(())).collect())
        }
        async fn flush(&self) -> Result<(), FaucetError> {
            Ok(())
        }
    }

    fn page(n: usize) -> Vec<Value> {
        (0..n).map(|i| json!({"i": i, "s": "xxxxxxxx"})).collect()
    }

    #[test]
    fn spec_validate_merge_and_sinks() {
        let a = BudgetSpec {
            max_records: Some(100),
            max_bytes: None,
            max_duration_secs: Some(60),
            allowed_sinks: vec!["warehouse".into(), "postgres".into()],
        };
        let b = BudgetSpec {
            max_records: Some(50),
            max_bytes: Some(1 << 20),
            max_duration_secs: None,
            allowed_sinks: vec!["postgres".into()],
        };
        let m = a.merge(&b);
        assert_eq!(m.max_records, Some(50));
        assert_eq!(m.max_bytes, Some(1 << 20));
        assert_eq!(m.max_duration_secs, Some(60));
        assert_eq!(m.allowed_sinks, vec!["postgres"]);
        assert!(m.sink_allowed("x", "postgres"));
        assert!(!m.sink_allowed("warehouse", "bigquery"));
        assert!(BudgetSpec::default().sink_allowed("any", "thing"));
        assert!(BudgetSpec::default().is_empty());
        assert!(!a.is_empty());
        assert!(a.validate().is_ok());
        assert!(
            BudgetSpec {
                max_records: Some(0),
                ..Default::default()
            }
            .validate()
            .unwrap_err()
            .contains("max_records")
        );
        assert!(
            BudgetSpec {
                allowed_sinks: vec![" ".into()],
                ..Default::default()
            }
            .validate()
            .is_err()
        );
        assert_eq!(BudgetKind::Bytes.as_str(), "max_bytes");
        let e = BudgetVerdict {
            kind: BudgetKind::Records,
            limit: 5,
            actual: 8,
        }
        .error();
        assert!(e.to_string().contains("max_records"), "{e}");
    }

    #[test]
    fn merge_keeps_the_one_sink_list_that_is_set() {
        let listed = BudgetSpec {
            allowed_sinks: vec!["pg".into()],
            ..Default::default()
        };
        let open = BudgetSpec::default();
        assert_eq!(listed.merge(&open).allowed_sinks, vec!["pg"]);
        assert_eq!(open.merge(&listed).allowed_sinks, vec!["pg"]);
    }

    #[tokio::test]
    async fn every_capability_is_forwarded_to_the_inner_sink() {
        let (sink, _state, _t) = BudgetSink::wrap(
            Box::new(CountingSink(AtomicUsize::new(0))),
            BudgetSpec::default(),
            CancellationToken::new(),
        );
        let inner = CountingSink(AtomicUsize::new(0));
        sink.flush().await.unwrap();
        assert_eq!(sink.connector_name(), inner.connector_name());
        assert_eq!(sink.dataset_uri(), inner.dataset_uri());
        assert!(sink.local_outputs().await.is_empty());
        assert_eq!(
            sink.supports_idempotent_writes(),
            inner.supports_idempotent_writes()
        );
        assert_eq!(sink.sink_guarantee(), inner.sink_guarantee());
        assert_eq!(
            sink.write_batch_is_replay_safe(),
            inner.write_batch_is_replay_safe()
        );
        assert_eq!(sink.dedups_by_key(), inner.dedups_by_key());
        assert_eq!(sink.supported_write_modes(), inner.supported_write_modes());
        assert_eq!(sink.last_committed_token("s").await.unwrap(), None);
        assert_eq!(sink.current_schema().await.unwrap(), None);
        assert!(!sink.supports_schema_evolution());
        assert!(
            sink.evolve_schema(&crate::drift::SchemaEvolution::default())
                .await
                .is_err()
        );
        assert!(!sink.supports_cleanup());
        assert!(!sink.supports_staged_load());
        let _ = sink
            .cleanup_scope(&BTreeMap::new(), &crate::cleanup::SeenKeys::new())
            .await;
        assert!(!sink.is_overwrite());
        let _ = sink.begin_overwrite().await;
        let _ = sink.commit_overwrite().await;
        let _ = sink.abort_overwrite().await;
        assert!(!sink.supports_rollback());
        let opts = crate::rollback::RollbackOptions {
            run_id_column: "_faucet_run_id".into(),
            mode: crate::rollback::RollbackMode::Append,
            force: false,
            dry_run: true,
        };
        assert!(sink.rollback_run("r", &opts).await.is_err());
        let _ = sink.forget_run("r").await;
        let _ = sink.rewind_commit_token("s", None).await;
        assert_eq!(sink.readback_source(), None);
        sink.set_roundtrip_recorder(Arc::new(crate::observability::RoundtripRecorder::new(
            crate::observability::RoundtripSide::Sink,
            "p",
            "r",
            "c",
        )));
        let _ = sink.check(&crate::check::CheckContext::default()).await;
    }

    #[cfg(feature = "arrow")]
    #[tokio::test]
    async fn columnar_pages_are_admitted_by_row_count() {
        use arrow::array::{Int64Array, RecordBatch};
        use arrow::datatypes::{DataType, Field, Schema};
        struct ColSink;
        #[async_trait]
        impl Sink for ColSink {
            async fn write_batch(&self, r: &[Value]) -> Result<usize, FaucetError> {
                Ok(r.len())
            }
            async fn flush(&self) -> Result<(), FaucetError> {
                Ok(())
            }
            fn supports_columnar(&self) -> bool {
                true
            }
            async fn write_batch_columnar(&self, b: &RecordBatch) -> Result<usize, FaucetError> {
                Ok(b.num_rows())
            }
        }
        let batch = |n: i64| {
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, false)])),
                vec![Arc::new(Int64Array::from((0..n).collect::<Vec<_>>()))],
            )
            .unwrap()
        };
        let cancel = CancellationToken::new();
        let (sink, state, _t) = BudgetSink::wrap(
            Box::new(ColSink),
            BudgetSpec {
                max_records: Some(5),
                ..Default::default()
            },
            cancel.clone(),
        );
        assert!(sink.supports_columnar());
        assert_eq!(sink.write_batch_columnar(&batch(3)).await.unwrap(), 3);
        assert_eq!(state.records(), 3);
        let err = sink.write_batch_columnar(&batch(3)).await.unwrap_err();
        assert!(
            matches!(err, FaucetError::BudgetExceeded { actual: 6, .. }),
            "{err}"
        );
        assert!(cancel.is_cancelled());
        assert!(
            sink.write_batch_columnar(&batch(1)).await.is_err(),
            "after the verdict"
        );
    }

    #[tokio::test]
    async fn records_ceiling_refuses_the_crossing_page_and_cancels() {
        let cancel = CancellationToken::new();
        let inner = Box::new(CountingSink(AtomicUsize::new(0)));
        let (sink, state, _timer) = BudgetSink::wrap(
            inner,
            BudgetSpec {
                max_records: Some(5),
                ..Default::default()
            },
            cancel.clone(),
        );
        assert_eq!(sink.write_batch(&page(3)).await.unwrap(), 3);
        assert_eq!(sink.write_batch_partial(&page(2)).await.unwrap().len(), 2);
        assert_eq!(state.records(), 5);
        let err = sink.write_batch(&page(1)).await.unwrap_err();
        assert!(
            matches!(err, FaucetError::BudgetExceeded { ref budget, limit: 5, actual: 6 } if budget == "max_records"),
            "{err}"
        );
        assert!(cancel.is_cancelled());
        let v = state.verdict().unwrap();
        assert_eq!(v.kind, BudgetKind::Records);
        // Everything after the verdict is refused too.
        assert!(
            sink.write_batch_idempotent(&page(1), "s", "t")
                .await
                .is_err()
        );
        assert_eq!(state.records(), 5, "the refused pages never landed");
    }

    #[tokio::test]
    async fn bytes_ceiling_uses_the_page_estimate() {
        let cancel = CancellationToken::new();
        let (sink, state, _t) = BudgetSink::wrap(
            Box::new(CountingSink(AtomicUsize::new(0))),
            BudgetSpec {
                max_bytes: Some(100),
                ..Default::default()
            },
            cancel.clone(),
        );
        let one = estimate_page_bytes(&page(1));
        assert_eq!(sink.write_batch(&page(1)).await.unwrap(), 1);
        assert_eq!(state.bytes(), one);
        let err = sink.write_batch(&page(10)).await.unwrap_err();
        assert!(
            matches!(err, FaucetError::BudgetExceeded { ref budget, limit: 100, .. } if budget == "max_bytes")
        );
        assert!(cancel.is_cancelled());
    }

    #[tokio::test]
    async fn duration_ceiling_cancels_from_the_timer() {
        let cancel = CancellationToken::new();
        let (sink, state, timer) = BudgetSink::wrap(
            Box::new(CountingSink(AtomicUsize::new(0))),
            BudgetSpec {
                max_duration_secs: Some(1),
                ..Default::default()
            },
            cancel.clone(),
        );
        assert!(state.verdict().is_none());
        tokio::time::timeout(std::time::Duration::from_secs(5), cancel.cancelled())
            .await
            .expect("timer cancels within the deadline");
        let v = state.verdict().unwrap();
        assert_eq!(v.kind, BudgetKind::Duration);
        assert_eq!(v.limit, 1);
        assert!(sink.write_batch(&page(1)).await.is_err());
        drop(timer);
        // No duration: no timer task at all.
        let (_s, st, t) = BudgetSink::wrap(
            Box::new(CountingSink(AtomicUsize::new(0))),
            BudgetSpec::default(),
            CancellationToken::new(),
        );
        assert!(t.0.is_none());
        assert!(st.verdict().is_none());
    }
}
