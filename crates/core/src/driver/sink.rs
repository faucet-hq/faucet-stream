//! The family-neutral sink driver contract and its [`Sink`] adapter.
//!
//! A [`SinkDriver`] is a destination's I/O and nothing else: write one page,
//! flush, and — for each lifecycle the destination supports — the small
//! primitive group that lifecycle needs ([`StagingDriver`] for overwrite,
//! [`CommitTokenDriver`] for exactly-once, [`CleanupDriver`],
//! [`SchemaDriver`], [`RollbackDriver`]). [`DriverSink`] turns it into a
//! [`Sink`]: it derives every capability from the groups the driver has, so
//! a driver cannot advertise a lifecycle it does not implement; it turns one
//! write primitive into the plain, reported and per-row write paths with row
//! accounting; and it owns the run's call context, which it hands to every
//! primitive.

use crate::accounting::WriteReport;
use crate::capabilities::Capabilities;
use crate::check::{CheckContext, CheckReport};
use crate::cleanup::SeenKeys;
use crate::dlq::BatchAtomicity;
use crate::drift::SchemaEvolution;
use crate::error::FaucetError;
use crate::idempotency::SinkGuarantee;
use crate::io::{CallContext, CallSlot};
use crate::local_outputs::LocalOutput;
use crate::observability::RoundtripRecorder;
use crate::rollback::{RollbackOptions, RollbackOutcome};
use crate::traits::{RowOutcome, Sink};
use crate::write_mode::WriteMode;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;

/// How a write may report a row it could not write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RowPolicy {
    /// The page is one unit: the plain and reported write paths. A driver
    /// that can tell a row is bad before writing anything fails the page
    /// without writing; one that cannot reports the row in
    /// [`PageWrite::failed`] and the adapter fails the page with it.
    WholePage,
    /// Write the good rows and report each bad one: the dead-letter path
    /// ([`Sink::write_batch_partial`]).
    PerRow,
}

/// What one [`SinkDriver::write`] did.
#[derive(Debug)]
#[non_exhaustive]
pub struct PageWrite {
    /// What happened to the rows that did not fail on their own.
    pub report: WriteReport,
    /// `(index in the page, error)` for every row that failed on its own.
    pub failed: Vec<(usize, FaucetError)>,
}

impl PageWrite {
    /// `n` rows written, none failed.
    pub fn accepted(n: usize) -> Self {
        Self::reported(WriteReport::accepted(n))
    }

    /// The rows are accounted for by `report`; none failed.
    pub fn reported(report: WriteReport) -> Self {
        Self {
            report,
            failed: Vec::new(),
        }
    }

    /// The same write with these rows failed on their own.
    pub fn with_failures(mut self, failed: Vec<(usize, FaucetError)>) -> Self {
        self.failed = failed;
        self
    }

    /// One outcome per row: each `Ok` row written, each `Err` row failed.
    pub fn from_outcomes(outcomes: Vec<RowOutcome>) -> Self {
        let mut failed = Vec::new();
        let mut ok = 0usize;
        for (i, o) in outcomes.into_iter().enumerate() {
            match o {
                Ok(()) => ok += 1,
                Err(e) => failed.push((i, e)),
            }
        }
        Self::accepted(ok).with_failures(failed)
    }
}

/// The overwrite (full refresh) primitives of a destination (#492).
///
/// [`DriverSink`] advertises [`WriteMode::Overwrite`] only for a driver that
/// returns one of these, and drives the lifecycle from it: stage before the
/// first write, publish only after a fully successful, uncancelled run,
/// discard on any other outcome.
#[async_trait]
pub trait StagingDriver: Send + Sync {
    /// Whether this instance is configured to replace its destination
    /// (`write_mode: overwrite`). Writes land in staging while it is.
    fn active(&self) -> bool;

    /// Prepare a fresh staging target, discarding one a crashed run left
    /// behind. The destination is not touched.
    async fn stage(&self, calls: &CallSlot) -> Result<(), FaucetError>;

    /// Make the staged data the destination, atomically: a reader sees the
    /// previous contents or the new ones, never a mix.
    async fn publish(&self, calls: &CallSlot) -> Result<(), FaucetError>;

    /// Drop the staging target. Safe when there is none; the destination is
    /// left exactly as it was.
    async fn discard(&self, calls: &CallSlot) -> Result<(), FaucetError>;

    /// Whether a staging target exists right now (a read-only probe).
    async fn staged(&self, calls: &CallSlot) -> Result<bool, FaucetError>;

    /// Whether [`staged`](Self::staged) can answer. A destination whose
    /// overwrite leaves no staging object it can look for returns `false`,
    /// and [`Sink::overwrite_staging_exists`] reports "unknown". Default:
    /// `true`.
    fn reports_staged(&self) -> bool {
        true
    }
}

/// The exactly-once primitives of a destination: rows and a commit token
/// committed as one unit (#215).
#[async_trait]
pub trait CommitTokenDriver: Send + Sync {
    /// The last token committed under `scope`, if any.
    async fn last_token(
        &self,
        calls: &CallSlot,
        scope: &str,
    ) -> Result<Option<String>, FaucetError>;

    /// Write `rows` and record `token` under `scope` atomically: both land or
    /// neither does. Store the token opaquely (it may carry a bookmark).
    async fn write_with_token(
        &self,
        calls: &CallSlot,
        rows: &[Value],
        scope: &str,
        token: &str,
    ) -> Result<WriteReport, FaucetError>;
}

/// The scoped-cleanup primitive of a destination (#478).
#[async_trait]
pub trait CleanupDriver: Send + Sync {
    /// Delete the rows matching `scope` whose key is not in `seen`, all or
    /// nothing. Returns the rows deleted. An empty `scope` is the whole
    /// destination (a complete-read cleanup).
    async fn delete_absent(
        &self,
        calls: &CallSlot,
        scope: &BTreeMap<String, Value>,
        seen: &SeenKeys,
    ) -> Result<u64, FaucetError>;
}

/// The schema primitives of a destination (#194).
#[async_trait]
pub trait SchemaDriver: Send + Sync {
    /// The live destination schema, `infer_schema`-shaped, or `None` when
    /// there is none yet.
    async fn current_schema(&self, calls: &CallSlot) -> Result<Option<Value>, FaucetError>;

    /// Whether [`evolve`](Self::evolve) is implemented.
    fn evolvable(&self) -> bool {
        false
    }

    /// Apply an additive evolution, idempotently.
    async fn evolve(
        &self,
        calls: &CallSlot,
        evolution: &SchemaEvolution,
    ) -> Result<(), FaucetError> {
        let _ = (calls, evolution);
        Err(FaucetError::Sink(
            "schema evolution is not supported".into(),
        ))
    }
}

/// The run-rollback primitives of a destination (#706).
#[async_trait]
pub trait RollbackDriver: Send + Sync {
    /// Whether this configuration can roll back (a SQL sink needs mapped
    /// columns). When it cannot, the primitives are still called, so their
    /// error can say why. Default: it can.
    fn enabled(&self) -> bool {
        true
    }

    /// Undo what `run_id` wrote, per `opts.mode`.
    async fn rollback_run(
        &self,
        calls: &CallSlot,
        run_id: &str,
        opts: &RollbackOptions,
    ) -> Result<RollbackOutcome, FaucetError>;

    /// Drop what was kept to make `run_id` undoable.
    async fn forget_run(&self, calls: &CallSlot, run_id: &str) -> Result<(), FaucetError>;

    /// Rewind the exactly-once watermark for `scope` to `token`.
    async fn rewind_commit_token(
        &self,
        calls: &CallSlot,
        scope: &str,
        token: Option<&str>,
    ) -> Result<(), FaucetError>;
}

/// The columnar write primitive of a destination that writes Arrow directly.
#[cfg(feature = "arrow")]
#[async_trait]
pub trait ColumnarDriver: Send + Sync {
    /// Write a record batch, returning the rows written.
    async fn write_columnar(
        &self,
        calls: &CallSlot,
        batch: &arrow::array::RecordBatch,
    ) -> Result<usize, FaucetError>;
}

/// A destination's I/O primitives (RFC 0014). Wrap it in a [`DriverSink`] to
/// get a [`Sink`].
///
/// Every async primitive receives the run's [`CallSlot`]; make each backend
/// request through it ([`CallSlot::call`] / [`CallSlot::call_once`]) so the
/// run's deadline, cancellation and round-trip metering apply. A driver never
/// stores a context itself.
///
/// The optional groups are how a driver declares a lifecycle: return one and
/// [`DriverSink`] advertises the capability and drives it; leave the default
/// `None` and the capability is off. There is no flag to set that could
/// disagree with the code.
///
/// ```
/// use faucet_core::driver::{DriverSink, PageWrite, RowPolicy, SinkDriver};
/// use faucet_core::{CallSlot, FaucetError, Sink, Value, async_trait};
/// use std::sync::Mutex;
///
/// /// Appends each page to a vector.
/// #[derive(Default)]
/// struct Memory(Mutex<Vec<Value>>);
///
/// #[async_trait]
/// impl SinkDriver for Memory {
///     fn connector_name(&self) -> &'static str {
///         "memory"
///     }
///     async fn write(
///         &self,
///         _calls: &CallSlot,
///         rows: &[Value],
///         _policy: RowPolicy,
///     ) -> Result<PageWrite, FaucetError> {
///         self.0.lock().unwrap().extend_from_slice(rows);
///         Ok(PageWrite::accepted(rows.len()))
///     }
/// }
///
/// # tokio::runtime::Runtime::new().unwrap().block_on(async {
/// let sink = DriverSink::new(Memory::default());
/// assert_eq!(sink.write_batch(&[faucet_core::json!({"id": 1})]).await?, 1);
/// assert!(!sink.supports_idempotent_writes()); // no CommitTokenDriver
/// # Ok::<(), FaucetError>(())
/// # }).unwrap();
/// ```
#[async_trait]
pub trait SinkDriver: Send + Sync {
    /// The connector's name, the `connector` metric label.
    fn connector_name(&self) -> &'static str;

    /// The JSON Schema of the connector's config.
    fn config_schema(&self) -> Value {
        serde_json::json!({"type": "object", "properties": {}})
    }

    /// The dataset this sink writes, for lineage (credential-free).
    fn dataset_uri(&self) -> String {
        format!("{}://unknown", self.connector_name())
    }

    /// The connector's own call defaults (timeout, retry). The run's context
    /// is layered over it.
    fn call_context(&self) -> CallContext {
        CallContext::new().without_timeout()
    }

    /// A non-mutating preflight probe.
    async fn check(
        &self,
        calls: &CallSlot,
        ctx: &CheckContext,
    ) -> Result<CheckReport, FaucetError> {
        let _ = (calls, ctx);
        Ok(CheckReport::not_implemented())
    }

    /// Write one page. Under [`RowPolicy::PerRow`], report the rows that
    /// failed on their own in [`PageWrite::failed`]; an `Err` means no row of
    /// the call was written.
    async fn write(
        &self,
        calls: &CallSlot,
        rows: &[Value],
        policy: RowPolicy,
    ) -> Result<PageWrite, FaucetError>;

    /// Make buffered writes durable.
    async fn flush(&self, calls: &CallSlot) -> Result<(), FaucetError> {
        let _ = calls;
        Ok(())
    }

    /// The write modes this instance applies. Must include
    /// [`WriteMode::Overwrite`] exactly when [`staging`](Self::staging) is
    /// `Some` ([`DriverSink::check_declarations`]).
    fn write_modes(&self) -> &'static [WriteMode] {
        &[WriteMode::Append]
    }

    /// Whether this instance is configured to merge rows by key
    /// (`write_mode: upsert|delete` with a `key`).
    fn dedups_by_key(&self) -> bool {
        false
    }

    /// Whether replaying a page after an ambiguous failure converges.
    fn write_is_replay_safe(&self) -> bool {
        self.dedups_by_key()
    }

    /// What a failed write leaves behind.
    fn batch_atomicity(&self) -> BatchAtomicity {
        BatchAtomicity::BestEffort
    }

    /// Whether the destination bulk-loads through an object-store stage.
    fn staged_load(&self) -> bool {
        false
    }

    /// Adjust the capabilities [`DriverSink`] derived, for a fact no
    /// primitive expresses (how unknown fields are treated, scoped
    /// overwrite). Default: unchanged.
    fn capabilities(&self, derived: Capabilities) -> Capabilities {
        derived
    }

    /// Adjust the delivery guarantee [`DriverSink`] derived (an external
    /// target whose keyed convergence faucet cannot vouch for). Default:
    /// unchanged.
    fn sink_guarantee(&self, derived: SinkGuarantee) -> SinkGuarantee {
        derived
    }

    /// The overwrite primitives.
    fn staging(&self) -> Option<&dyn StagingDriver> {
        None
    }

    /// The exactly-once primitives.
    fn commit_tokens(&self) -> Option<&dyn CommitTokenDriver> {
        None
    }

    /// The scoped-cleanup primitive.
    fn cleanup(&self) -> Option<&dyn CleanupDriver> {
        None
    }

    /// The schema primitives.
    fn schema(&self) -> Option<&dyn SchemaDriver> {
        None
    }

    /// The rollback primitives.
    fn rollback(&self) -> Option<&dyn RollbackDriver> {
        None
    }

    /// The columnar write primitive.
    #[cfg(feature = "arrow")]
    fn columnar(&self) -> Option<&dyn ColumnarDriver> {
        None
    }

    /// A `(kind, config)` source that reads the destination back.
    fn readback_source(&self) -> Option<(String, Value)> {
        None
    }

    /// Finalize a successful, uncancelled run (after the terminal flush).
    async fn complete_run(&self, calls: &CallSlot) -> Result<(), FaucetError> {
        let _ = calls;
        Ok(())
    }

    /// The local files this instance created.
    async fn local_outputs(&self) -> Vec<LocalOutput> {
        Vec::new()
    }

    /// The run's call context, for a driver whose clients hold their own
    /// (a storage backend). Most drivers use the slot they are handed.
    fn set_call_context(&self, ctx: CallContext) {
        let _ = ctx;
    }

    /// The run's round-trip recorder, for a driver whose clients hold their
    /// own.
    fn set_roundtrip_recorder(&self, recorder: Arc<RoundtripRecorder>) {
        let _ = recorder;
    }
}

/// A [`SinkDriver`] as a [`Sink`] (RFC 0014).
///
/// It plugs a driver into everything that takes a sink — the pipeline,
/// topology, the governance decorators, the conformance suite — and owns the
/// rules a driver would otherwise re-implement:
///
/// - **Capabilities follow the primitives.** Overwrite, exactly-once,
///   cleanup, schema evolution, rollback and the columnar path are on exactly
///   when the driver returns the matching group.
/// - **One write, three paths.** The plain and reported writes call
///   [`SinkDriver::write`] with [`RowPolicy::WholePage`] and fail on the first
///   failed row; the per-row write calls it with [`RowPolicy::PerRow`]. Each
///   result is checked against the rows offered, so a write that loses rows
///   fails with [`FaucetError::RowsUnaccounted`].
/// - **The call context is the run's.** [`Sink::set_call_context`] installs
///   into the slot every primitive receives.
pub struct DriverSink<D> {
    driver: D,
    calls: CallSlot,
}

impl<D: SinkDriver> DriverSink<D> {
    /// The sink over `driver`.
    ///
    /// # Panics
    ///
    /// In debug builds, when the driver's declarations disagree with its
    /// primitives ([`check_declarations`](Self::check_declarations)) — a
    /// driver that lists `overwrite` without a staging driver, or the
    /// reverse, is a programming error every connector test then catches.
    pub fn new(driver: D) -> Self {
        let sink = Self::new_unchecked(driver);
        if let Err(e) = sink.check_declarations() {
            debug_assert!(false, "{e}");
            tracing::error!(error = %e, "sink driver declarations disagree with its primitives");
        }
        sink
    }

    /// The sink over `driver`, without checking its declarations.
    #[doc(hidden)]
    pub fn new_unchecked(driver: D) -> Self {
        let calls = CallSlot::new(driver.call_context());
        Self { driver, calls }
    }

    /// The driver.
    pub fn driver(&self) -> &D {
        &self.driver
    }

    /// The driver, mutably (for a builder method of the connector).
    pub fn driver_mut(&mut self) -> &mut D {
        &mut self.driver
    }

    /// The call slot the primitives receive.
    pub fn calls(&self) -> &CallSlot {
        &self.calls
    }

    /// Unwrap the driver.
    pub fn into_driver(self) -> D {
        self.driver
    }

    /// Whether the driver's declarations agree with its primitives: the
    /// overwrite mode is declared exactly when a staging driver exists, an
    /// active staging driver is only reachable through that mode, and keyed
    /// dedup comes with a keyed write mode.
    pub fn check_declarations(&self) -> Result<(), FaucetError> {
        let modes = self.driver.write_modes();
        let name = self.driver.connector_name();
        let staging = self.driver.staging();
        if modes.contains(&WriteMode::Overwrite) != staging.is_some() {
            return Err(FaucetError::Config(format!(
                "sink driver '{name}': write mode overwrite is declared {} but a staging driver \
                 is {}",
                if modes.contains(&WriteMode::Overwrite) {
                    "on"
                } else {
                    "off"
                },
                if staging.is_some() {
                    "present"
                } else {
                    "absent"
                }
            )));
        }
        if self.driver.dedups_by_key()
            && !modes
                .iter()
                .any(|m| matches!(m, WriteMode::Upsert | WriteMode::Delete))
        {
            return Err(FaucetError::Config(format!(
                "sink driver '{name}': it dedups by key but declares no keyed write mode"
            )));
        }
        Ok(())
    }

    async fn whole_page(&self, records: &[Value]) -> Result<WriteReport, FaucetError> {
        let w = self
            .driver
            .write(&self.calls, records, RowPolicy::WholePage)
            .await?;
        if let Some((_, e)) = w.failed.into_iter().min_by_key(|(i, _)| *i) {
            return Err(e);
        }
        w.report.check(records.len())?;
        Ok(w.report)
    }
}

#[async_trait]
impl<D: SinkDriver> Sink for DriverSink<D> {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        Ok(self.whole_page(records).await?.accepted)
    }

    async fn write_batch_reported(&self, records: &[Value]) -> Result<WriteReport, FaucetError> {
        self.whole_page(records).await
    }

    async fn write_batch_partial(&self, records: &[Value]) -> Result<Vec<RowOutcome>, FaucetError> {
        let w = self
            .driver
            .write(&self.calls, records, RowPolicy::PerRow)
            .await?;
        let mut outcomes: Vec<RowOutcome> = records.iter().map(|_| Ok(())).collect();
        let failed = w.failed.len();
        for (i, e) in w.failed {
            match outcomes.get_mut(i) {
                Some(slot) => *slot = Err(e),
                None => {
                    return Err(FaucetError::Sink(format!(
                        "sink driver '{}' reported row {i} failed in a page of {}",
                        self.driver.connector_name(),
                        records.len()
                    )));
                }
            }
        }
        w.report.check(records.len().saturating_sub(failed))?;
        Ok(outcomes)
    }

    async fn flush(&self) -> Result<(), FaucetError> {
        self.driver.flush(&self.calls).await
    }

    fn batch_atomicity(&self) -> BatchAtomicity {
        self.driver.batch_atomicity()
    }

    #[cfg(feature = "arrow")]
    fn supports_columnar(&self) -> bool {
        self.driver.columnar().is_some()
    }

    #[cfg(feature = "arrow")]
    async fn write_batch_columnar(
        &self,
        batch: &arrow::array::RecordBatch,
    ) -> Result<usize, FaucetError> {
        match self.driver.columnar() {
            Some(c) => c.write_columnar(&self.calls, batch).await,
            None => {
                let rows = crate::columnar::record_batch_to_values(batch)?;
                self.write_batch(&rows).await
            }
        }
    }

    fn supports_idempotent_writes(&self) -> bool {
        self.driver.commit_tokens().is_some()
    }

    fn capabilities(&self) -> Capabilities {
        self.driver.capabilities(Capabilities::of_sink(self))
    }

    fn sink_guarantee(&self) -> SinkGuarantee {
        let derived = if self.supports_idempotent_writes() {
            SinkGuarantee::AtomicWatermark
        } else if self.driver.write_modes().contains(&WriteMode::Upsert) {
            SinkGuarantee::KeyedUpsert
        } else {
            SinkGuarantee::AtLeastOnce
        };
        self.driver.sink_guarantee(derived)
    }

    fn write_batch_is_replay_safe(&self) -> bool {
        self.driver.write_is_replay_safe()
    }

    fn dedups_by_key(&self) -> bool {
        self.driver.dedups_by_key()
    }

    fn supported_write_modes(&self) -> &'static [WriteMode] {
        self.driver.write_modes()
    }

    async fn current_schema(&self) -> Result<Option<Value>, FaucetError> {
        match self.driver.schema() {
            Some(s) => s.current_schema(&self.calls).await,
            None => Ok(None),
        }
    }

    fn supports_schema_evolution(&self) -> bool {
        self.driver.schema().is_some_and(SchemaDriver::evolvable)
    }

    async fn evolve_schema(&self, evolution: &SchemaEvolution) -> Result<(), FaucetError> {
        match self.driver.schema().filter(|s| s.evolvable()) {
            Some(s) => s.evolve(&self.calls, evolution).await,
            None => Err(FaucetError::Sink(format!(
                "sink '{}' does not support schema evolution",
                self.driver.connector_name()
            ))),
        }
    }

    fn supports_cleanup(&self) -> bool {
        self.driver.cleanup().is_some()
    }

    fn supports_staged_load(&self) -> bool {
        self.driver.staged_load()
    }

    async fn cleanup_scope(
        &self,
        scope: &BTreeMap<String, Value>,
        seen: &SeenKeys,
    ) -> Result<u64, FaucetError> {
        match self.driver.cleanup() {
            Some(c) => c.delete_absent(&self.calls, scope, seen).await,
            None => Err(FaucetError::Sink(format!(
                "sink '{}' does not support scoped cleanup",
                self.driver.connector_name()
            ))),
        }
    }

    #[allow(deprecated)] // drives the 1.x lifecycle hook until faucet-core 2.0 (RFC 0014)
    async fn write_batch_idempotent(
        &self,
        records: &[Value],
        scope: &str,
        token: &str,
    ) -> Result<usize, FaucetError> {
        Ok(self
            .write_batch_idempotent_reported(records, scope, token)
            .await?
            .accepted)
    }

    async fn write_batch_idempotent_reported(
        &self,
        records: &[Value],
        scope: &str,
        token: &str,
    ) -> Result<WriteReport, FaucetError> {
        match self.driver.commit_tokens() {
            Some(c) => c.write_with_token(&self.calls, records, scope, token).await,
            None => self.whole_page(records).await,
        }
    }

    async fn last_committed_token(&self, scope: &str) -> Result<Option<String>, FaucetError> {
        match self.driver.commit_tokens() {
            Some(c) => c.last_token(&self.calls, scope).await,
            None => Ok(None),
        }
    }

    fn is_overwrite(&self) -> bool {
        self.driver.staging().is_some_and(StagingDriver::active)
    }

    async fn begin_overwrite(&self) -> Result<(), FaucetError> {
        self.staging("write_mode: overwrite")?
            .stage(&self.calls)
            .await
    }

    async fn commit_overwrite(&self) -> Result<(), FaucetError> {
        self.staging("write_mode: overwrite")?
            .publish(&self.calls)
            .await
    }

    async fn abort_overwrite(&self) -> Result<(), FaucetError> {
        match self.driver.staging() {
            Some(s) => s.discard(&self.calls).await,
            None => Ok(()),
        }
    }

    async fn overwrite_staging_exists(&self) -> Result<Option<bool>, FaucetError> {
        match self.driver.staging() {
            Some(s) if s.reports_staged() => s.staged(&self.calls).await.map(Some),
            _ => Ok(None),
        }
    }

    fn supports_rollback(&self) -> bool {
        self.driver.rollback().is_some_and(RollbackDriver::enabled)
    }

    async fn rollback_run(
        &self,
        run_id: &str,
        opts: &RollbackOptions,
    ) -> Result<RollbackOutcome, FaucetError> {
        match self.driver.rollback() {
            Some(r) => r.rollback_run(&self.calls, run_id, opts).await,
            None => Err(FaucetError::Sink(format!(
                "sink '{}' does not support rollback",
                self.driver.connector_name()
            ))),
        }
    }

    async fn forget_run(&self, run_id: &str) -> Result<(), FaucetError> {
        match self.driver.rollback() {
            Some(r) => r.forget_run(&self.calls, run_id).await,
            None => Ok(()),
        }
    }

    async fn rewind_commit_token(
        &self,
        scope: &str,
        token: Option<&str>,
    ) -> Result<(), FaucetError> {
        match self.driver.rollback() {
            Some(r) => r.rewind_commit_token(&self.calls, scope, token).await,
            None => Err(FaucetError::Sink(format!(
                "sink '{}' does not support rewinding its commit token",
                self.driver.connector_name()
            ))),
        }
    }

    fn readback_source(&self) -> Option<(String, Value)> {
        self.driver.readback_source()
    }

    async fn complete_run(&self) -> Result<(), FaucetError> {
        self.driver.complete_run(&self.calls).await
    }

    fn config_schema(&self) -> Value {
        self.driver.config_schema()
    }

    fn connector_name(&self) -> &'static str {
        self.driver.connector_name()
    }

    fn set_roundtrip_recorder(&self, recorder: Arc<RoundtripRecorder>) {
        self.calls.install_recorder(Arc::clone(&recorder));
        self.driver.set_roundtrip_recorder(recorder);
    }

    fn set_call_context(&self, ctx: CallContext) {
        self.calls.install(ctx.clone());
        self.driver.set_call_context(ctx);
    }

    fn dataset_uri(&self) -> String {
        self.driver.dataset_uri()
    }

    async fn local_outputs(&self) -> Vec<LocalOutput> {
        self.driver.local_outputs().await
    }

    async fn check(&self, ctx: &CheckContext) -> Result<CheckReport, FaucetError> {
        self.driver.check(&self.calls, ctx).await
    }
}

impl<D: SinkDriver> DriverSink<D> {
    fn staging(&self, what: &str) -> Result<&dyn StagingDriver, FaucetError> {
        self.driver.staging().ok_or_else(|| {
            FaucetError::Sink(format!(
                "sink '{}' does not support {what}",
                self.driver.connector_name()
            ))
        })
    }
}
