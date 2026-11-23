//! Run budgets (#703): hard ceilings on what one run may move, so an
//! approved change (or any run) cannot exceed what was agreed. Every part of
//! a run (matrix rows, fan-out children, shards, topology sink nodes, the
//! DLQ) counts against one [`BudgetState`].
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
//!   [`FaucetError::BudgetExceeded`] — checked through [`Sink::admit_page`]
//!   before adaptive batching splits it — so nothing of it lands and the pipeline
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
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

/// A run budget.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BudgetSpec {
    /// Most records the run's destinations may accept, all rows together. A
    /// page that would cross it is refused whole and the run fails with
    /// `budget_exceeded`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_records: Option<u64>,
    /// Most estimated serialized bytes the sink may accept (same estimate as
    /// usage accounting). Refused whole at the page that would cross it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<u64>,
    /// Longest the run may take, measured from its start. Past it the run is cancelled
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

    /// The `allowed_sinks` entry a merge leaves when two non-empty lists have
    /// nothing in common: it matches no sink, so every row is refused.
    pub const DENY_ALL_SINKS: &'static str = "!none";

    /// The stricter of two budgets: the lower of each ceiling, and the
    /// intersection of the allowed-sink lists (either side's list alone when
    /// only one names any). Two lists with nothing in common allow no sink
    /// ([`Self::DENY_ALL_SINKS`]) — an empty list would mean "any".
    pub fn merge(&self, other: &BudgetSpec) -> BudgetSpec {
        self.merge_with(other, &|_: &str| None)
    }

    /// [`Self::merge`], where `kind_of` names the connector kind of a sink
    /// template: a template on one side survives when the other side allows
    /// its kind.
    pub fn merge_with(
        &self,
        other: &BudgetSpec,
        kind_of: &dyn Fn(&str) -> Option<String>,
    ) -> BudgetSpec {
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
            (false, false) => {
                let allows = |list: &[String], entry: &String| {
                    list.contains(entry) || kind_of(entry).is_some_and(|k| list.contains(&k))
                };
                let mut both: Vec<String> = Vec::new();
                for entry in self.allowed_sinks.iter().chain(&other.allowed_sinks) {
                    if entry != Self::DENY_ALL_SINKS
                        && allows(&self.allowed_sinks, entry)
                        && allows(&other.allowed_sinks, entry)
                        && !both.contains(entry)
                    {
                        both.push(entry.clone());
                    }
                }
                if both.is_empty() {
                    vec![Self::DENY_ALL_SINKS.to_string()]
                } else {
                    both
                }
            }
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
        if self.allowed_sinks.iter().any(|s| s == Self::DENY_ALL_SINKS) {
            return false;
        }
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

/// What a [`BudgetLedger`] granted.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct BudgetGrant {
    /// Records granted by this call (at most what was asked).
    pub records: u64,
    /// Estimated bytes granted by this call.
    pub bytes: u64,
    /// Records granted across the whole run, this call included.
    pub total_records: u64,
    /// Bytes granted across the whole run, this call included.
    pub total_bytes: u64,
}

impl BudgetGrant {
    /// A grant of `records` / `bytes`, the run having been granted
    /// `total_records` / `total_bytes` in all.
    pub fn new(records: u64, bytes: u64, total_records: u64, total_bytes: u64) -> Self {
        Self {
            records,
            bytes,
            total_records,
            total_bytes,
        }
    }
}

/// A run ledger shared by processes that execute parts of one run (the
/// shards of a sharded run on several replicas): it hands out allowance
/// against the run's ceilings, so no two parts can both spend the last of it.
/// Implementations must grant atomically and never past a ceiling.
#[async_trait]
pub trait BudgetLedger: Send + Sync {
    /// Grant up to `records` / `bytes` more against the run's ceilings.
    async fn grant(&self, records: u64, bytes: u64) -> Result<BudgetGrant, FaucetError>;

    /// Return allowance granted but not spent, so other parts can use it.
    async fn give_back(&self, records: u64, bytes: u64) -> Result<(), FaucetError>;
}

#[derive(Debug, Default, Clone, Copy)]
struct Counters {
    records: u64,
    bytes: u64,
    reserved_records: u64,
    reserved_bytes: u64,
    granted_records: u64,
    granted_bytes: u64,
}

/// The budget of one run: what its destinations have accepted, what pages in
/// flight have reserved, the verdict once a ceiling is crossed, and the run's
/// deadline. Every invocation of a run (matrix rows, fan-out children,
/// topology sink nodes, the DLQ) shares one, so the ceilings and the clock
/// cover the whole run. Optionally backed by a [`BudgetLedger`] when parts of
/// the run execute elsewhere.
#[derive(Default)]
pub struct BudgetState {
    counters: Mutex<Counters>,
    verdict: Mutex<Option<BudgetVerdict>>,
    deadline: Option<std::time::SystemTime>,
    max_duration_secs: Option<u64>,
    stop: CancellationToken,
    // The ledger only hands out allowance; the counters it feeds are behind
    // their own lock, so a panic inside it leaves nothing half-updated.
    ledger: std::panic::AssertUnwindSafe<Option<Arc<dyn BudgetLedger>>>,
    closed: std::sync::atomic::AtomicBool,
}

/// Why a page could not be reserved.
enum ReserveError {
    /// The run's budget is spent.
    Over(BudgetVerdict),
    /// The run's ledger could not be reached.
    Ledger(FaucetError),
}

impl std::fmt::Debug for BudgetState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BudgetState")
            .field("records", &self.records())
            .field("bytes", &self.bytes())
            .field("verdict", &self.verdict())
            .field("deadline", &self.deadline)
            .field("ledger", &self.ledger.is_some())
            .finish()
    }
}

impl BudgetState {
    /// The budget of a run that started at `started`: the duration ceiling
    /// of `spec` is measured from it.
    pub fn for_run(spec: &BudgetSpec, started: std::time::SystemTime) -> Self {
        Self {
            deadline: spec
                .max_duration_secs
                .map(|s| started + std::time::Duration::from_secs(s)),
            max_duration_secs: spec.max_duration_secs,
            ..Self::default()
        }
    }

    /// Draw the records / bytes allowance from `ledger` (in chunks) instead
    /// of counting against the ceilings locally.
    #[must_use]
    pub fn with_ledger(mut self, ledger: Arc<dyn BudgetLedger>) -> Self {
        self.ledger = std::panic::AssertUnwindSafe(Some(ledger));
        self
    }

    /// The verdict, if a ceiling was crossed.
    pub fn verdict(&self) -> Option<BudgetVerdict> {
        self.verdict
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// When the run's duration ceiling ends it, if one is set.
    pub fn deadline(&self) -> Option<std::time::SystemTime> {
        self.deadline
    }

    /// Cancelled when the run's budget is spent: every invocation of the run
    /// stops at its next page boundary.
    pub fn stopped(&self) -> &CancellationToken {
        &self.stop
    }

    fn set_verdict(&self, v: BudgetVerdict) {
        {
            let mut g = self.verdict.lock().unwrap_or_else(|e| e.into_inner());
            if g.is_none() {
                *g = Some(v);
            }
        }
        self.stop.cancel();
    }

    fn counters(&self) -> std::sync::MutexGuard<'_, Counters> {
        self.counters.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Records the destinations have accepted so far.
    pub fn records(&self) -> u64 {
        self.counters().records
    }

    /// Estimated bytes the destinations have accepted so far.
    pub fn bytes(&self) -> u64 {
        self.counters().bytes
    }

    /// Reserve `records` / `bytes` for a page about to be written. The check
    /// and the reservation happen under one lock, so concurrent invocations
    /// cannot both pass and overshoot. A refusal records the run's verdict.
    async fn reserve(
        &self,
        spec: &BudgetSpec,
        records: u64,
        bytes: u64,
    ) -> Result<(), ReserveError> {
        if let Some(v) = self.verdict() {
            return Err(ReserveError::Over(v));
        }
        loop {
            let shortfall = {
                let mut c = self.counters();
                let want_r = c.records + c.reserved_records + records;
                let want_b = c.records_bytes_total() + bytes;
                let (limit_r, limit_b) = match &*self.ledger {
                    Some(_) => (
                        spec.max_records.map(|_| c.granted_records),
                        spec.max_bytes.map(|_| c.granted_bytes),
                    ),
                    None => (spec.max_records, spec.max_bytes),
                };
                let short_r = limit_r.map_or(0, |l| want_r.saturating_sub(l));
                let short_b = limit_b.map_or(0, |l| want_b.saturating_sub(l));
                if short_r == 0 && short_b == 0 {
                    c.reserved_records += records;
                    c.reserved_bytes += bytes;
                    return Ok(());
                }
                if self.ledger.is_none() {
                    let v = if short_r > 0 {
                        BudgetVerdict {
                            kind: BudgetKind::Records,
                            limit: limit_r.unwrap_or_default(),
                            actual: want_r,
                        }
                    } else {
                        BudgetVerdict {
                            kind: BudgetKind::Bytes,
                            limit: limit_b.unwrap_or_default(),
                            actual: want_b,
                        }
                    };
                    drop(c);
                    self.set_verdict(v.clone());
                    return Err(ReserveError::Over(v));
                }
                (short_r, short_b)
            };
            let ledger = self.ledger.as_ref().expect("checked above");
            let chunk = |short: u64, max: Option<u64>| match max {
                Some(m) => short.max(m / 64),
                None => 0,
            };
            let want_r = chunk(shortfall.0, spec.max_records);
            let want_b = chunk(shortfall.1, spec.max_bytes);
            let grant = ledger
                .grant(want_r, want_b)
                .await
                .map_err(ReserveError::Ledger)?;
            {
                let mut c = self.counters();
                c.granted_records += grant.records;
                c.granted_bytes += grant.bytes;
            }
            if (shortfall.0 > 0 && grant.records < shortfall.0)
                || (shortfall.1 > 0 && grant.bytes < shortfall.1)
            {
                let v = if shortfall.0 > 0 && grant.records < shortfall.0 {
                    BudgetVerdict {
                        kind: BudgetKind::Records,
                        limit: spec.max_records.unwrap_or_default(),
                        actual: grant.total_records + (shortfall.0 - grant.records),
                    }
                } else {
                    BudgetVerdict {
                        kind: BudgetKind::Bytes,
                        limit: spec.max_bytes.unwrap_or_default(),
                        actual: grant.total_bytes + (shortfall.1 - grant.bytes),
                    }
                };
                self.set_verdict(v.clone());
                return Err(ReserveError::Over(v));
            }
        }
    }

    /// Turn a reservation into what the destination accepted; the rest is
    /// released.
    fn settle(&self, reserved: (u64, u64), accepted: (u64, u64)) {
        let mut c = self.counters();
        c.reserved_records = c.reserved_records.saturating_sub(reserved.0);
        c.reserved_bytes = c.reserved_bytes.saturating_sub(reserved.1);
        c.records += accepted.0;
        c.bytes += accepted.1;
    }

    /// Return the allowance a [`BudgetLedger`] granted this process but it
    /// did not spend. Call once when this process's part of the run ends;
    /// later calls do nothing. Without a ledger it does nothing.
    pub async fn close(&self) {
        let Some(ledger) = &*self.ledger else {
            return;
        };
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        let (r, b) = {
            let c = self.counters();
            (
                c.granted_records
                    .saturating_sub(c.records + c.reserved_records),
                c.granted_bytes.saturating_sub(c.bytes + c.reserved_bytes),
            )
        };
        if (r > 0 || b > 0)
            && let Err(e) = ledger.give_back(r, b).await
        {
            tracing::warn!(error = %e, "could not return unspent run budget to the ledger");
        }
    }
}

impl Counters {
    fn records_bytes_total(&self) -> u64 {
        self.bytes + self.reserved_bytes
    }
}

/// Whether one invocation was refused or stopped by its run's budget. The
/// run's verdict is shared; this flag decides which invocations it fails —
/// one that finished before the budget was spent stays a success.
#[derive(Debug, Default)]
pub struct BudgetStop {
    state: Mutex<(bool, bool)>,
}

impl BudgetStop {
    /// Mark the invocation stopped by the budget, unless it already finished.
    fn stop(&self) -> bool {
        let mut g = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if g.0 {
            return false;
        }
        g.1 = true;
        true
    }

    /// The invocation has finished: report whether the budget stopped it.
    pub fn finish(&self) -> bool {
        let mut g = self.state.lock().unwrap_or_else(|e| e.into_inner());
        g.0 = true;
        g.1
    }

    /// Whether the budget stopped (or refused a page of) the invocation.
    pub fn was_stopped(&self) -> bool {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).1
    }
}

/// One invocation's view of its run's budget, kept after the governed sink
/// is handed to the pipeline: turns a partial `Ok` into the budget failure
/// when, and only when, the budget stopped this invocation.
#[derive(Debug, Clone)]
pub struct BudgetHandle {
    state: Arc<BudgetState>,
    stop: Arc<BudgetStop>,
}

impl BudgetHandle {
    /// A handle on `state` for the invocation flagged by `stop`.
    pub fn new(state: Arc<BudgetState>, stop: Arc<BudgetStop>) -> Self {
        Self { state, stop }
    }

    /// The run's budget state.
    pub fn state(&self) -> &Arc<BudgetState> {
        &self.state
    }

    /// The verdict, when the budget stopped this invocation.
    pub fn stopped(&self) -> Option<BudgetVerdict> {
        self.stop
            .was_stopped()
            .then(|| self.state.verdict())
            .flatten()
    }

    /// Mark the invocation's pipeline finished, at the moment it returns:
    /// the run's verdict when the budget stopped it (refused one of its pages,
    /// or cancelled it while it ran), else `None`. Later budget stops no
    /// longer touch it. Idempotent.
    pub fn end(&self) -> Option<BudgetVerdict> {
        self.stop.finish().then(|| self.state.verdict()).flatten()
    }

    /// Mark the invocation finished; an `Ok` it reached only because the
    /// budget stopped it becomes the budget failure.
    pub fn finish<T>(&self, result: Result<T, FaucetError>) -> Result<T, FaucetError> {
        let stopped = self.stop.finish();
        match (&result, self.state.verdict()) {
            (Ok(_), Some(v)) if stopped => Err(v.error()),
            _ => result,
        }
    }
}

/// The enforcing decorator. Build with [`BudgetSink::wrap`].
pub struct BudgetSink {
    inner: Box<dyn Sink>,
    spec: BudgetSpec,
    state: Arc<BudgetState>,
    cancel: CancellationToken,
    stop: Arc<BudgetStop>,
    /// Records / bytes of a page admitted whole by `admit_page` and not yet
    /// written, so its sub-batches draw on that reservation.
    prepaid: Mutex<(u64, u64)>,
}

/// A page's reservation: settled with what was accepted, released if the
/// write fails, panics or is cancelled.
struct Reservation<'a> {
    state: &'a BudgetState,
    reserved: (u64, u64),
}

impl Reservation<'_> {
    fn settle(mut self, accepted: (u64, u64)) {
        self.state.settle(self.reserved, accepted);
        self.reserved = (0, 0);
    }
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        if self.reserved != (0, 0) {
            self.state.settle(self.reserved, (0, 0));
        }
    }
}

impl BudgetSink {
    /// Wrap `inner` as a run of its own. `cancel` is the run's cooperative
    /// token; the duration ceiling (when set) cancels it from a timer task
    /// that is aborted when the returned guard drops. Returns the sink, the
    /// readable state, and the guard the caller keeps alive for the run.
    pub fn wrap(
        inner: Box<dyn Sink>,
        spec: BudgetSpec,
        cancel: CancellationToken,
    ) -> (Self, Arc<BudgetState>, BudgetTimer) {
        let state = Arc::new(BudgetState::for_run(&spec, std::time::SystemTime::now()));
        let stop = Arc::new(BudgetStop::default());
        let timer = BudgetTimer::start(
            state.deadline,
            state.max_duration_secs,
            Arc::clone(&state),
            cancel.clone(),
            None,
        );
        (
            Self::with_stop(inner, spec, Arc::clone(&state), cancel, stop),
            state,
            timer,
        )
    }

    /// Wrap `inner` counting against an existing `state` — the destinations
    /// of one run share it (#870). No duration timer is started; the caller
    /// owns one ([`BudgetTimer::start_run`]).
    pub fn wrap_shared(
        inner: Box<dyn Sink>,
        spec: BudgetSpec,
        state: Arc<BudgetState>,
        cancel: CancellationToken,
    ) -> Self {
        Self::with_stop(inner, spec, state, cancel, Arc::new(BudgetStop::default()))
    }

    /// [`Self::wrap_shared`], flagging `stop` when this sink refuses a page.
    pub fn with_stop(
        inner: Box<dyn Sink>,
        spec: BudgetSpec,
        state: Arc<BudgetState>,
        cancel: CancellationToken,
        stop: Arc<BudgetStop>,
    ) -> Self {
        Self {
            inner,
            spec,
            state,
            cancel,
            stop,
            prepaid: Mutex::new((0, 0)),
        }
    }

    fn refused(&self, v: BudgetVerdict) -> FaucetError {
        self.stop.stop();
        self.cancel.cancel();
        v.error()
    }

    fn reserve_failed(&self, e: ReserveError) -> FaucetError {
        match e {
            ReserveError::Over(v) => self.refused(v),
            ReserveError::Ledger(e) => e,
        }
    }

    /// Reserve a page against the records / bytes ceilings before it is
    /// written; a page that would cross one is refused whole.
    async fn admit(&self, records: &[Value]) -> Result<(Reservation<'_>, u64), FaucetError> {
        let n = records.len() as u64;
        let bytes = self.page_bytes(records);
        if let Some(v) = self.state.verdict() {
            // Already over: refuse everything after the verdict, so a page
            // can't slip in between the cancel and the flush.
            return Err(self.refused(v));
        }
        let from_prepaid = {
            let mut p = self.prepaid.lock().unwrap_or_else(|e| e.into_inner());
            let r = p.0.min(n);
            let b = p.1.min(bytes);
            p.0 -= r;
            p.1 -= b;
            (r, b)
        };
        let extra = (n - from_prepaid.0, bytes - from_prepaid.1);
        if extra != (0, 0)
            && let Err(e) = self.state.reserve(&self.spec, extra.0, extra.1).await
        {
            self.state.settle(from_prepaid, (0, 0));
            return Err(self.reserve_failed(e));
        }
        Ok((
            Reservation {
                state: &self.state,
                reserved: (n, bytes),
            },
            bytes,
        ))
    }

    fn release_prepaid(&self) {
        let left = std::mem::take(&mut *self.prepaid.lock().unwrap_or_else(|e| e.into_inner()));
        if left != (0, 0) {
            self.state.settle(left, (0, 0));
        }
    }

    fn page_bytes(&self, records: &[Value]) -> u64 {
        if self.spec.max_bytes.is_some() {
            estimate_page_bytes(records)
        } else {
            0
        }
    }

    fn bytes_for(&self, accepted: u64, offered: u64, bytes: u64) -> u64 {
        if accepted >= offered {
            bytes
        } else if offered == 0 {
            0
        } else {
            ((u128::from(bytes) * u128::from(accepted)) / u128::from(offered)) as u64
        }
    }

    fn finish_write<T>(&self, r: Result<T, FaucetError>) -> Result<T, FaucetError> {
        if r.is_err() {
            self.release_prepaid();
        }
        r
    }
}

/// Aborts the budget's timer and stop watcher on drop.
pub struct BudgetTimer(Vec<tokio::task::JoinHandle<()>>);

impl BudgetTimer {
    /// Start the duration ceiling's timer for `state`, `max_duration_secs`
    /// from now: when it fires it records the verdict and cancels `cancel`.
    /// A `None` ceiling starts nothing.
    pub fn start_for(
        max_duration_secs: Option<u64>,
        state: Arc<BudgetState>,
        cancel: CancellationToken,
    ) -> Self {
        let deadline = max_duration_secs
            .map(|s| std::time::SystemTime::now() + std::time::Duration::from_secs(s));
        Self::start(deadline, max_duration_secs, state, cancel, None)
    }

    /// Start one invocation's watch over its run's budget: at the run's
    /// deadline record the duration verdict, and whenever the run's budget
    /// is spent (here or in another invocation) cancel `cancel` so this
    /// invocation stops at its next page boundary, flagging `stop`. An
    /// invocation that starts after the deadline is stopped at once.
    pub fn start_run(
        state: Arc<BudgetState>,
        cancel: CancellationToken,
        stop: Arc<BudgetStop>,
    ) -> Self {
        Self::start(
            state.deadline,
            state.max_duration_secs,
            Arc::clone(&state),
            cancel,
            Some(stop),
        )
    }

    #[allow(clippy::disallowed_methods)] // the duration budget's own timer
    fn start(
        deadline: Option<std::time::SystemTime>,
        limit: Option<u64>,
        state: Arc<BudgetState>,
        cancel: CancellationToken,
        stop: Option<Arc<BudgetStop>>,
    ) -> Self {
        let mut handles = Vec::new();
        if let Some(deadline) = deadline {
            let state = Arc::clone(&state);
            let cancel = cancel.clone();
            handles.push(tokio::spawn(async move {
                let wait = deadline
                    .duration_since(std::time::SystemTime::now())
                    .unwrap_or_default();
                tokio::time::sleep(wait).await;
                let secs = limit.unwrap_or_default();
                state.set_verdict(BudgetVerdict {
                    kind: BudgetKind::Duration,
                    limit: secs,
                    actual: secs,
                });
                cancel.cancel();
            }));
        }
        if let Some(stop) = stop {
            let state = Arc::clone(&state);
            handles.push(tokio::spawn(async move {
                state.stop.cancelled().await;
                if stop.stop() {
                    cancel.cancel();
                }
            }));
        }
        Self(handles)
    }
}

impl Drop for BudgetTimer {
    fn drop(&mut self) {
        for h in self.0.drain(..) {
            h.abort();
        }
    }
}

#[async_trait]
impl Sink for BudgetSink {
    // A native byte load cannot be counted before it lands, so it stays off;
    // everything else reaches the wrapped sink.
    crate::forward_sink!(inner; except
        admit_page, write_batch, write_batch_reported, write_governed, write_batch_partial,
        write_batch_idempotent, write_batch_idempotent_reported,
        write_batch_columnar, native_load_capabilities, load_native);

    async fn admit_page(&self, records: &[Value]) -> Result<(), FaucetError> {
        if let Some(v) = self.state.verdict() {
            return Err(self.refused(v));
        }
        self.release_prepaid();
        let (n, bytes) = (records.len() as u64, self.page_bytes(records));
        if let Err(e) = self.state.reserve(&self.spec, n, bytes).await {
            return Err(self.reserve_failed(e));
        }
        if let Err(e) = self.inner.admit_page(records).await {
            self.state.settle((n, bytes), (0, 0));
            return Err(e);
        }
        *self.prepaid.lock().unwrap_or_else(|e| e.into_inner()) = (n, bytes);
        Ok(())
    }
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        let (res, bytes) = self.admit(records).await?;
        let n = self.finish_write(self.inner.write_batch(records).await)?;
        res.settle((
            n as u64,
            self.bytes_for(n as u64, records.len() as u64, bytes),
        ));
        Ok(n)
    }
    async fn write_batch_reported(
        &self,
        records: &[Value],
    ) -> Result<crate::accounting::WriteReport, FaucetError> {
        let (res, bytes) = self.admit(records).await?;
        let r = self.finish_write(self.inner.write_batch_reported(records).await)?;
        let a = r.accepted as u64;
        res.settle((a, self.bytes_for(a, records.len() as u64, bytes)));
        Ok(r)
    }
    async fn write_governed(
        &self,
        page: &crate::governance::GovernedPage<'_>,
    ) -> Result<crate::accounting::WriteReport, FaucetError> {
        let (res, bytes) = self.admit(page.records()).await?;
        let r = self.finish_write(self.inner.write_governed(page).await)?;
        let a = r.accepted as u64;
        res.settle((a, self.bytes_for(a, page.len() as u64, bytes)));
        Ok(r)
    }
    async fn write_batch_partial(&self, records: &[Value]) -> Result<Vec<RowOutcome>, FaucetError> {
        let (res, _) = self.admit(records).await?;
        let outcomes = self.finish_write(self.inner.write_batch_partial(records).await)?;
        let ok = outcomes.iter().filter(|o| o.is_ok()).count() as u64;
        let bytes = if self.spec.max_bytes.is_some() {
            records
                .iter()
                .zip(&outcomes)
                .filter(|(_, o)| o.is_ok())
                .map(|(r, _)| crate::usage::estimate_json_bytes(r))
                .sum()
        } else {
            0
        };
        res.settle((ok, bytes));
        Ok(outcomes)
    }
    #[allow(deprecated)] // drives the 1.x lifecycle hook until faucet-core 2.0 (RFC 0014)
    async fn write_batch_idempotent(
        &self,
        records: &[Value],
        scope: &str,
        token: &str,
    ) -> Result<usize, FaucetError> {
        let (res, bytes) = self.admit(records).await?;
        let n = self.finish_write(
            self.inner
                .write_batch_idempotent(records, scope, token)
                .await,
        )?;
        res.settle((
            n as u64,
            self.bytes_for(n as u64, records.len() as u64, bytes),
        ));
        Ok(n)
    }

    #[allow(deprecated)] // drives the 1.x lifecycle hook until faucet-core 2.0 (RFC 0014)
    async fn write_batch_idempotent_reported(
        &self,
        records: &[Value],
        scope: &str,
        token: &str,
    ) -> Result<crate::accounting::WriteReport, FaucetError> {
        let (res, bytes) = self.admit(records).await?;
        let r = self.finish_write(
            self.inner
                .write_batch_idempotent_reported(records, scope, token)
                .await,
        )?;
        let a = r.accepted as u64;
        res.settle((a, self.bytes_for(a, records.len() as u64, bytes)));
        Ok(r)
    }
    #[cfg(feature = "arrow")]
    async fn write_batch_columnar(
        &self,
        batch: &arrow::array::RecordBatch,
    ) -> Result<usize, FaucetError> {
        // The same byte measure the usage meter charges a columnar page, so
        // a byte ceiling and the run's reported bytes agree.
        let n = batch.num_rows() as u64;
        let bytes = crate::columnar::batch_slice_bytes(batch);
        if let Some(v) = self.state.verdict() {
            return Err(self.refused(v));
        }
        if let Err(e) = self.state.reserve(&self.spec, n, bytes).await {
            return Err(self.reserve_failed(e));
        }
        let res = Reservation {
            state: &self.state,
            reserved: (n, bytes),
        };
        let written = self.inner.write_batch_columnar(batch).await?;
        res.settle((written as u64, self.bytes_for(written as u64, n, bytes)));
        Ok(written)
    }
}

#[allow(deprecated)] // the tests drive the 1.x lifecycle hooks directly (RFC 0014)
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
    fn disjoint_sink_lists_merge_to_deny_all_not_any() {
        let a = BudgetSpec {
            allowed_sinks: vec!["sandbox".into()],
            ..Default::default()
        };
        let b = BudgetSpec {
            allowed_sinks: vec!["x".into()],
            ..Default::default()
        };
        let m = a.merge(&b);
        assert_eq!(m.allowed_sinks, vec![BudgetSpec::DENY_ALL_SINKS]);
        assert!(!m.sink_allowed("sandbox", "postgres"));
        assert!(!m.sink_allowed("x", "x"));
        assert!(m.validate().is_ok());
        // Merging again keeps it closed.
        assert!(!m.merge(&a).sink_allowed("sandbox", "postgres"));
    }

    #[test]
    fn merge_with_keeps_a_template_whose_kind_the_other_side_allows() {
        let templates = |name: &str| (name == "sandbox").then(|| "postgres".to_string());
        let a = BudgetSpec {
            allowed_sinks: vec!["sandbox".into()],
            ..Default::default()
        };
        let b = BudgetSpec {
            allowed_sinks: vec!["postgres".into(), "bigquery".into()],
            ..Default::default()
        };
        assert_eq!(a.merge_with(&b, &templates).allowed_sinks, vec!["sandbox"]);
        assert_eq!(b.merge_with(&a, &templates).allowed_sinks, vec!["sandbox"]);
        assert_eq!(
            a.merge(&b).allowed_sinks,
            vec![BudgetSpec::DENY_ALL_SINKS],
            "without template knowledge the lists are disjoint"
        );
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
        assert_eq!(sink.batch_atomicity(), inner.batch_atomicity());
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
            .cleanup_scope(
                &std::collections::BTreeMap::new(),
                &crate::cleanup::SeenKeys::new(),
            )
            .await;
        assert!(!sink.is_overwrite());
        let _ = sink.begin_overwrite().await;
        let _ = sink.commit_overwrite().await;
        let _ = sink.abort_overwrite().await;
        let _ = sink.complete_run().await;
        assert!(!sink.supports_rollback());
        let opts = crate::rollback::RollbackOptions {
            run_id_column: "_faucet_run_id".into(),
            mode: crate::rollback::RollbackMode::Append,
            force: false,
            dry_run: true,
            later_runs: false,
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

    #[cfg(feature = "arrow")]
    #[tokio::test]
    async fn columnar_pages_are_refused_whole_by_the_byte_ceiling() {
        use arrow::array::{Int64Array, RecordBatch};
        use arrow::datatypes::{DataType, Field, Schema};
        struct ColSink(AtomicUsize);
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
                self.0.fetch_add(b.num_rows(), Ordering::SeqCst);
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
        let one = crate::columnar::batch_slice_bytes(&batch(10));
        assert!(one > 0);
        let cancel = CancellationToken::new();
        let (sink, state, _t) = BudgetSink::wrap(
            Box::new(ColSink(AtomicUsize::new(0))),
            BudgetSpec {
                max_bytes: Some(one + one / 2),
                ..Default::default()
            },
            cancel.clone(),
        );
        assert_eq!(sink.write_batch_columnar(&batch(10)).await.unwrap(), 10);
        assert_eq!(state.bytes(), one);
        let err = sink.write_batch_columnar(&batch(10)).await.unwrap_err();
        assert!(
            matches!(err, FaucetError::BudgetExceeded { ref budget, .. } if budget == "max_bytes"),
            "{err}"
        );
        assert!(cancel.is_cancelled());
        assert_eq!(state.records(), 10, "the crossing batch never landed");
        assert_eq!(state.bytes(), one);
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
        assert!(t.0.is_empty());
        assert!(st.verdict().is_none());
    }

    #[derive(Clone, Default)]
    struct SharedCount(Arc<AtomicUsize>);
    #[async_trait]
    impl Sink for SharedCount {
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
    }

    #[tokio::test]
    async fn admit_page_refuses_the_whole_page_and_prepays_an_admitted_one() {
        let cancel = CancellationToken::new();
        let inner = SharedCount::default();
        let (sink, state, _t) = BudgetSink::wrap(
            Box::new(inner.clone()),
            BudgetSpec {
                max_records: Some(5),
                max_bytes: Some(1 << 20),
                ..Default::default()
            },
            cancel.clone(),
        );
        sink.admit_page(&page(4)).await.unwrap();
        assert_eq!(sink.write_batch(&page(2)).await.unwrap(), 2);
        assert_eq!(sink.write_batch(&page(2)).await.unwrap(), 2);
        assert_eq!(state.records(), 4);
        let err = sink.admit_page(&page(2)).await.unwrap_err();
        assert!(
            matches!(err, FaucetError::BudgetExceeded { actual: 6, .. }),
            "{err}"
        );
        assert!(cancel.is_cancelled());
        assert!(sink.admit_page(&page(1)).await.is_err(), "verdict sticks");
        assert_eq!(inner.0.load(Ordering::Relaxed), 4);
    }

    #[tokio::test]
    async fn an_unprepaid_chunk_is_checked_on_its_own() {
        let (sink, _state, _t) = BudgetSink::wrap(
            Box::new(SharedCount::default()),
            BudgetSpec {
                max_records: Some(3),
                ..Default::default()
            },
            CancellationToken::new(),
        );
        sink.admit_page(&page(2)).await.unwrap();
        assert!(sink.write_batch(&page(4)).await.is_err());
    }

    async fn run_adaptive(
        dlq: bool,
    ) -> (Result<crate::pipeline::PipelineResult, FaucetError>, usize) {
        use crate::RunStreamOptions;
        use crate::pipeline::{StreamPage, run_stream};
        let inner = SharedCount::default();
        let (sink, _state, _t) = BudgetSink::wrap(
            Box::new(inner.clone()),
            BudgetSpec {
                max_records: Some(3),
                ..Default::default()
            },
            CancellationToken::new(),
        );
        let stream = futures::stream::iter(vec![Ok(StreamPage {
            records: page(5),
            bookmark: Some(json!(1)),
        })]);
        let cfg: crate::adaptive::AdaptiveBatchConfig =
            serde_json::from_value(json!({"enabled": true, "min": 1, "max": 2})).unwrap();
        let mut opts = RunStreamOptions::new().with_adaptive(cfg);
        if dlq {
            opts = opts.with_dlq(crate::dlq::DlqConfig::new(Arc::new(SharedCount::default())));
        }
        let r = run_stream(stream, &sink, opts).await;
        (r, inner.0.load(Ordering::Relaxed))
    }

    #[tokio::test]
    async fn adaptive_reslicing_lands_nothing_of_a_refused_page() {
        for dlq in [false, true] {
            let (r, landed) = run_adaptive(dlq).await;
            assert!(
                matches!(r, Err(FaucetError::BudgetExceeded { .. })),
                "dlq={dlq}: {r:?}"
            );
            assert_eq!(landed, 0, "dlq={dlq}");
        }
    }

    #[tokio::test]
    async fn budget_sink_forwards_every_hook_but_native() {
        let probe = crate::forwarding::HookSink::default();
        let (sink, _s, _t) = BudgetSink::wrap(
            Box::new(probe.clone()),
            BudgetSpec::default(),
            CancellationToken::new(),
        );
        crate::forwarding::assert_sink_forwards_every_hook(
            &sink,
            &probe,
            &["native_load_capabilities", "load_native"],
        )
        .await;
    }

    struct HalfRejecting;
    #[async_trait]
    impl Sink for HalfRejecting {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            Ok(records.len())
        }
        async fn write_batch_partial(
            &self,
            records: &[Value],
        ) -> Result<Vec<RowOutcome>, FaucetError> {
            Ok(records
                .iter()
                .enumerate()
                .map(|(i, _)| {
                    if i % 2 == 0 {
                        Ok(())
                    } else {
                        Err(FaucetError::Sink("rejected".into()))
                    }
                })
                .collect())
        }
    }

    #[tokio::test]
    async fn partial_write_charges_bytes_for_accepted_rows_only() {
        let (sink, state, _t) = BudgetSink::wrap(
            Box::new(HalfRejecting),
            BudgetSpec {
                max_bytes: Some(1 << 20),
                ..Default::default()
            },
            CancellationToken::new(),
        );
        let records = page(4);
        let outcomes = sink.write_batch_partial(&records).await.unwrap();
        assert_eq!(outcomes.iter().filter(|o| o.is_ok()).count(), 2);
        assert_eq!(state.records(), 2);
        let accepted = crate::usage::estimate_json_bytes(&records[0])
            + crate::usage::estimate_json_bytes(&records[2]);
        assert_eq!(state.bytes(), accepted);
        assert!(state.bytes() < estimate_page_bytes(&records));
    }

    fn records_spec(max: u64) -> BudgetSpec {
        BudgetSpec {
            max_records: Some(max),
            ..Default::default()
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_invocations_never_overshoot_the_run() {
        let state = Arc::new(BudgetState::for_run(
            &records_spec(100),
            std::time::SystemTime::now(),
        ));
        let landed = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..16 {
            let sink = BudgetSink::wrap_shared(
                Box::new(SharedCount(Arc::clone(&landed))),
                records_spec(100),
                Arc::clone(&state),
                CancellationToken::new(),
            );
            tasks.push(tokio::spawn(async move {
                let mut ok = 0;
                for _ in 0..10 {
                    if sink.write_batch(&page(3)).await.is_ok() {
                        ok += 3;
                    }
                    tokio::task::yield_now().await;
                }
                ok
            }));
        }
        let mut accepted = 0;
        for t in tasks {
            accepted += t.await.unwrap();
        }
        assert!(accepted <= 100, "{accepted}");
        assert_eq!(landed.load(Ordering::Relaxed), accepted);
        assert_eq!(state.records(), accepted as u64);
        assert!(state.verdict().is_some());
    }

    struct FailsOnce(AtomicUsize);
    #[async_trait]
    impl Sink for FailsOnce {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            if self.0.fetch_add(1, Ordering::Relaxed) == 0 {
                return Err(FaucetError::Sink("transient".into()));
            }
            Ok(records.len())
        }
    }

    #[tokio::test]
    async fn a_failed_write_gives_its_reservation_back() {
        let (sink, state, _t) = BudgetSink::wrap(
            Box::new(FailsOnce(AtomicUsize::new(0))),
            records_spec(4),
            CancellationToken::new(),
        );
        assert!(sink.write_batch(&page(4)).await.is_err());
        assert_eq!(state.records(), 0);
        assert_eq!(
            sink.write_batch(&page(4)).await.unwrap(),
            4,
            "the retry fits"
        );
        assert_eq!(state.records(), 4);
        assert!(state.verdict().is_none());
        // A page admitted whole and then failed releases its prepaid rows.
        let (sink, state, _t) = BudgetSink::wrap(
            Box::new(FailsOnce(AtomicUsize::new(0))),
            records_spec(4),
            CancellationToken::new(),
        );
        sink.admit_page(&page(4)).await.unwrap();
        assert!(sink.write_batch(&page(2)).await.is_err());
        sink.admit_page(&page(4)).await.unwrap();
        assert_eq!(sink.write_batch(&page(4)).await.unwrap(), 4);
        assert_eq!(state.records(), 4);
    }

    #[tokio::test]
    async fn a_byte_refusal_reserves_no_records() {
        let spec = BudgetSpec {
            max_records: Some(10),
            max_bytes: Some(1),
            ..Default::default()
        };
        let (sink, state, _t) = BudgetSink::wrap(
            Box::new(CountingSink(AtomicUsize::new(0))),
            spec,
            CancellationToken::new(),
        );
        let e = sink.write_batch(&page(1)).await.unwrap_err();
        assert!(e.to_string().contains("max_bytes"), "{e}");
        assert_eq!(state.counters().reserved_records, 0);
        assert_eq!(state.records(), 0);
    }

    #[derive(Default)]
    struct Ledger {
        granted: Mutex<u64>,
        returned: Mutex<Vec<u64>>,
        calls: AtomicUsize,
        max: u64,
    }
    #[async_trait]
    impl BudgetLedger for Ledger {
        async fn grant(&self, records: u64, _bytes: u64) -> Result<BudgetGrant, FaucetError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let mut g = self.granted.lock().unwrap();
            let r = records.min(self.max - *g);
            *g += r;
            Ok(BudgetGrant::new(r, 0, *g, 0))
        }
        async fn give_back(&self, records: u64, _bytes: u64) -> Result<(), FaucetError> {
            *self.granted.lock().unwrap() -= records;
            self.returned.lock().unwrap().push(records);
            Ok(())
        }
    }

    #[tokio::test]
    async fn a_ledger_backed_budget_draws_in_chunks_and_gives_back_once() {
        let ledger = Arc::new(Ledger {
            max: 640,
            ..Default::default()
        });
        let state = Arc::new(
            BudgetState::for_run(&records_spec(640), std::time::SystemTime::now())
                .with_ledger(ledger.clone()),
        );
        let sink = BudgetSink::wrap_shared(
            Box::new(CountingSink(AtomicUsize::new(0))),
            records_spec(640),
            Arc::clone(&state),
            CancellationToken::new(),
        );
        for _ in 0..5 {
            sink.write_batch(&page(2)).await.unwrap();
        }
        assert_eq!(ledger.calls.load(Ordering::Relaxed), 1, "one chunk of 10");
        assert_eq!(*ledger.granted.lock().unwrap(), 10);
        sink.write_batch(&page(1)).await.unwrap();
        assert_eq!(ledger.calls.load(Ordering::Relaxed), 2);
        state.close().await;
        state.close().await;
        assert_eq!(*ledger.returned.lock().unwrap(), vec![9]);
        assert_eq!(*ledger.granted.lock().unwrap(), 11);
    }

    #[tokio::test]
    async fn an_exhausted_ledger_refuses_the_page() {
        let ledger = Arc::new(Ledger {
            max: 3,
            ..Default::default()
        });
        let state = Arc::new(
            BudgetState::for_run(&records_spec(3), std::time::SystemTime::now())
                .with_ledger(ledger),
        );
        let sink = BudgetSink::wrap_shared(
            Box::new(CountingSink(AtomicUsize::new(0))),
            records_spec(3),
            Arc::clone(&state),
            CancellationToken::new(),
        );
        sink.write_batch(&page(2)).await.unwrap();
        let e = sink.write_batch(&page(2)).await.unwrap_err();
        assert!(e.to_string().contains("max_records"), "{e}");
        assert_eq!(state.verdict().unwrap().actual, 4);
        assert!(state.stopped().is_cancelled());
    }

    struct Unreachable;
    #[async_trait]
    impl BudgetLedger for Unreachable {
        async fn grant(&self, _: u64, _: u64) -> Result<BudgetGrant, FaucetError> {
            Err(FaucetError::State("history unavailable".into()))
        }
        async fn give_back(&self, _: u64, _: u64) -> Result<(), FaucetError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn an_unreachable_ledger_fails_the_write_without_a_verdict() {
        let state = Arc::new(
            BudgetState::for_run(&records_spec(3), std::time::SystemTime::now())
                .with_ledger(Arc::new(Unreachable)),
        );
        let sink = BudgetSink::wrap_shared(
            Box::new(CountingSink(AtomicUsize::new(0))),
            records_spec(3),
            Arc::clone(&state),
            CancellationToken::new(),
        );
        let e = sink.write_batch(&page(1)).await.unwrap_err();
        assert!(e.to_string().contains("history unavailable"), "{e}");
        assert!(state.verdict().is_none());
    }

    #[tokio::test]
    async fn an_invocation_starting_after_the_run_deadline_is_stopped_at_once() {
        let spec = BudgetSpec {
            max_duration_secs: Some(1),
            ..Default::default()
        };
        let started = std::time::SystemTime::now() - std::time::Duration::from_secs(5);
        let state = Arc::new(BudgetState::for_run(&spec, started));
        let cancel = CancellationToken::new();
        let stop = Arc::new(BudgetStop::default());
        let _t = BudgetTimer::start_run(Arc::clone(&state), cancel.clone(), Arc::clone(&stop));
        tokio::time::timeout(std::time::Duration::from_secs(5), cancel.cancelled())
            .await
            .unwrap();
        assert_eq!(state.verdict().unwrap().kind, BudgetKind::Duration);
        assert_eq!(state.verdict().unwrap().limit, 1);
        assert!(stop.finish());
    }

    #[tokio::test]
    async fn a_spent_run_stops_every_running_invocation_but_not_a_finished_one() {
        let state = Arc::new(BudgetState::for_run(
            &records_spec(5),
            std::time::SystemTime::now(),
        ));
        let finished = Arc::new(BudgetStop::default());
        let _f = BudgetTimer::start_run(
            Arc::clone(&state),
            CancellationToken::new(),
            Arc::clone(&finished),
        );
        assert!(
            !finished.finish(),
            "it completed before the budget was spent"
        );
        let running_cancel = CancellationToken::new();
        let running = Arc::new(BudgetStop::default());
        let _r = BudgetTimer::start_run(
            Arc::clone(&state),
            running_cancel.clone(),
            Arc::clone(&running),
        );
        let sink = BudgetSink::wrap_shared(
            Box::new(CountingSink(AtomicUsize::new(0))),
            records_spec(5),
            Arc::clone(&state),
            CancellationToken::new(),
        );
        assert!(sink.write_batch(&page(6)).await.is_err());
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            running_cancel.cancelled(),
        )
        .await
        .unwrap();
        assert!(running.finish());
        assert!(!finished.was_stopped());
    }

    /// Grants every record asked for and no bytes; cannot take allowance back.
    struct NoBytes;
    #[async_trait]
    impl BudgetLedger for NoBytes {
        async fn grant(&self, records: u64, _: u64) -> Result<BudgetGrant, FaucetError> {
            Ok(BudgetGrant {
                records,
                bytes: 0,
                total_records: records,
                total_bytes: 0,
            })
        }
        async fn give_back(&self, _: u64, _: u64) -> Result<(), FaucetError> {
            Err(FaucetError::State("ledger gone".into()))
        }
    }

    #[tokio::test]
    async fn a_ledger_short_on_bytes_stops_the_run_on_bytes() {
        let spec = BudgetSpec {
            max_records: Some(100),
            max_bytes: Some(1000),
            ..Default::default()
        };
        let state = Arc::new(
            BudgetState::for_run(&spec, std::time::SystemTime::now())
                .with_ledger(Arc::new(NoBytes)),
        );
        assert!(format!("{state:?}").contains("ledger: true"));
        assert!(matches!(
            state.reserve(&spec, 1, 10).await,
            Err(ReserveError::Over(v)) if v.kind == BudgetKind::Bytes
        ));
        assert!(matches!(
            state.reserve(&spec, 1, 1).await,
            Err(ReserveError::Over(_))
        ));
        state.close().await;
        state.close().await;
        let handle = BudgetHandle::new(Arc::clone(&state), Arc::new(BudgetStop::default()));
        assert!(Arc::ptr_eq(handle.state(), &state));
        assert!(Unreachable.give_back(1, 1).await.is_ok());
        let _timer = BudgetTimer::start_for(None, Arc::clone(&state), CancellationToken::new());
    }

    struct RefusesPages;
    #[async_trait]
    impl Sink for RefusesPages {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            Ok(records.len())
        }
        async fn admit_page(&self, _: &[Value]) -> Result<(), FaucetError> {
            Err(FaucetError::Sink("not now".into()))
        }
    }

    #[tokio::test]
    async fn a_page_the_destination_refuses_releases_its_reservation() {
        let state = Arc::new(BudgetState::for_run(
            &records_spec(5),
            std::time::SystemTime::now(),
        ));
        let sink = BudgetSink::wrap_shared(
            Box::new(RefusesPages),
            records_spec(5),
            Arc::clone(&state),
            CancellationToken::new(),
        );
        let e = sink.admit_page(&page(3)).await.unwrap_err();
        assert!(e.to_string().contains("not now"), "{e}");
        assert_eq!(state.records(), 0);
        assert_eq!(sink.bytes_for(1, 4, 100), 25);
        assert_eq!(RefusesPages.write_batch(&page(2)).await.unwrap(), 2);
        assert_eq!(sink.bytes_for(4, 4, 100), 100);
    }
}
