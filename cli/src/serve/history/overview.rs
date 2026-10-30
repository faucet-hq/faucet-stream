//! Fleet aggregates behind `GET /v1/overview` (#1006): run outcomes, records,
//! governance and cost per time bucket, the pipelines failing or running
//! slowest, and catalog freshness. The SQL backends group in the database
//! (`overview_sql!`); the memory backend and the trait default feed the same
//! [`Aggregator`], so every backend returns the same shape and ordering.

use super::catalog::{CatalogDataset, CatalogSchemaVersion};
use super::labels::LabelFilter;
use super::{RunRecord, RunStatus};
use chrono::{DateTime, TimeDelta, Timelike, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Pipelines listed per top-N section by default, and at most.
pub const DEFAULT_TOP: usize = 10;
pub const MAX_TOP: usize = 50;
/// Invocation durations the p95 is computed from: the newest in the window.
pub const DURATION_SAMPLE_CAP: usize = 10_000;
/// Buckets one response may span.
pub const MAX_BUCKETS: usize = 1_000;
/// Runs the trait default reads through `list` (backends without an override).
pub const DEFAULT_SCAN_CAP: usize = 50_000;
/// The name a run without one is grouped under.
pub const UNNAMED: &str = "";

/// The width of one series bucket. Boundaries are UTC.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Bucket {
    Hour,
    Day,
}

impl Bucket {
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim() {
            "hour" => Some(Self::Hour),
            "day" => Some(Self::Day),
            _ => None,
        }
    }

    /// Hours up to three days, days beyond.
    pub fn for_range(since: DateTime<Utc>, until: DateTime<Utc>) -> Self {
        if until - since <= TimeDelta::hours(72) {
            Self::Hour
        } else {
            Self::Day
        }
    }

    pub fn step(self) -> TimeDelta {
        match self {
            Self::Hour => TimeDelta::hours(1),
            Self::Day => TimeDelta::days(1),
        }
    }

    /// The start of the bucket holding `t`.
    pub fn floor(self, t: DateTime<Utc>) -> DateTime<Utc> {
        let hour = t
            .with_nanosecond(0)
            .and_then(|t| t.with_second(0))
            .and_then(|t| t.with_minute(0))
            .unwrap_or(t);
        match self {
            Self::Hour => hour,
            Self::Day => hour.with_hour(0).unwrap_or(hour),
        }
    }

    /// Characters of a fixed-width RFC 3339 timestamp that name the bucket.
    pub fn key_len(self) -> usize {
        match self {
            Self::Hour => 13,
            Self::Day => 10,
        }
    }

    /// The bucket start a timestamp prefix of [`key_len`](Self::key_len) names.
    pub fn parse_key(self, key: &str) -> Option<DateTime<Utc>> {
        let full = match self {
            Self::Hour => format!("{key}:00:00Z"),
            Self::Day => format!("{key}T00:00:00Z"),
        };
        DateTime::parse_from_rfc3339(&full)
            .ok()
            .map(|t| t.with_timezone(&Utc))
    }
}

/// What an overview covers: a window, a bucket width and the run filters of
/// `GET /v1/runs`.
#[derive(Debug, Clone)]
pub struct OverviewQuery {
    /// Inclusive.
    pub since: DateTime<Utc>,
    /// Exclusive.
    pub until: DateTime<Utc>,
    pub bucket: Bucket,
    pub pipeline: Option<String>,
    pub pool: Option<String>,
    pub labels: Vec<LabelFilter>,
    pub tenant: Option<String>,
    pub top: usize,
    /// Datasets whose last success is before this are stale; `None` leaves
    /// the freshness section out.
    pub stale_before: Option<DateTime<Utc>>,
}

impl OverviewQuery {
    /// A query over `[since, until)` with no filters.
    pub fn window(since: DateTime<Utc>, until: DateTime<Utc>) -> Self {
        Self {
            since,
            until,
            bucket: Bucket::for_range(since, until),
            pipeline: None,
            pool: None,
            labels: Vec::new(),
            tenant: None,
            top: DEFAULT_TOP,
            stale_before: None,
        }
    }

    /// Whether a run passes the filters (not the window).
    pub fn in_scope(&self, r: &RunRecord) -> bool {
        self.pipeline
            .as_deref()
            .is_none_or(|p| r.name.as_deref().unwrap_or(UNNAMED) == p)
            && self.pool.as_deref().is_none_or(|p| r.pool == p)
            && self
                .tenant
                .as_deref()
                .is_none_or(|t| r.tenant.as_deref() == Some(t))
            && super::labels::matches_all(&self.labels, &r.labels)
    }

    pub fn in_window(&self, t: DateTime<Utc>) -> bool {
        t >= self.since && t < self.until
    }

    /// Every bucket start in the window, oldest first.
    pub fn bucket_starts(&self) -> Vec<DateTime<Utc>> {
        let mut out = Vec::new();
        let mut t = self.bucket.floor(self.since);
        while t < self.until && out.len() < MAX_BUCKETS {
            out.push(t);
            t += self.bucket.step();
        }
        out
    }

    /// How many buckets the window spans.
    pub fn bucket_count(&self) -> usize {
        let span = self.until - self.bucket.floor(self.since);
        let step = self.bucket.step().num_seconds().max(1);
        (span.num_seconds().max(0) as usize).div_ceil(step as usize)
    }

    fn top(&self) -> usize {
        self.top.clamp(1, MAX_TOP)
    }
}

/// Rows the governance passes and the sink did not write, by where they went.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GovernanceCounts {
    /// Rows a transform removed.
    pub filtered: u64,
    /// Rows a governance pass quarantined to the DLQ.
    pub quarantined: u64,
    /// Rows the sink failed per row, sent to the DLQ.
    pub dlq: u64,
    pub coalesced: u64,
    pub skipped: u64,
    pub dropped: u64,
}

impl GovernanceCounts {
    pub fn of(rows: &faucet_core::RowAccountsSnapshot) -> Self {
        Self {
            filtered: rows.filtered(),
            quarantined: rows.quarantined,
            dlq: rows.failed,
            coalesced: rows.coalesced,
            skipped: rows.skipped,
            dropped: rows.dropped,
        }
    }

    pub fn add(&mut self, o: &Self) {
        self.filtered += o.filtered;
        self.quarantined += o.quarantined;
        self.dlq += o.dlq;
        self.coalesced += o.coalesced;
        self.skipped += o.skipped;
        self.dropped += o.dropped;
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunBucket {
    pub bucket_start: DateTime<Utc>,
    pub succeeded: u64,
    pub failed: u64,
    pub cancelled: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PipelineFailure {
    pub pipeline: String,
    /// Finished runs (succeeded, failed or cancelled).
    pub runs: u64,
    pub failed: u64,
    pub rate: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PipelineLatency {
    pub pipeline: String,
    pub p95_ms: u64,
    /// Invocations in the sample.
    pub runs: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DurationSample {
    pub invocations: u64,
    /// The window held more than [`DURATION_SAMPLE_CAP`] invocations; the
    /// p95 is over the newest.
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunsSection {
    /// Runs submitted in the window.
    pub total: u64,
    pub by_status: BTreeMap<String, u64>,
    pub series: Vec<RunBucket>,
    pub failure_rate_by_pipeline: Vec<PipelineFailure>,
    pub slowest: Vec<PipelineLatency>,
    pub duration_sample: DurationSample,
    /// Waiting now (queued or pending), whatever the window.
    pub queued: u64,
    /// Executing now (running or sharded), whatever the window.
    pub running: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WrittenBucket {
    pub bucket_start: DateTime<Utc>,
    pub written: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PipelineWritten {
    pub pipeline: String,
    pub written: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecordsSection {
    pub total: u64,
    pub series: Vec<WrittenBucket>,
    pub top_pipelines: Vec<PipelineWritten>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GovernanceBucket {
    pub bucket_start: DateTime<Utc>,
    #[serde(flatten)]
    pub counts: GovernanceCounts,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GovernanceSection {
    pub totals: GovernanceCounts,
    pub series: Vec<GovernanceBucket>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CostBucket {
    pub bucket_start: DateTime<Utc>,
    pub estimated: f64,
    pub hosted_equivalent: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PipelineCost {
    pub pipeline: String,
    pub estimated: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CostSection {
    /// The currency most invocations were priced in; figures are in it.
    pub currency: String,
    /// Usage records (invocations) in that currency.
    pub invocations: u64,
    pub estimated: f64,
    pub hosted_equivalent: f64,
    pub series: Vec<CostBucket>,
    pub by_pipeline: Vec<PipelineCost>,
    /// Other currencies seen in the window, left out of the figures.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub other_currencies: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StaleDataset {
    pub id: String,
    pub uri: String,
    pub pipeline: String,
    pub last_success: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SchemaChange {
    pub id: String,
    pub uri: String,
    /// Schema versions recorded in the window.
    pub changes: u64,
    pub last_changed: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FreshnessSection {
    /// Datasets in the catalog.
    pub datasets: u64,
    pub stale_before: DateTime<Utc>,
    pub stale_count: u64,
    pub stale: Vec<StaleDataset>,
    pub schema_changed_count: u64,
    pub schema_changed: Vec<SchemaChange>,
}

/// Everything the history backends aggregate for one overview.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OverviewAggregates {
    pub runs: RunsSection,
    pub records: RecordsSection,
    pub governance: GovernanceSection,
    /// `None` when no usage was recorded in the window.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost: Option<CostSection>,
    /// `None` when not asked for or the catalog is empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub freshness: Option<FreshnessSection>,
}

#[derive(Default)]
struct BucketTotals {
    succeeded: u64,
    failed: u64,
    cancelled: u64,
    written: u64,
    governance: GovernanceCounts,
}

/// Collects grouped facts from any backend and assembles the sections.
#[derive(Default)]
pub struct Aggregator {
    buckets: BTreeMap<DateTime<Utc>, BucketTotals>,
    by_status: BTreeMap<String, u64>,
    failures: BTreeMap<String, (u64, u64)>,
    written: BTreeMap<String, u64>,
    durations: BTreeMap<String, Vec<u64>>,
    sampled: usize,
    truncated: bool,
    queued: u64,
    running: u64,
}

impl Aggregator {
    /// `n` runs of `status` submitted in the bucket starting at `bucket`.
    pub fn add_status(&mut self, bucket: DateTime<Utc>, status: &str, n: u64) {
        *self.by_status.entry(status.to_string()).or_default() += n;
        let b = self.buckets.entry(bucket).or_default();
        match RunStatus::parse(status) {
            Some(RunStatus::Completed) => b.succeeded += n,
            Some(RunStatus::Failed) => b.failed += n,
            Some(RunStatus::Cancelled) => b.cancelled += n,
            _ => {}
        }
    }

    pub fn add_written(&mut self, bucket: DateTime<Utc>, n: u64) {
        self.buckets.entry(bucket).or_default().written += n;
    }

    pub fn add_governance(&mut self, bucket: DateTime<Utc>, g: &GovernanceCounts) {
        self.buckets.entry(bucket).or_default().governance.add(g);
    }

    /// A pipeline's finished and failed runs.
    pub fn add_failure(&mut self, pipeline: &str, finished: u64, failed: u64) {
        let e = self.failures.entry(pipeline.to_string()).or_default();
        e.0 += finished;
        e.1 += failed;
    }

    pub fn add_pipeline_written(&mut self, pipeline: &str, n: u64) {
        *self.written.entry(pipeline.to_string()).or_default() += n;
    }

    /// One invocation duration, newest first. `false` once the sample is full
    /// (the window held more), so the caller stops feeding.
    pub fn add_duration(&mut self, pipeline: &str, ms: u64) -> bool {
        if self.sampled >= DURATION_SAMPLE_CAP {
            self.truncated = true;
            return false;
        }
        self.sampled += 1;
        self.durations
            .entry(pipeline.to_string())
            .or_default()
            .push(ms);
        true
    }

    /// `n` runs currently in `status` (any submission time).
    pub fn add_current(&mut self, status: &str, n: u64) {
        match RunStatus::parse(status) {
            Some(RunStatus::Queued | RunStatus::Pending) => self.queued += n,
            Some(RunStatus::Running | RunStatus::Sharded) => self.running += n,
            _ => {}
        }
    }

    /// Feed one run record that is in scope (the memory path). Durations are
    /// fed separately, newest first.
    pub fn add_run(&mut self, q: &OverviewQuery, r: &RunRecord) {
        self.add_current(r.status.as_str(), 1);
        if !q.in_window(r.submitted_at) {
            return;
        }
        let bucket = q.bucket.floor(r.submitted_at);
        let name = r.name.as_deref().unwrap_or(UNNAMED);
        self.add_status(bucket, r.status.as_str(), 1);
        self.add_written(bucket, r.records_written);
        if r.status.is_terminal() {
            self.add_failure(name, 1, u64::from(r.status == RunStatus::Failed));
        }
        self.add_pipeline_written(name, r.records_written);
        for inv in &r.invocations {
            if let Some(rows) = &inv.rows {
                self.add_governance(bucket, &GovernanceCounts::of(rows));
            }
        }
    }

    /// Feed the durations of in-window, finished, in-scope runs, newest first.
    pub fn add_run_durations<'a>(&mut self, runs: impl IntoIterator<Item = &'a RunRecord>) {
        for r in runs {
            let name = r.name.as_deref().unwrap_or(UNNAMED);
            for inv in r.invocations.iter().filter(|i| !i.skipped) {
                if !self.add_duration(name, inv.duration_ms) {
                    return;
                }
            }
        }
    }

    pub fn finish(self, q: &OverviewQuery) -> (RunsSection, RecordsSection, GovernanceSection) {
        let top = q.top();
        let starts = q.bucket_starts();
        let empty = BucketTotals::default();
        let at = |t: &DateTime<Utc>| self.buckets.get(t).unwrap_or(&empty);
        let mut gov_totals = GovernanceCounts::default();
        let mut written_total = 0;
        for b in self.buckets.values() {
            gov_totals.add(&b.governance);
            written_total += b.written;
        }
        let runs = RunsSection {
            total: self.by_status.values().sum(),
            by_status: self.by_status.clone(),
            series: starts
                .iter()
                .map(|t| {
                    let b = at(t);
                    RunBucket {
                        bucket_start: *t,
                        succeeded: b.succeeded,
                        failed: b.failed,
                        cancelled: b.cancelled,
                    }
                })
                .collect(),
            failure_rate_by_pipeline: top_failures(&self.failures, top),
            slowest: slowest(&self.durations, top),
            duration_sample: DurationSample {
                invocations: self.sampled as u64,
                truncated: self.truncated,
            },
            queued: self.queued,
            running: self.running,
        };
        let records = RecordsSection {
            total: written_total,
            series: starts
                .iter()
                .map(|t| WrittenBucket {
                    bucket_start: *t,
                    written: at(t).written,
                })
                .collect(),
            top_pipelines: top_written(&self.written, top),
        };
        let governance = GovernanceSection {
            totals: gov_totals,
            series: starts
                .iter()
                .map(|t| GovernanceBucket {
                    bucket_start: *t,
                    counts: at(t).governance,
                })
                .collect(),
        };
        (runs, records, governance)
    }
}

/// Failing pipelines, highest failure rate first (then most failures, then
/// name), at most `top`.
pub fn top_failures(map: &BTreeMap<String, (u64, u64)>, top: usize) -> Vec<PipelineFailure> {
    let mut v: Vec<PipelineFailure> = map
        .iter()
        .filter(|(_, (runs, failed))| *failed > 0 && *runs > 0)
        .map(|(p, (runs, failed))| PipelineFailure {
            pipeline: p.clone(),
            runs: *runs,
            failed: *failed,
            rate: *failed as f64 / *runs as f64,
        })
        .collect();
    v.sort_by(|a, b| {
        b.rate
            .total_cmp(&a.rate)
            .then(b.failed.cmp(&a.failed))
            .then_with(|| a.pipeline.cmp(&b.pipeline))
    });
    v.truncate(top);
    v
}

/// Pipelines by records written, most first, at most `top`.
pub fn top_written(map: &BTreeMap<String, u64>, top: usize) -> Vec<PipelineWritten> {
    let mut v: Vec<PipelineWritten> = map
        .iter()
        .filter(|(_, n)| **n > 0)
        .map(|(p, n)| PipelineWritten {
            pipeline: p.clone(),
            written: *n,
        })
        .collect();
    v.sort_by(|a, b| {
        b.written
            .cmp(&a.written)
            .then_with(|| a.pipeline.cmp(&b.pipeline))
    });
    v.truncate(top);
    v
}

/// The nearest-rank 95th percentile of `values` (`0` for none).
pub fn p95(values: &[u64]) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let mut v = values.to_vec();
    v.sort_unstable();
    let rank = (v.len() * 95).div_ceil(100).max(1);
    v[rank - 1]
}

/// Pipelines by p95 invocation duration, slowest first, at most `top`.
pub fn slowest(map: &BTreeMap<String, Vec<u64>>, top: usize) -> Vec<PipelineLatency> {
    let mut v: Vec<PipelineLatency> = map
        .iter()
        .map(|(p, d)| PipelineLatency {
            pipeline: p.clone(),
            p95_ms: p95(d),
            runs: d.len() as u64,
        })
        .collect();
    v.sort_by(|a, b| {
        b.p95_ms
            .cmp(&a.p95_ms)
            .then_with(|| a.pipeline.cmp(&b.pipeline))
    });
    v.truncate(top);
    v
}

/// Bucket start → (estimated, hosted equivalent).
type CostBuckets = BTreeMap<DateTime<Utc>, (f64, f64)>;

/// Collects usage records' priced estimates, per currency.
#[derive(Default)]
pub struct CostAggregator {
    /// currency → (records, bucket → (estimated, hosted)).
    currencies: BTreeMap<String, (u64, CostBuckets)>,
    /// (currency, pipeline) → estimated.
    pipelines: BTreeMap<(String, String), f64>,
}

impl CostAggregator {
    pub fn add_bucket(
        &mut self,
        bucket: DateTime<Utc>,
        currency: &str,
        records: u64,
        estimated: f64,
        hosted: f64,
    ) {
        let c = self.currencies.entry(currency.to_string()).or_default();
        c.0 += records;
        let b = c.1.entry(bucket).or_default();
        b.0 += estimated;
        b.1 += hosted;
    }

    pub fn add_pipeline(&mut self, currency: &str, pipeline: &str, estimated: f64) {
        *self
            .pipelines
            .entry((currency.to_string(), pipeline.to_string()))
            .or_default() += estimated;
    }

    /// The memory path: one usage record.
    pub fn add_record(&mut self, q: &OverviewQuery, r: &crate::usage::UsageRecord) {
        let bucket = q.bucket.floor(r.recorded_at);
        self.add_bucket(
            bucket,
            &r.cost.currency,
            1,
            r.cost.total,
            r.cost.hosted_equivalent,
        );
        self.add_pipeline(&r.cost.currency, &r.pipeline, r.cost.total);
    }

    /// The currency the section reports in: the one with the most records
    /// (ties: alphabetical). `None` when nothing was recorded.
    pub fn dominant(&self) -> Option<String> {
        self.currencies
            .iter()
            .max_by(|a, b| a.1.0.cmp(&b.1.0).then_with(|| b.0.cmp(a.0)))
            .map(|(c, _)| c.clone())
    }

    pub fn finish(self, q: &OverviewQuery) -> Option<CostSection> {
        let currency = self.dominant()?;
        let (records, buckets) = self.currencies.get(&currency).cloned().unwrap_or_default();
        let mut by_pipeline: Vec<PipelineCost> = self
            .pipelines
            .iter()
            .filter(|((c, _), _)| *c == currency)
            .map(|((_, p), e)| PipelineCost {
                pipeline: p.clone(),
                estimated: *e,
            })
            .collect();
        by_pipeline.sort_by(|a, b| {
            b.estimated
                .total_cmp(&a.estimated)
                .then_with(|| a.pipeline.cmp(&b.pipeline))
        });
        by_pipeline.truncate(q.top());
        let (estimated, hosted_equivalent) = buckets
            .values()
            .fold((0.0, 0.0), |(e, h), (be, bh)| (e + be, h + bh));
        Some(CostSection {
            other_currencies: self
                .currencies
                .keys()
                .filter(|c| **c != currency)
                .cloned()
                .collect(),
            currency,
            invocations: records,
            estimated,
            hosted_equivalent,
            series: q
                .bucket_starts()
                .into_iter()
                .map(|t| {
                    let (e, h) = buckets.get(&t).copied().unwrap_or_default();
                    CostBucket {
                        bucket_start: t,
                        estimated: e,
                        hosted_equivalent: h,
                    }
                })
                .collect(),
            by_pipeline,
        })
    }
}

/// Whether `last_success` counts as stale against `before`, to the second
/// (how the SQL backends compare the stored text).
pub fn is_stale(last_success: DateTime<Utc>, before: DateTime<Utc>) -> bool {
    last_success.timestamp() < before.timestamp()
}

/// The freshness section over a catalog held in memory.
pub fn freshness_from<'a>(
    q: &OverviewQuery,
    datasets: impl IntoIterator<Item = &'a CatalogDataset>,
    versions: impl IntoIterator<Item = &'a CatalogSchemaVersion>,
) -> Option<FreshnessSection> {
    let before = q.stale_before?;
    let datasets: Vec<&CatalogDataset> = datasets.into_iter().collect();
    if datasets.is_empty() {
        return None;
    }
    let uri_of: BTreeMap<&str, &str> = datasets
        .iter()
        .map(|d| (d.id.as_str(), d.uri.as_str()))
        .collect();
    let mut stale: Vec<StaleDataset> = datasets
        .iter()
        .filter(|d| is_stale(d.last_success, before))
        .map(|d| StaleDataset {
            id: d.id.clone(),
            uri: d.uri.clone(),
            pipeline: d.pipeline.clone(),
            last_success: d.last_success,
        })
        .collect();
    stale.sort_by(|a, b| {
        a.last_success
            .cmp(&b.last_success)
            .then_with(|| a.id.cmp(&b.id))
    });
    let stale_count = stale.len() as u64;
    stale.truncate(q.top());
    let mut changed: BTreeMap<String, (u64, DateTime<Utc>)> = BTreeMap::new();
    for v in versions {
        if v.version > 1 && q.in_window(v.recorded_at) {
            let e = changed
                .entry(v.dataset_id.clone())
                .or_insert((0, v.recorded_at));
            e.0 += 1;
            e.1 = e.1.max(v.recorded_at);
        }
    }
    let mut schema_changed: Vec<SchemaChange> = changed
        .into_iter()
        .map(|(id, (changes, last_changed))| SchemaChange {
            uri: uri_of
                .get(id.as_str())
                .copied()
                .unwrap_or_default()
                .to_string(),
            id,
            changes,
            last_changed,
        })
        .collect();
    schema_changed.sort_by(|a, b| {
        b.last_changed
            .cmp(&a.last_changed)
            .then_with(|| a.id.cmp(&b.id))
    });
    let schema_changed_count = schema_changed.len() as u64;
    schema_changed.truncate(q.top());
    Some(FreshnessSection {
        datasets: datasets.len() as u64,
        stale_before: before,
        stale_count,
        stale,
        schema_changed_count,
        schema_changed,
    })
}

/// Aggregate run records and usage records held in memory. `runs` may hold
/// anything; the query decides what counts.
pub fn aggregate_in_memory<'a>(
    q: &OverviewQuery,
    runs: impl IntoIterator<Item = &'a RunRecord>,
    usage: impl IntoIterator<Item = &'a crate::usage::UsageRecord>,
    freshness: Option<FreshnessSection>,
) -> OverviewAggregates {
    let mut scoped: Vec<&RunRecord> = runs.into_iter().filter(|r| q.in_scope(r)).collect();
    let scoped_ids: std::collections::HashSet<&str> = scoped
        .iter()
        .flat_map(|r| {
            std::iter::once(r.run_id.as_str())
                .chain(r.invocations.iter().filter_map(|i| i.run_id.as_deref()))
        })
        .collect();
    let mut agg = Aggregator::default();
    for r in &scoped {
        agg.add_run(q, r);
    }
    scoped.retain(|r| q.in_window(r.submitted_at) && r.status.is_terminal());
    scoped.sort_by(|a, b| {
        b.submitted_at
            .cmp(&a.submitted_at)
            .then_with(|| b.run_id.cmp(&a.run_id))
    });
    agg.add_run_durations(scoped.iter().copied());
    let (runs, records, governance) = agg.finish(q);
    let mut cost = CostAggregator::default();
    for u in usage {
        if usage_in_scope(q, u) && (!filters_by_run(q) || scoped_ids.contains(u.run_id.as_str())) {
            cost.add_record(q, u);
        }
    }
    OverviewAggregates {
        runs,
        records,
        governance,
        cost: cost.finish(q),
        freshness,
    }
}

/// The trait default: read through `list`, `usage_list` and the catalog
/// listing, at most [`DEFAULT_SCAN_CAP`] runs and usage records.
pub async fn aggregate_by_listing<H: super::RunHistory + ?Sized>(
    h: &H,
    q: &OverviewQuery,
) -> Result<OverviewAggregates, super::HistoryError> {
    let mut runs: BTreeMap<String, RunRecord> = BTreeMap::new();
    let windowed = super::ListFilter {
        name: q.pipeline.clone(),
        since: Some(q.since),
        until: Some(q.until),
        tenant: q.tenant.clone(),
        labels: q.labels.clone(),
        ..Default::default()
    };
    let current = super::ListFilter {
        status: vec![
            RunStatus::Queued,
            RunStatus::Pending,
            RunStatus::Running,
            RunStatus::Sharded,
        ],
        since: None,
        until: None,
        ..windowed.clone()
    };
    for base in [windowed, current] {
        let mut filter = super::ListFilter {
            limit: 1_000,
            ..base
        };
        let mut read = 0;
        loop {
            let page = h.list(&filter).await?;
            read += page.runs.len();
            for r in page.runs {
                runs.insert(r.run_id.clone(), r);
            }
            match page.next_cursor {
                Some(c) if read < DEFAULT_SCAN_CAP => filter.cursor = Some(c),
                _ => break,
            }
        }
    }
    let usage = h
        .usage_list(&crate::usage::UsageFilter {
            since: Some(q.since),
            until: Some(q.until),
            pipeline: q.pipeline.clone(),
            tenant: q.tenant.clone(),
            limit: DEFAULT_SCAN_CAP,
        })
        .await?;
    let freshness = match q.stale_before {
        None => None,
        Some(_) => {
            let page = h
                .catalog_list_datasets(&super::catalog::CatalogListFilter {
                    limit: DEFAULT_SCAN_CAP,
                    ..Default::default()
                })
                .await?;
            freshness_from(q, &page.datasets, std::iter::empty())
        }
    };
    Ok(aggregate_in_memory(q, runs.values(), &usage, freshness))
}

/// Whether a usage record falls in the window and the tenant filter. The
/// pipeline, pool and label filters apply through its run (see the callers):
/// a usage record names the invocation's run id.
pub fn usage_in_scope(q: &OverviewQuery, u: &crate::usage::UsageRecord) -> bool {
    q.in_window(u.recorded_at)
        && q.tenant
            .as_deref()
            .is_none_or(|t| u.tenant.as_deref() == Some(t))
}

/// Whether the query narrows runs by something a usage record does not carry
/// (the run's name, pool or labels).
pub fn filters_by_run(q: &OverviewQuery) -> bool {
    q.pipeline.is_some() || q.pool.is_some() || !q.labels.is_empty()
}

/// SQL text for the `overview_sql!` queries, per dialect. Every value is a
/// bind; the queries read the fixed-width RFC 3339 text columns and the JSON
/// bodies.
#[cfg(any(feature = "serve-history-postgres", feature = "serve-history-sqlite"))]
pub mod sql {
    use super::{DURATION_SAMPLE_CAP, OverviewQuery};
    use crate::serve::history::sql::{Dialect, fmt_ts};

    /// A statement under construction: its binds, numbered for the dialect.
    pub struct Built {
        pub dialect: Dialect,
        pub binds: Vec<String>,
    }

    impl Built {
        pub fn new(dialect: Dialect) -> Self {
            Self {
                dialect,
                binds: Vec::new(),
            }
        }

        pub fn bind(&mut self, v: impl Into<String>) -> String {
            self.binds.push(v.into());
            match self.dialect {
                Dialect::Postgres => format!("${}", self.binds.len()),
                Dialect::Sqlite => "?".into(),
            }
        }

        fn pg(&self) -> bool {
            matches!(self.dialect, Dialect::Postgres)
        }

        /// The JSON document a text column holds.
        fn doc(&self, col: &str) -> String {
            if self.pg() {
                format!("CAST({col} AS json)")
            } else {
                col.to_string()
            }
        }

        /// A text value at `path` of a JSON document (`doc` already JSON for
        /// Postgres: see `doc`).
        pub fn text(&self, doc: &str, path: &[&str]) -> String {
            if self.pg() {
                format!("({doc} #>> '{{{}}}')", path.join(","))
            } else {
                format!("json_extract({doc}, '$.{}')", path.join("."))
            }
        }

        /// An integer at `path`, `0` when absent.
        pub fn int(&self, doc: &str, path: &[&str]) -> String {
            if self.pg() {
                format!("COALESCE(CAST({} AS BIGINT), 0)", self.text(doc, path))
            } else {
                format!("COALESCE(CAST({} AS INTEGER), 0)", self.text(doc, path))
            }
        }

        /// A number at `path`, `0` when absent.
        pub fn float(&self, doc: &str, path: &[&str]) -> String {
            if self.pg() {
                format!(
                    "COALESCE(CAST({} AS DOUBLE PRECISION), 0)",
                    self.text(doc, path)
                )
            } else {
                format!("COALESCE(CAST({} AS REAL), 0)", self.text(doc, path))
            }
        }

        /// `SUM` of an integer expression, as a 64-bit integer.
        pub fn sum_int(&self, e: &str) -> String {
            if self.pg() {
                format!("CAST(COALESCE(SUM({e}), 0) AS BIGINT)")
            } else {
                format!("COALESCE(SUM({e}), 0)")
            }
        }

        /// `SUM` of a float expression, as a double.
        pub fn sum_float(&self, e: &str) -> String {
            if self.pg() {
                format!("CAST(COALESCE(SUM({e}), 0) AS DOUBLE PRECISION)")
            } else {
                format!("CAST(COALESCE(SUM({e}), 0) AS REAL)")
            }
        }

        fn count_if(&self, cond: &str) -> String {
            self.sum_int(&format!("CASE WHEN {cond} THEN 1 ELSE 0 END"))
        }

        fn greatest0(&self, e: &str) -> String {
            if self.pg() {
                format!("GREATEST({e}, 0)")
            } else {
                format!("MAX({e}, 0)")
            }
        }

        /// `faucet_serve_runs r` joined to one row per invocation `i`.
        fn invocations_from(&self) -> String {
            if self.pg() {
                "faucet_serve_runs r CROSS JOIN LATERAL \
                 json_array_elements(CAST(r.body AS json) -> 'invocations') AS i(value)"
                    .into()
            } else {
                "faucet_serve_runs r, json_each(r.body, '$.invocations') AS i".into()
            }
        }

        fn not_skipped(&self) -> String {
            if self.pg() {
                "COALESCE(i.value ->> 'skipped', 'false') <> 'true'".into()
            } else {
                "COALESCE(json_extract(i.value, '$.skipped'), 0) = 0".into()
            }
        }

        /// The filter clauses (not the window) on runs aliased `r`.
        pub fn run_scope(&mut self, q: &OverviewQuery) -> Vec<String> {
            let mut out = self.run_only_scope(q);
            if let Some(t) = &q.tenant {
                let b = self.bind(t.clone());
                out.push(format!(
                    "r.run_id IN (SELECT run_id FROM faucet_tenant_runs WHERE tenant = {b})"
                ));
            }
            out
        }

        /// The clauses only a run carries: pool and labels.
        fn run_only_scope(&mut self, q: &OverviewQuery) -> Vec<String> {
            let mut out = Vec::new();
            if let Some(p) = &q.pipeline {
                let b = self.bind(p.clone());
                out.push(format!("COALESCE(r.name, '') = {b}"));
            }
            if let Some(p) = &q.pool {
                let b = self.bind(p.clone());
                out.push(format!("COALESCE(r.pool, 'default') = {b}"));
            }
            for f in &q.labels {
                let k = self.bind(f.key.clone());
                match &f.value {
                    Some(v) => {
                        let v = self.bind(v.clone());
                        out.push(format!(
                            "r.run_id IN (SELECT run_id FROM faucet_serve_run_labels \
                             WHERE key = {k} AND value = {v})"
                        ));
                    }
                    None => out.push(format!(
                        "r.run_id IN (SELECT run_id FROM faucet_serve_run_labels WHERE key = {k})"
                    )),
                }
            }
            out
        }

        fn run_window(&mut self, q: &OverviewQuery, col: &str) -> Vec<String> {
            let s = self.bind(fmt_ts(q.since));
            let u = self.bind(fmt_ts(q.until));
            vec![format!("{col} >= {s}"), format!("{col} < {u}")]
        }

        fn run_where(&mut self, q: &OverviewQuery) -> String {
            let mut c = self.run_window(q, "r.submitted_at");
            c.extend(self.run_scope(q));
            c.join(" AND ")
        }
    }

    fn bucket(q: &OverviewQuery, col: &str) -> String {
        format!("substr({col}, 1, {})", q.bucket.key_len())
    }

    const TERMINAL: &str = "r.status IN ('completed','failed','cancelled')";

    /// `(b, s, n, w)`: runs and records written per bucket and status.
    pub fn status_by_bucket(d: Dialect, q: &OverviewQuery) -> (String, Vec<String>) {
        let mut x = Built::new(d);
        let w = x.run_where(q);
        let b = bucket(q, "r.submitted_at");
        let written = x.sum_int(&x.int(&x.doc("r.body"), &["records_written"]));
        (
            format!(
                "SELECT {b} AS b, r.status AS s, COUNT(*) AS n, {written} AS w \
                 FROM faucet_serve_runs r WHERE {w} GROUP BY {b}, r.status"
            ),
            x.binds,
        )
    }

    /// `(p, n, f)`: the failing pipelines, highest failure rate first.
    pub fn failures(d: Dialect, q: &OverviewQuery) -> (String, Vec<String>) {
        let mut x = Built::new(d);
        let w = x.run_where(q);
        let finished = x.count_if(TERMINAL);
        let failed = x.count_if("r.status = 'failed'");
        let real = if x.pg() { "DOUBLE PRECISION" } else { "REAL" };
        (
            format!(
                "SELECT COALESCE(r.name, '') AS p, {finished} AS n, {failed} AS f \
                 FROM faucet_serve_runs r WHERE {w} GROUP BY COALESCE(r.name, '') \
                 HAVING {failed} > 0 \
                 ORDER BY CAST({failed} AS {real}) / {finished} DESC, {failed} DESC, \
                 COALESCE(r.name, '') ASC LIMIT {}",
                q.top()
            ),
            x.binds,
        )
    }

    /// `(p, w)`: pipelines by records written.
    pub fn top_written(d: Dialect, q: &OverviewQuery) -> (String, Vec<String>) {
        let mut x = Built::new(d);
        let w = x.run_where(q);
        let written = x.sum_int(&x.int(&x.doc("r.body"), &["records_written"]));
        (
            format!(
                "SELECT COALESCE(r.name, '') AS p, {written} AS w \
                 FROM faucet_serve_runs r WHERE {w} GROUP BY COALESCE(r.name, '') \
                 HAVING {written} > 0 ORDER BY {written} DESC, COALESCE(r.name, '') ASC LIMIT {}",
                q.top()
            ),
            x.binds,
        )
    }

    /// `(b, filtered, quarantined, dlq, coalesced, skipped, dropped)` per
    /// bucket, summed over every invocation's row accounting.
    pub fn governance(d: Dialect, q: &OverviewQuery) -> (String, Vec<String>) {
        let mut x = Built::new(d);
        let w = x.run_where(q);
        let b = bucket(q, "r.submitted_at");
        let f = |x: &Built, k: &str| x.sum_int(&x.int("i.value", &["rows", k]));
        let filtered = x.sum_int(&x.greatest0(&format!(
            "{} - {}",
            x.int("i.value", &["rows", "transform_in"]),
            x.int("i.value", &["rows", "transform_out"])
        )));
        (
            format!(
                "SELECT {b} AS b, {filtered} AS filtered, {} AS quarantined, {} AS dlq, \
                 {} AS coalesced, {} AS skipped, {} AS dropped FROM {} WHERE {w} GROUP BY {b}",
                f(&x, "quarantined"),
                f(&x, "failed"),
                f(&x, "coalesced"),
                f(&x, "skipped"),
                f(&x, "dropped"),
                x.invocations_from(),
            ),
            x.binds,
        )
    }

    /// `(p, d)`: finished invocations' durations, newest first, one past the
    /// sample cap (so a full sample is detected).
    pub fn durations(d: Dialect, q: &OverviewQuery) -> (String, Vec<String>) {
        let mut x = Built::new(d);
        let w = x.run_where(q);
        (
            format!(
                "SELECT COALESCE(r.name, '') AS p, {} AS d FROM {} \
                 WHERE {w} AND {TERMINAL} AND {} \
                 ORDER BY r.submitted_at DESC, r.run_id DESC LIMIT {}",
                x.int("i.value", &["duration_ms"]),
                x.invocations_from(),
                x.not_skipped(),
                DURATION_SAMPLE_CAP + 1
            ),
            x.binds,
        )
    }

    /// `(s, n)`: runs waiting or executing now, in scope.
    pub fn current(d: Dialect, q: &OverviewQuery) -> (String, Vec<String>) {
        let mut x = Built::new(d);
        let mut c = vec!["r.status IN ('queued','pending','running','sharded')".to_string()];
        c.extend(x.run_scope(q));
        (
            format!(
                "SELECT r.status AS s, COUNT(*) AS n FROM faucet_serve_runs r \
                 WHERE {} GROUP BY r.status",
                c.join(" AND ")
            ),
            x.binds,
        )
    }

    fn usage_where(x: &mut Built, q: &OverviewQuery) -> Vec<String> {
        let mut c = x.run_window(q, "u.recorded_at");
        if let Some(t) = &q.tenant {
            let b = x.bind(t.clone());
            c.push(format!("u.tenant = {b}"));
        }
        if super::filters_by_run(q) {
            let runs = x.run_only_scope(q).join(" AND ");
            let invocations = x.run_only_scope(q).join(" AND ");
            let inv_id = x.text("i.value", &["run_id"]);
            c.push(format!(
                "(u.run_id IN (SELECT r.run_id FROM faucet_serve_runs r WHERE {runs}) \
                 OR u.run_id IN (SELECT {inv_id} FROM {} WHERE {invocations}))",
                x.invocations_from()
            ));
        }
        c
    }

    fn currency(x: &Built) -> String {
        format!(
            "COALESCE({}, '')",
            x.text(&x.doc("u.body"), &["cost", "currency"])
        )
    }

    /// `(b, c, n, e, h)`: usage per bucket and currency.
    pub fn cost_by_bucket(d: Dialect, q: &OverviewQuery) -> (String, Vec<String>) {
        let mut x = Built::new(d);
        let w = usage_where(&mut x, q).join(" AND ");
        let b = bucket(q, "u.recorded_at");
        let c = currency(&x);
        let e = x.sum_float(&x.float(&x.doc("u.body"), &["cost", "total"]));
        let h = x.sum_float(&x.float(&x.doc("u.body"), &["cost", "hosted_equivalent"]));
        (
            format!(
                "SELECT {b} AS b, {c} AS c, COUNT(*) AS n, {e} AS e, {h} AS h \
                 FROM faucet_usage u WHERE {w} GROUP BY {b}, {c}"
            ),
            x.binds,
        )
    }

    /// `(p, e)`: pipelines by estimated cost in `currency`.
    pub fn cost_by_pipeline(d: Dialect, q: &OverviewQuery, cur: &str) -> (String, Vec<String>) {
        let mut x = Built::new(d);
        let mut w = usage_where(&mut x, q);
        let c = currency(&x);
        let cb = x.bind(cur);
        w.push(format!("{c} = {cb}"));
        let e = x.sum_float(&x.float(&x.doc("u.body"), &["cost", "total"]));
        (
            format!(
                "SELECT u.pipeline AS p, {e} AS e FROM faucet_usage u WHERE {} \
                 GROUP BY u.pipeline ORDER BY {e} DESC, u.pipeline ASC LIMIT {}",
                w.join(" AND "),
                q.top()
            ),
            x.binds,
        )
    }

    /// `(n)`: datasets in the catalog.
    pub fn dataset_count() -> String {
        "SELECT COUNT(*) AS n FROM faucet_catalog_datasets".into()
    }

    fn stale_clause(x: &mut Built, before: chrono::DateTime<chrono::Utc>) -> String {
        let ts = x.text(&x.doc("d.body"), &["last_success"]);
        let b = x.bind(fmt_ts(before)[..19].to_string());
        format!("substr({ts}, 1, 19) < {b}")
    }

    /// `(n)`: stale datasets.
    pub fn stale_count(d: Dialect, before: chrono::DateTime<chrono::Utc>) -> (String, Vec<String>) {
        let mut x = Built::new(d);
        let c = stale_clause(&mut x, before);
        (
            format!("SELECT COUNT(*) AS n FROM faucet_catalog_datasets d WHERE {c}"),
            x.binds,
        )
    }

    /// `(id, uri, p, t)`: the stalest datasets.
    pub fn stale_list(
        d: Dialect,
        q: &OverviewQuery,
        before: chrono::DateTime<chrono::Utc>,
    ) -> (String, Vec<String>) {
        let mut x = Built::new(d);
        let c = stale_clause(&mut x, before);
        let doc = x.doc("d.body");
        (
            format!(
                "SELECT d.id AS id, d.uri AS uri, COALESCE({}, '') AS p, {} AS t \
                 FROM faucet_catalog_datasets d WHERE {c} \
                 ORDER BY {} ASC, d.id ASC LIMIT {}",
                x.text(&doc, &["pipeline"]),
                x.text(&doc, &["last_success"]),
                x.text(&doc, &["last_success"]),
                q.top()
            ),
            x.binds,
        )
    }

    fn schema_where(x: &mut Built, q: &OverviewQuery) -> String {
        let mut c = x.run_window(q, "v.recorded_at");
        c.push("CAST(v.version AS INTEGER) > 1".into());
        c.join(" AND ")
    }

    /// `(n)`: datasets whose schema changed in the window.
    pub fn schema_changed_count(d: Dialect, q: &OverviewQuery) -> (String, Vec<String>) {
        let mut x = Built::new(d);
        let w = schema_where(&mut x, q);
        (
            format!(
                "SELECT COUNT(DISTINCT v.dataset_id) AS n \
                 FROM faucet_catalog_schema_versions v WHERE {w}"
            ),
            x.binds,
        )
    }

    /// `(id, uri, n, t)`: datasets whose schema changed, latest change first.
    pub fn schema_changed(d: Dialect, q: &OverviewQuery) -> (String, Vec<String>) {
        let mut x = Built::new(d);
        let w = schema_where(&mut x, q);
        (
            format!(
                "SELECT v.dataset_id AS id, COALESCE(MAX(ds.uri), '') AS uri, COUNT(*) AS n, \
                 MAX(v.recorded_at) AS t FROM faucet_catalog_schema_versions v \
                 LEFT JOIN faucet_catalog_datasets ds ON ds.id = v.dataset_id WHERE {w} \
                 GROUP BY v.dataset_id ORDER BY MAX(v.recorded_at) DESC, v.dataset_id ASC LIMIT {}",
                q.top()
            ),
            x.binds,
        )
    }
}

/// `RunHistory::overview_aggregates` for the SQL backends: the queries of
/// [`sql`] fed through [`Aggregator`] / [`CostAggregator`].
#[cfg(any(feature = "serve-history-postgres", feature = "serve-history-sqlite"))]
macro_rules! overview_sql {
    () => {
        /// `RunHistory::overview_aggregates`, grouped in the database.
        async fn overview_from_sql(
            &self,
            q: &$crate::serve::history::overview::OverviewQuery,
        ) -> Result<
            $crate::serve::history::overview::OverviewAggregates,
            $crate::serve::history::HistoryError,
        > {
            use sqlx::Row as _;
            use $crate::serve::history::overview::{self as ov, sql as osql};
            let backend = $crate::serve::history::sql::classify_backend_error;
            let d = self.stmts.dialect;
            macro_rules! fetch {
                ($built:expr) => {{
                    let (text, binds) = $built;
                    let mut query = sqlx::query(&text);
                    for b in &binds {
                        query = query.bind(b);
                    }
                    query.fetch_all(&self.pool).await.map_err(backend)?
                }};
            }
            let key = |s: String| {
                q.bucket.parse_key(&s).ok_or_else(|| {
                    $crate::serve::history::HistoryError::Backend(format!(
                        "overview: unreadable bucket `{s}`"
                    ))
                })
            };
            let u = |v: i64| u64::try_from(v).unwrap_or(0);
            let mut agg = ov::Aggregator::default();
            for r in fetch!(osql::status_by_bucket(d, q)) {
                let b = key(r.try_get("b").map_err(backend)?)?;
                let s: String = r.try_get("s").map_err(backend)?;
                agg.add_status(b, &s, u(r.try_get("n").map_err(backend)?));
                agg.add_written(b, u(r.try_get("w").map_err(backend)?));
            }
            for r in fetch!(osql::failures(d, q)) {
                let p: String = r.try_get("p").map_err(backend)?;
                agg.add_failure(
                    &p,
                    u(r.try_get("n").map_err(backend)?),
                    u(r.try_get("f").map_err(backend)?),
                );
            }
            for r in fetch!(osql::top_written(d, q)) {
                let p: String = r.try_get("p").map_err(backend)?;
                agg.add_pipeline_written(&p, u(r.try_get("w").map_err(backend)?));
            }
            for r in fetch!(osql::governance(d, q)) {
                let b = key(r.try_get("b").map_err(backend)?)?;
                let g = ov::GovernanceCounts {
                    filtered: u(r.try_get("filtered").map_err(backend)?),
                    quarantined: u(r.try_get("quarantined").map_err(backend)?),
                    dlq: u(r.try_get("dlq").map_err(backend)?),
                    coalesced: u(r.try_get("coalesced").map_err(backend)?),
                    skipped: u(r.try_get("skipped").map_err(backend)?),
                    dropped: u(r.try_get("dropped").map_err(backend)?),
                };
                agg.add_governance(b, &g);
            }
            for r in fetch!(osql::durations(d, q)) {
                let p: String = r.try_get("p").map_err(backend)?;
                if !agg.add_duration(&p, u(r.try_get("d").map_err(backend)?)) {
                    break;
                }
            }
            for r in fetch!(osql::current(d, q)) {
                let s: String = r.try_get("s").map_err(backend)?;
                agg.add_current(&s, u(r.try_get("n").map_err(backend)?));
            }
            let (runs, records, governance) = agg.finish(q);

            let mut cost = ov::CostAggregator::default();
            for r in fetch!(osql::cost_by_bucket(d, q)) {
                let b = key(r.try_get("b").map_err(backend)?)?;
                let c: String = r.try_get("c").map_err(backend)?;
                cost.add_bucket(
                    b,
                    &c,
                    u(r.try_get("n").map_err(backend)?),
                    r.try_get("e").map_err(backend)?,
                    r.try_get("h").map_err(backend)?,
                );
            }
            if let Some(cur) = cost.dominant() {
                for r in fetch!(osql::cost_by_pipeline(d, q, &cur)) {
                    let p: String = r.try_get("p").map_err(backend)?;
                    cost.add_pipeline(&cur, &p, r.try_get("e").map_err(backend)?);
                }
            }

            let freshness = match q.stale_before {
                None => None,
                Some(before) => {
                    let total = fetch!((osql::dataset_count(), Vec::<String>::new()))
                        .first()
                        .map(|r| r.try_get::<i64, _>("n"))
                        .transpose()
                        .map_err(backend)?
                        .unwrap_or(0);
                    if total == 0 {
                        None
                    } else {
                        macro_rules! count {
                            ($rows:expr) => {
                                $rows
                                    .first()
                                    .map(|r| r.try_get::<i64, _>("n"))
                                    .transpose()
                                    .map_err(backend)?
                                    .map(u)
                                    .unwrap_or(0)
                            };
                        }
                        let ts = |s: String| {
                            chrono::DateTime::parse_from_rfc3339(&s)
                                .map(|t| t.with_timezone(&chrono::Utc))
                                .map_err(|e| {
                                    $crate::serve::history::HistoryError::Backend(format!(
                                        "overview: unreadable timestamp `{s}`: {e}"
                                    ))
                                })
                        };
                        let stale_count = count!(fetch!(osql::stale_count(d, before)));
                        let mut stale = Vec::new();
                        for r in fetch!(osql::stale_list(d, q, before)) {
                            stale.push(ov::StaleDataset {
                                id: r.try_get("id").map_err(backend)?,
                                uri: r.try_get("uri").map_err(backend)?,
                                pipeline: r.try_get("p").map_err(backend)?,
                                last_success: ts(r.try_get("t").map_err(backend)?)?,
                            });
                        }
                        let schema_changed_count = count!(fetch!(osql::schema_changed_count(d, q)));
                        let mut schema_changed = Vec::new();
                        for r in fetch!(osql::schema_changed(d, q)) {
                            schema_changed.push(ov::SchemaChange {
                                id: r.try_get("id").map_err(backend)?,
                                uri: r.try_get("uri").map_err(backend)?,
                                changes: u(r.try_get("n").map_err(backend)?),
                                last_changed: ts(r.try_get("t").map_err(backend)?)?,
                            });
                        }
                        Some(ov::FreshnessSection {
                            datasets: u(total),
                            stale_before: before,
                            stale_count,
                            stale,
                            schema_changed_count,
                            schema_changed,
                        })
                    }
                }
            };
            Ok(ov::OverviewAggregates {
                runs,
                records,
                governance,
                cost: cost.finish(q),
                freshness,
            })
        }
    };
}
#[cfg(any(feature = "serve-history-postgres", feature = "serve-history-sqlite"))]
pub(crate) use overview_sql;

#[cfg(test)]
mod tests;
