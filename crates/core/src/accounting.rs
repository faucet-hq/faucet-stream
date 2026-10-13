//! Per-page row accounting (RFC 0013, #879).
//!
//! Every row the pipeline hands a sink must end up somewhere it can name:
//!
//! ```text
//! offered = accepted + coalesced + skipped + failed (DLQ) + quarantined + dropped
//! ```
//!
//! - **accepted** — the sink says it wrote the row;
//! - **coalesced** — the sink merged the row into another one of the same page
//!   (keyed upsert / delete, last write wins) and reported it through
//!   [`Sink::write_batch_reported`](crate::Sink::write_batch_reported);
//! - **skipped** — the sink deliberately did not write the row and said so in
//!   its report (a preview `--limit` cap, a record its configuration declines
//!   by design);
//! - **failed** — the sink reported a per-row failure and the row went to the
//!   DLQ;
//! - **quarantined** — a governance pass (quality, contract, schema drift,
//!   policy) routed the row to the DLQ before the write;
//! - **dropped** — a `resilience.poison.action: drop` discarded the row.
//!
//! A sink whose plain write returns fewer rows than it was offered, with no
//! per-row outcomes and no coalescing report, fails the page with
//! [`FaucetError::RowsUnaccounted`] before the bookmark advances, so a sink that
//! silently drops rows can no longer report a green run. Rows a transform
//! filtered out (or added) never reach the page, so they are counted
//! separately ([`RowAccountsSnapshot::transform_in`] / [`RowAccountsSnapshot::transform_out`]).

use crate::error::FaucetError;
use std::sync::atomic::{AtomicU64, Ordering};

/// What a sink did with a page it was offered, beyond the rows it wrote.
///
/// Returned by [`Sink::write_batch_reported`](crate::Sink::write_batch_reported).
/// A sink that merges rows within a page (keyed upsert / delete, last write
/// wins) reports the merged rows as `coalesced`, so the pipeline can tell them
/// apart from rows it dropped.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct WriteReport {
    /// Rows the sink wrote (what [`Sink::write_batch`](crate::Sink::write_batch)
    /// returns).
    pub accepted: usize,
    /// Rows merged into another row of the same page and therefore not written
    /// on their own.
    pub coalesced: usize,
    /// Rows the sink deliberately did not write, by design of its
    /// configuration (a preview cap, a declined record), and says so.
    pub skipped: usize,
}

impl WriteReport {
    /// A report of `accepted` written rows and nothing coalesced.
    pub fn accepted(accepted: usize) -> Self {
        Self {
            accepted,
            coalesced: 0,
            skipped: 0,
        }
    }

    /// The same report with `coalesced` merged rows.
    pub fn with_coalesced(mut self, coalesced: usize) -> Self {
        self.coalesced = coalesced;
        self
    }

    /// The same report with `skipped` rows the sink declined by design.
    pub fn with_skipped(mut self, skipped: usize) -> Self {
        self.skipped = skipped;
        self
    }

    /// For a sink that wrote `planned` rows out of `offered` after merging rows
    /// of the same key: `planned` accepted, the rest coalesced.
    pub fn keyed(offered: usize, planned: usize) -> Self {
        Self::accepted(planned).with_coalesced(offered.saturating_sub(planned))
    }

    /// Rows the report accounts for.
    pub fn accounted(&self) -> usize {
        self.accepted
            .saturating_add(self.coalesced)
            .saturating_add(self.skipped)
    }

    /// `Ok` when every one of `offered` rows is accounted for. A sink that
    /// reports more than it was offered (e.g. a database counting an update as
    /// two affected rows) is not a loss.
    pub fn check(&self, offered: usize) -> Result<(), FaucetError> {
        if self.accounted() >= offered {
            Ok(())
        } else {
            Err(FaucetError::RowsUnaccounted {
                offered: offered as u64,
                accepted: self.accepted as u64,
            })
        }
    }
}

/// Row counters for one run, filled in by the pipeline (and, for the transform
/// counts, by [`TransformingSource`](crate::TransformingSource)). Attach with
/// [`Pipeline::with_row_accounts`](crate::Pipeline::with_row_accounts); the
/// counts are complete when the run returns, `Err` included.
#[derive(Debug, Default)]
pub struct RowAccounts {
    transform_in: AtomicU64,
    transform_out: AtomicU64,
    offered: AtomicU64,
    accepted: AtomicU64,
    coalesced: AtomicU64,
    skipped: AtomicU64,
    failed: AtomicU64,
    quarantined: AtomicU64,
    dropped: AtomicU64,
    unaccounted: AtomicU64,
    non_finite: AtomicU64,
}

/// A point-in-time copy of [`RowAccounts`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
#[serde(default)]
pub struct RowAccountsSnapshot {
    /// Rows entering the transform chain.
    pub transform_in: u64,
    /// Rows the transform chain emitted.
    pub transform_out: u64,
    /// Rows offered to the governance passes and the sink.
    pub offered: u64,
    /// Rows the sink wrote.
    pub accepted: u64,
    /// Rows the sink merged into another row of the same page.
    pub coalesced: u64,
    /// Rows the sink declined by design and reported.
    pub skipped: u64,
    /// Rows the sink failed per row (routed to the DLQ).
    pub failed: u64,
    /// Rows a governance pass quarantined to the DLQ.
    pub quarantined: u64,
    /// Rows a poison-pill `drop` action discarded.
    pub dropped: u64,
    /// Rows a sink lost without saying so (a failed run).
    pub unaccounted: u64,
    /// Values with no JSON rendering (`NaN`, `±Infinity`) the source wrote as
    /// `null` (#885). A value count, not a row count.
    pub non_finite: u64,
}

impl RowAccountsSnapshot {
    /// Rows a transform removed (net; `0` when the chain expanded the input).
    pub fn filtered(&self) -> u64 {
        self.transform_in.saturating_sub(self.transform_out)
    }

    /// Whether every offered row is accounted for.
    pub fn balanced(&self) -> bool {
        self.accepted
            + self.coalesced
            + self.skipped
            + self.failed
            + self.quarantined
            + self.dropped
            >= self.offered
            && self.unaccounted == 0
    }

    /// One line for a run summary.
    pub fn summary_line(&self) -> String {
        let mut line = format!("rows: offered {}, accepted {}", self.offered, self.accepted);
        for (label, n) in [
            ("coalesced", self.coalesced),
            ("skipped", self.skipped),
            ("failed to DLQ", self.failed),
            ("quarantined", self.quarantined),
            ("dropped", self.dropped),
            ("filtered by transforms", self.filtered()),
            ("unaccounted", self.unaccounted),
            ("non-finite values written as null", self.non_finite),
        ] {
            if n > 0 {
                line.push_str(&format!(", {label} {n}"));
            }
        }
        line
    }
}

impl RowAccounts {
    /// Empty counters.
    pub fn new() -> Self {
        Self::default()
    }

    /// Copy the counters.
    pub fn snapshot(&self) -> RowAccountsSnapshot {
        let g = |a: &AtomicU64| a.load(Ordering::Relaxed);
        RowAccountsSnapshot {
            transform_in: g(&self.transform_in),
            transform_out: g(&self.transform_out),
            offered: g(&self.offered),
            accepted: g(&self.accepted),
            coalesced: g(&self.coalesced),
            skipped: g(&self.skipped),
            failed: g(&self.failed),
            quarantined: g(&self.quarantined),
            dropped: g(&self.dropped),
            unaccounted: g(&self.unaccounted),
            non_finite: g(&self.non_finite),
        }
    }

    pub(crate) fn add_non_finite(&self, n: u64) {
        self.non_finite.fetch_add(n, Ordering::Relaxed);
    }

    pub(crate) fn add_transform(&self, input: usize, output: usize) {
        self.transform_in.fetch_add(input as u64, Ordering::Relaxed);
        self.transform_out
            .fetch_add(output as u64, Ordering::Relaxed);
    }
}

/// One page's account, checked before its bookmark may advance.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct PageAccount {
    pub(crate) offered: usize,
    pub(crate) accepted: usize,
    pub(crate) coalesced: usize,
    pub(crate) skipped: usize,
    pub(crate) failed: usize,
    pub(crate) quarantined: usize,
    pub(crate) dropped: usize,
}

impl PageAccount {
    pub(crate) fn new(offered: usize) -> Self {
        Self {
            offered,
            ..Self::default()
        }
    }

    pub(crate) fn add_report(&mut self, r: WriteReport) {
        self.accepted += r.accepted;
        self.coalesced += r.coalesced;
        self.skipped += r.skipped;
    }

    fn accounted(&self) -> usize {
        self.accepted
            + self.coalesced
            + self.skipped
            + self.failed
            + self.quarantined
            + self.dropped
    }

    /// Fold the page into the run's counters and fail it when rows are
    /// missing. `labels` = (pipeline, row, connector) for the metric.
    pub(crate) fn settle(
        &self,
        accounts: Option<&RowAccounts>,
        labels: (&str, &str, &str),
    ) -> Result<(), FaucetError> {
        let missing = self.offered.saturating_sub(self.accounted());
        if let Some(a) = accounts {
            let add = |c: &AtomicU64, n: usize| {
                c.fetch_add(n as u64, Ordering::Relaxed);
            };
            add(&a.offered, self.offered);
            add(&a.accepted, self.accepted);
            add(&a.coalesced, self.coalesced);
            add(&a.skipped, self.skipped);
            add(&a.failed, self.failed);
            add(&a.quarantined, self.quarantined);
            add(&a.dropped, self.dropped);
            add(&a.unaccounted, missing);
        }
        if missing == 0 {
            return Ok(());
        }
        unaccounted_metric(labels, missing);
        tracing::error!(
            pipeline = labels.0,
            row = labels.1,
            connector = labels.2,
            offered = self.offered,
            accepted = self.accepted,
            missing,
            "sink accounted for fewer rows than it was offered; failing the page"
        );
        Err(FaucetError::RowsUnaccounted {
            offered: self.offered as u64,
            accepted: self.accepted as u64,
        })
    }
}

/// Count `n` rows a sink lost on `faucet_rows_unaccounted_total`.
pub(crate) fn unaccounted_metric(labels: (&str, &str, &str), n: usize) {
    metrics::counter!(
        "faucet_rows_unaccounted_total",
        "pipeline" => labels.0.to_string(),
        "row" => labels.1.to_string(),
        "connector" => labels.2.to_string(),
    )
    .increment(n as u64);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn report_accounts_for_coalesced_rows() {
        assert!(WriteReport::accepted(3).check(3).is_ok());
        assert!(WriteReport::accepted(4).check(3).is_ok());
        assert!(WriteReport::keyed(5, 3).check(5).is_ok());
        assert_eq!(WriteReport::keyed(5, 3).coalesced, 2);
        assert_eq!(WriteReport::keyed(2, 3).coalesced, 0);
        let err = WriteReport::accepted(2).check(3).unwrap_err();
        assert!(matches!(
            err,
            FaucetError::RowsUnaccounted {
                offered: 3,
                accepted: 2
            }
        ));
        assert_eq!(WriteReport::default().accounted(), 0);
        assert!(WriteReport::accepted(1).with_skipped(2).check(3).is_ok());
    }

    #[test]
    fn page_account_settles_into_the_run_counters() {
        let run = RowAccounts::new();
        let mut page = PageAccount::new(10);
        page.add_report(WriteReport::keyed(6, 5));
        page.failed = 1;
        page.quarantined = 2;
        page.dropped = 1;
        page.settle(Some(&run), ("p", "r", "c")).unwrap();
        let s = run.snapshot();
        assert_eq!(s.offered, 10);
        assert_eq!(s.accepted, 5);
        assert_eq!(s.coalesced, 1);
        assert!(s.balanced());
        assert_eq!(s.unaccounted, 0);

        let mut short = PageAccount::new(4);
        short.add_report(WriteReport::accepted(3));
        let err = short.settle(Some(&run), ("p", "r", "c")).unwrap_err();
        assert!(matches!(
            err,
            FaucetError::RowsUnaccounted {
                offered: 4,
                accepted: 3
            }
        ));
        let s = run.snapshot();
        assert_eq!(s.unaccounted, 1);
        assert!(!s.balanced());
        assert!(PageAccount::new(0).settle(None, ("p", "r", "c")).is_ok());
    }

    #[test]
    fn summary_line_lists_only_non_zero_buckets() {
        let run = RowAccounts::new();
        run.add_transform(12, 10);
        let mut page = PageAccount::new(10);
        page.add_report(WriteReport::accepted(9));
        page.quarantined = 1;
        page.settle(Some(&run), ("p", "r", "c")).unwrap();
        let s = run.snapshot();
        assert_eq!(s.filtered(), 2);
        assert_eq!(
            s.summary_line(),
            "rows: offered 10, accepted 9, quarantined 1, filtered by transforms 2"
        );
        let expanded = RowAccountsSnapshot {
            transform_in: 1,
            transform_out: 3,
            ..Default::default()
        };
        assert_eq!(expanded.filtered(), 0);
    }
}
