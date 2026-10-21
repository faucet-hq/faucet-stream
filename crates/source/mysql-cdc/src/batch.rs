//! Grouping committed transactions into pages (pure).
//!
//! Every page costs the pipeline a sink flush and a state write, so committed
//! transactions are coalesced into one page until it holds `batch_size`
//! records (a transaction is never split) or the oldest one has waited
//! `max_age`. A transaction with no captured rows only moves the pending
//! bookmark; a page holding only such progress is emitted after
//! [`PROGRESS_MAX_AGE`], so the persisted position keeps up with a busy
//! server whose changes are all filtered out. While an XA transaction is prepared but not yet decided, later
//! transactions are held back in commit order, so no bookmark is ever emitted
//! past rows whose outcome is unknown.

use crate::query::XaId;
use crate::state::Bookmark;
use serde_json::Value;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

/// How long bookmark-only progress (transactions with no captured rows) may
/// wait before it is emitted on its own page.
pub(crate) const PROGRESS_MAX_AGE: Duration = Duration::from_secs(30);

/// A page ready to be yielded.
#[derive(Debug, PartialEq)]
pub(crate) struct ReadyPage {
    pub records: Vec<Value>,
    pub bookmark: Bookmark,
}

/// Committed transactions waiting to become pages.
#[derive(Debug)]
pub(crate) struct PageBuilder {
    batch_size: usize,
    max_age: Duration,
    records: Vec<Value>,
    bookmark: Option<Bookmark>,
    oldest: Option<Instant>,
    /// When the pending bookmark first moved (with or without rows).
    progress_since: Option<Instant>,
    /// Prepared XA transactions: their rows and the txid they were given.
    prepared: BTreeMap<XaId, Vec<Value>>,
    /// When each prepared XA transaction was seen.
    prepared_at: BTreeMap<XaId, Instant>,
    /// Transactions committed while an XA transaction was undecided.
    held: Vec<(Vec<Value>, Bookmark)>,
    max_records: Option<usize>,
    /// Set when holding a transaction would exceed `max_records`.
    error: Option<String>,
}

impl PageBuilder {
    /// `batch_size == 0` accumulates everything into one page at the end.
    pub(crate) fn new(batch_size: usize, max_age: Duration, max_records: Option<usize>) -> Self {
        Self {
            batch_size,
            max_age,
            records: Vec::new(),
            bookmark: None,
            oldest: None,
            progress_since: None,
            prepared: BTreeMap::new(),
            prepared_at: BTreeMap::new(),
            held: Vec::new(),
            max_records,
            error: None,
        }
    }

    /// Rows held behind the oldest undecided XA transaction: transactions
    /// committed after it and other prepared ones. Its own rows are not held
    /// behind anything (the in-progress buffer cap bounds them).
    fn held_len(&self) -> usize {
        let blocker = self.oldest_undecided();
        self.held.iter().map(|(r, _)| r.len()).sum::<usize>()
            + self
                .prepared
                .iter()
                .filter(|(x, _)| Some(*x) != blocker)
                .map(|(_, r)| r.len())
                .sum::<usize>()
    }

    fn check_capacity(&self, extra: usize) -> Result<(), String> {
        match self.max_records {
            Some(max) if self.held_len() + extra > max => Err(format!(
                "mysql-cdc: rows of transactions held behind the undecided XA transaction ({}) \
                 exceed {max} (max_staged_records, 1,000,000 when unset); its own rows do not \
                 count. Find it with XA RECOVER and decide it (XA COMMIT / XA ROLLBACK), or \
                 raise max_staged_records",
                self.oldest_undecided()
                    .map_or_else(|| "?".to_string(), |x| x.to_string())
            )),
            _ => Ok(()),
        }
    }

    /// A transaction committed at `bookmark`. Returns the pages it completes.
    /// Holding it behind an undecided XA transaction past `max_records`
    /// records an error for [`Self::take_error`] instead.
    pub(crate) fn commit(
        &mut self,
        rows: Vec<Value>,
        bookmark: Bookmark,
        now: Instant,
    ) -> Vec<ReadyPage> {
        if !self.prepared.is_empty() {
            if let Err(e) = self.check_capacity(rows.len()) {
                self.error.get_or_insert(e);
            }
            self.held.push((rows, bookmark));
            return Vec::new();
        }
        self.add(rows, bookmark, now)
    }

    /// The capacity error a [`Self::commit`] recorded, if any.
    pub(crate) fn take_error(&mut self) -> Option<String> {
        self.error.take()
    }

    fn add(&mut self, rows: Vec<Value>, bookmark: Bookmark, now: Instant) -> Vec<ReadyPage> {
        if !rows.is_empty() && self.oldest.is_none() {
            self.oldest = Some(now);
        }
        self.progress_since.get_or_insert(now);
        self.records.extend(rows);
        self.bookmark = Some(bookmark);
        if self.batch_size != 0 && self.records.len() >= self.batch_size {
            self.take().into_iter().collect()
        } else {
            Vec::new()
        }
    }

    /// An XA transaction was prepared at `now`: its rows wait for the outcome.
    pub(crate) fn prepare(
        &mut self,
        xid: XaId,
        rows: Vec<Value>,
        now: Instant,
    ) -> Result<(), String> {
        let behind = self.oldest_undecided().is_some_and(|b| *b != xid);
        self.check_capacity(if behind { rows.len() } else { 0 })?;
        self.prepared_at.entry(xid.clone()).or_insert(now);
        self.prepared.entry(xid).or_default().extend(rows);
        Ok(())
    }

    fn oldest_undecided(&self) -> Option<&XaId> {
        self.prepared_at
            .iter()
            .min_by_key(|(_, at)| **at)
            .map(|(x, _)| x)
    }

    /// The longest-undecided prepared XA transaction and how long it has
    /// held every later transaction back.
    pub(crate) fn undecided(&self, now: Instant) -> Option<(&XaId, Duration)> {
        self.prepared_at
            .iter()
            .min_by_key(|(_, at)| **at)
            .map(|(x, at)| (x, now.saturating_duration_since(*at)))
    }

    /// The outcome of a prepared XA transaction, decided at `bookmark`.
    /// `false` when the transaction was not seen being prepared (it was
    /// prepared before this stream started).
    pub(crate) fn decide(
        &mut self,
        xid: &XaId,
        commit: bool,
        bookmark: Bookmark,
        now: Instant,
    ) -> (bool, Vec<ReadyPage>) {
        let rows = self.prepared.remove(xid);
        self.prepared_at.remove(xid);
        let known = rows.is_some();
        let rows = if commit {
            rows.unwrap_or_default()
        } else {
            Vec::new()
        };
        if !self.prepared.is_empty() {
            self.held.push((rows, bookmark));
            return (known, Vec::new());
        }
        let mut pages = Vec::new();
        for (held_rows, held_bm) in std::mem::take(&mut self.held) {
            pages.extend(self.add(held_rows, held_bm, now));
        }
        pages.extend(self.add(rows, bookmark, now));
        (known, pages)
    }

    /// When the pending page falls due: `max_age` after its oldest records,
    /// or [`PROGRESS_MAX_AGE`] after bookmark-only progress began. Never in
    /// accumulate-all mode.
    fn due_at(&self) -> Option<Instant> {
        if self.batch_size == 0 {
            return None;
        }
        match self.oldest {
            Some(t) => Some(t + self.max_age),
            None => self
                .progress_since
                .filter(|_| self.bookmark.is_some())
                .map(|t| t + PROGRESS_MAX_AGE),
        }
    }

    /// Whether the pending page is due.
    pub(crate) fn due(&self, now: Instant) -> bool {
        self.due_at().is_some_and(|t| now >= t)
    }

    /// How long until the pending page is due, if one is pending.
    pub(crate) fn time_left(&self, now: Instant) -> Option<Duration> {
        self.due_at().map(|t| t.saturating_duration_since(now))
    }

    /// Take the pending page: records plus the newest committed bookmark
    /// (a bookmark-only page when only empty transactions committed).
    pub(crate) fn take(&mut self) -> Option<ReadyPage> {
        self.oldest = None;
        self.progress_since = None;
        let bookmark = self.bookmark.take()?;
        Some(ReadyPage {
            records: std::mem::take(&mut self.records),
            bookmark,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Bookmark-only progress is emitted on its own page after
    /// `PROGRESS_MAX_AGE`, so filtered-out traffic still moves the persisted
    /// position; rows keep the short `max_age`.
    #[test]
    fn bookmark_only_progress_falls_due_after_the_progress_age() {
        let t0 = Instant::now();
        let mut b = PageBuilder::new(100, Duration::from_secs(1), None);
        assert!(b.commit(Vec::new(), bm(10), t0).is_empty());
        assert!(!b.due(t0 + Duration::from_secs(5)));
        assert_eq!(b.time_left(t0), Some(PROGRESS_MAX_AGE));
        assert!(
            b.commit(Vec::new(), bm(20), t0 + Duration::from_secs(10))
                .is_empty()
        );
        assert!(b.due(t0 + PROGRESS_MAX_AGE));
        let page = b.take().unwrap();
        assert!(page.records.is_empty());
        assert_eq!(page.bookmark, bm(20));
        assert_eq!(b.time_left(t0), None);
        // A row-bearing commit after progress uses `max_age`.
        b.commit(Vec::new(), bm(30), t0);
        b.commit(vec![json!({"a": 1})], bm(40), t0 + Duration::from_secs(2));
        assert!(b.due(t0 + Duration::from_secs(3)));
    }

    fn bm(pos: u64) -> Bookmark {
        Bookmark::FilePos {
            file: "b.000001".into(),
            pos,
        }
    }

    fn xid(g: &str) -> XaId {
        XaId {
            format_id: 1,
            gtrid: g.as_bytes().to_vec(),
            bqual: Vec::new(),
        }
    }

    #[test]
    fn coalesces_transactions_without_splitting_them() {
        let now = Instant::now();
        let mut b = PageBuilder::new(3, Duration::from_secs(1), None);
        assert!(b.commit(vec![json!(1), json!(2)], bm(10), now).is_empty());
        assert!(b.commit(Vec::new(), bm(15), now).is_empty());
        let pages = b.commit(vec![json!(3), json!(4)], bm(20), now);
        assert_eq!(
            pages,
            vec![ReadyPage {
                records: vec![json!(1), json!(2), json!(3), json!(4)],
                bookmark: bm(20)
            }]
        );
        assert!(b.take().is_none());
    }

    #[test]
    fn empty_commits_only_move_the_bookmark_and_age_flushes() {
        let now = Instant::now();
        let mut b = PageBuilder::new(100, Duration::from_secs(1), None);
        b.commit(Vec::new(), bm(5), now);
        assert!(!b.due(now), "bookmark-only progress waits PROGRESS_MAX_AGE");
        assert_eq!(b.time_left(now), Some(PROGRESS_MAX_AGE));
        b.commit(vec![json!(1)], bm(6), now);
        assert_eq!(b.time_left(now), Some(Duration::from_secs(1)));
        assert!(b.due(now + Duration::from_secs(2)));
        assert_eq!(
            b.take(),
            Some(ReadyPage {
                records: vec![json!(1)],
                bookmark: bm(6)
            })
        );
        b.commit(Vec::new(), bm(7), now);
        assert_eq!(
            b.take(),
            Some(ReadyPage {
                records: Vec::new(),
                bookmark: bm(7)
            })
        );
    }

    #[test]
    fn accumulate_all_mode_never_cuts() {
        let now = Instant::now();
        let mut b = PageBuilder::new(0, Duration::from_secs(1), None);
        for i in 0..5 {
            assert!(b.commit(vec![json!(i)], bm(i), now).is_empty());
        }
        assert!(!b.due(now + Duration::from_secs(10)));
        assert_eq!(b.time_left(now), None);
        assert_eq!(b.take().unwrap().records.len(), 5);
    }

    #[test]
    fn undecided_xa_holds_later_commits_in_order() {
        let now = Instant::now();
        let mut b = PageBuilder::new(1, Duration::from_secs(1), None);
        b.prepare(xid("a"), vec![json!("xa")], now).unwrap();
        assert!(b.commit(vec![json!("t1")], bm(30), now).is_empty());
        let (known, pages) = b.decide(&xid("a"), true, bm(40), now);
        assert!(known);
        let recs: Vec<_> = pages.iter().map(|p| p.records.clone()).collect();
        assert_eq!(recs, vec![vec![json!("t1")], vec![json!("xa")]]);
        assert_eq!(pages[1].bookmark, bm(40));
    }

    #[test]
    fn rollback_drops_rows_and_nested_xa_waits_for_the_last() {
        let now = Instant::now();
        let mut b = PageBuilder::new(10, Duration::from_secs(1), None);
        b.prepare(xid("a"), vec![json!("a")], now).unwrap();
        b.prepare(xid("b"), vec![json!("b")], now).unwrap();
        let (known, pages) = b.decide(&xid("a"), false, bm(50), now);
        assert!(known && pages.is_empty());
        let (_, pages) = b.decide(&xid("b"), true, bm(60), now);
        assert!(pages.is_empty(), "below batch_size");
        let page = b.take().unwrap();
        assert_eq!(page.records, vec![json!("b")]);
        assert_eq!(page.bookmark, bm(60));
        let (known, _) = b.decide(&xid("zz"), true, bm(70), now);
        assert!(!known);
    }

    #[test]
    fn the_oldest_undecided_xa_is_reported_with_its_age() {
        let t0 = Instant::now();
        let mut b = PageBuilder::new(10, Duration::from_secs(1), None);
        assert!(b.undecided(t0).is_none());
        b.prepare(xid("a"), vec![], t0).unwrap();
        b.prepare(xid("b"), vec![], t0 + Duration::from_secs(5))
            .unwrap();
        let (x, age) = b.undecided(t0 + Duration::from_secs(10)).unwrap();
        assert_eq!(x, &xid("a"));
        assert_eq!(age, Duration::from_secs(10));
        let _ = b.decide(&xid("a"), true, bm(1), t0);
        assert_eq!(b.undecided(t0).unwrap().0, &xid("b"));
        let _ = b.decide(&xid("b"), false, bm(2), t0);
        assert!(b.undecided(t0).is_none());
    }

    #[test]
    fn held_rows_are_bounded() {
        let now = Instant::now();
        let mut b = PageBuilder::new(10, Duration::from_secs(1), Some(2));
        b.prepare(xid("a"), vec![json!(1), json!(2), json!(3)], now)
            .expect("the blocking XA transaction's own rows are not held behind it");
        assert!(b.commit(vec![json!(4), json!(5)], bm(1), now).is_empty());
        assert!(b.take_error().is_none());
        assert!(b.commit(vec![json!(6)], bm(2), now).is_empty());
        let err = b.take_error().expect("capacity error");
        assert!(err.contains("max_staged_records"), "{err}");
        assert!(err.contains("X'61',X'',1"), "{err}");
        assert!(err.contains("XA RECOVER"), "{err}");
        assert!(b.prepare(xid("b"), vec![json!(4)], now).is_err());
    }
}
