//! The durable SCN bookmark.
//!
//! ```json
//! { "commit_scn": 2145738, "restart_scn": 2145697, "committed_xids": ["07000600F3010000"] }
//! ```
//!
//! - `commit_scn` — every transaction committed below it has been emitted.
//! - `committed_xids` — transactions committed *at* `commit_scn` already
//!   emitted (several can share one SCN).
//! - `restart_scn` — where mining resumes: the first SCN of the oldest
//!   transaction still open at the bookmark, so its changes are rebuilt from
//!   the start rather than lost.

use faucet_core::FaucetError;
use serde_json::{Value, json};

/// A resume position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Position {
    /// Highest emitted commit SCN.
    pub commit_scn: u64,
    /// First SCN to mine on resume.
    pub restart_scn: u64,
    /// Transactions already emitted at exactly `commit_scn`.
    pub committed_xids: Vec<String>,
}

impl Position {
    /// A position at `scn` with nothing emitted there, resuming from
    /// `restart` (which must not exceed `scn + 1`).
    pub fn at(scn: u64, restart: u64) -> Self {
        Self {
            commit_scn: scn,
            restart_scn: restart.min(scn.saturating_add(1)),
            committed_xids: Vec::new(),
        }
    }

    /// Where to anchor a fresh capture: at `current_scn`, reaching back to the
    /// oldest open transaction's start so it is captured whole.
    pub fn resume_from(current_scn: u64, oldest_open_start: Option<u64>) -> Self {
        let restart = oldest_open_start.map_or(current_scn + 1, |s| s.min(current_scn + 1));
        Self::at(current_scn, restart)
    }

    /// True when the transaction `xid` committing at `commit_scn` was already
    /// emitted before this position.
    pub fn already_emitted(&self, commit_scn: u64, xid: &str) -> bool {
        commit_scn < self.commit_scn
            || (commit_scn == self.commit_scn && self.committed_xids.iter().any(|x| x == xid))
    }

    /// Record that `xid` committed at `commit_scn` was emitted, with the
    /// oldest still-open transaction starting at `oldest_open` (if any).
    pub fn record_commit(&mut self, commit_scn: u64, xid: &str, oldest_open: Option<u64>) {
        if commit_scn > self.commit_scn {
            self.commit_scn = commit_scn;
            self.committed_xids.clear();
        }
        if commit_scn == self.commit_scn && !self.committed_xids.iter().any(|x| x == xid) {
            self.committed_xids.push(xid.to_string());
        }
        self.restart_scn = oldest_open.map_or(self.commit_scn, |s| s.min(self.commit_scn));
    }

    /// Advance past a fully-mined window ending at `end_scn` (inclusive).
    pub fn advance_to(&mut self, end_scn: u64, oldest_open: Option<u64>) {
        if end_scn > self.commit_scn {
            self.commit_scn = end_scn;
            self.committed_xids.clear();
        }
        let next = self.commit_scn.saturating_add(1);
        self.restart_scn = oldest_open.map_or(next, |s| s.min(next));
    }

    /// Serialize for the state store.
    pub fn to_value(&self) -> Value {
        json!({
            "commit_scn": self.commit_scn,
            "restart_scn": self.restart_scn,
            "committed_xids": self.committed_xids,
        })
    }

    /// Parse a stored bookmark.
    pub fn from_value(v: &Value) -> Result<Self, FaucetError> {
        let bad = |what: &str| FaucetError::State(format!("oracle-cdc bookmark: {what} (got {v})"));
        let obj = v.as_object().ok_or_else(|| bad("expected an object"))?;
        let scn = |k: &str| {
            obj.get(k)
                .and_then(Value::as_u64)
                .ok_or_else(|| bad(&format!("`{k}` must be a non-negative integer")))
        };
        let xids = match obj.get("committed_xids") {
            None | Some(Value::Null) => Vec::new(),
            Some(Value::Array(a)) => a
                .iter()
                .map(|x| {
                    x.as_str()
                        .map(str::to_string)
                        .ok_or_else(|| bad("xids must be strings"))
                })
                .collect::<Result<_, _>>()?,
            Some(_) => return Err(bad("`committed_xids` must be an array")),
        };
        Ok(Self {
            commit_scn: scn("commit_scn")?,
            restart_scn: scn("restart_scn")?,
            committed_xids: xids,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resume_reaches_back_to_open_transactions() {
        assert_eq!(Position::resume_from(100, None).restart_scn, 101);
        assert_eq!(Position::resume_from(100, Some(40)).restart_scn, 40);
        assert_eq!(Position::resume_from(100, Some(400)).restart_scn, 101);
        assert_eq!(Position::at(5, 9).restart_scn, 6);
    }

    #[test]
    fn skip_rules() {
        let mut p = Position::at(100, 90);
        assert!(p.already_emitted(99, "a"));
        assert!(!p.already_emitted(100, "a"));
        assert!(!p.already_emitted(101, "a"));
        p.record_commit(100, "a", Some(95));
        assert!(p.already_emitted(100, "a"));
        assert!(!p.already_emitted(100, "b"));
        assert_eq!(p.restart_scn, 95);
        p.record_commit(100, "b", None);
        p.record_commit(100, "b", None);
        assert_eq!(p.committed_xids, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(p.restart_scn, 100);
        p.record_commit(120, "c", Some(200));
        assert_eq!(p.commit_scn, 120);
        assert_eq!(p.committed_xids, vec!["c".to_string()]);
        assert_eq!(p.restart_scn, 120);
    }

    #[test]
    fn advancing_windows() {
        let mut p = Position::at(100, 101);
        p.record_commit(110, "x", None);
        p.advance_to(110, None);
        assert_eq!(
            p.committed_xids,
            vec!["x".to_string()],
            "same-SCN xids survive"
        );
        assert_eq!(p.restart_scn, 111);
        p.advance_to(150, Some(130));
        assert_eq!(p.commit_scn, 150);
        assert!(p.committed_xids.is_empty());
        assert_eq!(p.restart_scn, 130);
        p.advance_to(140, None);
        assert_eq!(p.commit_scn, 150, "never moves backwards");
    }

    #[test]
    fn value_round_trip_and_errors() {
        let mut p = Position::at(7, 3);
        p.committed_xids.push("X".into());
        assert_eq!(Position::from_value(&p.to_value()).unwrap(), p);
        let bare = json!({"commit_scn": 1, "restart_scn": 1});
        assert!(
            Position::from_value(&bare)
                .unwrap()
                .committed_xids
                .is_empty()
        );
        for bad in [
            json!(5),
            json!({"restart_scn": 1}),
            json!({"commit_scn": -1, "restart_scn": 1}),
            json!({"commit_scn": 1, "restart_scn": 1, "committed_xids": "x"}),
            json!({"commit_scn": 1, "restart_scn": 1, "committed_xids": [1]}),
        ] {
            assert!(Position::from_value(&bad).is_err(), "{bad}");
        }
    }
}
