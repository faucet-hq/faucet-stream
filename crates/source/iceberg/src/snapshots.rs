//! Pure snapshot-lineage logic: time-travel resolution, incremental planning,
//! and the bookmark shape.

use faucet_core::FaucetError;
use serde_json::{Value, json};

use crate::config::{OnExpired, OnRewrite, ReadMode};

/// The operation recorded in a snapshot summary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotOp {
    /// Only data files were added.
    Append,
    /// Files were rewritten without changing table data (compaction).
    Replace,
    /// Data was logically overwritten.
    Overwrite,
    /// Data was deleted (removed files or delete files).
    Delete,
}

impl SnapshotOp {
    /// Lower-case name as written in the snapshot summary.
    pub fn as_str(self) -> &'static str {
        match self {
            SnapshotOp::Append => "append",
            SnapshotOp::Replace => "replace",
            SnapshotOp::Overwrite => "overwrite",
            SnapshotOp::Delete => "delete",
        }
    }
}

impl From<&iceberg::spec::Operation> for SnapshotOp {
    fn from(op: &iceberg::spec::Operation) -> Self {
        match op {
            iceberg::spec::Operation::Append => SnapshotOp::Append,
            iceberg::spec::Operation::Replace => SnapshotOp::Replace,
            iceberg::spec::Operation::Overwrite => SnapshotOp::Overwrite,
            iceberg::spec::Operation::Delete => SnapshotOp::Delete,
        }
    }
}

/// The lineage facts the planner needs about one snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotInfo {
    /// Snapshot id.
    pub id: i64,
    /// Parent snapshot id.
    pub parent: Option<i64>,
    /// Commit time (epoch ms).
    pub timestamp_ms: i64,
    /// Summary operation.
    pub operation: SnapshotOp,
}

impl From<&iceberg::spec::Snapshot> for SnapshotInfo {
    fn from(s: &iceberg::spec::Snapshot) -> Self {
        Self {
            id: s.snapshot_id(),
            parent: s.parent_snapshot_id(),
            timestamp_ms: s.timestamp_ms(),
            operation: SnapshotOp::from(&s.summary().operation),
        }
    }
}

/// How the snapshots between a bookmark and the current snapshot relate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Lineage {
    /// Only appends (and data-preserving replaces): read these append
    /// snapshots, oldest first.
    Appends(Vec<i64>),
    /// A snapshot rewrote existing data.
    Rewrite {
        /// The first rewriting snapshot.
        snapshot_id: i64,
        /// Its operation.
        operation: SnapshotOp,
    },
    /// The bookmark is in the metadata but not an ancestor of the current
    /// snapshot (the table was rolled back or its branch replaced).
    NotAncestor,
    /// The bookmark, or a snapshot between it and the current one, is gone.
    Expired,
}

fn find(snapshots: &[SnapshotInfo], id: i64) -> Option<&SnapshotInfo> {
    snapshots.iter().find(|s| s.id == id)
}

/// Walk from `to` back through parents to `from`.
pub fn walk(snapshots: &[SnapshotInfo], from: i64, to: i64) -> Lineage {
    if find(snapshots, from).is_none() {
        return Lineage::Expired;
    }
    let mut chain = Vec::new();
    let mut cursor = to;
    while cursor != from {
        let Some(snap) = find(snapshots, cursor) else {
            return Lineage::Expired;
        };
        chain.push(snap);
        match snap.parent {
            Some(p) => cursor = p,
            None => return Lineage::NotAncestor,
        }
    }
    chain.reverse();
    let mut appends = Vec::new();
    for snap in chain {
        match snap.operation {
            SnapshotOp::Append => appends.push(snap.id),
            SnapshotOp::Replace => {}
            op => {
                return Lineage::Rewrite {
                    snapshot_id: snap.id,
                    operation: op,
                };
            }
        }
    }
    Lineage::Appends(appends)
}

/// The snapshot that was current at `target_ms`: the newest ancestor of
/// `current` committed at or before it.
pub fn resolve_as_of(
    snapshots: &[SnapshotInfo],
    current: Option<i64>,
    target_ms: i64,
) -> Option<i64> {
    let mut cursor = current;
    while let Some(id) = cursor {
        let snap = find(snapshots, id)?;
        if snap.timestamp_ms <= target_ms {
            return Some(id);
        }
        cursor = snap.parent;
    }
    None
}

/// The bookmark persisted after reading `snapshot_id`.
pub fn bookmark_value(snapshot_id: i64) -> Value {
    json!({ "snapshot_id": snapshot_id })
}

/// Parse a persisted bookmark (`{"snapshot_id": N}`, or a bare number).
pub fn parse_bookmark(v: &Value) -> Result<i64, FaucetError> {
    let id = match v {
        Value::Object(m) => m.get("snapshot_id"),
        other => Some(other),
    };
    id.and_then(|v| {
        v.as_i64()
            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
    })
    .ok_or_else(|| {
        FaucetError::State(format!(
            "iceberg: unrecognised bookmark {v}; expected {{\"snapshot_id\": <id>}}"
        ))
    })
}

/// One unit of read work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    /// Read every live data file of a snapshot; `checkpoint` persists the
    /// snapshot id as the bookmark afterwards.
    Full {
        /// Snapshot to read.
        snapshot_id: i64,
        /// Emit a bookmark once the snapshot is read.
        checkpoint: bool,
    },
    /// Read only the data files an append snapshot added, then bookmark it.
    Append(i64),
    /// Nothing to read; move the bookmark to this snapshot.
    Checkpoint(i64),
}

/// Config inputs the planner needs.
#[derive(Debug, Clone, Copy)]
pub struct PlanOptions {
    /// Read mode.
    pub mode: ReadMode,
    /// Rewrite policy (incremental).
    pub on_rewrite: OnRewrite,
    /// Expiry policy (incremental).
    pub on_expired: OnExpired,
    /// Pinned snapshot (full).
    pub snapshot_id: Option<i64>,
    /// Pinned instant, epoch ms (full).
    pub as_of_ms: Option<i64>,
}

/// Decide what a run reads.
pub fn plan(
    opts: &PlanOptions,
    snapshots: &[SnapshotInfo],
    current: Option<i64>,
    bookmark: Option<i64>,
) -> Result<Vec<Step>, FaucetError> {
    if opts.mode == ReadMode::Full {
        let target = match (opts.snapshot_id, opts.as_of_ms) {
            (Some(id), _) => {
                if find(snapshots, id).is_none() {
                    return Err(FaucetError::Source(format!(
                        "iceberg: snapshot {id} is not in the table metadata (it may have been \
                         expired); list live snapshots or drop `snapshot_id`"
                    )));
                }
                Some(id)
            }
            (None, Some(ms)) => Some(resolve_as_of(snapshots, current, ms).ok_or_else(|| {
                FaucetError::Source(format!(
                    "iceberg: no snapshot was current at or before {ms} ms since the epoch \
                     (older snapshots may have been expired)"
                ))
            })?),
            (None, None) => current,
        };
        return Ok(target
            .map(|snapshot_id| Step::Full {
                snapshot_id,
                checkpoint: false,
            })
            .into_iter()
            .collect());
    }

    let Some(current) = current else {
        return Ok(Vec::new());
    };
    let refresh = vec![Step::Full {
        snapshot_id: current,
        checkpoint: true,
    }];
    let Some(from) = bookmark else {
        return Ok(refresh);
    };
    if from == current {
        return Ok(vec![Step::Checkpoint(current)]);
    }
    match walk(snapshots, from, current) {
        Lineage::Appends(ids) => {
            let mut steps: Vec<Step> = ids.iter().copied().map(Step::Append).collect();
            if ids.last() != Some(&current) {
                steps.push(Step::Checkpoint(current));
            }
            Ok(steps)
        }
        Lineage::Rewrite {
            snapshot_id,
            operation,
        } => match opts.on_rewrite {
            OnRewrite::FullRefresh => {
                tracing::warn!(
                    snapshot_id,
                    operation = operation.as_str(),
                    "iceberg source: data rewritten since the bookmark; re-reading the table in full"
                );
                Ok(refresh)
            }
            OnRewrite::Fail => Err(FaucetError::Source(format!(
                "iceberg: snapshot {snapshot_id} since bookmark {from} is a `{}` snapshot, so an \
                 incremental read would miss changed or deleted rows; set `on_rewrite: \
                 full_refresh` to re-read the table",
                operation.as_str()
            ))),
        },
        Lineage::NotAncestor => match opts.on_rewrite {
            OnRewrite::FullRefresh => {
                tracing::warn!(
                    bookmark = from,
                    "iceberg source: bookmark is not an ancestor of the current snapshot; re-reading in full"
                );
                Ok(refresh)
            }
            OnRewrite::Fail => Err(FaucetError::Source(format!(
                "iceberg: bookmarked snapshot {from} is not an ancestor of current snapshot \
                 {current} (the table was rolled back or its history replaced); set \
                 `on_rewrite: full_refresh` to re-read the table"
            ))),
        },
        Lineage::Expired => match opts.on_expired {
            OnExpired::FullRefresh => {
                tracing::warn!(
                    bookmark = from,
                    "iceberg source: bookmarked snapshot history expired; re-reading in full"
                );
                Ok(refresh)
            }
            OnExpired::Fail => Err(FaucetError::Source(format!(
                "iceberg: bookmarked snapshot {from} (or a snapshot after it) has been expired \
                 from the table metadata, so the appended files cannot be identified; set \
                 `on_expired: full_refresh` to re-read the table, or run more often than the \
                 snapshot retention window"
            ))),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(id: i64, parent: Option<i64>, ts: i64, op: SnapshotOp) -> SnapshotInfo {
        SnapshotInfo {
            id,
            parent,
            timestamp_ms: ts,
            operation: op,
        }
    }

    fn lineage() -> Vec<SnapshotInfo> {
        use SnapshotOp::*;
        vec![
            s(1, None, 100, Append),
            s(2, Some(1), 200, Append),
            s(3, Some(2), 300, Replace),
            s(4, Some(3), 400, Append),
            s(5, Some(4), 500, Delete),
            s(6, Some(5), 600, Append),
            s(9, None, 900, Append),
        ]
    }

    fn opts(mode: ReadMode) -> PlanOptions {
        PlanOptions {
            mode,
            on_rewrite: OnRewrite::Fail,
            on_expired: OnExpired::Fail,
            snapshot_id: None,
            as_of_ms: None,
        }
    }

    #[test]
    fn walk_classifies_lineage() {
        let l = lineage();
        assert_eq!(walk(&l, 1, 4), Lineage::Appends(vec![2, 4]));
        assert_eq!(walk(&l, 2, 3), Lineage::Appends(vec![]));
        assert_eq!(
            walk(&l, 4, 6),
            Lineage::Rewrite {
                snapshot_id: 5,
                operation: SnapshotOp::Delete
            }
        );
        assert_eq!(walk(&l, 9, 6), Lineage::NotAncestor);
        assert_eq!(walk(&l, 42, 6), Lineage::Expired);
        let gap: Vec<_> = l.iter().filter(|x| x.id != 3).cloned().collect();
        assert_eq!(walk(&gap, 1, 4), Lineage::Expired);
    }

    #[test]
    fn as_of_follows_the_current_lineage() {
        let l = lineage();
        assert_eq!(resolve_as_of(&l, Some(6), 450), Some(4));
        assert_eq!(resolve_as_of(&l, Some(6), 600), Some(6));
        assert_eq!(resolve_as_of(&l, Some(6), 99), None);
        assert_eq!(resolve_as_of(&l, None, 1000), None);
        assert_eq!(resolve_as_of(&l, Some(77), 1000), None);
    }

    #[test]
    fn bookmark_round_trip() {
        assert_eq!(parse_bookmark(&bookmark_value(7)).unwrap(), 7);
        assert_eq!(parse_bookmark(&json!(8)).unwrap(), 8);
        assert_eq!(parse_bookmark(&json!({"snapshot_id": "9"})).unwrap(), 9);
        for bad in [
            json!({}),
            json!("x"),
            json!(null),
            json!({"snapshot_id": 1.5}),
        ] {
            assert!(matches!(parse_bookmark(&bad), Err(FaucetError::State(_))));
        }
    }

    #[test]
    fn op_names() {
        use iceberg::spec::Operation;
        for (op, name) in [
            (Operation::Append, "append"),
            (Operation::Replace, "replace"),
            (Operation::Overwrite, "overwrite"),
            (Operation::Delete, "delete"),
        ] {
            assert_eq!(SnapshotOp::from(&op).as_str(), name);
        }
    }

    #[test]
    fn full_mode_plans() {
        let l = lineage();
        let full = |id| Step::Full {
            snapshot_id: id,
            checkpoint: false,
        };
        assert_eq!(
            plan(&opts(ReadMode::Full), &l, Some(6), Some(1)).unwrap(),
            vec![full(6)]
        );
        assert!(
            plan(&opts(ReadMode::Full), &l, None, None)
                .unwrap()
                .is_empty()
        );

        let mut o = opts(ReadMode::Full);
        o.snapshot_id = Some(2);
        assert_eq!(plan(&o, &l, Some(6), None).unwrap(), vec![full(2)]);
        o.snapshot_id = Some(99);
        assert!(
            plan(&o, &l, Some(6), None)
                .unwrap_err()
                .to_string()
                .contains("expired")
        );

        let mut o = opts(ReadMode::Full);
        o.as_of_ms = Some(250);
        assert_eq!(plan(&o, &l, Some(6), None).unwrap(), vec![full(2)]);
        o.as_of_ms = Some(1);
        assert!(
            plan(&o, &l, Some(6), None)
                .unwrap_err()
                .to_string()
                .contains("at or before")
        );
    }

    #[test]
    fn incremental_plans() {
        let l = lineage();
        let o = opts(ReadMode::Incremental);
        let refresh = |id| {
            vec![Step::Full {
                snapshot_id: id,
                checkpoint: true,
            }]
        };
        assert!(plan(&o, &l, None, Some(1)).unwrap().is_empty());
        assert_eq!(plan(&o, &l, Some(4), None).unwrap(), refresh(4));
        assert_eq!(
            plan(&o, &l, Some(4), Some(4)).unwrap(),
            vec![Step::Checkpoint(4)]
        );
        assert_eq!(
            plan(&o, &l, Some(4), Some(1)).unwrap(),
            vec![Step::Append(2), Step::Append(4)]
        );
        assert_eq!(
            plan(&o, &l, Some(3), Some(1)).unwrap(),
            vec![Step::Append(2), Step::Checkpoint(3)]
        );
        assert_eq!(
            plan(&o, &l, Some(3), Some(2)).unwrap(),
            vec![Step::Checkpoint(3)]
        );
    }

    #[test]
    fn incremental_policies() {
        let l = lineage();
        let fail = opts(ReadMode::Incremental);
        let mut lenient = fail;
        lenient.on_rewrite = OnRewrite::FullRefresh;
        lenient.on_expired = OnExpired::FullRefresh;
        let refresh = vec![Step::Full {
            snapshot_id: 6,
            checkpoint: true,
        }];

        let e = plan(&fail, &l, Some(6), Some(4)).unwrap_err().to_string();
        assert!(
            e.contains("`delete` snapshot") && e.contains("on_rewrite"),
            "{e}"
        );
        assert_eq!(plan(&lenient, &l, Some(6), Some(4)).unwrap(), refresh);

        let e = plan(&fail, &l, Some(6), Some(9)).unwrap_err().to_string();
        assert!(e.contains("not an ancestor"), "{e}");
        assert_eq!(plan(&lenient, &l, Some(6), Some(9)).unwrap(), refresh);

        let e = plan(&fail, &l, Some(6), Some(42)).unwrap_err().to_string();
        assert!(e.contains("expired") && e.contains("on_expired"), "{e}");
        assert_eq!(plan(&lenient, &l, Some(6), Some(42)).unwrap(), refresh);
    }
}
