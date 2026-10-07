//! Pure change-shaping logic: operation-code normalization, the per-poll LSN
//! range plan, and CDC-envelope assembly. No I/O — every function here is
//! unit-tested without a live server, because these decisions are load-bearing
//! for correctness (a wrong op mapping or range bound silently corrupts the
//! downstream mirror).

use serde_json::{Map, Value, json};

use crate::config::StartPosition;
use crate::lsn::Lsn;

/// Alias we add to the change query so the commit LSN arrives as a hex string.
pub const LSN_ALIAS: &str = "__faucet_lsn";
/// Alias we add so the row sequence value arrives as a hex string.
pub const SEQVAL_ALIAS: &str = "__faucet_seqval";
/// The CDC operation column returned by `fn_cdc_get_all_changes`.
pub const OP_COLUMN: &str = "__$operation";

/// What to do with a CDC change row, from its `__$operation` code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpAction {
    /// Emit a change envelope with this normalized `__op` marker.
    Emit(&'static str),
    /// Skip the row (the before-image half of an update, `__$operation = 3`).
    Skip,
}

/// Normalize a SQL Server `__$operation` code into a change action.
///
/// | code | meaning              | `__op` |
/// |------|----------------------|--------|
/// | 1    | delete               | `d`    |
/// | 2    | insert               | `i`    |
/// | 3    | update (before image)| *skip* |
/// | 4    | update (after image) | `u`    |
///
/// `i`/`u` are non-delete markers so the `cdc_unwrap` stage treats them as
/// upserts; `d` matches its default delete vocabulary. The source queries in
/// `N'all'` mode (codes 1/2/4 only), but code 3 is still handled defensively so
/// an `all update old` result never panics.
pub fn op_action(operation: i64) -> Result<OpAction, faucet_core::FaucetError> {
    match operation {
        1 => Ok(OpAction::Emit("d")),
        2 => Ok(OpAction::Emit("i")),
        3 => Ok(OpAction::Skip),
        4 => Ok(OpAction::Emit("u")),
        other => Err(faucet_core::FaucetError::Source(format!(
            "mssql-cdc: unrecognized __$operation code {other} (expected 1, 2, 3, or 4)"
        ))),
    }
}

/// The plan for one capture-instance poll, derived purely from the current
/// bookmark and the instance's min/max LSN bounds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PollPlan {
    /// No range to query this poll. `set_bookmark` is `Some` only on a fresh
    /// `current` start, where we anchor the bookmark at the live max LSN so
    /// history is skipped and future polls resume from there.
    NoChanges { set_bookmark: Option<Lsn> },
    /// Query the half-open-on-the-low-end range `[from, to]` (the CDC function
    /// is inclusive of both ends; `from` is already `increment(bookmark)` so the
    /// last committed change is not re-read). `gap` is `true` when the resume
    /// point fell before the retained minimum LSN (the cleanup job purged
    /// changes) — the caller warns loudly.
    Query { from: Lsn, to: Lsn, gap: bool },
}

/// A capture instance's LSN range for one poll.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LsnBounds {
    /// `sys.fn_cdc_get_min_lsn(instance)` — oldest retained change.
    pub min: Option<Lsn>,
    /// `sys.fn_cdc_get_max_lsn()` — newest captured change.
    pub max: Option<Lsn>,
    /// `cdc.change_tables.start_lsn` — where the instance began capturing.
    pub start: Option<Lsn>,
}

/// Decide what to poll for a capture instance.
///
/// - `bookmark`: last committed LSN for this instance (`None` on a fresh run).
/// - `min_lsn` / `max_lsn`: the instance's retained range (`None` when CDC has
///   produced nothing yet, or the capture instance is not yet active).
pub fn plan_poll(bookmark: Option<Lsn>, bounds: LsnBounds, start: StartPosition) -> PollPlan {
    // A fresh `current` start anchors as soon as it can, so a change committed
    // before the next poll is never skipped: at the database's max LSN, or —
    // before the capture job has harvested anything — just before the
    // instance's own start LSN (every change of the instance is at or after it).
    if bookmark.is_none() && start == StartPosition::Current {
        let anchor = bounds
            .max
            .or_else(|| bounds.start.and_then(|s| s.decrement()));
        if anchor.is_some() {
            return PollPlan::NoChanges {
                set_bookmark: anchor,
            };
        }
    }
    // No max LSN => no change activity yet; nothing to do, keep the bookmark.
    let Some(to) = bounds.max else {
        return PollPlan::NoChanges { set_bookmark: None };
    };
    // No min LSN alongside a max is anomalous (min briefly unavailable); skip.
    let Some(min) = bounds.min else {
        return PollPlan::NoChanges { set_bookmark: None };
    };

    // Determine the candidate lower bound.
    let candidate = match bookmark {
        // Resume strictly after the last committed change.
        Some(bm) => match bm.increment() {
            Some(next) => next,
            // Unreachable saturation; nothing sensible to read.
            None => return PollPlan::NoChanges { set_bookmark: None },
        },
        None => match start {
            // Skip history: anchor at the current max, emit nothing this poll.
            StartPosition::Current => {
                return PollPlan::NoChanges {
                    set_bookmark: Some(to),
                };
            }
            // Replay whatever history is still retained.
            StartPosition::Earliest => min,
        },
    };

    // Nothing new committed since the bookmark.
    if candidate > to {
        return PollPlan::NoChanges { set_bookmark: None };
    }

    // If the resume point predates the retained minimum, the cleanup job removed
    // changes we never read — clamp forward and flag the gap.
    if candidate < min {
        return PollPlan::Query {
            from: min,
            to,
            gap: true,
        };
    }

    PollPlan::Query {
        from: candidate,
        to,
        gap: false,
    }
}

/// Apply the `on_gap` policy to a poll plan that had to jump past purged
/// changes (SQL-26). `Fail` refuses the run; `Skip` warns and goes on.
pub fn check_gap(
    on_gap: crate::config::OnGap,
    capture_instance: &str,
    resume_from: &str,
) -> Result<(), faucet_core::FaucetError> {
    match on_gap {
        crate::config::OnGap::Fail => Err(faucet_core::FaucetError::Source(format!(
            "mssql-cdc: capture instance '{capture_instance}' resumes before its retained \
             minimum LSN — the CDC cleanup job purged changes that were never read, so they \
             are lost. Re-snapshot the destination, then set `on_gap: skip` to resume from \
             the earliest retained change ({resume_from})"
        ))),
        crate::config::OnGap::Skip => {
            tracing::warn!(
                connector = "mssql-cdc",
                capture_instance,
                resume_from,
                "resume point predates the retained minimum LSN; the CDC cleanup job purged \
                 changes before they were read — resuming from the earliest retained change \
                 (on_gap: skip)"
            );
            Ok(())
        }
    }
}

/// Split a decoded change row's columns into the business columns (the table's
/// own columns) and drop every CDC metadata / helper column (`__$*` and the
/// `__faucet_lsn` alias). Pure.
pub fn business_columns(decoded: &Value) -> Map<String, Value> {
    let mut out = Map::new();
    if let Value::Object(map) = decoded {
        for (k, v) in map {
            if k == LSN_ALIAS || k == SEQVAL_ALIAS || k.starts_with("__$") {
                continue;
            }
            out.insert(k.clone(), v.clone());
        }
    }
    out
}

/// Build a CDC change-event envelope for one row.
///
/// Delete rows carry the removed values in `before` (so a downstream
/// `cdc_unwrap` delete can key off them); insert/update rows carry the current
/// image in `after`. The source queries in `N'all'` mode, so an update's
/// pre-image is not available and `before` is null for inserts and updates.
///
/// ```json
/// { "op": "i", "schema": "dbo", "table": "Orders",
///   "before": null, "after": {"id": 1, "amount": 9.99},
///   "lsn": "0000002a000000550003", "seqval": "0000002a0000005500..." }
/// ```
pub fn build_change_envelope(
    op: &str,
    schema: &str,
    table: &str,
    lsn_hex: &str,
    seqval_hex: Option<&str>,
    columns: Map<String, Value>,
) -> Value {
    let is_delete = op == "d";
    let (before, after) = if is_delete {
        (Value::Object(columns), Value::Null)
    } else {
        (Value::Null, Value::Object(columns))
    };

    let mut obj = Map::new();
    obj.insert("op".into(), json!(op));
    obj.insert("schema".into(), json!(schema));
    obj.insert("table".into(), json!(table));
    obj.insert("before".into(), before);
    obj.insert("after".into(), after);
    obj.insert("lsn".into(), json!(lsn_hex));
    if let Some(seq) = seqval_hex {
        obj.insert("seqval".into(), json!(seq));
    }
    Value::Object(obj)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lsn(hex: &str) -> Lsn {
        Lsn::from_hex(hex).unwrap()
    }

    // ── op_action: all four codes + unknown ───────────────────────────────────

    #[test]
    fn op_action_maps_all_codes() {
        assert_eq!(op_action(1).unwrap(), OpAction::Emit("d"));
        assert_eq!(op_action(2).unwrap(), OpAction::Emit("i"));
        assert_eq!(op_action(3).unwrap(), OpAction::Skip);
        assert_eq!(op_action(4).unwrap(), OpAction::Emit("u"));
    }

    #[test]
    fn op_action_rejects_unknown_code() {
        let err = op_action(7).unwrap_err();
        assert!(err.to_string().contains("__$operation code 7"), "{err}");
        assert!(op_action(0).is_err());
    }

    // ── plan_poll ─────────────────────────────────────────────────────────────

    fn plan(
        bookmark: Option<Lsn>,
        min: Option<Lsn>,
        max: Option<Lsn>,
        start: StartPosition,
    ) -> PollPlan {
        plan_poll(
            bookmark,
            LsnBounds {
                min,
                max,
                start: None,
            },
            start,
        )
    }

    #[test]
    fn a_fresh_current_start_anchors_before_the_capture_job_has_run() {
        // SQL-167: min is NULL for a while after enabling CDC; anchor anyway.
        let max = lsn("00000000000000000100");
        let only_max = LsnBounds {
            min: None,
            max: Some(max),
            start: None,
        };
        assert_eq!(
            plan_poll(None, only_max, StartPosition::Current),
            PollPlan::NoChanges {
                set_bookmark: Some(max)
            }
        );
        // Nothing harvested yet: anchor just before the instance's start LSN.
        let start = lsn("00000000000000000040");
        let unharvested = LsnBounds {
            min: None,
            max: None,
            start: Some(start),
        };
        assert_eq!(
            plan_poll(None, unharvested, StartPosition::Current),
            PollPlan::NoChanges {
                set_bookmark: start.decrement()
            }
        );
        assert_eq!(
            plan_poll(None, LsnBounds::default(), StartPosition::Current),
            PollPlan::NoChanges { set_bookmark: None }
        );
    }

    #[test]
    fn plan_no_max_lsn_is_no_changes() {
        let p = plan(
            None,
            Some(lsn("00000000000000000001")),
            None,
            StartPosition::Earliest,
        );
        assert_eq!(p, PollPlan::NoChanges { set_bookmark: None });
    }

    #[test]
    fn plan_fresh_current_anchors_at_max() {
        let max = lsn("00000000000000000100");
        let p = plan(
            None,
            Some(lsn("00000000000000000001")),
            Some(max),
            StartPosition::Current,
        );
        assert_eq!(
            p,
            PollPlan::NoChanges {
                set_bookmark: Some(max)
            }
        );
    }

    #[test]
    fn plan_fresh_earliest_queries_from_min() {
        let min = lsn("00000000000000000005");
        let max = lsn("00000000000000000100");
        let p = plan(None, Some(min), Some(max), StartPosition::Earliest);
        assert_eq!(
            p,
            PollPlan::Query {
                from: min,
                to: max,
                gap: false
            }
        );
    }

    #[test]
    fn plan_resume_queries_from_increment() {
        let bm = lsn("00000000000000000010");
        let min = lsn("00000000000000000001");
        let max = lsn("00000000000000000100");
        let p = plan(Some(bm), Some(min), Some(max), StartPosition::Current);
        assert_eq!(
            p,
            PollPlan::Query {
                from: bm.increment().unwrap(),
                to: max,
                gap: false
            }
        );
    }

    #[test]
    fn plan_resume_no_new_changes() {
        // bookmark == max: increment(bookmark) > max => nothing new.
        let bm = lsn("00000000000000000100");
        let min = lsn("00000000000000000001");
        let max = lsn("00000000000000000100");
        let p = plan(Some(bm), Some(min), Some(max), StartPosition::Current);
        assert_eq!(p, PollPlan::NoChanges { set_bookmark: None });
    }

    #[test]
    fn plan_resume_before_min_flags_gap_and_clamps() {
        // Cleanup purged past our bookmark: increment(bm) < min.
        let bm = lsn("00000000000000000001");
        let min = lsn("00000000000000000050");
        let max = lsn("00000000000000000100");
        let p = plan(Some(bm), Some(min), Some(max), StartPosition::Current);
        assert_eq!(
            p,
            PollPlan::Query {
                from: min,
                to: max,
                gap: true
            }
        );
    }

    // ── envelope shaping ──────────────────────────────────────────────────────

    #[test]
    fn a_purged_gap_fails_by_default_and_skips_only_when_asked() {
        let err = check_gap(crate::config::OnGap::default(), "dbo_orders", "0x01")
            .unwrap_err()
            .to_string();
        assert!(err.contains("dbo_orders"), "{err}");
        assert!(err.contains("on_gap: skip"), "{err}");
        assert!(check_gap(crate::config::OnGap::Skip, "dbo_orders", "0x01").is_ok());
    }

    #[test]
    fn on_gap_deserializes_and_defaults_to_fail() {
        let cfg: crate::config::MssqlCdcSourceConfig = serde_json::from_value(json!({
            "connection_url": "mssql://sa:pw@h/db",
            "capture_instances": ["dbo_t"],
        }))
        .unwrap();
        assert_eq!(cfg.on_gap, crate::config::OnGap::Fail);
        let skip: crate::config::MssqlCdcSourceConfig = serde_json::from_value(json!({
            "connection_url": "mssql://sa:pw@h/db",
            "capture_instances": ["dbo_t"],
            "on_gap": "skip",
        }))
        .unwrap();
        assert_eq!(skip.on_gap, crate::config::OnGap::Skip);
        assert!(format!("{skip:?}").contains("on_gap"));
    }

    #[test]
    fn business_columns_strips_metadata() {
        let decoded = json!({
            "__faucet_lsn": "00000000000000000001",
            "__faucet_seqval": "00000000000000000001",
            "__$start_lsn": "AAAA",
            "__$operation": 2,
            "__$seqval": "BBBB",
            "__$update_mask": "CC",
            "id": 1,
            "name": "alice"
        });
        let cols = business_columns(&decoded);
        assert_eq!(cols.len(), 2);
        assert_eq!(cols["id"], json!(1));
        assert_eq!(cols["name"], json!("alice"));
        assert!(!cols.contains_key("__$operation"));
        assert!(!cols.contains_key("__faucet_lsn"));
        assert!(!cols.contains_key("__faucet_seqval"));
    }

    #[test]
    fn insert_envelope_puts_columns_in_after() {
        let mut cols = Map::new();
        cols.insert("id".into(), json!(1));
        let env = build_change_envelope(
            "i",
            "dbo",
            "Orders",
            "00000000000000000003",
            Some("0000000000000000000300"),
            cols,
        );
        assert_eq!(env["op"], "i");
        assert_eq!(env["schema"], "dbo");
        assert_eq!(env["table"], "Orders");
        assert_eq!(env["before"], Value::Null);
        assert_eq!(env["after"]["id"], json!(1));
        assert_eq!(env["lsn"], "00000000000000000003");
        assert_eq!(env["seqval"], "0000000000000000000300");
    }

    #[test]
    fn delete_envelope_puts_columns_in_before() {
        let mut cols = Map::new();
        cols.insert("id".into(), json!(42));
        let env = build_change_envelope("d", "dbo", "Orders", "00000000000000000004", None, cols);
        assert_eq!(env["op"], "d");
        assert_eq!(env["before"]["id"], json!(42));
        assert_eq!(env["after"], Value::Null);
        // seqval omitted when not supplied.
        assert!(env.as_object().unwrap().get("seqval").is_none());
    }

    #[test]
    fn update_envelope_is_upsert_after() {
        let mut cols = Map::new();
        cols.insert("id".into(), json!(1));
        cols.insert("name".into(), json!("bob"));
        let env = build_change_envelope("u", "dbo", "Orders", "00000000000000000005", None, cols);
        assert_eq!(env["op"], "u");
        assert_eq!(env["before"], Value::Null);
        assert_eq!(env["after"]["name"], "bob");
    }
}
