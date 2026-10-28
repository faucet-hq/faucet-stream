//! Execution pools (#957): each replica serves a set of pools, each run
//! carries one, and a clustered replica claims only the runs in its pools.

use crate::serve::error::ServeError;
use crate::serve::history::InstanceRecord;

/// The pool of a run that names none, and of a replica started without `--pool`.
pub const DEFAULT_POOL: &str = "default";

/// The longest pool name accepted.
pub const MAX_POOL_LEN: usize = 63;

/// The `default` pool, owned (serde defaults).
pub fn default_pool() -> String {
    DEFAULT_POOL.to_string()
}

/// The pool set of a replica started without `--pool`.
pub fn default_only() -> Vec<String> {
    vec![default_pool()]
}

/// Whether `pool` is the default pool (serde `skip_serializing_if`).
pub fn is_default_pool(pool: &str) -> bool {
    pool == DEFAULT_POOL
}

/// Check a pool name: lowercase letters, digits, `-` and `_`, starting with a
/// letter or digit, at most [`MAX_POOL_LEN`] characters.
pub fn validate_name(name: &str) -> Result<(), String> {
    let ok = !name.is_empty()
        && name.len() <= MAX_POOL_LEN
        && name
            .bytes()
            .next()
            .is_some_and(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_');
    if ok {
        Ok(())
    } else {
        Err(format!(
            "invalid pool name `{name}`: use 1-{MAX_POOL_LEN} lowercase letters, digits, `-` or `_`, \
             starting with a letter or digit"
        ))
    }
}

/// The pools a replica serves: the validated, de-duplicated `--pool` values,
/// or `[default]` when none were given.
pub fn replica_pools(flags: &[String]) -> Result<Vec<String>, String> {
    let mut out: Vec<String> = Vec::new();
    for raw in flags {
        let name = raw.trim();
        if name.is_empty() {
            continue;
        }
        validate_name(name)?;
        if !out.iter().any(|p| p == name) {
            out.push(name.to_string());
        }
    }
    if out.is_empty() {
        out.push(default_pool());
    }
    out.sort();
    Ok(out)
}

/// The pool a run executes in: the request's, else the config's, else `default`.
pub fn resolve(request: Option<&str>, config: Option<&str>) -> Result<String, ServeError> {
    let (pool, origin) = match (request, config) {
        (Some(p), _) => (p, "request `pool`"),
        (None, Some(p)) => (p, "config `pool:`"),
        (None, None) => return Ok(default_pool()),
    };
    validate_name(pool).map_err(|m| ServeError::Unprocessable {
        message: format!("{origin}: {m}"),
        details: None,
    })?;
    Ok(pool.to_string())
}

/// An idempotency fingerprint that also names a non-default pool, so a key
/// replayed into another pool is a conflict; a `default` run keeps the
/// fingerprint it had before pools.
pub fn fold_fingerprint(fp: String, pool: Option<&str>) -> String {
    match pool {
        Some(p) if p != DEFAULT_POOL => format!("{fp}|pool={p}"),
        _ => fp,
    }
}

/// The pools list as one bound parameter for the claim queries: `,a,b,`.
pub fn bind_list(pools: &[String]) -> String {
    format!(",{},", pools.join(","))
}

/// Encode a member's pools for its capability row.
pub fn encode(pools: &[String]) -> String {
    pools.join(",")
}

/// Decode a capability row's pools; `None` (no row, or a member older than
/// pools) stays `None`.
pub fn decode(raw: Option<&str>) -> Option<Vec<String>> {
    raw.map(|s| {
        s.split(',')
            .map(str::trim)
            .filter(|p| !p.is_empty())
            .map(str::to_string)
            .collect()
    })
}

/// Whether `member` would claim a run in `pool`. A member that predates pools
/// claims every run, which is how it serves `default`.
pub fn member_serves(member: &InstanceRecord, pool: &str) -> bool {
    match &member.pools {
        Some(p) => p.iter().any(|x| x == pool),
        None => pool == DEFAULT_POOL,
    }
}

/// Every pool some live member serves, sorted and de-duplicated.
pub fn served_by(members: &[InstanceRecord]) -> Vec<String> {
    let mut out: Vec<String> = members
        .iter()
        .flat_map(|m| {
            m.pools
                .clone()
                .unwrap_or_else(|| vec![default_pool()])
                .into_iter()
        })
        .collect();
    out.sort();
    out.dedup();
    out
}

/// Members that predate pools: they claim any pending run, whatever its pool.
pub fn legacy_members(members: &[InstanceRecord]) -> Vec<&InstanceRecord> {
    members.iter().filter(|m| m.pools.is_none()).collect()
}

/// Why a clustered submission to `pool` is refused, or `None` when a live
/// member (or this replica, serving `own`) serves it and no member that
/// predates pools could steal it.
pub fn cluster_refusal(members: &[InstanceRecord], own: &[String], pool: &str) -> Option<String> {
    if pool != DEFAULT_POOL {
        let legacy = legacy_members(members);
        if let Some(m) = legacy.first() {
            return Some(format!(
                "pool `{pool}` cannot be used yet: cluster member {} ({}) runs a faucet release \
                 without execution pools and would claim the run whatever its pool; finish \
                 upgrading every replica first",
                m.instance_id,
                m.listen.as_deref().unwrap_or("unknown address")
            ));
        }
    }
    if own.iter().any(|p| p == pool) || members.iter().any(|m| member_serves(m, pool)) {
        return None;
    }
    let mut served = served_by(members);
    served.extend(own.iter().cloned());
    served.sort();
    served.dedup();
    Some(format!(
        "no live cluster member serves pool `{pool}` (live members serve: {}); start a replica \
         with `--pool {pool}`",
        if served.is_empty() {
            "none".to_string()
        } else {
            served.join(", ")
        }
    ))
}

/// Why a non-clustered server refuses `pool`, or `None` when it serves it.
pub fn local_refusal(served: &[String], pool: &str) -> Option<String> {
    if served.iter().any(|p| p == pool) {
        return None;
    }
    Some(format!(
        "this server does not serve pool `{pool}` (it serves: {}); start it with `--pool {pool}`",
        served.join(", ")
    ))
}

/// The terminal error of a pending run whose pool no live member served for
/// `waited_secs` (`--pool-wait-timeout-secs`).
pub fn wait_timeout_error(pool: &str, waited_secs: u64) -> String {
    format!(
        "pool_unavailable: no live cluster member served pool `{pool}` for {waited_secs}s \
         (--pool-wait-timeout-secs); the run was never started"
    )
}

/// One lease-loop step for pools (#957): report pending runs per pool and,
/// with `--pool-wait-timeout-secs`, fail the runs of a pool no live member
/// has served for that long.
pub async fn upkeep(state: &crate::serve::state::ServerState) {
    let cluster = state.cluster();
    let counts = match state.history().pending_by_pool().await {
        Ok(counts) => counts,
        Err(e) => {
            tracing::warn!(error = %e, "cluster: pending_by_pool failed");
            return;
        }
    };
    cluster.report_pending(&counts);
    let Some(wait) = cluster.pool_wait_timeout() else {
        return;
    };
    let members = cluster.live_members();
    if members.is_empty() {
        return;
    }
    let served = served_by(&members);
    let now = chrono::Utc::now();
    let unserved: Vec<String> = counts
        .into_iter()
        .map(|(p, _)| p)
        .filter(|p| !served.contains(p))
        .collect();
    let since = cluster.note_unserved(&unserved, now);
    let wait_c = chrono::Duration::from_std(wait).unwrap_or(chrono::Duration::MAX);
    let Some(before) = now.checked_sub_signed(wait_c) else {
        return;
    };
    if !since.values().any(|t| *t <= before) {
        return;
    }
    let stranded = match state.history().stranded_pending(&served, before).await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "cluster: stranded_pending failed");
            return;
        }
    };
    for mut rec in stranded {
        if since.get(&rec.pool).is_none_or(|t| *t > before) {
            continue;
        }
        rec.status = crate::serve::history::RunStatus::Failed;
        rec.finished_at = Some(now);
        rec.error = Some(wait_timeout_error(&rec.pool, wait.as_secs()));
        match state.history().fail_pending(&rec).await {
            Ok(true) => {
                tracing::error!(
                    run_id = %rec.run_id,
                    pool = %rec.pool,
                    "cluster: no live member served the run's pool within \
                     --pool-wait-timeout-secs; the run failed"
                );
                crate::serve::metrics::record_run_finished(
                    crate::serve::history::RunStatus::Failed,
                    POOL_UNAVAILABLE,
                );
                crate::serve::runner::finalized_elsewhere(&rec).await;
            }
            Ok(false) => {}
            Err(e) => {
                tracing::warn!(run_id = %rec.run_id, error = %e, "cluster: fail_pending failed")
            }
        }
    }
}

/// Refuse a submission to a pool no replica would run (#957). Clustered: a
/// live member must serve it (re-read from the history store when the lease
/// loop's view does not show one) and no member older than pools may be live
/// while it is not `default`. Single server: it must be one of its own.
pub async fn admit(state: &crate::serve::state::ServerState, pool: &str) -> Result<(), ServeError> {
    let cluster = state.cluster();
    if !cluster.enabled() {
        return match local_refusal(cluster.pools(), pool) {
            None => Ok(()),
            Some(message) => Err(ServeError::Unprocessable {
                message,
                details: None,
            }),
        };
    }
    if cluster_refusal(&cluster.live_members(), cluster.pools(), pool).is_none() {
        return Ok(());
    }
    let members = state
        .history()
        .live_instances(cluster.member_ttl())
        .await
        .map_err(|e| ServeError::from_history("reading cluster members", e))?;
    match cluster_refusal(&members, cluster.pools(), pool) {
        None => {
            cluster.set_live_members(members);
            Ok(())
        }
        Some(message) => Err(ServeError::Unprocessable {
            message,
            details: None,
        }),
    }
}

/// The `reason` of `faucet_serve_runs_total` for a run failed by
/// `--pool-wait-timeout-secs`.
pub const POOL_UNAVAILABLE: &str = "pool_unavailable";

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn member(id: &str, pools: Option<&[&str]>) -> InstanceRecord {
        InstanceRecord {
            instance_id: id.into(),
            started_at: Utc::now(),
            last_heartbeat: Utc::now(),
            listen: Some(format!("{id}:8080")),
            max_concurrent: 4,
            in_flight: 0,
            state_format: faucet_core::state_version::STATE_FORMAT,
            pools: pools.map(|p| p.iter().map(|s| s.to_string()).collect()),
        }
    }

    #[test]
    fn names_are_validated() {
        for ok in ["default", "a", "private-subnet", "gpu_1", "0x"] {
            assert!(validate_name(ok).is_ok(), "{ok}");
        }
        for bad in ["", "A", "-a", "_a", "a,b", "a b", "a%", &"x".repeat(64)] {
            let e = validate_name(bad).unwrap_err();
            assert!(e.contains("invalid pool name"), "{bad}: {e}");
        }
    }

    #[test]
    fn replica_pools_default_dedupe_and_sort() {
        assert_eq!(replica_pools(&[]).unwrap(), vec!["default"]);
        assert_eq!(
            replica_pools(&["b".into(), " a ".into(), "b".into(), "".into()]).unwrap(),
            vec!["a", "b"]
        );
        assert!(replica_pools(&["Bad".into()]).is_err());
    }

    #[test]
    fn resolve_precedence_and_validation() {
        assert_eq!(resolve(None, None).unwrap(), "default");
        assert_eq!(resolve(None, Some("cfg")).unwrap(), "cfg");
        assert_eq!(resolve(Some("req"), Some("cfg")).unwrap(), "req");
        match resolve(Some("Bad"), None).unwrap_err() {
            ServeError::Unprocessable { message, .. } => {
                assert!(message.starts_with("request `pool`: invalid pool name `Bad`"))
            }
            other => panic!("{other:?}"),
        }
        match resolve(None, Some("x y")).unwrap_err() {
            ServeError::Unprocessable { message, .. } => {
                assert!(message.starts_with("config `pool:`"))
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn fingerprint_folds_only_a_non_default_pool() {
        assert_eq!(fold_fingerprint("f".into(), None), "f");
        assert_eq!(fold_fingerprint("f".into(), Some("default")), "f");
        assert_eq!(fold_fingerprint("f".into(), Some("a")), "f|pool=a");
    }

    #[test]
    fn encodings_round_trip() {
        let p = vec!["a".to_string(), "b".to_string()];
        assert_eq!(bind_list(&p), ",a,b,");
        assert_eq!(decode(Some(&encode(&p))), Some(p));
        assert_eq!(decode(None), None);
        assert_eq!(decode(Some("")), Some(Vec::new()));
        assert!(is_default_pool("default") && !is_default_pool("a"));
    }

    #[test]
    fn membership_rules() {
        let a = member("a", Some(&["a"]));
        let legacy = member("old", None);
        assert!(member_serves(&a, "a") && !member_serves(&a, "default"));
        assert!(member_serves(&legacy, "default") && !member_serves(&legacy, "a"));
        assert_eq!(
            served_by(&[a.clone(), legacy.clone()]),
            vec!["a", "default"]
        );
        assert!(cluster_refusal(std::slice::from_ref(&a), &[], "a").is_none());
        let e = cluster_refusal(std::slice::from_ref(&a), &[], "b").unwrap();
        assert!(
            e.contains("pool `b`") && e.contains("live members serve: a"),
            "{e}"
        );
        assert!(
            cluster_refusal(&[], &[], "b")
                .unwrap()
                .contains("serve: none")
        );
        assert!(
            cluster_refusal(&[], &["b".into()], "b").is_none(),
            "this replica counts"
        );
        let e = cluster_refusal(&[], &["c".into()], "b").unwrap();
        assert!(e.contains("serve: c"), "{e}");
        let e = cluster_refusal(&[a.clone(), legacy.clone()], &["a".into()], "a").unwrap();
        assert!(
            e.contains("without execution pools") && e.contains("old:8080"),
            "{e}"
        );
        assert!(cluster_refusal(&[a, legacy], &[], "default").is_none());
    }

    #[test]
    fn local_rules_and_timeout_text() {
        let served = vec!["default".to_string()];
        assert!(local_refusal(&served, "default").is_none());
        let e = local_refusal(&served, "gpu").unwrap();
        assert!(
            e.contains("pool `gpu`") && e.contains("serves: default"),
            "{e}"
        );
        assert!(wait_timeout_error("a", 30).starts_with("pool_unavailable: "));
    }
}
