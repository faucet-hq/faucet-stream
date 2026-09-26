//! SQLite run-history backend integration tests (Phase 5, #127). SQLite is
//! embedded, so these exercise the *shared* SQL machinery (`history::sql`) used
//! by the Postgres backend too — against a real database file, no server needed.
//! Requires the `serve-history-sqlite` feature.
#![cfg(feature = "serve-history-sqlite")]

use chrono::{Duration as ChronoDuration, Utc};
use faucet_cli::serve::history::InstanceHeartbeat;
use faucet_cli::serve::history::sqlite::SqliteHistory;
use faucet_cli::serve::history::{
    Claim, DeleteOutcome, ListFilter, RunHistory, RunRecord, RunStatus,
};
use std::collections::BTreeMap;
use std::time::Duration;

async fn store(dir: &tempfile::TempDir, file: &str) -> SqliteHistory {
    store_with(dir, file, Duration::from_secs(3600), "test-instance").await
}

/// Build a backend with an explicit lease TTL + instance id, for the
/// instance-fencing tests (#146 H7).
async fn store_with(
    dir: &tempfile::TempDir,
    file: &str,
    lease_ttl: Duration,
    instance: &str,
) -> SqliteHistory {
    let path = dir.path().join(file);
    SqliteHistory::connect(
        &format!("sqlite:{}", path.display()),
        Duration::from_secs(3600),
        lease_ttl,
        instance.to_string(),
    )
    .await
    .expect("connect sqlite history")
}

fn rec(id: &str, status: RunStatus, submitted: chrono::DateTime<Utc>) -> RunRecord {
    let mut r = RunRecord::queued(id.into(), None, BTreeMap::new(), None, submitted);
    r.status = status;
    if status.is_terminal() {
        r.finished_at = Some(submitted);
    }
    r
}

#[tokio::test]
async fn upsert_get_and_missing() {
    let dir = tempfile::tempdir().unwrap();
    let h = store(&dir, "a.db").await;
    let mut r = rec("run-1", RunStatus::Running, Utc::now());
    r.name = Some("nightly".into());
    r.records_written = 7;
    h.upsert(&r).await.unwrap();

    let got = h.get("run-1").await.unwrap().expect("present");
    assert_eq!(got.run_id, "run-1");
    assert_eq!(got.status, RunStatus::Running);
    assert_eq!(got.name.as_deref(), Some("nightly"));
    assert_eq!(got.records_written, 7);
    assert!(h.get("missing").await.unwrap().is_none());
}

#[tokio::test]
async fn idempotency_fresh_replay_conflict_at_sql_layer() {
    let dir = tempfile::tempdir().unwrap();
    let h = store(&dir, "idem.db").await;
    let w = Duration::from_secs(3600);
    assert_eq!(
        h.claim_idempotency("k", "fp1", "run1", w).await.unwrap(),
        Claim::Fresh
    );
    assert_eq!(
        h.claim_idempotency("k", "fp1", "run2", w).await.unwrap(),
        Claim::Replay("run1".into())
    );
    assert_eq!(
        h.claim_idempotency("k", "fp2", "run3", w).await.unwrap(),
        Claim::Conflict
    );
    // Expired prior claim (zero window) is re-claimable.
    assert_eq!(
        h.claim_idempotency("k2", "fpa", "r1", Duration::ZERO)
            .await
            .unwrap(),
        Claim::Fresh
    );
    assert_eq!(
        h.claim_idempotency("k2", "fpb", "r2", Duration::ZERO)
            .await
            .unwrap(),
        Claim::Fresh
    );
}

#[tokio::test]
async fn list_orders_desc_filters_and_paginates() {
    let dir = tempfile::tempdir().unwrap();
    let h = store(&dir, "list.db").await;
    let t0 = Utc::now();
    for (i, id) in ["a", "b", "c"].iter().enumerate() {
        h.upsert(&rec(
            id,
            RunStatus::Completed,
            t0 + ChronoDuration::seconds(i as i64),
        ))
        .await
        .unwrap();
    }
    // Newest first, page size 2 → [c, b], cursor = b.
    let page = h
        .list(&ListFilter {
            limit: 2,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(
        page.runs
            .iter()
            .map(|r| r.run_id.clone())
            .collect::<Vec<_>>(),
        vec!["c", "b"]
    );
    assert_eq!(page.next_cursor.as_deref(), Some("b"));
    // Next page from the cursor → [a].
    let page2 = h
        .list(&ListFilter {
            limit: 2,
            cursor: Some("b".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(
        page2
            .runs
            .iter()
            .map(|r| r.run_id.clone())
            .collect::<Vec<_>>(),
        vec!["a"]
    );
    assert!(page2.next_cursor.is_none());

    // Status filter.
    h.upsert(&rec(
        "x",
        RunStatus::Failed,
        t0 + ChronoDuration::seconds(10),
    ))
    .await
    .unwrap();
    let failed = h
        .list(&ListFilter {
            status: vec![RunStatus::Failed],
            limit: 50,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(failed.runs.len(), 1);
    assert_eq!(failed.runs[0].run_id, "x");

    // Multi-status membership: the comma-wrapped SQL clause must match each
    // listed status exactly (a/b/c are Completed from `rec`, x is Failed).
    let multi = h
        .list(&ListFilter {
            status: vec![RunStatus::Failed, RunStatus::Completed],
            limit: 50,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(multi.runs.len(), 4, "failed + completed");
    // A status with no matches contributes nothing (exact-token match — no
    // substring false-positives between names).
    let none = h
        .list(&ListFilter {
            status: vec![RunStatus::Sharded],
            limit: 50,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(none.runs.is_empty());
    // Empty vec = every status.
    let all = h
        .list(&ListFilter {
            status: Vec::new(),
            limit: 50,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(all.runs.len(), 4);
    // The multi-status filter composes with keyset pagination: filtered pages
    // stay dense and the cursor stays valid.
    let p1 = h
        .list(&ListFilter {
            status: vec![RunStatus::Failed, RunStatus::Completed],
            limit: 2,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(p1.runs.len(), 2);
    let p2 = h
        .list(&ListFilter {
            status: vec![RunStatus::Failed, RunStatus::Completed],
            limit: 50,
            cursor: p1.next_cursor.clone(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(p1.runs.len() + p2.runs.len(), 4, "no gaps across pages");
}

#[tokio::test]
async fn delete_respects_terminal_state() {
    let dir = tempfile::tempdir().unwrap();
    let h = store(&dir, "del.db").await;
    h.upsert(&rec("run", RunStatus::Running, Utc::now()))
        .await
        .unwrap();
    assert_eq!(h.delete("run").await.unwrap(), DeleteOutcome::StillRunning);
    assert_eq!(h.delete("nope").await.unwrap(), DeleteOutcome::NotFound);
    h.upsert(&rec("run", RunStatus::Completed, Utc::now()))
        .await
        .unwrap();
    assert_eq!(h.delete("run").await.unwrap(), DeleteOutcome::Deleted);
    assert!(h.get("run").await.unwrap().is_none());
}

#[tokio::test]
async fn recover_orphans_marks_expired_lease_non_terminal_failed() {
    let dir = tempfile::tempdir().unwrap();
    {
        // A zero TTL makes the owner's lease expire immediately, so the orphan
        // is recoverable as soon as the owning "process" goes away.
        let h = store_with(&dir, "recover.db", Duration::ZERO, "inst-a").await;
        h.upsert(&rec("orphan", RunStatus::Running, Utc::now()))
            .await
            .unwrap();
        h.upsert(&rec("done", RunStatus::Completed, Utc::now()))
            .await
            .unwrap();
    } // drop the first "process"

    // A new instance reconnects (simulating a restart) and recovers.
    let h2 = store_with(&dir, "recover.db", Duration::from_secs(30), "inst-b").await;
    let recovered = h2.recover_orphans().await.unwrap();
    assert_eq!(
        recovered, 1,
        "only the non-terminal expired-lease run is recovered"
    );
    let orphan = h2.get("orphan").await.unwrap().unwrap();
    assert_eq!(orphan.status, RunStatus::Failed);
    assert!(orphan.error.as_deref().unwrap().contains("lease expired"));
    // The already-terminal run is untouched.
    assert_eq!(
        h2.get("done").await.unwrap().unwrap().status,
        RunStatus::Completed
    );
    // Idempotent: a second pass finds nothing (the orphan is now terminal).
    assert_eq!(h2.recover_orphans().await.unwrap(), 0);
}

/// The H7 fix: a healthy peer's in-flight run carries a live (future) lease, so
/// a *different* instance's `recover_orphans` must NOT mark it failed.
#[tokio::test]
async fn recover_orphans_skips_live_lease_of_another_instance() {
    let dir = tempfile::tempdir().unwrap();
    // Instance A upserts a Running run with a long lease (it's alive).
    let a = store_with(&dir, "fence.db", Duration::from_secs(3600), "inst-a").await;
    a.upsert(&rec("a-run", RunStatus::Running, Utc::now()))
        .await
        .unwrap();

    // Instance B starts against the same DB and runs recovery. A's run has a
    // live lease, so it must be left alone.
    let b = store_with(&dir, "fence.db", Duration::from_secs(3600), "inst-b").await;
    assert_eq!(
        b.recover_orphans().await.unwrap(),
        0,
        "a live peer's run must not be recovered"
    );
    assert_eq!(
        b.get("a-run").await.unwrap().unwrap().status,
        RunStatus::Running,
        "the peer's run must still be Running"
    );
}

/// `renew_leases` is scoped to the calling instance's own non-terminal runs.
#[tokio::test]
async fn renew_leases_is_owner_and_status_scoped() {
    let dir = tempfile::tempdir().unwrap();
    let a = store_with(&dir, "renew.db", Duration::from_secs(3600), "inst-a").await;
    a.upsert(&rec("a-running", RunStatus::Running, Utc::now()))
        .await
        .unwrap();
    a.upsert(&rec("a-done", RunStatus::Completed, Utc::now()))
        .await
        .unwrap();

    // A renews only its own non-terminal run (the completed one is excluded).
    assert_eq!(a.renew_leases().await.unwrap(), 1);

    // A different instance owns nothing here, so it renews nothing.
    let b = store_with(&dir, "renew.db", Duration::from_secs(3600), "inst-b").await;
    assert_eq!(b.renew_leases().await.unwrap(), 0);
}

/// A heartbeat renews the lease, so a previously-recoverable run becomes
/// protected from a peer's recovery scan.
#[tokio::test]
async fn renew_leases_protects_a_run_from_recovery() {
    let dir = tempfile::tempdir().unwrap();
    // TTL 0 → the run's lease is born expired (recoverable).
    let a = store_with(&dir, "protect.db", Duration::ZERO, "inst-a").await;
    a.upsert(&rec("a-run", RunStatus::Running, Utc::now()))
        .await
        .unwrap();

    // A peer would recover it right now (expired lease)...
    let b = store_with(&dir, "protect.db", Duration::from_secs(3600), "inst-b").await;
    // ...but first A heartbeats with a fresh, long lease.
    let a_live = store_with(&dir, "protect.db", Duration::from_secs(3600), "inst-a").await;
    assert_eq!(a_live.renew_leases().await.unwrap(), 1);

    assert_eq!(
        b.recover_orphans().await.unwrap(),
        0,
        "the heartbeat extended the lease, so the run must no longer be an orphan"
    );
    assert_eq!(
        b.get("a-run").await.unwrap().unwrap().status,
        RunStatus::Running
    );
}

/// End-to-end: boot `faucet serve --history sqlite:…`, submit a run over HTTP,
/// and confirm it is persisted through the SQLite-backed history (GET + list).
/// Proves the `history::connect` → `FallbackHistory` → handler path is wired.
#[tokio::test(flavor = "multi_thread")]
async fn server_with_sqlite_history_persists_runs() {
    use faucet_cli::cli::ServeArgs;
    use faucet_cli::serve::ServeConfig;

    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("serve.db");
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let args = ServeArgs {
        listen: format!("127.0.0.1:{port}"),
        auth_token: None,
        auth_config: None,
        read_token: None,
        write_token: None,
        admin_token: None,
        no_auth: true,
        max_concurrent_runs: Some(2),
        max_queued_runs: Some(8),
        default_config: None,
        history: Some(format!("sqlite:{}", db.display())),
        cors_origin: vec![],
        body_limit_bytes: 1_048_576,
        shutdown_grace_secs: 5,
        retain_terminal_runs_secs: 604_800,
        idempotency_retention_secs: 86_400,
        log_retention_secs: 604_800,
        log_max_lines_per_run: 100_000,
        local_output_retention_days: 7,
        local_output_in_flight_grace_secs: 60,
        preview_local_outputs: false,
        preview_default_rows: 500,
        preview_max_rows: 5_000,
        lease_ttl_secs: 30,
        probe_timeout_secs: 5,
        env_file: None,
        no_env_file: true,
        no_ui: false,
        cluster: false,
        cluster_poll_secs: 2,
        cluster_max_attempts: 3,
        triggers: None,
        templates_sync: None,
        policy: None,
        callback_allow_host: Vec::new(),
        mcp: false,
        mcp_allow_mutations: false,
        require_approval: Vec::new(),
        approval_expiry_secs: 86_400,
    };
    let mut config = ServeConfig::from_args(args).unwrap();
    config.log_level = "warn".into();
    tokio::spawn(async move {
        let _ = faucet_cli::serve::run_server(config, Default::default()).await;
    });

    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");
    // Wait for liveness.
    let mut up = false;
    for _ in 0..200 {
        if client
            .get(format!("{base}/healthz"))
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
        {
            up = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(up, "server did not come up");
    // /readyz must be 200 — the SQLite backend connected (not degraded).
    assert_eq!(
        client
            .get(format!("{base}/readyz"))
            .send()
            .await
            .unwrap()
            .status(),
        200,
        "readyz must be 200 with a healthy sqlite backend"
    );

    // Submit a trivial run (connectors may be absent in this build → it ends
    // 'failed', but the record is still persisted by the history backend).
    let body = serde_json::json!({
        "config": "version: 1\npipeline:\n  source: { type: csv, config: { path: in.csv } }\n  sink: { type: jsonl, config: { path: out.jsonl } }\n"
    });
    let submit: serde_json::Value = client
        .post(format!("{base}/v1/runs"))
        .json(&body)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let run_id = submit["run_id"].as_str().unwrap().to_string();

    // Poll until terminal.
    let mut terminal = false;
    for _ in 0..400 {
        let rec: serde_json::Value = client
            .get(format!("{base}/v1/runs/{run_id}"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if matches!(
            rec["status"].as_str().unwrap_or(""),
            "completed" | "failed" | "cancelled"
        ) {
            terminal = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(terminal, "run never reached a terminal state");

    // The run is retrievable and appears in the list — i.e. it was persisted via
    // the SQLite backend, then read back through it.
    let listed: serde_json::Value = client
        .get(format!("{base}/v1/runs"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        listed["runs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["run_id"] == run_id),
        "submitted run must be listed from the sqlite-backed history"
    );

    // The row really is in the database file.
    let h = store(&dir, "serve.db").await;
    assert!(
        h.get(&run_id).await.unwrap().is_some(),
        "run must be physically present in the sqlite file"
    );
}

/// Column profiles (#708) append per (dataset, recorded_at), read back newest
/// first, and are pruned to `PROFILE_RETAIN`.
#[tokio::test]
async fn catalog_profiles_append_read_back_and_prune() {
    use faucet_cli::serve::history::catalog::{CatalogProfileRecord, PROFILE_RETAIN};
    let dir = tempfile::tempdir().unwrap();
    let s = store(&dir, "profiles.db").await;
    let base = Utc::now() - ChronoDuration::hours(48);
    let record = |i: i64| {
        let mut p = faucet_core::Profiler::new(faucet_core::ProfilingSpec::default());
        p.observe_page(&[serde_json::json!({"a": i, "b": "x"})]);
        CatalogProfileRecord {
            run_id: format!("run-{i}"),
            pipeline: "p".into(),
            row: "r".into(),
            recorded_at: base + ChronoDuration::seconds(i),
            profile: p.finish(),
            drift: Vec::new(),
            baseline_runs: i.max(0) as usize,
        }
    };
    assert!(
        s.catalog_profile_history("ds", 10)
            .await
            .unwrap()
            .is_empty()
    );
    for i in 0..(PROFILE_RETAIN as i64 + 5) {
        s.catalog_record_profile("ds", &record(i)).await.unwrap();
    }
    // A replay of the same (dataset, recorded_at) is a no-op.
    s.catalog_record_profile("ds", &record(PROFILE_RETAIN as i64 + 4))
        .await
        .unwrap();
    let all = s.catalog_profile_history("ds", 1000).await.unwrap();
    assert_eq!(
        all.len(),
        PROFILE_RETAIN,
        "pruned to the newest {PROFILE_RETAIN}"
    );
    assert_eq!(all[0].run_id, format!("run-{}", PROFILE_RETAIN + 4));
    assert_eq!(all.last().unwrap().run_id, "run-5");
    assert_eq!(all[0].profile.columns["a"].distinct, 1);
    let two = s.catalog_profile_history("ds", 2).await.unwrap();
    assert_eq!(two.len(), 2);
    assert!(
        s.catalog_profile_history("other", 10)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn purge_drops_expired_terminal_runs() {
    let dir = tempfile::tempdir().unwrap();
    let h = store(&dir, "purge.db").await;
    h.upsert(&rec(
        "old",
        RunStatus::Completed,
        Utc::now() - ChronoDuration::seconds(120),
    ))
    .await
    .unwrap();
    h.upsert(&rec("live", RunStatus::Running, Utc::now()))
        .await
        .unwrap();
    // retain_for = 0 → every terminal record is expired; running is kept.
    let removed = h.purge_expired(Duration::ZERO).await.unwrap();
    assert_eq!(removed, 1);
    assert!(h.get("old").await.unwrap().is_none());
    assert!(h.get("live").await.unwrap().is_some());
}

#[tokio::test]
async fn delete_also_removes_matching_idem_claim_at_sql_layer() {
    // M8 (#146): deleting a run drops its idempotency claim, so a replay of the
    // key starts a fresh run instead of 404-ing on the missing record until the
    // claim self-expires. Exercises the shared SQL `delete_idem_by_run`.
    let dir = tempfile::tempdir().unwrap();
    let h = store(&dir, "delete_idem.db").await;
    let w = Duration::from_secs(3600);
    assert_eq!(
        h.claim_idempotency("k", "fp", "r1", w).await.unwrap(),
        Claim::Fresh
    );
    let mut r = RunRecord::queued(
        "r1".into(),
        None,
        BTreeMap::new(),
        Some("k".into()),
        Utc::now(),
    );
    r.status = RunStatus::Completed;
    r.finished_at = Some(Utc::now());
    h.upsert(&r).await.unwrap();

    assert_eq!(h.delete("r1").await.unwrap(), DeleteOutcome::Deleted);
    // The claim is gone → a fresh run, not a replay of the deleted one.
    assert_eq!(
        h.claim_idempotency("k", "fp", "r2", w).await.unwrap(),
        Claim::Fresh
    );
}

/// Two instances against one DB never both claim the same pending run.
#[tokio::test]
async fn claim_pending_is_exclusive_across_instances() {
    let dir = tempfile::tempdir().unwrap();
    let a = store_with(
        &dir,
        "claim.db",
        std::time::Duration::from_secs(30),
        "inst-a",
    )
    .await;
    let b = store_with(
        &dir,
        "claim.db",
        std::time::Duration::from_secs(30),
        "inst-b",
    )
    .await;

    // One pending run. `upsert` writes status='pending' from RunStatus::Pending,
    // so claim_pending will find it.
    let mut p = rec("p1", RunStatus::Pending, Utc::now());
    p.config_body = Some("version: 1".into());
    a.upsert(&p).await.unwrap();

    // Drive both instances concurrently so the conditional-claim race is actually
    // exercised (both select the same pending candidate, then race the guarded
    // UPDATE; SQLite's single writer + the `WHERE status='pending'` guard mean only
    // one's UPDATE affects the row).
    let (ra, rb) = tokio::join!(a.claim_pending(8), b.claim_pending(8));
    let got_a = ra.unwrap();
    let got_b = rb.unwrap();
    assert_eq!(
        got_a.len() + got_b.len(),
        1,
        "exactly one instance claims it"
    );
    let stored = a.get("p1").await.unwrap().unwrap();
    assert_eq!(stored.status, RunStatus::Running);
    // The returned record carries the config body for re-execution.
    let claimed = got_a.into_iter().chain(got_b).next().unwrap();
    assert_eq!(claimed.config_body.as_deref(), Some("version: 1"));
}

/// reclaim re-queues an expired-lease run, bumping attempt; at the cap it fails.
#[tokio::test]
async fn reclaim_requeues_then_poisons_at_cap() {
    let dir = tempfile::tempdir().unwrap();
    // Zero TTL → the running run's lease is already expired.
    let h = store_with(&dir, "reclaim.db", std::time::Duration::ZERO, "inst-a").await;
    let mut r = rec("o1", RunStatus::Running, Utc::now());
    r.config_body = Some("version: 1".into());
    h.upsert(&r).await.unwrap();

    // attempt 0 → requeued (attempt becomes 1), status Pending.
    let rep = h.reclaim_orphans(2).await.unwrap();
    assert_eq!((rep.requeued, rep.failed), (1, 0));
    let after = h.get("o1").await.unwrap().unwrap();
    assert_eq!(after.status, RunStatus::Pending);
    assert_eq!(after.attempt, 1);

    // Put it back to Running (simulate a re-claim that died again) and reclaim:
    // attempt 1 → requeued (attempt 2).
    let mut again = after;
    again.status = RunStatus::Running;
    h.upsert(&again).await.unwrap();
    let rep2 = h.reclaim_orphans(2).await.unwrap();
    assert_eq!((rep2.requeued, rep2.failed), (1, 0));
    let after2 = h.get("o1").await.unwrap().unwrap();
    assert_eq!(after2.attempt, 2);

    // attempt 2 >= cap 2 → poison Failed.
    let mut again2 = after2;
    again2.status = RunStatus::Running;
    h.upsert(&again2).await.unwrap();
    let rep3 = h.reclaim_orphans(2).await.unwrap();
    assert_eq!((rep3.requeued, rep3.failed), (0, 1));
    let dead = h.get("o1").await.unwrap().unwrap();
    assert_eq!(dead.status, RunStatus::Failed);
    assert!(dead.error.unwrap().contains("reclaimed"));
}

#[tokio::test]
async fn membership_heartbeat_and_liveness() {
    let dir = tempfile::tempdir().unwrap();
    let a = store_with(
        &dir,
        "members.db",
        std::time::Duration::from_secs(30),
        "inst-a",
    )
    .await;
    let b = store_with(
        &dir,
        "members.db",
        std::time::Duration::from_secs(30),
        "inst-b",
    )
    .await;
    let beat = |n: u32| InstanceHeartbeat {
        started_at: Utc::now(),
        listen: Some("127.0.0.1:8080".into()),
        max_concurrent: 4,
        in_flight: n,
    };
    a.heartbeat_instance(&beat(1)).await.unwrap();
    b.heartbeat_instance(&beat(0)).await.unwrap();
    let live = a
        .live_instances(std::time::Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(live.len(), 2);
    // A zero-window liveness query sees nobody (all heartbeats are "old").
    let none = a.live_instances(std::time::Duration::ZERO).await.unwrap();
    assert_eq!(none.len(), 0);
}

#[tokio::test]
async fn finalize_owned_is_owner_fenced() {
    let dir = tempfile::tempdir().unwrap();
    let a = store_with(
        &dir,
        "fence.db",
        std::time::Duration::from_secs(30),
        "inst-a",
    )
    .await;
    let b = store_with(
        &dir,
        "fence.db",
        std::time::Duration::from_secs(30),
        "inst-b",
    )
    .await;
    // a owns the run (upsert stamps owner=inst-a).
    let r = rec("f1", RunStatus::Running, Utc::now());
    a.upsert(&r).await.unwrap();
    // b tries to finalize → fenced out (owner mismatch).
    let mut term = a.get("f1").await.unwrap().unwrap();
    term.status = RunStatus::Completed;
    assert!(
        !b.finalize_owned(&term).await.unwrap(),
        "non-owner is fenced"
    );
    assert_eq!(
        a.get("f1").await.unwrap().unwrap().status,
        RunStatus::Running
    );
    // a (the owner) finalizes → lands.
    assert!(a.finalize_owned(&term).await.unwrap());
    assert_eq!(
        a.get("f1").await.unwrap().unwrap().status,
        RunStatus::Completed
    );
}

#[tokio::test]
async fn cross_instance_cancel_flag_and_pickup() {
    let dir = tempfile::tempdir().unwrap();
    let a = store_with(
        &dir,
        "cancel.db",
        std::time::Duration::from_secs(30),
        "inst-a",
    )
    .await;
    let b = store_with(
        &dir,
        "cancel.db",
        std::time::Duration::from_secs(30),
        "inst-b",
    )
    .await;
    // a is running r1.
    a.upsert(&rec("r1", RunStatus::Running, Utc::now()))
        .await
        .unwrap();
    // b requests cancel; a sees it via pending_cancellations.
    b.request_cancel("r1").await.unwrap();
    assert_eq!(
        a.pending_cancellations().await.unwrap(),
        vec!["r1".to_string()]
    );
    assert!(
        b.pending_cancellations().await.unwrap().is_empty(),
        "b owns nothing"
    );

    // cancel_pending only cancels a still-pending run.
    a.upsert(&rec("p2", RunStatus::Pending, Utc::now()))
        .await
        .unwrap();
    assert!(a.cancel_pending("p2").await.unwrap());
    assert_eq!(
        a.get("p2").await.unwrap().unwrap().status,
        RunStatus::Cancelled
    );
    // A running run is not pending → false.
    assert!(!a.cancel_pending("r1").await.unwrap());
}

// ── pipeline template registry (#444) ────────────────────────────────────────
//
// The template lifecycle has two independent implementations — the in-memory
// backend and the shared SQL machinery in `history::sql` — so the memory-backed
// unit tests say nothing about the SQL launch log, its read-max-seq-then-insert
// retry, or the delete cascade. These exercise the SQL side against a real
// database file.

#[cfg(feature = "templates")]
mod templates {
    use super::*;
    use faucet_cli::serve::history::templates::{
        DeprecationRecord, TemplateDraft, TemplateId, TemplateStatus,
    };
    use faucet_cli::serve::load::ConfigFormat;

    fn draft(id: &str, description: Option<&str>) -> TemplateDraft {
        TemplateDraft {
            id: TemplateId::parse(id).unwrap(),
            name: Some(id.to_string()),
            description: description.map(str::to_string),
            body: format!("version: 1\nname: {id}\n"),
            format: ConfigFormat::Yaml,
            params: Default::default(),
            created_by: Some("tester".into()),
            kind: faucet_cli::hub::TemplateKind::Pipeline,
        }
    }

    #[tokio::test]
    async fn launch_log_drives_stable_previous_and_status() {
        let dir = tempfile::tempdir().unwrap();
        let s = store(&dir, "tpl-launch.db").await;

        for _ in 0..3 {
            s.template_register(&draft("orders", Some("about orders")))
                .await
                .unwrap();
        }
        assert_eq!(s.template_versions("orders").await.unwrap(), vec![3, 2, 1]);

        // Registering is inert: three versions exist, the template is a draft.
        let st = s.template_state("orders").await.unwrap();
        assert_eq!(st.status, TemplateStatus::Draft);
        assert_eq!((st.stable, st.previous, st.newest), (None, None, Some(3)));

        // First launch: stable moves, previous stays unset (nothing to go back
        // to). The return is the assigned launch-log sequence number.
        assert_eq!(
            s.template_launch("orders", 1, Some("ci")).await.unwrap(),
            Some(1)
        );
        let st = s.template_state("orders").await.unwrap();
        assert_eq!(st.status, TemplateStatus::Launched);
        assert_eq!((st.stable, st.previous), (Some(1), None));

        // Re-launching what is already live is a no-op (`None`) — appending would
        // make `previous` a duplicate of `stable` and destroy the rollback target.
        assert_eq!(s.template_launch("orders", 1, None).await.unwrap(), None);
        assert_eq!(s.template_launches("orders").await.unwrap().len(), 1);

        // Launching a different version advances stable; the old one becomes
        // `previous`.
        assert_eq!(
            s.template_launch("orders", 3, Some("ci")).await.unwrap(),
            Some(2)
        );
        let st = s.template_state("orders").await.unwrap();
        assert_eq!(
            (st.stable, st.previous, st.newest),
            (Some(3), Some(1), Some(3))
        );

        // The log is append-only and newest-first, with provenance.
        let log = s.template_launches("orders").await.unwrap();
        assert_eq!(
            log.iter().map(|l| (l.seq, l.version)).collect::<Vec<_>>(),
            vec![(2, 3), (1, 1)]
        );
        assert_eq!(log[0].launched_by.as_deref(), Some("ci"));

        // Rolling back is an ordinary launch, so the log keeps growing and
        // `previous` becomes the version just rolled off.
        assert_eq!(s.template_launch("orders", 1, None).await.unwrap(), Some(3));
        let st = s.template_state("orders").await.unwrap();
        assert_eq!((st.stable, st.previous), (Some(1), Some(3)));
        assert_eq!(s.template_launches("orders").await.unwrap().len(), 3);
    }

    #[tokio::test]
    async fn deprecation_marker_round_trips_and_clears() {
        let dir = tempfile::tempdir().unwrap();
        let s = store(&dir, "tpl-deprecate.db").await;
        s.template_register(&draft("legacy", None)).await.unwrap();
        s.template_launch("legacy", 1, None).await.unwrap();

        assert!(s.template_deprecation("legacy").await.unwrap().is_none());

        let marker = DeprecationRecord {
            deprecated_at: Utc::now(),
            deprecated_by: Some("admin".into()),
            reason: Some("superseded".into()),
        };
        s.template_set_deprecation("legacy", Some(&marker))
            .await
            .unwrap();
        let st = s.template_state("legacy").await.unwrap();
        assert_eq!(st.status, TemplateStatus::Deprecated);
        let stored = st.deprecation.expect("marker present in state");
        assert_eq!(stored.reason.as_deref(), Some("superseded"));
        assert_eq!(stored.deprecated_by.as_deref(), Some("admin"));
        // A deprecated template still serves `stable` — retiring must not break
        // existing callers.
        assert_eq!(st.stable, Some(1));

        s.template_set_deprecation("legacy", None).await.unwrap();
        let st = s.template_state("legacy").await.unwrap();
        assert_eq!(st.status, TemplateStatus::Launched);
        assert!(st.deprecation.is_none());
    }

    #[tokio::test]
    async fn a_version_deprecation_round_trips_skips_newest_and_cascades_on_delete() {
        let dir = tempfile::tempdir().unwrap();
        let s = store(&dir, "tpl-version-deprecate.db").await;
        for _ in 0..3 {
            s.template_register(&draft("orders", None)).await.unwrap();
        }
        s.template_launch("orders", 2, None).await.unwrap();
        let marker = DeprecationRecord {
            deprecated_at: Utc::now(),
            deprecated_by: Some("admin".into()),
            reason: Some("bad build".into()),
        };
        s.template_set_version_deprecation("orders", 3, Some(&marker))
            .await
            .unwrap();
        s.template_set_version_deprecation("orders", 2, Some(&marker))
            .await
            .unwrap();

        let st = s.template_state("orders").await.unwrap();
        assert_eq!(
            st.deprecated_versions
                .iter()
                .map(|d| d.version)
                .collect::<Vec<_>>(),
            vec![3, 2],
            "newest first"
        );
        assert_eq!(
            st.deprecated_versions[0].record.reason.as_deref(),
            Some("bad build")
        );
        assert_eq!(st.newest, Some(1), "`newest` skips retired versions");
        assert_eq!(st.stable, Some(2), "a retired live version keeps serving");
        assert_eq!(
            st.status,
            TemplateStatus::Launched,
            "the template itself is not retired"
        );

        s.template_set_version_deprecation("orders", 2, None)
            .await
            .unwrap();
        assert_eq!(
            s.template_version_deprecations("orders")
                .await
                .unwrap()
                .len(),
            1
        );

        s.template_delete("orders", Some(3)).await.unwrap();
        assert!(
            s.template_version_deprecations("orders")
                .await
                .unwrap()
                .is_empty()
        );
        s.template_set_version_deprecation("orders", 1, Some(&marker))
            .await
            .unwrap();
        s.template_delete("orders", None).await.unwrap();
        assert!(
            s.template_version_deprecations("orders")
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn deleting_a_version_cascades_to_channels_and_launch_entries() {
        let dir = tempfile::tempdir().unwrap();
        let s = store(&dir, "tpl-cascade.db").await;
        for _ in 0..2 {
            s.template_register(&draft("orders", None)).await.unwrap();
        }
        s.template_set_tag("orders", "prod", 1).await.unwrap();
        s.template_set_tag("orders", "dev", 2).await.unwrap();
        s.template_launch("orders", 1, None).await.unwrap();
        s.template_launch("orders", 2, None).await.unwrap();

        // Dropping v1 must take its channel *and* its launch-log entries with it,
        // so no pointer outlives its target.
        assert_eq!(s.template_delete("orders", Some(1)).await.unwrap(), 1);
        assert_eq!(
            s.template_tags("orders")
                .await
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>(),
            vec![("dev".to_string(), 2)]
        );
        let log = s.template_launches("orders").await.unwrap();
        assert_eq!(log.iter().map(|l| l.version).collect::<Vec<_>>(), vec![2]);
        let st = s.template_state("orders").await.unwrap();
        assert_eq!((st.stable, st.previous), (Some(2), None));

        // Deleting the whole template clears everything.
        assert_eq!(s.template_delete("orders", None).await.unwrap(), 1);
        assert!(s.template_versions("orders").await.unwrap().is_empty());
        assert!(s.template_tags("orders").await.unwrap().is_empty());
        assert!(s.template_launches("orders").await.unwrap().is_empty());
    }

    /// #666 — heavy contention on the register path.
    ///
    /// The six-way test below was flaky rather than broken: every step of the
    /// version-assignment retry loop was retried *except* `tx.commit()`, and in
    /// WAL mode a transaction that read and then wrote can be refused at COMMIT
    /// with `database is locked` when another writer committed in between —
    /// SQLite answers that immediately rather than honouring `busy_timeout`,
    /// because waiting could deadlock. Whether a given run tripped it was luck.
    ///
    /// Sixteen writers make the overlap near-certain, so this fails reliably
    /// without the fix and passes with it — and it asserts the *guarantee*
    /// (sixteen distinct versions, none lost) rather than merely "no error".
    #[tokio::test]
    async fn heavy_concurrent_registers_assign_distinct_versions() {
        let dir = tempfile::tempdir().unwrap();
        let s = std::sync::Arc::new(store(&dir, "tpl-contention.db").await);

        const WRITERS: u32 = 64;
        let mut set = tokio::task::JoinSet::new();
        for _ in 0..WRITERS {
            let s = s.clone();
            set.spawn(async move { s.template_register(&draft("orders", None)).await });
        }

        let mut versions: Vec<u32> = Vec::new();
        let mut errors: Vec<String> = Vec::new();
        while let Some(joined) = set.join_next().await {
            match joined.expect("task panicked") {
                Ok(rec) => versions.push(rec.version),
                Err(e) => errors.push(e.to_string()),
            }
        }
        assert!(
            errors.is_empty(),
            "every register must survive contention — a transient lock is the \
             backend's problem, not the caller's: {errors:?}"
        );

        versions.sort_unstable();
        assert_eq!(
            versions,
            (1..=WRITERS).collect::<Vec<u32>>(),
            "each writer must get its own version, with none lost or reused"
        );
    }

    #[tokio::test]
    async fn concurrent_registers_and_launches_never_lose_a_write() {
        let dir = tempfile::tempdir().unwrap();
        let s = std::sync::Arc::new(store(&dir, "tpl-concurrent.db").await);

        // Version assignment and launch-seq assignment both read-max-then-insert,
        // so they need the PK-conflict retry to be correct under contention.
        let mut set = tokio::task::JoinSet::new();
        for _ in 0..6 {
            let s = s.clone();
            set.spawn(async move { s.template_register(&draft("orders", None)).await.unwrap() });
        }
        let mut versions: Vec<u32> = Vec::new();
        while let Some(r) = set.join_next().await {
            versions.push(r.unwrap().version);
        }
        versions.sort_unstable();
        assert_eq!(versions, vec![1, 2, 3, 4, 5, 6], "no lost registers");

        let mut set = tokio::task::JoinSet::new();
        for v in 1..=6u32 {
            let s = s.clone();
            set.spawn(async move { s.template_launch("orders", v, None).await.unwrap() });
        }
        while let Some(r) = set.join_next().await {
            r.unwrap();
        }
        let log = s.template_launches("orders").await.unwrap();
        let mut seqs: Vec<u32> = log.iter().map(|l| l.seq).collect();
        seqs.sort_unstable();
        assert_eq!(
            seqs,
            vec![1, 2, 3, 4, 5, 6],
            "no lost launches, no duplicate seq"
        );
    }

    #[tokio::test]
    async fn list_folds_to_the_newest_version_per_id() {
        let dir = tempfile::tempdir().unwrap();
        let s = store(&dir, "tpl-list.db").await;
        s.template_register(&draft("a", Some("about a")))
            .await
            .unwrap();
        s.template_register(&draft("a", Some("about a")))
            .await
            .unwrap();
        s.template_register(&draft("b", None)).await.unwrap();

        let listed = s.template_list().await.unwrap();
        let mut rows: Vec<(String, u32)> = listed.into_iter().map(|t| (t.id, t.version)).collect();
        rows.sort();
        assert_eq!(
            rows,
            vec![("a".to_string(), 2), ("b".to_string(), 1)],
            "one row per id, at its newest version"
        );
    }
}

/// The local-output ledger's two backends must answer a given filter
/// identically — same rows, same order, same truncation (#587).
///
/// This is the test `Stmts::local_output_query` points at. The SQL backend pushes
/// the filter and `LIMIT` down (so a never-purged table cannot become an
/// unbounded scan) while the memory backend applies the shared pure predicate in
/// Rust; that divergence in *mechanism* is exactly what needs pinning. It also
/// covers the tie-break: several rows sharing a `last_written_at` used to order
/// arbitrarily under SQL, so a limited page could differ between backends.
#[tokio::test]
async fn local_output_backends_agree() {
    use faucet_cli::local_outputs::{LocalOutputFilter, LocalOutputObservation};
    use faucet_cli::serve::history::memory::MemoryHistory;
    use std::path::PathBuf;

    let dir = tempfile::tempdir().unwrap();
    let sql = store(&dir, "ledger.db").await;
    let mem = MemoryHistory::new(Duration::from_secs(3600));

    let at = |s: &str| {
        chrono::DateTime::parse_from_rfc3339(s)
            .unwrap()
            .with_timezone(&Utc)
    };
    let obs = |path: &str, dataset: &str, pipeline: &str, when: &str| LocalOutputObservation {
        path: PathBuf::from(path),
        dataset_uri: format!("file://{path}"),
        dataset_id: dataset.to_string(),
        kind: "jsonl".into(),
        pipeline: pipeline.to_string(),
        row: "default".into(),
        run_id: "run-1".into(),
        pre_existing: false,
        replaced: false,
        retention_days: None,
        observed_at: at(when),
    };

    // Deliberately includes three rows sharing one `last_written_at` — the tie
    // case — plus two pipelines, two datasets, and one collected row.
    let rows = [
        obs("/tmp/c.jsonl", "ds1", "alpha", "2026-08-03T00:00:00Z"),
        obs("/tmp/a.jsonl", "ds1", "alpha", "2026-08-02T00:00:00Z"),
        obs("/tmp/b.jsonl", "ds2", "beta", "2026-08-02T00:00:00Z"),
        obs("/tmp/d.jsonl", "ds2", "beta", "2026-08-02T00:00:00Z"),
        obs("/tmp/e.jsonl", "ds1", "beta", "2026-08-01T00:00:00Z"),
    ];
    for o in &rows {
        sql.local_output_record(o).await.unwrap();
        mem.local_output_record(o).await.unwrap();
    }
    // Collect one so `include_deleted` has something to hide/show.
    let gone = faucet_cli::local_outputs::ledger::output_id(&PathBuf::from("/tmp/e.jsonl"));
    for store in [&sql as &dyn RunHistory, &mem as &dyn RunHistory] {
        assert!(
            store
                .local_output_mark_deleted(&gone, at("2026-08-09T00:00:00Z"), 11)
                .await
                .unwrap()
        );
    }

    let filters = [
        LocalOutputFilter::default(),
        LocalOutputFilter {
            include_deleted: true,
            ..Default::default()
        },
        LocalOutputFilter {
            dataset_id: Some("ds1".into()),
            ..Default::default()
        },
        LocalOutputFilter {
            pipeline: Some("beta".into()),
            ..Default::default()
        },
        LocalOutputFilter {
            dataset_id: Some("ds2".into()),
            pipeline: Some("beta".into()),
            ..Default::default()
        },
        // Limits that cut *through* the tie group, where an unstable order shows.
        LocalOutputFilter {
            limit: 1,
            ..Default::default()
        },
        LocalOutputFilter {
            limit: 2,
            ..Default::default()
        },
        LocalOutputFilter {
            limit: 3,
            ..Default::default()
        },
        LocalOutputFilter {
            include_deleted: true,
            limit: 4,
            ..Default::default()
        },
        LocalOutputFilter {
            dataset_id: Some("nope".into()),
            ..Default::default()
        },
    ];

    for (i, filter) in filters.iter().enumerate() {
        let from_sql: Vec<String> = sql
            .local_output_list(filter)
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.path)
            .collect();
        let from_mem: Vec<String> = mem
            .local_output_list(filter)
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.path)
            .collect();
        assert_eq!(
            from_sql, from_mem,
            "backends disagree on filter #{i}: {filter:?}"
        );
        if filter.limit > 0 {
            assert!(
                from_sql.len() <= filter.limit,
                "filter #{i} returned more than its limit"
            );
        }
    }

    // A limit must not under-fill: pushing LIMIT into SQL is only safe because
    // the WHERE mirrors the pure predicate exactly.
    let page = sql
        .local_output_list(&LocalOutputFilter {
            limit: 3,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(page.len(), 3, "a full page must come back full");

    // And `get` agrees with `list` about a collected row.
    let expired = sql.local_output_get(&gone).await.unwrap().unwrap();
    assert!(expired.deleted_at.is_some());
    assert_eq!(expired.deleted_bytes, Some(11));
}

// ── Cost & usage accounting (#704) ──────────────────────────────────────────

#[tokio::test]
async fn usage_records_round_trip_filter_and_dedupe() {
    use faucet_cli::usage::{PricingSpec, RecordIdentity, UsageFilter, build_record};
    use faucet_core::usage::UsageSnapshot;
    let dir = tempfile::tempdir().unwrap();
    let store = store(&dir, "usage.db").await;
    let now = Utc::now();
    let mk = |run: &str, pipeline: &str, at: chrono::DateTime<Utc>| {
        build_record(
            RecordIdentity {
                run_id: run,
                pipeline,
                row: "default",
                source_kind: "csv",
                sink_kind: "jsonl",
                dataset_id: Some("ds".into()),
                dataset_uri: Some("file:///out.jsonl".into()),
            },
            UsageSnapshot {
                records_read: 10,
                records_written: 10,
                bytes_read: 100,
                bytes_written: 100,
                ..Default::default()
            },
            250,
            false,
            &PricingSpec::default(),
            at,
        )
    };
    let a = mk("run-a", "p", now - ChronoDuration::hours(2));
    let b = mk("run-b", "p", now - ChronoDuration::hours(1));
    let c = mk("run-c", "other", now);
    for r in [&a, &b, &c] {
        store.usage_record(r).await.unwrap();
    }
    // A retried write of the same (run, row) is a no-op, never a double count.
    store.usage_record(&a).await.unwrap();

    let all = store.usage_list(&UsageFilter::default()).await.unwrap();
    assert_eq!(
        all.iter().map(|r| r.run_id.as_str()).collect::<Vec<_>>(),
        vec!["run-c", "run-b", "run-a"],
        "newest first"
    );
    let p_only = store
        .usage_list(&UsageFilter {
            pipeline: Some("p".into()),
            since: Some(now - ChronoDuration::minutes(90)),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(p_only.len(), 1);
    assert_eq!(p_only[0].run_id, "run-b");
    assert_eq!(p_only[0].usage.records_written, 10);
    assert_eq!(p_only[0].dataset_id.as_deref(), Some("ds"));
    let capped = store
        .usage_list(&UsageFilter {
            limit: 2,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(capped.len(), 2);
    let until = store
        .usage_list(&UsageFilter {
            until: Some(now - ChronoDuration::minutes(90)),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(until.len(), 1);
    assert_eq!(until[0].run_id, "run-a");
}
