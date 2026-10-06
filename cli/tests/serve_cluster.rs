//! Clustered-serve integration tests (#197). The in-process tests share one
//! SQLite file between two backends (two "instances"); the two-process test
//! spawns real `faucet serve --cluster` children.

#![cfg(all(feature = "serve", feature = "serve-history-sqlite"))]

use faucet_cli::serve::history::sqlite::SqliteHistory;
use faucet_cli::serve::history::{RunHistory, RunRecord, RunStatus};
use std::time::Duration;

async fn backend(
    dir: &tempfile::TempDir,
    file: &str,
    lease: Duration,
    inst: &str,
) -> SqliteHistory {
    let url = format!("sqlite:{}", dir.path().join(file).display());
    SqliteHistory::connect(&url, Duration::from_secs(3600), lease, inst.to_string())
        .await
        .unwrap()
}

fn pending(id: &str) -> RunRecord {
    let mut r = RunRecord::queued(
        id.into(),
        None,
        Default::default(),
        None,
        chrono::Utc::now(),
    );
    r.status = RunStatus::Pending;
    r.config_body = Some("version: 1".into());
    r
}

/// The acceptance core: one instance claims a run, "crashes" (lease expires), a
/// survivor reclaims it back to Pending and then claims it — and at no point do
/// both instances hold it simultaneously.
#[tokio::test]
async fn failover_reassigns_a_dead_instances_run_without_double_claim() {
    let dir = tempfile::tempdir().unwrap();
    // inst-a has a zero lease → anything it claims is immediately reclaimable
    // (simulating a crash right after claiming).
    let a = backend(&dir, "fo.db", Duration::ZERO, "inst-a").await;
    let b = backend(&dir, "fo.db", Duration::from_secs(3600), "inst-b").await;

    a.upsert(&pending("r1")).await.unwrap();

    // inst-a claims it.
    let claimed_a = a.claim_pending(4).await.unwrap();
    assert_eq!(claimed_a.len(), 1);
    // inst-b cannot also claim it (it is now Running).
    assert!(
        b.claim_pending(4).await.unwrap().is_empty(),
        "no double-claim"
    );

    // inst-a "crashes"; inst-b reclaims (a's lease is already expired since a's
    // claim used a zero TTL).
    let report = b.reclaim_orphans(3).await.unwrap();
    assert_eq!((report.requeued, report.failed), (1, 0));
    assert_eq!(
        a.get("r1").await.unwrap().unwrap().status,
        RunStatus::Pending
    );

    // inst-b now claims the re-queued run and finalizes it (owner-fenced).
    let claimed_b = b.claim_pending(4).await.unwrap();
    assert_eq!(claimed_b.len(), 1);
    let mut term = b.get("r1").await.unwrap().unwrap();
    term.status = RunStatus::Completed;
    assert!(b.finalize_owned(&term).await.unwrap(), "owner b finalizes");
    assert_eq!(
        a.get("r1").await.unwrap().unwrap().status,
        RunStatus::Completed
    );
}

/// Concurrent claim from two instances over one file never double-claims a batch.
#[tokio::test]
async fn concurrent_claims_partition_the_pending_set() {
    let dir = tempfile::tempdir().unwrap();
    let a = backend(&dir, "part.db", Duration::from_secs(3600), "inst-a").await;
    let b = backend(&dir, "part.db", Duration::from_secs(3600), "inst-b").await;
    for i in 0..10 {
        a.upsert(&pending(&format!("r{i}"))).await.unwrap();
    }
    let (ra, rb) = tokio::join!(a.claim_pending(10), b.claim_pending(10));
    let ca = ra.unwrap();
    let cb = rb.unwrap();
    assert_eq!(ca.len() + cb.len(), 10, "every run claimed exactly once");
    let mut ids: Vec<String> = ca.iter().chain(&cb).map(|r| r.run_id.clone()).collect();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 10, "no run claimed by both instances");
}

// --- two-process acceptance test ---------------------------------------------
// Spawns two real `faucet serve --cluster` processes against one SQLite history
// DB. Submits N runs to instance A, then SIGKILLs A; instance B must reclaim and
// finish every run. The assertion confirms all runs reach a terminal state on
// the survivor (proving cross-instance reassignment).
//
// This test caught bug #228: the first cluster run an instance executed
// underflowed the `queued` backpressure counter in serve/registry.rs to
// usize::MAX (execute_run called mark_running()'s queued-=1, but a cluster run
// never reserved a local queue slot), which panicked the worker threads in
// debug and wedged backpressure (permanent 429) in release. Fixed by
// `mark_running_unqueued` on the claim path + saturating counter decrements.
//
// To run explicitly:
//   cargo test -p faucet-cli \
//     --features serve,serve-history-sqlite,source-csv,sink-jsonl \
//     --test serve_cluster two_process_cluster -- --nocapture
#[cfg(unix)]
#[tokio::test]
async fn two_process_cluster_reassigns_on_kill() {
    // cargo-llvm-cov instruments the spawned `faucet serve` binaries, which slows
    // them past this test's lease/timing windows on CI runners (it passes
    // uninstrumented in the `Test` job, and the in-process failover tests above
    // cover the claim/reclaim/fence logic under coverage). Skip only under
    // llvm-cov, which sets CARGO_LLVM_COV in the test environment.
    if std::env::var_os("CARGO_LLVM_COV").is_some() {
        eprintln!(
            "skipping two_process_cluster_reassigns_on_kill under cargo-llvm-cov \
             (instrumented spawned binaries break the lease timing; the Test job runs it)"
        );
        return;
    }

    use std::process::{Child, Command};
    use tokio::time::sleep;

    // RAII guard: SIGKILL the spawned child on drop so a panic anywhere in the
    // test never orphans a `faucet serve` process. (An orphan keeps the test
    // harness's stdout pipe open, which hangs `cargo test` reporting.)
    struct Killer(Child);
    impl Drop for Killer {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    let bin = env!("CARGO_BIN_EXE_faucet");
    let dir = tempfile::tempdir().unwrap();
    let db = format!("sqlite:{}", dir.path().join("cluster.db").display());
    let out = dir.path().join("out.jsonl");
    let input = dir.path().join("in.csv");
    std::fs::write(&input, "id\n1\n").unwrap();
    let config = format!(
        "version: 1\npipeline:\n  source: {{ type: csv, config: {{ path: \"{}\" }} }}\n  sink: {{ type: jsonl, config: {{ path: \"{}\", append: true }} }}\n",
        input.display(),
        out.display(),
    );

    fn free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    let port_a = free_port();
    let port_b = free_port();

    let spawn = |port: u16| {
        Killer(
            Command::new(bin)
                .args([
                    "serve",
                    "--no-auth",
                    "--cluster",
                    "--history",
                    &db,
                    "--listen",
                    &format!("127.0.0.1:{port}"),
                    // 10s, not 2s: under heavy CI load a debug-build process can be
                    // descheduled past a short lease, falsely expiring it so the peer
                    // reclaims its still-live runs and the cluster thrashes (#235).
                    // Failover after a real kill is still well within the 90s budget.
                    "--lease-ttl-secs",
                    "10",
                    "--cluster-poll-secs",
                    "1",
                ])
                .env("FAUCET_LOG", "warn")
                .spawn()
                .expect("spawn faucet serve"),
        )
    };

    let client = reqwest::Client::new();
    let wait_healthy = |port: u16| {
        let client = client.clone();
        async move {
            for _ in 0..200 {
                if client
                    .get(format!("http://127.0.0.1:{port}/healthz"))
                    .send()
                    .await
                    .map(|r| r.status().is_success())
                    .unwrap_or(false)
                {
                    return true;
                }
                sleep(Duration::from_millis(50)).await;
            }
            false
        }
    };

    let mut a = spawn(port_a);
    let b = spawn(port_b);
    // Poll /healthz rather than sleeping a fixed interval — a debug-build binary
    // can take well over a second to bind its listener under load.
    assert!(wait_healthy(port_a).await, "instance A became healthy");
    assert!(wait_healthy(port_b).await, "instance B became healthy");

    let mut run_ids = Vec::new();
    for _ in 0..5 {
        // The submit-side queue applies backpressure (429 + Retry-After) while the
        // local reservation is briefly held during the Pending upsert; startup
        // SQLite contention can also surface a transient 503. A real client retries;
        // a generous bounded budget keeps the test deterministic under CI load (#235).
        let mut v = None;
        for _ in 0..100 {
            let resp = client
                .post(format!("http://127.0.0.1:{port_a}/v1/runs"))
                .json(&serde_json::json!({ "config": config, "config_format": "yaml" }))
                .send()
                .await
                .unwrap();
            // 429 = queue backpressure (reservation briefly held during the Pending
            // upsert); 503 = history backend transiently degraded (two processes
            // contending on the shared SQLite history at startup). Both are transient
            // — a real client retries, so does the test (#235).
            if resp.status() == 429 || resp.status() == 503 {
                sleep(Duration::from_millis(100)).await;
                continue;
            }
            assert_eq!(resp.status(), 202, "submit accepted");
            v = Some(resp.json::<serde_json::Value>().await.unwrap());
            break;
        }
        let v = v.expect("submit accepted within retry budget");
        run_ids.push(v["run_id"].as_str().unwrap().to_string());
    }

    // Kill A immediately so some runs are still Pending/Running on it.
    let _ = a.0.kill();
    let _ = a.0.wait();

    // Poll B until every run is terminal. Exits early when all are done; the cap is
    // generous (≤ ~90s) so a slow/contended CI runner has time for B to reclaim the
    // expired-lease runs and execute them — failover detection itself is fast
    // (10s lease + 1s poll), but debug-build execution under load is not (#235).
    let deadline = std::time::Instant::now() + Duration::from_secs(90);
    loop {
        let mut all_done = true;
        for id in &run_ids {
            let v: serde_json::Value = client
                .get(format!("http://127.0.0.1:{port_b}/v1/runs/{id}"))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            let status = v["status"].as_str().unwrap_or("");
            if !matches!(status, "completed" | "failed" | "cancelled") {
                all_done = false;
                break;
            }
        }
        if all_done || std::time::Instant::now() > deadline {
            break;
        }
        sleep(Duration::from_millis(500)).await;
    }

    // Every run reached a terminal state on the survivor B — the acceptance proof
    // that B reclaimed and finished the runs A had not yet completed when killed.
    let mut terminal = 0;
    for id in &run_ids {
        let v: serde_json::Value = client
            .get(format!("http://127.0.0.1:{port_b}/v1/runs/{id}"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if matches!(
            v["status"].as_str().unwrap_or(""),
            "completed" | "failed" | "cancelled"
        ) {
            terminal += 1;
        }
    }
    drop(b); // SIGKILL the survivor before the assertion (so a fail still reaps it).
    assert_eq!(
        terminal,
        run_ids.len(),
        "every run reached a terminal state on the survivor B"
    );
}

fn sharded(id: &str) -> RunRecord {
    let mut r = pending(id);
    r.status = RunStatus::Sharded;
    r
}

fn shards(n: usize) -> Vec<faucet_cli::serve::history::ShardInsert> {
    (0..n)
        .map(|i| faucet_cli::serve::history::ShardInsert {
            shard_id: i.to_string(),
            descriptor: serde_json::json!({ "i": i }),
            size_estimate: Some((n - i) as u64),
        })
        .collect()
}

/// SERVE-14: cancelling a sharded run stops every shard that has not started —
/// including one requeued after its worker's lease lapsed — and the run ends
/// `cancelled`, not `failed`.
#[tokio::test]
async fn cancelling_a_sharded_run_stops_its_pending_shards_and_ends_cancelled() {
    use faucet_cli::serve::history::ShardOutcome;
    let dir = tempfile::tempdir().unwrap();
    let a = backend(&dir, "c.db", Duration::from_secs(60), "inst-a").await;
    let b = backend(&dir, "c.db", Duration::from_millis(50), "inst-b").await;
    a.upsert(&sharded("big")).await.unwrap();
    a.insert_shards("big", &shards(4)).await.unwrap();

    assert_eq!(b.claim_shards(1).await.unwrap().len(), 1);
    assert_eq!(a.claim_shards(1).await.unwrap().len(), 1);
    a.request_cancel("big").await.unwrap();

    let p = a.shard_progress("big").await.unwrap();
    assert_eq!((p.pending, p.cancelled, p.running), (0, 2, 2));
    assert!(a.claim_shards(10).await.unwrap().is_empty());

    tokio::time::sleep(Duration::from_millis(120)).await;
    let report = a.reclaim_shards(5).await.unwrap();
    assert_eq!(report.requeued, 1, "b's lapsed shard is reclaimed");
    assert!(a.claim_shards(10).await.unwrap().is_empty());
    let p = a.shard_progress("big").await.unwrap();
    assert_eq!(
        (p.pending, p.cancelled, p.running),
        (0, 3, 1),
        "a reclaimed shard of a cancel-requested run is cancelled, not requeued"
    );

    assert!(
        a.finalize_shard("big", "1", ShardOutcome::Cancelled)
            .await
            .unwrap()
    );
    let p = a.shard_progress("big").await.unwrap();
    assert!(p.all_terminal());
    assert_eq!(p.parent_outcome().0, RunStatus::Cancelled);

    assert_eq!(a.finalize_completed_sharded_parents().await.unwrap(), 1);
    assert_eq!(
        a.get("big").await.unwrap().unwrap().status,
        RunStatus::Cancelled
    );
}
