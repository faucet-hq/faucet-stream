//! #736: cluster members advertise the newest state format they read, so a
//! mixed-version cluster keeps writing bookmarks an older member can read.
#![cfg(feature = "serve-history-sqlite")]

use chrono::Utc;
use faucet_cli::serve::history::sqlite::SqliteHistory;
use faucet_cli::serve::history::{InstanceHeartbeat, RunHistory, cluster_state_format};
use faucet_core::state_version::STATE_FORMAT;
use std::time::Duration;

async fn member(dir: &tempfile::TempDir, id: &str) -> SqliteHistory {
    SqliteHistory::connect(
        &format!("sqlite:{}", dir.path().join("cluster.db").display()),
        Duration::from_secs(3600),
        Duration::from_secs(30),
        id.to_string(),
    )
    .await
    .expect("connect sqlite history")
}

fn beat(state_format: u32) -> InstanceHeartbeat {
    InstanceHeartbeat {
        started_at: Utc::now(),
        listen: Some("127.0.0.1:8080".into()),
        max_concurrent: 4,
        in_flight: 0,
        state_format,
    }
}

async fn caps_rows(dir: &tempfile::TempDir) -> i64 {
    let pool = sqlx::SqlitePool::connect(&format!(
        "sqlite:{}",
        dir.path().join("cluster.db").display()
    ))
    .await
    .unwrap();
    sqlx::query_scalar("SELECT COUNT(*) FROM faucet_serve_instance_caps")
        .fetch_one(&pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn the_cluster_reads_the_lowest_advertised_state_format() {
    let dir = tempfile::tempdir().unwrap();
    let a = member(&dir, "inst-a").await;
    let b = member(&dir, "inst-b").await;
    a.heartbeat_instance(&beat(STATE_FORMAT)).await.unwrap();
    b.heartbeat_instance(&beat(STATE_FORMAT)).await.unwrap();
    let live = a.live_instances(Duration::from_secs(60)).await.unwrap();
    assert!(live.iter().all(|m| m.state_format == STATE_FORMAT));
    assert_eq!(cluster_state_format(&live), STATE_FORMAT);

    // A member that predates versioned state never writes a capabilities
    // row: it reads as format 0 and holds the cluster to bare bookmarks.
    let pool = sqlx::SqlitePool::connect(&format!(
        "sqlite:{}",
        dir.path().join("cluster.db").display()
    ))
    .await
    .unwrap();
    sqlx::query("DELETE FROM faucet_serve_instance_caps WHERE instance_id = 'inst-b'")
        .execute(&pool)
        .await
        .unwrap();
    let live = a.live_instances(Duration::from_secs(60)).await.unwrap();
    let old = live.iter().find(|m| m.instance_id == "inst-b").unwrap();
    assert_eq!(old.state_format, 0);
    assert_eq!(cluster_state_format(&live), 0);

    // With nobody live the cluster reads what this release reads.
    assert_eq!(cluster_state_format(&[]), STATE_FORMAT);

    // Pruning a member drops its capabilities row too.
    assert_eq!(caps_rows(&dir).await, 1);
    tokio::time::sleep(Duration::from_millis(20)).await;
    a.purge_expired(Duration::ZERO).await.unwrap();
    assert!(
        a.live_instances(Duration::from_secs(60))
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(caps_rows(&dir).await, 0);
}
