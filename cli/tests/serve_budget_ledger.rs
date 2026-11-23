//! The run budget of a sharded run, kept in a SQLite run history: shard
//! ledgers of one run never grant past its ceiling, and a shard's budget state
//! returns what it reserved and did not write.
#![cfg(all(feature = "serve", feature = "serve-history-sqlite"))]

use faucet_cli::serve::budget_ledger::{HistoryLedger, shard_budget_state};
use faucet_cli::serve::history::RunHistory;
use faucet_cli::serve::history::sqlite::SqliteHistory;
use faucet_core::{BudgetLedger, BudgetSpec, Sink};
use std::sync::Arc;
use std::time::Duration;

async fn history(dir: &tempfile::TempDir) -> Arc<dyn RunHistory> {
    let url = format!("sqlite:{}", dir.path().join("h.db").display());
    Arc::new(
        SqliteHistory::connect(
            &url,
            Duration::from_secs(3600),
            Duration::from_secs(3600),
            "me".into(),
        )
        .await
        .unwrap(),
    )
}

fn records(max: u64) -> BudgetSpec {
    BudgetSpec {
        max_records: Some(max),
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shard_ledgers_share_one_ceiling() {
    let dir = tempfile::tempdir().unwrap();
    let history = history(&dir).await;
    let mut tasks = Vec::new();
    for _ in 0..4 {
        let h = Arc::clone(&history);
        tasks.push(tokio::spawn(async move {
            let ledger = HistoryLedger::new(h, "run", records(50));
            let mut got = 0;
            for _ in 0..10 {
                got += ledger.grant(3, 0).await.unwrap().records;
            }
            got
        }));
    }
    let mut total = 0;
    for t in tasks {
        total += t.await.unwrap();
    }
    assert_eq!(total, 50);
    let ledger = HistoryLedger::new(Arc::clone(&history), "run", records(50));
    ledger.give_back(20, 0).await.unwrap();
    assert_eq!(ledger.grant(30, 0).await.unwrap().records, 20);
}

struct Accepts;

#[faucet_core::async_trait]
impl Sink for Accepts {
    async fn write_batch(
        &self,
        r: &[serde_json::Value],
    ) -> Result<usize, faucet_core::FaucetError> {
        Ok(r.len())
    }
}

#[tokio::test]
async fn a_shard_returns_what_it_did_not_write() {
    let dir = tempfile::tempdir().unwrap();
    let history = history(&dir).await;
    let spec = records(500);
    let state = shard_budget_state(Arc::clone(&history), "r", &spec, chrono::Utc::now());
    let sink = faucet_core::BudgetSink::wrap_shared(
        Box::new(Accepts),
        spec,
        Arc::clone(&state),
        Default::default(),
    );
    sink.write_batch(&vec![serde_json::json!({"a": 1}); 4])
        .await
        .unwrap();
    state.close().await;
    assert_eq!(history.budget_spent("r").await.unwrap(), Some((4, 0)));
}
