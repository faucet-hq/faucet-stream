//! The run budget of a sharded run, kept in the run history: shards of one
//! run execute on several replicas, so their allowance is granted from one
//! row per run (`faucet_serve_run_budgets`) and none can spend past the
//! run's ceilings.

use std::sync::Arc;

use faucet_core::{BudgetGrant, BudgetLedger, BudgetSpec, FaucetError};

use crate::serve::history::RunHistory;

/// A [`BudgetLedger`] over the run history's ledger row for one run.
pub struct HistoryLedger {
    history: Arc<dyn RunHistory>,
    run_id: String,
    spec: BudgetSpec,
}

impl HistoryLedger {
    /// The ledger of run `run_id`, granting against `spec`'s ceilings.
    pub fn new(history: Arc<dyn RunHistory>, run_id: impl Into<String>, spec: BudgetSpec) -> Self {
        Self {
            history,
            run_id: run_id.into(),
            spec,
        }
    }
}

#[async_trait::async_trait]
impl BudgetLedger for HistoryLedger {
    async fn grant(&self, records: u64, bytes: u64) -> Result<BudgetGrant, FaucetError> {
        self.history
            .budget_grant(
                &self.run_id,
                (records, bytes),
                (self.spec.max_records, self.spec.max_bytes),
            )
            .await
            .map_err(|e| FaucetError::State(format!("run budget ledger: {e}")))
    }

    async fn give_back(&self, records: u64, bytes: u64) -> Result<(), FaucetError> {
        self.history
            .budget_give_back(&self.run_id, records, bytes)
            .await
            .map_err(|e| FaucetError::State(format!("run budget ledger: {e}")))
    }
}

/// The budget state of a shard of run `run_id`, started at `started`: its
/// records / bytes are granted from the run's ledger row, and its deadline
/// is the run's.
pub fn shard_budget_state(
    history: Arc<dyn RunHistory>,
    run_id: &str,
    spec: &BudgetSpec,
    started: chrono::DateTime<chrono::Utc>,
) -> Arc<faucet_core::BudgetState> {
    let ledger = Arc::new(HistoryLedger::new(history, run_id, spec.clone()));
    Arc::new(faucet_core::BudgetState::for_run(spec, started.into()).with_ledger(ledger))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::history::memory::MemoryHistory;

    fn spec(max: u64) -> BudgetSpec {
        BudgetSpec {
            max_records: Some(max),
            ..Default::default()
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shards_of_one_run_never_overspend_its_ledger() {
        let history: Arc<dyn RunHistory> =
            Arc::new(MemoryHistory::new(std::time::Duration::from_secs(60)));
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let h = Arc::clone(&history);
            tasks.push(tokio::spawn(async move {
                let ledger = HistoryLedger::new(h, "run", spec(100));
                let mut got = 0;
                for _ in 0..20 {
                    got += ledger.grant(7, 0).await.unwrap().records;
                }
                got
            }));
        }
        let mut total = 0;
        for t in tasks {
            total += t.await.unwrap();
        }
        assert_eq!(total, 100);
        assert_eq!(history.budget_spent("run").await.unwrap(), Some((100, 0)));
        let ledger = HistoryLedger::new(Arc::clone(&history), "run", spec(100));
        ledger.give_back(30, 0).await.unwrap();
        assert_eq!(ledger.grant(50, 0).await.unwrap().records, 30);
    }

    struct Null;
    #[async_trait::async_trait]
    impl faucet_core::Sink for Null {
        async fn write_batch(&self, r: &[serde_json::Value]) -> Result<usize, FaucetError> {
            Ok(r.len())
        }
    }

    #[tokio::test]
    async fn a_shard_state_draws_from_the_run_and_returns_what_it_kept() {
        use faucet_core::Sink as _;
        let history: Arc<dyn RunHistory> =
            Arc::new(MemoryHistory::new(std::time::Duration::from_secs(60)));
        let started = chrono::Utc::now() - chrono::Duration::seconds(30);
        let with_clock = BudgetSpec {
            max_records: Some(640),
            max_duration_secs: Some(60),
            ..Default::default()
        };
        let state = shard_budget_state(Arc::clone(&history), "r", &with_clock, started);
        let deadline: chrono::DateTime<chrono::Utc> = state.deadline().unwrap().into();
        assert_eq!(deadline, started + chrono::Duration::seconds(60));
        let sink = faucet_core::BudgetSink::wrap_shared(
            Box::new(Null),
            with_clock,
            Arc::clone(&state),
            Default::default(),
        );
        let page = vec![serde_json::json!({"a": 1}); 3];
        sink.write_batch(&page).await.unwrap();
        assert_eq!(history.budget_spent("r").await.unwrap(), Some((10, 0)));
        state.close().await;
        assert_eq!(history.budget_spent("r").await.unwrap(), Some((3, 0)));
    }
}
