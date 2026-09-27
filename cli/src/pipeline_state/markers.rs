//! The run lease + run-outcome marker bracket around one invocation, as the
//! executor (and topology mode) drive it. Both halves are boxed so they add a
//! pointer, not their state machines, to the caller's future.

use super::lease::{self, LeaseGuard};
use super::outcome::{self, OutcomeEvent};
use chrono::Utc;
use faucet_core::StateStore;
use futures::future::BoxFuture;
use std::sync::Arc;

/// What the run reported beyond its result, kept on the outcome marker.
#[derive(Debug, Clone, Default)]
pub struct OutcomeExtras {
    pub batches: Option<faucet_core::BatchOutcomes>,
    pub lag: Option<faucet_core::SourceLag>,
}

/// The bracket's state between `begin` and `finish`.
#[derive(Default)]
pub struct RunMarkers {
    /// The store the markers live in — also handed to the invocation so it is
    /// not built twice.
    pub store: Option<Arc<dyn StateStore>>,
    lease: Option<LeaseGuard>,
}

impl RunMarkers {
    /// Open the bracket: take the lease when `lease` (roots / products).
    pub fn begin<'a>(
        store: Option<Arc<dyn StateStore>>,
        base: &'a str,
        run_id: &'a str,
        take_lease: bool,
    ) -> BoxFuture<'a, Self> {
        Box::pin(async move {
            let lease = match (&store, take_lease) {
                (Some(s), true) => lease::acquire(Arc::clone(s), base, run_id).await,
                _ => None,
            };
            Self { store, lease }
        })
    }

    /// Close the bracket: record the outcome (unless `record` is false — a
    /// cancelled run) and release the lease. `outcome` is the records written
    /// or the failure's `(kind, message)`.
    pub fn finish(
        self,
        base: String,
        run_id: String,
        outcome: Result<u64, (String, String)>,
        duration_ms: u64,
        record: bool,
        extras: OutcomeExtras,
    ) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            if let (Some(store), true) = (&self.store, record) {
                let (records, error_kind, error) = match outcome {
                    Ok(n) => (n, None, None),
                    Err((kind, msg)) => (0, Some(kind), Some(outcome::scrub_error(&msg))),
                };
                outcome::record(
                    store.as_ref(),
                    &base,
                    OutcomeEvent {
                        at: Utc::now(),
                        run_id,
                        records,
                        duration_ms,
                        error_kind,
                        error,
                        batches: extras.batches,
                        lag: extras.lag,
                    },
                )
                .await;
            }
            if let Some(l) = self.lease {
                l.release().await;
            }
        })
    }
}

/// The leading identifier of a `Debug` rendering — an error enum's variant
/// name (`Sink("…")` → `Sink`).
pub fn kind_label(debug: &str) -> String {
    let head: String = debug
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    if head.is_empty() {
        "Error".into()
    } else {
        head
    }
}

/// The kind label of a CLI error: the wrapped `FaucetError`'s variant when
/// there is one.
pub fn error_kind(err: &crate::error::CliError) -> String {
    match err {
        crate::error::CliError::Faucet(e) => kind_label(&format!("{e:?}")),
        other => kind_label(&format!("{other:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::CliError;
    use faucet_core::{FaucetError, MemoryStateStore};

    #[test]
    fn labels_error_kinds() {
        assert_eq!(kind_label("Sink(\"x\")"), "Sink");
        assert_eq!(kind_label("CircuitOpen { failures: 1 }"), "CircuitOpen");
        assert_eq!(kind_label("(weird)"), "Error");
        assert_eq!(
            error_kind(&CliError::Faucet(FaucetError::Sink("d".into()))),
            "Sink"
        );
        assert_eq!(error_kind(&CliError::Config("x".into())), "Config");
    }

    #[tokio::test]
    async fn bracket_records_and_releases() {
        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        let m = RunMarkers::begin(Some(Arc::clone(&store)), "p::r", "run-1", true).await;
        assert!(lease::read(store.as_ref(), "p::r").await.unwrap().is_some());
        m.finish(
            "p::r".into(),
            "run-1".into(),
            Err(("Sink".into(), "boom".into())),
            5,
            true,
            Default::default(),
        )
        .await;
        assert!(lease::read(store.as_ref(), "p::r").await.unwrap().is_none());
        let o = outcome::read(store.as_ref(), "p::r").await.unwrap();
        assert_eq!(o.last_failure.unwrap().error_kind.as_deref(), Some("Sink"));

        let m = RunMarkers::begin(Some(Arc::clone(&store)), "p::r", "run-2", false).await;
        let extras = OutcomeExtras {
            batches: Some(faucet_core::BatchOutcomes {
                attempted: 2,
                committed: 1,
                dlq_all: 1,
                ..Default::default()
            }),
            lag: Some(faucet_core::SourceLag::bytes(10)),
        };
        m.finish("p::r".into(), "run-2".into(), Ok(3), 5, true, extras)
            .await;
        let success = outcome::read(store.as_ref(), "p::r")
            .await
            .unwrap()
            .last_success
            .unwrap();
        assert_eq!(success.records, 3);
        assert_eq!(success.batches.unwrap().dlq_all, 1);
        assert_eq!(success.lag, Some(faucet_core::SourceLag::bytes(10)));

        let m = RunMarkers::begin(Some(Arc::clone(&store)), "p::r", "run-3", true).await;
        m.finish(
            "p::r".into(),
            "run-3".into(),
            Ok(9),
            5,
            false,
            Default::default(),
        )
        .await;
        let o = outcome::read(store.as_ref(), "p::r").await.unwrap();
        assert_eq!(
            o.last_success.unwrap().run_id,
            "run-2",
            "a cancelled run is not recorded"
        );

        RunMarkers::begin(None, "p::r", "x", true)
            .await
            .finish(
                "p::r".into(),
                "x".into(),
                Ok(1),
                1,
                true,
                Default::default(),
            )
            .await;
        RunMarkers::default()
            .finish(
                "p::r".into(),
                "x".into(),
                Ok(1),
                1,
                true,
                Default::default(),
            )
            .await;
    }
}
