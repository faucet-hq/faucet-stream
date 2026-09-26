//! The per-invocation run-outcome marker (`{base}::__status__`): when a row
//! last succeeded and last failed, with what — the durable fact `faucet status`
//! reads without any run history store. Written by the executor after every
//! real (non-preview, non-shard) invocation; never fails the run it records.

use super::keys::status_key;
use chrono::{DateTime, Utc};
use faucet_core::StateStore;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Longest error message kept (the marker is status, not a log).
pub const MAX_ERROR_CHARS: usize = 500;

/// One run, as the marker remembers it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OutcomeEvent {
    pub at: DateTime<Utc>,
    pub run_id: String,
    #[serde(default)]
    pub records: u64,
    #[serde(default)]
    pub duration_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// The stored marker.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct RunOutcomes {
    pub last_success: Option<OutcomeEvent>,
    pub last_failure: Option<OutcomeEvent>,
    /// Failures since the last success.
    pub consecutive_failures: u32,
}

impl RunOutcomes {
    /// Decode a stored value; an unreadable one degrades to empty.
    pub fn from_value(v: Value) -> Self {
        serde_json::from_value(v).unwrap_or_default()
    }

    /// Whether the most recent recorded run failed.
    pub fn failing(&self) -> bool {
        match (&self.last_success, &self.last_failure) {
            (_, None) => false,
            (None, Some(_)) => true,
            (Some(s), Some(f)) => f.at > s.at,
        }
    }

    /// Fold one run in.
    pub fn record(&mut self, event: OutcomeEvent) {
        if event.error.is_some() || event.error_kind.is_some() {
            self.consecutive_failures = self.consecutive_failures.saturating_add(1);
            self.last_failure = Some(event);
        } else {
            self.consecutive_failures = 0;
            self.last_success = Some(event);
        }
    }
}

/// Redact and bound an error message before it is persisted.
pub fn scrub_error(message: &str) -> String {
    let redacted = crate::secrets::registry::redact(message).into_owned();
    if redacted.chars().count() <= MAX_ERROR_CHARS {
        return redacted;
    }
    let mut cut: String = redacted.chars().take(MAX_ERROR_CHARS).collect();
    cut.push('…');
    cut
}

/// Read-modify-write the marker for `base`. Monitoring: errors are logged.
pub async fn record(store: &dyn StateStore, base: &str, event: OutcomeEvent) {
    let key = status_key(base);
    let mut outcomes = match store.get(&key).await {
        Ok(v) => v.map(RunOutcomes::from_value).unwrap_or_default(),
        Err(e) => {
            tracing::warn!(key, error = %e, "run-outcome marker unreadable; not updating it");
            return;
        }
    };
    outcomes.record(event);
    let value = serde_json::to_value(&outcomes).unwrap_or(Value::Null);
    if let Err(e) = store.put(&key, &value).await {
        tracing::warn!(key, error = %e, "run-outcome marker could not be written");
    }
}

/// Read the marker for `base`.
pub async fn read(
    store: &dyn StateStore,
    base: &str,
) -> Result<RunOutcomes, faucet_core::FaucetError> {
    Ok(store
        .get(&status_key(base))
        .await?
        .map(RunOutcomes::from_value)
        .unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::MemoryStateStore;
    use serde_json::json;

    fn ev(secs: i64, err: Option<&str>) -> OutcomeEvent {
        OutcomeEvent {
            at: DateTime::from_timestamp(secs, 0).unwrap(),
            run_id: format!("r{secs}"),
            records: 3,
            duration_ms: 10,
            error_kind: err.map(|_| "sink".into()),
            error: err.map(str::to_owned),
        }
    }

    #[test]
    fn folds_success_and_failure() {
        let mut o = RunOutcomes::default();
        assert!(!o.failing());
        o.record(ev(10, Some("boom")));
        o.record(ev(20, Some("boom")));
        assert!(o.failing());
        assert_eq!(o.consecutive_failures, 2);
        o.record(ev(30, None));
        assert!(!o.failing());
        assert_eq!(o.consecutive_failures, 0);
        o.record(ev(40, Some("again")));
        assert!(o.failing());
        assert_eq!(o.last_success.as_ref().unwrap().run_id, "r30");
    }

    #[test]
    fn unreadable_value_is_empty_and_errors_are_bounded() {
        assert_eq!(
            RunOutcomes::from_value(json!("nope")),
            RunOutcomes::default()
        );
        let long = "x".repeat(MAX_ERROR_CHARS + 10);
        let s = scrub_error(&long);
        assert_eq!(s.chars().count(), MAX_ERROR_CHARS + 1);
        assert!(s.ends_with('…'));
        assert_eq!(scrub_error("short"), "short");
    }

    #[tokio::test]
    async fn record_and_read_round_trip() {
        let store = MemoryStateStore::new();
        record(&store, "p::r", ev(10, None)).await;
        record(&store, "p::r", ev(20, Some("bad"))).await;
        let o = read(&store, "p::r").await.unwrap();
        assert_eq!(o.last_success.unwrap().run_id, "r10");
        assert_eq!(o.last_failure.unwrap().error.as_deref(), Some("bad"));
        assert!(
            read(&store, "p::other")
                .await
                .unwrap()
                .last_success
                .is_none()
        );
    }

    #[tokio::test]
    async fn record_skips_when_the_store_fails() {
        struct Broken;
        #[faucet_core::async_trait]
        impl StateStore for Broken {
            async fn get(&self, _: &str) -> Result<Option<Value>, faucet_core::FaucetError> {
                Err(faucet_core::FaucetError::State("down".into()))
            }
            async fn put(&self, _: &str, _: &Value) -> Result<(), faucet_core::FaucetError> {
                Err(faucet_core::FaucetError::State("down".into()))
            }
            async fn delete(&self, _: &str) -> Result<(), faucet_core::FaucetError> {
                Ok(())
            }
        }
        record(&Broken, "p::r", ev(1, None)).await;
        struct WriteBroken(MemoryStateStore);
        #[faucet_core::async_trait]
        impl StateStore for WriteBroken {
            async fn get(&self, k: &str) -> Result<Option<Value>, faucet_core::FaucetError> {
                self.0.get(k).await
            }
            async fn put(&self, _: &str, _: &Value) -> Result<(), faucet_core::FaucetError> {
                Err(faucet_core::FaucetError::State("ro".into()))
            }
            async fn delete(&self, _: &str) -> Result<(), faucet_core::FaucetError> {
                Ok(())
            }
        }
        let s = WriteBroken(MemoryStateStore::new());
        record(&s, "p::r", ev(1, None)).await;
        assert!(read(&s, "p::r").await.unwrap().last_success.is_none());
        assert!(read(&Broken, "p::r").await.is_err());
    }
}
