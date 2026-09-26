//! The run lease (`{base}::__lease__`): written when a real invocation starts,
//! renewed while it runs, removed when it ends. `faucet state set|reset|import`
//! refuse to touch a row whose lease is live, and `faucet status` reports the
//! run as in flight. A crashed run stops renewing, so its lease expires after
//! [`LEASE_TTL`] instead of blocking the row forever.

use super::keys::lease_key;
use chrono::{DateTime, Utc};
use faucet_core::StateStore;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;

/// How long a lease stays live without a renewal.
pub const LEASE_TTL: Duration = Duration::from_secs(60);
/// How often a running invocation renews its lease.
pub const RENEW_EVERY: Duration = Duration::from_secs(20);

/// The stored lease.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunLease {
    pub run_id: String,
    pub pid: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    pub acquired_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

impl RunLease {
    fn new(run_id: &str, now: DateTime<Utc>) -> Self {
        Self {
            run_id: run_id.to_string(),
            pid: std::process::id(),
            host: std::env::var("HOSTNAME")
                .ok()
                .or_else(|| std::env::var("COMPUTERNAME").ok())
                .filter(|h| !h.is_empty()),
            acquired_at: now,
            expires_at: expiry(now),
        }
    }

    /// Whether the lease still holds at `now`.
    pub fn is_live(&self, now: DateTime<Utc>) -> bool {
        self.expires_at > now
    }

    /// Decode a stored value (`None` when unreadable).
    pub fn from_value(v: Value) -> Option<Self> {
        serde_json::from_value(v).ok()
    }
}

fn expiry(now: DateTime<Utc>) -> DateTime<Utc> {
    now + chrono::Duration::from_std(LEASE_TTL).unwrap_or_else(|_| chrono::Duration::seconds(60))
}

/// Read the lease for `base`, live or not.
pub async fn read(
    store: &dyn StateStore,
    base: &str,
) -> Result<Option<RunLease>, faucet_core::FaucetError> {
    Ok(store
        .get(&lease_key(base))
        .await?
        .and_then(RunLease::from_value))
}

/// Holds a lease; renews it in the background until [`release`](Self::release).
pub struct LeaseGuard {
    store: Arc<dyn StateStore>,
    key: String,
    run_id: String,
    renew: tokio::task::JoinHandle<()>,
}

/// Take the lease for `base`. Monitoring: a store error is logged and the run
/// proceeds unleased.
pub async fn acquire(store: Arc<dyn StateStore>, base: &str, run_id: &str) -> Option<LeaseGuard> {
    acquire_every(store, base, run_id, RENEW_EVERY).await
}

async fn acquire_every(
    store: Arc<dyn StateStore>,
    base: &str,
    run_id: &str,
    every: Duration,
) -> Option<LeaseGuard> {
    let key = lease_key(base);
    let lease = RunLease::new(run_id, Utc::now());
    let value = serde_json::to_value(&lease).unwrap_or(Value::Null);
    if let Err(e) = store.put(&key, &value).await {
        tracing::warn!(key, error = %e, "run lease could not be written; continuing unleased");
        return None;
    }
    let renew = {
        let store = Arc::clone(&store);
        let key = key.clone();
        tokio::spawn(async move {
            let mut lease = lease;
            loop {
                tokio::time::sleep(every).await;
                lease.expires_at = expiry(Utc::now());
                let value = serde_json::to_value(&lease).unwrap_or(Value::Null);
                if let Err(e) = store.put(&key, &value).await {
                    tracing::warn!(key, error = %e, "run lease renewal failed");
                }
            }
        })
    };
    Some(LeaseGuard {
        store,
        key,
        run_id: run_id.to_string(),
        renew,
    })
}

impl LeaseGuard {
    /// Stop renewing and remove the lease, unless another run has taken it.
    pub async fn release(self) {
        self.renew.abort();
        match self.store.get(&self.key).await {
            Ok(Some(v))
                if RunLease::from_value(v.clone()).is_some_and(|l| l.run_id != self.run_id) => {}
            Ok(_) => {
                if let Err(e) = self.store.delete(&self.key).await {
                    tracing::warn!(key = %self.key, error = %e, "run lease could not be removed");
                }
            }
            Err(e) => {
                tracing::warn!(key = %self.key, error = %e, "run lease unreadable at release");
            }
        }
    }
}

impl Drop for LeaseGuard {
    fn drop(&mut self) {
        self.renew.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::{FaucetError, MemoryStateStore};

    #[tokio::test]
    async fn acquire_renew_release() {
        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        let guard = acquire_every(
            Arc::clone(&store),
            "p::r",
            "run-1",
            Duration::from_millis(5),
        )
        .await
        .unwrap();
        let first = read(store.as_ref(), "p::r").await.unwrap().unwrap();
        assert_eq!(first.run_id, "run-1");
        assert_eq!(first.pid, std::process::id());
        assert!(first.is_live(Utc::now()));
        assert!(!first.is_live(first.expires_at));
        tokio::time::sleep(Duration::from_millis(40)).await;
        let renewed = read(store.as_ref(), "p::r").await.unwrap().unwrap();
        assert!(renewed.expires_at >= first.expires_at);
        guard.release().await;
        assert!(read(store.as_ref(), "p::r").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn release_leaves_a_lease_another_run_took() {
        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        let guard = acquire(Arc::clone(&store), "p::r", "run-1").await.unwrap();
        let _other = acquire(Arc::clone(&store), "p::r", "run-2").await.unwrap();
        guard.release().await;
        let held = read(store.as_ref(), "p::r").await.unwrap().unwrap();
        assert_eq!(held.run_id, "run-2");
        assert!(RunLease::from_value(serde_json::json!(1)).is_none());
    }

    struct Flaky {
        inner: MemoryStateStore,
        fail_put: bool,
        fail_get: bool,
        fail_delete: bool,
    }

    #[faucet_core::async_trait]
    impl StateStore for Flaky {
        async fn get(&self, k: &str) -> Result<Option<Value>, FaucetError> {
            if self.fail_get {
                return Err(FaucetError::State("get down".into()));
            }
            self.inner.get(k).await
        }
        async fn put(&self, k: &str, v: &Value) -> Result<(), FaucetError> {
            if self.fail_put {
                return Err(FaucetError::State("put down".into()));
            }
            self.inner.put(k, v).await
        }
        async fn delete(&self, k: &str) -> Result<(), FaucetError> {
            if self.fail_delete {
                return Err(FaucetError::State("delete down".into()));
            }
            self.inner.delete(k).await
        }
    }

    fn flaky(fail_put: bool, fail_get: bool, fail_delete: bool) -> Arc<dyn StateStore> {
        Arc::new(Flaky {
            inner: MemoryStateStore::new(),
            fail_put,
            fail_get,
            fail_delete,
        })
    }

    #[tokio::test]
    async fn store_errors_never_fail_the_run() {
        assert!(
            acquire(flaky(true, false, false), "p::r", "r")
                .await
                .is_none()
        );
        let g = acquire(flaky(false, true, false), "p::r", "r")
            .await
            .unwrap();
        g.release().await;
        let g = acquire(flaky(false, false, true), "p::r", "r")
            .await
            .unwrap();
        g.release().await;
        let g = acquire(flaky(false, false, false), "p::r", "r")
            .await
            .unwrap();
        drop(g);
    }
}
