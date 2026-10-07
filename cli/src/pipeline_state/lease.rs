//! The run lease (`{base}::__lease__`): written when a real invocation starts,
//! renewed while it runs, removed when it ends. `faucet state set|reset|import`
//! refuse to touch a row whose lease is live, and `faucet status` reports the
//! run as in flight. A second run of the same row is refused while the lease
//! is live (unless forced), so two runs never start from one bookmark and race
//! it; the take is a [`StateStore::compare_and_put`], atomic on stores that
//! support it. A crashed run stops renewing, so its lease expires after
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

/// Another run holds the row's live lease.
#[derive(Debug, Clone, PartialEq)]
pub struct LeaseHeld(pub RunLease);

impl LeaseHeld {
    /// The refusal shown to the operator.
    pub fn message(&self, base: &str) -> String {
        let l = &self.0;
        format!(
            "'{base}' is already being run by run {} (pid {}{}, lease expires {}); two runs \
             of one row would start from the same bookmark and race it. Wait for it to \
             finish, or pass --force if that run is gone",
            l.run_id,
            l.pid,
            l.host
                .as_deref()
                .map(|h| format!(" on {h}"))
                .unwrap_or_default(),
            l.expires_at.to_rfc3339(),
        )
    }
}

/// Take the lease for `base` unconditionally (replacing a live one).
/// Monitoring: a store error is logged and the run proceeds unleased.
pub async fn acquire(store: Arc<dyn StateStore>, base: &str, run_id: &str) -> Option<LeaseGuard> {
    acquire_every(store, base, run_id, RENEW_EVERY).await
}

async fn acquire_every(
    store: Arc<dyn StateStore>,
    base: &str,
    run_id: &str,
    every: Duration,
) -> Option<LeaseGuard> {
    try_acquire_every(store, base, run_id, true, every)
        .await
        .unwrap_or_default()
}

/// Take the lease for `base`, refusing with [`LeaseHeld`] when another run
/// holds a live one (unless `force`). A store error is logged and the run
/// proceeds unleased.
pub async fn try_acquire(
    store: Arc<dyn StateStore>,
    base: &str,
    run_id: &str,
    force: bool,
) -> Result<Option<LeaseGuard>, LeaseHeld> {
    try_acquire_every(store, base, run_id, force, RENEW_EVERY).await
}

/// Attempts at the conditional write before giving up on a contended lease.
const TAKE_ATTEMPTS: usize = 3;

async fn try_acquire_every(
    store: Arc<dyn StateStore>,
    base: &str,
    run_id: &str,
    force: bool,
    every: Duration,
) -> Result<Option<LeaseGuard>, LeaseHeld> {
    let key = lease_key(base);
    let lease = RunLease::new(run_id, Utc::now());
    let value = serde_json::to_value(&lease).unwrap_or(Value::Null);
    if force {
        if let Err(e) = store.put(&key, &value).await {
            tracing::warn!(key, error = %e, "run lease could not be written; continuing unleased");
            return Ok(None);
        }
        return Ok(Some(spawn_guard(store, key, run_id, lease, value, every)));
    }
    for _ in 0..TAKE_ATTEMPTS {
        let current = match store.get(&key).await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(key, error = %e, "run lease unreadable; continuing unleased");
                return Ok(None);
            }
        };
        if let Some(held) = current.clone().and_then(RunLease::from_value)
            && held.run_id != run_id
            && held.is_live(Utc::now())
        {
            return Err(LeaseHeld(held));
        }
        match store.compare_and_put(&key, current.as_ref(), &value).await {
            Ok(true) => return Ok(Some(spawn_guard(store, key, run_id, lease, value, every))),
            Ok(false) => continue,
            Err(e) => {
                tracing::warn!(key, error = %e, "run lease could not be written; continuing unleased");
                return Ok(None);
            }
        }
    }
    match read(store.as_ref(), base).await {
        Ok(Some(held)) if held.run_id != run_id && held.is_live(Utc::now()) => Err(LeaseHeld(held)),
        _ => {
            tracing::warn!(key, "run lease kept changing under us; continuing unleased");
            Ok(None)
        }
    }
}

fn spawn_guard(
    store: Arc<dyn StateStore>,
    key: String,
    run_id: &str,
    lease: RunLease,
    value: Value,
    every: Duration,
) -> LeaseGuard {
    let renew = {
        let store = Arc::clone(&store);
        let key = key.clone();
        tokio::spawn(async move {
            let mut lease = lease;
            let mut last = value;
            loop {
                tokio::time::sleep(every).await;
                lease.expires_at = expiry(Utc::now());
                let next = serde_json::to_value(&lease).unwrap_or(Value::Null);
                match store.compare_and_put(&key, Some(&last), &next).await {
                    Ok(true) => last = next,
                    Ok(false) => {
                        tracing::warn!(key, "run lease was taken by another run; stopped renewing");
                        return;
                    }
                    Err(e) => tracing::warn!(key, error = %e, "run lease renewal failed"),
                }
            }
        })
    };
    LeaseGuard {
        store,
        key,
        run_id: run_id.to_string(),
        renew,
    }
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

    struct OneWrite {
        inner: MemoryStateStore,
        writes: std::sync::atomic::AtomicUsize,
    }

    #[faucet_core::async_trait]
    impl StateStore for OneWrite {
        async fn get(&self, k: &str) -> Result<Option<Value>, FaucetError> {
            self.inner.get(k).await
        }
        async fn put(&self, k: &str, v: &Value) -> Result<(), FaucetError> {
            if self
                .writes
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                > 0
            {
                return Err(FaucetError::State("gone".into()));
            }
            self.inner.put(k, v).await
        }
        async fn delete(&self, k: &str) -> Result<(), FaucetError> {
            self.inner.delete(k).await
        }
    }

    #[tokio::test]
    async fn a_failed_renewal_keeps_the_run_going() {
        let store: Arc<dyn StateStore> = Arc::new(OneWrite {
            inner: MemoryStateStore::new(),
            writes: Default::default(),
        });
        let g = acquire_every(Arc::clone(&store), "p::r", "r", Duration::from_millis(2))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(read(store.as_ref(), "p::r").await.unwrap().is_some());
        g.release().await;
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

    #[tokio::test]
    async fn a_live_lease_from_another_run_refuses_the_take() {
        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        let first = try_acquire(Arc::clone(&store), "p::r", "run-1", false)
            .await
            .unwrap()
            .unwrap();
        let held = match try_acquire(Arc::clone(&store), "p::r", "run-2", false).await {
            Err(h) => h,
            Ok(_) => panic!("a live lease must refuse"),
        };
        assert_eq!(held.0.run_id, "run-1");
        let msg = held.message("p::r");
        assert!(msg.contains("run-1") && msg.contains("--force"), "{msg}");
        let again = try_acquire(Arc::clone(&store), "p::r", "run-1", false)
            .await
            .unwrap();
        assert!(again.is_some(), "the holder may re-take its own lease");
        drop(again);
        first.release().await;
        let mut stale = RunLease::new("dead", Utc::now() - chrono::Duration::seconds(600));
        stale.host = Some("h".into());
        store
            .put(&lease_key("p::r"), &serde_json::to_value(&stale).unwrap())
            .await
            .unwrap();
        assert!(LeaseHeld(stale).message("p::r").contains("on h"));
        let g = try_acquire(Arc::clone(&store), "p::r", "run-3", false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            read(store.as_ref(), "p::r").await.unwrap().unwrap().run_id,
            "run-3"
        );
        g.release().await;
        assert!(
            try_acquire(flaky(false, true, false), "p::r", "r", false)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            try_acquire(flaky(true, false, false), "p::r", "r", false)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn a_forced_take_stops_the_old_holder_renewing() {
        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        let old = try_acquire_every(
            Arc::clone(&store),
            "p::r",
            "old",
            false,
            Duration::from_millis(5),
        )
        .await
        .unwrap()
        .unwrap();
        let new = try_acquire(Arc::clone(&store), "p::r", "new", true)
            .await
            .unwrap()
            .unwrap();
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert_eq!(
            read(store.as_ref(), "p::r").await.unwrap().unwrap().run_id,
            "new"
        );
        old.release().await;
        new.release().await;
    }

    struct Racing {
        inner: MemoryStateStore,
        holder: Value,
    }

    #[faucet_core::async_trait]
    impl StateStore for Racing {
        async fn get(&self, k: &str) -> Result<Option<Value>, FaucetError> {
            self.inner.get(k).await
        }
        async fn put(&self, k: &str, v: &Value) -> Result<(), FaucetError> {
            self.inner.put(k, v).await
        }
        async fn delete(&self, k: &str) -> Result<(), FaucetError> {
            self.inner.delete(k).await
        }
        async fn compare_and_put(
            &self,
            k: &str,
            _expected: Option<&Value>,
            _v: &Value,
        ) -> Result<bool, FaucetError> {
            self.inner.put(k, &self.holder).await?;
            Ok(false)
        }
    }

    #[tokio::test]
    async fn losing_every_race_to_a_live_run_refuses() {
        let holder = serde_json::to_value(RunLease::new("winner", Utc::now())).unwrap();
        let store: Arc<dyn StateStore> = Arc::new(Racing {
            inner: MemoryStateStore::new(),
            holder,
        });
        match try_acquire(store, "p::r", "loser", false).await {
            Err(h) => assert_eq!(h.0.run_id, "winner"),
            Ok(_) => panic!("must refuse"),
        }
        let dead = serde_json::to_value(RunLease::new(
            "dead",
            Utc::now() - chrono::Duration::seconds(600),
        ))
        .unwrap();
        let store: Arc<dyn StateStore> = Arc::new(Racing {
            inner: MemoryStateStore::new(),
            holder: dead,
        });
        assert!(
            try_acquire(store, "p::r", "loser", false)
                .await
                .unwrap()
                .is_none()
        );
    }
}
