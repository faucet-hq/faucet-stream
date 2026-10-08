//! The run lease (`{base}::__lease__`): written when a real invocation starts,
//! renewed while it runs, removed when it ends. `faucet state set|reset|import`
//! refuse to touch a row whose lease is live, and `faucet status` reports the
//! run as in flight. A second run of the same row is refused while the lease
//! is live (unless forced), so two runs never start from one bookmark and race
//! it; the take is a [`StateStore::compare_and_put`], atomic on stores that
//! support it. A crashed run stops renewing, so its lease expires after
//! [`LEASE_TTL`] instead of blocking the row forever; a holder on this host
//! whose process is gone (or, in this process, whose run has ended) is stale
//! at once and taken over without `--force`.

use super::keys::lease_key;
use chrono::{DateTime, Utc};
use faucet_core::StateStore;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};
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
    /// The holder's PID namespace (Linux): two containers can share a host
    /// name while their process ids mean different processes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid_ns: Option<String>,
    pub acquired_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

impl RunLease {
    fn new(run_id: &str, now: DateTime<Utc>) -> Self {
        Self {
            run_id: run_id.to_string(),
            pid: std::process::id(),
            host: this_host().clone(),
            pid_ns: this_pid_ns().clone(),
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

/// This machine's host name, as leases record it.
fn this_host() -> &'static Option<String> {
    static HOST: LazyLock<Option<String>> = LazyLock::new(|| {
        os_host_name()
            .or_else(|| std::env::var("HOSTNAME").ok())
            .or_else(|| std::env::var("COMPUTERNAME").ok())
            .filter(|h| !h.is_empty())
    });
    &HOST
}

/// This process's PID namespace (`None` off Linux).
fn this_pid_ns() -> &'static Option<String> {
    static NS: LazyLock<Option<String>> = LazyLock::new(|| {
        std::fs::read_link("/proc/self/ns/pid")
            .ok()
            .map(|p| p.to_string_lossy().into_owned())
    });
    &NS
}

#[cfg(unix)]
fn os_host_name() -> Option<String> {
    let mut buf = [0u8; 256];
    // SAFETY: the buffer is valid for `buf.len()` bytes; gethostname
    // NUL-terminates within it on success.
    let rc = unsafe { libc::gethostname(buf.as_mut_ptr().cast(), buf.len()) };
    if rc != 0 {
        return None;
    }
    let end = buf.iter().position(|b| *b == 0).unwrap_or(buf.len());
    String::from_utf8(buf[..end].to_vec()).ok()
}

#[cfg(not(unix))]
fn os_host_name() -> Option<String> {
    None
}

/// Whether process `pid` on this host is still running (`None`: cannot tell).
#[cfg(unix)]
fn pid_alive(pid: u32) -> Option<bool> {
    let pid = libc::pid_t::try_from(pid).ok()?;
    // SAFETY: signal 0 only checks that the process exists.
    if unsafe { libc::kill(pid, 0) } == 0 {
        return Some(true);
    }
    match std::io::Error::last_os_error().raw_os_error() {
        Some(libc::ESRCH) => Some(false),
        Some(libc::EPERM) => Some(true),
        _ => None,
    }
}

#[cfg(not(unix))]
fn pid_alive(_pid: u32) -> Option<bool> {
    None
}

/// Guards this process holds, by `(lease key, run id)`.
static HELD_HERE: LazyLock<Mutex<HashMap<(String, String), usize>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn held_here(key: &str, run_id: &str) -> bool {
    HELD_HERE
        .lock()
        .map(|h| h.contains_key(&(key.to_string(), run_id.to_string())))
        .unwrap_or(false)
}

fn set_held_here(key: &str, run_id: &str, held: bool) {
    if let Ok(mut h) = HELD_HERE.lock() {
        let entry = (key.to_string(), run_id.to_string());
        if held {
            *h.entry(entry).or_default() += 1;
        } else if let Some(n) = h.get_mut(&entry) {
            *n -= 1;
            if *n == 0 {
                h.remove(&entry);
            }
        }
    }
}

/// Who holds a live lease, as far as this process can tell.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Holder {
    /// A run in this process that still holds its guard.
    ThisProcess,
    /// A process on this host that is still running.
    Alive,
    /// Gone: a run in this process that ended, or a dead process on this host.
    Gone,
    /// On another host (or unknown): only the TTL or `--force` frees it.
    Remote,
}

fn classify_holder(key: &str, lease: &RunLease) -> Holder {
    let same_host =
        lease.host.is_some() && lease.host == *this_host() && lease.pid_ns == *this_pid_ns();
    classify(
        same_host,
        lease.pid == std::process::id(),
        || held_here(key, &lease.run_id),
        || pid_alive(lease.pid),
    )
}

fn classify(
    same_host: bool,
    same_pid: bool,
    held_here: impl FnOnce() -> bool,
    alive: impl FnOnce() -> Option<bool>,
) -> Holder {
    if !same_host {
        return Holder::Remote;
    }
    if same_pid {
        return if held_here() {
            Holder::ThisProcess
        } else {
            Holder::Gone
        };
    }
    match alive() {
        Some(false) => Holder::Gone,
        Some(true) => Holder::Alive,
        None => Holder::Remote,
    }
}

/// How long a take waits for a run in this process that is being torn down
/// (an aborted task drops its guard a moment after the abort).
const LOCAL_TEARDOWN_WAIT: Duration = Duration::from_secs(2);

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
    renew: Option<tokio::task::JoinHandle<()>>,
    stop: tokio_util::sync::CancellationToken,
    released: bool,
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
            && !holder_gone(&key, &held).await
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
        Ok(Some(held))
            if held.run_id != run_id
                && held.is_live(Utc::now())
                && classify_holder(&key, &held) != Holder::Gone =>
        {
            Err(LeaseHeld(held))
        }
        _ => {
            tracing::warn!(key, "run lease kept changing under us; continuing unleased");
            Ok(None)
        }
    }
}

/// Whether a live lease's holder is gone, so the lease may be taken over
/// without `--force`. Waits briefly for a run in this process that is being
/// torn down.
async fn holder_gone(key: &str, held: &RunLease) -> bool {
    let deadline = tokio::time::Instant::now() + LOCAL_TEARDOWN_WAIT;
    loop {
        match classify_holder(key, held) {
            Holder::Gone => {
                tracing::warn!(
                    key,
                    run_id = %held.run_id,
                    pid = held.pid,
                    "run lease held by a run that is gone on this host; taking it over"
                );
                return true;
            }
            Holder::ThisProcess if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            _ => return false,
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
    let stop = tokio_util::sync::CancellationToken::new();
    let renew = {
        let store = Arc::clone(&store);
        let key = key.clone();
        let stop = stop.clone();
        tokio::spawn(async move {
            let mut lease = lease;
            let mut last = value;
            loop {
                tokio::select! {
                    biased;
                    _ = stop.cancelled() => return,
                    _ = tokio::time::sleep(every) => {}
                }
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
    set_held_here(&key, run_id, true);
    LeaseGuard {
        store,
        key,
        run_id: run_id.to_string(),
        renew: Some(renew),
        stop,
        released: false,
    }
}

/// Expire our lease at `key` with a conditional write, so a run that took the
/// lease in the meantime keeps it.
async fn expire_lease(store: &dyn StateStore, key: &str, run_id: &str) {
    let current = match store.get(key).await {
        Ok(Some(v)) => v,
        Ok(None) => return,
        Err(e) => {
            tracing::warn!(key, error = %e, "run lease unreadable at release");
            return;
        }
    };
    let Some(mut lease) = RunLease::from_value(current.clone()).filter(|l| l.run_id == run_id)
    else {
        return;
    };
    lease.expires_at = Utc::now() - chrono::Duration::seconds(1);
    let expired = serde_json::to_value(&lease).unwrap_or(Value::Null);
    if let Err(e) = store.compare_and_put(key, Some(&current), &expired).await {
        tracing::warn!(key, error = %e, "run lease could not be released");
    }
}

/// Remove the lease at `key` unless another run has taken it.
async fn release_lease(store: &dyn StateStore, key: &str, run_id: &str) {
    match store.get(key).await {
        Ok(Some(v)) if RunLease::from_value(v.clone()).is_some_and(|l| l.run_id != run_id) => {}
        Ok(_) => {
            if let Err(e) = store.delete(key).await {
                tracing::warn!(key, error = %e, "run lease could not be removed");
            }
        }
        Err(e) => {
            tracing::warn!(key, error = %e, "run lease unreadable at release");
        }
    }
}

impl LeaseGuard {
    /// Stop renewing and remove the lease, unless another run has taken it.
    /// The renewal is stopped between writes and awaited first, so an
    /// in-flight renewal can never re-create the lease after it is removed.
    pub async fn release(mut self) {
        self.stop.cancel();
        if let Some(renew) = self.renew.take() {
            let _ = renew.await;
        }
        release_lease(self.store.as_ref(), &self.key, &self.run_id).await;
        self.released = true;
    }
}

/// A guard dropped without [`LeaseGuard::release`] (a cancelled or aborted
/// run) stops renewing and releases the lease in the background, so the next
/// run of the row is not refused until the TTL runs out.
impl Drop for LeaseGuard {
    fn drop(&mut self) {
        self.stop.cancel();
        set_held_here(&self.key, &self.run_id, false);
        let renew = self.renew.take();
        if self.released {
            return;
        }
        match tokio::runtime::Handle::try_current() {
            Ok(rt) => {
                let store = Arc::clone(&self.store);
                let key = std::mem::take(&mut self.key);
                let run_id = std::mem::take(&mut self.run_id);
                rt.spawn(async move {
                    if let Some(renew) = renew {
                        let _ = renew.await;
                    }
                    expire_lease(store.as_ref(), &key, &run_id).await;
                });
            }
            Err(_) => {
                if let Some(renew) = renew {
                    renew.abort();
                }
            }
        }
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
        let mut winner = RunLease::new("winner", Utc::now());
        winner.pid = live_foreign_pid();
        let holder = serde_json::to_value(winner).unwrap();
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

    /// A process on this host that outlives the test (init / launchd).
    fn live_foreign_pid() -> u32 {
        1
    }

    fn dead_pid() -> u32 {
        let mut child = std::process::Command::new("true").spawn().unwrap();
        let pid = child.id();
        child.wait().unwrap();
        pid
    }

    async fn plant(store: &dyn StateStore, base: &str, lease: &RunLease) {
        store
            .put(&lease_key(base), &serde_json::to_value(lease).unwrap())
            .await
            .unwrap();
    }

    #[test]
    fn holders_are_classified_by_host_process_and_guard() {
        let never = || -> bool { panic!("not consulted") };
        let unknown = || -> Option<bool> { panic!("not consulted") };
        assert_eq!(classify(false, true, never, unknown), Holder::Remote);
        assert_eq!(classify(true, true, || true, unknown), Holder::ThisProcess);
        assert_eq!(classify(true, true, || false, unknown), Holder::Gone);
        assert_eq!(classify(true, false, never, || Some(true)), Holder::Alive);
        assert_eq!(classify(true, false, never, || Some(false)), Holder::Gone);
        assert_eq!(classify(true, false, never, || None), Holder::Remote);
    }

    #[cfg(unix)]
    #[test]
    fn host_and_pid_probes() {
        assert!(this_host().as_deref().is_some_and(|h| !h.is_empty()));
        assert_eq!(pid_alive(std::process::id()), Some(true));
        assert_eq!(pid_alive(live_foreign_pid()), Some(true));
        assert_eq!(pid_alive(dead_pid()), Some(false));
        assert_eq!(pid_alive(u32::MAX), None);
    }

    #[tokio::test]
    async fn a_dropped_guard_frees_the_row_for_the_next_run_at_once() {
        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        let crashed = try_acquire(Arc::clone(&store), "p::r", "crashed", false)
            .await
            .unwrap()
            .unwrap();
        drop(crashed);
        let next = try_acquire(Arc::clone(&store), "p::r", "next", false)
            .await
            .expect("an aborted run's lease never refuses the next run")
            .unwrap();
        assert_eq!(
            read(store.as_ref(), "p::r").await.unwrap().unwrap().run_id,
            "next"
        );
        next.release().await;
        assert!(read(store.as_ref(), "p::r").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn the_background_release_expires_only_its_own_lease() {
        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        let g = try_acquire(Arc::clone(&store), "p::r", "a", false)
            .await
            .unwrap()
            .unwrap();
        drop(g);
        for _ in 0..100 {
            if read(store.as_ref(), "p::r")
                .await
                .unwrap()
                .is_some_and(|l| !l.is_live(Utc::now()))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let expired = read(store.as_ref(), "p::r").await.unwrap().unwrap();
        assert_eq!(expired.run_id, "a");
        assert!(
            !expired.is_live(Utc::now()),
            "dropped guard expires its lease"
        );

        expire_lease(store.as_ref(), "p::r", "someone-else").await;
        let mut other = RunLease::new("b", Utc::now());
        other.pid = live_foreign_pid();
        plant(store.as_ref(), "p::r", &other).await;
        expire_lease(store.as_ref(), "p::r", "a").await;
        assert!(
            read(store.as_ref(), "p::r")
                .await
                .unwrap()
                .unwrap()
                .is_live(Utc::now()),
            "another run's lease is left alone"
        );
        expire_lease(store.as_ref(), "absent", "a").await;
        expire_lease(flaky(false, true, false).as_ref(), "p::r", "a").await;
    }

    #[tokio::test]
    async fn a_dead_holder_on_this_host_is_taken_over_without_force() {
        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        let mut dead = RunLease::new("dead", Utc::now());
        dead.pid = dead_pid();
        plant(store.as_ref(), "p::r", &dead).await;
        let g = try_acquire(Arc::clone(&store), "p::r", "next", false)
            .await
            .expect("a dead holder on this host is stale")
            .unwrap();
        g.release().await;

        let gone_here = RunLease::new("gone", Utc::now());
        plant(store.as_ref(), "p::r", &gone_here).await;
        let g = try_acquire(Arc::clone(&store), "p::r", "next", false)
            .await
            .expect("an ended run of this process is stale")
            .unwrap();
        g.release().await;
    }

    #[tokio::test]
    async fn a_holder_elsewhere_still_needs_the_ttl_or_force() {
        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        let mut remote = RunLease::new("remote", Utc::now());
        remote.host = Some("some-other-host.invalid".into());
        remote.pid = dead_pid();
        plant(store.as_ref(), "p::r", &remote).await;
        assert!(
            try_acquire(Arc::clone(&store), "p::r", "next", false)
                .await
                .is_err()
        );
        let mut alive = RunLease::new("alive", Utc::now());
        alive.pid = live_foreign_pid();
        plant(store.as_ref(), "p::r", &alive).await;
        assert!(
            try_acquire(Arc::clone(&store), "p::r", "next", false)
                .await
                .is_err()
        );
        let g = try_acquire(Arc::clone(&store), "p::r", "next", true)
            .await
            .unwrap()
            .unwrap();
        g.release().await;
    }

    #[tokio::test]
    async fn a_run_in_this_process_being_torn_down_is_waited_for() {
        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        let held = try_acquire(Arc::clone(&store), "p::r", "old", false)
            .await
            .unwrap()
            .unwrap();
        let dropper = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            drop(held);
        });
        let g = try_acquire(Arc::clone(&store), "p::r", "new", false)
            .await
            .expect("taken once the old run's guard is dropped")
            .unwrap();
        dropper.await.unwrap();
        g.release().await;
    }

    /// A store whose conditional write is a read then a slow write (a store
    /// without an atomic compare-and-put), so a renewal is still in flight
    /// when the guard is released.
    struct SlowCas {
        inner: Arc<MemoryStateStore>,
        in_cas: Arc<tokio::sync::Notify>,
    }

    #[faucet_core::async_trait]
    impl StateStore for SlowCas {
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
            expected: Option<&Value>,
            v: &Value,
        ) -> Result<bool, FaucetError> {
            if expected.is_none() {
                return self.inner.compare_and_put(k, expected, v).await;
            }
            self.in_cas.notify_one();
            let (inner, k, e, v) = (
                Arc::clone(&self.inner),
                k.to_string(),
                expected.cloned(),
                v.clone(),
            );
            // Like a statement already sent to a server: it commits even if
            // the caller stops waiting.
            tokio::spawn(async move {
                if inner.get(&k).await? != e {
                    return Ok(false);
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
                inner.put(&k, &v).await.map(|()| true)
            })
            .await
            .unwrap()
        }
    }

    #[tokio::test]
    async fn a_renewal_in_flight_never_outlives_release() {
        let in_cas = Arc::new(tokio::sync::Notify::new());
        let store: Arc<dyn StateStore> = Arc::new(SlowCas {
            inner: Arc::new(MemoryStateStore::new()),
            in_cas: Arc::clone(&in_cas),
        });
        let g = try_acquire_every(
            Arc::clone(&store),
            "p::r",
            "r",
            false,
            Duration::from_millis(5),
        )
        .await
        .unwrap()
        .unwrap();
        in_cas.notified().await;
        g.release().await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            read(store.as_ref(), "p::r").await.unwrap().is_none(),
            "a late renewal re-created the released lease"
        );
    }

    #[test]
    fn a_guard_dropped_outside_a_runtime_stops_renewing() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        let g = rt
            .block_on(acquire(Arc::clone(&store), "p::r", "r"))
            .unwrap();
        drop(g);
        let lease = rt.block_on(read(store.as_ref(), "p::r")).unwrap();
        assert!(
            lease.is_some(),
            "no runtime to release on; the TTL frees it"
        );
    }
}
