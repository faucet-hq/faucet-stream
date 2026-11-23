//! The output lock: a lease, held in side files beside the commit ledger,
//! that serialises the writers of one output — every overwrite commit, and
//! every append that rewrites a shared object.
//!
//! Files `lock-<epoch>`; the current lock is the highest epoch. A writer
//! takes epoch `N+1` by creating it only when absent (S3 `If-None-Match`,
//! GCS `ifGenerationMatch=0`, Azure `If-None-Match`, SFTP `EXCL`, local
//! `O_EXCL`), so only one of any number of racing writers wins it. The holder
//! rewrites its body every [`LockTimings::renew`]; a waiter takes over a
//! lock whose body has not changed for [`LockTimings::lease`] of its own
//! clock (no clocks are compared across hosts). The holder fences itself:
//! once its last renewal was *sent* more than half a lease ago — by either
//! the monotonic or the wall clock, so a suspended host counts too — every
//! check fails, and every call it makes while holding the lock is cut off at
//! that instant. Released locks keep their file (body `released`), so epochs
//! only grow and a writer that lost its lock never meets a recycled one.

use super::backend::{SideCreate, StorageBackend};
use faucet_core::FaucetError;
use faucet_core::io::CallContext;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant, SystemTime};

/// The side-file name prefix of lock epochs.
pub(crate) const LOCK_PREFIX: &str = "lock-";
const RELEASED: &str = "released";

/// How long locks live and how often they are renewed and polled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LockTimings {
    /// How long a waiter must see a lock unchanged before taking it over.
    pub lease: Duration,
    /// How often the holder rewrites its lock.
    pub renew: Duration,
    /// How often a waiter looks again.
    pub poll: Duration,
    /// How long a commit or an append waits for the lock before failing.
    pub wait_max: Duration,
    /// How long an abort waits before leaving recovery to the next run.
    pub abort_wait: Duration,
}

impl Default for LockTimings {
    fn default() -> Self {
        Self {
            lease: Duration::from_secs(120),
            renew: Duration::from_secs(30),
            poll: Duration::from_secs(5),
            wait_max: Duration::from_secs(60 * 60),
            abort_wait: Duration::from_secs(30),
        }
    }
}

static TIMINGS: RwLock<Option<LockTimings>> = RwLock::new(None);

/// Replace the lock timings for this process (tests that simulate a crashed
/// holder use millisecond leases). Not part of the stable API.
#[doc(hidden)]
pub fn set_lock_timings(timings: Option<LockTimings>) {
    *TIMINGS.write().unwrap_or_else(|p| p.into_inner()) = timings;
}

pub(crate) fn timings() -> LockTimings {
    let set = *TIMINGS.read().unwrap_or_else(|p| p.into_inner());
    #[cfg(test)]
    return set.unwrap_or_else(tests::fast);
    #[cfg(not(test))]
    set.unwrap_or_default()
}

/// Why the lock is taken (written into its body, for people reading it).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Purpose {
    Commit,
    Append,
}

impl Purpose {
    fn as_str(self) -> &'static str {
        match self {
            Self::Commit => "commit",
            Self::Append => "append",
        }
    }
}

/// What [`acquire`] came to.
pub(crate) enum Acquired {
    /// The lock is held until the guard is released.
    Held(LockGuard),
    /// The backend cannot coordinate writers; go on without a lock.
    Unsupported,
    /// Another writer held it for the whole wait.
    Busy,
}

fn epoch_of(name: &str) -> Option<u64> {
    name.strip_prefix(LOCK_PREFIX)?.parse().ok()
}

fn lock_name(epoch: u64) -> String {
    format!("{LOCK_PREFIX}{epoch:020}")
}

fn body(owner: &str, beat: u64, purpose: Purpose) -> String {
    let wall = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format!(
        "owner={owner} beat={beat} wall={wall} purpose={}",
        purpose.as_str()
    )
}

fn names_owner(text: &str, owner: &str) -> bool {
    text.split_whitespace()
        .any(|f| f.strip_prefix("owner=") == Some(owner))
}

/// Who is asking: the task, or the thread when not inside a task (a
/// runtime's `block_on`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Asker {
    Task(tokio::task::Id),
    Thread(std::thread::ThreadId),
}

fn asker() -> Asker {
    tokio::task::try_id().map_or_else(|| Asker::Thread(std::thread::current().id()), Asker::Task)
}

/// Locks this process holds, by output, with who holds each: the same
/// task asking for a lock it holds would wait for itself.
fn held() -> &'static Mutex<HashMap<String, Asker>> {
    static HELD: OnceLock<Mutex<HashMap<String, Asker>>> = OnceLock::new();
    HELD.get_or_init(Default::default)
}

fn warn_once(set: &'static OnceLock<Mutex<HashSet<String>>>, key: &str) -> bool {
    set.get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(key.to_string())
}

static UNSUPPORTED_WARNED: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
static PROBED: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();

/// Take the lock of the output `label` names, waiting up to `wait_max`
/// while another writer holds it.
pub(crate) async fn acquire(
    backend: &Arc<dyn StorageBackend>,
    label: &str,
    purpose: Purpose,
    wait_max: Duration,
    ctx: &CallContext,
) -> Result<Acquired, FaucetError> {
    {
        let held = held().lock().unwrap_or_else(|p| p.into_inner());
        if held.get(label) == Some(&asker()) {
            return Err(FaucetError::Sink(format!(
                "file sink: '{label}' is already locked by this same write; two sinks of one \
                 run write the same output"
            )));
        }
    }
    let t = timings();
    let owner = uuid::Uuid::new_v4().simple().to_string();
    let start = Instant::now();
    let mut seen: Option<(u64, String, Instant)> = None;
    let mut told = false;
    loop {
        let top = backend
            .list_side_files(LOCK_PREFIX)
            .await?
            .iter()
            .filter_map(|n| epoch_of(n))
            .max()
            .unwrap_or(0);
        let free = if top == 0 {
            true
        } else {
            match backend.read_side_file(&lock_name(top)).await? {
                None => continue_after_vanish(&mut seen),
                Some(text) if text.trim() == RELEASED => true,
                Some(text) => match &seen {
                    Some((e, b, since)) if *e == top && *b == text => {
                        if since.elapsed() >= t.lease {
                            tracing::warn!(
                                output = %label,
                                epoch = top,
                                "file sink: taking over the lock of a writer that stopped \
                                 renewing it"
                            );
                            true
                        } else {
                            false
                        }
                    }
                    _ => {
                        seen = Some((top, text, Instant::now()));
                        false
                    }
                },
            }
        };
        if free {
            let epoch = top + 1;
            let name = lock_name(epoch);
            let text = body(&owner, 0, purpose);
            let sent = (Instant::now(), SystemTime::now());
            let created = match backend.create_side_file(&name, &text).await {
                Ok(c) => c,
                Err(e) => match backend.read_side_file(&name).await {
                    Ok(Some(b)) if names_owner(&b, &owner) => SideCreate::Created,
                    _ => return Err(e),
                },
            };
            match created {
                SideCreate::Unsupported => {
                    if warn_once(&UNSUPPORTED_WARNED, label) {
                        tracing::warn!(
                            output = %label,
                            "file sink: this storage backend cannot coordinate concurrent \
                             writers of one output; run them one at a time"
                        );
                    }
                    return Ok(Acquired::Unsupported);
                }
                SideCreate::Exists => {
                    seen = None;
                    continue;
                }
                SideCreate::Created => {}
            }
            match backend.read_side_file(&name).await? {
                Some(b) if names_owner(&b, &owner) => {}
                _ => {
                    seen = None;
                    continue;
                }
            }
            if warn_once(&PROBED, label)
                && backend.create_side_file(&name, &text).await? == SideCreate::Created
            {
                tracing::warn!(
                    output = %label,
                    "file sink: this store does not honour conditional writes \
                     (If-None-Match), so concurrent writers of this output are not coordinated"
                );
            }
            for old in backend.list_side_files(LOCK_PREFIX).await? {
                if epoch_of(&old).is_some_and(|e| e < epoch) {
                    let _ = backend.delete_side_file(&old).await;
                }
            }
            return Ok(Acquired::Held(LockGuard::start(
                backend.clone(),
                label.to_string(),
                epoch,
                owner,
                purpose,
                sent,
                t,
                ctx.clone(),
            )));
        }
        if start.elapsed() >= wait_max {
            return Ok(Acquired::Busy);
        }
        if !told {
            told = true;
            tracing::info!(
                output = %label,
                epoch = top,
                "file sink: another writer holds this output; waiting"
            );
        }
        let left = wait_max.saturating_sub(start.elapsed());
        ctx.wait(t.poll.min(left).max(Duration::from_millis(1)))
            .await?;
    }
}

fn continue_after_vanish(seen: &mut Option<(u64, String, Instant)>) -> bool {
    *seen = None;
    false
}

struct Shared {
    lost: AtomicBool,
    /// When the last renewal that succeeded was sent, by both clocks.
    sent: Mutex<(Instant, SystemTime)>,
    beat: AtomicU64,
    fence: Duration,
}

impl Shared {
    fn fenced(&self) -> bool {
        if self.lost.load(Ordering::SeqCst) {
            return true;
        }
        let (mono, wall) = *self.sent.lock().unwrap_or_else(|p| p.into_inner());
        mono.elapsed() >= self.fence || wall.elapsed().is_ok_and(|d| d >= self.fence)
    }

    fn fence_at(&self) -> Instant {
        let (mono, wall) = *self.sent.lock().unwrap_or_else(|p| p.into_inner());
        let by_wall = self
            .fence
            .saturating_sub(wall.elapsed().unwrap_or_default());
        (mono + self.fence).min(Instant::now() + by_wall)
    }
}

/// A held output lock. Release it with [`release`](Self::release); a guard
/// dropped unreleased (a hard-dropped run) stops renewing and releases it in
/// the background.
pub(crate) struct LockGuard {
    backend: Arc<dyn StorageBackend>,
    label: String,
    name: String,
    epoch: u64,
    owner: String,
    shared: Arc<Shared>,
    renewal: Option<tokio::task::JoinHandle<()>>,
    done: bool,
}

impl LockGuard {
    #[allow(clippy::too_many_arguments)]
    fn start(
        backend: Arc<dyn StorageBackend>,
        label: String,
        epoch: u64,
        owner: String,
        purpose: Purpose,
        sent: (Instant, SystemTime),
        t: LockTimings,
        ctx: CallContext,
    ) -> Self {
        let shared = Arc::new(Shared {
            lost: AtomicBool::new(false),
            sent: Mutex::new(sent),
            beat: AtomicU64::new(0),
            fence: t.lease / 2,
        });
        let name = lock_name(epoch);
        held()
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(label.clone(), asker());
        let renewal = tokio::spawn(renew(
            backend.clone(),
            name.clone(),
            epoch,
            owner.clone(),
            purpose,
            shared.clone(),
            t.renew,
            ctx,
        ));
        Self {
            backend,
            label,
            name,
            epoch,
            owner,
            shared,
            renewal: Some(renewal),
            done: false,
        }
    }

    fn lost_error(&self) -> FaucetError {
        FaucetError::Sink(format!(
            "file sink: this run lost its hold on '{}' (it could not renew its lock in time, or \
             another writer took the lock over), so it stopped before changing more of it",
            self.label
        ))
    }

    /// Fail when the hold may have lapsed. No I/O.
    pub(crate) fn check(&self) -> Result<(), FaucetError> {
        if self.shared.fenced() {
            Err(self.lost_error())
        } else {
            Ok(())
        }
    }

    /// [`check`](Self::check), then read the lock back: it must still be the
    /// highest epoch and name this holder. Run before each destructive phase.
    pub(crate) async fn verify(&self) -> Result<(), FaucetError> {
        self.check()?;
        let top = self
            .backend
            .list_side_files(LOCK_PREFIX)
            .await?
            .iter()
            .filter_map(|n| epoch_of(n))
            .max();
        let body = self.backend.read_side_file(&self.name).await?;
        if top != Some(self.epoch) || !body.is_some_and(|b| names_owner(&b, &self.owner)) {
            self.shared.lost.store(true, Ordering::SeqCst);
            return Err(self.lost_error());
        }
        self.check()
    }

    /// Run `fut` only while the hold lasts: refused when it already lapsed,
    /// cut off at the instant it would.
    pub(crate) async fn fenced<T>(
        &self,
        fut: impl std::future::Future<Output = Result<T, FaucetError>>,
    ) -> Result<T, FaucetError> {
        self.check()?;
        let at = tokio::time::Instant::from_std(self.shared.fence_at());
        match tokio::time::timeout_at(at, fut).await {
            Ok(r) => r,
            Err(_) => {
                self.shared.lost.store(true, Ordering::SeqCst);
                Err(self.lost_error())
            }
        }
    }

    /// Stop renewing and mark the lock released.
    pub(crate) async fn release(mut self) {
        self.done = true;
        if let Some(t) = self.renewal.take() {
            t.abort();
        }
        forget(&self.label);
        if !self.shared.lost.load(Ordering::SeqCst)
            && let Err(e) = self.backend.write_side_file(&self.name, RELEASED).await
        {
            tracing::warn!(output = %self.label, error = %e, "file sink: could not release the output lock; the next writer takes it over once its lease runs out");
        }
    }

    #[cfg(test)]
    pub(crate) fn epoch(&self) -> u64 {
        self.epoch
    }
}

fn forget(label: &str) {
    held()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .remove(label);
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        if let Some(t) = self.renewal.take() {
            t.abort();
        }
        forget(&self.label);
        if self.shared.lost.load(Ordering::SeqCst) {
            return;
        }
        if let Ok(h) = tokio::runtime::Handle::try_current() {
            let (backend, name) = (self.backend.clone(), self.name.clone());
            h.spawn(async move {
                let _ = backend.write_side_file(&name, RELEASED).await;
            });
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn renew(
    backend: Arc<dyn StorageBackend>,
    name: String,
    epoch: u64,
    owner: String,
    purpose: Purpose,
    shared: Arc<Shared>,
    every: Duration,
    ctx: CallContext,
) {
    loop {
        if ctx.wait(every).await.is_err() {
            shared.lost.store(true, Ordering::SeqCst);
            return;
        }
        let sent = (Instant::now(), SystemTime::now());
        let still_ours = async {
            let top = backend
                .list_side_files(LOCK_PREFIX)
                .await?
                .iter()
                .filter_map(|n| epoch_of(n))
                .max();
            let body = backend.read_side_file(&name).await?;
            Ok::<_, FaucetError>(
                top == Some(epoch) && body.is_some_and(|b| names_owner(&b, &owner)),
            )
        };
        let beat = shared.beat.fetch_add(1, Ordering::SeqCst) + 1;
        let ok = match still_ours.await {
            Ok(true) => backend
                .write_side_file(&name, &body(&owner, beat, purpose))
                .await
                .is_ok(),
            _ => false,
        };
        if !ok {
            shared.lost.store(true, Ordering::SeqCst);
            return;
        }
        *shared.sent.lock().unwrap_or_else(|p| p.into_inner()) = sent;
    }
}

#[cfg(test)]
#[path = "lock_tests.rs"]
mod tests;
