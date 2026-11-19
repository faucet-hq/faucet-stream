#![allow(clippy::disallowed_methods)] // tests drive time directly

use super::*;
use crate::write::backend::Area;
use crate::write::layout::NameTemplate;
use crate::write::local::LocalBackend;
use crate::write::remote::RemoteBackend;
use crate::write::remote::tests::Mem;
use std::sync::atomic::Ordering::SeqCst;

/// The timings every unit test of the crate runs with.
pub(crate) fn fast() -> LockTimings {
    LockTimings {
        lease: Duration::from_millis(400),
        renew: Duration::from_millis(40),
        poll: Duration::from_millis(10),
        wait_max: Duration::from_secs(10),
        abort_wait: Duration::from_millis(300),
    }
}

fn template() -> NameTemplate {
    NameTemplate {
        name: "out.jsonl".into(),
    }
}

fn remote(mem: &Arc<Mem>) -> Arc<dyn StorageBackend> {
    Arc::new(RemoteBackend::new(mem.clone(), "a/", &template(), None).unwrap())
}

const LOCKS: &str = "a/.faucet-overwrite-out.jsonl.commits/";

async fn take(b: &Arc<dyn StorageBackend>, label: &str, wait: Duration) -> Acquired {
    acquire(b, label, Purpose::Commit, wait, &CallContext::new())
        .await
        .unwrap()
}

fn held_guard(a: Acquired) -> LockGuard {
    match a {
        Acquired::Held(g) => g,
        Acquired::Unsupported => panic!("unsupported"),
        Acquired::Busy => panic!("busy"),
    }
}

#[tokio::test]
async fn an_empty_output_is_locked_and_released_with_growing_epochs() {
    let mem = Arc::new(Mem::default());
    let b = remote(&mem);
    let g = held_guard(take(&b, "t1", Duration::from_secs(1)).await);
    assert_eq!(g.epoch(), 1);
    g.verify().await.unwrap();
    g.release().await;
    assert_eq!(mem.text(&format!("{LOCKS}{}", lock_name(1))), RELEASED);
    let g = held_guard(take(&b, "t1", Duration::from_secs(1)).await);
    assert_eq!(g.epoch(), 2, "a released lock is taken at the next epoch");
    g.release().await;
    let locks: Vec<String> = mem
        .keys()
        .into_iter()
        .filter(|k| k.contains("lock-"))
        .collect();
    assert_eq!(
        locks,
        vec![format!("{LOCKS}{}", lock_name(2))],
        "older epochs are deleted"
    );
}

#[tokio::test]
async fn a_second_writer_waits_while_the_holder_renews() {
    let mem = Arc::new(Mem::default());
    let b = remote(&mem);
    let g = held_guard(take(&b, "t2", Duration::from_secs(1)).await);
    let b1 = b.clone();
    let busy = tokio::spawn(async move { take(&b1, "t2", fast().lease * 2).await })
        .await
        .unwrap();
    assert!(
        matches!(busy, Acquired::Busy),
        "a renewed lock is never taken over, however long the wait"
    );
    let b2 = b.clone();
    let waiter = tokio::spawn(async move { take(&b2, "t2", Duration::from_secs(5)).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!waiter.is_finished());
    g.release().await;
    let g2 = held_guard(waiter.await.unwrap());
    assert_eq!(g2.epoch(), 2);
    g2.release().await;
}

#[tokio::test]
async fn a_crashed_holders_lock_is_taken_over_after_the_lease() {
    let mem = Arc::new(Mem::default());
    let b = remote(&mem);
    b.write_side_file(&lock_name(4), "owner=dead beat=3 wall=1 purpose=commit")
        .await
        .unwrap();
    let started = Instant::now();
    let g = held_guard(take(&b, "t3", Duration::from_secs(5)).await);
    assert!(started.elapsed() >= fast().lease, "waited out the lease");
    assert_eq!(g.epoch(), 5);
    g.release().await;
}

#[tokio::test]
async fn an_empty_lock_body_counts_as_alive() {
    let mem = Arc::new(Mem::default());
    let b = remote(&mem);
    b.write_side_file(&lock_name(1), "").await.unwrap();
    assert!(matches!(
        take(&b, "t4", fast().lease / 2).await,
        Acquired::Busy
    ));
    let g = held_guard(take(&b, "t4", Duration::from_secs(5)).await);
    assert_eq!(g.epoch(), 2);
    g.release().await;
}

#[tokio::test]
async fn racing_takers_get_one_epoch_each_never_the_same() {
    let mem = Arc::new(Mem::default());
    let b = remote(&mem);
    b.write_side_file(&lock_name(1), "owner=dead beat=0 wall=1 purpose=commit")
        .await
        .unwrap();
    let (a, c) = tokio::join!(
        take(&b, "t5a", Duration::from_secs(5)),
        take(&b, "t5b", Duration::from_millis(900)),
    );
    let (winner, other) = match (a, c) {
        (Acquired::Held(g), other) | (other, Acquired::Held(g)) => (g, other),
        _ => panic!("one taker wins"),
    };
    assert_eq!(winner.epoch(), 2);
    assert!(
        matches!(other, Acquired::Busy),
        "the other waits behind the winner"
    );
    winner.release().await;
}

#[tokio::test]
async fn a_holder_that_cannot_renew_fences_itself() {
    let mem = Arc::new(Mem::default());
    let b = remote(&mem);
    let g = held_guard(take(&b, "t6", Duration::from_secs(1)).await);
    g.check().unwrap();
    mem.fail_side_writes.store(true, SeqCst);
    tokio::time::sleep(fast().renew * 3).await;
    let e = g.check().unwrap_err();
    assert!(e.to_string().contains("lost its hold"), "{e}");
    mem.fail_side_writes.store(false, SeqCst);
    assert!(g.fenced(async { Ok(()) }).await.is_err());
    g.release().await;
    let body = mem.text(&format!("{LOCKS}{}", lock_name(1)));
    assert_ne!(
        body, RELEASED,
        "a lost lock is not marked released by its former holder"
    );
}

#[tokio::test]
async fn a_paused_holder_is_fenced_by_either_clock_and_its_calls_cut_off() {
    let mem = Arc::new(Mem::default());
    let b = remote(&mem);
    let g = held_guard(take(&b, "t7", Duration::from_secs(1)).await);
    if let Some(t) = &g.renewal {
        t.abort();
    }
    let fence = fast().lease / 2;
    let past_wall = SystemTime::now() - fence * 2;
    *g.shared.sent.lock().unwrap() = (Instant::now(), past_wall);
    assert!(
        g.check().is_err(),
        "a suspended host's wall clock fences it"
    );
    *g.shared.sent.lock().unwrap() = (Instant::now(), SystemTime::now());
    g.check().unwrap();
    let slow = g
        .fenced(async {
            tokio::time::sleep(fence * 3).await;
            Ok(())
        })
        .await;
    assert!(
        slow.is_err(),
        "a call still running at the fence is cut off"
    );
    assert!(g.check().is_err());
    g.release().await;
}

#[tokio::test]
async fn verify_sees_a_takeover() {
    let mem = Arc::new(Mem::default());
    let b = remote(&mem);
    let g = held_guard(take(&b, "t8", Duration::from_secs(1)).await);
    b.create_side_file(&lock_name(2), "owner=other beat=0 wall=1 purpose=commit")
        .await
        .unwrap();
    assert!(g.verify().await.is_err());
    assert!(g.check().is_err());
    g.release().await;
    assert_ne!(
        mem.text(&format!("{LOCKS}{}", lock_name(1))),
        RELEASED,
        "the taken-over epoch is left alone"
    );
}

#[tokio::test]
async fn the_same_task_asking_twice_fails_fast() {
    let mem = Arc::new(Mem::default());
    let b = remote(&mem);
    let g = held_guard(take(&b, "t9", Duration::from_secs(1)).await);
    let e = match acquire(
        &b,
        "t9",
        Purpose::Append,
        Duration::from_secs(5),
        &CallContext::new(),
    )
    .await
    {
        Err(e) => e,
        Ok(_) => panic!("a nested acquire must fail"),
    };
    assert!(e.to_string().contains("already locked"), "{e}");
    g.release().await;
}

#[tokio::test]
async fn backends_without_conditional_create_are_unsupported() {
    let mem = Arc::new(Mem::default());
    mem.no_create_new.store(true, SeqCst);
    let b = remote(&mem);
    assert!(matches!(
        take(&b, "t10", Duration::from_secs(1)).await,
        Acquired::Unsupported
    ));
    struct Plain;
    #[faucet_core::async_trait]
    impl StorageBackend for Plain {
        fn describe(&self, _: Area, n: &str) -> String {
            n.into()
        }
        fn scratch_path(&self, _: Area, n: &str) -> std::path::PathBuf {
            n.into()
        }
        async fn prepare(&self, _: Area) -> Result<(), FaucetError> {
            Ok(())
        }
        async fn list(&self, _: Area) -> Result<Vec<String>, FaucetError> {
            Ok(vec![])
        }
        async fn exists(&self, _: Area, _: &str) -> Result<bool, FaucetError> {
            Ok(false)
        }
        async fn fetch(&self, _: Area, _: &str, _: &std::path::Path) -> Result<(), FaucetError> {
            Ok(())
        }
        async fn publish(&self, _: &std::path::Path, _: Area, _: &str) -> Result<(), FaucetError> {
            Ok(())
        }
        async fn delete(&self, _: Area, _: &str) -> Result<(), FaucetError> {
            Ok(())
        }
        async fn promote(&self, _: &str) -> Result<(), FaucetError> {
            Ok(())
        }
    }
    let plain: Arc<dyn StorageBackend> = Arc::new(Plain);
    assert!(matches!(
        take(&plain, "t10b", Duration::from_secs(1)).await,
        Acquired::Unsupported
    ));
    assert_eq!(plain.read_side_file("x").await.unwrap(), None);
    plain.write_side_file("x", "y").await.unwrap();
    plain.delete_side_file("x").await.unwrap();
    assert!(plain.list_side_files("").await.unwrap().is_empty());
}

#[tokio::test]
async fn a_store_ignoring_the_precondition_is_warned_about_and_still_locked() {
    let mem = Arc::new(Mem::default());
    mem.ignores_precondition.store(true, SeqCst);
    let b = remote(&mem);
    let g = held_guard(take(&b, "t11", Duration::from_secs(1)).await);
    assert_eq!(g.epoch(), 1);
    g.release().await;
}

#[tokio::test]
async fn an_ambiguous_create_is_read_back_not_assumed() {
    let mem = Arc::new(Mem::default());
    mem.fail_create.store(true, SeqCst);
    let b = remote(&mem);
    let e = match acquire(
        &b,
        "t12",
        Purpose::Commit,
        Duration::from_secs(1),
        &CallContext::new(),
    )
    .await
    {
        Err(e) => e,
        Ok(_) => panic!("a failed create that left nothing is an error"),
    };
    assert!(e.to_string().contains("timed out"), "{e}");
}

#[tokio::test]
async fn a_cancelled_wait_ends_the_acquire() {
    let mem = Arc::new(Mem::default());
    let b = remote(&mem);
    let g = held_guard(take(&b, "t13", Duration::from_secs(1)).await);
    let cancel = faucet_core::CancellationToken::new();
    let ctx = CallContext::new().with_cancel(cancel.clone());
    let b2 = b.clone();
    let waiter = tokio::spawn(async move {
        acquire(&b2, "t13", Purpose::Commit, Duration::from_secs(5), &ctx)
            .await
            .map(|_| ())
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    cancel.cancel();
    assert!(waiter.await.unwrap().is_err());
    g.release().await;
}

#[tokio::test]
async fn a_dropped_guard_releases_in_the_background() {
    let mem = Arc::new(Mem::default());
    let b = remote(&mem);
    let g = held_guard(take(&b, "t14", Duration::from_secs(1)).await);
    drop(g);
    let g = held_guard(take(&b, "t14", Duration::from_millis(300)).await);
    assert_eq!(g.epoch(), 2, "taken without waiting out the lease");
    g.release().await;
}

#[tokio::test]
async fn a_local_output_is_locked_with_exclusive_creates() {
    let dir = tempfile::tempdir().unwrap();
    let b: Arc<dyn StorageBackend> = Arc::new(LocalBackend::new(
        dir.path().to_str().unwrap(),
        &template(),
        true,
    ));
    let g = held_guard(take(&b, "t15", Duration::from_secs(1)).await);
    let side = dir.path().join(".faucet-overwrite-out.jsonl.commits");
    assert!(side.join(lock_name(1)).is_file());
    assert_eq!(
        b.create_side_file(&lock_name(1), "x").await.unwrap(),
        SideCreate::Exists
    );
    let busy = take(&b, "t15b", Duration::from_millis(50)).await;
    assert!(matches!(busy, Acquired::Busy));
    g.release().await;
    let crashed = std::fs::read_to_string(side.join(lock_name(1))).unwrap();
    assert_eq!(crashed, RELEASED);
    std::fs::write(
        side.join(lock_name(2)),
        "owner=dead beat=1 wall=1 purpose=append",
    )
    .unwrap();
    let g = held_guard(take(&b, "t15", Duration::from_secs(5)).await);
    assert_eq!(g.epoch(), 3);
    g.release().await;
    assert_eq!(
        b.list_side_files(LOCK_PREFIX).await.unwrap(),
        vec![lock_name(3)]
    );
}
