//! Broker lease extension for queue sources (#789 MSG-09).
//!
//! A queue source that acks a page only after the sink holds it keeps every
//! in-flight message on a broker lease (an SQS visibility timeout, a Pub/Sub
//! ack deadline, a JetStream `ack_wait`). Assembling a page and writing it can
//! outlast that lease, after which the broker redelivers the message into the
//! same run — a duplicate, and for SQS a stale receipt handle whose delete
//! fails. [`LeaseExtender`] runs one background task that periodically renews
//! the lease of whatever the source currently holds.
//!
//! The source owns the set: it calls [`LeaseExtender::hold`] whenever the set
//! of un-acked messages changes (after a receive, after an ack). The extender
//! never acks or nacks — it only renews — so dropping it (the stream ending or
//! being dropped) simply stops renewing, leaving the broker to redeliver.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Periodically renews the lease of the messages a queue source holds.
///
/// The renewal task is aborted when the extender is dropped.
pub struct LeaseExtender<T> {
    held: Arc<Mutex<Vec<T>>>,
    task: tokio::task::JoinHandle<()>,
}

impl<T: Clone + Send + 'static> LeaseExtender<T> {
    /// Spawn the renewal task: every `every`, `renew` is called with a
    /// snapshot of the held messages (skipped while none are held).
    pub fn spawn<F, Fut>(every: Duration, renew: F) -> Self
    where
        F: Fn(Vec<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send,
    {
        let held: Arc<Mutex<Vec<T>>> = Arc::new(Mutex::new(Vec::new()));
        let shared = Arc::clone(&held);
        let task = tokio::spawn(async move {
            let mut tick = tokio::time::interval(every);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            tick.tick().await;
            loop {
                tick.tick().await;
                let snapshot = match shared.lock() {
                    Ok(g) => g.clone(),
                    Err(_) => return,
                };
                if !snapshot.is_empty() {
                    renew(snapshot).await;
                }
            }
        });
        Self { held, task }
    }

    /// Replace the set of held messages.
    pub fn hold(&self, items: Vec<T>) {
        if let Ok(mut g) = self.held.lock() {
            *g = items;
        }
    }

    /// The number of messages currently held.
    pub fn held(&self) -> usize {
        self.held.lock().map(|g| g.len()).unwrap_or(0)
    }
}

impl<T> Drop for LeaseExtender<T> {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test(start_paused = true)]
    async fn renews_only_while_messages_are_held_and_stops_on_drop() {
        let calls = Arc::new(Mutex::new(Vec::<Vec<u32>>::new()));
        let seen = Arc::clone(&calls);
        let extender = LeaseExtender::spawn(Duration::from_secs(5), move |items: Vec<u32>| {
            let seen = Arc::clone(&seen);
            async move {
                seen.lock().unwrap().push(items);
            }
        });

        tokio::time::sleep(Duration::from_secs(11)).await;
        assert!(
            calls.lock().unwrap().is_empty(),
            "nothing held, nothing renewed"
        );

        extender.hold(vec![1, 2]);
        assert_eq!(extender.held(), 2);
        tokio::time::sleep(Duration::from_secs(5)).await;
        extender.hold(vec![2]);
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert_eq!(*calls.lock().unwrap(), vec![vec![1, 2], vec![2]]);

        drop(extender);
        tokio::time::sleep(Duration::from_secs(30)).await;
        assert_eq!(calls.lock().unwrap().len(), 2, "no renewal after drop");
    }

    #[tokio::test(start_paused = true)]
    async fn an_empty_hold_pauses_renewal() {
        let count = Arc::new(AtomicUsize::new(0));
        let c = Arc::clone(&count);
        let extender = LeaseExtender::spawn(Duration::from_secs(1), move |_: Vec<u8>| {
            let c = Arc::clone(&c);
            async move {
                c.fetch_add(1, Ordering::SeqCst);
            }
        });
        extender.hold(vec![7]);
        tokio::time::sleep(Duration::from_millis(1500)).await;
        extender.hold(Vec::new());
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }
}
