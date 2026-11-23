//! Background delivery of notifications.
//!
//! A run hands its events to [`dispatch`] and moves on: delivery happens on a
//! task, so a slow or dead channel never holds a run (or its serve slot).
//! Batches of one lane — a tenant's pipeline row, the incident key — are
//! delivered in the order they were dispatched, so a failure trigger always
//! precedes the success that resolves it. The queue is bounded per lane and in
//! total, each batch has a deadline, and [`flush`] waits for what is pending
//! (a short-lived process calls it before exiting).

use super::event::NotifyEvent;
use super::{Notifier, metrics};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;
use tokio::sync::{Notify, oneshot};
use tracing::Instrument;

/// Batches queued or being delivered, in total.
pub const MAX_PENDING_BATCHES: usize = 1024;
/// Batches queued or being delivered on one lane: a broken channel on one
/// frequently-run row cannot take the whole queue.
pub const MAX_LANE_BATCHES: usize = 16;
/// How long one batch may spend delivering; events left after it are dropped.
pub const BATCH_DEADLINE: Duration = Duration::from_secs(120);
/// How long a process waits for pending notifications at exit.
pub const FLUSH_TIMEOUT: Duration = Duration::from_secs(30);
/// How long it waits after an interrupt (Ctrl-C).
pub const INTERRUPTED_FLUSH_TIMEOUT: Duration = Duration::from_secs(3);

/// The ordering key of a batch: `(tenant, pipeline, row)`.
pub type Lane = (Option<String>, String, String);

#[derive(Default)]
struct LaneState {
    /// Resolves when the newest batch of the lane is done.
    tail: Option<oneshot::Receiver<()>>,
    /// Batches of the lane queued or being delivered.
    queued: usize,
}

struct Queue {
    lanes: Mutex<HashMap<Lane, LaneState>>,
    pending: AtomicUsize,
    idle: Notify,
}

static QUEUE: LazyLock<Queue> = LazyLock::new(|| Queue {
    lanes: Mutex::new(HashMap::new()),
    pending: AtomicUsize::new(0),
    idle: Notify::new(),
});

/// Deliver `events` through `notifier` on a background task, after every
/// earlier batch of `lane`. Returns `false` (and counts the drop) when the
/// lane or the whole queue is full. Must be called inside a Tokio runtime.
pub fn dispatch(notifier: &Arc<Notifier>, lane: Lane, events: Vec<NotifyEvent>) -> bool {
    dispatch_with(notifier, lane, events, BATCH_DEADLINE)
}

fn dispatch_with(
    notifier: &Arc<Notifier>,
    lane: Lane,
    events: Vec<NotifyEvent>,
    deadline: Duration,
) -> bool {
    if events.is_empty() {
        return true;
    }
    let q = &*QUEUE;
    let (done, prev) = {
        let mut lanes = q.lanes.lock().unwrap_or_else(|e| e.into_inner());
        let state = lanes.entry(lane.clone()).or_default();
        if state.queued >= MAX_LANE_BATCHES
            || q.pending.load(Ordering::SeqCst) >= MAX_PENDING_BATCHES
        {
            drop(lanes);
            metrics::record_dropped("*", "queue_full");
            tracing::warn!(
                pipeline = %lane.1,
                row = %lane.2,
                events = events.len(),
                "notification queue is full; dropping this run's notifications"
            );
            return false;
        }
        state.queued += 1;
        q.pending.fetch_add(1, Ordering::SeqCst);
        metrics::set_pending(q.pending.load(Ordering::SeqCst));
        let (tx, rx) = oneshot::channel();
        (tx, state.tail.replace(rx))
    };
    let notifier = Arc::clone(notifier);
    let release = Finish(lane);
    let task = async move {
        let _release = release;
        if let Some(prev) = prev {
            let _ = prev.await;
        }
        deliver(&notifier, events, deadline).await;
        drop(done);
    };
    tokio::spawn(faucet_core::http::inherit_reference_policy(task).in_current_span());
    true
}

async fn deliver(notifier: &Notifier, events: Vec<NotifyEvent>, deadline: Duration) {
    let until = tokio::time::Instant::now() + deadline;
    let mut events = events.into_iter();
    while let Some(event) = events.next() {
        if tokio::time::timeout_at(until, notifier.emit(event))
            .await
            .is_err()
        {
            let left = 1 + events.len();
            for _ in 0..left {
                metrics::record_dropped("*", "deadline");
            }
            tracing::warn!(
                events = left,
                "notification batch passed its delivery deadline; dropping the rest"
            );
            return;
        }
    }
}

/// Releases a batch's lane and queue slot when its task ends, panics or is
/// dropped.
struct Finish(Lane);

impl Drop for Finish {
    fn drop(&mut self) {
        finish(&self.0);
    }
}

fn finish(lane: &Lane) {
    let q = &*QUEUE;
    {
        let mut lanes = q.lanes.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(state) = lanes.get_mut(lane) {
            state.queued = state.queued.saturating_sub(1);
            if state.queued == 0 {
                lanes.remove(lane);
            }
        }
    }
    let left = q.pending.fetch_sub(1, Ordering::SeqCst).saturating_sub(1);
    metrics::set_pending(left);
    q.idle.notify_waiters();
}

/// Batches queued or being delivered.
pub fn pending() -> usize {
    QUEUE.pending.load(Ordering::SeqCst)
}

/// Wait until every pending batch is delivered, at most `timeout`. Returns
/// the number still pending (and logs a warning) when the time runs out.
pub async fn flush(timeout: Duration) -> usize {
    let until = tokio::time::Instant::now() + timeout;
    loop {
        let idle = QUEUE.idle.notified();
        let left = pending();
        if left == 0 {
            return 0;
        }
        if tokio::time::timeout_at(until, idle).await.is_err() {
            let left = pending();
            if left > 0 {
                tracing::warn!(
                    batches = left,
                    "exiting with notifications still being delivered; they are dropped"
                );
            }
            return left;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::notify::{ChannelSpec, NotificationSpec, WebhookConfig};
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    async fn webhook_notifier(server: &MockServer) -> Arc<Notifier> {
        let spec: NotificationSpec = serde_json::from_value(serde_json::json!({
            "name": "hook",
            "on": ["run_failure", "run_success"],
            "channel": {"type": "webhook", "config": {"url": format!("{}/hook", server.uri())}}
        }))
        .unwrap();
        assert!(matches!(
            spec.channel,
            ChannelSpec::Webhook(WebhookConfig { .. })
        ));
        Notifier::from_specs(&[spec]).unwrap().unwrap()
    }

    fn lane(row: &str) -> Lane {
        (None, "queue-test".into(), row.into())
    }

    fn events(server_row: &str, n: usize) -> Vec<NotifyEvent> {
        (0..n)
            .map(|i| {
                NotifyEvent::run_failure("queue-test", server_row, "sink", format!("boom {i}"))
            })
            .collect()
    }

    #[tokio::test]
    async fn batches_of_one_lane_arrive_in_dispatch_order() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(200)))
            .mount(&server)
            .await;
        let notifier = webhook_notifier(&server).await;
        assert!(dispatch(
            &notifier,
            lane("ordered"),
            vec![NotifyEvent::run_failure(
                "queue-test",
                "ordered",
                "sink",
                "first"
            )]
        ));
        assert!(dispatch(
            &notifier,
            lane("ordered"),
            vec![NotifyEvent::run_success("queue-test", "ordered", 1)]
        ));
        assert_eq!(flush(Duration::from_secs(10)).await, 0);
        let got: Vec<String> = server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .map(|r| {
                serde_json::from_slice::<serde_json::Value>(&r.body).unwrap()["event"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string()
            })
            .collect();
        assert_eq!(got, ["run_failure", "run_success"]);
    }

    #[tokio::test]
    async fn dispatch_returns_before_delivery_and_flush_waits_for_it() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(300)))
            .mount(&server)
            .await;
        let notifier = webhook_notifier(&server).await;
        let started = std::time::Instant::now();
        assert!(dispatch(&notifier, lane("bg"), events("bg", 1)));
        assert!(started.elapsed() < Duration::from_millis(200));
        assert_eq!(flush(Duration::from_secs(10)).await, 0);
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_full_lane_drops_new_batches() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(500)))
            .mount(&server)
            .await;
        let notifier = webhook_notifier(&server).await;
        let accepted = (0..MAX_LANE_BATCHES + 3)
            .filter(|_| dispatch(&notifier, lane("full"), events("full", 1)))
            .count();
        assert_eq!(accepted, MAX_LANE_BATCHES);
        assert!(dispatch(&notifier, lane("other"), events("other", 1)));
        flush(Duration::from_secs(30)).await;
    }

    #[tokio::test]
    async fn a_batch_past_its_deadline_drops_the_rest() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(400)))
            .mount(&server)
            .await;
        let notifier = webhook_notifier(&server).await;
        assert!(dispatch_with(
            &notifier,
            lane("deadline"),
            events("deadline", 3),
            Duration::from_millis(100)
        ));
        assert_eq!(flush(Duration::from_secs(10)).await, 0);
        assert!(server.received_requests().await.unwrap().len() <= 1);
    }

    fn lane_is_free(l: &Lane) -> bool {
        !QUEUE
            .lanes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains_key(l)
    }

    #[tokio::test]
    async fn a_batch_task_that_panics_frees_its_lane() {
        let l = lane("panics");
        QUEUE
            .lanes
            .lock()
            .unwrap()
            .entry(l.clone())
            .or_default()
            .queued += 1;
        QUEUE.pending.fetch_add(1, Ordering::SeqCst);
        let release = Finish(l.clone());
        let joined = tokio::spawn(async move {
            let _release = release;
            panic!("delivery failed");
        })
        .await;
        assert!(joined.is_err());
        assert!(lane_is_free(&l));
    }

    #[test]
    #[allow(clippy::disallowed_methods)] // lets the batch task start before the runtime goes
    fn a_batch_dropped_with_its_runtime_frees_its_lane() {
        let host = tokio::runtime::Runtime::new().unwrap();
        let (server, notifier) = host.block_on(async {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(5)))
                .mount(&server)
                .await;
            let notifier = webhook_notifier(&server).await;
            (server, notifier)
        });
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            assert!(dispatch(&notifier, lane("dropped"), events("dropped", 1)));
            tokio::time::sleep(Duration::from_millis(50)).await;
        });
        assert!(!lane_is_free(&lane("dropped")));
        drop(rt);
        assert!(lane_is_free(&lane("dropped")));
        host.block_on(async move { drop(server) });
    }

    #[tokio::test]
    async fn flush_gives_up_after_its_timeout() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(2)))
            .mount(&server)
            .await;
        let notifier = webhook_notifier(&server).await;
        assert!(dispatch(&notifier, lane("slow"), events("slow", 1)));
        assert!(flush(Duration::from_millis(50)).await >= 1);
        flush(Duration::from_secs(10)).await;
    }
}
