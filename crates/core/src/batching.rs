//! Re-chunking and bounded concurrency for connector writes and reads
//! (RFC 0013, #880).
//!
//! Every sink re-chunks the page it is handed into backend-sized requests, and
//! many run those requests concurrently under a limit. [`chunks`] honours the
//! `batch_size` contract (`0` = no re-chunking, the whole page is one request)
//! and [`for_each_concurrent`] / [`map_concurrent`] run work with at most `n`
//! in flight, stopping at the first error.
//!
//! ```
//! # async fn ex() -> Result<(), faucet_core::FaucetError> {
//! use faucet_core::batching::{chunks, map_concurrent};
//!
//! let page: Vec<u32> = (0..10).collect();
//! let sums = map_concurrent(chunks(&page, 4), 2, |chunk| async move {
//!     Ok(chunk.iter().sum::<u32>())
//! })
//! .await?;
//! assert_eq!(sums, vec![6, 22, 17]);
//! # Ok(())
//! # }
//! ```

use crate::error::FaucetError;
use futures::{StreamExt, TryStreamExt};
use std::future::Future;

/// Split `items` into slices of at most `batch_size`. `batch_size == 0` means
/// "don't re-chunk": the whole slice is one chunk. An empty slice yields no
/// chunks.
pub fn chunks<T>(items: &[T], batch_size: usize) -> impl ExactSizeIterator<Item = &[T]> {
    let size = if batch_size == 0 {
        items.len().max(1)
    } else {
        batch_size
    };
    items.chunks(size)
}

/// Split an owned vector the same way as [`chunks`], moving the items.
pub fn into_chunks<T>(items: Vec<T>, batch_size: usize) -> Vec<Vec<T>> {
    if items.is_empty() {
        return Vec::new();
    }
    if batch_size == 0 || items.len() <= batch_size {
        return vec![items];
    }
    let mut out = Vec::with_capacity(items.len().div_ceil(batch_size));
    let mut iter = items.into_iter();
    loop {
        let chunk: Vec<T> = iter.by_ref().take(batch_size).collect();
        if chunk.is_empty() {
            return out;
        }
        out.push(chunk);
    }
}

/// Run `f` on every item with at most `limit` in flight (`0` is treated as
/// `1`). Stops at, and returns, the first error; work already in flight is
/// dropped.
pub async fn for_each_concurrent<I, F, Fut>(items: I, limit: usize, f: F) -> Result<(), FaucetError>
where
    I: IntoIterator,
    F: FnMut(I::Item) -> Fut,
    Fut: Future<Output = Result<(), FaucetError>>,
{
    futures::stream::iter(items)
        .map(f)
        .buffer_unordered(limit.max(1))
        .try_for_each(|()| async { Ok(()) })
        .await
}

/// Run `f` on every item with at most `limit` in flight (`0` is treated as
/// `1`) and return the results **in input order**. Stops at, and returns, the
/// first error.
pub async fn map_concurrent<I, F, Fut, T>(
    items: I,
    limit: usize,
    f: F,
) -> Result<Vec<T>, FaucetError>
where
    I: IntoIterator,
    F: FnMut(I::Item) -> Fut,
    Fut: Future<Output = Result<T, FaucetError>>,
{
    futures::stream::iter(items)
        .map(f)
        .buffered(limit.max(1))
        .try_collect()
        .await
}

/// Fetch-and-decode `items` ahead of the consumer, on tasks of their own, and
/// hand the results back **in input order**.
///
/// `f` turns each item into a future; the future is spawned on the runtime,
/// so it keeps running while the consumer of the returned stream is busy
/// elsewhere (writing the previous page, say). That is the difference from
/// [`futures::StreamExt::buffered`], whose futures advance only while the
/// stream is polled.
///
/// Guarantees:
///
/// - **Order.** Results are yielded strictly in the order of `items`,
///   whatever order the tasks finish in.
/// - **Bound.** At most `concurrency` items (`0` is treated as `1`) are
///   running or finished-but-not-yet-yielded at any time; the next item is
///   taken from `items` only when a result is yielded. Peak memory is
///   therefore `concurrency` results plus the one the consumer holds.
/// - **Errors.** The first error in input order is yielded in its place;
///   every later task is aborted and the stream ends. Results before it are
///   yielded first.
/// - **Panics.** A task that panics yields a [`FaucetError::Custom`] naming
///   the panic message, with the same consequences as an error.
/// - **Drop.** Dropping the stream aborts every task still running. An
///   aborted task stops at its next `.await`.
/// - **Context.** Each task runs in the span that was current when it was
///   spawned and carries the caller's reference policy
///   ([`inherit_reference_policy`](crate::http::inherit_reference_policy))
///   and non-finite counter
///   ([`NonFiniteCounter`](crate::fidelity::NonFiniteCounter)). Pacing,
///   rate-limit budgets and the cancel token belong inside the future (run
///   its requests through the connector's
///   [`CallSlot`](crate::io::CallSlot)).
///
/// Tasks are spawned from `poll_next`, so the stream must be polled inside a
/// Tokio runtime.
///
/// # When not to use it
///
/// Only for reads that are safe to perform before the previous result has
/// been written: object bodies, result partitions, pages addressed by a
/// known key. **Never** for a source whose read acknowledges, commits or
/// consumes: a message consumer that acks on the next poll, a webhook source
/// answering senders on commit, a feed delivery, a Kafka consumer in group
/// member mode. Reading ahead there acknowledges data that was not written.
/// For the same reason this is a tool for a source's own fetches; the
/// pipeline never polls a source ahead of the sink.
pub fn prefetch_ordered<I, F, Fut, T>(
    items: I,
    concurrency: usize,
    f: F,
) -> impl futures::Stream<Item = Result<T, FaucetError>> + Send + Unpin
where
    I: IntoIterator,
    I::IntoIter: Send,
    F: FnMut(I::Item) -> Fut + Send,
    Fut: Future<Output = Result<T, FaucetError>> + Send + 'static,
    T: Send + 'static,
{
    Prefetch {
        items: Some(items.into_iter()),
        f,
        limit: concurrency.max(1),
        tasks: std::collections::VecDeque::new(),
    }
}

struct Prefetch<I, F, T> {
    items: Option<I>,
    f: F,
    limit: usize,
    tasks: std::collections::VecDeque<tokio::task::JoinHandle<Result<T, FaucetError>>>,
}

impl<I, F, T> Unpin for Prefetch<I, F, T> {}

impl<I, F, Fut, T> Prefetch<I, F, T>
where
    I: Iterator,
    F: FnMut(I::Item) -> Fut,
    Fut: Future<Output = Result<T, FaucetError>> + Send + 'static,
    T: Send + 'static,
{
    fn fill(&mut self) {
        use tracing::Instrument;
        while self.tasks.len() < self.limit {
            let Some(item) = self.items.as_mut().and_then(Iterator::next) else {
                self.items = None;
                return;
            };
            let fut = crate::http::inherit_reference_policy((self.f)(item));
            let counter = crate::fidelity::NonFiniteCounter::current();
            let fut = async move {
                match counter {
                    Some(counter) => counter.scope(fut).await,
                    None => fut.await,
                }
            };
            self.tasks.push_back(tokio::spawn(fut.in_current_span()));
        }
    }

    fn stop(&mut self) {
        self.items = None;
        for task in self.tasks.drain(..) {
            task.abort();
        }
    }
}

impl<I, F, T> Drop for Prefetch<I, F, T> {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl<I, F, Fut, T> futures::Stream for Prefetch<I, F, T>
where
    I: Iterator,
    F: FnMut(I::Item) -> Fut,
    Fut: Future<Output = Result<T, FaucetError>> + Send + 'static,
    T: Send + 'static,
{
    type Item = Result<T, FaucetError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll;
        let this = &mut *self;
        this.fill();
        let Some(front) = this.tasks.front_mut() else {
            return Poll::Ready(None);
        };
        let joined = match std::pin::Pin::new(front).poll(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(joined) => joined,
        };
        this.tasks.pop_front();
        let result = match joined {
            Ok(result) => result,
            Err(e) if e.is_panic() => {
                let panic = e.into_panic();
                let msg = panic
                    .downcast_ref::<&'static str>()
                    .map(|s| (*s).to_string())
                    .or_else(|| panic.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "<non-string panic payload>".to_string());
                Err(FaucetError::Custom(
                    format!("panic in prefetch task: {msg}").into(),
                ))
            }
            Err(_) => Err(FaucetError::Cancelled {
                operation: "prefetch task".into(),
            }),
        };
        match &result {
            Ok(_) => this.fill(),
            Err(_) => this.stop(),
        }
        Poll::Ready(Some(result))
    }
}

/// A concurrency limit shared across calls: a semaphore with `n` permits
/// (`0` is treated as `1`). Cheap to clone; every clone shares the permits.
/// Use it when the work to bound is spread over separate calls (requests
/// spawned from several `write_batch` invocations); within one call prefer
/// [`map_concurrent`].
#[derive(Debug, Clone)]
pub struct ConcurrencyLimit {
    permits: std::sync::Arc<tokio::sync::Semaphore>,
    limit: usize,
}

impl ConcurrencyLimit {
    /// A limit of `n` concurrent runs.
    pub fn new(n: usize) -> Self {
        let limit = n.max(1);
        Self {
            permits: std::sync::Arc::new(tokio::sync::Semaphore::new(limit)),
            limit,
        }
    }

    /// The configured limit.
    pub fn limit(&self) -> usize {
        self.limit
    }

    /// Run `fut` once a permit is free.
    pub async fn run<T>(&self, fut: impl Future<Output = T>) -> T {
        let _permit = self
            .permits
            .acquire()
            .await
            .expect("the semaphore is never closed");
        fut.await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[test]
    fn chunks_honour_the_batch_size_contract() {
        let v: Vec<u8> = (0..5).collect();
        let sizes: Vec<usize> = chunks(&v, 2).map(<[u8]>::len).collect();
        assert_eq!(sizes, vec![2, 2, 1]);
        assert_eq!(chunks(&v, 0).len(), 1, "0 = one chunk");
        assert_eq!(chunks(&v, 0).next().unwrap().len(), 5);
        assert_eq!(chunks(&v, 9).len(), 1);
        assert_eq!(chunks::<u8>(&[], 0).len(), 0);
        assert_eq!(chunks::<u8>(&[], 3).len(), 0);
    }

    #[test]
    fn into_chunks_moves_items() {
        assert_eq!(
            into_chunks(vec![1, 2, 3, 4, 5], 2),
            vec![vec![1, 2], vec![3, 4], vec![5]]
        );
        assert_eq!(into_chunks(vec![1, 2, 3], 0), vec![vec![1, 2, 3]]);
        assert_eq!(into_chunks(vec![1, 2], 2), vec![vec![1, 2]]);
        assert!(into_chunks(Vec::<u8>::new(), 2).is_empty());
        assert_eq!(into_chunks(vec![1, 2, 3, 4], 2).len(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn concurrency_is_bounded_and_order_kept() {
        let live = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let out = map_concurrent(0..8u64, 3, |i| {
            let (live, peak) = (live.clone(), peak.clone());
            async move {
                let now = live.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                #[allow(clippy::disallowed_methods)]
                tokio::time::sleep(Duration::from_millis(10 * (8 - i))).await;
                live.fetch_sub(1, Ordering::SeqCst);
                Ok(i * 2)
            }
        })
        .await
        .unwrap();
        assert_eq!(out, (0..8).map(|i| i * 2).collect::<Vec<_>>());
        assert_eq!(peak.load(Ordering::SeqCst), 3);
        let done = Arc::new(AtomicUsize::new(0));
        let d = done.clone();
        for_each_concurrent(0..5, 0, move |_| {
            let d = d.clone();
            async move {
                d.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .await
        .unwrap();
        assert_eq!(done.load(Ordering::SeqCst), 5);
    }

    #[tokio::test(start_paused = true)]
    async fn a_shared_limit_bounds_separate_calls() {
        let limit = ConcurrencyLimit::new(0);
        assert_eq!(limit.limit(), 1);
        let limit = ConcurrencyLimit::new(2);
        let live = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..6 {
            let (limit, live, peak) = (limit.clone(), live.clone(), peak.clone());
            tasks.push(tokio::spawn(async move {
                limit
                    .run(async {
                        let now = live.fetch_add(1, Ordering::SeqCst) + 1;
                        peak.fetch_max(now, Ordering::SeqCst);
                        #[allow(clippy::disallowed_methods)]
                        tokio::time::sleep(Duration::from_millis(5)).await;
                        live.fetch_sub(1, Ordering::SeqCst);
                    })
                    .await
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        assert_eq!(peak.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn the_first_error_stops_the_run() {
        let r = for_each_concurrent(0..5, 2, |i| async move {
            if i == 2 {
                Err(FaucetError::Sink("boom".into()))
            } else {
                Ok(())
            }
        })
        .await;
        assert!(matches!(r, Err(FaucetError::Sink(m)) if m == "boom"));
        let r: Result<Vec<u8>, _> =
            map_concurrent(0..3u8, 1, |_| async { Err(FaucetError::Sink("x".into())) }).await;
        assert!(r.is_err());
    }

    fn gate() -> (
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    ) {
        tokio::sync::oneshot::channel()
    }

    #[tokio::test(start_paused = true)]
    async fn prefetch_keeps_input_order_whatever_order_tasks_finish_in() {
        let finished = Arc::new(std::sync::Mutex::new(Vec::new()));
        let log = finished.clone();
        let out: Vec<u64> = prefetch_ordered(0..9u64, 4, move |i| {
            let log = log.clone();
            async move {
                #[allow(clippy::disallowed_methods)]
                tokio::time::sleep(Duration::from_millis(10 * ((i * 7) % 5))).await;
                log.lock().unwrap().push(i);
                Ok(i * 10)
            }
        })
        .try_collect()
        .await
        .unwrap();
        assert_eq!(out, (0..9).map(|i| i * 10).collect::<Vec<_>>());
        let finished = finished.lock().unwrap().clone();
        assert_ne!(finished, (0..9).collect::<Vec<_>>(), "tasks overlapped");
        assert_eq!(finished.len(), 9);
    }

    #[tokio::test(start_paused = true)]
    async fn prefetch_runs_ahead_while_the_consumer_is_busy_but_never_past_the_bound() {
        let started = Arc::new(AtomicUsize::new(0));
        let done = Arc::new(AtomicUsize::new(0));
        let (s, d) = (started.clone(), done.clone());
        let mut stream = prefetch_ordered(0..10usize, 3, move |i| {
            let (s, d) = (s.clone(), d.clone());
            async move {
                s.fetch_add(1, Ordering::SeqCst);
                #[allow(clippy::disallowed_methods)]
                tokio::time::sleep(Duration::from_millis(5)).await;
                d.fetch_add(1, Ordering::SeqCst);
                Ok(i)
            }
        });
        assert_eq!(
            started.load(Ordering::SeqCst),
            0,
            "nothing runs before a poll"
        );
        assert_eq!(stream.next().await.unwrap().unwrap(), 0);
        #[allow(clippy::disallowed_methods)]
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(
            started.load(Ordering::SeqCst),
            4,
            "0 yielded, 1..=3 in the window"
        );
        assert_eq!(done.load(Ordering::SeqCst), 4, "they finished unpolled");
        for want in 1..10 {
            assert_eq!(stream.next().await.unwrap().unwrap(), want);
            #[allow(clippy::disallowed_methods)]
            tokio::time::sleep(Duration::from_secs(1)).await;
            let held = started.load(Ordering::SeqCst) - (want + 1);
            assert!(held <= 3, "{held} results held");
        }
        assert!(stream.next().await.is_none());
        assert!(stream.next().await.is_none());
        assert_eq!(started.load(Ordering::SeqCst), 10);
    }

    #[tokio::test(start_paused = true)]
    async fn prefetch_treats_zero_concurrency_as_one() {
        let live = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let (l, p) = (live.clone(), peak.clone());
        let out: Vec<u8> = prefetch_ordered(0..5u8, 0, move |i| {
            let (l, p) = (l.clone(), p.clone());
            async move {
                p.fetch_max(l.fetch_add(1, Ordering::SeqCst) + 1, Ordering::SeqCst);
                #[allow(clippy::disallowed_methods)]
                tokio::time::sleep(Duration::from_millis(1)).await;
                l.fetch_sub(1, Ordering::SeqCst);
                Ok(i)
            }
        })
        .try_collect()
        .await
        .unwrap();
        assert_eq!(out, vec![0, 1, 2, 3, 4]);
        assert_eq!(peak.load(Ordering::SeqCst), 1);
        let none: Vec<u8> = prefetch_ordered(Vec::<u8>::new(), 4, |i| async move { Ok(i) })
            .try_collect()
            .await
            .unwrap();
        assert!(none.is_empty());
    }

    struct SetOnDrop(Arc<AtomicUsize>);
    impl Drop for SetOnDrop {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn dropping_the_prefetch_stream_aborts_its_tasks() {
        let started = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicUsize::new(0));
        let finished = Arc::new(AtomicUsize::new(0));
        let (s, d, f) = (started.clone(), dropped.clone(), finished.clone());
        let mut stream = prefetch_ordered(0..6u8, 3, move |i| {
            let (s, d, f) = (s.clone(), d.clone(), f.clone());
            async move {
                let _guard = SetOnDrop(d);
                s.fetch_add(1, Ordering::SeqCst);
                if i > 0 {
                    std::future::pending::<()>().await;
                }
                f.fetch_add(1, Ordering::SeqCst);
                Ok(i)
            }
        });
        assert_eq!(stream.next().await.unwrap().unwrap(), 0);
        while started.load(Ordering::SeqCst) < 4 {
            tokio::task::yield_now().await;
        }
        drop(stream);
        while dropped.load(Ordering::SeqCst) < 4 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            started.load(Ordering::SeqCst),
            4,
            "items 4 and 5 never started"
        );
        assert_eq!(finished.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn prefetch_yields_the_first_error_in_order_then_stops_and_cancels_the_rest() {
        let dropped = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(AtomicUsize::new(0));
        let (release_first, first) = gate();
        let mut first = Some(first);
        let (d, s) = (dropped.clone(), started.clone());
        let mut stream = prefetch_ordered(0..8u8, 4, move |i| {
            let gate = if i == 0 { first.take() } else { None };
            let (d, s) = (d.clone(), s.clone());
            async move {
                let _guard = SetOnDrop(d);
                s.fetch_add(1, Ordering::SeqCst);
                match i {
                    0 => {
                        let _ = gate.expect("first item").await;
                        Ok(i)
                    }
                    1 => Err(FaucetError::Source("item 1 failed".into())),
                    2 => Err(FaucetError::Source("item 2 failed".into())),
                    _ => std::future::pending().await,
                }
            }
        });
        let mut next = stream.next();
        assert!(futures::poll!(&mut next).is_pending(), "item 0 is not done");
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert!(
            futures::poll!(&mut next).is_pending(),
            "a later error does not overtake item 0"
        );
        release_first.send(()).unwrap();
        assert_eq!(next.await.unwrap().unwrap(), 0);
        let err = stream.next().await.unwrap().unwrap_err();
        assert!(
            matches!(&err, FaucetError::Source(m) if m == "item 1 failed"),
            "{err}"
        );
        assert!(stream.next().await.is_none());
        assert!(stream.next().await.is_none());
        let started_now = started.load(Ordering::SeqCst);
        assert!(started_now <= 5, "items past the window never started");
        while dropped.load(Ordering::SeqCst) < started_now {
            tokio::task::yield_now().await;
        }
        assert_eq!(started.load(Ordering::SeqCst), started_now);
    }

    #[tokio::test]
    async fn a_panicking_prefetch_task_is_a_typed_error_not_a_hang() {
        for owned in [false, true] {
            let mut stream = prefetch_ordered(0..4u8, 2, move |i| async move {
                if i == 1 && owned {
                    panic!("{}", format!("boom {i}"));
                } else if i == 1 {
                    panic!("static boom");
                }
                Ok(i)
            });
            assert_eq!(stream.next().await.unwrap().unwrap(), 0);
            let err = stream.next().await.unwrap().unwrap_err();
            let want = if owned { "boom 1" } else { "static boom" };
            assert_eq!(
                err.to_string(),
                FaucetError::Custom(format!("panic in prefetch task: {want}").into()).to_string()
            );
            assert!(matches!(err, FaucetError::Custom(_)));
            assert!(stream.next().await.is_none());
        }
        let mut stream = prefetch_ordered(0..2u8, 1, |i| async move {
            if i == 0 {
                std::panic::panic_any(7u8);
            }
            Ok(i)
        });
        let err = stream.next().await.unwrap().unwrap_err();
        assert!(
            err.to_string().contains("<non-string panic payload>"),
            "{err}"
        );
    }

    #[test]
    fn a_prefetch_task_lost_with_its_runtime_is_a_cancelled_error() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let mut stream = prefetch_ordered(0..3u8, 2, |i| async move {
            std::future::pending::<()>().await;
            Ok(i)
        });
        rt.block_on(async {
            assert!(futures::poll!(stream.next()).is_pending());
        });
        drop(rt);
        let err = futures::executor::block_on(stream.next())
            .unwrap()
            .unwrap_err();
        assert!(
            matches!(&err, FaucetError::Cancelled { operation } if operation == "prefetch task"),
            "{err}"
        );
        assert!(futures::executor::block_on(stream.next()).is_none());
    }

    #[tokio::test]
    async fn prefetch_tasks_carry_the_reference_policy_the_counter_and_the_span() {
        use tracing::Instrument;
        let policy = Arc::new(crate::resolve::ReferencePolicy::open());
        let counter = crate::fidelity::NonFiniteCounter::new();
        let span = tracing::info_span!("prefetch-test");
        let span_id = span.id();
        let seen = crate::http::with_reference_policy(
            policy.clone(),
            counter.scope(
                async {
                    prefetch_ordered(0..3u8, 2, |_| async {
                        crate::fidelity::record_non_finite();
                        Ok((
                            crate::http::current_reference_policy(),
                            crate::fidelity::NonFiniteCounter::current().is_some(),
                            tracing::Span::current().id(),
                        ))
                    })
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap()
                }
                .instrument(span),
            ),
        )
        .await;
        assert_eq!(seen.len(), 3);
        for (task_policy, has_counter, task_span) in seen {
            assert!(Arc::ptr_eq(&task_policy.expect("policy carried"), &policy));
            assert!(has_counter);
            assert_eq!(task_span, span_id);
        }
        assert_eq!(counter.count(), 3);
        let bare = prefetch_ordered(0..1u8, 1, |_| async {
            Ok((
                crate::http::current_reference_policy().is_none(),
                crate::fidelity::NonFiniteCounter::current().is_none(),
            ))
        })
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
        assert_eq!(bare, vec![(true, true)]);
    }

    #[tokio::test(start_paused = true)]
    async fn prefetch_overlaps_fetching_with_a_slow_consumer() {
        let begun = tokio::time::Instant::now();
        let mut stream = prefetch_ordered(0..6u8, 2, |i| async move {
            #[allow(clippy::disallowed_methods)]
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(i)
        });
        let mut got = Vec::new();
        while let Some(item) = stream.next().await {
            got.push(item.unwrap());
            #[allow(clippy::disallowed_methods)]
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(got, vec![0, 1, 2, 3, 4, 5]);
        assert_eq!(begun.elapsed(), Duration::from_millis(700));
    }
}
