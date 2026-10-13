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
}
