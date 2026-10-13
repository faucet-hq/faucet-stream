//! Running Redshift round trips through a [`CallContext`] (#880).
//!
//! A `sqlx` query future borrows its connection pool, and a row cursor is
//! polled by `&mut`, so neither can be rebuilt for a retry. [`once`] runs one
//! such future as a single call; [`next_rows`] pulls up to a page of rows from
//! a cursor as one call, so a stalled cursor ends at the run's deadline or
//! cancel without paying the call's overhead on every row.

use faucet_core::FaucetError;
use faucet_core::io::CallContext;
use futures::{Stream, TryStreamExt};
use std::future::Future;

/// Run `fut` as the single call `op` through `calls`.
pub async fn once<T, Fut>(calls: &CallContext, op: &'static str, fut: Fut) -> Result<T, FaucetError>
where
    Fut: Future<Output = Result<T, FaucetError>>,
{
    calls.call_once(op, Box::pin(fut)).await
}

/// Up to `max` rows from `rows`, read as one call `op` through `calls`. Fewer
/// than `max` rows (possibly none) means the cursor is exhausted.
pub async fn next_rows<S, T>(
    calls: &CallContext,
    op: &'static str,
    rows: &mut S,
    max: usize,
    map_err: fn(sqlx::Error) -> FaucetError,
) -> Result<Vec<T>, FaucetError>
where
    S: Stream<Item = Result<T, sqlx::Error>> + Unpin,
{
    once(calls, op, async {
        let mut out = Vec::new();
        while out.len() < max {
            match rows.try_next().await.map_err(map_err)? {
                Some(row) => out.push(row),
                None => break,
            }
        }
        Ok(out)
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn err(e: sqlx::Error) -> FaucetError {
        FaucetError::Source(e.to_string())
    }

    #[tokio::test]
    async fn once_never_resends_under_a_retrying_context() {
        let fast = Duration::from_millis(1);
        let ctx = CallContext::new().with_retry(faucet_core::RetryPolicy {
            max_attempts: 2,
            base: fast,
            max: fast,
            jitter: false,
            ..Default::default()
        });
        let attempts = std::sync::atomic::AtomicUsize::new(0);
        let e = once::<(), _>(&ctx, "query", async {
            attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Err(FaucetError::RateLimited(fast))
        })
        .await
        .expect_err("the one attempt fails");
        assert!(matches!(e, FaucetError::RateLimited(_)), "{e:?}");
        assert_eq!(attempts.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn pages_a_cursor_and_surfaces_its_errors() {
        let mut rows = futures::stream::iter(vec![Ok(1), Ok(2), Ok(3)]);
        let ctx = CallContext::new();
        assert_eq!(
            next_rows(&ctx, "fetch", &mut rows, 2, err).await.unwrap(),
            [1, 2]
        );
        assert_eq!(
            next_rows(&ctx, "fetch", &mut rows, 2, err).await.unwrap(),
            [3]
        );
        assert!(
            next_rows(&ctx, "fetch", &mut rows, 2, err)
                .await
                .unwrap()
                .is_empty()
        );

        let mut bad = futures::stream::iter(vec![Ok(1), Err(sqlx::Error::RowNotFound)]);
        let e = next_rows(&ctx, "fetch", &mut bad, 5, err)
            .await
            .unwrap_err();
        assert!(matches!(e, FaucetError::Source(_)), "{e:?}");
    }

    #[tokio::test]
    async fn a_stalled_cursor_ends_at_the_deadline() {
        let mut stalled = futures::stream::pending::<Result<i32, sqlx::Error>>();
        let ctx = CallContext::new()
            .with_deadline(tokio::time::Instant::now() + Duration::from_millis(50));
        let e = next_rows(&ctx, "fetch", &mut stalled, 5, err)
            .await
            .unwrap_err();
        assert!(matches!(e, FaucetError::Timeout { .. }), "{e:?}");
        let ok = once(&ctx.clone(), "query", async { Ok(7) }).await;
        assert!(ok.is_err(), "the deadline has passed");
        let ok = once(&CallContext::new(), "query", async { Ok(7) }).await;
        assert_eq!(ok.unwrap(), 7);
    }
}
