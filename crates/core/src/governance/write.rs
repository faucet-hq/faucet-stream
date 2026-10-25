//! The engine's write gateway (RFC 0014): every write `faucet-core` drives
//! goes through one of these functions, and each takes a governed value. The
//! pipeline loops never call a sink's write methods themselves — a test below
//! scans them to keep it that way.

use super::page::{GovernedEnvelopes, GovernedNative, GovernedPage};
use crate::accounting::WriteReport;
use crate::error::FaucetError;
use crate::traits::{RowOutcome, Sink};

/// Write a governed page and report every row.
pub(crate) async fn reported<Si: Sink + ?Sized>(
    sink: &Si,
    page: &GovernedPage<'_>,
) -> Result<WriteReport, FaucetError> {
    sink.write_governed(page).await
}

/// Write a governed page and report one outcome per row.
pub(crate) async fn partial<Si: Sink + ?Sized>(
    sink: &Si,
    page: &GovernedPage<'_>,
) -> Result<Vec<RowOutcome>, FaucetError> {
    sink.write_batch_partial(page.records()).await
}

/// Offer a whole governed page before it is split into sub-batches.
pub(crate) async fn admit<Si: Sink + ?Sized>(
    sink: &Si,
    page: &GovernedPage<'_>,
) -> Result<(), FaucetError> {
    sink.admit_page(page.records()).await
}

/// Write a governed page and its commit token atomically.
pub(crate) async fn idempotent<Si: Sink + ?Sized>(
    sink: &Si,
    page: &GovernedPage<'_>,
    scope: &str,
    token: &str,
) -> Result<WriteReport, FaucetError> {
    // The engine drives legacy sinks and driver-backed sinks through `Sink` until 3b.
    #[allow(deprecated)]
    sink.write_batch_idempotent_reported(page.records(), scope, token)
        .await
}

/// Write a governed columnar batch.
#[cfg(feature = "arrow")]
pub(crate) async fn columnar<Si: Sink + ?Sized>(
    sink: &Si,
    batch: &super::page::GovernedBatch,
) -> Result<usize, FaucetError> {
    sink.write_batch_columnar(batch.batch()).await
}

/// Load a native byte batch no record pass applies to.
pub(crate) async fn native<Si: Sink + ?Sized>(
    sink: &Si,
    batch: GovernedNative,
    scope: &str,
    ctx: crate::native::NativeLoadContext,
) -> Result<usize, FaucetError> {
    sink.load_native(batch.into_inner(), scope, ctx).await
}

/// Write governed envelopes to a DLQ sink (its own governed stack, #870).
/// Every envelope must land: a row the DLQ skipped is a lost row, so a short
/// write fails with `RowsUnaccounted` instead of counting it as dead-lettered.
pub(crate) async fn dlq(
    sink: &dyn Sink,
    envelopes: &GovernedEnvelopes,
) -> Result<usize, FaucetError> {
    let offered = envelopes.envelopes().len();
    let report = sink.write_batch_reported(envelopes.envelopes()).await?;
    if report.accepted.saturating_add(report.coalesced) < offered {
        return Err(FaucetError::RowsUnaccounted {
            offered: offered as u64,
            accepted: report.accepted as u64,
        });
    }
    Ok(report.accepted)
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    struct Short(usize, usize);
    #[async_trait::async_trait]
    impl crate::Sink for Short {
        async fn write_batch(&self, r: &[Value]) -> Result<usize, crate::FaucetError> {
            Ok(r.len().min(self.0))
        }
        async fn write_batch_reported(
            &self,
            r: &[Value],
        ) -> Result<crate::accounting::WriteReport, crate::FaucetError> {
            let n = r.len().min(self.0);
            Ok(crate::accounting::WriteReport::accepted(n).with_skipped(self.1.min(r.len() - n)))
        }
    }

    #[tokio::test]
    async fn a_dlq_that_writes_fewer_envelopes_than_offered_fails() {
        let env = super::GovernedEnvelopes::sealed((0..5).map(|i| json!({"i": i})).collect());
        let full = Short(10, 0);
        assert_eq!(super::dlq(&full, &env).await.unwrap(), 5);
        let skipping = Short(2, 3);
        let err = super::dlq(&skipping, &env).await.unwrap_err();
        assert!(
            matches!(
                err,
                crate::FaucetError::RowsUnaccounted {
                    offered: 5,
                    accepted: 2
                }
            ),
            "{err}"
        );
    }
    /// The structural half of the typestate: outside this gateway, the code
    /// that drives a run never calls a sink's write methods directly, so
    /// every write goes through a function that takes a governed value.
    #[test]
    fn the_engine_loops_write_only_through_the_gateway() {
        const DRIVERS: &[(&str, &str)] = &[
            ("pipeline.rs", include_str!("../pipeline.rs")),
            ("topology.rs", include_str!("../topology.rs")),
        ];
        const WRITES: &[&str] = &[
            ".write_batch(",
            ".write_batch_reported(",
            ".write_batch_partial(",
            ".write_batch_idempotent(",
            ".write_batch_idempotent_reported(",
            ".write_batch_columnar(",
            ".write_governed(",
            ".load_native(",
            ".admit_page(",
        ];
        for (file, src) in DRIVERS {
            let code = src.split("\nmod tests {").next().unwrap_or(src);
            assert!(code.len() > 10_000, "{file}: the scan found no code");
            for (n, line) in code.lines().enumerate() {
                let line = line.trim_start();
                if line.starts_with("//") {
                    continue;
                }
                for w in WRITES {
                    assert!(
                        !line.contains(w),
                        "{file}:{}: `{w}` bypasses the governed write gateway: {line}",
                        n + 1
                    );
                }
            }
        }
    }
}
