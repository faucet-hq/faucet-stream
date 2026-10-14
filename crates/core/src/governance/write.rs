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
pub(crate) async fn dlq(
    sink: &dyn Sink,
    envelopes: &GovernedEnvelopes,
) -> Result<usize, FaucetError> {
    sink.write_batch(envelopes.envelopes()).await
}

#[cfg(test)]
mod tests {
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
