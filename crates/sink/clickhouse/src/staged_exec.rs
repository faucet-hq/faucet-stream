//! Staged-load **execution shim** (#528) — the network data-plane for the
//! ClickHouse staged bulk load: upload the page to the object store and issue
//! the `INSERT … SELECT FROM s3()/gcs()` statement over the HTTP interface.
//!
//! This code cannot be exercised in CI (it needs a live ClickHouse server that
//! can itself reach a live S3/GCS bucket), so it is deliberately isolated here
//! and excluded from patch-coverage — the same treatment the GCS connectors'
//! data-plane I/O gets. All the *pure* logic it depends on (SQL/URL generation
//! in [`crate::staged`] and the upload-and-build step
//! [`ClickHouseSink::stage_and_build_sql`]) stays unit-tested and inside the
//! coverage denominator.

use faucet_core::FaucetError;
use faucet_core::util::{DEFAULT_ERROR_BODY_MAX_LEN, check_http_response};
use serde_json::Value;

use crate::sink::ClickHouseSink;

impl ClickHouseSink {
    /// Send the staged-load `INSERT … SELECT FROM s3(…)` over the HTTP
    /// interface. The statement (which may carry storage credentials) rides the
    /// request body, never the URL: a body-less POST is rejected with HTTP 411,
    /// and a URL lands in proxy and access logs.
    pub(crate) async fn send_query(&self, statement: &str) -> Result<(), FaucetError> {
        let req = crate::sink::staged_request(
            &self.client,
            &self.base_url,
            &self.config.connection,
            statement,
        );
        let resp = req.send().await?;
        check_http_response(resp, DEFAULT_ERROR_BODY_MAX_LEN).await?;
        Ok(())
    }

    /// Staged bulk load (#528): upload the page to the object store, then have
    /// the ClickHouse server pull it with `s3()` / `gcs()`.
    pub(crate) async fn write_batch_staged(
        &self,
        records: &[Value],
        staging: &crate::config::ClickHouseStagingConfig,
    ) -> Result<usize, FaucetError> {
        use faucet_core::staging::{StageUploader, StagingFormat, StagingScheme};

        // Restrict to what the s3()/gcs() path supports.
        let loc = staging.spec.validate(
            &[StagingScheme::S3, StagingScheme::Gcs],
            &[StagingFormat::Jsonl, StagingFormat::Csv],
        )?;
        let uploader = StageUploader::from_location(loc)?;
        let (staged, sql) = self
            .stage_and_build_sql(&uploader, records, staging)
            .await?;
        let result = self.send_query(&sql).await;
        uploader
            .cleanup(&[staged], staging.spec.cleanup, result.is_ok())
            .await;
        result?;
        tracing::debug!(records = records.len(), "ClickHouse staged load written");
        Ok(records.len())
    }
}
