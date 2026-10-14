//! Staged-load **execution shim** (#528) — the network data-plane for the MSSQL
//! staged bulk load: upload the page to Azure Blob / ADLS and run `COPY INTO`
//! over the tiberius pool.
//!
//! This code cannot be exercised in CI (it needs a live SQL Server that can
//! itself reach a live Azure Blob container, and `MssqlSink` cannot even be
//! constructed without a reachable server), so it is deliberately isolated here
//! and excluded from patch-coverage — the same treatment the GCS connectors'
//! data-plane I/O gets. All the *pure* logic it depends on (SQL/URL generation
//! and the upload-and-build step [`crate::staged::build_staged_copy_sql`]) stays
//! unit-tested and inside the coverage denominator.

use faucet_core::FaucetError;
use serde_json::Value;

use crate::sink::MssqlDriver;

impl MssqlDriver {
    /// Staged bulk load (#528): upload the page to Azure and `COPY INTO`.
    pub(crate) async fn write_batch_staged(
        &self,
        records: &[Value],
        staging: &crate::config::MssqlStagingConfig,
    ) -> Result<usize, FaucetError> {
        use faucet_core::staging::{StageUploader, StagingFormat, StagingScheme};
        use std::sync::atomic::Ordering;

        // `COPY INTO` reads Azure Blob / ADLS and CSV only.
        let loc = staging
            .spec
            .validate(&[StagingScheme::Azure], &[StagingFormat::Csv])?;
        let uploader = StageUploader::from_location(loc)?;
        let seq = self.stage_seq.fetch_add(1, Ordering::Relaxed);
        let (staged, sql) = crate::staged::build_staged_copy_sql(
            &uploader,
            &self.table_quoted,
            &self.config.table,
            &self.stage_run_id,
            seq,
            records,
            staging,
        )
        .await?;

        let run = async {
            let mut conn = self.checkout().await?;
            self.on_conn(&mut conn, "copy", false, async |c| {
                c.simple_query(sql.as_str())
                    .await
                    .map_err(|e| FaucetError::Sink(format!("MSSQL COPY INTO failed: {e}")))?
                    .into_results()
                    .await
                    .map_err(|e| FaucetError::Sink(format!("MSSQL COPY INTO failed: {e}")))?;
                Ok(())
            })
            .await
        }
        .await;
        uploader
            .cleanup(&[staged], staging.spec.cleanup, run.is_ok())
            .await;
        run?;
        tracing::debug!(records = records.len(), "MSSQL staged load written");
        Ok(records.len())
    }
}
