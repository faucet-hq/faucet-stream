//! Data Movement Catalog (#279) — the CLI-side write path.
//!
//! The catalog accumulates, run over run, the operational history of every
//! dataset a pipeline touches: identity, a deduplicated schema timeline,
//! volume/freshness stats, and lineage edges. Storage rides the serve
//! run-history backends (`crate::serve::history`); this module is the glue
//! the executor calls after every successful **root** invocation:
//!
//! - [`spec`] — the top-level `catalog:` config block (`faucet schema catalog`).
//! - [`model`] — pure URI canonicalization + sample-schema inference.
//! - [`CatalogHandle`] / [`connect_from_spec`] — the store handle carried on
//!   `ExecuteOptions` (serve passes its own history backend; the CLI runtimes
//!   connect from the `catalog:` block).
//! - [`record`] — the never-fails-the-run write (mirrors the SLA / lineage
//!   "log-and-continue" contract).
//!
//! Gated on the `catalog` Cargo feature (implies `serve` for the storage
//! backends and `lineage` for record sampling + column-lineage derivation).

pub mod model;
pub mod snapshot;
pub mod spec;

pub use spec::CatalogSpec;

use crate::error::{CliError, CliResult};
use crate::serve::config::HistoryBackendSpec;
use crate::serve::history::catalog::ConfigSnapshot;
use crate::serve::history::{self, RunHistory, catalog::CatalogUpdate};
use std::sync::Arc;
use std::time::Duration;

/// Default per-side schema-inference sample cap when no `catalog:` block set
/// one (the serve write path, which has no block).
pub const DEFAULT_SAMPLE_RECORDS: usize = 100;

/// The catalog store handle carried on `ExecuteOptions`. Cheaply cloneable.
#[derive(Clone)]
pub struct CatalogHandle {
    pub store: Arc<dyn RunHistory>,
    /// Provenance run id recorded on every catalog row this run produces —
    /// the serve run id when running under `faucet serve`, `None` for CLI
    /// runtimes (each invocation then stamps its own observability run id).
    pub run_id: Option<String>,
    /// Per-side schema-inference sample cap.
    pub sample_records: usize,
}

impl std::fmt::Debug for CatalogHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogHandle")
            .field("run_id", &self.run_id)
            .field("sample_records", &self.sample_records)
            .finish_non_exhaustive()
    }
}

/// Build a catalog store from the `catalog:` block. Errors are config-level
/// (bad scheme / missing build feature) and fail fast at load time; an
/// *unreachable* SQL backend degrades to in-memory via `FallbackHistory`
/// (logged, run unaffected) exactly like `faucet serve --history`.
pub async fn connect_from_spec(spec: &CatalogSpec) -> CliResult<CatalogHandle> {
    let backend = parse_url(&spec.url)?;
    let store = history::connect(
        &backend,
        // Idempotency claims + run leases are run-history concerns; the
        // catalog-only connection never uses them.
        Duration::from_secs(3600),
        Duration::from_secs(30),
        &uuid::Uuid::now_v7().to_string(),
    )
    .await?;
    Ok(CatalogHandle {
        store,
        run_id: None,
        sample_records: spec.sample_records,
    })
}

/// Parse the `catalog.url` field into a history-backend selection.
fn parse_url(url: &str) -> CliResult<HistoryBackendSpec> {
    match url {
        "memory" => Ok(HistoryBackendSpec::Memory),
        u if u.starts_with("postgres://") || u.starts_with("postgresql://") => {
            Ok(HistoryBackendSpec::Postgres(u.to_string()))
        }
        u if u.starts_with("sqlite:") => Ok(HistoryBackendSpec::Sqlite(u.to_string())),
        other => Err(CliError::Config(format!(
            "catalog.url '{other}' is not recognised — expected 'memory', 'sqlite:<path>', \
             or a 'postgres://…' URL"
        ))),
    }
}

/// Persist one run's catalog update. Monitoring must never take down the run
/// it observes: any backend error is logged once per call and swallowed.
pub async fn record(handle: &CatalogHandle, update: &CatalogUpdate) {
    if let Err(e) = handle.store.catalog_record(update).await {
        tracing::warn!(
            pipeline = %update.pipeline,
            row = %update.row,
            error = %e,
            "catalog write failed — run unaffected"
        );
    }
}

/// Persist one run's column profile for a dataset (#708). Best-effort, same
/// never-fails-the-run contract as [`record`].
pub async fn record_profile(
    handle: &CatalogHandle,
    dataset_id: &str,
    record: &crate::serve::history::catalog::CatalogProfileRecord,
) {
    if let Err(e) = handle
        .store
        .catalog_record_profile(dataset_id, record)
        .await
    {
        tracing::warn!(
            pipeline = %record.pipeline,
            row = %record.row,
            error = %e,
            "catalog profile write failed — run unaffected"
        );
    }
}

/// Persist the latest resolved+expanded config snapshot for `faucet plan --diff`
/// (#374). Best-effort, same never-fails-the-run contract as [`record`].
pub async fn record_config_snapshot(handle: &CatalogHandle, snapshot: &ConfigSnapshot) {
    if let Err(e) = handle.store.catalog_record_config_snapshot(snapshot).await {
        tracing::warn!(
            pipeline = %snapshot.pipeline,
            error = %e,
            "config-snapshot write failed — run unaffected"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn connect_memory_and_reject_unknown_scheme() {
        let handle = connect_from_spec(&CatalogSpec {
            url: "memory".into(),
            sample_records: 25,
        })
        .await
        .unwrap();
        assert_eq!(handle.sample_records, 25);
        assert!(handle.run_id.is_none());

        let err = connect_from_spec(&CatalogSpec {
            url: "mysql://nope".into(),
            sample_records: 100,
        })
        .await
        .unwrap_err();
        assert!(err.to_string().contains("catalog.url"), "{err}");
    }

    #[test]
    fn parse_url_recognises_all_three_schemes() {
        assert!(matches!(
            parse_url("sqlite:./cat.db"),
            Ok(HistoryBackendSpec::Sqlite(u)) if u == "sqlite:./cat.db"
        ));
        assert!(matches!(
            parse_url("postgres://h/db"),
            Ok(HistoryBackendSpec::Postgres(_))
        ));
        assert!(matches!(
            parse_url("postgresql://h/db"),
            Ok(HistoryBackendSpec::Postgres(_))
        ));
        assert!(matches!(
            parse_url("memory"),
            Ok(HistoryBackendSpec::Memory)
        ));
        assert!(parse_url("bogus").is_err());
    }

    #[tokio::test]
    async fn handle_debug_never_prints_the_store() {
        let handle = connect_from_spec(&CatalogSpec {
            url: "memory".into(),
            sample_records: 7,
        })
        .await
        .unwrap();
        let dbg = format!("{handle:?}");
        assert!(dbg.contains("sample_records: 7"), "{dbg}");
        assert!(dbg.contains(".."), "non-exhaustive marker expected: {dbg}");
    }
}
