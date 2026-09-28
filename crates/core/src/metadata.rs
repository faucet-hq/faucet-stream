//! Optional `_faucet_*` run/lineage metadata columns (#510).
//!
//! An opt-in, connector-agnostic **sink decorator** that stamps a small set of
//! metadata columns onto every row — the faucet-native analogue of Singer's
//! `_sdc_*` columns, for freshness/lineage and debugging. Because it's a
//! decorator (like [`CleanupTracker`](crate::cleanup::CleanupTracker)) it works
//! for every sink without per-connector code.
//!
//! ```yaml
//! metadata_columns:
//!   prefix: "_faucet"                       # column-name prefix (default)
//!   columns: [extracted_at, loaded_at, run_id, source]
//! ```

use crate::error::FaucetError;
use crate::traits::{RowOutcome, Sink};
use chrono::Utc;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// A metadata column to stamp onto every row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MetadataColumn {
    /// When the record entered the pipeline (ingest time). In the sink-decorator
    /// model this is captured just before the write, so it's ≈ `loaded_at`.
    ExtractedAt,
    /// Sink write time.
    LoadedAt,
    /// The pipeline run id.
    RunId,
    /// The source connector kind / stream id.
    Source,
    /// A monotonic per-run row ordinal.
    Sequence,
}

impl MetadataColumn {
    /// The column-name suffix (appended to the prefix as `{prefix}_{suffix}`).
    pub fn suffix(self) -> &'static str {
        match self {
            MetadataColumn::ExtractedAt => "extracted_at",
            MetadataColumn::LoadedAt => "loaded_at",
            MetadataColumn::RunId => "run_id",
            MetadataColumn::Source => "source",
            MetadataColumn::Sequence => "sequence",
        }
    }
}

fn default_prefix() -> String {
    "_faucet".to_owned()
}
fn default_true() -> bool {
    true
}

/// The `metadata_columns:` config block.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MetadataColumnsSpec {
    /// Master switch (default `true` when the block is present).
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Column-name prefix (default `_faucet`).
    #[serde(default = "default_prefix")]
    pub prefix: String,
    /// Columns to stamp. Empty = the default set
    /// (`extracted_at`, `loaded_at`, `run_id`, `source`).
    #[serde(default)]
    pub columns: Vec<MetadataColumn>,
}

impl Default for MetadataColumnsSpec {
    fn default() -> Self {
        Self {
            enabled: true,
            prefix: default_prefix(),
            columns: Vec::new(),
        }
    }
}

const DEFAULT_COLUMNS: &[MetadataColumn] = &[
    MetadataColumn::ExtractedAt,
    MetadataColumn::LoadedAt,
    MetadataColumn::RunId,
    MetadataColumn::Source,
];

/// A validated metadata-columns policy.
#[derive(Debug, Clone)]
pub struct CompiledMetadata {
    prefix: String,
    columns: Vec<MetadataColumn>,
}

impl CompiledMetadata {
    /// Compile a spec: resolve the default column set and validate the prefix.
    /// Returns `Ok(None)` when disabled (so the caller skips the decorator).
    pub fn compile(spec: &MetadataColumnsSpec) -> Result<Option<Self>, FaucetError> {
        if !spec.enabled {
            return Ok(None);
        }
        if spec.prefix.trim().is_empty() {
            return Err(FaucetError::Config(
                "metadata_columns: `prefix` must not be empty".into(),
            ));
        }
        let columns = if spec.columns.is_empty() {
            DEFAULT_COLUMNS.to_vec()
        } else {
            spec.columns.clone()
        };
        Ok(Some(Self {
            prefix: spec.prefix.clone(),
            columns,
        }))
    }

    fn column_name(&self, col: MetadataColumn) -> String {
        format!("{}_{}", self.prefix, col.suffix())
    }
}

/// Per-run values the decorator stamps.
#[derive(Debug, Clone)]
pub struct MetadataContext {
    /// The pipeline run id.
    pub run_id: String,
    /// The source connector kind / stream id.
    pub source: String,
}

/// A [`Sink`] decorator that stamps `_faucet_*` metadata columns onto every row
/// before delegating the write.
pub struct MetadataSink {
    inner: Box<dyn Sink>,
    meta: CompiledMetadata,
    ctx: MetadataContext,
    seq: AtomicU64,
}

impl std::fmt::Debug for MetadataSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetadataSink")
            .field("prefix", &self.meta.prefix)
            .field("columns", &self.meta.columns)
            .finish()
    }
}

impl MetadataSink {
    /// Wrap a sink so every written row carries the configured metadata columns.
    pub fn new(inner: Box<dyn Sink>, meta: CompiledMetadata, ctx: MetadataContext) -> Self {
        Self {
            inner,
            meta,
            ctx,
            seq: AtomicU64::new(0),
        }
    }

    /// Return a copy of `records` with the metadata columns injected. Non-object
    /// records pass through unchanged (nowhere to stamp).
    fn stamp(&self, records: &[Value]) -> Vec<Value> {
        let now = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        records
            .iter()
            .map(|rec| match rec {
                Value::Object(map) => {
                    let mut map = map.clone();
                    self.inject(&mut map, &now);
                    Value::Object(map)
                }
                other => other.clone(),
            })
            .collect()
    }

    fn inject(&self, map: &mut Map<String, Value>, now: &str) {
        for &col in &self.meta.columns {
            let name = self.meta.column_name(col);
            let value = match col {
                MetadataColumn::ExtractedAt | MetadataColumn::LoadedAt => {
                    Value::String(now.to_owned())
                }
                MetadataColumn::RunId => Value::String(self.ctx.run_id.clone()),
                MetadataColumn::Source => Value::String(self.ctx.source.clone()),
                MetadataColumn::Sequence => Value::from(self.seq.fetch_add(1, Ordering::Relaxed)),
            };
            map.insert(name, value);
        }
    }
}

#[async_trait::async_trait]
impl Sink for MetadataSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        self.inner.write_batch(&self.stamp(records)).await
    }

    async fn write_batch_partial(&self, records: &[Value]) -> Result<Vec<RowOutcome>, FaucetError> {
        self.inner.write_batch_partial(&self.stamp(records)).await
    }

    async fn write_batch_idempotent(
        &self,
        records: &[Value],
        scope: &str,
        token: &str,
    ) -> Result<usize, FaucetError> {
        self.inner
            .write_batch_idempotent(&self.stamp(records), scope, token)
            .await
    }

    async fn flush(&self) -> Result<(), FaucetError> {
        self.inner.flush().await
    }

    // ── Pure forwarding ──────────────────────────────────────────────────────
    async fn local_outputs(&self) -> Vec<crate::local_outputs::LocalOutput> {
        self.inner.local_outputs().await
    }
    async fn check(
        &self,
        ctx: &crate::check::CheckContext,
    ) -> Result<crate::check::CheckReport, FaucetError> {
        self.inner.check(ctx).await
    }
    fn supports_cleanup(&self) -> bool {
        self.inner.supports_cleanup()
    }
    async fn cleanup_scope(
        &self,
        scope: &BTreeMap<String, Value>,
        seen: &crate::cleanup::SeenKeys,
    ) -> Result<u64, FaucetError> {
        self.inner.cleanup_scope(scope, seen).await
    }
    fn supports_idempotent_writes(&self) -> bool {
        self.inner.supports_idempotent_writes()
    }
    async fn last_committed_token(&self, scope: &str) -> Result<Option<String>, FaucetError> {
        self.inner.last_committed_token(scope).await
    }
    fn supported_write_modes(&self) -> &'static [crate::write_mode::WriteMode] {
        self.inner.supported_write_modes()
    }
    fn dedups_by_key(&self) -> bool {
        self.inner.dedups_by_key()
    }
    fn batch_atomicity(&self) -> crate::dlq::BatchAtomicity {
        self.inner.batch_atomicity()
    }
    fn sink_guarantee(&self) -> crate::idempotency::SinkGuarantee {
        self.inner.sink_guarantee()
    }
    async fn current_schema(&self) -> Result<Option<Value>, FaucetError> {
        self.inner.current_schema().await
    }
    fn supports_schema_evolution(&self) -> bool {
        self.inner.supports_schema_evolution()
    }
    async fn evolve_schema(
        &self,
        evolution: &crate::drift::SchemaEvolution,
    ) -> Result<(), FaucetError> {
        self.inner.evolve_schema(evolution).await
    }
    fn config_schema(&self) -> Value {
        self.inner.config_schema()
    }
    fn connector_name(&self) -> &'static str {
        self.inner.connector_name()
    }
    fn dataset_uri(&self) -> String {
        self.inner.dataset_uri()
    }
    fn is_overwrite(&self) -> bool {
        self.inner.is_overwrite()
    }
    async fn begin_overwrite(&self) -> Result<(), FaucetError> {
        self.inner.begin_overwrite().await
    }
    async fn commit_overwrite(&self) -> Result<(), FaucetError> {
        self.inner.commit_overwrite().await
    }
    async fn abort_overwrite(&self) -> Result<(), FaucetError> {
        self.inner.abort_overwrite().await
    }
    async fn complete_run(&self) -> Result<(), FaucetError> {
        self.inner.complete_run().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Mutex;

    #[derive(Debug, Default)]
    struct CapturingSink {
        rows: Mutex<Vec<Value>>,
    }
    #[async_trait::async_trait]
    impl Sink for CapturingSink {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            self.rows.lock().unwrap().extend_from_slice(records);
            Ok(records.len())
        }
        fn config_schema(&self) -> Value {
            json!({})
        }
    }

    fn spec(cols: &[MetadataColumn]) -> MetadataColumnsSpec {
        MetadataColumnsSpec {
            enabled: true,
            prefix: "_faucet".into(),
            columns: cols.to_vec(),
        }
    }

    fn ctx() -> MetadataContext {
        MetadataContext {
            run_id: "run-42".into(),
            source: "rest".into(),
        }
    }

    #[test]
    fn compile_resolves_defaults_and_respects_disable() {
        let c = CompiledMetadata::compile(&spec(&[])).unwrap().unwrap();
        assert_eq!(c.columns, DEFAULT_COLUMNS.to_vec());
        // disabled → None
        let disabled = MetadataColumnsSpec {
            enabled: false,
            ..Default::default()
        };
        assert!(CompiledMetadata::compile(&disabled).unwrap().is_none());
        // empty prefix → error
        let bad = MetadataColumnsSpec {
            prefix: " ".into(),
            ..Default::default()
        };
        assert!(CompiledMetadata::compile(&bad).is_err());
    }

    #[tokio::test]
    async fn stamps_all_column_kinds_with_prefix() {
        let meta = CompiledMetadata::compile(&spec(&[
            MetadataColumn::ExtractedAt,
            MetadataColumn::LoadedAt,
            MetadataColumn::RunId,
            MetadataColumn::Source,
            MetadataColumn::Sequence,
        ]))
        .unwrap()
        .unwrap();
        let inner = Box::new(CapturingSink::default());
        let sink = MetadataSink::new(inner, meta, ctx());
        assert_eq!(
            sink.batch_atomicity(),
            crate::dlq::BatchAtomicity::BestEffort
        );
        let n = sink
            .write_batch(&[json!({"id": 1}), json!({"id": 2})])
            .await
            .unwrap();
        assert_eq!(n, 2);
        // Re-wrap to read the captured rows would need the inner; instead assert
        // via a fresh capturing sink shared by Arc — simpler: re-stamp directly.
        let stamped = sink.stamp(&[json!({"id": 1}), json!({"id": 2})]);
        let r0 = &stamped[0];
        assert_eq!(r0["id"], 1);
        assert_eq!(r0["_faucet_run_id"], "run-42");
        assert_eq!(r0["_faucet_source"], "rest");
        assert!(r0["_faucet_extracted_at"].is_string());
        assert!(r0["_faucet_loaded_at"].is_string());
        // Sequence is monotonic across records.
        assert_eq!(
            stamped[0]["_faucet_sequence"].as_u64().unwrap() + 1,
            stamped[1]["_faucet_sequence"].as_u64().unwrap()
        );
    }

    #[test]
    fn non_object_records_pass_through() {
        let meta = CompiledMetadata::compile(&spec(&[MetadataColumn::RunId]))
            .unwrap()
            .unwrap();
        let sink = MetadataSink::new(Box::new(CapturingSink::default()), meta, ctx());
        let out = sink.stamp(&[json!("scalar"), json!(42)]);
        assert_eq!(out, vec![json!("scalar"), json!(42)]);
    }

    #[tokio::test]
    async fn delegates_every_sink_method_to_inner() {
        let meta = CompiledMetadata::compile(&spec(&[MetadataColumn::RunId]))
            .unwrap()
            .unwrap();
        let sink = MetadataSink::new(Box::new(CapturingSink::default()), meta, ctx());

        // Write paths stamp + forward.
        assert_eq!(sink.write_batch(&[json!({"a": 1})]).await.unwrap(), 1);
        assert_eq!(
            sink.write_batch_partial(&[json!({"a": 1})])
                .await
                .unwrap()
                .len(),
            1
        );
        let _ = sink
            .write_batch_idempotent(&[json!({"a": 1})], "scope", "tok")
            .await;
        sink.flush().await.unwrap();

        // Pure forwards (inner uses trait defaults).
        let _ = sink.check(&crate::check::CheckContext::default()).await;
        assert!(!sink.supports_cleanup());
        let _ = sink
            .cleanup_scope(&BTreeMap::new(), &crate::cleanup::SeenKeys::new())
            .await;
        assert!(!sink.supports_idempotent_writes());
        assert!(sink.last_committed_token("s").await.unwrap().is_none());
        let _ = sink.supported_write_modes();
        let _ = sink.dedups_by_key();
        let _ = sink.sink_guarantee();
        let _ = sink.current_schema().await.unwrap();
        let _ = sink.supports_schema_evolution();
        let _ = sink
            .evolve_schema(&crate::drift::SchemaEvolution::default())
            .await;
        let _ = sink.config_schema();
        let _ = sink.connector_name();
        let _ = sink.dataset_uri();
        assert!(!sink.is_overwrite());
        let _ = sink.begin_overwrite().await;
        let _ = sink.commit_overwrite().await;
        sink.abort_overwrite().await.unwrap();
        sink.complete_run().await.unwrap();

        assert!(format!("{sink:?}").contains("MetadataSink"));
    }

    #[test]
    fn custom_prefix_and_subset() {
        let meta = CompiledMetadata::compile(&MetadataColumnsSpec {
            enabled: true,
            prefix: "_dt".into(),
            columns: vec![MetadataColumn::RunId],
        })
        .unwrap()
        .unwrap();
        let sink = MetadataSink::new(Box::new(CapturingSink::default()), meta, ctx());
        let out = sink.stamp(&[json!({"a": 1})]);
        assert_eq!(out[0]["_dt_run_id"], "run-42");
        assert!(out[0].get("_dt_loaded_at").is_none());
    }
}
