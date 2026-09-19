//! Delta Lake sink executor.
//!
//! Lazily opens (or creates) the target Delta table on the first `write_batch`
//! so the schema can be inferred from real records, then appends via
//! delta-rs's low-level [`RecordBatchWriter`] — one Delta commit per
//! [`flush`](faucet_core::Sink::flush). No datafusion is pulled in.
//!
//! ## Flush / commit contract
//!
//! `RecordBatchWriter` buffers written batches into parquet in memory; the
//! Delta transaction is only committed by `flush_and_commit`. The pipeline
//! calls [`flush`](faucet_core::Sink::flush) after every bookmark-carrying
//! page, so each page becomes its own atomic Delta commit. **A dropped sink
//! that never `flush`es loses the buffered, uncommitted batch** — the same
//! contract as the Parquet sink.

use std::collections::HashSet;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use deltalake::DeltaTable;
use deltalake::kernel::StructType;
use deltalake::kernel::engine::arrow_conversion::TryIntoKernel;
use deltalake::operations::create::CreateBuilder;
use deltalake::writer::{DeltaWriter, RecordBatchWriter};
use faucet_common_delta::convert::infer_arrow_schema;
use faucet_core::{FaucetError, WriteMode};
use serde_json::Value;
use tokio::sync::Mutex;

use crate::config::DeltaSinkConfig;

/// A sink that appends JSON records to an Apache Delta Lake table.
pub struct DeltaSink {
    config: DeltaSinkConfig,
    state: Mutex<SinkState>,
}

/// Mutable per-run state, guarded by a `Mutex` so `write_batch(&self, …)` can
/// mutate the open table / writer.
struct SinkState {
    /// The open Delta table, established on first write. Advanced in place by
    /// each `flush_and_commit`.
    table: Option<DeltaTable>,
    /// The record-batch writer bound to `table`. Rebuilt after a table (re)open.
    writer: Option<RecordBatchWriter>,
    /// The Arrow schema locked in on first write; every subsequent batch is
    /// decoded against it. A record that diverges fails the batch (v1: no
    /// schema evolution).
    schema: Option<SchemaRef>,
    /// Fields warned-about as dropped (present in a record, absent from the
    /// locked schema). Deduped to one line per field per run.
    warned_fields: HashSet<String>,
    /// Whether any batch has been buffered since the last commit (so `flush`
    /// can skip a no-op commit).
    pending: bool,
}

impl SinkState {
    fn new() -> Self {
        Self {
            table: None,
            writer: None,
            schema: None,
            warned_fields: HashSet::new(),
            pending: false,
        }
    }
}

impl DeltaSink {
    /// Build a new Delta sink. Validates config eagerly; the table is opened
    /// lazily on the first write.
    pub async fn new(config: DeltaSinkConfig) -> Result<Self, FaucetError> {
        config
            .validate()
            .map_err(|e| FaucetError::Config(format!("invalid delta sink config: {e}")))?;
        // Register cloud object-store handlers up front so a bad scheme fails
        // predictably rather than on first write.
        config.connection.register_handlers();
        Ok(Self {
            config,
            state: Mutex::new(SinkState::new()),
        })
    }

    /// Ensure the table + writer + schema are established, inferring the schema
    /// from `records` on the very first call.
    async fn ensure_open(
        &self,
        state: &mut SinkState,
        records: &[Value],
    ) -> Result<(), FaucetError> {
        if state.schema.is_none() {
            let schema = infer_arrow_schema(records, self.config.effective_sample_size())?;
            state.schema = Some(schema);
        }
        let schema = state.schema.clone().expect("schema set above");
        self.ensure_table_writer(state, &schema).await
    }

    /// Establish the table + record-batch writer for an already-locked `schema`.
    /// Shared by the JSON path ([`ensure_open`](Self::ensure_open), which infers
    /// the schema from records) and the columnar path
    /// ([`write_batch_columnar`](faucet_core::Sink::write_batch_columnar), which
    /// takes it from the incoming `RecordBatch`).
    async fn ensure_table_writer(
        &self,
        state: &mut SinkState,
        schema: &SchemaRef,
    ) -> Result<(), FaucetError> {
        if state.table.is_none() {
            let table = self.open_or_create(schema).await?;
            state.table = Some(table);
        }
        if state.writer.is_none() {
            let table = state.table.as_ref().expect("table set above");
            let writer = RecordBatchWriter::for_table(table).map_err(|e| {
                FaucetError::Sink(format!("delta: could not build record-batch writer: {e}"))
            })?;
            state.writer = Some(writer);
        }
        Ok(())
    }

    /// Open the existing table or create it from the inferred schema.
    async fn open_or_create(&self, schema: &SchemaRef) -> Result<DeltaTable, FaucetError> {
        if let Some(table) = self.config.connection.open_optional().await? {
            return Ok(table);
        }
        if !self.config.create_if_not_missing {
            return Err(FaucetError::Sink(format!(
                "delta: table '{}' does not exist and create_if_not_missing is false",
                self.config.connection.redacted_uri()
            )));
        }
        self.create_table(schema).await
    }

    /// Create a new Delta table from the inferred Arrow schema + partitioning.
    async fn create_table(&self, schema: &SchemaRef) -> Result<DeltaTable, FaucetError> {
        // Every partition column must exist in the record schema.
        for col in &self.config.partition_by {
            if schema.field_with_name(col).is_err() {
                return Err(FaucetError::Sink(format!(
                    "delta: partition column '{col}' not present in the inferred record schema"
                )));
            }
        }

        let delta_schema: StructType = schema.as_ref().try_into_kernel().map_err(|e| {
            FaucetError::Sink(format!(
                "delta: could not convert Arrow schema to Delta: {e}"
            ))
        })?;

        let mut builder = CreateBuilder::new()
            .with_location(self.config.connection.location_string()?)
            .with_storage_options(self.config.connection.merged_storage_options())
            .with_columns(delta_schema.fields().cloned());
        if !self.config.partition_by.is_empty() {
            builder = builder.with_partition_columns(self.config.partition_by.clone());
        }

        builder.await.map_err(|e| {
            FaucetError::Sink(format!(
                "delta: could not create table '{}': {e}",
                self.config.connection.redacted_uri()
            ))
        })
    }

    /// Decode a chunk of JSON records into a `RecordBatch` against the locked
    /// schema, warning once per dropped unknown field.
    fn encode_batch(
        &self,
        warned_fields: &mut HashSet<String>,
        schema: SchemaRef,
        records: &[Value],
    ) -> Result<RecordBatch, FaucetError> {
        warn_on_unknown_fields(warned_fields, &schema, records);

        let mut decoder = arrow_json::ReaderBuilder::new(schema.clone())
            .build_decoder()
            .map_err(|e| FaucetError::Sink(format!("delta: could not build json decoder: {e}")))?;
        decoder.serialize(records).map_err(|e| {
            FaucetError::Sink(format!("delta: record does not match table schema: {e}"))
        })?;
        decoder
            .flush()
            .map_err(|e| FaucetError::Sink(format!("delta: json decode error: {e}")))?
            .ok_or_else(|| FaucetError::Sink("delta: json decoder produced no batch".to_string()))
    }

    /// Write one chunk of records into the buffered writer.
    async fn write_chunk(
        &self,
        state: &mut SinkState,
        records: &[Value],
    ) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }
        self.ensure_open(state, records).await?;
        let schema = state.schema.clone().expect("schema set");
        let batch = self.encode_batch(&mut state.warned_fields, schema, records)?;
        let rows = batch.num_rows();
        let writer = state.writer.as_mut().expect("writer set");
        writer
            .write(batch)
            .await
            .map_err(|e| FaucetError::Sink(format!("delta: write failed: {e}")))?;
        state.pending = true;
        self.roll_if_over_target(state).await?;
        Ok(rows)
    }

    /// Commit whatever the writer has buffered, then drop it so the next write
    /// rebuilds a fresh writer against the advanced table.
    ///
    /// Shared by `flush` and [`roll_if_over_target`](Self::roll_if_over_target)
    /// — the two differ only in *when* they fire, not in what a commit is.
    async fn commit_buffered(&self, state: &mut SinkState) -> Result<(), FaucetError> {
        if !state.pending {
            return Ok(());
        }
        // Take the writer + table out to satisfy the borrow checker, commit,
        // then put the table back and drop the (now-flushed) writer so the next
        // page rebuilds a fresh writer against the advanced table.
        let mut writer = match state.writer.take() {
            Some(w) => w,
            None => return Ok(()),
        };
        let mut table = state
            .table
            .take()
            .ok_or_else(|| FaucetError::Sink("delta: flush without an open table".to_string()))?;
        let version = writer
            .flush_and_commit(&mut table)
            .await
            .map_err(|e| FaucetError::Sink(format!("delta: commit failed: {e}")))?;
        tracing::debug!(version, uri = %self.config.connection.redacted_uri(), "delta commit");
        state.table = Some(table);
        state.pending = false;
        Ok(())
    }

    /// Commit early once the writer's in-memory parquet buffer reaches
    /// `target_file_size`, bounding peak memory and sizing the output files.
    ///
    /// This is what makes `target_file_size` mean anything (#620). It was
    /// declared and validated but **never read**, because delta-rs's
    /// `RecordBatchWriter` has no target-size knob: it writes exactly one file
    /// per `flush_and_commit`, so file size is governed entirely by *flush
    /// cadence*. Wiring the knob and bounding memory are therefore the same
    /// change.
    ///
    /// Before this, the only flush was at end-of-run — and a bulk source emits
    /// no bookmarks, so peak memory was O(whole dataset) and a large table
    /// could OOM. The cost is a few commits per run instead of one, which is
    /// the trade the knob exists to let an operator make.
    ///
    /// Unset means unbounded, preserving the previous single-commit behaviour
    /// for anyone relying on it.
    async fn roll_if_over_target(&self, state: &mut SinkState) -> Result<(), FaucetError> {
        let Some(target) = self.config.target_file_size else {
            return Ok(());
        };
        let buffered = match state.writer.as_ref() {
            Some(w) => w.buffer_len(),
            None => return Ok(()),
        };
        if buffered >= target {
            tracing::debug!(
                buffered,
                target,
                "delta: buffered parquet reached target_file_size; committing early"
            );
            self.commit_buffered(state).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl faucet_core::Sink for DeltaSink {
    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(DeltaSinkConfig))
            .expect("schema serialization")
    }

    fn connector_name(&self) -> &'static str {
        "delta"
    }

    fn dataset_uri(&self) -> String {
        self.config.connection.redacted_uri()
    }

    fn supported_write_modes(&self) -> &'static [WriteMode] {
        // Append-only in v1 (mirrors the Iceberg sink). MERGE/upsert is a
        // version-gated follow-up (#317 "Out of scope").
        &[WriteMode::Append]
    }

    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};
        let started = std::time::Instant::now();
        // Metadata-only open (no data scan). A reachable store passes whether
        // or not the table exists yet — `create_if_not_missing` handles an
        // absent table at write time.
        let probe =
            match tokio::time::timeout(ctx.timeout, self.config.connection.open_optional()).await {
                Ok(Ok(_)) => Probe::pass("table", started.elapsed()),
                Ok(Err(e)) => Probe::fail_hint(
                    "table",
                    started.elapsed(),
                    format!("delta sink probe failed: {e}"),
                    "Verify table_uri, credentials, and object-store reachability.",
                ),
                Err(_) => Probe::fail_hint(
                    "table",
                    started.elapsed(),
                    format!("delta sink probe timed out after {:?}", ctx.timeout),
                    "Check object-store network reachability.",
                ),
            };
        Ok(CheckReport::single(probe))
    }

    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }
        let mut state = self.state.lock().await;
        let bs = self.config.batch_size;
        let mut total = 0;
        if bs == 0 || records.len() <= bs {
            total += self.write_chunk(&mut state, records).await?;
        } else {
            for chunk in records.chunks(bs) {
                total += self.write_chunk(&mut state, chunk).await?;
            }
        }
        Ok(total)
    }

    /// delta-rs writes via `RecordBatchWriter`, so the sink consumes Arrow
    /// batches natively (#375): a `parquet → delta` / `delta → delta` chain
    /// never materializes `Value`.
    #[cfg(feature = "arrow")]
    fn supports_columnar(&self) -> bool {
        true
    }

    /// Write an Arrow batch straight into the buffered `RecordBatchWriter`,
    /// skipping the `Value → RecordBatch` encode. The schema is locked from the
    /// first batch's own schema (the columnar analogue of inferring it from the
    /// first records); partition columns must be present in the batch (the Delta
    /// source appends them), matching the JSON path's create-table contract.
    #[cfg(feature = "arrow")]
    async fn write_batch_columnar(&self, batch: &RecordBatch) -> Result<usize, FaucetError> {
        if batch.num_rows() == 0 {
            return Ok(0);
        }
        let mut state = self.state.lock().await;
        if state.schema.is_none() {
            state.schema = Some(batch.schema());
        }
        let schema = state.schema.clone().expect("schema set above");
        self.ensure_table_writer(&mut state, &schema).await?;
        let rows = batch.num_rows();
        let writer = state.writer.as_mut().expect("writer set");
        writer
            .write(batch.clone())
            .await
            .map_err(|e| FaucetError::Sink(format!("delta: columnar write failed: {e}")))?;
        state.pending = true;
        self.roll_if_over_target(&mut state).await?;
        Ok(rows)
    }

    async fn flush(&self) -> Result<(), FaucetError> {
        let mut state = self.state.lock().await;
        self.commit_buffered(&mut state).await
    }
}

/// Emit a one-shot warning for each field present in `records` but absent from
/// the locked `schema` (such fields are dropped by the JSON decoder).
fn warn_on_unknown_fields(
    warned_fields: &mut HashSet<String>,
    schema: &SchemaRef,
    records: &[Value],
) {
    for rec in records {
        if let Value::Object(map) = rec {
            for key in map.keys() {
                if schema.field_with_name(key).is_err() && warned_fields.insert(key.clone()) {
                    tracing::warn!(
                        field = %key,
                        "delta sink: dropping field not present in the table schema"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::Sink;

    #[tokio::test]
    async fn trait_metadata_methods() {
        let sink = DeltaSink::new(DeltaSinkConfig::new("file:///tmp/delta_meta"))
            .await
            .unwrap();
        assert_eq!(sink.connector_name(), "delta");
        assert_eq!(sink.dataset_uri(), "file:///tmp/delta_meta");
        assert_eq!(sink.supported_write_modes(), &[WriteMode::Append]);
        assert!(sink.config_schema().is_object());
    }

    #[tokio::test]
    async fn create_table_rejects_missing_partition_column() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().join("p").to_string_lossy().into_owned();
        let mut cfg = DeltaSinkConfig::new(&uri);
        cfg.partition_by = vec!["nope".into()];
        let sink = DeltaSink::new(cfg).await.unwrap();
        let err = sink
            .write_batch(&[serde_json::json!({"id": 1})])
            .await
            .unwrap_err();
        assert!(err.to_string().contains("partition column"), "{err}");
    }
}
