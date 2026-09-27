//! The Iceberg source: plans a run from the table's snapshot lineage, scans the
//! selected data files through iceberg-rust, and yields Arrow batches as rows
//! or columnar pages.

use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use arrow::array::RecordBatch;
use async_trait::async_trait;
use faucet_core::shard::{HashShard, ShardSpec, parse_hash_shard, plan_hash_shards};
use faucet_core::{FaucetError, Stream, StreamPage};
use futures::{StreamExt, TryStreamExt};
use iceberg::scan::{ArrowRecordBatchStream, FileScanTask};
use iceberg::spec::{DataContentType, ManifestContentType, ManifestStatus};
use iceberg::table::Table;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use serde_json::Value;
use tokio::sync::OnceCell;

use crate::config::{IcebergSourceConfig, ReadMode, parse_timestamp_ms};
use crate::convert::{batch_to_rows, descriptor, select_tasks};
use crate::filter::{self, Expr};
use crate::snapshots::{PlanOptions, SnapshotInfo, Step, bookmark_value, parse_bookmark, plan};

/// Unit the scan emits before it is shaped into row or columnar pages.
enum Event {
    Batch(RecordBatch),
    Checkpoint(i64),
}

type EventStream<'a> = Pin<Box<dyn Stream<Item = Result<Event, FaucetError>> + Send + 'a>>;

fn source_err(context: &str, e: impl std::fmt::Display) -> FaucetError {
    FaucetError::Source(format!("iceberg: {context}: {e}"))
}

/// A source that reads an Apache Iceberg table.
pub struct IcebergSource {
    config: IcebergSourceConfig,
    ident: TableIdent,
    filter: Option<Expr>,
    catalog: OnceCell<Arc<dyn Catalog>>,
    bookmark: Mutex<Option<i64>>,
    shard: Mutex<Option<HashShard>>,
}

impl IcebergSource {
    /// Build a source. Validates the config; the catalog is connected on first
    /// use so a preflight `check()` reports connection problems.
    pub async fn new(config: IcebergSourceConfig) -> Result<Self, FaucetError> {
        Self::build(config, OnceCell::new())
    }

    /// Build a source over an already-connected catalog.
    pub fn with_catalog(
        config: IcebergSourceConfig,
        catalog: Arc<dyn Catalog>,
    ) -> Result<Self, FaucetError> {
        Self::build(config, OnceCell::new_with(Some(catalog)))
    }

    fn build(
        config: IcebergSourceConfig,
        catalog: OnceCell<Arc<dyn Catalog>>,
    ) -> Result<Self, FaucetError> {
        config.validate()?;
        let ident = config.table_ident()?;
        let filter = config.filter.as_deref().map(filter::parse).transpose()?;
        Ok(Self {
            config,
            ident,
            filter,
            catalog,
            bookmark: Mutex::new(None),
            shard: Mutex::new(None),
        })
    }

    async fn catalog(&self) -> Result<Arc<dyn Catalog>, FaucetError> {
        self.catalog
            .get_or_try_init(|| faucet_common_iceberg::build_catalog(&self.config.catalog))
            .await
            .cloned()
    }

    async fn load_table(&self) -> Result<Table, FaucetError> {
        self.catalog()
            .await?
            .load_table(&self.ident)
            .await
            .map_err(|e| source_err(&format!("could not load table {}", self.ident), e))
    }

    fn plan_options(&self) -> Result<PlanOptions, FaucetError> {
        Ok(PlanOptions {
            mode: self.config.mode,
            on_rewrite: self.config.on_rewrite,
            on_expired: self.config.on_expired,
            snapshot_id: self.config.snapshot_id,
            as_of_ms: self
                .config
                .as_of_timestamp
                .as_deref()
                .map(parse_timestamp_ms)
                .transpose()?,
        })
    }

    fn batch_size(&self) -> Option<usize> {
        (self.config.batch_size > 0).then_some(self.config.batch_size)
    }

    /// Data files the append snapshot `snapshot_id` added.
    async fn files_added_by(
        &self,
        table: &Table,
        snapshot_id: i64,
    ) -> Result<HashSet<String>, FaucetError> {
        let snapshot = table
            .metadata()
            .snapshot_by_id(snapshot_id)
            .ok_or_else(|| source_err("snapshot vanished while planning", snapshot_id))?;
        let list = table
            .manifest_list_reader(snapshot)
            .load()
            .await
            .map_err(|e| source_err("could not read manifest list", e))?;
        let mut added = HashSet::new();
        for manifest in list.entries() {
            if manifest.content != ManifestContentType::Data
                || manifest.added_snapshot_id != snapshot_id
            {
                continue;
            }
            let loaded = manifest
                .load_manifest(table.file_io())
                .await
                .map_err(|e| source_err("could not read manifest", e))?;
            for entry in loaded.entries() {
                if entry.status() == ManifestStatus::Added
                    && entry.snapshot_id() == Some(snapshot_id)
                    && entry.content_type() == DataContentType::Data
                {
                    added.insert(entry.file_path().to_string());
                }
            }
        }
        Ok(added)
    }

    /// Scan one snapshot (optionally only the files in `added`).
    async fn scan(
        &self,
        table: &Table,
        snapshot_id: i64,
        added: Option<&HashSet<String>>,
    ) -> Result<ArrowRecordBatchStream, FaucetError> {
        let metadata = table.metadata();
        let snapshot = metadata
            .snapshot_by_id(snapshot_id)
            .ok_or_else(|| source_err("snapshot vanished while planning", snapshot_id))?;
        let schema = snapshot
            .schema(metadata)
            .map_err(|e| source_err("could not resolve the snapshot schema", e))?;

        let mut builder = table.scan().snapshot_id(snapshot_id);
        builder = if self.config.columns.is_empty() {
            builder.select_all()
        } else {
            builder.select(self.config.columns.iter())
        };
        if let Some(expr) = &self.filter {
            builder = builder.with_filter(filter::to_predicate(expr, &schema)?);
        }
        let scan = builder
            .with_batch_size(self.batch_size())
            .build()
            .map_err(|e| source_err("could not build the table scan", e))?;
        let tasks: Vec<FileScanTask> = scan
            .plan_files()
            .await
            .map_err(|e| source_err("could not plan the scan", e))?
            .try_collect()
            .await
            .map_err(|e| source_err("could not plan the scan", e))?;
        let shard = *self.shard.lock().expect("shard lock");
        let tasks = select_tasks(tasks, added, shard)?;
        tracing::debug!(
            snapshot_id,
            files = tasks.len(),
            table = %self.ident,
            "iceberg source scanning snapshot"
        );

        let mut reader = table
            .reader_builder()
            .with_data_file_concurrency_limit(self.config.concurrency);
        if let Some(n) = self.batch_size() {
            reader = reader.with_batch_size(n);
        }
        let result = reader
            .build()
            .read(futures::stream::iter(tasks.into_iter().map(Ok)).boxed())
            .map_err(|e| source_err("could not start reading data files", e))?;
        Ok(result.stream())
    }

    fn events(&self) -> EventStream<'_> {
        Box::pin(async_stream::try_stream! {
            let opts = self.plan_options()?;
            let table = self.load_table().await?;
            let metadata = table.metadata();
            let snapshots: Vec<SnapshotInfo> =
                metadata.snapshots().map(|s| SnapshotInfo::from(s.as_ref())).collect();
            let bookmark = *self.bookmark.lock().expect("bookmark lock");
            let steps = plan(&opts, &snapshots, metadata.current_snapshot_id(), bookmark)?;

            for step in steps {
                let (snapshot_id, added, checkpoint) = match step {
                    Step::Checkpoint(id) => {
                        yield Event::Checkpoint(id);
                        continue;
                    }
                    Step::Full { snapshot_id, checkpoint } => (snapshot_id, None, checkpoint),
                    Step::Append(id) => (id, Some(self.files_added_by(&table, id).await?), true),
                };
                let mut batches = self.scan(&table, snapshot_id, added.as_ref()).await?;
                while let Some(batch) = batches.next().await {
                    let batch = batch.map_err(|e| source_err("read error", e))?;
                    if batch.num_rows() > 0 {
                        yield Event::Batch(batch);
                    }
                }
                if checkpoint {
                    yield Event::Checkpoint(snapshot_id);
                }
            }
        })
    }
}

#[async_trait]
impl faucet_core::Source for IcebergSource {
    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(IcebergSourceConfig))
            .expect("schema serialization")
    }

    fn connector_name(&self) -> &'static str {
        "iceberg"
    }

    fn dataset_uri(&self) -> String {
        format!(
            "iceberg://{}/{}.{}",
            self.config.catalog.kind(),
            self.ident.namespace().join("."),
            self.ident.name()
        )
    }

    fn state_key(&self) -> Option<String> {
        (self.config.mode == ReadMode::Incremental).then(|| {
            format!(
                "iceberg:{}.{}",
                self.ident.namespace().join("."),
                self.ident.name()
            )
        })
    }

    async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
        let id = parse_bookmark(&bookmark)?;
        *self.bookmark.lock().expect("bookmark lock") = Some(id);
        Ok(())
    }

    fn is_shardable(&self) -> bool {
        true
    }

    async fn enumerate_shards(&self, target: usize) -> Result<Vec<ShardSpec>, FaucetError> {
        Ok(plan_hash_shards(target))
    }

    async fn apply_shard(&self, shard: &ShardSpec) -> Result<(), FaucetError> {
        *self.shard.lock().expect("shard lock") = parse_hash_shard(shard, "iceberg")?;
        Ok(())
    }

    fn supports_discover(&self) -> bool {
        true
    }

    async fn discover(&self) -> Result<Vec<faucet_core::discover::DatasetDescriptor>, FaucetError> {
        let catalog = self.catalog().await?;
        let mut queue: Vec<NamespaceIdent> = catalog
            .list_namespaces(None)
            .await
            .map_err(|e| source_err("could not list namespaces", e))?;
        let mut seen: HashSet<NamespaceIdent> = queue.iter().cloned().collect();
        let mut out = Vec::new();
        while let Some(ns) = queue.pop() {
            if let Ok(children) = catalog.list_namespaces(Some(&ns)).await {
                for child in children {
                    if seen.insert(child.clone()) {
                        queue.push(child);
                    }
                }
            }
            let tables = catalog
                .list_tables(&ns)
                .await
                .map_err(|e| source_err(&format!("could not list tables in {ns}"), e))?;
            for ident in tables {
                let table = catalog
                    .load_table(&ident)
                    .await
                    .map_err(|e| source_err(&format!("could not load table {ident}"), e))?;
                let metadata = table.metadata();
                let rows = metadata
                    .current_snapshot()
                    .and_then(|s| s.summary().additional_properties.get("total-records"))
                    .and_then(|v| v.parse().ok());
                out.push(descriptor(
                    ident.namespace().as_ref(),
                    ident.name(),
                    metadata.current_schema(),
                    rows,
                ));
            }
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};
        let started = std::time::Instant::now();
        let probe = match tokio::time::timeout(ctx.timeout, self.load_table()).await {
            Err(_) => Probe::fail_hint(
                "table",
                started.elapsed(),
                format!("iceberg source probe timed out after {:?}", ctx.timeout),
                "Check network reachability to the catalog endpoint.",
            ),
            Ok(Err(e)) => Probe::fail_hint(
                "table",
                started.elapsed(),
                e.to_string(),
                "Verify the catalog settings and that `table` exists.",
            ),
            Ok(Ok(table)) => {
                let schema = table.metadata().current_schema();
                let missing: Vec<&String> = self
                    .config
                    .columns
                    .iter()
                    .filter(|c| schema.field_by_name(c).is_none())
                    .collect();
                let filter = self
                    .filter
                    .as_ref()
                    .map(|e| filter::to_predicate(e, schema))
                    .transpose();
                match (missing.is_empty(), filter) {
                    (false, _) => Probe::fail_hint(
                        "table",
                        started.elapsed(),
                        format!("iceberg: `columns` not in the table schema: {missing:?}"),
                        "Fix the projection in `columns`.",
                    ),
                    (true, Err(e)) => Probe::fail_hint(
                        "table",
                        started.elapsed(),
                        e.to_string(),
                        "Fix the `filter` expression.",
                    ),
                    (true, Ok(_)) => Probe::pass("table", started.elapsed()),
                }
            }
        };
        Ok(CheckReport::single(probe))
    }

    async fn fetch_with_context(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        let mut out = Vec::new();
        let mut pages = self.stream_pages(context, self.config.batch_size);
        while let Some(page) = pages.next().await {
            out.extend(page?.records);
        }
        Ok(out)
    }

    fn stream_pages<'a>(
        &'a self,
        _context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let one_page = self.config.batch_size == 0;
        Box::pin(async_stream::try_stream! {
            let mut events = self.events();
            let mut buffered: Vec<Value> = Vec::new();
            while let Some(event) = events.next().await {
                match event? {
                    Event::Batch(batch) => {
                        let rows = batch_to_rows(&batch)?;
                        if one_page {
                            buffered.extend(rows);
                        } else {
                            yield StreamPage { records: rows, bookmark: None };
                        }
                    }
                    Event::Checkpoint(id) => {
                        yield StreamPage {
                            records: std::mem::take(&mut buffered),
                            bookmark: Some(bookmark_value(id)),
                        };
                    }
                }
            }
            if !buffered.is_empty() {
                yield StreamPage { records: buffered, bookmark: None };
            }
        })
    }

    /// Iceberg scans are Arrow-native, so the source joins the columnar fast
    /// path (#375) and an `iceberg → parquet` chain never materializes `Value`.
    #[cfg(feature = "arrow")]
    fn supports_columnar(&self) -> bool {
        true
    }

    #[cfg(feature = "arrow")]
    fn stream_batches<'a>(
        &'a self,
        _context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<faucet_core::ColumnarPage, FaucetError>> + Send + 'a>>
    {
        Box::pin(self.events().map(|event| {
            event.map(|e| match e {
                Event::Batch(batch) => faucet_core::ColumnarPage::new(batch, None),
                Event::Checkpoint(id) => faucet_core::ColumnarPage::new(
                    RecordBatch::new_empty(Arc::new(arrow::datatypes::Schema::empty())),
                    Some(bookmark_value(id)),
                ),
            })
        }))
    }
}
