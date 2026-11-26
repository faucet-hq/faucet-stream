//! The Iceberg source: plans a run from the table's snapshot lineage, scans the
//! selected data files through iceberg-rust, and yields Arrow batches as rows
//! or columnar pages.

use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use arrow::array::RecordBatch;
use async_trait::async_trait;
use faucet_common_iceberg::{StorageReload, credential_expiry_ms, storage_reload};
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
use crate::convert::{
    UuidColumns, batch_to_rows, descriptor, ensure_no_variant_projected, select_tasks,
};
use crate::filter::{self, Expr};
use crate::refresh::{self, Credentials, REFRESH_TIMEOUT, TaskQueue};
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

/// The wall clock in milliseconds since the Unix epoch, to compare with the
/// expiry a catalog sends with its credentials.
fn wall_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

use faucet_core::CallExt as _;

/// A catalog without nested namespaces: listing children of a namespace is
/// unsupported, or a REST catalog rejects the `parent` filter (400, 405 or
/// 501). Discovery treats that namespace as having no children.
fn nested_listing_unsupported(e: &iceberg::Error) -> bool {
    if e.kind() == iceberg::ErrorKind::FeatureUnsupported {
        return true;
    }
    // The REST client reports any other status as `Unexpected`, with the
    // status only in the error's context (rendered as `status: <code> …`).
    e.kind() == iceberg::ErrorKind::Unexpected && {
        let text = e.to_string();
        ["status: 400 ", "status: 405 ", "status: 501 "]
            .iter()
            .any(|s| text.contains(s))
    }
}

/// A source that reads an Apache Iceberg table.
pub struct IcebergSource {
    config: IcebergSourceConfig,
    ident: TableIdent,
    filter: Option<Expr>,
    catalog: OnceCell<Arc<dyn Catalog>>,
    bookmark: Mutex<Option<i64>>,
    shard: Mutex<Option<HashShard>>,
    /// Set once the table's storage was checked (#1108).
    preflighted: OnceCell<()>,
    /// Treat the storage credentials as vended whatever they are (tests).
    refresh_forced: bool,
    /// Backend-call context (#880): catalog calls, scan planning and every
    /// batch read run through it.
    calls: faucet_core::CallSlot,
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
            preflighted: OnceCell::new(),
            refresh_forced: false,
            calls: faucet_core::CallSlot::new(faucet_core::CallContext::new().without_timeout()),
        })
    }

    /// Refresh the table's storage during a read as if its credentials were
    /// vended, whatever they are. For tests of the refresh on a local table.
    #[doc(hidden)]
    pub fn with_storage_refresh_forced(mut self) -> Self {
        self.refresh_forced = true;
        self
    }

    async fn catalog(&self) -> Result<Arc<dyn Catalog>, FaucetError> {
        self.catalog
            .get_or_try_init(|| faucet_common_iceberg::build_catalog(&self.config.catalog))
            .await
            .cloned()
    }

    async fn load_table(&self) -> Result<Table, FaucetError> {
        self.calls
            .call_once("load", async {
                self.catalog()
                    .await?
                    .load_table(&self.ident)
                    .await
                    .map_err(|e| source_err(&format!("could not load table {}", self.ident), e))
            })
            .await
    }

    /// `faucet doctor`'s `storage` probe, bounded by `ctx.timeout`.
    async fn storage_probe(
        &self,
        ctx: &faucet_core::check::CheckContext,
        table: &Table,
    ) -> faucet_core::check::Probe {
        use faucet_common_iceberg::preflight::StorageVerdict;
        use faucet_core::check::Probe;
        let started = std::time::Instant::now();
        let hint = "Give the storage credentials: have the catalog vend them \
                    (catalog.access_delegation: vended_credentials) or set them in \
                    catalog.properties.";
        let checked = tokio::time::timeout(
            ctx.timeout,
            self.calls
                .call_once("probe", async { Ok(self.storage_verdict(table).await) }),
        )
        .await;
        match checked {
            Ok(Ok(StorageVerdict::Refuse(e))) => {
                Probe::fail_hint("storage", started.elapsed(), e.to_string(), hint)
            }
            Ok(Ok(StorageVerdict::Warn(w))) => {
                Probe::fail_hint("storage", started.elapsed(), w, hint)
            }
            Ok(Ok(_)) => Probe::pass("storage", started.elapsed()),
            Ok(Err(e)) => Probe::fail_hint("storage", started.elapsed(), e.to_string(), hint),
            Err(_) => Probe::fail_hint(
                "storage",
                started.elapsed(),
                format!("iceberg storage probe timed out after {:?}", ctx.timeout),
                "Check network reachability to the object store.",
            ),
        }
    }

    /// The table's storage checked against the configured credentials.
    async fn storage_verdict(
        &self,
        table: &Table,
    ) -> faucet_common_iceberg::preflight::StorageVerdict {
        let inner = self.config.catalog.inner();
        let expect = faucet_common_iceberg::preflight::StorageExpectation::new(
            &inner.properties,
            inner.access_delegation,
        );
        faucet_common_iceberg::preflight::check_table_storage(table, &expect).await
    }

    /// Check the table's storage once per instance, before the first page: a
    /// refusal fails the read, any other probe failure is a warning.
    async fn preflight(&self, table: &Table) -> Result<(), FaucetError> {
        use faucet_common_iceberg::preflight::StorageVerdict;
        self.preflighted
            .get_or_try_init(|| async {
                let verdict = self
                    .calls
                    .call_once("probe", async { Ok(self.storage_verdict(table).await) })
                    .await;
                match verdict {
                    Ok(StorageVerdict::Refuse(e)) => return Err(e),
                    Ok(StorageVerdict::Warn(w)) => tracing::warn!(table = %self.ident, "{w}"),
                    Ok(_) => {}
                    Err(e @ FaucetError::Cancelled { .. }) => return Err(e),
                    Err(e) => tracing::warn!(
                        table = %self.ident,
                        "iceberg: the storage check of table {} did not finish ({e}); the first \
                         read will show whether the storage works",
                        self.ident
                    ),
                }
                Ok(())
            })
            .await
            .map(|_| ())
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
            let loaded = table
                .manifest_reader()
                .read(manifest)
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

    /// The schema `snapshot_id` reads with.
    fn snapshot_schema(
        table: &Table,
        snapshot_id: i64,
    ) -> Result<iceberg::spec::SchemaRef, FaucetError> {
        let metadata = table.metadata();
        let snapshot = metadata
            .snapshot_by_id(snapshot_id)
            .ok_or_else(|| source_err("snapshot vanished while planning", snapshot_id))?;
        snapshot
            .schema(metadata)
            .map_err(|e| source_err("could not resolve the snapshot schema", e))
    }

    /// Whether the schema a run would read holds only columns the reader can
    /// decode: the snapshot the config selects in `mode: full`, the current
    /// schema in `mode: incremental` (each step is checked when the run starts).
    fn readable_schema(&self, table: &Table) -> Result<(), FaucetError> {
        let metadata = table.metadata();
        let snapshots: Vec<SnapshotInfo> = metadata
            .snapshots()
            .map(|s| SnapshotInfo::from(s.as_ref()))
            .collect();
        let selected = match self.config.mode {
            ReadMode::Incremental => None,
            ReadMode::Full => self
                .plan_options()
                .and_then(|opts| plan(&opts, &snapshots, metadata.current_snapshot_id(), None))
                .ok()
                .and_then(|steps| {
                    steps.into_iter().find_map(|step| match step {
                        Step::Full { snapshot_id, .. } => Some(snapshot_id),
                        _ => None,
                    })
                }),
        };
        let schema = match selected {
            Some(id) => Self::snapshot_schema(table, id)?,
            None => metadata.current_schema().clone(),
        };
        ensure_no_variant_projected(&schema, &self.config.columns)
    }

    /// Plan one snapshot (optionally only the files in `added`): the data
    /// files to read, with their delete files.
    async fn plan_tasks(
        &self,
        table: &Table,
        snapshot_id: i64,
        added: Option<&HashSet<String>>,
    ) -> Result<(Vec<FileScanTask>, UuidColumns), FaucetError> {
        let schema = Self::snapshot_schema(table, snapshot_id)?;
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
        let tasks: Vec<FileScanTask> = self
            .calls
            .call_once("plan", async {
                scan.plan_files()
                    .await
                    .map_err(|e| source_err("could not plan the scan", e))?
                    .try_collect()
                    .await
                    .map_err(|e| source_err("could not plan the scan", e))
            })
            .await?;
        let shard = *self.shard.lock().expect("shard lock");
        let tasks = select_tasks(tasks, added, shard)?;
        tracing::debug!(
            snapshot_id,
            files = tasks.len(),
            table = %self.ident,
            "iceberg source scanning snapshot"
        );
        Ok((tasks, UuidColumns::of(&schema)))
    }

    /// Read planned data files through `table`'s storage: the tasks `queue`
    /// hands out until `until`.
    fn read_tasks(
        &self,
        table: &Table,
        queue: TaskQueue,
        until: Option<tokio::time::Instant>,
    ) -> Result<ArrowRecordBatchStream, FaucetError> {
        let mut reader = table
            .reader_builder()
            .with_data_file_concurrency_limit(self.config.concurrency);
        if let Some(n) = self.batch_size() {
            reader = reader.with_batch_size(n);
        }
        let result = reader
            .build()
            .read(refresh::feed(queue, until))
            .map_err(|e| source_err("could not start reading data files", e))?;
        Ok(result.stream())
    }

    /// Whether `storage` (a table's storage properties) holds credentials the
    /// catalog vended.
    fn vended(&self, storage: &HashMap<String, String>) -> bool {
        self.refresh_forced
            || faucet_common_iceberg::holds_vended_credentials(
                storage,
                &self.config.catalog.inner().properties,
            )
    }

    /// How old `table`'s storage credentials are, counted from now.
    fn credentials_of(&self, table: &Table) -> Credentials {
        let storage = table.file_io().config().props();
        Credentials::new(
            self.vended(storage),
            std::time::Duration::from_secs(self.config.credential_refresh_secs),
            credential_expiry_ms(storage),
            tokio::time::Instant::now(),
        )
    }

    /// Take fresh storage credentials when they are due (#1117): load the
    /// table again and keep only its storage, so the plan and the schema of
    /// this read stay what they were. A table that is gone or was replaced
    /// fails the read; any other failure keeps the current credentials and is
    /// tried again later.
    async fn refresh_storage_if_due(
        &self,
        table: &mut Table,
        credentials: &mut Credentials,
    ) -> Result<(), FaucetError> {
        if !credentials.due(tokio::time::Instant::now(), wall_ms()) {
            return Ok(());
        }
        let loaded = self
            .calls
            .get()
            .with_timeout(REFRESH_TIMEOUT)
            .call_once("load", async {
                Ok(self.catalog().await?.load_table(&self.ident).await)
            })
            .await;
        let now = tokio::time::Instant::now();
        let why = match loaded {
            Err(e @ FaucetError::Cancelled { .. }) => return Err(e),
            Err(e) => e.to_string(),
            Ok(loaded) => match storage_reload(table, loaded) {
                StorageReload::Fresh(fresh) if !self.vended(fresh.file_io().config().props()) => {
                    "the catalog returned the table without vended credentials".to_string()
                }
                StorageReload::Fresh(fresh) => {
                    let storage = fresh.file_io().config().props();
                    credentials.refreshed(self.vended(storage), credential_expiry_ms(storage), now);
                    *table = fresh;
                    tracing::debug!(table = %self.ident, "iceberg: took fresh storage credentials");
                    return Ok(());
                }
                StorageReload::Replaced => {
                    return Err(FaucetError::Source(format!(
                        "iceberg: table {} was replaced by another table (uuid changed) while it \
                         was being read; the read stops here",
                        self.ident
                    )));
                }
                StorageReload::Gone => {
                    return Err(FaucetError::Source(format!(
                        "iceberg: table {} no longer exists in the catalog; it was dropped while \
                         it was being read",
                        self.ident
                    )));
                }
                StorageReload::Failed(why) => why,
            },
        };
        credentials.failed(now);
        tracing::warn!(
            table = %self.ident,
            "iceberg: could not take fresh storage credentials for table {} ({why}); reading on \
             with the current ones and trying again later",
            self.ident
        );
        Ok(())
    }

    fn events(&self) -> EventStream<'_> {
        Box::pin(async_stream::try_stream! {
            let opts = self.plan_options()?;
            let mut table = self.load_table().await?;
            self.preflight(&table).await?;
            let mut credentials = self.credentials_of(&table);
            let steps = {
                let metadata = table.metadata();
                let snapshots: Vec<SnapshotInfo> =
                    metadata.snapshots().map(|s| SnapshotInfo::from(s.as_ref())).collect();
                let bookmark = *self.bookmark.lock().expect("bookmark lock");
                plan(&opts, &snapshots, metadata.current_snapshot_id(), bookmark)?
            };
            // Checked for every step before the first page.
            for step in &steps {
                if let Step::Full { snapshot_id, .. } | Step::Append(snapshot_id) = step {
                    let schema = Self::snapshot_schema(&table, *snapshot_id)?;
                    ensure_no_variant_projected(&schema, &self.config.columns)?;
                }
            }

            for step in steps {
                let (snapshot_id, incremental, checkpoint) = match step {
                    Step::Checkpoint(id) => {
                        yield Event::Checkpoint(id);
                        continue;
                    }
                    Step::Full { snapshot_id, checkpoint } => (snapshot_id, false, checkpoint),
                    Step::Append(id) => (id, true, true),
                };
                self.refresh_storage_if_due(&mut table, &mut credentials).await?;
                let added = match incremental {
                    true => Some(
                        self.calls
                            .call_once("plan", self.files_added_by(&table, snapshot_id))
                            .await?,
                    ),
                    false => None,
                };
                let (tasks, uuids) = self.plan_tasks(&table, snapshot_id, added.as_ref()).await?;
                let queue = refresh::queue(tasks);
                let reads = self.calls.get().with_roundtrip_metering(false);
                // One reader per generation of credentials.
                while !refresh::is_empty(&queue) {
                    self.refresh_storage_if_due(&mut table, &mut credentials).await?;
                    let until = credentials.due_at(tokio::time::Instant::now(), wall_ms());
                    let mut batches = self.read_tasks(&table, queue.clone(), until)?;
                    while let Some(batch) = batches.next().within_ctx(&reads, "read").await? {
                        let batch = batch.map_err(|e| source_err("read error", e))?;
                        if batch.num_rows() > 0 {
                            yield Event::Batch(uuids.apply(batch)?);
                        }
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

    fn set_call_context(&self, ctx: faucet_core::io::CallContext) {
        self.calls.install(ctx);
    }

    fn set_roundtrip_recorder(
        &self,
        recorder: std::sync::Arc<faucet_core::observability::RoundtripRecorder>,
    ) {
        self.calls.install_recorder(recorder);
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
        let catalog = self.calls.call_once("catalog", self.catalog()).await?;
        let mut queue: Vec<NamespaceIdent> = catalog
            .list_namespaces(None)
            .within(&self.calls, "list")
            .await?
            .map_err(|e| source_err("could not list namespaces", e))?;
        let mut seen: HashSet<NamespaceIdent> = queue.iter().cloned().collect();
        let mut out = Vec::new();
        let mut warned_nested = false;
        while let Some(ns) = queue.pop() {
            match catalog
                .list_namespaces(Some(&ns))
                .within(&self.calls, "list")
                .await?
            {
                Ok(children) => {
                    for child in children {
                        if seen.insert(child.clone()) {
                            queue.push(child);
                        }
                    }
                }
                // Dropped while it was being listed: no tables to list either.
                Err(e) if e.kind() == iceberg::ErrorKind::NamespaceNotFound => continue,
                Err(e) if nested_listing_unsupported(&e) => {
                    if !warned_nested {
                        warned_nested = true;
                        tracing::warn!(
                            namespace = %ns.to_url_string(),
                            error = %e,
                            "iceberg: the catalog does not list nested namespaces; discovering \
                             only the top-level ones"
                        );
                    }
                }
                Err(e) => {
                    return Err(source_err(
                        &format!("could not list the namespaces under {}", ns.to_url_string()),
                        e,
                    ));
                }
            }
            let tables = catalog
                .list_tables(&ns)
                .within(&self.calls, "list")
                .await?
                .map_err(|e| source_err(&format!("could not list tables in {ns}"), e))?;
            for ident in tables {
                let table = catalog
                    .load_table(&ident)
                    .within(&self.calls, "load")
                    .await?
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
        let mut loaded = None;
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
                    (true, Ok(_)) => match self.readable_schema(&table) {
                        Err(e) => Probe::fail_hint(
                            "table",
                            started.elapsed(),
                            e.to_string(),
                            "List the columns to read in `columns`.",
                        ),
                        Ok(()) => {
                            loaded = Some(table.clone());
                            Probe::pass("table", started.elapsed())
                        }
                    },
                }
            }
        };
        let mut report = CheckReport::single(probe);
        if let Some(table) = loaded {
            report.probes.push(self.storage_probe(ctx, &table).await);
        }
        Ok(report)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_an_unsupported_or_rejected_parent_listing_means_no_nested_namespaces() {
        use iceberg::{Error, ErrorKind};
        let status = |code: &str| {
            Error::new(ErrorKind::Unexpected, "unexpected status").with_context("status", code)
        };
        assert!(nested_listing_unsupported(&Error::new(
            ErrorKind::FeatureUnsupported,
            "x"
        )));
        for code in [
            "400 Bad Request",
            "405 Method Not Allowed",
            "501 Not Implemented",
        ] {
            assert!(nested_listing_unsupported(&status(code)), "{code}");
        }
        assert!(!nested_listing_unsupported(&status("403 Forbidden")));
        assert!(!nested_listing_unsupported(&status(
            "500 Internal Server Error"
        )));
        // A vanished namespace is handled on its own (its tables are skipped).
        assert!(!nested_listing_unsupported(&Error::new(
            ErrorKind::NamespaceNotFound,
            "x"
        )));
    }
}
