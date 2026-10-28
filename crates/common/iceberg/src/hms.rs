//! A Hive Metastore catalog that can commit.
//!
//! `iceberg-catalog-hms` 0.10 creates, loads and drops tables but its
//! `update_table` returns `FeatureUnsupported`, so no snapshot can be
//! committed through it. [`HmsCommitCatalog`] wraps it, delegates every other
//! operation, and commits the way Iceberg's Java `HiveTableOperations` does on
//! a Hive 3 metastore:
//!
//! 1. take an `EXCLUSIVE` table lock (`lock`, then `check_lock` while it
//!    waits);
//! 2. load the table, check the commit's requirements against it and apply its
//!    updates, giving new metadata at `metadata/<version+1>-<uuid>.metadata.json`;
//! 3. write that file through the table's `FileIO`;
//! 4. re-read the metastore entry and refuse unless its `metadata_location`
//!    is still the one the commit was based on (the optimistic check);
//! 5. `alter_table` the entry to the new `metadata_location`, recording the
//!    old one as `previous_metadata_location`;
//! 6. `unlock`.
//!
//! A commit that loses (another writer moved the table, the lock was refused
//! or never granted, a requirement no longer holds) fails with a retryable
//! `CatalogCommitConflicts`, so iceberg's `Transaction::commit` reloads the
//! table, rebuilds the commit and tries again under the table's
//! `commit.retry.*` properties. A failure after `alter_table` was sent but not
//! answered is ambiguous and is never retried: retrying could append the same
//! data files twice.
//!
//! Hive 4 removed `get_table`, which the wrapped client calls, so a Hive 3.x
//! metastore is required.

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::net::ToSocketAddrs;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use faucet_core::FaucetError;
use hive_metastore::{
    CheckLockRequest, DataOperationType, EnvironmentContext, FieldSchema, LockComponent, LockLevel,
    LockRequest, LockResponse, LockState, LockType, ThriftHiveMetastoreClient,
    ThriftHiveMetastoreClientBuilder, UnlockRequest,
};
use iceberg::io::StorageFactory;
use iceberg::spec::{PrimitiveType, Schema, Type};
use iceberg::table::Table;
use iceberg::{
    Catalog, CatalogBuilder, Error, ErrorKind, MetadataLocation, Namespace, NamespaceIdent, Result,
    TableCommit, TableCreation, TableIdent,
};
use iceberg_catalog_hms::{
    HMS_CATALOG_PROP_THRIFT_TRANSPORT, HMS_CATALOG_PROP_URI, HmsCatalog, HmsCatalogBuilder,
    THRIFT_TRANSPORT_FRAMED,
};
use pilota::{AHashMap, FastStr};
use volo_thrift::MaybeException;

/// Catalog property: how long a commit waits for the metastore table lock, in
/// milliseconds (default 180000, as Java's `iceberg.hive.lock-timeout-ms`).
pub const HMS_LOCK_TIMEOUT_PROP: &str = "hms.lock-timeout-ms";

const DEFAULT_LOCK_TIMEOUT: Duration = Duration::from_secs(180);
const LOCK_CHECK_MIN_WAIT: Duration = Duration::from_millis(50);
const LOCK_CHECK_MAX_WAIT: Duration = Duration::from_secs(5);

const METADATA_LOCATION: &str = "metadata_location";
const PREVIOUS_METADATA_LOCATION: &str = "previous_metadata_location";
const DO_NOT_UPDATE_STATS: &str = "DO_NOT_UPDATE_STATS";
const AGENT: &str = "faucet-iceberg";

/// The HMS catalog with a working [`Catalog::update_table`].
pub struct HmsCommitCatalog {
    inner: HmsCatalog,
    client: ThriftHiveMetastoreClient,
    lock_timeout: Duration,
}

impl Debug for HmsCommitCatalog {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HmsCommitCatalog")
            .field("inner", &self.inner)
            .field("lock_timeout", &self.lock_timeout)
            .finish_non_exhaustive()
    }
}

impl HmsCommitCatalog {
    /// Build the catalog from HMS catalog properties (`uri` = `host:port`,
    /// `warehouse`, `thrift_transport`, [`HMS_LOCK_TIMEOUT_PROP`], FileIO
    /// properties) and a storage factory.
    pub async fn load(
        name: &str,
        mut props: HashMap<String, String>,
        storage_factory: Arc<dyn StorageFactory>,
    ) -> std::result::Result<Self, FaucetError> {
        let lock_timeout = lock_timeout(props.remove(HMS_LOCK_TIMEOUT_PROP).as_deref())?;
        let address = props.get(HMS_CATALOG_PROP_URI).cloned().unwrap_or_default();
        let framed = props
            .get(HMS_CATALOG_PROP_THRIFT_TRANSPORT)
            .is_some_and(|t| t.eq_ignore_ascii_case(THRIFT_TRANSPORT_FRAMED));
        let inner = HmsCatalogBuilder::default()
            .with_storage_factory(storage_factory)
            .load(name, props)
            .await
            .map_err(|e| FaucetError::Config(format!("iceberg: HMS catalog init failed: {e}")))?;
        let client = metastore_client(&address, framed)?;
        Ok(Self {
            inner,
            client,
            lock_timeout,
        })
    }

    async fn acquire_lock(&self, db: &str, table: &str) -> Result<MetastoreLock> {
        let response = thrift(
            self.client.lock(lock_request(db, table)).await,
            "lock the table",
        )?;
        let lock = MetastoreLock {
            client: self.client.clone(),
            id: Some(response.lockid),
        };
        let started = Instant::now();
        let mut state = response.state;
        let mut wait = LOCK_CHECK_MIN_WAIT;
        loop {
            match lock_step(state) {
                LockStep::Acquired => return Ok(lock),
                LockStep::Refused => {
                    lock.release().await;
                    return Err(conflict(format!(
                        "the metastore refused the lock on {db}.{table} ({})",
                        state.to_string()
                    )));
                }
                LockStep::Wait => {}
            }
            if started.elapsed() >= self.lock_timeout {
                lock.release().await;
                return Err(conflict(format!(
                    "timed out after {:?} waiting for the metastore lock on {db}.{table}",
                    self.lock_timeout
                )));
            }
            pause(wait).await;
            wait = (wait * 2).min(LOCK_CHECK_MAX_WAIT);
            let check = CheckLockRequest {
                lockid: response.lockid,
                txnid: None,
                elapsed_ms: None,
            };
            let checked: LockResponse =
                thrift(self.client.check_lock(check).await, "check the table lock")?;
            state = checked.state;
        }
    }

    async fn commit_locked(&self, db: &str, name: &str, commit: TableCommit) -> Result<Table> {
        let ident = commit.identifier().clone();
        let base = self.inner.load_table(&ident).await?;
        let base_location = base.metadata_location_result()?.to_string();
        let (staged, staged_location) = stage(commit, base).await?;

        let mut entry = match self.get_entry(db, name).await {
            Ok(entry) => entry,
            Err(e) => {
                discard(&staged, &staged_location).await;
                return Err(e);
            }
        };
        if let Err(e) = check_unchanged(&base_location, &entry.parameters) {
            discard(&staged, &staged_location).await;
            return Err(e);
        }
        set_commit_parameters(&mut entry.parameters, &staged_location, &base_location);
        if let Some(sd) = entry.sd.as_mut() {
            sd.cols = Some(hive_columns(staged.metadata().current_schema()));
        }

        let context = EnvironmentContext {
            properties: Some(AHashMap::from([(
                FastStr::from_static_str(DO_NOT_UPDATE_STATS),
                FastStr::from_static_str("true"),
            )])),
        };
        match self
            .client
            .alter_table_with_environment_context(
                db.to_string().into(),
                name.to_string().into(),
                entry,
                context,
            )
            .await
        {
            Ok(MaybeException::Ok(())) => Ok(staged),
            Ok(MaybeException::Exception(e)) => {
                discard(&staged, &staged_location).await;
                Err(Error::new(
                    ErrorKind::Unexpected,
                    format!("the metastore refused the commit of {ident}: {e:?}"),
                ))
            }
            Err(e) => Err(Error::new(
                ErrorKind::Unexpected,
                format!(
                    "the commit of {ident} may or may not have landed: alter_table got no \
                     answer ({e}); not retried"
                ),
            )),
        }
    }

    async fn get_entry(&self, db: &str, name: &str) -> Result<hive_metastore::Table> {
        thrift(
            self.client
                .get_table(db.to_string().into(), name.to_string().into())
                .await,
            "read the table entry",
        )
    }
}

#[faucet_core::async_trait]
impl Catalog for HmsCommitCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(parent).await
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        self.inner.create_namespace(namespace, properties).await
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        self.inner.get_namespace(namespace).await
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool> {
        self.inner.namespace_exists(namespace).await
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        self.inner.update_namespace(namespace, properties).await
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        self.inner.drop_namespace(namespace).await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        self.inner.list_tables(namespace).await
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        self.inner.create_table(namespace, creation).await
    }

    async fn load_table(&self, table: &TableIdent) -> Result<Table> {
        self.inner.load_table(table).await
    }

    async fn drop_table(&self, table: &TableIdent) -> Result<()> {
        self.inner.drop_table(table).await
    }

    async fn purge_table(&self, table: &TableIdent) -> Result<()> {
        self.inner.purge_table(table).await
    }

    async fn table_exists(&self, table: &TableIdent) -> Result<bool> {
        self.inner.table_exists(table).await
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> Result<()> {
        self.inner.rename_table(src, dest).await
    }

    async fn register_table(&self, table: &TableIdent, metadata_location: String) -> Result<Table> {
        self.inner.register_table(table, metadata_location).await
    }

    async fn update_table(&self, commit: TableCommit) -> Result<Table> {
        let (db, name) = hive_names(commit.identifier())?;
        let lock = self.acquire_lock(&db, &name).await?;
        let outcome = self.commit_locked(&db, &name, commit).await;
        lock.release().await;
        outcome
    }
}

/// Check `commit`'s requirements against `base`, apply its updates and write
/// the resulting metadata file; returns the staged table and that file's
/// location (`<table>/metadata/<version+1>-<uuid>.metadata.json`).
async fn stage(commit: TableCommit, base: Table) -> Result<(Table, String)> {
    let staged = commit.apply(base)?;
    let location = staged.metadata_location_result()?.to_string();
    staged
        .metadata()
        .write_to(staged.file_io(), &MetadataLocation::from_str(&location)?)
        .await?;
    Ok((staged, location))
}

/// An acquired (or requested) metastore lock. Released explicitly; a guard
/// dropped while still holding it (the commit future was cancelled) releases
/// it on a spawned task so it does not block other writers until it expires.
struct MetastoreLock {
    client: ThriftHiveMetastoreClient,
    id: Option<i64>,
}

impl MetastoreLock {
    async fn release(mut self) {
        if let Some(id) = self.id.take() {
            unlock(&self.client, id).await;
        }
    }
}

impl Drop for MetastoreLock {
    fn drop(&mut self) {
        if let Some(id) = self.id.take()
            && let Ok(handle) = tokio::runtime::Handle::try_current()
        {
            let client = self.client.clone();
            handle.spawn(async move { unlock(&client, id).await });
        }
    }
}

async fn unlock(client: &ThriftHiveMetastoreClient, id: i64) {
    match client.unlock(UnlockRequest { lockid: id }).await {
        Ok(MaybeException::Ok(())) => {}
        Ok(MaybeException::Exception(e)) => {
            tracing::warn!(lock = id, "iceberg: the metastore refused to unlock: {e:?}");
        }
        Err(e) => tracing::warn!(
            lock = id,
            "iceberg: unlocking the metastore lock failed: {e}"
        ),
    }
}

/// Best-effort removal of a metadata file no metastore entry will point at.
async fn discard(staged: &Table, location: &str) {
    if let Err(e) = staged.file_io().delete(location).await {
        tracing::warn!("iceberg: could not delete the unused metadata file {location}: {e}");
    }
}

#[allow(clippy::disallowed_methods)] // lock poll inside a commit that runs under the sink's CallSlot
async fn pause(d: Duration) {
    tokio::time::sleep(d).await;
}

fn metastore_client(
    address: &str,
    framed: bool,
) -> std::result::Result<ThriftHiveMetastoreClient, FaucetError> {
    let socket = address
        .to_socket_addrs()
        .map_err(|e| FaucetError::Config(format!("iceberg: HMS address {address:?}: {e}")))?
        .next()
        .ok_or_else(|| {
            FaucetError::Config(format!(
                "iceberg: HMS address {address:?} resolves to nothing"
            ))
        })?;
    let builder = ThriftHiveMetastoreClientBuilder::new("hms").address(socket);
    Ok(if framed {
        builder
            .make_codec(volo_thrift::codec::default::DefaultMakeCodec::framed())
            .build()
    } else {
        builder
            .make_codec(volo_thrift::codec::default::DefaultMakeCodec::buffered())
            .build()
    })
}

fn lock_timeout(value: Option<&str>) -> std::result::Result<Duration, FaucetError> {
    match value {
        None => Ok(DEFAULT_LOCK_TIMEOUT),
        Some(v) => v
            .trim()
            .parse::<u64>()
            .ok()
            .filter(|ms| *ms > 0)
            .map(Duration::from_millis)
            .ok_or_else(|| {
                FaucetError::Config(format!(
                    "iceberg: catalog property {HMS_LOCK_TIMEOUT_PROP} must be a positive \
                     number of milliseconds, got {v:?}"
                ))
            }),
    }
}

fn lock_request(db: &str, table: &str) -> LockRequest {
    let user = std::env::var("USER").unwrap_or_else(|_| AGENT.to_string());
    let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "localhost".to_string());
    LockRequest {
        component: vec![LockComponent {
            r#type: LockType::EXCLUSIVE,
            level: LockLevel::TABLE,
            dbname: db.to_string().into(),
            tablename: Some(table.to_string().into()),
            operation_type: Some(DataOperationType::NO_TXN),
            ..Default::default()
        }],
        txnid: None,
        user: user.into(),
        hostname: host.into(),
        agent_info: Some(FastStr::from_static_str(AGENT)),
    }
}

#[derive(Debug, PartialEq, Eq)]
enum LockStep {
    Acquired,
    Wait,
    Refused,
}

fn lock_step(state: LockState) -> LockStep {
    match state {
        LockState::ACQUIRED => LockStep::Acquired,
        LockState::WAITING => LockStep::Wait,
        _ => LockStep::Refused,
    }
}

fn hive_names(ident: &TableIdent) -> Result<(String, String)> {
    match ident.namespace().as_ref().as_slice() {
        [db] if !db.is_empty() => Ok((db.clone(), ident.name().to_string())),
        _ => Err(Error::new(
            ErrorKind::DataInvalid,
            format!("a Hive Metastore table needs a one-level namespace, got {ident}"),
        )),
    }
}

fn conflict(message: String) -> Error {
    Error::new(ErrorKind::CatalogCommitConflicts, message).with_retryable(true)
}

/// The optimistic check: the entry must still point at the metadata the
/// commit was built on.
fn check_unchanged(expected: &str, params: &Option<AHashMap<FastStr, FastStr>>) -> Result<()> {
    let current = params
        .as_ref()
        .and_then(|p| p.get(METADATA_LOCATION))
        .map(|v| v.as_str());
    if current == Some(expected) {
        Ok(())
    } else {
        Err(conflict(format!(
            "the table moved from {expected} to {} while committing",
            current.unwrap_or("<no metadata_location>")
        )))
    }
}

fn set_commit_parameters(
    params: &mut Option<AHashMap<FastStr, FastStr>>,
    new_location: &str,
    previous_location: &str,
) {
    let params = params.get_or_insert_with(AHashMap::default);
    params.insert(
        FastStr::from_static_str(METADATA_LOCATION),
        new_location.to_string().into(),
    );
    params.insert(
        FastStr::from_static_str(PREVIOUS_METADATA_LOCATION),
        previous_location.to_string().into(),
    );
}

/// The Hive column list for an Iceberg schema (top-level fields; nested types
/// rendered as Hive type strings).
fn hive_columns(schema: &Schema) -> Vec<FieldSchema> {
    schema
        .as_struct()
        .fields()
        .iter()
        .map(|f| FieldSchema {
            name: Some(f.name.clone().into()),
            r#type: Some(hive_type(&f.field_type).into()),
            comment: f.doc.clone().map(Into::into),
        })
        .collect()
}

fn hive_type(t: &Type) -> String {
    match t {
        Type::Primitive(p) => match p {
            PrimitiveType::Boolean => "boolean".into(),
            PrimitiveType::Int => "int".into(),
            PrimitiveType::Long => "bigint".into(),
            PrimitiveType::Float => "float".into(),
            PrimitiveType::Double => "double".into(),
            PrimitiveType::Date => "date".into(),
            PrimitiveType::Timestamp | PrimitiveType::TimestampNs => "timestamp".into(),
            PrimitiveType::Timestamptz | PrimitiveType::TimestamptzNs => {
                "timestamp with local time zone".into()
            }
            PrimitiveType::Time | PrimitiveType::String | PrimitiveType::Uuid => "string".into(),
            PrimitiveType::Binary | PrimitiveType::Fixed(_) => "binary".into(),
            PrimitiveType::Decimal { precision, scale } => format!("decimal({precision},{scale})"),
        },
        Type::Struct(s) => format!(
            "struct<{}>",
            s.fields()
                .iter()
                .map(|f| format!("{}:{}", f.name, hive_type(&f.field_type)))
                .collect::<Vec<_>>()
                .join(",")
        ),
        Type::List(l) => format!("array<{}>", hive_type(&l.element_field.field_type)),
        Type::Map(m) => format!(
            "map<{},{}>",
            hive_type(&m.key_field.field_type),
            hive_type(&m.value_field.field_type)
        ),
    }
}

fn thrift<T, E: Debug>(
    outcome: std::result::Result<MaybeException<T, E>, volo_thrift::ClientError>,
    what: &str,
) -> Result<T> {
    match outcome {
        Ok(MaybeException::Ok(v)) => Ok(v),
        Ok(MaybeException::Exception(e)) => Err(Error::new(
            ErrorKind::Unexpected,
            format!("hive metastore: could not {what}: {e:?}"),
        )),
        Err(e) => Err(Error::new(
            ErrorKind::Unexpected,
            format!("hive metastore: could not {what}: {e}"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use iceberg::io::{FileIOBuilder, LocalFsStorageFactory};
    use iceberg::spec::TableMetadata;
    use iceberg::spec::{ListType, MapType, NestedField, StructType, TableMetadataBuilder};
    use iceberg::transaction::{ApplyTransactionAction, Transaction};

    use super::*;

    fn schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap()
    }

    async fn local_table(dir: &std::path::Path) -> Table {
        let creation = TableCreation::builder()
            .name("events".to_string())
            .location(format!("file://{}", dir.display()))
            .schema(schema())
            .build();
        let metadata = TableMetadataBuilder::from_table_creation(creation)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let location = MetadataLocation::new_with_metadata(metadata.location(), &metadata);
        let io = FileIOBuilder::new(Arc::new(LocalFsStorageFactory)).build();
        metadata.write_to(&io, &location).await.unwrap();
        Table::builder()
            .file_io(io)
            .metadata_location(location.to_string())
            .metadata(metadata)
            .identifier(TableIdent::from_strs(["db", "events"]).unwrap())
            .runtime(iceberg::Runtime::try_current().unwrap())
            .build()
            .unwrap()
    }

    /// Stages every commit it is handed against the table it serves.
    #[derive(Debug)]
    struct StagingCatalog(Table);

    #[faucet_core::async_trait]
    impl Catalog for StagingCatalog {
        async fn list_namespaces(&self, _: Option<&NamespaceIdent>) -> Result<Vec<NamespaceIdent>> {
            unimplemented!()
        }
        async fn create_namespace(
            &self,
            _: &NamespaceIdent,
            _: HashMap<String, String>,
        ) -> Result<Namespace> {
            unimplemented!()
        }
        async fn get_namespace(&self, _: &NamespaceIdent) -> Result<Namespace> {
            unimplemented!()
        }
        async fn namespace_exists(&self, _: &NamespaceIdent) -> Result<bool> {
            unimplemented!()
        }
        async fn update_namespace(
            &self,
            _: &NamespaceIdent,
            _: HashMap<String, String>,
        ) -> Result<()> {
            unimplemented!()
        }
        async fn drop_namespace(&self, _: &NamespaceIdent) -> Result<()> {
            unimplemented!()
        }
        async fn list_tables(&self, _: &NamespaceIdent) -> Result<Vec<TableIdent>> {
            unimplemented!()
        }
        async fn create_table(&self, _: &NamespaceIdent, _: TableCreation) -> Result<Table> {
            unimplemented!()
        }
        async fn load_table(&self, _: &TableIdent) -> Result<Table> {
            Ok(self.0.clone())
        }
        async fn drop_table(&self, _: &TableIdent) -> Result<()> {
            unimplemented!()
        }
        async fn purge_table(&self, _: &TableIdent) -> Result<()> {
            unimplemented!()
        }
        async fn table_exists(&self, _: &TableIdent) -> Result<bool> {
            unimplemented!()
        }
        async fn rename_table(&self, _: &TableIdent, _: &TableIdent) -> Result<()> {
            unimplemented!()
        }
        async fn register_table(&self, _: &TableIdent, _: String) -> Result<Table> {
            unimplemented!()
        }
        async fn update_table(&self, commit: TableCommit) -> Result<Table> {
            Ok(stage(commit, self.0.clone()).await?.0)
        }
    }

    #[tokio::test]
    async fn a_staged_commit_writes_the_next_metadata_version() {
        let dir = tempfile::tempdir().unwrap();
        let table = local_table(dir.path()).await;
        let base = table.metadata_location().unwrap().to_string();
        assert!(base.contains("/metadata/00000-"), "{base}");
        let catalog = StagingCatalog(table.clone());
        let tx = Transaction::new(&table);
        let tx = tx
            .update_table_properties()
            .set("k".to_string(), "v".to_string())
            .apply(tx)
            .unwrap();
        let staged = tx.commit(&catalog).await.unwrap();
        let next = staged.metadata_location().unwrap();
        let prefix = format!("file://{}/metadata/00001-", dir.path().display());
        assert!(next.starts_with(&prefix), "{next}");
        assert!(next.ends_with(".metadata.json"), "{next}");
        assert_eq!(
            staged.metadata().properties().get("k").map(String::as_str),
            Some("v")
        );
        let written = TableMetadata::read_from(staged.file_io(), next)
            .await
            .unwrap();
        assert_eq!(written.properties().get("k").map(String::as_str), Some("v"));
        assert_eq!(written.uuid(), table.metadata().uuid());
    }

    #[test]
    fn the_optimistic_check_passes_only_on_the_base_location() {
        let params = Some(AHashMap::from([(
            FastStr::from_static_str(METADATA_LOCATION),
            FastStr::from_static_str("s3://b/t/metadata/00001-a.metadata.json"),
        )]));
        assert!(check_unchanged("s3://b/t/metadata/00001-a.metadata.json", &params).is_ok());

        let moved =
            check_unchanged("s3://b/t/metadata/00000-z.metadata.json", &params).unwrap_err();
        assert_eq!(moved.kind(), ErrorKind::CatalogCommitConflicts);
        assert!(moved.retryable());
        assert!(moved.to_string().contains("00001-a"), "{moved}");

        let missing = check_unchanged("x", &None).unwrap_err();
        assert!(missing.retryable());
        assert!(
            missing.to_string().contains("<no metadata_location>"),
            "{missing}"
        );
    }

    #[test]
    fn commit_parameters_point_at_the_new_file_and_keep_the_old() {
        let mut params = Some(AHashMap::from([
            (
                FastStr::from_static_str("EXTERNAL"),
                FastStr::from_static_str("TRUE"),
            ),
            (
                FastStr::from_static_str(METADATA_LOCATION),
                FastStr::from_static_str("old"),
            ),
        ]));
        set_commit_parameters(&mut params, "new", "old");
        let p = params.unwrap();
        assert_eq!(p.get(METADATA_LOCATION).map(|v| v.as_str()), Some("new"));
        assert_eq!(
            p.get(PREVIOUS_METADATA_LOCATION).map(|v| v.as_str()),
            Some("old")
        );
        assert_eq!(p.get("EXTERNAL").map(|v| v.as_str()), Some("TRUE"));

        let mut empty = None;
        set_commit_parameters(&mut empty, "n", "o");
        assert_eq!(empty.unwrap().len(), 2);
    }

    #[test]
    fn lock_states_map_to_steps() {
        assert_eq!(lock_step(LockState::ACQUIRED), LockStep::Acquired);
        assert_eq!(lock_step(LockState::WAITING), LockStep::Wait);
        assert_eq!(lock_step(LockState::ABORT), LockStep::Refused);
        assert_eq!(lock_step(LockState::NOT_ACQUIRED), LockStep::Refused);
    }

    #[test]
    fn the_lock_request_is_one_exclusive_table_lock() {
        let req = lock_request("db", "events");
        assert_eq!(req.component.len(), 1);
        let c = &req.component[0];
        assert_eq!(c.r#type, LockType::EXCLUSIVE);
        assert_eq!(c.level, LockLevel::TABLE);
        assert_eq!(c.dbname.as_str(), "db");
        assert_eq!(c.tablename.as_deref(), Some("events"));
        assert_eq!(c.operation_type, Some(DataOperationType::NO_TXN));
        assert_eq!(req.txnid, None);
        assert_eq!(req.agent_info.as_deref(), Some(AGENT));
        assert!(!req.user.is_empty());
    }

    #[test]
    fn hive_names_need_one_namespace_level() {
        let ok = TableIdent::from_strs(["db", "t"]).unwrap();
        assert_eq!(
            hive_names(&ok).unwrap(),
            ("db".to_string(), "t".to_string())
        );
        let nested = TableIdent::from_strs(["a", "b", "t"]).unwrap();
        let err = hive_names(&nested).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(!err.retryable());
    }

    #[test]
    fn lock_timeout_reads_milliseconds() {
        assert_eq!(lock_timeout(None).unwrap(), DEFAULT_LOCK_TIMEOUT);
        assert_eq!(
            lock_timeout(Some(" 2500 ")).unwrap(),
            Duration::from_millis(2500)
        );
        for bad in ["0", "-1", "soon"] {
            let err = lock_timeout(Some(bad)).unwrap_err();
            assert!(err.to_string().contains(HMS_LOCK_TIMEOUT_PROP), "{err}");
        }
    }

    #[test]
    fn hive_columns_render_every_type() {
        let fields = vec![
            NestedField::required(1, "b", Type::Primitive(PrimitiveType::Boolean)),
            NestedField::required(2, "i", Type::Primitive(PrimitiveType::Int)),
            NestedField::required(3, "l", Type::Primitive(PrimitiveType::Long)),
            NestedField::required(4, "f", Type::Primitive(PrimitiveType::Float)),
            NestedField::required(5, "d", Type::Primitive(PrimitiveType::Double)),
            NestedField::required(6, "dt", Type::Primitive(PrimitiveType::Date)),
            NestedField::required(7, "ts", Type::Primitive(PrimitiveType::Timestamp)),
            NestedField::required(8, "tz", Type::Primitive(PrimitiveType::Timestamptz)),
            NestedField::required(9, "s", Type::Primitive(PrimitiveType::String)),
            NestedField::required(10, "u", Type::Primitive(PrimitiveType::Uuid)),
            NestedField::required(11, "bin", Type::Primitive(PrimitiveType::Fixed(4))),
            NestedField::required(
                12,
                "dec",
                Type::Primitive(PrimitiveType::Decimal {
                    precision: 10,
                    scale: 2,
                }),
            ),
            NestedField::required(
                13,
                "st",
                Type::Struct(StructType::new(vec![
                    NestedField::required(14, "x", Type::Primitive(PrimitiveType::Int)).into(),
                ])),
            ),
            NestedField::required(
                15,
                "arr",
                Type::List(ListType::new(
                    NestedField::list_element(16, Type::Primitive(PrimitiveType::String), true)
                        .into(),
                )),
            ),
            NestedField::required(
                17,
                "m",
                Type::Map(MapType::new(
                    NestedField::map_key_element(18, Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::map_value_element(19, Type::Primitive(PrimitiveType::Long), true)
                        .into(),
                )),
            ),
        ]
        .into_iter()
        .map(Arc::new)
        .collect::<Vec<_>>();
        let mut documented = (*fields[0]).clone();
        documented.doc = Some("flag".to_string());
        let mut all = vec![Arc::new(documented)];
        all.extend(fields.into_iter().skip(1));
        let schema = Schema::builder().with_fields(all).build().unwrap();
        let cols = hive_columns(&schema);
        let rendered: Vec<(&str, &str)> = cols
            .iter()
            .map(|c| (c.name.as_deref().unwrap(), c.r#type.as_deref().unwrap()))
            .collect();
        assert_eq!(
            rendered,
            vec![
                ("b", "boolean"),
                ("i", "int"),
                ("l", "bigint"),
                ("f", "float"),
                ("d", "double"),
                ("dt", "date"),
                ("ts", "timestamp"),
                ("tz", "timestamp with local time zone"),
                ("s", "string"),
                ("u", "string"),
                ("bin", "binary"),
                ("dec", "decimal(10,2)"),
                ("st", "struct<x:int>"),
                ("arr", "array<string>"),
                ("m", "map<string,bigint>"),
            ]
        );
        assert_eq!(cols[0].comment.as_deref(), Some("flag"));
        assert_eq!(cols[1].comment, None);
    }

    #[test]
    fn thrift_outcomes_become_iceberg_errors() {
        let ok: Result<u8> = thrift(Ok(MaybeException::<u8, String>::Ok(7)), "x");
        assert_eq!(ok.unwrap(), 7);
        let exc = thrift(
            Ok(MaybeException::<u8, String>::Exception("boom".into())),
            "lock it",
        )
        .unwrap_err();
        assert!(exc.to_string().contains("could not lock it"), "{exc}");
        assert!(exc.to_string().contains("boom"), "{exc}");
        assert!(!exc.retryable());
    }

    #[test]
    fn an_unresolvable_address_is_a_config_error() {
        match metastore_client("", false) {
            Err(FaucetError::Config(msg)) => assert!(msg.contains("HMS address"), "{msg}"),
            Err(other) => panic!("expected a Config error, got {other:?}"),
            Ok(_) => panic!("an empty address must not build a client"),
        }
    }
}
