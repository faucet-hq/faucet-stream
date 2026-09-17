//! Synthetic `Source` / `Sink` doubles the battery drives (and that connector
//! authors can reuse in their own tests).
//!
//! The doubles come in **conformant** and deliberately **non-conformant**
//! flavours. The non-conformant ones (`FailingSource`, `PanickingSource`,
//! `LyingIdempotentSink`, `LyingKeyedSink`, `NoOpEvolvingSink`,
//! `MultiPageZeroSource`, `EmptyNameSource`, `ErringCheckSource`,
//! `ErringCheckSink`) exist so the battery's own unit tests can prove each
//! check actually *fails* when the contract is violated — a check that can
//! never fail is worthless. The conformant ones (`CountingSource`, `TestSink`,
//! `EvolvingSink`) demonstrate the contract genuinely.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use faucet_core::check::{CheckContext, CheckReport};
use faucet_core::drift::SchemaEvolution;
use faucet_core::write_mode::{DeleteMarker, WriteMode};
use faucet_core::{FaucetError, Sink, Source, StreamPage, Value, async_trait};
use futures_core::Stream;
use serde_json::json;

/// Field the conformance battery uses to flag a record as a delete when
/// exercising an upsert sink's delete path. Aliases the `cdc_unwrap` marker
/// default rather than re-spelling it, so the battery can never assert against
/// a field the real transform stopped stamping (#654 M17). A sink under test
/// must be configured with a `delete_marker { field: "__op", values: ["d"] }`
/// for the delete branch of
/// [`assert_write_modes_truthful`](crate::assert_write_modes_truthful) to run.
pub const DELETE_MARKER_FIELD: &str = faucet_core::stage::CDC_DEFAULT_MARKER_FIELD;
/// Value of [`DELETE_MARKER_FIELD`] that means "this record is a delete".
pub const DELETE_MARKER_VALUE: &str = "d";

/// A source that lazily emits `total` synthetic records (`{"n": i}`) in pages of
/// its configured `batch` (or the `stream_pages` hint), **without** buffering
/// the whole set — so it exercises the bounded-memory contract genuinely.
///
/// It also honours incremental resume: after `stream_pages` runs to completion
/// it emits a `{"n": total}` bookmark; feeding that back via
/// [`apply_start_bookmark`](Source::apply_start_bookmark) makes the next run
/// start at that offset (so a fully-consumed source resumes to zero records).
/// Construct with [`CountingSource::non_resumable`] to model a source that
/// *ignores* the bookmark — used to prove the bookmark-roundtrip check fails.
pub struct CountingSource {
    total: usize,
    batch: usize,
    resumable: bool,
    start: Arc<Mutex<usize>>,
}

impl CountingSource {
    /// `total` records, chunked into pages of `batch` (0 = one page). Resumable.
    pub fn new(total: usize, batch: usize) -> Self {
        Self {
            total,
            batch,
            resumable: true,
            start: Arc::new(Mutex::new(0)),
        }
    }

    /// Like [`new`](Self::new) but ignores any applied bookmark — a source that
    /// silently restarts from the beginning on resume (contract violation).
    pub fn non_resumable(total: usize, batch: usize) -> Self {
        Self {
            total,
            batch,
            resumable: false,
            start: Arc::new(Mutex::new(0)),
        }
    }
}

#[async_trait]
impl Source for CountingSource {
    async fn fetch_with_context(
        &self,
        _context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        let start = *self.start.lock().unwrap();
        Ok((start..self.total).map(|i| json!({ "n": i })).collect())
    }

    fn stream_pages<'a>(
        &'a self,
        _context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        // Like real sources, the double treats its own configured `batch` as
        // authoritative and ignores the pipeline hint. `batch == 0` is the
        // "no batching" sentinel — emit the whole set as one page (useful for
        // exercising the bounded-memory check's failure path).
        let batch = if self.batch == 0 {
            self.total.max(1)
        } else {
            self.batch
        };
        let total = self.total;
        let start = (*self.start.lock().unwrap()).min(total);
        Box::pin(async_stream::try_stream! {
            let mut n = start;
            if n >= total {
                // Fully consumed on resume: still emit one empty page carrying
                // the terminal bookmark so the pipeline advances its checkpoint.
                yield StreamPage { records: Vec::new(), bookmark: Some(json!({ "n": total })) };
                return;
            }
            while n < total {
                let end = (n + batch).min(total);
                let records: Vec<Value> = (n..end).map(|i| json!({ "n": i })).collect();
                n = end;
                let bookmark = if n >= total { Some(json!({ "n": total })) } else { None };
                yield StreamPage { records, bookmark };
            }
        })
    }

    fn connector_name(&self) -> &'static str {
        "counting-source"
    }

    fn state_key(&self) -> Option<String> {
        Some("conformance:counting".to_string())
    }

    async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
        if !self.resumable {
            return Ok(());
        }
        if let Some(n) = bookmark.get("n").and_then(|v| v.as_u64()) {
            *self.start.lock().unwrap() = n as usize;
        }
        Ok(())
    }
}

/// A source whose read path always returns a typed [`FaucetError`] — models an
/// unreachable endpoint / bad credentials. Used to prove the
/// `errors-not-panics` check passes on a well-behaved failure.
pub struct FailingSource;

#[async_trait]
impl Source for FailingSource {
    async fn fetch_with_context(
        &self,
        _context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        Err(FaucetError::Source(
            "unreachable endpoint (test double)".to_string(),
        ))
    }

    fn connector_name(&self) -> &'static str {
        "failing-source"
    }
}

/// A source whose read path **panics** — models a buggy connector that unwraps
/// on unexpected input. Used to prove the `errors-not-panics` check *fails*
/// (catches the unwind) rather than letting the panic escape silently.
pub struct PanickingSource;

#[async_trait]
impl Source for PanickingSource {
    async fn fetch_with_context(
        &self,
        _context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        panic!("connector bug: unwrap() on a None value");
    }

    fn connector_name(&self) -> &'static str {
        "panicking-source"
    }
}

/// A sink that records everything written, optionally deduplicating by a key
/// field (upsert), optionally advertising the atomic-watermark idempotent path.
///
/// Modes:
/// - [`TestSink::new`] — append-only, non-idempotent.
/// - [`TestSink::keyed`] — dedups by key on `write_batch` (keyed-upsert /
///   `dedups_by_key`), advertises `Upsert`/`Delete`.
/// - [`TestSink::keyed_upsert`] — like `keyed`, but also honours a delete
///   marker (`{"__op": "d"}`) so a delete-marked record genuinely *removes* the
///   keyed row — used to exercise the delete path of
///   [`assert_write_modes_truthful`](crate::assert_write_modes_truthful).
/// - [`TestSink::idempotent`] — additionally advertises
///   `supports_idempotent_writes` and stores a per-scope commit token, so the
///   atomic-watermark path can be exercised.
#[derive(Clone, Default)]
pub struct TestSink {
    key_field: Option<String>,
    idempotent: bool,
    delete_marker: Option<DeleteMarker>,
    keyed: Arc<Mutex<HashMap<String, Value>>>,
    appended: Arc<Mutex<Vec<Value>>>,
    tokens: Arc<Mutex<HashMap<String, String>>>,
    write_calls: Arc<Mutex<usize>>,
}

impl TestSink {
    /// An append-only recording sink.
    pub fn new() -> Self {
        Self::default()
    }

    /// An upsert sink that dedups by `key_field` in `write_batch`.
    pub fn keyed(key_field: impl Into<String>) -> Self {
        Self {
            key_field: Some(key_field.into()),
            ..Self::default()
        }
    }

    /// A keyed upsert sink that additionally honours the standard delete marker
    /// (a record carrying [`DELETE_MARKER_FIELD`] = [`DELETE_MARKER_VALUE`]
    /// removes its keyed row), so both the upsert and delete paths can be
    /// exercised through `write_batch`.
    pub fn keyed_upsert(key_field: impl Into<String>) -> Self {
        Self {
            key_field: Some(key_field.into()),
            delete_marker: Some(DeleteMarker {
                field: DELETE_MARKER_FIELD.to_string(),
                values: vec![DELETE_MARKER_VALUE.to_string()],
            }),
            ..Self::default()
        }
    }

    /// An upsert sink that also commits an atomic watermark token per scope,
    /// so it advertises (and honours) `supports_idempotent_writes`.
    pub fn idempotent(key_field: impl Into<String>) -> Self {
        Self {
            key_field: Some(key_field.into()),
            idempotent: true,
            ..Self::default()
        }
    }

    /// Number of distinct rows currently stored (keyed) or appended.
    pub fn len(&self) -> usize {
        if self.key_field.is_some() {
            self.keyed.lock().unwrap().len()
        } else {
            self.appended.lock().unwrap().len()
        }
    }

    /// Whether the sink holds no rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Total number of records passed to `write_batch` across all calls
    /// (counts re-delivered duplicates).
    pub fn total_written(&self) -> usize {
        *self.write_calls.lock().unwrap()
    }

    /// Whether `record` is flagged as a delete by this sink's configured marker.
    fn is_delete_marked(&self, record: &Value) -> bool {
        match &self.delete_marker {
            Some(dm) => record
                .get(&dm.field)
                .and_then(|v| v.as_str())
                .is_some_and(|s| dm.values.iter().any(|m| m == s)),
            None => false,
        }
    }
}

#[async_trait]
impl Sink for TestSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        *self.write_calls.lock().unwrap() += records.len();
        match &self.key_field {
            Some(field) => {
                let mut map = self.keyed.lock().unwrap();
                for r in records {
                    let key = r.get(field).map(|v| v.to_string()).ok_or_else(|| {
                        FaucetError::Sink(format!("record missing key `{field}`"))
                    })?;
                    // A delete-marked record removes its keyed row (upsert
                    // sinks with a `delete_marker`); otherwise insert/overwrite.
                    if self.is_delete_marked(r) {
                        map.remove(&key);
                    } else {
                        map.insert(key, r.clone());
                    }
                }
            }
            None => self
                .appended
                .lock()
                .unwrap()
                .extend(records.iter().cloned()),
        }
        Ok(records.len())
    }

    fn supports_idempotent_writes(&self) -> bool {
        self.idempotent
    }

    fn dedups_by_key(&self) -> bool {
        self.key_field.is_some()
    }

    fn supported_write_modes(&self) -> &'static [WriteMode] {
        if self.key_field.is_some() {
            &[WriteMode::Append, WriteMode::Upsert, WriteMode::Delete]
        } else {
            &[WriteMode::Append]
        }
    }

    async fn write_batch_idempotent(
        &self,
        records: &[Value],
        scope: &str,
        token: &str,
    ) -> Result<usize, FaucetError> {
        // Store the token opaquely (last-write-wins). Monotonicity is enforced
        // by the pipeline via `last_committed_token`, not by the sink — the
        // double models a real atomic-watermark commit faithfully.
        self.tokens
            .lock()
            .unwrap()
            .insert(scope.to_string(), token.to_string());
        self.write_batch(records).await
    }

    async fn last_committed_token(&self, scope: &str) -> Result<Option<String>, FaucetError> {
        Ok(self.tokens.lock().unwrap().get(scope).cloned())
    }

    fn connector_name(&self) -> &'static str {
        "test-sink"
    }
}

/// A sink that **claims** `supports_idempotent_writes` but does not actually
/// store a commit token (it just appends). Used to prove the idempotent-replay
/// and capabilities checks *fail* against a lying sink.
#[derive(Clone, Default)]
pub struct LyingIdempotentSink {
    appended: Arc<Mutex<Vec<Value>>>,
}

impl LyingIdempotentSink {
    /// A fresh lying sink.
    pub fn new() -> Self {
        Self::default()
    }
    /// Rows appended so far.
    pub fn len(&self) -> usize {
        self.appended.lock().unwrap().len()
    }
    /// Whether the sink holds no rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[async_trait]
impl Sink for LyingIdempotentSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        self.appended
            .lock()
            .unwrap()
            .extend(records.iter().cloned());
        Ok(records.len())
    }

    fn supports_idempotent_writes(&self) -> bool {
        true // the lie — it never persists a token (default methods apply).
    }

    fn connector_name(&self) -> &'static str {
        "lying-idempotent-sink"
    }
}

/// A sink that **claims** to dedup by key (`dedups_by_key` + `Upsert` in
/// `supported_write_modes`) but actually appends duplicates. Used to prove the
/// keyed-convergence branch of the idempotent-replay check *fails*.
#[derive(Clone, Default)]
pub struct LyingKeyedSink {
    appended: Arc<Mutex<Vec<Value>>>,
}

impl LyingKeyedSink {
    /// A fresh lying keyed sink.
    pub fn new() -> Self {
        Self::default()
    }
    /// Rows appended so far.
    pub fn len(&self) -> usize {
        self.appended.lock().unwrap().len()
    }
    /// Whether the sink holds no rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[async_trait]
impl Sink for LyingKeyedSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        self.appended
            .lock()
            .unwrap()
            .extend(records.iter().cloned());
        Ok(records.len())
    }

    fn dedups_by_key(&self) -> bool {
        true // the lie — it never dedups.
    }

    fn supported_write_modes(&self) -> &'static [WriteMode] {
        &[WriteMode::Append, WriteMode::Upsert]
    }

    fn connector_name(&self) -> &'static str {
        "lying-keyed-sink"
    }
}

/// A schemaless-to-typed sink that maintains a live destination schema and
/// **genuinely evolves** it: `evolve_schema` adds/overwrites the columns of a
/// [`SchemaEvolution`] into the stored schema, so a fresh `current_schema()`
/// reflects the change. Used to prove
/// [`assert_schema_evolution_effective`](crate::assert_schema_evolution_effective)
/// passes for a sink that actually applies the DDL.
#[derive(Clone)]
pub struct EvolvingSink {
    /// `column name -> JSON-Schema type fragment`.
    columns: Arc<Mutex<HashMap<String, Value>>>,
}

impl Default for EvolvingSink {
    fn default() -> Self {
        let mut cols = HashMap::new();
        cols.insert("id".to_string(), json!({ "type": "integer" }));
        Self {
            columns: Arc::new(Mutex::new(cols)),
        }
    }
}

impl EvolvingSink {
    /// A fresh evolving sink seeded with a single `id: integer` column.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of columns currently in the destination schema.
    pub fn column_count(&self) -> usize {
        self.columns.lock().unwrap().len()
    }
}

/// Build an `infer_schema`-shaped object schema from a column map.
fn schema_from_columns(cols: &HashMap<String, Value>) -> Value {
    let props: serde_json::Map<String, Value> =
        cols.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    json!({ "type": "object", "properties": props })
}

#[async_trait]
impl Sink for EvolvingSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        Ok(records.len())
    }

    async fn current_schema(&self) -> Result<Option<Value>, FaucetError> {
        Ok(Some(schema_from_columns(&self.columns.lock().unwrap())))
    }

    fn supports_schema_evolution(&self) -> bool {
        true
    }

    async fn evolve_schema(&self, evolution: &SchemaEvolution) -> Result<(), FaucetError> {
        let mut cols = self.columns.lock().unwrap();
        for change in evolution.additions.iter().chain(&evolution.widenings) {
            cols.insert(change.name.clone(), change.to.clone());
        }
        Ok(())
    }

    fn connector_name(&self) -> &'static str {
        "evolving-sink"
    }
}

/// A sink that **claims** `supports_schema_evolution` and exposes a fixed
/// destination schema, but whose `evolve_schema` is a silent no-op — a fresh
/// `current_schema()` never changes. Used to prove
/// [`assert_schema_evolution_effective`](crate::assert_schema_evolution_effective)
/// *fails* against a sink that only pretends to evolve.
#[derive(Clone, Default)]
pub struct NoOpEvolvingSink;

#[async_trait]
impl Sink for NoOpEvolvingSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        Ok(records.len())
    }

    async fn current_schema(&self) -> Result<Option<Value>, FaucetError> {
        Ok(Some(json!({
            "type": "object",
            "properties": { "id": { "type": "integer" } }
        })))
    }

    fn supports_schema_evolution(&self) -> bool {
        true // the lie — evolve_schema does nothing.
    }

    async fn evolve_schema(&self, _evolution: &SchemaEvolution) -> Result<(), FaucetError> {
        Ok(()) // accepted, but the schema never actually changes.
    }

    fn connector_name(&self) -> &'static str {
        "noop-evolving-sink"
    }
}

/// A source that emits `total` records in multiple non-empty pages **even when
/// asked to page with `batch_size = 0`** — violating the "no batching = single
/// page" contract. Used to prove
/// [`assert_batch_size_zero_single_page`](crate::assert_batch_size_zero_single_page)
/// *fails*.
pub struct MultiPageZeroSource {
    total: usize,
    page: usize,
}

impl MultiPageZeroSource {
    /// `total` records emitted in fixed pages of `page` (defaults to 2),
    /// regardless of the `batch_size` hint.
    pub fn new(total: usize) -> Self {
        Self { total, page: 2 }
    }
}

#[async_trait]
impl Source for MultiPageZeroSource {
    async fn fetch_with_context(
        &self,
        _context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        Ok((0..self.total).map(|i| json!({ "n": i })).collect())
    }

    fn stream_pages<'a>(
        &'a self,
        _context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        // Deliberately ignores the batch_size=0 "single page" sentinel.
        let total = self.total;
        let page = self.page.max(1);
        Box::pin(async_stream::try_stream! {
            let mut n = 0;
            while n < total {
                let end = (n + page).min(total);
                let records: Vec<Value> = (n..end).map(|i| json!({ "n": i })).collect();
                n = end;
                let bookmark = if n >= total { Some(json!({ "n": total })) } else { None };
                yield StreamPage { records, bookmark };
            }
        })
    }

    fn connector_name(&self) -> &'static str {
        "multi-page-zero-source"
    }
}

/// A source whose `connector_name()` is the empty string — a cardinality-rule
/// violation (it would surface as the `"unknown"` metric label). Used to prove
/// [`assert_connector_name_nonempty`](crate::assert_connector_name_nonempty)
/// *fails*.
pub struct EmptyNameSource;

#[async_trait]
impl Source for EmptyNameSource {
    async fn fetch_with_context(
        &self,
        _context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        Ok(Vec::new())
    }

    fn connector_name(&self) -> &'static str {
        "" // the violation.
    }
}

/// A source whose `check()` returns `Err` instead of surfacing the probe
/// failure as a [`ProbeStatus::Fail`](faucet_core::check::ProbeStatus) inside
/// `Ok(report)`. Used to prove
/// [`assert_preflight_check_wellformed`](crate::assert_preflight_check_wellformed)
/// *fails*.
pub struct ErringCheckSource;

#[async_trait]
impl Source for ErringCheckSource {
    async fn fetch_with_context(
        &self,
        _context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        Ok(Vec::new())
    }

    async fn check(&self, _ctx: &CheckContext) -> Result<CheckReport, FaucetError> {
        Err(FaucetError::Source(
            "probe failed — but returned as Err instead of a Fail probe".to_string(),
        ))
    }

    fn connector_name(&self) -> &'static str {
        "erring-check-source"
    }
}

/// A sink whose `check()` returns `Err` instead of a `Fail` probe. Used to prove
/// [`assert_sink_preflight_check_wellformed`](crate::assert_sink_preflight_check_wellformed)
/// *fails*.
#[derive(Clone, Default)]
pub struct ErringCheckSink;

#[async_trait]
impl Sink for ErringCheckSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        Ok(records.len())
    }

    async fn check(&self, _ctx: &CheckContext) -> Result<CheckReport, FaucetError> {
        Err(FaucetError::Sink(
            "probe failed — but returned as Err instead of a Fail probe".to_string(),
        ))
    }

    fn connector_name(&self) -> &'static str {
        "erring-check-sink"
    }
}

/// A source with a catalog it can [`discover`](Source::discover) — used to
/// exercise [`assert_discover_roundtrips`](crate::assert_discover_roundtrips).
///
/// Each discovered dataset becomes a [`DatasetDescriptor`](faucet_core::DatasetDescriptor)
/// whose `config_patch` is `{"dataset": <name>}` — the partial override a
/// `rebuild` closure deep-merges to select that dataset. The read path is
/// irrelevant to the check (the `rebuild` closure returns the source that is
/// actually read), so [`fetch_with_context`](Source::fetch_with_context)
/// returns nothing.
///
/// Construct with [`DiscoverableSource::new`] for a populated catalog, or
/// [`DiscoverableSource::empty`] to model a source that advertises discovery
/// but finds no datasets (used to prove the check *fails* on an empty catalog).
pub struct DiscoverableSource {
    datasets: Vec<String>,
}

impl DiscoverableSource {
    /// A discoverable source over two synthetic datasets (`orders`, `customers`).
    pub fn new() -> Self {
        Self {
            datasets: vec!["orders".to_string(), "customers".to_string()],
        }
    }

    /// A discoverable source whose catalog is empty — `discover()` returns no
    /// descriptors even though `supports_discover()` is `true`.
    pub fn empty() -> Self {
        Self {
            datasets: Vec::new(),
        }
    }
}

impl Default for DiscoverableSource {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Source for DiscoverableSource {
    async fn fetch_with_context(
        &self,
        _context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        Ok(Vec::new())
    }

    fn supports_discover(&self) -> bool {
        true
    }

    async fn discover(&self) -> Result<Vec<faucet_core::DatasetDescriptor>, FaucetError> {
        Ok(self
            .datasets
            .iter()
            .map(|name| {
                faucet_core::DatasetDescriptor::new(
                    name.clone(),
                    "table",
                    json!({ "dataset": name }),
                )
            })
            .collect())
    }

    fn connector_name(&self) -> &'static str {
        "discoverable-source"
    }
}

/// A sink that buffers writes and only makes them **durable on
/// [`flush`](Sink::flush)** — modelling a real buffered sink whose output is
/// committed at flush time (a Parquet footer, an S3 multipart completion).
/// Used to exercise [`assert_cancellation_flushes`](crate::assert_cancellation_flushes):
/// the pipeline must flush at the cancellation page boundary or the staged rows
/// are lost.
///
/// Construct with [`BufferedSink::new`] for a faithful sink (flush commits the
/// staging buffer) or [`BufferedSink::broken`] for one whose `flush` silently
/// drops the buffer (used to prove the check *fails* when a cancel does not
/// yield durable output).
#[derive(Clone)]
pub struct BufferedSink {
    staged: Arc<Mutex<Vec<Value>>>,
    durable: Arc<Mutex<Vec<Value>>>,
    commit_on_flush: bool,
}

impl BufferedSink {
    /// A buffered sink whose `flush` durably commits everything staged so far.
    pub fn new() -> Self {
        Self {
            staged: Arc::new(Mutex::new(Vec::new())),
            durable: Arc::new(Mutex::new(Vec::new())),
            commit_on_flush: true,
        }
    }

    /// A broken buffered sink whose `flush` is a silent no-op — staged rows
    /// never become durable, so any output buffered when the run ends is lost.
    pub fn broken() -> Self {
        Self {
            commit_on_flush: false,
            ..Self::new()
        }
    }

    /// Number of rows that are **durable** (committed via a flush).
    pub fn durable_len(&self) -> usize {
        self.durable.lock().unwrap().len()
    }

    /// Number of rows currently staged but not yet flushed.
    pub fn staged_len(&self) -> usize {
        self.staged.lock().unwrap().len()
    }
}

impl Default for BufferedSink {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Sink for BufferedSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        self.staged.lock().unwrap().extend(records.iter().cloned());
        Ok(records.len())
    }

    async fn flush(&self) -> Result<(), FaucetError> {
        if self.commit_on_flush {
            let mut staged = self.staged.lock().unwrap();
            self.durable.lock().unwrap().extend(staged.drain(..));
        }
        Ok(())
    }

    fn connector_name(&self) -> &'static str {
        "buffered-sink"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::drift::ColumnChange;
    use futures::StreamExt;
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn delete_marker_tracks_the_cdc_unwrap_default() {
        // Connector authors configure their sink's `delete_marker` against these
        // literals, so the aliasing must not silently change what they are.
        assert_eq!(DELETE_MARKER_FIELD, "__op");
        assert_eq!(DELETE_MARKER_VALUE, "d");
        assert_eq!(
            DELETE_MARKER_FIELD,
            faucet_core::stage::CDC_DEFAULT_MARKER_FIELD
        );
    }

    #[tokio::test]
    async fn counting_source_resumes_and_ignores_when_non_resumable() {
        let s = CountingSource::new(5, 2);
        assert_eq!(s.state_key().as_deref(), Some("conformance:counting"));
        assert_eq!(s.connector_name(), "counting-source");
        assert_eq!(
            s.fetch_with_context(&HashMap::new()).await.unwrap().len(),
            5
        );
        // Resume from the terminal bookmark → no records left.
        s.apply_start_bookmark(json!({ "n": 5 })).await.unwrap();
        assert!(
            s.fetch_with_context(&HashMap::new())
                .await
                .unwrap()
                .is_empty()
        );

        // A non-resumable source ignores the applied bookmark.
        let nr = CountingSource::non_resumable(5, 2);
        nr.apply_start_bookmark(json!({ "n": 5 })).await.unwrap();
        assert_eq!(
            nr.fetch_with_context(&HashMap::new()).await.unwrap().len(),
            5
        );
    }

    #[tokio::test]
    async fn test_sink_accessors() {
        let s = TestSink::new();
        assert!(s.is_empty());
        s.write_batch(&[json!({ "id": 1 })]).await.unwrap();
        assert!(!s.is_empty());
        assert_eq!(s.len(), 1);
        assert_eq!(s.total_written(), 1);
        assert_eq!(s.connector_name(), "test-sink");
    }

    #[tokio::test]
    async fn lying_idempotent_sink_never_persists_a_token() {
        let s = LyingIdempotentSink::new();
        assert!(s.is_empty());
        assert!(s.supports_idempotent_writes());
        assert_eq!(s.connector_name(), "lying-idempotent-sink");
        s.write_batch_idempotent(&[json!({ "id": 1 })], "scope", "00000000000000000001")
            .await
            .unwrap();
        assert_eq!(s.len(), 1);
        assert!(s.last_committed_token("scope").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn lying_keyed_sink_appends_duplicates() {
        let s = LyingKeyedSink::new();
        assert!(s.is_empty());
        assert!(s.dedups_by_key());
        assert!(s.supported_write_modes().contains(&WriteMode::Upsert));
        assert_eq!(s.connector_name(), "lying-keyed-sink");
        s.write_batch(&[json!({ "id": 1 })]).await.unwrap();
        s.write_batch(&[json!({ "id": 1 })]).await.unwrap();
        assert_eq!(s.len(), 2, "lying keyed sink does not dedup");
    }

    #[tokio::test]
    async fn failing_and_panicking_source_labels() {
        assert_eq!(FailingSource.connector_name(), "failing-source");
        assert_eq!(PanickingSource.connector_name(), "panicking-source");
        assert!(FailingSource.fetch_all().await.is_err());
    }

    #[tokio::test]
    async fn test_sink_delete_marker_removes_row() {
        let s = TestSink::keyed_upsert("id");
        assert!(s.supported_write_modes().contains(&WriteMode::Delete));
        s.write_batch(&[json!({ "id": 1, "v": "a" })])
            .await
            .unwrap();
        assert_eq!(s.len(), 1);
        // A delete-marked record removes the keyed row.
        s.write_batch(&[json!({ "id": 1, "__op": "d" })])
            .await
            .unwrap();
        assert_eq!(s.len(), 0, "delete marker must remove the row");
        // A keyed sink without a marker never treats a record as a delete.
        let plain = TestSink::keyed("id");
        plain
            .write_batch(&[json!({ "id": 2, "__op": "d" })])
            .await
            .unwrap();
        assert_eq!(plain.len(), 1, "no marker configured → the row is upserted");
    }

    #[tokio::test]
    async fn evolving_sink_evolves_and_noop_does_not() {
        let evo = EvolvingSink::new();
        assert_eq!(evo.connector_name(), "evolving-sink");
        assert_eq!(evo.write_batch(&[json!({ "id": 1 })]).await.unwrap(), 1);
        assert_eq!(evo.column_count(), 1);
        let evolution = SchemaEvolution {
            additions: vec![ColumnChange {
                name: "email".to_string(),
                from: None,
                to: json!({ "type": "string" }),
            }],
            widenings: Vec::new(),
            relax_nullability: Vec::new(),
        };
        evo.evolve_schema(&evolution).await.unwrap();
        assert_eq!(evo.column_count(), 2);
        let schema = evo.current_schema().await.unwrap().unwrap();
        assert!(schema["properties"]["email"].is_object());

        let noop = NoOpEvolvingSink;
        assert!(noop.supports_schema_evolution());
        assert_eq!(noop.write_batch(&[json!({ "id": 1 })]).await.unwrap(), 1);
        let before = noop.current_schema().await.unwrap().unwrap();
        noop.evolve_schema(&evolution).await.unwrap();
        let after = noop.current_schema().await.unwrap().unwrap();
        assert_eq!(before, after, "noop evolve must not change the schema");
    }

    #[tokio::test]
    async fn multi_page_zero_source_emits_multiple_pages_and_fetches() {
        let s = MultiPageZeroSource::new(6);
        assert_eq!(s.connector_name(), "multi-page-zero-source");
        let ctx: HashMap<String, Value> = HashMap::new();
        assert_eq!(s.fetch_with_context(&ctx).await.unwrap().len(), 6);
        let mut stream = s.stream_pages(&ctx, 0);
        let mut pages = 0usize;
        let mut records = 0usize;
        while let Some(p) = stream.next().await {
            let p = p.unwrap();
            pages += 1;
            records += p.records.len();
        }
        assert_eq!(records, 6);
        assert!(pages > 1, "must emit more than one page under batch_size=0");
    }

    #[tokio::test]
    async fn empty_name_and_erring_check_doubles() {
        assert_eq!(EmptyNameSource.connector_name(), "");
        assert!(
            EmptyNameSource
                .fetch_with_context(&HashMap::new())
                .await
                .unwrap()
                .is_empty()
        );

        let ctx = CheckContext::default();
        assert_eq!(ErringCheckSource.connector_name(), "erring-check-source");
        assert!(
            ErringCheckSource
                .fetch_with_context(&HashMap::new())
                .await
                .unwrap()
                .is_empty()
        );
        assert!(ErringCheckSource.check(&ctx).await.is_err());

        let sink = ErringCheckSink;
        assert_eq!(sink.connector_name(), "erring-check-sink");
        assert_eq!(sink.write_batch(&[json!({ "x": 1 })]).await.unwrap(), 1);
        assert!(sink.check(&ctx).await.is_err());
    }

    #[tokio::test]
    async fn discoverable_source_enumerates_its_catalog() {
        let s = DiscoverableSource::new();
        assert_eq!(s.connector_name(), "discoverable-source");
        assert!(s.supports_discover());
        let ds = s.discover().await.unwrap();
        assert_eq!(ds.len(), 2);
        assert_eq!(ds[0].name, "orders");
        assert_eq!(ds[0].config_patch, json!({ "dataset": "orders" }));
        // The read path is deliberately empty — the rebuild closure supplies the
        // source that is actually read.
        assert!(
            s.fetch_with_context(&HashMap::new())
                .await
                .unwrap()
                .is_empty()
        );

        // The empty-catalog variant still advertises discovery.
        let empty = DiscoverableSource::empty();
        assert!(empty.supports_discover());
        assert!(empty.discover().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn buffered_sink_only_durable_after_flush_unless_broken() {
        let s = BufferedSink::new();
        assert_eq!(s.connector_name(), "buffered-sink");
        s.write_batch(&[json!({ "id": 1 }), json!({ "id": 2 })])
            .await
            .unwrap();
        // Staged, not yet durable.
        assert_eq!(s.staged_len(), 2);
        assert_eq!(s.durable_len(), 0);
        s.flush().await.unwrap();
        assert_eq!(s.staged_len(), 0);
        assert_eq!(s.durable_len(), 2, "flush must commit the staged rows");

        // The broken variant never commits, even on flush.
        let broken = BufferedSink::broken();
        broken.write_batch(&[json!({ "id": 1 })]).await.unwrap();
        broken.flush().await.unwrap();
        assert_eq!(broken.durable_len(), 0, "broken flush drops the buffer");
    }
}
