//! MongoDB sink implementation.

use crate::config::MongoSinkConfig;
use async_trait::async_trait;
use faucet_core::FaucetError;
use futures::StreamExt;
use mongodb::Client;
use mongodb::bson::{self, Bson, Document};
use serde_json::{Map, Value};

/// Max in-flight `replace_one` / `delete_one` operations issued concurrently
/// from a single planned page. The planner has already deduped by key, so
/// every concurrent op targets a distinct key — there is no intra-batch
/// ordering hazard. A modest bound keeps round-trips overlapping without
/// flooding the connection pool.
const APPLY_CONCURRENCY: usize = 50;

/// Max `commit_transaction()` retries while the error carries the driver's
/// `UnknownTransactionCommitResult` label (the driver-recommended retry loop,
/// bounded so a partitioned primary can't wedge the pipeline forever).
const MAX_COMMIT_RETRIES: usize = 8;

/// MongoDB server error code for `NamespaceExists` (raised by
/// `create_collection` when the collection already exists — benign for the
/// idempotent pre-creation of the watermark/data collections).
const NAMESPACE_EXISTS_CODE: i32 = 48;

/// Convert a JSON object map (a key filter from
/// [`faucet_core::key_to_filter`]) into a BSON filter [`Document`].
///
/// Returns a `Sink` error if the map does not convert to a BSON document.
fn json_map_to_bson_filter(map: &Map<String, Value>) -> Result<Document, FaucetError> {
    MongoSink::value_to_document(&Value::Object(map.clone()))
}

/// A single planned upsert/delete op, pre-converted to BSON so all conversion
/// errors surface before any write (or transaction) is issued.
enum PlannedOp {
    /// `replace_one(filter, replacement).upsert(true)`.
    Upsert(Document, Document),
    /// `delete_one(filter)`.
    Delete(Document),
}

/// A page fully prepared (planned + converted to BSON) **before** the
/// exactly-once transaction is opened, so no preparation failure can leave a
/// dangling transaction.
enum PreparedWrite {
    /// Append mode: the whole page as one `insert_many`.
    Append(Vec<Document>),
    /// Upsert/delete mode: the planned per-document ops.
    Planned(Vec<PlannedOp>),
}

// ---------------------------------------------------------------------------
// Exactly-once (effectively-once) helpers — pure, unit-testable functions.
// ---------------------------------------------------------------------------

/// Filter selecting the per-scope watermark document in the
/// [`_faucet_commit_token`](faucet_core::idempotency::COMMIT_TOKEN_TABLE)
/// collection. The scope is the document `_id`, so MongoDB's mandatory
/// unique index on `_id` enforces one watermark per scope for free.
fn commit_token_filter(scope: &str) -> Document {
    bson::doc! { "_id": scope }
}

/// Full watermark document `{ _id: <scope>, token: <token> }` upserted (via
/// `replace_one(upsert = true)`) inside the same transaction as the page's
/// data writes. The token is **opaque** — it may contain `#` + JSON payload —
/// and is stored/returned verbatim, never parsed.
fn commit_token_doc(scope: &str, token: &str) -> Document {
    bson::doc! {
        "_id": scope,
        faucet_core::idempotency::COMMIT_TOKEN_TOKEN_COL: token,
    }
}

/// Extract the opaque token string from a watermark document read back by
/// [`last_committed_token`](faucet_core::Sink::last_committed_token).
///
/// A missing or non-string `token` field means the watermark collection was
/// tampered with (or written by something other than this sink) — surface a
/// typed error rather than silently treating the scope as uncommitted, which
/// would replay pages and duplicate data.
fn token_from_commit_doc(doc: &Document) -> Result<String, FaucetError> {
    match doc.get(faucet_core::idempotency::COMMIT_TOKEN_TOKEN_COL) {
        Some(Bson::String(s)) => Ok(s.clone()),
        Some(other) => Err(FaucetError::Sink(format!(
            "malformed watermark document in '{}': expected a string '{}' field, got {other:?}",
            faucet_core::idempotency::COMMIT_TOKEN_TABLE,
            faucet_core::idempotency::COMMIT_TOKEN_TOKEN_COL,
        ))),
        None => Err(FaucetError::Sink(format!(
            "malformed watermark document in '{}': missing the '{}' field",
            faucet_core::idempotency::COMMIT_TOKEN_TABLE,
            faucet_core::idempotency::COMMIT_TOKEN_TOKEN_COL,
        ))),
    }
}

/// True when a server error message indicates the deployment does not support
/// multi-document transactions — i.e. a standalone `mongod` (transactions
/// require a replica set or sharded cluster).
///
/// The canonical standalone-server message is
/// `"Transaction numbers are only allowed on a replica set member or mongos"`
/// (code 20, `IllegalOperation`). The Rust driver *rewrites* that exact server
/// errmsg into `"This MongoDB deployment does not support retryable writes"`
/// when rendering the error, so both phrasings are matched; older/alternate
/// builds phrase it as `"Transactions are not supported"`. Matched
/// case-insensitively over the rendered error string so the predicate stays
/// pure and unit-testable.
fn is_transactions_unsupported(message: &str) -> bool {
    let m = message.to_ascii_lowercase();
    m.contains("transaction numbers are only allowed on a replica set member or mongos")
        || m.contains("does not support retryable writes")
        || m.contains("transactions are not supported")
}

/// Map a driver error raised while starting or executing a transaction to a
/// typed [`FaucetError`]. A standalone-server "transactions unavailable"
/// failure gets a self-explanatory message (the config error is the
/// deployment topology, not the data); everything else keeps the original
/// context + driver message.
fn classify_transaction_error(context: &str, message: &str) -> FaucetError {
    if is_transactions_unsupported(message) {
        FaucetError::Sink(format!(
            "mongodb exactly-once (write_batch_idempotent) requires a replica set or sharded \
             cluster — transactions are unavailable on a standalone server: {message}"
        ))
    } else {
        FaucetError::Sink(format!("{context}: {message}"))
    }
}

/// True when a `create_collection` command error code means "already exists"
/// (`NamespaceExists`, code 48) — benign for idempotent pre-creation.
fn is_namespace_exists_code(code: Option<i32>) -> bool {
    code == Some(NAMESPACE_EXISTS_CODE)
}

/// Extract the server command error code from a driver error, if any.
fn command_error_code(e: &mongodb::error::Error) -> Option<i32> {
    match e.kind.as_ref() {
        mongodb::error::ErrorKind::Command(c) => Some(c.code),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Scoped cleanup (#478) — pure filter construction.
// ---------------------------------------------------------------------------

/// Ceiling on the serialized size of the scoped-cleanup `delete_many` filter.
///
/// The written-key set goes into the filter as a single `$nin` / `$nor` clause,
/// and that clause **cannot be chunked**: splitting it would make each chunk
/// delete the keys the other chunks kept — i.e. delete documents this run
/// actually wrote. So a filter too large for one command has to be refused, not
/// split. MongoDB rejects any command document above `maxBsonObjectSize`
/// (16 MiB); we cut off at half of that, leaving room for the `delete` command
/// envelope the driver wraps around the filter.
const MAX_CLEANUP_FILTER_BYTES: usize = 8 * 1024 * 1024;

/// Refuse a cleanup filter the server would reject, naming the ceiling and the
/// way out.
///
/// Issuing it anyway would surface as a driver error that says nothing about how
/// to fix it — and the natural workaround (split the keys across several
/// deletes) is unsafe here, so the ceiling is a hard stop rather than a hint.
fn check_cleanup_filter_size(filter_bytes: usize, written_keys: usize) -> Result<(), FaucetError> {
    if filter_bytes <= MAX_CLEANUP_FILTER_BYTES {
        return Ok(());
    }
    Err(FaucetError::Sink(format!(
        "cleanup: the delete filter for {written_keys} written keys serializes to \
         {filter_bytes} bytes, over this sink's ceiling of {MAX_CLEANUP_FILTER_BYTES} bytes \
         (MongoDB rejects any command document above its 16 MiB limit, and the written-key set \
         cannot be split across several deletes without deleting documents this run wrote). \
         Nothing was deleted — narrow the completeness claim so fewer documents fall inside one \
         scope."
    )))
}

/// Verify every accumulated key tuple is addressed by the same fields, in the
/// same order, as the sink's configured `key`.
///
/// The cleanup deletes everything in the scope that is *not* in this set, so a
/// tuple naming a different field would make a written document look unwritten
/// — and delete it. The pipeline builds the tuples from the sink's own `key`, so
/// a mismatch is an internal invariant violation; it is checked anyway because
/// the cost is a few string comparisons and the failure mode is data loss.
fn check_key_alignment(key: &[String], seen: &[faucet_core::KeyTuple]) -> Result<(), FaucetError> {
    for kt in seen {
        let aligned = kt.0.len() == key.len() && kt.0.iter().zip(key).all(|((c, _), k)| c == k);
        if !aligned {
            let got: Vec<&str> = kt.0.iter().map(|(c, _)| c.as_str()).collect();
            return Err(FaucetError::Sink(format!(
                "cleanup: a written-key tuple is keyed by {got:?} but the sink's key is {key:?} \
                 — refusing to delete, because a mismatched key makes written documents look \
                 unwritten"
            )));
        }
    }
    Ok(())
}

/// Build the `delete_many` filter selecting documents in `scope` whose key was
/// **not** written by this run (#478).
///
/// Shape: `{"$and": [ <one clause per scope field>, <the "not written" clause> ]}`.
/// Each predicate is its own `$and` clause rather than a sibling field in one
/// document because a scope field may *also* be a key field (a child sync keyed
/// on `["contact_id", "assoc_id"]` and scoped by `contact_id`) — as siblings the
/// second occurrence would overwrite the first in the map and silently widen the
/// delete to the whole collection.
///
/// The "not written" clause depends on the key width, because BSON has no tuple
/// `$nin`:
///
/// - **Single-column key** → `{ <key>: { "$nin": [v, …] } }`. The operator the
///   server can answer straight from that field's index, so this is the shape
///   worth special-casing rather than folding into the general one.
/// - **Composite key** → `{ "$nor": [ {a: …, b: …}, … ] }` — "matches none of
///   the written tuples". `$nor` over per-tuple equality documents is the one
///   construction that compares a whole tuple with plain query operators. The
///   alternative — hashing the tuple into a derived `_id` and `$nin`-ing that —
///   only works when the key *is* `_id`, which this sink does not require (the
///   key is an arbitrary match filter), and it would silently mismatch every
///   document written before the derivation existed. `$expr` was likewise
///   rejected: it forces a full collection scan and cannot use an index.
///
/// Both forms also match documents in the scope that are **missing** the key
/// field(s) — deliberately: a document with no key cannot have been written by
/// this run, so within a claimed-complete scope it is stale by definition.
///
/// An empty `seen` set drops the "not written" clause entirely, leaving the
/// scope predicate alone so **every** document in the scope is deleted. That is
/// not a degenerate case but the motivating one: the source claimed the scope is
/// complete and reported no records in it.
fn cleanup_filter_json(
    scope: &std::collections::BTreeMap<String, Value>,
    key: &[String],
    seen: &[faucet_core::KeyTuple],
) -> Value {
    let mut clauses: Vec<Value> = scope
        .iter()
        .map(|(field, v)| {
            let mut m = Map::with_capacity(1);
            m.insert(field.clone(), v.clone());
            Value::Object(m)
        })
        .collect();

    if !seen.is_empty() {
        let not_written = if key.len() == 1 {
            // Single column: `$nin` over the written values.
            let values: Vec<Value> = seen
                .iter()
                .map(|kt| kt.0[0].1.clone())
                .collect::<Vec<Value>>();
            let mut nin = Map::with_capacity(1);
            nin.insert("$nin".to_string(), Value::Array(values));
            let mut m = Map::with_capacity(1);
            m.insert(key[0].clone(), Value::Object(nin));
            Value::Object(m)
        } else {
            // Composite: `$nor` over one equality document per written tuple.
            let tuples: Vec<Value> = seen
                .iter()
                .map(|kt| Value::Object(faucet_core::key_to_filter(kt)))
                .collect();
            let mut m = Map::with_capacity(1);
            m.insert("$nor".to_string(), Value::Array(tuples));
            Value::Object(m)
        };
        clauses.push(not_written);
    }

    let mut root = Map::with_capacity(1);
    root.insert("$and".to_string(), Value::Array(clauses));
    Value::Object(root)
}

/// A sink that inserts JSON records into a MongoDB collection.
///
/// Each record must be a JSON object. Non-object values produce an error.
/// Records are inserted in batches using `insert_many`.
pub struct MongoSink {
    config: MongoSinkConfig,
    client: Client,
    /// One-shot guard for pre-creating the data + watermark collections before
    /// the first exactly-once transaction (a failed init is retried on the
    /// next call). Collections referenced inside a multi-document transaction
    /// must exist up front on MongoDB < 4.4; pre-creating keeps the
    /// exactly-once path portable and removes a first-run failure mode.
    eo_collections_ready: tokio::sync::OnceCell<()>,
}

impl MongoSink {
    /// Create a new MongoDB sink, establishing the client connection.
    pub async fn new(config: MongoSinkConfig) -> Result<Self, FaucetError> {
        faucet_core::validate_batch_size(config.batch_size)?;
        // Validate write-mode config up front (config-only, so before connecting
        // is fine): upsert/delete require a non-empty `key`. MongoDB is
        // schemaless, so there is no column-mapping guard to apply.
        config.write.validate()?;
        let client = Client::with_uri_str(&config.connection_uri)
            .await
            .map_err(|e| FaucetError::Config(format!("MongoDB connection failed: {e}")))?;

        Ok(Self {
            config,
            client,
            eo_collections_ready: tokio::sync::OnceCell::new(),
        })
    }

    /// Staging collection used while an overwrite run is in flight.
    fn staging_collection(&self) -> String {
        format!(
            "{}{}",
            self.config.collection,
            faucet_core::idempotency::OVERWRITE_STAGING_SUFFIX
        )
    }

    /// The collection the append/insert path targets. For `write_mode:
    /// overwrite` every insert in this sink's lifetime lands in the staging
    /// collection (cleared by [`begin_overwrite`], atomically swapped over the
    /// real collection by [`commit_overwrite`]); otherwise the configured one.
    fn effective_collection(&self) -> String {
        if self.config.write.is_overwrite() {
            self.staging_collection()
        } else {
            self.config.collection.clone()
        }
    }

    /// Build the match-filter [`Document`] for an upsert row by pulling the
    /// configured `key` columns out of the row. The planner
    /// ([`faucet_core::plan_writes`]) has already validated that every key
    /// column is present and non-null on each upsert row, so a missing column
    /// here is an internal invariant violation rather than user data error.
    fn filter_from_row(row: &Value, key: &[String]) -> Result<Document, FaucetError> {
        let obj = row
            .as_object()
            .ok_or_else(|| FaucetError::Sink("upsert row is not a JSON object".to_string()))?;
        let mut filter = Map::with_capacity(key.len());
        for col in key {
            match obj.get(col) {
                Some(v) => {
                    filter.insert(col.clone(), v.clone());
                }
                None => {
                    return Err(FaucetError::Sink(format!(
                        "upsert row missing key column '{col}' after planning"
                    )));
                }
            }
        }
        json_map_to_bson_filter(&filter)
    }

    /// Apply a planned page of upserts and deletes to the collection.
    ///
    /// Each upsert row is committed with `replace_one(filter, replacement)
    /// .upsert(true)` and each delete with `delete_one(filter)`. We use the
    /// per-document `replace_one(upsert)` / `delete_one` primitives (not the
    /// namespaced `Client::bulk_write`) for compatibility with all supported
    /// MongoDB server versions; throughput is recovered by issuing the ops
    /// concurrently via `buffer_unordered`. The planner already deduped keys
    /// (last-write-wins), so concurrent ops target distinct keys and there is
    /// no intra-batch ordering hazard.
    ///
    /// Returns the number of upserts + deletes applied.
    async fn apply_plan(&self, plan: &faucet_core::WritePlan) -> Result<usize, FaucetError> {
        let collection = self
            .client
            .database(&self.config.database)
            .collection::<Document>(&self.config.collection);

        // Build a single homogeneous op stream of (filter, replacement?) so
        // upserts and deletes run through one bounded `buffer_unordered`.
        use PlannedOp as Op;
        let ops = self.build_plan_ops(plan)?;

        let applied = ops.len();

        futures::stream::iter(ops.into_iter().map(|op| {
            let collection = collection.clone();
            async move {
                match op {
                    Op::Upsert(filter, replacement) => collection
                        .replace_one(filter, replacement)
                        .upsert(true)
                        .await
                        .map(|_| ())
                        .map_err(|e| {
                            FaucetError::Sink(format!("MongoDB replace_one (upsert) failed: {e}"))
                        }),
                    Op::Delete(filter) => collection
                        .delete_one(filter)
                        .await
                        .map(|_| ())
                        .map_err(|e| FaucetError::Sink(format!("MongoDB delete_one failed: {e}"))),
                }
            }
        }))
        .buffer_unordered(APPLY_CONCURRENCY)
        .collect::<Vec<Result<(), FaucetError>>>()
        .await
        .into_iter()
        .collect::<Result<Vec<()>, FaucetError>>()?;

        tracing::info!(
            applied,
            upserts = plan.upserts.len(),
            deletes = plan.deletes.len(),
            database = %self.config.database,
            collection = %self.config.collection,
            "MongoDB upsert/delete write complete"
        );

        Ok(applied)
    }

    /// Convert a planned page ([`faucet_core::WritePlan`]) into pre-converted
    /// BSON ops, so every conversion / key-extraction error surfaces before
    /// any write (or transaction) is issued.
    fn build_plan_ops(&self, plan: &faucet_core::WritePlan) -> Result<Vec<PlannedOp>, FaucetError> {
        let key = &self.config.write.key;
        let mut ops: Vec<PlannedOp> = Vec::with_capacity(plan.upserts.len() + plan.deletes.len());
        for row in &plan.upserts {
            let filter = Self::filter_from_row(row, key)?;
            let replacement = Self::value_to_document(row)?;
            ops.push(PlannedOp::Upsert(filter, replacement));
        }
        for kt in &plan.deletes {
            let filter = json_map_to_bson_filter(&faucet_core::key_to_filter(kt))?;
            ops.push(PlannedOp::Delete(filter));
        }
        Ok(ops)
    }

    /// Delete documents in `scope` whose key was not written by this run (#478).
    ///
    /// One `delete_many` with the filter built by [`cleanup_filter_json`] — no
    /// transaction, so **no replica set is required** (unlike the exactly-once
    /// path). That is safe here even though `delete_many` is only atomic
    /// per-document: the predicate itself excludes every written key, so a
    /// delete interrupted half-way can only ever have removed stale documents,
    /// never one this run wrote. Re-running the cleanup simply finishes the job.
    /// The cost of the missing transaction is that concurrent readers can
    /// observe the scope mid-delete (partially cleaned), and a document inserted
    /// into the scope by another writer while the delete runs may or may not be
    /// removed.
    ///
    /// An empty `seen` set is **not** a no-op — it means the source reported the
    /// scope as empty, so every document in it is stale and must go. That is the
    /// case this feature exists for.
    async fn cleanup_scope_impl(
        &self,
        scope: &std::collections::BTreeMap<String, Value>,
        seen: &faucet_core::SeenKeys,
    ) -> Result<u64, FaucetError> {
        let key = &self.config.write.key;
        if key.is_empty() {
            return Err(FaucetError::Sink(
                "cleanup requires a non-empty `key`".to_string(),
            ));
        }
        if scope.is_empty() {
            // Defence in depth: `CleanupPolicy::new` already refuses this. With
            // no scope predicate the filter would match the whole collection —
            // the difference between a cleanup and a truncate.
            return Err(FaucetError::Sink(
                "cleanup: refusing an empty completeness claim — with no scope predicate the \
                 delete would match every document in the collection"
                    .to_string(),
            ));
        }
        check_key_alignment(key, seen.keys())?;

        let filter = Self::value_to_document(&cleanup_filter_json(scope, key, seen.keys()))?;

        // Refuse a filter the server would reject (see MAX_CLEANUP_FILTER_BYTES
        // for why it cannot be split into several deletes instead).
        let filter_bytes = bson::to_vec(&filter)
            .map_err(|e| FaucetError::Sink(format!("cleanup: filter serialization failed: {e}")))?
            .len();
        check_cleanup_filter_size(filter_bytes, seen.len())?;

        let deleted = self
            .client
            .database(&self.config.database)
            .collection::<Document>(&self.config.collection)
            .delete_many(filter)
            .await
            .map_err(|e| FaucetError::Sink(format!("cleanup: delete_many failed: {e}")))?
            .deleted_count;

        tracing::info!(
            deleted,
            written_keys = seen.len(),
            database = %self.config.database,
            collection = %self.config.collection,
            "MongoDB scoped cleanup complete"
        );
        Ok(deleted)
    }

    /// Handle to the per-database watermark collection
    /// ([`_faucet_commit_token`](faucet_core::idempotency::COMMIT_TOKEN_TABLE)).
    fn commit_token_collection(&self) -> mongodb::Collection<Document> {
        self.client
            .database(&self.config.database)
            .collection::<Document>(faucet_core::idempotency::COMMIT_TOKEN_TABLE)
    }

    /// Pre-create the data + watermark collections once per sink instance
    /// (tolerating `NamespaceExists`), so the exactly-once transaction never
    /// has to create a collection implicitly (unsupported on MongoDB < 4.4).
    async fn ensure_exactly_once_collections(&self) -> Result<(), FaucetError> {
        self.eo_collections_ready
            .get_or_try_init(|| async {
                let db = self.client.database(&self.config.database);
                for name in [
                    self.config.collection.as_str(),
                    faucet_core::idempotency::COMMIT_TOKEN_TABLE,
                ] {
                    match db.create_collection(name).await {
                        Ok(()) => {}
                        Err(e) if is_namespace_exists_code(command_error_code(&e)) => {}
                        Err(e) => {
                            return Err(FaucetError::Sink(format!(
                                "MongoDB create_collection('{name}') failed: {e}"
                            )));
                        }
                    }
                }
                Ok(())
            })
            .await
            .map(|_| ())
    }

    /// Run one page's data writes **plus** the watermark upsert inside an
    /// already-started transaction on `session`. On `Err` the caller aborts
    /// the transaction (best-effort) so nothing from the page is committed.
    ///
    /// A [`mongodb::ClientSession`] must not be used concurrently, so the
    /// planned upsert/delete ops run **sequentially** here — unlike the
    /// at-least-once path's `buffer_unordered` fan-out. That is the throughput
    /// tradeoff of exactly-once on this sink; atomicity requires the single
    /// session.
    async fn apply_in_transaction(
        &self,
        session: &mut mongodb::ClientSession,
        prepared: PreparedWrite,
        scope: &str,
        token: &str,
    ) -> Result<usize, FaucetError> {
        let collection = self
            .client
            .database(&self.config.database)
            .collection::<Document>(&self.config.collection);

        let written = match prepared {
            PreparedWrite::Append(docs) => {
                let n = docs.len();
                // One page = one transaction: the whole page goes in a single
                // `insert_many` (no `batch_size` re-chunking on this path), so
                // the data and its watermark commit as one atomic unit.
                // `ordered` is irrelevant inside a transaction — any write
                // error aborts the whole transaction — so the driver default
                // is used. (`insert_many` rejects an empty slice, and an empty
                // page still needs its watermark, hence the guard.)
                if !docs.is_empty() {
                    collection
                        .insert_many(docs)
                        .session(&mut *session)
                        .await
                        .map_err(|e| {
                            classify_transaction_error(
                                "MongoDB insert_many (exactly-once) failed",
                                &e.to_string(),
                            )
                        })?;
                }
                n
            }
            PreparedWrite::Planned(ops) => {
                let n = ops.len();
                for op in ops {
                    match op {
                        PlannedOp::Upsert(filter, replacement) => {
                            collection
                                .replace_one(filter, replacement)
                                .upsert(true)
                                .session(&mut *session)
                                .await
                                .map(|_| ())
                                .map_err(|e| {
                                    classify_transaction_error(
                                        "MongoDB replace_one (exactly-once upsert) failed",
                                        &e.to_string(),
                                    )
                                })?;
                        }
                        PlannedOp::Delete(filter) => {
                            collection
                                .delete_one(filter)
                                .session(&mut *session)
                                .await
                                .map(|_| ())
                                .map_err(|e| {
                                    classify_transaction_error(
                                        "MongoDB delete_one (exactly-once) failed",
                                        &e.to_string(),
                                    )
                                })?;
                        }
                    }
                }
                n
            }
        };

        // The watermark upserts in the SAME transaction as the data, so on
        // crash either both land or neither does — that is what makes the
        // replay skip-on-resume produce zero duplicates.
        self.commit_token_collection()
            .replace_one(commit_token_filter(scope), commit_token_doc(scope, token))
            .upsert(true)
            .session(session)
            .await
            .map_err(|e| {
                classify_transaction_error("MongoDB commit-token upsert failed", &e.to_string())
            })?;

        Ok(written)
    }

    /// Convert a `serde_json::Value` to a `bson::Document`.
    ///
    /// Returns a `Sink` error if the value is not a JSON object.
    fn value_to_document(val: &Value) -> Result<Document, FaucetError> {
        let bson = bson::to_bson(val)
            .map_err(|e| FaucetError::Sink(format!("failed to convert JSON to BSON: {e}")))?;
        match bson {
            Bson::Document(doc) => Ok(doc),
            other => Err(FaucetError::Sink(format!(
                "expected a JSON object, got BSON type: {other:?}"
            ))),
        }
    }
}

#[async_trait]
impl faucet_core::Sink for MongoSink {
    fn connector_name(&self) -> &'static str {
        "mongodb"
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(MongoSinkConfig))
            .expect("schema serialization")
    }

    fn supports_cleanup(&self) -> bool {
        // Unconditional: MongoDB is schemaless, so the scope and key predicates
        // address document fields that need no declaration up front (there is no
        // column-mapping mode to exclude, as on the SQL sinks). The remaining
        // requirement — a non-empty `key` — is enforced by `WriteSpec::validate`
        // at config-load time (cleanup implies `write_mode: upsert`) and again in
        // `cleanup_scope`.
        true
    }

    async fn cleanup_scope(
        &self,
        scope: &std::collections::BTreeMap<String, Value>,
        seen: &faucet_core::SeenKeys,
    ) -> Result<u64, FaucetError> {
        self.cleanup_scope_impl(scope, seen).await
    }

    fn supported_write_modes(&self) -> &'static [faucet_core::WriteMode] {
        &[
            faucet_core::WriteMode::Append,
            faucet_core::WriteMode::Upsert,
            faucet_core::WriteMode::Delete,
            faucet_core::WriteMode::Overwrite,
        ]
    }

    fn is_overwrite(&self) -> bool {
        self.config.write.is_overwrite()
    }

    /// Drop any leftover staging collection so the overwrite run starts with an
    /// empty one (a plain `insert_many` auto-creates it on first write).
    async fn begin_overwrite(&self) -> Result<(), FaucetError> {
        let db = self.client.database(&self.config.database);
        // Best-effort drop; a missing namespace is not an error for our purpose.
        if let Err(e) = db
            .collection::<Document>(&self.staging_collection())
            .drop()
            .await
        {
            tracing::debug!(error = %e, "mongodb overwrite: staging drop before begin (ignored)");
        }
        Ok(())
    }

    /// Atomically replace the destination via MongoDB's `renameCollection` with
    /// `dropTarget: true`: the freshly-loaded staging collection is renamed over
    /// the real one in a single server-side operation, so a reader never sees a
    /// half-replaced collection and a mid-run failure (before this point) leaves
    /// the old collection untouched.
    ///
    /// Requires the `renameCollection` privilege and that both collections live
    /// in the same database. `renameCollection` is not supported on sharded
    /// collections — a documented limitation of overwrite on MongoDB.
    async fn commit_overwrite(&self) -> Result<(), FaucetError> {
        let from = format!("{}.{}", self.config.database, self.staging_collection());
        let to = format!("{}.{}", self.config.database, self.config.collection);
        self.client
            .database("admin")
            .run_command(bson::doc! {
                "renameCollection": from,
                "to": to,
                "dropTarget": true,
            })
            .await
            .map_err(|e| {
                FaucetError::Sink(format!(
                    "mongodb overwrite swap (renameCollection) failed: {e}"
                ))
            })?;
        Ok(())
    }

    /// Drop the staging collection so a failed/cancelled overwrite leaves
    /// nothing behind. Best-effort — the destination was never touched.
    async fn abort_overwrite(&self) -> Result<(), FaucetError> {
        if let Err(e) = self
            .client
            .database(&self.config.database)
            .collection::<Document>(&self.staging_collection())
            .drop()
            .await
        {
            tracing::debug!(error = %e, "mongodb overwrite: staging drop on abort (ignored)");
        }
        Ok(())
    }

    fn dedups_by_key(&self) -> bool {
        self.config.write.dedups_by_key()
    }

    fn dataset_uri(&self) -> String {
        format!(
            "{}/{}/{}",
            faucet_core::redact_uri_credentials(&self.config.connection_uri),
            self.config.database,
            self.config.collection
        )
    }

    /// Non-mutating preflight probe: run the `ping` admin command against the
    /// configured database via the existing client (probe name `"ping"`).
    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};

        let started = std::time::Instant::now();
        let hint = "check connection_uri / credentials / that the MongoDB server is reachable";

        let db = self.client.database(&self.config.database);
        let probe =
            match tokio::time::timeout(ctx.timeout, db.run_command(bson::doc! {"ping": 1})).await {
                Ok(Ok(_)) => Probe::pass("ping", started.elapsed()),
                Ok(Err(e)) => Probe::fail_hint("ping", started.elapsed(), e.to_string(), hint),
                Err(_) => Probe::fail_hint("ping", started.elapsed(), "timed out", hint),
            };
        Ok(CheckReport::single(probe))
    }

    /// Write records to MongoDB.
    ///
    /// When `config.batch_size > 0` and the input slice is larger than
    /// `batch_size`, the slice is split into chunks of `batch_size` documents
    /// and each chunk is sent as a separate `insert_many` call. When
    /// `config.batch_size == 0`, the entire slice is sent in a single
    /// `insert_many` request — useful when upstream `StreamPage`s are already
    /// sized for MongoDB's preferred per-request limits.
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        // Upsert / delete routing: plan the page (dedup last-write-wins, strip
        // the delete marker) and apply per-document `replace_one(upsert)` /
        // `delete_one` ops. Append falls through to the `insert_many` fast path.
        if matches!(
            self.config.write.write_mode,
            faucet_core::WriteMode::Upsert | faucet_core::WriteMode::Delete
        ) {
            let plan = faucet_core::plan_writes(records, &self.config.write);
            if let Some((idx, msg)) = plan.failed.first() {
                return Err(FaucetError::Sink(format!(
                    "mongodb {}: row {idx}: {msg}",
                    self.config.write.write_mode.as_str()
                )));
            }
            return self.apply_plan(&plan).await;
        }

        // Append and overwrite are insert-shaped; overwrite inserts land in the
        // staging collection via `effective_collection`.
        let collection = self
            .client
            .database(&self.config.database)
            .collection::<Document>(&self.effective_collection());

        // `batch_size = 0` is the "no batching" sentinel: forward whatever
        // upstream handed us as a single `insert_many`, preserving
        // `StreamPage` framing. Otherwise re-chunk into `batch_size` slices.
        let effective_chunk = if self.config.batch_size == 0 {
            records.len()
        } else {
            self.config.batch_size
        };

        let mut total_written = 0usize;

        for chunk in records.chunks(effective_chunk) {
            let docs: Vec<Document> = chunk
                .iter()
                .map(Self::value_to_document)
                .collect::<Result<Vec<_>, _>>()?;

            let opts = mongodb::options::InsertManyOptions::builder()
                .ordered(self.config.ordered)
                .build();
            collection
                .insert_many(&docs)
                .with_options(opts)
                .await
                .map_err(|e| FaucetError::Sink(format!("MongoDB insert_many failed: {e}")))?;

            total_written += docs.len();
            tracing::debug!(batch_size = docs.len(), "MongoDB batch inserted");
        }

        tracing::info!(
            records = total_written,
            database = %self.config.database,
            collection = %self.config.collection,
            "MongoDB write complete"
        );

        Ok(total_written)
    }

    /// Write a batch and report per-row outcomes.
    ///
    /// In append mode this delegates to [`write_batch`](faucet_core::Sink::write_batch) and
    /// maps a single success onto an all-`Ok(())` vector (the trait default).
    /// In upsert/delete mode the good rows are applied (upserts + deletes), and
    /// only the rows whose key could not be extracted (missing / null key) are
    /// reported as `Err` so the pipeline routes them to the DLQ per-row instead
    /// of sending the whole page.
    async fn write_batch_partial(
        &self,
        records: &[Value],
    ) -> Result<Vec<faucet_core::RowOutcome>, FaucetError> {
        if !matches!(
            self.config.write.write_mode,
            faucet_core::WriteMode::Upsert | faucet_core::WriteMode::Delete
        ) {
            // Append and overwrite: insert-shaped, no per-row key failures.
            self.write_batch(records).await?;
            return Ok(records.iter().map(|_| Ok(())).collect());
        }

        let plan = faucet_core::plan_writes(records, &self.config.write);
        self.apply_plan(&plan).await?;

        let mut outcomes: Vec<faucet_core::RowOutcome> = records.iter().map(|_| Ok(())).collect();
        for (idx, msg) in &plan.failed {
            outcomes[*idx] = Err(FaucetError::Sink(format!(
                "mongodb {}: {msg}",
                self.config.write.write_mode.as_str()
            )));
        }
        Ok(outcomes)
    }

    fn supports_idempotent_writes(&self) -> bool {
        true
    }

    /// Read the current watermark for `scope` from the
    /// [`_faucet_commit_token`](faucet_core::idempotency::COMMIT_TOKEN_TABLE)
    /// collection. Returns `Ok(None)` for an unknown scope (or a missing
    /// collection). The token is opaque and returned verbatim — never parsed.
    async fn last_committed_token(&self, scope: &str) -> Result<Option<String>, FaucetError> {
        let doc = self
            .commit_token_collection()
            .find_one(commit_token_filter(scope))
            .await
            .map_err(|e| FaucetError::Sink(format!("MongoDB commit-token read failed: {e}")))?;
        doc.map(|d| token_from_commit_doc(&d)).transpose()
    }

    /// Write one page and its commit-token watermark **atomically**, inside a
    /// single multi-document transaction.
    ///
    /// Requires a **replica set or sharded cluster** — MongoDB transactions
    /// are unavailable on a standalone server, and that failure is surfaced
    /// as a self-explanatory typed error.
    ///
    /// Semantics on this path (vs. the at-least-once `write_batch`):
    /// - **One page = one transaction.** Append mode inserts the whole page
    ///   in a single `insert_many` — `batch_size` re-chunking does NOT apply
    ///   here (chunking would break page↔watermark atomicity). Size pages via
    ///   the source's `batch_size` instead.
    /// - **Upsert/delete ops run sequentially** with the session (a
    ///   `ClientSession` cannot be used concurrently), trading the
    ///   at-least-once path's concurrent fan-out for atomicity.
    /// - The commit is retried while the driver reports
    ///   `UnknownTransactionCommitResult` (bounded), per the driver's
    ///   recommended pattern; any other failure aborts the transaction
    ///   (best-effort) and surfaces the original error.
    async fn write_batch_idempotent(
        &self,
        records: &[Value],
        scope: &str,
        token: &str,
    ) -> Result<usize, FaucetError> {
        self.ensure_exactly_once_collections().await?;

        // Prepare (plan + convert to BSON) BEFORE the transaction so a
        // key-extraction or conversion failure aborts without ever opening a
        // transaction — mirroring the postgres sink's fail-fast on plan errors.
        let prepared = if matches!(self.config.write.write_mode, faucet_core::WriteMode::Append) {
            PreparedWrite::Append(
                records
                    .iter()
                    .map(Self::value_to_document)
                    .collect::<Result<Vec<_>, _>>()?,
            )
        } else {
            let plan = faucet_core::plan_writes(records, &self.config.write);
            if let Some((idx, msg)) = plan.failed.first() {
                return Err(FaucetError::Sink(format!(
                    "mongodb {}: row {idx}: {msg}",
                    self.config.write.write_mode.as_str()
                )));
            }
            PreparedWrite::Planned(self.build_plan_ops(&plan)?)
        };

        let mut session = self
            .client
            .start_session()
            .await
            .map_err(|e| FaucetError::Sink(format!("MongoDB session start failed: {e}")))?;
        session.start_transaction().await.map_err(|e| {
            classify_transaction_error("MongoDB transaction start failed", &e.to_string())
        })?;

        let written = match self
            .apply_in_transaction(&mut session, prepared, scope, token)
            .await
        {
            Ok(written) => written,
            Err(e) => {
                // Best-effort abort so the server releases the transaction's
                // locks promptly; the original error is what matters.
                if let Err(abort_err) = session.abort_transaction().await {
                    tracing::debug!(error = %abort_err, "MongoDB abort_transaction failed (best-effort)");
                }
                return Err(e);
            }
        };

        // Driver-recommended commit retry: `commit_transaction` is itself
        // retryable while the error carries the UnknownTransactionCommitResult
        // label (e.g. a primary failover mid-commit). Bounded so a persistent
        // outage surfaces instead of spinning forever.
        let mut attempt = 0usize;
        loop {
            match session.commit_transaction().await {
                Ok(()) => break,
                Err(e)
                    if e.contains_label(mongodb::error::UNKNOWN_TRANSACTION_COMMIT_RESULT)
                        && attempt < MAX_COMMIT_RETRIES =>
                {
                    attempt += 1;
                    tracing::warn!(
                        attempt,
                        error = %e,
                        "MongoDB commit returned UnknownTransactionCommitResult; retrying commit"
                    );
                }
                Err(e) => {
                    return Err(classify_transaction_error(
                        "MongoDB transaction commit failed",
                        &e.to_string(),
                    ));
                }
            }
        }

        tracing::info!(
            records = written,
            scope,
            token,
            database = %self.config.database,
            collection = %self.config.collection,
            "MongoDB exactly-once page committed with watermark"
        );

        Ok(written)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // dataset_uri test is skipped: MongoSink::new() requires a live MongoDB
    // connection (Client::with_uri_str connects in new()), and no offline
    // constructor exists.

    #[test]
    fn filter_doc_from_key_tuple() {
        let kt = faucet_core::KeyTuple(vec![
            ("tenant".to_string(), serde_json::json!("acme")),
            ("id".to_string(), serde_json::json!(7)),
        ]);
        let m = faucet_core::key_to_filter(&kt);
        assert_eq!(m.get("tenant"), Some(&serde_json::json!("acme")));
        assert_eq!(m.get("id"), Some(&serde_json::json!(7)));
        // and it converts to a bson filter Document via the sink's converter:
        let doc = super::json_map_to_bson_filter(&m).expect("filter converts to bson");
        assert_eq!(doc.get_str("tenant").unwrap(), "acme");
        assert_eq!(doc.get_i64("id").unwrap(), 7);
    }

    #[test]
    fn filter_from_row_pulls_only_key_columns() {
        let row = json!({"_id": 5, "name": "a", "extra": true});
        let doc = MongoSink::filter_from_row(&row, &["_id".to_string()]).expect("filter");
        assert_eq!(doc.get_i64("_id").unwrap(), 5);
        assert!(
            !doc.contains_key("name"),
            "filter must contain only key columns"
        );
        assert!(!doc.contains_key("extra"));
    }

    #[test]
    fn value_to_document_object() {
        let val = json!({"name": "Alice", "age": 30});
        let doc = MongoSink::value_to_document(&val).unwrap();
        assert_eq!(doc.get_str("name").unwrap(), "Alice");
        assert_eq!(doc.get_i64("age").unwrap(), 30);
    }

    #[test]
    fn value_to_document_non_object_fails() {
        let val = json!([1, 2, 3]);
        let result = MongoSink::value_to_document(&val);
        assert!(result.is_err());
        assert!(matches!(result, Err(FaucetError::Sink(_))));
    }

    #[test]
    fn value_to_document_string_fails() {
        let val = json!("not an object");
        let result = MongoSink::value_to_document(&val);
        assert!(result.is_err());
    }

    #[test]
    fn value_to_document_nested() {
        let val = json!({"user": {"name": "Bob"}, "tags": ["a", "b"]});
        let doc = MongoSink::value_to_document(&val).unwrap();
        let inner = doc.get_document("user").unwrap();
        assert_eq!(inner.get_str("name").unwrap(), "Bob");
    }

    #[test]
    fn value_to_document_empty_object() {
        let val = json!({});
        let doc = MongoSink::value_to_document(&val).unwrap();
        assert!(doc.is_empty());
    }

    #[test]
    fn value_to_document_null_fails() {
        let val = Value::Null;
        let result = MongoSink::value_to_document(&val);
        assert!(result.is_err());
    }

    // -- exactly-once pure helpers ------------------------------------------

    #[test]
    fn commit_token_filter_uses_scope_as_id() {
        let filter = super::commit_token_filter("pipe::row");
        assert_eq!(filter.len(), 1, "filter must match on _id only");
        assert_eq!(filter.get_str("_id").unwrap(), "pipe::row");
    }

    #[test]
    fn commit_token_doc_shape() {
        let doc = super::commit_token_doc("pipe::row", "00000000000000000042");
        assert_eq!(doc.get_str("_id").unwrap(), "pipe::row");
        assert_eq!(
            doc.get_str(faucet_core::idempotency::COMMIT_TOKEN_TOKEN_COL)
                .unwrap(),
            "00000000000000000042"
        );
        assert_eq!(doc.len(), 2, "watermark doc carries only _id + token");
    }

    #[test]
    fn commit_token_doc_token_is_opaque() {
        // Tokens may carry '#' + JSON payload; the doc must store it verbatim.
        let token = r##"00000000000000000007#{"lsn":"0/16B3748"}"##;
        let doc = super::commit_token_doc("s", token);
        assert_eq!(
            doc.get_str(faucet_core::idempotency::COMMIT_TOKEN_TOKEN_COL)
                .unwrap(),
            token
        );
    }

    #[test]
    fn token_from_commit_doc_reads_string_token() {
        let doc = super::commit_token_doc("s", "tok-1");
        assert_eq!(super::token_from_commit_doc(&doc).unwrap(), "tok-1");
    }

    #[test]
    fn token_from_commit_doc_missing_field_is_typed_error() {
        let doc = bson::doc! { "_id": "s" };
        let err = super::token_from_commit_doc(&doc).unwrap_err();
        match err {
            FaucetError::Sink(m) => assert!(m.contains("missing"), "got: {m}"),
            other => panic!("expected Sink error, got {other:?}"),
        }
    }

    #[test]
    fn token_from_commit_doc_non_string_token_is_typed_error() {
        let doc = bson::doc! { "_id": "s", "token": 42_i64 };
        let err = super::token_from_commit_doc(&doc).unwrap_err();
        match err {
            FaucetError::Sink(m) => assert!(m.contains("expected a string"), "got: {m}"),
            other => panic!("expected Sink error, got {other:?}"),
        }
    }

    #[test]
    fn transactions_unsupported_detects_standalone_message() {
        // The canonical standalone-server message (code 20, IllegalOperation).
        assert!(super::is_transactions_unsupported(
            "Command failed: Transaction numbers are only allowed on a replica set member or mongos"
        ));
        // Case-insensitive.
        assert!(super::is_transactions_unsupported(
            "TRANSACTION NUMBERS ARE ONLY ALLOWED ON A REPLICA SET MEMBER OR MONGOS"
        ));
        // The Rust driver's rewrite of the same server error (what actually
        // surfaces from a standalone `mongod` through mongodb v3).
        assert!(super::is_transactions_unsupported(
            "Kind: Command failed: Error code 20 (IllegalOperation): This MongoDB deployment \
             does not support retryable writes. Please add retryWrites=false to your \
             connection string."
        ));
        // Alternate phrasing.
        assert!(super::is_transactions_unsupported(
            "Transactions are not supported by this deployment"
        ));
    }

    #[test]
    fn transactions_unsupported_ignores_other_errors() {
        assert!(!super::is_transactions_unsupported("duplicate key error"));
        assert!(!super::is_transactions_unsupported(
            "connection refused: mongodb://localhost:27017"
        ));
        assert!(!super::is_transactions_unsupported(""));
    }

    #[test]
    fn classify_transaction_error_maps_standalone_to_replica_set_message() {
        let orig = "Transaction numbers are only allowed on a replica set member or mongos";
        let err = super::classify_transaction_error("MongoDB insert_many failed", orig);
        match err {
            FaucetError::Sink(m) => {
                assert!(
                    m.contains("requires a replica set or sharded cluster"),
                    "got: {m}"
                );
                assert!(m.contains("write_batch_idempotent"), "got: {m}");
                assert!(m.contains(orig), "original error must be preserved: {m}");
            }
            other => panic!("expected Sink error, got {other:?}"),
        }
    }

    #[test]
    fn classify_transaction_error_keeps_context_for_other_errors() {
        let err = super::classify_transaction_error("MongoDB commit failed", "network timeout");
        match err {
            FaucetError::Sink(m) => {
                assert_eq!(m, "MongoDB commit failed: network timeout");
            }
            other => panic!("expected Sink error, got {other:?}"),
        }
    }

    #[test]
    fn namespace_exists_code_predicate() {
        assert!(super::is_namespace_exists_code(Some(48)));
        assert!(!super::is_namespace_exists_code(Some(20)));
        assert!(!super::is_namespace_exists_code(None));
    }

    // -- scoped cleanup (#478) pure helpers ---------------------------------

    fn kt(pairs: &[(&str, Value)]) -> faucet_core::KeyTuple {
        faucet_core::KeyTuple(
            pairs
                .iter()
                .map(|(c, v)| (c.to_string(), v.clone()))
                .collect(),
        )
    }

    fn scope_of(pairs: &[(&str, Value)]) -> std::collections::BTreeMap<String, Value> {
        pairs
            .iter()
            .map(|(c, v)| (c.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn cleanup_filter_single_key_uses_nin() {
        let filter = super::cleanup_filter_json(
            &scope_of(&[("contact_id", json!(7))]),
            &["id".to_string()],
            &[kt(&[("id", json!(1))]), kt(&[("id", json!("a"))])],
        );
        assert_eq!(
            filter,
            json!({"$and": [
                {"contact_id": 7},
                {"id": {"$nin": [1, "a"]}},
            ]})
        );
    }

    #[test]
    fn cleanup_filter_composite_key_uses_nor_of_tuples() {
        // No tuple `$nin` in BSON — a composite key becomes "matches none of the
        // written tuples", with each tuple compared as a whole.
        let filter = super::cleanup_filter_json(
            &scope_of(&[("contact_id", json!(7))]),
            &["tenant".to_string(), "id".to_string()],
            &[
                kt(&[("tenant", json!("acme")), ("id", json!(1))]),
                kt(&[("tenant", json!("acme")), ("id", json!(2))]),
            ],
        );
        assert_eq!(
            filter,
            json!({"$and": [
                {"contact_id": 7},
                {"$nor": [
                    {"tenant": "acme", "id": 1},
                    {"tenant": "acme", "id": 2},
                ]},
            ]})
        );
    }

    #[test]
    fn cleanup_filter_empty_seen_deletes_the_whole_scope() {
        // The motivating case: the source says "this scope is now empty", so the
        // filter must be the scope predicate alone — NOT a no-op, and not an
        // empty `$nin`/`$nor` (an empty `$nor` is rejected by the server).
        let filter = super::cleanup_filter_json(
            &scope_of(&[("contact_id", json!(7))]),
            &["id".to_string()],
            &[],
        );
        assert_eq!(filter, json!({"$and": [{"contact_id": 7}]}));
    }

    #[test]
    fn cleanup_filter_keeps_scope_when_it_overlaps_the_key() {
        // Regression guard: as sibling fields in one document, the second
        // `contact_id` would overwrite the first and the delete would widen to
        // the whole collection. Separate `$and` clauses keep both predicates.
        let filter = super::cleanup_filter_json(
            &scope_of(&[("contact_id", json!(7))]),
            &["contact_id".to_string(), "assoc_id".to_string()],
            &[kt(&[("contact_id", json!(7)), ("assoc_id", json!(1))])],
        );
        let clauses = filter["$and"].as_array().expect("$and array");
        assert_eq!(clauses.len(), 2, "{filter}");
        assert_eq!(clauses[0], json!({"contact_id": 7}), "scope survives");
        assert_eq!(
            clauses[1],
            json!({"$nor": [{"contact_id": 7, "assoc_id": 1}]})
        );
    }

    #[test]
    fn cleanup_filter_and_clause_per_scope_column() {
        let filter = super::cleanup_filter_json(
            &scope_of(&[("a", json!(1)), ("b", json!("x"))]),
            &["id".to_string()],
            &[kt(&[("id", json!(5))])],
        );
        let clauses = filter["$and"].as_array().unwrap();
        assert_eq!(
            clauses.len(),
            3,
            "two scope clauses + the not-written clause"
        );
        // BTreeMap ordering makes the scope clauses deterministic.
        assert_eq!(clauses[0], json!({"a": 1}));
        assert_eq!(clauses[1], json!({"b": "x"}));
    }

    #[test]
    fn cleanup_filter_converts_to_bson() {
        let filter = super::cleanup_filter_json(
            &scope_of(&[("contact_id", json!(7))]),
            &["id".to_string()],
            &[kt(&[("id", json!(1))])],
        );
        let doc = MongoSink::value_to_document(&filter).expect("filter converts to bson");
        assert_eq!(doc.get_array("$and").unwrap().len(), 2);
    }

    #[test]
    fn key_alignment_accepts_matching_tuples() {
        let key = vec!["tenant".to_string(), "id".to_string()];
        let seen = vec![kt(&[("tenant", json!("acme")), ("id", json!(1))])];
        assert!(super::check_key_alignment(&key, &seen).is_ok());
    }

    #[test]
    fn key_alignment_rejects_a_differently_keyed_tuple() {
        // A tuple keyed on another field would make written documents look
        // unwritten — i.e. delete them.
        let key = vec!["id".to_string()];
        let seen = vec![kt(&[("other", json!(1))])];
        let err = super::check_key_alignment(&key, &seen).expect_err("must refuse");
        assert!(err.to_string().contains("refusing to delete"), "{err}");
    }

    #[test]
    fn key_alignment_rejects_a_wrong_width_tuple() {
        let key = vec!["tenant".to_string(), "id".to_string()];
        let seen = vec![kt(&[("tenant", json!("acme"))])];
        assert!(super::check_key_alignment(&key, &seen).is_err());
    }

    #[test]
    fn filter_size_within_the_ceiling_is_accepted() {
        assert!(super::check_cleanup_filter_size(super::MAX_CLEANUP_FILTER_BYTES, 10).is_ok());
    }

    #[test]
    fn oversized_filter_is_refused_with_the_ceiling_named() {
        // The written-key clause cannot be chunked, so an outsized key set must
        // be refused outright rather than half-issued.
        let err = super::check_cleanup_filter_size(super::MAX_CLEANUP_FILTER_BYTES + 1, 250_000)
            .expect_err("must refuse");
        let msg = err.to_string();
        assert!(
            msg.contains(&super::MAX_CLEANUP_FILTER_BYTES.to_string()),
            "{msg}"
        );
        assert!(msg.contains("250000"), "{msg}");
        assert!(msg.contains("Nothing was deleted"), "{msg}");
    }

    #[test]
    fn a_realistic_key_set_stays_well_under_the_ceiling() {
        // Sanity check on the ceiling's sizing: the core cleanup default tracks
        // up to 100k keys, and a filter that wide must still fit in one command.
        let seen: Vec<faucet_core::KeyTuple> =
            (0..100_000).map(|i| kt(&[("id", json!(i))])).collect();
        let filter = super::cleanup_filter_json(
            &scope_of(&[("contact_id", json!(7))]),
            &["id".to_string()],
            &seen,
        );
        let doc = MongoSink::value_to_document(&filter).unwrap();
        let bytes = bson::to_vec(&doc).unwrap().len();
        assert!(
            super::check_cleanup_filter_size(bytes, seen.len()).is_ok(),
            "100k scalar keys serialized to {bytes} bytes"
        );
    }

    #[tokio::test]
    async fn new_rejects_out_of_range_batch_size() {
        let mut config = MongoSinkConfig::new("mongodb://localhost:27017", "db", "c");
        config.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
        match MongoSink::new(config).await {
            Err(faucet_core::FaucetError::Config(m)) => {
                assert!(m.contains("batch_size"), "got: {m}")
            }
            _ => panic!("expected a batch_size Config error"),
        }
    }
}
