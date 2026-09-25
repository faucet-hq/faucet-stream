//! Elasticsearch bulk index sink.

use crate::config::{ElasticsearchAuth, ElasticsearchSinkConfig};
use async_trait::async_trait;
use faucet_core::util::{DEFAULT_ERROR_BODY_MAX_LEN, check_http_response};
use faucet_core::{
    AuthSpec, FaucetError, SchemaEvolution, SharedAuthProvider, SqlBaseType, json_schema_base_type,
};
use reqwest::Client;
use serde_json::Value;
use std::sync::atomic::{AtomicBool, Ordering};

/// True when a page is split into more than one `_bulk` chunk *and* documents
/// get auto-generated IDs (no `id_field`). In that configuration an earlier
/// chunk can commit before a later chunk fails; because the bookmark only
/// advances after the whole page is written, a resumed run re-sends the earlier
/// chunk, and auto-generated IDs make those re-sends **duplicates** rather than
/// idempotent overwrites. Setting `id_field` (or configuring a DLQ, whose
/// per-row `write_batch_partial` path avoids the whole-page re-send) makes
/// resume idempotent.
fn resume_dup_risk(chunk_count: usize, has_id_field: bool) -> bool {
    chunk_count > 1 && !has_id_field
}

/// A sink that writes JSON records to an Elasticsearch index using the bulk API.
pub struct ElasticsearchSink {
    config: ElasticsearchSinkConfig,
    client: Client,
    /// Optional shared auth provider. When set it takes precedence over inline
    /// auth. Injected by the CLI (to resolve `auth: { ref }`) or directly by
    /// library callers who want to share one token across multiple sinks.
    auth_provider: Option<SharedAuthProvider>,
    /// One-shot guard so the resume-duplication warning (see [`resume_dup_risk`])
    /// is logged at most once per sink instance, not per page.
    resume_dup_warned: AtomicBool,
    /// One-shot guard for the "cannot change an existing field's mapping" debug
    /// log emitted by [`evolve_schema`](faucet_core::Sink::evolve_schema) when an
    /// evolution carries widenings / nullability relaxations (no-ops on ES).
    evolve_noop_warned: AtomicBool,
    /// In-flight `write_mode: overwrite` state (#494). `begin_overwrite` records
    /// the fresh staging physical index (which every `write_batch` then targets)
    /// plus the alias's previous physical targets to detach on commit;
    /// `commit`/`abort` clear it. `None` outside an overwrite run.
    overwrite: std::sync::Mutex<Option<OverwriteState>>,
}

/// Staging state for an Elasticsearch `write_mode: overwrite` run (#494).
#[derive(Clone, Debug)]
struct OverwriteState {
    /// Fresh physical index this run writes into (e.g. `orders-faucet-ovw-<n>`).
    staging: String,
    /// Physical indices the read alias currently points at, to remove on commit.
    previous: Vec<String>,
}

/// Unique staging physical-index name for an overwrite run — the alias target's
/// stand-in until the atomic swap. Pure so it can be unit-tested.
fn staging_index_name(alias: &str, nonce: u128) -> String {
    format!("{alias}-faucet-ovw-{nonce:x}")
}

/// Build the body for an atomic `POST /_aliases` swap: detach `alias` from every
/// `previous` physical index and attach it to `staging`, all applied atomically
/// by Elasticsearch. Pure.
fn build_alias_swap_actions(alias: &str, staging: &str, previous: &[String]) -> Value {
    let mut actions: Vec<Value> = previous
        .iter()
        .map(|idx| serde_json::json!({ "remove": { "index": idx, "alias": alias } }))
        .collect();
    actions.push(serde_json::json!({ "add": { "index": staging, "alias": alias } }));
    serde_json::json!({ "actions": actions })
}

impl ElasticsearchSink {
    /// Create a new Elasticsearch sink from the given configuration.
    ///
    /// Returns [`FaucetError::Config`] if `batch_size` exceeds
    /// `MAX_BATCH_SIZE` (#78/#44).
    pub fn new(config: ElasticsearchSinkConfig) -> Result<Self, FaucetError> {
        faucet_core::validate_batch_size(config.batch_size)?;
        // Schemaless target: upsert/delete only need a non-empty `key` (no
        // column-mapping guard like the SQL sinks).
        config.write.validate()?;
        Ok(Self {
            config,
            client: Client::new(),
            auth_provider: None,
            resume_dup_warned: AtomicBool::new(false),
            evolve_noop_warned: AtomicBool::new(false),
            overwrite: std::sync::Mutex::new(None),
        })
    }

    /// The physical index the current `write_batch` should target: the overwrite
    /// staging index while an overwrite run is in flight, otherwise the
    /// configured `index` (which may be an alias).
    fn write_index(&self) -> String {
        self.overwrite
            .lock()
            .expect("overwrite lock")
            .as_ref()
            .map(|s| s.staging.clone())
            .unwrap_or_else(|| self.config.index.clone())
    }

    /// Physical indices the read alias `alias` currently points at. Empty when
    /// the alias does not exist yet (first overwrite run). Errors when `alias`
    /// names a **concrete index** — overwrite requires an alias (#494).
    async fn overwrite_alias_targets(
        &self,
        alias: &str,
        auth: &ElasticsearchAuth,
    ) -> Result<Vec<String>, FaucetError> {
        let url = format!("{}/_alias/{}", self.config.base_url, alias);
        let resp = Self::apply_auth_value(self.client.get(&url), auth)
            .send()
            .await?;
        if resp.status().as_u16() == 404 {
            // No alias of that name. If a concrete index owns the name, refuse —
            // there is no atomic replace of a concrete index.
            let head_url = format!("{}/{}", self.config.base_url, alias);
            let head = Self::apply_auth_value(self.client.head(&head_url), auth)
                .send()
                .await?;
            if head.status().is_success() {
                return Err(FaucetError::Sink(format!(
                    "elasticsearch overwrite: `{alias}` is a concrete index, not an alias. \
                     write_mode: overwrite swaps an alias atomically, so point `index` at an \
                     alias (or a not-yet-existing name) instead."
                )));
            }
            return Ok(Vec::new());
        }
        let resp = check_http_response(resp, DEFAULT_ERROR_BODY_MAX_LEN).await?;
        let body: Value = resp.json().await?;
        Ok(body
            .as_object()
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default())
    }

    /// Read `index`'s mappings so the staging index inherits them; `None` if the
    /// index or its mappings can't be read (staging then relies on dynamic mapping).
    async fn overwrite_read_mappings(
        &self,
        index: &str,
        auth: &ElasticsearchAuth,
    ) -> Result<Option<Value>, FaucetError> {
        let url = format!("{}/{}/_mapping", self.config.base_url, index);
        let resp = Self::apply_auth_value(self.client.get(&url), auth)
            .send()
            .await?;
        if !resp.status().is_success() {
            return Ok(None);
        }
        let body: Value = resp.json().await?;
        Ok(body
            .as_object()
            .and_then(|m| m.values().next())
            .and_then(|v| v.get("mappings"))
            .cloned())
    }

    /// Create the staging physical index, seeding its mappings when known.
    async fn overwrite_create_index(
        &self,
        index: &str,
        mappings: Option<Value>,
        auth: &ElasticsearchAuth,
    ) -> Result<(), FaucetError> {
        let mut body = serde_json::Map::new();
        if let Some(m) = mappings {
            body.insert("mappings".to_string(), m);
        }
        let url = format!("{}/{}", self.config.base_url, index);
        let req = self
            .client
            .put(&url)
            .header("Content-Type", "application/json")
            .body(serde_json::to_string(&Value::Object(body)).map_err(|e| {
                FaucetError::Sink(format!("overwrite: serialize create-index body: {e}"))
            })?);
        let resp = Self::apply_auth_value(req, auth).send().await?;
        check_http_response(resp, DEFAULT_ERROR_BODY_MAX_LEN).await?;
        Ok(())
    }

    /// `POST /<index>/_refresh` so freshly-staged docs are searchable pre-swap.
    async fn overwrite_refresh(
        &self,
        index: &str,
        auth: &ElasticsearchAuth,
    ) -> Result<(), FaucetError> {
        let url = format!("{}/{}/_refresh", self.config.base_url, index);
        let resp = Self::apply_auth_value(self.client.post(&url), auth)
            .send()
            .await?;
        check_http_response(resp, DEFAULT_ERROR_BODY_MAX_LEN).await?;
        Ok(())
    }

    /// `DELETE /<index>`.
    async fn overwrite_delete_index(
        &self,
        index: &str,
        auth: &ElasticsearchAuth,
    ) -> Result<(), FaucetError> {
        let url = format!("{}/{}", self.config.base_url, index);
        let resp = Self::apply_auth_value(self.client.delete(&url), auth)
            .send()
            .await?;
        check_http_response(resp, DEFAULT_ERROR_BODY_MAX_LEN).await?;
        Ok(())
    }

    /// Attach a shared [`AuthProvider`](faucet_core::AuthProvider). When set,
    /// the provider supplies the credential for every request (taking precedence
    /// over inline auth). Used by the CLI to resolve `auth: { ref }`, and by
    /// library callers who inject one provider into many sinks.
    pub fn with_auth_provider(mut self, provider: SharedAuthProvider) -> Self {
        self.auth_provider = Some(provider);
        self
    }

    /// Resolve the effective [`ElasticsearchAuth`] for the current batch.
    ///
    /// Resolution order:
    /// 1. If a shared provider is attached, call it and map the credential.
    /// 2. Otherwise use the inline auth from config.
    /// 3. If the config is a `Reference` with no provider, return an error.
    async fn resolve_auth(&self) -> Result<ElasticsearchAuth, FaucetError> {
        if let Some(p) = &self.auth_provider {
            return faucet_common_elasticsearch::credential_to_auth(p.credential().await?);
        }
        match &self.config.auth {
            AuthSpec::Inline(a) => Ok(a.clone()),
            AuthSpec::Reference(r) => Err(FaucetError::Auth(format!(
                "auth references provider '{}' but no provider was supplied",
                r.name
            ))),
        }
    }

    /// Apply an [`ElasticsearchAuth`] to a request builder.
    fn apply_auth_value(
        req: reqwest::RequestBuilder,
        auth: &ElasticsearchAuth,
    ) -> reqwest::RequestBuilder {
        match auth {
            ElasticsearchAuth::None => req,
            ElasticsearchAuth::Basic { username, password } => {
                req.basic_auth(username, Some(password))
            }
            ElasticsearchAuth::Bearer { token } => req.bearer_auth(token),
            ElasticsearchAuth::ApiKey { key } => {
                req.header("Authorization", format!("ApiKey {key}"))
            }
        }
    }

    /// Send a `POST /_bulk` request for a slice of records and return the raw
    /// response body as a [`Value`].
    ///
    /// All HTTP-level errors (non-2xx status, network failures, JSON parse
    /// errors) surface as `Err(FaucetError::…)`. Item-level errors inside the
    /// response body are left to the caller to inspect.
    ///
    /// `auth` must be pre-resolved by the caller (once per `write_batch`) so
    /// the provider is not called on every chunk.
    async fn send_bulk_raw(
        &self,
        chunk: &[Value],
        auth: &ElasticsearchAuth,
    ) -> Result<Value, FaucetError> {
        let body = self.build_bulk_body(chunk)?;
        self.send_bulk_body(body, auth).await
    }

    /// Send a pre-built NDJSON `_bulk` body and return the parsed response.
    ///
    /// Shared by the append path ([`send_bulk_raw`](Self::send_bulk_raw)) and
    /// the upsert/delete path ([`build_plan_body`](Self::build_plan_body)).
    async fn send_bulk_body(
        &self,
        body: String,
        auth: &ElasticsearchAuth,
    ) -> Result<Value, FaucetError> {
        let url = format!("{}/_bulk", self.config.base_url);
        let req = self
            .client
            .post(&url)
            .header("Content-Type", "application/x-ndjson")
            .body(body);
        let req = Self::apply_auth_value(req, auth);
        let resp = req.send().await?;
        let resp = check_http_response(resp, DEFAULT_ERROR_BODY_MAX_LEN).await?;
        let resp_body: Value = resp.json().await?;
        Ok(resp_body)
    }

    /// Check a `_bulk` response body for item-level errors and return an outer
    /// `Err` matching the append path's behaviour (#78/#32). `Ok(())` when the
    /// bulk request reports no errors.
    fn check_bulk_errors(resp_body: &Value) -> Result<(), FaucetError> {
        if resp_body
            .get("errors")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            let error_items = extract_bulk_error_messages(resp_body);
            if let Some(first) = error_items.first() {
                return Err(FaucetError::Sink(format!(
                    "Elasticsearch bulk request had {} errors: {first}",
                    error_items.len(),
                )));
            }
            return Err(FaucetError::Sink(
                "Elasticsearch bulk request reported errors:true but no per-item error \
                 could be extracted from the response — treating as a hard failure to \
                 avoid silently dropping records"
                    .into(),
            ));
        }
        Ok(())
    }

    /// Build the action-metadata map for a `_bulk` action line, seeded with the
    /// configured `_index` and an optional explicit `_id`.
    fn action_meta(&self, id: Option<String>) -> serde_json::Map<String, Value> {
        let mut action_meta = serde_json::Map::new();
        action_meta.insert("_index".to_string(), Value::String(self.write_index()));
        if let Some(id) = id {
            action_meta.insert("_id".to_string(), Value::String(id));
        }
        action_meta
    }

    /// Append an `{ "<action>": {...} }` line followed by an optional doc line
    /// to the NDJSON `body`.
    fn push_bulk_line(
        body: &mut String,
        action: &str,
        meta: serde_json::Map<String, Value>,
        doc: Option<&Value>,
    ) -> Result<(), FaucetError> {
        let action_line = serde_json::to_string(&serde_json::json!({ action: meta }))
            .map_err(|e| FaucetError::Sink(format!("failed to serialize bulk action: {e}")))?;
        body.push_str(&action_line);
        body.push('\n');
        if let Some(doc) = doc {
            let doc_line = serde_json::to_string(doc)
                .map_err(|e| FaucetError::Sink(format!("failed to serialize record: {e}")))?;
            body.push_str(&doc_line);
            body.push('\n');
        }
        Ok(())
    }

    /// Build the NDJSON bulk request body for a slice of records (append mode).
    ///
    /// Each record is preceded by an `{"index": {...}}` action line.
    /// If `id_field` is configured, the corresponding value from each record
    /// is used as the document `_id`.
    fn build_bulk_body(&self, records: &[Value]) -> Result<String, FaucetError> {
        let mut body = String::new();

        for record in records {
            let id = self.config.id_field.as_ref().and_then(|id_field| {
                record.get(id_field).map(|id_val| match id_val {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
            });
            let meta = self.action_meta(id);
            Self::push_bulk_line(&mut body, "index", meta, Some(record))?;
        }

        Ok(body)
    }

    /// Delete documents in `scope` whose key was not written by this run (#478).
    ///
    /// One `POST /<index>/_delete_by_query?refresh=true` with the body built by
    /// [`build_cleanup_query`].
    ///
    /// **Atomicity caveat — there is none, and it is visible.** Elasticsearch has
    /// no transactions: `_delete_by_query` takes a snapshot of the index, then
    /// deletes the matching documents in batches. So the scope passes through
    /// partially-cleaned states that concurrent searches can observe, and a
    /// mid-flight failure leaves some stale documents deleted and others not.
    /// Two things keep that safe rather than merely tolerable:
    ///
    /// 1. The query excludes every written `_id`, so **no partial outcome can
    ///    remove a document this run wrote** — only stale ones, in some order.
    /// 2. A partial outcome is never reported as success:
    ///    [`deleted_from_delete_by_query`] turns any failure / version conflict /
    ///    timeout into an error naming how many documents were removed. The next
    ///    run re-derives the same scope and finishes the job (the operation is
    ///    idempotent — re-deleting an already-deleted document is a no-op).
    ///
    /// A document that another writer changes mid-delete raises a version
    /// conflict and is left in place (`conflicts=abort`, the default) rather than
    /// being deleted on the strength of a stale snapshot.
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
            // no scope predicate the query would match the whole index — the
            // difference between a cleanup and a wipe.
            return Err(FaucetError::Sink(
                "cleanup: refusing an empty completeness claim — with no scope predicate the \
                 delete would match every document in the index"
                    .to_string(),
            ));
        }
        check_key_alignment(key, seen.keys())?;

        let ids = cleanup_doc_ids(seen.keys());
        check_cleanup_id_count(ids.len())?;
        let body = build_cleanup_query(scope, &ids);

        let auth = self.resolve_auth().await?;
        // `refresh=true` so the deletions are visible to searches as soon as the
        // call returns — a cleanup whose effect is invisible for the next second
        // reads as a no-op to anything checking the destination.
        let url = format!(
            "{}/{}/_delete_by_query?refresh=true",
            self.config.base_url, self.config.index
        );
        let req = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .body(serde_json::to_string(&body).map_err(|e| {
                FaucetError::Sink(format!("cleanup: failed to serialize delete query: {e}"))
            })?);
        let resp = Self::apply_auth_value(req, &auth).send().await?;

        // A missing index holds no stale documents. Detect the 404 before
        // `check_http_response`, which treats it as an error.
        if resp.status().as_u16() == 404 {
            tracing::debug!(
                index = %self.config.index,
                "Elasticsearch scoped cleanup: index does not exist, nothing to delete"
            );
            return Ok(0);
        }
        let resp = check_http_response(resp, DEFAULT_ERROR_BODY_MAX_LEN).await?;
        let resp_body: Value = resp.json().await?;
        let deleted = deleted_from_delete_by_query(&resp_body)?;

        tracing::info!(
            deleted,
            written_keys = ids.len(),
            index = %self.config.index,
            "Elasticsearch scoped cleanup complete"
        );
        Ok(deleted)
    }

    /// Build the NDJSON bulk body for an upsert/delete [`WritePlan`](faucet_core::WritePlan).
    ///
    /// Each `plan.upserts` row becomes an `{"index":{"_id":…}}` action (an
    /// idempotent overwrite) whose `_id` is derived from the row's `key`
    /// columns — this **overrides** any `id_field`. The row itself (already
    /// marker-stripped by the planner) is the doc line.
    ///
    /// Each `plan.deletes` key tuple becomes a `{"delete":{"_id":…}}` action
    /// with **no** doc line.
    fn build_plan_body(&self, plan: &faucet_core::WritePlan) -> Result<String, FaucetError> {
        let key = &self.config.write.key;
        let mut body = String::new();

        for row in &plan.upserts {
            let id = doc_id_from_row(row, key);
            let meta = self.action_meta(Some(id));
            Self::push_bulk_line(&mut body, "index", meta, Some(row))?;
        }
        for kt in &plan.deletes {
            // Composite ids are canonical-JSON encoded by the injective core
            // helper; the separator is retained for API stability and unused
            // for multi-column keys.
            let id = faucet_core::key_to_doc_id(kt, ":");
            let meta = self.action_meta(Some(id));
            Self::push_bulk_line(&mut body, "delete", meta, None)?;
        }

        Ok(body)
    }
}

/// Build a document `_id` from an upsert row's `key` columns, in `key` order.
///
/// Delegates to the injective [`faucet_core::key_to_doc_id`] so a single-column
/// key renders as its plain string / JSON form and a **composite** key renders
/// as a canonical JSON array of its values (never a separator-join, which is not
/// injective — `["a_","b"]` and `["a","_b"]` would both collapse to `"a__b"`,
/// silently overwriting two distinct rows). The separator argument is retained
/// only for API stability and is unused for composite keys.
///
/// Key columns are guaranteed present — [`faucet_core::plan_writes`] validated
/// them before the row reached `plan.upserts`. A missing column would only
/// occur on a planner contract violation, so it renders as `null` (via the core
/// helper) rather than panicking.
fn doc_id_from_row(row: &Value, key: &[String]) -> String {
    let kt = faucet_core::KeyTuple(
        key.iter()
            .map(|col| {
                let v = row.get(col).cloned().unwrap_or(Value::Null);
                (col.clone(), v)
            })
            .collect(),
    );
    faucet_core::key_to_doc_id(&kt, ":")
}

// ---------------------------------------------------------------------------
// Scoped cleanup (#478) — pure query construction + response parsing.
// ---------------------------------------------------------------------------

/// Ceiling on the number of written document ids one cleanup query may carry.
///
/// The written keys become an `ids` query, which Elasticsearch expands into a
/// terms lookup on `_id` and caps at `index.max_terms_count` (default 65 536).
/// The set cannot be split across several `_delete_by_query` calls to get under
/// the cap: each call would delete the ids the *other* calls excluded — i.e.
/// delete the documents this run wrote. So an oversized set is refused outright.
const MAX_CLEANUP_IDS: usize = 65_536;

/// Verify every accumulated key tuple is addressed by the same fields, in the
/// same order, as the sink's configured `key`.
///
/// The written-document `_id`s are derived from the tuple **in key order** (see
/// [`doc_id_from_row`]), and the cleanup deletes everything in the scope whose
/// `_id` is not in that list. A tuple in a different order would derive a
/// different `_id`, making a written document look unwritten — and delete it.
/// The pipeline builds the tuples from the sink's own `key`, so a mismatch is an
/// internal invariant violation; it is checked anyway because the cost is a few
/// string comparisons and the failure mode is data loss.
fn check_key_alignment(key: &[String], seen: &[faucet_core::KeyTuple]) -> Result<(), FaucetError> {
    for kt in seen {
        let aligned = kt.0.len() == key.len() && kt.0.iter().zip(key).all(|((c, _), k)| c == k);
        if !aligned {
            let got: Vec<&str> = kt.0.iter().map(|(c, _)| c.as_str()).collect();
            return Err(FaucetError::Sink(format!(
                "cleanup: a written-key tuple is keyed by {got:?} but the sink's key is {key:?} \
                 — refusing to delete, because a mismatched key derives a different document \
                 _id and would make written documents look unwritten"
            )));
        }
    }
    Ok(())
}

/// Document `_id`s for the keys this run wrote, using the **same** injective
/// derivation the upsert path uses ([`faucet_core::key_to_doc_id`]).
///
/// Sharing the derivation is what makes the cleanup correct: `write_batch`
/// indexes each upsert row under `key_to_doc_id(key)`, so "the ids this run
/// wrote" is exactly this list — including for composite keys, which render as
/// canonical JSON rather than a lossy separator join.
fn cleanup_doc_ids(seen: &[faucet_core::KeyTuple]) -> Vec<String> {
    seen.iter()
        .map(|kt| faucet_core::key_to_doc_id(kt, ":"))
        .collect()
}

/// Refuse a written-key set too large for one `_delete_by_query`, naming the
/// bound and the way out.
fn check_cleanup_id_count(ids: usize) -> Result<(), FaucetError> {
    if ids <= MAX_CLEANUP_IDS {
        return Ok(());
    }
    Err(FaucetError::Sink(format!(
        "cleanup: this run wrote {ids} documents in the claimed scope, over this sink's ceiling \
         of {MAX_CLEANUP_IDS} ids for one _delete_by_query (Elasticsearch caps a terms lookup at \
         `index.max_terms_count`, 65536 by default, and the id set cannot be split across \
         several queries without deleting documents this run wrote). Nothing was deleted — \
         narrow the completeness claim so fewer documents fall inside one scope."
    )))
}

/// Build the `_delete_by_query` body selecting documents in `scope` whose `_id`
/// is **not** among the ones this run wrote (#478).
///
/// Shape:
/// ```json
/// {"query": {"bool": {
///   "filter":   [{"term": {"contact_id": 7}}, …],
///   "must_not": [{"ids": {"values": ["1", "2"]}}]
/// }}}
/// ```
///
/// The scope predicates go in `filter` (not `must`) — they are exact equality
/// with no relevance contribution, so the filter context skips scoring and is
/// cacheable. Each is a `term` query, which matches the **indexed** value: a
/// scope field mapped as analyzed `text` will not match and the cleanup would
/// delete nothing; such a field needs a `keyword` mapping (or a `.keyword`
/// sub-field named in the claim).
///
/// The written set is excluded by `_id` rather than by `must_not` terms on the
/// key fields, because `_id` is exactly what the upsert path addresses: it needs
/// no mapping, works unchanged for composite keys, and cannot be defeated by an
/// analyzed key field.
///
/// An empty `seen` set omits the `must_not` clause entirely, leaving the scope
/// predicate alone so **every** document in the scope is deleted. That is not a
/// degenerate case but the motivating one: the source claimed the scope is
/// complete and reported no records in it.
fn build_cleanup_query(scope: &std::collections::BTreeMap<String, Value>, ids: &[String]) -> Value {
    let filter: Vec<Value> = scope
        .iter()
        .map(|(field, v)| {
            let mut term = serde_json::Map::with_capacity(1);
            term.insert(field.clone(), v.clone());
            serde_json::json!({ "term": Value::Object(term) })
        })
        .collect();

    let mut bool_query = serde_json::Map::with_capacity(2);
    bool_query.insert("filter".to_string(), Value::Array(filter));
    if !ids.is_empty() {
        bool_query.insert(
            "must_not".to_string(),
            serde_json::json!([{ "ids": { "values": ids } }]),
        );
    }

    serde_json::json!({ "query": { "bool": Value::Object(bool_query) } })
}

/// Read the deleted-document count out of a `_delete_by_query` response,
/// refusing to report success on a partial run.
///
/// `_delete_by_query` is a scan-and-delete, so it can stop part-way and still
/// answer `200 OK` with a body describing what it managed to do. Reporting the
/// `deleted` count alone would tell the caller "the scope is clean" when stale
/// documents remain, so any `failures`, version conflict, or timeout is surfaced
/// as an error that states how many documents *were* removed. The next run
/// re-derives the same scope and finishes the job.
fn deleted_from_delete_by_query(body: &Value) -> Result<u64, FaucetError> {
    let deleted = body.get("deleted").and_then(Value::as_u64).ok_or_else(|| {
        FaucetError::Sink(
            "cleanup: malformed _delete_by_query response — no numeric 'deleted' field".to_string(),
        )
    })?;

    if let Some(failures) = body.get("failures").and_then(Value::as_array)
        && !failures.is_empty()
    {
        return Err(FaucetError::Sink(format!(
            "cleanup: _delete_by_query reported {} failure(s) after deleting {deleted} \
             document(s) — the scope may still hold stale documents; first failure: {}",
            failures.len(),
            failures[0]
        )));
    }

    let conflicts = body
        .get("version_conflicts")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    if conflicts > 0 {
        return Err(FaucetError::Sink(format!(
            "cleanup: _delete_by_query hit {conflicts} version conflict(s) after deleting \
             {deleted} document(s) — those documents changed while the delete ran and were left \
             in place; the scope may still hold stale documents"
        )));
    }

    if body
        .get("timed_out")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Err(FaucetError::Sink(format!(
            "cleanup: _delete_by_query timed out after deleting {deleted} document(s) — the \
             scope may still hold stale documents"
        )));
    }

    Ok(deleted)
}

/// A [`faucet_core::WritePlan`] paired with, for each emitted bulk action (in
/// body order: all upserts then all deletes), the **original page indices**
/// that deduped into it. Used by `write_batch_partial` to attribute per-item
/// `_bulk` results back to original records for per-row DLQ routing (#F14).
struct PlanWithOrigins {
    plan: faucet_core::WritePlan,
    /// One entry per emitted bulk action, in the same order as
    /// `plan.upserts` followed by `plan.deletes`. Each entry is the list of
    /// original page indices that the (deduped) action represents.
    origins: Vec<Vec<usize>>,
    /// `(page_index, message)` for rows whose key could not be extracted —
    /// mirrors [`faucet_core::WritePlan::failed`].
    failed: Vec<(usize, String)>,
}

/// Replay the [`faucet_core::plan_writes`] partition (same key extraction, same
/// last-write-wins dedup, same upsert/delete routing) while additionally
/// recording the original page indices behind each emitted action.
///
/// This is intentionally a faithful re-derivation of the core planner so the
/// resulting `plan` is byte-for-byte what `plan_writes` would produce; the only
/// extra output is the origin-index mapping, which the core planner discards.
/// `WriteMode::Append` must never reach here (callers route append separately).
fn plan_origins(page: &[Value], spec: &faucet_core::WriteSpec) -> PlanWithOrigins {
    use faucet_core::{KeyTuple, WriteMode};

    // A planned action plus the original indices that fed into it.
    enum Slot {
        Upsert(Value, Vec<usize>),
        Delete(KeyTuple, Vec<usize>),
    }

    let key = &spec.key;
    let marker = spec.delete_marker.as_ref();
    let mut failed: Vec<(usize, String)> = Vec::new();
    let mut index: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    let mut order: Vec<Slot> = Vec::new();

    for (i, rec) in page.iter().enumerate() {
        // Extract key in `key` order; missing/null key → failed (matches core).
        let obj = match rec.as_object() {
            Some(o) => o,
            None => {
                failed.push((i, "record is not a JSON object".to_string()));
                continue;
            }
        };
        let mut kv: Vec<(String, Value)> = Vec::with_capacity(key.len());
        let mut key_err: Option<String> = None;
        for col in key {
            match obj.get(col) {
                None => {
                    key_err = Some(format!("missing key column '{col}'"));
                    break;
                }
                Some(Value::Null) => {
                    key_err = Some(format!("null value for key column '{col}'"));
                    break;
                }
                Some(v) => kv.push((col.clone(), v.clone())),
            }
        }
        if let Some(msg) = key_err {
            failed.push((i, msg));
            continue;
        }
        let key_tuple = KeyTuple(kv);

        // Stable canonical dedup string (matches core's `canonical`).
        let canon = {
            let arr: Vec<&Value> = key_tuple.0.iter().map(|(_, v)| v).collect();
            serde_json::to_string(&arr).expect("a Vec<&serde_json::Value> always serializes")
        };

        let is_delete = match spec.write_mode {
            WriteMode::Delete => true,
            WriteMode::Upsert => is_delete_marked(rec, marker),
            // Append, and any mode ES does not implement (e.g. Overwrite, which
            // it rejects at config-load): treated as a plain index. `WriteMode`
            // is `#[non_exhaustive]`, so the wildcard is required.
            _ => false,
        };

        let new_slot = if is_delete {
            Slot::Delete(key_tuple, vec![i])
        } else {
            Slot::Upsert(strip_marker(rec.clone(), marker), vec![i])
        };

        match index.get(&canon) {
            Some(&pos) => {
                // Last-write-wins: replace the action but ACCUMULATE origins so
                // a per-item failure for this key fails every input row for it.
                let origins = match &order[pos] {
                    Slot::Upsert(_, o) | Slot::Delete(_, o) => {
                        let mut o = o.clone();
                        o.push(i);
                        o
                    }
                };
                order[pos] = match new_slot {
                    Slot::Upsert(v, _) => Slot::Upsert(v, origins),
                    Slot::Delete(k, _) => Slot::Delete(k, origins),
                };
            }
            None => {
                index.insert(canon, order.len());
                order.push(new_slot);
            }
        }
    }

    // Split into the WritePlan + origins in body order: upserts first, then
    // deletes (exactly what `build_plan_body` emits).
    let mut plan = faucet_core::WritePlan {
        upserts: Vec::new(),
        deletes: Vec::new(),
        failed: failed.clone(),
    };
    let mut upsert_origins: Vec<Vec<usize>> = Vec::new();
    let mut delete_origins: Vec<Vec<usize>> = Vec::new();
    for slot in order {
        match slot {
            Slot::Upsert(v, o) => {
                plan.upserts.push(v);
                upsert_origins.push(o);
            }
            Slot::Delete(k, o) => {
                plan.deletes.push(k);
                delete_origins.push(o);
            }
        }
    }
    let mut origins = upsert_origins;
    origins.extend(delete_origins);

    PlanWithOrigins {
        plan,
        origins,
        failed,
    }
}

/// True when `rec`'s `marker.field` equals one of `marker.values`. Mirrors the
/// private `is_delete_marked` in `faucet_core::write_mode`.
fn is_delete_marked(rec: &Value, marker: Option<&faucet_core::DeleteMarker>) -> bool {
    let Some(dm) = marker else { return false };
    let Some(v) = rec.get(&dm.field) else {
        return false;
    };
    let Some(s) = v.as_str() else { return false };
    dm.values.iter().any(|m| m == s)
}

/// Remove `marker.field` from an upsert row. Mirrors the private `strip_marker`
/// in `faucet_core::write_mode`.
fn strip_marker(mut rec: Value, marker: Option<&faucet_core::DeleteMarker>) -> Value {
    if let (Some(dm), Value::Object(map)) = (marker, &mut rec) {
        map.remove(&dm.field);
    }
    rec
}

/// Map an Elasticsearch field-mapping `type` to a JSON-Schema base type name.
///
/// Numeric families collapse to `integer`/`number`, `boolean` is its own base,
/// `object`/`nested` are `object`, and everything else (`keyword`, `text`,
/// `date`, `ip`, …) is treated as `string`.
fn es_type_to_json(es_type: &str) -> &'static str {
    match es_type {
        "long" | "integer" | "short" | "byte" => "integer",
        "double" | "float" | "half_float" | "scaled_float" => "number",
        "boolean" => "boolean",
        "object" | "nested" => "object",
        _ => "string",
    }
}

/// Map a backend-neutral [`SqlBaseType`] to the Elasticsearch field-mapping
/// `type` used when adding a column via `PUT /<index>/_mapping`.
fn base_to_es(base: SqlBaseType) -> &'static str {
    match base {
        SqlBaseType::Integer => "long",
        SqlBaseType::Double => "double",
        SqlBaseType::Boolean => "boolean",
        SqlBaseType::Text => "keyword",
        SqlBaseType::Json => "object",
    }
}

/// Extract per-item error messages from a `_bulk` response body.
///
/// Each `items` entry is `{ "<action>": { ..., "error": {...} } }` where
/// `<action>` is `index` / `create` / `update` / `delete`. We read the error
/// from whichever action key is present, so all bulk operation types are
/// handled (not just `index`).
fn extract_bulk_error_messages(resp_body: &Value) -> Vec<String> {
    resp_body
        .get("items")
        .and_then(|v| v.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    item.as_object()
                        .and_then(|m| m.values().next())
                        .and_then(|action| action.get("error"))
                        .map(|e| e.to_string())
                })
                .collect()
        })
        .unwrap_or_default()
}

#[async_trait]
impl faucet_core::Sink for ElasticsearchSink {
    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(ElasticsearchSinkConfig))
            .expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        format!(
            "{}/{}",
            faucet_core::redact_uri_credentials(&self.config.base_url),
            self.config.index
        )
    }

    fn supports_cleanup(&self) -> bool {
        // Unconditional: Elasticsearch addresses documents by `_id`, which every
        // index has, so the cleanup needs nothing declared up front (there is no
        // column-mapping mode to exclude, as on the SQL sinks). The remaining
        // requirement — a non-empty `key`, so the written `_id`s can be derived
        // — is enforced by `WriteSpec::validate` at config-load time (cleanup
        // implies `write_mode: upsert`) and again in `cleanup_scope`.
        true
    }

    async fn cleanup_scope(
        &self,
        scope: &std::collections::BTreeMap<String, Value>,
        seen: &faucet_core::SeenKeys,
    ) -> Result<u64, FaucetError> {
        self.cleanup_scope_impl(scope, seen).await
    }

    /// Elasticsearch is schemaless and `_id`-addressable, so all three write
    /// modes are supported: upsert and delete derive the document `_id` from
    /// the configured `key`, and the `_bulk` `index` / `delete` actions are
    /// idempotent overwrites / removals by `_id`.
    fn supported_write_modes(&self) -> &'static [faucet_core::WriteMode] {
        &[
            faucet_core::WriteMode::Append,
            faucet_core::WriteMode::Upsert,
            faucet_core::WriteMode::Delete,
            faucet_core::WriteMode::Overwrite,
        ]
    }

    fn dedups_by_key(&self) -> bool {
        self.config.write.dedups_by_key()
    }

    fn is_overwrite(&self) -> bool {
        self.config.write.is_overwrite()
    }

    /// Prepare an alias-backed overwrite (#494). The configured `index` **must be
    /// an alias** (or not yet exist): a fresh physical index `<index>-faucet-ovw-…`
    /// is created (copying the current target's mappings when there is one), this
    /// run's writes are indexed into it, and `commit_overwrite` atomically moves
    /// the alias. Refusing a *concrete* index named `index` is what keeps the swap
    /// safe — there is no atomic replace of a concrete index in Elasticsearch.
    async fn begin_overwrite(&self) -> Result<(), FaucetError> {
        let auth = self.resolve_auth().await?;
        let alias = self.config.index.clone();

        // Discover the alias's current physical targets (if any) and reject a
        // concrete index of the same name.
        let previous = self.overwrite_alias_targets(&alias, &auth).await?;
        let mappings = match previous.first() {
            Some(idx) => self.overwrite_read_mappings(idx, &auth).await?,
            None => None,
        };

        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let staging = staging_index_name(&alias, nonce);
        self.overwrite_create_index(&staging, mappings, &auth)
            .await?;

        *self.overwrite.lock().expect("overwrite lock") =
            Some(OverwriteState { staging, previous });
        Ok(())
    }

    /// Atomically repoint the alias to the staging index and drop the old
    /// physical indices. The `POST /_aliases` action set is applied atomically by
    /// Elasticsearch, so a reader never sees the alias unbound or pointing at two
    /// generations at once.
    async fn commit_overwrite(&self) -> Result<(), FaucetError> {
        let state = self
            .overwrite
            .lock()
            .expect("overwrite lock")
            .clone()
            .ok_or_else(|| {
                FaucetError::Sink("commit_overwrite called without begin_overwrite".into())
            })?;
        let auth = self.resolve_auth().await?;
        let alias = self.config.index.clone();

        // Make the staged docs searchable before the swap.
        self.overwrite_refresh(&state.staging, &auth).await?;

        let body = build_alias_swap_actions(&alias, &state.staging, &state.previous);
        let url = format!("{}/_aliases", self.config.base_url);
        let req = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .body(serde_json::to_string(&body).map_err(|e| {
                FaucetError::Sink(format!("overwrite: serialize alias actions: {e}"))
            })?);
        let resp = Self::apply_auth_value(req, &auth).send().await?;
        check_http_response(resp, DEFAULT_ERROR_BODY_MAX_LEN).await?;

        // Best-effort drop of the now-detached old physical indices.
        for old in &state.previous {
            if let Err(e) = self.overwrite_delete_index(old, &auth).await {
                tracing::warn!(index = %old, error = %e, "overwrite: could not delete old index after swap");
            }
        }
        *self.overwrite.lock().expect("overwrite lock") = None;
        tracing::info!(alias = %alias, staging = %state.staging, "Elasticsearch overwrite committed (alias swapped)");
        Ok(())
    }

    /// Discard the staging index after a failed/cancelled overwrite — the alias
    /// and its current target are left untouched.
    async fn abort_overwrite(&self) -> Result<(), FaucetError> {
        let staging = self
            .overwrite
            .lock()
            .expect("overwrite lock")
            .as_ref()
            .map(|s| s.staging.clone());
        if let Some(staging) = staging {
            let auth = self.resolve_auth().await?;
            self.overwrite_delete_index(&staging, &auth).await?;
        }
        *self.overwrite.lock().expect("overwrite lock") = None;
        Ok(())
    }

    /// Elasticsearch can add new fields to an existing index in place via
    /// `PUT /<index>/_mapping`, so additive schema evolution is supported.
    /// (Changing an existing field's mapping type is *not* possible — see
    /// [`evolve_schema`](faucet_core::Sink::evolve_schema).)
    fn supports_schema_evolution(&self) -> bool {
        true
    }

    /// Read the index's live field mappings via `GET /<index>/_mapping`.
    ///
    /// Returns an `infer_schema`-shaped object schema with every field marked
    /// nullable (Elasticsearch has no NOT NULL concept). A `404` (index does not
    /// exist) yields `Ok(None)`; an index that exists with no explicit
    /// `properties` yields an empty `{"type":"object","properties":{}}`.
    async fn current_schema(&self) -> Result<Option<Value>, FaucetError> {
        let auth = self.resolve_auth().await?;
        let url = format!("{}/{}/_mapping", self.config.base_url, self.config.index);
        let req = Self::apply_auth_value(self.client.get(&url), &auth);
        let resp = req.send().await?;

        // A missing index is reported as drift-inert (Ok(None)), not an error.
        // Detect the 404 *before* check_http_response, which treats it as an error.
        if resp.status().as_u16() == 404 {
            return Ok(None);
        }
        let resp = check_http_response(resp, DEFAULT_ERROR_BODY_MAX_LEN).await?;
        let body: Value = resp.json().await?;

        // Shape: { "<index>": { "mappings": { "properties": { "<f>": {"type": …} } } } }.
        // The top-level key is the concrete index name (exactly one entry).
        let index_obj = body
            .get(&self.config.index)
            .or_else(|| body.as_object().and_then(|m| m.values().next()));
        let mappings = index_obj.and_then(|v| v.get("mappings"));
        let properties = mappings
            .and_then(|m| m.get("properties"))
            .and_then(|p| p.as_object());

        let mut out_props = serde_json::Map::new();
        if let Some(properties) = properties {
            for (field, def) in properties {
                let es_type = def.get("type").and_then(|t| t.as_str()).unwrap_or("object");
                let base = es_type_to_json(es_type);
                // ES has no NOT NULL → every field is nullable.
                out_props.insert(field.clone(), serde_json::json!({ "type": [base, "null"] }));
            }
        }

        Ok(Some(serde_json::json!({
            "type": "object",
            "properties": out_props,
        })))
    }

    /// Apply an additive schema evolution to the index via
    /// `PUT /<index>/_mapping`.
    ///
    /// Only [`additions`](faucet_core::SchemaEvolution::additions) are applied —
    /// Elasticsearch cannot change an existing field's mapping type or
    /// nullability in place, so
    /// [`widenings`](faucet_core::SchemaEvolution::widenings) and
    /// [`relax_nullability`](faucet_core::SchemaEvolution::relax_nullability) are
    /// no-ops (a one-shot `debug` log notes the limitation). A `PUT` is only
    /// issued when there is at least one addition.
    async fn evolve_schema(&self, evolution: &SchemaEvolution) -> Result<(), FaucetError> {
        if (!evolution.widenings.is_empty() || !evolution.relax_nullability.is_empty())
            && !self.evolve_noop_warned.swap(true, Ordering::Relaxed)
        {
            tracing::debug!(
                index = %self.config.index,
                "elasticsearch cannot change an existing field's mapping type / nullability; \
                 left as-is"
            );
        }

        if evolution.additions.is_empty() {
            return Ok(());
        }

        let mut properties = serde_json::Map::new();
        for change in &evolution.additions {
            let base = json_schema_base_type(&change.to).unwrap_or(SqlBaseType::Text);
            properties.insert(
                change.name.clone(),
                serde_json::json!({ "type": base_to_es(base) }),
            );
        }
        let body = serde_json::json!({ "properties": properties });

        let auth = self.resolve_auth().await?;
        let url = format!("{}/{}/_mapping", self.config.base_url, self.config.index);
        let req = self
            .client
            .put(&url)
            .header("Content-Type", "application/json")
            .body(serde_json::to_string(&body).map_err(|e| {
                FaucetError::Sink(format!("failed to serialize mapping update: {e}"))
            })?);
        let req = Self::apply_auth_value(req, &auth);
        let resp = req.send().await?;
        check_http_response(resp, DEFAULT_ERROR_BODY_MAX_LEN).await?;
        tracing::debug!(
            index = %self.config.index,
            added = evolution.additions.len(),
            "Elasticsearch mapping evolved (fields added)"
        );
        Ok(())
    }

    /// Non-mutating preflight probe.
    ///
    /// Runs `GET /_cluster/health` over the existing reqwest client (probe
    /// name `"health"`). When an index is configured, a second probe
    /// (`"schema"`) issues `HEAD /<index>`: a `404` is reported as a
    /// [`Skip`](faucet_core::check::ProbeStatus::Skip) ("index not found"),
    /// any other HTTP response is a pass, and a transport error is a failure.
    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};

        // Auth is shared by both probes; if it can't be resolved the whole
        // check fails on the `health` probe.
        let auth = match self.resolve_auth().await {
            Ok(a) => a,
            Err(e) => {
                return Ok(CheckReport::single(Probe::fail_hint(
                    "health",
                    std::time::Duration::ZERO,
                    e.to_string(),
                    "check the configured auth / that a shared auth provider is wired up",
                )));
            }
        };

        let mut probes = Vec::new();
        let health_hint =
            "check base_url / auth / that the Elasticsearch cluster is reachable and healthy";

        // ── Probe 1: GET /_cluster/health ───────────────────────────────────
        // The per-request `.timeout(ctx.timeout)` bounds the call; this crate
        // has no direct `tokio` dependency so we rely on reqwest's own timeout.
        let started = std::time::Instant::now();
        let health_url = format!("{}/_cluster/health", self.config.base_url);
        let req = Self::apply_auth_value(self.client.get(&health_url), &auth).timeout(ctx.timeout);
        let health_probe = match req.send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    Probe::pass("health", started.elapsed())
                } else {
                    Probe::fail_hint(
                        "health",
                        started.elapsed(),
                        format!("cluster health returned HTTP {}", resp.status().as_u16()),
                        health_hint,
                    )
                }
            }
            Err(e) if e.is_timeout() => {
                Probe::fail_hint("health", started.elapsed(), "timed out", health_hint)
            }
            Err(e) => Probe::fail_hint("health", started.elapsed(), e.to_string(), health_hint),
        };
        let health_failed = matches!(
            health_probe.status,
            faucet_core::check::ProbeStatus::Fail { .. }
        );
        probes.push(health_probe);

        // ── Probe 2 (optional): HEAD /<index> ───────────────────────────────
        // Only run when the cluster itself is reachable — a transport failure
        // on the index HEAD would just duplicate the health failure.
        if !health_failed && !self.config.index.is_empty() {
            let started = std::time::Instant::now();
            let index_hint = "check that the index exists / base_url is correct";
            let index_url = format!("{}/{}", self.config.base_url, self.config.index);
            let req =
                Self::apply_auth_value(self.client.head(&index_url), &auth).timeout(ctx.timeout);
            let schema_probe = match req.send().await {
                // 404 → index absent: report as Skip, not a failure (it may be
                // auto-created on first write).
                Ok(resp) if resp.status().as_u16() == 404 => {
                    Probe::skip("schema", format!("index '{}' not found", self.config.index))
                }
                // Any other HTTP response means the host answered — the index
                // exists (2xx) or the request was rejected for some non-404
                // reason; either way the endpoint is reachable.
                Ok(_) => Probe::pass("schema", started.elapsed()),
                Err(e) if e.is_timeout() => {
                    Probe::fail_hint("schema", started.elapsed(), "timed out", index_hint)
                }
                Err(e) => Probe::fail_hint("schema", started.elapsed(), e.to_string(), index_hint),
            };
            probes.push(schema_probe);
        }

        Ok(CheckReport { probes })
    }

    /// Write records to Elasticsearch using the `_bulk` API.
    ///
    /// When `config.batch_size > 0` and the input slice is larger than
    /// `batch_size`, the slice is split into chunks of `batch_size`
    /// documents and each chunk is sent as a separate `POST /_bulk` HTTP
    /// call. When `config.batch_size == 0`, the entire upstream
    /// [`StreamPage`](faucet_core::StreamPage) is sent in a single bulk
    /// request — useful when the source already sizes pages for
    /// Elasticsearch's `_bulk` sweet spot (5–15 MB NDJSON per call).
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        // Resolve auth once per write_batch call; reuse across chunks.
        let auth = self.resolve_auth().await?;

        // Upsert / delete routing: plan the page (dedup last-write-wins, strip
        // the delete marker) and emit `index` / `delete` bulk actions whose
        // `_id` derives from `key`. Append **and Overwrite** fall through to the
        // existing chunked `index` fast path below — an overwrite run indexes
        // into the staging physical index (via `action_meta` → `write_index`),
        // and `commit_overwrite` swaps the alias afterward.
        if !matches!(
            self.config.write.write_mode,
            faucet_core::WriteMode::Append | faucet_core::WriteMode::Overwrite
        ) {
            let plan = faucet_core::plan_writes(records, &self.config.write);
            if let Some((idx, msg)) = plan.failed.first() {
                return Err(FaucetError::Sink(format!(
                    "elasticsearch {}: row {idx}: {msg}",
                    self.config.write.write_mode.as_str()
                )));
            }
            let body = self.build_plan_body(&plan)?;
            let written = plan.upserts.len() + plan.deletes.len();
            if written == 0 {
                return Ok(0);
            }
            let resp_body = self.send_bulk_body(body, &auth).await?;
            Self::check_bulk_errors(&resp_body)?;
            tracing::debug!(
                upserts = plan.upserts.len(),
                deletes = plan.deletes.len(),
                "Elasticsearch upsert/delete bulk written"
            );
            return Ok(written);
        }

        let mut total_written = 0;

        let chunks: Vec<&[Value]> = if self.config.batch_size == 0 {
            // Sentinel: forward the entire upstream page as a single
            // `_bulk` POST. Caller is responsible for staying under
            // Elasticsearch's per-request limits.
            vec![records]
        } else {
            records.chunks(self.config.batch_size).collect()
        };

        // At-least-once + auto-generated IDs + multi-chunk page = duplicates on a
        // resumed run (an earlier chunk commits, a later one fails, the bookmark
        // doesn't advance, and the re-sent earlier chunk is re-indexed under new
        // IDs). Warn once so operators set `id_field` (idempotent overwrite) or a
        // DLQ (per-row outcomes, no whole-page re-send).
        if resume_dup_risk(chunks.len(), self.config.id_field.is_some())
            && !self.resume_dup_warned.swap(true, Ordering::Relaxed)
        {
            tracing::warn!(
                index = %self.config.index,
                chunks = chunks.len(),
                "Elasticsearch sink: a page split across multiple _bulk chunks with \
                 auto-generated document IDs (no id_field) can produce DUPLICATES on a \
                 resumed run. Set id_field for idempotent overwrites, or configure a DLQ.",
            );
        }

        for chunk in chunks {
            let resp_body = self.send_bulk_raw(chunk, &auth).await?;

            // Check for item-level errors in the bulk response. `errors: true`
            // with no extractable per-item error is treated as a hard failure
            // rather than silently dropping the chunk (#78/#32).
            Self::check_bulk_errors(&resp_body)?;

            total_written += chunk.len();
            tracing::debug!(records = chunk.len(), "Elasticsearch bulk batch written");
        }

        Ok(total_written)
    }

    /// Write records using the `_bulk` API, returning a per-row outcome.
    ///
    /// Unlike [`write_batch`](faucet_core::Sink::write_batch), this method never collapses
    /// item-level Elasticsearch errors into a single outer `Err`. Each
    /// document maps to exactly one [`faucet_core::RowOutcome`]:
    ///
    /// - `Ok(())` — the item was accepted (no `"error"` key in the response
    ///   action object).
    /// - `Err(FaucetError::Sink(_))` — Elasticsearch rejected the document
    ///   (the `"error"` object from the response is included in the message).
    ///
    /// HTTP-level failures (non-2xx status, network errors) still surface as
    /// an outer `Err`, because the entire chunk could not be sent.
    ///
    /// When the server returns fewer items than records sent (a malformed
    /// response), the missing tail positions are padded with
    /// `Err(FaucetError::Sink("… truncated …"))` so the caller always
    /// receives exactly `records.len()` outcomes.
    async fn write_batch_partial(
        &self,
        records: &[Value],
    ) -> Result<Vec<faucet_core::RowOutcome>, FaucetError> {
        if records.is_empty() {
            return Ok(Vec::new());
        }

        // Resolve auth once per write_batch_partial call; reuse across chunks.
        let auth = self.resolve_auth().await?;

        // Upsert / delete routing with PER-ROW fidelity (#F14).
        //
        // The whole point of overriding `write_batch_partial` is to give the
        // DLQ router per-row outcomes so `OnBatchError::DlqAll` only enqueues
        // rows that genuinely failed. The old code parsed the `_bulk` response
        // but then called `check_bulk_errors(..)?`, collapsing any per-item
        // error into an OUTER `Err` — under `dlq_all` that re-routed EVERY row
        // in the page (including the upsert/delete rows Elasticsearch had
        // already applied) to the DLQ while the bookmark still advanced →
        // silent downstream duplication. We now attribute each `_bulk` item
        // result back to its original page index/indices and return per-row
        // outcomes, never an outer `Err` for an item-level rejection.
        // Overwrite is insert-shaped into the staging index — falls through to
        // the append partial path below (all rows targeted at `write_index`).
        if !matches!(
            self.config.write.write_mode,
            faucet_core::WriteMode::Append | faucet_core::WriteMode::Overwrite
        ) {
            // `plan_origins` mirrors `plan_writes` (same key extraction, same
            // last-write-wins dedup) but additionally records, for each emitted
            // bulk action, the original page indices that fed into it. Because
            // the planner dedups by key, several input rows can map to one
            // action; if that action is rejected we mark all of those indices
            // `Err` (the final write for that key failed); if it succeeds they
            // are all `Ok`.
            let planned = plan_origins(records, &self.config.write);

            let mut outcomes: Vec<faucet_core::RowOutcome> =
                (0..records.len()).map(|_| Ok(())).collect();
            // Missing/null-key rows: marked `Err` at their original index.
            for (idx, msg) in &planned.failed {
                outcomes[*idx] = Err(FaucetError::Sink(format!(
                    "elasticsearch {}: row {idx}: {msg}",
                    self.config.write.write_mode.as_str()
                )));
            }

            if !planned.plan.upserts.is_empty() || !planned.plan.deletes.is_empty() {
                let body = self.build_plan_body(&planned.plan)?;
                // A transport/HTTP failure means the whole chunk could not be
                // sent — that genuinely aborts the batch (outer `Err`), matching
                // the append path. Item-level rejections are handled per-row
                // below.
                let resp_body = self.send_bulk_body(body, &auth).await?;

                // `items` are in request order: all `index` (upsert) actions
                // first, then all `delete` actions — exactly the order
                // `build_plan_body` emits and `planned.origins` records.
                let items = resp_body
                    .get("items")
                    .and_then(|v| v.as_array())
                    .map(|v| v.as_slice())
                    .unwrap_or(&[]);
                let action_count = planned.plan.upserts.len() + planned.plan.deletes.len();

                for (action_pos, origins) in planned.origins.iter().enumerate() {
                    // Read the per-item result. The `_bulk` item object is keyed
                    // by the action verb (`index` for upserts, `delete` for
                    // deletes); accept any of the verbs defensively.
                    let item_err = items.get(action_pos).and_then(|item| {
                        item.get("index")
                            .or_else(|| item.get("create"))
                            .or_else(|| item.get("delete"))
                            .or_else(|| item.get("update"))
                            .and_then(|a| a.get("error"))
                    });
                    let outcome: faucet_core::RowOutcome = if let Some(err) = item_err {
                        Err(FaucetError::Sink(format!(
                            "Elasticsearch item rejected: {err}"
                        )))
                    } else if action_pos >= items.len() {
                        // Server returned fewer items than actions sent — treat
                        // the missing tail as failed rather than silently
                        // dropping records.
                        Err(FaucetError::Sink(
                            "Elasticsearch bulk response truncated — item outcome missing".into(),
                        ))
                    } else {
                        Ok(())
                    };
                    // Propagate this action's result to every original index that
                    // deduped into it.
                    for &orig in origins {
                        // A genuine per-row failure overrides the default `Ok`;
                        // never overwrite an already-recorded missing-key `Err`.
                        if outcomes[orig].is_ok() {
                            outcomes[orig] = match &outcome {
                                Ok(()) => Ok(()),
                                Err(e) => Err(FaucetError::Sink(e.to_string())),
                            };
                        }
                    }
                }
                debug_assert_eq!(planned.origins.len(), action_count);
            }
            return Ok(outcomes);
        }

        let chunks: Vec<&[Value]> = if self.config.batch_size == 0 {
            vec![records]
        } else {
            records.chunks(self.config.batch_size).collect()
        };

        let mut outcomes: Vec<faucet_core::RowOutcome> = Vec::with_capacity(records.len());

        for chunk in chunks {
            let resp_body = self.send_bulk_raw(chunk, &auth).await?;

            let items = resp_body
                .get("items")
                .and_then(|v| v.as_array())
                .map(|v| v.as_slice())
                .unwrap_or(&[]);

            let mut chunk_outcomes: Vec<faucet_core::RowOutcome> = Vec::with_capacity(chunk.len());

            for item in items.iter().take(chunk.len()) {
                let action = item.get("index").or_else(|| item.get("create"));
                let error = action.and_then(|a| a.get("error"));
                if let Some(err) = error {
                    chunk_outcomes.push(Err(FaucetError::Sink(format!(
                        "Elasticsearch item rejected: {err}"
                    ))));
                } else {
                    chunk_outcomes.push(Ok(()));
                }
            }

            // Pad any missing tail positions defensively.
            while chunk_outcomes.len() < chunk.len() {
                chunk_outcomes.push(Err(FaucetError::Sink(
                    "Elasticsearch bulk response truncated — row outcome missing".into(),
                )));
            }

            outcomes.extend(chunk_outcomes);
        }

        Ok(outcomes)
    }

    fn connector_name(&self) -> &'static str {
        "elasticsearch"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::Sink as _;

    #[test]
    fn dataset_uri_combines_base_url_and_index() {
        let config = ElasticsearchSinkConfig::new("http://localhost:9200", "my-index");
        let sink = ElasticsearchSink::new(config).unwrap();
        assert_eq!(sink.dataset_uri(), "http://localhost:9200/my-index");
    }

    #[test]
    fn resume_dup_risk_only_when_multichunk_and_no_id_field() {
        // The duplication-on-resume risk exists only when a page splits into
        // >1 _bulk chunk AND documents get auto-generated IDs.
        assert!(
            resume_dup_risk(2, false),
            "multi-chunk + no id_field is risky"
        );
        assert!(
            !resume_dup_risk(1, false),
            "single chunk can't partially commit"
        );
        assert!(
            !resume_dup_risk(5, true),
            "id_field makes re-sends idempotent"
        );
        assert!(!resume_dup_risk(1, true));
    }
    use serde_json::json;

    #[test]
    fn extract_bulk_errors_reads_any_action_type() {
        // index + create actions, one with an error each.
        let body = json!({
            "errors": true,
            "items": [
                {"index": {"status": 201}},
                {"index": {"status": 400, "error": {"type": "mapper_parsing_exception"}}},
                {"create": {"status": 409, "error": {"type": "version_conflict"}}},
            ]
        });
        let errs = extract_bulk_error_messages(&body);
        assert_eq!(errs.len(), 2, "{errs:?}");
        assert!(errs.iter().any(|e| e.contains("mapper_parsing_exception")));
        assert!(errs.iter().any(|e| e.contains("version_conflict")));
    }

    #[test]
    fn extract_bulk_errors_empty_when_no_item_errors() {
        // errors:true but the items shape carries no extractable error — the
        // caller treats this empty result as a hard failure (#78/#32).
        let body = json!({"errors": true, "items": [{"weird": {"status": 500}}]});
        assert!(extract_bulk_error_messages(&body).is_empty());
    }

    #[test]
    fn new_rejects_oversized_batch_size() {
        // Regression for #78/#44.
        let config = ElasticsearchSinkConfig::new("http://localhost:9200", "idx")
            .with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(ElasticsearchSink::new(config).is_err());
    }

    #[test]
    fn bulk_body_without_id_field() {
        let config = ElasticsearchSinkConfig::new("http://localhost:9200", "test_idx");
        let sink = ElasticsearchSink::new(config).unwrap();

        let records = vec![
            json!({"name": "Alice", "age": 30}),
            json!({"name": "Bob", "age": 25}),
        ];

        let body = sink.build_bulk_body(&records).unwrap();
        let lines: Vec<&str> = body.trim().split('\n').collect();

        // 2 records = 4 lines (action + data for each).
        assert_eq!(lines.len(), 4);

        // Verify action lines contain the index.
        let action: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(action["index"]["_index"], "test_idx");
        assert!(action["index"].get("_id").is_none());

        // Verify data lines.
        let data: Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(data["name"], "Alice");
    }

    #[test]
    fn bulk_body_with_id_field() {
        let config =
            ElasticsearchSinkConfig::new("http://localhost:9200", "test_idx").id_field("doc_id");
        let sink = ElasticsearchSink::new(config).unwrap();

        let records = vec![
            json!({"doc_id": "abc-123", "name": "Alice"}),
            json!({"doc_id": 42, "name": "Bob"}),
            json!({"name": "Charlie"}), // missing id field
        ];

        let body = sink.build_bulk_body(&records).unwrap();
        let lines: Vec<&str> = body.trim().split('\n').collect();
        assert_eq!(lines.len(), 6);

        // First record: string id.
        let action0: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(action0["index"]["_id"], "abc-123");

        // Second record: numeric id serialized as string.
        let action1: Value = serde_json::from_str(lines[2]).unwrap();
        assert_eq!(action1["index"]["_id"], "42");

        // Third record: no id field, so no _id in action.
        let action2: Value = serde_json::from_str(lines[4]).unwrap();
        assert!(action2["index"].get("_id").is_none());
    }

    #[test]
    fn es_mapping_to_json_schema_types() {
        // Numeric families collapse to integer / number.
        assert_eq!(es_type_to_json("long"), "integer");
        assert_eq!(es_type_to_json("integer"), "integer");
        assert_eq!(es_type_to_json("short"), "integer");
        assert_eq!(es_type_to_json("byte"), "integer");
        assert_eq!(es_type_to_json("double"), "number");
        assert_eq!(es_type_to_json("float"), "number");
        assert_eq!(es_type_to_json("half_float"), "number");
        assert_eq!(es_type_to_json("scaled_float"), "number");
        // Boolean + object families.
        assert_eq!(es_type_to_json("boolean"), "boolean");
        assert_eq!(es_type_to_json("object"), "object");
        assert_eq!(es_type_to_json("nested"), "object");
        // Everything else → string.
        assert_eq!(es_type_to_json("keyword"), "string");
        assert_eq!(es_type_to_json("text"), "string");
        assert_eq!(es_type_to_json("date"), "string");
        assert_eq!(es_type_to_json("ip"), "string");
        assert_eq!(es_type_to_json("geo_point"), "string");
    }

    #[test]
    fn base_to_es_types() {
        assert_eq!(base_to_es(SqlBaseType::Integer), "long");
        assert_eq!(base_to_es(SqlBaseType::Double), "double");
        assert_eq!(base_to_es(SqlBaseType::Boolean), "boolean");
        assert_eq!(base_to_es(SqlBaseType::Text), "keyword");
        assert_eq!(base_to_es(SqlBaseType::Json), "object");
    }

    #[test]
    fn doc_id_composite_key_is_canonical_json_not_separator_join() {
        // F13: the core helper is now injective — a composite key is encoded as
        // a canonical JSON array, NOT a `:`-join, so the separator can't collide.
        let kt = faucet_core::KeyTuple(vec![
            ("tenant".to_string(), serde_json::json!("acme")),
            ("id".to_string(), serde_json::json!(7)),
        ]);
        assert_eq!(faucet_core::key_to_doc_id(&kt, ":"), "[\"acme\",7]");
    }

    #[test]
    fn doc_id_from_row_uses_injective_core_encoding() {
        // Composite key: now canonical-JSON encoded (F13) — NOT a separator
        // join — so it is injective. In `key` declaration order.
        let row = json!({"id": 7, "tenant": "acme", "v": "x"});
        let key = vec!["tenant".to_string(), "id".to_string()];
        // Matches faucet_core::key_to_doc_id for the same KeyTuple.
        let kt = faucet_core::KeyTuple(vec![
            ("tenant".to_string(), json!("acme")),
            ("id".to_string(), json!(7)),
        ]);
        assert_eq!(
            doc_id_from_row(&row, &key),
            faucet_core::key_to_doc_id(&kt, ":")
        );

        // Single string key column → rendered plain (no separator possible).
        let row = json!({"id": "abc-123"});
        assert_eq!(doc_id_from_row(&row, &["id".to_string()]), "abc-123");
    }

    #[test]
    fn doc_id_from_row_composite_is_injective_no_collision() {
        // F13 regression: two distinct composite keys that would collide under a
        // naive separator-join must now produce DISTINCT `_id`s.
        let key = vec!["a".to_string(), "b".to_string()];
        let id1 = doc_id_from_row(&json!({"a": "x_", "b": "y"}), &key);
        let id2 = doc_id_from_row(&json!({"a": "x", "b": "_y"}), &key);
        assert_ne!(
            id1, id2,
            "distinct composite keys must not collapse to the same _id"
        );
        // And both go through the injective core helper, not a `:`-join.
        assert!(!id1.contains("x_:y") && !id2.contains("x:_y"));
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
    fn cleanup_query_filters_scope_and_excludes_written_ids() {
        let ids = cleanup_doc_ids(&[kt(&[("id", json!(1))]), kt(&[("id", json!("a"))])]);
        let q = build_cleanup_query(&scope_of(&[("contact_id", json!(7))]), &ids);
        assert_eq!(
            q,
            json!({"query": {"bool": {
                "filter": [{"term": {"contact_id": 7}}],
                "must_not": [{"ids": {"values": ["1", "a"]}}],
            }}})
        );
    }

    #[test]
    fn cleanup_query_empty_seen_deletes_the_whole_scope() {
        // The motivating case: the source says "this scope is now empty", so the
        // query is the scope predicate alone — NOT a no-op, and no `must_not`.
        let q = build_cleanup_query(&scope_of(&[("contact_id", json!(7))]), &[]);
        assert_eq!(
            q,
            json!({"query": {"bool": {"filter": [{"term": {"contact_id": 7}}]}}})
        );
        assert!(
            q["query"]["bool"].get("must_not").is_none(),
            "an empty id list must omit must_not, not send an empty ids query"
        );
    }

    #[test]
    fn cleanup_query_one_term_per_scope_field() {
        let q = build_cleanup_query(
            &scope_of(&[("a", json!(1)), ("b", json!("x"))]),
            &["1".to_string()],
        );
        // BTreeMap ordering makes the scope clauses deterministic.
        assert_eq!(
            q["query"]["bool"]["filter"],
            json!([{"term": {"a": 1}}, {"term": {"b": "x"}}])
        );
    }

    #[test]
    fn cleanup_ids_match_the_upsert_id_derivation() {
        // The cleanup only deletes what the upsert path did NOT write, so its
        // ids must be byte-identical to the ones `build_plan_body` indexes under
        // — including for composite keys (canonical JSON, not a `:`-join).
        let row = json!({"tenant": "acme", "id": 7});
        let key = vec!["tenant".to_string(), "id".to_string()];
        let ids = cleanup_doc_ids(&[kt(&[("tenant", json!("acme")), ("id", json!(7))])]);
        assert_eq!(ids, vec![doc_id_from_row(&row, &key)]);
        assert_eq!(ids[0], "[\"acme\",7]");
    }

    #[test]
    fn key_alignment_accepts_matching_tuples() {
        let key = vec!["tenant".to_string(), "id".to_string()];
        let seen = vec![kt(&[("tenant", json!("acme")), ("id", json!(1))])];
        assert!(check_key_alignment(&key, &seen).is_ok());
    }

    #[test]
    fn key_alignment_rejects_a_reordered_or_renamed_tuple() {
        // Order matters: `key_to_doc_id` is order-sensitive, so a reordered
        // tuple derives a different `_id` and would delete a written document.
        let key = vec!["tenant".to_string(), "id".to_string()];
        let reordered = vec![kt(&[("id", json!(1)), ("tenant", json!("acme"))])];
        let err = check_key_alignment(&key, &reordered).expect_err("must refuse");
        assert!(err.to_string().contains("refusing to delete"), "{err}");

        let renamed = vec![kt(&[("tenant", json!("acme")), ("other", json!(1))])];
        assert!(check_key_alignment(&key, &renamed).is_err());
    }

    #[test]
    fn id_count_within_the_ceiling_is_accepted() {
        assert!(check_cleanup_id_count(MAX_CLEANUP_IDS).is_ok());
    }

    #[test]
    fn oversized_id_set_is_refused_with_the_bound_named() {
        // The id set cannot be split across queries, so an outsized one must be
        // refused outright rather than half-issued.
        let err = check_cleanup_id_count(MAX_CLEANUP_IDS + 1).expect_err("must refuse");
        let msg = err.to_string();
        assert!(msg.contains(&MAX_CLEANUP_IDS.to_string()), "{msg}");
        assert!(msg.contains("Nothing was deleted"), "{msg}");
    }

    #[test]
    fn delete_by_query_reports_deleted_count() {
        let body =
            json!({"deleted": 3, "version_conflicts": 0, "failures": [], "timed_out": false});
        assert_eq!(deleted_from_delete_by_query(&body).unwrap(), 3);
    }

    #[test]
    fn delete_by_query_failures_are_not_reported_as_success() {
        // A partial delete answers 200 OK; reporting its `deleted` count would
        // claim the scope is clean while stale documents remain.
        let body = json!({
            "deleted": 2,
            "failures": [{"index": "idx", "cause": {"type": "es_rejected_execution_exception"}}],
        });
        let err = deleted_from_delete_by_query(&body).expect_err("must surface the failure");
        let msg = err.to_string();
        assert!(msg.contains("deleting 2"), "{msg}");
        assert!(msg.contains("es_rejected_execution_exception"), "{msg}");
    }

    #[test]
    fn delete_by_query_version_conflicts_are_surfaced() {
        let body = json!({"deleted": 5, "version_conflicts": 2, "failures": []});
        let err = deleted_from_delete_by_query(&body).expect_err("must surface the conflict");
        assert!(err.to_string().contains("version conflict"), "{err}");
    }

    #[test]
    fn delete_by_query_timeout_is_surfaced() {
        let body = json!({"deleted": 1, "version_conflicts": 0, "failures": [], "timed_out": true});
        let err = deleted_from_delete_by_query(&body).expect_err("must surface the timeout");
        assert!(err.to_string().contains("timed out"), "{err}");
    }

    #[test]
    fn delete_by_query_malformed_response_is_typed_error() {
        let err = deleted_from_delete_by_query(&json!({"acknowledged": true}))
            .expect_err("must refuse a body with no deleted count");
        assert!(err.to_string().contains("malformed"), "{err}");
    }

    #[test]
    fn plan_body_upsert_uses_key_id_and_strips_marker() {
        use faucet_core::{DeleteMarker, WriteMode, WriteSpec};

        let config = ElasticsearchSinkConfig {
            id_field: Some("ignored".to_string()),
            write: WriteSpec {
                write_mode: WriteMode::Upsert,
                key: vec!["id".to_string()],
                delete_marker: Some(DeleteMarker {
                    field: "__op".to_string(),
                    values: vec!["d".to_string()],
                }),
                rollback: None,
            },
            ..ElasticsearchSinkConfig::new("http://localhost:9200", "idx")
        };
        let sink = ElasticsearchSink::new(config).unwrap();

        let records = vec![
            json!({"id": 1, "v": "a"}),
            json!({"id": 2, "v": "x", "__op": "d"}),
        ];
        let plan = faucet_core::plan_writes(&records, &sink.config.write);
        let body = sink.build_plan_body(&plan).unwrap();
        let lines: Vec<&str> = body.trim().split('\n').collect();

        // 1 upsert (action + doc) + 1 delete (action only) = 3 lines.
        assert_eq!(lines.len(), 3, "{lines:?}");

        // Upsert action: `_id` derived from the key (overrides id_field).
        let action0: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(action0["index"]["_id"], "1");
        assert_eq!(action0["index"]["_index"], "idx");
        // Doc line: the marker field is stripped.
        let doc0: Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(doc0["v"], "a");
        assert!(doc0.get("__op").is_none());

        // Delete action: key-derived `_id`, NO doc line follows.
        let action1: Value = serde_json::from_str(lines[2]).unwrap();
        assert_eq!(action1["delete"]["_id"], "2");
        assert_eq!(action1["delete"]["_index"], "idx");
    }

    #[test]
    fn staging_index_name_is_prefixed_and_unique() {
        let a = staging_index_name("orders", 0x1a2b);
        assert!(a.starts_with("orders-faucet-ovw-"), "{a}");
        assert_ne!(a, staging_index_name("orders", 0x1a2c));
    }

    #[test]
    fn alias_swap_actions_remove_all_previous_then_add_staging() {
        let body = build_alias_swap_actions(
            "orders",
            "orders-faucet-ovw-1",
            &["orders-old-a".to_string(), "orders-old-b".to_string()],
        );
        let actions = body["actions"].as_array().unwrap();
        assert_eq!(actions.len(), 3, "two removes + one add");
        assert_eq!(actions[0]["remove"]["index"], "orders-old-a");
        assert_eq!(actions[0]["remove"]["alias"], "orders");
        assert_eq!(actions[1]["remove"]["index"], "orders-old-b");
        assert_eq!(actions[2]["add"]["index"], "orders-faucet-ovw-1");
        assert_eq!(actions[2]["add"]["alias"], "orders");
    }

    #[test]
    fn alias_swap_first_run_only_adds() {
        let body = build_alias_swap_actions("orders", "orders-faucet-ovw-1", &[]);
        let actions = body["actions"].as_array().unwrap();
        assert_eq!(actions.len(), 1);
        assert!(actions[0].get("add").is_some());
    }
}
