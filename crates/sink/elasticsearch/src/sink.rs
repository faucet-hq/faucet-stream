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
}

/// The alias that marks an overwrite run's staging index. `begin_overwrite`
/// attaches it to the fresh staging index, every write targets it, and
/// `commit_overwrite` / `abort_overwrite` resolve the staging index through it.
/// The cluster holds the state because the CLI runs begin, the writes and the
/// commit on different sink instances. Pure.
fn staging_alias(alias: &str) -> String {
    format!("{alias}-faucet-ovw-staging")
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
    actions
        .push(serde_json::json!({ "remove": { "index": staging, "alias": staging_alias(alias) } }));
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
        config.validate()?;
        // Bounded connect and request timeouts: a half-open connection or a
        // wedged node must fail the request, not hang the run (#789 MSG-58).
        let client = Client::builder()
            .connect_timeout(std::time::Duration::from_secs(config.connect_timeout_secs))
            .timeout(std::time::Duration::from_secs(config.request_timeout_secs))
            .build()
            .map_err(|e| FaucetError::Config(format!("elasticsearch HTTP client: {e}")))?;
        Ok(Self {
            config,
            client,
            auth_provider: None,
            resume_dup_warned: AtomicBool::new(false),
            evolve_noop_warned: AtomicBool::new(false),
        })
    }

    /// The index a write targets: the staging alias under `write_mode:
    /// overwrite`, otherwise the configured `index` (which may be an alias).
    fn write_index(&self) -> String {
        if self.config.write.is_overwrite() {
            staging_alias(&self.config.index)
        } else {
            self.config.index.clone()
        }
    }

    /// Physical indices `alias` points at; empty when no such alias exists.
    async fn alias_targets(
        &self,
        alias: &str,
        auth: &ElasticsearchAuth,
    ) -> Result<Option<Vec<String>>, FaucetError> {
        let url = format!("{}/_alias/{}", self.config.base_url, alias);
        let resp = Self::apply_auth_value(self.client.get(&url), auth)
            .send()
            .await?;
        if resp.status().as_u16() == 404 {
            return Ok(None);
        }
        let resp = check_http_response(resp, DEFAULT_ERROR_BODY_MAX_LEN).await?;
        let body: Value = resp.json().await?;
        Ok(Some(
            body.as_object()
                .map(|m| m.keys().cloned().collect())
                .unwrap_or_default(),
        ))
    }

    /// The overwrite run's staging index, resolved through [`staging_alias`].
    async fn overwrite_staging(&self, auth: &ElasticsearchAuth) -> Result<String, FaucetError> {
        let marker = staging_alias(&self.config.index);
        match self.alias_targets(&marker, auth).await?.as_deref() {
            Some([staging]) => Ok(staging.clone()),
            Some(many) if !many.is_empty() => Err(FaucetError::Sink(format!(
                "elasticsearch overwrite: `{marker}` points at {} indices ({}); expected the \
                 one staging index begin_overwrite created",
                many.len(),
                many.join(", ")
            ))),
            _ => Err(FaucetError::Sink(format!(
                "elasticsearch overwrite: no staging index behind `{marker}` — \
                 begin_overwrite did not run, or another run committed or aborted it"
            ))),
        }
    }

    /// Physical indices the read alias `alias` currently points at. Empty when
    /// the alias does not exist yet (first overwrite run). Errors when `alias`
    /// names a **concrete index** — overwrite requires an alias (#494).
    async fn overwrite_alias_targets(
        &self,
        alias: &str,
        auth: &ElasticsearchAuth,
    ) -> Result<Vec<String>, FaucetError> {
        if let Some(targets) = self.alias_targets(alias, auth).await? {
            return Ok(targets);
        }
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
        Ok(Vec::new())
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

    /// Read `index`'s settings, reduced to the ones a replacement index must
    /// keep ([`copyable_settings`]); `None` when they can't be read.
    async fn overwrite_read_settings(
        &self,
        index: &str,
        auth: &ElasticsearchAuth,
    ) -> Result<Option<Value>, FaucetError> {
        let url = format!("{}/{}/_settings", self.config.base_url, index);
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
            .and_then(|v| v.get("settings"))
            .and_then(|s| s.get("index"))
            .map(copyable_settings))
    }

    /// Create the staging physical index behind `marker`, seeding its mappings
    /// and settings when known.
    async fn overwrite_create_index(
        &self,
        index: &str,
        marker: &str,
        mappings: Option<Value>,
        settings: Option<Value>,
        auth: &ElasticsearchAuth,
    ) -> Result<(), FaucetError> {
        let mut body = serde_json::Map::new();
        if let Some(m) = mappings {
            body.insert("mappings".to_string(), m);
        }
        if let Some(st) = settings.filter(|v| v.as_object().is_some_and(|o| !o.is_empty())) {
            body.insert("settings".to_string(), serde_json::json!({ "index": st }));
        }
        body.insert("aliases".to_string(), serde_json::json!({ marker: {} }));
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

    /// Send a pre-built NDJSON `_bulk` body and return the parsed response.
    ///
    /// The transport under [`bulk_items`](Self::bulk_items).
    async fn send_bulk_body(
        &self,
        body: String,
        auth: &ElasticsearchAuth,
    ) -> Result<Value, FaucetError> {
        let url = format!("{}/_bulk", self.config.base_url);
        let mut req = self.client.post(&url);
        if self.config.write.is_overwrite() {
            // Never auto-create a concrete index named after the staging alias.
            req = req.query(&[("require_alias", "true")]);
        }
        let req = req
            .header("Content-Type", "application/x-ndjson")
            .body(body);
        let req = Self::apply_auth_value(req, auth);
        let resp = req.send().await?;
        let resp = check_http_response(resp, DEFAULT_ERROR_BODY_MAX_LEN).await?;
        let resp_body: Value = resp.json().await?;
        Ok(resp_body)
    }

    /// Send `entries` (one `_bulk` action each) and return one result object
    /// per entry, in order (`Value::Null` when the response omitted it).
    ///
    /// Items rejected with `429` / `503` (`es_rejected_execution_exception`, a
    /// saturated write thread pool) are transient: they are re-sent alone,
    /// with exponential backoff, up to [`ITEM_RETRY_LIMIT`] times, so a briefly
    /// overloaded cluster neither DLQs valid rows nor makes the outer retry
    /// re-index the items that already succeeded (#789 MSG-43).
    async fn bulk_items(
        &self,
        entries: &[String],
        auth: &ElasticsearchAuth,
    ) -> Result<Vec<Value>, FaucetError> {
        let mut results = vec![Value::Null; entries.len()];
        let mut pending: Vec<usize> = (0..entries.len()).collect();
        let mut attempt: u32 = 0;
        while !pending.is_empty() {
            let body: String = pending.iter().map(|&i| entries[i].as_str()).collect();
            let resp = self.send_bulk_body(body, auth).await?;
            let items = resp
                .get("items")
                .and_then(Value::as_array)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            // `errors: false` with no item list vouches for every item; a list
            // shorter than the request is truncated, and a missing item fails.
            let all_ok =
                items.is_empty() && resp.get("errors").and_then(Value::as_bool) == Some(false);
            let mut retry = Vec::new();
            for (pos, &i) in pending.iter().enumerate() {
                match items.get(pos) {
                    Some(item) => {
                        if attempt < ITEM_RETRY_LIMIT && retriable_item(item) {
                            retry.push(i);
                        }
                        results[i] = item.clone();
                    }
                    None if all_ok => results[i] = serde_json::json!({}),
                    None => results[i] = Value::Null,
                }
            }
            if retry.is_empty() {
                break;
            }
            attempt += 1;
            tracing::debug!(
                items = retry.len(),
                attempt,
                "Elasticsearch bulk items rejected as overloaded; retrying them"
            );
            tokio::time::sleep(item_retry_delay(attempt)).await;
            pending = retry;
        }
        Ok(results)
    }

    /// The `_id` an append writes under: `id_field`'s value when it is a
    /// string, number or boolean; `None` (Elasticsearch generates one) when
    /// `id_field` is unset or the record lacks it. A `null`, object or array
    /// is an error — rendering it would put every such record on one `_id`
    /// such as `"null"` and silently keep only the last (#789 MSG-20).
    fn append_id(&self, record: &Value) -> Result<Option<String>, FaucetError> {
        let Some(field) = &self.config.id_field else {
            return Ok(None);
        };
        match record.get(field) {
            None => Ok(None),
            Some(Value::String(s)) => Ok(Some(s.clone())),
            Some(v @ (Value::Number(_) | Value::Bool(_))) => Ok(Some(v.to_string())),
            Some(other) => Err(FaucetError::Sink(format!(
                "id_field '{field}' is {}, not a string, number or boolean",
                match other {
                    Value::Null => "null",
                    Value::Array(_) => "an array",
                    _ => "an object",
                }
            ))),
        }
    }

    /// The `_bulk` entry appending `record` (`op_type` action + document).
    fn append_entry(&self, record: &Value) -> Result<String, FaucetError> {
        let meta = self.action_meta(self.append_id(record)?);
        let mut entry = String::new();
        Self::push_bulk_line(&mut entry, self.config.op_type.as_str(), meta, Some(record))?;
        Ok(entry)
    }

    /// The `_bulk` entry for one planned upsert/delete action.
    fn plan_entry(&self, action: &PlannedAction) -> Result<String, FaucetError> {
        let mut entry = String::new();
        let meta = self.action_meta(Some(action.id.clone()));
        match &action.doc {
            Some(doc) => Self::push_bulk_line(&mut entry, "index", meta, Some(doc))?,
            None => Self::push_bulk_line(&mut entry, "delete", meta, None)?,
        }
        Ok(entry)
    }

    /// `entries` in `batch_size` chunks (`0` = one chunk).
    fn chunked<'e, T>(&self, entries: &'e [T]) -> Vec<&'e [T]> {
        if self.config.batch_size == 0 || entries.is_empty() {
            vec![entries]
        } else {
            entries.chunks(self.config.batch_size).collect()
        }
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

    /// The index's mapped `properties`, or `None` when the index is missing.
    async fn index_properties(
        &self,
        auth: &ElasticsearchAuth,
    ) -> Result<Option<Value>, FaucetError> {
        let url = format!("{}/{}/_mapping", self.config.base_url, self.config.index);
        let resp = Self::apply_auth_value(self.client.get(&url), auth)
            .send()
            .await?;
        if resp.status().as_u16() == 404 {
            return Ok(None);
        }
        let resp = check_http_response(resp, DEFAULT_ERROR_BODY_MAX_LEN).await?;
        let body: Value = resp.json().await?;
        Ok(Some(
            body.get(&self.config.index)
                .or_else(|| body.as_object().and_then(|m| m.values().next()))
                .and_then(|v| v.get("mappings"))
                .and_then(|m| m.get("properties"))
                .cloned()
                .unwrap_or_else(|| serde_json::json!({})),
        ))
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

        let auth = self.resolve_auth().await?;
        // A `term` query matches the indexed value, so a scope field mapped as
        // analyzed `text` (the dynamic-mapping default for strings) matches
        // nothing and the cleanup silently deletes nothing (#789 MSG-29).
        // Query its `keyword` sub-field instead, or refuse.
        let properties = match self.index_properties(&auth).await? {
            Some(p) => p,
            None => {
                tracing::debug!(
                    index = %self.config.index,
                    "Elasticsearch scoped cleanup: index does not exist, nothing to delete"
                );
                return Ok(0);
            }
        };
        let mut term_scope = std::collections::BTreeMap::new();
        for (field, value) in scope {
            let target = term_field(&properties, field).map_err(FaucetError::Sink)?;
            term_scope.insert(target, value.clone());
        }
        let body = build_cleanup_query(&term_scope, &ids);

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
#[cfg(test)]
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

/// One planned upsert/delete `_bulk` action and the original page indices that
/// deduped into it (used to attribute per-item `_bulk` results back to records
/// for per-row DLQ routing, #F14).
struct PlannedAction {
    /// The document `_id` ([`faucet_core::key_to_doc_id`]).
    id: String,
    /// `Some(doc)` for an upsert (`index` action), `None` for a delete.
    doc: Option<Value>,
    /// Original page indices behind this action.
    origins: Vec<usize>,
}

/// An upsert/delete page planned into `_bulk` actions.
struct PlanWithOrigins {
    /// Actions in the order their `_id` first appears in the page.
    actions: Vec<PlannedAction>,
    /// `(page_index, message)` for rows whose key could not be extracted.
    failed: Vec<(usize, String)>,
    upserts: usize,
    deletes: usize,
}

/// Partition an upsert/delete page into `_bulk` actions: the same key
/// extraction and last-write-wins dedup as [`faucet_core::plan_writes`], but
/// deduplicated by the **document `_id`** each row addresses. Keys `7` and
/// `"7"` both write `_id "7"`, so they are one document; deduplicating by the
/// JSON value kept both and emitted every upsert before every delete, so a
/// page ending in a delete of `"7"` could be followed by an upsert of `7`
/// (#789 MSG-94). `WriteMode::Append` must never reach here.
fn plan_origins(page: &[Value], spec: &faucet_core::WriteSpec) -> PlanWithOrigins {
    use faucet_core::{KeyTuple, WriteMode};

    let key = &spec.key;
    let marker = spec.delete_marker.as_ref();
    let mut failed: Vec<(usize, String)> = Vec::new();
    let mut index: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    let mut actions: Vec<PlannedAction> = Vec::new();

    for (i, rec) in page.iter().enumerate() {
        let Some(obj) = rec.as_object() else {
            failed.push((i, "record is not a JSON object".to_string()));
            continue;
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
        let id = faucet_core::key_to_doc_id(&KeyTuple(kv), ":");

        let is_delete = match spec.write_mode {
            WriteMode::Delete => true,
            WriteMode::Upsert => is_delete_marked(rec, marker),
            _ => false,
        };
        let doc = (!is_delete).then(|| strip_marker(rec.clone(), marker));

        match index.get(&id) {
            Some(&pos) => {
                // Last-write-wins, accumulating origins so a per-item failure
                // for this `_id` fails every input row behind it.
                actions[pos].doc = doc;
                actions[pos].origins.push(i);
            }
            None => {
                index.insert(id.clone(), actions.len());
                actions.push(PlannedAction {
                    id,
                    doc,
                    origins: vec![i],
                });
            }
        }
    }

    let upserts = actions.iter().filter(|a| a.doc.is_some()).count();
    let deletes = actions.len() - upserts;
    PlanWithOrigins {
        actions,
        failed,
        upserts,
        deletes,
    }
}

/// How many times a `429` / `503` bulk item is re-sent on its own.
const ITEM_RETRY_LIMIT: u32 = 5;

/// Backoff before re-send `attempt` (1-based): 200 ms doubling, capped at 5 s.
fn item_retry_delay(attempt: u32) -> std::time::Duration {
    let ms = 200u64.saturating_mul(1u64 << attempt.saturating_sub(1).min(5));
    std::time::Duration::from_millis(ms.min(5_000))
}

/// The action object of one `_bulk` response item (`index`/`create`/…).
fn item_action(item: &Value) -> Option<&Value> {
    item.as_object().and_then(|m| m.values().next())
}

/// Whether a `_bulk` item failed transiently: `429` / `503`, or the
/// thread-pool rejection Elasticsearch reports for an overloaded node.
fn retriable_item(item: &Value) -> bool {
    let Some(action) = item_action(item) else {
        return false;
    };
    if action.get("error").is_none() {
        return false;
    }
    matches!(
        action.get("status").and_then(Value::as_u64),
        Some(429 | 503)
    ) || action
        .get("error")
        .and_then(|e| e.get("type"))
        .and_then(Value::as_str)
        == Some("es_rejected_execution_exception")
}

/// `Some(message)` when a `_bulk` item failed (or is missing — `Value::Null`).
fn item_failure(item: &Value) -> Option<String> {
    if item.is_null() {
        return Some("Elasticsearch bulk response truncated — item outcome missing".into());
    }
    item_action(item)
        .and_then(|a| a.get("error"))
        .map(|e| format!("Elasticsearch item rejected: {e}"))
}

/// The first failure among `items`, as the batch error.
fn first_failure(items: &[Value]) -> Result<(), FaucetError> {
    let failures: Vec<String> = items.iter().filter_map(item_failure).collect();
    match failures.first() {
        None => Ok(()),
        Some(first) => Err(FaucetError::Sink(format!(
            "Elasticsearch bulk request had {} errors: {first}",
            failures.len()
        ))),
    }
}

/// The settings a replacement index must carry over from the one it replaces
/// (`GET /<idx>/_settings` → `settings.index`), without the read-only and
/// per-index identity keys Elasticsearch refuses on create. Copying only the
/// mappings lost analyzers (so mappings referencing them failed), shard and
/// replica counts, `refresh_interval`, index sorting and lifecycle policy
/// (#789 MSG-51).
fn copyable_settings(index_settings: &Value) -> Value {
    const KEEP: &[&str] = &[
        "analysis",
        "number_of_shards",
        "number_of_replicas",
        "refresh_interval",
        "sort",
        "mapping",
        "max_result_window",
        "similarity",
        "codec",
        "default_pipeline",
        "final_pipeline",
        "lifecycle",
        "max_ngram_diff",
        "max_shingle_diff",
    ];
    let mut out = serde_json::Map::new();
    if let Some(obj) = index_settings.as_object() {
        for (k, v) in obj {
            if KEEP.contains(&k.as_str()) {
                out.insert(k.clone(), v.clone());
            }
        }
    }
    // A rollover alias belongs to the index being replaced, not the stand-in.
    if let Some(Value::Object(lc)) = out.get_mut("lifecycle") {
        lc.remove("rollover_alias");
        lc.remove("indexing_complete");
    }
    Value::Object(out)
}

/// The field a cleanup `term` query must target for scope `field`: the field
/// itself, or its `keyword` sub-field when it is mapped as analyzed `text`.
/// A `text` field without a keyword sub-field cannot be matched exactly, so
/// the cleanup is refused rather than deleting nothing. Dotted names walk
/// object properties; an unmapped field is returned unchanged.
fn term_field(properties: &Value, field: &str) -> Result<String, String> {
    let mut props = properties;
    let mut def: Option<&Value> = None;
    for part in field.split('.') {
        match props.get(part) {
            Some(d) => {
                def = Some(d);
                props = d.get("properties").unwrap_or(&Value::Null);
            }
            None => return Ok(field.to_string()),
        }
    }
    let Some(def) = def else {
        return Ok(field.to_string());
    };
    if def.get("type").and_then(Value::as_str) != Some("text") {
        return Ok(field.to_string());
    }
    let keyword = def.get("fields").and_then(Value::as_object).and_then(|f| {
        f.iter()
            .find(|(_, d)| d.get("type").and_then(Value::as_str) == Some("keyword"))
            .map(|(name, _)| name.clone())
    });
    match keyword {
        Some(sub) => Ok(format!("{field}.{sub}")),
        None => Err(format!(
            "cleanup: scope field '{field}' is mapped as analyzed `text` with no `keyword` \
             sub-field, so an exact match is impossible and the cleanup would delete nothing — \
             map it as `keyword` (or add a `keyword` sub-field)"
        )),
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

/// The mapping for a column added by `evolve`. An array maps to its item type
/// (Elasticsearch fields are multi-valued), not `object`, which rejects every
/// later document carrying scalars (#789 MSG-50). Strings map to `keyword`
/// with `ignore_above`, so an over-long value is stored but not indexed rather
/// than rejecting its document (#789 MSG-90).
fn field_mapping(fragment: &Value) -> Value {
    let is_array = match fragment.get("type") {
        Some(Value::String(t)) => t == "array",
        Some(Value::Array(ts)) => ts.iter().any(|t| t == "array"),
        _ => false,
    };
    let base = if is_array {
        fragment
            .get("items")
            .and_then(json_schema_base_type)
            .unwrap_or(SqlBaseType::Text)
    } else {
        json_schema_base_type(fragment).unwrap_or(SqlBaseType::Text)
    };
    match base_to_es(base) {
        "keyword" => serde_json::json!({ "type": "keyword", "ignore_above": KEYWORD_IGNORE_ABOVE }),
        t => serde_json::json!({ "type": t }),
    }
}

/// `ignore_above` for evolved `keyword` fields: Lucene's 32 766-byte term
/// limit divided by the worst-case 4 bytes per UTF-8 character.
const KEYWORD_IGNORE_ABOVE: u64 = 8191;

#[async_trait]
impl faucet_core::Sink for ElasticsearchSink {
    fn batch_atomicity(&self) -> faucet_core::BatchAtomicity {
        self.config.batch_atomicity()
    }

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
        let marker = staging_alias(&alias);

        // Discover the alias's current physical targets (if any) and reject a
        // concrete index of the same name.
        let previous = self.overwrite_alias_targets(&alias, &auth).await?;
        let (mappings, settings) = match previous.first() {
            Some(idx) => (
                self.overwrite_read_mappings(idx, &auth).await?,
                self.overwrite_read_settings(idx, &auth).await?,
            ),
            None => (None, None),
        };

        // A run that crashed before commit or abort left its staging index
        // behind the marker; it was never live, so drop it.
        for stale in self
            .alias_targets(&marker, &auth)
            .await?
            .unwrap_or_default()
        {
            if !previous.contains(&stale) {
                tracing::warn!(index = %stale, "overwrite: dropping a staging index left by an earlier run");
                self.overwrite_delete_index(&stale, &auth).await?;
            }
        }

        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let staging = staging_index_name(&alias, nonce);
        self.overwrite_create_index(&staging, &marker, mappings, settings, &auth)
            .await
    }

    /// Atomically repoint the alias to the staging index and drop the old
    /// physical indices. The `POST /_aliases` action set is applied atomically by
    /// Elasticsearch, so a reader never sees the alias unbound or pointing at two
    /// generations at once. Both indices are read from the cluster, so this runs
    /// on any sink instance.
    async fn commit_overwrite(&self) -> Result<(), FaucetError> {
        let auth = self.resolve_auth().await?;
        let alias = self.config.index.clone();
        let staging = self.overwrite_staging(&auth).await?;
        let previous: Vec<String> = self
            .overwrite_alias_targets(&alias, &auth)
            .await?
            .into_iter()
            .filter(|idx| idx != &staging)
            .collect();

        // Make the staged docs searchable before the swap.
        self.overwrite_refresh(&staging, &auth).await?;

        let body = build_alias_swap_actions(&alias, &staging, &previous);
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
        for old in &previous {
            if let Err(e) = self.overwrite_delete_index(old, &auth).await {
                tracing::warn!(index = %old, error = %e, "overwrite: could not delete old index after swap");
            }
        }
        tracing::info!(alias = %alias, staging = %staging, "Elasticsearch overwrite committed (alias swapped)");
        Ok(())
    }

    /// Discard the staging index after a failed/cancelled overwrite — the alias
    /// and its current target are left untouched.
    async fn abort_overwrite(&self) -> Result<(), FaucetError> {
        let auth = self.resolve_auth().await?;
        let marker = staging_alias(&self.config.index);
        let live = self
            .alias_targets(&self.config.index, &auth)
            .await?
            .unwrap_or_default();
        for staging in self
            .alias_targets(&marker, &auth)
            .await?
            .unwrap_or_default()
        {
            if !live.contains(&staging) {
                self.overwrite_delete_index(&staging, &auth).await?;
            }
        }
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
                // ES has no NOT NULL → every field is nullable, and any field
                // holds an array of its type (#789 MSG-50).
                out_props.insert(
                    field.clone(),
                    serde_json::json!({ "type": [base, "array", "null"] }),
                );
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
            properties.insert(change.name.clone(), field_mapping(&change.to));
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

        // Upsert / delete routing: plan the page (dedup last-write-wins by
        // document `_id`, strip the delete marker) and emit `index` / `delete`
        // actions. Append **and Overwrite** use the append entries — an
        // overwrite run indexes into the staging index (via `action_meta` →
        // `write_index`), and `commit_overwrite` swaps the alias afterward.
        if !matches!(
            self.config.write.write_mode,
            faucet_core::WriteMode::Append | faucet_core::WriteMode::Overwrite
        ) {
            let planned = plan_origins(records, &self.config.write);
            if let Some((idx, msg)) = planned.failed.first() {
                return Err(FaucetError::Sink(format!(
                    "elasticsearch {}: row {idx}: {msg}",
                    self.config.write.write_mode.as_str()
                )));
            }
            if planned.actions.is_empty() {
                return Ok(0);
            }
            let entries = planned
                .actions
                .iter()
                .map(|a| self.plan_entry(a))
                .collect::<Result<Vec<_>, _>>()?;
            let auth = self.resolve_auth().await?;
            // Re-chunked by `batch_size` like the append path (#789 MSG-90).
            for chunk in self.chunked(&entries) {
                first_failure(&self.bulk_items(chunk, &auth).await?)?;
            }
            tracing::debug!(
                upserts = planned.upserts,
                deletes = planned.deletes,
                "Elasticsearch upsert/delete bulk written"
            );
            return Ok(planned.actions.len());
        }

        // Every entry is built before anything is sent, so an unusable
        // `id_field` value fails the batch without writing part of it.
        let entries = records
            .iter()
            .map(|r| self.append_entry(r))
            .collect::<Result<Vec<_>, _>>()?;
        let chunks = self.chunked(&entries);

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

        let auth = self.resolve_auth().await?;
        let mut total_written = 0;
        for chunk in chunks {
            // `errors: true` with no extractable per-item error is a failure
            // too (a missing item reads as truncated), never a silent drop.
            first_failure(&self.bulk_items(chunk, &auth).await?)?;
            total_written += chunk.len();
            tracing::debug!(records = chunk.len(), "Elasticsearch bulk batch written");
        }
        Ok(total_written)
    }

    /// Write records using the `_bulk` API, returning a per-row outcome.
    ///
    /// Item-level rejections never collapse into an outer `Err`: each record
    /// maps to exactly one [`faucet_core::RowOutcome`] — `Ok(())` when its
    /// action was accepted, `Err` when Elasticsearch rejected it (after the
    /// transient `429`/`503` retries of [`bulk_items`](Self::bulk_items)),
    /// when its response item is missing, or when the record could not be
    /// turned into an action (a missing/`null` key, an unusable `id_field`
    /// value). Only a transport/HTTP failure is an outer `Err`.
    async fn write_batch_partial(
        &self,
        records: &[Value],
    ) -> Result<Vec<faucet_core::RowOutcome>, FaucetError> {
        if records.is_empty() {
            return Ok(Vec::new());
        }
        let mut outcomes: Vec<faucet_core::RowOutcome> =
            (0..records.len()).map(|_| Ok(())).collect();
        // (entry, original page indices behind it)
        let mut sendable: Vec<(String, Vec<usize>)> = Vec::new();

        if !matches!(
            self.config.write.write_mode,
            faucet_core::WriteMode::Append | faucet_core::WriteMode::Overwrite
        ) {
            // Several input rows can dedup into one action; its result is
            // propagated to all of them (#F14).
            let planned = plan_origins(records, &self.config.write);
            for (idx, msg) in &planned.failed {
                outcomes[*idx] = Err(FaucetError::Sink(format!(
                    "elasticsearch {}: row {idx}: {msg}",
                    self.config.write.write_mode.as_str()
                )));
            }
            for action in &planned.actions {
                sendable.push((self.plan_entry(action)?, action.origins.clone()));
            }
        } else {
            for (i, record) in records.iter().enumerate() {
                match self.append_entry(record) {
                    Ok(entry) => sendable.push((entry, vec![i])),
                    Err(e) => outcomes[i] = Err(e),
                }
            }
        }

        if sendable.is_empty() {
            return Ok(outcomes);
        }
        let auth = self.resolve_auth().await?;
        for chunk in self.chunked(&sendable) {
            let entries: Vec<String> = chunk.iter().map(|(e, _)| e.clone()).collect();
            let items = self.bulk_items(&entries, &auth).await?;
            for ((_, origins), item) in chunk.iter().zip(&items) {
                if let Some(msg) = item_failure(item) {
                    for &orig in origins {
                        if outcomes[orig].is_ok() {
                            outcomes[orig] = Err(FaucetError::Sink(msg.clone()));
                        }
                    }
                }
            }
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
    fn item_failures_read_any_action_type_and_missing_items() {
        let ok = json!({"index": {"status": 201}});
        let bad = json!({"index": {"status": 400, "error": {"type": "mapper_parsing_exception"}}});
        let conflict = json!({"create": {"status": 409, "error": {"type": "version_conflict"}}});
        assert!(item_failure(&ok).is_none());
        assert!(
            item_failure(&bad)
                .unwrap()
                .contains("mapper_parsing_exception")
        );
        assert!(
            item_failure(&conflict)
                .unwrap()
                .contains("version_conflict")
        );
        assert!(item_failure(&Value::Null).unwrap().contains("truncated"));
        let err = first_failure(&[ok.clone(), bad, conflict]).unwrap_err();
        assert!(err.to_string().contains("had 2 errors"), "{err}");
        assert!(first_failure(&[ok]).is_ok());
    }

    #[test]
    fn only_overload_rejections_are_retried() {
        let r429 = json!({"index": {"status": 429, "error": {"type": "x"}}});
        let r503 = json!({"index": {"status": 503, "error": {"type": "x"}}});
        let pool =
            json!({"index": {"status": 500, "error": {"type": "es_rejected_execution_exception"}}});
        let parse =
            json!({"index": {"status": 400, "error": {"type": "mapper_parsing_exception"}}});
        let ok = json!({"index": {"status": 201}});
        assert!(retriable_item(&r429) && retriable_item(&r503) && retriable_item(&pool));
        assert!(!retriable_item(&parse) && !retriable_item(&ok) && !retriable_item(&Value::Null));
        assert_eq!(item_retry_delay(1).as_millis(), 200);
        assert_eq!(item_retry_delay(3).as_millis(), 800);
        assert_eq!(item_retry_delay(30).as_millis(), 5_000);
    }

    #[test]
    fn settings_copy_keeps_what_the_index_needs_and_drops_identity() {
        let got = copyable_settings(&json!({
            "number_of_shards": "3",
            "number_of_replicas": "1",
            "refresh_interval": "30s",
            "analysis": {"analyzer": {"a": {"type": "custom", "tokenizer": "standard"}}},
            "lifecycle": {"name": "p", "rollover_alias": "orders"},
            "uuid": "abc",
            "creation_date": "1",
            "provided_name": "orders-1",
            "version": {"created": "1"}
        }));
        assert_eq!(
            got,
            json!({
                "number_of_shards": "3",
                "number_of_replicas": "1",
                "refresh_interval": "30s",
                "analysis": {"analyzer": {"a": {"type": "custom", "tokenizer": "standard"}}},
                "lifecycle": {"name": "p"}
            })
        );
        assert_eq!(copyable_settings(&json!("x")), json!({}));
    }

    #[test]
    fn cleanup_terms_target_keyword_sub_fields_of_text() {
        let props = json!({
            "tenant": {"type": "text", "fields": {"raw": {"type": "keyword"}}},
            "plain": {"type": "keyword"},
            "body": {"type": "text"},
            "owner": {"properties": {"name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}}}}
        });
        assert_eq!(term_field(&props, "tenant").unwrap(), "tenant.raw");
        assert_eq!(term_field(&props, "plain").unwrap(), "plain");
        assert_eq!(
            term_field(&props, "owner.name").unwrap(),
            "owner.name.keyword"
        );
        assert_eq!(term_field(&props, "unmapped").unwrap(), "unmapped");
        assert!(term_field(&props, "body").unwrap_err().contains("keyword"));
    }

    #[test]
    fn evolved_mappings_use_item_types_and_bounded_keywords() {
        assert_eq!(
            field_mapping(&json!({"type": "array", "items": {"type": "integer"}})),
            json!({"type": "long"})
        );
        assert_eq!(
            field_mapping(&json!({"type": ["array", "null"], "items": {"type": "string"}})),
            json!({"type": "keyword", "ignore_above": 8191})
        );
        assert_eq!(
            field_mapping(&json!({"type": "string"})),
            json!({"type": "keyword", "ignore_above": 8191})
        );
        assert_eq!(
            field_mapping(&json!({"type": "boolean"})),
            json!({"type": "boolean"})
        );
        assert_eq!(
            field_mapping(&json!({"type": "array"})),
            json!({"type": "keyword", "ignore_above": 8191})
        );
    }

    #[test]
    fn plan_dedups_by_document_id_in_page_order() {
        use faucet_core::{WriteMode, WriteSpec};
        let spec = WriteSpec {
            write_mode: WriteMode::Upsert,
            key: vec!["id".to_string()],
            delete_marker: Some(faucet_core::DeleteMarker {
                field: "__op".to_string(),
                values: vec!["d".to_string()],
            }),
            rollback: None,
        };
        let page = vec![
            json!({"id": 7, "v": 1}),
            json!({"id": 8, "v": 2}),
            json!({"id": "7", "__op": "d"}),
            json!({"v": 3}),
        ];
        let planned = plan_origins(&page, &spec);
        assert_eq!(planned.actions.len(), 2, "7 and \"7\" are one document");
        assert_eq!(planned.actions[0].id, "7");
        assert!(planned.actions[0].doc.is_none(), "the later delete wins");
        assert_eq!(planned.actions[0].origins, vec![0, 2]);
        assert_eq!((planned.upserts, planned.deletes), (1, 1));
        assert_eq!(planned.failed.len(), 1);
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

        let body: String = records
            .iter()
            .map(|r| sink.append_entry(r).unwrap())
            .collect();
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

        let body: String = records
            .iter()
            .map(|r| sink.append_entry(r).unwrap())
            .collect();
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

        // A null / array / object id is refused rather than rendered "null".
        for bad in [
            json!({"doc_id": null}),
            json!({"doc_id": [1]}),
            json!({"doc_id": {}}),
        ] {
            let err = sink.append_entry(&bad).unwrap_err();
            assert!(err.to_string().contains("not a string"), "{err}");
        }
    }

    #[test]
    fn create_op_type_emits_create_actions() {
        let mut config = ElasticsearchSinkConfig::new("http://localhost:9200", "logs-app");
        config.op_type = crate::config::ElasticsearchOpType::Create;
        let sink = ElasticsearchSink::new(config).unwrap();
        let entry = sink.append_entry(&json!({"m": 1})).unwrap();
        let action: Value = serde_json::from_str(entry.lines().next().unwrap()).unwrap();
        assert_eq!(action["create"]["_index"], "logs-app");
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
        // ids must be byte-identical to the ones `plan_entry` indexes under
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
        let planned = plan_origins(&records, &sink.config.write);
        let body: String = planned
            .actions
            .iter()
            .map(|a| sink.plan_entry(a).unwrap())
            .collect();
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
        assert_eq!(
            actions.len(),
            4,
            "two removes + one add + the marker detach"
        );
        assert_eq!(actions[0]["remove"]["index"], "orders-old-a");
        assert_eq!(actions[0]["remove"]["alias"], "orders");
        assert_eq!(actions[1]["remove"]["index"], "orders-old-b");
        assert_eq!(actions[2]["add"]["index"], "orders-faucet-ovw-1");
        assert_eq!(actions[2]["add"]["alias"], "orders");
        assert_eq!(actions[3]["remove"]["index"], "orders-faucet-ovw-1");
        assert_eq!(actions[3]["remove"]["alias"], "orders-faucet-ovw-staging");
    }

    #[test]
    fn alias_swap_first_run_only_adds() {
        let body = build_alias_swap_actions("orders", "orders-faucet-ovw-1", &[]);
        let actions = body["actions"].as_array().unwrap();
        assert_eq!(actions.len(), 2);
        assert!(actions[0].get("add").is_some());
        assert_eq!(actions[1]["remove"]["alias"], "orders-faucet-ovw-staging");
    }
}
