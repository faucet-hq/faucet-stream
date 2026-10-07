//! MongoDB stream executor.

use crate::config::MongoSourceConfig;
use async_trait::async_trait;
use faucet_core::{FaucetError, Stream, StreamPage};
use mongodb::Client;
use mongodb::bson::{self, Bson, Document};
use mongodb::options::FindOptions;
use serde_json::Value;
use std::pin::Pin;

/// Documents sampled per collection by [`Source::discover`] to infer a
/// representative schema. Kept small — discovery must stay cheap.
const DISCOVER_SAMPLE_SIZE: i64 = 10;

/// A configured MongoDB source that connects to a collection and fetches documents.
///
/// The MongoDB `Client` is created once during construction and reused across
/// all `fetch_all()` calls. It maintains an internal connection pool.
pub struct MongoSource {
    config: MongoSourceConfig,
    client: Client,
}

impl MongoSource {
    /// Create a new MongoDB source from the given configuration.
    ///
    /// This establishes the MongoDB client (with its internal connection pool)
    /// immediately.
    pub async fn new(config: MongoSourceConfig) -> Result<Self, FaucetError> {
        faucet_core::validate_batch_size(config.batch_size)?;
        let client = Client::with_uri_str(&config.connection_uri)
            .await
            .map_err(|e| FaucetError::Source(format!("MongoDB connection failed: {e}")))?;

        Ok(Self { config, client })
    }

    /// Fetch all matching documents from the configured collection.
    pub async fn fetch_all(&self) -> Result<Vec<Value>, FaucetError> {
        let db = self.client.database(&self.config.database);
        let collection = db.collection::<Document>(&self.config.collection);

        let filter = self
            .config
            .filter
            .as_ref()
            .map(json_value_to_document)
            .transpose()?;

        let mut find_options = FindOptions::default();

        if let Some(ref proj) = self.config.projection {
            find_options.projection = Some(json_value_to_document(proj)?);
        }
        if let Some(ref sort) = self.config.sort {
            find_options.sort = Some(json_value_to_document(sort)?);
        }
        if let Some(limit) = self.config.limit {
            find_options.limit = Some(limit);
        }
        if let Some(cursor_batch_size) = self.config.cursor_batch_size {
            find_options.batch_size = Some(cursor_batch_size);
        }

        let mut cursor = collection
            .find(filter.unwrap_or_default())
            .with_options(find_options)
            .await
            .map_err(|e| FaucetError::Source(format!("MongoDB find failed: {e}")))?;

        let mut records = Vec::new();

        while cursor
            .advance()
            .await
            .map_err(|e| FaucetError::Source(format!("MongoDB cursor advance failed: {e}")))?
        {
            let doc = cursor
                .deserialize_current()
                .map_err(|e| FaucetError::Source(format!("MongoDB deserialization failed: {e}")))?;

            let value = bson_document_to_json_value(&doc)?;
            records.push(value);
        }

        tracing::info!(
            records = records.len(),
            database = %self.config.database,
            collection = %self.config.collection,
            "MongoDB fetch complete"
        );

        Ok(records)
    }
}

#[async_trait]
impl faucet_core::Source for MongoSource {
    async fn fetch_with_context(
        &self,
        context: &std::collections::HashMap<String, serde_json::Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        if context.is_empty() {
            return MongoSource::fetch_all(self).await;
        }

        // Substitute context placeholders into filter, projection, and sort.
        let filter = substitute_optional_value(&self.config.filter, context, "filter")?;
        let projection = substitute_optional_value(&self.config.projection, context, "projection")?;
        let sort = substitute_optional_value(&self.config.sort, context, "sort")?;

        let db = self.client.database(&self.config.database);
        let collection = db.collection::<Document>(&self.config.collection);

        let filter_doc = filter.as_ref().map(json_value_to_document).transpose()?;

        let mut find_options = FindOptions::default();
        if let Some(ref proj) = projection {
            find_options.projection = Some(json_value_to_document(proj)?);
        }
        if let Some(ref s) = sort {
            find_options.sort = Some(json_value_to_document(s)?);
        }
        if let Some(limit) = self.config.limit {
            find_options.limit = Some(limit);
        }
        if let Some(cursor_batch_size) = self.config.cursor_batch_size {
            find_options.batch_size = Some(cursor_batch_size);
        }

        let mut cursor = collection
            .find(filter_doc.unwrap_or_default())
            .with_options(find_options)
            .await
            .map_err(|e| FaucetError::Source(format!("MongoDB find failed: {e}")))?;

        let mut records = Vec::new();
        while cursor
            .advance()
            .await
            .map_err(|e| FaucetError::Source(format!("MongoDB cursor advance failed: {e}")))?
        {
            let doc = cursor
                .deserialize_current()
                .map_err(|e| FaucetError::Source(format!("MongoDB deserialization failed: {e}")))?;
            records.push(bson_document_to_json_value(&doc)?);
        }

        tracing::info!(
            records = records.len(),
            database = %self.config.database,
            collection = %self.config.collection,
            "MongoDB fetch complete (with context)"
        );

        Ok(records)
    }

    /// Stream documents from the underlying MongoDB cursor without buffering
    /// the full result set. Each emitted [`StreamPage`] holds up to
    /// [`MongoSourceConfig::batch_size`] documents.
    ///
    /// The trait-level `batch_size` argument is ignored in favour of the
    /// config field — the config is the user-facing knob the README
    /// documents, and routing the pipeline-supplied hint through it would
    /// silently override an explicit config value.
    ///
    /// `batch_size = 0` drains the entire cursor into a single page. The
    /// MongoDB source has no incremental-replication mode today, so every
    /// emitted page carries `bookmark: None`.
    ///
    /// Note: [`MongoSourceConfig::cursor_batch_size`] is independent — it
    /// controls the driver's per-round-trip batch size, while `batch_size`
    /// controls how many documents are buffered before a `StreamPage` is
    /// yielded to the pipeline.
    fn stream_pages<'a>(
        &'a self,
        context: &'a std::collections::HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let batch_size = self.config.batch_size;

        Box::pin(async_stream::try_stream! {
            // Substitute context placeholders into filter, projection, sort
            // (matching fetch_with_context's behaviour).
            let (filter, projection, sort) = if context.is_empty() {
                (
                    self.config.filter.clone(),
                    self.config.projection.clone(),
                    self.config.sort.clone(),
                )
            } else {
                (
                    substitute_optional_value(&self.config.filter, context, "filter")?,
                    substitute_optional_value(&self.config.projection, context, "projection")?,
                    substitute_optional_value(&self.config.sort, context, "sort")?,
                )
            };

            let db = self.client.database(&self.config.database);
            let collection = db.collection::<Document>(&self.config.collection);

            let filter_doc = filter.as_ref().map(json_value_to_document).transpose()?;

            let mut find_options = FindOptions::default();
            if let Some(ref proj) = projection {
                find_options.projection = Some(json_value_to_document(proj)?);
            }
            if let Some(ref s) = sort {
                find_options.sort = Some(json_value_to_document(s)?);
            }
            if let Some(limit) = self.config.limit {
                find_options.limit = Some(limit);
            }
            if let Some(cursor_batch_size) = self.config.cursor_batch_size {
                find_options.batch_size = Some(cursor_batch_size);
            }

            let mut cursor = collection
                .find(filter_doc.unwrap_or_default())
                .with_options(find_options)
                .await
                .map_err(|e| FaucetError::Source(format!("MongoDB find failed: {e}")))?;

            let chunk = if batch_size == 0 { usize::MAX } else { batch_size };
            let initial_capacity = if batch_size == 0 { 1024 } else { batch_size };
            let mut buffer: Vec<Value> = Vec::with_capacity(initial_capacity);
            let mut total = 0usize;

            while cursor
                .advance()
                .await
                .map_err(|e| FaucetError::Source(format!("MongoDB cursor advance failed: {e}")))?
            {
                let doc = cursor
                    .deserialize_current()
                    .map_err(|e| FaucetError::Source(format!("MongoDB deserialization failed: {e}")))?;
                buffer.push(bson_document_to_json_value(&doc)?);
                if buffer.len() >= chunk {
                    let page = std::mem::replace(&mut buffer, Vec::with_capacity(initial_capacity));
                    total += page.len();
                    yield StreamPage { records: page, bookmark: None };
                }
            }
            if !buffer.is_empty() {
                total += buffer.len();
                yield StreamPage { records: buffer, bookmark: None };
            }

            tracing::info!(
                records = total,
                batch_size,
                database = %self.config.database,
                collection = %self.config.collection,
                "MongoDB source stream complete",
            );
        })
    }

    fn connector_name(&self) -> &'static str {
        "mongodb"
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(MongoSourceConfig))
            .expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        collection_uri(
            &self.config.connection_uri,
            &self.config.database,
            &self.config.collection,
        )
    }

    fn supports_discover(&self) -> bool {
        true
    }

    /// Enumerate the collections in the configured database (excluding
    /// `system.*` and faucet's own collections), with a row estimate from `estimated_document_count`
    /// (collection metadata — no scan) and a schema inferred from a bounded
    /// `DISCOVER_SAMPLE_SIZE`-document sample per collection.
    async fn discover(&self) -> Result<Vec<faucet_core::DatasetDescriptor>, FaucetError> {
        let db = self.client.database(&self.config.database);
        let mut names: Vec<String> = db
            .list_collection_names()
            .await
            .map_err(|e| FaucetError::Source(format!("mongodb: catalog discovery failed: {e}")))?
            .into_iter()
            .filter(|name| is_discoverable(name))
            .collect();
        names.sort();

        let mut datasets = Vec::with_capacity(names.len());
        for name in names {
            let collection = db.collection::<Document>(&name);
            let estimated = collection.estimated_document_count().await.map_err(|e| {
                FaucetError::Source(format!(
                    "mongodb: catalog discovery failed (count for {name:?}): {e}"
                ))
            })?;

            // Bounded sample for schema inference — the same BSON→JSON
            // conversion the fetch path uses, capped at DISCOVER_SAMPLE_SIZE.
            let mut cursor = collection
                .find(Document::new())
                .limit(DISCOVER_SAMPLE_SIZE)
                .await
                .map_err(|e| {
                    FaucetError::Source(format!(
                        "mongodb: catalog discovery failed (sample for {name:?}): {e}"
                    ))
                })?;
            let mut sample = Vec::new();
            while cursor.advance().await.map_err(|e| {
                FaucetError::Source(format!(
                    "mongodb: catalog discovery failed (sample for {name:?}): {e}"
                ))
            })? {
                let doc = cursor.deserialize_current().map_err(|e| {
                    FaucetError::Source(format!(
                        "mongodb: catalog discovery failed (decode for {name:?}): {e}"
                    ))
                })?;
                sample.push(bson_document_to_json_value(&doc)?);
            }

            datasets.push(descriptor_for_collection(&name, &sample, Some(estimated)));
        }
        Ok(datasets)
    }
}

/// Build a [`DatasetDescriptor`](faucet_core::DatasetDescriptor) for one
/// collection from its name, a small JSON document sample, and a cheap
/// metadata row estimate. An empty sample yields no schema (an empty
/// collection has no shape to report). Pure — unit-testable without a live
/// server.
fn descriptor_for_collection(
    name: &str,
    sample: &[Value],
    estimated_rows: Option<u64>,
) -> faucet_core::DatasetDescriptor {
    let mut descriptor = faucet_core::DatasetDescriptor::new(
        name,
        "collection",
        serde_json::json!({ "collection": name }),
    )
    .with_primary_key(vec!["_id".to_string()]);
    if !sample.is_empty() {
        descriptor = descriptor.with_schema(faucet_core::schema::infer_schema(sample));
    }
    if let Some(rows) = estimated_rows {
        descriptor = descriptor.with_estimated_rows(rows);
    }
    descriptor
}

/// Substitute context placeholders in an optional JSON value.
///
/// Serialises the value to a string, runs [`substitute_context_json`] (which
/// properly escapes string values for JSON safety), then deserialises back.
/// Returns `None` when the input is `None`.
fn substitute_optional_value(
    value: &Option<Value>,
    context: &std::collections::HashMap<String, Value>,
    field_name: &str,
) -> Result<Option<Value>, FaucetError> {
    match value {
        Some(v) => {
            let s = serde_json::to_string(v).map_err(|e| {
                FaucetError::Config(format!("failed to serialize {field_name}: {e}"))
            })?;
            let s = faucet_core::util::substitute_context_json(&s, context);
            let resolved = serde_json::from_str(&s).map_err(|e| {
                FaucetError::Config(format!("failed to parse substituted {field_name}: {e}"))
            })?;
            Ok(Some(resolved))
        }
        None => Ok(None),
    }
}

/// Convert a `serde_json::Value` to a `bson::Document`.
///
/// The value must be a JSON object; other types produce a `Config` error.
fn json_value_to_document(val: &Value) -> Result<Document, FaucetError> {
    let bson = bson::to_bson(val)
        .map_err(|e| FaucetError::Config(format!("failed to convert JSON to BSON: {e}")))?;
    match bson {
        Bson::Document(doc) => Ok(doc),
        other => Err(FaucetError::Config(format!(
            "expected a JSON object, got BSON type: {other:?}"
        ))),
    }
}

/// Convert a `bson::Document` to a `serde_json::Value`.
fn bson_document_to_json_value(doc: &Document) -> Result<Value, FaucetError> {
    let bson = Bson::Document(doc.clone());
    let relaxed = bson.into_relaxed_extjson();
    Ok(relaxed)
}

/// Whether discovery lists a collection: not a `system.*` collection and not
/// one faucet maintains (the exactly-once watermark, the rollback journal, an
/// overwrite's staging collection), which a wildcard `include` would
/// otherwise pick up as data (#789 MSG-92).
fn is_discoverable(name: &str) -> bool {
    !(name.starts_with("system.")
        || name == faucet_core::idempotency::COMMIT_TOKEN_TABLE
        || name == faucet_core::RUN_JOURNAL_TABLE
        || name.contains(faucet_core::idempotency::OVERWRITE_STAGING_SUFFIX))
}

/// `mongodb://hosts/<db>/<collection>` with the URI's credentials, path
/// (auth database) and query options removed, so a URI carrying options
/// still yields one well-formed, stable catalog identity (#789 MSG-92).
pub(crate) fn collection_uri(connection_uri: &str, database: &str, collection: &str) -> String {
    let redacted = faucet_core::redact_uri_credentials(connection_uri);
    let base = match redacted.find("://") {
        Some(i) => {
            let rest = &redacted[i + 3..];
            let end = rest.find(['/', '?']).unwrap_or(rest.len());
            &redacted[..i + 3 + end]
        }
        None => redacted.as_str(),
    };
    format!("{base}/{database}/{collection}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn json_object_to_document() {
        let val = json!({"name": "Alice", "age": 30});
        let doc = json_value_to_document(&val).unwrap();
        assert_eq!(doc.get_str("name").unwrap(), "Alice");
        assert_eq!(doc.get_i64("age").unwrap(), 30);
    }

    #[test]
    fn json_non_object_to_document_fails() {
        let val = json!([1, 2, 3]);
        let result = json_value_to_document(&val);
        assert!(result.is_err());
        assert!(matches!(result, Err(FaucetError::Config(_))));
    }

    #[test]
    fn json_string_to_document_fails() {
        let val = json!("not an object");
        let result = json_value_to_document(&val);
        assert!(result.is_err());
    }

    #[test]
    fn bson_document_roundtrip() {
        let mut doc = Document::new();
        doc.insert("name", "Bob");
        doc.insert("score", 42);
        let value = bson_document_to_json_value(&doc).unwrap();
        assert_eq!(value["name"], "Bob");
        assert_eq!(value["score"], 42);
    }

    #[test]
    fn nested_document_conversion() {
        let val = json!({"user": {"name": "Alice", "tags": ["admin", "user"]}});
        let doc = json_value_to_document(&val).unwrap();
        let inner = doc.get_document("user").unwrap();
        assert_eq!(inner.get_str("name").unwrap(), "Alice");

        let back = bson_document_to_json_value(&doc).unwrap();
        assert_eq!(back["user"]["name"], "Alice");
        assert_eq!(back["user"]["tags"][0], "admin");
    }

    #[test]
    fn empty_filter_converts() {
        let val = json!({});
        let doc = json_value_to_document(&val).unwrap();
        assert!(doc.is_empty());
    }

    #[tokio::test]
    async fn new_rejects_out_of_range_batch_size() {
        let mut config = MongoSourceConfig::new("mongodb://localhost:27017", "db", "c");
        config.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
        match MongoSource::new(config).await {
            Err(faucet_core::FaucetError::Config(m)) => {
                assert!(m.contains("batch_size"), "got: {m}")
            }
            _ => panic!("expected a batch_size Config error"),
        }
    }

    // dataset_uri is a pure-config method; MongoSource requires an async
    // constructor with a live server, so we verify the logic directly.
    #[test]
    fn dataset_uri_strips_credentials_path_and_options() {
        assert_eq!(
            collection_uri("mongodb://u:p@h:27017", "mydb", "events"),
            "mongodb://h:27017/mydb/events"
        );
        assert_eq!(
            collection_uri(
                "mongodb://u:p@h1:27017,h2:27017/admin?replicaSet=rs0&tls=true",
                "mydb",
                "events"
            ),
            "mongodb://h1:27017,h2:27017/mydb/events"
        );
        assert_eq!(
            collection_uri("mongodb+srv://c.example.net/?retryWrites=true", "d", "c"),
            "mongodb+srv://c.example.net/d/c"
        );
        assert_eq!(collection_uri("weird", "d", "c"), "weird/d/c");
    }

    #[test]
    fn faucet_internal_collections_are_not_discovered() {
        assert!(is_discoverable("orders"));
        assert!(!is_discoverable("system.views"));
        assert!(!is_discoverable("_faucet_commit_token"));
        assert!(!is_discoverable("_faucet_run_journal"));
        assert!(!is_discoverable("orders__faucet_ovw"));
        assert!(!is_discoverable("orders__faucet_ovw_old"));
    }

    // --- substitute_optional_value (the private context-interpolation helper) ---

    #[test]
    fn substitute_optional_value_none_passthrough() {
        let ctx: std::collections::HashMap<String, Value> = std::collections::HashMap::new();
        let out = substitute_optional_value(&None, &ctx, "filter").unwrap();
        assert!(out.is_none(), "None input must yield None output");
    }

    #[test]
    fn substitute_optional_value_replaces_string_placeholder() {
        let mut ctx: std::collections::HashMap<String, Value> = std::collections::HashMap::new();
        // Placeholders use the flat `{key}` syntax over top-level context keys.
        ctx.insert("user_id".into(), json!("abc-123"));
        // The placeholder lives inside a JSON string value; substitution must
        // preserve JSON validity (string stays a string).
        let filter = Some(json!({"owner": "{user_id}"}));
        let out = substitute_optional_value(&filter, &ctx, "filter")
            .unwrap()
            .expect("Some input yields Some output");
        assert_eq!(out, json!({"owner": "abc-123"}));
    }

    #[test]
    fn substitute_optional_value_no_placeholder_is_identity() {
        let ctx: std::collections::HashMap<String, Value> = std::collections::HashMap::new();
        let proj = Some(json!({"_id": 0, "name": 1}));
        let out = substitute_optional_value(&proj, &ctx, "projection")
            .unwrap()
            .unwrap();
        assert_eq!(out, json!({"_id": 0, "name": 1}));
    }

    #[test]
    fn substitute_optional_value_escapes_value_for_json_safety() {
        // A context value containing a double-quote must be escaped so the
        // re-parsed JSON is valid and the literal characters survive.
        let mut ctx: std::collections::HashMap<String, Value> = std::collections::HashMap::new();
        ctx.insert("name".into(), json!("a\"b"));
        let filter = Some(json!({"n": "{name}"}));
        let out = substitute_optional_value(&filter, &ctx, "filter")
            .unwrap()
            .unwrap();
        assert_eq!(out, json!({"n": "a\"b"}));
    }

    // --- bson_document_to_json_value: relaxed extended-JSON shapes ---

    #[test]
    fn bson_object_id_converts_to_oid_extjson() {
        use mongodb::bson::oid::ObjectId;
        let mut doc = Document::new();
        let oid = ObjectId::parse_str("64ab00112233445566778899").unwrap();
        doc.insert("_id", oid);
        let value = bson_document_to_json_value(&doc).unwrap();
        // Relaxed extended JSON renders an ObjectId as {"$oid": "<hex>"}.
        assert_eq!(value["_id"]["$oid"], "64ab00112233445566778899");
    }

    #[test]
    fn bson_datetime_converts_to_date_extjson() {
        use mongodb::bson::DateTime;
        let mut doc = Document::new();
        // 1_000_000 ms past the epoch.
        doc.insert("created_at", DateTime::from_millis(1_000_000));
        let value = bson_document_to_json_value(&doc).unwrap();
        // Relaxed extended JSON renders a post-1970 datetime as
        // {"$date": "<RFC3339>"}.
        assert!(
            value["created_at"]["$date"].is_string(),
            "expected $date string, got {value:?}"
        );
    }

    #[test]
    fn bson_null_and_array_and_nested_convert() {
        use mongodb::bson::Bson;
        let mut doc = Document::new();
        doc.insert("missing", Bson::Null);
        doc.insert("tags", vec!["a", "b"]);
        let mut nested = Document::new();
        nested.insert("k", 7i32);
        doc.insert("nested", nested);
        let value = bson_document_to_json_value(&doc).unwrap();
        assert_eq!(value["missing"], Value::Null);
        assert_eq!(value["tags"], json!(["a", "b"]));
        assert_eq!(value["nested"]["k"], 7);
    }

    #[test]
    fn bson_int64_converts_to_number() {
        let mut doc = Document::new();
        doc.insert("big", 9_000_000_000i64);
        let value = bson_document_to_json_value(&doc).unwrap();
        // Relaxed extended JSON renders an i64 as a bare JSON number.
        assert_eq!(value["big"], json!(9_000_000_000i64));
    }

    // ── discover: pure descriptor building ──────────────────────────────────

    #[test]
    fn descriptor_infers_schema_from_sample() {
        let sample = vec![
            json!({"id": 1, "name": "alpha", "score": 1.5}),
            json!({"id": 2, "name": "beta"}),
        ];
        let d = descriptor_for_collection("orders", &sample, Some(120));
        assert_eq!(d.primary_key, Some(vec!["_id".to_string()]));
        assert_eq!(d.name, "orders");
        assert_eq!(d.kind, "collection");
        assert_eq!(d.config_patch, json!({"collection": "orders"}));
        assert_eq!(d.estimated_rows, Some(120));
        let schema = d.schema.as_ref().expect("schema from non-empty sample");
        assert_eq!(schema["type"], "object");
        assert_eq!(schema["properties"]["id"]["type"], "integer");
        assert_eq!(schema["properties"]["name"]["type"], "string");
        assert_eq!(
            schema["properties"]["score"]["type"],
            json!(["null", "number"]),
            "field absent from one sampled doc is nullable"
        );
    }

    #[test]
    fn descriptor_empty_sample_has_no_schema() {
        let d = descriptor_for_collection("empty", &[], Some(0));
        assert_eq!(d.name, "empty");
        assert_eq!(d.kind, "collection");
        assert_eq!(d.config_patch, json!({"collection": "empty"}));
        assert_eq!(d.estimated_rows, Some(0));
        assert!(
            d.schema.is_none(),
            "empty collection has no shape to report"
        );
    }

    #[test]
    fn descriptor_without_estimate_omits_rows() {
        let d = descriptor_for_collection("c", &[json!({"k": true})], None);
        assert_eq!(d.estimated_rows, None);
        let schema = d.schema.as_ref().unwrap();
        assert_eq!(schema["properties"]["k"]["type"], "boolean");
    }

    #[tokio::test]
    async fn source_advertises_discover() {
        use faucet_core::Source as _;
        // `Client::with_uri_str` does no I/O for a well-formed URI, so the
        // capability flag is checkable offline; `discover()` against the
        // unreachable server surfaces the typed discovery error.
        let config = MongoSourceConfig::new(
            "mongodb://127.0.0.1:1/?connectTimeoutMS=200&serverSelectionTimeoutMS=200",
            "db",
            "c",
        );
        let source = MongoSource::new(config).await.expect("client construct");
        assert!(source.supports_discover());
        let err = source.discover().await.unwrap_err();
        assert!(
            err.to_string().contains("catalog discovery failed"),
            "typed error: {err}"
        );
    }
}
