//! Cached HTTP client for the Confluent Schema Registry REST API.

use crate::schema_registry::SchemaRegistryConfig;
use faucet_core::FaucetError;
use lru::LruCache;
use serde::Deserialize;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Raw schema document returned by the registry.
#[derive(Debug, Clone, Deserialize)]
pub struct RegistrySchema {
    pub schema: String,
    #[serde(
        default = "default_schema_type",
        rename = "schemaType",
        alias = "schema_type"
    )]
    pub schema_type: String,
    #[serde(default)]
    pub references: Vec<SchemaReference>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SchemaReference {
    pub name: String,
    pub subject: String,
    pub version: i32,
}

fn default_schema_type() -> String {
    "AVRO".into()
}

/// The most references followed from one schema, transitively.
const MAX_REFERENCES: usize = 256;

impl RegistrySchema {
    /// Refuse a schema registered as another type than the codec reads
    /// (`AVRO`, `PROTOBUF`, `JSON`).
    pub fn expect_type(&self, want: &str) -> Result<(), FaucetError> {
        if self.schema_type.eq_ignore_ascii_case(want) {
            Ok(())
        } else {
            Err(FaucetError::Source(format!(
                "schema registry: the writer schema is registered as {}, but the configured \
                 format reads {want}",
                self.schema_type
            )))
        }
    }
}

/// HTTP client that fetches and caches Schema Registry entries.
///
/// Cloning is cheap (`Arc`); the cache is shared across clones.
#[derive(Clone)]
pub struct SchemaRegistryClient {
    http: reqwest::Client,
    base_url: String,
    auth: Option<crate::BasicAuth>,
    cache: Arc<Mutex<LruCache<u32, RegistrySchema>>>,
    /// Caches `register_schema` results so the same `(subject, schema)` does
    /// not POST to the registry on every produced record (#78/#30). Keyed by
    /// subject, then schema type + schema text.
    register_cache: Arc<Mutex<RegisterCache>>,
    /// Parsed/compiled schemas keyed by codec + schema text, so a codec
    /// parses an Avro schema, compiles a `.proto` or builds a JSON Schema
    /// validator once per schema rather than once per message (#789 MSG-65).
    parsed: Arc<std::sync::Mutex<ParsedCache>>,
    /// Backend-call context (#880), shared by clones: the connector forwards
    /// the run's context here, so a registry request ends at the run's
    /// deadline and is metered (`schema`, `register`).
    calls: Arc<faucet_core::CallSlot>,
    /// Schemas fetched by subject and version, for references.
    versions: Arc<Mutex<LruCache<(String, i32), RegistrySchema>>>,
    /// The built codec / descriptor pool / validator per codec and schema
    /// id. A registered schema id never changes, so a hit skips fetching the
    /// schema and resolving its references again.
    compiled: Arc<std::sync::Mutex<CompiledCache>>,
}

type CompiledCache = LruCache<(&'static str, u32), Arc<dyn std::any::Any + Send + Sync>>;

type ParsedSchema = Arc<dyn std::any::Any + Send + Sync>;

/// Schema text → (codec kind, parsed schema).
type ParsedCache = LruCache<String, Vec<(String, ParsedSchema)>>;

/// Subject → (schema type, schema text, registered id).
type RegisterCache = LruCache<String, Vec<(String, String, u32)>>;

/// Entries one cache key keeps for its second key part (codec kind, schema
/// type and text); the oldest is dropped past it.
const ENTRIES_PER_KEY: usize = 16;

/// Look up `matches` among `key`'s entries without building a composite key.
fn entry_of<'a, E>(
    cache: &'a mut LruCache<String, Vec<E>>,
    key: &str,
    matches: impl Fn(&E) -> bool,
) -> Option<&'a E> {
    cache.get(key)?.iter().find(|e| matches(e))
}

/// Store `entry` under `key`, replacing one `matches` finds.
fn put_entry<E>(
    cache: &mut LruCache<String, Vec<E>>,
    key: &str,
    entry: E,
    matches: impl Fn(&E) -> bool,
) {
    match cache.get_mut(key) {
        Some(entries) => {
            entries.retain(|e| !matches(e));
            if entries.len() >= ENTRIES_PER_KEY {
                entries.remove(0);
            }
            entries.push(entry);
        }
        None => {
            cache.put(key.to_string(), vec![entry]);
        }
    }
}

impl SchemaRegistryClient {
    pub fn new(config: &SchemaRegistryConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let http = faucet_core::http::Client::builder()
            .timeout(config.request_timeout)
            .build_reqwest()
            .map_err(|e| FaucetError::Config(format!("schema-registry HTTP client: {e}")))?;
        let capacity = NonZeroUsize::new(config.cache_capacity).unwrap();
        Ok(Self {
            http,
            base_url: config.url.trim_end_matches('/').to_string(),
            auth: config.auth.clone(),
            cache: Arc::new(Mutex::new(LruCache::new(capacity))),
            register_cache: Arc::new(Mutex::new(LruCache::new(capacity))),
            parsed: Arc::new(std::sync::Mutex::new(LruCache::new(capacity))),
            versions: Arc::new(Mutex::new(LruCache::new(capacity))),
            compiled: Arc::new(std::sync::Mutex::new(LruCache::new(capacity))),
            calls: Arc::new(faucet_core::CallSlot::new(
                faucet_core::CallContext::new().without_timeout(),
            )),
        })
    }

    /// Install the run's call context (the connector forwards its own).
    pub fn set_call_context(&self, ctx: faucet_core::CallContext) {
        self.calls.install(ctx);
    }

    /// Install the round-trip recorder registry requests are counted on.
    pub fn set_roundtrip_recorder(
        &self,
        recorder: Arc<faucet_core::observability::RoundtripRecorder>,
    ) {
        self.calls.install_recorder(recorder);
    }

    /// The parsed form of `text` for codec `kind`, built with `build` on the
    /// first request and shared afterwards. A failed build is not cached.
    pub fn parsed<T, F>(&self, kind: &str, text: &str, build: F) -> Result<Arc<T>, FaucetError>
    where
        T: Send + Sync + 'static,
        F: FnOnce(&str) -> Result<T, FaucetError>,
    {
        if let Some(hit) =
            self.parsed.lock().ok().and_then(|mut c| {
                entry_of(&mut c, text, |(k, _)| k == kind).map(|(_, v)| v.clone())
            })
            && let Ok(t) = hit.downcast::<T>()
        {
            return Ok(t);
        }
        let built = Arc::new(build(text)?);
        if let Ok(mut c) = self.parsed.lock() {
            put_entry(
                &mut c,
                text,
                (kind.to_string(), built.clone() as ParsedSchema),
                |(k, _)| k == kind,
            );
        }
        Ok(built)
    }

    /// The value a codec of `kind` built for `schema_id` and stored with
    /// [`remember_compiled`](Self::remember_compiled), if still cached.
    pub fn compiled_for<T>(&self, kind: &'static str, schema_id: u32) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        let hit = self
            .compiled
            .lock()
            .ok()
            .and_then(|mut c| c.get(&(kind, schema_id)).cloned())?;
        hit.downcast::<T>().ok()
    }

    /// Store what a codec of `kind` built for `schema_id` (with every
    /// reference resolved), so later messages with that id reuse it.
    pub fn remember_compiled<T>(&self, kind: &'static str, schema_id: u32, value: Arc<T>)
    where
        T: Send + Sync + 'static,
    {
        if let Ok(mut c) = self.compiled.lock() {
            c.put(
                (kind, schema_id),
                value as Arc<dyn std::any::Any + Send + Sync>,
            );
        }
    }

    fn apply_auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.auth {
            Some(a) => req.basic_auth(&a.username, Some(&a.password)),
            None => req,
        }
    }

    /// Fetch the schema by ID, consulting the LRU first.
    pub async fn get_schema(&self, schema_id: u32) -> Result<RegistrySchema, FaucetError> {
        {
            let mut cache = self.cache.lock().await;
            if let Some(hit) = cache.get(&schema_id) {
                return Ok(hit.clone());
            }
        }

        let url = format!("{}/schemas/ids/{schema_id}", self.base_url);
        let schema: RegistrySchema = self
            .calls
            .call_once("schema", async {
                let resp = self
                    .apply_auth(self.http.get(&url))
                    .send()
                    .await
                    .map_err(FaucetError::Http)?;
                if !resp.status().is_success() {
                    return Err(FaucetError::Source(format!(
                        "schema registry GET {url} returned {}",
                        resp.status()
                    )));
                }
                resp.json()
                    .await
                    .map_err(|e| FaucetError::Source(format!("schema registry JSON decode: {e}")))
            })
            .await?;

        let mut cache = self.cache.lock().await;
        cache.put(schema_id, schema.clone());
        Ok(schema)
    }

    /// Every schema `schema` references, transitively, each named by the
    /// reference name the referring schema uses; a schema comes after those it
    /// references. A cycle or more than 256 references is an error.
    pub async fn references(
        &self,
        schema: &RegistrySchema,
    ) -> Result<Vec<(String, RegistrySchema)>, FaucetError> {
        let mut out: Vec<(String, RegistrySchema)> = Vec::new();
        let mut done: std::collections::HashSet<(String, i32)> = Default::default();
        let mut path: Vec<(String, i32)> = Vec::new();
        for r in &schema.references {
            Box::pin(self.collect_reference(r, &mut out, &mut done, &mut path)).await?;
        }
        Ok(out)
    }

    async fn collect_reference(
        &self,
        r: &SchemaReference,
        out: &mut Vec<(String, RegistrySchema)>,
        done: &mut std::collections::HashSet<(String, i32)>,
        path: &mut Vec<(String, i32)>,
    ) -> Result<(), FaucetError> {
        let key = (r.subject.clone(), r.version);
        if done.contains(&key) {
            return Ok(());
        }
        if path.contains(&key) {
            return Err(FaucetError::Source(format!(
                "schema registry: schema references form a cycle through {} version {}",
                r.subject, r.version
            )));
        }
        if done.len() >= MAX_REFERENCES {
            return Err(FaucetError::Source(format!(
                "schema registry: a schema references more than {MAX_REFERENCES} schemas"
            )));
        }
        let referenced = self.get_subject_version(&r.subject, r.version).await?;
        path.push(key.clone());
        for inner in &referenced.references {
            Box::pin(self.collect_reference(inner, out, done, path)).await?;
        }
        path.pop();
        done.insert(key);
        out.push((r.name.clone(), referenced));
        Ok(())
    }

    /// Fetch the schema registered under `subject` at `version`.
    pub async fn get_subject_version(
        &self,
        subject: &str,
        version: i32,
    ) -> Result<RegistrySchema, FaucetError> {
        let key = (subject.to_string(), version);
        if let Some(hit) = self.versions.lock().await.get(&key) {
            return Ok(hit.clone());
        }
        let url = format!(
            "{}/subjects/{}/versions/{version}",
            self.base_url,
            faucet_core::encode_url_component(subject)
        );
        let schema: RegistrySchema = self
            .calls
            .call_once("schema", async {
                let resp = self
                    .apply_auth(self.http.get(&url))
                    .send()
                    .await
                    .map_err(FaucetError::Http)?;
                if !resp.status().is_success() {
                    return Err(FaucetError::Source(format!(
                        "schema registry GET {url} returned {}",
                        resp.status()
                    )));
                }
                resp.json()
                    .await
                    .map_err(|e| FaucetError::Source(format!("schema registry JSON decode: {e}")))
            })
            .await?;
        self.versions.lock().await.put(key, schema.clone());
        Ok(schema)
    }

    /// Register a schema under `subject`, returning the registry-assigned ID.
    pub async fn register_schema(
        &self,
        subject: &str,
        schema_type: &str,
        schema_text: &str,
    ) -> Result<u32, FaucetError> {
        // The registry's register endpoint is idempotent, but POSTing on every
        // record is a severe throughput ceiling — cache the assigned id keyed
        // by the full (subject, type, schema) tuple (#78/#30).
        let same = |(t, text, _): &(String, String, u32)| t == schema_type && text == schema_text;
        {
            let mut cache = self.register_cache.lock().await;
            if let Some(&(_, _, id)) = entry_of(&mut cache, subject, same) {
                return Ok(id);
            }
        }

        let url = format!(
            "{}/subjects/{}/versions",
            self.base_url,
            faucet_core::encode_url_component(subject)
        );
        let body = serde_json::json!({
            "schemaType": schema_type,
            "schema": schema_text,
        });
        #[derive(Deserialize)]
        struct RegisterResp {
            id: u32,
        }
        let parsed: RegisterResp = self
            .calls
            .call_once("register", async {
                let resp = self
                    .apply_auth(
                        self.http
                            .post(&url)
                            .header("Content-Type", "application/vnd.schemaregistry.v1+json")
                            .json(&body),
                    )
                    .send()
                    .await
                    .map_err(FaucetError::Http)?;
                if !resp.status().is_success() {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    return Err(FaucetError::Sink(format!(
                        "schema registry POST {url} returned {status}: {body}"
                    )));
                }
                resp.json().await.map_err(|e| {
                    FaucetError::Sink(format!("schema registry register JSON decode: {e}"))
                })
            })
            .await?;
        put_entry(
            &mut *self.register_cache.lock().await,
            subject,
            (schema_type.to_string(), schema_text.to_string(), parsed.id),
            same,
        );
        Ok(parsed.id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SchemaRegistryConfig;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[test]
    fn entries_are_found_by_both_key_parts_and_capped_per_key() {
        let mut cache: LruCache<String, Vec<(String, u32)>> =
            LruCache::new(NonZeroUsize::new(4).unwrap());
        put_entry(&mut cache, "s", ("a".into(), 1), |(k, _)| k == "a");
        put_entry(&mut cache, "s", ("b".into(), 2), |(k, _)| k == "b");
        put_entry(&mut cache, "s", ("a".into(), 3), |(k, _)| k == "a");
        assert_eq!(
            entry_of(&mut cache, "s", |(k, _)| k == "a"),
            Some(&("a".into(), 3))
        );
        assert_eq!(
            entry_of(&mut cache, "s", |(k, _)| k == "b"),
            Some(&("b".into(), 2))
        );
        assert_eq!(entry_of(&mut cache, "t", |(k, _)| k == "a"), None);
        assert_eq!(cache.get("s").unwrap().len(), 2);
        for i in 0..ENTRIES_PER_KEY as u32 + 3 {
            let k = format!("k{i}");
            put_entry(&mut cache, "s", (k.clone(), i), |(e, _)| *e == k);
        }
        let kept = cache.get("s").unwrap();
        assert_eq!(kept.len(), ENTRIES_PER_KEY);
        assert_eq!(kept.last().unwrap().1, ENTRIES_PER_KEY as u32 + 2);
        assert!(entry_of(&mut cache, "s", |(k, _)| k == "a").is_none());
    }

    #[test]
    fn parsed_schemas_are_kept_per_codec_kind() {
        let client =
            SchemaRegistryClient::new(&SchemaRegistryConfig::new("http://127.0.0.1:1")).unwrap();
        let builds = std::sync::atomic::AtomicUsize::new(0);
        let build = |t: &str| {
            builds.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok::<String, FaucetError>(t.to_uppercase())
        };
        let a = client.parsed("avro", "text", build).unwrap();
        let b = client.parsed("avro", "text", build).unwrap();
        let c = client.parsed("protobuf", "text", build).unwrap();
        assert!(Arc::ptr_eq(&a, &b));
        assert!(!Arc::ptr_eq(&a, &c));
        assert_eq!(*c, "TEXT");
        assert_eq!(builds.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn register_schema_keeps_one_id_per_type_and_text() {
        use wiremock::matchers::body_partial_json;
        let server = MockServer::start().await;
        for (schema, id) in [("\"string\"", 1), ("\"long\"", 2)] {
            Mock::given(method("POST"))
                .and(path("/subjects/s-value/versions"))
                .and(body_partial_json(
                    serde_json::json!({"schema": schema, "schemaType": "AVRO"}),
                ))
                .respond_with(
                    ResponseTemplate::new(200).set_body_json(serde_json::json!({"id": id})),
                )
                .expect(1)
                .mount(&server)
                .await;
        }
        Mock::given(method("POST"))
            .and(path("/subjects/s-value/versions"))
            .and(body_partial_json(serde_json::json!({"schemaType": "JSON"})))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"id": 3})))
            .expect(1)
            .mount(&server)
            .await;
        let client = SchemaRegistryClient::new(&SchemaRegistryConfig::new(server.uri())).unwrap();
        for _ in 0..3 {
            assert_eq!(
                client
                    .register_schema("s-value", "AVRO", "\"string\"")
                    .await
                    .unwrap(),
                1
            );
            assert_eq!(
                client
                    .register_schema("s-value", "AVRO", "\"long\"")
                    .await
                    .unwrap(),
                2
            );
            assert_eq!(
                client
                    .register_schema("s-value", "JSON", "\"string\"")
                    .await
                    .unwrap(),
                3
            );
        }
        drop(server);
    }

    #[tokio::test]
    async fn get_schema_caches_after_first_fetch() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/schemas/ids/1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "schema": "{\"type\":\"string\"}",
                "schemaType": "AVRO",
            })))
            .expect(1) // exactly one network call
            .mount(&server)
            .await;

        let client = SchemaRegistryClient::new(&SchemaRegistryConfig::new(server.uri())).unwrap();
        let first = client.get_schema(1).await.unwrap();
        let second = client.get_schema(1).await.unwrap();
        assert_eq!(first.schema, second.schema);
    }

    #[tokio::test]
    async fn a_hung_registry_request_ends_at_the_call_deadline() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/schemas/ids/7"))
            .respond_with(ResponseTemplate::new(200).set_delay(std::time::Duration::from_secs(30)))
            .mount(&server)
            .await;
        let client = SchemaRegistryClient::new(&SchemaRegistryConfig::new(server.uri())).unwrap();
        client
            .set_call_context(faucet_core::CallContext::new().with_deadline(
                tokio::time::Instant::now() + std::time::Duration::from_millis(200),
            ));
        let started = std::time::Instant::now();
        let err = client.get_schema(7).await.unwrap_err();
        assert!(matches!(err, FaucetError::Timeout { .. }), "{err:?}");
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
    }

    #[tokio::test]
    async fn get_schema_returns_error_on_404() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/schemas/ids/99"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        let client = SchemaRegistryClient::new(&SchemaRegistryConfig::new(server.uri())).unwrap();
        assert!(client.get_schema(99).await.is_err());
    }

    #[tokio::test]
    async fn register_schema_returns_id() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/subjects/test-value/versions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"id": 7})))
            .mount(&server)
            .await;
        let client = SchemaRegistryClient::new(&SchemaRegistryConfig::new(server.uri())).unwrap();
        let id = client
            .register_schema("test-value", "AVRO", "\"string\"")
            .await
            .unwrap();
        assert_eq!(id, 7);
    }

    #[tokio::test]
    async fn register_schema_caches_after_first_post() {
        // Regression for #78/#30: registering the same (subject, schema) must
        // hit the registry only once, not once per produced record.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/subjects/test-value/versions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"id": 7})))
            .expect(1) // exactly one network call across repeated registrations
            .mount(&server)
            .await;
        let client = SchemaRegistryClient::new(&SchemaRegistryConfig::new(server.uri())).unwrap();
        for _ in 0..5 {
            let id = client
                .register_schema("test-value", "AVRO", "\"string\"")
                .await
                .unwrap();
            assert_eq!(id, 7);
        }
        // wiremock asserts expect(1) on drop.
        drop(server);
    }

    #[test]
    fn validate_accepts_http_url() {
        let c = SchemaRegistryConfig::new("http://localhost:8081");
        assert!(c.validate().is_ok());
    }

    #[test]
    fn validate_rejects_non_http_scheme() {
        let mut c = SchemaRegistryConfig::new("ftp://localhost");
        c.cache_capacity = 1024;
        c.request_timeout = std::time::Duration::from_secs(10);
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_rejects_zero_cache_capacity() {
        let mut c = SchemaRegistryConfig::new("http://localhost");
        c.cache_capacity = 0;
        assert!(c.validate().is_err());
    }
}
