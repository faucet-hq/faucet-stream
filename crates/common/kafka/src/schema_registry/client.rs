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
    #[serde(default = "default_schema_type")]
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
    /// subject + schema type + schema text.
    register_cache: Arc<Mutex<LruCache<String, u32>>>,
    /// Parsed/compiled schemas keyed by codec + schema text, so a codec
    /// parses an Avro schema, compiles a `.proto` or builds a JSON Schema
    /// validator once per schema rather than once per message (#789 MSG-65).
    parsed: Arc<std::sync::Mutex<LruCache<String, Arc<dyn std::any::Any + Send + Sync>>>>,
}

impl SchemaRegistryClient {
    pub fn new(config: &SchemaRegistryConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let http = reqwest::Client::builder()
            .timeout(config.request_timeout)
            .build()
            .map_err(|e| FaucetError::Config(format!("schema-registry HTTP client: {e}")))?;
        let capacity = NonZeroUsize::new(config.cache_capacity).unwrap();
        Ok(Self {
            http,
            base_url: config.url.trim_end_matches('/').to_string(),
            auth: config.auth.clone(),
            cache: Arc::new(Mutex::new(LruCache::new(capacity))),
            register_cache: Arc::new(Mutex::new(LruCache::new(capacity))),
            parsed: Arc::new(std::sync::Mutex::new(LruCache::new(capacity))),
        })
    }

    /// The parsed form of `text` for codec `kind`, built with `build` on the
    /// first request and shared afterwards. A failed build is not cached.
    pub fn parsed<T, F>(&self, kind: &str, text: &str, build: F) -> Result<Arc<T>, FaucetError>
    where
        T: Send + Sync + 'static,
        F: FnOnce(&str) -> Result<T, FaucetError>,
    {
        let key = format!("{kind}\u{0}{text}");
        if let Some(hit) = self
            .parsed
            .lock()
            .ok()
            .and_then(|mut c| c.get(&key).cloned())
            && let Ok(t) = hit.downcast::<T>()
        {
            return Ok(t);
        }
        let built = Arc::new(build(text)?);
        if let Ok(mut c) = self.parsed.lock() {
            c.put(key, built.clone() as Arc<dyn std::any::Any + Send + Sync>);
        }
        Ok(built)
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
        let schema: RegistrySchema = resp
            .json()
            .await
            .map_err(|e| FaucetError::Source(format!("schema registry JSON decode: {e}")))?;

        let mut cache = self.cache.lock().await;
        cache.put(schema_id, schema.clone());
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
        let cache_key = format!("{subject}\u{0}{schema_type}\u{0}{schema_text}");
        {
            let mut cache = self.register_cache.lock().await;
            if let Some(&id) = cache.get(&cache_key) {
                return Ok(id);
            }
        }

        let url = format!(
            "{}/subjects/{}/versions",
            self.base_url,
            urlencoding::encode(subject)
        );
        let body = serde_json::json!({
            "schemaType": schema_type,
            "schema": schema_text,
        });
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
        #[derive(Deserialize)]
        struct RegisterResp {
            id: u32,
        }
        let parsed: RegisterResp = resp
            .json()
            .await
            .map_err(|e| FaucetError::Sink(format!("schema registry register JSON decode: {e}")))?;
        self.register_cache.lock().await.put(cache_key, parsed.id);
        Ok(parsed.id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SchemaRegistryConfig;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

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
