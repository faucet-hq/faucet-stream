//! Elasticsearch source configuration.

use faucet_core::{AuthSpec, DEFAULT_BATCH_SIZE, FaucetError, validate_batch_size};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub use faucet_common_elasticsearch::ElasticsearchAuth;

/// Configuration for the Elasticsearch search source.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ElasticsearchSourceConfig {
    /// Base URL of the Elasticsearch cluster (e.g. `"http://localhost:9200"`).
    pub base_url: String,
    /// Index name to search.
    pub index: String,
    /// Elasticsearch query DSL. Defaults to `{"match_all": {}}`.
    pub query: Value,
    /// Scroll context timeout (e.g. `"1m"`). Defaults to `"1m"`.
    pub scroll_timeout: String,
    /// Authentication: either inline (`{ type, config }`) or a `{ ref: <name> }`
    /// pointer to a shared provider in the CLI's top-level `auth:` catalog.
    pub auth: AuthSpec<ElasticsearchAuth>,
    /// Maximum number of scroll pages to fetch. `None` means no limit.
    pub max_pages: Option<usize>,
    /// Records per emitted [`StreamPage`](faucet_core::StreamPage), which is
    /// also the `size` parameter passed to the Elasticsearch scroll API
    /// (`POST /{index}/_search?scroll={timeout}&size={batch_size}`). Each
    /// scroll response becomes exactly one `StreamPage`. Defaults to
    /// [`DEFAULT_BATCH_SIZE`].
    ///
    /// `batch_size = 0` is the "no batching" sentinel: the source issues a
    /// single non-scroll `_search` request with `size = 10_000` (the default
    /// `index.max_result_window`) and emits one `StreamPage`. Use it for
    /// small indices or for sinks (e.g. SQL `COPY`, BigQuery load jobs) that
    /// prefer one large request to many small ones. Indices that have raised
    /// their `max_result_window` will still cap at 10_000 — raise this knob
    /// or switch back to scroll if you need more.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

impl ElasticsearchSourceConfig {
    /// Create a new config with the required fields and sensible defaults.
    pub fn new(base_url: impl Into<String>, index: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            index: index.into(),
            query: json!({"match_all": {}}),
            scroll_timeout: "1m".to_string(),
            auth: AuthSpec::Inline(ElasticsearchAuth::None),
            max_pages: None,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    /// Set the Elasticsearch query DSL.
    pub fn query(mut self, q: Value) -> Self {
        self.query = q;
        self
    }

    /// Set the scroll context timeout (e.g. `"5m"`).
    pub fn scroll_timeout(mut self, t: impl Into<String>) -> Self {
        self.scroll_timeout = t.into();
        self
    }

    /// Set the authentication method.
    pub fn auth(mut self, a: ElasticsearchAuth) -> Self {
        self.auth = AuthSpec::Inline(a);
        self
    }

    /// Set the maximum number of scroll pages to fetch.
    pub fn max_pages(mut self, n: usize) -> Self {
        self.max_pages = Some(n);
        self
    }

    /// Set the per-page document count for both the scroll API's `size`
    /// parameter and the emitted [`StreamPage`](faucet_core::StreamPage)
    /// size.
    ///
    /// Pass `0` to opt out of scroll entirely — the source will issue a
    /// single `_search` with `size = 10_000` and emit one page.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Validate the config at load time so a bad config fails fast with a typed
    /// [`FaucetError::Config`] instead of surfacing deep in a run: rejects an
    /// out-of-range `batch_size` (`> MAX_BATCH_SIZE`) and an empty `base_url` or
    /// `index`.
    pub fn validate(&self) -> Result<(), FaucetError> {
        if self.base_url.trim().is_empty() {
            return Err(FaucetError::Config(
                "Elasticsearch source requires a non-empty `base_url`".into(),
            ));
        }
        if self.index.trim().is_empty() {
            return Err(FaucetError::Config(
                "Elasticsearch source requires a non-empty `index`".into(),
            ));
        }
        validate_batch_size(self.batch_size)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = ElasticsearchSourceConfig::new("http://localhost:9200", "my_index");
        assert_eq!(config.base_url, "http://localhost:9200");
        assert_eq!(config.index, "my_index");
        assert_eq!(config.query, json!({"match_all": {}}));
        assert_eq!(config.scroll_timeout, "1m");
        assert!(config.max_pages.is_none());
    }

    #[test]
    fn builder_methods() {
        let config = ElasticsearchSourceConfig::new("http://es:9200/", "idx")
            .query(json!({"term": {"status": "active"}}))
            .scroll_timeout("5m")
            .max_pages(10)
            .auth(ElasticsearchAuth::Bearer {
                token: "tok".into(),
            });
        assert_eq!(config.base_url, "http://es:9200");
        assert_eq!(config.scroll_timeout, "5m");
        assert_eq!(config.max_pages, Some(10));
    }

    #[test]
    fn batch_size_defaults_to_default_batch_size() {
        let config = ElasticsearchSourceConfig::new("http://localhost:9200", "idx");
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn with_batch_size_overrides_default() {
        let config =
            ElasticsearchSourceConfig::new("http://localhost:9200", "idx").with_batch_size(500);
        assert_eq!(config.batch_size, 500);
    }

    #[test]
    fn batch_size_zero_is_accepted_as_no_batching_sentinel() {
        let config =
            ElasticsearchSourceConfig::new("http://localhost:9200", "idx").with_batch_size(0);
        assert_eq!(config.batch_size, 0);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_ok());
    }

    #[test]
    fn batch_size_above_max_is_rejected_by_validate_batch_size() {
        let config = ElasticsearchSourceConfig::new("http://localhost:9200", "idx")
            .with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_err());
    }

    #[test]
    fn batch_size_deserializes_from_json() {
        let json = r#"{
            "base_url": "http://localhost:9200",
            "index": "idx",
            "query": {"match_all": {}},
            "scroll_timeout": "1m",
            "auth": {"type": "none"},
            "batch_size": 250
        }"#;
        let config: ElasticsearchSourceConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, 250);
        assert!(matches!(config.auth, faucet_core::AuthSpec::Inline(_)));
    }

    #[test]
    fn batch_size_defaults_when_missing_from_json() {
        let json = r#"{
            "base_url": "http://localhost:9200",
            "index": "idx",
            "query": {"match_all": {}},
            "scroll_timeout": "1m",
            "auth": {"type": "none"}
        }"#;
        let config: ElasticsearchSourceConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn validate_accepts_valid_config() {
        assert!(
            ElasticsearchSourceConfig::new("http://localhost:9200", "idx")
                .validate()
                .is_ok()
        );
    }

    #[test]
    fn validate_rejects_oversized_batch_size() {
        let config = ElasticsearchSourceConfig::new("http://localhost:9200", "idx")
            .with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(matches!(config.validate(), Err(FaucetError::Config(_))));
    }

    #[test]
    fn validate_rejects_empty_base_url() {
        assert!(matches!(
            ElasticsearchSourceConfig::new("  ", "idx").validate(),
            Err(FaucetError::Config(_))
        ));
    }

    #[test]
    fn validate_rejects_empty_index() {
        assert!(matches!(
            ElasticsearchSourceConfig::new("http://localhost:9200", "").validate(),
            Err(FaucetError::Config(_))
        ));
    }
}
