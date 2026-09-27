//! Elasticsearch sink configuration.

use faucet_core::{AuthSpec, DEFAULT_BATCH_SIZE};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub use faucet_common_elasticsearch::ElasticsearchAuth;

/// Deprecated alias retained for one minor release. Removed in `0.4.0`.
#[deprecated(since = "0.3.0", note = "renamed to `ElasticsearchAuth`")]
pub type ElasticsearchSinkAuth = ElasticsearchAuth;

/// Configuration for the Elasticsearch bulk index sink.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ElasticsearchSinkConfig {
    /// Base URL of the Elasticsearch cluster (e.g. `"http://localhost:9200"`).
    pub base_url: String,
    /// Target index name.
    pub index: String,
    /// Authentication: either inline (`{ type, config }`) or a `{ ref: <name> }`
    /// pointer to a shared provider in the CLI's top-level `auth:` catalog.
    pub auth: AuthSpec<ElasticsearchAuth>,
    /// Maximum documents per `_bulk` HTTP request. Defaults to
    /// [`DEFAULT_BATCH_SIZE`].
    ///
    /// When the upstream `StreamPage` carries more records than `batch_size`,
    /// the sink slices the page into `batch_size`-row chunks and issues one
    /// `POST /_bulk` per chunk. When `batch_size = 0`, the page is sent as a
    /// single bulk request — useful when the source already sizes pages for
    /// Elasticsearch's per-request sweet spot.
    ///
    /// `batch_size = 0` is the "no batching" sentinel: the entire upstream
    /// page is forwarded in one `_bulk` call. Elasticsearch's documented
    /// sweet spot for `_bulk` payloads is **5–15 MB** of NDJSON per request;
    /// the right document count depends on average document size. A
    /// reasonable starting range is **1000–5000 docs** for typical
    /// log/event payloads (~1–5 KB per document); lower it for fat
    /// documents (analytics aggregates, large nested objects), raise it
    /// for tiny ones.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Optional JSON field name to use as the document `_id`.
    ///
    /// If `None`, Elasticsearch auto-generates a fresh ID for every indexed
    /// document. Under faucet's at-least-once contract that means a **resumed or
    /// retried run re-indexes already-written records as duplicates** (a page
    /// can partially commit across `_bulk` chunks before failing, and the
    /// bookmark only advances once the whole page is written). Set `id_field` to
    /// a stable business key so re-sends become **idempotent overwrites** (the
    /// recommended setting for resumable pipelines); alternatively, configure a
    /// DLQ, whose per-row `write_batch_partial` path avoids the whole-page re-send.
    pub id_field: Option<String>,
    /// Write semantics: `append` (default), `upsert`, or `delete`.
    ///
    /// In `upsert` / `delete` modes the document `_id` is derived from the
    /// configured `key` columns (joined with `:` for composite keys) — this
    /// **supersedes `id_field`**. An `upsert` issues a `_bulk` `index` action
    /// (an idempotent overwrite by `_id`); a `delete` issues a `_bulk` `delete`
    /// action by `_id`. Duplicate keys within a single batch collapse
    /// last-write-wins.
    #[serde(flatten)]
    pub write: faucet_core::WriteSpec,
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

impl ElasticsearchSinkConfig {
    /// Create a new config with the required fields and sensible defaults.
    pub fn new(base_url: impl Into<String>, index: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            index: index.into(),
            auth: AuthSpec::Inline(ElasticsearchAuth::None),
            batch_size: DEFAULT_BATCH_SIZE,
            id_field: None,
            write: faucet_core::WriteSpec::default(),
        }
    }

    /// Set the authentication method.
    pub fn auth(mut self, a: ElasticsearchAuth) -> Self {
        self.auth = AuthSpec::Inline(a);
        self
    }

    /// Set the per-request document count for `POST /_bulk`.
    ///
    /// Pass `0` to opt out of re-chunking — the sink forwards each upstream
    /// [`StreamPage`](faucet_core::StreamPage) as a single bulk request.
    /// Elasticsearch's `_bulk` sweet spot is 5–15 MB of NDJSON per call
    /// (typically 1000–5000 documents); adjust based on average document
    /// size.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the JSON field to use as the document `_id`.
    pub fn id_field(mut self, field: impl Into<String>) -> Self {
        self.id_field = Some(field.into());
        self
    }
}

impl ElasticsearchSinkConfig {
    /// What a failed batch write leaves behind (#737): per-item outcomes come from one _bulk request per chunk; an outer error after the first chunk leaves earlier chunks indexed.
    pub fn batch_atomicity(&self) -> faucet_core::BatchAtomicity {
        if self.batch_size == 0 {
            faucet_core::BatchAtomicity::PerRow
        } else {
            faucet_core::BatchAtomicity::BestEffort
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = ElasticsearchSinkConfig::new("http://localhost:9200", "my_index");
        assert_eq!(config.base_url, "http://localhost:9200");
        assert_eq!(config.index, "my_index");
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
        assert!(config.id_field.is_none());
    }

    #[test]
    fn builder_methods() {
        let config = ElasticsearchSinkConfig::new("http://es:9200/", "idx")
            .with_batch_size(100)
            .id_field("doc_id")
            .auth(ElasticsearchAuth::Bearer {
                token: "tok".into(),
            });
        assert_eq!(config.base_url, "http://es:9200");
        assert_eq!(config.batch_size, 100);
        assert_eq!(config.id_field.as_deref(), Some("doc_id"));
    }

    #[test]
    fn batch_size_defaults_to_default_batch_size() {
        let config = ElasticsearchSinkConfig::new("http://localhost:9200", "idx");
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn with_batch_size_overrides_default() {
        let config =
            ElasticsearchSinkConfig::new("http://localhost:9200", "idx").with_batch_size(500);
        assert_eq!(config.batch_size, 500);
    }

    #[test]
    fn batch_size_zero_is_accepted_as_no_batching_sentinel() {
        let config =
            ElasticsearchSinkConfig::new("http://localhost:9200", "idx").with_batch_size(0);
        assert_eq!(config.batch_size, 0);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_ok());
    }

    #[test]
    fn batch_size_above_max_is_rejected_by_validate_batch_size() {
        let config = ElasticsearchSinkConfig::new("http://localhost:9200", "idx")
            .with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_err());
    }

    #[test]
    fn batch_size_deserializes_from_json() {
        let json = r#"{
            "base_url": "http://localhost:9200",
            "index": "idx",
            "auth": {"type": "none"},
            "batch_size": 2500,
            "id_field": null
        }"#;
        let config: ElasticsearchSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, 2500);
        assert!(matches!(config.auth, faucet_core::AuthSpec::Inline(_)));
    }

    #[test]
    fn batch_size_defaults_when_missing_from_json() {
        let json = r#"{
            "base_url": "http://localhost:9200",
            "index": "idx",
            "auth": {"type": "none"},
            "id_field": null
        }"#;
        let config: ElasticsearchSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    #[allow(deprecated)]
    fn deprecated_sink_auth_alias_is_canonical_type() {
        // Compile-time check: assignment proves the alias resolves to the
        // canonical `ElasticsearchAuth` type. Removed in 0.4.0 together with
        // the alias itself.
        let _: ElasticsearchSinkAuth = ElasticsearchAuth::None;
    }

    #[test]
    fn batch_atomicity_matches_the_write_path() {
        let c: ElasticsearchSinkConfig = serde_json::from_value(serde_json::json!({"base_url": "http://localhost:9200", "index": "idx", "auth": {"type": "none"}})).unwrap();
        assert_eq!(c.batch_atomicity(), faucet_core::BatchAtomicity::BestEffort);
        let c: ElasticsearchSinkConfig = serde_json::from_value(serde_json::json!({"base_url": "http://localhost:9200", "index": "idx", "auth": {"type": "none"}, "batch_size": 0})).unwrap();
        assert_eq!(c.batch_atomicity(), faucet_core::BatchAtomicity::PerRow);
    }
}
