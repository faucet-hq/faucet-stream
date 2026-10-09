//! The acme source: the one module that performs I/O.

use crate::config::AcmeSourceConfig;
use faucet_core::{
    FaucetError, Source, Stream, StreamPage, Value, async_stream, async_trait, execute_with_retry,
    json,
};
use serde::Deserialize;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Mutex;
use std::time::Duration;

const MAX_API_LIMIT: usize = 1000;
const RETRY_BASE: Duration = Duration::from_millis(500);
const DEFAULT_RETRY_AFTER: Duration = Duration::from_secs(60);
const MAX_ERROR_BODY: usize = 512;

#[derive(Deserialize)]
struct AcmePage {
    data: Vec<Value>,
}

/// acme source connector.
pub struct AcmeSource {
    config: AcmeSourceConfig,
    client: reqwest::Client,
    records_url: reqwest::Url,
    start_after: Mutex<Option<Value>>,
}

impl AcmeSource {
    /// Validates the config and builds the HTTP client once. The client pools
    /// connections, so every request in every run reuses it.
    pub fn new(config: AcmeSourceConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        faucet_core::state::validate_state_key(&config.state_key())
            .map_err(|e| FaucetError::Config(format!("acme: {e}")))?;
        let records_url = records_url(&config.base_url, &config.collection)?;
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.timeout_secs))
            .build()?;
        Ok(Self {
            config,
            client,
            records_url,
            start_after: Mutex::new(None),
        })
    }

    /// Builds the source from a raw `source.config` value, as a custom
    /// `faucet` binary's `PluginRegistry` factory receives it.
    pub fn from_value(config: Value) -> Result<Self, FaucetError> {
        let config: AcmeSourceConfig = faucet_core::serde_json::from_value(config)
            .map_err(|e| FaucetError::Config(format!("acme: invalid config: {e}")))?;
        Self::new(config)
    }

    fn start_cursor(&self) -> Result<Option<Value>, FaucetError> {
        self.start_after
            .lock()
            .map(|g| g.clone())
            .map_err(|_| FaucetError::Source("acme: cursor lock poisoned".into()))
    }

    async fn fetch_page(
        &self,
        after: Option<&Value>,
        limit: usize,
    ) -> Result<Vec<Value>, FaucetError> {
        execute_with_retry(self.config.max_retries, RETRY_BASE, || async move {
            let mut req = self
                .client
                .get(self.records_url.clone())
                .bearer_auth(&self.config.token)
                .query(&[("limit", limit.to_string())]);
            if let Some(after) = after {
                req = req.query(&[("after", cursor_param(after))]);
            }
            let resp = req.send().await?;
            let status = resp.status();
            if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                return Err(FaucetError::RateLimited(retry_after(&resp)));
            }
            if !status.is_success() {
                let url = resp.url().to_string();
                let body = resp.text().await.unwrap_or_default();
                return Err(FaucetError::HttpStatus {
                    status: status.as_u16(),
                    url,
                    body: truncate(body),
                });
            }
            let bytes = resp.bytes().await?;
            let page: AcmePage = faucet_core::serde_json::from_slice(&bytes)?;
            Ok(page.data)
        })
        .await
    }
}

#[async_trait]
impl Source for AcmeSource {
    async fn fetch_with_context(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        Ok(self.fetch_with_context_incremental(context).await?.0)
    }

    async fn fetch_with_context_incremental(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
        let mut pages = self.stream_pages(context, 0);
        let mut records = Vec::new();
        let mut bookmark = None;
        while let Some(page) = std::future::poll_fn(|cx| pages.as_mut().poll_next(cx)).await {
            let page = page?;
            records.extend(page.records);
            if page.bookmark.is_some() {
                bookmark = page.bookmark;
            }
        }
        Ok((records, bookmark))
    }

    fn stream_pages<'a>(
        &'a self,
        _context: &'a HashMap<String, Value>,
        batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let page_size = self.config.page_size.unwrap_or(batch_size);
        let single_page = page_size == 0;
        let limit = if single_page {
            MAX_API_LIMIT
        } else {
            page_size.min(MAX_API_LIMIT)
        };
        Box::pin(async_stream::try_stream! {
            let mut cursor = self.start_cursor()?;
            let mut all = Vec::new();
            loop {
                let records = self.fetch_page(cursor.as_ref(), limit).await?;
                let exhausted = records.len() < limit;
                if let Some(last) = records.last() {
                    cursor = Some(cursor_of(last, &self.config.cursor_field)?);
                }
                if single_page {
                    all.extend(records);
                } else if !records.is_empty() {
                    yield StreamPage { records, bookmark: cursor.as_ref().map(bookmark) };
                }
                if exhausted {
                    break;
                }
            }
            if single_page && !all.is_empty() {
                yield StreamPage { records: all, bookmark: cursor.as_ref().map(bookmark) };
            }
        })
    }

    fn state_key(&self) -> Option<String> {
        Some(self.config.state_key())
    }

    async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
        let after = bookmark.get("after").cloned().ok_or_else(|| {
            FaucetError::State(format!("acme: bookmark {bookmark} has no `after` field"))
        })?;
        *self
            .start_after
            .lock()
            .map_err(|_| FaucetError::Source("acme: cursor lock poisoned".into()))? = Some(after);
        Ok(())
    }

    fn config_schema(&self) -> Value {
        faucet_core::serde_json::to_value(faucet_core::schema_for!(AcmeSourceConfig))
            .unwrap_or(Value::Null)
    }

    fn connector_name(&self) -> &'static str {
        "acme"
    }

    fn dataset_uri(&self) -> String {
        format!(
            "acme://{}/{}",
            self.records_url.host_str().unwrap_or("unknown"),
            self.config.collection
        )
    }
}

fn records_url(base_url: &str, collection: &str) -> Result<reqwest::Url, FaucetError> {
    let mut url = reqwest::Url::parse(base_url)
        .map_err(|e| FaucetError::Config(format!("acme: invalid base_url: {e}")))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(FaucetError::Config("acme: base_url must be http(s)".into()));
    }
    url.path_segments_mut()
        .map_err(|_| FaucetError::Config("acme: base_url cannot take a path".into()))?
        .pop_if_empty()
        .extend(["collections", collection, "records"]);
    Ok(url)
}

fn cursor_of(record: &Value, field: &str) -> Result<Value, FaucetError> {
    record
        .get(field)
        .filter(|v| !v.is_null())
        .cloned()
        .ok_or_else(|| {
            FaucetError::Source(format!(
                "acme: record has no `{field}`; cannot advance the cursor"
            ))
        })
}

fn cursor_param(cursor: &Value) -> String {
    match cursor {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn bookmark(cursor: &Value) -> Value {
    json!({ "after": cursor })
}

fn retry_after(resp: &reqwest::Response) -> Duration {
    resp.headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_RETRY_AFTER)
}

fn truncate(body: String) -> String {
    if body.len() <= MAX_ERROR_BODY {
        body
    } else {
        body.chars().take(MAX_ERROR_BODY).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_url_appends_collection_path() {
        let url = records_url("https://acme.example.com/api/", "orders").unwrap();
        assert_eq!(
            url.as_str(),
            "https://acme.example.com/api/collections/orders/records"
        );
    }

    #[test]
    fn records_url_rejects_non_http() {
        assert!(matches!(
            records_url("ftp://acme.example.com", "orders"),
            Err(FaucetError::Config(_))
        ));
    }

    #[test]
    fn missing_cursor_field_is_an_error_not_a_skip() {
        let err = cursor_of(&json!({"name": "x"}), "id").unwrap_err();
        assert!(matches!(err, FaucetError::Source(_)), "{err:?}");
        assert!(cursor_of(&json!({"id": null}), "id").is_err());
    }

    #[test]
    fn cursor_param_does_not_quote_strings() {
        assert_eq!(cursor_param(&json!("a1")), "a1");
        assert_eq!(cursor_param(&json!(42)), "42");
    }

    #[tokio::test]
    async fn malformed_bookmark_is_a_state_error() {
        let source = AcmeSource::new(AcmeSourceConfig::new("http://x", "t", "orders")).unwrap();
        let err = source
            .apply_start_bookmark(json!({"cursor": 1}))
            .await
            .unwrap_err();
        assert!(matches!(err, FaucetError::State(_)), "{err:?}");
    }

    #[test]
    fn from_value_reports_bad_config_as_config_error() {
        let err = AcmeSource::from_value(json!({"base_url": "http://x"}))
            .err()
            .unwrap();
        assert!(matches!(err, FaucetError::Config(_)), "{err:?}");
        let ok = json!({"base_url": "http://x", "token": "t", "collection": "orders"});
        assert!(AcmeSource::from_value(ok).is_ok());
    }

    #[test]
    fn invalid_state_key_is_rejected_at_construction() {
        let mut cfg = AcmeSourceConfig::new("http://x", "t", "orders");
        cfg.state_key = Some("../escape".into());
        assert!(matches!(AcmeSource::new(cfg), Err(FaucetError::Config(_))));
    }
}
