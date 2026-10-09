//! The acme sink: the one module that performs I/O.

use crate::config::AcmeSinkConfig;
use faucet_core::check::{CheckContext, CheckReport, Probe};
use faucet_core::{FaucetError, RowOutcome, Sink, Value, WriteMode, async_trait, plan_writes};
use std::time::{Duration, Instant};

const MAX_ERROR_BODY: usize = 512;
static WRITE_MODES: &[WriteMode] = &[WriteMode::Append, WriteMode::Upsert];

/// acme sink connector.
pub struct AcmeSink {
    config: AcmeSinkConfig,
    client: reqwest::Client,
    collection_url: reqwest::Url,
    bulk_url: reqwest::Url,
    upsert_key: String,
}

impl AcmeSink {
    /// Validates the config and builds the HTTP client once.
    pub fn new(config: AcmeSinkConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let collection_url = collection_url(&config.base_url, &config.collection)?;
        let mut bulk_url = collection_url.clone();
        bulk_url
            .path_segments_mut()
            .map_err(|_| FaucetError::Config("acme: base_url cannot take a path".into()))?
            .extend(["records", "bulk"]);
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.timeout_secs))
            .build()?;
        let upsert_key = config.write.key.join(",");
        Ok(Self {
            config,
            client,
            collection_url,
            bulk_url,
            upsert_key,
        })
    }

    /// Builds the sink from a raw `sink.config` value, as a custom `faucet`
    /// binary's `PluginRegistry` factory receives it.
    pub fn from_value(config: Value) -> Result<Self, FaucetError> {
        let config: AcmeSinkConfig = faucet_core::serde_json::from_value(config)
            .map_err(|e| FaucetError::Config(format!("acme: invalid config: {e}")))?;
        Self::new(config)
    }

    async fn post(&self, rows: &[Value]) -> Result<usize, FaucetError> {
        let mut req = self
            .client
            .post(self.bulk_url.clone())
            .bearer_auth(&self.config.token)
            .json(rows);
        if self.config.write.dedups_by_key() {
            req = req.query(&[("mode", "upsert"), ("key", self.upsert_key.as_str())]);
        }
        let resp = req.send().await?;
        let status = resp.status();
        if !status.is_success() {
            let url = resp.url().to_string();
            let body = resp.text().await.unwrap_or_default();
            return Err(FaucetError::sink_status(
                Some(status.as_u16()),
                url,
                truncate(body),
            ));
        }
        Ok(rows.len())
    }

    async fn send_all(&self, rows: &[Value]) -> Result<usize, FaucetError> {
        let mut written = 0;
        for chunk in rows.chunks(self.config.max_request_records) {
            written += self.post(chunk).await?;
        }
        Ok(written)
    }
}

#[async_trait]
impl Sink for AcmeSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }
        if !self.config.write.dedups_by_key() {
            return self.send_all(records).await;
        }
        let plan = plan_writes(records, &self.config.write);
        if let Some((row, reason)) = plan.failed.first() {
            return Err(FaucetError::Sink(format!(
                "acme: row {row} cannot be upserted: {reason}"
            )));
        }
        self.send_all(&plan.upserts).await?;
        Ok(records.len())
    }

    async fn write_batch_partial(&self, records: &[Value]) -> Result<Vec<RowOutcome>, FaucetError> {
        let mut outcomes: Vec<RowOutcome> = records.iter().map(|_| Ok(())).collect();
        if !self.config.write.dedups_by_key() {
            self.send_all(records).await?;
            return Ok(outcomes);
        }
        let plan = plan_writes(records, &self.config.write);
        self.send_all(&plan.upserts).await?;
        for (row, reason) in plan.failed {
            outcomes[row] = Err(FaucetError::Sink(format!("acme: {reason}")));
        }
        Ok(outcomes)
    }

    fn supported_write_modes(&self) -> &'static [WriteMode] {
        WRITE_MODES
    }

    fn dedups_by_key(&self) -> bool {
        self.config.write.dedups_by_key()
    }

    fn config_schema(&self) -> Value {
        faucet_core::serde_json::to_value(faucet_core::schema_for!(AcmeSinkConfig))
            .unwrap_or(Value::Null)
    }

    fn connector_name(&self) -> &'static str {
        "acme"
    }

    fn dataset_uri(&self) -> String {
        format!(
            "acme://{}/{}",
            self.collection_url.host_str().unwrap_or("unknown"),
            self.config.collection
        )
    }

    async fn check(&self, ctx: &CheckContext) -> Result<CheckReport, FaucetError> {
        let started = Instant::now();
        let result = self
            .client
            .get(self.collection_url.clone())
            .bearer_auth(&self.config.token)
            .timeout(ctx.timeout)
            .send()
            .await;
        let probe = match result {
            Ok(resp) if resp.status().is_success() => Probe::pass("connect", started.elapsed()),
            Ok(resp) => Probe::fail(
                "connect",
                started.elapsed(),
                format!(
                    "collection endpoint returned HTTP {}",
                    resp.status().as_u16()
                ),
            ),
            Err(e) => Probe::fail(
                "connect",
                started.elapsed(),
                format!("request failed: {}", e.without_url()),
            ),
        };
        Ok(CheckReport::single(probe))
    }
}

fn collection_url(base_url: &str, collection: &str) -> Result<reqwest::Url, FaucetError> {
    let mut url = reqwest::Url::parse(base_url)
        .map_err(|e| FaucetError::Config(format!("acme: invalid base_url: {e}")))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(FaucetError::Config("acme: base_url must be http(s)".into()));
    }
    url.path_segments_mut()
        .map_err(|_| FaucetError::Config("acme: base_url cannot take a path".into()))?
        .pop_if_empty()
        .extend(["collections", collection]);
    Ok(url)
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
    fn bulk_url_is_under_the_collection() {
        let sink = AcmeSink::new(AcmeSinkConfig::new(
            "https://a.example.com/api",
            "t",
            "orders",
        ))
        .unwrap();
        assert_eq!(
            sink.bulk_url.as_str(),
            "https://a.example.com/api/collections/orders/records/bulk"
        );
    }

    #[test]
    fn append_is_not_replay_safe_upsert_is() {
        let append = AcmeSink::new(AcmeSinkConfig::new("http://x", "t", "orders")).unwrap();
        assert!(!append.write_batch_is_replay_safe());
        assert!(!append.supports_idempotent_writes());

        let upsert =
            AcmeSink::new(AcmeSinkConfig::new("http://x", "t", "orders").upsert(&["id"])).unwrap();
        assert!(upsert.dedups_by_key());
        assert!(upsert.write_batch_is_replay_safe());
    }

    #[test]
    fn from_value_rejects_delete_marker() {
        let cfg = faucet_core::json!({
            "base_url": "http://x", "token": "t", "collection": "orders",
            "write_mode": "upsert", "key": ["id"],
            "delete_marker": {"field": "op", "values": ["d"]}
        });
        let err = AcmeSink::from_value(cfg).err().unwrap();
        assert!(matches!(err, FaucetError::Config(_)), "{err:?}");
    }

    #[tokio::test]
    async fn empty_batch_is_a_no_op() {
        let sink = AcmeSink::new(AcmeSinkConfig::new("http://127.0.0.1:9", "t", "orders")).unwrap();
        assert_eq!(sink.write_batch(&[]).await.unwrap(), 0);
    }
}
