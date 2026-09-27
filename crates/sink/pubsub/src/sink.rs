//! The Pub/Sub `Sink` implementation: encode → batched publish with bounded
//! concurrency → per-message partial-failure outcomes (DLQ-routable).
//!
//! **SDK-touching module.** All `gcloud-pubsub` calls live here (client
//! construction is in `faucet-common-pubsub`), so a real-compile fixup for a
//! differing SDK version is localised to `PubsubSink::new`, `publish_chunk`,
//! and `check`. The record→message logic (`encode_records`,
//! `assemble_row_outcomes`) is pure and unit-tested offline.

use crate::config::PubsubSinkConfig;
use crate::encode::{Prepared, prepare};
use faucet_common_pubsub::PubsubMessage;
use faucet_core::{FaucetError, RowOutcome};
use gcloud_pubsub::client::Client;
use gcloud_pubsub::publisher::{Publisher, PublisherConfig};
use serde_json::Value;
use std::collections::BTreeMap;

/// Google Cloud Pub/Sub sink. See the crate README for semantics.
pub struct PubsubSink {
    config: PubsubSinkConfig,
    client: Client,
    publisher: Publisher,
}

/// Encode every record; per-record failures (bad payload, unresolvable
/// ordering key, malformed attributes) land in the error map keyed by input
/// index. Pure.
pub(crate) fn encode_records(
    records: &[Value],
    config: &PubsubSinkConfig,
) -> (Vec<(usize, Prepared)>, BTreeMap<usize, FaucetError>) {
    let mut prepared = Vec::with_capacity(records.len());
    let mut failures = BTreeMap::new();
    for (index, record) in records.iter().enumerate() {
        match prepare(
            record,
            config.value_format,
            &config.ordering_key,
            config.attributes_field.as_deref(),
        ) {
            Ok(p) => prepared.push((index, p)),
            Err(e) => {
                failures.insert(index, e);
            }
        }
    }
    (prepared, failures)
}

/// Merge encode failures + publish outcomes into input-ordered per-row
/// results. Pure.
pub(crate) fn assemble_row_outcomes(
    len: usize,
    mut encode_failures: BTreeMap<usize, FaucetError>,
    mut shipped: BTreeMap<usize, Result<(), FaucetError>>,
) -> Vec<RowOutcome> {
    (0..len)
        .map(|i| {
            if let Some(err) = encode_failures.remove(&i) {
                Err(err)
            } else {
                shipped.remove(&i).unwrap_or(Ok(()))
            }
        })
        .collect()
}

impl PubsubSink {
    /// Create a new Pub/Sub sink. Validates the config, builds the client, and
    /// opens a reusable publisher. Ordered delivery is driven per-message: when
    /// an `ordering_key` strategy is configured, `encode` stamps each message's
    /// ordering key and the publisher sequences same-key messages automatically
    /// (the `gcloud-pubsub` publisher has no client-level ordering toggle).
    pub async fn new(config: PubsubSinkConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let client = faucet_common_pubsub::build_client(&config.connection).await?;
        let publisher = client
            .topic(&config.topic)
            .new_publisher(Some(PublisherConfig::default()));
        Ok(Self {
            config,
            client,
            publisher,
        })
    }

    /// Publish one chunk of prepared records concurrently, returning per-index
    /// outcomes.
    async fn publish_chunk(
        &self,
        chunk: Vec<(usize, Prepared)>,
    ) -> BTreeMap<usize, Result<(), FaucetError>> {
        use futures::StreamExt;
        // Enqueue every message; the publisher bundles them internally.
        let mut awaiters = Vec::with_capacity(chunk.len());
        for (index, p) in chunk {
            let msg = PubsubMessage {
                data: p.data,
                attributes: p.attributes,
                ordering_key: p.ordering_key,
                ..Default::default()
            };
            let awaiter = self.publisher.publish(msg).await;
            awaiters.push((index, awaiter));
        }
        let mut outcomes = BTreeMap::new();
        let mut stream =
            futures::stream::iter(awaiters.into_iter().map(|(index, awaiter)| async move {
                let result = awaiter
                    .get()
                    .await
                    .map(|_message_id| ())
                    .map_err(|e| FaucetError::Sink(format!("pubsub: publish failed: {e}")));
                (index, result)
            }))
            .buffer_unordered(self.config.concurrency);
        while let Some((index, result)) = stream.next().await {
            outcomes.insert(index, result);
        }
        outcomes
    }

    /// Publish all prepared records in `batch_size` chunks, merging outcomes.
    async fn publish_all(
        &self,
        prepared: Vec<(usize, Prepared)>,
    ) -> BTreeMap<usize, Result<(), FaucetError>> {
        let mut merged = BTreeMap::new();
        let mut iter = prepared.into_iter();
        loop {
            let chunk: Vec<_> = iter.by_ref().take(self.config.batch_size).collect();
            if chunk.is_empty() {
                break;
            }
            merged.extend(self.publish_chunk(chunk).await);
        }
        merged
    }
}

#[faucet_core::async_trait]
impl faucet_core::Sink for PubsubSink {
    fn batch_atomicity(&self) -> faucet_core::BatchAtomicity {
        self.config.batch_atomicity()
    }

    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }
        let (prepared, encode_failures) = encode_records(records, &self.config);
        let outcomes = self.publish_all(prepared).await;
        let delivered = outcomes.values().filter(|o| o.is_ok()).count();
        let failed = encode_failures.len() + (outcomes.len() - delivered);
        if failed > 0 {
            let first = encode_failures
                .values()
                .next()
                .map(ToString::to_string)
                .or_else(|| {
                    outcomes
                        .values()
                        .find_map(|o| o.as_ref().err().map(ToString::to_string))
                })
                .unwrap_or_default();
            return Err(FaucetError::Sink(format!(
                "pubsub: {failed} of {} record(s) failed to publish (first: {first})",
                records.len()
            )));
        }
        tracing::info!(
            topic = %self.config.topic,
            records = delivered,
            "pubsub sink write complete"
        );
        Ok(delivered)
    }

    /// Per-row outcomes in input order: encode failures and per-message publish
    /// rejections come back as `Err` rows for the DLQ router.
    async fn write_batch_partial(&self, records: &[Value]) -> Result<Vec<RowOutcome>, FaucetError> {
        let (prepared, encode_failures) = encode_records(records, &self.config);
        let shipped = self.publish_all(prepared).await;
        Ok(assemble_row_outcomes(
            records.len(),
            encode_failures,
            shipped,
        ))
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(PubsubSinkConfig))
            .expect("schema serialization")
    }

    fn connector_name(&self) -> &'static str {
        "pubsub"
    }

    fn dataset_uri(&self) -> String {
        format!(
            "pubsub://{}/topics/{}",
            self.config
                .connection
                .project_id
                .as_deref()
                .unwrap_or("default"),
            self.config.topic
        )
    }

    /// Side-effect-free probe: confirm the topic exists (no messages written).
    async fn check(
        &self,
        ctx: &faucet_core::CheckContext,
    ) -> Result<faucet_core::CheckReport, FaucetError> {
        use faucet_core::{CheckReport, Probe};
        let start = std::time::Instant::now();
        let topic = self.client.topic(&self.config.topic);
        let fut = topic.exists(None);
        let probe = match tokio::time::timeout(ctx.timeout, fut).await {
            Err(_) => Probe::fail("topic_exists", start.elapsed(), "timed out"),
            Ok(Ok(true)) => Probe::pass("topic_exists", start.elapsed()),
            Ok(Ok(false)) => Probe::fail(
                "topic_exists",
                start.elapsed(),
                format!("topic '{}' does not exist", self.config.topic),
            ),
            Ok(Err(e)) => Probe::fail("topic_exists", start.elapsed(), e.to_string()),
        };
        Ok(CheckReport::single(probe))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{OrderingKey, ValueFormat};
    use serde_json::json;

    #[test]
    fn encode_records_partitions_failures_by_index() {
        let mut cfg = PubsubSinkConfig::new("orders");
        cfg.ordering_key = OrderingKey::Field { name: "id".into() };
        let records = vec![
            json!({"id": "a", "v": 1}), // ok
            json!({"v": 2}),            // no ordering-key field
            json!({"id": "c", "v": 3}), // ok
        ];
        let (prepared, failures) = encode_records(&records, &cfg);
        assert_eq!(prepared.len(), 2);
        assert_eq!(prepared[0].0, 0);
        assert_eq!(prepared[1].0, 2);
        assert_eq!(failures.len(), 1);
        assert!(failures[&1].to_string().contains("id"), "{}", failures[&1]);
    }

    #[test]
    fn encode_records_string_format_failure_is_per_record() {
        let mut cfg = PubsubSinkConfig::new("orders");
        cfg.value_format = ValueFormat::String;
        let (prepared, failures) = encode_records(&[json!({"not": "a string"}), json!("ok")], &cfg);
        assert_eq!(prepared.len(), 1, "only the string record encodes");
        assert_eq!(prepared[0].0, 1);
        assert_eq!(failures.len(), 1);
        assert!(failures.contains_key(&0));
    }

    #[test]
    fn assemble_row_outcomes_interleaves_encode_and_publish_results() {
        let mut encode_failures = BTreeMap::new();
        encode_failures.insert(1usize, FaucetError::Sink("bad encode".into()));
        let mut shipped = BTreeMap::new();
        shipped.insert(0usize, Ok(()));
        shipped.insert(2usize, Err(FaucetError::Sink("publish rejected".into())));
        // index 3 absent from both → defaults to Ok

        let outcomes = assemble_row_outcomes(4, encode_failures, shipped);
        assert_eq!(outcomes.len(), 4);
        assert!(outcomes[0].is_ok());
        assert!(
            outcomes[1]
                .as_ref()
                .unwrap_err()
                .to_string()
                .contains("bad encode")
        );
        assert!(
            outcomes[2]
                .as_ref()
                .unwrap_err()
                .to_string()
                .contains("publish rejected")
        );
        assert!(outcomes[3].is_ok());
    }

    #[test]
    fn config_schema_exposes_fields() {
        let schema = serde_json::to_value(faucet_core::schema_for!(PubsubSinkConfig)).unwrap();
        assert!(schema["properties"]["topic"].is_object());
        assert!(schema["properties"]["ordering_key"].is_object());
    }

    #[tokio::test]
    async fn new_rejects_invalid_config() {
        // Validation runs before any client build → offline config error.
        let mut cfg = PubsubSinkConfig::new("orders");
        cfg.batch_size = 0;
        // `PubsubSink` holds a non-`Debug` SDK publisher, so `unwrap_err` (which
        // needs the `Ok` type to be `Debug`) is unavailable — match instead.
        let err = match PubsubSink::new(cfg).await {
            Ok(_) => panic!("expected a config error for batch_size = 0"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("batch_size"), "{err}");
    }
}
