//! The SQS `Source` implementation: long-poll `ReceiveMessage`, buffer to
//! `batch_size`, delete each page's receipt handles **after** the page has been
//! written downstream, and terminate on `idle_timeout_secs` / `max_messages`.
//!
//! ## Why deletion happens after the yield
//!
//! In an `async_stream` generator, statements before a `yield` run *before* the
//! consumer sees the page; statements after it resume only once the consumer has
//! come back for the next one — i.e. after the pipeline wrote the page to the
//! sink and persisted it. Deleting on the near side of the `yield` would destroy
//! the messages before anything durable had happened, so a sink error, an abort,
//! or a crash would lose them permanently: at-most-once.
//!
//! So each page's receipt handles are parked in `pending` and deleted at the top
//! of the following iteration (and once more after the final page resumes). A
//! failure anywhere in between simply means the deletes never happen and SQS
//! redelivers after the visibility timeout — at-least-once, as documented. This
//! mirrors the Pub/Sub and NATS sources, which ack the same way (#456 C1).

use crate::config::{MAX_RECEIVE_BATCH, SqsSourceConfig};
use aws_sdk_sqs::Client;
use aws_sdk_sqs::types::{
    ChangeMessageVisibilityBatchRequestEntry, DeleteMessageBatchRequestEntry,
};
use faucet_core::lease::LeaseExtender;
use faucet_core::{FaucetError, Stream, StreamPage};
use serde_json::Value;
use std::pin::Pin;
use std::time::{Duration, Instant};

/// AWS SQS source. See the crate README for semantics.
pub struct SqsSource {
    config: SqsSourceConfig,
    client: Client,
}

/// Decode one SQS message body: the parsed JSON value if the body is valid
/// JSON, otherwise the raw body wrapped as a JSON string. Pure.
pub(crate) fn decode_body(body: &str) -> Value {
    serde_json::from_str::<Value>(body).unwrap_or_else(|_| Value::String(body.to_string()))
}

/// The informational bookmark every page carries. The broker, not faucet,
/// tracks the queue position; the bookmark exists so the pipeline flushes the
/// sink before resuming the generator, which is when the page's messages are
/// deleted (MSG-07). Pure.
pub(crate) fn page_bookmark(queue_url: &str, consumed: usize) -> Value {
    serde_json::json!({ "queue": queue_url, "consumed": consumed })
}

/// Every receipt handle the source currently holds un-deleted: the page being
/// assembled plus the yielded pages awaiting deletion. Pure.
pub(crate) fn held_handles(assembling: &[Option<String>], yielded: &[String]) -> Vec<String> {
    assembling
        .iter()
        .flatten()
        .cloned()
        .chain(yielded.iter().cloned())
        .collect()
}

/// Renew the visibility timeout of `handles`, chunked to the 10-entry API cap.
/// Best-effort: a failure is logged and the message may be redelivered.
async fn extend_visibility(client: Client, queue_url: String, handles: Vec<String>, secs: i32) {
    for chunk in handles.chunks(MAX_RECEIVE_BATCH as usize) {
        let entries: Result<Vec<_>, _> = chunk
            .iter()
            .enumerate()
            .map(|(i, rh)| {
                ChangeMessageVisibilityBatchRequestEntry::builder()
                    .id(i.to_string())
                    .receipt_handle(rh)
                    .visibility_timeout(secs)
                    .build()
            })
            .collect();
        let Ok(entries) = entries else { continue };
        if let Err(e) = client
            .change_message_visibility_batch()
            .queue_url(&queue_url)
            .set_entries(Some(entries))
            .send()
            .await
        {
            tracing::warn!(
                queue = %queue_url,
                error = %e.into_service_error(),
                "sqs: visibility renewal failed; held messages may be redelivered"
            );
        }
    }
}

impl SqsSource {
    /// Create a new SQS source. Validates the config and builds the AWS client;
    /// no queue I/O happens until the first `ReceiveMessage` at stream time
    /// (construction is offline).
    pub async fn new(config: SqsSourceConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let client = faucet_common_sqs::build_client(
            config.region.as_deref(),
            config.endpoint_url.as_deref(),
            &config.credentials,
        )
        .await?;
        Ok(Self { config, client })
    }

    /// Delete a page's receipt handles, chunked to the 10-entry API cap. A
    /// whole-request failure propagates as a typed error; per-entry failures
    /// are logged and left for SQS to redeliver (at-least-once).
    async fn delete_handles(&self, handles: &[String]) -> Result<(), FaucetError> {
        for chunk in handles.chunks(MAX_RECEIVE_BATCH as usize) {
            let entries: Vec<DeleteMessageBatchRequestEntry> = chunk
                .iter()
                .enumerate()
                .map(|(i, rh)| {
                    DeleteMessageBatchRequestEntry::builder()
                        .id(i.to_string())
                        .receipt_handle(rh)
                        .build()
                        .map_err(|e| {
                            FaucetError::Source(format!("sqs: delete entry build failed: {e}"))
                        })
                })
                .collect::<Result<_, _>>()?;
            let out = self
                .client
                .delete_message_batch()
                .queue_url(&self.config.queue_url)
                .set_entries(Some(entries))
                .send()
                .await
                .map_err(|e| {
                    FaucetError::Source(format!(
                        "sqs: DeleteMessageBatch on '{}' failed: {}",
                        self.config.queue_url,
                        e.into_service_error()
                    ))
                })?;
            for f in out.failed() {
                tracing::warn!(
                    queue = %self.config.queue_url,
                    id = f.id(),
                    code = f.code(),
                    "sqs: message delete failed; message will be redelivered"
                );
            }
        }
        Ok(())
    }
}

#[faucet_core::async_trait]
impl faucet_core::Source for SqsSource {
    /// Drain the queue to termination (`idle_timeout_secs` / `max_messages` —
    /// at least one is enforced at construction) and return every message.
    async fn fetch_with_context(
        &self,
        context: &std::collections::HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        use futures::StreamExt;
        let mut pages = self.stream_pages(context, self.config.batch_size);
        let mut all = Vec::new();
        while let Some(page) = pages.next().await {
            all.extend(page?.records);
        }
        Ok(all)
    }

    fn stream_pages<'a>(
        &'a self,
        _context: &'a std::collections::HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let batch_size = self.config.batch_size;
        let chunk = if batch_size == 0 {
            usize::MAX
        } else {
            batch_size
        };

        Box::pin(async_stream::try_stream! {
            let idle = self.config.idle_timeout_secs.map(Duration::from_secs);
            let max = self.config.max_messages;
            // `buffer` and `handles` stay index-aligned: one handle slot per
            // buffered record (None if a message somehow lacked a receipt
            // handle — such a record is emitted but not deleted, so SQS
            // redelivers it, preserving at-least-once).
            let mut buffer: Vec<Value> = Vec::new();
            let mut handles: Vec<Option<String>> = Vec::new();
            // Receipt handles of pages already yielded but not yet deleted. Drained
            // at the top of the next iteration — by then the consumer has resumed
            // us, so the page is durable downstream (see the module docs).
            let mut pending: Vec<String> = Vec::new();
            let mut total = 0usize;
            let mut last_activity = Instant::now();
            let renew_secs = self.config.visibility_extension_secs;
            let lease = (renew_secs > 0).then(|| {
                let client = self.client.clone();
                let queue = self.config.queue_url.clone();
                LeaseExtender::spawn(
                    Duration::from_secs(u64::from(renew_secs / 3).max(1)),
                    move |handles: Vec<String>| {
                        extend_visibility(client.clone(), queue.clone(), handles, renew_secs as i32)
                    },
                )
            });

            loop {
                if !pending.is_empty() {
                    self.delete_handles(&std::mem::take(&mut pending)).await?;
                    if let Some(l) = &lease {
                        l.hold(held_handles(&handles, &pending));
                    }
                }

                // Reached the message cap → stop.
                let remaining = match max {
                    Some(m) if total >= m => break,
                    Some(m) => Some(m - total),
                    None => None,
                };
                let want = remaining
                    .map_or(MAX_RECEIVE_BATCH, |r| r.min(MAX_RECEIVE_BATCH as usize) as i32);

                let resp = self
                    .client
                    .receive_message()
                    .queue_url(&self.config.queue_url)
                    .max_number_of_messages(want)
                    .wait_time_seconds(self.config.wait_time_seconds)
                    .send()
                    .await
                    .map_err(|e| {
                        FaucetError::Source(format!(
                            "sqs: ReceiveMessage on '{}' failed: {}",
                            self.config.queue_url,
                            e.into_service_error()
                        ))
                    })?;

                let messages = resp.messages();
                if messages.is_empty() {
                    if let Some(window) = idle
                        && last_activity.elapsed() >= window
                    {
                        tracing::info!(
                            queue = %self.config.queue_url,
                            idle_secs = window.as_secs(),
                            "sqs: idle termination reached"
                        );
                        break;
                    }
                    continue;
                }
                last_activity = Instant::now();

                for msg in messages {
                    buffer.push(decode_body(msg.body().unwrap_or("")));
                    handles.push(msg.receipt_handle().map(str::to_string));
                    total += 1;
                }
                if let Some(l) = &lease {
                    l.hold(held_handles(&handles, &pending));
                }

                // Emit every full page. Its handles are parked, not deleted: the
                // page is not durable until the consumer resumes us. The page
                // carries a bookmark so the pipeline flushes the sink before it
                // resumes us — otherwise a buffering sink could still hold the
                // records when they are deleted.
                while buffer.len() >= chunk {
                    let page: Vec<Value> = buffer.drain(..chunk).collect();
                    let to_delete: Vec<String> =
                        handles.drain(..chunk).flatten().collect();
                    pending.extend(to_delete);
                    yield StreamPage {
                        records: page,
                        bookmark: Some(page_bookmark(&self.config.queue_url, total)),
                    };
                }

                if let Some(m) = max
                    && total >= m
                {
                    tracing::info!(
                        queue = %self.config.queue_url,
                        max = m,
                        "sqs: max_messages reached"
                    );
                    break;
                }
            }

            // Flush whatever is left as a final (short) page, then delete the
            // last two pages' handles: `pending` (yielded in the loop above) and
            // this page's, which is durable once the consumer resumes us. If the
            // consumer stops polling instead, nothing is deleted and SQS
            // redelivers — the safe direction.
            if !buffer.is_empty() {
                let to_delete: Vec<String> = handles.into_iter().flatten().collect();
                pending.extend(to_delete);
                yield StreamPage {
                    records: buffer,
                    bookmark: Some(page_bookmark(&self.config.queue_url, total)),
                };
            }
            if !pending.is_empty() {
                self.delete_handles(&pending).await?;
            }
            drop(lease);
            tracing::info!(
                queue = %self.config.queue_url,
                records = total,
                "sqs source stream complete"
            );
        })
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(SqsSourceConfig))
            .expect("schema serialization")
    }

    fn consumes_destructively(&self) -> bool {
        true
    }

    fn connector_name(&self) -> &'static str {
        "sqs"
    }

    fn dataset_uri(&self) -> String {
        let name = self
            .config
            .queue_url
            .rsplit('/')
            .find(|s| !s.is_empty())
            .unwrap_or(self.config.queue_url.as_str());
        format!(
            "sqs://{}/{}",
            self.config.region.as_deref().unwrap_or("default"),
            name
        )
    }

    /// Side-effect-free probe: `GetQueueAttributes` (no messages consumed). The
    /// default first-page probe could block for the full long-poll window on a
    /// quiet queue.
    async fn check(
        &self,
        ctx: &faucet_core::CheckContext,
    ) -> Result<faucet_core::CheckReport, FaucetError> {
        use faucet_core::{CheckReport, Probe};
        let start = std::time::Instant::now();
        let fut = self
            .client
            .get_queue_attributes()
            .queue_url(&self.config.queue_url)
            .send();
        let probe = match tokio::time::timeout(ctx.timeout, fut).await {
            Err(_) => Probe::fail("get_queue_attributes", start.elapsed(), "timed out"),
            Ok(Ok(_)) => Probe::pass("get_queue_attributes", start.elapsed()),
            Ok(Err(e)) => Probe::fail(
                "get_queue_attributes",
                start.elapsed(),
                e.into_service_error().to_string(),
            ),
        };
        Ok(CheckReport::single(probe))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::Source as _;

    fn decode(s: &str) -> Value {
        decode_body(s)
    }

    #[test]
    fn decode_body_parses_json_else_string() {
        assert_eq!(decode(r#"{"a":1}"#), serde_json::json!({"a": 1}));
        assert_eq!(decode("[1,2,3]"), serde_json::json!([1, 2, 3]));
        assert_eq!(decode("not json"), Value::String("not json".into()));
        assert_eq!(decode(""), Value::String(String::new()));
    }

    async fn offline_source(mut config: SqsSourceConfig) -> SqsSource {
        config.endpoint_url = Some("http://127.0.0.1:1".into()); // unroutable
        config.region = Some("us-east-1".into());
        config.credentials = faucet_common_sqs::SqsCredentials::AccessKey {
            access_key_id: "test".into(),
            secret_access_key: "test".into(),
            session_token: None,
        };
        SqsSource::new(config).await.expect("source builds")
    }

    #[tokio::test]
    async fn new_validates_config() {
        let err = match SqsSource::new(SqsSourceConfig::new("https://q")).await {
            Err(e) => e,
            Ok(_) => panic!("config without a termination knob must be rejected"),
        };
        assert!(err.to_string().contains("idle_timeout_secs"), "{err}");
    }

    #[tokio::test]
    async fn identity_overrides() {
        let mut cfg = SqsSourceConfig::new("https://sqs.us-east-1.amazonaws.com/1/events");
        cfg.max_messages = Some(10);
        let source = offline_source(cfg).await;
        assert_eq!(source.connector_name(), "sqs");
        assert_eq!(source.dataset_uri(), "sqs://us-east-1/events");
        assert_eq!(source.state_key(), None);
        assert!(!source.supports_exactly_once());
        let schema = source.config_schema();
        assert!(
            schema["properties"]["queue_url"].is_object(),
            "schema exposes config fields"
        );
    }

    #[tokio::test]
    async fn stream_pages_surfaces_receive_errors() {
        use futures::StreamExt;
        let mut cfg = SqsSourceConfig::new("https://q");
        cfg.max_messages = Some(10);
        cfg.wait_time_seconds = 0;
        let source = offline_source(cfg).await;
        let ctx = std::collections::HashMap::new();
        let mut pages = source.stream_pages(&ctx, 10);
        let first = pages.next().await.expect("one item");
        let err = first.unwrap_err();
        assert!(err.to_string().contains("ReceiveMessage"), "{err}");
    }

    #[tokio::test]
    async fn check_probe_fails_cleanly_offline() {
        let mut cfg = SqsSourceConfig::new("https://q");
        cfg.max_messages = Some(10);
        let source = offline_source(cfg).await;
        let report = source
            .check(&faucet_core::CheckContext {
                timeout: Duration::from_millis(500),
            })
            .await
            .unwrap();
        assert_eq!(
            report.failed_count(),
            1,
            "unreachable endpoint → fail probe"
        );
    }

    #[test]
    fn every_page_carries_an_informational_bookmark() {
        assert_eq!(
            page_bookmark("https://q", 4),
            serde_json::json!({"queue": "https://q", "consumed": 4})
        );
    }

    #[test]
    fn held_handles_cover_the_assembling_page_and_yielded_pages() {
        let assembling = vec![Some("a".to_string()), None, Some("b".to_string())];
        let yielded = vec!["c".to_string()];
        assert_eq!(held_handles(&assembling, &yielded), vec!["a", "b", "c"]);
        assert!(held_handles(&[], &[]).is_empty());
    }
}
