//! The Pub/Sub `Source` implementation: streaming pull, per-message record
//! assembly, cumulative informational bookmark, ack **at durable page
//! boundaries**, and idle / max-messages termination.
//!
//! **SDK-touching module.** All `gcloud-pubsub` calls live here (client
//! construction is in `faucet-common-pubsub`), so a real-compile fixup for a
//! differing SDK version is localised to `pull_messages`, `ack_messages`,
//! `renew_deadlines`,
//! `subscribe`, and `check`.

use crate::config::PubsubSourceConfig;
use crate::convert::{MessageParts, record_with_policy, timestamp_millis};
use crate::state::{PubsubBookmark, state_key};
use faucet_core::CallExt as _;
use faucet_core::io::CallContext;
use faucet_core::lease::LeaseExtender;
use faucet_core::{FaucetError, Stream, StreamPage};
use gcloud_pubsub::client::Client;
use gcloud_pubsub::subscriber::ReceivedMessage;
use gcloud_pubsub::subscription::Subscription;
use serde_json::Value;
use std::pin::Pin;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Google Cloud Pub/Sub source. See the crate README for semantics.
pub struct PubsubSource {
    config: PubsubSourceConfig,
    client: Client,
    /// Bookmark applied by the pipeline before streaming — informational only
    /// (Pub/Sub redelivers unacked messages; there is no client-side seek).
    start_bookmark: Mutex<Option<PubsubBookmark>>,
    /// Backend-call context (#880): pulls and the idle pause run through it.
    calls: faucet_core::CallSlot,
}

impl PubsubSource {
    /// Create a new Pub/Sub source. Validates the config and builds the client.
    pub async fn new(config: PubsubSourceConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let client = faucet_common_pubsub::build_client(&config.connection).await?;
        Ok(Self {
            config,
            client,
            start_bookmark: Mutex::new(None),
            calls: faucet_core::CallSlot::new(CallContext::new().without_timeout()),
        })
    }

    fn subscription(&self) -> Subscription {
        self.client.subscription(&self.config.subscription)
    }
}

/// Pull up to `max` messages. Thin SDK shim.
async fn pull_messages(
    subscription: &Subscription,
    max: usize,
) -> Result<Vec<ReceivedMessage>, FaucetError> {
    match subscription.pull(max as i32, None).await {
        Ok(messages) => Ok(messages),
        // A long-poll parked on a quiet subscription past the client's RPC
        // deadline is an empty pull, not a failure.
        Err(e) if pull_timed_out(e.code() as i32) => Ok(Vec::new()),
        Err(e) => Err(FaucetError::Source(format!("pubsub: pull failed: {e}"))),
    }
}

/// gRPC `DEADLINE_EXCEEDED`.
fn pull_timed_out(code: i32) -> bool {
    code == 4
}

/// Return messages to the subscription for immediate redelivery (a pull
/// response the run will not process). Best-effort. Thin SDK shim.
async fn nack_messages(acks: &CallContext, messages: Vec<ReceivedMessage>) {
    use futures::StreamExt;
    futures::stream::iter(messages)
        .map(|m| async move {
            let nacked = match m.nack().within_ctx(acks, "nack").await {
                Ok(r) => r.map_err(|e| e.to_string()),
                Err(e) => Err(e.to_string()),
            };
            if let Err(e) = nacked {
                tracing::warn!(error = %e, "pubsub: nack failed; the message redelivers after its ack deadline");
            }
        })
        .buffer_unordered(RENEW_CONCURRENCY)
        .collect::<Vec<()>>()
        .await;
}

/// How many messages one pull asks for: never more than the run still needs,
/// so `max_messages` does not strand a pulled tail that is neither acked nor
/// nacked (#789 MSG-46). Pure.
pub(crate) fn pull_size(per_pull: usize, max_messages: Option<usize>, total: usize) -> usize {
    match max_messages {
        Some(max) => per_pull.min(max.saturating_sub(total)).max(1),
        None => per_pull,
    }
}

/// How long an idle-terminated run waits for a pull response already in
/// flight before cancelling it.
const LATE_PULL_GRACE: Duration = Duration::from_secs(2);

/// Ack ids per `Acknowledge` RPC — well under the request-size limit.
const ACK_BATCH: usize = 500;
/// Concurrent `ModifyAckDeadline` calls while renewing held messages.
const RENEW_CONCURRENCY: usize = 16;

/// Ack a batch of messages with one `Acknowledge` RPC per [`ACK_BATCH`] ids
/// (best-effort — a failed ack means redelivery, i.e. at-least-once, never data
/// loss). Thin SDK shim.
async fn ack_messages(
    acks: &CallContext,
    subscription: &Subscription,
    messages: &[ReceivedMessage],
) {
    for chunk in messages.chunks(ACK_BATCH) {
        let ids = chunk.iter().map(|m| m.ack_id().to_string()).collect();
        let acked = match subscription.ack(ids).within_ctx(acks, "ack").await {
            Ok(r) => r.map_err(|e| e.to_string()),
            Err(e) => Err(e.to_string()),
        };
        if let Err(e) = acked {
            tracing::warn!(error = %e, "pubsub: ack failed; messages will be redelivered");
        }
    }
}

/// Renew the ack deadline of every held message. Best-effort. Thin SDK shim.
async fn renew_deadlines(acks: CallContext, messages: Vec<ReceivedMessage>, secs: i32) {
    use futures::StreamExt;
    let acks = &acks;
    futures::stream::iter(messages)
        .map(|m| async move {
            let renewed = match m.modify_ack_deadline(secs).within_ctx(acks, "renew").await {
                Ok(r) => r.map_err(|e| e.to_string()),
                Err(e) => Err(e.to_string()),
            };
            if let Err(e) = renewed {
                tracing::warn!(error = %e, "pubsub: ack deadline renewal failed");
            }
        })
        .buffer_unordered(RENEW_CONCURRENCY)
        .collect::<Vec<()>>()
        .await;
}

/// The context acks, nacks and lease renewals run in: the run's recorder and
/// a bounded attempt, but never its cancel token — an ack is how a page the
/// pipeline already made durable leaves the subscription.
fn ack_context(calls: &CallContext) -> CallContext {
    let ctx = CallContext::new().without_retry();
    match calls.recorder() {
        Some(r) => ctx.with_recorder(r.clone()),
        None => ctx,
    }
}

/// How often held messages are renewed: every 5 s, or every third of the
/// window when that is shorter. Pure.
pub(crate) fn renew_every(extension_secs: u32) -> Duration {
    Duration::from_secs(u64::from((extension_secs / 3).clamp(1, 5)))
}

impl PubsubSource {
    /// Epoch-millis publish time of a received message, if the server set it.
    fn publish_millis(m: &ReceivedMessage) -> Option<i64> {
        m.message
            .publish_time
            .as_ref()
            .map(|t| timestamp_millis(t.seconds, t.nanos))
    }
}

#[faucet_core::async_trait]
impl faucet_core::Source for PubsubSource {
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
        let chunk = if self.config.batch_size == 0 {
            usize::MAX
        } else {
            self.config.batch_size
        };
        let idle = self.config.idle_termination_secs.map(Duration::from_secs);
        let max_messages = self.config.max_messages;
        let per_pull = self.config.max_messages_per_pull;
        let format = self.config.value_format;
        let attributes_key = self.config.attributes_key.clone();
        let on_decode_error = self.config.on_decode_error;
        let extension = self.config.ack_deadline_extension_secs;

        Box::pin(async_stream::try_stream! {
            let subscription = self.subscription();
            let calls = self.calls.get();
            let acks = ack_context(&calls);
            let lease = (extension > 0).then(|| {
                let acks = acks.clone();
                LeaseExtender::spawn(renew_every(extension), move |held: Vec<ReceivedMessage>| {
                    renew_deadlines(acks.clone(), held, extension as i32)
                })
            });
            let hold = |page: &[ReceivedMessage], yielded: &[ReceivedMessage]| {
                if let Some(l) = &lease {
                    l.hold(page.iter().chain(yielded).cloned().collect());
                }
            };

            let mut cumulative = self
                .start_bookmark
                .lock()
                .expect("bookmark mutex poisoned")
                .clone()
                .unwrap_or_default();

            // Messages of pages yielded since the last ack. Acked at the top of
            // the next iteration — by then the pipeline has written each page to
            // the sink and persisted its bookmark, so acking is safe (a crash
            // before the ack redelivers, i.e. at-least-once).
            let mut pending: Vec<ReceivedMessage> = Vec::new();
            let mut buffer: Vec<Value> = Vec::new();
            let mut page_msgs: Vec<ReceivedMessage> = Vec::new();
            let mut total = 0usize;
            let mut last_activity = Instant::now();

            'consume: loop {
                if !pending.is_empty() {
                    ack_messages(&acks, &subscription, &pending).await;
                    pending.clear();
                    hold(&page_msgs, &pending);
                }

                let want = pull_size(per_pull, max_messages, total);
                // The pull runs as its own task so an idle timeout does not
                // drop a response the server already leased: the late
                // messages are nacked for prompt redelivery instead of
                // waiting out their ack deadline (#789 MSG-82). Every RPC is
                // bounded by the client's deadline (#789 MSG-60).
                let pull_sub = self.subscription();
                let mut pull = tokio::spawn(async move { pull_messages(&pull_sub, want).await });
                let waited = match idle {
                    Some(window) => match tokio::time::timeout(
                        window,
                        (&mut pull).within_ctx(&calls, "pull"),
                    )
                    .await
                    {
                        Ok(waited) => waited,
                        Err(_) => {
                            tracing::info!(
                                subscription = %self.config.subscription,
                                idle_secs = window.as_secs(),
                                "pubsub: idle termination reached"
                            );
                            // A response already on its way is nacked; a pull
                            // still parked server-side is cancelled, which
                            // leases nothing.
                            match tokio::time::timeout(LATE_PULL_GRACE, &mut pull).await {
                                Ok(Ok(Ok(late))) if !late.is_empty() => {
                                    nack_messages(&acks, late).await
                                }
                                Ok(_) => {}
                                Err(_) => pull.abort(),
                            }
                            break 'consume;
                        }
                    },
                    None => (&mut pull).within_ctx(&calls, "pull").await,
                };
                let joined = waited.inspect_err(|_| pull.abort())?;
                let messages = joined
                    .map_err(|e| FaucetError::Source(format!("pubsub: pull task failed: {e}")))??;

                if messages.is_empty() {
                    if let Some(window) = idle
                        && last_activity.elapsed() >= window
                    {
                        tracing::info!(
                            subscription = %self.config.subscription,
                            idle_secs = window.as_secs(),
                            "pubsub: idle termination reached"
                        );
                        break 'consume;
                    }
                    // No idle window (max-messages only) and no messages: yield
                    // to the scheduler briefly to avoid a hot spin.
                    calls.wait(Duration::from_millis(200)).await?;
                    continue;
                }
                last_activity = Instant::now();
                if lease.is_some() {
                    let mut pulled = messages.clone();
                    pulled.extend(page_msgs.iter().cloned());
                    hold(&pulled, &pending);
                }

                let mut messages = messages.into_iter();
                while let Some(m) = messages.next() {
                    let record = record_with_policy(
                        &MessageParts {
                            data: &m.message.data,
                            attributes: &m.message.attributes,
                            message_id: &m.message.message_id,
                            ordering_key: &m.message.ordering_key,
                            publish_time_millis: Self::publish_millis(&m),
                        },
                        format,
                        &attributes_key,
                        on_decode_error,
                    )?;
                    cumulative.advance(&m.message.message_id);
                    if let Some(record) = record {
                        buffer.push(record);
                    }
                    page_msgs.push(m);
                    total += 1;

                    if buffer.len() >= chunk {
                        let records = std::mem::take(&mut buffer);
                        pending.append(&mut page_msgs);
                        yield StreamPage {
                            records,
                            bookmark: Some(cumulative.to_value()),
                        };
                    }

                    if let Some(max) = max_messages
                        && total >= max
                    {
                        tracing::info!(
                            subscription = %self.config.subscription,
                            max,
                            "pubsub: max_messages reached"
                        );
                        // Defensive: a server returning more than requested.
                        let rest: Vec<ReceivedMessage> = messages.collect();
                        if !rest.is_empty() {
                            nack_messages(&acks, rest).await;
                        }
                        break 'consume;
                    }
                }
            }

            // Flush any buffered-but-un-yielded records as a final page.
            if !buffer.is_empty() {
                let records = std::mem::take(&mut buffer);
                pending.append(&mut page_msgs);
                yield StreamPage {
                    records,
                    bookmark: Some(cumulative.to_value()),
                };
            }
            // Skipped (undecodable) messages after the last page carry no
            // record, so nothing has to be durable before they are acked.
            pending.append(&mut page_msgs);

            // Every remaining page has now resumed past its `yield`, so it is
            // durable — ack it before returning.
            if !pending.is_empty() {
                ack_messages(&acks, &subscription, &pending).await;
            }
            drop(lease);

            tracing::info!(
                subscription = %self.config.subscription,
                records = total,
                "pubsub source stream complete"
            );
        })
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(PubsubSourceConfig))
            .expect("schema serialization")
    }

    fn state_key(&self) -> Option<String> {
        Some(state_key(&self.config.subscription))
    }

    async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
        *self.start_bookmark.lock().expect("bookmark mutex poisoned") =
            Some(PubsubBookmark::from_value(&bookmark));
        Ok(())
    }

    fn consumes_destructively(&self) -> bool {
        true
    }

    fn connector_name(&self) -> &'static str {
        "pubsub"
    }

    fn set_call_context(&self, ctx: faucet_core::io::CallContext) {
        self.calls.install(ctx);
    }

    fn set_roundtrip_recorder(
        &self,
        recorder: std::sync::Arc<faucet_core::observability::RoundtripRecorder>,
    ) {
        self.calls.install_recorder(recorder);
    }

    fn dataset_uri(&self) -> String {
        format!(
            "pubsub://{}/subscriptions/{}",
            self.config
                .connection
                .project_id
                .as_deref()
                .unwrap_or("default"),
            self.config.subscription
        )
    }

    /// Side-effect-free probe: confirm the subscription exists (no messages
    /// consumed). The default first-page probe could block for the full idle
    /// window on a quiet subscription.
    async fn check(
        &self,
        ctx: &faucet_core::CheckContext,
    ) -> Result<faucet_core::CheckReport, FaucetError> {
        use faucet_core::{CheckReport, Probe};
        let start = std::time::Instant::now();
        let subscription = self.subscription();
        let fut = subscription.exists(None).within(&self.calls, "exists");
        let probe = match tokio::time::timeout(ctx.timeout, fut).await {
            Err(_) => Probe::fail("subscription_exists", start.elapsed(), "timed out"),
            Ok(Err(e)) => Probe::fail("subscription_exists", start.elapsed(), e.to_string()),
            Ok(Ok(r)) => match r {
                Ok(true) => Probe::pass("subscription_exists", start.elapsed()),
                Ok(false) => Probe::fail(
                    "subscription_exists",
                    start.elapsed(),
                    format!("subscription '{}' does not exist", self.config.subscription),
                ),
                Err(e) => Probe::fail("subscription_exists", start.elapsed(), e.to_string()),
            },
        };
        Ok(CheckReport::single(probe))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_deadline_exceeded_is_an_empty_pull() {
        assert!(pull_timed_out(4));
        assert!(!pull_timed_out(14));
        assert!(!pull_timed_out(0));
    }

    // Constructing a live client needs the emulator or real GCP, so the
    // network-bound trait methods are exercised by `tests/integration.rs`
    // (emulator-gated). Here we cover the offline, pure-ish surface.

    #[test]
    fn pulls_never_exceed_what_the_run_still_needs() {
        assert_eq!(pull_size(1000, None, 50), 1000);
        assert_eq!(pull_size(1000, Some(30), 25), 5);
        assert_eq!(pull_size(10, Some(30), 0), 10);
        assert_eq!(pull_size(10, Some(30), 30), 1);
    }

    #[test]
    fn state_key_and_dataset_uri_helpers() {
        // dataset_uri/state_key are built from config strings only.
        assert_eq!(state_key("orders-sub"), "pubsub:orders-sub");
    }

    #[test]
    fn config_schema_exposes_fields() {
        let schema = serde_json::to_value(faucet_core::schema_for!(PubsubSourceConfig)).unwrap();
        assert!(schema["properties"]["subscription"].is_object());
        assert!(schema["properties"]["value_format"].is_object());
    }

    #[tokio::test]
    async fn new_rejects_config_without_termination() {
        // Validation runs before any client build, so this fails offline with a
        // config error (never a network error).
        // `PubsubSource` holds a non-`Debug` SDK subscription, so `unwrap_err`
        // (which needs the `Ok` type to be `Debug`) is unavailable — match.
        let err = match PubsubSource::new(PubsubSourceConfig::new("orders-sub")).await {
            Ok(_) => panic!("expected a termination-config error"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("idle_termination_secs"), "{err}");
    }

    #[test]
    fn renewal_runs_every_five_seconds_or_a_third_of_a_short_window() {
        assert_eq!(renew_every(60), Duration::from_secs(5));
        assert_eq!(renew_every(12), Duration::from_secs(4));
        assert_eq!(renew_every(10), Duration::from_secs(3));
        assert_eq!(renew_every(600), Duration::from_secs(5));
    }
}
