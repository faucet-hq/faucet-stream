//! `RabbitMqSink` — the AMQP publisher (the one module that does I/O).
//!
//! Records are encoded and routed row by row; each chunk of `batch_size`
//! messages is published back-to-back on one channel and then its publisher
//! confirms are awaited together (`FuturesUnordered`). A per-row problem — an
//! unresolvable routing key, an unencodable record, a broker `nack`, or a
//! `mandatory` return — becomes that row's error in `write_batch_partial`, so
//! the pipeline can route it to a DLQ. A channel-level failure fails the batch.
//!
//! Built on the messaging family driver (RFC 0014): `RabbitMqDriver` routes and
//! encodes one record and publishes a page's messages; `MessageSinkDriver`
//! reports each row and serves the plain, reported and per-row write paths.

use crate::config::{RabbitMqSinkConfig, RoutingKey};
use async_trait::async_trait;
use faucet_common_rabbitmq::{amqp_error, check_exchange, declare_exchange, encode_payload};
use faucet_core::driver::{DriverSink, MessageDriver, MessageSinkDriver, OutboundMessage};
use faucet_core::{CallSlot, FaucetError, RowOutcome, Sink};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use jsonpath_rust::JsonPath;
use lapin::options::{BasicPublishOptions, ConfirmSelectOptions};
use lapin::{BasicProperties, Confirmation};
use serde_json::Value;
use std::time::Instant;
use tokio::sync::Mutex;

/// A sink that publishes each record as a RabbitMQ message.
///
/// The connection is opened on the first write and reused; if the channel is
/// found closed (e.g. after a broker-side error) the next write reconnects.
///
/// Built on the messaging family driver (RFC 0014).
pub struct RabbitMqSink {
    inner: DriverSink<MessageSinkDriver<RabbitMqDriver>>,
}

/// The AMQP I/O primitives the messaging driver runs on.
pub(crate) struct RabbitMqDriver {
    config: RabbitMqSinkConfig,
    routing: RoutingKey,
    state: Mutex<Option<Publisher>>,
}

struct Publisher {
    conn: lapin::Connection,
    channel: lapin::Channel,
}

impl RabbitMqSink {
    /// Create a new RabbitMQ sink. Validates the config; does not connect.
    pub async fn new(config: RabbitMqSinkConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let routing = config.routing()?;
        Ok(Self {
            inner: DriverSink::new(MessageSinkDriver::new(RabbitMqDriver {
                config,
                routing,
                state: Mutex::new(None),
            })),
        })
    }

    #[cfg(test)]
    fn properties(&self) -> BasicProperties {
        self.inner.driver().driver().properties()
    }
}

#[async_trait]
impl Sink for RabbitMqSink {
    faucet_core::forward_sink!(inner);
}

impl RabbitMqDriver {
    async fn channel(&self, calls: &CallSlot) -> Result<lapin::Channel, FaucetError> {
        let mut guard = self.state.lock().await;
        if let Some(p) = guard.as_ref()
            && p.channel.status().connected()
            && p.conn.status().connected()
        {
            return Ok(p.channel.clone());
        }
        let (conn, channel) = calls
            .call_once("connect", async {
                let conn = faucet_common_rabbitmq::connect(&self.config.connection).await?;
                let channel = conn
                    .create_channel()
                    .await
                    .map_err(|e| sink_error("open channel", e))?;
                if self.config.confirm {
                    channel
                        .confirm_select(ConfirmSelectOptions::default())
                        .await
                        .map_err(|e| sink_error("enable publisher confirms", e))?;
                }
                if let Some(kind) = self.config.exchange_kind {
                    declare_exchange(&channel, &self.config.exchange, kind).await?;
                }
                Ok((conn, channel))
            })
            .await?;
        *guard = Some(Publisher {
            conn,
            channel: channel.clone(),
        });
        Ok(channel)
    }

    fn properties(&self) -> BasicProperties {
        let props = BasicProperties::default()
            .with_content_type(self.config.value_format.content_type().into());
        if self.config.persistent {
            props.with_delivery_mode(2)
        } else {
            props.with_delivery_mode(1)
        }
    }

    /// Publish every message in chunks of `batch_size`, awaiting each
    /// chunk's confirms; returns one outcome per message.
    async fn publish_messages(
        &self,
        calls: &CallSlot,
        messages: Vec<OutboundMessage>,
    ) -> Result<Vec<RowOutcome>, FaucetError> {
        let mut outcomes: Vec<RowOutcome> = messages.iter().map(|_| Ok(())).collect();
        if messages.is_empty() {
            return Ok(outcomes);
        }
        let prepared: Vec<(usize, (String, Vec<u8>))> = messages
            .into_iter()
            .map(|m| (m.destination, m.payload))
            .enumerate()
            .collect();

        let channel = self.channel(calls).await?;
        let props = self.properties();
        let options = BasicPublishOptions {
            mandatory: self.config.effective_mandatory(),
            ..BasicPublishOptions::default()
        };
        let chunk = if self.config.batch_size == 0 {
            prepared.len()
        } else {
            self.config.batch_size
        };

        let limit = std::time::Duration::from_secs(self.config.publish_timeout_secs);
        for (n, group) in prepared.chunks(chunk).enumerate() {
            // Bounded: a broker resource alarm (`connection.blocked`) holds
            // publishers indefinitely (#789 MSG-84).
            let done = calls
                .call_once("publish", async {
                    Ok(tokio::time::timeout(
                        limit,
                        self.publish_group(&channel, group, &props, options, &mut outcomes),
                    )
                    .await)
                })
                .await;
            match done {
                Ok(Ok(published)) => published?,
                Ok(Err(_)) => {
                    let unconfirmed = prepared[n * chunk..].iter().map(|(i, _)| *i);
                    fail_unconfirmed(&mut outcomes, unconfirmed, limit);
                    return Ok(outcomes);
                }
                // The run's deadline cut the group (#880).
                Err(cause) => {
                    let unconfirmed = prepared[n * chunk..].iter().map(|(i, _)| *i);
                    fail_cut(&mut outcomes, unconfirmed, &cause);
                    return Ok(outcomes);
                }
            }
        }
        Ok(outcomes)
    }

    async fn publish_group(
        &self,
        channel: &lapin::Channel,
        group: &[(usize, (String, Vec<u8>))],
        props: &lapin::BasicProperties,
        options: BasicPublishOptions,
        outcomes: &mut [RowOutcome],
    ) -> Result<(), FaucetError> {
        {
            let mut confirms = FuturesUnordered::new();
            for (i, (key, body)) in group {
                let confirm = channel
                    .basic_publish(
                        self.config.exchange.as_str().into(),
                        key.as_str().into(),
                        options,
                        body,
                        props.clone(),
                    )
                    .await
                    .map_err(|e| sink_error("publish", e))?;
                let i = *i;
                confirms.push(async move { (i, confirm.await) });
            }
            while let Some((i, result)) = confirms.next().await {
                let confirmation = result.map_err(|e| sink_error("publisher confirm", e))?;
                outcomes[i] = confirmation_outcome(confirmation, &self.config.exchange);
            }
        }
        Ok(())
    }
}

/// Map a publisher confirmation to a row outcome. Pure.
pub(crate) fn confirmation_outcome(c: Confirmation, exchange: &str) -> RowOutcome {
    match c {
        Confirmation::Ack(None) | Confirmation::NotRequested => Ok(()),
        Confirmation::Ack(Some(returned)) => Err(FaucetError::Sink(format!(
            "rabbitmq sink: message returned as unroutable by exchange '{exchange}' \
             (routing key '{}'): {} {}",
            returned.delivery.routing_key.as_str(),
            returned.reply_code,
            returned.reply_text.as_str()
        ))),
        Confirmation::Nack(_) => Err(FaucetError::Sink(format!(
            "rabbitmq sink: broker negatively acknowledged a message on exchange '{exchange}'"
        ))),
    }
}

/// Derive one record's routing key. Pure.
pub(crate) fn resolve_routing_key(
    record: &Value,
    routing: &RoutingKey,
) -> Result<String, FaucetError> {
    let (value, what) = match routing {
        RoutingKey::Static(k) => return Ok(k.clone()),
        RoutingKey::Field(f) => (record.get(f).cloned(), format!("routing_key_field '{f}'")),
        RoutingKey::JsonPath(p) => (
            record
                .query(p)
                .map_err(|e| FaucetError::Config(format!("invalid JSONPath '{p}': {e}")))?
                .into_iter()
                .next()
                .cloned(),
            format!("routing_key_jsonpath '{p}'"),
        ),
    };
    let key = match value {
        None => {
            return Err(FaucetError::Sink(format!(
                "rabbitmq sink: {what} matched nothing in the record"
            )));
        }
        Some(Value::String(s)) => s,
        Some(Value::Number(n)) => n.to_string(),
        Some(Value::Bool(b)) => b.to_string(),
        Some(Value::Null) => {
            return Err(FaucetError::Sink(format!(
                "rabbitmq sink: {what} resolved to null"
            )));
        }
        Some(_) => {
            return Err(FaucetError::Sink(format!(
                "rabbitmq sink: {what} resolved to a container — routing keys must be scalars"
            )));
        }
    };
    if key.len() > 255 {
        return Err(FaucetError::Sink(format!(
            "rabbitmq sink: {what} resolved to a routing key longer than 255 bytes"
        )));
    }
    Ok(key)
}

fn sink_error(context: &str, e: lapin::Error) -> FaucetError {
    match amqp_error(context, e) {
        FaucetError::Source(m) => FaucetError::Sink(m),
        other => other,
    }
}

#[async_trait]
impl MessageDriver for RabbitMqDriver {
    fn connector_name(&self) -> &'static str {
        "rabbitmq"
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(RabbitMqSinkConfig)).unwrap_or(Value::Null)
    }

    fn dataset_uri(&self) -> String {
        let exchange = if self.config.exchange.is_empty() {
            "(default)"
        } else {
            self.config.exchange.as_str()
        };
        format!(
            "{}?exchange={exchange}",
            self.config.connection.display_address()
        )
    }

    fn call_context(&self) -> faucet_core::CallContext {
        faucet_core::CallContext::new().without_timeout()
    }

    fn batch_atomicity(&self) -> faucet_core::BatchAtomicity {
        self.config.batch_atomicity()
    }

    /// Resolve the routing key and encode the body; a record that cannot be
    /// routed or encoded fails alone.
    fn encode(&self, record: &Value) -> Result<OutboundMessage, FaucetError> {
        let key = resolve_routing_key(record, &self.routing)?;
        let body = encode_payload(record, self.config.value_format)?;
        Ok(OutboundMessage::new(key, body))
    }

    /// `publish` opens the channel and bounds each `batch_size` chunk itself.
    fn publish_is_one_call(&self) -> bool {
        false
    }

    async fn publish(
        &self,
        calls: &CallSlot,
        group: Vec<OutboundMessage>,
    ) -> Result<Vec<RowOutcome>, FaucetError> {
        self.publish_messages(calls, group).await
    }

    fn page_error(&self, offered: usize, failed: &[(usize, FaucetError)]) -> Option<FaucetError> {
        let (_, first) = failed.first()?;
        Some(FaucetError::Sink(format!(
            "rabbitmq sink: {} of {offered} record(s) failed (first: {first})",
            failed.len()
        )))
    }

    /// Connect, then passively check the target exchange — nothing is published.
    async fn check(
        &self,
        _calls: &CallSlot,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};
        let start = Instant::now();
        let conn = match tokio::time::timeout(
            ctx.timeout,
            faucet_common_rabbitmq::connect(&self.config.connection),
        )
        .await
        {
            Ok(Ok(conn)) => conn,
            Ok(Err(e)) => {
                return Ok(CheckReport::single(Probe::fail_hint(
                    "connect",
                    start.elapsed(),
                    e.to_string(),
                    "verify the broker address, vhost, credentials and TLS settings",
                )));
            }
            Err(_) => {
                return Ok(CheckReport::single(Probe::fail_hint(
                    "connect",
                    start.elapsed(),
                    "connect timed out",
                    "no broker responded within the check timeout",
                )));
            }
        };
        let mut report = CheckReport::single(Probe::pass("connect", start.elapsed()));
        let probe = if self.config.exchange.is_empty() {
            Probe::skip("exchange", "the default exchange always exists")
        } else {
            let ex_start = Instant::now();
            let checked = match conn.create_channel().await {
                Ok(channel) => check_exchange(&channel, &self.config.exchange).await,
                Err(e) => Err(sink_error("open channel", e)),
            };
            match checked {
                Ok(()) => Probe::pass("exchange", ex_start.elapsed()),
                Err(_) if self.config.exchange_kind.is_some() => Probe::skip(
                    "exchange",
                    format!(
                        "exchange '{}' does not exist yet; it is declared on the first write",
                        self.config.exchange
                    ),
                ),
                Err(e) => Probe::fail_hint(
                    "exchange",
                    ex_start.elapsed(),
                    e.to_string(),
                    "create the exchange or set `exchange_kind` so the sink declares it",
                ),
            }
        };
        report.probes.push(probe);
        let _ = conn.close(200, "faucet check complete".into()).await;
        Ok(report)
    }
}

/// A publish group that timed out leaves its rows, and every row of the
/// groups after it, failed with the timeout unless already decided (earlier
/// groups' confirms, rows that failed on their own).
/// The error is the typed, retriable [`FaucetError::Timeout`] (a broker
/// resource alarm blocking publishers is the usual cause).
fn fail_unconfirmed(
    outcomes: &mut [faucet_core::RowOutcome],
    rows: impl Iterator<Item = usize>,
    after: std::time::Duration,
) {
    for i in rows {
        if outcomes[i].is_ok() {
            outcomes[i] = Err(FaucetError::Timeout {
                operation: "rabbitmq publish (broker blocked by a resource alarm?)".into(),
                after,
            });
        }
    }
}

/// Like [`fail_unconfirmed`], for a group the run's call context cut short:
/// every listed row not yet decided fails with that cause.
fn fail_cut(
    outcomes: &mut [faucet_core::RowOutcome],
    rows: impl Iterator<Item = usize>,
    cause: &FaucetError,
) {
    for i in rows {
        if outcomes[i].is_ok() {
            outcomes[i] = Err(match cause {
                FaucetError::Cancelled { operation } => FaucetError::Cancelled {
                    operation: operation.clone(),
                },
                FaucetError::Timeout { operation, after } => FaucetError::Timeout {
                    operation: operation.clone(),
                    after: *after,
                },
                other => FaucetError::Sink(format!("rabbitmq publish: {other}")),
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    /// The value-fidelity corpus (#885): every record this sink encodes reads
    /// back unchanged.
    #[test]
    fn the_fidelity_corpus_encodes_unchanged() {
        use faucet_conformance::fidelity;
        let sent = fidelity::corpus();
        let mut landed = Vec::new();
        for record in &sent {
            let bytes: Vec<u8> = faucet_common_rabbitmq::encode_payload(
                record,
                faucet_common_rabbitmq::RabbitMqValueFormat::Json,
            )
            .unwrap();
            landed.push(faucet_core::fidelity::from_slice(&bytes).unwrap());
        }
        fidelity::assert_round_trip(&sent, &landed, fidelity::Tolerance::exact());
    }

    #[test]
    fn a_cut_group_fails_only_rows_not_yet_decided() {
        let mut outcomes: Vec<faucet_core::RowOutcome> =
            vec![Err(FaucetError::Sink("own".into())), Ok(()), Ok(()), Ok(())];
        fail_cut(
            &mut outcomes,
            [0, 1].into_iter(),
            &FaucetError::Cancelled {
                operation: "publish call".into(),
            },
        );
        assert!(matches!(&outcomes[0], Err(FaucetError::Sink(m)) if m == "own"));
        assert!(matches!(&outcomes[1], Err(FaucetError::Cancelled { .. })));
        fail_cut(
            &mut outcomes,
            [2].into_iter(),
            &FaucetError::Timeout {
                operation: "publish call (deadline)".into(),
                after: std::time::Duration::ZERO,
            },
        );
        assert!(matches!(&outcomes[2], Err(FaucetError::Timeout { .. })));
        fail_cut(
            &mut outcomes,
            [3].into_iter(),
            &FaucetError::Source("x".into()),
        );
        assert!(matches!(&outcomes[3], Err(FaucetError::Sink(m)) if m.contains("x")));
    }

    #[test]
    fn a_timeout_fails_only_rows_not_yet_decided() {
        let mut outcomes: Vec<faucet_core::RowOutcome> = vec![
            Ok(()),
            Err(FaucetError::Sink("unroutable".into())),
            Ok(()),
            Ok(()),
        ];
        fail_unconfirmed(
            &mut outcomes,
            [1usize, 3].into_iter(),
            std::time::Duration::from_secs(60),
        );
        assert!(outcomes[0].is_ok() && outcomes[2].is_ok());
        assert!(
            outcomes[1]
                .as_ref()
                .unwrap_err()
                .to_string()
                .contains("unroutable")
        );
        assert!(
            outcomes[3]
                .as_ref()
                .unwrap_err()
                .to_string()
                .contains("timed out after 60s")
        );
        assert_eq!(
            faucet_core::resilience::classify(outcomes[3].as_ref().unwrap_err()),
            Some(faucet_core::resilience::RetryClass::Timeout)
        );
    }
    use lapin::message::{BasicReturnMessage, Delivery};
    use serde_json::json;

    #[test]
    fn routing_static_field_jsonpath() {
        let rec = json!({"kind": "order.created", "n": 5, "ok": true, "meta": {"route": "r1"}});
        assert_eq!(
            resolve_routing_key(&rec, &RoutingKey::Static("q".into())).unwrap(),
            "q"
        );
        assert_eq!(
            resolve_routing_key(&rec, &RoutingKey::Field("kind".into())).unwrap(),
            "order.created"
        );
        assert_eq!(
            resolve_routing_key(&rec, &RoutingKey::Field("n".into())).unwrap(),
            "5"
        );
        assert_eq!(
            resolve_routing_key(&rec, &RoutingKey::Field("ok".into())).unwrap(),
            "true"
        );
        assert_eq!(
            resolve_routing_key(&rec, &RoutingKey::JsonPath("$.meta.route".into())).unwrap(),
            "r1"
        );
    }

    #[test]
    fn routing_errors_are_per_row() {
        let rec = json!({"nil": null, "obj": {"a": 1}, "long": "x".repeat(256)});
        for (routing, needle) in [
            (RoutingKey::Field("missing".into()), "matched nothing"),
            (RoutingKey::Field("nil".into()), "null"),
            (RoutingKey::Field("obj".into()), "container"),
            (RoutingKey::Field("long".into()), "255"),
            (RoutingKey::JsonPath("$.missing".into()), "matched nothing"),
        ] {
            let err = resolve_routing_key(&rec, &routing).unwrap_err();
            assert!(matches!(err, FaucetError::Sink(_)));
            assert!(err.to_string().contains(needle), "{err}");
        }
        let err = resolve_routing_key(&rec, &RoutingKey::JsonPath("$[".into())).unwrap_err();
        assert!(matches!(err, FaucetError::Config(_)));
    }

    #[test]
    fn confirmations_map_to_outcomes() {
        assert!(confirmation_outcome(Confirmation::Ack(None), "ex").is_ok());
        assert!(confirmation_outcome(Confirmation::NotRequested, "ex").is_ok());
        let err = confirmation_outcome(Confirmation::Nack(None), "ex").unwrap_err();
        assert!(err.to_string().contains("negatively"));
        let returned = BasicReturnMessage {
            delivery: Delivery::mock(0, "ex".into(), "nowhere".into(), false, Vec::new()),
            reply_code: 312,
            reply_text: "NO_ROUTE".into(),
        };
        let err = confirmation_outcome(Confirmation::Ack(Some(returned)), "ex").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("unroutable") && msg.contains("nowhere") && msg.contains("312"));
    }

    #[tokio::test]
    async fn properties_follow_config() {
        let sink = RabbitMqSink::new(RabbitMqSinkConfig::to_queue("q"))
            .await
            .unwrap();
        let p = sink.properties();
        assert_eq!(*p.delivery_mode(), Some(2));
        assert_eq!(
            p.content_type().as_ref().map(|s| s.as_str()),
            Some("application/json")
        );
        let mut cfg = RabbitMqSinkConfig::to_queue("q");
        cfg.persistent = false;
        let sink = RabbitMqSink::new(cfg).await.unwrap();
        assert_eq!(*sink.properties().delivery_mode(), Some(1));
    }

    #[tokio::test]
    async fn new_validates_and_describes() {
        let mut cfg = RabbitMqSinkConfig::to_queue("q");
        cfg.routing_key = None;
        assert!(RabbitMqSink::new(cfg).await.is_err());
        let sink = RabbitMqSink::new(RabbitMqSinkConfig::to_queue("q"))
            .await
            .unwrap();
        assert_eq!(sink.connector_name(), "rabbitmq");
        assert_eq!(
            sink.dataset_uri(),
            "amqp://127.0.0.1:5672/%2f?exchange=(default)"
        );
        assert!(sink.config_schema().is_object());
        let mut cfg = RabbitMqSinkConfig::to_queue("q");
        cfg.exchange = "events".into();
        let sink = RabbitMqSink::new(cfg).await.unwrap();
        assert!(sink.dataset_uri().ends_with("exchange=events"));
    }

    fn unreachable() -> RabbitMqSinkConfig {
        let mut cfg = RabbitMqSinkConfig::to_queue("q");
        cfg.connection.port = Some(1);
        cfg.connection.connect_timeout_secs = 5;
        cfg
    }

    #[tokio::test]
    async fn empty_batches_never_connect() {
        let sink = RabbitMqSink::new(unreachable()).await.unwrap();
        assert_eq!(sink.write_batch(&[]).await.unwrap(), 0);
        assert!(sink.write_batch_partial(&[]).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn unpreparable_rows_fail_without_connecting() {
        let mut cfg = unreachable();
        cfg.routing_key = None;
        cfg.routing_key_field = Some("route".into());
        let sink = RabbitMqSink::new(cfg).await.unwrap();
        let out = sink.write_batch_partial(&[json!({"id": 1})]).await.unwrap();
        assert!(out[0].is_err());
        let err = sink.write_batch(&[json!({"id": 1})]).await.unwrap_err();
        assert!(err.to_string().contains("1 of 1"), "{err}");
    }

    #[tokio::test]
    async fn unreachable_broker_errors_not_panics() {
        let sink = RabbitMqSink::new(unreachable()).await.unwrap();
        let err = sink.write_batch(&[json!({"id": 1})]).await.unwrap_err();
        assert!(err.to_string().contains("connect to"), "{err}");
        let report = sink
            .check(&faucet_core::check::CheckContext::default())
            .await
            .unwrap();
        assert_eq!(report.failed_count(), 1);
    }

    #[tokio::test]
    async fn check_times_out() {
        let mut cfg = unreachable();
        cfg.connection.port = None;
        cfg.connection.host = Some("10.255.255.1".into());
        let sink = RabbitMqSink::new(cfg).await.unwrap();
        let ctx = faucet_core::check::CheckContext {
            timeout: std::time::Duration::from_millis(200),
        };
        assert_eq!(sink.check(&ctx).await.unwrap().failed_count(), 1);
    }

    #[test]
    fn sink_error_rewraps_source_variant() {
        let e = lapin::Error::from(lapin::ErrorKind::IOError(std::sync::Arc::new(
            std::io::Error::other("x"),
        )));
        assert!(matches!(sink_error("ctx", e), FaucetError::Sink(_)));
    }
}
