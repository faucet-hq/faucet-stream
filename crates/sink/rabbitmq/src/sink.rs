//! `RabbitMqSink` — the AMQP publisher (the one module that does I/O).
//!
//! Records are encoded and routed row by row; each chunk of `batch_size`
//! messages is published back-to-back on one channel and then its publisher
//! confirms are awaited together (`FuturesUnordered`). A per-row problem — an
//! unresolvable routing key, an unencodable record, a broker `nack`, or a
//! `mandatory` return — becomes that row's error in `write_batch_partial`, so
//! the pipeline can route it to a DLQ. A channel-level failure fails the batch.

use crate::config::{RabbitMqSinkConfig, RoutingKey};
use async_trait::async_trait;
use faucet_common_rabbitmq::{amqp_error, check_exchange, declare_exchange, encode_payload};
use faucet_core::{FaucetError, RowOutcome, Sink};
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
pub struct RabbitMqSink {
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
            config,
            routing,
            state: Mutex::new(None),
        })
    }

    async fn channel(&self) -> Result<lapin::Channel, FaucetError> {
        let mut guard = self.state.lock().await;
        if let Some(p) = guard.as_ref()
            && p.channel.status().connected()
            && p.conn.status().connected()
        {
            return Ok(p.channel.clone());
        }
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

    /// Resolve and encode one record. Pure.
    fn prepare(&self, record: &Value) -> Result<(String, Vec<u8>), FaucetError> {
        let key = resolve_routing_key(record, &self.routing)?;
        let body = encode_payload(record, self.config.value_format)?;
        Ok((key, body))
    }

    /// Publish every preparable record; returns one outcome per input row.
    async fn publish(&self, records: &[Value]) -> Result<Vec<RowOutcome>, FaucetError> {
        let mut outcomes: Vec<RowOutcome> = Vec::with_capacity(records.len());
        let mut prepared = Vec::with_capacity(records.len());
        for (i, record) in records.iter().enumerate() {
            match self.prepare(record) {
                Ok(msg) => {
                    outcomes.push(Ok(()));
                    prepared.push((i, msg));
                }
                Err(e) => outcomes.push(Err(e)),
            }
        }
        if prepared.is_empty() {
            return Ok(outcomes);
        }

        let channel = self.channel().await?;
        let props = self.properties();
        let options = BasicPublishOptions {
            mandatory: self.config.mandatory,
            ..BasicPublishOptions::default()
        };
        let chunk = if self.config.batch_size == 0 {
            prepared.len()
        } else {
            self.config.batch_size
        };

        for group in prepared.chunks(chunk) {
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
        Ok(outcomes)
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
impl Sink for RabbitMqSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }
        let outcomes = self.publish(records).await?;
        let failed: Vec<&FaucetError> = outcomes.iter().filter_map(|o| o.as_ref().err()).collect();
        if let Some(first) = failed.first() {
            return Err(FaucetError::Sink(format!(
                "rabbitmq sink: {} of {} record(s) failed (first: {first})",
                failed.len(),
                records.len()
            )));
        }
        Ok(records.len())
    }

    async fn write_batch_partial(&self, records: &[Value]) -> Result<Vec<RowOutcome>, FaucetError> {
        if records.is_empty() {
            return Ok(Vec::new());
        }
        self.publish(records).await
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(RabbitMqSinkConfig)).unwrap_or(Value::Null)
    }

    fn connector_name(&self) -> &'static str {
        "rabbitmq"
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

    /// Connect, then passively check the target exchange — nothing is published.
    async fn check(
        &self,
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

#[cfg(test)]
mod tests {
    use super::*;
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
