//! A third-party connector registered with a `ConnectorDescriptor` is gated
//! exactly like a built-in (RFC 0013, #876): the capabilities it declares pass
//! the upsert / exactly-once gates, and a sink that declares it drops unknown
//! fields is refused under `on_drift: fail` (#883). One process-global
//! registry per test binary, so this lives in its own file.

use faucet_cli::config::PipelineConfig;
use faucet_cli::expand::expand;
use faucet_cli::registry::{self, PluginRegistry};
use faucet_core::capabilities::config;
use faucet_core::{
    Capabilities, ConnectorDescriptor, DeliveryGuarantee, EffectivelyOnceMechanism, FaucetError,
    SchemaReport, Sink, SinkGuarantee, Source, WriteMode, async_trait,
};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Once;

struct LogSource;

#[async_trait]
impl Source for LogSource {
    async fn fetch_with_context(
        &self,
        _: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        Ok(vec![json!({"id": 1})])
    }
    fn supports_exactly_once(&self) -> bool {
        true
    }
}

fn log_source(_: &Value) -> Capabilities {
    Capabilities::builder().exactly_once(true).build()
}

struct KeyedSink {
    keyed: bool,
}

#[async_trait]
impl Sink for KeyedSink {
    async fn write_batch(&self, r: &[Value]) -> Result<usize, FaucetError> {
        Ok(r.len())
    }
    fn supported_write_modes(&self) -> &'static [WriteMode] {
        &[WriteMode::Append, WriteMode::Upsert, WriteMode::Delete]
    }
    fn supports_idempotent_writes(&self) -> bool {
        true
    }
    fn dedups_by_key(&self) -> bool {
        self.keyed
    }
}

fn keyed_sink(c: &Value) -> Capabilities {
    Capabilities::builder()
        .write_modes([WriteMode::Append, WriteMode::Upsert, WriteMode::Delete])
        .idempotent_writes(true)
        .keyed_upsert(true)
        .keyed_dedup(config::dedups_by_key(c))
        .build()
}

struct LossySink;

#[async_trait]
impl Sink for LossySink {
    async fn write_batch(&self, r: &[Value]) -> Result<usize, FaucetError> {
        Ok(r.len())
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities::of_sink(self)
            .to_builder()
            .schema_report(SchemaReport::DropsUnknownFields)
            .build()
    }
}

fn lossy_sink(_: &Value) -> Capabilities {
    Capabilities::append_only_sink()
        .to_builder()
        .schema_report(SchemaReport::DropsUnknownFields)
        .build()
}

const LOG_SOURCE: ConnectorDescriptor = ConnectorDescriptor::new("log-source", log_source);
const KEYED_SINK: ConnectorDescriptor = ConnectorDescriptor::new("keyed-sink", keyed_sink);
const LOSSY_SINK: ConnectorDescriptor = ConnectorDescriptor::new("lossy-sink", lossy_sink);

fn install() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        PluginRegistry::with_builtins()
            .register_source_described(
                LOG_SOURCE,
                |_| Ok(Box::new(LogSource) as Box<dyn Source>),
                || json!({"type": "object"}),
                "log source",
            )
            .register_sink_described(
                KEYED_SINK,
                |cfg| {
                    Ok(Box::new(KeyedSink {
                        keyed: config::dedups_by_key(&cfg),
                    }) as Box<dyn Sink>)
                },
                || json!({"type": "object"}),
                "keyed sink",
            )
            .register_sink_described(
                LOSSY_SINK,
                |_| Ok(Box::new(LossySink) as Box<dyn Sink>),
                || json!({"type": "object"}),
                "lossy sink",
            )
            .register_sink_with(
                "plain-sink",
                |_| Ok(Box::new(LossySink) as Box<dyn Sink>),
                || json!({"type": "object"}),
                "undescribed sink",
            )
            .install()
            .expect("registry installs");
    });
}

fn load(yaml: &str) -> PipelineConfig {
    PipelineConfig::from_text(yaml, Path::new("p.yaml")).expect("config parses")
}

#[test]
fn a_described_plugin_sink_passes_the_upsert_gate() {
    install();
    let cfg = load(
        "version: 1\npipeline:\n  source: { type: log-source, config: {} }\n  \
         sink: { type: keyed-sink, config: { write_mode: upsert, key: [id] } }\n",
    );
    let nodes = expand(&cfg).expect("a declared upsert is accepted");
    assert_eq!(
        nodes[0].delivery_guarantee,
        DeliveryGuarantee::EffectivelyOnce(EffectivelyOnceMechanism::KeyedUpsert)
    );
    let caps = registry::sink_capabilities(
        "keyed-sink",
        &json!({"write_mode": "upsert", "key": ["id"]}),
    );
    assert!(caps.keyed_dedup());
    assert_eq!(caps.sink_guarantee(), SinkGuarantee::AtomicWatermark);
}

#[test]
fn an_undescribed_plugin_sink_is_append_only() {
    install();
    let cfg = load(
        "version: 1\npipeline:\n  source: { type: log-source, config: {} }\n  \
         sink: { type: plain-sink, config: { write_mode: upsert, key: [id] } }\n",
    );
    let err = expand(&cfg)
        .expect_err("an undeclared upsert is refused")
        .to_string();
    assert!(err.contains("not supported by sink 'plain-sink'"), "{err}");
    assert!(
        err.contains("keyed-sink"),
        "the capable plugin is listed: {err}"
    );
    assert!(registry::sink_descriptor("plain-sink").is_none());
}

#[test]
fn a_described_plugin_pair_passes_the_exactly_once_gate() {
    install();
    let dir = tempfile::tempdir().unwrap();
    let cfg = load(&format!(
        "version: 1\ndelivery: exactly_once\npipeline:\n  source: {{ type: log-source, config: {{}} }}\n  \
         sink: {{ type: keyed-sink, config: {{}} }}\n  state: {{ type: file, config: {{ path: '{}' }} }}\n",
        dir.path().display()
    ));
    let nodes = expand(&cfg).expect("declared exactly-once is accepted");
    assert_eq!(
        nodes[0].delivery_guarantee,
        DeliveryGuarantee::EffectivelyOnce(EffectivelyOnceMechanism::AtomicWatermark)
    );
}

#[test]
fn on_drift_fail_against_a_sink_that_drops_fields_is_refused() {
    install();
    let cfg = load(
        "version: 1\npipeline:\n  source: { type: log-source, config: {} }\n  \
         sink: { type: lossy-sink, config: {} }\n  schema: { on_drift: fail }\n",
    );
    let err = expand(&cfg).expect_err("refused at load").to_string();
    assert!(
        err.contains(faucet_core::compat::DRIFT_FAIL_DROPS_FIELDS),
        "{err}"
    );
    let warn = load(
        "version: 1\npipeline:\n  source: { type: log-source, config: {} }\n  \
         sink: { type: lossy-sink, config: {} }\n  schema: { on_drift: warn }\n",
    );
    expand(&warn).expect("a warning policy is fine");
}

#[tokio::test]
async fn plugin_descriptors_match_the_built_connectors() {
    install();
    let auth = HashMap::new();
    for cfg in [json!({}), json!({"write_mode": "upsert", "key": ["id"]})] {
        let sink = registry::build_sink("keyed-sink", cfg.clone(), &auth)
            .await
            .unwrap();
        faucet_conformance::assert_descriptor_matches_sink(
            &KEYED_SINK,
            "keyed-sink",
            &cfg,
            sink.as_ref(),
        );
    }
    let lossy = registry::build_sink("lossy-sink", json!({}), &auth)
        .await
        .unwrap();
    faucet_conformance::assert_descriptor_matches_sink(
        &LOSSY_SINK,
        "lossy-sink",
        &json!({}),
        lossy.as_ref(),
    );
    let src = registry::build_source("log-source", json!({}), &auth, None)
        .await
        .unwrap();
    faucet_conformance::assert_descriptor_matches_source(
        &LOG_SOURCE,
        "log-source",
        &json!({}),
        src.as_ref(),
    );
}
