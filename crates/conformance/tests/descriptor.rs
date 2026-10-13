//! The descriptor-vs-built assertions (RFC 0013, #876) pass for a connector
//! whose descriptor agrees with its trait methods, and fail for one that
//! drifts.

use faucet_conformance::{assert_descriptor_matches_sink, assert_descriptor_matches_source};
use faucet_core::capabilities::config;
use faucet_core::{
    Capabilities, ConnectorDescriptor, FaucetError, Sink, Source, Value, WriteMode, async_trait,
    json,
};
use std::collections::HashMap;

struct KeyedSink {
    keyed: bool,
}

#[async_trait]
impl Sink for KeyedSink {
    async fn write_batch(&self, r: &[Value]) -> Result<usize, FaucetError> {
        Ok(r.len())
    }
    fn supported_write_modes(&self) -> &'static [WriteMode] {
        &[WriteMode::Append, WriteMode::Upsert]
    }
    fn dedups_by_key(&self) -> bool {
        self.keyed
    }
}

fn keyed_caps(c: &Value) -> Capabilities {
    Capabilities::builder()
        .write_modes([WriteMode::Append, WriteMode::Upsert])
        .keyed_upsert(true)
        .keyed_dedup(config::dedups_by_key(c))
        .build()
}

const KEYED: ConnectorDescriptor = ConnectorDescriptor::new("keyed", keyed_caps);

struct DiscoveringSource;

#[async_trait]
impl Source for DiscoveringSource {
    async fn fetch_with_context(
        &self,
        _: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        Ok(vec![])
    }
    fn supports_discover(&self) -> bool {
        true
    }
}

fn discovering(_: &Value) -> Capabilities {
    Capabilities::builder().discover(true).build()
}

const DISCOVERING: ConnectorDescriptor = ConnectorDescriptor::new("disc", discovering);

#[test]
fn agreeing_descriptors_pass() {
    for (cfg, keyed) in [
        (json!({}), false),
        (json!({"write_mode": "upsert", "key": ["id"]}), true),
    ] {
        assert_descriptor_matches_sink(&KEYED, "keyed", &cfg, &KeyedSink { keyed });
    }
    assert_descriptor_matches_source(&DISCOVERING, "disc", &json!({}), &DiscoveringSource);
}

#[test]
#[should_panic(expected = "disagree")]
fn a_drifting_sink_descriptor_fails() {
    assert_descriptor_matches_sink(&KEYED, "keyed", &json!({}), &KeyedSink { keyed: true });
}

#[test]
#[should_panic(expected = "disagree")]
fn a_drifting_source_descriptor_fails() {
    let plain = ConnectorDescriptor::new("disc", faucet_core::capabilities::plain_source);
    assert_descriptor_matches_source(&plain, "disc", &json!({}), &DiscoveringSource);
}

#[test]
#[should_panic(expected = "descriptor kind")]
fn a_wrong_kind_fails() {
    assert_descriptor_matches_source(&DISCOVERING, "other", &json!({}), &DiscoveringSource);
}
