//! A plugin connector is confined by `faucet serve` from what its schema
//! declares, exactly like a built-in (#876): a field annotated
//! `x-faucet-sandbox` is forced on, and a `command` field marks it as running
//! a program. Own test binary: it installs the process-global registry.
#![cfg(feature = "serve")]

use faucet_cli::config::PipelineConfig;
use faucet_cli::registry::PluginRegistry;
use faucet_cli::serve::load::{
    confine_sql, sandbox_fields, sink_is_sandboxed, source_is_sandboxed, subprocess_kinds_in,
};
use faucet_core::{FaucetError, Sink, Source, async_trait};
use serde_json::{Value, json};
use std::collections::HashMap;

struct Nothing;

#[async_trait]
impl Source for Nothing {
    async fn fetch_with_context(
        &self,
        _ctx: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        Ok(Vec::new())
    }
}

#[async_trait]
impl Sink for Nothing {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        Ok(records.len())
    }
}

fn confined_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "query": {"type": "string"},
            "lockdown": {"type": "boolean", "x-faucet-sandbox": true}
        }
    })
}

#[test]
fn plugin_connectors_are_confined_from_their_schema() {
    PluginRegistry::with_builtins()
        .register_source_with(
            "boxed-sql",
            |_| Ok(Box::new(Nothing) as Box<dyn Source>),
            confined_schema,
            "A plugin SQL source with a sandbox switch",
        )
        .register_sink_with(
            "boxed-sink",
            |_| Ok(Box::new(Nothing) as Box<dyn Sink>),
            confined_schema,
            "A plugin sink with a sandbox switch",
        )
        .register_source_with(
            "plugin-tap",
            |_| Ok(Box::new(Nothing) as Box<dyn Source>),
            || json!({"type": "object", "properties": {"program": {"type": "string", "x-faucet-resource": "command"}}}),
            "A plugin source that runs a program",
        )
        .install()
        .expect("registry installs");

    assert!(source_is_sandboxed("boxed-sql") && sink_is_sandboxed("boxed-sink"));
    assert!(!source_is_sandboxed("plugin-tap") && !source_is_sandboxed("no-such-kind"));
    assert_eq!(
        &*sandbox_fields(true, "boxed-sql"),
        &["lockdown".to_string()]
    );

    let mut cfg = PipelineConfig::from_text(
        "version: 1\npipeline:\n  source: { type: boxed-sql, config: { query: x, lockdown: false } }\n  sink: { type: boxed-sink, config: {} }\n",
        std::path::Path::new("p.yaml"),
    )
    .unwrap();
    confine_sql(&mut cfg);
    let p = &cfg.pipeline;
    assert_eq!(p.source.as_ref().unwrap().config["lockdown"], true);
    assert_eq!(p.sink.as_ref().unwrap().config["lockdown"], true);

    let doc =
        json!({"pipeline": {"source": {"type": "plugin-tap"}, "sink": {"type": "boxed-sink"}}});
    assert_eq!(
        subprocess_kinds_in(&doc).into_iter().collect::<Vec<_>>(),
        vec!["plugin-tap".to_string()]
    );
}
