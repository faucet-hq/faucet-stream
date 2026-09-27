//! End-to-end test for third-party connector plugin loading (#60).
//!
//! Registers a custom source + sink via [`PluginRegistry`], installs the
//! registry as the process-global one, and drives a YAML config that references
//! them by `type:` through the real `faucet run` code path — asserting records
//! flow from the custom source to the custom sink, and that `faucet list` /
//! `faucet schema` surface the custom connectors.
//!
//! This lives in its own integration-test binary because it installs the
//! process-global connector registry (a `OnceLock`, settable once per process).

use clap::Parser;
use faucet_cli::cli::Cli;
use faucet_cli::registry::{self, PluginRegistry};
use faucet_core::{FaucetError, Sink, Source, async_trait};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// A custom source that emits two fixed records (no I/O).
struct MockSource;

#[async_trait]
impl Source for MockSource {
    async fn fetch_with_context(
        &self,
        _ctx: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        Ok(vec![json!({"id": 1}), json!({"id": 2})])
    }

    fn config_schema(&self) -> Value {
        json!({"type": "object", "title": "MockSourceConfig"})
    }

    fn connector_name(&self) -> &'static str {
        "mock-source"
    }
}

/// A custom sink that appends every written record to a shared buffer.
struct MockSink(Arc<Mutex<Vec<Value>>>);

#[async_trait]
impl Sink for MockSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        self.0.lock().unwrap().extend_from_slice(records);
        Ok(records.len())
    }

    fn config_schema(&self) -> Value {
        json!({"type": "object", "title": "MockSinkConfig"})
    }

    fn connector_name(&self) -> &'static str {
        "mock-sink"
    }
}

#[tokio::test]
async fn custom_connectors_flow_from_yaml() {
    let captured = Arc::new(Mutex::new(Vec::<Value>::new()));
    let sink_buf = captured.clone();

    let registry = PluginRegistry::with_builtins()
        .register_source_with(
            "mock-source",
            |_cfg| Ok(Box::new(MockSource) as Box<dyn Source>),
            || json!({"type": "object", "title": "MockSourceConfig"}),
            "Mock source for the plugin-loading test",
        )
        .register_sink_with(
            "mock-sink",
            move |_cfg| Ok(Box::new(MockSink(sink_buf.clone())) as Box<dyn Sink>),
            || json!({"type": "object", "title": "MockSinkConfig"}),
            "Mock sink for the plugin-loading test",
        );
    registry.install().expect("registry installs cleanly");

    // A plugin source's bookmarks are owned by its own connector name (#736).
    let codec = registry::state_codec_for(Some(("mock-source", &json!({}))));
    assert_eq!((codec.owner.as_str(), codec.schema), ("mock-source", 0));

    // `faucet list` / `faucet schema` surface the custom connectors.
    assert!(
        registry::source_descriptions()
            .iter()
            .any(|(n, _)| *n == "mock-source"),
        "custom source should appear in listings"
    );
    assert!(registry::sink_exists("mock-sink"));
    assert_eq!(
        registry::source_schema("mock-source").unwrap()["title"],
        json!("MockSourceConfig")
    );

    // Drive a YAML config referencing the custom connectors by `type:` through
    // the real `faucet run` dispatch path.
    let dir = tempfile::tempdir().unwrap();
    let cfg_path = dir.path().join("pipe.yaml");
    std::fs::write(
        &cfg_path,
        "version: 1\nname: plugin-test\npipeline:\n  source:\n    type: mock-source\n  sink:\n    type: mock-sink\n",
    )
    .unwrap();

    let cli =
        Cli::try_parse_from(["faucet", "run", cfg_path.to_str().unwrap()]).expect("cli parses");
    faucet_cli::run_command(cli)
        .await
        .expect("pipeline runs to completion");

    let records = captured.lock().unwrap();
    assert_eq!(records.len(), 2, "both records reached the custom sink");
    assert_eq!(records[0], json!({"id": 1}));
}
