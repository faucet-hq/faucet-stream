//! Every `faucet_cli::registry` / `faucet_cli::auth_catalog` path an embedder
//! used before the registry moved into `faucet-registry` (#995) still resolves,
//! with the same shape.

use faucet_cli::CliResult;
use faucet_cli::auth_catalog::{AuthCatalog, build_auth_catalog};
use faucet_cli::registry::{
    PluginRegistry, build_sink, build_source, sink_exists, sink_kinds, source_exists, source_kinds,
    source_reads_a_delta, source_resumes_from_bookmark, validate_sink_config,
    validate_source_config,
};
use faucet_core::{Sink, Source};
use serde_json::{Value, json};

#[tokio::test]
async fn embedder_paths_resolve_and_behave() {
    let _: fn() -> Vec<&'static str> = source_kinds;
    let _: fn() -> Vec<&'static str> = sink_kinds;
    let _: fn(&str) -> bool = source_exists;
    let _: fn(&str) -> bool = sink_exists;
    let _: fn(&str, &str, Value) -> CliResult<()> = validate_source_config;
    let _: fn(&str, &str, Value) -> CliResult<()> = validate_sink_config;
    let _: fn(&str, &Value) -> bool = source_resumes_from_bookmark;
    let _: fn(&str, &Value) -> bool = source_reads_a_delta;
    let _: fn(Option<&std::collections::HashMap<String, Value>>) -> CliResult<AuthCatalog> =
        build_auth_catalog;

    let auth: AuthCatalog = build_auth_catalog(None).unwrap();
    let source: CliResult<Box<dyn Source>> = build_source("no-such", json!({}), &auth, None).await;
    let sink: CliResult<Box<dyn Sink>> = build_sink("no-such", json!({}), &auth).await;
    assert!(source.is_err() && sink.is_err());
    assert!(!source_exists("no-such") && !sink_exists("no-such"));
    assert!(source_reads_a_delta("webhook", &json!({})));
    assert!(source_resumes_from_bookmark("kafka", &json!({})));
    assert!(!source_kinds().is_empty() || !sink_kinds().is_empty());

    let registry: PluginRegistry = PluginRegistry::with_builtins();
    drop(registry);
    let _: Option<faucet_cli::registry::SourceFactory> = None;
    let _: Option<faucet_cli::registry::SinkFactory> = None;
}
