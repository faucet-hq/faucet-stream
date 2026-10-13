//! #881: `faucet serve` checks every connector field annotated
//! `x-faucet-resource` against its reference policy, so a connector it has
//! never seen — here a plugin registered at startup — is confined for
//! caller-supplied configs without any serve code change.
//!
//! Its own test binary: it installs the process-global plugin registry.
#![cfg(feature = "serve")]

use faucet_cli::cli::ReferenceArgs;
use faucet_cli::registry::PluginRegistry;
use faucet_cli::serve::load::{BodyOrigin, ConfigFormat, load_submission_with};
use faucet_cli::serve::references::ServeReferencePolicy;
use faucet_core::{FaucetError, Sink, Source, async_trait};
use serde_json::{Value, json};
use std::collections::HashMap;

struct NoSource;

#[async_trait]
impl Source for NoSource {
    async fn fetch_with_context(
        &self,
        _: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        Ok(Vec::new())
    }
    fn config_schema(&self) -> Value {
        Value::Null
    }
}

struct NoSink;

#[async_trait]
impl Sink for NoSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        Ok(records.len())
    }
    fn config_schema(&self) -> Value {
        Value::Null
    }
}

fn install() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        PluginRegistry::with_builtins()
            .register_source_with(
                "acme-feed",
                |_| Ok(Box::new(NoSource) as Box<dyn Source>),
                || {
                    json!({
                        "type": "object",
                        "properties": {
                            "cert_bundle": {"type": "string", "x-faucet-resource": "path"},
                            "feed": {"type": "string", "x-faucet-resource": "url"},
                            "label": {"type": "string"}
                        }
                    })
                },
                "plugin source with resource fields",
            )
            .register_sink_with(
                "acme-null",
                |_| Ok(Box::new(NoSink) as Box<dyn Sink>),
                || json!({"type": "object"}),
                "plugin sink",
            )
            .register_sink_with(
                "acme-exec",
                |_| Ok(Box::new(NoSink) as Box<dyn Sink>),
                || {
                    json!({
                        "type": "object",
                        "properties": {"hook": {"type": "string", "x-faucet-resource": "command"}}
                    })
                },
                "plugin sink that runs a program",
            )
            .install()
            .expect("registry installs");
    });
}

fn config(source: Value, sink: &str) -> String {
    json!({
        "version": 1,
        "name": "plugin-policy",
        "pipeline": {
            "source": {"type": "acme-feed", "config": source},
            "sink": {"type": sink, "config": {"hook": "/bin/true"}}
        }
    })
    .to_string()
}

async fn load(body: &str, origin: BodyOrigin, refs: &ServeReferencePolicy) -> Result<(), String> {
    load_submission_with(body, ConfigFormat::Json, None, None, origin, None, refs)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

const CALLER: BodyOrigin = BodyOrigin::Caller {
    allow_subprocess: false,
};

#[tokio::test]
async fn a_new_connector_field_is_confined_without_serve_changes() {
    install();
    let root = tempfile::tempdir().unwrap();
    let refs = ServeReferencePolicy::from_args(&ReferenceArgs {
        allow_path_root: vec![root.path().to_path_buf()],
        ..Default::default()
    })
    .unwrap();
    let inside = root.path().join("ca.pem").display().to_string();

    load(
        &config(
            json!({"cert_bundle": inside, "label": "/etc/x"}),
            "acme-null",
        ),
        CALLER,
        &refs,
    )
    .await
    .expect("a path under an allowed root loads");

    let err = load(
        &config(json!({"cert_bundle": "/etc/hosts"}), "acme-null"),
        CALLER,
        &refs,
    )
    .await
    .unwrap_err();
    assert!(
        err.contains("/etc/hosts") && err.contains("reference policy"),
        "{err}"
    );

    let err = load(
        &config(
            json!({"cert_bundle": inside, "feed": "http://169.254.169.254/latest"}),
            "acme-null",
        ),
        CALLER,
        &refs,
    )
    .await
    .unwrap_err();
    assert!(err.contains("metadata"), "{err}");

    let err = load(
        &config(json!({"cert_bundle": inside}), "acme-exec"),
        CALLER,
        &refs,
    )
    .await
    .unwrap_err();
    assert!(
        err.contains("acme-exec") && err.contains("--allow-subprocess-connectors"),
        "{err}"
    );

    load(
        &config(json!({"cert_bundle": "/etc/hosts"}), "acme-exec"),
        BodyOrigin::Trusted,
        &refs,
    )
    .await
    .expect("an operator-written body is not confined");
}

#[tokio::test]
async fn url_hosts_and_env_follow_the_flags() {
    install();
    let refs = ServeReferencePolicy::from_args(&ReferenceArgs {
        allow_url_host: vec!["feeds.example.com".into()],
        allow_env: vec!["FAUCET_881_ALLOWED".into()],
        ..Default::default()
    })
    .unwrap();
    load(
        &config(json!({"feed": "https://feeds.example.com/x"}), "acme-null"),
        CALLER,
        &refs,
    )
    .await
    .expect("a listed host loads");
    let err = load(
        &config(
            json!({"feed": "https://elsewhere.example.com/x"}),
            "acme-null",
        ),
        CALLER,
        &refs,
    )
    .await
    .unwrap_err();
    assert!(err.contains("elsewhere.example.com"), "{err}");

    // SAFETY: test-only variables, set before any load reads them.
    unsafe {
        std::env::set_var("FAUCET_881_ALLOWED", "ok");
        std::env::set_var("FAUCET_881_DENIED", "secret-value");
    }
    load(
        &config(json!({"label": "${env:FAUCET_881_ALLOWED}"}), "acme-null"),
        CALLER,
        &refs,
    )
    .await
    .expect("an allowed variable resolves");
    let err = load(
        &config(json!({"label": "${env:FAUCET_881_DENIED}"}), "acme-null"),
        CALLER,
        &refs,
    )
    .await
    .unwrap_err();
    assert!(
        err.contains("FAUCET_881_DENIED")
            && err.contains("--allow-env")
            && !err.contains("secret-value"),
        "{err}"
    );
    let err = load(
        &config(json!({"label": "${file:/etc/hosts}"}), "acme-null"),
        CALLER,
        &refs,
    )
    .await
    .unwrap_err();
    assert!(err.contains("${file:/etc/hosts}"), "{err}");
}
