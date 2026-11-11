//! A config a caller submits to `faucet serve` may not make a connector
//! authenticate with the server's own identity unless the server runs with
//! `--allow-host-credentials`, and a value a caller supplies for a template
//! param may not reach such a connector either. Connectors answer through
//! their descriptor (`with_host_identity`); a custom connector with no answer
//! is refused for callers. Inside a caller's run, a connector that checks at
//! build time (`require_host_credentials`) is refused the same way.
//!
//! Its own test binary: it installs the process-global plugin registry. The
//! tests run one at a time, since each server installs its reference policy
//! process-wide for the runs it executes.
#![allow(clippy::disallowed_methods)] // tests drive time and raw clients directly
#![cfg(feature = "templates")]

use std::collections::HashMap;
use std::time::Duration;

use faucet_cli::cli::{ReferenceArgs, ServeArgs};
use faucet_cli::registry::PluginRegistry;
use faucet_cli::serve::config::ServeConfig;
use faucet_core::capabilities::config::tag_at;
use faucet_core::{ConnectorDescriptor, FaucetError, Sink, Source, async_trait};
use serde_json::{Value, json};

#[path = "support/ports.rs"]
mod ports;
use ports::free_port;

/// A source that reads one record; with `probe: true` it asks, as a client
/// built from the server's identity would, whether that is allowed.
struct CloudFeed {
    probe: bool,
}

#[async_trait]
impl Source for CloudFeed {
    async fn fetch_with_context(
        &self,
        _: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        if self.probe {
            faucet_core::resolve::require_host_credentials("cloud-feed", "credentials")?;
        }
        Ok(vec![json!({"id": 1})])
    }
}

struct NullSink;

#[async_trait]
impl Sink for NullSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        Ok(records.len())
    }
}

/// `credentials` defaults to the server's identity; `access_key` names keys.
fn cloud_identity(config: &Value) -> Vec<String> {
    match tag_at(config, "/credentials").unwrap_or("default") {
        "access_key" => Vec::new(),
        _ => vec!["credentials".to_string()],
    }
}

fn no_identity(_: &Value) -> Vec<String> {
    Vec::new()
}

const CLOUD_FEED: ConnectorDescriptor =
    ConnectorDescriptor::new("cloud-feed", faucet_core::capabilities::plain_source)
        .with_host_identity(cloud_identity);
const CLOUD_OUT: ConnectorDescriptor =
    ConnectorDescriptor::new("cloud-out", faucet_core::capabilities::append_only_sink)
        .with_host_identity(cloud_identity);
const NULL_OUT: ConnectorDescriptor =
    ConnectorDescriptor::new("null-out", faucet_core::capabilities::append_only_sink)
        .with_host_identity(no_identity);

fn install() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let object = || json!({"type": "object"});
        PluginRegistry::with_builtins()
            .register_source_described(
                CLOUD_FEED,
                |c| {
                    let probe = c.get("probe").and_then(Value::as_bool).unwrap_or(false);
                    Ok(Box::new(CloudFeed { probe }) as Box<dyn Source>)
                },
                object,
                "source that may use the server's identity",
            )
            .register_source("silent-feed", |_| {
                Ok(Box::new(CloudFeed { probe: false }) as Box<dyn Source>)
            })
            .register_sink_described(
                CLOUD_OUT,
                |_| Ok(Box::new(NullSink) as Box<dyn Sink>),
                object,
                "sink that may use the server's identity",
            )
            .register_sink_described(
                NULL_OUT,
                |_| Ok(Box::new(NullSink) as Box<dyn Sink>),
                object,
                "sink that uses no identity of the server's",
            )
            .install()
            .expect("registry installs");
    });
}

fn args(port: u16, allow_host_credentials: bool) -> ServeArgs {
    ServeArgs {
        listen: format!("127.0.0.1:{port}"),
        auth_token: None,
        auth_config: None,
        read_token: None,
        write_token: None,
        admin_token: None,
        no_auth: true,
        allow_unauthenticated_network: false,
        max_concurrent_runs: Some(2),
        max_queued_runs: Some(16),
        default_config: None,
        history: Some("memory".into()),
        cors_origin: vec![],
        body_limit_bytes: 1_048_576,
        shutdown_grace_secs: 5,
        retain_terminal_runs_secs: 604_800,
        idempotency_retention_secs: 86_400,
        log_retention_secs: 604_800,
        log_max_lines_per_run: 100_000,
        log_buffer: Default::default(),
        local_output_retention_days: 7,
        local_output_in_flight_grace_secs: 60,
        preview_local_outputs: false,
        preview_default_rows: 500,
        preview_max_rows: 5_000,
        lease_ttl_secs: 30,
        probe_timeout_secs: 5,
        env_file: None,
        no_env_file: true,
        no_ui: true,
        cluster: false,
        cluster_poll_secs: 1,
        cluster_max_attempts: 3,
        triggers: None,
        schedules: Default::default(),
        templates_sync: None,
        policy: None,
        policy_store: Default::default(),
        otel_config: None,
        callback_allow_host: Vec::new(),
        origins: Default::default(),
        mcp: false,
        mcp_allow_mutations: false,
        require_approval: Vec::new(),
        approval_expiry_secs: 86_400,
        vault_key: None,
        vault_previous_key: Vec::new(),
        connect_providers: None,
        allow_subprocess_connectors: false,
        grafana_url: None,
        references: ReferenceArgs {
            allow_host_credentials,
            ..Default::default()
        },
        pools: Default::default(),
    }
}

struct Api {
    client: reqwest::Client,
    base: String,
}

impl Api {
    async fn get(&self, path: &str) -> (u16, Value) {
        let r = self
            .client
            .get(format!("{}{path}", self.base))
            .send()
            .await
            .unwrap();
        let code = r.status().as_u16();
        (code, r.json().await.unwrap_or(Value::Null))
    }

    async fn post(&self, path: &str, body: Value) -> (u16, Value) {
        let r = self
            .client
            .post(format!("{}{path}", self.base))
            .json(&body)
            .send()
            .await
            .unwrap();
        let code = r.status().as_u16();
        (code, r.json().await.unwrap_or(Value::Null))
    }

    async fn submit(&self, config: Value) -> (u16, Value) {
        self.post("/v1/runs", json!({ "config": config.to_string() }))
            .await
    }

    async fn wait_terminal(&self, run_id: &str) -> Value {
        for _ in 0..800 {
            let (_, r) = self.get(&format!("/v1/runs/{run_id}")).await;
            if matches!(
                r["status"].as_str(),
                Some("completed" | "failed" | "cancelled")
            ) {
                return r;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("run {run_id} did not finish");
    }
}

async fn start(allow_host_credentials: bool) -> Api {
    install();
    let port = free_port();
    let mut config = ServeConfig::from_args(args(port, allow_host_credentials)).unwrap();
    config.log_level = "warn".into();
    tokio::spawn(async move {
        let _ = faucet_cli::serve::run_server(config, Default::default()).await;
    });
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");
    for _ in 0..400 {
        if client
            .get(format!("{base}/healthz"))
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
        {
            return Api { client, base };
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("server did not come up");
}

fn pipeline(name: &str, source: Value, sink: Value) -> Value {
    json!({
        "version": 1,
        "name": name,
        "pipeline": { "source": source, "sink": sink }
    })
}

fn keys() -> Value {
    json!({"type": "access_key", "config": {"access_key_id": "AKIA-CALLER", "secret_access_key": "caller-secret-value"}})
}

fn null_out() -> Value {
    json!({"type": "null-out", "config": {}})
}

/// A pipeline template whose source uses the server's identity and whose
/// `prefix` param a caller may supply.
fn template(name: &str) -> String {
    json!({
        "version": 1,
        "kind": "pipeline",
        "name": name,
        "params": { "prefix": { "type": "string", "default": "exports/" } },
        "pipeline": {
            "source": {
                "type": "cloud-feed",
                "config": { "prefix": "${param.prefix}", "probe": true }
            },
            "sink": { "type": "null-out", "config": {} }
        }
    })
    .to_string()
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
async fn caller_configs_using_the_server_identity_are_refused() {
    let api = start(false).await;

    let ambient = pipeline(
        "ambient",
        json!({"type": "cloud-feed", "config": {"credentials": {"type": "profile", "config": {"profile_name": "server-profile"}}}}),
        null_out(),
    );
    let (code, v) = api.submit(ambient).await;
    assert_eq!(code, 403, "{v}");
    let text = v.to_string();
    assert!(text.contains("server's own identity"), "{text}");
    assert!(
        text.contains("source `cloud-feed` (`credentials`)"),
        "{text}"
    );
    assert!(text.contains("--allow-host-credentials"), "{text}");
    assert!(!text.contains("server-profile"), "never a value: {text}");

    let absent = pipeline(
        "absent",
        json!({"type": "cloud-feed", "config": {}}),
        null_out(),
    );
    let (code, v) = api.submit(absent).await;
    assert_eq!(code, 403, "an absent field is the default variant: {v}");

    let own = pipeline(
        "own-keys",
        json!({"type": "cloud-feed", "config": {"credentials": keys()}}),
        null_out(),
    );
    let (code, v) = api.submit(own).await;
    assert_eq!(code, 202, "{v}");
    let run = api.wait_terminal(v["run_id"].as_str().unwrap()).await;
    assert_eq!(run["status"], "completed", "{run}");

    let silent = pipeline(
        "silent",
        json!({"type": "silent-feed", "config": {}}),
        null_out(),
    );
    let (code, v) = api.submit(silent).await;
    assert_eq!(code, 403, "{v}");
    let text = v.to_string();
    assert!(text.contains("register_source_described"), "{text}");
    assert!(text.contains("with_host_identity"), "{text}");

    let dlq = json!({
        "version": 1,
        "name": "dlq",
        "pipeline": {
            "source": {"type": "cloud-feed", "config": {"credentials": keys()}},
            "sink": null_out(),
            "dlq": {"sink": {"type": "cloud-out", "config": {}}}
        }
    });
    let (code, v) = api.submit(dlq).await;
    assert_eq!(code, 403, "{v}");
    assert!(v.to_string().contains("dlq sink `cloud-out`"), "{v}");
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
async fn matrix_rows_that_supply_keys_are_judged_after_expansion() {
    let api = start(false).await;
    let rows = |second: Value| {
        json!({
            "version": 1,
            "name": "rows",
            "pipeline": {
                "source": {"type": "cloud-feed", "config": {}},
                "sink": null_out()
            },
            "matrix": [
                {"id": "a", "source": {"config": {"credentials": keys()}}},
                second
            ]
        })
    };
    let (code, v) = api.submit(rows(json!({"id": "b"}))).await;
    assert_eq!(code, 403, "{v}");
    let text = v.to_string();
    assert!(text.contains("row `b` source"), "{text}");
    assert!(!text.contains("row `a`"), "{text}");
    let (code, v) = api
        .submit(rows(
            json!({"id": "b", "source": {"config": {"credentials": keys()}}}),
        ))
        .await;
    assert_eq!(code, 202, "{v}");
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
async fn a_caller_param_reaching_the_server_identity_is_refused() {
    let api = start(false).await;
    let (code, v) = api
        .post("/v1/templates", json!({ "config": template("exports") }))
        .await;
    assert_eq!(code, 201, "{v}");
    let warnings = v["warnings"].to_string();
    assert!(warnings.contains("host credentials"), "{v}");
    assert!(warnings.contains("--allow-host-credentials"), "{v}");

    // The template's own defaults are the operator's: the server identity is allowed.
    let (code, v) = api
        .post("/v1/templates/exports/runs", json!({ "version": 1 }))
        .await;
    assert_eq!(code, 202, "{v}");
    let run = api.wait_terminal(v["run_id"].as_str().unwrap()).await;
    assert_eq!(run["status"], "completed", "{run}");

    let (code, v) = api
        .post(
            "/v1/templates/exports/runs",
            json!({ "version": 1, "params": { "prefix": "other-team/" } }),
        )
        .await;
    assert_eq!(code, 403, "{v}");
    let text = v.to_string();
    assert!(text.contains("param 'prefix'"), "{text}");
    assert!(text.contains("server's own identity"), "{text}");
    assert!(text.contains("--allow-host-credentials"), "{text}");
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
async fn a_connector_checking_at_build_time_is_refused_inside_a_caller_run() {
    let api = start(false).await;
    let probing = pipeline(
        "probing",
        json!({"type": "cloud-feed", "config": {"credentials": keys(), "probe": true}}),
        null_out(),
    );
    let (code, v) = api.submit(probing).await;
    assert_eq!(code, 202, "{v}");
    let run = api.wait_terminal(v["run_id"].as_str().unwrap()).await;
    assert_eq!(run["status"], "failed", "{run}");
    assert!(
        run.to_string().contains("--allow-host-credentials"),
        "{run}"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
async fn the_operator_flag_admits_the_server_identity_for_callers() {
    let api = start(true).await;
    let ambient = pipeline(
        "ambient-allowed",
        json!({"type": "cloud-feed", "config": {"probe": true}}),
        null_out(),
    );
    let (code, v) = api.submit(ambient).await;
    assert_eq!(code, 202, "{v}");
    let run = api.wait_terminal(v["run_id"].as_str().unwrap()).await;
    assert_eq!(run["status"], "completed", "{run}");

    let (code, v) = api
        .post(
            "/v1/templates",
            json!({ "config": template("exports-allowed") }),
        )
        .await;
    assert_eq!(code, 201, "{v}");
    assert!(
        !v["warnings"].to_string().contains("host credentials"),
        "{v}"
    );
    let (code, v) = api
        .post(
            "/v1/templates/exports-allowed/runs",
            json!({ "version": 1, "params": { "prefix": "other-team/" } }),
        )
        .await;
    assert_eq!(code, 202, "{v}");
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
async fn the_doctor_first_preflight_runs_in_the_runs_scope() {
    let api = start(true).await;
    let ambient = pipeline(
        "preflight-allowed",
        json!({"type": "cloud-feed", "config": {"probe": true}}),
        null_out(),
    );
    let (code, v) = api
        .post(
            "/v1/runs",
            json!({ "config": ambient.to_string(), "doctor_first": true }),
        )
        .await;
    assert_eq!(code, 202, "{v}");
    let run = api.wait_terminal(v["run_id"].as_str().unwrap()).await;
    assert_eq!(run["status"], "completed", "{run}");

    let api = start(false).await;
    let probing = pipeline(
        "preflight-refused",
        json!({"type": "cloud-feed", "config": {"credentials": keys(), "probe": true}}),
        null_out(),
    );
    let (code, v) = api
        .post(
            "/v1/runs",
            json!({ "config": probing.to_string(), "doctor_first": true }),
        )
        .await;
    assert_eq!(code, 422, "{v}");
    assert!(v.to_string().contains("--allow-host-credentials"), "{v}");
}
