//! Owner-scoped template ids (`acme/billing`, #682) over HTTP: the `/` in an id
//! is percent-encoded into one path segment (`/v1/templates/acme%2Fbilling`),
//! which every per-template route accepts; the raw two-segment form is not a
//! template route.

#![cfg(feature = "templates")]

use std::time::Duration;

use faucet_cli::cli::ServeArgs;
use faucet_cli::serve::config::ServeConfig;
use serde_json::{Value, json};

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn args(port: u16) -> ServeArgs {
    ServeArgs {
        listen: format!("127.0.0.1:{port}"),
        auth_token: None,
        auth_config: None,
        read_token: None,
        write_token: None,
        admin_token: None,
        no_auth: true,
        max_concurrent_runs: Some(2),
        max_queued_runs: Some(8),
        default_config: None,
        history: None,
        cors_origin: vec![],
        body_limit_bytes: 1_048_576,
        shutdown_grace_secs: 5,
        retain_terminal_runs_secs: 604_800,
        idempotency_retention_secs: 86_400,
        log_retention_secs: 604_800,
        log_max_lines_per_run: 100_000,
        local_output_retention_days: 7,
        local_output_in_flight_grace_secs: 60,
        preview_local_outputs: false,
        preview_default_rows: 500,
        preview_max_rows: 5_000,
        lease_ttl_secs: 30,
        probe_timeout_secs: 5,
        env_file: None,
        no_env_file: true,
        no_ui: false,
        cluster: false,
        cluster_poll_secs: 2,
        cluster_max_attempts: 3,
        triggers: None,
        templates_sync: None,
        policy: None,
        callback_allow_host: Vec::new(),
        mcp: false,
        mcp_allow_mutations: false,
        require_approval: Vec::new(),
        approval_expiry_secs: 86_400,
        vault_key: None,
        vault_previous_key: Vec::new(),
        connect_providers: None,
    }
}

async fn start(port: u16) -> (reqwest::Client, String) {
    let mut config = ServeConfig::from_args(args(port)).unwrap();
    config.log_level = "warn".into();
    tokio::spawn(async move {
        let _ = faucet_cli::serve::run_server(config, Default::default()).await;
    });
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");
    let mut up = false;
    for _ in 0..400 {
        if client
            .get(format!("{base}/healthz"))
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
        {
            up = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(up, "server did not come up");
    (client, base)
}

fn source(dir: &std::path::Path) -> String {
    std::fs::write(dir.join("invoices.csv"), "id,total\n1,10\n").unwrap();
    format!(
        "kind: source-template\nname: billing\nowner: acme\ndescription: Acme billing exports\nparams:\n  data_dir: {{ type: string, default: {} }}\nsource:\n  type: csv\n  config:\n    path: \"${{param.data_dir}}/invoices.csv\"\nstreams:\n  - name: invoices\n    write: [append]\n",
        dir.display()
    )
}

fn sink(dir: &std::path::Path) -> String {
    format!(
        "kind: sink-template\nname: files\nowner: faucet-hq\ndescription: Local JSON Lines\nparams:\n  out_dir: {{ type: string, default: {} }}\nsink:\n  type: jsonl\n  config: {{}}\nper_stream:\n  path: \"${{param.out_dir}}/${{owner}}/${{source}}/${{stream}}.jsonl\"\n",
        dir.display()
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn owner_ids_are_addressed_as_one_percent_encoded_segment() {
    let dir = tempfile::tempdir().unwrap();
    let (client, base) = start(free_port()).await;
    for body in [source(dir.path()), sink(dir.path())] {
        let r = client
            .post(format!("{base}/v1/templates"))
            .json(&json!({ "config": body, "launch": true }))
            .send()
            .await
            .unwrap();
        assert!(r.status().is_success(), "{}", r.text().await.unwrap());
    }

    let got: Value = client
        .get(format!("{base}/v1/templates/acme%2Fbilling"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(got["id"], "acme/billing");

    let r = client
        .post(format!("{base}/v1/templates/acme%2Fbilling/runs"))
        .json(&json!({ "sink": "faucet-hq/files" }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 202, "{}", r.text().await.unwrap());
    let run: Value = r.json().await.unwrap();
    assert_eq!(run["template_id"], "acme/billing");
    assert_eq!(run["sink_template"], "faucet-hq/files");

    // The unencoded form is two path segments, so it names no template route.
    let raw = client
        .get(format!("{base}/v1/templates/acme/billing"))
        .send()
        .await
        .unwrap();
    assert!(!raw.status().is_success(), "{}", raw.status());
}
