//! Template hosting + sync over HTTP (RFC 0006 / #589): a `faucet serve`
//! started with `--templates-sync` pulls a mock GitHub origin on start,
//! advertises its origins on `GET /v1/templates`, re-pulls on
//! `POST /v1/templates/sync` (dry-run and real), and writes a version back on
//! `POST /v1/templates/{id}/publish`. A server without origins refuses both
//! with a 422 that names the flag.

#![cfg(feature = "templates-sync")]

use std::time::Duration;

use faucet_cli::cli::ServeArgs;
use faucet_cli::serve::config::ServeConfig;
use serde_json::{Value, json};
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

const BODY: &str = "version: 1\nname: nightly\npipeline:\n  source: {type: rest, config: {base_url: \"https://x\", path: /e}}\n  sink: {type: stdout, config: {}}\n";

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn args(port: u16, sync: Option<std::path::PathBuf>) -> ServeArgs {
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
        templates_sync: sync,
        policy: None,
        callback_allow_host: Vec::new(),
        mcp: false,
        mcp_allow_mutations: false,
        require_approval: Vec::new(),
        approval_expiry_secs: 86_400,
    }
}

async fn start(port: u16, sync: Option<std::path::PathBuf>) -> (reqwest::Client, String) {
    let mut config = ServeConfig::from_args(args(port, sync)).unwrap();
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

/// Mount a GitHub contents-API directory listing + raw reads.
async fn mount_repo(server: &MockServer, files: &[(&str, &str)]) {
    let entries: Vec<Value> = files
        .iter()
        .map(|(n, _)| {
            json!({
                "name": n, "type": "file", "sha": "abc",
                "url": format!("{}/repos/acme/tpl/contents/templates/{n}?ref=main", server.uri()),
            })
        })
        .collect();
    Mock::given(method("GET"))
        .and(path("/repos/acme/tpl/contents/templates"))
        .and(query_param("ref", "main"))
        .and(header("Authorization", "Bearer ghp_test_token_value"))
        .respond_with(ResponseTemplate::new(200).set_body_json(entries))
        .mount(server)
        .await;
    for (n, body) in files {
        Mock::given(method("GET"))
            .and(path(format!("/repos/acme/tpl/contents/templates/{n}")))
            .and(header("Accept", "application/vnd.github.raw+json"))
            .respond_with(ResponseTemplate::new(200).set_body_string(*body))
            .mount(server)
            .await;
    }
}

fn sync_file(dir: &std::path::Path, api: &str) -> std::path::PathBuf {
    let p = dir.join("sync.yaml");
    std::fs::write(
        &p,
        format!(
            "version: 1\norigins:\n  - name: gh\n    prefix: plat-\n    launch: follow\n    prune: deprecate\n    source:\n      type: github\n      config:\n        repo: acme/tpl\n        path: templates\n        api_base: \"{api}\"\n        token: \"${{env:FAUCET_TEST_GH_TOKEN}}\"\n"
        ),
    )
    .unwrap();
    p
}

#[tokio::test(flavor = "multi_thread")]
async fn server_pulls_on_start_then_syncs_and_publishes_over_http() {
    // SAFETY (test): the variable is private to this test binary.
    unsafe { std::env::set_var("FAUCET_TEST_GH_TOKEN", "ghp_test_token_value") };
    let gh = MockServer::start().await;
    mount_repo(
        &gh,
        &[
            ("nightly.yaml", BODY),
            (
                "nightly.faucet.yaml",
                "launch: true\ndescription: Nightly sync",
            ),
            ("draft.yaml", BODY),
        ],
    )
    .await;
    let dir = tempfile::tempdir().unwrap();
    let sync = sync_file(dir.path(), &gh.uri());
    let (client, base) = start(free_port(), Some(sync)).await;

    // Pulled on start; the list advertises the origins.
    let list: Value = client
        .get(format!("{base}/v1/templates"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let ids: Vec<&str> = list["templates"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["id"].as_str().unwrap())
        .collect();
    assert!(
        ids.contains(&"plat-nightly") && ids.contains(&"plat-draft"),
        "{ids:?}"
    );
    let origins = list["sync"]["origins"]
        .as_array()
        .expect("sync.origins advertised");
    assert_eq!(origins.len(), 1);
    assert_eq!(origins[0]["name"], "gh");
    assert_eq!(origins[0]["kind"], "github");
    assert_eq!(origins[0]["prefix"], "plat-");
    assert_eq!(origins[0]["launch"], "follow");
    assert_eq!(origins[0]["prune"], "deprecate");
    assert!(origins[0].get("interval_secs").is_none());

    // The sidecar launched `nightly`; `draft` stayed a draft.
    let nightly: Value = client
        .get(format!("{base}/v1/templates/plat-nightly"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(nightly["status"], "launched", "{nightly}");
    assert_eq!(nightly["description"], "Nightly sync");
    assert_eq!(nightly["created_by"], "sync:gh");
    let draft = client
        .get(format!("{base}/v1/templates/plat-draft?version=newest"))
        .send()
        .await
        .unwrap();
    assert_eq!(draft.status(), 200);
    assert_eq!(draft.json::<Value>().await.unwrap()["status"], "draft");

    // A dry run re-plans without changing anything: everything is unchanged.
    let dry: Value = client
        .post(format!("{base}/v1/templates/sync"))
        .json(&json!({ "dry_run": true }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(dry["dry_run"], true);
    assert_eq!(dry["origin_errors"], json!([]));
    let plan = dry["reports"][0]["plan"].as_array().unwrap();
    assert!(plan.iter().all(|a| a["action"] == "unchanged"), "{plan:?}");
    assert!(dry["reports"][0].get("outcome").is_none());

    // Upstream changes: `draft` vanishes (→ deprecate), `nightly` changes body
    // (→ new version, launched by the sidecar).
    gh.reset().await;
    let changed = BODY.replace("path: /e", "path: /e2");
    mount_repo(
        &gh,
        &[
            ("nightly.yaml", &changed),
            ("nightly.faucet.yaml", "launch: true"),
        ],
    )
    .await;
    // An empty body is accepted (defaults).
    let res = client
        .post(format!("{base}/v1/templates/sync"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let real: Value = res.json().await.unwrap();
    assert_eq!(real["dry_run"], false);
    let outcome = &real["reports"][0]["outcome"];
    assert_eq!(outcome["registered"][0]["id"], "plat-nightly");
    assert_eq!(outcome["registered"][0]["version"], 2);
    assert_eq!(outcome["deprecated"], json!(["plat-draft"]));
    assert_eq!(outcome["failed"], json!([]));
    let nightly: Value = client
        .get(format!("{base}/v1/templates/plat-nightly"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(nightly["version"], 2, "stable follows the sidecar");
    // An HTTP-triggered register is attributed to the principal, not `sync:`.
    assert_ne!(nightly["created_by"], "sync:gh");
    let draft: Value = client
        .get(format!("{base}/v1/templates/plat-draft?version=newest"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(draft["status"], "deprecated");

    // `origin` filter: unknown name is a 422; known name is honoured.
    let res = client
        .post(format!("{base}/v1/templates/sync"))
        .json(&json!({ "origin": "nope" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 422);
    let res: Value = client
        .post(format!("{base}/v1/templates/sync"))
        .json(&json!({ "origin": "gh", "dry_run": true }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(res["reports"].as_array().unwrap().len(), 1);

    // Publish v1 of nightly back to the origin.
    Mock::given(method("GET"))
        .and(path("/repos/acme/tpl/contents/templates/nightly.yaml"))
        .and(header("Accept", "application/vnd.github+json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "name": "nightly.yaml", "type": "file", "sha": "oldsha", "url": "u"
        })))
        .mount(&gh)
        .await;
    Mock::given(method("PUT"))
        .and(path("/repos/acme/tpl/contents/templates/nightly.yaml"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "content": { "html_url": "https://github.com/acme/tpl/blob/main/templates/nightly.yaml" }
        })))
        .mount(&gh)
        .await;
    let res = client
        .post(format!("{base}/v1/templates/plat-nightly/publish"))
        .json(&json!({ "origin": "gh", "version": 1 }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let pub_res: Value = res.json().await.unwrap();
    assert_eq!(pub_res["version"], 1);
    assert_eq!(pub_res["name"], "nightly.yaml");
    assert_eq!(pub_res["origin"], "gh");
    assert!(
        pub_res["location"]
            .as_str()
            .unwrap()
            .ends_with("nightly.yaml")
    );
    let puts: Vec<_> = gh
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.method == "PUT")
        .collect();
    assert_eq!(puts.len(), 1);
    let payload: Value = serde_json::from_slice(&puts[0].body).unwrap();
    assert_eq!(payload["sha"], "oldsha", "an update carries the blob sha");

    // Publishing an id outside the origin's namespace / a missing template.
    let res = client
        .post(format!("{base}/v1/templates/elsewhere/publish"))
        .json(&json!({ "origin": "gh" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 422);
    let res = client
        .post(format!("{base}/v1/templates/plat-missing/publish"))
        .json(&json!({ "origin": "gh" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 404);

    // Audit trail carries both actions.
    let audit: Value = client
        .get(format!("{base}/v1/audit?limit=50"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let actions: Vec<&str> = audit["entries"]
        .as_array()
        .expect("entries")
        .iter()
        .filter_map(|r| r["action"].as_str())
        .collect();
    assert!(actions.contains(&"template.sync"), "{actions:?}");
    assert!(actions.contains(&"template.publish"), "{actions:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_server_without_origins_refuses_sync_and_publish() {
    let (client, base) = start(free_port(), None).await;
    let list: Value = client
        .get(format!("{base}/v1/templates"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(list.get("sync").is_none(), "no origins → no `sync` block");
    let res = client
        .post(format!("{base}/v1/templates/sync"))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 422);
    let text = res.text().await.unwrap();
    assert!(text.contains("--templates-sync"), "{text}");
    let res = client
        .post(format!("{base}/v1/templates/x/publish"))
        .json(&json!({ "origin": "gh" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 422);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_invalid_sync_file_fails_startup() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("bad.yaml");
    std::fs::write(
        &p,
        "version: 1\norigins:\n  - {name: a, prefix: x-, source: {type: github, config: {repo: a/b}}}\n  - {name: b, prefix: x-, source: {type: github, config: {repo: a/c}}}\n",
    )
    .unwrap();
    let mut config = ServeConfig::from_args(args(free_port(), Some(p))).unwrap();
    config.log_level = "warn".into();
    let err = faucet_cli::serve::run_server(config, Default::default())
        .await
        .expect_err("overlapping prefixes must refuse to start");
    assert!(err.to_string().contains("overlapping"), "{err}");
}
