//! Two `faucet run` processes of one row started together (#829): exactly
//! one runs, the other is refused by the row's run lease before it reads
//! the source, so no record is written twice.
#![cfg(all(feature = "source-rest", feature = "sink-file"))]

use std::process::{Command, Output};
use std::time::Duration;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn faucet_run(config: &std::path::Path) -> std::process::Child {
    Command::new(env!("CARGO_BIN_EXE_faucet"))
        .args(["run", "--no-env-file", "--output", "json"])
        .arg(config)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn faucet run")
}

fn text(o: &Output) -> String {
    format!(
        "status={:?}\nstdout={}\nstderr={}",
        o.status,
        String::from_utf8_lossy(&o.stdout),
        String::from_utf8_lossy(&o.stderr)
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn two_concurrent_runs_of_one_row_run_once() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"data": [{"id": 1}, {"id": 2}, {"id": 3}]}))
                // Long enough that the second process starts while the first
                // still holds the lease.
                .set_delay(Duration::from_secs(3)),
        )
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out.jsonl");
    let config = dir.path().join("leased.yaml");
    std::fs::write(
        &config,
        format!(
            "version: 1\nname: leased\npipeline:\n  source:\n    type: rest\n    config:\n      \
             base_url: {}\n      path: /items\n      records_path: \"$.data[*]\"\n      \
             pagination: {{ type: None }}\n  sink:\n    type: file\n    config:\n      \
             path: {}\n      mode: append\n  state:\n    type: file\n    config:\n      \
             path: {}\n",
            server.uri(),
            out.display(),
            dir.path().join("state").display(),
        ),
    )
    .unwrap();

    let a = faucet_run(&config);
    let b = faucet_run(&config);
    let (a, b) = tokio::task::spawn_blocking(move || {
        (a.wait_with_output().unwrap(), b.wait_with_output().unwrap())
    })
    .await
    .unwrap();

    let (ok, refused) = match (a.status.success(), b.status.success()) {
        (true, false) => (&a, &b),
        (false, true) => (&b, &a),
        _ => panic!(
            "exactly one run must succeed\n--- a\n{}\n--- b\n{}",
            text(&a),
            text(&b)
        ),
    };
    let report: serde_json::Value = serde_json::from_slice(&refused.stdout)
        .unwrap_or_else(|e| panic!("{e}: {}", text(refused)));
    let error = report["rows"][0]["error"].as_str().unwrap_or_default();
    assert!(
        error.contains("--force"),
        "the refused run names the lease and its holder: {}",
        text(refused)
    );
    let holder: serde_json::Value = serde_json::from_slice(&ok.stdout).unwrap();
    let holder_run = holder["rows"][0]["run_id"].as_str().unwrap();
    assert!(
        error.contains(holder_run),
        "the refusal names the run holding the lease ({holder_run}): {error}"
    );

    let written = std::fs::read_to_string(&out).unwrap();
    assert_eq!(written.lines().count(), 3, "no duplicate rows: {written}");
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1, "the refused run never read the source");

    // The lease was released: the next run starts without `--force`.
    let again = faucet_run(&config);
    let again = tokio::task::spawn_blocking(move || again.wait_with_output().unwrap())
        .await
        .unwrap();
    assert!(again.status.success(), "{}", text(&again));
}
