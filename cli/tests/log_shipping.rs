//! Log shipping over OTLP from the local spool (#806): `faucet run`,
//! `faucet schedule` and `faucet logs ship` against a mock collector (gRPC and
//! HTTP/protobuf), including a collector outage, the spool bounds and secret
//! redaction. Runs in its own binary: it installs the CLI's capture layer as the
//! process-global subscriber.
#![cfg(all(feature = "otel", feature = "schedule"))]

#[path = "support/otlp_mock.rs"]
mod otlp_mock;

use clap::Parser;
use faucet_cli::cli::Cli;
use faucet_cli::logship::LogExportStatus;
use faucet_cli::logship::spool::Spool;
use otlp_mock::{start_grpc, start_http, wait_for};
use serial_test::serial;
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::OnceLock;

fn install_subscriber() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;
        let _ = tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::new("info"))
            .with(faucet_cli::logship::session::SpoolLayer)
            .try_init();
    });
}

fn write_config(
    dir: &Path,
    endpoint: &str,
    protocol: &str,
    spool: &Path,
    extra: &str,
) -> std::path::PathBuf {
    let input = dir.join("in.csv");
    std::fs::write(&input, "id,name\n1,alice\n2,bob\n3,carol\n").unwrap();
    let output = dir.join("out.jsonl");
    let cfg = format!(
        "version: 1\nname: shipped\npipeline:\n  source: {{ type: csv, config: {{ path: \"{}\" }} }}\n  sink: {{ type: jsonl, config: {{ path: \"{}\" }} }}\nobservability:\n  otel:\n    endpoint: \"{endpoint}\"\n    protocol: {protocol}\n    export: [logs]\n    service_name: faucet-test\n    timeout_secs: 2\n  logs:\n    spool_dir: \"{}\"\n    flush_timeout_secs: 3\n{extra}",
        input.display(),
        output.display(),
        spool.display(),
    );
    let path = dir.join("faucet.yaml");
    std::fs::write(&path, cfg).unwrap();
    path
}

async fn faucet(args: &[&str]) -> faucet_cli::CliResult<()> {
    let mut argv = vec!["faucet"];
    argv.extend_from_slice(args);
    faucet_cli::run_command(Cli::parse_from(argv)).await
}

fn seqs(recs: &[otlp_mock::Record]) -> Vec<u64> {
    recs.iter()
        .map(|r| r.attr("faucet.seq").unwrap().parse().unwrap())
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn run_ships_every_line_with_its_attributes_over_grpc() {
    install_subscriber();
    let (col, ep) = start_grpc().await;
    let dir = tempfile::tempdir().unwrap();
    let spool = dir.path().join("spool");
    let cfg = write_config(dir.path(), &ep, "grpc", &spool, "");
    faucet(&["run", cfg.to_str().unwrap()]).await.unwrap();

    let recs = col.records();
    assert!(!recs.is_empty(), "the collector received the run's lines");
    let runs: BTreeSet<&str> = recs.iter().filter_map(|r| r.attr("run_id")).collect();
    assert_eq!(runs.len(), 1, "one `faucet run` is one run: {runs:?}");
    let run_id = runs.into_iter().next().unwrap().to_string();
    assert!(recs.iter().all(|r| r.service == "faucet-test"));
    assert!(recs.iter().all(|r| r.attr("pipeline").is_some()));
    assert!(recs.iter().all(|r| r.attr("target").is_some()));
    assert!(recs.iter().all(|r| r.severity_number > 0));
    let in_pipeline = recs
        .iter()
        .find(|r| r.body.contains("pipeline streaming run complete"))
        .expect("the pipeline's completion line was shipped");
    assert!(in_pipeline.attr("row").is_some());
    assert!(in_pipeline.attr("invocation_id").is_some());
    assert!(
        in_pipeline
            .scope_attrs
            .contains_key("faucet.batch.first_seq")
    );

    // Every captured line went out exactly once, in order.
    let s = seqs(&recs);
    let unique: BTreeSet<u64> = s.iter().copied().collect();
    assert_eq!(unique.len(), s.len(), "no line was sent twice");
    let sp = Spool::open(&spool).unwrap();
    let meta = sp.read_meta(&run_id).unwrap();
    assert_eq!(s.len() as u64, meta.last_seq);
    assert!(meta.ended_at.is_some());
    let cursor = sp.read_cursor(&run_id);
    assert_eq!(cursor.delivered_seq, Some(meta.last_seq));
    let view = faucet_cli::logship::spool::run_view(&sp, &run_id).unwrap();
    assert_eq!(view.status, LogExportStatus::Exported);
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn collector_down_then_logs_ship_delivers_each_line_once_over_http() {
    install_subscriber();
    let (col, ep, _server) = start_http().await;
    col.set_down(true);
    let dir = tempfile::tempdir().unwrap();
    let spool = dir.path().join("spool");
    let cfg = write_config(dir.path(), &ep, "http", &spool, "");
    // The run itself never fails because the collector is down.
    faucet(&["run", cfg.to_str().unwrap()]).await.unwrap();
    assert_eq!(col.request_count(), 0);
    let sp = Spool::open(&spool).unwrap();
    let ids = sp.run_ids();
    assert_eq!(ids.len(), 1);
    let view = faucet_cli::logship::spool::run_view(&sp, &ids[0]).unwrap();
    assert_eq!(view.status, LogExportStatus::Failed);
    assert!(view.pending_lines > 0);
    assert!(view.last_error.is_some());

    // Still down: `faucet logs ship` reports the run as undelivered.
    let err = faucet(&[
        "logs",
        "ship",
        cfg.to_str().unwrap(),
        "--spool",
        spool.to_str().unwrap(),
        "--timeout-secs",
        "2",
    ])
    .await
    .unwrap_err();
    assert!(matches!(
        err,
        faucet_cli::CliError::LogsUndelivered { runs: 1 }
    ));

    // Back up: everything ships, once.
    col.set_down(false);
    faucet(&[
        "logs",
        "ship",
        "--spool",
        spool.to_str().unwrap(),
        "--endpoint",
        &ep,
        "--protocol",
        "http",
        "--json",
    ])
    .await
    .unwrap();
    let recs = col.records();
    let s = seqs(&recs);
    let meta = sp.read_meta(&ids[0]).unwrap();
    assert_eq!(
        s.len() as u64,
        meta.last_seq,
        "every buffered line delivered"
    );
    assert_eq!(
        s.iter().copied().collect::<BTreeSet<_>>().len(),
        s.len(),
        "exactly once"
    );
    // A second ship sends nothing more.
    faucet(&[
        "logs",
        "ship",
        "--spool",
        spool.to_str().unwrap(),
        "--endpoint",
        &ep,
        "--protocol",
        "http",
    ])
    .await
    .unwrap();
    assert_eq!(seqs(&col.records()).len(), s.len());
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn schedule_ticks_are_runs_and_recover_after_an_outage() {
    install_subscriber();
    let (col, ep) = start_grpc().await;
    col.set_down(true);
    let dir = tempfile::tempdir().unwrap();
    let spool = dir.path().join("spool");
    let cfg = write_config(
        dir.path(),
        &ep,
        "grpc",
        &spool,
        "schedule:\n  cron: \"0 0 * * *\"\n",
    );
    faucet(&["schedule", cfg.to_str().unwrap(), "--once"])
        .await
        .unwrap();
    let sp = Spool::open(&spool).unwrap();
    let ids = sp.run_ids();
    assert_eq!(ids.len(), 1, "the tick is one spool run");
    assert_eq!(col.request_count(), 0);

    col.set_down(false);
    faucet(&[
        "logs",
        "ship",
        cfg.to_str().unwrap(),
        "--spool",
        spool.to_str().unwrap(),
    ])
    .await
    .unwrap();
    let recs = col.records_where("run_id", &ids[0]);
    assert!(
        recs.iter()
            .any(|r| r.body.contains("pipeline streaming run complete")),
        "the tick's pipeline lines are tagged with its run id: {:?} meta {:?} all {:?}",
        recs.iter().map(|r| &r.body).collect::<Vec<_>>(),
        sp.read_meta(&ids[0]),
        col.records()
            .iter()
            .map(|r| (&r.body, r.attr("run_id")))
            .collect::<Vec<_>>()
    );
    let meta = sp.read_meta(&ids[0]).unwrap();
    assert_eq!(seqs(&recs).len() as u64, meta.last_seq);
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn the_background_shipper_drains_once_the_collector_returns() {
    use faucet_cli::logship::LogsSpec;
    use faucet_cli::logship::session::LogShipSession;
    install_subscriber();
    let (col, ep) = start_grpc().await;
    col.set_down(true);
    let dir = tempfile::tempdir().unwrap();
    let cfg_path = write_config(dir.path(), &ep, "grpc", &dir.path().join("spool"), "");
    let cfg = faucet_cli::config::PipelineConfig::from_path(&cfg_path, None).unwrap();
    let session = LogShipSession::start(&cfg, "bg").unwrap();
    session.begin_run("bg-run", Default::default());
    session.set_default_run(Some("bg-run"));
    faucet_cli::secrets::registry::register("hunter2-shipping-secret");
    for i in 0..5 {
        tracing::info!(i, "background line hunter2-shipping-secret");
    }
    session.set_default_run(None);
    session.end_run("bg-run");
    session
        .flush_writer(std::time::Duration::from_secs(2))
        .await;
    tokio::time::sleep(std::time::Duration::from_millis(2500)).await;
    assert_eq!(col.request_count(), 0, "nothing acknowledged while down");
    col.set_down(false);
    assert!(
        wait_for(15, || col.records_where("run_id", "bg-run").len() == 5).await,
        "the background shipper delivered after the outage"
    );
    // The registered secret never reached the spool or the collector.
    let spooled = std::fs::read_to_string(session.spool().segment("bg-run")).unwrap();
    assert!(!spooled.contains("hunter2-shipping-secret"));
    assert!(!col.raw().contains("hunter2-shipping-secret"));
    session.shutdown().await;

    // A spool-only session (no exporter) keeps lines buffered.
    let quiet = LogShipSession::with_exporter(
        LogsSpec {
            spool_dir: Some(dir.path().join("quiet")),
            flush_timeout_secs: 1,
            ..Default::default()
        },
        None,
        "quiet",
        &cfg,
    )
    .unwrap();
    quiet.begin_run("q", Default::default());
    quiet.set_default_run(Some("q"));
    tracing::info!("kept locally");
    quiet.set_default_run(None);
    let view = quiet.finish_run("q").await;
    assert_eq!(view.status, LogExportStatus::Pending);
    assert_eq!(view.pending_lines, 1);
    drop(quiet);
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn topology_runs_and_json_log_format_report_log_export() {
    install_subscriber();
    let (col, ep) = start_grpc().await;
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    std::fs::write(&input, "id\n1\n").unwrap();
    let spool = dir.path().join("spool");
    let topo = format!(
        "version: 1\nname: topo\npipeline:\n  sources:\n    src: {{ type: csv, config: {{ path: \"{}\" }} }}\n  sinks:\n    out: {{ type: jsonl, config: {{ path: \"{}\" }} }}\n  nodes:\n    read: {{ kind: source, ref: src }}\n    write: {{ kind: sink, ref: out }}\n  edges:\n    - {{ from: read, to: write }}\nobservability:\n  otel: {{ endpoint: \"{ep}\", export: [logs] }}\n  logs: {{ spool_dir: \"{}\", flush_timeout_secs: 3 }}\n",
        input.display(),
        dir.path().join("t.jsonl").display(),
        spool.display()
    );
    let path = dir.path().join("topo.yaml");
    std::fs::write(&path, topo).unwrap();
    faucet(&["run", path.to_str().unwrap()]).await.unwrap();
    faucet(&["run", path.to_str().unwrap(), "--output", "json"])
        .await
        .unwrap();
    let cfg = write_config(dir.path(), &ep, "grpc", &spool, "");
    faucet_cli::cli::set_log_format(faucet_cli::cli::LogFormat::Json);
    let r = faucet(&["run", cfg.to_str().unwrap()]).await;
    faucet_cli::cli::set_log_format(faucet_cli::cli::LogFormat::Text);
    r.unwrap();
    assert!(
        col.records()
            .iter()
            .any(|r| r.attr("pipeline") == Some("topo"))
    );
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn logs_ship_notifies_through_the_configs_channels() {
    install_subscriber();
    let hook = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .respond_with(wiremock::ResponseTemplate::new(200))
        .mount(&hook)
        .await;
    let (col, ep) = start_grpc().await;
    col.set_down(true);
    let dir = tempfile::tempdir().unwrap();
    let spool = dir.path().join("spool");
    let extra = format!(
        "    notify_after_secs: 0\nnotifications:\n  - name: hook\n    on: [log_export_failed]\n    channel: {{ type: webhook, config: {{ url: \"{}/h\" }} }}\n",
        hook.uri()
    );
    let cfg = write_config(dir.path(), &ep, "grpc", &spool, &extra);
    faucet(&["run", cfg.to_str().unwrap()]).await.unwrap();
    let _ = faucet(&["logs", "ship", cfg.to_str().unwrap(), "--timeout-secs", "1"]).await;
    assert!(
        !hook.received_requests().await.unwrap().is_empty(),
        "a failing export notified"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn run_output_json_reports_log_export() {
    install_subscriber();
    let (col, ep) = start_grpc().await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path(), &ep, "grpc", &dir.path().join("spool"), "");
    faucet(&["run", cfg.to_str().unwrap(), "--output", "json"])
        .await
        .unwrap();
    faucet(&["run", cfg.to_str().unwrap()]).await.unwrap();
    assert!(col.request_count() >= 2);
}
