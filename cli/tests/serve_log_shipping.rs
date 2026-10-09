//! `faucet serve` log shipping (#806): the run-history log store as the
//! durable buffer, the delivery watermark + lease in both history backends,
//! delivery-aware retention and bounds, cluster takeover on SQLite, a restart
//! mid-outage, and the run API / log endpoint surfaces against a mock OTLP
//! collector.
#![cfg(all(feature = "serve", feature = "otel", feature = "serve-history-sqlite"))]

#[path = "support/otlp_mock.rs"]
mod otlp_mock;

use faucet_cli::cli::ServeArgs;
use faucet_cli::logship::LogExportStatus;
use faucet_cli::serve::ServeConfig;
use faucet_cli::serve::history::memory::MemoryHistory;
use faucet_cli::serve::history::sqlite::SqliteHistory;
use faucet_cli::serve::history::{RunHistory, RunLogLine, RunRecord};
use faucet_cli::serve::log_export::{LogExport, seq_at};
use otlp_mock::{start_grpc, start_http};
use std::collections::BTreeMap;
use std::time::Duration;

fn args(port: u16) -> ServeArgs {
    ServeArgs {
        listen: format!("127.0.0.1:{port}"),
        auth_token: None,
        auth_config: None,
        read_token: None,
        write_token: None,
        admin_token: None,
        no_auth: true,
        max_concurrent_runs: Some(4),
        max_queued_runs: Some(16),
        default_config: None,
        history: None,
        cors_origin: vec![],
        body_limit_bytes: 1_048_576,
        shutdown_grace_secs: 5,
        retain_terminal_runs_secs: 604_800,
        idempotency_retention_secs: 86_400,
        log_retention_secs: 86_400,
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
        cluster_poll_secs: 2,
        cluster_max_attempts: 3,
        triggers: None,
        templates_sync: None,
        policy: None,
        otel_config: None,
        callback_allow_host: Vec::new(),
        mcp: false,
        mcp_allow_mutations: false,
        require_approval: Vec::new(),
        approval_expiry_secs: 86_400,
        vault_key: None,
        vault_previous_key: Vec::new(),
        connect_providers: None,
        allow_subprocess_connectors: false,
    }
}

fn otel(endpoint: &str, protocol: faucet_core::OtelProtocol) -> faucet_core::OtelConfig {
    let mut c: faucet_core::OtelConfig =
        serde_json::from_value(serde_json::json!({ "export": ["logs"], "timeout_secs": 2 }))
            .unwrap();
    c.endpoint = endpoint.to_string();
    c.protocol = protocol;
    c
}

fn export_config(otel: Option<faucet_core::OtelConfig>) -> ServeConfig {
    let mut c = ServeConfig::from_args(args(0)).unwrap();
    c.otel = otel;
    c.lease_ttl = Duration::from_secs(30);
    c
}

async fn sqlite(path: &std::path::Path, instance: &str, lease: Duration) -> SqliteHistory {
    SqliteHistory::connect(
        &format!("sqlite:{}", path.display()),
        Duration::from_secs(3600),
        lease,
        instance.to_string(),
    )
    .await
    .unwrap()
}

fn line(seq: u64, text: &str) -> RunLogLine {
    let ts = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let mut attrs = BTreeMap::new();
    attrs.insert("target".to_string(), "faucet_core::pipeline".to_string());
    attrs.insert("row".to_string(), "r1".to_string());
    RunLogLine {
        seq,
        line: format!("{ts} INFO faucet_core::pipeline: {text}"),
        ts,
        level: "INFO".into(),
        attrs,
    }
}

/// `n` lines captured `age` ago.
fn lines_at(age: Duration, n: u64, text: &str) -> Vec<RunLogLine> {
    let base = seq_at(chrono::Utc::now() - chrono::Duration::from_std(age).unwrap()) & !0xFFF;
    (0..n)
        .map(|i| line(base + i, &format!("{text} {i}")))
        .collect()
}

async fn record_run(h: &dyn RunHistory, id: &str, name: &str, tenant: Option<&str>) {
    let mut rec = RunRecord::queued(
        id.into(),
        Some(name.into()),
        Default::default(),
        None,
        chrono::Utc::now(),
    );
    rec.tenant = tenant.map(str::to_string);
    h.upsert(&rec).await.unwrap();
}

/// The delivery bookkeeping contract, shared by both backends.
async fn delivery_contract(h: &dyn RunHistory) {
    assert!(h.log_ship_row("none").await.unwrap().is_none());
    assert!(
        !h.log_ship_claim("none", Duration::from_secs(5))
            .await
            .unwrap()
    );
    let lines = lines_at(Duration::from_secs(10), 4, "l");
    h.record_run_logs("r", &lines).await.unwrap();
    let page = h.list_run_logs("r", None, 10).await.unwrap();
    assert_eq!(
        page.lines[0].attrs.get("row").map(String::as_str),
        Some("r1")
    );
    let row = h.log_ship_row("r").await.unwrap().unwrap();
    assert_eq!(row.total_seq, lines[3].seq);
    assert!(row.has_pending());
    assert_eq!(h.log_ship_rows(true).await.unwrap().len(), 1);
    let st = h.run_log_stats("r", None, None).await.unwrap();
    assert_eq!(st.lines, 4);
    assert!(st.bytes > 0);
    assert_eq!(st.min_seq, Some(lines[0].seq));
    assert_eq!(st.max_seq, Some(lines[3].seq));
    let st = h
        .run_log_stats("r", Some(lines[0].seq), Some(lines[2].seq))
        .await
        .unwrap();
    assert_eq!(st.lines, 2);

    // Lease-fenced writes need the lease.
    assert!(!h.log_ship_ack("r", lines[1].seq).await.unwrap());
    assert!(!h.log_ship_fail("r", "x").await.unwrap());
    assert!(h.log_ship_claim("r", Duration::from_secs(5)).await.unwrap());
    assert!(h.log_ship_fail("r", "collector down").await.unwrap());
    let row = h.log_ship_row("r").await.unwrap().unwrap();
    assert_eq!(row.last_error.as_deref(), Some("collector down"));
    assert!(row.failing_since.is_some());
    h.log_ship_mark_notified("r", true, false).await.unwrap();
    assert!(h.log_ship_row("r").await.unwrap().unwrap().notified_failure);
    assert!(h.log_ship_ack("r", lines[1].seq).await.unwrap());
    let row = h.log_ship_row("r").await.unwrap().unwrap();
    assert_eq!(row.delivered_seq, Some(lines[1].seq));
    assert!(row.last_error.is_none() && row.failing_since.is_none());
    assert!(
        !row.notified_failure,
        "a success re-arms the failure notification"
    );
    h.log_ship_release("r").await.unwrap();
    assert!(h.log_ship_row("r").await.unwrap().unwrap().owner.is_none());

    h.log_ship_add_dropped("r", 3).await.unwrap();
    h.log_ship_add_dropped("r", 2).await.unwrap();
    assert_eq!(h.log_ship_row("r").await.unwrap().unwrap().dropped, 5);
    assert_eq!(
        h.delete_run_logs_through("r", lines[1].seq).await.unwrap(),
        2
    );
    assert_eq!(h.run_log_stats("r", None, None).await.unwrap().lines, 2);
    assert_eq!(h.log_ship_rows(false).await.unwrap().len(), 1);
    h.log_ship_forget("r").await.unwrap();
    assert!(h.log_ship_row("r").await.unwrap().is_none());
}

#[tokio::test]
async fn delivery_contract_memory() {
    delivery_contract(&MemoryHistory::new(Duration::from_secs(60))).await;
}

#[tokio::test]
async fn delivery_contract_sqlite() {
    let dir = tempfile::tempdir().unwrap();
    delivery_contract(&sqlite(&dir.path().join("h.db"), "a", Duration::from_secs(30)).await).await;
}

#[tokio::test]
async fn cluster_one_instance_ships_a_run_and_a_peer_takes_over_after_its_lease() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("cluster.db");
    let a = sqlite(&db, "instance-a", Duration::from_secs(1)).await;
    let b = sqlite(&db, "instance-b", Duration::from_secs(1)).await;
    a.record_run_logs("run", &lines_at(Duration::from_secs(10), 3, "x"))
        .await
        .unwrap();
    assert!(
        a.log_ship_claim("run", Duration::from_secs(1))
            .await
            .unwrap()
    );
    assert!(
        !b.log_ship_claim("run", Duration::from_secs(1))
            .await
            .unwrap(),
        "no second shipper while the lease is live"
    );
    assert!(
        a.log_ship_claim("run", Duration::from_secs(1))
            .await
            .unwrap(),
        "the holder renews"
    );
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert!(
        b.log_ship_claim("run", Duration::from_secs(1))
            .await
            .unwrap(),
        "a peer takes over once the lease lapses"
    );
    assert!(
        !a.log_ship_ack("run", 1).await.unwrap(),
        "the old holder is fenced"
    );

    // The shipper of a live-leased run skips it; once B ships, A has nothing.
    let (col, ep) = start_grpc().await;
    let mut cfg = export_config(Some(otel(&ep, faucet_core::OtelProtocol::Grpc)));
    cfg.lease_ttl = Duration::from_secs(30);
    let ex = LogExport::from_config(&cfg);
    let s = ex.ship_once(&a).await.unwrap();
    assert_eq!(s.skipped, 1);
    assert_eq!(col.request_count(), 0);
    let s = ex.ship_once(&b).await.unwrap();
    assert_eq!(s.shipped, 3);
    assert_eq!(ex.ship_once(&a).await.unwrap().shipped, 0);
    assert_eq!(col.records().len(), 3);
}

#[tokio::test]
async fn outage_then_restart_delivers_every_line_exactly_once() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("restart.db");
    let (col, ep, _srv) = start_http().await;
    col.set_down(true);
    let cfg = export_config(Some(otel(&ep, faucet_core::OtelProtocol::Http)));
    {
        let h = sqlite(&db, "before", Duration::from_secs(1)).await;
        record_run(&h, "run-1", "orders", Some("acme")).await;
        h.record_run_logs("run-1", &lines_at(Duration::from_secs(10), 700, "line"))
            .await
            .unwrap();
        let ex = LogExport::from_config(&cfg);
        let s = ex.ship_once(&h).await.unwrap();
        assert!(s.failed);
        let rec = h.get("run-1").await.unwrap().unwrap();
        let v = ex.view(&h, &rec).await;
        assert_eq!(v.status, LogExportStatus::Failed);
        assert_eq!(v.pending_lines, 700);
        assert!(v.last_error.is_some());
    }
    // A restarted server (a new instance on the same database) resumes.
    col.set_down(false);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let h = sqlite(&db, "after", Duration::from_secs(30)).await;
    let ex = LogExport::from_config(&cfg);
    let s = ex.ship_once(&h).await.unwrap();
    assert_eq!(s.shipped, 700);
    assert_eq!(ex.ship_once(&h).await.unwrap().shipped, 0);
    let recs = col.records();
    assert_eq!(recs.len(), 700);
    let seqs: std::collections::BTreeSet<&str> =
        recs.iter().filter_map(|r| r.attr("faucet.seq")).collect();
    assert_eq!(seqs.len(), 700, "each line once");
    let r = &recs[0];
    assert_eq!(r.attr("serve_run_id"), Some("run-1"));
    assert_eq!(r.attr("pipeline"), Some("orders"));
    assert_eq!(r.attr("tenant"), Some("acme"));
    assert_eq!(r.attr("row"), Some("r1"));
    assert!(
        r.body.starts_with("line "),
        "body without the rendered prefix: {}",
        r.body
    );
    let rec = h.get("run-1").await.unwrap().unwrap();
    let v = ex.view(&h, &rec).await;
    assert_eq!(v.status, LogExportStatus::Exported);
    assert_eq!(v.pending_lines, 0);
    assert!(v.delivered_at.is_some());
}

#[tokio::test]
async fn retention_follows_delivery_and_bounds_drop_the_oldest() {
    let (col, ep) = start_grpc().await;
    let h = MemoryHistory::new(Duration::from_secs(60));
    let mut cfg = export_config(Some(otel(&ep, faucet_core::OtelProtocol::Grpc)));
    cfg.log_retention = Duration::ZERO;
    cfg.log_buffer.max_age = Duration::from_secs(3600);
    cfg.log_buffer.max_bytes = 1_000_000;
    let ex = LogExport::from_config(&cfg);

    // Delivered lines go once retention (0 here) passes.
    record_run(&h, "done", "p", None).await;
    h.record_run_logs("done", &lines_at(Duration::from_secs(10), 3, "d"))
        .await
        .unwrap();
    ex.ship_once(&h).await.unwrap();
    assert_eq!(col.records().len(), 3);
    let m = ex.maintain(&h).await.unwrap();
    assert_eq!(m.purged, 3);
    assert_eq!(h.run_log_stats("done", None, None).await.unwrap().lines, 0);

    // Undelivered past max_age: dropped, counted, partially_dropped.
    col.set_down(true);
    record_run(&h, "old", "p", None).await;
    h.record_run_logs("old", &lines_at(Duration::from_secs(7200), 4, "o"))
        .await
        .unwrap();
    h.record_run_logs("old", &lines_at(Duration::from_secs(10), 2, "n"))
        .await
        .unwrap();
    let m = ex.maintain(&h).await.unwrap();
    assert_eq!(m.dropped_max_age, 4);
    assert_eq!(m.gauges.lines, 2);
    let rec = h.get("old").await.unwrap().unwrap();
    let v = ex.view(&h, &rec).await;
    assert_eq!(v.status, LogExportStatus::PartiallyDropped);
    assert_eq!(v.dropped_lines, 4);
    assert_eq!(v.pending_lines, 2);

    // Over max_bytes: the oldest undelivered lines go first.
    let mut tight = cfg.clone();
    tight.log_buffer.max_bytes = 1;
    let ex2 = LogExport::from_config(&tight);
    let m = ex2.maintain(&h).await.unwrap();
    assert!(m.dropped_max_bytes >= 1);
    assert_eq!(
        h.run_log_stats("old", None, None).await.unwrap().lines,
        2 - m.dropped_max_bytes
    );

    // A run whose record and lines are gone loses its delivery row.
    h.record_run_logs("ghost", &lines_at(Duration::from_secs(10), 1, "g"))
        .await
        .unwrap();
    h.delete_run_logs_through("ghost", u64::MAX - 1)
        .await
        .unwrap();
    ex.maintain(&h).await.unwrap();
    assert!(h.log_ship_row("ghost").await.unwrap().is_none());
}

#[tokio::test]
async fn without_an_exporter_retention_is_time_based() {
    let h = MemoryHistory::new(Duration::from_secs(60));
    let mut cfg = export_config(None);
    cfg.log_retention = Duration::from_secs(60);
    let ex = LogExport::from_config(&cfg);
    assert!(!ex.configured());
    let old = chrono::Utc::now() - chrono::Duration::hours(2);
    let mut l = line(1, "ancient");
    l.ts = old.to_rfc3339();
    h.record_run_logs("r", &[l]).await.unwrap();
    let m = ex.maintain(&h).await.unwrap();
    assert_eq!(m.purged, 1);
    assert!(h.log_ship_row("r").await.unwrap().is_none());
    assert_eq!(ex.ship_once(&h).await.unwrap(), Default::default());
    record_run(&h, "x", "p", None).await;
    let rec = h.get("x").await.unwrap().unwrap();
    assert_eq!(
        ex.view(&h, &rec).await.status,
        LogExportStatus::NotConfigured
    );
    faucet_cli::serve::log_export::final_flush(&ex, &h, Duration::from_secs(1)).await;
}

#[cfg(feature = "notify")]
#[tokio::test]
async fn failing_exports_and_drops_notify_the_runs_channels() {
    let hook = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .respond_with(wiremock::ResponseTemplate::new(200))
        .mount(&hook)
        .await;
    let (col, ep) = start_grpc().await;
    col.set_down(true);
    let h = MemoryHistory::new(Duration::from_secs(60));
    let mut cfg = export_config(Some(otel(&ep, faucet_core::OtelProtocol::Grpc)));
    cfg.log_buffer.notify_after = Duration::ZERO;
    cfg.log_buffer.max_age = Duration::from_secs(60);
    cfg.log_buffer.link_template =
        Some("https://logs.example/explore?run={run_id}&t={tenant}".into());
    let ex = LogExport::from_config(&cfg);
    let mut rec = RunRecord::queued(
        "n-1".into(),
        Some("orders".into()),
        Default::default(),
        None,
        chrono::Utc::now(),
    );
    rec.config_body = Some(format!(
        "version: 1\nnotifications:\n  - name: hook\n    on: [log_export_failed]\n    channel: {{ type: webhook, config: {{ url: \"{}/hook\" }} }}\n",
        hook.uri()
    ));
    h.upsert(&rec).await.unwrap();
    h.record_run_logs("n-1", &lines_at(Duration::from_secs(10), 2, "a"))
        .await
        .unwrap();
    ex.ship_once(&h).await.unwrap();
    let sent = hook.received_requests().await.unwrap_or_default();
    assert_eq!(sent.len(), 1, "a failing export notifies once");
    assert!(String::from_utf8_lossy(&sent[0].body).contains("log_export_failed"));
    let row = h.log_ship_row("n-1").await.unwrap().unwrap();
    assert!(row.notified_failure);
    ex.ship_once(&h).await.unwrap();

    h.record_run_logs("n-1", &lines_at(Duration::from_secs(3600), 1, "stale"))
        .await
        .unwrap();
    ex.maintain(&h).await.unwrap();
    assert!(h.log_ship_row("n-1").await.unwrap().unwrap().notified_drop);
    let v = ex.view(&h, &rec).await;
    assert_eq!(
        v.link.as_deref(),
        Some("https://logs.example/explore?run=n-1&t=")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn serve_ships_a_runs_logs_and_reports_it_on_the_run() {
    let (col, ep) = start_grpc().await;
    let dir = tempfile::tempdir().unwrap();
    let otel_path = dir.path().join("otel.yaml");
    std::fs::write(
        &otel_path,
        format!("endpoint: \"{ep}\"\nexport: [logs]\nservice_name: faucet-serve-test\n"),
    )
    .unwrap();
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut a = args(port);
    a.otel_config = Some(otel_path);
    a.history = Some(format!("sqlite:{}", dir.path().join("h.db").display()));
    a.log_buffer.log_link_template = Some("https://g/explore?q={run_id}".into());
    let mut config = ServeConfig::from_args(a).unwrap();
    config.log_level = "info".into();
    tokio::spawn(async move {
        let _ = faucet_cli::serve::run_server(config, Default::default()).await;
    });
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    assert!(
        wait_for_async(30, || {
            let c = client.clone();
            let u = format!("{base}/healthz");
            async move {
                c.get(u)
                    .send()
                    .await
                    .map(|r| r.status().is_success())
                    .unwrap_or(false)
            }
        })
        .await
    );
    let input = dir.path().join("in.csv");
    std::fs::write(&input, "id\n1\n2\n").unwrap();
    let yaml = format!(
        "version: 1\nname: served\npipeline:\n  source: {{ type: csv, config: {{ path: \"{}\" }} }}\n  sink: {{ type: jsonl, config: {{ path: \"{}\" }} }}\n",
        input.display(),
        dir.path().join("out.jsonl").display()
    );
    let submit: serde_json::Value = client
        .post(format!("{base}/v1/runs"))
        .json(&serde_json::json!({ "config": yaml, "name": "served" }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let id = submit["run_id"].as_str().unwrap().to_string();
    let mut last = serde_json::Value::Null;
    for _ in 0..300 {
        last = client
            .get(format!("{base}/v1/runs/{id}"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if last["log_export"]["status"] == "exported" && last["status"] == "completed" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(last["log_export"]["status"], "exported", "{last}");
    assert_eq!(
        last["log_export"]["link"],
        format!("https://g/explore?q={id}")
    );
    let recs = col.records_where("serve_run_id", &id);
    assert!(!recs.is_empty());
    assert!(recs.iter().all(|r| r.service == "faucet-serve-test"));
    assert!(
        recs.iter().all(|r| r.attr("pipeline") == Some("served")),
        "{:?}",
        recs.iter()
            .map(|r| (r.attr("pipeline"), &r.body))
            .collect::<Vec<_>>()
    );

    // A known run with no local lines left answers with the link.
    let h = sqlite(&dir.path().join("h.db"), "probe", Duration::from_secs(30)).await;
    h.delete_run_logs_through(&id, u64::MAX - 1).await.unwrap();
    let body = client
        .get(format!("{base}/v1/runs/{id}/logs?format=jsonl"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(body.contains("\"aged_out\":true"), "{body}");
    let text = client
        .get(format!("{base}/v1/runs/{id}/logs?format=text"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(text.contains("https://g/explore"), "{text}");
}

async fn wait_for_async<F, Fut>(secs: u64, mut cond: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = std::time::Instant::now() + Duration::from_secs(secs);
    while std::time::Instant::now() < deadline {
        if cond().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}
