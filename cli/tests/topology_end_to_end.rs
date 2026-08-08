//! Topology mode (#71 / #72) end-to-end tests: fan-out (tee), fan-in (merge),
//! and cross-source join, plus the config-validation error paths.
//!
//! Binary-driven tests exercise the `run` / `validate` / `preview` command
//! wiring via `assert_cmd`; the error-path tests call
//! `faucet_cli::topology::build_topology` directly to assert the typed
//! `CliError` variants.
#![cfg(all(
    feature = "source-csv",
    feature = "sink-jsonl",
    feature = "sink-stdout",
    feature = "transforms"
))]

use assert_cmd::Command;
use predicates::str::contains;
use std::fs;
use std::path::Path;
use tempfile::TempDir;

fn write(path: &Path, body: &str) {
    fs::write(path, body).unwrap();
}

fn orders_csv(dir: &Path) -> std::path::PathBuf {
    let p = dir.join("orders.csv");
    write(
        &p,
        "order_id,country_code,amount\n1,US,10\n2,US,5\n3,IN,7\n4,DE,3\n",
    );
    p
}

fn countries_csv(dir: &Path) -> std::path::PathBuf {
    let p = dir.join("countries.csv");
    write(&p, "code,country\nUS,United States\nIN,India\nDE,Germany\n");
    p
}

// ── tee (fan-out) ─────────────────────────────────────────────────────────────

#[test]
fn tee_fans_out_to_two_sinks() {
    let dir = TempDir::new().unwrap();
    let csv = orders_csv(dir.path());
    let a = dir.path().join("a.jsonl");
    let b = dir.path().join("b.jsonl");
    let cfg = dir.path().join("faucet.yaml");
    write(
        &cfg,
        &format!(
            r#"version: 1
name: tee_test
pipeline:
  sources:
    orders: {{ type: csv, config: {{ path: {csv} }} }}
  sinks:
    a: {{ type: jsonl, config: {{ path: {a} }} }}
    b: {{ type: jsonl, config: {{ path: {b} }} }}
  nodes:
    # `config` inline override (deep-merged onto the template) + a transform node.
    src: {{ kind: source, ref: orders, config: {{ batch_size: 2 }} }}
    norm: {{ kind: transform, transforms: [ {{ type: keys_case, config: {{ mode: snake }} }} ] }}
    fan: {{ kind: tee, channel_capacity: 2, fanout: 2 }}
    # `type` inline override (same kind as the template — exercises the override path).
    wa: {{ kind: sink, ref: a, type: jsonl }}
    wb: {{ kind: sink, ref: b }}
  edges:
    - {{ from: src, to: norm }}
    - {{ from: norm, to: fan }}
    - {{ from: fan, to: wa }}
    - {{ from: fan, to: wb }}
"#,
            csv = csv.display(),
            a = a.display(),
            b = b.display()
        ),
    );

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["run"])
        .arg(&cfg)
        .assert()
        .success()
        .stderr(contains("2 sink node"));

    assert_eq!(fs::read_to_string(&a).unwrap().lines().count(), 4);
    assert_eq!(fs::read_to_string(&b).unwrap().lines().count(), 4);
}

// ── merge (fan-in) ────────────────────────────────────────────────────────────

#[test]
fn merge_fans_in_two_sources() {
    let dir = TempDir::new().unwrap();
    let orders = orders_csv(dir.path());
    let countries = countries_csv(dir.path());
    let out = dir.path().join("combined.jsonl");
    let cfg = dir.path().join("faucet.yaml");
    write(
        &cfg,
        &format!(
            r#"version: 1
name: merge_test
pipeline:
  sources:
    o: {{ type: csv, config: {{ path: {orders} }} }}
    c: {{ type: csv, config: {{ path: {countries} }} }}
  sinks:
    out: {{ type: jsonl, config: {{ path: {out} }} }}
  nodes:
    ro: {{ kind: source, ref: o }}
    rc: {{ kind: source, ref: c }}
    m: {{ kind: merge }}
    w: {{ kind: sink, ref: out }}
  edges:
    - {{ from: ro, to: m }}
    - {{ from: rc, to: m }}
    - {{ from: m, to: w }}
"#,
            orders = orders.display(),
            countries = countries.display(),
            out = out.display()
        ),
    );

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["run"])
        .arg(&cfg)
        .assert()
        .success();

    // 4 orders + 3 countries.
    assert_eq!(fs::read_to_string(&out).unwrap().lines().count(), 7);
}

// ── join ───────────────────────────────────────────────────────────────────────

#[test]
fn join_enriches_orders_with_country() {
    let dir = TempDir::new().unwrap();
    let orders = orders_csv(dir.path());
    let countries = countries_csv(dir.path());
    let out = dir.path().join("enriched.jsonl");
    let cfg = dir.path().join("faucet.yaml");
    write(
        &cfg,
        &format!(
            r#"version: 1
name: join_test
pipeline:
  sources:
    o: {{ type: csv, config: {{ path: {orders} }} }}
    c: {{ type: csv, config: {{ path: {countries} }} }}
  sinks:
    out: {{ type: jsonl, config: {{ path: {out} }} }}
  nodes:
    ro: {{ kind: source, ref: o }}
    rc: {{ kind: source, ref: c }}
    j:
      kind: join
      mode: left
      build: {{ edge: c_in, key: code }}
      probe: {{ edge: o_in, key: country_code }}
      project:
        - {{ from: country, as: country_name }}
    w: {{ kind: sink, ref: out }}
  edges:
    - {{ from: rc, to: j, as: c_in }}
    - {{ from: ro, to: j, as: o_in }}
    - {{ from: j, to: w }}
"#,
            orders = orders.display(),
            countries = countries.display(),
            out = out.display()
        ),
    );

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["run"])
        .arg(&cfg)
        .assert()
        .success();

    let body = fs::read_to_string(&out).unwrap();
    assert_eq!(body.lines().count(), 4);
    assert!(body.contains("\"country_name\":\"United States\""));
    assert!(body.contains("\"country_name\":\"India\""));
    assert!(body.contains("\"country_name\":\"Germany\""));
}

// ── validate + preview ──────────────────────────────────────────────────────────

#[test]
fn validate_reports_topology_summary() {
    let dir = TempDir::new().unwrap();
    let csv = orders_csv(dir.path());
    let out = dir.path().join("o.jsonl");
    let cfg = dir.path().join("faucet.yaml");
    write(
        &cfg,
        &format!(
            r#"version: 1
name: validate_test
pipeline:
  sources:
    o: {{ type: csv, config: {{ path: {csv} }} }}
  sinks:
    out: {{ type: jsonl, config: {{ path: {out} }} }}
  nodes:
    s: {{ kind: source, ref: o }}
    w: {{ kind: sink, ref: out }}
  edges:
    - {{ from: s, to: w }}
"#,
            csv = csv.display(),
            out = out.display()
        ),
    );

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["validate"])
        .arg(&cfg)
        .assert()
        .success()
        .stdout(contains("2 node(s), 1 edge(s) — valid"));
}

#[test]
fn preview_prints_source_records() {
    let dir = TempDir::new().unwrap();
    let csv = orders_csv(dir.path());
    let out = dir.path().join("o.jsonl");
    let cfg = dir.path().join("faucet.yaml");
    write(
        &cfg,
        &format!(
            r#"version: 1
name: preview_test
pipeline:
  sources:
    o: {{ type: csv, config: {{ path: {csv} }} }}
  sinks:
    out: {{ type: jsonl, config: {{ path: {out} }} }}
  nodes:
    s: {{ kind: source, ref: o }}
    w: {{ kind: sink, ref: out }}
  edges:
    - {{ from: s, to: w }}
"#,
            csv = csv.display(),
            out = out.display()
        ),
    );

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["preview", "--limit", "2"])
        .arg(&cfg)
        .assert()
        .success()
        .stdout(contains("order_id"));
}

// ── error paths (direct calls) ───────────────────────────────────────────────────

use faucet_cli::auth_catalog::build_auth_catalog;
use faucet_cli::config::PipelineConfig;
use faucet_cli::error::CliError;
use faucet_cli::topology::build_topology;

fn parse(yaml: &str) -> PipelineConfig {
    PipelineConfig::from_text(yaml, Path::new("test.yaml")).expect("parses")
}

#[tokio::test]
async fn rejects_matrix_and_nodes_together() {
    let cfg = parse(
        r#"version: 1
name: both
pipeline:
  sources:
    o: { type: csv, config: { path: /tmp/x.csv } }
  sinks:
    out: { type: jsonl, config: { path: /tmp/y.jsonl } }
  nodes:
    s: { kind: source, ref: o }
    w: { kind: sink, ref: out }
  edges:
    - { from: s, to: w }
matrix:
  - id: extra
"#,
    );
    let auth = build_auth_catalog(None).unwrap();
    let err = build_topology(&cfg, &auth).await.unwrap_err();
    assert!(matches!(err, CliError::MatrixAndNodesBothPresent));
}

#[tokio::test]
async fn rejects_edge_to_unknown_node() {
    let cfg = parse(
        r#"version: 1
name: badedge
pipeline:
  sources:
    o: { type: csv, config: { path: /tmp/x.csv } }
  sinks:
    out: { type: jsonl, config: { path: /tmp/y.jsonl } }
  nodes:
    s: { kind: source, ref: o }
    w: { kind: sink, ref: out }
  edges:
    - { from: s, to: ghost }
"#,
    );
    let auth = build_auth_catalog(None).unwrap();
    let err = build_topology(&cfg, &auth).await.unwrap_err();
    assert!(
        matches!(err, CliError::EdgeEndpointMissing { ref name, .. } if name == "ghost"),
        "{err:?}"
    );
}

#[tokio::test]
async fn rejects_unknown_template_ref() {
    let cfg = parse(
        r#"version: 1
name: badref
pipeline:
  sources:
    o: { type: csv, config: { path: /tmp/x.csv } }
  sinks:
    out: { type: jsonl, config: { path: /tmp/y.jsonl } }
  nodes:
    s: { kind: source, ref: nonexistent }
    w: { kind: sink, ref: out }
  edges:
    - { from: s, to: w }
"#,
    );
    let auth = build_auth_catalog(None).unwrap();
    let err = build_topology(&cfg, &auth).await.unwrap_err();
    assert!(matches!(err, CliError::UnknownTemplate { .. }), "{err:?}");
}

#[tokio::test]
async fn rejects_edge_from_unknown_node() {
    let cfg = parse(
        r#"version: 1
name: badfrom
pipeline:
  sources:
    o: { type: csv, config: { path: /tmp/x.csv } }
  sinks:
    out: { type: jsonl, config: { path: /tmp/y.jsonl } }
  nodes:
    s: { kind: source, ref: o }
    w: { kind: sink, ref: out }
  edges:
    - { from: ghost, to: w }
    - { from: s, to: w }
"#,
    );
    let auth = build_auth_catalog(None).unwrap();
    let err = build_topology(&cfg, &auth).await.unwrap_err();
    assert!(
        matches!(err, CliError::EdgeEndpointMissing { ref name, .. } if name == "ghost"),
        "{err:?}"
    );
}

#[tokio::test]
async fn preview_rejects_matrix_and_nodes() {
    let cfg = parse(
        r#"version: 1
name: pv_both
pipeline:
  sources:
    o: { type: csv, config: { path: /tmp/x.csv } }
  sinks:
    out: { type: jsonl, config: { path: /tmp/y.jsonl } }
  nodes:
    s: { kind: source, ref: o }
    w: { kind: sink, ref: out }
  edges:
    - { from: s, to: w }
matrix:
  - id: extra
"#,
    );
    let auth = build_auth_catalog(None).unwrap();
    let err = faucet_cli::topology::preview(&cfg, &auth, 5)
        .await
        .unwrap_err();
    assert!(matches!(err, CliError::MatrixAndNodesBothPresent));
}

#[tokio::test]
async fn preview_errors_when_no_source_node() {
    // A nodes map with no `source` kind: preview finds nothing to emit.
    let cfg = parse(
        r#"version: 1
name: pv_nosrc
pipeline:
  sinks:
    out: { type: jsonl, config: { path: /tmp/y.jsonl } }
  nodes:
    w: { kind: sink, ref: out }
"#,
    );
    let auth = build_auth_catalog(None).unwrap();
    let err = faucet_cli::topology::preview(&cfg, &auth, 5)
        .await
        .unwrap_err();
    assert!(matches!(err, CliError::InvalidTopology { .. }), "{err:?}");
}

#[tokio::test]
async fn run_topology_direct_with_uncancelled_token() {
    // Exercises `run_topology` directly (records-written mapping + the cancel
    // branch) with a non-cancelled token.
    let dir = TempDir::new().unwrap();
    let csv = orders_csv(dir.path());
    let out = dir.path().join("o.jsonl");
    let cfg = parse(&format!(
        r#"version: 1
name: direct_run
pipeline:
  sources:
    o: {{ type: csv, config: {{ path: {csv} }} }}
  sinks:
    out: {{ type: jsonl, config: {{ path: {out} }} }}
  nodes:
    s: {{ kind: source, ref: o }}
    w: {{ kind: sink, ref: out }}
  edges:
    - {{ from: s, to: w }}
"#,
        csv = csv.display(),
        out = out.display()
    ));
    let auth = build_auth_catalog(None).unwrap();
    let cancel = faucet_core::CancellationToken::new();
    let summary = faucet_cli::topology::run_topology(
        &cfg,
        &auth,
        faucet_cli::topology::TopologyRunOptions {
            cancel: Some(cancel),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(summary.invocations.len(), 1);
    assert_eq!(summary.invocations[0].records_written, 4);
    assert_eq!(summary.invocations[0].row_id, "w");
}

#[tokio::test]
async fn rejects_missing_default_template() {
    // A source node with no `ref` defaults to the `default` template; with only
    // a named template and no legacy `pipeline.source`, that is a MissingTemplate.
    let cfg = parse(
        r#"version: 1
name: nodefault
pipeline:
  sources:
    o: { type: csv, config: { path: /tmp/x.csv } }
  sinks:
    out: { type: jsonl, config: { path: /tmp/y.jsonl } }
  nodes:
    s: { kind: source }
    w: { kind: sink, ref: out }
  edges:
    - { from: s, to: w }
"#,
    );
    let auth = build_auth_catalog(None).unwrap();
    let err = build_topology(&cfg, &auth).await.unwrap_err();
    assert!(matches!(err, CliError::MissingTemplate { .. }), "{err:?}");
}

// ── run-command coverage: state, on_error, output formats, failures ──────────────

#[test]
fn run_with_state_and_stop_on_error() {
    let dir = TempDir::new().unwrap();
    let csv = orders_csv(dir.path());
    let out = dir.path().join("o.jsonl");
    let state = dir.path().join("state");
    let cfg = dir.path().join("faucet.yaml");
    write(
        &cfg,
        &format!(
            r#"version: 1
name: state_stop
execution:
  on_error: stop
pipeline:
  state: {{ type: file, config: {{ path: {state} }} }}
  sources:
    o: {{ type: csv, config: {{ path: {csv} }} }}
  sinks:
    out: {{ type: jsonl, config: {{ path: {out} }} }}
  nodes:
    s: {{ kind: source, ref: o }}
    w: {{ kind: sink, ref: out }}
  edges:
    - {{ from: s, to: w }}
"#,
            state = state.display(),
            csv = csv.display(),
            out = out.display()
        ),
    );

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["run"])
        .arg(&cfg)
        .assert()
        .success();
    assert_eq!(fs::read_to_string(&out).unwrap().lines().count(), 4);
}

#[test]
fn run_output_json_and_ndjson() {
    let dir = TempDir::new().unwrap();
    let csv = orders_csv(dir.path());
    let out = dir.path().join("o.jsonl");
    let cfg = dir.path().join("faucet.yaml");
    write(
        &cfg,
        &format!(
            r#"version: 1
name: outfmt
pipeline:
  sources:
    o: {{ type: csv, config: {{ path: {csv} }} }}
  sinks:
    out: {{ type: jsonl, config: {{ path: {out} }} }}
  nodes:
    s: {{ kind: source, ref: o }}
    w: {{ kind: sink, ref: out }}
  edges:
    - {{ from: s, to: w }}
"#,
            csv = csv.display(),
            out = out.display()
        ),
    );

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["run", "--output", "json"])
        .arg(&cfg)
        .assert()
        .success()
        .stdout(contains("totals"));

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["run", "--output", "ndjson"])
        .arg(&cfg)
        .assert()
        .success()
        .stdout(contains("\"row_id\""));
}

#[test]
fn run_with_dlq_block() {
    let dir = TempDir::new().unwrap();
    let csv = orders_csv(dir.path());
    let out = dir.path().join("o.jsonl");
    let dlq = dir.path().join("dlq.jsonl");
    let cfg = dir.path().join("faucet.yaml");
    write(
        &cfg,
        &format!(
            r#"version: 1
name: dlq_topo
pipeline:
  dlq:
    sink: {{ type: jsonl, config: {{ path: {dlq} }} }}
  sources:
    o: {{ type: csv, config: {{ path: {csv} }} }}
  sinks:
    out: {{ type: jsonl, config: {{ path: {out} }} }}
  nodes:
    s: {{ kind: source, ref: o }}
    w: {{ kind: sink, ref: out }}
  edges:
    - {{ from: s, to: w }}
"#,
            dlq = dlq.display(),
            csv = csv.display(),
            out = out.display()
        ),
    );

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["run"])
        .arg(&cfg)
        .assert()
        .success();
    // Happy path: everything written to the main sink, nothing to the DLQ.
    assert_eq!(fs::read_to_string(&out).unwrap().lines().count(), 4);
}

#[test]
fn run_reports_node_failure_under_continue() {
    // A source pointed at a missing file fails; under the default `continue`
    // policy the run exits non-zero with a TopologyHadFailures error.
    let dir = TempDir::new().unwrap();
    let out = dir.path().join("o.jsonl");
    let missing = dir.path().join("does_not_exist.csv");
    let cfg = dir.path().join("faucet.yaml");
    write(
        &cfg,
        &format!(
            r#"version: 1
name: failrun
pipeline:
  sources:
    o: {{ type: csv, config: {{ path: {missing} }} }}
  sinks:
    out: {{ type: jsonl, config: {{ path: {out} }} }}
  nodes:
    s: {{ kind: source, ref: o }}
    w: {{ kind: sink, ref: out }}
  edges:
    - {{ from: s, to: w }}
"#,
            missing = missing.display(),
            out = out.display()
        ),
    );

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["run"])
        .arg(&cfg)
        .assert()
        .failure();
}

#[tokio::test]
async fn rejects_invalid_graph_arity() {
    // A source with an incoming edge is an arity violation caught by the core
    // validator and surfaced as InvalidTopology.
    let cfg = parse(
        r#"version: 1
name: badarity
pipeline:
  sources:
    o: { type: csv, config: { path: /tmp/x.csv } }
  sinks:
    out: { type: jsonl, config: { path: /tmp/y.jsonl } }
  nodes:
    s: { kind: source, ref: o }
    w: { kind: sink, ref: out }
  edges:
    - { from: s, to: w }
    - { from: w, to: s }
"#,
    );
    let auth = build_auth_catalog(None).unwrap();
    let err = build_topology(&cfg, &auth).await.unwrap_err();
    assert!(matches!(err, CliError::InvalidTopology { .. }), "{err:?}");
}

// ── #456: topology mode must not bypass the governance layer ─────────────────

/// #456 C2: `--dry-run` used to be silently ignored in topology mode, so it
/// performed real writes. It must now write nothing.
#[test]
fn dry_run_writes_nothing_in_topology_mode() {
    let dir = TempDir::new().unwrap();
    let csv = orders_csv(dir.path());
    let out = dir.path().join("out.jsonl");
    let cfg_path = dir.path().join("faucet.yaml");
    write(
        &cfg_path,
        &format!(
            r#"version: 1
name: dry
pipeline:
  sources:
    o: {{ type: csv, config: {{ path: {csv} }} }}
  sinks:
    out: {{ type: jsonl, config: {{ path: {out} }} }}
  nodes:
    s: {{ kind: source, ref: o }}
    w: {{ kind: sink, ref: out }}
  edges:
    - {{ from: s, to: w }}
"#,
            csv = csv.display(),
            out = out.display()
        ),
    );
    Command::cargo_bin("faucet")
        .unwrap()
        .args(["run", cfg_path.to_str().unwrap(), "--dry-run"])
        .assert()
        .success();
    assert!(
        !out.exists(),
        "--dry-run must not create the destination file"
    );
}

/// #456 C3: a topology declaring `masking:` must actually mask. Before the fix
/// the block parsed, validated, ran — and PII reached the sink in the clear.
#[test]
#[cfg(feature = "masking")]
fn masking_applies_in_topology_mode() {
    let dir = TempDir::new().unwrap();
    let src = dir.path().join("people.csv");
    write(&src, "id,email\n1,a@b.c\n");
    let out = dir.path().join("masked.jsonl");
    let cfg_path = dir.path().join("faucet.yaml");
    write(
        &cfg_path,
        &format!(
            r#"version: 1
name: masked
pipeline:
  masking:
    rules:
      - name: hide-email
        match: {{ fields: [email] }}
        action: {{ type: redact, mask: "***" }}
  sources:
    o: {{ type: csv, config: {{ path: {src} }} }}
  sinks:
    out: {{ type: jsonl, config: {{ path: {out} }} }}
  nodes:
    s: {{ kind: source, ref: o }}
    w: {{ kind: sink, ref: out }}
  edges:
    - {{ from: s, to: w }}
"#,
            src = src.display(),
            out = out.display()
        ),
    );
    Command::cargo_bin("faucet")
        .unwrap()
        .args(["run", cfg_path.to_str().unwrap()])
        .assert()
        .success();
    let body = fs::read_to_string(&out).unwrap();
    assert!(body.contains("***"), "email must be masked: {body}");
    assert!(
        !body.contains("a@b.c"),
        "raw PII must not reach the sink: {body}"
    );
}

/// #456 H2: an exactly-once config that cannot be honoured must be rejected
/// rather than silently downgraded to at-least-once. (Since #458 topology mode
/// *does* support exactly-once — this asserts the refusal path still holds when a
/// requirement is unmet.)
#[tokio::test]
async fn rejects_exactly_once_in_topology_mode() {
    let cfg = parse(
        r#"version: 1
name: eo
delivery: exactly_once
pipeline:
  sources:
    o: { type: csv, config: { path: /tmp/x.csv } }
  sinks:
    out: { type: jsonl, config: { path: /tmp/y.jsonl } }
  nodes:
    s: { kind: source, ref: o }
    w: { kind: sink, ref: out }
  edges:
    - { from: s, to: w }
"#,
    );
    let auth = build_auth_catalog(None).unwrap();
    let err = build_topology(&cfg, &auth).await.unwrap_err();
    let msg = err.to_string();
    // The invariant #456 H2 is about: an unsupportable exactly-once config is
    // *refused*, never silently downgraded to at-least-once. Since #458 the
    // refusal is per-node and names the limiting side (here the csv source)
    // instead of rejecting topology mode wholesale.
    assert!(msg.contains("exactly_once"), "{msg}");
    assert!(msg.contains("csv"), "names what is limiting: {msg}");
}

/// #456 H4: `${now.*}` was never resolved in topology mode, so the literal token
/// reached the connector and a dated path became a directory named `${now.date}`.
#[test]
fn now_tokens_resolve_in_topology_mode() {
    let dir = TempDir::new().unwrap();
    let csv = orders_csv(dir.path());
    let cfg_path = dir.path().join("faucet.yaml");
    let out_dir = dir.path().join("dt=${now.date}");
    write(
        &cfg_path,
        &format!(
            r#"version: 1
name: dated
pipeline:
  sources:
    o: {{ type: csv, config: {{ path: {csv} }} }}
  sinks:
    out: {{ type: jsonl, config: {{ path: "{base}/dt=${{now.date}}/part.jsonl" }} }}
  nodes:
    s: {{ kind: source, ref: o }}
    w: {{ kind: sink, ref: out }}
  edges:
    - {{ from: s, to: w }}
"#,
            csv = csv.display(),
            base = dir.path().display()
        ),
    );
    Command::cargo_bin("faucet")
        .unwrap()
        .args([
            "run",
            cfg_path.to_str().unwrap(),
            "--clock",
            "2026-03-04T00:00:00Z",
        ])
        .assert()
        .success();
    assert!(
        dir.path().join("dt=2026-03-04/part.jsonl").exists(),
        "the clock must be substituted into the sink path"
    );
    assert!(
        !out_dir.exists(),
        "the literal `${{now.date}}` token must never reach the connector"
    );
}

/// #456 M2 + #459: `validate` must never print a clean bill of health for a
/// block it does not enforce. Every top-level block is applied per sink node
/// now, so the assertion inverts: none of them may be reported as ignored.
#[test]
#[cfg(feature = "lineage")]
fn validate_reports_no_ignored_blocks() {
    let dir = TempDir::new().unwrap();
    let csv = orders_csv(dir.path());
    let cfg_path = dir.path().join("faucet.yaml");
    write(
        &cfg_path,
        &format!(
            r#"version: 1
name: inert
sla:
  max_staleness_secs: 3600
lineage:
  namespace: test
  transport: {{ type: file, config: {{ path: {ln} }} }}
pipeline:
  state: {{ type: file, config: {{ path: {st} }} }}
  sources:
    o: {{ type: csv, config: {{ path: {csv} }} }}
  sinks:
    out: {{ type: stdout, config: {{}} }}
  nodes:
    s: {{ kind: source, ref: o }}
    w: {{ kind: sink, ref: out }}
  edges:
    - {{ from: s, to: w }}
"#,
            csv = csv.display(),
            ln = dir.path().join("lineage.jsonl").display(),
            st = dir.path().join("state").display()
        ),
    );
    let out = Command::cargo_bin("faucet")
        .unwrap()
        .args(["validate", cfg_path.to_str().unwrap()])
        .assert()
        .success();
    let stdout = String::from_utf8_lossy(&out.get_output().stdout).to_string();
    assert!(
        !stdout.contains("is ignored"),
        "no block may be reported inert: {stdout}"
    );
}

// ── #458 / #459: exactly-once + per-sink-node observability ─────────────────

/// #458: the four atomic-watermark requirements are checked per node at
/// config-load time. The message must name the *limiting* side.
#[tokio::test]
async fn exactly_once_gate_names_the_limiting_side() {
    // A non-deterministic source: csv cannot replay positionally.
    let cfg = parse(
        r#"version: 1
name: eo
delivery: exactly_once
pipeline:
  state: { type: file, config: { path: ./st } }
  sources:
    o: { type: csv, config: { path: /tmp/x.csv } }
  sinks:
    out: { type: sqlite, config: { connection_url: "sqlite::memory:", table: t } }
  nodes:
    s: { kind: source, ref: o }
    w: { kind: sink, ref: out }
  edges:
    - { from: s, to: w }
"#,
    );
    let auth = build_auth_catalog(None).unwrap();
    let err = build_topology(&cfg, &auth).await.unwrap_err().to_string();
    assert!(err.contains("csv"), "names the source: {err}");
    assert!(err.contains("deterministic-replay"), "{err}");
}

/// A memory state store cannot carry a commit sequence across a restart.
#[tokio::test]
async fn exactly_once_requires_durable_state() {
    let cfg = parse(
        r#"version: 1
name: eo
delivery: exactly_once
pipeline:
  state: { type: memory, config: {} }
  sources:
    o: { type: postgres-cdc, config: { connection_url: "postgres://x/y", slot: s, publication: p } }
  sinks:
    out: { type: sqlite, config: { connection_url: "sqlite::memory:", table: t } }
  nodes:
    s: { kind: source, ref: o }
    w: { kind: sink, ref: out }
  edges:
    - { from: s, to: w }
"#,
    );
    let auth = build_auth_catalog(None).unwrap();
    let err = build_topology(&cfg, &auth).await.unwrap_err().to_string();
    assert!(err.contains("memory"), "{err}");
    assert!(err.contains("durable"), "{err}");
}

/// Several sources means no sound resume anchor for a watermark, so the gate
/// refuses and points at the keyed-upsert alternative.
#[tokio::test]
async fn exactly_once_refuses_a_multi_source_graph() {
    let cfg = parse(
        r#"version: 1
name: eo
delivery: exactly_once
pipeline:
  state: { type: file, config: { path: ./st } }
  sources:
    a: { type: postgres-cdc, config: { connection_url: "postgres://x/y", slot: s1, publication: p } }
    b: { type: postgres-cdc, config: { connection_url: "postgres://x/y", slot: s2, publication: p } }
  sinks:
    out: { type: sqlite, config: { connection_url: "sqlite::memory:", table: t } }
  nodes:
    sa: { kind: source, ref: a }
    sb: { kind: source, ref: b }
    m: { kind: merge }
    w: { kind: sink, ref: out }
  edges:
    - { from: sa, to: m }
    - { from: sb, to: m }
    - { from: m, to: w }
"#,
    );
    let auth = build_auth_catalog(None).unwrap();
    let err = build_topology(&cfg, &auth).await.unwrap_err().to_string();
    assert!(err.contains("exactly one source node"), "{err}");
    assert!(err.contains("upsert"), "offers the alternative: {err}");
}

/// #459: `resilience:` IS applied in topology mode (it rides the governance set),
/// so it must not be listed as ignored — the earlier warning was wrong.
#[test]
fn validate_does_not_claim_resilience_is_ignored() {
    let dir = TempDir::new().unwrap();
    let csv = orders_csv(dir.path());
    let cfg_path = dir.path().join("faucet.yaml");
    write(
        &cfg_path,
        &format!(
            r#"version: 1
name: resil
resilience:
  retry: {{ max_attempts: 3 }}
pipeline:
  sources:
    o: {{ type: csv, config: {{ path: {csv} }} }}
  sinks:
    out: {{ type: stdout, config: {{}} }}
  nodes:
    s: {{ kind: source, ref: o }}
    w: {{ kind: sink, ref: out }}
  edges:
    - {{ from: s, to: w }}
"#,
            csv = csv.display()
        ),
    );
    let out = Command::cargo_bin("faucet")
        .unwrap()
        .args(["validate", cfg_path.to_str().unwrap()])
        .assert()
        .success();
    let stdout = String::from_utf8_lossy(&out.get_output().stdout).to_string();
    assert!(
        !stdout.contains("`resilience:` is ignored"),
        "resilience is wired; must not be reported as ignored: {stdout}"
    );
}

/// #459: a sink's inputs are every source that reaches it — one for a linear or
/// tee graph, several for a merge/join. This is what lineage and the catalog now
/// model, so the traversal is asserted directly.
#[test]
fn reaching_sources_maps_each_sink_to_its_inputs() {
    let cfg = parse(
        r#"version: 1
name: reach
pipeline:
  sources:
    a: { type: csv, config: { path: /tmp/a.csv } }
    b: { type: csv, config: { path: /tmp/b.csv } }
  sinks:
    one: { type: jsonl, config: { path: /tmp/1.jsonl } }
    two: { type: jsonl, config: { path: /tmp/2.jsonl } }
  nodes:
    sa: { kind: source, ref: a }
    sb: { kind: source, ref: b }
    m: { kind: merge }
    fan: { kind: tee, fanout: 2 }
    w1: { kind: sink, ref: one }
    w2: { kind: sink, ref: two }
  edges:
    - { from: sa, to: m }
    - { from: sb, to: m }
    - { from: m, to: fan }
    - { from: fan, to: w1 }
    - { from: fan, to: w2 }
"#,
    );
    let reaching = faucet_cli::topology::reaching_sources(&cfg);
    // Both sinks sit downstream of the merge, so both have two inputs.
    assert_eq!(reaching["w1"], vec!["sa".to_string(), "sb".to_string()]);
    assert_eq!(reaching["w2"], vec!["sa".to_string(), "sb".to_string()]);
    // Only sink nodes are keyed.
    assert!(!reaching.contains_key("m"));
    assert!(!reaching.contains_key("sa"));
}

/// A linear graph gives exactly one input per sink — the shape every matrix
/// pipeline has, and the one where column lineage stays expressible.
#[test]
fn reaching_sources_is_one_input_for_a_linear_graph() {
    let cfg = parse(
        r#"version: 1
name: linear
pipeline:
  sources:
    a: { type: csv, config: { path: /tmp/a.csv } }
  sinks:
    one: { type: jsonl, config: { path: /tmp/1.jsonl } }
  nodes:
    s: { kind: source, ref: a }
    t: { kind: transform, transforms: [ { type: keys_case, config: { mode: snake } } ] }
    w: { kind: sink, ref: one }
  edges:
    - { from: s, to: t }
    - { from: t, to: w }
"#,
    );
    let reaching = faucet_cli::topology::reaching_sources(&cfg);
    assert_eq!(reaching["w"], vec!["s".to_string()]);
}

/// #459: with catalog and SLA wired, `validate` must no longer report them as
/// ignored — only genuinely-unwired blocks may appear.
#[test]
fn validate_no_longer_claims_sla_is_ignored() {
    let dir = TempDir::new().unwrap();
    let csv = orders_csv(dir.path());
    let cfg_path = dir.path().join("faucet.yaml");
    write(
        &cfg_path,
        &format!(
            r#"version: 1
name: inert2
sla:
  max_staleness_secs: 3600
pipeline:
  state: {{ type: file, config: {{ path: {st} }} }}
  sources:
    o: {{ type: csv, config: {{ path: {csv} }} }}
  sinks:
    out: {{ type: stdout, config: {{}} }}
  nodes:
    s: {{ kind: source, ref: o }}
    w: {{ kind: sink, ref: out }}
  edges:
    - {{ from: s, to: w }}
"#,
            csv = csv.display(),
            st = dir.path().join("state").display()
        ),
    );
    let out = Command::cargo_bin("faucet")
        .unwrap()
        .args(["validate", cfg_path.to_str().unwrap()])
        .assert()
        .success();
    let stdout = String::from_utf8_lossy(&out.get_output().stdout).to_string();
    assert!(
        !stdout.contains("`sla:` is ignored"),
        "sla is wired per sink node now: {stdout}"
    );
}

/// #459: a merge sink's OpenLineage job must name **both** sources as inputs.
///
/// This is the end-to-end proof, not a unit check of the context builder: it runs
/// a real two-source graph with a file transport and reads the emitted events
/// back. An earlier revision threaded node identities through the builder but
/// never populated them, so every event was silently suppressed — a test that
/// only asserted "the run succeeded" could not tell the difference.
#[test]
#[cfg(feature = "lineage")]
fn lineage_emits_a_job_per_sink_node_with_every_reaching_source() {
    let dir = TempDir::new().unwrap();
    let a = dir.path().join("a.csv");
    let b = dir.path().join("b.csv");
    write(&a, "id,v\n1,x\n2,y\n");
    write(&b, "id,v\n3,z\n");
    let out = dir.path().join("out.jsonl");
    let events = dir.path().join("lineage.jsonl");
    let cfg_path = dir.path().join("faucet.yaml");
    write(
        &cfg_path,
        &format!(
            r#"version: 1
name: merged
lineage:
  namespace: test-ns
  transport: {{ type: file, config: {{ path: {ev} }} }}
pipeline:
  sources:
    a: {{ type: csv, config: {{ path: {a} }} }}
    b: {{ type: csv, config: {{ path: {b} }} }}
  sinks:
    out: {{ type: jsonl, config: {{ path: {out} }} }}
  nodes:
    sa: {{ kind: source, ref: a }}
    sb: {{ kind: source, ref: b }}
    m: {{ kind: merge }}
    w: {{ kind: sink, ref: out }}
  edges:
    - {{ from: sa, to: m }}
    - {{ from: sb, to: m }}
    - {{ from: m, to: w }}
"#,
            a = a.display(),
            b = b.display(),
            out = out.display(),
            ev = events.display()
        ),
    );
    Command::cargo_bin("faucet")
        .unwrap()
        .args(["run", cfg_path.to_str().unwrap()])
        .assert()
        .success();

    let body = fs::read_to_string(&events).expect("lineage transport wrote events");
    let evs: Vec<serde_json::Value> = body
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("each line is one RunEvent"))
        .collect();
    assert!(!evs.is_empty(), "at least one event: {body}");

    // One job per sink node, named `{pipeline}.{node_id}`.
    for e in &evs {
        assert_eq!(e["job"]["name"], "merged.w", "job per sink node: {e}");
        assert_eq!(e["job"]["namespace"], "test-ns");
    }
    let kinds: Vec<&str> = evs
        .iter()
        .map(|e| e["eventType"].as_str().unwrap_or_default())
        .collect();
    assert!(kinds.contains(&"START"), "START emitted: {kinds:?}");
    assert!(kinds.contains(&"COMPLETE"), "COMPLETE emitted: {kinds:?}");

    // The terminal event names both sources as inputs and carries no column
    // lineage (not derivable across a merge).
    let done = evs
        .iter()
        .find(|e| e["eventType"] == "COMPLETE")
        .expect("terminal event");
    let inputs = done["inputs"].as_array().expect("inputs array");
    assert_eq!(inputs.len(), 2, "both sources are inputs: {done}");
    let mut names: Vec<String> = inputs
        .iter()
        .map(|d| d["name"].as_str().unwrap_or_default().to_string())
        .collect();
    names.sort();
    assert!(
        names[0].ends_with("a.csv") && names[1].ends_with("b.csv"),
        "inputs name the real files: {names:?}"
    );
    assert_eq!(done["outputs"].as_array().map(Vec::len), Some(1));
    let facets = &done["outputs"][0]["facets"];
    assert!(
        facets.get("columnLineage").is_none(),
        "column lineage is not fabricated for a multi-input sink: {facets}"
    );
}

/// A single-source graph is the shape where column lineage stays expressible, so
/// the linear case must keep exactly one input — the check that the N-input
/// change did not turn every job into a fan-in.
#[test]
#[cfg(feature = "lineage")]
fn lineage_keeps_one_input_for_a_linear_graph() {
    let dir = TempDir::new().unwrap();
    let csv = orders_csv(dir.path());
    let out = dir.path().join("out.jsonl");
    let events = dir.path().join("lineage.jsonl");
    let cfg_path = dir.path().join("faucet.yaml");
    write(
        &cfg_path,
        &format!(
            r#"version: 1
name: linear
lineage:
  namespace: ns
  transport: {{ type: file, config: {{ path: {ev} }} }}
pipeline:
  sources:
    o: {{ type: csv, config: {{ path: {csv} }} }}
  sinks:
    out: {{ type: jsonl, config: {{ path: {out} }} }}
  nodes:
    s: {{ kind: source, ref: o }}
    w: {{ kind: sink, ref: out }}
  edges:
    - {{ from: s, to: w }}
"#,
            csv = csv.display(),
            out = out.display(),
            ev = events.display()
        ),
    );
    Command::cargo_bin("faucet")
        .unwrap()
        .args(["run", cfg_path.to_str().unwrap()])
        .assert()
        .success();
    let body = fs::read_to_string(&events).unwrap();
    let done: serde_json::Value = body
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str::<serde_json::Value>(l).unwrap())
        .find(|e| e["eventType"] == "COMPLETE")
        .expect("terminal event");
    assert_eq!(done["inputs"].as_array().map(Vec::len), Some(1));
    assert_eq!(done["job"]["name"], "linear.w");
}

/// #459: the catalog records a dataset per source and per sink, plus one edge per
/// (source, sink) pair the graph actually connects — so a merge produces two
/// edges into the same sink, each carrying its own source's volume.
///
/// Read back through `faucet catalog`, which is the surface an operator uses, so
/// the test fails if either the write path or the read path regresses.
#[test]
#[cfg(all(feature = "catalog", feature = "serve-history-sqlite"))]
fn catalog_records_a_dataset_per_node_and_an_edge_per_pair() {
    let dir = TempDir::new().unwrap();
    let a = dir.path().join("a.csv");
    let b = dir.path().join("b.csv");
    write(&a, "id,v\n1,x\n2,y\n");
    write(&b, "id,v\n3,z\n");
    let out = dir.path().join("out.jsonl");
    let store = dir.path().join("catalog.db");
    let cfg_path = dir.path().join("faucet.yaml");
    write(
        &cfg_path,
        &format!(
            r#"version: 1
name: cat-merge
catalog:
  url: sqlite:{store}
pipeline:
  sources:
    a: {{ type: csv, config: {{ path: {a} }} }}
    b: {{ type: csv, config: {{ path: {b} }} }}
  sinks:
    out: {{ type: jsonl, config: {{ path: {out} }} }}
  nodes:
    sa: {{ kind: source, ref: a }}
    sb: {{ kind: source, ref: b }}
    m: {{ kind: merge }}
    w: {{ kind: sink, ref: out }}
  edges:
    - {{ from: sa, to: m }}
    - {{ from: sb, to: m }}
    - {{ from: m, to: w }}
"#,
            a = a.display(),
            b = b.display(),
            out = out.display(),
            store = store.display()
        ),
    );
    Command::cargo_bin("faucet")
        .unwrap()
        .args(["run", cfg_path.to_str().unwrap()])
        .assert()
        .success();

    let listed = Command::cargo_bin("faucet")
        .unwrap()
        .args([
            "catalog",
            "datasets",
            "--config",
            cfg_path.to_str().unwrap(),
            "--json",
        ])
        .assert()
        .success();
    let body = String::from_utf8_lossy(&listed.get_output().stdout).to_string();
    let json: serde_json::Value = serde_json::from_str(&body).expect("--json emits JSON");
    let rows = json
        .get("datasets")
        .and_then(|d| d.as_array())
        .or_else(|| json.as_array())
        .expect("a dataset list");
    let uris: Vec<String> = rows
        .iter()
        .map(|d| d["uri"].as_str().unwrap_or_default().to_string())
        .collect();
    // Both sources and the sink, not just the sink.
    for want in ["a.csv", "b.csv", "out.jsonl"] {
        assert!(
            uris.iter().any(|u| u.ends_with(want)),
            "{want} recorded: {uris:?}"
        );
    }

    // The lineage graph carries one edge per contributing source, and the volumes
    // are per-source (2 + 1) rather than the sink total repeated.
    let shown = Command::cargo_bin("faucet")
        .unwrap()
        .args([
            "catalog",
            "lineage",
            "--config",
            cfg_path.to_str().unwrap(),
            "--json",
        ])
        .assert()
        .success();
    let detail: serde_json::Value =
        serde_json::from_str(&String::from_utf8_lossy(&shown.get_output().stdout)).unwrap();
    let edges = detail["edges"].as_array().expect("edges array");
    assert_eq!(edges.len(), 2, "one edge per contributing source: {detail}");
    for e in edges {
        assert!(
            e["dst_uri"]
                .as_str()
                .unwrap_or_default()
                .ends_with("out.jsonl"),
            "both edges land on the sink: {e}"
        );
    }
    let mut vols: Vec<u64> = edges
        .iter()
        .map(|e| e["last_records"].as_u64().unwrap_or_default())
        .collect();
    vols.sort_unstable();
    assert_eq!(
        vols,
        vec![1, 2],
        "per-source volume, not the sink total: {edges:?}"
    );
}
