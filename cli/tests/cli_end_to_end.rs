//! Integration tests for the `faucet` binary. Each test drives the binary
//! built by cargo via `assert_cmd`, mirroring how users invoke it.

use assert_cmd::Command;
use predicates::str::contains;
use std::fs;
use std::path::Path;
use tempfile::TempDir;

/// Helper: a config that reads two records from a CSV file and writes them
/// as JSONL. Uses absolute paths so the test isn't sensitive to cwd.
fn csv_to_jsonl_yaml(csv: &Path, out: &Path) -> String {
    format!(
        r#"version: 1
name: csv_to_jsonl_smoke
pipeline:
  source:
    type: csv
    config:
      path: {csv}
  sink:
    type: jsonl
    config:
      path: {out}
"#,
        csv = csv.display(),
        out = out.display(),
    )
}

#[test]
fn list_lists_compiled_in_connectors() {
    Command::cargo_bin("faucet")
        .unwrap()
        .arg("list")
        .assert()
        .success()
        .stdout(contains("Sources:"))
        .stdout(contains("Sinks:"))
        .stdout(contains("rest "))
        .stdout(contains("jsonl "));
}

#[cfg(feature = "transforms")]
#[test]
fn list_lists_compiled_in_transforms() {
    Command::cargo_bin("faucet")
        .unwrap()
        .arg("list")
        .assert()
        .success()
        .stdout(contains("Transforms:"))
        // Two-column rendering: name + one-line description.
        .stdout(contains("flatten "))
        .stdout(contains("keys_case "))
        .stdout(contains("Re-case every key"));
}

#[cfg(feature = "transforms")]
#[test]
fn schema_prints_transform_schema() {
    // `flatten` has a single optional field — the schema must surface it
    // (with the default `__` separator).
    Command::cargo_bin("faucet")
        .unwrap()
        .args(["schema", "transform", "flatten"])
        .assert()
        .success()
        .stdout(contains("\"separator\""))
        .stdout(contains("\"__\""));
}

#[cfg(feature = "transforms")]
#[test]
fn schema_transform_keys_case_lists_modes() {
    // The KeyCaseMode enum must round-trip through JsonSchema so users
    // discover valid values without reading the source.
    Command::cargo_bin("faucet")
        .unwrap()
        .args(["schema", "transform", "keys_case"])
        .assert()
        .success()
        .stdout(contains("snake"))
        .stdout(contains("camel"))
        .stdout(contains("screaming_snake"));
}

#[test]
fn schema_rejects_unknown_transform() {
    Command::cargo_bin("faucet")
        .unwrap()
        .args(["schema", "transform", "make_uppercase"])
        .assert()
        .failure()
        .stderr(contains("unknown transform 'make_uppercase'"));
}

#[test]
fn schema_prints_jsonl_sink_schema() {
    Command::cargo_bin("faucet")
        .unwrap()
        .args(["schema", "sink", "jsonl"])
        .assert()
        .success()
        .stdout(contains("\"path\""));
}

#[cfg(feature = "masking")]
#[test]
fn schema_prints_masking_schema() {
    Command::cargo_bin("faucet")
        .unwrap()
        .args(["schema", "masking"])
        .assert()
        .success()
        .stdout(contains("MaskingSpec"))
        .stdout(contains("value_detector"));
}

#[cfg(feature = "masking")]
#[test]
fn masking_verb_shows_per_destination_scoping() {
    // Two named sinks; one rule scoped to `secure` only, one unscoped. The
    // verb must show the scoped rule under `secure` but not under `default`.
    let dir = TempDir::new().unwrap();
    let cfg = dir.path().join("faucet.yaml");
    fs::write(
        &cfg,
        r#"version: 1
name: masking_scope_smoke
pipeline:
  source: { type: csv, config: { path: ./in.csv } }
  sinks:
    default: { type: jsonl, config: { path: ./a.jsonl } }
    secure:  { type: jsonl, config: { path: ./b.jsonl } }
  masking:
    rules:
      - name: everywhere
        match: { value_detector: email }
        action: { type: redact }
      - name: secure-only
        match: { fields: [ssn] }
        action: { type: hash }
        applies_to: [secure]
matrix:
  - id: to_default
    sink: { ref: default }
  - id: to_secure
    sink: { ref: secure }
"#,
    )
    .unwrap();
    Command::cargo_bin("faucet")
        .unwrap()
        .args(["masking", "--no-env-file"])
        .arg(&cfg)
        .assert()
        .success()
        .stdout(contains("masking — valid (2 rules)"))
        .stdout(contains("- default [jsonl]: everywhere"))
        .stdout(contains("- secure [jsonl]: everywhere, secure-only"));
}

#[cfg(feature = "masking")]
#[test]
fn masking_verb_errors_without_a_masking_block() {
    let dir = TempDir::new().unwrap();
    let cfg = dir.path().join("faucet.yaml");
    fs::write(
        &cfg,
        csv_to_jsonl_yaml(Path::new("./in.csv"), Path::new("./out.jsonl")),
    )
    .unwrap();
    Command::cargo_bin("faucet")
        .unwrap()
        .args(["masking", "--no-env-file"])
        .arg(&cfg)
        .assert()
        .failure()
        .stderr(contains("no `pipeline.masking:` block"));
}

#[test]
fn schema_rejects_unknown_kind() {
    Command::cargo_bin("faucet")
        .unwrap()
        .args(["schema", "source", "nope"])
        .assert()
        .failure()
        .stderr(contains("unknown source 'nope'"));
}

#[test]
fn init_scaffolds_pipeline_yaml() {
    let dir = TempDir::new().unwrap();
    let out = dir.path().join("pipeline.yaml");
    Command::cargo_bin("faucet")
        .unwrap()
        .args(["init", "my_pipeline", "--output"])
        .arg(&out)
        .assert()
        .success()
        .stdout(contains("wrote"));
    let body = fs::read_to_string(&out).unwrap();
    assert!(body.contains("name: my_pipeline"));
    assert!(body.contains("type: rest"));
}

#[test]
fn init_refuses_to_overwrite_existing_file() {
    let dir = TempDir::new().unwrap();
    let out = dir.path().join("pipeline.yaml");
    fs::write(&out, "version: 1\n").unwrap();
    Command::cargo_bin("faucet")
        .unwrap()
        .args(["init", "again", "--output"])
        .arg(&out)
        .assert()
        .failure()
        .stderr(contains("refusing to overwrite"));
}

#[test]
fn init_no_args_uses_rest_jsonl_defaults() {
    let dir = TempDir::new().unwrap();
    let out = dir.path().join("pipeline.yaml");
    Command::cargo_bin("faucet")
        .unwrap()
        .args(["init", "--output"])
        .arg(&out)
        .assert()
        .success();
    let body = fs::read_to_string(&out).unwrap();
    assert!(body.contains("name: my-pipeline"));
    assert!(body.contains("type: rest"));
    assert!(body.contains("type: jsonl"));
}

#[test]
fn init_with_source_sink_flags_uses_those_kinds() {
    let dir = TempDir::new().unwrap();
    let out = dir.path().join("pipeline.yaml");
    Command::cargo_bin("faucet")
        .unwrap()
        .args([
            "init", "my_pipe", "--source", "rest", "--sink", "bigquery", "-o",
        ])
        .arg(&out)
        .assert()
        .success();
    let body = fs::read_to_string(&out).unwrap();
    assert!(body.contains("name: my_pipe"));
    assert!(body.contains("type: rest"));
    assert!(body.contains("type: bigquery"));
    // Required fields are surfaced with the REQUIRED marker so users know
    // exactly what to fill in.
    assert!(body.contains("# REQUIRED"));
    assert!(body.contains("project_id"));
    assert!(body.contains("dataset_id"));
    // Optional fields are commented out so users don't accidentally override
    // their connector-level defaults.
    assert!(body.contains("# batch_size"));
    // Tagged-enum fields (here: BigQuery `credentials:` and REST `auth:`) emit
    // every variant as a commented "alternative" block so users can switch
    // without bouncing to `faucet schema`. Default variant is inline; the
    // alternatives header announces the rest.
    assert!(
        body.contains("Alternative variants"),
        "missing alternatives block:\n{body}"
    );
    assert!(
        body.contains("# type: bearer"),
        "REST bearer alternative missing:\n{body}"
    );
    assert!(
        body.contains("# type: oauth2"),
        "REST oauth2 alternative missing:\n{body}"
    );
    assert!(
        body.contains("# type: application_default"),
        "BigQuery application_default alternative missing:\n{body}"
    );
}

#[test]
fn init_unknown_source_kind_lists_available_kinds() {
    let dir = TempDir::new().unwrap();
    let out = dir.path().join("pipeline.yaml");
    Command::cargo_bin("faucet")
        .unwrap()
        .args(["init", "--source", "nope", "--output"])
        .arg(&out)
        .assert()
        .failure()
        .stderr(contains("unknown source 'nope'"))
        .stderr(contains("rest"));
}

#[test]
fn init_unknown_sink_kind_lists_available_kinds() {
    let dir = TempDir::new().unwrap();
    let out = dir.path().join("pipeline.yaml");
    Command::cargo_bin("faucet")
        .unwrap()
        .args(["init", "--sink", "nope", "--output"])
        .arg(&out)
        .assert()
        .failure()
        .stderr(contains("unknown sink 'nope'"))
        .stderr(contains("jsonl"));
}

#[test]
fn init_force_overwrites_existing_file() {
    let dir = TempDir::new().unwrap();
    let out = dir.path().join("pipeline.yaml");
    fs::write(&out, "stale: contents\n").unwrap();
    Command::cargo_bin("faucet")
        .unwrap()
        .args(["init", "--force", "--output"])
        .arg(&out)
        .assert()
        .success();
    let body = fs::read_to_string(&out).unwrap();
    assert!(!body.contains("stale: contents"));
    assert!(body.contains("type: rest"));
}

#[test]
fn init_output_is_valid_yaml() {
    let dir = TempDir::new().unwrap();
    let out = dir.path().join("pipeline.yaml");
    Command::cargo_bin("faucet")
        .unwrap()
        .args(["init", "--source", "rest", "--sink", "jsonl", "--output"])
        .arg(&out)
        .assert()
        .success();
    let body = fs::read_to_string(&out).unwrap();
    // The scaffold itself parses as YAML even before the user fills in the
    // REQUIRED fields — the placeholders are valid YAML values. (Semantic
    // validation via `faucet validate` would still fail because of the empty
    // `base_url`, but `serde_yaml` should consume the structure.)
    serde_yaml::from_str::<serde_yaml::Value>(&body).expect("init output should parse as YAML");
}

#[test]
fn validate_accepts_csv_to_jsonl_yaml() {
    let dir = TempDir::new().unwrap();
    let csv = dir.path().join("in.csv");
    let out = dir.path().join("out.jsonl");
    fs::write(&csv, "name,score\nalice,1\nbob,2\n").unwrap();

    let yaml = csv_to_jsonl_yaml(&csv, &out);
    let cfg = dir.path().join("pipeline.yaml");
    fs::write(&cfg, yaml).unwrap();

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["validate"])
        .arg(&cfg)
        .assert()
        .success()
        .stdout(contains("source=csv"))
        .stdout(contains("sink=jsonl"))
        .stdout(contains("rows=1"));
}

#[test]
fn validate_accepts_upsert_on_postgres_with_key() {
    // Upsert on a supported sink with a non-empty key passes load-time
    // validation. `validate` only expands + reports — no DB connection.
    let dir = TempDir::new().unwrap();
    let cfg = dir.path().join("pipeline.yaml");
    fs::write(
        &cfg,
        r#"version: 1
name: upsert_ok
pipeline:
  source:
    type: rest
    config:
      base_url: http://x
  sink:
    type: postgres
    config:
      connection_url: postgres://x
      table_name: t
      column_mapping: auto_map
      write_mode: upsert
      key: [id]
"#,
    )
    .unwrap();

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["validate"])
        .arg(&cfg)
        .assert()
        .success()
        .stdout(contains("sink=postgres"));
}

#[test]
fn validate_rejects_upsert_on_jsonl_sink() {
    // Upsert on an append-only sink fails load-time validation with a clear
    // message naming the mode and the sink.
    let dir = TempDir::new().unwrap();
    let cfg = dir.path().join("pipeline.yaml");
    fs::write(
        &cfg,
        r#"version: 1
name: upsert_bad
pipeline:
  source:
    type: rest
    config:
      base_url: http://x
  sink:
    type: jsonl
    config:
      path: out.jsonl
      write_mode: upsert
      key: [id]
"#,
    )
    .unwrap();

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["validate"])
        .arg(&cfg)
        .assert()
        .failure()
        .stderr(contains("write_mode"))
        .stderr(contains("upsert"))
        .stderr(contains("jsonl"));
}

#[test]
fn run_executes_csv_to_jsonl_pipeline() {
    let dir = TempDir::new().unwrap();
    let csv = dir.path().join("in.csv");
    let out = dir.path().join("out.jsonl");
    fs::write(&csv, "name,score\nalice,1\nbob,2\n").unwrap();

    let yaml = csv_to_jsonl_yaml(&csv, &out);
    let cfg = dir.path().join("pipeline.yaml");
    fs::write(&cfg, yaml).unwrap();

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["run"])
        .arg(&cfg)
        .assert()
        .success()
        .stderr(contains("wrote 2 records"))
        .stderr(contains("1 invocation"));

    let lines: Vec<_> = fs::read_to_string(&out)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect();
    assert_eq!(lines.len(), 2);
    assert!(lines[0].contains("\"alice\""));
}

/// A `schema:` drift block must not break a normal run when the sink reports
/// no `current_schema` (jsonl is schemaless): the drift pass is inert. This
/// proves the executor wiring (`with_schema_drift`) compiles and is harmless
/// against a schemaless sink — same output as the plain csv→jsonl run.
#[test]
fn run_with_schema_drift_warn_against_schemaless_sink_is_inert() {
    let dir = TempDir::new().unwrap();
    let csv = dir.path().join("in.csv");
    let out = dir.path().join("out.jsonl");
    fs::write(&csv, "name,score\nalice,1\nbob,2\n").unwrap();

    let yaml = format!(
        r#"version: 1
name: csv_to_jsonl_drift
pipeline:
  source:
    type: csv
    config:
      path: {csv}
  sink:
    type: jsonl
    config:
      path: {out}
  schema:
    on_drift: warn
"#,
        csv = csv.display(),
        out = out.display(),
    );
    let cfg = dir.path().join("pipeline.yaml");
    fs::write(&cfg, yaml).unwrap();

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["run"])
        .arg(&cfg)
        .assert()
        .success()
        .stderr(contains("wrote 2 records"))
        .stderr(contains("1 invocation"));

    // Output is unchanged: the drift policy is present but harmless.
    let lines: Vec<_> = fs::read_to_string(&out)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect();
    assert_eq!(lines.len(), 2);
    assert!(lines[0].contains("\"alice\""));
}

/// Create a SQLite database file with `table_sql` using the system `sqlite3`
/// CLI. Used to pre-seed a destination table whose shape is a strict subset of
/// the incoming page so the drift pass has something to detect. Returns whether
/// the `sqlite3` binary was available (tests skip themselves if not).
fn sqlite_exec(db: &Path, sql: &str) -> bool {
    match std::process::Command::new("sqlite3")
        .arg(db)
        .arg(sql)
        .output()
    {
        Ok(out) => {
            assert!(
                out.status.success(),
                "sqlite3 failed: {}",
                String::from_utf8_lossy(&out.stderr)
            );
            true
        }
        // `sqlite3` not installed on this host — caller skips the test.
        Err(_) => false,
    }
}

/// Read the output of a `sqlite3 <db> <query>` invocation as trimmed stdout.
fn sqlite_query(db: &Path, sql: &str) -> String {
    let out = std::process::Command::new("sqlite3")
        .arg(db)
        .arg(sql)
        .output()
        .expect("sqlite3 query");
    assert!(
        out.status.success(),
        "sqlite3 query failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// End-to-end proof that the schema-drift policy actually *fires* through a
/// schema-reporting sink — the real regression guard for the executor seam
/// (`with_schema_drift`). Unlike the schemaless-jsonl test above (which is inert
/// because jsonl reports no `current_schema`), this drives csv → **sqlite** with
/// the destination table pre-created as a strict subset (`id` only) of the
/// incoming rows (which also carry `name`). With `pipeline.schema.on_drift:
/// fail` the run MUST abort.
///
/// This exercises the whole chain: config parses the `schema:` block →
/// `expand` carries it onto the node → `executor` attaches the policy via
/// `pipeline.with_schema_drift(...)` → `run_stream` runs the per-page drift pass
/// → the sqlite `current_schema` PRAGMA returns `{id}` → `diff_schema` finds the
/// `name` addition → the `fail` policy raises `FaucetError::SchemaDrift` →
/// the run exits non-zero. If the wiring were broken (policy never attached),
/// the drift pass would not run and the page would write successfully — so a
/// SUCCESS here would catch a regression in the seam.
#[test]
fn run_schema_drift_fail_fires_through_a_schema_reporting_sink() {
    let dir = TempDir::new().unwrap();
    let csv = dir.path().join("in.csv");
    let db = dir.path().join("dest.db");
    // Rows carry both `id` and `name`; the destination table will have only `id`.
    fs::write(&csv, "id,name\n1,alice\n2,bob\n").unwrap();

    // Pre-create the destination table as a STRICT SUBSET of the source rows.
    if !sqlite_exec(&db, "CREATE TABLE t (id INTEGER);") {
        eprintln!("skipping: sqlite3 CLI not available");
        return;
    }

    let yaml = format!(
        r#"version: 1
name: csv_to_sqlite_drift_fail
pipeline:
  source:
    type: csv
    config:
      path: {csv}
  sink:
    type: sqlite
    config:
      database_url: sqlite:{db}
      table_name: t
      column_mapping: auto_map
  schema:
    on_drift: fail
"#,
        csv = csv.display(),
        db = db.display(),
    );
    let cfg = dir.path().join("pipeline.yaml");
    fs::write(&cfg, yaml).unwrap();

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["run"])
        .arg(&cfg)
        .assert()
        .failure()
        // The aborting FaucetError reaches stderr via the executor's
        // `tracing::error!(error = %err, ...)`; it names the drifted column.
        .stderr(contains("Schema drift"))
        .stderr(contains("name"));

    // The page never committed: the table is still empty.
    assert_eq!(sqlite_query(&db, "SELECT count(*) FROM t;"), "0");
}

/// Positive counterpart: same csv → sqlite setup with the destination table a
/// strict subset (`id` only), but `pipeline.schema.on_drift: ignore`. The run
/// must SUCCEED and the unknown `name` column is stripped before the write, so
/// the destination ends up with the two `id` values and nothing else. This
/// proves the policy fires and takes the `ignore` branch (drop-unknown-fields),
/// the complement of the `fail` test above.
#[test]
fn run_schema_drift_ignore_strips_unknown_columns_through_sqlite() {
    let dir = TempDir::new().unwrap();
    let csv = dir.path().join("in.csv");
    let db = dir.path().join("dest.db");
    fs::write(&csv, "id,name\n1,alice\n2,bob\n").unwrap();

    if !sqlite_exec(&db, "CREATE TABLE t (id INTEGER);") {
        eprintln!("skipping: sqlite3 CLI not available");
        return;
    }

    let yaml = format!(
        r#"version: 1
name: csv_to_sqlite_drift_ignore
pipeline:
  source:
    type: csv
    config:
      path: {csv}
  sink:
    type: sqlite
    config:
      database_url: sqlite:{db}
      table_name: t
      column_mapping: auto_map
  schema:
    on_drift: ignore
"#,
        csv = csv.display(),
        db = db.display(),
    );
    let cfg = dir.path().join("pipeline.yaml");
    fs::write(&cfg, yaml).unwrap();

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["run"])
        .arg(&cfg)
        .assert()
        .success()
        .stderr(contains("wrote 2 records"));

    // Both rows landed with only the in-schema `id` column; `name` was stripped.
    assert_eq!(sqlite_query(&db, "SELECT count(*) FROM t;"), "2");
    assert_eq!(sqlite_query(&db, "SELECT id FROM t ORDER BY id;"), "1\n2");
}

#[test]
fn run_with_dry_run_does_not_touch_the_sink_path() {
    let dir = TempDir::new().unwrap();
    let csv = dir.path().join("in.csv");
    let out = dir.path().join("out.jsonl");
    fs::write(&csv, "name\nalice\nbob\n").unwrap();

    let yaml = csv_to_jsonl_yaml(&csv, &out);
    let cfg = dir.path().join("pipeline.yaml");
    fs::write(&cfg, yaml).unwrap();

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["run", "--dry-run"])
        .arg(&cfg)
        .assert()
        .success();

    assert!(
        !out.exists(),
        "dry-run must not write to the configured sink path"
    );
}

#[test]
fn run_with_limit_caps_records_written() {
    let dir = TempDir::new().unwrap();
    let csv = dir.path().join("in.csv");
    let out = dir.path().join("out.jsonl");
    fs::write(&csv, "name\nalice\nbob\ncarol\n").unwrap();

    let yaml = csv_to_jsonl_yaml(&csv, &out);
    let cfg = dir.path().join("pipeline.yaml");
    fs::write(&cfg, yaml).unwrap();

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["run", "--limit", "2"])
        .arg(&cfg)
        .assert()
        .success();

    let body = fs::read_to_string(&out).unwrap();
    assert_eq!(body.lines().count(), 2);
}

#[test]
fn run_with_state_path_persists_a_bookmark_dir() {
    // CSV source doesn't return bookmarks; this just exercises the wiring so
    // the override doesn't crash when present alongside a non-stateful source.
    let dir = TempDir::new().unwrap();
    let csv = dir.path().join("in.csv");
    let out = dir.path().join("out.jsonl");
    fs::write(&csv, "name\nalice\n").unwrap();
    let yaml = csv_to_jsonl_yaml(&csv, &out);
    let cfg = dir.path().join("pipeline.yaml");
    fs::write(&cfg, yaml).unwrap();
    let state_dir = dir.path().join("state");

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["run", "--state-path"])
        .arg(&state_dir)
        .arg(&cfg)
        .assert()
        .success();
}

#[test]
fn env_interpolation_resolves_inside_config_values() {
    let dir = TempDir::new().unwrap();
    let csv = dir.path().join("in.csv");
    let out = dir.path().join("out.jsonl");
    fs::write(&csv, "name\nalice\n").unwrap();

    let cfg_text = format!(
        r#"version: 1
pipeline:
  source:
    type: csv
    config:
      path: ${{env:FAUCET_TEST_CSV_PATH}}
  sink:
    type: jsonl
    config:
      path: {out}
"#,
        out = out.display()
    );
    let cfg = dir.path().join("pipeline.yaml");
    fs::write(&cfg, cfg_text).unwrap();

    Command::cargo_bin("faucet")
        .unwrap()
        .env("FAUCET_TEST_CSV_PATH", &csv)
        .args(["run"])
        .arg(&cfg)
        .assert()
        .success();
}

#[test]
fn shipped_example_yamls_pass_validate() {
    // Validate every example under cli/examples/. The YAMLs reference
    // environment variables that the env-interpolator needs present, so
    // stuff placeholders in for every var any example mentions. `validate`
    // is offline — placeholders are safe.
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let env_placeholders: &[(&str, &str)] = &[
        ("API_KEY", "x"),
        ("API_TOKEN", "x"),
        ("API_USER", "x"),
        ("API_PASS", "x"),
        ("AUTH_TOKEN", "x"),
        ("DATABRICKS_TOKEN", "x"),
        ("ES_USER", "x"),
        ("ES_PASS", "x"),
        ("ES_API_KEY", "x"),
        ("GCP_KEY_JSON", "{}"),
        ("GITHUB_TOKEN", "x"),
        ("GRPC_API_KEY", "x"),
        ("GRPC_TOKEN", "x"),
        ("INGEST_TOKEN", "x"),
        ("INGEST_USER", "x"),
        ("INGEST_PASS", "x"),
        // postgres_to_bigquery_with_lineage.yaml (OpenLineage HTTP transport).
        ("MARQUEZ_URL", "http://localhost:5000/api/v1/lineage"),
        ("PG_URL", "postgres://u:p@localhost/db"),
        // postgres_cdc_to_postgres_upsert.yaml (CDC source + upsert mirror).
        ("SOURCE_PG_URL", "postgres://u:p@localhost/src"),
        ("DEST_PG_URL", "postgres://u:p@localhost/dst"),
        ("SNOWFLAKE_OAUTH_TOKEN", "x"),
        ("SOAP_USER", "x"),
        ("SOAP_PASS", "x"),
        ("STRIPE_TOKEN", "x"),
        ("FEED_TOKEN", "x"),
        // sftp_to_jsonl.yaml (SFTP source password).
        ("SFTP_PASSWORD", "x"),
        // airtable_to_jsonl.yaml (Airtable PAT + base id, via the rest source).
        ("AIRTABLE_TOKEN", "x"),
        ("AIRTABLE_BASE_ID", "appXXXXXXXXXXXXXX"),
        // shared_auth_rest.yaml (top-level `auth:` catalog provider).
        ("API_BASE_URL", "https://api.example.com"),
        ("API_TOKEN_URL", "https://auth.example.com/oauth/token"),
        ("API_CLIENT_ID", "x"),
        ("API_CLIENT_SECRET", "x"),
        (
            "GOOGLE_APPLICATION_CREDENTIALS",
            "/tmp/service-account.json",
        ),
        // csv_to_jsonl_with_notifications.yaml (#280 notification channels).
        (
            "SLACK_WEBHOOK_URL",
            "https://hooks.slack.com/services/T0/B0/x",
        ),
        ("PAGERDUTY_ROUTING_KEY", "x"),
        ("FAUCET_WEBHOOK_SECRET", "x"),
    ];
    let examples_dir = std::path::Path::new(manifest_dir).join("examples");
    // Some examples interpolate `${file:./snowflake_key.pem}`. We run faucet
    // with cwd set to a temp dir that holds a placeholder PEM so the file
    // directive can resolve.
    let workdir = TempDir::new().unwrap();
    fs::write(workdir.path().join("snowflake_key.pem"), "dummy-key").unwrap();
    // `validate` compiles every transform chain, and some transforms read files
    // at compile time — the `sql` transform resolves its reference relations, the
    // `wasm` transform loads its module. Those examples name the fixtures by a
    // repo-relative path, so stage the fixture trees at the same relative paths
    // under the temp cwd. Add to this list if a new example references a
    // compile-time fixture; a missing entry fails loudly below rather than
    // silently skipping the example.
    let repo_root = std::path::Path::new(manifest_dir).parent().unwrap();
    for rel in ["cli/examples/data", "examples/wasm-transforms"] {
        let src_dir = repo_root.join(rel);
        let dst_dir = workdir.path().join(rel);
        fs::create_dir_all(&dst_dir).unwrap();
        for entry in fs::read_dir(&src_dir)
            .unwrap_or_else(|e| panic!("fixture dir {} is missing: {e}", src_dir.display()))
        {
            let src = entry.unwrap().path();
            if src.is_file() {
                fs::copy(&src, dst_dir.join(src.file_name().unwrap())).unwrap();
            }
        }
    }

    let mut count = 0;
    for entry in fs::read_dir(&examples_dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|e| e.to_str()) != Some("yaml") {
            continue;
        }
        // serve_minimal.yaml is a `faucet serve --default-config` partial: it
        // carries workspace defaults only (no source/sink — those arrive per
        // HTTP request), so it intentionally does not pass standalone expand.
        if path.file_name().and_then(|f| f.to_str()) == Some("serve_minimal.yaml") {
            continue;
        }
        // Skip examples that require a feature the test binary wasn't built
        // with.  In CI `--all-features` covers everything; local feature-
        // specific test runs (e.g. `--features serve`) must not trip on
        // example YAMLs that need the orthogonal `schedule` feature (or vice
        // versa).
        #[cfg(not(feature = "schedule"))]
        {
            let yaml_text = fs::read_to_string(&path).unwrap_or_default();
            if yaml_text.contains("\nschedule:") || yaml_text.starts_with("schedule:") {
                continue;
            }
        }
        // `masking:` is a deny_unknown_fields key gated on the `masking`
        // feature; a build without it can't parse those examples.
        #[cfg(not(feature = "masking"))]
        {
            let yaml_text = fs::read_to_string(&path).unwrap_or_default();
            if yaml_text.contains("\n  masking:") || yaml_text.contains("\nmasking:") {
                continue;
            }
        }
        // `notifications:` is a deny_unknown_fields key gated on the `notify`
        // feature; a build without it can't parse those examples.
        #[cfg(not(feature = "notify"))]
        {
            let yaml_text = fs::read_to_string(&path).unwrap_or_default();
            if yaml_text.contains("\nnotifications:") {
                continue;
            }
        }
        // `catalog:` is a deny_unknown_fields key gated on the `catalog`
        // feature; a build without it can't parse those examples.
        #[cfg(not(feature = "catalog"))]
        {
            let yaml_text = fs::read_to_string(&path).unwrap_or_default();
            if yaml_text.contains("\ncatalog:") || yaml_text.starts_with("catalog:") {
                continue;
            }
        }
        // The `sql` / `wasm` transforms are feature-gated, and `validate` now
        // compiles every transform chain — so an example using one cannot
        // validate in a build that lacks it.
        #[cfg(not(feature = "transform-sql"))]
        {
            let yaml_text = fs::read_to_string(&path).unwrap_or_default();
            if yaml_text.contains("type: sql") {
                continue;
            }
        }
        #[cfg(not(feature = "transform-wasm"))]
        {
            let yaml_text = fs::read_to_string(&path).unwrap_or_default();
            if yaml_text.contains("type: wasm") {
                continue;
            }
        }
        count += 1;
        let mut cmd = Command::cargo_bin("faucet").unwrap();
        for (k, v) in env_placeholders {
            cmd.env(k, v);
        }
        // `--no-secrets` validates grammar / structure / expansion without
        // resolving secrets-manager directives (e.g. `${vault:...}`), which
        // would otherwise require live backends unavailable in CI. It is a
        // no-op for the (majority) of examples that reference no secrets.
        cmd.current_dir(workdir.path())
            .args(["validate", "--no-secrets"])
            .arg(&path)
            .assert()
            .success();
    }
    assert!(count >= 30, "expected many YAML examples, got {count}");
}

#[cfg(feature = "masking")]
#[test]
fn run_masks_pii_before_writing_to_the_sink() {
    // End-to-end: a real `faucet run` masks matching fields before they reach
    // the sink. Exercises the executor's masking attach + the run_stream pass.
    let dir = TempDir::new().unwrap();
    let csv = dir.path().join("in.csv");
    let out = dir.path().join("out.jsonl");
    fs::write(&csv, "name,email,uid\nAl,al@example.com,u1\n").unwrap();
    let cfg = dir.path().join("faucet.yaml");
    fs::write(
        &cfg,
        format!(
            r#"version: 1
name: mask_run
pipeline:
  source: {{ type: csv, config: {{ path: {csv} }} }}
  masking:
    key: k
    rules:
      - match: {{ value_detector: email }}
        action: {{ type: redact }}
      - match: {{ fields: [uid] }}
        action: {{ type: hash }}
  sink: {{ type: jsonl, config: {{ path: {out} }} }}
"#,
            csv = csv.display(),
            out = out.display(),
        ),
    )
    .unwrap();
    Command::cargo_bin("faucet")
        .unwrap()
        .args(["run", "--no-env-file"])
        .arg(&cfg)
        .assert()
        .success();
    let written = fs::read_to_string(&out).unwrap();
    let rec: serde_json::Value = serde_json::from_str(written.lines().next().unwrap()).unwrap();
    assert_eq!(rec["name"], "Al", "non-PII untouched");
    assert_eq!(rec["email"], "***", "email redacted before the sink");
    assert_eq!(
        rec["uid"].as_str().unwrap().len(),
        64,
        "uid replaced by a 64-hex-char hash"
    );
    assert_ne!(rec["uid"], "u1");
}

#[test]
fn run_auto_discovers_faucet_yaml_and_dotenv_in_cwd() {
    // #55: cwd-based config + .env auto-discovery. `faucet run` with no
    // positional path picks up `faucet.yaml`, and `${env:VAR}` resolves
    // against a `.env` in the same directory.
    let dir = TempDir::new().unwrap();
    let csv = dir.path().join("in.csv");
    let out = dir.path().join("out.jsonl");
    fs::write(&csv, "name\nzed\n").unwrap();
    fs::write(
        dir.path().join(".env"),
        format!("DISCOVERED_OUT={}\n", out.display()),
    )
    .unwrap();
    fs::write(
        dir.path().join("faucet.yaml"),
        format!(
            r#"version: 1
pipeline:
  source:
    type: csv
    config:
      path: {csv}
  sink:
    type: jsonl
    config:
      path: ${{env:DISCOVERED_OUT}}
"#,
            csv = csv.display(),
        ),
    )
    .unwrap();

    Command::cargo_bin("faucet")
        .unwrap()
        .current_dir(dir.path())
        .env_remove("DISCOVERED_OUT")
        .arg("run")
        .assert()
        .success()
        .stderr(contains("wrote 1 record"));

    assert!(out.exists(), "auto-discovered run should produce output");
}

#[test]
fn run_with_no_config_and_no_from_env_errors() {
    // No positional path, no --from-env, no faucet.* in cwd → clear error.
    let dir = TempDir::new().unwrap();
    Command::cargo_bin("faucet")
        .unwrap()
        .current_dir(dir.path())
        .arg("run")
        .assert()
        .failure()
        .stderr(contains("no pipeline config"));
}

#[test]
fn run_no_env_file_skips_dotenv_auto_load() {
    // With --no-env-file, a present .env must NOT be loaded. We prove this by
    // requiring an env var that is only defined in .env, and asserting failure.
    let dir = TempDir::new().unwrap();
    let csv = dir.path().join("in.csv");
    fs::write(&csv, "name\nx\n").unwrap();
    fs::write(
        dir.path().join(".env"),
        "FAUCET_TEST_SKIPPED_PATH=/tmp/should-not-be-read.jsonl\n",
    )
    .unwrap();
    fs::write(
        dir.path().join("faucet.yaml"),
        format!(
            r#"version: 1
pipeline:
  source:
    type: csv
    config:
      path: {csv}
  sink:
    type: jsonl
    config:
      path: ${{env:FAUCET_TEST_SKIPPED_PATH}}
"#,
            csv = csv.display(),
        ),
    )
    .unwrap();

    Command::cargo_bin("faucet")
        .unwrap()
        .current_dir(dir.path())
        .env_remove("FAUCET_TEST_SKIPPED_PATH")
        .args(["run", "--no-env-file"])
        .assert()
        .failure()
        .stderr(contains("FAUCET_TEST_SKIPPED_PATH"));
}

#[test]
fn init_with_template_flag_names_the_template() {
    let dir = TempDir::new().unwrap();
    let out = dir.path().join("p.yaml");
    Command::cargo_bin("faucet")
        .unwrap()
        .args(["init", "--template", "users_api", "--output"])
        .arg(&out)
        .assert()
        .success();
    let body = fs::read_to_string(&out).unwrap();
    assert!(
        body.contains("  sources:\n    users_api:"),
        "expected `  sources:\\n    users_api:` in:\n{body}"
    );
    assert!(
        body.contains("  sinks:\n    users_api:"),
        "expected `  sinks:\\n    users_api:` in:\n{body}"
    );
}

#[test]
fn missing_env_var_in_config_is_reported() {
    let dir = TempDir::new().unwrap();
    let cfg = dir.path().join("pipeline.yaml");
    fs::write(
        &cfg,
        r#"version: 1
pipeline:
  source:
    type: csv
    config:
      path: ${env:FAUCET_DEFINITELY_UNSET}
  sink:
    type: jsonl
    config:
      path: /tmp/no.jsonl
"#,
    )
    .unwrap();

    Command::cargo_bin("faucet")
        .unwrap()
        .env_remove("FAUCET_DEFINITELY_UNSET")
        .args(["validate"])
        .arg(&cfg)
        .assert()
        .failure()
        .stderr(contains("missing environment variable"));
}

#[test]
fn init_output_loads_and_expands() {
    // Regression guard: ensure `faucet init` produces a YAML file that
    // PipelineConfig::from_path + expand() accept without error. This catches
    // indent / structural bugs (e.g. CONFIG_INDENT at the wrong depth) that
    // substring assertions miss — a misplaced indent causes the connector config
    // to parse as `null`, but the kinds still appear as sibling keys, so
    // body.contains("base_url") would pass while the config is semantically wrong.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("p.yaml");
    Command::cargo_bin("faucet")
        .unwrap()
        .args(["init", "--source", "rest", "--sink", "jsonl", "--output"])
        .arg(&path)
        .assert()
        .success();

    let cfg = faucet_cli::config::PipelineConfig::from_path(&path, None)
        .expect("init output must load via PipelineConfig::from_path");
    let nodes = faucet_cli::expand::expand(&cfg).expect("init output must expand cleanly");
    assert_eq!(nodes.len(), 1, "expected exactly one expanded node");
    assert_eq!(nodes[0].source.kind, "rest");
    assert_eq!(nodes[0].sink.kind, "jsonl");
    // Crucially: the connector config must be properly nested under `config:`,
    // not floated up as siblings. Verify the source config is a non-null object
    // (an empty object would also signal structural breakage).
    assert!(
        nodes[0].source.config.is_object(),
        "source config must be a JSON object (got {:?}); \
         likely a CONFIG_INDENT bug causing fields to float above `config:`",
        nodes[0].source.config
    );
}

/// Helper: a csv→jsonl config with a `contract:` block and (optionally) a DLQ.
#[cfg(feature = "contract")]
fn contract_yaml(csv: &Path, out: &Path, dlq: Option<&Path>, on_breach: &str) -> String {
    let dlq_block = match dlq {
        Some(p) => format!(
            "  dlq:\n    sink: {{ type: jsonl, config: {{ path: {} }} }}\n",
            p.display()
        ),
        None => String::new(),
    };
    format!(
        r#"version: 1
name: csv_contract
pipeline:
  source:
    type: csv
    config:
      path: {csv}
  sink:
    type: jsonl
    config:
      path: {out}
{dlq_block}  contract:
    version: "1.0.0"
    on_breach: {on_breach}
    fields:
      - {{ name: name, type: string }}
      - {{ name: status, type: string, enum: [open, closed] }}
"#,
        csv = csv.display(),
        out = out.display(),
    )
}

#[cfg(feature = "contract")]
#[test]
fn run_with_contract_quarantine_routes_breaches_to_dlq() {
    let dir = TempDir::new().unwrap();
    let csv = dir.path().join("in.csv");
    let out = dir.path().join("out.jsonl");
    let dlq = dir.path().join("dlq.jsonl");
    fs::write(&csv, "name,status\nalice,open\nbob,bogus\n").unwrap();

    let cfg = dir.path().join("pipeline.yaml");
    fs::write(&cfg, contract_yaml(&csv, &out, Some(&dlq), "quarantine")).unwrap();

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["run"])
        .arg(&cfg)
        .assert()
        .success()
        .stderr(contains("wrote 1 record"));

    let out_body = fs::read_to_string(&out).unwrap();
    assert!(out_body.contains("\"alice\""), "{out_body}");
    assert!(!out_body.contains("\"bob\""), "{out_body}");
    let dlq_body = fs::read_to_string(&dlq).unwrap();
    assert!(dlq_body.contains("ContractViolation"), "{dlq_body}");
    assert!(dlq_body.contains("\"bob\""), "{dlq_body}");
    assert!(dlq_body.contains("1.0.0"), "{dlq_body}");
}

#[cfg(feature = "contract")]
#[test]
fn run_with_contract_fail_aborts_run() {
    let dir = TempDir::new().unwrap();
    let csv = dir.path().join("in.csv");
    let out = dir.path().join("out.jsonl");
    fs::write(&csv, "name,status\nalice,open\nbob,bogus\n").unwrap();

    let cfg = dir.path().join("pipeline.yaml");
    fs::write(&cfg, contract_yaml(&csv, &out, None, "fail")).unwrap();

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["run"])
        .arg(&cfg)
        .assert()
        .failure()
        .stderr(contains("Contract v1.0.0 violated"))
        .stderr(contains("status"));
}

#[cfg(feature = "contract")]
#[test]
fn run_with_contract_warn_writes_everything() {
    let dir = TempDir::new().unwrap();
    let csv = dir.path().join("in.csv");
    let out = dir.path().join("out.jsonl");
    fs::write(&csv, "name,status\nalice,open\nbob,bogus\n").unwrap();

    let cfg = dir.path().join("pipeline.yaml");
    fs::write(&cfg, contract_yaml(&csv, &out, None, "warn")).unwrap();

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["run"])
        .arg(&cfg)
        .assert()
        .success()
        .stderr(contains("wrote 2 records"));

    let out_body = fs::read_to_string(&out).unwrap();
    assert!(
        out_body.contains("\"bob\""),
        "warn must write breaching records"
    );
}

#[cfg(feature = "contract")]
#[test]
fn validate_rejects_contract_quarantine_without_dlq() {
    let dir = TempDir::new().unwrap();
    let csv = dir.path().join("in.csv");
    let out = dir.path().join("out.jsonl");
    let cfg = dir.path().join("pipeline.yaml");
    fs::write(&cfg, contract_yaml(&csv, &out, None, "quarantine")).unwrap();

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["validate"])
        .arg(&cfg)
        .assert()
        .failure()
        .stderr(contains("on_breach: quarantine"))
        .stderr(contains("dlq"));
}

#[cfg(feature = "contract")]
#[test]
fn contract_command_prints_summary() {
    let dir = TempDir::new().unwrap();
    let csv = dir.path().join("in.csv");
    let out = dir.path().join("out.jsonl");
    let cfg = dir.path().join("pipeline.yaml");
    fs::write(&cfg, contract_yaml(&csv, &out, None, "warn")).unwrap();

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["contract"])
        .arg(&cfg)
        .assert()
        .success()
        .stdout(contains("contract v1.0.0 — valid (2 fields)"))
        .stdout(contains("on_breach: warn"))
        .stdout(contains("- status: string (enum[2])"));
}

#[cfg(feature = "contract")]
#[test]
fn contract_command_exports_json_schema_and_openlineage() {
    let dir = TempDir::new().unwrap();
    let csv = dir.path().join("in.csv");
    let out = dir.path().join("out.jsonl");
    let cfg = dir.path().join("pipeline.yaml");
    fs::write(&cfg, contract_yaml(&csv, &out, None, "warn")).unwrap();

    let assert = Command::cargo_bin("faucet")
        .unwrap()
        .args(["contract"])
        .arg(&cfg)
        .args(["--export", "json-schema"])
        .assert()
        .success();
    let schema: serde_json::Value =
        serde_json::from_slice(&assert.get_output().stdout).expect("valid JSON schema output");
    assert_eq!(schema["x-faucet-contract-version"], "1.0.0");
    assert_eq!(schema["type"], "object");
    assert_eq!(schema["required"], serde_json::json!(["name", "status"]));

    let assert = Command::cargo_bin("faucet")
        .unwrap()
        .args(["contract"])
        .arg(&cfg)
        .args(["--export", "openlineage"])
        .assert()
        .success();
    let facet: serde_json::Value =
        serde_json::from_slice(&assert.get_output().stdout).expect("valid OL facet output");
    assert!(
        facet["_schemaURL"]
            .as_str()
            .unwrap()
            .contains("SchemaDatasetFacet")
    );
    assert_eq!(facet["fields"].as_array().unwrap().len(), 2);
}

#[cfg(feature = "contract")]
#[test]
fn contract_command_errors_without_contract_block() {
    let dir = TempDir::new().unwrap();
    let csv = dir.path().join("in.csv");
    let out = dir.path().join("out.jsonl");
    let cfg = dir.path().join("pipeline.yaml");
    fs::write(&cfg, csv_to_jsonl_yaml(&csv, &out)).unwrap();

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["contract"])
        .arg(&cfg)
        .assert()
        .failure()
        .stderr(contains("no `pipeline.contract:` block"));
}

#[cfg(feature = "contract")]
#[test]
fn schema_contract_prints_contract_spec_schema() {
    let assert = Command::cargo_bin("faucet")
        .unwrap()
        .args(["schema", "contract"])
        .assert()
        .success();
    let schema: serde_json::Value =
        serde_json::from_slice(&assert.get_output().stdout).expect("valid JSON output");
    assert!(schema["properties"].get("version").is_some());
    assert!(schema["properties"].get("fields").is_some());
    assert!(schema["properties"].get("on_breach").is_some());
}

#[test]
fn list_json_emits_structured_listing() {
    let assert = Command::cargo_bin("faucet")
        .unwrap()
        .args(["list", "--json"])
        .assert()
        .success();
    let out: serde_json::Value =
        serde_json::from_slice(&assert.get_output().stdout).expect("valid JSON output");
    let source_names: Vec<&str> = out["sources"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["name"].as_str().unwrap())
        .collect();
    let sink_names: Vec<&str> = out["sinks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["name"].as_str().unwrap())
        .collect();
    assert!(source_names.contains(&"rest"), "sources: {source_names:?}");
    assert!(sink_names.contains(&"jsonl"), "sinks: {sink_names:?}");
    assert!(out["transforms"].is_array());
    assert!(out["state_stores"].is_array());
}

#[test]
fn list_available_human_and_json() {
    // Human path: the registry index renders with the header + the compiled
    // marker legend.
    Command::cargo_bin("faucet")
        .unwrap()
        .args(["list", "--available"])
        .assert()
        .success()
        .stdout(contains("Registry connectors"));

    // JSON path: a `connectors` array whose entries carry kind/name/compiled.
    let assert = Command::cargo_bin("faucet")
        .unwrap()
        .args(["list", "--available", "--json"])
        .assert()
        .success();
    let out: serde_json::Value =
        serde_json::from_slice(&assert.get_output().stdout).expect("valid JSON output");
    let rows = out["connectors"].as_array().expect("connectors array");
    assert!(!rows.is_empty(), "registry index should list connectors");
    let first = &rows[0];
    for key in ["kind", "name", "description", "compiled"] {
        assert!(
            first.get(key).is_some(),
            "connector entry missing `{key}`: {first}"
        );
    }
    assert!(
        first["compiled"].is_boolean(),
        "`compiled` should be a bool"
    );
    // `rest` is a built-in source and is compiled into this binary.
    let rest = rows
        .iter()
        .find(|c| c["kind"] == "source" && c["name"] == "rest")
        .expect("rest source in the registry index");
    assert_eq!(rest["compiled"], true, "rest should be marked compiled");
}

#[test]
fn validate_json_topology_mode() {
    let dir = TempDir::new().unwrap();
    let cfg = dir.path().join("topo.yaml");
    // A minimal linear topology (source node → sink node) exercises the
    // topology-mode `--json` branch of `faucet validate`.
    fs::write(
        &cfg,
        r#"version: 1
name: topo_smoke
pipeline:
  sources:
    a: { type: csv, config: { path: ./in.csv } }
  sinks:
    o: { type: jsonl, config: { path: ./out.jsonl } }
  nodes:
    s: { kind: source, ref: a }
    w: { kind: sink, ref: o }
  edges:
    - { from: s, to: w }
"#,
    )
    .unwrap();
    let assert = Command::cargo_bin("faucet")
        .unwrap()
        .args(["validate", cfg.to_str().unwrap(), "--json"])
        .assert()
        .success();
    let out: serde_json::Value =
        serde_json::from_slice(&assert.get_output().stdout).expect("valid JSON output");
    assert_eq!(out["valid"], true);
    assert_eq!(out["mode"], "topology");
    assert_eq!(out["name"], "topo_smoke");
    assert_eq!(out["node_count"], 2);
    assert_eq!(out["edge_count"], 1);
    let node_ids: Vec<&str> = out["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n["id"].as_str().unwrap())
        .collect();
    assert!(
        node_ids.contains(&"s") && node_ids.contains(&"w"),
        "nodes: {node_ids:?}"
    );
}

#[test]
fn schema_list_enumerates_targets() {
    let assert = Command::cargo_bin("faucet")
        .unwrap()
        .args(["schema", "--list"])
        .assert()
        .success();
    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    assert!(stdout.contains("source"), "{stdout}");
    assert!(stdout.contains("sink"), "{stdout}");
    assert!(stdout.contains("dlq"), "{stdout}");
}

#[test]
fn validate_json_emits_summary() {
    let dir = TempDir::new().unwrap();
    let csv = dir.path().join("in.csv");
    let out = dir.path().join("out.jsonl");
    // `validate` is offline, so the CSV need not exist; still write it so the
    // config is fully realistic.
    fs::write(&csv, "id\n1\n").unwrap();
    let cfg = dir.path().join("faucet.yaml");
    fs::write(&cfg, csv_to_jsonl_yaml(&csv, &out)).unwrap();

    let assert = Command::cargo_bin("faucet")
        .unwrap()
        .args(["validate", cfg.to_str().unwrap(), "--json"])
        .assert()
        .success();
    let summary: serde_json::Value =
        serde_json::from_slice(&assert.get_output().stdout).expect("valid JSON output");
    assert_eq!(summary["valid"], true);
    assert_eq!(summary["name"], "csv_to_jsonl_smoke");
    assert_eq!(summary["row_count"], 1);
    let rows = summary["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["source"], "csv");
    assert_eq!(rows[0]["sink"], "jsonl");
    assert_eq!(rows[0]["role"], "root");
}
