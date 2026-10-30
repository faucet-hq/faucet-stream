use super::*;

/// A caller-supplied fragment (an inline overlay) is screened like a
/// submitted config: references, subprocess connectors, paths and URLs.
#[test]
fn a_caller_fragment_is_screened_like_a_submitted_config() {
    let root = tempfile::tempdir().unwrap();
    let args = crate::cli::ReferenceArgs {
        allow_path_root: vec![root.path().to_path_buf()],
        allow_env: vec!["FAUCET_FRAGMENT_OK".into()],
        ..Default::default()
    };
    let refs = crate::serve::references::ServeReferencePolicy::from_args(&args).unwrap();
    let caller = refs.for_origin(
        BodyOrigin::Caller {
            allow_subprocess: false,
        },
        None,
    );
    let inside = root.path().join("state").display().to_string();
    let ok = serde_json::json!({
        "execution": { "max_concurrent": 1 },
        "state": { "type": "file", "config": { "path": inside } },
        "notifications": [{ "channel": { "type": "webhook", "config": {
            "url": "https://hooks.example/${env:FAUCET_FRAGMENT_OK}" } } }],
        "dlq": { "sink": { "type": "jsonl", "config": { "path": format!("{inside}/dlq.jsonl") } } },
    });
    screen_caller_fragment(&ok, &caller).expect("an allowed fragment passes");

    let forbidden = |doc: Value, needle: &str| {
        let err = screen_caller_fragment(&doc, &caller).unwrap_err();
        assert!(
            matches!(&err, ServeError::Forbidden(m) if m.contains(needle)),
            "{needle}: {err:?}"
        );
    };
    forbidden(
        serde_json::json!({ "state": { "type": "file", "config": { "path": "/proc/self" } } }),
        "/proc/self",
    );
    forbidden(
        serde_json::json!({ "dlq": { "sink": { "type": "jsonl", "config": {
                "path": "/var/faucet-elsewhere/dlq.jsonl" } } } }),
        "faucet-elsewhere",
    );
    forbidden(
        serde_json::json!({ "sla": { "note": "${file:/etc/hostname}" } }),
        "/etc/hostname",
    );
    forbidden(
        serde_json::json!({ "x": "http://169.254.169.254/latest" }),
        "metadata",
    );
    forbidden(
        serde_json::json!({ "x": "https://${env:FAUCET_FRAGMENT_OK}/hook" }),
        "may not come from a token",
    );
    // A value bound after screening (params, vars, row tokens) is refused
    // wherever it appears; the clock tokens stay usable.
    forbidden(
        serde_json::json!({ "notifications": [{ "channel": { "type": "webhook",
                "config": { "url": "${param.api_root}" } } }] }),
        "${param.api_root}",
    );
    forbidden(
        serde_json::json!({ "state": { "type": "postgres", "config": { "url": "${vars.db}" } } }),
        "${vars.db}",
    );
    screen_caller_fragment(
        &serde_json::json!({ "state": { "type": "file", "config": {
                "path": format!("{inside}/${{now.date}}") } } }),
        &caller,
    )
    .expect("a clock token is not a bound value");
    let err = screen_caller_fragment(
        &serde_json::json!({ "dlq": { "sink": { "type": "singer", "config": {} } } }),
        &caller,
    )
    .unwrap_err();
    assert!(matches!(err, ServeError::Unprocessable { .. }), "{err:?}");
    // Open (operator) policy still refuses the system trees.
    assert!(
        screen_caller_fragment(
            &serde_json::json!({ "state": { "type": "file", "config": { "path": "/sys/x" } } }),
            &ReferencePolicy::open()
        )
        .is_err()
    );
    assert!(!is_absolute_url("://x"));
    assert!(!is_absolute_url("1http://x"));
    assert!(!is_absolute_url("a b://x"));
    assert!(is_absolute_url("postgres+ssl://h/db"));
}

/// A path a request names directly (a DLQ location) is held to the
/// caller policy, the system trees and a tenant's root.
#[test]
fn a_request_path_is_screened_like_a_connector_path() {
    let base = tempfile::tempdir().unwrap();
    let tenants = base.path().join("tenants");
    std::fs::create_dir_all(tenants.join("acme")).unwrap();
    std::fs::create_dir_all(tenants.join("other")).unwrap();
    let args = crate::cli::ReferenceArgs {
        allow_path_root: vec![base.path().to_path_buf()],
        tenant_path_root: Some(tenants.clone()),
        ..Default::default()
    };
    let refs = crate::serve::references::ServeReferencePolicy::from_args(&args).unwrap();
    let caller = BodyOrigin::Caller {
        allow_subprocess: false,
    };
    let acme = refs.for_origin(caller, Some("acme"));
    let own = tenants.join("acme/dlq.jsonl").display().to_string();
    let theirs = tenants.join("other/dlq.jsonl").display().to_string();
    assert!(refuse_request_path_with("from", &own, true, true, &acme).is_ok());
    let err = refuse_request_path_with("from", &theirs, false, true, &acme).unwrap_err();
    assert!(
        matches!(&err, ServeError::Forbidden(m) if m.contains("`from`") && m.contains("--tenant-path-root")),
        "{err:?}"
    );
    let open = ReferencePolicy::open();
    for bad in ["/proc/self/environ", "/sys/kernel", "/dev/sda"] {
        let err = refuse_request_path_with("location", bad, false, false, &open).unwrap_err();
        assert!(
            matches!(&err, ServeError::Forbidden(m) if m.contains("system tree")),
            "{bad}"
        );
    }
    let operator = refs.for_origin(caller, None);
    let outside = "/var/tmp-faucet-not-a-root/dlq.jsonl";
    let err = refuse_request_path_with("location", outside, true, false, &operator).unwrap_err();
    assert!(
        matches!(&err, ServeError::Forbidden(m) if m.contains("--allow-path-root") && !m.contains("tenant")),
        "{err:?}"
    );
    assert!(refuse_request_path("location", "/etc/shadow", true, None).is_err());
}

/// The sandbox switches come from the connector's schema (#876).
#[cfg(all(feature = "source-sqlite", feature = "sink-sqlite"))]
#[test]
fn sandboxed_kinds_are_read_from_the_schema() {
    assert_eq!(&*sandbox_fields(true, "sqlite"), &["sandbox".to_string()]);
    assert!(source_is_sandboxed("sqlite"));
    assert!(
        !sink_is_sandboxed("sqlite"),
        "the sqlite sink runs no caller SQL"
    );
    assert!(!source_is_sandboxed("no-such-kind"));
    let mut cfg = json!({});
    force_override_value(&mut cfg);
    assert!(cfg.as_object().unwrap().is_empty());
}

fn force_override_value(cfg: &mut Value) {
    let mut opt = Some(cfg.clone());
    force_override(&mut opt, true, "no-such-kind");
    *cfg = opt.unwrap();
    let mut none = None;
    force_override(&mut none, true, "no-such-kind");
    assert!(none.is_none(), "an unconfined kind gets no override block");
}

#[cfg(all(
    feature = "source-duckdb",
    feature = "sink-duckdb",
    feature = "source-sqlite"
))]
#[test]
fn every_sql_connector_position_is_sandboxed() {
    let doc = json!({
        "version": 1,
        "pipeline": {
            "source": { "type": "duckdb", "config": { "database": ":memory:", "query": "SELECT 1", "sandbox": false } },
            "sources": {
                "lite": { "type": "sqlite", "config": { "database_url": "sqlite::memory:", "query": "SELECT 1" } },
                "csv": { "type": "csv", "config": { "path": "a.csv" } }
            },
            "sink": { "type": "duckdb", "config": { "database": ":memory:", "table_name": "t" } },
            "sinks": { "lite": { "type": "sqlite", "config": { "database_url": "sqlite::memory:", "table": "t" } } },
            "dlq": { "sink": { "type": "duckdb", "config": { "database": ":memory:", "table_name": "d" } } }
        },
        "matrix": [
            { "id": "a", "source": { "ref": "lite", "config": { "sandbox": false } } },
            { "id": "b", "source": { "ref": "csv" }, "sink": { "type": "duckdb", "config": { "database": ":memory:", "table_name": "u" } },
              "dlq": { "sink": { "type": "duckdb", "config": { "database": ":memory:", "table_name": "e" } } } },
            { "id": "c", "source": { "ref": "csv" }, "sink": { "ref": "lite" } }
        ]
    });
    let mut cfg = PipelineConfig::from_value(doc).unwrap();
    confine_sql(&mut cfg);
    let on = |v: &Value| v.get("sandbox") == Some(&Value::Bool(true));
    let p = &cfg.pipeline;
    assert!(on(&p.source.as_ref().unwrap().config));
    assert!(on(&p.sources["lite"].config));
    assert!(!on(&p.sources["csv"].config));
    assert!(on(&p.sink.as_ref().unwrap().config));
    assert!(
        !on(&p.sinks["lite"].config),
        "the sqlite sink runs no caller SQL"
    );
    assert!(on(&p.dlq.as_ref().unwrap().sink.config));
    assert!(on(cfg.matrix[0]
        .source
        .as_ref()
        .unwrap()
        .config
        .as_ref()
        .unwrap()));
    assert!(on(cfg.matrix[1]
        .sink
        .as_ref()
        .unwrap()
        .config
        .as_ref()
        .unwrap()));
    assert!(on(&cfg.matrix[1]
        .dlq
        .as_ref()
        .unwrap()
        .as_ref()
        .unwrap()
        .sink
        .config));
    assert!(cfg.matrix[2].sink.as_ref().unwrap().config.is_none());

    let mut nodes = expand(&cfg).unwrap();
    for n in &mut nodes {
        if let Some(m) = n.source.config.as_object_mut() {
            m.insert("sandbox".into(), Value::Bool(false));
        }
    }
    confine_sql_nodes(&mut nodes);
    let a = nodes.iter().find(|n| n.id == "a").unwrap();
    assert!(on(&a.source.config));
    let b = nodes.iter().find(|n| n.id == "b").unwrap();
    assert!(!on(&b.source.config));
    assert!(on(&b.sink.config));
    assert!(on(&b.dlq.as_ref().unwrap().sink.config));
}

#[cfg(all(
    feature = "source-duckdb",
    feature = "sink-duckdb",
    feature = "source-sqlite"
))]
#[test]
fn topology_nodes_fan_out_sources_and_the_mirror_snapshot_are_sandboxed() {
    let topo = json!({
        "version": 1,
        "pipeline": {
            "sources": { "d": { "type": "duckdb", "config": { "database": ":memory:", "query": "SELECT 1" } } },
            "sinks": { "default": { "type": "duckdb", "config": { "database": ":memory:", "table_name": "t" } } },
            "nodes": {
                "src": { "kind": "source", "ref": "d", "config": { "sandbox": false } },
                "lite": { "kind": "source", "type": "sqlite", "config": { "database_url": "sqlite::memory:", "query": "SELECT 1" } },
                "csv": { "kind": "source", "type": "csv", "config": { "path": "a.csv" } },
                "out": { "kind": "sink" },
                "m": { "kind": "merge" }
            },
            "edges": []
        }
    });
    let mut cfg: PipelineConfig = serde_json::from_value(topo).unwrap();
    confine_sql(&mut cfg);
    let node_cfg = |id: &str| match &cfg.pipeline.nodes[id] {
        crate::config::NodeSpec::Source { config, .. }
        | crate::config::NodeSpec::Sink { config, .. } => config.clone(),
        _ => None,
    };
    let on =
        |v: Option<Value>| v.and_then(|v| v.get("sandbox").cloned()) == Some(Value::Bool(true));
    assert!(on(node_cfg("src")));
    assert!(on(node_cfg("lite")));
    assert!(!on(node_cfg("csv")));
    assert!(on(node_cfg("out")));

    let fan = json!({
        "version": 1,
        "pipeline": {
            "sources": { "lite": { "type": "sqlite", "config": { "database_url": "sqlite::memory:", "query": "SELECT 1" } } },
            "sink": { "type": "jsonl", "config": { "path": "o.jsonl" } }
        },
        "matrix": [ { "id": "r", "source": { "ref": "lite" },
            "fan_out": { "source": { "type": "sqlite", "config": { "database_url": "sqlite::memory:", "query": "SELECT 1 AS id" } }, "select": "$.id", "as": "id" } } ]
    });
    let mut cfg: PipelineConfig = serde_json::from_value(fan).unwrap();
    confine_sql(&mut cfg);
    let d = cfg.matrix[0].discover.as_ref().unwrap();
    assert_eq!(d.source.config.as_ref().unwrap()["sandbox"], true);

    let mirror = json!({
        "version": 1,
        "pipeline": {
            "source": { "type": "postgres-cdc", "config": {} },
            "sink": { "type": "jsonl", "config": { "path": "o.jsonl" } }
        },
        "mirror": { "mode": "snapshot_then_cdc",
            "snapshot": { "source": { "type": "sqlite", "config": { "database_url": "sqlite::memory:", "query": "SELECT 1" } } } }
    });
    let mut cfg: PipelineConfig = serde_json::from_value(mirror).unwrap();
    confine_sql(&mut cfg);
    assert_eq!(
        cfg.replication.as_ref().unwrap().snapshot.source.config["sandbox"],
        true
    );
}

#[test]
fn only_a_server_file_keeps_host_access() {
    assert!(BodyOrigin::Trusted.confines_sql());
    assert!(
        BodyOrigin::Caller {
            allow_subprocess: true
        }
        .confines_sql()
    );
    assert!(!BodyOrigin::ServerFile.confines_sql());
}

#[cfg(all(
    feature = "source-duckdb",
    feature = "sink-duckdb",
    feature = "source-sqlite"
))]
#[tokio::test]
async fn a_loaded_submission_is_sandboxed_unless_it_came_from_a_server_file() {
    let body = json!({
            "version": 1,
            "pipeline": {
                "source": { "type": "duckdb", "config": { "database": ":memory:", "query": "SELECT 1", "sandbox": false } },
                "sink": { "type": "jsonl", "config": { "path": "o.jsonl" } }
            }
        })
        .to_string();
    let caller = load_submission(&body, ConfigFormat::Json, None, None, BodyOrigin::Trusted)
        .await
        .unwrap();
    assert_eq!(caller.nodes[0].source.config["sandbox"], true);
    let file = load_submission(
        &body,
        ConfigFormat::Json,
        None,
        None,
        BodyOrigin::ServerFile,
    )
    .await
    .unwrap();
    assert_eq!(file.nodes[0].source.config["sandbox"], false);
}

#[test]
fn a_path_that_normalizes_into_proc_is_protected() {
    assert!(file_is_protected("/proc/self/environ"));
    assert!(file_is_protected("/tmp/../proc/self/environ"));
    assert!(file_is_protected("/nonexistent/./../proc/1/environ"));
    assert_eq!(
        lexical_normalize(std::path::Path::new("/a/./b/../c")),
        std::path::PathBuf::from("/a/c")
    );
}
use serde_json::json;

fn base() -> Value {
    json!({
        "version": 1,
        "pipeline": {
            "source": { "type": "csv", "config": { "path": "DEFAULT.csv" } },
            "sink": { "type": "jsonl", "config": { "path": "out.jsonl" } }
        }
    })
}

#[tokio::test]
async fn submitted_overrides_default() {
    let body = r#"{ "pipeline": { "source": { "config": { "path": "OVERRIDE.csv" } } } }"#;
    let loaded = load_submission(
        body,
        ConfigFormat::Json,
        Some(&base()),
        None,
        BodyOrigin::Trusted,
    )
    .await
    .unwrap();
    // The override wins; the default sink survives the merge.
    let node = &loaded.nodes[0];
    assert_eq!(node.source.config["path"], "OVERRIDE.csv");
    assert_eq!(node.sink.config["path"], "out.jsonl");
}

#[tokio::test]
async fn missing_version_without_base_is_unprocessable() {
    // version defaults to 1 via serde, so this exercises the expand/validation
    // failure path (pipeline with no source/sink). Accept either layer's error.
    let body = r#"{ "pipeline": {} }"#;
    let err = load_submission(body, ConfigFormat::Json, None, None, BodyOrigin::Trusted)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        ServeError::Unprocessable { .. } | ServeError::BadConfig(_)
    ));
}

#[cfg(feature = "schedule")]
#[tokio::test]
async fn schedule_block_is_rejected() {
    let body = r#"
version: 1
pipeline:
  source: { type: csv, config: { path: x.csv } }
  sink: { type: jsonl, config: { path: out.jsonl } }
schedule:
  cron: "0 * * * *"
  timezone: UTC
"#;
    let err = load_submission(body, ConfigFormat::Yaml, None, None, BodyOrigin::Trusted)
        .await
        .unwrap_err();
    match err {
        ServeError::BadConfig(m) => assert!(m.contains("schedule:")),
        other => panic!("expected BadConfig, got {other:?}"),
    }
}

#[tokio::test]
async fn invalid_yaml_is_bad_config() {
    let err = load_submission("{[bad", ConfigFormat::Yaml, None, None, BodyOrigin::Trusted)
        .await
        .unwrap_err();
    assert!(matches!(err, ServeError::BadConfig(_)));
}

#[tokio::test]
async fn submitted_extends_is_rejected_with_composition_hint() {
    // Composition must NOT run for HTTP-submitted bodies — otherwise a client
    // could read arbitrary server files via `extends`. `deny_unknown_fields`
    // rejects the key during `from_value` (no I/O), and `friendly_parse_error`
    // attaches the composition hint.
    let body = "version: 1\nextends: /etc/passwd\npipeline:\n  source: { type: csv, config: { path: x.csv } }\n  sink: { type: jsonl, config: { path: o.jsonl } }\n";
    let err = load_submission(body, ConfigFormat::Yaml, None, None, BodyOrigin::Trusted)
        .await
        .unwrap_err();
    // ServeError doesn't implement Display; pull the inner message directly.
    let msg = match &err {
        ServeError::Unprocessable { message, .. } => message.clone(),
        ServeError::BadConfig(m) => m.clone(),
        other => format!("{other:?}"),
    };
    // Assert the hint fired (not merely that serde named the field) so a
    // regression in `friendly_parse_error` is caught.
    assert!(
        msg.contains("composition"),
        "submitted extends must be rejected with the composition hint, got: {msg}"
    );
}

fn csv_body(path: &str) -> String {
    format!(
        "version: 1\npipeline:\n  source: {{ type: csv, config: {{ path: \"{path}\" }} }}\n  sink: {{ type: jsonl, config: {{ path: out.jsonl }} }}\n"
    )
}

const SINGER: &str = "version: 1\npipeline:\n  source: { type: singer, config: { executable: /bin/sh, stream: x } }\n  sink: { type: jsonl, config: { path: out.jsonl } }\n";

fn message(e: &ServeError) -> String {
    e.api_error().error.message
}

#[test]
fn server_secret_references_are_found() {
    let doc = json!({
        "a": "${env:FAUCET_VAULT_KEY}",
        "b": ["x ${secret:FAUCET_SERVE_ADMIN_TOKEN} y"],
        "c": {"${file:/proc/1/environ}": "${file:/proc/self/cmdline}"},
        "ok": "${env:HOME} ${file:./data.csv} ${vault:kv/x} ${users.id}"
    });
    let refs = server_secret_refs(&doc);
    assert_eq!(refs.len(), 4, "{refs:?}");
    assert!(refs.iter().all(|r| !r.contains("HOME")));

    protect_env("FAUCET_LOAD_TEST_PRINCIPAL");
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("tok");
    std::fs::write(&file, "x").unwrap();
    protect_file(&file);
    let doc = json!([
        "${env:FAUCET_LOAD_TEST_PRINCIPAL}",
        format!("${{file:{}}}", file.display()),
        format!("${{file:{}/./tok}}", dir.path().display()),
    ]);
    assert_eq!(server_secret_refs(&doc).len(), 3);
    assert!(refuse_server_secrets(&json!({"x": "${env:HOME}"})).is_ok());
    let err = refuse_server_secrets(&doc).unwrap_err();
    assert!(matches!(err, ServeError::Forbidden(_)));
}

#[test]
fn secret_manager_references_the_server_resolved_are_protected() {
    protect_secret_ref("vault", "secret/load-test#admin");
    let refs = server_secret_refs(&json!([
        "${vault:secret/load-test}",
        "${vault:secret/load-test#other}",
        "${aws-sm:secret/load-test}",
        "${vault:secret/load-test-2}",
    ]));
    assert_eq!(
        refs,
        vec![
            "${vault:secret/load-test}".to_string(),
            "${vault:secret/load-test#other}".to_string()
        ]
    );
}

#[test]
fn protect_input_covers_the_file_and_everything_it_reads() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("providers.yaml");
    std::fs::write(&input, "x").unwrap();
    let read = dir.path().join("client-secret");
    protect_input(
        &input,
        &json!({"a": "${env:LOAD_TEST_INPUT_VAR}", "b": format!("${{file:{}}}", read.display()),
                    "c": "${gcp-sm:projects/p/secrets/load-test/versions/1}"}),
    );
    let refs = server_secret_refs(&json!([
        format!("${{file:{}}}", input.display()),
        format!("${{file:{}}}", read.display()),
        "${env:LOAD_TEST_INPUT_VAR}",
        "${secret:LOAD_TEST_INPUT_VAR}",
        "${gcp-sm:projects/p/secrets/load-test/versions/1}",
    ]));
    assert_eq!(refs.len(), 5, "{refs:?}");
    for name in [
        "FAUCET_SERVE_HISTORY",
        "FAUCET_VAULT_PREVIOUS_KEYS",
        "FAUCET_SNAPSHOT_KEY",
    ] {
        assert!(env_is_protected(name), "{name}");
    }
}

#[test]
fn sqlite_urls_name_their_file() {
    assert_eq!(sqlite_path("sqlite:/var/h.db"), Some("/var/h.db"));
    assert_eq!(
        sqlite_path("sqlite:///var/h.db?mode=rwc"),
        Some("/var/h.db")
    );
    assert_eq!(sqlite_path("sqlite::memory:"), None);
    assert_eq!(sqlite_path("postgres://x"), None);
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("history.db");
    protect_sqlite_url(&format!("sqlite:{}", db.display()));
    assert!(file_is_protected(db.to_str().unwrap()));
    assert!(file_is_protected(&format!("{}-wal", db.display())));
}

fn read_protected(path: &str) -> bool {
    data_path_is_protected(path, Access::Read { recursive: false })
}

#[test]
fn local_paths_and_globs_that_reach_protected_files_are_found() {
    let dir = tempfile::tempdir().unwrap();
    let secrets = dir.path().join("etc");
    std::fs::create_dir(&secrets).unwrap();
    let auth = secrets.join("auth.yaml");
    std::fs::write(&auth, "x").unwrap();
    protect_file(&auth);
    let data = dir.path().join("data");
    std::fs::create_dir(&data).unwrap();
    let d = dir.path().display();

    assert!(read_protected("/proc/self/environ"));
    assert!(read_protected("/sys/kernel"));
    assert!(read_protected("/dev/stdin"));
    assert!(!read_protected("/dev/null"));
    assert!(read_protected(&auth.display().to_string()));
    assert!(read_protected(&format!("{d}/data/../etc/auth.yaml")));
    assert!(
        read_protected(&format!("{d}/etc")),
        "a directory holding one"
    );
    assert!(read_protected(&format!("{d}/etc/*.yaml")));
    assert!(
        read_protected(&format!("{d}/*")),
        "a glob that recurses into it"
    );
    assert!(read_protected(&format!("{d}/data/*/../../etc/auth.yaml")));
    assert!(read_protected(&format!("{d}/etc/${{now.date}}.yaml")));
    assert!(!read_protected(&format!("{d}/data/in.csv")));
    assert!(!read_protected(&format!("{d}/data/*.csv")));
    assert!(!read_protected(&format!("{d}/etc/*.csv")));
    assert!(!read_protected(&format!(
        "{d}/data/dt=${{now.date}}/x.jsonl"
    )));

    assert_eq!(local_path("sqlite:/x.db"), Some("/x.db"));
    assert_eq!(local_path("file:///x"), Some("/x"));
    assert_eq!(local_path("file:./x"), Some("./x"));
    assert_eq!(local_path("s3://b/k"), None);
    assert_eq!(local_path(":memory:"), None);
    assert_eq!(replace_tokens("a/${now.date}/b${x"), "a/*/b*");
}

#[test]
fn data_paths_are_checked_only_in_local_kinds_and_path_keys() {
    let doc = json!({
        "pipeline": {
            "source": {"type": "file", "config": {"path": "/proc/self/environ", "format": "raw_text"}},
            "sink": {"type": "rest", "config": {"path": "/proc/x"}},
            "sources": {"q": {"type": "duckdb", "config": {"database": "/dev/zero", "query": "/proc"}}}
        },
        "state": {"type": "file", "config": {"dir": "/sys/x"}},
        "lineage": {"transport": {"type": "file", "config": {"path": "out.jsonl"}}}
    });
    let found = server_data_paths(&doc, false);
    assert_eq!(found.len(), 3, "{found:?}");
    assert!(found.iter().all(|f| !f.contains("/proc/x")));

    let caller = json!({"type": "jsonl", "config": {"path": "out/${parent.name}.jsonl"}});
    assert_eq!(server_data_paths(&caller, true).len(), 1);
    assert!(server_data_paths(&caller, false).is_empty());
    let clock = json!({"type": "jsonl", "config": {"path": "out/${now.date}.jsonl"}});
    assert!(server_data_paths(&clock, true).is_empty());
}

#[test]
fn a_wildcard_in_any_segment_that_can_reach_a_system_tree_is_refused() {
    for glob in [
        "/p*/self/environ",
        "/*/self/environ",
        "/**/environ",
        "/[p]roc/self/environ",
        "/pro?/self/environ",
        "/[/x",
        "/${x}/self/environ",
    ] {
        assert!(read_protected(glob), "{glob}");
    }
    assert!(segments_can_reach(&["**", "x"], &["proc"]));
    assert!(segments_can_reach(&["proc"], &["proc"]));
    assert!(!segments_can_reach(&[], &["proc"]));
    assert!(!segments_can_reach(&["data", "*"], &["proc"]));
    assert!(!pattern_can_reach_system_dir("/data/*/x.csv"));
    assert!(!read_protected("/no-such-faucet-dir/*/x.csv"));
}

#[cfg(unix)]
#[test]
fn a_glob_match_that_is_a_symlink_into_a_system_tree_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink("/dev/zero", dir.path().join("link")).unwrap();
    std::fs::write(dir.path().join("plain.csv"), "a\n").unwrap();
    let d = dir.path().display();
    assert!(read_protected(&format!("{d}/l*")));
    assert!(!read_protected(&format!("{d}/*.csv")));
    assert!(
        !data_path_is_protected(&format!("{d}/l*"), Access::Write),
        "a sink path is never expanded"
    );
}

#[test]
fn writing_beside_a_protected_file_is_allowed_but_reading_it_is_not() {
    let dir = tempfile::tempdir().unwrap();
    let deep = dir.path().join("a").join("b");
    std::fs::create_dir_all(&deep).unwrap();
    let db = deep.join("history.db");
    std::fs::write(&db, "x").unwrap();
    protect_file(&db);
    let d = dir.path().display();
    let b = deep.display();
    let write = |p: &str| data_path_is_protected(p, Access::Write);
    assert!(write(&db.display().to_string()));
    assert!(
        write(&format!("{b}/{{part}}.db")),
        "a template that can name it"
    );
    assert!(!write(&b.to_string()), "a state store directory holding it");
    assert!(!write(&format!("{b}/out.jsonl")));
    assert!(read_protected(&b.to_string()), "a listing of its directory");
    assert!(!read_protected(&format!("{d}/a")), "a listing one level up");
    assert!(data_path_is_protected(
        &format!("{d}/a"),
        Access::Read { recursive: true }
    ));
    assert_eq!(replace_templates("x/{part}/y{z"), "x/*/y*");

    let state = json!({"pipeline": {"state": {"type": "file", "config": {"path": b.to_string()}}}});
    assert!(server_data_paths(&state, true).is_empty());
    let recursive = json!({"pipeline": {"source": {"type": "file", "config": {"path": format!("{d}/a"), "recursive": true}}}});
    assert_eq!(server_data_paths(&recursive, true).len(), 1);
    let node = json!({"pipeline": {"nodes": {"out": {"kind": "sink", "type": "file", "config": {"path": b.to_string()}}}}});
    assert!(server_data_paths(&node, true).is_empty());
}

#[test]
fn sql_transform_relations_are_path_checked() {
    let doc = json!({"pipeline": {"transforms": [{"type": "sql", "config": {
    "query": "SELECT * FROM batch, env",
    "relations": [
        {"name": "env", "source": {"type": "csv", "path": "/proc/self/environ", "has_header": false}},
        {"name": "j", "source": {"type": "jsonl", "path": "/sys/x"}},
        {"name": "v", "source": {"type": "values", "columns": ["a"], "rows": [[1]]}}
    ]}}]}});
    let found = server_data_paths(&doc, true);
    assert_eq!(found, vec!["/proc/self/environ", "/sys/x"]);
}

fn cfg_of(yaml: &str) -> PipelineConfig {
    PipelineConfig::from_value(crate::yaml::from_str(yaml).unwrap()).unwrap()
}

#[test]
fn template_overrides_without_a_type_are_checked_against_the_template_kind() {
    let caller = BodyOrigin::Caller {
        allow_subprocess: false,
    };
    let topology = cfg_of(
        "version: 1\npipeline:\n  sources:\n    x: { type: csv, config: { path: in.csv } }\n  sinks:\n    o: { type: jsonl, config: { path: out.jsonl } }\n  nodes:\n    src: { kind: source, ref: x, config: { path: /proc/self/environ } }\n    out: { kind: sink, ref: o }\n  edges:\n    - [src, out]\n",
    );
    let err = refuse_server_paths(&topology, &[], caller).unwrap_err();
    assert!(message(&err).contains("/proc/self/environ"), "{err:?}");

    let fan_out = cfg_of(
        "version: 1\npipeline:\n  sources:\n    x: { type: csv, config: { path: in.csv } }\n  sink: { type: stdout, config: {} }\nmatrix:\n  - id: r\n    fan_out:\n      source: { ref: x, config: { path: /sys/kernel } }\n      select: \"$.id\"\n      as: v\n  - id: standalone\n    fan_out:\n      source: { type: file, config: { path: /dev/stdin } }\n      select: \"$.id\"\n      as: w\n",
    );
    let err = refuse_server_paths(&fan_out, &[], caller).unwrap_err();
    assert!(message(&err).contains("/sys/kernel"), "{err:?}");
    assert!(message(&err).contains("/dev/stdin"), "{err:?}");

    let retyped = cfg_of(
        "version: 1\npipeline:\n  sources:\n    x: { type: rest, config: { path: /proc/self/environ } }\n  sink: { type: stdout, config: {} }\nmatrix:\n  - id: r\n    source: { ref: x, type: file }\n",
    );
    assert!(refuse_server_paths(&retyped, &[], caller).is_err());

    let sink = cfg_of(
        "version: 1\npipeline:\n  source: { type: csv, config: { path: in.csv } }\n  sinks:\n    o: { type: jsonl, config: { path: out.jsonl } }\nmatrix:\n  - id: r\n    sink: { ref: o, config: { path: /dev/stdout } }\n",
    );
    assert!(refuse_server_paths(&sink, &[], caller).is_err());

    let fine = cfg_of(
        "version: 1\npipeline:\n  sources:\n    x: { type: rest, config: { url: \"http://h\", path: /users } }\n  sink: { type: stdout, config: {} }\nmatrix:\n  - id: r\n    source: { ref: x, config: { path: \"/users/${now.date}\" } }\n  - id: missing\n    source: { ref: nope, config: { path: /proc/x } }\n",
    );
    assert!(refuse_server_paths(&fine, &[], caller).is_ok());
}

#[cfg(all(
    feature = "source-duckdb",
    feature = "sink-duckdb",
    feature = "source-sqlite"
))]
#[test]
fn bound_probes_and_the_verify_destination_are_sandboxed() {
    let mut cfg = cfg_of(
        "version: 1\npipeline:\n  source: { type: csv, config: { path: in.csv } }\n  sink: { type: stdout, config: {} }\npartition:\n  kind: integer\n  from: 0\n  chunk_size: 10\n  bounds: inclusive\n  to: { from_source: { type: duckdb, config: { database: \":memory:\", query: \"SELECT 1 AS m\" } }, value_path: \"$.m\" }\nverify:\n  key: [id]\n  destination: { type: sqlite, config: { database: x.db, query: \"SELECT 1\" } }\nmatrix:\n  - id: r\n    partition:\n      kind: offset\n      chunk_size: 10\n      total: { from_source: { type: sqlite, config: { database: x.db, query: \"SELECT 1 AS n\" } }, value_path: \"$.n\" }\n  - id: plain\n    partition: { kind: integer, from: 0, to: 5, chunk_size: 1, bounds: inclusive }\n",
    );
    confine_sql(&mut cfg);
    let top = cfg.partition.as_mut().and_then(|p| p.probe_mut()).unwrap();
    assert_eq!(top.from_source.config["sandbox"], true);
    let row = cfg.matrix[0]
        .partition
        .as_mut()
        .and_then(|p| p.probe_mut())
        .unwrap();
    assert_eq!(row.from_source.config["sandbox"], true);
    assert!(
        cfg.matrix[1]
            .partition
            .as_mut()
            .unwrap()
            .probe_mut()
            .is_none()
    );
    let dest = cfg.verify.as_ref().unwrap().destination.as_ref().unwrap();
    assert_eq!(dest.config["sandbox"], true);
}

#[cfg(feature = "source-duckdb")]
#[test]
fn the_reconcile_count_probe_is_sandboxed() {
    let mut cfg = cfg_of(
        "version: 1\npipeline:\n  source: { type: csv, config: { path: in.csv } }\n  sink: { type: stdout, config: {} }\nreconcile:\n  count: { type: duckdb, config: { database: \":memory:\", query: \"SELECT 1 AS n\" } }\n",
    );
    confine_sql(&mut cfg);
    assert_eq!(
        cfg.reconcile.as_ref().unwrap().count.config["sandbox"],
        true
    );
}

/// A transform that reads a file (`lookup`'s `jsonl`) is screened through
/// its schema's annotations like a connector.
#[tokio::test]
async fn a_lookup_transform_reading_outside_the_caller_roots_is_refused() {
    let caller = BodyOrigin::Caller {
        allow_subprocess: false,
    };
    for (path, protected) in [
        ("/var/faucet-not-a-root/ref.jsonl", false),
        ("/proc/self/environ", true),
    ] {
        let body = format!(
            "version: 1\npipeline:\n  source: {{ type: csv, config: {{ path: in.csv }} }}\n  sink: {{ type: stdout, config: {{}} }}\n  transforms:\n    - {{ type: lookup, config: {{ jsonl: \"{path}\", on: {{ record: k, ref: k }}, add: {{ v: v }} }} }}\n"
        );
        let err = load_submission(&body, ConfigFormat::Yaml, None, None, caller)
            .await
            .unwrap_err();
        assert!(
            matches!(&err, ServeError::Forbidden(m) if m.contains(path)
                    && (m.contains("system trees") == protected)),
            "{path}: {err:?}"
        );
    }
    let inside = std::env::temp_dir().join("faucet-lookup-ref.jsonl");
    let body = format!(
        "version: 1\npipeline:\n  source: {{ type: csv, config: {{ path: in.csv }} }}\n  sink: {{ type: stdout, config: {{}} }}\n  transforms:\n    - {{ type: lookup, config: {{ jsonl: \"{}\", on: {{ record: k, ref: k }}, add: {{ v: v }} }} }}\n",
        inside.display()
    );
    let err = load_submission(&body, ConfigFormat::Yaml, None, None, caller).await;
    assert!(
        !matches!(err, Err(ServeError::Forbidden(_))),
        "a path under the caller roots passes the screen: {err:?}"
    );
}

/// A `wasm` transform's module path is screened like any other file a
/// caller's config names.
#[cfg(feature = "transform-wasm")]
#[tokio::test]
async fn a_wasm_module_outside_the_caller_roots_is_refused() {
    let caller = BodyOrigin::Caller {
        allow_subprocess: false,
    };
    for path in ["/var/faucet-not-a-root/t.wasm", "/proc/self/environ"] {
        let body = format!(
            "version: 1\npipeline:\n  source: {{ type: csv, config: {{ path: in.csv }} }}\n  sink: {{ type: stdout, config: {{}} }}\n  transforms:\n    - {{ type: wasm, config: {{ module: \"{path}\" }} }}\n"
        );
        let err = load_submission(&body, ConfigFormat::Yaml, None, None, caller)
            .await
            .unwrap_err();
        assert!(
            matches!(&err, ServeError::Forbidden(m) if m.contains(path)),
            "{path}: {err:?}"
        );
    }
}

#[tokio::test]
async fn a_reconcile_probe_reading_a_protected_path_is_refused() {
    let caller = BodyOrigin::Caller {
        allow_subprocess: false,
    };
    let body = "version: 1\npipeline:\n  source: { type: csv, config: { path: in.csv } }\n  sink: { type: stdout, config: {} }\nreconcile:\n  count: { type: file, config: { path: /proc/self/environ, format: raw_text } }\n";
    let err = load_submission(body, ConfigFormat::Yaml, None, None, caller)
        .await
        .unwrap_err();
    assert!(matches!(err, ServeError::Forbidden(_)), "{err:?}");
}

#[tokio::test]
async fn a_probe_reading_a_protected_path_is_refused_before_it_runs() {
    let caller = BodyOrigin::Caller {
        allow_subprocess: false,
    };
    let body = "version: 1\npipeline:\n  source: { type: csv, config: { path: in.csv } }\n  sink: { type: stdout, config: {} }\npartition:\n  kind: integer\n  from: 0\n  chunk_size: 10\n  bounds: inclusive\n  to: { from_source: { type: file, config: { path: /proc/self/environ, format: raw_text } }, value_path: \"$.nope\" }\n";
    let err = load_submission(body, ConfigFormat::Yaml, None, None, caller)
        .await
        .unwrap_err();
    assert!(matches!(err, ServeError::Forbidden(_)), "{err:?}");
}

#[tokio::test]
async fn loading_refuses_local_paths_to_server_files_even_through_a_row_override() {
    let caller = BodyOrigin::Caller {
        allow_subprocess: false,
    };
    let direct = "version: 1\npipeline:\n  source: { type: file, config: { path: /proc/self/environ, format: raw_text } }\n  sink: { type: stdout, config: {} }\n";
    let err = load_submission(direct, ConfigFormat::Yaml, None, None, caller)
        .await
        .unwrap_err();
    assert!(matches!(err, ServeError::Forbidden(_)), "{err:?}");
    assert!(message(&err).contains("/proc/self/environ"));

    let via_row = "version: 1\npipeline:\n  sources:\n    x: { type: csv, config: { path: in.csv } }\n  sink: { type: stdout, config: {} }\nmatrix:\n  - id: r\n    source: { ref: x, config: { path: /proc/self/environ } }\n";
    let err = load_submission(via_row, ConfigFormat::Yaml, None, None, caller)
        .await
        .unwrap_err();
    assert!(matches!(err, ServeError::Forbidden(_)), "{err:?}");
}

#[tokio::test]
async fn a_secret_reference_arriving_through_an_env_value_is_refused() {
    protect_secret_ref("vault", "secret/load-test-env");
    // SAFETY: test-only variable, read by this test alone.
    unsafe { std::env::set_var("LOAD_TEST_INDIRECT", "${vault:secret/load-test-env}") };
    let err = load_submission(
        &csv_body("${env:LOAD_TEST_INDIRECT}"),
        ConfigFormat::Yaml,
        None,
        None,
        BodyOrigin::Trusted,
    )
    .await
    .unwrap_err();
    assert!(matches!(err, ServeError::Forbidden(_)), "{err:?}");
}

#[test]
fn resolution_failures_that_may_pass_on_retry_are_unavailable() {
    use crate::error::CliError;
    let io = CliError::ReadInterpolatedFile {
        path: "/x".into(),
        source: std::io::Error::new(std::io::ErrorKind::NotFound, "gone"),
    };
    assert!(matches!(resolution_error(io), ServeError::Unavailable(_)));
    let fetch = CliError::SecretFetchFailed {
        scheme: "vault".into(),
        reference: "kv/x".into(),
        source: "503".into(),
    };
    assert!(matches!(
        resolution_error(fetch),
        ServeError::Unavailable(_)
    ));
    let auth = CliError::SecretAuthFailed {
        scheme: "vault".into(),
        hint: "token expired".into(),
    };
    assert!(matches!(resolution_error(auth), ServeError::Unavailable(_)));
    let missing = CliError::MissingEnvVar {
        var: "X".into(),
        location: "a".into(),
    };
    assert!(matches!(
        resolution_error(missing),
        ServeError::BadConfig(_)
    ));
    let absent = CliError::SecretNotFound {
        scheme: "vault".into(),
        reference: "kv/x".into(),
    };
    assert!(matches!(resolution_error(absent), ServeError::BadConfig(_)));
}

#[cfg(any(feature = "source-singer", feature = "sink-singer"))]
#[test]
fn subprocess_kinds_are_found_anywhere() {
    let doc = json!({
        "pipeline": {"sources": {"a": {"type": "singer"}}, "sink": {"type": "jsonl"}},
        "matrix": [{"sink": {"type": "singer"}}],
        "transforms": [{"type": "flatten"}]
    });
    assert_eq!(
        subprocess_kinds_in(&doc).into_iter().collect::<Vec<_>>(),
        vec!["singer".to_string()]
    );
    let caller = BodyOrigin::Caller {
        allow_subprocess: false,
    };
    let allowed = BodyOrigin::Caller {
        allow_subprocess: true,
    };
    assert!(refuse_subprocess(&doc, BodyOrigin::Trusted, true).is_ok());
    assert!(refuse_subprocess(&doc, allowed, false).is_ok());
    assert!(refuse_subprocess(&json!({"type": "csv"}), caller, false).is_ok());
    let err = refuse_subprocess(&doc, caller, false).unwrap_err();
    assert!(message(&err).contains("--allow-subprocess-connectors"));
    let err = refuse_subprocess(&doc, allowed, true).unwrap_err();
    assert!(message(&err).contains("tenant"));
}

#[tokio::test]
async fn loading_refuses_server_secrets_and_unallowed_subprocesses() {
    let err = load_submission(
        &csv_body("${env:FAUCET_SERVE_AUTH_TOKEN}"),
        ConfigFormat::Yaml,
        None,
        None,
        BodyOrigin::Trusted,
    )
    .await
    .unwrap_err();
    assert!(matches!(err, ServeError::Forbidden(_)), "{err:?}");
    let err = load_submission(
        SINGER,
        ConfigFormat::Yaml,
        None,
        None,
        BodyOrigin::Caller {
            allow_subprocess: false,
        },
    )
    .await
    .unwrap_err();
    assert!(message(&err).contains("--allow-subprocess-connectors"));
    for origin in [
        BodyOrigin::Trusted,
        BodyOrigin::Caller {
            allow_subprocess: true,
        },
    ] {
        if let Err(e) = load_submission(SINGER, ConfigFormat::Yaml, None, None, origin).await {
            assert!(
                !message(&e).contains("--allow-subprocess-connectors"),
                "{e:?}"
            );
        }
    }
}

/// A `${scheme:…}` that is no credential reference is not screened, and a
/// load-time directive in a local path takes no value from records.
#[test]
fn unknown_schemes_pass_and_load_time_paths_are_static() {
    let deny = ReferencePolicy::deny_all();
    screen_references(&serde_json::json!({"a": "${nope:x}"}), &deny).unwrap();
    assert!(!takes_record_value("/data/${env:HOME}/x.jsonl"));
    assert!(!takes_record_value("/data/${now.date}/x.jsonl"));
    assert!(takes_record_value("/data/${parent.id}/x.jsonl"));
}

#[test]
fn references_go_through_the_policy_after_the_credential_overlay() {
    let deny = ReferencePolicy::deny_all();
    let doc = serde_json::json!({"a": "${env:SOME_VAR}", "b": "$${env:ESCAPED}"});
    let err = screen_references(&doc, &deny).unwrap_err().to_string();
    assert!(
        err.contains("${env:SOME_VAR}") && err.contains("--allow-env"),
        "{err}"
    );
    assert!(!err.contains("ESCAPED"), "{err}");
    screen_references(&doc, &ReferencePolicy::open()).unwrap();
    let skip: std::collections::BTreeSet<String> =
        ["${env:SOME_VAR}".to_string()].into_iter().collect();
    screen_references_except(&doc, &deny, &skip).unwrap();
    let err = screen_references(
        &serde_json::json!(["${file:/etc/hosts}", "${vault:a/b}"]),
        &deny,
    )
    .unwrap_err()
    .to_string();
    assert!(
        err.contains("${file:/etc/hosts}") && err.contains("${vault:a/b}"),
        "{err}"
    );
    let overlay = screen_references(
        &serde_json::json!("${env:FAUCET_VAULT_KEY}"),
        &ReferencePolicy::open(),
    )
    .unwrap_err()
    .to_string();
    assert!(overlay.contains("server's own credentials"), "{overlay}");
    refuse_server_secrets(&serde_json::json!("${env:ANY}")).unwrap();
    assert_eq!(
        load_time_tokens(&serde_json::json!({"x": "${env:A} ${row.b}"})),
        ["${env:A}".to_string()].into_iter().collect()
    );
}

#[test]
fn auth_catalog_endpoints_follow_the_url_policy() {
    let mut cfg = PipelineConfig::from_value(serde_json::json!({
            "version": 1,
            "auth": {
                "idp": {"type": "oauth2", "config": {"token_url": "https://idp.example.com/t"}},
                "meta": {"type": "token_endpoint", "config": {"url": "http://169.254.169.254/x"}},
                "plain": {"type": "static"}
            },
            "pipeline": {"source": {"type": "rest", "config": {}}, "sink": {"type": "stdout", "config": {}}}
        }))
        .unwrap();
    let open = ReferencePolicy::open();
    let err = screen_auth_catalog(&cfg, &open).unwrap_err().to_string();
    assert!(
        err.contains("auth.meta.config.url") && !err.contains("idp"),
        "{err}"
    );
    cfg.auth.as_mut().unwrap().remove("meta");
    screen_auth_catalog(&cfg, &open).unwrap();
    let hosts = ReferencePolicy::open().with_url_hosts(["other.example.com"]);
    assert!(screen_auth_catalog(&cfg, &hosts).is_err());
}

#[cfg(feature = "notify")]
#[test]
fn notification_channel_urls_follow_the_url_policy() {
    let cfg = PipelineConfig::from_value(serde_json::json!({
            "version": 1,
            "notifications": [
                {"name": "ok", "channel": {"type": "webhook", "config": {"url": "https://hooks.example.com/x"}}},
                {"name": "meta", "channel": {"type": "slack", "config": {"webhook_url": "http://169.254.169.254/x"}}},
                {"name": "pd", "channel": {"type": "pagerduty", "config": {"routing_key": "k"}}}
            ],
            "pipeline": {"source": {"type": "rest", "config": {}}, "sink": {"type": "stdout", "config": {}}}
        }))
        .unwrap();
    let hosts = ReferencePolicy::open().with_url_hosts(["hooks.example.com"]);
    let err = screen_notifications(&cfg, &hosts).unwrap_err().to_string();
    assert!(
        err.contains("notifications `meta` (slack)") && !err.contains("`ok`"),
        "{err}"
    );
    assert!(
        !err.contains("`pd`"),
        "the default endpoint is not a caller URL: {err}"
    );
}

#[test]
fn every_provider_endpoint_and_file_is_screened() {
    let caller = ReferencePolicy::deny_all()
        .any_url_scheme()
        .any_url_host()
        .with_path_roots([std::env::temp_dir()]);
    let one = |name: &str, spec: Value, policy: &ReferencePolicy, by_caller: bool| {
        screen_auth_providers([(format!("auth.{name}"), &spec)], policy, by_caller)
    };
    let flow = serde_json::json!({"type": "flow", "config": {"steps": [
            {"request": {"url": "https://login.example/x"}},
            {"request": {"url": "http://169.254.169.254/latest/meta-data/"}}]}});
    let err = one("f", flow, &caller, true).unwrap_err().to_string();
    assert!(err.contains("auth.f.config.steps.1.request.url"), "{err}");
    let sa = serde_json::json!({"type": "google_service_account", "config": {
            "key_json": {"token_uri": "http://metadata.google.internal/t"}}});
    let err = one("g", sa, &caller, true).unwrap_err().to_string();
    assert!(err.contains("key_json.token_uri"), "{err}");
    let tmp = std::env::temp_dir().join("k.json").display().to_string();
    let key_file = serde_json::json!({"type": "google_service_account", "config": {
            "key_file": tmp, "scopes": ["s"]}});
    let err = one("g", key_file.clone(), &caller, true)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("auth.g.config.key_file") && err.contains("key_json"),
        "{err}"
    );
    one("g", key_file, &ReferencePolicy::open(), false).expect("an operator's key file");
    let persist = serde_json::json!({"type": "oauth2_refresh", "config": {
            "token_url": "https://idp.example/t", "persist": {"path": tmp, "key": "k"}}});
    let err = one("p", persist, &caller, true).unwrap_err().to_string();
    assert!(err.contains("auth.p.config.persist.path"), "{err}");
    let sys = serde_json::json!({"type": "oauth2_refresh", "config": {
            "token_url": "https://idp.example/t", "persist": {"path": "/proc/self", "key": "k"}}});
    let err = one("p", sys, &ReferencePolicy::open(), false)
        .unwrap_err()
        .to_string();
    assert!(err.contains("system tree"), "{err}");
}

/// A caller's body may not name a file through an auth provider, while a
/// provider only the operator's `--default-config` declares may.
#[tokio::test]
async fn a_callers_auth_providers_are_screened_at_load() {
    let caller = BodyOrigin::Caller {
        allow_subprocess: false,
    };
    let refs = crate::serve::references::ServeReferencePolicy::default();
    let pipeline = serde_json::json!({
        "source": { "type": "csv", "config": { "path": "in.csv" } },
        "sink": { "type": "jsonl", "config": { "path": "out.jsonl" } }
    });
    let key = std::env::temp_dir().join("faucet-no-such-key.json");
    let provider = serde_json::json!({ "type": "google_service_account", "config": {
            "key_file": key.display().to_string(), "scopes": ["s"] } });
    let body = serde_json::json!({ "version": 1, "auth": { "g": provider },
            "pipeline": pipeline })
    .to_string();
    let err = load_submission_with(&body, ConfigFormat::Json, None, None, caller, None, &refs)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, ServeError::Forbidden(m) if m.contains("auth.g.config.key_file")),
        "{err:?}"
    );
    // The same provider in the operator's base is not the caller's: it gets
    // past the screen (and fails only because the key does not exist).
    let base = serde_json::json!({ "version": 1, "auth": { "g": provider } });
    let body = serde_json::json!({ "pipeline": pipeline }).to_string();
    let err = load_submission_with(
        &body,
        ConfigFormat::Json,
        Some(&base),
        None,
        caller,
        None,
        &refs,
    )
    .await
    .unwrap_err();
    assert!(
        !matches!(&err, ServeError::Forbidden(_)),
        "an operator's provider was screened as the caller's: {err:?}"
    );
}

#[test]
fn subprocess_screening_follows_origin_and_tenant() {
    let doc = serde_json::json!({"pipeline": {"source": {"type": "singer", "config": {}}}});
    refuse_subprocess(&doc, BodyOrigin::Trusted, true).unwrap();
    let allowed = BodyOrigin::Caller {
        allow_subprocess: true,
    };
    refuse_subprocess(&doc, allowed, false).unwrap();
    let err = refuse_subprocess(&doc, allowed, true)
        .unwrap_err()
        .to_string();
    assert!(err.contains("tenant"), "{err}");
    let err = screen_document(
        &doc,
        BodyOrigin::Caller {
            allow_subprocess: false,
        },
    )
    .unwrap_err()
    .to_string();
    assert!(err.contains("--allow-subprocess-connectors"), "{err}");
    screen_document(&serde_json::json!({}), allowed).unwrap();
}

#[test]
fn policy_refusals_name_the_flags() {
    let cfg = PipelineConfig::from_value(serde_json::json!({
        "version": 1,
        "pipeline": {
            "source": {"type": "csv", "config": {"path": "/srv/elsewhere/in.csv"}},
            "sink": {"type": "jsonl", "config": {"path": "/srv/elsewhere/out.jsonl"}},
            "state": {"type": "file", "config": {"path": "/srv/elsewhere/state"}}
        }
    }))
    .unwrap();
    let caller = BodyOrigin::Caller {
        allow_subprocess: false,
    };
    let policy = ReferencePolicy::deny_all().with_path_roots(["/srv/data"]);
    let err = refuse_server_paths_with(&cfg, &[], caller, &policy)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("/srv/elsewhere/state") && err.contains("--allow-path-root"),
        "{err}"
    );
    refuse_server_paths_with(&cfg, &[], BodyOrigin::Trusted, &ReferencePolicy::open()).unwrap();
    let rows = PipelineConfig::from_value(serde_json::json!({
        "version": 1,
        "pipeline": {
            "source": {"type": "csv", "config": {"path": "/srv/data/${row.id}.csv"}},
            "sink": {"type": "jsonl", "config": {"path": "/srv/data/out.jsonl"}}
        }
    }))
    .unwrap();
    let err = refuse_server_paths_with(&rows, &[], caller, &policy)
        .unwrap_err()
        .to_string();
    assert!(err.contains("from records"), "{err}");
}

/// A URL host a submitted config takes from records cannot be checked
/// against any list, so it is refused for a caller even when the host
/// policy is open; a host fixed in the config, or a token in the path,
/// still passes.
#[cfg(all(feature = "source-rest", feature = "sink-stdout"))]
#[test]
fn a_caller_url_host_may_not_come_from_records() {
    let cfg = |base: &str| {
        PipelineConfig::from_value(serde_json::json!({
            "version": 1,
            "pipeline": {
                "source": {"type": "rest", "config": {"base_url": base}},
                "sink": {"type": "stdout", "config": {}}
            }
        }))
        .unwrap()
    };
    let caller = BodyOrigin::Caller {
        allow_subprocess: false,
    };
    let open = ReferencePolicy::open();
    for bad in ["https://${parent.host}/v1", "${parent.next_url}"] {
        let err = refuse_server_paths_with(&cfg(bad), &[], caller, &open)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("rest/base_url") && err.contains("may not take a value from records"),
            "{bad}: {err}"
        );
        refuse_server_paths_with(&cfg(bad), &[], BodyOrigin::Trusted, &open).unwrap();
    }
    for ok in [
        "https://api.example.com/items/${parent.id}",
        "https://api.example.com/${now.date}",
    ] {
        refuse_server_paths_with(&cfg(ok), &[], caller, &open).unwrap();
    }
}

fn caller_fields(entries: &[(&str, &str)]) -> crate::params::CallerFields {
    let mut f = crate::params::CallerFields::default();
    for (p, n) in entries {
        f.insert(*p, *n);
    }
    f
}

fn rooted(root: &std::path::Path) -> ReferencePolicy {
    ReferencePolicy::deny_all()
        .with_path_roots([root.to_path_buf()])
        .any_url_scheme()
        .any_url_host()
}

/// #989: a caller-supplied param value in a DLQ / sink path outside the
/// caller path roots is refused naming the param and the field; the same
/// path written by the template, or a value inside a root, passes.
#[cfg(all(feature = "source-rest", feature = "sink-file"))]
#[test]
fn a_caller_param_in_a_path_meets_the_caller_policy() {
    let root = tempfile::tempdir().unwrap();
    let inside = root.path().join("dlq.jsonl");
    let doc = |dlq: &str| {
        json!({
            "version": 1,
            "pipeline": {
                "source": { "type": "rest", "config": { "base_url": "https://api.example.com" } },
                "sink": { "type": "file", "config": { "path": "/etc/faucet-out.jsonl" } },
                "dlq": { "sink": { "type": "file", "config": { "path": dlq } } }
            }
        })
    };
    let check = |d: &Value, f: &crate::params::CallerFields, tenant: bool| {
        let cfg = PipelineConfig::from_value(d.clone()).unwrap();
        refuse_caller_params(d, &cfg, f, &rooted(root.path()), tenant)
    };
    let f = caller_fields(&[("/pipeline/dlq/sink/config/path", "x")]);
    let err = check(&doc("/etc/faucet-x"), &f, false)
        .unwrap_err()
        .to_string();
    assert!(err.contains(CALLER_PARAM_REFUSAL), "{err}");
    assert!(
        err.contains("param 'x' sets `pipeline.dlq.sink.config.path` to /etc/faucet-x"),
        "{err}"
    );
    assert!(err.contains("trusted: true"), "{err}");
    assert!(!err.contains("tenant"), "{err}");
    let err = check(&doc("/etc/faucet-x"), &f, true)
        .unwrap_err()
        .to_string();
    assert!(err.contains("--tenant-path-root"), "{err}");
    // The template's own /etc sink path is not the caller's.
    check(&doc(&inside.display().to_string()), &f, false).unwrap();
    check(&doc("/etc/faucet-x"), &Default::default(), false).unwrap();
    // A `..` cannot climb out of a root.
    let climb = format!("{}/../../etc/x", root.path().display());
    assert!(check(&doc(&climb), &f, false).is_err());
}

/// The caller's value reaches a field through `${vars.*}` and through a
/// matrix row's override of its template; a row that replaces the
/// template's caller value with its own drops it.
#[cfg(all(feature = "source-rest", feature = "sink-file"))]
#[test]
fn caller_values_follow_vars_and_row_overrides() {
    let root = tempfile::tempdir().unwrap();
    let policy = rooted(root.path());
    let inside = root.path().join("a.jsonl").display().to_string();
    let doc = json!({
        "version": 1,
        "vars": { "dir": "/etc" },
        "pipeline": {
            "source": { "type": "rest", "config": { "base_url": "https://api.example.com" } },
            "sinks": { "out": { "type": "file", "config": { "path": inside } } },
            "state": { "type": "file", "config": { "path": "${vars.dir}/state" } }
        },
        "matrix": [
            { "id": "a", "sink": { "ref": "out", "config": { "path": "/etc/row.jsonl" } } },
            { "id": "b", "sink": { "ref": "out" } }
        ]
    });
    let cfg = PipelineConfig::from_value(doc.clone()).unwrap();
    let err = refuse_caller_params(
        &doc,
        &cfg,
        &caller_fields(&[("/vars/dir", "d")]),
        &policy,
        false,
    )
    .unwrap_err()
    .to_string();
    assert!(
        err.contains("param 'd' sets `pipeline.state.config.path`"),
        "{err}"
    );
    let err = refuse_caller_params(
        &doc,
        &cfg,
        &caller_fields(&[("/matrix/0/sink/config/path", "p")]),
        &policy,
        false,
    )
    .unwrap_err()
    .to_string();
    assert!(
        err.contains("`matrix.0.sink.config.path` to /etc/row.jsonl"),
        "{err}"
    );
    // The template's value is the caller's; row `a` replaces it.
    let t = json!({ "path": "x", "nested": { "a": "1" } });
    let merged = merged_caller_fields(
        &t,
        &caller_fields(&[("/path", "p"), ("/nested/a", "q")]),
        &json!({ "path": "/own", "nested": { "b": "2" } }),
        &Default::default(),
    );
    assert!(!merged.covers("/path"));
    assert!(merged.covers("/nested/a"));
    let merged = merged_caller_fields(
        &t,
        &caller_fields(&[("/nested", "q")]),
        &json!({ "nested": "flat" }),
        &caller_fields(&[("/x", "r")]),
    );
    assert!(!merged.covers("/nested") && merged.covers("/x"));
    refuse_caller_params(
        &doc,
        &cfg,
        &caller_fields(&[("/pipeline/sinks/out/config/path", "p")]),
        &policy,
        false,
    )
    .unwrap();
}

/// A caller's URL meets the host rules (link-local / metadata refused),
/// wherever it lands; a caller-chosen auth provider file is refused; an
/// alias key is followed.
#[cfg(all(feature = "source-rest", feature = "sink-file"))]
#[test]
fn caller_urls_and_auth_files_meet_the_caller_policy() {
    let root = tempfile::tempdir().unwrap();
    let policy = rooted(root.path());
    let out = root.path().join("o.jsonl").display().to_string();
    let doc = |url: &str| {
        json!({
            "version": 1,
            "auth": { "svc": { "type": "oauth2_refresh", "config": {
                "token_url": "https://idp.example.com/token",
                "client_id": "c", "client_secret": "s", "refresh_token": "r",
                "persist": { "path": "/etc/tok.json" } } } },
            "pipeline": {
                "source": { "type": "rest", "config": { "base_url": url } },
                "sink": { "type": "file", "config": { "path": out } }
            }
        })
    };
    let check = |d: &Value, f: &crate::params::CallerFields| {
        let cfg = PipelineConfig::from_value(d.clone()).unwrap();
        refuse_caller_params(d, &cfg, f, &policy, false)
    };
    let f = caller_fields(&[("/pipeline/source/config/base_url", "api")]);
    let err = check(&doc("http://169.254.169.254/latest"), &f)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("param 'api' sets `pipeline.source.config.base_url`"),
        "{err}"
    );
    check(&doc("https://api.example.com/v1"), &f).unwrap();
    let f = caller_fields(&[("/auth/svc/config/persist/path", "tok")]);
    let err = check(&doc("https://api.example.com"), &f)
        .unwrap_err()
        .to_string();
    assert!(err.contains("`auth.svc.config.persist.path`"), "{err}");
    let aliased = propagate_caller_fields(
        &json!({}),
        &caller_fields(&[
            ("/replication/snapshot/source/config/x", "a"),
            ("/matrix/0/discover/source", "b"),
        ]),
    );
    assert!(aliased.covers("/mirror/snapshot/source/config/x"));
    assert!(aliased.covers("/matrix/0/fan_out/source/config"));
}

/// #989: a refusal shows only what the caller sent — never the
/// operator's `${env:}` / `${file:}` text resolved around it, in the
/// value or in the policy's reason — and is redacted.
#[cfg(all(feature = "source-rest", feature = "sink-file"))]
#[test]
fn a_refusal_shows_only_the_callers_own_text() {
    let root = tempfile::tempdir().unwrap();
    let policy = ReferencePolicy::deny_all()
        .with_path_roots([root.path().to_path_buf()])
        .any_url_scheme()
        .with_url_hosts(["api.example.com".to_string()]);
    let out = root.path().join("o.jsonl").display().to_string();
    let doc = json!({
        "version": 1,
        "pipeline": {
            "source": { "type": "rest", "config": {
                "base_url": "https://evil.example.net/v1?key=svc-key-from-operator-env" } },
            "sink": { "type": "file", "config": { "path": out } }
        }
    });
    let cfg = PipelineConfig::from_value(doc.clone()).unwrap();
    let mut f = crate::params::CallerFields::default();
    f.insert_as(
        "/pipeline/source/config/base_url",
        "host",
        crate::params::caller::Wrote::Part,
    );
    f.add_text("host", "evil.example.net");
    let err = refuse_caller_params(&doc, &cfg, &f, &policy, false)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("param 'host' sets `pipeline.source.config.base_url`"),
        "{err}"
    );
    assert!(err.contains("to …evil.example.net…"), "{err}");
    assert!(!err.contains("svc-key-from-operator-env"), "{err}");
    // Without the caller's texts (a stored record), no value is shown.
    let mut bare = crate::params::CallerFields::default();
    bare.insert_as(
        "/pipeline/source/config/base_url",
        "host",
        crate::params::caller::Wrote::Part,
    );
    let err = refuse_caller_params(&doc, &cfg, &bare, &policy, false)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("sets `pipeline.source.config.base_url` ("),
        "{err}"
    );
    assert!(!err.contains("evil.example.net"), "{err}");
    assert!(!err.contains("svc-key-from-operator-env"), "{err}");
}
