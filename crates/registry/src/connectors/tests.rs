#[cfg(all(feature = "sink-bigquery", feature = "arrow"))]
#[test]
fn validate_refuses_a_truncating_bigquery_bulk_load() {
    let config = |disposition: &str| {
        serde_json::json!({
            "project_id": "p",
            "dataset_id": "d",
            "table_id": "t",
            "auth": {"type": "application_default"},
            "bulk_load": {"staging_bucket": "b", "write_disposition": disposition}
        })
    };
    let err = super::validate_sink_config("bigquery", "out", config("WRITE_TRUNCATE"))
        .unwrap_err()
        .to_string();
    assert!(err.contains("write_disposition"), "{err}");
    super::validate_sink_config("bigquery", "out", config("WRITE_APPEND")).unwrap();
}
use super::*;

use serde_json::json;

#[test]
fn queue_sources_consume_destructively_by_kind() {
    let empty = serde_json::json!({});
    for kind in ["sqs", "pubsub", "rabbitmq"] {
        assert!(source_kind_consumes_destructively(kind, &empty), "{kind}");
    }
    assert!(!source_kind_consumes_destructively("nats", &empty));
    assert!(source_kind_consumes_destructively(
        "nats",
        &serde_json::json!({"jetstream_stream": "S"})
    ));
    assert!(!source_kind_consumes_destructively("kafka", &empty));
}

#[test]
fn bookmark_schema_and_migration_per_source_kind() {
    assert_eq!(source_state_schema("csv"), 0);
    assert_eq!(
        migrate_source_state("csv", 0, json!({"a": 1})).unwrap(),
        json!({"a": 1})
    );
    assert!(migrate_source_state("csv", 1, json!({})).is_err());
    #[cfg(feature = "source-mongodb-cdc")]
    {
        assert_eq!(source_state_schema("mongodb-cdc"), 1);
        let m = migrate_source_state("mongodb-cdc", 0, json!({"resume_token": {"_data": "A"}}))
            .unwrap();
        assert_eq!(m["invalidate"], json!(false));
        assert_eq!(state_codec_for(Some(("mongodb-cdc", &json!({})))).schema, 1);
    }
    let topo = state_codec_for(None);
    assert_eq!(
        (topo.owner.as_str(), topo.schema, topo.legacy),
        ("topology", 0, false)
    );
    assert_eq!(state_codec_for(Some(("csv", &json!({})))).owner, "csv");
}

/// Every compiled source kind reaches a real deserializer (#609).
///
/// Driven off `source_descriptions()` rather than a hand-written list, so a
/// connector added without a validation arm fails here instead of silently
/// accepting any config until its first triggered run — the exact defect
/// #609 was filed for.
///
/// A non-object config is the one input that no connector config can
/// deserialize, whatever its fields, so it proves the arm is wired without
/// needing a valid fixture per kind. An arm that returned `Ok` (a stub, or
/// a copy-paste that dropped the `check`) fails.
#[test]
fn every_compiled_source_kind_validates_its_own_config_type() {
    for (kind, _) in source_descriptions() {
        let err = validate_source_config(kind, "row-a", serde_json::json!(42))
            .expect_err("a non-object config cannot deserialize into any connector config");
        let msg = err.to_string();
        assert!(
            msg.contains(kind),
            "the failure must name the connector kind so the operator knows which \
                 entry is wrong: {msg}"
        );
        assert!(
            msg.contains("row-a"),
            "the failure must name the row/template entry: {msg}"
        );
    }
}

/// The sink half of the same guarantee.
#[test]
fn every_compiled_sink_kind_validates_its_own_config_type() {
    for (kind, _) in sink_descriptions() {
        let err = validate_sink_config(kind, "row-b", serde_json::json!("not an object"))
            .expect_err("a non-object config cannot deserialize into any connector config");
        let msg = err.to_string();
        assert!(msg.contains(kind), "must name the kind: {msg}");
        assert!(msg.contains("row-b"), "must name the entry: {msg}");
    }
}

/// An empty object is the *other* half: it must be accepted by configs whose
/// every field is optional and rejected — by name — where something is
/// genuinely required. Either outcome is fine; a panic or a non-actionable
/// message is not.
#[test]
fn an_empty_config_object_is_never_a_panic_and_always_actionable() {
    for (kind, _) in source_descriptions() {
        if let Err(e) = validate_source_config(kind, "row-c", serde_json::json!({})) {
            let msg = e.to_string();
            assert!(msg.contains(kind) && msg.contains("row-c"), "{msg}");
        }
    }
    for (kind, _) in sink_descriptions() {
        if let Err(e) = validate_sink_config(kind, "row-d", serde_json::json!({})) {
            let msg = e.to_string();
            assert!(msg.contains(kind) && msg.contains("row-d"), "{msg}");
        }
    }
}

/// An unknown kind names the kind and lists what *is* available, rather
/// than reporting the config valid.
#[test]
fn an_unknown_kind_is_refused_with_the_available_kinds() {
    let src = validate_source_config("not-a-connector", "row", serde_json::json!({}))
        .expect_err("an unknown source kind must not validate");
    assert!(src.to_string().contains("not-a-connector"), "{src}");
    let sink = validate_sink_config("not-a-connector", "row", serde_json::json!({}))
        .expect_err("an unknown sink kind must not validate");
    assert!(sink.to_string().contains("not-a-connector"), "{sink}");
}

// A trivial in-memory source used to exercise custom registration without
// any I/O.
#[derive(Clone)]
struct DummySource;
#[faucet_core::async_trait]
impl Source for DummySource {
    async fn fetch_with_context(
        &self,
        _ctx: &std::collections::HashMap<String, Value>,
    ) -> Result<Vec<Value>, faucet_core::FaucetError> {
        Ok(vec![serde_json::json!({"ok": true})])
    }
    fn config_schema(&self) -> Value {
        serde_json::json!({"type": "object"})
    }
}

#[cfg(feature = "source-csv")]
#[test]
fn register_source_rejects_builtin_collision() {
    // `csv` is a built-in whenever that feature is on; use a name we know is
    // built-in under --all-features to assert the collision guard fires.
    let reg = PluginRegistry::with_builtins()
        .register_source("csv", |_| Ok(Box::new(DummySource) as Box<dyn Source>));
    // install() surfaces the stashed error WITHOUT touching the global
    // (errors are checked before the OnceLock is set), so this is race-free.
    let err = reg
        .install()
        .expect_err("built-in collision must be rejected");
    match err {
        RegistryError::Config(msg) => assert!(msg.contains("built-in source"), "{msg}"),
        other => panic!("expected Config error, got {other:?}"),
    }
}

#[test]
fn register_source_rejects_duplicate() {
    let reg = PluginRegistry::new()
        .register_source("dup", |_| Ok(Box::new(DummySource) as Box<dyn Source>))
        .register_source("dup", |_| Ok(Box::new(DummySource) as Box<dyn Source>));
    let err = reg
        .install()
        .expect_err("duplicate registration must be rejected");
    match err {
        RegistryError::Config(msg) => assert!(msg.contains("more than once"), "{msg}"),
        other => panic!("expected Config error, got {other:?}"),
    }
}

#[test]
fn register_sink_rejects_duplicate() {
    // Build a registry with a duplicate sink and confirm the error is
    // stashed; we inspect it via the private field rather than install()
    // so no global state is touched even indirectly.
    let reg = PluginRegistry::new()
        .register_sink("dupsink", |_| Err(RegistryError::Config("unused".into())))
        .register_sink("dupsink", |_| Err(RegistryError::Config("unused".into())));
    assert!(
        reg.errors.iter().any(|e| e.contains("more than once")),
        "{:?}",
        reg.errors
    );
}

/// RFC 0014: a plugin registered without a descriptor is listed (and
/// warned about); a described one is not.
#[test]
fn plugins_without_a_descriptor_are_listed() {
    fn caps(_: &Value) -> faucet_core::Capabilities {
        faucet_core::Capabilities::append_only_sink()
    }
    let reg = PluginRegistry::new()
        .register_source(
            "plain_src",
            |_| Ok(Box::new(DummySource) as Box<dyn Source>),
        )
        .register_sink("plain_sink", |_| {
            Err(RegistryError::Config("unused".into()))
        })
        .register_sink_described(
            ConnectorDescriptor::new("described_sink", caps),
            |_| Err(RegistryError::Config("unused".into())),
            || serde_json::json!({"type": "object"}),
            "",
        );
    assert_eq!(
        undescribed_in(&reg),
        vec![
            "sink:plain_sink".to_string(),
            "source:plain_src".to_string()
        ]
    );
    assert!(
        undescribed_plugins().is_empty(),
        "the default binary has none"
    );
    warn_undescribed_plugins();
}

#[test]
fn custom_descriptions_use_default_when_blank() {
    let reg = PluginRegistry::new()
        .register_source("acme", |_| Ok(Box::new(DummySource) as Box<dyn Source>));
    let descs = reg.custom_source_descriptions();
    assert_eq!(descs.len(), 1);
    assert_eq!(descs[0].0, "acme");
    assert_eq!(descs[0].1, "custom source connector");
}

#[test]
fn custom_descriptions_carry_explicit_summary() {
    let reg = PluginRegistry::new().register_source_with(
        "acme",
        |_| Ok(Box::new(DummySource) as Box<dyn Source>),
        || serde_json::json!({"type": "object", "title": "acme"}),
        "Acme widget source",
    );
    let descs = reg.custom_source_descriptions();
    assert_eq!(descs[0], ("acme", "Acme widget source"));
    // The schema closure is what `faucet schema source acme` would print.
    assert_eq!(
        (reg.sources.get("acme").unwrap().schema)()["title"],
        serde_json::json!("acme")
    );
}

#[cfg(feature = "source-rest")]
#[test]
fn rest_source_appears_in_listings() {
    assert!(source_kinds().contains(&"rest"));
}

#[cfg(feature = "sink-jsonl")]
#[test]
fn jsonl_sink_appears_in_listings() {
    assert!(sink_kinds().contains(&"jsonl"));
}

#[tokio::test]
async fn unknown_source_kind_errors() {
    let err = build_source("nope", serde_json::json!({}), &AuthCatalog::new(), None)
        .await
        .err()
        .expect("should fail");
    match err {
        RegistryError::UnknownConnector { kind, name, .. } => {
            assert_eq!(kind, "source");
            assert_eq!(name, "nope");
        }
        other => panic!("expected UnknownConnector, got {other:?}"),
    }
}

#[tokio::test]
async fn unknown_sink_kind_errors() {
    let err = build_sink("nope", serde_json::json!({}), &AuthCatalog::new())
        .await
        .err()
        .expect("should fail");
    assert!(matches!(
        err,
        RegistryError::UnknownConnector { kind: "sink", .. }
    ));
}

#[cfg(feature = "source-rest")]
#[test]
fn rest_schema_is_object() {
    let s = source_schema("rest").unwrap();
    assert!(s.is_object());
}

#[cfg(feature = "sink-jsonl")]
#[test]
fn jsonl_schema_is_object() {
    let s = sink_schema("jsonl").unwrap();
    assert!(s.is_object());
}

#[test]
fn scrub_config_error_redacts_quoted_values() {
    // A serde "invalid type" error echoes the offending value in double
    // quotes — must be redacted so a secret can't reach the log (#78/#38).
    let msg =
        r#"invalid type: string "sk-super-secret-123", expected a sequence at line 1 column 9"#;
    let scrubbed = scrub_config_error(msg);
    assert!(!scrubbed.contains("sk-super-secret-123"), "{scrubbed}");
    assert!(scrubbed.contains("<redacted>"), "{scrubbed}");
    // Structural context outside the quotes is preserved.
    assert!(scrubbed.contains("invalid type"), "{scrubbed}");
    assert!(scrubbed.contains("expected a sequence"), "{scrubbed}");
}

#[test]
fn scrub_config_error_handles_escapes_backticks_and_keeps_keys() {
    let msg = r#"invalid type: string "pa\"ss-tail-secret", expected a sequence"#;
    let scrubbed = scrub_config_error(msg);
    assert!(!scrubbed.contains("tail-secret"), "{scrubbed}");
    let msg = "invalid type: integer `48213907`, expected a string at line 1 column 3";
    let scrubbed = scrub_config_error(msg);
    assert!(!scrubbed.contains("48213907"), "{scrubbed}");
    assert!(
        scrubbed.contains("expected a string at line 1"),
        "{scrubbed}"
    );
    let msg = "unknown variant `hunter2`, expected one of `bearer`, `basic`";
    let scrubbed = scrub_config_error(msg);
    assert!(!scrubbed.contains("hunter2"), "{scrubbed}");
    assert!(scrubbed.contains("`bearer`"), "{scrubbed}");
    let msg = "unknown field `tokn`, expected `token`";
    assert_eq!(scrub_config_error(msg), msg);
    assert_eq!(
        scrub_config_error("missing field `url`"),
        "missing field `url`"
    );
    assert!(scrub_config_error("unterminated `abc").contains("<redacted>"));
}

#[test]
fn scrub_config_error_truncates_long_messages() {
    let msg = "x".repeat(500);
    let scrubbed = scrub_config_error(&msg);
    assert!(
        scrubbed.chars().count() <= 201,
        "len {}",
        scrubbed.chars().count()
    );
    assert!(scrubbed.ends_with('…'));
}

// A `(kind, config)` pair that builds without performing any network/disk
// I/O — the CSV source's `new()` only stores config, so we can drive the
// real `build_source` dispatch arm and inspect the resulting trait object.
#[cfg(feature = "source-csv")]
#[tokio::test]
async fn build_source_csv_succeeds_without_io() {
    let src = build_source(
        "csv",
        serde_json::json!({ "path": "/tmp/does-not-need-to-exist.csv" }),
        &AuthCatalog::new(),
        None,
    )
    .await
    .expect("csv source should build without I/O");
    // The CSV source overrides `connector_name()` with its friendly,
    // YAML-`type`-matching label (#61).
    assert_eq!(src.connector_name(), "csv");
}

// The JSONL sink's `new()` is also pure (it opens the file lazily on first
// write), so building it exercises the sink dispatch arm with no I/O.
#[cfg(feature = "sink-jsonl")]
#[tokio::test]
async fn build_sink_jsonl_succeeds_without_io() {
    let sink = build_sink(
        "jsonl",
        serde_json::json!({ "path": "/tmp/does-not-need-to-exist.jsonl" }),
        &AuthCatalog::new(),
    )
    .await
    .expect("jsonl sink should build without I/O");
    assert_eq!(sink.connector_name(), "jsonl");
}

// The stdout sink builds without any config fields and without I/O.
#[cfg(feature = "sink-stdout")]
#[tokio::test]
async fn build_sink_stdout_succeeds_without_io() {
    let sink = build_sink("stdout", serde_json::json!({}), &AuthCatalog::new())
        .await
        .expect("stdout sink should build without I/O");
    // The stdout sink overrides `connector_name()` with its friendly,
    // YAML-`type`-matching label (#61).
    assert_eq!(sink.connector_name(), "stdout");
}

// Exercise the Delta source+sink registry arms end to end: build both via
// the registry, round-trip a page through a real local table, and confirm
// the schema + description arms resolve.
#[cfg(all(feature = "source-delta", feature = "sink-delta"))]
#[tokio::test]
async fn delta_registry_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("reg_delta").to_string_lossy().into_owned();

    assert!(source_schema("delta").is_ok());
    assert!(sink_schema("delta").is_ok());
    assert!(source_descriptions().iter().any(|(n, _)| *n == "delta"));
    assert!(sink_descriptions().iter().any(|(n, _)| *n == "delta"));

    let sink = build_sink(
        "delta",
        serde_json::json!({ "table_uri": uri }),
        &AuthCatalog::new(),
    )
    .await
    .expect("delta sink builds");
    assert_eq!(sink.connector_name(), "delta");
    let n = sink
        .write_batch(&[serde_json::json!({"id": 1}), serde_json::json!({"id": 2})])
        .await
        .expect("write");
    assert_eq!(n, 2);
    sink.flush().await.expect("flush");

    let source = build_source(
        "delta",
        serde_json::json!({ "table_uri": uri }),
        &AuthCatalog::new(),
        None,
    )
    .await
    .expect("delta source builds");
    assert_eq!(source.connector_name(), "delta");
    let rows = source
        .fetch_with_context(&std::collections::HashMap::new())
        .await
        .expect("read");
    assert_eq!(rows.len(), 2);
}

// The Databricks source builds from the registry (no I/O in `new`), and its
// schema + description arms resolve.
#[cfg(feature = "source-databricks")]
#[tokio::test]
async fn databricks_registry_source_builds() {
    assert!(source_schema("databricks").is_ok());
    assert!(
        source_descriptions()
            .iter()
            .any(|(n, _)| *n == "databricks")
    );
    let cfg = serde_json::json!({
        "workspace_url": "https://x.cloud.databricks.com",
        "warehouse_id": "wh1",
        "sql": "SELECT 1",
        "auth": { "type": "pat", "config": { "token": "t" } }
    });
    let src = build_source("databricks", cfg, &AuthCatalog::new(), None)
        .await
        .expect("databricks source builds");
    assert_eq!(src.connector_name(), "databricks");
}

// A malformed config for a known connector must surface as a typed
// `InvalidConnectorConfig` from the `decode` helper, not a panic.
#[cfg(feature = "source-csv")]
#[tokio::test]
async fn build_source_csv_invalid_config_errors() {
    // `path` is a required String; supplying an integer is a type error.
    // `Box<dyn Source>` is not `Debug`, so match the Result directly rather
    // than using `expect_err`.
    let res = build_source(
        "csv",
        serde_json::json!({ "path": 42 }),
        &AuthCatalog::new(),
        None,
    )
    .await;
    match res {
        Err(RegistryError::InvalidConnectorConfig { kind, name, .. }) => {
            assert_eq!(kind, "source");
            assert_eq!(name, "csv");
        }
        Ok(_) => panic!("expected InvalidConnectorConfig, got Ok"),
        Err(other) => panic!("expected InvalidConnectorConfig, got {other:?}"),
    }
}

// The CSV / GraphQL sources construct infallibly (`new() -> Self`), so the
// registry is their config-load fail-fast point: an out-of-range
// `batch_size` or an empty required field must be rejected at `build_source`
// (a typed `FaucetError::Config`), not surfaced deep in a run.
#[cfg(feature = "source-csv")]
#[tokio::test]
async fn build_source_csv_rejects_oversized_batch_size() {
    let res = build_source(
        "csv",
        serde_json::json!({
            "path": "/tmp/x.csv",
            "batch_size": faucet_core::MAX_BATCH_SIZE + 1
        }),
        &AuthCatalog::new(),
        None,
    )
    .await;
    assert!(matches!(
        res,
        Err(RegistryError::Faucet(faucet_core::FaucetError::Config(_)))
    ));
}

#[cfg(feature = "source-graphql")]
#[tokio::test]
async fn build_source_graphql_rejects_empty_endpoint() {
    let res = build_source(
        "graphql",
        serde_json::json!({
            "endpoint": "",
            "query": "query { x }",
            "variables": {},
            "auth": { "type": "none" }
        }),
        &AuthCatalog::new(),
        None,
    )
    .await;
    assert!(matches!(
        res,
        Err(RegistryError::Faucet(faucet_core::FaucetError::Config(_)))
    ));
}

// `source_schema` must return a JSON object that surfaces the connector's
// config fields (here: the required `path`).
#[cfg(feature = "source-csv")]
#[test]
fn source_schema_csv_exposes_path_property() {
    let schema = source_schema("csv").expect("csv schema");
    let props = schema
        .get("properties")
        .and_then(Value::as_object)
        .expect("schema should have a properties object");
    assert!(props.contains_key("path"), "schema props: {props:?}");
}

#[cfg(feature = "sink-jsonl")]
#[test]
fn sink_schema_jsonl_exposes_path_property() {
    let schema = sink_schema("jsonl").expect("jsonl schema");
    let props = schema
        .get("properties")
        .and_then(Value::as_object)
        .expect("schema should have a properties object");
    assert!(props.contains_key("path"), "schema props: {props:?}");
}

#[test]
fn unknown_source_schema_errors_with_available_list() {
    let err = source_schema("definitely-not-a-source").expect_err("unknown source");
    match err {
        RegistryError::UnknownConnector {
            kind,
            name,
            available,
        } => {
            assert_eq!(kind, "source");
            assert_eq!(name, "definitely-not-a-source");
            // Under `--all-features` the available list is non-empty.
            assert!(!available.is_empty());
        }
        other => panic!("expected UnknownConnector, got {other:?}"),
    }
}

#[test]
fn unknown_sink_schema_errors() {
    let err = sink_schema("definitely-not-a-sink").expect_err("unknown sink");
    assert!(matches!(
        err,
        RegistryError::UnknownConnector { kind: "sink", .. }
    ));
}

#[cfg(feature = "source-csv")]
#[test]
fn source_exists_is_true_for_known_and_false_for_unknown() {
    assert!(source_exists("csv"));
    assert!(!source_exists("definitely-not-a-source"));
}

#[cfg(feature = "sink-jsonl")]
#[test]
fn sink_exists_is_true_for_known_and_false_for_unknown() {
    assert!(sink_exists("jsonl"));
    assert!(!sink_exists("definitely-not-a-sink"));
}

// Descriptions back `faucet list`: non-empty, with a one-line summary, and
// each name must resolve to a real schema (no orphan listing).
#[test]
fn source_descriptions_are_non_empty_and_consistent() {
    let descs = source_descriptions();
    // This crate can be built with no connector at all.
    if cfg!(feature = "source-rest") {
        assert!(!descs.is_empty());
    }
    for (name, summary) in &descs {
        assert!(!name.is_empty(), "empty connector name");
        assert!(!summary.is_empty(), "empty summary for {name}");
        assert!(
            source_schema(name).is_ok(),
            "listed source `{name}` has no schema"
        );
    }
}

#[test]
fn sink_descriptions_are_non_empty_and_consistent() {
    let descs = sink_descriptions();
    if cfg!(feature = "sink-jsonl") {
        assert!(!descs.is_empty());
    }
    for (name, summary) in &descs {
        assert!(!name.is_empty(), "empty connector name");
        assert!(!summary.is_empty(), "empty summary for {name}");
        assert!(
            sink_schema(name).is_ok(),
            "listed sink `{name}` has no schema"
        );
    }
}

// `*_kinds()` is derived from `*_descriptions()`; under `--all-features`
// the canonical built-in connectors must be present.
#[cfg(all(feature = "source-csv", feature = "source-rest"))]
#[test]
fn source_kinds_contains_expected_builtins() {
    let kinds = source_kinds();
    assert!(kinds.contains(&"csv"));
    assert!(kinds.contains(&"rest"));
}

#[cfg(all(feature = "sink-jsonl", feature = "sink-stdout"))]
#[test]
fn sink_kinds_contains_expected_builtins() {
    let kinds = sink_kinds();
    assert!(kinds.contains(&"jsonl"));
    assert!(kinds.contains(&"stdout"));
}

// Build a catalog holding one `static` bearer provider, then build a
// connector whose config carries `auth: { ref: "tok" }` — exercising the
// `with_auth_provider` injection branch in the dispatch arm.
#[cfg(feature = "source-rest")]
#[tokio::test]
async fn build_source_injects_referenced_auth_provider() {
    let mut specs = std::collections::HashMap::new();
    specs.insert(
        "tok".to_string(),
        serde_json::json!({"type": "static", "config": {"token": "abc"}}),
    );
    let catalog = auth_catalog::build_auth_catalog(Some(&specs)).expect("catalog");

    let src = build_source("rest", rest_config_with_auth_ref("tok"), &catalog, None)
        .await
        .expect("rest source with a resolvable auth ref should build");
    assert_eq!(src.connector_name(), "rest");
}

// A minimal, fully-valid rest config (built from the real constructor so
// every required field is present) carrying an `auth: { ref }` pointer.
#[cfg(feature = "source-rest")]
fn rest_config_with_auth_ref(name: &str) -> Value {
    let cfg = faucet_source_rest::RestStreamConfig::new("https://api.example.com", "/v1");
    let mut v = serde_json::to_value(cfg).expect("serialize rest config");
    v.as_object_mut()
        .unwrap()
        .insert("auth".to_string(), serde_json::json!({ "ref": name }));
    v
}

// An `auth: { ref }` pointing at a name absent from the catalog must surface
// as `UnknownAuthProvider`, not silently build without auth.
#[cfg(feature = "source-rest")]
#[tokio::test]
async fn build_source_unknown_auth_ref_errors() {
    let res = build_source(
        "rest",
        rest_config_with_auth_ref("missing"),
        &AuthCatalog::new(),
        None,
    )
    .await;
    match res {
        Err(RegistryError::UnknownAuthProvider { name, .. }) => assert_eq!(name, "missing"),
        Ok(_) => panic!("expected UnknownAuthProvider, got Ok"),
        Err(other) => panic!("expected UnknownAuthProvider, got {other:?}"),
    }
}

#[cfg(feature = "sink-singer")]
#[tokio::test]
async fn singer_sink_builds_validates_and_registers_secrets() {
    let auth = crate::auth_catalog::AuthCatalog::new();
    let cfg = json!({
        "target_command": "target-jsonl",
        "target_config": {"api_token": "singer-reg-secret-1234"},
        "stream": "s"
    });
    crate::secrets::test_support::install();
    match build_sink("singer", cfg.clone(), &auth).await {
        Err(RegistryError::SubprocessRefused { role: "sink", kind }) => {
            assert_eq!(kind, "singer")
        }
        other => panic!("expected SubprocessRefused, got {:?}", other.map(|_| ())),
    }
    assert!(!crate::secrets::test_support::reported(
        "singer-reg-secret-1234"
    ));
    let trusted = crate::BuildPolicy::trusted();
    let sink = build_sink_with("singer", cfg.clone(), &auth, &trusted)
        .await
        .unwrap();
    assert_eq!(sink.connector_name(), "singer");
    assert!(sink_runs_program("singer"));
    assert!(crate::secrets::test_support::reported(
        "singer-reg-secret-1234"
    ));
    assert_eq!(
        sink_batch_atomicity("singer", &cfg),
        Some(faucet_core::BatchAtomicity::BestEffort)
    );
    assert!(sink_schema("singer").is_ok());
    assert!(validate_sink_config("singer", "row", cfg).is_ok());
    let bad = json!({"target_command": "t", "write_mode": "delete", "key": ["id"]});
    assert!(validate_sink_config("singer", "row", bad).is_err());
    assert!(sink_descriptions().iter().any(|(k, _)| *k == "singer"));
}

#[cfg(feature = "sink-databricks")]
#[tokio::test]
async fn databricks_registry_sink_builds_with_auth_ref() {
    assert!(sink_schema("databricks").is_ok());
    assert!(sink_kinds().contains(&"databricks"));
    let mut specs = std::collections::HashMap::new();
    specs.insert(
        "dbx".to_string(),
        serde_json::json!({"type": "static", "config": {"token": "abc"}}),
    );
    let catalog = auth_catalog::build_auth_catalog(Some(&specs)).expect("catalog");
    let cfg = serde_json::json!({
        "workspace_url": "https://x.cloud.databricks.com",
        "warehouse_id": "w",
        "auth": { "ref": "dbx" },
        "schema": "s",
        "table": "t",
    });
    validate_sink_config("databricks", "row", cfg.clone()).expect("valid databricks config");
    let sink = build_sink("databricks", cfg, &catalog)
        .await
        .expect("databricks sink builds");
    assert_eq!(sink.connector_name(), "databricks");
    assert!(sink.supports_idempotent_writes());
}

#[cfg(all(
    feature = "source-iceberg",
    feature = "source-dynamodb",
    feature = "sink-dynamodb"
))]
#[test]
fn iceberg_and_dynamodb_are_registered() {
    assert!(source_schema("iceberg").is_ok());
    assert!(source_schema("dynamodb").is_ok());
    assert!(sink_schema("dynamodb").is_ok());
    assert!(source_kinds().contains(&"iceberg"));
    assert!(source_kinds().contains(&"dynamodb"));
    assert!(sink_kinds().contains(&"dynamodb"));
    let err = validate_sink_config(
        "dynamodb",
        "row",
        serde_json::json!({"table_name": "t", "write_mode": "overwrite"}),
    )
    .expect_err("dynamodb rejects overwrite");
    assert!(err.to_string().contains("overwrite"), "{err}");
    validate_source_config(
        "dynamodb",
        "row",
        serde_json::json!({"table_name": "t", "mode": "scan"}),
    )
    .expect("scan config is valid");
}

#[cfg(feature = "source-webhook")]
#[test]
fn validate_runs_the_webhook_config_checks() {
    let err = validate_source_config("webhook", "row", serde_json::json!({"path": "webhook"}))
        .expect_err("a path without a leading slash is refused");
    assert!(err.to_string().contains("must start with `/`"), "{err}");
    let err = validate_source_config(
        "webhook",
        "row",
        serde_json::json!({"path": "/w", "signature": {"header": "", "secret": ""}}),
    )
    .expect_err("an empty signature is refused");
    assert!(err.to_string().contains("signature.header"), "{err}");
    validate_source_config("webhook", "row", serde_json::json!({"path": "/w"}))
        .expect("a plain path is valid");
}

/// A first-party kind this build left out is assumed to support everything,
/// so no load-time gate refuses it before the build reports it unknown.
#[test]
fn an_uncompiled_builtin_is_assumed_capable() {
    let keyed = serde_json::json!({"write_mode": "upsert", "key": ["id"]});
    let c = uncompiled_capabilities(&keyed);
    assert_eq!(c.write_modes().len(), 4);
    assert!(c.idempotent_writes() && c.keyed_upsert() && c.keyed_dedup());
    assert!(c.cleanup() && c.rollback() && c.schema_evolution());
    assert!(c.scoped_overwrite() && c.direct_overwrite() && c.staged_load());
    assert!(c.exactly_once() && c.discover() && c.shardable());
    assert!(c.range_read() && c.range_digest() && c.reports_lag());
    assert!(!uncompiled_capabilities(&serde_json::json!({})).keyed_dedup());
    assert!(!uncompiled_builtin("no-such-kind", "sink"));
}

/// The kind-level capabilities the built-in connectors declare — what the
/// hand-kept `*_KINDS` allowlists used to say (#876). A kind this build did
/// not compile in is skipped.
#[test]
fn builtin_kind_capabilities_are_pinned() {
    use faucet_core::WriteMode::{Append as A, Delete as D, Overwrite as O, Upsert as U};
    #[allow(clippy::type_complexity)]
    let sinks: &[(&str, &[faucet_core::WriteMode], bool, bool, bool, bool)] = &[
        // kind, write modes, idempotent, keyed upsert, cleanup, schema evolution
        ("postgres", &[A, U, D, O], true, true, true, true),
        ("sqlite", &[A, U, D, O], true, true, true, true),
        ("mysql", &[A, U, D, O], true, true, true, true),
        ("mssql", &[A, U, D, O], true, true, true, true),
        ("mongodb", &[A, U, D, O], true, true, true, false),
        ("elasticsearch", &[A, U, D, O], false, true, true, true),
        ("bigquery", &[A, U, D, O], true, true, true, true),
        ("spanner", &[A, U, D], true, true, true, true),
        ("dynamodb", &[A, U, D], false, true, false, false),
        ("databricks", &[A, U, D, O], true, true, false, true),
        ("oracle", &[A, U, D, O], true, true, false, true),
        ("iceberg", &[A], true, false, false, true),
        ("kafka", &[A], true, false, false, false),
        ("snowflake", &[A], true, false, false, false),
        ("redis", &[A], true, false, false, false),
        ("singer", &[A, U, O], false, false, false, false),
        ("file", &[A, O], false, false, false, false),
        ("jsonl", &[A], false, false, false, false),
        ("stdout", &[A], false, false, false, false),
    ];
    let mut checked = 0;
    let compiled = sinks.iter().filter(|s| sink_descriptor(s.0).is_some());
    for (kind, modes, idempotent, keyed, cleanup, evolution) in compiled {
        checked += 1;
        let c = sink_kind_capabilities(kind);
        assert_eq!(c.write_modes(), *modes, "{kind} write modes");
        assert_eq!(c.idempotent_writes(), *idempotent, "{kind} idempotent");
        assert_eq!(c.keyed_upsert(), *keyed, "{kind} keyed upsert");
        assert_eq!(c.cleanup(), *cleanup, "{kind} cleanup");
        assert_eq!(c.schema_evolution(), *evolution, "{kind} schema evolution");
        let expected = if *idempotent {
            faucet_core::SinkGuarantee::AtomicWatermark
        } else if *keyed {
            faucet_core::SinkGuarantee::KeyedUpsert
        } else {
            faucet_core::SinkGuarantee::AtLeastOnce
        };
        assert_eq!(c.sink_guarantee(), expected, "{kind} guarantee");
    }
    let sources: &[(&str, bool, bool)] = &[
        // kind, exactly-once replay, discover
        ("postgres-cdc", true, false),
        ("mysql-cdc", true, false),
        ("mssql-cdc", true, false),
        ("mongodb-cdc", true, false),
        ("oracle-cdc", true, false),
        ("kafka", true, false),
        ("postgres", false, true),
        ("oracle", false, true),
        ("iceberg", false, true),
        ("dynamodb", false, true),
        ("file", false, true),
        ("csv", false, false),
    ];
    let compiled = sources.iter().filter(|s| source_descriptor(s.0).is_some());
    for (kind, exactly_once, discover) in compiled {
        checked += 1;
        let c = source_kind_capabilities(kind);
        assert_eq!(c.exactly_once(), *exactly_once, "{kind} exactly-once");
        assert_eq!(c.discover(), *discover, "{kind} discover");
    }
    if cfg!(feature = "sink-jsonl") {
        assert!(checked > 0, "no built-in connector compiled in");
    }
    // Spot checks of the capabilities no per-capability trait method has.
    let spot = |kind: &str, f: fn(&Capabilities) -> bool, want: bool, sink: bool| {
        let present = if sink {
            sink_descriptor(kind).is_some()
        } else {
            source_descriptor(kind).is_some()
        };
        let c = if sink {
            sink_kind_capabilities(kind)
        } else {
            source_kind_capabilities(kind)
        };
        let checked = present.then_some(c);
        checked
            .iter()
            .for_each(|c| assert_eq!(f(c), want, "{kind}"));
    };
    spot("bigquery", Capabilities::staged_load, true, true);
    spot("redshift", Capabilities::staged_load, true, true);
    spot("postgres", Capabilities::staged_load, false, true);
    spot("postgres", Capabilities::rollback, true, true);
    spot("mongodb", Capabilities::rollback, false, true);
    spot("postgres", Capabilities::scoped_overwrite, true, true);
    spot("bigquery", Capabilities::scoped_overwrite, true, true);
    spot("sqlite", Capabilities::scoped_overwrite, false, true);
    spot("bigquery", Capabilities::direct_overwrite, true, true);
    spot("postgres", Capabilities::direct_overwrite, false, true);
    spot("postgres", Capabilities::range_read, true, false);
    spot("sqlite", Capabilities::range_read, true, false);
    spot("csv", Capabilities::range_read, false, false);
    spot("postgres", Capabilities::range_digest, true, false);
    spot("sqlite", Capabilities::range_digest, false, false);
    spot("kafka", Capabilities::reports_lag, true, false);
    spot("postgres", Capabilities::reports_lag, false, false);
}

/// Config-dependent capabilities are answered from the config (#876): a
/// SQL sink cleans up and rolls back only under column mapping.
#[cfg(feature = "sink-sqlite")]
#[test]
fn sink_capabilities_read_the_config() {
    let json_column = sink_capabilities("sqlite", &json!({"table_name": "t"}));
    assert!(!json_column.cleanup() && !json_column.rollback());
    assert!(!json_column.keyed_dedup());
    let mapped = sink_capabilities(
        "sqlite",
        &json!({"column_mapping": "auto_map", "write_mode": "upsert", "key": ["id"]}),
    );
    assert!(mapped.cleanup() && mapped.keyed_dedup());
    assert_eq!(
        sink_capabilities("sqlite", &Value::Null),
        sink_capabilities("sqlite", &json!({})),
        "an absent config is an empty one, not the kind-level view"
    );
}

#[cfg(not(feature = "sink-oracle"))]
#[test]
fn an_uncompiled_builtin_is_not_refused_at_load() {
    let c = sink_capabilities("oracle", &json!({"write_mode": "upsert", "key": ["id"]}));
    assert!(c.supports_write_mode(faucet_core::WriteMode::Upsert));
    assert!(c.keyed_dedup() && c.idempotent_writes());
    assert!(!sink_capabilities("oracle", &json!({})).keyed_dedup());
    assert_eq!(
        sink_kind_capabilities("oracle"),
        Capabilities::append_only_sink(),
        "listings show only what is compiled in"
    );
}

#[cfg(not(feature = "source-oracle-cdc"))]
#[test]
fn an_uncompiled_builtin_source_is_not_refused_at_load() {
    assert!(source_capabilities("oracle-cdc", &json!({})).exactly_once());
}

#[test]
fn unknown_kinds_declare_nothing() {
    assert!(source_descriptor("no-such-source").is_none());
    assert!(sink_descriptor("no-such-sink").is_none());
    assert_eq!(
        source_capabilities("no-such-source", &json!({})),
        Capabilities::plain_source()
    );
    assert_eq!(
        sink_capabilities("no-such-sink", &json!({})),
        Capabilities::append_only_sink()
    );
    assert_eq!(
        sink_kind_capabilities("no-such-sink"),
        Capabilities::append_only_sink()
    );
    assert_eq!(
        source_kind_capabilities("no-such-source"),
        Capabilities::plain_source()
    );
    assert_eq!(
        sink_kinds_where(|c| c.supports_write_mode(faucet_core::WriteMode::Append)).is_empty(),
        sink_kinds().is_empty()
    );
    assert!(source_kinds_where(|_| false).is_empty());
}

#[test]
fn edit_distance_is_levenshtein() {
    assert_eq!(edit_distance("", ""), 0);
    assert_eq!(edit_distance("abc", "abc"), 0);
    assert_eq!(edit_distance("", "abc"), 3);
    assert_eq!(edit_distance("abc", ""), 3);
    assert_eq!(edit_distance("batch_size", "batch_size"), 0);
    assert_eq!(edit_distance("batch_sizes", "batch_size"), 1);
    assert_eq!(edit_distance("kitten", "sitting"), 3);
}

#[test]
fn nearest_key_only_suggests_a_plausible_typo() {
    let known: Vec<String> = ["batch_size", "verify_checksum", "prefix"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    assert_eq!(
        nearest_key("verify_checksums", known.iter()),
        Some("verify_checksum")
    );
    assert_eq!(nearest_key("batch_sze", known.iter()), Some("batch_size"));
    // Not a typo of anything here — better to say nothing than to guess.
    assert_eq!(nearest_key("region", known.iter()), None);
}

/// #789 CLI-136: validate runs each config's own `validate()` for these
/// kinds too, so a config refused at run time is not reported valid.
#[cfg(all(
    feature = "source-elasticsearch",
    feature = "sink-sftp",
    feature = "sink-azure-blob"
))]
#[test]
fn validate_runs_the_connector_validate_for_es_sftp_and_azure() {
    let es = serde_json::to_value(faucet_source_elasticsearch::ElasticsearchSourceConfig::new(
        "http://localhost:9200",
        "",
    ))
    .unwrap();
    let err = validate_source_config("elasticsearch", "r", es).unwrap_err();
    assert!(err.to_string().contains("non-empty `index`"), "{err}");

    let mut az =
        serde_json::to_value(faucet_sink_azure_blob::AzureBlobSinkConfig::new("c")).unwrap();
    validate_sink_config("azure-blob", "r", az.clone()).unwrap();
    az["batch_size"] = json!(2_000_000);
    assert!(validate_sink_config("azure-blob", "r", az).is_err());

    let conn = faucet_sink_sftp::SftpConnectionConfig::with_password("h", "u", "p");
    let mut sftp = serde_json::to_value(faucet_sink_sftp::SftpSinkConfig::new(conn, "/o")).unwrap();
    validate_sink_config("sftp", "r", sftp.clone()).unwrap();
    sftp["batch_size"] = json!(2_000_000);
    assert!(validate_sink_config("sftp", "r", sftp).is_err());
}

/// #789 CLI-72: the executor's own `_overwrite_staging` flag passes the
/// unknown-key gate, so a grouped BigQuery overwrite can be built.
#[cfg(feature = "sink-bigquery")]
#[test]
fn executor_injected_keys_pass_the_unknown_key_gate() {
    let schema = sink_schema("bigquery").unwrap();
    let cfg = json!({
        "project_id": "p", "dataset_id": "d", "table_id": "t",
        "write_mode": "overwrite", "_overwrite_staging": true
    });
    reject_unknown_config_keys("sink", "bigquery", "row", &cfg, &schema).unwrap();
    let typo = json!({ "project_id": "p", "_overwrite_stagin": true });
    assert!(reject_unknown_config_keys("sink", "bigquery", "row", &typo, &schema).is_err());
}

/// The executor's overwrite run token is taken out of a sink config
/// before it is decoded, and only a string (or null) is accepted.
#[test]
fn the_overwrite_run_token_is_taken_out_of_the_config() {
    let (cfg, run) = take_overwrite_run(
        "sqlite",
        json!({"a": 1, "_overwrite_run": "67000000abcdef"}),
    )
    .unwrap();
    assert_eq!(
        (cfg, run.as_deref()),
        (json!({"a": 1}), Some("67000000abcdef"))
    );
    let (cfg, run) = take_overwrite_run("sqlite", json!({"_overwrite_run": null})).unwrap();
    assert_eq!((cfg, run), (json!({}), None));
    let (cfg, run) = take_overwrite_run("sqlite", json!("x")).unwrap();
    assert_eq!((cfg, run), (json!("x"), None));
    assert!(take_overwrite_run("sqlite", json!({"_overwrite_run": 7})).is_err());
    let schema = json!({"type": "object", "properties": {"a": {}}});
    reject_unknown_config_keys(
        "sink",
        "sqlite",
        "row",
        &json!({"a": 1, "_overwrite_run": "67000000abcdef"}),
        &schema,
    )
    .unwrap();
}

/// A sink that stages an overwrite joins the run; a malformed token is
/// refused before it can reach a table name.
#[cfg(feature = "sink-sqlite")]
#[tokio::test]
async fn a_staging_sink_is_built_joined_to_its_overwrite_run() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}?mode=rwc", dir.path().join("t.db").display());
    let cfg = |token: &str| {
        json!({
            "database_url": url, "table_name": "t", "column_mapping": "auto_map",
            "write_mode": "overwrite", "_overwrite_run": token
        })
    };
    let auth = AuthCatalog::default();
    build_sink("sqlite", cfg("67000000abcdef"), &auth)
        .await
        .unwrap();
    let err = build_sink("sqlite", cfg("../x"), &auth)
        .await
        .err()
        .unwrap();
    assert!(err.to_string().contains("_overwrite_run"), "{err}");
}

#[cfg(all(feature = "sink-sftp", feature = "sink-databricks"))]
#[tokio::test]
async fn sftp_and_databricks_sinks_join_their_overwrite_run() {
    let auth = AuthCatalog::default();
    let sftp = |token: &str| {
        json!({
            "host": "sftp.example.com", "username": "user",
            "type": "password", "config": {"password": "secret"},
            "path": "/upload", "file_name": "out.jsonl",
            "write_mode": "overwrite", "_overwrite_run": token
        })
    };
    let dbx = |token: &str| {
        json!({
            "workspace_url": "https://x", "warehouse_id": "wh",
            "auth": {"type": "pat", "config": {"token": "t"}},
            "catalog": "main", "schema": "s", "table": "t",
            "write_mode": "overwrite", "_overwrite_run": token
        })
    };
    for (kind, cfg) in [
        ("sftp", &sftp as &dyn Fn(&str) -> Value),
        ("databricks", &dbx),
    ] {
        build_sink(kind, cfg("67000000abcdef"), &auth)
            .await
            .unwrap_or_else(|e| panic!("{kind}: {e}"));
        let err = build_sink(kind, cfg("../x"), &auth).await.err().unwrap();
        assert!(err.to_string().contains("overwrite"), "{kind}: {err}");
    }
}

#[cfg(feature = "source-bigquery")]
#[tokio::test]
async fn a_bigquery_source_takes_the_retry_policy() {
    let sa = json!({
        "type": "service_account", "project_id": "dummy", "private_key_id": "dummy",
        "private_key": include_str!("../../tests/fixtures/bigquery_test_key.pem"),
        "client_email": "dummy@developer.gserviceaccount.com", "client_id": "dummy",
        "token_uri": "http://127.0.0.1:9/token",
    });
    let cfg = json!({
        "project_id": "p",
        "auth": {"type": "service_account_key", "config": {"json": sa.to_string()}},
        "query": "SELECT 1",
    });
    let policy = faucet_core::RetryPolicy::default();
    let src = build_source("bigquery", cfg, &AuthCatalog::new(), Some(&policy))
        .await
        .unwrap();
    assert_eq!(src.connector_name(), "bigquery");
}

#[test]
fn unknown_connector_keys_are_rejected_with_the_declared_list() {
    let schema = json!({
        "type": "object",
        "properties": { "bucket": {}, "prefix": {}, "verify_checksum": {} }
    });
    let ok = json!({ "bucket": "b", "verify_checksum": true });
    assert!(reject_unknown_config_keys("source", "s3", "row", &ok, &schema).is_ok());

    // The motivating case: one letter off, so integrity checking silently
    // stayed OFF while the config reads as if it were on.
    let typo = json!({ "bucket": "b", "verify_checksums": true });
    let err = reject_unknown_config_keys("source", "s3", "row", &typo, &schema)
        .expect_err("a typo'd key must not be silently ignored");
    let msg = err.to_string();
    assert!(msg.contains("verify_checksums"), "{msg}");
    assert!(
        msg.contains("did you mean `verify_checksum`"),
        "the near-miss must be named, or the user re-reads the whole schema: {msg}"
    );
}

#[test]
fn a_declared_catch_all_schema_accepts_any_key() {
    // A config with a keyless `flatten`-map says "every key is mine";
    // rejecting there would break a legitimate passthrough config.
    let schema = json!({
        "type": "object",
        "properties": { "url": {} },
        "additionalProperties": { "type": "string" }
    });
    let cfg = json!({ "url": "u", "anything_at_all": "x" });
    assert!(reject_unknown_config_keys("sink", "http", "row", &cfg, &schema).is_ok());

    // An explicit `false` is the opposite claim and still rejects.
    let strict = json!({
        "type": "object",
        "properties": { "url": {} },
        "additionalProperties": false
    });
    assert!(reject_unknown_config_keys("sink", "http", "row", &cfg, &strict).is_err());
}

#[test]
fn a_non_object_config_or_schema_is_left_alone() {
    // `config: "${vars.x}"` is resolved elsewhere; a schema with no
    // `properties` (a `oneOf` wrapper, a plugin that declines to describe
    // itself) carries no key list to check against.
    let schema = json!({ "type": "object", "properties": { "a": {} } });
    assert!(reject_unknown_config_keys("source", "k", "row", &json!("${vars.x}"), &schema).is_ok());
    assert!(
        reject_unknown_config_keys("source", "k", "row", &json!({"zzz": 1}), &json!({})).is_ok()
    );
}

#[test]
fn a_flattened_tagged_enum_contributes_its_variant_keys() {
    // `#[serde(flatten)] auth: SftpAuth` renders as a root-level `oneOf`
    // of variant objects, not as inlined `properties` — so `type` and
    // `config` are legitimate top-level keys even though the struct never
    // names them. Intersecting the branches would reject a correct config;
    // unioning keeps this a typo check.
    let schema = json!({
        "type": "object",
        "properties": { "host": {}, "username": {} },
        "oneOf": [
            { "type": "object", "properties": { "type": {}, "config": {} } },
            { "type": "object", "properties": { "type": {}, "key_path": {} } }
        ]
    });
    let cfg = json!({ "host": "h", "username": "u", "type": "password", "config": {} });
    assert!(reject_unknown_config_keys("source", "sftp", "row", &cfg, &schema).is_ok());
    // A key in no branch is still caught.
    let bad = json!({ "host": "h", "usernme": "u" });
    assert!(reject_unknown_config_keys("source", "sftp", "row", &bad, &schema).is_err());
}

#[test]
fn a_declared_serde_alias_is_accepted() {
    // schemars does not emit serde `alias`es, so a config that declares
    // one lists it under `x-faucet-aliases`. Without this the gate would
    // reject a key serde accepts — which is how a documented back-compat
    // alias silently stops working (#580 hit exactly this on delta and
    // iceberg's `create_table` rename).
    let schema = json!({
        "type": "object",
        "properties": { "create_table": {} },
        "x-faucet-aliases": ["create_if_missing"]
    });
    let cfg = json!({ "create_if_missing": true });
    assert!(reject_unknown_config_keys("sink", "iceberg", "row", &cfg, &schema).is_ok());
    // An undeclared key is still rejected.
    assert!(
        reject_unknown_config_keys("sink", "iceberg", "row", &json!({"nope": 1}), &schema).is_err()
    );
}

#[test]
fn a_ref_into_defs_is_followed() {
    let schema = json!({
        "type": "object",
        "properties": { "a": {} },
        "allOf": [ { "$ref": "#/$defs/Extra" } ],
        "$defs": { "Extra": { "type": "object", "properties": { "b": {} } } }
    });
    assert!(
        reject_unknown_config_keys("sink", "k", "row", &json!({"a": 1, "b": 2}), &schema).is_ok()
    );
    assert!(reject_unknown_config_keys("sink", "k", "row", &json!({"c": 3}), &schema).is_err());
    // An unresolvable ref means we don't know the key set → skip, never
    // reject.
    let dangling = json!({
        "type": "object",
        "properties": { "a": {} },
        "allOf": [ { "$ref": "#/$defs/Missing" } ]
    });
    assert!(reject_unknown_config_keys("sink", "k", "row", &json!({"c": 3}), &dangling).is_ok());
}

#[cfg(feature = "source-sftp")]
#[test]
fn the_shipped_sftp_shape_with_flattened_auth_is_accepted() {
    // Guards the real schema, not a hand-written stand-in: a shipped
    // example used exactly this shape and the first version of this check
    // rejected it.
    let cfg = json!({
        "host": "sftp.example.com", "port": 22, "username": "reporting",
        "type": "password", "config": { "password": "x" },
        "known_hosts": { "mode": "accept_new" },
        "path": "/exports/daily", "glob": "*.jsonl",
        "format": "jsonl", "batch_size": 1000
    });
    assert!(validate_source_config("sftp", "row", cfg).is_ok());
}

#[cfg(feature = "sink-bigquery")]
#[test]
fn a_flattened_config_still_declares_its_flattened_keys() {
    // The whole reason for the boundary check: `deny_unknown_fields` is
    // refused on a struct with `#[serde(flatten)]`, and 25 configs flatten.
    // schemars inlines the flattened members, so the key list stays right.
    let schema = sink_schema("bigquery").expect("bigquery schema");
    let props = schema["properties"].as_object().expect("properties");
    assert!(props.contains_key("write_mode"), "flattened WriteSpec key");
    assert!(props.contains_key("project_id"), "own key");

    let good = json!({
        "project_id": "p", "dataset_id": "d", "table_id": "t",
        "auth": {"type": "application_default"}, "write_mode": "append"
    });
    assert!(validate_sink_config("bigquery", "row", good).is_ok());

    let bad = json!({
        "project_id": "p", "dataset_id": "d", "table_id": "t",
        "auth": {"type": "application_default"}, "write_modes": "append"
    });
    let err = validate_sink_config("bigquery", "row", bad)
        .expect_err("a typo'd flattened key must be rejected");
    assert!(err.to_string().contains("write_modes"), "{err}");
}

#[test]
fn concurrency_override_sets_the_first_declared_knob() {
    let schema = json!({
        "type": "object",
        "properties": { "connection_url": {}, "max_connections": {}, "batch_size": {} }
    });
    let mut cfg = json!({ "connection_url": "postgres://u@h/db", "max_connections": 10 });
    assert_eq!(
        apply_concurrency_override(&schema, &mut cfg, 20),
        Some("max_connections")
    );
    assert_eq!(
        cfg["max_connections"], 20,
        "the override must win over the config"
    );
    assert_eq!(
        cfg["connection_url"], "postgres://u@h/db",
        "nothing else touched"
    );
}

#[test]
fn concurrency_override_is_a_no_op_without_a_knob() {
    // A schemaless file sink has nothing to override. Silently adding a key
    // would also be caught by the unknown-key gate, so this must not write.
    let schema = json!({ "type": "object", "properties": { "path": {} } });
    let mut cfg = json!({ "path": "./out.jsonl" });
    assert_eq!(apply_concurrency_override(&schema, &mut cfg, 20), None);
    assert_eq!(cfg, json!({ "path": "./out.jsonl" }));
}

#[test]
fn concurrency_override_prefers_knobs_in_a_fixed_order() {
    // A connector declaring two must resolve deterministically, or the same
    // request would tune different things on different builds.
    let schema = json!({
        "type": "object",
        "properties": { "concurrency": {}, "max_connections": {} }
    });
    let mut cfg = json!({});
    assert_eq!(
        apply_concurrency_override(&schema, &mut cfg, 3),
        Some("max_connections")
    );
    assert!(cfg.get("concurrency").is_none());
}

#[cfg(feature = "source-postgres")]
#[test]
fn a_real_source_schema_resolves_to_its_pool_knob() {
    let mut cfg = json!({ "connection_url": "postgres://u@h/db", "query": "SELECT 1" });
    assert_eq!(
        override_source_concurrency("postgres", &mut cfg, 20),
        Some("max_connections")
    );
    assert_eq!(cfg["max_connections"], 20);
    // The result must still deserialize — an override that produced an
    // invalid config would fail only at run time.
    validate_source_config("postgres", "row", cfg).expect("overridden config stays valid");
}

#[cfg(feature = "source-rest")]
#[test]
fn the_rest_source_resolves_to_its_request_knob() {
    let mut cfg = json!({ "base_url": "https://api.example.com", "path": "/x" });
    assert_eq!(
        override_source_concurrency("rest", &mut cfg, 6),
        Some("request_concurrency")
    );
    assert_eq!(cfg["request_concurrency"], 6);
    validate_source_config("rest", "row", cfg).expect("overridden config stays valid");
    // The pre-#670 spelling still loads.
    let old = json!({ "base_url": "https://a", "path": "/x", "partition_concurrency": 2, "partitions": [{"id": 1}] });
    validate_source_config("rest", "row", old).expect("old spelling accepted");
}

#[test]
fn an_unknown_connector_kind_is_ignored_rather_than_erroring() {
    let mut cfg = json!({ "a": 1 });
    assert_eq!(
        override_source_concurrency("not-a-connector", &mut cfg, 4),
        None
    );
    assert_eq!(
        override_sink_concurrency("not-a-connector", &mut cfg, 4),
        None
    );
}
