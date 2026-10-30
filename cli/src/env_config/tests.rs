use super::*;
use serde_json::json;

#[test]
fn secret_keys_are_matched_by_their_last_word() {
    for k in [
        "password",
        "db_password",
        "clientSecret",
        "access_token",
        "x-api-key",
        "api_key",
        "AccountKey",
        "SharedAccessSignature",
        "sig",
        "PWD",
        "Authorization",
        "auth",
        "private_key",
        "apikey",
    ] {
        assert!(is_secret_key(k), "{k}");
    }
    for k in [
        "key",
        "keys",
        "partition_key",
        "sort_key",
        "message_key",
        "key_field",
        "author",
        "signature_field",
        "keyspace",
        "password_field",
        "token_url",
        "private",
        "WHERE api_key ",
    ] {
        assert!(!is_secret_key(k), "{k}");
    }
}

#[test]
fn column_names_and_queries_register_nothing() {
    assert!(
        embedded_secrets(
            "FAUCET_SINK_DYNAMODB_CONFIG_JSON",
            r#"{"partition_key":"customer_id","sort_key":"order_ts","author":"someone"}"#
        )
        .is_empty()
    );
    assert!(
        embedded_secrets(
            "FAUCET_SOURCE_PG_QUERY",
            "SELECT * FROM t WHERE sort_key = 'customer_id'"
        )
        .is_empty()
    );
    assert_eq!(
        embedded_secrets(
            "FAUCET_SOURCE_X_CONNECTION_STRING",
            "AccountName=acct;AccountKey=k3y-5521;EndpointSuffix=core"
        ),
        vec!["k3y-5521".to_string()]
    );
}

#[test]
fn a_url_password_is_collected_raw_and_percent_decoded() {
    let found = embedded_secrets(
        "FAUCET_SOURCE_URL",
        "postgres://app:p%40ss%2Fword@db.example:5432/app",
    );
    assert!(found.contains(&"p%40ss%2Fword".to_string()), "{found:?}");
    assert!(found.contains(&"p@ss/word".to_string()), "{found:?}");
}

#[cfg(unix)]
#[test]
fn a_nested_type_field_is_not_a_phantom_template() {
    let env: HashMap<String, String> = [
        ("FAUCET_SOURCES_PG_TYPE", "sqlite"),
        ("FAUCET_SOURCES_PG_SLOT_TYPE", "temporary"),
        ("FAUCET_SOURCES_PG_PATH", "a.db"),
        ("FAUCET_SOURCES_PG_API_TYPE", "sqlite"),
        ("FAUCET_SOURCES_PG_API_PATH", "b.db"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    let out = build_named_catalog(&env, "FAUCET_SOURCES_", &["sqlite"]).unwrap();
    let mut names: Vec<&String> = out.keys().collect();
    names.sort();
    assert_eq!(names, vec!["pg", "pg_api"]);
    assert_eq!(out["pg"].config["slot_type"], "temporary");
    assert_eq!(out["pg"].config["path"], "a.db");
    assert_eq!(out["pg_api"].config["path"], "b.db");
}

#[test]
fn non_utf8_variables_are_skipped_unless_they_are_faucet_ones() {
    use std::os::unix::ffi::OsStringExt as _;
    let bad = || std::ffi::OsString::from_vec(vec![0xff, 0xfe]);
    let env = utf8_env([
        ("OTHER".into(), bad()),
        ("FAUCET_SOURCE".into(), "csv".into()),
    ])
    .unwrap();
    assert_eq!(env.get("FAUCET_SOURCE").map(String::as_str), Some("csv"));
    assert!(!env.contains_key("OTHER"));
    let err = utf8_env([("FAUCET_SINK".into(), bad())])
        .unwrap_err()
        .to_string();
    assert!(err.contains("FAUCET_SINK"), "{err}");
}

#[test]
#[serial_test::serial]
fn secret_looking_faucet_values_are_registered() {
    let env: HashMap<String, String> = [
        ("FAUCET_SOURCE_PG_PASSWORD", "pg-pass-4417"),
        ("FAUCET_SOURCE_PG_TABLE", "orders-table-4417"),
        ("FAUCET_SINK_PG_DATABASE_URL", "postgres://u:p@db-4417/x"),
        (
            "FAUCET_SOURCE_REST_BASE_URL",
            "https://api-4417.example.com",
        ),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    register_secret_values(&env);
    let out = crate::secrets::registry::redact(
        "pg-pass-4417 orders-table-4417 postgres://u:p@db-4417/x https://api-4417.example.com",
    );
    assert_eq!(
        out,
        "*** orders-table-4417 *** https://api-4417.example.com"
    );
}

#[test]
fn credentials_inside_json_and_urls_are_registered_on_their_own() {
    let env: HashMap<String, String> = [
        (
            "FAUCET_SOURCE_REST_AUTH_JSON",
            r#"{"type":"bearer","config":{"token":"tok-live-5521"}}"#,
        ),
        (
            "FAUCET_SOURCE_REST_HEADERS_JSON",
            r#"{"x-api-key":"hdr-key-5521","accept":"application/json"}"#,
        ),
        (
            "FAUCET_SOURCE_POSTGRES_CONNECTION_URL",
            "postgres://h-5521/db?user=u&password=q%2Fpw-5521",
        ),
        (
            "FAUCET_SINK_PG_DATABASE_URL",
            "postgres://u:url-pw-5521@db/x",
        ),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    register_secret_values(&env);
    assert!(embedded_secrets("FAUCET_SINK_PG_KEY_JSON", r#"["account_5521"]"#).is_empty());
    assert!(
        embedded_secrets("FAUCET_SINK_KAFKA_CONFIG_JSON", r#"{"key":"account_5521"}"#).is_empty()
    );
    let out = crate::secrets::registry::redact(
        "Bearer tok-live-5521 hdr-key-5521 application/json q/pw-5521 url-pw-5521 h-5521",
    );
    assert_eq!(out, "Bearer *** *** application/json *** *** h-5521");
}

fn env(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect()
}

#[test]
fn extract_scope_lowercases_field_names() {
    let e = env(&[("FAUCET_SOURCE_REST_BASE_URL", "https://x.example")]);
    let v = extract_scope(&e, "FAUCET_SOURCE_REST_").unwrap();
    assert_eq!(v, json!({"base_url": "https://x.example"}));
}

#[test]
fn extract_scope_ignores_unrelated_keys() {
    let e = env(&[
        ("FAUCET_SOURCE_REST_BASE_URL", "https://x.example"),
        ("PATH", "/usr/bin"),
        ("FAUCET_SINK_JSONL_PATH", "./out.jsonl"),
    ]);
    let v = extract_scope(&e, "FAUCET_SOURCE_REST_").unwrap();
    assert_eq!(v, json!({"base_url": "https://x.example"}));
}

#[test]
fn named_templates_do_not_leak_across_prefix_overlapping_names() {
    // M17 (#146): template `users` must NOT absorb `users_api`'s vars just
    // because `FAUCET_SOURCES_USERS_` is a prefix of `FAUCET_SOURCES_USERS_API_`.
    let e = env(&[
        ("FAUCET_SOURCES_USERS_TYPE", "rest"),
        ("FAUCET_SOURCES_USERS_BASE_URL", "https://u"),
        ("FAUCET_SOURCES_USERS_API_TYPE", "rest"),
        ("FAUCET_SOURCES_USERS_API_BASE_URL", "https://api"),
        ("FAUCET_SOURCES_USERS_API_TIMEOUT", "30"),
    ]);
    let out = build_named_sources(&e).unwrap();

    let users = out.get("users").expect("users template").config.clone();
    let users = users.as_object().unwrap();
    assert_eq!(
        users.get("base_url").and_then(|v| v.as_str()),
        Some("https://u")
    );
    assert!(
        !users.contains_key("api_base_url"),
        "users must not absorb users_api's vars"
    );
    assert!(!users.contains_key("api_timeout"));

    let api = out
        .get("users_api")
        .expect("users_api template")
        .config
        .clone();
    let api = api.as_object().unwrap();
    assert_eq!(
        api.get("base_url").and_then(|v| v.as_str()),
        Some("https://api")
    );
    assert_eq!(api.get("timeout").and_then(|v| v.as_i64()), Some(30));
}

#[test]
fn extract_scope_coerces_numbers_and_bools() {
    let e = env(&[
        ("FAUCET_SOURCE_REST_TIMEOUT_SECS", "30"),
        ("FAUCET_SOURCE_REST_FOLLOW_REDIRECTS", "true"),
        ("FAUCET_SOURCE_REST_BASE_URL", "https://x.example"),
    ]);
    let v = extract_scope(&e, "FAUCET_SOURCE_REST_").unwrap();
    assert_eq!(v["timeout_secs"], json!(30));
    assert_eq!(v["follow_redirects"], json!(true));
    assert_eq!(v["base_url"], json!("https://x.example"));
}

#[test]
fn extract_scope_handles_json_suffix() {
    let e = env(&[(
        "FAUCET_SOURCE_REST_AUTH_JSON",
        r#"{"type":"ApiKey","header":"Authorization","value":"Bearer x"}"#,
    )]);
    let v = extract_scope(&e, "FAUCET_SOURCE_REST_").unwrap();
    assert_eq!(
        v["auth"],
        json!({"type": "ApiKey", "header": "Authorization", "value": "Bearer x"})
    );
}

#[test]
fn extract_scope_rejects_invalid_json_suffix() {
    let e = env(&[("FAUCET_SOURCE_REST_AUTH_JSON", "not-json")]);
    let err = extract_scope(&e, "FAUCET_SOURCE_REST_").unwrap_err();
    match err {
        CliError::InvalidEnvJson { var, .. } => {
            assert_eq!(var, "FAUCET_SOURCE_REST_AUTH_JSON")
        }
        other => panic!("expected InvalidEnvJson, got {other:?}"),
    }
}

#[test]
fn extract_scope_conflict_scalar_then_json() {
    let e = env(&[
        ("FAUCET_SOURCE_REST_AUTH", "bearer"),
        ("FAUCET_SOURCE_REST_AUTH_JSON", r#"{"type":"ApiKey"}"#),
    ]);
    let err = extract_scope(&e, "FAUCET_SOURCE_REST_").unwrap_err();
    match err {
        CliError::EnvConflict {
            field,
            scalar_var,
            json_var,
        } => {
            assert_eq!(field, "auth");
            assert_eq!(scalar_var, "FAUCET_SOURCE_REST_AUTH");
            assert_eq!(json_var, "FAUCET_SOURCE_REST_AUTH_JSON");
        }
        other => panic!("expected EnvConflict, got {other:?}"),
    }
}

#[test]
fn extract_scope_conflict_detection_is_order_independent() {
    // HashMap iteration order is randomized per instance via the random
    // hasher state; loop 50 times to exercise both ordering branches
    // (scalar-arriving-first AND json-arriving-first) statistically.
    for _ in 0..50 {
        let e = env(&[
            ("FAUCET_SOURCE_REST_AUTH", "bearer"),
            ("FAUCET_SOURCE_REST_AUTH_JSON", r#"{"type":"ApiKey"}"#),
        ]);
        let err = extract_scope(&e, "FAUCET_SOURCE_REST_").unwrap_err();
        match err {
            CliError::EnvConflict {
                field,
                scalar_var,
                json_var,
            } => {
                assert_eq!(field, "auth");
                assert_eq!(scalar_var, "FAUCET_SOURCE_REST_AUTH");
                assert_eq!(json_var, "FAUCET_SOURCE_REST_AUTH_JSON");
            }
            other => panic!("expected EnvConflict, got {other:?}"),
        }
    }
}

#[test]
fn extract_scope_skips_bare_prefix() {
    let e = env(&[("FAUCET_SOURCE_REST_", "ignored")]);
    let v = extract_scope(&e, "FAUCET_SOURCE_REST_").unwrap();
    assert_eq!(v, json!({}));
}

#[test]
fn extract_scope_empty_when_no_matches() {
    let e = env(&[("PATH", "/usr/bin")]);
    let v = extract_scope(&e, "FAUCET_SOURCE_REST_").unwrap();
    assert_eq!(v, json!({}));
}

#[test]
fn build_source_reads_selector_and_scope() {
    let e = env(&[
        ("FAUCET_SOURCE", "rest"),
        ("FAUCET_SOURCE_REST_BASE_URL", "https://x.example"),
        ("FAUCET_SOURCE_REST_TIMEOUT_SECS", "30"),
    ]);
    let spec = build_source(&e).unwrap();
    assert_eq!(spec.kind, "rest");
    assert_eq!(spec.config["base_url"], json!("https://x.example"));
    assert_eq!(spec.config["timeout_secs"], json!(30));
}

#[test]
fn build_source_uses_kind_scope_so_other_kinds_dont_leak() {
    let e = env(&[
        ("FAUCET_SOURCE", "csv"),
        ("FAUCET_SOURCE_CSV_PATH", "./in.csv"),
        ("FAUCET_SOURCE_REST_BASE_URL", "https://other.example"),
    ]);
    let spec = build_source(&e).unwrap();
    assert_eq!(spec.kind, "csv");
    assert_eq!(spec.config, json!({"path": "./in.csv"}));
}

#[test]
fn build_source_errors_when_selector_missing() {
    let e = env(&[("FAUCET_SOURCE_REST_BASE_URL", "https://x.example")]);
    let err = build_source(&e).unwrap_err();
    match err {
        CliError::MissingEnvSelector { var } => assert_eq!(var, "FAUCET_SOURCE"),
        other => panic!("expected MissingEnvSelector, got {other:?}"),
    }
}

#[test]
fn build_source_errors_when_selector_empty() {
    let e = env(&[("FAUCET_SOURCE", "")]);
    let err = build_source(&e).unwrap_err();
    match err {
        CliError::MissingEnvSelector { var } => assert_eq!(var, "FAUCET_SOURCE"),
        other => panic!("expected MissingEnvSelector, got {other:?}"),
    }
}

#[test]
fn build_sink_reads_selector_and_scope() {
    let e = env(&[
        ("FAUCET_SINK", "jsonl"),
        ("FAUCET_SINK_JSONL_PATH", "./out.jsonl"),
    ]);
    let spec = build_sink(&e).unwrap();
    assert_eq!(spec.kind, "jsonl");
    assert_eq!(spec.config, json!({"path": "./out.jsonl"}));
}

#[test]
fn build_sink_errors_when_selector_missing() {
    let e = env(&[("FAUCET_SINK_JSONL_PATH", "./out.jsonl")]);
    let err = build_sink(&e).unwrap_err();
    match err {
        CliError::MissingEnvSelector { var } => assert_eq!(var, "FAUCET_SINK"),
        other => panic!("expected MissingEnvSelector, got {other:?}"),
    }
}

#[test]
fn build_sink_errors_when_selector_empty() {
    let e = env(&[("FAUCET_SINK", "")]);
    let err = build_sink(&e).unwrap_err();
    match err {
        CliError::MissingEnvSelector { var } => assert_eq!(var, "FAUCET_SINK"),
        other => panic!("expected MissingEnvSelector, got {other:?}"),
    }
}

#[test]
fn build_state_returns_none_when_unset() {
    let e = env(&[("FAUCET_SOURCE", "rest")]);
    let spec = build_state(&e).unwrap();
    assert!(spec.is_none());
}

#[test]
fn build_state_returns_none_when_empty() {
    let e = env(&[("FAUCET_STATE", "")]);
    let spec = build_state(&e).unwrap();
    assert!(spec.is_none());
}

#[test]
fn build_state_reads_file_backend() {
    let e = env(&[
        ("FAUCET_STATE", "file"),
        ("FAUCET_STATE_FILE_PATH", "./.faucet-state"),
    ]);
    let spec = build_state(&e).unwrap().unwrap();
    assert_eq!(spec.kind, "file");
    assert_eq!(spec.config, json!({"path": "./.faucet-state"}));
}

#[test]
fn build_state_reads_memory_with_empty_scope() {
    let e = env(&[("FAUCET_STATE", "memory")]);
    let spec = build_state(&e).unwrap().unwrap();
    assert_eq!(spec.kind, "memory");
    assert_eq!(spec.config, json!({}));
}

#[test]
fn build_transforms_empty_when_unset() {
    let e = env(&[("FAUCET_SOURCE", "rest")]);
    let t = build_transforms(&e).unwrap();
    assert!(t.is_empty());
}

#[test]
fn build_transforms_single_kind_no_config() {
    let e = env(&[("FAUCET_TRANSFORM_1", "snake_case")]);
    let t = build_transforms(&e).unwrap();
    assert_eq!(t.len(), 1);
    assert_eq!(t[0].kind, "snake_case");
    assert_eq!(t[0].config, json!({}));
}

#[test]
fn build_transforms_ordered_and_with_config() {
    let e = env(&[
        ("FAUCET_TRANSFORM_1", "snake_case"),
        ("FAUCET_TRANSFORM_2", "flatten"),
        ("FAUCET_TRANSFORM_2_SEPARATOR", "__"),
    ]);
    let t = build_transforms(&e).unwrap();
    assert_eq!(t.len(), 2);
    assert_eq!(t[0].kind, "snake_case");
    assert_eq!(t[1].kind, "flatten");
    assert_eq!(t[1].config, json!({"separator": "__"}));
}

#[test]
fn build_transforms_handles_double_digit_indices() {
    let e = env(&[
        ("FAUCET_TRANSFORM_1", "snake_case"),
        ("FAUCET_TRANSFORM_2", "flatten"),
        ("FAUCET_TRANSFORM_3", "rename_keys"),
    ]);
    let t = build_transforms(&e).unwrap();
    assert_eq!(t.len(), 3);
    assert_eq!(t[2].kind, "rename_keys");
}

#[test]
fn build_transforms_gap_errors() {
    let e = env(&[
        ("FAUCET_TRANSFORM_1", "snake_case"),
        ("FAUCET_TRANSFORM_3", "flatten"),
    ]);
    let err = build_transforms(&e).unwrap_err();
    match err {
        CliError::TransformIndexGap { missing } => assert_eq!(missing, 2),
        other => panic!("expected TransformIndexGap, got {other:?}"),
    }
}

#[test]
fn build_transforms_must_start_at_one() {
    let e = env(&[("FAUCET_TRANSFORM_2", "snake_case")]);
    let err = build_transforms(&e).unwrap_err();
    match err {
        CliError::TransformIndexGap { missing } => assert_eq!(missing, 1),
        other => panic!("expected TransformIndexGap, got {other:?}"),
    }
}

#[test]
fn build_transforms_ignores_field_vars_when_indexing_kinds() {
    // FAUCET_TRANSFORM_1_SEPARATOR should NOT be mistaken for a kind at index 1.
    let e = env(&[("FAUCET_TRANSFORM_1_SEPARATOR", "__")]);
    // No FAUCET_TRANSFORM_1 selector means no transforms at all (not a gap error,
    // because the indices set is empty).
    let t = build_transforms(&e).unwrap();
    assert!(t.is_empty());
}

#[test]
fn picks_up_named_source_templates() {
    let e = env(&[
        // Legacy default
        ("FAUCET_SOURCE", "rest"),
        ("FAUCET_SOURCE_REST_BASE_URL", "https://default.example"),
        ("FAUCET_SINK", "jsonl"),
        ("FAUCET_SINK_JSONL_PATH", "./o.jsonl"),
        // Named templates
        ("FAUCET_SOURCES_USERS_API_TYPE", "rest"),
        ("FAUCET_SOURCES_USERS_API_BASE_URL", "https://users.example"),
        ("FAUCET_SOURCES_POSTS_API_TYPE", "rest"),
        ("FAUCET_SOURCES_POSTS_API_BASE_URL", "https://posts.example"),
        ("FAUCET_SINKS_ARCHIVE_TYPE", "jsonl"),
        ("FAUCET_SINKS_ARCHIVE_PATH", "./archive.jsonl"),
    ]);
    let cfg = build_pipeline_config(&e).unwrap();
    assert!(cfg.pipeline.source.is_some());
    assert_eq!(cfg.pipeline.sources.len(), 2);
    assert_eq!(cfg.pipeline.sources["users_api"].kind, "rest");
    assert_eq!(
        cfg.pipeline.sources["users_api"].config["base_url"],
        "https://users.example"
    );
    assert_eq!(cfg.pipeline.sinks["archive"].kind, "jsonl");
}

/// The env-built config goes through the file-load tail: `${vars.*}`
/// resolves, and a secrets directive is never handed to a connector
/// verbatim (#789 CLI-139).
#[tokio::test]
async fn the_env_built_config_gets_the_load_tail() {
    let built = build_pipeline_config(&env(&[
        ("FAUCET_SOURCE", "rest"),
        ("FAUCET_SOURCE_REST_BASE_URL", "${vars.api_base}"),
        ("FAUCET_SINK", "jsonl"),
        ("FAUCET_SINK_JSONL_PATH", "./o.jsonl"),
        ("FAUCET_VARS_API_BASE", "https://api.example.com"),
    ]))
    .unwrap();
    let cfg = PipelineConfig::finish_built(built, std::path::Path::new("<env>"))
        .await
        .unwrap();
    assert_eq!(
        cfg.pipeline.source.unwrap().config["base_url"],
        json!("https://api.example.com")
    );
    let secret = build_pipeline_config(&env(&[
        ("FAUCET_SOURCE", "rest"),
        ("FAUCET_SOURCE_REST_BASE_URL", "https://x"),
        (
            "FAUCET_SOURCE_REST_HEADERS_JSON",
            r#"{"x-key": "${vault:secret/data/api#key}"}"#,
        ),
        ("FAUCET_SINK", "jsonl"),
        ("FAUCET_SINK_JSONL_PATH", "./o.jsonl"),
    ]))
    .unwrap();
    assert!(
        PipelineConfig::finish_built(secret, std::path::Path::new("<env>"))
            .await
            .is_err(),
        "an unresolvable directive is refused, never sent as the credential"
    );
}

#[test]
fn picks_up_vars_block() {
    let e = env(&[
        ("FAUCET_SOURCE", "rest"),
        ("FAUCET_SOURCE_REST_BASE_URL", "https://x.example"),
        ("FAUCET_SINK", "jsonl"),
        ("FAUCET_SINK_JSONL_PATH", "./o.jsonl"),
        ("FAUCET_VARS_API_BASE", "https://api.example.com"),
        ("FAUCET_VARS_REGION", "us-east-1"),
    ]);
    let cfg = build_pipeline_config(&e).unwrap();
    let vars = cfg.vars.unwrap();
    assert_eq!(vars["api_base"], "https://api.example.com");
    assert_eq!(vars["region"], "us-east-1");
}

#[test]
fn named_source_only_no_legacy_works() {
    // Verifies: with no FAUCET_SOURCE / FAUCET_SINK but only named
    // templates, the singular source/sink are None and the catalogs are
    // populated. (No MissingEnvSelector.)
    let e = env(&[
        ("FAUCET_SOURCES_USERS_API_TYPE", "rest"),
        ("FAUCET_SOURCES_USERS_API_BASE_URL", "https://x.example"),
        ("FAUCET_SINKS_ARCHIVE_TYPE", "jsonl"),
        ("FAUCET_SINKS_ARCHIVE_PATH", "./o.jsonl"),
    ]);
    let cfg = build_pipeline_config(&e).unwrap();
    assert!(cfg.pipeline.source.is_none());
    assert!(cfg.pipeline.sink.is_none());
    assert_eq!(cfg.pipeline.sources["users_api"].kind, "rest");
    assert_eq!(cfg.pipeline.sinks["archive"].kind, "jsonl");
}

#[test]
fn no_source_anywhere_errors() {
    // Neither legacy nor named sources — still must error.
    let e = env(&[
        ("FAUCET_SINK", "jsonl"),
        ("FAUCET_SINK_JSONL_PATH", "./o.jsonl"),
    ]);
    let err = build_pipeline_config(&e).unwrap_err();
    assert!(matches!(err, CliError::MissingEnvSelector { .. }));
}

#[test]
fn build_pipeline_config_minimal_csv_to_jsonl() {
    let e = env(&[
        ("FAUCET_SOURCE", "csv"),
        ("FAUCET_SOURCE_CSV_PATH", "./in.csv"),
        ("FAUCET_SINK", "jsonl"),
        ("FAUCET_SINK_JSONL_PATH", "./out.jsonl"),
    ]);
    let cfg = build_pipeline_config(&e).unwrap();
    assert_eq!(cfg.version, 1);
    assert_eq!(cfg.pipeline.source.as_ref().unwrap().kind, "csv");
    assert_eq!(
        cfg.pipeline.source.as_ref().unwrap().config,
        json!({"path": "./in.csv"})
    );
    assert_eq!(cfg.pipeline.sink.as_ref().unwrap().kind, "jsonl");
    assert_eq!(
        cfg.pipeline.sink.as_ref().unwrap().config,
        json!({"path": "./out.jsonl"})
    );
    assert!(cfg.pipeline.transforms.is_empty());
    assert!(cfg.pipeline.state.is_none());
    assert!(cfg.name.is_none());
}

#[test]
fn build_pipeline_config_uses_faucet_name_when_set() {
    let e = env(&[
        ("FAUCET_NAME", "github-issues"),
        ("FAUCET_SOURCE", "csv"),
        ("FAUCET_SOURCE_CSV_PATH", "./in.csv"),
        ("FAUCET_SINK", "jsonl"),
        ("FAUCET_SINK_JSONL_PATH", "./out.jsonl"),
    ]);
    let cfg = build_pipeline_config(&e).unwrap();
    assert_eq!(cfg.name.as_deref(), Some("github-issues"));
}

#[test]
fn build_pipeline_config_treats_empty_name_as_none() {
    let e = env(&[
        ("FAUCET_NAME", ""),
        ("FAUCET_SOURCE", "csv"),
        ("FAUCET_SOURCE_CSV_PATH", "./in.csv"),
        ("FAUCET_SINK", "jsonl"),
        ("FAUCET_SINK_JSONL_PATH", "./out.jsonl"),
    ]);
    let cfg = build_pipeline_config(&e).unwrap();
    assert!(cfg.name.is_none());
}

#[test]
fn build_pipeline_config_with_state_and_transforms() {
    let e = env(&[
        ("FAUCET_SOURCE", "csv"),
        ("FAUCET_SOURCE_CSV_PATH", "./in.csv"),
        ("FAUCET_SINK", "jsonl"),
        ("FAUCET_SINK_JSONL_PATH", "./out.jsonl"),
        ("FAUCET_STATE", "file"),
        ("FAUCET_STATE_FILE_PATH", "./.faucet-state"),
        ("FAUCET_TRANSFORM_1", "snake_case"),
        ("FAUCET_TRANSFORM_2", "flatten"),
        ("FAUCET_TRANSFORM_2_SEPARATOR", "__"),
    ]);
    let cfg = build_pipeline_config(&e).unwrap();
    assert_eq!(cfg.pipeline.transforms.len(), 2);
    assert_eq!(cfg.pipeline.state.as_ref().unwrap().kind, "file");
}

#[test]
fn build_pipeline_config_missing_source_errors() {
    let e = env(&[
        ("FAUCET_SINK", "jsonl"),
        ("FAUCET_SINK_JSONL_PATH", "./out.jsonl"),
    ]);
    let err = build_pipeline_config(&e).unwrap_err();
    assert!(matches!(err, CliError::MissingEnvSelector { .. }));
}

#[test]
fn build_pipeline_config_missing_sink_errors() {
    let e = env(&[
        ("FAUCET_SOURCE", "csv"),
        ("FAUCET_SOURCE_CSV_PATH", "./in.csv"),
    ]);
    let err = build_pipeline_config(&e).unwrap_err();
    assert!(matches!(err, CliError::MissingEnvSelector { .. }));
}
