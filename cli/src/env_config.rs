//! Build a `PipelineConfig` from a snapshot of `FAUCET_*` environment variables.
//!
//! The public surface is intentionally split between pure functions (taking a
//! `HashMap<String, String>` env snapshot — fully testable) and a thin shell
//! `from_process_env()` that captures `std::env::vars()`.
//!
//! See `cli/README.md` and issue #42 for the user-facing variable schema.

use crate::config::{ConnectorSpec, PipelineConfig, PipelineSpec, StateStoreSpec, TransformSpec};
use crate::error::{CliError, CliResult};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, HashMap};

/// Walk every env var starting with `prefix`, strip the prefix, lowercase the
/// remainder into a field name, apply the `_JSON` precedence rule, and assemble
/// a `Value::Object`. Returns the object verbatim — no shape validation; the
/// connector's own `Deserialize` impl is the gate.
///
/// The `_JSON` suffix is the escape hatch for nested / tagged-enum fields
/// (auth, pagination, etc.) that don't flatten cleanly into env-var names.
pub fn extract_scope(env: &HashMap<String, String>, prefix: &str) -> CliResult<Value> {
    let mut object: Map<String, Value> = Map::new();
    // Track which env var supplied each field so a conflict error can name both.
    let mut json_fields: HashMap<String, String> = HashMap::new();
    let mut scalar_fields: HashMap<String, String> = HashMap::new();

    for (key, value) in env {
        let Some(suffix) = key.strip_prefix(prefix) else {
            continue;
        };
        if suffix.is_empty() {
            // Bare prefix (e.g. exactly "FAUCET_SOURCE_REST_") — skip, no field.
            continue;
        }
        let lowercase = suffix.to_ascii_lowercase();
        if let Some(field) = lowercase.strip_suffix("_json") {
            if let Some(scalar_var) = scalar_fields.get(field) {
                return Err(CliError::EnvConflict {
                    field: field.to_owned(),
                    scalar_var: scalar_var.clone(),
                    json_var: key.clone(),
                });
            }
            let parsed: Value =
                serde_json::from_str(value).map_err(|e| CliError::InvalidEnvJson {
                    var: key.clone(),
                    message: e.to_string(),
                })?;
            object.insert(field.to_owned(), parsed);
            json_fields.insert(field.to_owned(), key.clone());
        } else {
            if let Some(json_var) = json_fields.get(&lowercase) {
                return Err(CliError::EnvConflict {
                    field: lowercase.clone(),
                    scalar_var: key.clone(),
                    json_var: json_var.clone(),
                });
            }
            object.insert(lowercase.clone(), coerce_scalar(value));
            scalar_fields.insert(lowercase, key.clone());
        }
    }
    Ok(Value::Object(object))
}

/// Try to parse `s` as a JSON value (numbers, bools, null, strings, objects,
/// arrays). Falls back to a plain `Value::String` if the parse fails. This
/// matches YAML's auto-typing so `30` becomes a number and `true` becomes a
/// bool — the connector's `Deserialize` impl gets exactly what it would have
/// gotten via YAML.
///
/// **Caveat:** a value that *looks* like JSON is auto-typed, so the literal
/// strings `"true"` / `"false"` / `"null"` / `"123"` / `"1.5"` become a bool /
/// null / number, not a string. When a connector field must stay a string with
/// one of those values (e.g. an API key that is all digits, or a literal
/// `"null"` token), set it via the `*_JSON` variant with the value quoted —
/// e.g. `FAUCET_SOURCE_REST_TOKEN_JSON='"0123"'` — which bypasses this
/// coercion (#78 LOW).
fn coerce_scalar(s: &str) -> Value {
    serde_json::from_str::<Value>(s).unwrap_or_else(|_| Value::String(s.to_owned()))
}

/// Construct the source [`ConnectorSpec`] from `FAUCET_SOURCE` + `FAUCET_SOURCE_<KIND>_*`.
pub fn build_source(env: &HashMap<String, String>) -> CliResult<ConnectorSpec> {
    let kind = env
        .get("FAUCET_SOURCE")
        .filter(|v| !v.is_empty())
        .ok_or_else(|| CliError::MissingEnvSelector {
            var: "FAUCET_SOURCE".to_owned(),
        })?
        .clone();
    let prefix = format!(
        "FAUCET_SOURCE_{}_",
        kind.to_ascii_uppercase().replace('-', "_")
    );
    let config = extract_scope(env, &prefix)?;
    Ok(ConnectorSpec {
        kind,
        config,
        transforms: None,
        inherit_transforms: true,
        status: None,
        tags: Vec::new(),
        complete_for: None,
    })
}

/// Construct the sink [`ConnectorSpec`] from `FAUCET_SINK` + `FAUCET_SINK_<KIND>_*`.
pub fn build_sink(env: &HashMap<String, String>) -> CliResult<ConnectorSpec> {
    let kind = env
        .get("FAUCET_SINK")
        .filter(|v| !v.is_empty())
        .ok_or_else(|| CliError::MissingEnvSelector {
            var: "FAUCET_SINK".to_owned(),
        })?
        .clone();
    let prefix = format!(
        "FAUCET_SINK_{}_",
        kind.to_ascii_uppercase().replace('-', "_")
    );
    let config = extract_scope(env, &prefix)?;
    Ok(ConnectorSpec {
        kind,
        config,
        transforms: None,
        inherit_transforms: true,
        status: None,
        tags: Vec::new(),
        complete_for: None,
    })
}

/// Construct an optional [`StateStoreSpec`] from `FAUCET_STATE` + `FAUCET_STATE_<KIND>_*`.
/// Returns `Ok(None)` when `FAUCET_STATE` is unset or empty (the common case).
pub fn build_state(env: &HashMap<String, String>) -> CliResult<Option<StateStoreSpec>> {
    let Some(kind) = env.get("FAUCET_STATE").filter(|v| !v.is_empty()).cloned() else {
        return Ok(None);
    };
    let prefix = format!(
        "FAUCET_STATE_{}_",
        kind.to_ascii_uppercase().replace('-', "_")
    );
    let config = extract_scope(env, &prefix)?;
    Ok(Some(StateStoreSpec { kind, config }))
}

/// Construct an ordered `Vec<TransformSpec>` from `FAUCET_TRANSFORM_<N>` selectors
/// and `FAUCET_TRANSFORM_<N>_<FIELD>` config fields. Indices must be contiguous
/// starting at 1; any gap is an error so a misnumbered var never silently drops
/// a transform.
pub fn build_transforms(env: &HashMap<String, String>) -> CliResult<Vec<TransformSpec>> {
    // Collect the kind selectors first — keys that are exactly
    // `FAUCET_TRANSFORM_<digits>` (no trailing field name).
    let mut kinds: BTreeMap<u32, String> = BTreeMap::new();
    for (key, value) in env {
        let Some(rest) = key.strip_prefix("FAUCET_TRANSFORM_") else {
            continue;
        };
        if rest.is_empty() || !rest.chars().all(|c| c.is_ascii_digit()) {
            continue;
        }
        let Ok(idx) = rest.parse::<u32>() else {
            continue;
        };
        kinds.insert(idx, value.clone());
    }
    if kinds.is_empty() {
        return Ok(Vec::new());
    }
    // Indices must be 1, 2, 3, …
    for (expected, actual) in (1u32..).zip(kinds.keys().copied()) {
        if expected != actual {
            return Err(CliError::TransformIndexGap { missing: expected });
        }
    }
    // Harvest per-transform config blocks.
    let mut out = Vec::with_capacity(kinds.len());
    for (idx, kind) in kinds {
        let prefix = format!("FAUCET_TRANSFORM_{idx}_");
        let config = extract_scope(env, &prefix)?;
        out.push(TransformSpec { kind, config });
    }
    Ok(out)
}

/// Harvest named source templates from `FAUCET_SOURCES_<NAME>_TYPE` selectors
/// and `FAUCET_SOURCES_<NAME>_<FIELD>` config fields. `<NAME>` is lowercased.
pub fn build_named_sources(
    env: &HashMap<String, String>,
) -> CliResult<HashMap<String, ConnectorSpec>> {
    build_named_catalog(env, "FAUCET_SOURCES_")
}

/// Same as [`build_named_sources`] but for sinks via `FAUCET_SINKS_<NAME>_*`.
pub fn build_named_sinks(
    env: &HashMap<String, String>,
) -> CliResult<HashMap<String, ConnectorSpec>> {
    build_named_catalog(env, "FAUCET_SINKS_")
}

fn build_named_catalog(
    env: &HashMap<String, String>,
    prefix: &str,
) -> CliResult<HashMap<String, ConnectorSpec>> {
    // First sweep: find each template's `<NAME>` by spotting
    // `<prefix><NAME>_TYPE`.
    let mut kinds: HashMap<String, String> = HashMap::new();
    for (key, value) in env {
        let Some(suffix) = key.strip_prefix(prefix) else {
            continue;
        };
        let Some(name_upper) = suffix.strip_suffix("_TYPE") else {
            continue;
        };
        if name_upper.is_empty() {
            continue;
        }
        kinds.insert(name_upper.to_ascii_lowercase(), value.clone());
    }
    // All template scope prefixes, so each env var can be assigned to its
    // LONGEST matching prefix. Without this, a template like `users` would
    // absorb `users_api`'s vars, since `FAUCET_SOURCES_USERS_` is a prefix of
    // `FAUCET_SOURCES_USERS_API_` — the shorter template silently gets the
    // longer one's fields (#146 M17).
    let scope_prefixes: Vec<String> = kinds
        .keys()
        .map(|name| format!("{prefix}{}_", name.to_ascii_uppercase()))
        .collect();

    // Second sweep: harvest each template's config block.
    let mut out: HashMap<String, ConnectorSpec> = HashMap::new();
    for (name, kind) in kinds {
        let scope_prefix = format!("{prefix}{}_", name.to_ascii_uppercase());
        // A view of `env` containing only the vars whose longest matching
        // template prefix is THIS template's — i.e. drop any var that also
        // matches a longer sibling prefix nested under this one.
        let scoped: HashMap<String, String> = env
            .iter()
            .filter(|(k, _)| {
                k.starts_with(&scope_prefix)
                    && !scope_prefixes.iter().any(|other| {
                        other.len() > scope_prefix.len() && k.starts_with(other.as_str())
                    })
            })
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let mut config = extract_scope(&scoped, &scope_prefix)?;
        // Remove the `type` field — it's the selector, not a config field.
        if let Value::Object(m) = &mut config {
            m.remove("type");
        }
        out.insert(
            name,
            ConnectorSpec {
                kind,
                config,
                transforms: None,
                inherit_transforms: true,
                status: None,
                tags: Vec::new(),
                complete_for: None,
            },
        );
    }
    Ok(out)
}

/// Harvest `FAUCET_VARS_<KEY>` into the top-level vars map. Returns `None`
/// when no `FAUCET_VARS_*` variables are set.
pub fn build_vars(env: &HashMap<String, String>) -> Option<HashMap<String, Value>> {
    let mut out: HashMap<String, Value> = HashMap::new();
    for (key, value) in env {
        let Some(name_upper) = key.strip_prefix("FAUCET_VARS_") else {
            continue;
        };
        if name_upper.is_empty() {
            continue;
        }
        out.insert(name_upper.to_ascii_lowercase(), coerce_scalar(value));
    }
    if out.is_empty() { None } else { Some(out) }
}

/// Construct a complete [`PipelineConfig`] from an env snapshot.
pub fn build_pipeline_config(env: &HashMap<String, String>) -> CliResult<PipelineConfig> {
    let source = match env.get("FAUCET_SOURCE").filter(|v| !v.is_empty()) {
        Some(_) => Some(build_source(env)?),
        None => None,
    };
    let sink = match env.get("FAUCET_SINK").filter(|v| !v.is_empty()) {
        Some(_) => Some(build_sink(env)?),
        None => None,
    };
    let sources = build_named_sources(env)?;
    let sinks = build_named_sinks(env)?;
    let state = build_state(env)?;
    let transforms = build_transforms(env)?;
    let vars = build_vars(env);
    let name = env.get("FAUCET_NAME").cloned().filter(|s| !s.is_empty());

    // At least one source and one sink must be declared somewhere.
    if source.is_none() && sources.is_empty() {
        return Err(CliError::MissingEnvSelector {
            var: "FAUCET_SOURCE (or FAUCET_SOURCES_<NAME>_TYPE)".to_owned(),
        });
    }
    if sink.is_none() && sinks.is_empty() {
        return Err(CliError::MissingEnvSelector {
            var: "FAUCET_SINK (or FAUCET_SINKS_<NAME>_TYPE)".to_owned(),
        });
    }
    Ok(PipelineConfig {
        kind: None,
        version: 1,
        name,
        vars,
        // Pure-env mode has no `params:` surface: every value already comes from
        // the environment, which is what params would be overriding.
        params: Default::default(),
        // Pure-env mode doesn't (yet) assemble a shared `auth:` catalog; inline
        // auth via FAUCET_*_AUTH_JSON still works.
        auth: None,
        pipeline: PipelineSpec {
            source,
            sink,
            sources,
            sinks,
            transforms,
            state,
            dlq: None,
            #[cfg(feature = "quality")]
            quality: None,
            #[cfg(feature = "contract")]
            contract: None,
            #[cfg(feature = "masking")]
            masking: None,
            schema: None,
            // Pure-env mode does not assemble a topology graph.
            nodes: std::collections::HashMap::new(),
            edges: Vec::new(),
        },
        matrix: Vec::new(),
        execution: None,
        selection: None,
        observability: None,
        delivery: faucet_core::DeliveryMode::default(),
        resilience: None,
        // Pure-env mode doesn't (yet) assemble an `sla:` block.
        sla: None,
        reconcile: None,
        verify: None,
        rollback: None,
        shard: None,
        replication: None,
        backfill: None,
        metadata_columns: None,
        // Pure-env mode doesn't assemble a `local_outputs:` block; outputs are
        // still tracked (the default) under the runtime's own retention window.
        #[cfg(feature = "catalog")]
        local_outputs: None,
        partition: None,
        #[cfg(feature = "schedule")]
        schedule: None,
        #[cfg(feature = "lineage")]
        lineage: None,
        // Pure-env mode doesn't (yet) assemble a `catalog:` block.
        #[cfg(feature = "catalog")]
        catalog: None,
        #[cfg(feature = "notify")]
        notifications: Vec::new(),
    })
}

/// Snapshot `std::env::vars()` and call [`build_pipeline_config`].
pub fn from_process_env() -> CliResult<PipelineConfig> {
    let env: HashMap<String, String> = std::env::vars().collect();
    build_pipeline_config(&env)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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
}
