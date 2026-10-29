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
        attributes: Default::default(),
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
        attributes: Default::default(),
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
    build_named_catalog(env, "FAUCET_SOURCES_", &crate::registry::source_kinds())
}

/// Same as [`build_named_sources`] but for sinks via `FAUCET_SINKS_<NAME>_*`.
pub fn build_named_sinks(
    env: &HashMap<String, String>,
) -> CliResult<HashMap<String, ConnectorSpec>> {
    build_named_catalog(env, "FAUCET_SINKS_", &crate::registry::sink_kinds())
}

fn build_named_catalog(
    env: &HashMap<String, String>,
    prefix: &str,
    known_kinds: &[&str],
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
    // A `*_TYPE` var nested under another template whose value is no
    // connector kind is that template's field (`PG_SLOT_TYPE=temporary` is
    // `pg`'s `slot_type`), not a template of its own (#789 CLI-138).
    let candidates: Vec<String> = kinds.keys().cloned().collect();
    kinds.retain(|name, kind| {
        known_kinds.contains(&kind.as_str())
            || !candidates
                .iter()
                .any(|other| other != name && name.starts_with(&format!("{other}_")))
    });
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
                attributes: Default::default(),
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
        requires_faucet: None,
        version: 1,
        name,
        vars,
        // Pure-env mode has no `params:` surface: every value already comes from
        // the environment, which is what params would be overriding.
        params: Default::default(),
        tests: None,
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
        profiling: None,
        #[cfg(feature = "policy")]
        policy: None,
        reconcile: None,
        verify: None,
        rollback: None,
        usage: None,
        budget: None,
        pool: None,
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

/// [`from_process_env`] followed by the load tail every file load runs:
/// `${vars.*}` / template references and secrets-manager directives are
/// resolved, so neither reaches a connector verbatim (#789 CLI-139).
pub async fn load_from_process_env() -> CliResult<PipelineConfig> {
    PipelineConfig::finish_built(from_process_env()?, std::path::Path::new("<environment>")).await
}

/// Snapshot the process environment and call [`build_pipeline_config`].
/// Variables that are not UTF-8 are skipped (a `FAUCET_*` one is an error),
/// and secret-looking `FAUCET_*` values are registered for log redaction.
pub fn from_process_env() -> CliResult<PipelineConfig> {
    let env = utf8_env(std::env::vars_os())?;
    register_secret_values(&env);
    build_pipeline_config(&env)
}

fn utf8_env(
    vars: impl IntoIterator<Item = (std::ffi::OsString, std::ffi::OsString)>,
) -> CliResult<HashMap<String, String>> {
    let mut env = HashMap::new();
    for (k, v) in vars {
        match (k.into_string(), v.into_string()) {
            (Ok(k), Ok(v)) => {
                env.insert(k, v);
            }
            (Ok(k), Err(_)) if k.starts_with("FAUCET_") => {
                return Err(CliError::Config(format!(
                    "environment variable {k} is not valid UTF-8"
                )));
            }
            _ => {}
        }
    }
    Ok(env)
}

/// Words that mark a `FAUCET_*` variable as holding a credential.
const SECRET_NAME_PARTS: &[&str] = &[
    "PASSWORD",
    "PASSWD",
    "SECRET",
    "TOKEN",
    "KEY",
    "CREDENTIAL",
    "AUTH",
    "PRIVATE",
    "CONNECTION_STRING",
    "DSN",
];

fn register_secret_values(env: &HashMap<String, String>) {
    for (k, v) in env {
        if !k.starts_with("FAUCET_") {
            continue;
        }
        let secret_name = SECRET_NAME_PARTS.iter().any(|p| k.contains(p));
        let url_with_userinfo = k.contains("URL")
            && v.split_once("://")
                .is_some_and(|(_, rest)| rest.split(['/', '?']).next().unwrap_or("").contains('@'));
        if url_with_userinfo || secret_name {
            crate::secrets::registry::register(v);
        }
        // A credential is often echoed on its own (an auth error, a header
        // dump), so register the parts that hold one too.
        for part in embedded_secrets(k, v) {
            crate::secrets::registry::register(&part);
        }
    }
}

/// Words that make a key name a credential wherever they appear in its last
/// word (`password`, `db_password`, `client_secret`, `access_token`).
const SECRET_WORDS: &[&str] = &[
    "password",
    "passwd",
    "passphrase",
    "secret",
    "token",
    "credential",
];

/// Last words that name a credential on their own.
const SECRET_LAST_WORDS: &[&str] = &[
    "pwd",
    "auth",
    "authorization",
    "sig",
    "signature",
    "apikey",
    "accesskey",
    "secretkey",
    "privatekey",
];

/// Words that turn a trailing `key` into a credential (`api_key`,
/// `AccountKey`); any other `key` names a column (`partition_key`).
const KEY_QUALIFIERS: &[&str] = &[
    "api",
    "access",
    "secret",
    "private",
    "client",
    "account",
    "signing",
    "master",
    "encryption",
    "shared",
    "subscription",
    "app",
    "service",
    "storage",
    "sas",
    "auth",
    "hmac",
];

/// Lower-case words of a key name, split at non-alphanumerics and camelCase
/// boundaries (`x-api-key`, `AccountKey`, `partition_key`).
fn key_words(key: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut cur = String::new();
    let mut prev_lower = false;
    for c in key.chars() {
        if !c.is_ascii_alphanumeric() {
            if !cur.is_empty() {
                words.push(std::mem::take(&mut cur));
            }
            prev_lower = false;
            continue;
        }
        if c.is_ascii_uppercase() && prev_lower && !cur.is_empty() {
            words.push(std::mem::take(&mut cur));
        }
        prev_lower = c.is_ascii_lowercase() || c.is_ascii_digit();
        cur.push(c.to_ascii_lowercase());
    }
    if !cur.is_empty() {
        words.push(cur);
    }
    words
}

/// Whether a key name inside a JSON value or a URL query / connection string
/// names a credential. Matched by its last word, so `sort_key`, `key_field`,
/// `author` and `signature_field` name columns, not secrets.
fn is_secret_key(key: &str) -> bool {
    if key.trim().contains(char::is_whitespace) {
        return false;
    }
    let words = key_words(key);
    let Some(last) = words.last() else {
        return false;
    };
    if SECRET_WORDS.iter().any(|w| last.contains(w)) || SECRET_LAST_WORDS.contains(&last.as_str()) {
        return true;
    }
    matches!(last.as_str(), "key" | "keys")
        && words.len() >= 2
        && KEY_QUALIFIERS.contains(&words[words.len() - 2].as_str())
}

/// Name parts that mark a whole `*_JSON` value as a credential block (an
/// `auth` or `credentials` object), not just a value holding a key list.
const CREDENTIAL_BLOCK_PARTS: &[&str] = &[
    "AUTH",
    "CREDENTIAL",
    "SECRET",
    "PASSWORD",
    "PASSWD",
    "TOKEN",
    "PRIVATE",
];

/// The credentials embedded in one `FAUCET_*` value: every string leaf of a
/// credential-block `*_JSON` value (`*_AUTH_JSON`, `*_CREDENTIALS_JSON`, but
/// not a `type` discriminator), the leaves under secret-looking keys of any
/// other `*_JSON` value, a URL's userinfo password, and the values of
/// secret-looking `key=value` pairs in a query or connection string. Values
/// shorter than four characters are skipped. Pure.
fn embedded_secrets(name: &str, value: &str) -> Vec<String> {
    let mut out = Vec::new();
    if name.ends_with("_JSON")
        && let Ok(json) = serde_json::from_str::<Value>(value)
    {
        let block = CREDENTIAL_BLOCK_PARTS.iter().any(|p| name.contains(p));
        collect_json_secrets(&json, block, &mut out);
    }
    if let Ok(url) = url::Url::parse(value)
        && let Some(pw) = url.password()
    {
        out.push(pw.to_string());
        if let Ok(decoded) = urlencoding_decode(pw) {
            out.push(decoded);
        }
    }
    for pair in value.split(['?', '&', ';']) {
        if let Some((k, v)) = pair.split_once('=')
            && is_secret_key(k.trim())
        {
            let v = v.trim();
            out.push(v.to_string());
            if let Ok(decoded) = urlencoding_decode(v) {
                out.push(decoded);
            }
        }
    }
    out.retain(|s| s.len() >= 4);
    out.sort();
    out.dedup();
    out
}

fn collect_json_secrets(v: &Value, under_secret: bool, out: &mut Vec<String>) {
    match v {
        Value::String(s) if under_secret => out.push(s.clone()),
        Value::Array(a) => a
            .iter()
            .for_each(|x| collect_json_secrets(x, under_secret, out)),
        Value::Object(o) => o.iter().for_each(|(k, x)| {
            if k != "type" {
                collect_json_secrets(x, under_secret || is_secret_key(k), out)
            }
        }),
        _ => {}
    }
}

/// Percent-decode `s`; `Err` when it decodes to the same text or not UTF-8.
fn urlencoding_decode(s: &str) -> Result<String, ()> {
    let decoded: Vec<u8> = url::form_urlencoded::parse(format!("x={s}").as_bytes())
        .next()
        .map(|(_, v)| v.into_owned().into_bytes())
        .ok_or(())?;
    let decoded = String::from_utf8(decoded).map_err(|_| ())?;
    if decoded == s { Err(()) } else { Ok(decoded) }
}

#[cfg(test)]
mod tests;
