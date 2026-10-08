//! `faucet migrate` — upgrade a config written against an older `faucet`
//! grammar to the current shape (#388).
//!
//! Each migration is a **pure** `serde_json::Value → serde_json::Value`
//! transform with a before/after unit test. The command reads a config, applies
//! every rule, and (unless `--check`) rewrites it in place, printing which rules
//! fired. Migrations are **idempotent**: running `migrate` on an already-current
//! config changes nothing and exits 0.
//!
//! Two rules ship today:
//!
//! 1. **Top-level `source:` / `sink:` → `pipeline.source` / `pipeline.sink`**
//!    (the pre-#54 shape). If the document has top-level `source`/`sink` keys
//!    and no `pipeline`, they move under a new `pipeline:` map.
//! 2. **Legacy auth → adjacently-tagged `{ type, config }`** (the pre-#113
//!    shape). Any `auth:` / `credentials:` object carrying a `type` string plus
//!    sibling fields but no `config` has those siblings folded into `config`.
//!
//! Comments are not preserved (the config is parsed and re-serialized) — the
//! same limitation `faucet fmt` documents.

use crate::cli::MigrateArgs;
use crate::error::{CliError, CliResult};
use serde_json::{Map, Value};
use std::path::Path;

/// One stored key's migration (#736).
#[derive(Debug, Clone, serde::Serialize)]
pub struct StateKeyMigration {
    pub row: String,
    pub key: String,
    pub owner: String,
    /// `current`, `enveloped` (a pre-versioning value rewritten in the
    /// envelope), `migrated` (`from_schema` → `to_schema`), or `refused`.
    pub action: &'static str,
    pub from_schema: u32,
    pub to_schema: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// `faucet migrate --state`.
#[derive(Debug, Clone, serde::Serialize)]
pub struct StateMigrationReport {
    pub pipeline: String,
    pub check: bool,
    pub keys: Vec<StateKeyMigration>,
    /// Runs found holding a lease that `--force` overrode.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

impl StateMigrationReport {
    /// Keys that are not current (`--check` fails on them).
    pub fn pending(&self) -> usize {
        self.keys.iter().filter(|k| k.action != "current").count()
    }
}

/// Plan (and unless `check`, apply) the migration of every row's bookmark keys.
/// Refuses while a run holds a selected row's lease.
pub async fn migrate_state(
    target: &crate::pipeline_state::PipelineTarget,
    stores: &crate::pipeline_state::ops::Stores,
    row: Option<&str>,
    check: bool,
) -> CliResult<StateMigrationReport> {
    migrate_state_with(target, stores, row, check, false).await
}

/// [`migrate_state`], with `force` overriding a live run's lease. Each key is
/// re-read immediately before it is rewritten, so a bookmark a run persisted
/// after the listing is migrated rather than overwritten with the older value.
pub async fn migrate_state_with(
    target: &crate::pipeline_state::PipelineTarget,
    stores: &crate::pipeline_state::ops::Stores,
    row: Option<&str>,
    check: bool,
    force: bool,
) -> CliResult<StateMigrationReport> {
    use crate::pipeline_state::keys::KeyKind;
    use faucet_core::state_version::{StoredState, resolve_with};
    let rows = target.select(row)?;
    let entries = crate::pipeline_state::ops::collect_keys(target, stores).await?;
    let mut keys = Vec::new();
    let mut warnings = Vec::new();
    for r in rows {
        let codec = crate::pipeline_state::ops::row_codec(r);
        let kind = r.source.as_ref().map(|(k, _)| k.clone());
        let selected: Vec<&crate::pipeline_state::ops::KeyEntry> = entries
            .iter()
            .filter(|e| {
                e.key.kind == KeyKind::Bookmark && e.key.row.as_deref() == Some(r.id.as_str())
            })
            .collect();
        if !check && let Some(store) = stores.for_row(&r.id) {
            let mut bases = vec![target.base_key(&r.id)];
            bases.extend(selected.iter().filter_map(|e| e.key.base(&target.pipeline)));
            bases.dedup();
            warnings.extend(
                crate::pipeline_state::ops::refuse_if_running(
                    store.as_ref(),
                    target,
                    &bases,
                    crate::pipeline_state::ops::LeaseScope::Bases,
                    force,
                    chrono::Utc::now(),
                )
                .await?,
            );
        }
        for e in selected {
            let current = if check {
                Some(e.value.clone())
            } else {
                match stores.for_key(&e.key) {
                    Some(store) => store.get(&e.key.key).await?,
                    None => Some(e.value.clone()),
                }
            };
            let Some(value) = current else { continue };
            let stored = StoredState::parse(&value);
            let resolved = resolve_with(
                &e.key.key,
                &value,
                &codec.owner,
                codec.schema,
                |from, data| match &kind {
                    Some(k) => crate::registry::migrate_source_state(k, from, data),
                    None => Ok(data),
                },
            );
            let (action, detail, write) = match resolved {
                Err(err) => ("refused", Some(err.to_string()), None),
                Ok(res) if res.migrated_from.is_some() => ("migrated", None, Some(res.data)),
                Ok(res) if res.legacy => ("enveloped", None, Some(res.data)),
                Ok(_) => ("current", None, None),
            };
            if let (Some(data), false) = (&write, check) {
                let store = stores.for_key(&e.key).ok_or_else(|| {
                    CliError::Config(format!("no state store holds '{}'", e.key.key))
                })?;
                store.put(&e.key.key, &codec.encode(data)).await?;
            }
            keys.push(StateKeyMigration {
                row: r.id.clone(),
                key: e.key.key.clone(),
                owner: codec.owner.clone(),
                action,
                from_schema: stored.schema,
                to_schema: codec.schema,
                detail,
            });
        }
    }
    Ok(StateMigrationReport {
        pipeline: target.pipeline.clone(),
        check,
        keys,
        warnings,
    })
}

/// Human rendering of a state migration report.
pub fn render_state_report(r: &StateMigrationReport) -> String {
    let mut out = format!(
        "pipeline {} — {}\n",
        r.pipeline,
        if r.check {
            "state check"
        } else {
            "state migration"
        }
    );
    if r.keys.is_empty() {
        out.push_str("  no stored bookmarks\n");
    }
    for k in &r.keys {
        let what = match k.action {
            "current" => format!("current ({} schema {})", k.owner, k.to_schema),
            "enveloped" if r.check => {
                "stored before versioning — would be rewritten in the envelope".to_string()
            }
            "enveloped" => "rewritten in the versioned envelope".to_string(),
            "migrated" if r.check => format!(
                "needs migration: {} schema {} → {}",
                k.owner, k.from_schema, k.to_schema
            ),
            "migrated" => format!(
                "migrated: {} schema {} → {}",
                k.owner, k.from_schema, k.to_schema
            ),
            _ => format!("REFUSED — {}", k.detail.as_deref().unwrap_or("")),
        };
        out.push_str(&format!("  {:<14} {}  {}\n", k.row, k.key, what));
    }
    out
}

async fn run_state(args: &MigrateArgs) -> CliResult<()> {
    let load = crate::cli::StateLoadArgs {
        json: args.json,
        env_file: args.env_file.clone(),
        no_env_file: args.no_env_file,
        profile: args.profile.clone(),
    };
    let (_, target, _) = crate::commands::state::load(args.config.as_deref(), &load).await?;
    let stores = crate::pipeline_state::ops::Stores::build(&target, None).await?;
    let report = migrate_state_with(
        &target,
        &stores,
        args.row.as_deref(),
        args.check,
        args.force,
    )
    .await?;
    for w in &report.warnings {
        eprintln!("warning: {w}");
    }
    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|e| CliError::Internal(e.to_string()))?
        );
    } else {
        print!("{}", render_state_report(&report));
    }
    let refused = report.keys.iter().filter(|k| k.action == "refused").count();
    if refused > 0 {
        return Err(CliError::Config(format!(
            "{refused} stored bookmark(s) cannot be read by this release — see above"
        )));
    }
    if args.check && report.pending() > 0 {
        return Err(CliError::Config(format!(
            "{} stored bookmark(s) are not current; run `faucet migrate --state` to upgrade them",
            report.pending()
        )));
    }
    Ok(())
}

/// Execute the `migrate` subcommand.
pub async fn run(args: MigrateArgs) -> CliResult<()> {
    if args.state {
        return run_state(&args).await;
    }
    let cwd = std::env::current_dir()?;
    let path = match args.config.clone() {
        Some(p) => p,
        None => crate::env_loader::discover_config_path(&cwd)
            .ok_or_else(|| CliError::Config("no config file found to migrate".into()))?,
    };

    let text = std::fs::read_to_string(&path)
        .map_err(|e| CliError::Config(format!("cannot read '{}': {e}", path.display())))?;
    let format = ConfigFormat::from_path(&path)?;
    let mut value = format.parse(&text, &path)?;

    let applied = migrate_value(&mut value);

    if applied.is_empty() {
        println!("{}: already current — no migration needed", path.display());
        return Ok(());
    }

    let rendered = format.render(&value, &path)?;

    if args.check {
        eprintln!(
            "{}: migration needed ({} rule{}):",
            path.display(),
            applied.len(),
            if applied.len() == 1 { "" } else { "s" }
        );
        for rule in &applied {
            eprintln!("  - {rule}");
        }
        return Err(CliError::Config(format!(
            "{} is not up to date; run `faucet migrate` to upgrade it",
            path.display()
        )));
    }

    if args.stdout {
        print!("{rendered}");
        return Ok(());
    }

    std::fs::write(&path, rendered)
        .map_err(|e| CliError::Config(format!("cannot write '{}': {e}", path.display())))?;
    println!(
        "{}: migrated ({} rule{} applied):",
        path.display(),
        applied.len(),
        if applied.len() == 1 { "" } else { "s" }
    );
    for rule in &applied {
        println!("  - {rule}");
    }
    Ok(())
}

/// The on-disk serialization of a config file.
#[derive(Clone, Copy)]
enum ConfigFormat {
    Yaml,
    Json,
}

impl ConfigFormat {
    fn from_path(path: &Path) -> CliResult<Self> {
        match path.extension().and_then(|e| e.to_str()) {
            Some("yaml") | Some("yml") => Ok(ConfigFormat::Yaml),
            Some("json") => Ok(ConfigFormat::Json),
            _ => Err(CliError::Config(format!(
                "unsupported config extension for '{}' (expected .yaml/.yml/.json)",
                path.display()
            ))),
        }
    }

    fn parse(self, text: &str, path: &Path) -> CliResult<Value> {
        let err = |e: String| CliError::Config(format!("cannot parse '{}': {e}", path.display()));
        match self {
            ConfigFormat::Yaml => serde_yaml::from_str(text).map_err(|e| err(e.to_string())),
            ConfigFormat::Json => serde_json::from_str(text).map_err(|e| err(e.to_string())),
        }
    }

    fn render(self, value: &Value, path: &Path) -> CliResult<String> {
        let err =
            |e: String| CliError::Config(format!("cannot serialize '{}': {e}", path.display()));
        match self {
            ConfigFormat::Yaml => serde_yaml::to_string(value).map_err(|e| err(e.to_string())),
            ConfigFormat::Json => {
                let mut s = serde_json::to_string_pretty(value).map_err(|e| err(e.to_string()))?;
                s.push('\n');
                Ok(s)
            }
        }
    }
}

/// Apply every migration rule in order, returning a human description of each
/// rule that actually changed the document. An empty result means the config is
/// already current. Pure and idempotent.
pub(crate) fn migrate_value(value: &mut Value) -> Vec<String> {
    let mut applied = Vec::new();
    if migrate_toplevel_source_sink(value) {
        applied.push(
            "wrapped top-level `source:` / `sink:` in a `pipeline:` block (pre-#54 shape)".into(),
        );
    }
    let n = migrate_legacy_auth(value);
    if n > 0 {
        applied.push(format!(
            "folded {n} legacy `auth`/`credentials` block{} into `{{ type, config }}` (pre-#113 shape)",
            if n == 1 { "" } else { "s" }
        ));
    }
    applied
}

/// Rule 1: move top-level `source:` / `sink:` under a new `pipeline:` map.
/// No-op if there is already a `pipeline:` key or neither top-level key exists.
fn migrate_toplevel_source_sink(value: &mut Value) -> bool {
    let Value::Object(root) = value else {
        return false;
    };
    if root.contains_key("pipeline") {
        return false;
    }
    let has_source = root.contains_key("source");
    let has_sink = root.contains_key("sink");
    if !has_source && !has_sink {
        return false;
    }
    let mut pipeline = Map::new();
    if let Some(s) = root.remove("source") {
        pipeline.insert("source".into(), s);
    }
    if let Some(s) = root.remove("sink") {
        pipeline.insert("sink".into(), s);
    }
    // Also relocate the sibling `transforms:` / `state:` blocks, which lived at
    // the top level alongside the old `source:`/`sink:`.
    for key in ["transforms", "state"] {
        if let Some(v) = root.remove(key) {
            pipeline.insert(key.into(), v);
        }
    }
    root.insert("pipeline".into(), Value::Object(pipeline));
    true
}

/// Rule 2: fold a legacy `auth`/`credentials` object of the shape
/// `{ type: X, <field>: … }` into `{ type: X, config: { <field>: … } }`.
/// Recurses through the whole document. Returns the number of blocks migrated.
///
/// Only objects reached under a key literally named `auth` or `credentials` are
/// considered, and only when they carry a `type` string, at least one other
/// field, and no existing `config` — so it never touches an already-migrated
/// block or an unrelated object that happens to have a `type` field.
fn migrate_legacy_auth(value: &mut Value) -> usize {
    let mut count = 0;
    walk_auth(value, false, &mut count);
    count
}

fn walk_auth(value: &mut Value, under_auth_key: bool, count: &mut usize) {
    match value {
        Value::Object(map) => {
            if under_auth_key && is_legacy_auth(map) {
                fold_auth_config(map);
                *count += 1;
            }
            for (k, v) in map.iter_mut() {
                let child_is_auth = k == "auth" || k == "credentials";
                walk_auth(v, child_is_auth, count);
            }
        }
        Value::Array(items) => {
            for v in items.iter_mut() {
                // Array elements are not themselves "under" the auth key name.
                walk_auth(v, false, count);
            }
        }
        _ => {}
    }
}

/// A map is the legacy auth shape if it has a string `type`, no `config`, and at
/// least one field besides `type`.
fn is_legacy_auth(map: &Map<String, Value>) -> bool {
    map.get("type").is_some_and(Value::is_string)
        && !map.contains_key("config")
        && map.keys().any(|k| k != "type")
}

/// Move every field except `type` into a nested `config` object.
fn fold_auth_config(map: &mut Map<String, Value>) {
    let type_val = map.remove("type");
    let mut config = Map::new();
    let keys: Vec<String> = map.keys().cloned().collect();
    for k in keys {
        if let Some(v) = map.remove(&k) {
            config.insert(k, v);
        }
    }
    map.clear();
    if let Some(t) = type_val {
        map.insert("type".into(), t);
    }
    map.insert("config".into(), Value::Object(config));
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn wraps_toplevel_source_sink_into_pipeline() {
        let mut v = json!({
            "version": 1,
            "source": { "type": "rest", "config": { "url": "x" } },
            "sink": { "type": "jsonl", "config": { "path": "o.jsonl" } },
            "transforms": [{ "flatten": {} }]
        });
        let applied = migrate_value(&mut v);
        assert_eq!(applied.len(), 1);
        assert!(v.get("source").is_none());
        assert!(v.get("sink").is_none());
        let p = v.get("pipeline").unwrap();
        assert_eq!(p["source"]["type"], "rest");
        assert_eq!(p["sink"]["type"], "jsonl");
        assert!(p.get("transforms").is_some());
    }

    #[test]
    fn toplevel_rule_is_noop_when_pipeline_present() {
        let mut v = json!({
            "version": 1,
            "pipeline": { "source": { "type": "rest" }, "sink": { "type": "jsonl" } }
        });
        assert!(migrate_value(&mut v).is_empty());
    }

    #[test]
    fn folds_legacy_auth_into_type_config() {
        let mut v = json!({
            "version": 1,
            "pipeline": {
                "source": {
                    "type": "rest",
                    "config": {
                        "url": "x",
                        "auth": { "type": "bearer", "token": "${env:TOK}" }
                    }
                },
                "sink": { "type": "jsonl", "config": { "path": "o.jsonl" } }
            }
        });
        let applied = migrate_value(&mut v);
        assert_eq!(applied.len(), 1);
        let auth = &v["pipeline"]["source"]["config"]["auth"];
        assert_eq!(auth["type"], "bearer");
        assert_eq!(auth["config"]["token"], "${env:TOK}");
        // No stray sibling left behind.
        assert!(auth.get("token").is_none());
    }

    #[test]
    fn auth_rule_is_idempotent_and_skips_current_shape() {
        let v = json!({
            "pipeline": { "source": { "type": "rest", "config": {
                "auth": { "type": "bearer", "config": { "token": "t" } }
            }}}
        });
        // Already `{type, config}` → untouched.
        assert!(migrate_value(&mut v.clone()).is_empty());
        // And running twice is a no-op on the second pass.
        let mut once = v.clone();
        migrate_value(&mut once);
        let mut twice = once.clone();
        assert!(migrate_value(&mut twice).is_empty());
        assert_eq!(once, twice);
    }

    #[test]
    fn does_not_touch_non_auth_objects_with_a_type_field() {
        // A `source: { type, config }` has a `type` but is NOT under an
        // auth/credentials key, so it must be left alone.
        let mut v = json!({
            "pipeline": {
                "source": { "type": "rest", "url": "x" },
                "sink": { "type": "jsonl" }
            }
        });
        let before = v.clone();
        migrate_value(&mut v);
        // The source's `url` sibling is NOT folded into a config (it's not auth).
        assert_eq!(v["pipeline"]["source"], before["pipeline"]["source"]);
    }

    #[test]
    fn both_rules_compose() {
        let mut v = json!({
            "source": { "type": "rest", "config": {
                "auth": { "type": "basic", "user": "u", "pass": "p" }
            }},
            "sink": { "type": "jsonl", "config": { "path": "o" } }
        });
        let applied = migrate_value(&mut v);
        assert_eq!(
            applied.len(),
            2,
            "both the source/sink wrap and the auth fold fire"
        );
        let auth = &v["pipeline"]["source"]["config"]["auth"];
        assert_eq!(auth["type"], "basic");
        assert_eq!(auth["config"]["user"], "u");
        assert_eq!(auth["config"]["pass"], "p");
    }

    #[test]
    fn fully_current_config_needs_no_migration() {
        let mut v = json!({
            "version": 1,
            "pipeline": {
                "source": { "type": "rest", "config": { "url": "x",
                    "auth": { "type": "bearer", "config": { "token": "t" } } } },
                "sink": { "type": "jsonl", "config": { "path": "o.jsonl" } }
            }
        });
        assert!(migrate_value(&mut v).is_empty());
    }

    // ── ConfigFormat + the `run` command flow ────────────────────────────────

    #[test]
    fn config_format_from_path() {
        assert!(matches!(
            ConfigFormat::from_path(Path::new("a.yaml")),
            Ok(ConfigFormat::Yaml)
        ));
        assert!(matches!(
            ConfigFormat::from_path(Path::new("a.yml")),
            Ok(ConfigFormat::Yaml)
        ));
        assert!(matches!(
            ConfigFormat::from_path(Path::new("a.json")),
            Ok(ConfigFormat::Json)
        ));
        assert!(ConfigFormat::from_path(Path::new("a.toml")).is_err());
    }

    #[test]
    fn config_format_parse_render_roundtrip() {
        let p = Path::new("f.yaml");
        let v = ConfigFormat::Yaml
            .parse("version: 1\nname: demo\n", p)
            .unwrap();
        assert_eq!(v["name"], "demo");
        let s = ConfigFormat::Yaml.render(&v, p).unwrap();
        assert!(s.contains("name: demo"));

        let pj = Path::new("f.json");
        let vj = ConfigFormat::Json.parse(r#"{"version":1}"#, pj).unwrap();
        let sj = ConfigFormat::Json.render(&vj, pj).unwrap();
        assert!(sj.ends_with('\n') && sj.contains("\"version\""));
    }

    const LEGACY_YAML: &str = "version: 1\n\
source:\n  type: rest\n  config:\n    base_url: https://x\n    auth: { type: bearer, token: t }\n\
sink:\n  type: jsonl\n  config: { path: out.jsonl }\n";

    fn write_tmp(name: &str, body: &str) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join(name);
        std::fs::write(&path, body).expect("write");
        (dir, path)
    }

    #[tokio::test]
    async fn run_rewrites_legacy_file_in_place() {
        let (_d, path) = write_tmp("old.yaml", LEGACY_YAML);
        run(MigrateArgs {
            config: Some(path.clone()),
            check: false,
            stdout: false,
            state: false,
            row: None,
            json: false,
            force: false,
            env_file: None,
            no_env_file: false,
            profile: None,
        })
        .await
        .unwrap();
        let after = std::fs::read_to_string(&path).unwrap();
        assert!(after.contains("pipeline:"), "{after}");
        assert!(after.contains("config:"));
        // Idempotent: a second migrate reports no change and leaves the file.
        let before2 = std::fs::read_to_string(&path).unwrap();
        run(MigrateArgs {
            config: Some(path.clone()),
            check: false,
            stdout: false,
            state: false,
            row: None,
            json: false,
            force: false,
            env_file: None,
            no_env_file: false,
            profile: None,
        })
        .await
        .unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), before2);
    }

    #[tokio::test]
    async fn run_check_errors_on_legacy_and_passes_on_current() {
        let (_d, legacy) = write_tmp("old.yaml", LEGACY_YAML);
        let err = run(MigrateArgs {
            config: Some(legacy),
            check: true,
            stdout: false,
            state: false,
            row: None,
            json: false,
            force: false,
            env_file: None,
            no_env_file: false,
            profile: None,
        })
        .await;
        assert!(err.is_err(), "--check must fail on a legacy config");

        let current = "version: 1\npipeline:\n  source: { type: rest, config: { base_url: x } }\n  sink: { type: jsonl, config: { path: o } }\n";
        let (_d2, cur) = write_tmp("cur.yaml", current);
        run(MigrateArgs {
            config: Some(cur),
            check: true,
            stdout: false,
            state: false,
            row: None,
            json: false,
            force: false,
            env_file: None,
            no_env_file: false,
            profile: None,
        })
        .await
        .expect("--check passes on a current config");
    }

    #[tokio::test]
    async fn run_stdout_does_not_write_the_file() {
        let (_d, path) = write_tmp("old.yaml", LEGACY_YAML);
        let original = std::fs::read_to_string(&path).unwrap();
        run(MigrateArgs {
            config: Some(path.clone()),
            check: false,
            stdout: true,
            state: false,
            row: None,
            json: false,
            force: false,
            env_file: None,
            no_env_file: false,
            profile: None,
        })
        .await
        .unwrap();
        // --stdout prints the migrated config but leaves the file untouched.
        assert_eq!(std::fs::read_to_string(&path).unwrap(), original);
    }
}
