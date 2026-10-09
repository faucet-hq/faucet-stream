//! `requires_faucet` (#853): the faucet versions a config or template was
//! written for. A binary outside the range refuses the document before it is
//! parsed, so a newer config fails loudly on an older binary instead of being
//! misread.

use crate::error::{CliError, CliResult};
use semver::{Version, VersionReq};
use serde_json::Value;

/// The top-level key holding the constraint.
pub const KEY: &str = "requires_faucet";

/// This binary's version.
pub fn current() -> Version {
    Version::parse(env!("CARGO_PKG_VERSION")).expect("CARGO_PKG_VERSION is semver")
}

/// The constraint `faucet init` writes: this binary's minor version or later.
pub fn default_requirement() -> String {
    let v = current();
    format!(">={}.{}", v.major, v.minor)
}

/// Check `doc`'s top-level `requires_faucet`, if any, against this binary.
pub fn check_document(doc: &Value, what: &str) -> CliResult<()> {
    match doc.get(KEY) {
        None | Some(Value::Null) => Ok(()),
        Some(Value::String(req)) => check(req, what),
        Some(other) => Err(CliError::Config(format!(
            "{KEY} must be a version requirement string such as \">=1.15\", got {other}"
        ))),
    }
}

/// Check one requirement against this binary.
pub fn check(requirement: &str, what: &str) -> CliResult<()> {
    check_against(requirement, &current(), what)
}

/// Check `requirement` against `version`.
///
/// Standard semver matching applies, so an explicit pre-release comparator
/// (`=1.15.0-rc.1`) matches that pre-release. A pre-release binary otherwise
/// counts as the newest release before it: `1.15.0-rc.1` satisfies `>=1.14`
/// but not `>=1.15`, because it may lack what 1.15.0 ships.
pub fn check_against(requirement: &str, version: &Version, what: &str) -> CliResult<()> {
    let req = VersionReq::parse(requirement.trim()).map_err(|e| {
        CliError::Config(format!(
            "{KEY}: '{requirement}' is not a version requirement ({e}); write e.g. \">=1.15\""
        ))
    })?;
    let satisfied = req.matches(version)
        || (!version.pre.is_empty() && predecessor(version).is_some_and(|p| req.matches(&p)));
    if satisfied {
        return Ok(());
    }
    Err(CliError::IncompatibleFaucet {
        what: what.to_string(),
        required: requirement.trim().to_string(),
        version: version.to_string(),
    })
}

/// The newest release ordered before the pre-release `v`.
fn predecessor(v: &Version) -> Option<Version> {
    let (major, minor, patch) = match (v.major, v.minor, v.patch) {
        (major, minor, patch) if patch > 0 => (major, minor, patch - 1),
        (major, minor, _) if minor > 0 => (major, minor - 1, u64::MAX),
        (major, _, _) if major > 0 => (major - 1, u64::MAX, u64::MAX),
        _ => return None,
    };
    Some(Version::new(major, minor, patch))
}

/// The `mise.toml` tool that installs faucet from its GitHub releases.
pub const MISE_TOOL: &str = "github:faucet-hq/faucet-stream";

/// The release tag prefix mise strips to get the version.
pub const MISE_VERSION_PREFIX: &str = "faucet-cli-v";

/// `existing` (a `mise.toml`, if there is one) with faucet pinned at
/// `version`, or `None` when it already pins faucet.
pub fn pin_mise(existing: Option<&str>, version: &Version) -> CliResult<Option<String>> {
    let mut doc: toml_edit::DocumentMut = existing
        .unwrap_or("")
        .parse()
        .map_err(|e| CliError::Config(format!("mise.toml is not valid TOML: {e}")))?;
    let tools = doc
        .entry("tools")
        .or_insert_with(|| toml_edit::Item::Table(toml_edit::Table::new()));
    let Some(tools) = tools.as_table_like_mut() else {
        return Err(CliError::Config("mise.toml: `tools` is not a table".into()));
    };
    if tools.contains_key(MISE_TOOL) {
        return Ok(None);
    }
    let mut entry = toml_edit::InlineTable::new();
    entry.insert("version", version.to_string().into());
    entry.insert("version_prefix", MISE_VERSION_PREFIX.into());
    tools.insert(MISE_TOOL, toml_edit::value(entry));
    let mut out = doc.to_string();
    if existing.is_none() {
        out = format!(
            "# The faucet version this project uses. `mise install` installs it; the\n\
             # matching container image is ghcr.io/faucet-hq/faucet-stream:{version}.\n{out}"
        );
    }
    Ok(Some(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn v(s: &str) -> Version {
        Version::parse(s).unwrap()
    }

    #[test]
    fn a_satisfied_requirement_passes() {
        assert!(check_against(">=1.13", &v("1.14.2"), "pipeline").is_ok());
        assert!(check_against(" ^1.14 ", &v("1.14.0"), "pipeline").is_ok());
        assert!(check_against("=1.14.2", &v("1.14.2"), "pipeline").is_ok());
    }

    #[test]
    fn an_unsatisfied_requirement_names_both_versions() {
        let err = check_against(">=1.13", &v("1.12.0"), "pipeline").unwrap_err();
        assert!(matches!(err, CliError::IncompatibleFaucet { .. }));
        assert_eq!(
            err.to_string(),
            "this pipeline requires faucet >=1.13; this binary is 1.12.0"
        );
        let err = check_against("<2", &v("2.0.0"), "source template").unwrap_err();
        assert!(err.to_string().starts_with("this source template requires"));
    }

    #[test]
    fn a_malformed_requirement_is_a_config_error() {
        let err = check_against("soon", &v("1.0.0"), "pipeline").unwrap_err();
        assert!(matches!(err, CliError::Config(_)));
        assert!(
            err.to_string()
                .contains("'soon' is not a version requirement")
        );
    }

    #[test]
    fn a_pre_release_binary_counts_as_the_release_before_it() {
        assert!(check_against(">=1.14", &v("1.15.0-rc.1"), "pipeline").is_ok());
        assert!(check_against(">=1.15", &v("1.15.0-rc.1"), "pipeline").is_err());
        assert!(check_against("=1.15.0-rc.1", &v("1.15.0-rc.1"), "pipeline").is_ok());
        assert!(check_against(">=1.15.1", &v("1.15.2-beta"), "pipeline").is_ok());
        assert!(check_against(">=1.9", &v("2.0.0-alpha"), "pipeline").is_ok());
        assert!(check_against(">=0.0.0", &v("0.0.0-alpha"), "pipeline").is_err());
    }

    #[test]
    fn documents_are_checked_by_their_top_level_key() {
        assert!(check_document(&json!({"version": 1}), "pipeline").is_ok());
        assert!(check_document(&json!({KEY: null}), "pipeline").is_ok());
        assert!(check_document(&json!({KEY: ">=0.1"}), "pipeline").is_ok());
        assert!(matches!(
            check_document(&json!({KEY: ">=999"}), "pipeline"),
            Err(CliError::IncompatibleFaucet { .. })
        ));
        let err = check_document(&json!({KEY: 1.15}), "pipeline").unwrap_err();
        assert!(
            err.to_string()
                .contains("must be a version requirement string")
        );
    }

    #[test]
    fn a_new_mise_file_pins_faucet() {
        let out = pin_mise(None, &v("1.15.2")).unwrap().unwrap();
        assert!(
            out.contains("ghcr.io/faucet-hq/faucet-stream:1.15.2"),
            "{out}"
        );
        let doc: toml_edit::DocumentMut = out.parse().unwrap();
        let entry = &doc["tools"][MISE_TOOL];
        assert_eq!(entry["version"].as_str(), Some("1.15.2"));
        assert_eq!(entry["version_prefix"].as_str(), Some("faucet-cli-v"));
    }

    #[test]
    fn an_existing_mise_file_keeps_its_tools_and_comments() {
        let existing = "# team tools\n[tools]\nnode = \"22\"\n\n[env]\nFOO = \"1\"\n";
        let out = pin_mise(Some(existing), &v("1.15.0")).unwrap().unwrap();
        assert!(
            out.starts_with("# team tools\n[tools]\nnode = \"22\"\n"),
            "{out}"
        );
        assert!(out.contains("[env]\nFOO = \"1\""), "{out}");
        assert!(out.contains(MISE_TOOL), "{out}");
        let no_tools = pin_mise(Some("[env]\nA = \"b\"\n"), &v("1.15.0"))
            .unwrap()
            .unwrap();
        assert!(no_tools.contains("[tools]"), "{no_tools}");
    }

    #[test]
    fn an_existing_pin_is_left_alone() {
        let existing = format!("[tools]\n\"{MISE_TOOL}\" = \"1.10.0\"\n");
        assert_eq!(pin_mise(Some(&existing), &v("1.15.0")).unwrap(), None);
    }

    #[test]
    fn a_broken_mise_file_is_refused() {
        assert!(pin_mise(Some("[tools"), &v("1.0.0")).is_err());
        assert!(pin_mise(Some("tools = 1"), &v("1.0.0")).is_err());
    }

    const TOO_NEW_YAML: &str =
        "requires_faucet: \">=999\"\nversion: 1\npipeline:\n  future_block: {}\n";

    #[test]
    fn every_pipeline_load_refuses_a_config_for_a_newer_faucet() {
        use crate::config::{PipelineConfig, RunInputs};
        let refused = |r: CliResult<PipelineConfig>| {
            let err = r.unwrap_err();
            assert!(matches!(err, CliError::IncompatibleFaucet { .. }), "{err}");
        };
        let yaml = std::path::Path::new("p.yaml");
        refused(PipelineConfig::from_text(TOO_NEW_YAML, yaml));
        let json = r#"{"requires_faucet": ">=999", "version": 1, "pipeline": {"future": 1}}"#;
        refused(PipelineConfig::from_text(
            json,
            std::path::Path::new("p.json"),
        ));
        refused(PipelineConfig::from_value(
            serde_json::from_str(json).unwrap(),
        ));
        let dir = tempfile::tempdir().unwrap();
        for (file, text) in [("p.yaml", TOO_NEW_YAML), ("p.json", json)] {
            let path = dir.path().join(file);
            std::fs::write(&path, text).unwrap();
            refused(PipelineConfig::from_path_with(
                &path,
                None,
                &RunInputs::placeholders(),
            ));
        }
        let ok = "requires_faucet: \">=1\"\nversion: 1\npipeline:\n  source: { type: csv, config: { path: a.csv } }\n  sink: { type: jsonl, config: { path: b.jsonl } }\n";
        let cfg = PipelineConfig::from_text(ok, yaml).unwrap();
        assert_eq!(cfg.requires_faucet.as_deref(), Some(">=1"));
    }

    #[test]
    fn hub_templates_for_a_newer_faucet_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("s.yaml");
        std::fs::write(
            &src,
            "requires_faucet: \">=999\"\nkind: source-template\nname: s\n",
        )
        .unwrap();
        let err = crate::hub::parse_source_file(&src).unwrap_err();
        assert!(
            err.to_string().starts_with("this source template requires"),
            "{err}"
        );
        let sink = dir.path().join("k.yaml");
        std::fs::write(
            &sink,
            "requires_faucet: \">=999\"\nkind: sink-template\nname: k\n",
        )
        .unwrap();
        let err = crate::hub::parse_sink_file(&sink).unwrap_err();
        assert!(
            err.to_string().starts_with("this sink template requires"),
            "{err}"
        );
        let overlay = json!({"kind": "deployment", "name": "d", KEY: ">=999"});
        let err = crate::hub::DeploymentTemplate::from_value(overlay).unwrap_err();
        assert!(
            err.to_string()
                .starts_with("this deployment overlay requires"),
            "{err}"
        );
        let overlay = json!({"kind": "deployment", "name": "d", KEY: ">=1"});
        let d = crate::hub::DeploymentTemplate::from_value(overlay).unwrap();
        assert_eq!(d.requires_faucet.as_deref(), Some(">=1"));
    }

    #[test]
    fn the_init_default_admits_this_binary() {
        let req = default_requirement();
        assert!(req.starts_with(">="));
        assert!(check(&req, "pipeline").is_ok());
    }
}
