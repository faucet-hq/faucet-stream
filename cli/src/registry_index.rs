//! Connector registry index (#208): the discovery/distribution layer behind
//! `faucet search`, `faucet install`, and `faucet list --available`.
//!
//! The index is a committed, **feature-independent** JSON catalog
//! (`cli/connectors/registry.json`, embedded at build time) of every connector the
//! ecosystem knows about — the built-in `verified` ones plus any community
//! `faucet-source-*` / `faucet-sink-*` crates added by PR. It is deliberately
//! decoupled from which connectors a given binary compiled in, so `search` can
//! surface a connector you don't yet have and `install` can tell you how to get
//! it. Pass `--index <path>` to point at a custom/mirror index.

use crate::error::{CliError, CliResult};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// The committed built-in index, embedded so `search`/`install` work offline
/// and regardless of compiled features.
const EMBEDDED_INDEX: &str = include_str!("../connectors/registry.json");

/// One connector in the registry index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorEntry {
    /// System name / YAML `type:` value (e.g. `kafka`).
    pub name: String,
    /// `"source"` or `"sink"`.
    pub kind: String,
    /// Display name (e.g. `PostgreSQL CDC`).
    #[serde(default)]
    pub title: Option<String>,
    /// Connector Hub category: `databases` / `cdc` / `warehouses` /
    /// `streaming` / `files` / `apis` / `bridges`.
    #[serde(default)]
    pub category: Option<String>,
    /// Verified = a first-party built-in that ships in the `faucet` binary.
    /// Community connectors set `false`.
    #[serde(default = "default_true")]
    pub verified: bool,
    /// One-line summary.
    #[serde(default)]
    pub description: String,
    /// Crate name (defaults to `faucet-<kind>-<name>`).
    #[serde(rename = "crate", default)]
    pub krate: Option<String>,
    /// CLI feature flag that compiles this connector in (defaults to
    /// `<kind>-<name>`). Applies to built-ins; a community connector may set it
    /// or leave it null (consumed via a custom binary).
    #[serde(default)]
    pub feature: Option<String>,
    /// Extra crates.io keywords to match `search` against.
    #[serde(default)]
    pub keywords: Vec<String>,
    /// faucet-core version compatibility (semver requirement), informational.
    #[serde(default)]
    pub core_compat: Option<String>,
    /// Maturity tier from the connector conformance score (#330):
    /// `stable` / `experimental` / `beta` / `draft`. For verified built-ins this
    /// is validated against the score computed by [`crate::conformance`] (see the
    /// `builtin_tiers_match_conformance` test), so it can never drift from the
    /// code. Community connectors may declare it or leave it null.
    #[serde(default)]
    pub tier: Option<String>,
}

fn default_true() -> bool {
    true
}

impl ConnectorEntry {
    /// Resolved crate name.
    pub fn crate_name(&self) -> String {
        self.krate
            .clone()
            .unwrap_or_else(|| format!("faucet-{}-{}", self.kind, self.name))
    }

    /// Resolved CLI feature flag.
    pub fn feature_flag(&self) -> String {
        self.feature
            .clone()
            .unwrap_or_else(|| format!("{}-{}", self.kind, self.name))
    }

    /// Whether this entry matches a lowercase search term (name / description /
    /// keywords / crate).
    pub fn matches(&self, lowered_term: &str) -> bool {
        self.name.to_lowercase().contains(lowered_term)
            || self.description.to_lowercase().contains(lowered_term)
            || self.crate_name().to_lowercase().contains(lowered_term)
            || self
                .keywords
                .iter()
                .any(|k| k.to_lowercase().contains(lowered_term))
    }
}

/// The parsed registry index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryIndex {
    #[serde(default = "default_version")]
    pub version: u32,
    pub connectors: Vec<ConnectorEntry>,
}

fn default_version() -> u32 {
    1
}

impl RegistryIndex {
    /// The embedded built-in index.
    pub fn embedded() -> Self {
        serde_json::from_str(EMBEDDED_INDEX)
            .expect("embedded cli/connectors/registry.json is valid")
    }

    /// Load from `path`, or the embedded index when `None`.
    pub fn load(path: Option<&Path>) -> CliResult<Self> {
        match path {
            None => Ok(Self::embedded()),
            Some(p) => {
                let text = std::fs::read_to_string(p)?;
                serde_json::from_str(&text).map_err(|e| {
                    CliError::Config(format!("invalid connector index `{}`: {e}", p.display()))
                })
            }
        }
    }

    /// Entries matching `term` (case-insensitive), sorted by kind then name.
    pub fn search(&self, term: &str) -> Vec<&ConnectorEntry> {
        let t = term.to_lowercase();
        let mut hits: Vec<&ConnectorEntry> =
            self.connectors.iter().filter(|c| c.matches(&t)).collect();
        hits.sort_by(|a, b| {
            (a.kind.as_str(), a.name.as_str()).cmp(&(b.kind.as_str(), b.name.as_str()))
        });
        hits
    }

    /// Find entries by exact `name`, optionally constrained to a `kind`.
    pub fn find(&self, name: &str, kind: Option<&str>) -> Vec<&ConnectorEntry> {
        self.connectors
            .iter()
            .filter(|c| c.name == name && kind.map(|k| k == c.kind).unwrap_or(true))
            .collect()
    }
}

/// How to obtain a connector — the pure output of `faucet install`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InstallRecipe {
    /// A verified built-in already compiled into this binary.
    AlreadyAvailable { feature: String },
    /// A verified built-in — reinstall the CLI with its feature enabled.
    CargoInstall { feature: String },
    /// A community connector — build a custom binary that registers it.
    CustomBinary { krate: String, feature: String },
}

/// Decide the install recipe for `entry`. `compiled_in` reports whether this
/// binary already has the connector (via the connector registry).
pub fn install_recipe(entry: &ConnectorEntry, compiled_in: bool) -> InstallRecipe {
    let feature = entry.feature_flag();
    if !entry.verified {
        return InstallRecipe::CustomBinary {
            krate: entry.crate_name(),
            feature,
        };
    }
    if compiled_in {
        InstallRecipe::AlreadyAvailable { feature }
    } else {
        InstallRecipe::CargoInstall { feature }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_index_parses_and_is_non_empty() {
        let idx = RegistryIndex::embedded();
        assert_eq!(idx.version, 1);
        assert!(idx.connectors.len() > 30, "expected the built-in catalog");
        // Every built-in entry derives a crate/feature.
        let kafka = idx
            .find("kafka", Some("source"))
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(kafka.crate_name(), "faucet-source-kafka");
        assert_eq!(kafka.feature_flag(), "source-kafka");
        assert!(kafka.verified);
    }

    #[test]
    fn search_is_case_insensitive_over_fields() {
        let idx = RegistryIndex::embedded();
        let hits = idx.search("KAFKA");
        assert!(hits.iter().any(|c| c.name == "kafka" && c.kind == "source"));
        assert!(hits.iter().any(|c| c.name == "kafka" && c.kind == "sink"));
        // description match
        assert!(!idx.search("cdc").is_empty());
        // no match
        assert!(idx.search("definitely-not-a-connector").is_empty());
    }

    #[test]
    fn install_recipe_for_builtin_compiled_and_not() {
        let idx = RegistryIndex::embedded();
        let entry = idx
            .find("bigquery", Some("sink"))
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(
            install_recipe(entry, true),
            InstallRecipe::AlreadyAvailable {
                feature: "sink-bigquery".into()
            }
        );
        assert_eq!(
            install_recipe(entry, false),
            InstallRecipe::CargoInstall {
                feature: "sink-bigquery".into()
            }
        );
    }

    #[test]
    fn install_recipe_for_community_is_custom_binary() {
        let entry = ConnectorEntry {
            name: "acme".into(),
            kind: "source".into(),
            title: None,
            category: None,
            verified: false,
            description: "community".into(),
            krate: None,
            feature: None,
            keywords: vec![],
            core_compat: None,
            tier: None,
        };
        assert_eq!(
            install_recipe(&entry, false),
            InstallRecipe::CustomBinary {
                krate: "faucet-source-acme".into(),
                feature: "source-acme".into()
            }
        );
    }

    #[test]
    fn load_from_path_parses_custom_index() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("idx.json");
        std::fs::write(
            &p,
            r#"{"version":1,"connectors":[{"name":"acme","kind":"source","verified":false,"description":"Acme","crate":"acme-faucet"}]}"#,
        )
        .unwrap();
        let idx = RegistryIndex::load(Some(&p)).unwrap();
        let e = &idx.connectors[0];
        assert_eq!(e.crate_name(), "acme-faucet"); // explicit crate override
        assert_eq!(e.feature_flag(), "source-acme"); // derived
        assert!(!e.verified);
    }

    // Maintenance guard: adding a built-in connector without an index entry
    // fails here (under `default`/`--all-features`, every built-in is compiled
    // in, so `source_kinds()`/`sink_kinds()` is the full built-in set).
    #[test]
    fn index_covers_every_compiled_builtin() {
        let idx = RegistryIndex::embedded();
        for k in crate::registry::source_kinds() {
            assert!(
                idx.find(k, Some("source")).iter().any(|c| c.verified),
                "built-in source `{k}` is missing a verified entry in cli/connectors/registry.json"
            );
        }
        for k in crate::registry::sink_kinds() {
            assert!(
                idx.find(k, Some("sink")).iter().any(|c| c.verified),
                "built-in sink `{k}` is missing a verified entry in cli/connectors/registry.json"
            );
        }
    }

    // Every verified built-in must declare a `tier` in the index, and it must
    // equal the tier the conformance scorer computes from the connector's real
    // capabilities — so the published catalog can never drift from the code.
    #[test]
    fn builtin_tiers_match_conformance() {
        let idx = RegistryIndex::embedded();
        for k in crate::registry::source_kinds() {
            let entry = idx
                .find(k, Some("source"))
                .into_iter()
                .find(|c| c.verified)
                .unwrap_or_else(|| panic!("built-in source `{k}` missing a verified entry"));
            let want = crate::conformance::tier_for(k, true).as_str();
            assert_eq!(
                entry.tier.as_deref(),
                Some(want),
                "source `{k}` registry tier {:?} != computed `{want}`",
                entry.tier
            );
        }
        for k in crate::registry::sink_kinds() {
            let entry = idx
                .find(k, Some("sink"))
                .into_iter()
                .find(|c| c.verified)
                .unwrap_or_else(|| panic!("built-in sink `{k}` missing a verified entry"));
            let want = crate::conformance::tier_for(k, false).as_str();
            assert_eq!(
                entry.tier.as_deref(),
                Some(want),
                "sink `{k}` registry tier {:?} != computed `{want}`",
                entry.tier
            );
        }
    }

    #[test]
    fn load_bad_index_errors() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("bad.json");
        std::fs::write(&p, "{ not json").unwrap();
        assert!(matches!(
            RegistryIndex::load(Some(&p)),
            Err(CliError::Config(_))
        ));
    }
}
