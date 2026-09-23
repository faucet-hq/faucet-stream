//! The `--templates-sync` file (RFC 0006 / #589): a declarative list of remote
//! **origins** a server pulls pipeline templates from.
//!
//! Pure data + validation — no I/O. The one rule enforced here is the
//! single-source-of-truth invariant: every origin owns an id **namespace**
//! (its `prefix`), and two origins whose prefixes overlap are a load-time
//! error, so no template can ever have two owners and "who wins" never needs
//! deciding at runtime.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::error::{CliError, CliResult};

/// Top-level sync file. `version` must be `1`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SyncFile {
    /// Schema version; must be `1`.
    pub version: u32,
    /// Remote origins to pull from. Each owns the id namespace named by its
    /// `prefix`; prefixes must not overlap.
    pub origins: Vec<Origin>,
}

/// One remote store of templates.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Origin {
    /// Operator-facing name, used in `--origin`, the console, logs and the audit
    /// record. Unique within the file.
    pub name: String,
    /// Where the templates live.
    pub source: OriginSource,
    /// Id namespace this origin owns. A remote file `tenant-sync.yaml` under an
    /// origin with `prefix: platform-` registers as `platform-tenant-sync`.
    /// May be empty (the origin then owns the whole unprefixed namespace, and
    /// can be the only origin).
    #[serde(default)]
    pub prefix: String,
    /// Whether a pull may move `stable`. Default `ignore`: pull registers and
    /// moves nothing, preserving the #444 rule that a register moves nobody.
    #[serde(default)]
    pub launch: LaunchPolicy,
    /// What to do with a local template this origin owns that has vanished
    /// upstream. Default `keep`. Never deletes — a delete cascades to the
    /// launch log and would silently repoint `stable`.
    #[serde(default)]
    pub prune: PrunePolicy,
    /// Re-pull every this many seconds. `None` (default) = pull on start and on
    /// demand only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_secs: Option<u64>,
}

/// The remote store behind an origin. Adjacently tagged (`{type, config}`) like
/// every other connector block.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "config", rename_all = "snake_case")]
pub enum OriginSource {
    /// A directory in a GitHub repository, read through the contents API — a
    /// private repo needs only a token, and no git binary.
    Github(GithubSource),
    /// An S3 prefix.
    S3(S3Source),
    /// A Google Cloud Storage prefix.
    Gcs(GcsSource),
    /// An Azure Blob Storage prefix.
    AzureBlob(AzureBlobSource),
}

impl OriginSource {
    /// Short kind label for logs and the dry-run plan.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Github(_) => "github",
            Self::S3(_) => "s3",
            Self::Gcs(_) => "gcs",
            Self::AzureBlob(_) => "azure_blob",
        }
    }

    /// Whether this source needs the `templates-sync-object-store` feature.
    pub fn needs_object_store(&self) -> bool {
        !matches!(self, Self::Github(_))
    }
}

/// GitHub contents-API origin.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GithubSource {
    /// `owner/name`.
    pub repo: String,
    /// Branch, tag, or commit SHA. Default `main`.
    #[serde(default = "default_ref")]
    pub r#ref: String,
    /// Directory of `*.yaml` / `*.json` templates. Default: the repo root.
    /// Mutually exclusive with `paths`.
    #[serde(default)]
    pub path: String,
    /// Several directories read as one origin — e.g. a Template Hub catalog's
    /// `[source-templates, sink-templates]`. Template stems must be unique
    /// across them (a hub's source and sink names share one id namespace).
    /// `publish` writes to the first.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub paths: Vec<String>,
    /// Personal-access or app token. Use `${env:…}` / `${secret:…}`; the value
    /// is registered for log redaction.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    /// API base, for GitHub Enterprise. Default `https://api.github.com`.
    #[serde(default = "default_github_api")]
    pub api_base: String,
}

fn default_ref() -> String {
    "main".into()
}
fn default_github_api() -> String {
    "https://api.github.com".into()
}

/// S3 origin. Credentials come from the SDK default chain (env, profile, IAM).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct S3Source {
    pub bucket: String,
    /// Key prefix (directory) holding the templates.
    #[serde(default)]
    pub prefix: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    /// Custom endpoint for S3-compatible stores (MinIO, …).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
}

/// GCS origin. Credentials come from Application Default Credentials.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GcsSource {
    pub bucket: String,
    #[serde(default)]
    pub prefix: String,
}

/// Azure Blob origin. Credentials come from the `AZURE_*` environment.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AzureBlobSource {
    pub container: String,
    #[serde(default)]
    pub prefix: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account: Option<String>,
}

/// Whether, and when, a pull may move `stable`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum LaunchPolicy {
    /// Register only; never move `stable`. The safe default.
    #[default]
    Ignore,
    /// Launch a pulled version only when its sidecar says `launch: true`.
    Follow,
    /// Launch **every** pulled version. GitOps mode: merging upstream changes
    /// what production triggers resolve to. Opt-in, and audited.
    Always,
}

/// What a pull does with a local, origin-owned template that is gone upstream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PrunePolicy {
    /// Report it as orphaned and leave it alone.
    #[default]
    Keep,
    /// Mark it deprecated (never delete).
    Deprecate,
}

/// The optional `<stem>.faucet.yaml` sidecar carrying release intent, kept
/// separate so the config body itself stays runnable with `faucet run`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Sidecar {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// This version should become `stable` on pull (honoured under
    /// `launch: follow`).
    #[serde(default)]
    pub launch: bool,
    /// Assignable channels (`dev`, `staging`, …) to point at the pulled version.
    #[serde(default)]
    pub tags: Vec<String>,
}

impl SyncFile {
    /// Structural validation: version, non-empty, unique names, and the
    /// single-owner rule over prefixes.
    pub fn validate(&self) -> CliResult<()> {
        if self.version != 1 {
            return Err(CliError::Config(format!(
                "templates-sync: unsupported version {} (expected 1)",
                self.version
            )));
        }
        if self.origins.is_empty() {
            return Err(CliError::Config(
                "templates-sync: `origins` is empty — nothing to sync".into(),
            ));
        }
        let mut names: HashMap<&str, usize> = HashMap::new();
        for (i, o) in self.origins.iter().enumerate() {
            if o.name.trim().is_empty() {
                return Err(CliError::Config(format!(
                    "templates-sync: origins[{i}] has an empty `name`"
                )));
            }
            if let Some(prev) = names.insert(&o.name, i) {
                return Err(CliError::Config(format!(
                    "templates-sync: origin name '{}' is used twice (origins[{prev}] and [{i}])",
                    o.name
                )));
            }
            if let OriginSource::Github(g) = &o.source
                && !g.path.is_empty()
                && !g.paths.is_empty()
            {
                return Err(CliError::Config(format!(
                    "templates-sync: origin '{}': set `path` or `paths`, not both",
                    o.name
                )));
            }
            if o.interval_secs == Some(0) {
                return Err(CliError::Config(format!(
                    "templates-sync: origin '{}': `interval_secs` must be > 0 (omit it to disable)",
                    o.name
                )));
            }
        }
        // Single-owner rule: no prefix may be a prefix of another origin's prefix
        // (including equality, and the empty prefix which owns everything).
        for (i, a) in self.origins.iter().enumerate() {
            for b in &self.origins[i + 1..] {
                if a.prefix.starts_with(&b.prefix) || b.prefix.starts_with(&a.prefix) {
                    return Err(CliError::Config(format!(
                        "templates-sync: origins '{}' (prefix '{}') and '{}' (prefix '{}') own \
                         overlapping id namespaces — every template must have exactly one owner",
                        a.name, a.prefix, b.name, b.prefix
                    )));
                }
            }
        }
        Ok(())
    }

    /// Find an origin by name.
    pub fn origin(&self, name: &str) -> CliResult<&Origin> {
        self.origins.iter().find(|o| o.name == name).ok_or_else(|| {
            let known: Vec<&str> = self.origins.iter().map(|o| o.name.as_str()).collect();
            CliError::Config(format!(
                "templates-sync: no origin named '{name}' (known: {})",
                known.join(", ")
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_github_origin_takes_path_or_paths_not_both() {
        let mut file = SyncFile {
            version: 1,
            origins: vec![origin("hub", "")],
        };
        let OriginSource::Github(g) = &mut file.origins[0].source else {
            unreachable!()
        };
        g.paths = vec!["source-templates".into(), "sink-templates".into()];
        file.validate().expect("paths alone is fine");
        let OriginSource::Github(g) = &mut file.origins[0].source else {
            unreachable!()
        };
        g.path = "templates".into();
        let err = file.validate().unwrap_err().to_string();
        assert!(err.contains("set `path` or `paths`, not both"), "{err}");
    }

    fn origin(name: &str, prefix: &str) -> Origin {
        Origin {
            name: name.into(),
            source: OriginSource::Github(GithubSource {
                repo: "acme/tpl".into(),
                r#ref: default_ref(),
                path: String::new(),
                paths: Vec::new(),
                token: None,
                api_base: default_github_api(),
            }),
            prefix: prefix.into(),
            launch: LaunchPolicy::default(),
            prune: PrunePolicy::default(),
            interval_secs: None,
        }
    }

    #[test]
    fn parses_the_rfc_example_shape() {
        let yaml = r#"
version: 1
origins:
  - name: platform
    source:
      type: github
      config: { repo: acme/data-templates, ref: main, path: templates/ }
    prefix: platform-
  - name: partner
    source:
      type: s3
      config: { bucket: acme-templates, prefix: shared/, region: us-east-1 }
    prefix: partner-
    launch: follow
"#;
        let f: SyncFile = serde_yaml::from_str(yaml).expect("parse");
        f.validate().expect("valid");
        assert_eq!(f.origins.len(), 2);
        assert_eq!(f.origins[0].source.kind(), "github");
        assert_eq!(f.origins[1].launch, LaunchPolicy::Follow);
        assert!(f.origins[1].source.needs_object_store());
        assert!(!f.origins[0].source.needs_object_store());
    }

    /// The single-owner rule: overlapping prefixes are a load-time error, so
    /// "who wins" never has to be decided at runtime.
    #[test]
    fn overlapping_prefixes_are_rejected() {
        let f = SyncFile {
            version: 1,
            origins: vec![origin("a", "team-"), origin("b", "team-eu-")],
        };
        let err = f.validate().expect_err("overlap must fail");
        assert!(
            err.to_string().contains("overlapping id namespaces"),
            "{err}"
        );

        // Equal prefixes overlap too.
        let f = SyncFile {
            version: 1,
            origins: vec![origin("a", "x-"), origin("b", "x-")],
        };
        assert!(f.validate().is_err());

        // The empty prefix owns everything, so it cannot coexist.
        let f = SyncFile {
            version: 1,
            origins: vec![origin("a", ""), origin("b", "x-")],
        };
        assert!(f.validate().is_err());
    }

    #[test]
    fn disjoint_prefixes_are_fine_and_a_lone_empty_prefix_is_allowed() {
        SyncFile {
            version: 1,
            origins: vec![origin("a", "plat-"), origin("b", "part-")],
        }
        .validate()
        .expect("disjoint");
        SyncFile {
            version: 1,
            origins: vec![origin("only", "")],
        }
        .validate()
        .expect("a single unprefixed origin owns the whole namespace");
    }

    #[test]
    fn structural_errors_name_the_problem() {
        assert!(
            SyncFile {
                version: 2,
                origins: vec![origin("a", "")]
            }
            .validate()
            .unwrap_err()
            .to_string()
            .contains("version 2")
        );
        assert!(
            SyncFile {
                version: 1,
                origins: vec![]
            }
            .validate()
            .unwrap_err()
            .to_string()
            .contains("empty")
        );
        assert!(
            SyncFile {
                version: 1,
                origins: vec![origin("dup", "a-"), origin("dup", "b-")]
            }
            .validate()
            .unwrap_err()
            .to_string()
            .contains("used twice")
        );
        let mut zero = origin("z", "z-");
        zero.interval_secs = Some(0);
        assert!(
            SyncFile {
                version: 1,
                origins: vec![zero]
            }
            .validate()
            .unwrap_err()
            .to_string()
            .contains("interval_secs")
        );
    }

    #[test]
    fn origin_lookup_lists_known_names_on_miss() {
        let f = SyncFile {
            version: 1,
            origins: vec![origin("plat", "p-")],
        };
        assert!(f.origin("plat").is_ok());
        let err = f.origin("nope").unwrap_err().to_string();
        assert!(err.contains("nope") && err.contains("plat"), "{err}");
    }

    #[test]
    fn sidecar_defaults_are_inert() {
        let s: Sidecar = serde_yaml::from_str("{}").unwrap();
        assert_eq!(s, Sidecar::default());
        assert!(!s.launch);
        let s: Sidecar = serde_yaml::from_str("launch: true\ntags: [dev]").unwrap();
        assert!(s.launch);
        assert_eq!(s.tags, vec!["dev"]);
    }
}
