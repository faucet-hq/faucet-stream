//! Template Hub (#571): source/sink-split pipeline templates composed at run
//! time.
//!
//! - [`spec`] — the two document kinds (`kind: source-template` with
//!   `streams[]` + per-stream `write` preferences; `kind: sink-template` with
//!   `per_stream` addressing) and their validation.
//! - [`mod@compose`] — pure `source × sink → PipelineConfig` composition with
//!   ordered write-mode fallback and per-stream incompatibility errors.
//! - [`catalog`] — a hub directory (`source-templates/`, `sink-templates/`),
//!   its compatibility matrix, the publishability lint, and the renderers
//!   behind the docs page and `index.json`.
//!
//! This module is glue over existing machinery — `pipeline.sources/sinks` +
//! `ref:`, `matrix:` fan-out, transform layering, the `params:` pass, and the
//! registry's write-mode capabilities — so a composed pipeline runs through
//! `expand` / `run_expanded` untouched and inherits every gate they enforce.
//! Zero `faucet-core` changes.

pub mod catalog;
pub mod compose;
#[cfg(feature = "hub-remote")]
pub mod remote;
pub mod spec;

use std::path::{Path, PathBuf};

use serde_json::Value;

use crate::config::{PipelineConfig, RunInputs};
use crate::error::{CliError, CliResult};

pub use catalog::Catalog;
pub use compose::{Composition, compose};
pub use spec::{SinkTemplate, SourceTemplate, Stream, TemplateKind, WriteChoice};

/// Default hub directory (relative to the working directory), used when it
/// exists and neither `--hub` nor `FAUCET_HUB` is set.
pub const DEFAULT_HUB_DIR: &str = "hub";
/// The public catalog (#677), used when no local hub is configured or present.
pub const DEFAULT_REMOTE_HUB: &str = "github:faucet-hq/template-hub";

/// Where a hub catalog lives: a local directory, or a directory in a GitHub
/// repository fetched through the contents API and cached locally.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HubLocation {
    Dir(PathBuf),
    Github {
        /// `owner/name`.
        repo: String,
        /// Branch, tag, or commit. Default `main`.
        r#ref: String,
        /// Directory inside the repo holding `source-templates/` etc. Empty =
        /// the repository root.
        path: String,
    },
}

impl HubLocation {
    /// Parse a `--hub` / `FAUCET_HUB` value.
    ///
    /// - `github:owner/repo[@ref][/path]`
    /// - `https://github.com/owner/repo[/tree/<ref>[/path]]`
    /// - anything else is a local directory.
    pub fn parse(raw: &str) -> CliResult<Self> {
        let raw = raw.trim();
        if let Some(rest) = raw.strip_prefix("github:") {
            return Self::parse_github_spec(rest);
        }
        for prefix in ["https://github.com/", "http://github.com/", "github.com/"] {
            if let Some(rest) = raw.strip_prefix(prefix) {
                let rest = rest.trim_end_matches('/').trim_end_matches(".git");
                let mut parts = rest.splitn(3, '/');
                let owner = parts.next().unwrap_or_default();
                let name = parts.next().unwrap_or_default();
                let tail = parts.next().unwrap_or_default();
                if owner.is_empty() || name.is_empty() {
                    return Err(CliError::Config(format!(
                        "hub '{raw}': a GitHub URL needs `owner/repo` after github.com/"
                    )));
                }
                let (r#ref, path) = match tail.strip_prefix("tree/") {
                    Some(t) => {
                        let (r, p) = t.split_once('/').unwrap_or((t, ""));
                        (r.to_string(), p.to_string())
                    }
                    None if tail.is_empty() => ("main".to_string(), String::new()),
                    None => {
                        return Err(CliError::Config(format!(
                            "hub '{raw}': only `/tree/<ref>[/path]` is understood after the repository"
                        )));
                    }
                };
                return Ok(Self::Github {
                    repo: format!("{owner}/{name}"),
                    r#ref,
                    path: path.trim_matches('/').to_string(),
                });
            }
        }
        Ok(Self::Dir(PathBuf::from(raw)))
    }

    fn parse_github_spec(rest: &str) -> CliResult<Self> {
        // owner/repo[@ref][/path]
        let (repo_and_ref, path) = match rest.find('/').and_then(|i| rest[i + 1..].find('/')) {
            Some(second) => {
                let first = rest.find('/').unwrap_or_default();
                let cut = first + 1 + second;
                (&rest[..cut], rest[cut + 1..].trim_matches('/'))
            }
            None => (rest, ""),
        };
        let (repo, r#ref) = match repo_and_ref.split_once('@') {
            Some((r, v)) if !v.is_empty() => (r, v),
            Some((_, _)) => {
                return Err(CliError::Config(format!(
                    "hub 'github:{rest}': empty ref after '@'"
                )));
            }
            None => (repo_and_ref, "main"),
        };
        if repo.split('/').filter(|s| !s.is_empty()).count() != 2 {
            return Err(CliError::Config(format!(
                "hub 'github:{rest}': expected `github:owner/repo[@ref][/path]`"
            )));
        }
        Ok(Self::Github {
            repo: repo.to_string(),
            r#ref: r#ref.to_string(),
            path: path.to_string(),
        })
    }

    pub fn is_remote(&self) -> bool {
        matches!(self, Self::Github { .. })
    }

    /// Human-readable form for messages.
    pub fn describe(&self) -> String {
        match self {
            Self::Dir(p) => p.display().to_string(),
            Self::Github { repo, r#ref, path } if path.is_empty() => format!("github:{repo}@{ref}"),
            Self::Github { repo, r#ref, path } => format!("github:{repo}@{ref}/{path}"),
        }
    }

    /// Stable filesystem-safe key for the cache directory.
    pub fn cache_key(&self) -> String {
        let raw = match self {
            Self::Dir(p) => format!("dir/{}", p.display()),
            Self::Github { repo, r#ref, path } => format!("github/{repo}/{ref}/{path}"),
        };
        raw.trim_end_matches('/')
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '/' || c == '.' || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect()
    }
}

/// Decide where the hub is: `--hub`, else `FAUCET_HUB`, else `./hub` when that
/// directory exists, else the public catalog ([`DEFAULT_REMOTE_HUB`]).
pub fn hub_location(flag: Option<&str>) -> CliResult<HubLocation> {
    if let Some(f) = flag {
        return HubLocation::parse(f);
    }
    if let Ok(env) = std::env::var("FAUCET_HUB")
        && !env.trim().is_empty()
    {
        return HubLocation::parse(&env);
    }
    if Path::new(DEFAULT_HUB_DIR).is_dir() {
        return Ok(HubLocation::Dir(PathBuf::from(DEFAULT_HUB_DIR)));
    }
    HubLocation::parse(DEFAULT_REMOTE_HUB)
}

/// Resolve the hub to a local directory, fetching (or reusing the cached
/// snapshot of) a remote one.
pub async fn resolve_hub(flag: Option<&str>) -> CliResult<PathBuf> {
    match hub_location(flag)? {
        HubLocation::Dir(p) => Ok(p),
        #[cfg(feature = "hub-remote")]
        loc @ HubLocation::Github { .. } => {
            remote::fetch_cached(&loc, &remote::cache_root(), "https://api.github.com").await
        }
        #[cfg(not(feature = "hub-remote"))]
        loc @ HubLocation::Github { .. } => Err(CliError::Config(format!(
            "hub {}: remote hubs need the `hub-remote` build feature; pass --hub <local directory> instead",
            loc.describe()
        ))),
    }
}

/// Read the `kind:` of a hub document without parsing the rest, so the
/// ordinary loaders can recognise a template handed to them by mistake.
pub fn detect_kind(value: &Value) -> Option<TemplateKind> {
    TemplateKind::parse(value.get("kind")?.as_str()?)
}

/// Peek at a config file: if it is a hub template, return its kind so the
/// caller can point the user at `--source` / `--sink` instead of failing on
/// "unknown field `kind`".
pub fn detect_kind_in_file(path: &Path) -> Option<TemplateKind> {
    let text = std::fs::read_to_string(path).ok()?;
    let value = parse_untyped(&text, path).ok()?;
    detect_kind(&value)
}

fn parse_untyped(text: &str, path: &Path) -> CliResult<Value> {
    let is_json = path
        .extension()
        .and_then(|e| e.to_str())
        .is_some_and(|e| e.eq_ignore_ascii_case("json"));
    if is_json {
        serde_json::from_str(text)
            .map_err(|e| CliError::Config(format!("{}: invalid JSON: {e}", path.display())))
    } else {
        serde_yaml::from_str(text)
            .map_err(|e| CliError::Config(format!("{}: invalid YAML: {e}", path.display())))
    }
}

fn read(path: &Path) -> CliResult<String> {
    std::fs::read_to_string(path)
        .map_err(|e| CliError::Config(format!("reading hub template {}: {e}", path.display())))
}

/// Parse + validate a `source-template` file.
pub fn parse_source_file(path: &Path) -> CliResult<SourceTemplate> {
    let value = parse_untyped(&read(path)?, path)?;
    match detect_kind(&value) {
        Some(TemplateKind::SourceTemplate) => {}
        Some(other) => {
            return Err(CliError::Config(format!(
                "{}: is a {}; `--source` needs a source-template",
                path.display(),
                other.as_str()
            )));
        }
        None => {
            return Err(CliError::Config(format!(
                "{}: not a hub template — a source template starts with `kind: source-template`",
                path.display()
            )));
        }
    }
    let t: SourceTemplate = serde_json::from_value(value)
        .map_err(|e| CliError::Config(format!("{}: {e}", path.display())))?;
    t.validate()
        .map_err(|e| CliError::Config(format!("{}: {e}", path.display())))?;
    Ok(t)
}

/// Parse + validate a `sink-template` file.
pub fn parse_sink_file(path: &Path) -> CliResult<SinkTemplate> {
    let value = parse_untyped(&read(path)?, path)?;
    match detect_kind(&value) {
        Some(TemplateKind::SinkTemplate) => {}
        Some(other) => {
            return Err(CliError::Config(format!(
                "{}: is a {}; `--sink` needs a sink-template",
                path.display(),
                other.as_str()
            )));
        }
        None => {
            return Err(CliError::Config(format!(
                "{}: not a hub template — a sink template starts with `kind: sink-template`",
                path.display()
            )));
        }
    }
    let t: SinkTemplate = serde_json::from_value(value)
        .map_err(|e| CliError::Config(format!("{}: {e}", path.display())))?;
    t.validate()
        .map_err(|e| CliError::Config(format!("{}: {e}", path.display())))?;
    Ok(t)
}

/// Every hub id under `base`: top-level stems plus `owner/stem` one level down.
fn known_ids(base: &Path) -> Vec<String> {
    let mut out = Vec::new();
    let Ok(rd) = std::fs::read_dir(base) else {
        return out;
    };
    for e in rd.flatten() {
        let p = e.path();
        let Some(n) = p.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if n.starts_with('.') {
            continue;
        }
        if p.is_dir() {
            if let Ok(sub) = std::fs::read_dir(&p) {
                for f in sub.flatten() {
                    if let Some(stem) = f.path().file_stem().and_then(|s| s.to_str())
                        && !stem.starts_with('.')
                        && f.path().is_file()
                    {
                        out.push(format!("{n}/{stem}"));
                    }
                }
            }
        } else if let Some(stem) = p.file_stem().and_then(|s| s.to_str()) {
            out.push(stem.to_string());
        }
    }
    out
}

/// A version selector on a hub locator: `id@stable` (the default), `id@newest`,
/// or `id@N` (#682). Versions are the catalog's — each accepted change to a
/// template is the next number — and live in the hub's `index.json`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HubVersion {
    Stable,
    Newest,
    Pinned(u32),
}

/// Split `id[@selector]`. A locator that is an existing file path is never
/// split.
pub fn split_selector(locator: &str) -> CliResult<(&str, Option<HubVersion>)> {
    if Path::new(locator).is_file() {
        return Ok((locator, None));
    }
    let Some((id, sel)) = locator.rsplit_once('@') else {
        return Ok((locator, None));
    };
    let v = match sel {
        "stable" => HubVersion::Stable,
        "newest" => HubVersion::Newest,
        n => match n.parse::<u32>() {
            Ok(n) if n > 0 => HubVersion::Pinned(n),
            _ => {
                return Err(CliError::Config(format!(
                    "hub locator '{locator}': `@{sel}` is not a version — use `@stable`, `@newest`, or `@<number>`"
                )));
            }
        },
    };
    Ok((id, Some(v)))
}

/// Turn a `--source` / `--sink` value into a file: an existing path is used
/// as-is; otherwise it is a hub id looked up as `<hub>/<subdir>/<id>.{yaml,yml,json}`
/// — `owner/name` resolves under `<subdir>/<owner>/`, a bare `name` at the top
/// level (the hub's official templates).
pub fn resolve_locator(locator: &str, hub: &Path, subdir: &str) -> CliResult<PathBuf> {
    let as_path = Path::new(locator);
    if as_path.is_file() {
        return Ok(as_path.to_path_buf());
    }
    let base = hub.join(subdir);
    let (owner, name) = spec::split_hub_id(locator);
    let dir = match owner {
        Some(o) => base.join(o),
        None => base.clone(),
    };
    for ext in ["yaml", "yml", "json"] {
        let candidate = dir.join(format!("{name}.{ext}"));
        if candidate.is_file() {
            return Ok(candidate);
        }
    }
    let mut known = known_ids(&base);
    known.sort();
    if let (None, Some(_)) = (
        owner,
        known.iter().find(|k| k.ends_with(&format!("/{name}"))),
    ) {
        let variants: Vec<&String> = known
            .iter()
            .filter(|k| k.ends_with(&format!("/{name}")))
            .collect();
        return Err(CliError::Config(format!(
            "no official hub template '{name}', but {} published one: {} — pick one with `--{} <owner>/{name}`",
            variants.len(),
            variants
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join(", "),
            if subdir == catalog::SOURCE_DIR {
                "source"
            } else {
                "sink"
            }
        )));
    }
    Err(CliError::Config(format!(
        "no hub template '{locator}': not a file, and {} has no {}{name}.yaml{}",
        base.display(),
        owner.map(|o| format!("{o}/")).unwrap_or_default(),
        if known.is_empty() {
            " (set --hub / FAUCET_HUB, or pass a path)".to_string()
        } else {
            format!(" (known: {})", known.join(", "))
        }
    )))
}

/// Load the source template named or pathed by `locator`.
pub async fn load_source(locator: &str, hub: &Path) -> CliResult<SourceTemplate> {
    parse_source_file(&locate(locator, hub, catalog::SOURCE_DIR).await?)
}

/// Load the sink template named or pathed by `locator`.
pub async fn load_sink(locator: &str, hub: &Path) -> CliResult<SinkTemplate> {
    parse_sink_file(&locate(locator, hub, catalog::SINK_DIR).await?)
}

/// Compose two locators into a [`Composition`].
pub async fn compose_locators(source: &str, sink: &str, hub: &Path) -> CliResult<Composition> {
    let s = load_source(source, hub).await?;
    let k = load_sink(sink, hub).await?;
    compose(&s, &k)
}

/// The per-template version history a catalog's `index.json` carries (#682):
/// written by the catalog's CI from git history, read here to honour
/// `@stable` / `@newest` / `@N`.
#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct IndexVersions {
    /// The catalog commit the snapshot's files come from.
    #[serde(default)]
    pub commit: Option<String>,
    #[serde(default)]
    pub sources: Vec<IndexEntry>,
    #[serde(default)]
    pub sinks: Vec<IndexEntry>,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct IndexEntry {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub stable: Option<u32>,
    #[serde(default)]
    pub newest: Option<u32>,
    #[serde(default)]
    pub versions: Vec<IndexVersion>,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct IndexVersion {
    pub version: u32,
    pub commit: String,
}

impl IndexVersions {
    /// `<hub>/index.json`, when the catalog ships one.
    pub fn load(hub: &Path) -> Option<Self> {
        let text = std::fs::read_to_string(hub.join("index.json")).ok()?;
        serde_json::from_str(&text).ok()
    }

    pub fn entry(&self, subdir: &str, id: &str) -> Option<&IndexEntry> {
        let list = if subdir == catalog::SOURCE_DIR {
            &self.sources
        } else {
            &self.sinks
        };
        list.iter()
            .find(|e| e.id == id || (e.id.is_empty() && e.name == id))
    }
}

impl IndexEntry {
    /// The commit a selector resolves to, when this entry carries history.
    pub fn commit_for(&self, sel: HubVersion) -> CliResult<Option<&IndexVersion>> {
        if self.versions.is_empty() {
            return Ok(None);
        }
        let want = match sel {
            HubVersion::Newest => self
                .newest
                .or_else(|| self.versions.iter().map(|v| v.version).max()),
            HubVersion::Stable => self
                .stable
                .or(self.newest)
                .or_else(|| self.versions.iter().map(|v| v.version).max()),
            HubVersion::Pinned(n) => Some(n),
        };
        let Some(want) = want else { return Ok(None) };
        self.versions
            .iter()
            .find(|v| v.version == want)
            .map(Some)
            .ok_or_else(|| {
                let have: Vec<String> = self
                    .versions
                    .iter()
                    .map(|v| v.version.to_string())
                    .collect();
                CliError::Config(format!(
                    "hub template '{}' has no version {want} (versions: {})",
                    self.id,
                    have.join(", ")
                ))
            })
    }
}

/// Resolve a locator to a file, honouring an `@selector` against the hub's
/// `index.json`. With no selector the catalog's **stable** version is used
/// when the index names one; a version whose commit is not the snapshot's is
/// fetched from the remote hub the snapshot was taken from (a local directory
/// hub has no history, so a selector is an error there).
pub async fn locate(locator: &str, hub: &Path, subdir: &str) -> CliResult<PathBuf> {
    let (id, sel) = split_selector(locator)?;
    let head = resolve_locator(id, hub, subdir);
    let index = IndexVersions::load(hub);
    let entry = index.as_ref().and_then(|i| i.entry(subdir, id));
    let target = match (entry, sel) {
        (Some(e), sel) => e.commit_for(sel.unwrap_or(HubVersion::Stable))?,
        (None, Some(_)) => {
            return Err(CliError::Config(format!(
                "hub locator '{locator}': this hub has no version history for '{id}' — selectors need a catalog whose index.json records versions (the public hub does); drop the `@…` to use the file as-is"
            )));
        }
        (None, None) => None,
    };
    let Some(target) = target else { return head };
    let snapshot_commit = index.as_ref().and_then(|i| i.commit.clone());
    if snapshot_commit.as_deref() == Some(target.commit.as_str()) {
        return head;
    }
    fetch_version(hub, subdir, id, target, head.ok().as_deref()).await
}

#[cfg(feature = "hub-remote")]
async fn fetch_version(
    hub: &Path,
    subdir: &str,
    id: &str,
    target: &IndexVersion,
    head_file: Option<&Path>,
) -> CliResult<PathBuf> {
    let loc = remote::snapshot_location(hub).ok_or_else(|| {
        CliError::Config(format!(
            "hub template '{id}' v{} lives at catalog commit {} — a local directory hub cannot fetch it; use a remote hub (`--hub github:…`)",
            target.version,
            &target.commit[..7.min(target.commit.len())]
        ))
    })?;
    let ext = head_file
        .and_then(|p| p.extension().and_then(|e| e.to_str()))
        .unwrap_or("yaml");
    let (owner, name) = spec::split_hub_id(id);
    let rel = match owner {
        Some(o) => format!("{subdir}/{o}/{name}.{ext}"),
        None => format!("{subdir}/{name}.{ext}"),
    };
    remote::fetch_file_at(
        &loc,
        &remote::cache_root(),
        "https://api.github.com",
        &target.commit,
        &rel,
    )
    .await
}

#[cfg(not(feature = "hub-remote"))]
async fn fetch_version(
    _hub: &Path,
    _subdir: &str,
    id: &str,
    target: &IndexVersion,
    _head_file: Option<&Path>,
) -> CliResult<PathBuf> {
    Err(CliError::Config(format!(
        "hub template '{id}' v{} lives at another catalog commit; fetching it needs the `hub-remote` build feature",
        target.version
    )))
}

/// The synthetic path a composed document is loaded under (error messages
/// and the YAML parser selection).
pub fn composed_path(source: &str, sink: &str) -> PathBuf {
    PathBuf::from(format!("<hub:{source}+{sink}>.yaml"))
}

/// Load a composition as a runnable [`PipelineConfig`] — the same
/// interpolation + params binding a file goes through, plus async secret
/// resolution — so `faucet run --source X --sink Y` is the ordinary run path
/// from here on.
pub async fn load_composed_async(c: &Composition, inputs: &RunInputs) -> CliResult<PipelineConfig> {
    let path = composed_path(&c.source, &c.sink);
    PipelineConfig::from_text_async_with(&c.to_yaml()?, &path, inputs).await
}

/// Offline variant (`faucet validate` / the catalog test): secret directives
/// are rejected rather than resolved.
pub fn load_composed(c: &Composition, inputs: &RunInputs) -> CliResult<PipelineConfig> {
    let path = composed_path(&c.source, &c.sink);
    PipelineConfig::from_text_with(&c.to_yaml()?, &path, inputs)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SRC: &str = "kind: source-template\nname: acme\nsource: {type: rest, config: {base_url: \"https://a\", path: /}}\nstreams: [{name: t}]\n";
    const SINK: &str = "kind: sink-template\nname: files\nsink: {type: jsonl, config: {}}\nper_stream: {path: \"./out/${stream}.jsonl\"}\n";

    fn hub() -> tempfile::TempDir {
        let d = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(d.path().join("source-templates")).unwrap();
        std::fs::create_dir_all(d.path().join("sink-templates")).unwrap();
        std::fs::write(d.path().join("source-templates/acme.yaml"), SRC).unwrap();
        std::fs::write(d.path().join("sink-templates/files.yml"), SINK).unwrap();
        d
    }

    #[test]
    fn hub_location_precedence_flag_env_local_default_remote() {
        // SAFETY (test): env var private to this test binary.
        unsafe { std::env::remove_var("FAUCET_HUB") };
        // No flag, no env: `./hub` exists in the repo checkout the tests run
        // from, so it wins; otherwise the public catalog does.
        let default = hub_location(None).unwrap();
        if Path::new(DEFAULT_HUB_DIR).is_dir() {
            assert_eq!(default, HubLocation::Dir(PathBuf::from("hub")));
        } else {
            assert_eq!(default, HubLocation::parse(DEFAULT_REMOTE_HUB).unwrap());
        }
        assert_eq!(
            hub_location(Some("/x")).unwrap(),
            HubLocation::Dir(PathBuf::from("/x"))
        );
        unsafe { std::env::set_var("FAUCET_HUB", "/from-env") };
        assert_eq!(
            hub_location(None).unwrap(),
            HubLocation::Dir(PathBuf::from("/from-env"))
        );
        assert_eq!(
            hub_location(Some("/flag")).unwrap(),
            HubLocation::Dir(PathBuf::from("/flag"))
        );
        unsafe { std::env::set_var("FAUCET_HUB", "github:acme/hub@v2/catalog") };
        assert!(hub_location(None).unwrap().is_remote());
        unsafe { std::env::remove_var("FAUCET_HUB") };
    }

    #[test]
    fn hub_location_parses_github_specs_and_urls() {
        let gh = |repo: &str, r: &str, path: &str| HubLocation::Github {
            repo: repo.into(),
            r#ref: r.into(),
            path: path.into(),
        };
        assert_eq!(
            HubLocation::parse("github:acme/hub").unwrap(),
            gh("acme/hub", "main", "")
        );
        assert_eq!(
            HubLocation::parse("github:acme/hub@v2").unwrap(),
            gh("acme/hub", "v2", "")
        );
        assert_eq!(
            HubLocation::parse("github:acme/hub@v2/catalog/hub").unwrap(),
            gh("acme/hub", "v2", "catalog/hub")
        );
        assert_eq!(
            HubLocation::parse("github:acme/hub/catalog").unwrap(),
            gh("acme/hub", "main", "catalog")
        );
        assert_eq!(
            HubLocation::parse("https://github.com/acme/hub").unwrap(),
            gh("acme/hub", "main", "")
        );
        assert_eq!(
            HubLocation::parse("https://github.com/acme/hub.git/").unwrap(),
            gh("acme/hub", "main", "")
        );
        assert_eq!(
            HubLocation::parse("https://github.com/acme/hub/tree/dev/catalog").unwrap(),
            gh("acme/hub", "dev", "catalog")
        );
        assert_eq!(
            HubLocation::parse("./hub").unwrap(),
            HubLocation::Dir(PathBuf::from("./hub"))
        );

        for bad in [
            "github:acme",
            "github:acme/hub@",
            "https://github.com/acme",
            "https://github.com/acme/hub/blob/main/x",
        ] {
            assert!(HubLocation::parse(bad).is_err(), "{bad} must be rejected");
        }

        let loc = gh("acme/hub", "v2", "catalog");
        assert_eq!(loc.describe(), "github:acme/hub@v2/catalog");
        assert_eq!(
            gh("acme/hub", "main", "").describe(),
            "github:acme/hub@main"
        );
        assert_eq!(loc.cache_key(), "github/acme/hub/v2/catalog");
        assert_eq!(
            gh("a/b", "feature/x", "").cache_key(),
            "github/a/b/feature/x"
        );
        assert!(!HubLocation::Dir(PathBuf::from("/x")).is_remote());
    }

    #[test]
    fn locators_accept_paths_and_ids_and_name_known_ids_on_miss() {
        let d = hub();
        let by_id = resolve_locator("acme", d.path(), "source-templates").unwrap();
        assert!(by_id.ends_with("source-templates/acme.yaml"));
        let by_yml = resolve_locator("files", d.path(), "sink-templates").unwrap();
        assert!(by_yml.ends_with("files.yml"));
        let by_path = resolve_locator(by_id.to_str().unwrap(), Path::new("/nowhere"), "x").unwrap();
        assert_eq!(by_path, by_id);
        let err = resolve_locator("nope", d.path(), "source-templates")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("no hub template 'nope'") && err.contains("known: acme"),
            "{err}"
        );
        let err = resolve_locator("nope", Path::new("/nowhere"), "source-templates")
            .unwrap_err()
            .to_string();
        assert!(err.contains("set --hub / FAUCET_HUB"), "{err}");
    }

    #[tokio::test]
    async fn parsers_check_kind_and_validate() {
        let d = hub();
        assert_eq!(load_source("acme", d.path()).await.unwrap().name, "acme");
        assert_eq!(load_sink("files", d.path()).await.unwrap().name, "files");
        // Wrong kind for the slot.
        let err = parse_source_file(&d.path().join("sink-templates/files.yml"))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("is a sink-template; `--source` needs a source-template"),
            "{err}"
        );
        let err = parse_sink_file(&d.path().join("source-templates/acme.yaml"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("is a source-template; `--sink`"), "{err}");
        // Not a template at all.
        let plain = d.path().join("plain.yaml");
        std::fs::write(&plain, "version: 1\npipeline: {}\n").unwrap();
        assert!(
            parse_source_file(&plain)
                .unwrap_err()
                .to_string()
                .contains("not a hub template")
        );
        assert!(
            parse_sink_file(&plain)
                .unwrap_err()
                .to_string()
                .contains("not a hub template")
        );
        assert_eq!(detect_kind_in_file(&plain), None);
        assert_eq!(
            detect_kind_in_file(&d.path().join("source-templates/acme.yaml")),
            Some(TemplateKind::SourceTemplate)
        );
        assert_eq!(detect_kind_in_file(Path::new("/nope.yaml")), None);
        // Broken YAML and JSON both surface the path.
        std::fs::write(&plain, ": : :").unwrap();
        assert!(
            parse_source_file(&plain)
                .unwrap_err()
                .to_string()
                .contains("invalid YAML")
        );
        let j = d.path().join("t.json");
        std::fs::write(&j, "{").unwrap();
        assert!(
            parse_sink_file(&j)
                .unwrap_err()
                .to_string()
                .contains("invalid JSON")
        );
        std::fs::write(&j, r#"{"kind":"sink-template","name":"j","sink":{"type":"jsonl","config":{}},"per_stream":{"path":"${stream}"}}"#).unwrap();
        assert_eq!(parse_sink_file(&j).unwrap().name, "j");
        assert!(
            parse_source_file(Path::new("/nope.yaml"))
                .unwrap_err()
                .to_string()
                .contains("reading hub template")
        );
    }

    const OWNED_SRC: &str = "kind: source-template\nname: acme\nowner: octo\nsource: {type: rest, config: {base_url: \"https://o\", path: /}}\nstreams: [{name: t}]\n";

    #[tokio::test]
    async fn owner_namespaces_resolve_and_ambiguity_is_named() {
        let d = hub();
        std::fs::create_dir_all(d.path().join("source-templates/octo")).unwrap();
        std::fs::write(d.path().join("source-templates/octo/acme.yaml"), OWNED_SRC).unwrap();
        // Same short name, two namespaces: both resolve, to different files.
        let official = resolve_locator("acme", d.path(), catalog::SOURCE_DIR).unwrap();
        let owned = resolve_locator("octo/acme", d.path(), catalog::SOURCE_DIR).unwrap();
        assert!(official.ends_with("source-templates/acme.yaml"));
        assert!(owned.ends_with("source-templates/octo/acme.yaml"));
        let t = load_source("octo/acme", d.path()).await.unwrap();
        assert_eq!(t.id(), "octo/acme");
        let c = compose_locators("octo/acme", "files", d.path())
            .await
            .unwrap();
        assert_eq!(
            c.name, "octo/acme",
            "the pipeline (and state-key prefix) is the full id"
        );
        assert_eq!(c.document["name"], serde_json::json!("octo/acme"));

        // No official template of that name, but a community one exists.
        std::fs::write(
            d.path().join("source-templates/octo/hr.yaml"),
            OWNED_SRC.replace("name: acme", "name: hr"),
        )
        .unwrap();
        let err = resolve_locator("hr", d.path(), catalog::SOURCE_DIR)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("no official hub template 'hr'") && err.contains("octo/hr"),
            "{err}"
        );
        let err = resolve_locator("octo/nope", d.path(), catalog::SOURCE_DIR)
            .unwrap_err()
            .to_string();
        assert!(err.contains("octo/nope.yaml"), "{err}");
    }

    #[test]
    fn selectors_split_off_the_locator() {
        assert_eq!(split_selector("acme").unwrap(), ("acme", None));
        assert_eq!(
            split_selector("octo/acme@stable").unwrap(),
            ("octo/acme", Some(HubVersion::Stable))
        );
        assert_eq!(
            split_selector("acme@newest").unwrap(),
            ("acme", Some(HubVersion::Newest))
        );
        assert_eq!(
            split_selector("acme@3").unwrap(),
            ("acme", Some(HubVersion::Pinned(3)))
        );
        for bad in ["acme@", "acme@0", "acme@latest", "acme@v3"] {
            assert!(split_selector(bad).is_err(), "{bad}");
        }
        // An existing file path is never split, whatever it contains.
        let d = tempfile::tempdir().unwrap();
        let f = d.path().join("x@2.yaml");
        std::fs::write(&f, "kind: sink-template\n").unwrap();
        assert_eq!(split_selector(f.to_str().unwrap()).unwrap().1, None);
    }

    #[tokio::test]
    async fn locate_honours_the_catalog_index_versions() {
        let d = hub();
        // No index: a selector is an error, no selector is the file.
        let err = locate("acme@2", d.path(), catalog::SOURCE_DIR)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("no version history"), "{err}");
        assert!(locate("acme", d.path(), catalog::SOURCE_DIR).await.is_ok());

        // An index whose stable version IS the snapshot commit → the file as-is.
        let index = serde_json::json!({
            "commit": "head000",
            "sources": [{"id": "acme", "name": "acme", "newest": 2, "stable": 2,
                          "versions": [{"version": 1, "commit": "old000"}, {"version": 2, "commit": "head000"}]}],
            "sinks": []
        });
        std::fs::write(d.path().join("index.json"), index.to_string()).unwrap();
        assert!(
            locate("acme", d.path(), catalog::SOURCE_DIR)
                .await
                .unwrap()
                .ends_with("acme.yaml")
        );
        assert!(
            locate("acme@newest", d.path(), catalog::SOURCE_DIR)
                .await
                .unwrap()
                .ends_with("acme.yaml")
        );
        // A version at another commit needs a remote hub; a directory hub says so.
        let err = locate("acme@1", d.path(), catalog::SOURCE_DIR)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("catalog commit old000") || err.contains("hub-remote"),
            "{err}"
        );
        let err = locate("acme@9", d.path(), catalog::SOURCE_DIR)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("no version 9") && err.contains("1, 2"),
            "{err}"
        );

        // `stable` behind `newest`: the default selector follows stable.
        let e = IndexEntry {
            id: "acme".into(),
            name: "acme".into(),
            stable: Some(1),
            newest: Some(2),
            versions: vec![
                IndexVersion {
                    version: 1,
                    commit: "a".into(),
                },
                IndexVersion {
                    version: 2,
                    commit: "b".into(),
                },
            ],
        };
        assert_eq!(
            e.commit_for(HubVersion::Stable).unwrap().unwrap().commit,
            "a"
        );
        assert_eq!(
            e.commit_for(HubVersion::Newest).unwrap().unwrap().commit,
            "b"
        );
        assert_eq!(
            e.commit_for(HubVersion::Pinned(2)).unwrap().unwrap().commit,
            "b"
        );
        let none = IndexEntry::default();
        assert!(none.commit_for(HubVersion::Stable).unwrap().is_none());
    }

    #[tokio::test]
    async fn compose_locators_yields_a_loadable_pipeline() {
        let d = hub();
        let c = compose_locators("acme", "files", d.path()).await.unwrap();
        assert_eq!(c.name, "acme");
        let cfg = load_composed(&c, &RunInputs::default()).unwrap();
        assert_eq!(cfg.name.as_deref(), Some("acme"));
        assert_eq!(cfg.matrix.len(), 1);
        assert_eq!(composed_path("a", "b"), PathBuf::from("<hub:a+b>.yaml"));
    }
}
