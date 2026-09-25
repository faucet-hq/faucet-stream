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
pub mod trust;

use std::path::{Path, PathBuf};

use serde_json::Value;

use crate::config::{PipelineConfig, RunInputs};
use crate::error::{CliError, CliResult};

pub use catalog::Catalog;
pub use compose::{Composition, compose};
pub use spec::{
    DeploymentTemplate, SinkTemplate, SourceTemplate, Stream, StreamOverlay, TemplateKind,
    WriteChoice,
};

/// Where a hub keeps deployment overlays (#679), beside `source-templates/`
/// and `sink-templates/`. Overlays are usually private to one deployment, so
/// most are passed as a file path instead.
pub const DEPLOYMENT_DIR: &str = "deployments";

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
    resolve_location(hub_location(flag)?).await
}

/// One hub in an ordered search list (#696): where it is, and its local
/// snapshot directory.
#[derive(Debug, Clone)]
pub struct ResolvedHub {
    pub location: HubLocation,
    pub dir: PathBuf,
}

/// The hubs to search, in order: each `--hub` value (commas separate several,
/// so `FAUCET_HUB` can list more than one), else the single default. The same
/// hub given twice is searched once.
pub fn hub_locations(flags: &[String]) -> CliResult<Vec<HubLocation>> {
    let given: Vec<&str> = flags
        .iter()
        .flat_map(|f| f.split(','))
        .map(str::trim)
        .filter(|f| !f.is_empty())
        .collect();
    if given.is_empty() {
        return Ok(vec![hub_location(None)?]);
    }
    let mut out: Vec<HubLocation> = Vec::new();
    for g in given {
        let loc = HubLocation::parse(g)?;
        if !out.iter().any(|o| o.cache_key() == loc.cache_key()) {
            out.push(loc);
        }
    }
    Ok(out)
}

/// Resolve every hub in `flags` to a local snapshot, in order.
pub async fn resolve_hubs(flags: &[String]) -> CliResult<Vec<ResolvedHub>> {
    let mut out = Vec::new();
    for location in hub_locations(flags)? {
        let dir = resolve_location(location.clone()).await?;
        out.push(ResolvedHub { location, dir });
    }
    Ok(out)
}

/// The hubs each side of a composition is looked up in (#696).
#[derive(Debug, Clone)]
pub struct HubSides {
    pub source: Vec<ResolvedHub>,
    pub sink: Vec<ResolvedHub>,
    pub overlay: Vec<ResolvedHub>,
}

/// A side's own `--source-hub` / `--sink-hub` / `--overlay-hub` wins;
/// otherwise it searches the `--hub` list. The shared list is resolved only
/// when some side needs it, so a run that names both sides' hubs never
/// touches the default hub.
pub async fn resolve_sides(
    hubs: &[String],
    source_hub: Option<&str>,
    sink_hub: Option<&str>,
    overlay_hub: Option<&str>,
) -> CliResult<HubSides> {
    let shared = if [source_hub, sink_hub, overlay_hub]
        .iter()
        .any(Option::is_none)
    {
        resolve_hubs(hubs).await?
    } else {
        Vec::new()
    };
    async fn side(flag: Option<&str>, shared: &[ResolvedHub]) -> CliResult<Vec<ResolvedHub>> {
        match flag {
            Some(f) => resolve_hubs(&[f.to_string()]).await,
            None => Ok(shared.to_vec()),
        }
    }
    Ok(HubSides {
        source: side(source_hub, &shared).await?,
        sink: side(sink_hub, &shared).await?,
        overlay: side(overlay_hub, &shared).await?,
    })
}

/// Split a hub-qualified locator, `<hub>:<id>` (#696): `<hub>` is a GitHub
/// hub spec (`github:owner/repo[@ref][/path]` or a github.com URL) or an
/// existing directory. Anything else — a plain id, a file path — is `None`.
pub fn split_qualified(locator: &str) -> Option<(HubLocation, &str)> {
    if Path::new(locator).is_file() {
        return None;
    }
    let (hub, id) = locator.rsplit_once(':')?;
    if id.is_empty() {
        return None;
    }
    let remote = hub.starts_with("github:") || hub.starts_with("https://github.com/");
    if !remote && !Path::new(hub).is_dir() {
        return None;
    }
    HubLocation::parse(hub).ok().map(|loc| (loc, id))
}

/// Find `locator` in `hubs`, in order, returning the file and the hub it came
/// from. A qualified locator goes straight to its own hub; a path is used as
/// is. When no hub has it, the error names every hub searched.
pub async fn locate_in(
    locator: &str,
    hubs: &[ResolvedHub],
    subdir: &str,
) -> CliResult<(PathBuf, Option<String>)> {
    if let Some((loc, id)) = split_qualified(locator) {
        let dir = resolve_location(loc.clone()).await?;
        return Ok((locate(id, &dir, subdir).await?, Some(loc.describe())));
    }
    if Path::new(locator).is_file() {
        return Ok((PathBuf::from(locator), None));
    }
    let mut misses = Vec::new();
    for h in hubs {
        let found = if subdir == DEPLOYMENT_DIR {
            resolve_locator(locator, &h.dir, subdir)
        } else {
            locate(locator, &h.dir, subdir).await
        };
        match found {
            Ok(p) => return Ok((p, Some(h.location.describe()))),
            Err(e) => misses.push((h.location.describe(), e.to_string())),
        }
    }
    match misses.len() {
        0 => Err(CliError::Config(format!(
            "no hub to look up '{locator}' in"
        ))),
        1 => Err(CliError::Config(misses.remove(0).1)),
        n => Err(CliError::Config(format!(
            "no hub template '{locator}' in any of the {n} hubs searched:\n{}",
            misses
                .iter()
                .map(|(hub, why)| format!("  - {hub}: {why}"))
                .collect::<Vec<_>>()
                .join("\n")
        ))),
    }
}

/// Compose a pairing whose sides may come from different hubs (#696),
/// recording where each came from.
pub async fn compose_across(
    source: &str,
    sink: &str,
    overlay: Option<&str>,
    sides: &HubSides,
) -> CliResult<Composition> {
    let (source_file, source_hub) = locate_in(source, &sides.source, catalog::SOURCE_DIR).await?;
    let (sink_file, sink_hub) = locate_in(sink, &sides.sink, catalog::SINK_DIR).await?;
    let s = parse_source_file(&source_file)?;
    let k = parse_sink_file(&sink_file)?;
    let mut c = compose(&s, &k)?;
    c.source_hub = source_hub;
    c.sink_hub = sink_hub;
    if let Some(o) = overlay {
        let (file, hub) = locate_in(o, &sides.overlay, DEPLOYMENT_DIR).await?;
        c = c.apply_overlay(&parse_deployment_file(&file)?)?;
        c.overlay_hub = hub;
    }
    Ok(c)
}

/// Resolve one hub location to a local directory.
pub async fn resolve_location(location: HubLocation) -> CliResult<PathBuf> {
    match location {
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

/// Community variants of one name, best first by the catalog's trust
/// signals (#685), each with its summary: `octo/hr (★ 12 · updated …), acme/hr`.
fn ranked_variants(variants: &[&String], index: Option<&IndexVersions>, subdir: &str) -> String {
    let entries: Vec<(&str, Option<&IndexEntry>)> = variants
        .iter()
        .map(|id| (id.as_str(), index.and_then(|i| i.entry(subdir, id))))
        .collect();
    let mut cands: Vec<(trust::Candidate<'_>, String)> = entries
        .iter()
        .map(|(id, e)| {
            let t = e.and_then(|e| e.trust.as_ref());
            let summary = t.map(|t| t.summary()).unwrap_or_default();
            (
                trust::Candidate {
                    id,
                    official: e.is_some_and(|e| e.official),
                    trust: t,
                },
                summary,
            )
        })
        .collect();
    cands.sort_by(|a, b| trust::rank(&a.0, &b.0));
    cands
        .iter()
        .map(|(c, summary)| {
            if summary.is_empty() {
                c.id.to_string()
            } else {
                format!("{} ({summary})", c.id)
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
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
                        && is_template_file(&f.path())
                    {
                        out.push(format!("{n}/{stem}"));
                    }
                }
            }
        } else if let Some(stem) = p.file_stem().and_then(|s| s.to_str())
            && is_template_file(&p)
        {
            out.push(stem.to_string());
        }
    }
    out
}

/// A template document — not a namespace's `OWNERS` file or a `*.faucet.yaml` sidecar.
fn is_template_file(p: &Path) -> bool {
    let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
    p.is_file()
        && matches!(
            p.extension().and_then(|e| e.to_str()),
            Some("yaml" | "yml" | "json")
        )
        && !name.contains(".faucet.")
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
/// — `owner/name` resolves under `<subdir>/<owner>/`; a bare `name` resolves
/// to a top-level file, else to the hub's official `faucet-hq/` namespace.
pub fn resolve_locator(locator: &str, hub: &Path, subdir: &str) -> CliResult<PathBuf> {
    let as_path = Path::new(locator);
    if as_path.is_file() {
        return Ok(as_path.to_path_buf());
    }
    let base = hub.join(subdir);
    let (owner, name) = spec::split_hub_id(locator);
    // `owner/name` → the owner directory. A bare `name` → a top-level file
    // (an unscoped template), else the hub's official namespace.
    let dirs: Vec<PathBuf> = match owner {
        Some(o) => vec![base.join(o)],
        None => vec![base.clone(), base.join(spec::OFFICIAL_OWNER)],
    };
    for dir in &dirs {
        for ext in ["yaml", "yml", "json"] {
            let candidate = dir.join(format!("{name}.{ext}"));
            if candidate.is_file() {
                return Ok(candidate);
            }
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
            "no hub template '{name}' at the top level or under {}/, but {} published one: {} — pick one with `--{} <owner>/{name}`",
            spec::OFFICIAL_OWNER,
            variants.len(),
            ranked_variants(&variants, IndexVersions::load(hub).as_ref(), subdir),
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
    compose_locators_overlaid(source, sink, None, hub).await
}

/// [`compose_locators`] plus an optional deployment overlay (#679), applied last.
pub async fn compose_locators_overlaid(
    source: &str,
    sink: &str,
    overlay: Option<&str>,
    hub: &Path,
) -> CliResult<Composition> {
    let s = load_source(source, hub).await?;
    let k = load_sink(sink, hub).await?;
    let c = compose(&s, &k)?;
    match overlay {
        Some(o) => c.apply_overlay(&load_deployment(o, hub)?),
        None => Ok(c),
    }
}

/// The error for a hub document handed to `run` / `validate` as a plain
/// config: say what it is and how to use it instead. `None` for a pipeline.
pub fn misplaced_document(path: &Path, verb: &str) -> Option<String> {
    let kind = detect_kind_in_file(path)?;
    let (what, how) = match kind {
        TemplateKind::SourceTemplate | TemplateKind::SinkTemplate => (
            format!("hub {}", kind.as_str()),
            format!(
                "compose it: `faucet {verb} --source <source-template> --sink <sink-template>`"
            ),
        ),
        TemplateKind::Deployment => (
            "deployment overlay".to_string(),
            format!(
                "apply it over a pairing: `faucet {verb} --source <source-template> --sink <sink-template> --overlay {}`",
                path.display()
            ),
        ),
        TemplateKind::Pipeline => return None,
    };
    Some(format!("{} is a {what} — {how}", path.display()))
}

/// Parse + validate a `kind: deployment` file (#679).
pub fn parse_deployment_file(path: &Path) -> CliResult<DeploymentTemplate> {
    let value = parse_untyped(&read(path)?, path)?;
    match detect_kind(&value) {
        Some(TemplateKind::Deployment) => {}
        Some(other) => {
            return Err(CliError::Config(format!(
                "{}: is a {}; `--overlay` needs a deployment",
                path.display(),
                other.as_str()
            )));
        }
        None => {
            return Err(CliError::Config(format!(
                "{}: not a deployment overlay — it starts with `kind: deployment`",
                path.display()
            )));
        }
    }
    DeploymentTemplate::from_value(value)
        .map_err(|e| CliError::Config(format!("{}: {e}", path.display())))
}

/// Load the deployment overlay named or pathed by `locator`: a file, or an id
/// under `<hub>/deployments/`.
pub fn load_deployment(locator: &str, hub: &Path) -> CliResult<DeploymentTemplate> {
    parse_deployment_file(&resolve_locator(locator, hub, DEPLOYMENT_DIR)?)
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
    #[serde(default)]
    pub official: bool,
    /// Stars, freshness and track record, when the catalog records them (#685).
    #[serde(default)]
    pub trust: Option<trust::TrustSignals>,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct IndexVersion {
    pub version: u32,
    pub commit: String,
    /// Retired by the publisher (#691): still resolvable by an explicit `@N`,
    /// with a warning, but never chosen by `@newest` and hidden from listings.
    #[serde(default)]
    pub deprecated: bool,
    /// Why, and usually which version to use instead.
    #[serde(default)]
    pub reason: Option<String>,
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
    /// The highest version this entry records — the body the catalog's files hold.
    pub fn newest_version(&self) -> Option<u32> {
        self.versions
            .iter()
            .map(|v| v.version)
            .max()
            .or(self.newest)
    }

    /// The highest version the publisher has not deprecated (#691) — what
    /// `@newest` resolves to.
    pub fn newest_live_version(&self) -> Option<u32> {
        if self.versions.is_empty() {
            return self.newest;
        }
        self.versions
            .iter()
            .filter(|v| !v.deprecated)
            .map(|v| v.version)
            .max()
    }

    /// The versions a listing should offer: every one not deprecated.
    pub fn live_versions(&self) -> Vec<u32> {
        let mut v: Vec<u32> = self
            .versions
            .iter()
            .filter(|v| !v.deprecated)
            .map(|v| v.version)
            .collect();
        v.sort_unstable();
        v
    }

    /// The warning for resolving a deprecated version, or `None` when it is live.
    pub fn deprecation_warning(&self, v: &IndexVersion) -> Option<String> {
        if !v.deprecated {
            return None;
        }
        let id = if self.id.is_empty() {
            &self.name
        } else {
            &self.id
        };
        let mut msg = format!("warning: {id} v{} is deprecated", v.version);
        if let Some(reason) = v.reason.as_deref().filter(|r| !r.trim().is_empty()) {
            msg.push_str(&format!(": {reason}"));
        }
        if let Some(stable) = self.stable.filter(|s| *s != v.version) {
            msg.push_str(&format!(" — stable is v{stable}"));
        }
        Some(msg)
    }

    /// The commit a selector resolves to, when this entry carries history.
    pub fn commit_for(&self, sel: HubVersion) -> CliResult<Option<&IndexVersion>> {
        if self.versions.is_empty() {
            return Ok(None);
        }
        let want = match sel {
            HubVersion::Newest => match self.newest_live_version() {
                Some(v) => Some(v),
                None => {
                    return Err(CliError::Config(format!(
                        "hub template '{}' has no live version: every version is deprecated",
                        self.id
                    )));
                }
            },
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
    let (given, sel) = split_selector(locator)?;
    let head = resolve_locator(given, hub, subdir);
    let id = canonical_id(given, head.as_deref().ok(), hub, subdir);
    let id = id.as_str();
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
    if let Some(warning) = entry.and_then(|e| e.deprecation_warning(target)) {
        tracing::warn!(template = %id, version = target.version, "{warning}");
        eprintln!("{warning}");
    }
    // The checkout's file is the newest version's body (versions are deduped by
    // body), whatever commit the index was later regenerated at (#688).
    let newest = entry.and_then(IndexEntry::newest_version);
    let snapshot_commit = index.as_ref().and_then(|i| i.commit.clone());
    if newest == Some(target.version) || snapshot_commit.as_deref() == Some(target.commit.as_str())
    {
        return head;
    }
    fetch_version(hub, subdir, id, target, head.ok().as_deref()).await
}

/// The id a locator names once the official-namespace shorthand is applied: a
/// bare name that did not resolve to a top-level file is `faucet-hq/<name>`,
/// which is how the catalog's `index.json` keys it.
fn canonical_id(given: &str, head: Option<&Path>, hub: &Path, subdir: &str) -> String {
    let (owner, name) = spec::split_hub_id(given);
    if owner.is_some() || Path::new(given).is_file() {
        return given.to_string();
    }
    let top_level = head.is_some_and(|p| p.parent() == Some(hub.join(subdir).as_path()));
    if top_level {
        given.to_string()
    } else {
        spec::hub_id(Some(spec::OFFICIAL_OWNER), name)
    }
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
    fn hub_lists_split_on_commas_keep_order_and_drop_repeats() {
        let locs = hub_locations(&[
            "github:acme/private, github:faucet-hq/template-hub".into(),
            "github:acme/private".into(),
        ])
        .unwrap();
        assert_eq!(
            locs.iter().map(HubLocation::describe).collect::<Vec<_>>(),
            vec![
                "github:acme/private@main",
                "github:faucet-hq/template-hub@main"
            ]
        );
        assert_eq!(
            hub_locations(&[]).unwrap().len(),
            1,
            "the default when none is given"
        );
        assert!(hub_locations(&["github:".into()]).is_err());
    }

    #[test]
    fn qualified_locators_split_only_on_a_real_hub() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path().display().to_string();
        let (loc, id) = split_qualified("github:acme/private-hub:acme/netsuite@3").unwrap();
        assert_eq!(
            (loc.describe().as_str(), id),
            ("github:acme/private-hub@main", "acme/netsuite@3")
        );
        let qualified = format!("{d}:files");
        let (loc, id) = split_qualified(&qualified).unwrap();
        assert_eq!((loc, id), (HubLocation::Dir(dir.path().into()), "files"));
        for plain in [
            "acme/netsuite",
            "github:acme/hub",
            "nowhere:files",
            "https://x.y/a:b",
            "github:acme/hub:",
        ] {
            assert!(split_qualified(plain).is_none(), "{plain}");
        }
        let file = dir.path().join("a:b.yaml");
        std::fs::write(&file, "x").unwrap();
        assert!(
            split_qualified(file.to_str().unwrap()).is_none(),
            "a file path is never split"
        );
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
            err.contains("no hub template 'hr'")
                && err.contains("faucet-hq/")
                && err.contains("octo/hr"),
            "{err}"
        );
        // A bare name falls through to the official faucet-hq namespace.
        std::fs::create_dir_all(d.path().join("source-templates/faucet-hq")).unwrap();
        std::fs::write(
            d.path().join("source-templates/faucet-hq/crm.yaml"),
            OWNED_SRC
                .replace("name: acme", "name: crm")
                .replace("owner: octo", "owner: faucet-hq"),
        )
        .unwrap();
        let crm = resolve_locator("crm", d.path(), catalog::SOURCE_DIR).unwrap();
        assert!(crm.ends_with("source-templates/faucet-hq/crm.yaml"));
        assert!(load_source("crm", d.path()).await.unwrap().is_official());
        let err = resolve_locator("octo/nope", d.path(), catalog::SOURCE_DIR)
            .unwrap_err()
            .to_string();
        assert!(err.contains("octo/nope.yaml"), "{err}");
    }

    #[test]
    fn ambiguous_names_list_variants_best_first_with_their_trust() {
        let d = hub();
        for owner in ["acme", "octo", "zed"] {
            let dir = d.path().join(format!("source-templates/{owner}"));
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(
                dir.join("hr.yaml"),
                OWNED_SRC
                    .replace("name: acme", "name: hr")
                    .replace("owner: octo", &format!("owner: {owner}")),
            )
            .unwrap();
            // Neither a namespace's OWNERS file nor a sidecar is a template.
            std::fs::write(dir.join("OWNERS"), "owners: []\n").unwrap();
            std::fs::write(dir.join("hr.faucet.yaml"), "launch: true\n").unwrap();
        }
        let index = serde_json::json!({
            "sources": [
                {"id": "acme/hr", "name": "hr", "trust": {"stars": 3, "updated": "2026-09-01"}},
                {"id": "octo/hr", "name": "hr", "trust": {"stars": 40, "updated": "2026-01-01", "open_issues": 2}},
                {"id": "zed/hr", "name": "hr"}
            ],
            "sinks": []
        });
        std::fs::write(d.path().join("index.json"), index.to_string()).unwrap();
        let err = resolve_locator("hr", d.path(), catalog::SOURCE_DIR)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains(
                "3 published one: octo/hr (★ 40 · updated 2026-01-01 · 2 open issues), acme/hr (★ 3 · updated 2026-09-01), zed/hr"
            ),
            "{err}"
        );
        let known = known_ids(&d.path().join("source-templates"));
        assert!(
            known
                .iter()
                .all(|k| !k.ends_with("OWNERS") && !k.contains(".faucet")),
            "{known:?}"
        );
        assert!(known.contains(&"octo/hr".to_string()));
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
    async fn locate_keys_the_index_by_the_official_id() {
        let d = hub();
        std::fs::create_dir_all(d.path().join("source-templates/faucet-hq")).unwrap();
        std::fs::write(
            d.path().join("source-templates/faucet-hq/crm.yaml"),
            OWNED_SRC
                .replace("name: acme", "name: crm")
                .replace("owner: octo", "owner: faucet-hq"),
        )
        .unwrap();
        let index = serde_json::json!({
            "commit": "head000",
            "sources": [{"id": "faucet-hq/crm", "name": "crm", "newest": 2, "stable": 2,
                          "versions": [{"version": 1, "commit": "old000"}, {"version": 2, "commit": "head000"}]}],
            "sinks": []
        });
        std::fs::write(d.path().join("index.json"), index.to_string()).unwrap();
        // The bare shorthand finds the faucet-hq entry: stable is the snapshot file…
        assert!(
            locate("crm", d.path(), catalog::SOURCE_DIR)
                .await
                .unwrap()
                .ends_with("source-templates/faucet-hq/crm.yaml")
        );
        // …and an older version is looked up under the full id.
        let err = locate("crm@1", d.path(), catalog::SOURCE_DIR)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("'faucet-hq/crm' v1"), "{err}");
        // A top-level (unscoped) file keeps its bare id.
        assert_eq!(
            canonical_id(
                "acme",
                Some(&d.path().join("source-templates/acme.yaml")),
                d.path(),
                catalog::SOURCE_DIR
            ),
            "acme"
        );
        assert_eq!(
            canonical_id("gone", None, d.path(), catalog::SOURCE_DIR),
            "faucet-hq/gone"
        );
    }

    #[tokio::test]
    async fn the_checkout_file_is_the_newest_version_whatever_commit_the_index_names() {
        let d = hub();
        // Regenerated at a later commit than the template's last change (#688).
        let index = serde_json::json!({
            "commit": "later000",
            "sources": [{"id": "acme", "name": "acme", "newest": 2, "stable": 2,
                          "versions": [{"version": 1, "commit": "old000"}, {"version": 2, "commit": "mid000"}]}],
            "sinks": []
        });
        std::fs::write(d.path().join("index.json"), index.to_string()).unwrap();
        for loc in ["acme", "acme@stable", "acme@newest", "acme@2"] {
            assert!(
                locate(loc, d.path(), catalog::SOURCE_DIR)
                    .await
                    .unwrap_or_else(|e| panic!("{loc}: {e}"))
                    .ends_with("acme.yaml"),
                "{loc}"
            );
        }
        // An older version still needs its own commit's body.
        let err = locate("acme@1", d.path(), catalog::SOURCE_DIR)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("old000") || err.contains("hub-remote"),
            "{err}"
        );
        let e = IndexEntry {
            newest: Some(7),
            ..Default::default()
        };
        assert_eq!(e.newest_version(), Some(7));
        assert_eq!(IndexEntry::default().newest_version(), None);
    }

    #[tokio::test]
    async fn locate_resolves_a_pinned_deprecated_version_and_newest_skips_it() {
        // #691: the head file is v2, which the publisher deprecated; stable is v1.
        let d = hub();
        let index = serde_json::json!({
            "commit": "head000",
            "sources": [{"id": "acme", "name": "acme", "newest": 2, "stable": 1,
                          "versions": [{"version": 1, "commit": "old000"},
                                       {"version": 2, "commit": "head000", "deprecated": true, "reason": "broken"}]}],
            "sinks": []
        });
        std::fs::write(d.path().join("index.json"), index.to_string()).unwrap();
        // An explicit pin still resolves (the warning goes to stderr).
        assert!(
            locate("acme@2", d.path(), catalog::SOURCE_DIR)
                .await
                .unwrap()
                .ends_with("acme.yaml")
        );
        // `@newest` is v1 now, which a directory hub cannot fetch — proving it
        // did not pick the deprecated head.
        let err = locate("acme@newest", d.path(), catalog::SOURCE_DIR)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("old000") || err.contains("hub-remote"),
            "{err}"
        );
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
                    ..Default::default()
                },
                IndexVersion {
                    version: 2,
                    commit: "b".into(),
                    ..Default::default()
                },
            ],
            ..Default::default()
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

        // #691: deprecated versions — `@newest` skips them, an explicit pin
        // still resolves (with a warning), and live_versions omits them.
        let v = |n: u32, dep: bool| IndexVersion {
            version: n,
            commit: format!("c{n}"),
            deprecated: dep,
            reason: dep.then(|| format!("v{n} drops a stream")),
        };
        let e = IndexEntry {
            id: "acme/erp".into(),
            stable: Some(2),
            newest: Some(3),
            versions: vec![v(1, true), v(2, false), v(3, true)],
            ..Default::default()
        };
        assert_eq!(e.newest_live_version(), Some(2));
        assert_eq!(e.live_versions(), vec![2]);
        assert_eq!(
            e.commit_for(HubVersion::Newest).unwrap().unwrap().commit,
            "c2"
        );
        let pinned = e.commit_for(HubVersion::Pinned(1)).unwrap().unwrap();
        assert_eq!(pinned.commit, "c1");
        assert_eq!(
            e.deprecation_warning(pinned).as_deref(),
            Some("warning: acme/erp v1 is deprecated: v1 drops a stream — stable is v2")
        );
        assert!(e.deprecation_warning(&v(2, false)).is_none());
        // No reason, and the deprecated version is stable itself: no dangling clauses.
        let lone = IndexEntry {
            name: "solo".into(),
            stable: Some(1),
            versions: vec![IndexVersion {
                version: 1,
                commit: "x".into(),
                deprecated: true,
                reason: None,
            }],
            ..Default::default()
        };
        assert_eq!(
            lone.deprecation_warning(&lone.versions[0]).as_deref(),
            Some("warning: solo v1 is deprecated")
        );
        let err = lone.commit_for(HubVersion::Newest).unwrap_err().to_string();
        assert!(err.contains("every version is deprecated"), "{err}");
        // An entry with no recorded history falls back to its `newest` field.
        let bare = IndexEntry {
            newest: Some(4),
            ..Default::default()
        };
        assert_eq!(bare.newest_live_version(), Some(4));
        // Old index.json without the fields still parses.
        let parsed: IndexVersion =
            serde_json::from_value(serde_json::json!({"version": 1, "commit": "a"})).unwrap();
        assert!(!parsed.deprecated && parsed.reason.is_none());
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

    const OVERLAY: &str = "kind: deployment\nname: prod\nstate: { type: memory }\n";

    #[tokio::test]
    async fn overlays_load_from_a_path_or_the_hub_and_apply_over_a_pairing() {
        let d = hub();
        std::fs::create_dir_all(d.path().join("deployments")).unwrap();
        std::fs::write(d.path().join("deployments/prod.yaml"), OVERLAY).unwrap();
        let by_id = compose_locators_overlaid("acme", "files", Some("prod"), d.path())
            .await
            .unwrap();
        assert_eq!(by_id.overlay.as_deref(), Some("prod"));
        assert_eq!(
            by_id.document["pipeline"]["state"]["type"],
            serde_json::json!("memory")
        );
        let path = d.path().join("deployments/prod.yaml");
        let by_path =
            compose_locators_overlaid("acme", "files", Some(path.to_str().unwrap()), d.path())
                .await
                .unwrap();
        assert_eq!(by_path.overlay_contributes, vec!["pipeline.state"]);
        // The composed + overlaid document is still a loadable pipeline.
        load_composed(&by_path, &RunInputs::default()).unwrap();
        let plain = compose_locators("acme", "files", d.path()).await.unwrap();
        assert!(plain.overlay.is_none());
    }

    #[test]
    fn a_deployment_file_is_checked_for_its_kind() {
        let d = hub();
        let err = parse_deployment_file(&d.path().join("source-templates/acme.yaml"))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("is a source-template") && err.contains("--overlay"),
            "{err}"
        );
        let plain = d.path().join("plain.yaml");
        std::fs::write(&plain, "version: 1\npipeline: {}\n").unwrap();
        let err = parse_deployment_file(&plain).unwrap_err().to_string();
        assert!(err.contains("kind: deployment"), "{err}");
        let bad = d.path().join("bad.yaml");
        std::fs::write(&bad, "kind: deployment\nname: x\nmatrix: []\n").unwrap();
        let err = parse_deployment_file(&bad).unwrap_err().to_string();
        assert!(
            err.contains("bad.yaml") && err.contains("cannot be set here"),
            "{err}"
        );
    }

    #[test]
    fn misplaced_documents_point_at_the_right_flag() {
        let d = hub();
        let src = d.path().join("source-templates/acme.yaml");
        let msg = misplaced_document(&src, "run").unwrap();
        assert!(
            msg.contains("is a hub source-template") && msg.contains("faucet run --source"),
            "{msg}"
        );
        let ov = d.path().join("prod.yaml");
        std::fs::write(&ov, OVERLAY).unwrap();
        let msg = misplaced_document(&ov, "validate").unwrap();
        assert!(
            msg.contains("deployment overlay") && msg.contains("--overlay"),
            "{msg}"
        );
        let plain = d.path().join("p.yaml");
        std::fs::write(&plain, "kind: pipeline\nversion: 1\n").unwrap();
        assert!(misplaced_document(&plain, "run").is_none());
        assert!(misplaced_document(Path::new("/nope.yaml"), "run").is_none());
    }
}
