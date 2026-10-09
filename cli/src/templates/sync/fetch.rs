//! Reading (and, for `publish`, writing) template files at an origin.
//!
//! Two thin I/O adapters — the GitHub contents API and `object_store` — behind
//! one [`Fetcher`] / [`Publisher`] pair, plus the pure [`pair_files`] step that
//! turns a flat directory listing into templates + their sidecars. Everything
//! that decides *what* a file means lives in `pair_files`, so the adapters only
//! move bytes.
//!
//! Layout contract (RFC 0006): the template id is the file **stem**; a sidecar
//! is `<stem>.faucet.yaml` (or `.yml` / `.json`) beside it. Only the direct
//! children of the configured directory are read — a README or a nested
//! folder is ignored.

use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt as _;
use serde::Serialize;

use super::spec::{GithubSource, OriginSource, Sidecar};
use crate::error::{CliError, CliResult};
use crate::serve::load::ConfigFormat;

/// How many files are fetched concurrently from one origin.
const FETCH_CONCURRENCY: usize = 8;

/// One file read from an origin: its basename and UTF-8 body.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RemoteFile {
    pub name: String,
    pub body: String,
    /// Set when the file is listed but could not be read (transient error,
    /// not UTF-8). The template it belongs to is skipped, not treated as gone.
    pub error: Option<String>,
}

/// A template as the planner sees it: the id stem (before the origin's
/// prefix), its body, and the parsed sidecar if one was present.
#[derive(Debug, Clone, PartialEq)]
pub struct RemoteTemplate {
    pub stem: String,
    pub body: String,
    pub format: ConfigFormat,
    pub sidecar: Option<Sidecar>,
    /// Set when the origin's catalog marks this body's version deprecated
    /// (#691): the reason, and the planner skips the template.
    pub retired: Option<String>,
    /// Set when the template is upstream but cannot be used this pull (an
    /// unreadable file, an invalid sidecar, an ambiguous stem): the planner
    /// reports it and still counts it as present, so it is never pruned.
    pub unusable: Option<String>,
}

impl RemoteTemplate {
    fn unusable(stem: String, why: String) -> Self {
        Self {
            stem,
            body: String::new(),
            format: ConfigFormat::Yaml,
            sidecar: None,
            retired: None,
            unusable: Some(why),
        }
    }
}

/// Result of pairing a directory listing.
#[derive(Debug, Default, Clone, PartialEq, Serialize)]
pub struct Paired {
    #[serde(skip)]
    pub templates: Vec<RemoteTemplate>,
    /// Files that were recognized but could not be used (broken sidecar,
    /// duplicate stem, sidecar without a template). Surfaced in the report.
    pub warnings: Vec<String>,
}

const SIDECAR_SUFFIXES: [&str; 3] = [".faucet.yaml", ".faucet.yml", ".faucet.json"];

/// Whether a basename is one the sync reads at all (template or sidecar).
pub fn is_candidate(name: &str) -> bool {
    template_format(name).is_some() || sidecar_stem(name).is_some()
}

fn basename(name: &str) -> &str {
    name.rsplit('/').next().unwrap_or(name)
}

fn template_format(name: &str) -> Option<ConfigFormat> {
    let lower = name.to_ascii_lowercase();
    if basename(&lower).starts_with('.') || sidecar_stem(&lower).is_some() {
        return None;
    }
    if lower.ends_with(".yaml") || lower.ends_with(".yml") {
        Some(ConfigFormat::Yaml)
    } else if lower.ends_with(".json") {
        Some(ConfigFormat::Json)
    } else {
        None
    }
}

fn sidecar_stem(name: &str) -> Option<&str> {
    if basename(name).starts_with('.') {
        return None;
    }
    SIDECAR_SUFFIXES
        .iter()
        .find_map(|s| name.strip_suffix(s).filter(|stem| !stem.is_empty()))
}

fn strip_ext(name: &str) -> &str {
    name.rsplit_once('.').map(|(stem, _)| stem).unwrap_or(name)
}

/// Pair templates with their sidecars. Pure.
///
/// A template that cannot be used this pull (its file or sidecar unreadable,
/// its sidecar invalid, two files claiming its stem) is returned with
/// `RemoteTemplate::unusable` set, so the planner keeps it out of the
/// orphan set instead of deprecating a live template over a typo.
pub fn pair_files(files: Vec<RemoteFile>) -> Paired {
    use std::collections::BTreeMap;
    type Entry = (String, ConfigFormat, String, Option<String>);
    let mut templates: BTreeMap<String, Vec<Entry>> = BTreeMap::new();
    let mut sidecars: BTreeMap<String, (String, String, Option<String>)> = BTreeMap::new();
    let mut warnings = Vec::new();

    for f in files {
        if let Some(stem) = sidecar_stem(&f.name) {
            if let Some((prev, _, _)) =
                sidecars.insert(stem.to_string(), (f.name.clone(), f.body, f.error))
            {
                warnings.push(format!(
                    "'{stem}': two sidecars ('{prev}' and '{}'); using '{}'",
                    f.name, f.name
                ));
            }
        } else if let Some(fmt) = template_format(&f.name) {
            templates
                .entry(strip_ext(&f.name).to_string())
                .or_default()
                .push((f.name, fmt, f.body, f.error));
        }
    }

    let mut out = Vec::new();
    for (stem, mut entries) in templates {
        if entries.len() > 1 {
            let names: Vec<&str> = entries.iter().map(|(n, _, _, _)| n.as_str()).collect();
            let why = format!(
                "'{stem}': ambiguous — {} all name the same template; skipped",
                names.join(", ")
            );
            warnings.push(why.clone());
            sidecars.remove(&stem);
            out.push(RemoteTemplate::unusable(stem, why));
            continue;
        }
        let (name, format, body, error) = entries.pop().expect("one entry");
        if let Some(e) = error {
            let why = format!("'{name}' could not be read ({e}); template skipped");
            warnings.push(why.clone());
            sidecars.remove(&stem);
            out.push(RemoteTemplate::unusable(stem, why));
            continue;
        }
        let sidecar = match sidecars.remove(&stem) {
            None => None,
            Some((name, _, Some(e))) => {
                let why =
                    format!("'{stem}': sidecar '{name}' could not be read ({e}); template skipped");
                warnings.push(why.clone());
                out.push(RemoteTemplate::unusable(stem, why));
                continue;
            }
            Some((name, text, None)) => match serde_yaml::from_str::<Sidecar>(&text) {
                Ok(s) => Some(s),
                Err(e) => {
                    let why =
                        format!("'{stem}': sidecar '{name}' is invalid ({e}); template skipped");
                    warnings.push(why.clone());
                    out.push(RemoteTemplate::unusable(stem, why));
                    continue;
                }
            },
        };
        out.push(RemoteTemplate {
            stem,
            body,
            format,
            sidecar,
            retired: None,
            unusable: None,
        });
    }
    for (stem, (name, _, _)) in sidecars {
        warnings.push(format!(
            "'{name}': sidecar without a '{stem}.yaml' / '.json' template"
        ));
    }
    Paired {
        templates: out,
        warnings,
    }
}

/// Lists and reads the candidate files at an origin.
#[async_trait]
pub trait Fetcher: Send + Sync {
    async fn list(&self) -> CliResult<Vec<RemoteFile>>;

    /// The origin's Template Hub `index.json`, when it is a catalog (#691).
    async fn catalog_index(&self) -> CliResult<Option<crate::hub::IndexVersions>> {
        Ok(None)
    }
}

/// Mark every template whose body is a version the catalog deprecated.
///
/// The files an origin serves are each template's newest catalog version, so
/// that is the version checked. A template id may appear in both the source
/// and sink lists; either marking it retires the body.
pub fn apply_catalog_index(templates: &mut [RemoteTemplate], index: &crate::hub::IndexVersions) {
    for t in templates.iter_mut() {
        let entries = index
            .sources
            .iter()
            .chain(index.sinks.iter())
            .filter(|e| e.id == t.stem || (e.id.is_empty() && e.name == t.stem));
        for e in entries {
            let Some(head) = e.newest_version() else {
                continue;
            };
            if let Some(v) = e
                .versions
                .iter()
                .find(|v| v.version == head && v.deprecated)
            {
                let why = v.reason.clone().unwrap_or_else(|| "no reason given".into());
                t.retired = Some(format!("catalog v{head} is deprecated: {why}"));
            }
        }
    }
}

/// Writes one file to an origin (`faucet template publish`).
#[async_trait]
pub trait Publisher: Send + Sync {
    /// Create or overwrite `name` under the origin's directory; returns a
    /// human-readable location of what was written.
    async fn put(&self, name: &str, body: &str) -> CliResult<String>;

    /// [`Self::put`] for a template of `kind`, so an origin reading several
    /// directories writes it where the next pull reads that kind from.
    async fn put_kind(
        &self,
        name: &str,
        body: &str,
        _kind: crate::hub::spec::TemplateKind,
    ) -> CliResult<String> {
        self.put(name, body).await
    }
}

/// Directory-listing size at which the GitHub contents API truncates.
const CONTENTS_LIST_CAP: usize = 1000;

/// The `paths:` directory a template of `kind` belongs in: the one named for
/// the kind (`source-templates`, `sink-templates`, `deployments`), else the
/// only one, else the first for a pipeline. `None` = ambiguous.
pub fn dir_for_kind<'a>(dirs: &[&'a str], kind: crate::hub::spec::TemplateKind) -> Option<&'a str> {
    use crate::hub::spec::TemplateKind;
    let named = match kind {
        TemplateKind::SourceTemplate => Some("source-templates"),
        TemplateKind::SinkTemplate => Some("sink-templates"),
        TemplateKind::Deployment => Some("deployments"),
        TemplateKind::TestSuite => Some("test-suites"),
        TemplateKind::Pipeline => None,
    };
    if let Some(n) = named
        && let Some(d) = dirs.iter().find(|d| d.rsplit('/').next() == Some(n))
    {
        return Some(d);
    }
    if dirs.len() == 1 || named.is_none() {
        return dirs.first().copied();
    }
    None
}

fn is_commit_sha(s: &str) -> bool {
    s.len() == 40 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

fn io_err(kind: &str, msg: impl std::fmt::Display) -> CliError {
    CliError::Internal(format!("templates-sync {kind}: {msg}"))
}

// ── GitHub ──────────────────────────────────────────────────────────────────

/// GitHub contents-API adapter. No git binary: a private repo needs only a
/// token, and the same client publishes with a contents `PUT`.
pub struct GithubFetcher {
    client: reqwest::Client,
    cfg: GithubSource,
    /// The commit `ref` resolved to, once per fetcher, so a listing and every
    /// download read one snapshot even if the branch moves mid-pull.
    pinned: tokio::sync::OnceCell<String>,
}

#[derive(Debug, serde::Deserialize)]
struct TreeListing {
    #[serde(default)]
    tree: Vec<TreeEntry>,
    #[serde(default)]
    truncated: bool,
}

#[derive(Debug, serde::Deserialize)]
struct TreeEntry {
    path: String,
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    sha: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct ContentsEntry {
    name: String,
    #[serde(rename = "type")]
    kind: String,
    url: String,
    #[serde(default)]
    sha: Option<String>,
}

impl GithubFetcher {
    pub fn new(cfg: &GithubSource) -> CliResult<Self> {
        if cfg.repo.split('/').filter(|s| !s.is_empty()).count() != 2 {
            return Err(CliError::Config(format!(
                "templates-sync github: `repo` must be `owner/name`, got '{}'",
                cfg.repo
            )));
        }
        let client = reqwest::Client::builder()
            .user_agent(concat!("faucet-stream/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|e| io_err("github", format!("building HTTP client: {e}")))?;
        Ok(Self {
            client,
            cfg: cfg.clone(),
            pinned: tokio::sync::OnceCell::new(),
        })
    }

    /// The commit the configured `ref` points at now (resolved once). Falls
    /// back to the ref itself when the commits API cannot resolve it, so the
    /// listing reports the real error.
    async fn read_ref(&self) -> &str {
        self.pinned
            .get_or_init(|| async {
                let base = self.cfg.api_base.trim_end_matches('/');
                let url = format!("{base}/repos/{}/commits/{}", self.cfg.repo, self.cfg.r#ref);
                let resp = self
                    .request(reqwest::Method::GET, &url, "application/vnd.github.sha")
                    .send()
                    .await;
                match resp {
                    Ok(r) if r.status().is_success() => match r.text().await {
                        Ok(t) if is_commit_sha(t.trim()) => t.trim().to_string(),
                        _ => self.cfg.r#ref.clone(),
                    },
                    _ => self.cfg.r#ref.clone(),
                }
            })
            .await
    }

    /// The directories this origin reads: `paths` when set, else `path`.
    fn dirs(&self) -> Vec<&str> {
        if self.cfg.paths.is_empty() {
            vec![self.cfg.path.trim_matches('/')]
        } else {
            self.cfg.paths.iter().map(|p| p.trim_matches('/')).collect()
        }
    }

    /// Where `publish` writes: the single `path`, or the first of `paths`.
    fn dir(&self) -> &str {
        self.dirs()[0]
    }

    #[cfg(test)]
    fn contents_url(&self, name: Option<&str>) -> String {
        self.contents_url_in(self.dir(), name)
    }

    fn contents_url_in(&self, dir: &str, name: Option<&str>) -> String {
        let base = self.cfg.api_base.trim_end_matches('/');
        let mut path = dir.to_string();
        if let Some(n) = name {
            if !path.is_empty() {
                path.push('/');
            }
            path.push_str(n);
        }
        format!("{base}/repos/{}/contents/{path}", self.cfg.repo)
    }

    fn request(&self, method: reqwest::Method, url: &str, accept: &str) -> reqwest::RequestBuilder {
        let mut r = self
            .client
            .request(method, url)
            .header("Accept", accept)
            .header("X-GitHub-Api-Version", "2022-11-28");
        if let Some(t) = &self.cfg.token {
            r = r.bearer_auth(t);
        }
        r
    }

    async fn send(&self, req: reqwest::RequestBuilder, what: &str) -> CliResult<reqwest::Response> {
        let resp = req
            .send()
            .await
            .map_err(|e| io_err("github", format!("{what}: {e}")))?;
        self.check(resp, what).await
    }

    async fn check(&self, resp: reqwest::Response, what: &str) -> CliResult<reqwest::Response> {
        let status = resp.status();
        if status.is_success() {
            return Ok(resp);
        }
        let body = resp.text().await.unwrap_or_default();
        let hint = match status.as_u16() {
            401 | 403 => " (check the token and its repository scope)",
            404 => " (check `repo`, `ref`, and `path`; a private repo needs a token)",
            _ => "",
        };
        Err(io_err(
            "github",
            format!(
                "{what}: HTTP {status}{hint}: {}",
                body.chars().take(300).collect::<String>()
            ),
        ))
    }

    async fn read_raw(&self, entry_url: &str) -> CliResult<String> {
        let resp = self
            .send(
                self.request(
                    reqwest::Method::GET,
                    entry_url,
                    "application/vnd.github.raw+json",
                ),
                "reading file",
            )
            .await?;
        resp.text()
            .await
            .map_err(|e| io_err("github", format!("reading file body: {e}")))
    }
}

impl GithubFetcher {
    async fn list_dir(&self, dir: &str) -> CliResult<Vec<ContentsEntry>> {
        let entries = self.list_dir_contents(dir).await?;
        if entries.len() >= CONTENTS_LIST_CAP {
            // The contents API stops at 1000 entries; anything past it would
            // look removed upstream. Read the directory from the git tree.
            return self.list_dir_tree(dir).await;
        }
        Ok(entries)
    }

    async fn list_dir_tree(&self, dir: &str) -> CliResult<Vec<ContentsEntry>> {
        let base = self.cfg.api_base.trim_end_matches('/');
        let url = format!(
            "{base}/repos/{}/git/trees/{}?recursive=1",
            self.cfg.repo,
            self.read_ref().await
        );
        let resp = self
            .send(
                self.request(reqwest::Method::GET, &url, "application/vnd.github+json"),
                "listing the git tree",
            )
            .await?;
        let tree: TreeListing = resp
            .json()
            .await
            .map_err(|e| io_err("github", format!("decoding the git tree: {e}")))?;
        if tree.truncated {
            return Err(io_err(
                "github",
                format!(
                    "'{dir}' in {} is too large to list (the git tree is truncated); split it \
                     across several `paths:`",
                    self.cfg.repo
                ),
            ));
        }
        let prefix = if dir.is_empty() {
            String::new()
        } else {
            format!("{dir}/")
        };
        Ok(tree
            .tree
            .into_iter()
            .filter_map(|e| {
                let name = e.path.strip_prefix(&prefix)?;
                (!name.is_empty() && !name.contains('/')).then(|| ContentsEntry {
                    name: name.to_string(),
                    kind: if e.kind == "tree" { "dir" } else { "file" }.to_string(),
                    url: self.contents_url_in(dir, Some(name)),
                    sha: e.sha,
                })
            })
            .collect())
    }

    async fn list_dir_contents(&self, dir: &str) -> CliResult<Vec<ContentsEntry>> {
        let url = format!(
            "{}?ref={}",
            self.contents_url_in(dir, None),
            self.read_ref().await
        );
        let resp = self
            .send(
                self.request(reqwest::Method::GET, &url, "application/vnd.github+json"),
                "listing directory",
            )
            .await?;
        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| io_err("github", format!("decoding directory listing: {e}")))?;
        match body {
            serde_json::Value::Array(_) => serde_json::from_value(body)
                .map_err(|e| io_err("github", format!("decoding directory listing: {e}"))),
            _ => Err(CliError::Config(format!(
                "templates-sync github: '{dir}' in {} is a file, not a directory",
                self.cfg.repo
            ))),
        }
    }
}

#[async_trait]
impl Fetcher for GithubFetcher {
    /// A catalog origin (`paths:`, the Template Hub layout) keeps `index.json`
    /// at the repository root. Absent → not a catalog.
    async fn catalog_index(&self) -> CliResult<Option<crate::hub::IndexVersions>> {
        if self.cfg.paths.is_empty() {
            return Ok(None);
        }
        let url = format!(
            "{}?ref={}",
            self.contents_url_in("", Some("index.json")),
            self.read_ref().await
        );
        let resp = self
            .request(
                reqwest::Method::GET,
                &url,
                "application/vnd.github.raw+json",
            )
            .send()
            .await
            .map_err(|e| io_err("github", format!("reading index.json: {e}")))?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let resp = self.check(resp, "reading index.json").await?;
        let text = resp
            .text()
            .await
            .map_err(|e| io_err("github", format!("reading index.json: {e}")))?;
        serde_json::from_str(&text)
            .map(Some)
            .map_err(|e| io_err("github", format!("decoding index.json: {e}")))
    }

    async fn list(&self) -> CliResult<Vec<RemoteFile>> {
        // Files directly in each directory, plus one level of subdirectories —
        // a Template Hub's `<owner>/<name>.yaml` layout (#682), whose stems
        // become `owner/name` ids.
        let mut entries: Vec<ContentsEntry> = Vec::new();
        for dir in self.dirs() {
            for e in self.list_dir(dir).await? {
                if e.kind == "dir" && !e.name.starts_with('.') {
                    let sub = if dir.is_empty() {
                        e.name.clone()
                    } else {
                        format!("{dir}/{}", e.name)
                    };
                    for mut f in self.list_dir(&sub).await? {
                        if f.kind == "file" {
                            f.name = format!("{}/{}", e.name, f.name);
                            entries.push(f);
                        }
                    }
                } else {
                    entries.push(e);
                }
            }
        }
        let wanted: Vec<ContentsEntry> = entries
            .into_iter()
            .filter(|e| e.kind == "file" && is_candidate(&e.name))
            .collect();
        let rref = self.read_ref().await;
        let files: Vec<RemoteFile> = futures::stream::iter(wanted)
            .map(|e| async move {
                let url = format!("{}?ref={rref}", e.url.split('?').next().unwrap_or(&e.url));
                match self.read_raw(&url).await {
                    Ok(body) => RemoteFile {
                        name: e.name,
                        body,
                        error: None,
                    },
                    Err(err) => RemoteFile {
                        name: e.name,
                        body: String::new(),
                        error: Some(err.to_string()),
                    },
                }
            })
            .buffer_unordered(FETCH_CONCURRENCY)
            .collect()
            .await;
        Ok(files)
    }
}

#[async_trait]
impl Publisher for GithubFetcher {
    async fn put(&self, name: &str, body: &str) -> CliResult<String> {
        self.put_in(self.dir(), name, body).await
    }

    async fn put_kind(
        &self,
        name: &str,
        body: &str,
        kind: crate::hub::spec::TemplateKind,
    ) -> CliResult<String> {
        let dirs = self.dirs();
        let dir = dir_for_kind(&dirs, kind).ok_or_else(|| {
            CliError::Config(format!(
                "templates-sync github: none of this origin's `paths:` ({}) holds {} files — add \
                 one, or publish to an origin that reads them",
                dirs.join(", "),
                kind.as_str()
            ))
        })?;
        self.put_in(dir, name, body).await
    }
}

impl GithubFetcher {
    async fn put_in(&self, dir: &str, name: &str, body: &str) -> CliResult<String> {
        use base64::Engine as _;
        let url = self.contents_url_in(dir, Some(name));
        // An update must carry the blob sha of what it replaces.
        let existing = self
            .request(
                reqwest::Method::GET,
                &format!("{url}?ref={}", self.cfg.r#ref),
                "application/vnd.github+json",
            )
            .send()
            .await
            .map_err(|e| io_err("github", format!("checking existing file: {e}")))?;
        let sha = if existing.status().is_success() {
            existing
                .json::<ContentsEntry>()
                .await
                .map_err(|e| io_err("github", format!("decoding existing file: {e}")))?
                .sha
        } else if existing.status().as_u16() == 404 {
            None
        } else {
            let status = existing.status();
            let text = existing.text().await.unwrap_or_default();
            return Err(io_err(
                "github",
                format!(
                    "checking existing file: HTTP {status}: {}",
                    text.chars().take(300).collect::<String>()
                ),
            ));
        };
        let mut payload = serde_json::json!({
            "message": format!("faucet template publish: {name}"),
            "content": base64::engine::general_purpose::STANDARD.encode(body.as_bytes()),
            "branch": self.cfg.r#ref,
        });
        if let Some(sha) = sha {
            payload["sha"] = serde_json::Value::String(sha);
        }
        let resp = self
            .send(
                self.request(reqwest::Method::PUT, &url, "application/vnd.github+json")
                    .json(&payload),
                "writing file",
            )
            .await?;
        let v: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| io_err("github", format!("decoding write response: {e}")))?;
        Ok(v["content"]["html_url"]
            .as_str()
            .map(str::to_string)
            .unwrap_or_else(|| format!("{}:{}/{}", self.cfg.repo, self.cfg.r#ref, name)))
    }
}

// ── object stores ───────────────────────────────────────────────────────────

/// `object_store`-backed adapter (S3 / GCS / Azure Blob). Generic over the
/// store so the listing/pairing logic is tested against an in-memory store.
#[cfg(feature = "templates-sync-object-store")]
use object_store::ObjectStoreExt as _;

#[cfg(feature = "templates-sync-object-store")]
pub struct ObjectStoreFetcher {
    store: Arc<dyn object_store::ObjectStore>,
    prefix: Option<object_store::path::Path>,
    label: String,
}

#[cfg(feature = "templates-sync-object-store")]
impl ObjectStoreFetcher {
    pub fn new(
        store: Arc<dyn object_store::ObjectStore>,
        prefix: &str,
        label: impl Into<String>,
    ) -> Self {
        let trimmed = prefix.trim_matches('/');
        Self {
            store,
            prefix: (!trimmed.is_empty()).then(|| object_store::path::Path::from(trimmed)),
            label: label.into(),
        }
    }

    /// Build the client for a configured origin source. Credentials come from
    /// the environment / SDK default chain, as for the trigger watchers.
    pub fn from_source(source: &OriginSource) -> CliResult<Self> {
        match source {
            OriginSource::S3(s) => {
                let mut b =
                    object_store::aws::AmazonS3Builder::from_env().with_bucket_name(&s.bucket);
                if let Some(r) = &s.region {
                    b = b.with_region(r);
                }
                if let Some(e) = &s.endpoint {
                    b = b.with_endpoint(e).with_allow_http(true);
                }
                let store = b.build().map_err(|e| {
                    CliError::Config(format!("templates-sync s3: building client: {e}"))
                })?;
                Ok(Self::new(
                    Arc::new(store),
                    &s.prefix,
                    format!("s3://{}", s.bucket),
                ))
            }
            OriginSource::Gcs(g) => {
                let store = object_store::gcp::GoogleCloudStorageBuilder::from_env()
                    .with_bucket_name(&g.bucket)
                    .build()
                    .map_err(|e| {
                        CliError::Config(format!("templates-sync gcs: building client: {e}"))
                    })?;
                Ok(Self::new(
                    Arc::new(store),
                    &g.prefix,
                    format!("gs://{}", g.bucket),
                ))
            }
            OriginSource::AzureBlob(a) => {
                let mut b = object_store::azure::MicrosoftAzureBuilder::from_env()
                    .with_container_name(&a.container);
                if let Some(acct) = &a.account {
                    b = b.with_account(acct);
                }
                let store = b.build().map_err(|e| {
                    CliError::Config(format!("templates-sync azure_blob: building client: {e}"))
                })?;
                Ok(Self::new(
                    Arc::new(store),
                    &a.prefix,
                    format!("azure://{}", a.container),
                ))
            }
            OriginSource::Github(_) => Err(CliError::Internal(
                "templates-sync: github origin routed to the object-store fetcher".into(),
            )),
        }
    }

    fn child(&self, name: &str) -> object_store::path::Path {
        match &self.prefix {
            Some(p) => p.clone().join(name),
            None => object_store::path::Path::from(name),
        }
    }

    fn depth(&self) -> usize {
        self.prefix.as_ref().map(|p| p.parts().count()).unwrap_or(0)
    }
}

#[cfg(feature = "templates-sync-object-store")]
#[async_trait]
impl Fetcher for ObjectStoreFetcher {
    async fn list(&self) -> CliResult<Vec<RemoteFile>> {
        use futures::TryStreamExt as _;
        let depth = self.depth();
        let metas: Vec<object_store::ObjectMeta> = self
            .store
            .list(self.prefix.as_ref())
            .try_collect()
            .await
            .map_err(|e| io_err(&self.label, format!("listing: {e}")))?;
        // Direct children, plus one level of owner directories (#682) whose
        // stems become `owner/name` — the same layout the GitHub reader takes.
        let wanted: Vec<(String, object_store::path::Path)> = metas
            .into_iter()
            .filter_map(|m| {
                let parts: Vec<String> = m
                    .location
                    .parts()
                    .skip(depth)
                    .map(|p| p.as_ref().to_string())
                    .collect();
                let name = match parts.as_slice() {
                    [file] => file.clone(),
                    [owner, file] if !owner.starts_with('.') => format!("{owner}/{file}"),
                    _ => return None,
                };
                is_candidate(&name).then_some((name, m.location))
            })
            .collect();
        let files: Vec<RemoteFile> = futures::stream::iter(wanted)
            .map(|(name, loc)| async move {
                let read = async {
                    let bytes = self
                        .store
                        .get(&loc)
                        .await
                        .map_err(|e| format!("reading {loc}: {e}"))?
                        .bytes()
                        .await
                        .map_err(|e| format!("reading {loc}: {e}"))?;
                    String::from_utf8(bytes.to_vec())
                        .map_err(|e| format!("{loc} is not UTF-8: {e}"))
                };
                match read.await {
                    Ok(body) => RemoteFile {
                        name,
                        body,
                        error: None,
                    },
                    Err(e) => RemoteFile {
                        name,
                        body: String::new(),
                        error: Some(e),
                    },
                }
            })
            .buffer_unordered(FETCH_CONCURRENCY)
            .collect()
            .await;
        Ok(files)
    }
}

#[cfg(feature = "templates-sync-object-store")]
#[async_trait]
impl Publisher for ObjectStoreFetcher {
    async fn put(&self, name: &str, body: &str) -> CliResult<String> {
        let loc = self.child(name);
        self.store
            .put(&loc, object_store::PutPayload::from(body.to_string()))
            .await
            .map_err(|e| io_err(&self.label, format!("writing {loc}: {e}")))?;
        Ok(format!("{}/{loc}", self.label))
    }
}

/// Pick the adapter for an origin's source.
pub fn fetcher_for(source: &OriginSource) -> CliResult<Arc<dyn Fetcher>> {
    match source {
        OriginSource::Github(g) => Ok(Arc::new(GithubFetcher::new(g)?)),
        #[cfg(feature = "templates-sync-object-store")]
        other => Ok(Arc::new(ObjectStoreFetcher::from_source(other)?)),
        #[cfg(not(feature = "templates-sync-object-store"))]
        other => Err(object_store_missing(other)),
    }
}

/// Pick the write adapter for an origin's source.
pub fn publisher_for(source: &OriginSource) -> CliResult<Arc<dyn Publisher>> {
    match source {
        OriginSource::Github(g) => Ok(Arc::new(GithubFetcher::new(g)?)),
        #[cfg(feature = "templates-sync-object-store")]
        other => Ok(Arc::new(ObjectStoreFetcher::from_source(other)?)),
        #[cfg(not(feature = "templates-sync-object-store"))]
        other => Err(object_store_missing(other)),
    }
}

#[cfg(not(feature = "templates-sync-object-store"))]
fn object_store_missing(source: &OriginSource) -> CliError {
    CliError::Config(format!(
        "templates-sync: a `{}` origin needs a build with the `templates-sync-object-store` feature",
        source.kind()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn f(name: &str, body: &str) -> RemoteFile {
        RemoteFile {
            name: name.into(),
            body: body.into(),
            error: None,
        }
    }

    #[test]
    fn candidate_names() {
        assert!(is_candidate("a.yaml"));
        assert!(is_candidate("a.YML"));
        assert!(is_candidate("a.json"));
        assert!(is_candidate("a.faucet.yaml"));
        assert!(!is_candidate("README.md"));
        assert!(!is_candidate(".faucet.yaml"), "a sidecar needs a stem");
        assert!(!is_candidate(".hidden.yaml"), "dotfiles are ignored");
        assert_eq!(
            template_format("a.faucet.yaml"),
            None,
            "sidecars are not templates"
        );
        assert_eq!(template_format("a.json"), Some(ConfigFormat::Json));
    }

    #[test]
    fn pairs_templates_with_sidecars_and_ignores_noise() {
        let p = pair_files(vec![
            f("README.md", "# docs"),
            f("b.json", "{}"),
            f("a.yaml", "version: 1"),
            f("a.faucet.yaml", "launch: true\ndescription: A"),
        ]);
        assert!(p.warnings.is_empty(), "{:?}", p.warnings);
        assert_eq!(p.templates.len(), 2);
        let a = &p.templates[0];
        assert_eq!(a.stem, "a");
        assert_eq!(a.format, ConfigFormat::Yaml);
        let side = a.sidecar.as_ref().expect("sidecar");
        assert!(side.launch);
        assert_eq!(side.description.as_deref(), Some("A"));
        let b = &p.templates[1];
        assert_eq!(
            (b.stem.as_str(), b.format, b.sidecar.is_none()),
            ("b", ConfigFormat::Json, true)
        );
    }

    #[test]
    fn ambiguous_stems_and_broken_sidecars_are_skipped_with_a_warning() {
        let p = pair_files(vec![
            f("dup.yaml", "a: 1"),
            f("dup.json", "{}"),
            f("dup.faucet.yaml", "launch: true"),
            f("bad.yaml", "a: 1"),
            f("bad.faucet.yaml", "launch: yes please"),
            f("lonely.faucet.yaml", "{}"),
            f("ok.yaml", "a: 1"),
        ]);
        let usable: Vec<&str> = p
            .templates
            .iter()
            .filter(|t| t.unusable.is_none())
            .map(|t| t.stem.as_str())
            .collect();
        assert_eq!(usable, vec!["ok"]);
        // The dropped stems are still reported as upstream, so a prune never
        // deprecates them (#789 CLI-109).
        let unusable: Vec<&str> = p
            .templates
            .iter()
            .filter(|t| t.unusable.is_some())
            .map(|t| t.stem.as_str())
            .collect();
        assert_eq!(unusable, vec!["bad", "dup"]);
        assert_eq!(p.warnings.len(), 3, "{:?}", p.warnings);
        assert!(p.warnings.iter().any(|w| w.contains("'dup': ambiguous")));
        assert!(p.warnings.iter().any(|w| w.contains("'bad': sidecar")));
        assert!(p.warnings.iter().any(|w| w.contains("lonely.faucet.yaml")));
    }

    #[test]
    fn unreadable_files_make_their_template_unusable() {
        let broken = |name: &str| RemoteFile {
            name: name.into(),
            body: String::new(),
            error: Some("HTTP 502".into()),
        };
        let p = pair_files(vec![
            broken("a.yaml"),
            f("b.yaml", "b: 1"),
            broken("b.faucet.yaml"),
            f("c.yaml", "c: 1"),
        ]);
        let state: Vec<(&str, bool)> = p
            .templates
            .iter()
            .map(|t| (t.stem.as_str(), t.unusable.is_some()))
            .collect();
        assert_eq!(state, vec![("a", true), ("b", true), ("c", false)]);
        assert!(
            p.templates[0]
                .unusable
                .as_deref()
                .unwrap()
                .contains("HTTP 502")
        );
    }

    #[test]
    fn publish_picks_the_directory_for_the_kind() {
        use crate::hub::spec::TemplateKind;
        let both = ["source-templates", "hub/sink-templates"];
        assert_eq!(
            dir_for_kind(&both, TemplateKind::SinkTemplate),
            Some("hub/sink-templates")
        );
        assert_eq!(
            dir_for_kind(&both, TemplateKind::SourceTemplate),
            Some("source-templates")
        );
        assert_eq!(dir_for_kind(&both, TemplateKind::Deployment), None);
        assert_eq!(
            dir_for_kind(&both, TemplateKind::Pipeline),
            Some("source-templates")
        );
        assert_eq!(
            dir_for_kind(&["templates"], TemplateKind::SinkTemplate),
            Some("templates")
        );
        assert!(is_commit_sha(&"a".repeat(40)));
        assert!(!is_commit_sha("main"));
    }

    #[test]
    fn two_sidecars_for_one_stem_warn_and_last_wins() {
        let p = pair_files(vec![
            f("a.yaml", "a: 1"),
            f("a.faucet.yml", "launch: false"),
            f("a.faucet.yaml", "launch: true"),
        ]);
        assert_eq!(p.warnings.len(), 1);
        assert!(p.templates[0].sidecar.as_ref().unwrap().launch);
    }

    #[test]
    fn github_config_is_validated_and_urls_are_shaped() {
        let bad = GithubSource {
            repo: "nope".into(),
            r#ref: "main".into(),
            path: String::new(),
            paths: Vec::new(),
            token: None,
            api_base: "https://api.github.com".into(),
        };
        assert!(GithubFetcher::new(&bad).is_err());
        let good = GithubSource {
            repo: "acme/t".into(),
            r#ref: "main".into(),
            path: "/templates/".into(),
            paths: Vec::new(),
            token: Some("tok".into()),
            api_base: "https://ghe.example/api/v3/".into(),
        };
        let g = GithubFetcher::new(&good).unwrap();
        assert_eq!(
            g.contents_url(None),
            "https://ghe.example/api/v3/repos/acme/t/contents/templates"
        );
        assert_eq!(
            g.contents_url(Some("x.yaml")),
            "https://ghe.example/api/v3/repos/acme/t/contents/templates/x.yaml"
        );
        let root = GithubFetcher::new(&GithubSource {
            path: String::new(),
            paths: Vec::new(),
            ..good
        })
        .unwrap();
        assert_eq!(
            root.contents_url(Some("x.yaml")),
            "https://ghe.example/api/v3/repos/acme/t/contents/x.yaml"
        );
    }

    #[cfg(feature = "templates-sync-object-store")]
    mod object_store_tests {
        use super::*;
        use object_store::memory::InMemory;
        use object_store::path::Path;

        async fn seeded() -> Arc<InMemory> {
            let s = Arc::new(InMemory::new());
            for (k, v) in [
                ("tpl/a.yaml", "a: 1"),
                ("tpl/a.faucet.yaml", "launch: true"),
                ("tpl/b.json", "{}"),
                ("tpl/README.md", "no"),
                ("tpl/nested/c.yaml", "nested: true"),
                ("other/d.yaml", "elsewhere"),
            ] {
                s.put(
                    &Path::from(k),
                    object_store::PutPayload::from(v.to_string()),
                )
                .await
                .unwrap();
            }
            s
        }

        #[tokio::test]
        async fn lists_candidate_children_and_owner_directories() {
            let s = seeded().await;
            let f = ObjectStoreFetcher::new(s.clone(), "/tpl/", "mem");
            let mut files = f.list().await.unwrap();
            files.sort_by(|a, b| a.name.cmp(&b.name));
            let names: Vec<&str> = files.iter().map(|f| f.name.as_str()).collect();
            // One level of owner directories reads as `owner/name`, like a
            // GitHub origin, so a published `owner/name` id round-trips.
            assert_eq!(
                names,
                vec!["a.faucet.yaml", "a.yaml", "b.json", "nested/c.yaml"]
            );
            assert_eq!(files[1].body, "a: 1");
            // Pairing on top of the listing.
            let p = pair_files(files);
            assert_eq!(p.templates.len(), 3);
            assert!(p.templates[0].sidecar.as_ref().unwrap().launch);
        }

        #[tokio::test]
        async fn empty_prefix_reads_the_root_and_owner_directories() {
            let s = Arc::new(InMemory::new());
            s.put(
                &Path::from("r.yaml"),
                object_store::PutPayload::from("r: 1".to_string()),
            )
            .await
            .unwrap();
            s.put(
                &Path::from("d/x.yaml"),
                object_store::PutPayload::from("x: 1".to_string()),
            )
            .await
            .unwrap();
            let f = ObjectStoreFetcher::new(s, "", "mem");
            let mut files = f.list().await.unwrap();
            files.sort_by(|a, b| a.name.cmp(&b.name));
            let names: Vec<&str> = files.iter().map(|f| f.name.as_str()).collect();
            assert_eq!(names, vec!["d/x.yaml", "r.yaml"]);
        }

        #[tokio::test]
        async fn put_writes_under_the_prefix_and_reports_the_location() {
            let s = Arc::new(InMemory::new());
            let f = ObjectStoreFetcher::new(s.clone(), "tpl", "mem");
            let loc = f.put("new.yaml", "v: 1").await.unwrap();
            assert_eq!(loc, "mem/tpl/new.yaml");
            let got = s
                .get(&Path::from("tpl/new.yaml"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            assert_eq!(&got[..], b"v: 1");
            // Overwrite is a plain put.
            f.put("new.yaml", "v: 2").await.unwrap();
            let got = s
                .get(&Path::from("tpl/new.yaml"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            assert_eq!(&got[..], b"v: 2");
        }

        #[tokio::test]
        async fn non_utf8_objects_are_reported_per_file() {
            let s = Arc::new(InMemory::new());
            s.put(
                &Path::from("x.yaml"),
                object_store::PutPayload::from(vec![0xff, 0xfe]),
            )
            .await
            .unwrap();
            let f = ObjectStoreFetcher::new(s, "", "mem");
            let files = f.list().await.unwrap();
            let err = files[0].error.as_deref().unwrap();
            assert!(err.contains("not UTF-8"), "{err}");
        }

        #[test]
        fn from_source_builds_each_store_kind() {
            use crate::templates::sync::spec::{AzureBlobSource, GcsSource, S3Source};
            // S3 with an explicit endpoint/region builds without credentials
            // (they are resolved lazily on first request).
            let s3 = OriginSource::S3(S3Source {
                bucket: "b".into(),
                prefix: "p".into(),
                region: Some("us-east-1".into()),
                endpoint: Some("http://localhost:9000".into()),
            });
            let f = ObjectStoreFetcher::from_source(&s3).unwrap();
            assert_eq!(f.label, "s3://b");
            assert_eq!(f.depth(), 1);
            let gcs = OriginSource::Gcs(GcsSource {
                bucket: "g".into(),
                prefix: String::new(),
            });
            // GCS may fail without ADC — either way it must not panic, and a
            // failure is a typed config error.
            match ObjectStoreFetcher::from_source(&gcs) {
                Ok(f) => assert_eq!(f.label, "gs://g"),
                Err(e) => assert!(e.to_string().contains("templates-sync gcs"), "{e}"),
            }
            let az = OriginSource::AzureBlob(AzureBlobSource {
                container: "c".into(),
                prefix: "a/b".into(),
                account: Some("acct".into()),
            });
            match ObjectStoreFetcher::from_source(&az) {
                Ok(f) => {
                    assert_eq!(f.label, "azure://c");
                    assert_eq!(f.depth(), 2);
                }
                Err(e) => assert!(e.to_string().contains("templates-sync azure_blob"), "{e}"),
            }
            let gh = OriginSource::Github(GithubSource {
                repo: "a/b".into(),
                r#ref: "main".into(),
                path: String::new(),
                paths: Vec::new(),
                token: None,
                api_base: "https://api.github.com".into(),
            });
            assert!(ObjectStoreFetcher::from_source(&gh).is_err());
        }
    }

    #[test]
    fn catalog_index_retires_only_a_deprecated_head_version() {
        let index: crate::hub::IndexVersions = serde_json::from_value(serde_json::json!({
            "sources": [
                {"id": "acme/erp", "stable": 2, "versions": [
                    {"version": 2, "commit": "a"},
                    {"version": 3, "commit": "b", "deprecated": true, "reason": "drops invoices"}]},
                {"id": "acme/crm", "versions": [
                    {"version": 1, "commit": "c", "deprecated": true},
                    {"version": 2, "commit": "d"}]}
            ],
            "sinks": [{"name": "legacy", "versions": [
                {"version": 1, "commit": "e", "deprecated": true}]}]
        }))
        .unwrap();
        let t = |stem: &str| RemoteTemplate {
            stem: stem.into(),
            body: "x".into(),
            format: ConfigFormat::Yaml,
            sidecar: None,
            retired: None,
            unusable: None,
        };
        let mut ts = vec![t("acme/erp"), t("acme/crm"), t("legacy"), t("other")];
        apply_catalog_index(&mut ts, &index);
        assert_eq!(
            ts[0].retired.as_deref(),
            Some("catalog v3 is deprecated: drops invoices")
        );
        assert!(
            ts[1].retired.is_none(),
            "only an older version is deprecated"
        );
        assert_eq!(
            ts[2].retired.as_deref(),
            Some("catalog v1 is deprecated: no reason given")
        );
        assert!(ts[3].retired.is_none());
    }

    #[tokio::test]
    async fn github_catalog_index_is_read_only_for_catalog_origins() {
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/repos/acme/hub/contents/index.json"))
            .and(query_param("ref", "main"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                r#"{"sources":[{"id":"acme/erp","versions":[{"version":1,"commit":"a"}]}]}"#,
            ))
            .mount(&server)
            .await;
        let src = |paths: Vec<String>, repo: &str| GithubSource {
            repo: repo.into(),
            r#ref: "main".into(),
            path: String::new(),
            paths,
            token: None,
            api_base: server.uri(),
        };
        let catalog =
            GithubFetcher::new(&src(vec!["source-templates".into()], "acme/hub")).unwrap();
        let idx = catalog.catalog_index().await.unwrap().expect("an index");
        assert_eq!(idx.sources[0].id, "acme/erp");
        // A plain `path:` origin is not a catalog: no request is made.
        let plain = GithubFetcher::new(&src(Vec::new(), "acme/hub")).unwrap();
        assert!(plain.catalog_index().await.unwrap().is_none());
        // A catalog-shaped origin without index.json (404) is not a catalog either.
        let missing = GithubFetcher::new(&src(vec!["s".into()], "acme/none")).unwrap();
        Mock::given(method("GET"))
            .and(path("/repos/acme/none/contents/index.json"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        assert!(missing.catalog_index().await.unwrap().is_none());
        // A malformed index is a typed error the caller turns into a warning.
        Mock::given(method("GET"))
            .and(path("/repos/acme/bad/contents/index.json"))
            .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
            .mount(&server)
            .await;
        let bad = GithubFetcher::new(&src(vec!["s".into()], "acme/bad")).unwrap();
        let err = bad.catalog_index().await.unwrap_err().to_string();
        assert!(err.contains("decoding index.json"), "{err}");
    }

    /// The ref is resolved to a commit once and every listing and download
    /// reads that commit (#789 CLI-103); a directory past the contents API's
    /// 1000-entry cap is read from the git tree (#789 CLI-113).
    #[tokio::test]
    async fn github_reads_one_commit_and_lists_large_directories_from_the_tree() {
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        let sha = "0123456789abcdef0123456789abcdef01234567";
        Mock::given(method("GET"))
            .and(path("/repos/acme/big/commits/main"))
            .respond_with(ResponseTemplate::new(200).set_body_string(sha))
            .mount(&server)
            .await;
        let entries: Vec<serde_json::Value> = (0..1000)
            .map(|i| serde_json::json!({"name": format!("t{i}.md"), "type": "file", "url": "x"}))
            .collect();
        Mock::given(method("GET"))
            .and(path("/repos/acme/big/contents/tpl"))
            .and(query_param("ref", sha))
            .respond_with(ResponseTemplate::new(200).set_body_json(entries))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/repos/acme/big/git/trees/{sha}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "truncated": false,
                "tree": [
                    {"path": "tpl", "type": "tree"},
                    {"path": "tpl/a.yaml", "type": "blob", "sha": "1"},
                    {"path": "tpl/z1001.yaml", "type": "blob", "sha": "2"},
                    {"path": "tpl/deep/x/y.yaml", "type": "blob"},
                    {"path": "other/b.yaml", "type": "blob"}
                ]
            })))
            .mount(&server)
            .await;
        for name in ["a.yaml", "z1001.yaml"] {
            Mock::given(method("GET"))
                .and(path(format!("/repos/acme/big/contents/tpl/{name}")))
                .and(query_param("ref", sha))
                .respond_with(ResponseTemplate::new(200).set_body_string(format!("n: {name}")))
                .mount(&server)
                .await;
        }
        let fetcher = GithubFetcher::new(&GithubSource {
            repo: "acme/big".into(),
            r#ref: "main".into(),
            path: "tpl".into(),
            paths: Vec::new(),
            token: None,
            api_base: server.uri(),
        })
        .unwrap();
        let mut files = fetcher.list().await.unwrap();
        files.sort_by(|a, b| a.name.cmp(&b.name));
        let got: Vec<(&str, Option<&str>)> = files
            .iter()
            .map(|f| (f.name.as_str(), f.error.as_deref()))
            .collect();
        assert_eq!(got, vec![("a.yaml", None), ("z1001.yaml", None)]);
        assert_eq!(files[1].body, "n: z1001.yaml");

        // A truncated tree refuses rather than dropping templates.
        server.reset().await;
        Mock::given(method("GET"))
            .and(path("/repos/acme/big/contents/tpl"))
            .respond_with(ResponseTemplate::new(200).set_body_json(
                (0..1000)
                    .map(|i| serde_json::json!({"name": format!("t{i}"), "type": "file", "url": "x"}))
                    .collect::<Vec<_>>(),
            ))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/repos/acme/big/git/trees/{sha}")))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"truncated": true, "tree": []})),
            )
            .mount(&server)
            .await;
        let err = fetcher.list().await.unwrap_err().to_string();
        assert!(err.contains("too large to list"), "{err}");
    }

    #[tokio::test]
    async fn github_publish_writes_a_sink_template_to_the_sink_directory() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/repos/acme/hub/contents/sink-templates/files.yaml"))
            .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
                "content": {"html_url": "https://github.com/acme/hub/blob/main/sink-templates/files.yaml"}
            })))
            .expect(1)
            .mount(&server)
            .await;
        let fetcher = GithubFetcher::new(&GithubSource {
            repo: "acme/hub".into(),
            r#ref: "main".into(),
            path: String::new(),
            paths: vec!["source-templates".into(), "sink-templates".into()],
            token: None,
            api_base: server.uri(),
        })
        .unwrap();
        let loc = fetcher
            .put_kind(
                "files.yaml",
                "kind: sink-template",
                crate::hub::spec::TemplateKind::SinkTemplate,
            )
            .await
            .unwrap();
        assert!(loc.ends_with("sink-templates/files.yaml"), "{loc}");
        let err = fetcher
            .put_kind(
                "ops.yaml",
                "kind: deployment",
                crate::hub::spec::TemplateKind::Deployment,
            )
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("holds deployment files"), "{err}");
    }

    #[tokio::test]
    async fn github_paths_lists_every_directory_as_one_origin() {
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        let entry = |name: &str| serde_json::json!({"name": name, "type": "file", "url": format!("{}/raw/{name}", server.uri()), "sha": "abc"});
        // An owner directory (#682) lists as `owner/name` stems.
        Mock::given(method("GET"))
            .and(path("/repos/acme/hub/contents/source-templates"))
            .and(query_param("ref", "main"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                entry("acme.yaml"),
                {"name": "octo", "type": "dir", "url": format!("{}/x", server.uri())}
            ])))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/repos/acme/hub/contents/source-templates/octo"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                entry("hr.yaml"),
                entry("hr.faucet.yaml")
            ])))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/repos/acme/hub/contents/sink-templates"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!([entry("files.yaml")])),
            )
            .mount(&server)
            .await;
        for name in ["acme.yaml", "files.yaml", "hr.yaml", "hr.faucet.yaml"] {
            let body = if name.contains(".faucet.") {
                "launch: true\n".to_string()
            } else {
                format!("name: {name}")
            };
            Mock::given(method("GET"))
                .and(path(format!("/raw/{name}")))
                .respond_with(ResponseTemplate::new(200).set_body_string(body))
                .mount(&server)
                .await;
        }
        let fetcher = GithubFetcher::new(&GithubSource {
            repo: "acme/hub".into(),
            r#ref: "main".into(),
            path: String::new(),
            paths: vec!["source-templates".into(), "sink-templates".into()],
            token: None,
            api_base: server.uri(),
        })
        .unwrap();
        let mut files = fetcher.list().await.expect("list");
        files.sort_by(|a, b| a.name.cmp(&b.name));
        assert_eq!(
            files.iter().map(|f| f.name.as_str()).collect::<Vec<_>>(),
            [
                "acme.yaml",
                "files.yaml",
                "octo/hr.faucet.yaml",
                "octo/hr.yaml"
            ]
        );
        let paired = pair_files(files);
        let stems: Vec<&str> = paired.templates.iter().map(|t| t.stem.as_str()).collect();
        assert_eq!(stems, ["acme", "files", "octo/hr"]);
        assert!(
            paired.templates[2].sidecar.is_some(),
            "the sidecar pairs across the owner directory"
        );
        // `publish` targets the first directory.
        assert_eq!(fetcher.dir(), "source-templates");
        assert!(
            fetcher
                .contents_url(Some("x.yaml"))
                .ends_with("/contents/source-templates/x.yaml")
        );
    }
}
