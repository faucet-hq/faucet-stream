//! Remote hub catalogs (#677): `--hub github:owner/repo[@ref][/path]` fetches
//! a catalog laid out like `hub/` through the GitHub contents API and caches
//! it on disk, pinned to the commit it was taken from.
//!
//! The cache is the unit of correctness: a fetch is either complete (a
//! `.complete` marker beside the files) or absent, never partial, so a run
//! that composes from the cache sees one consistent commit. When the network
//! is unreachable the last complete fetch is used with a warning — never an
//! empty catalog, which would turn "offline" into "no such template".

use std::path::{Path, PathBuf};

use futures::StreamExt as _;

use super::HubLocation;
use crate::error::{CliError, CliResult};

/// Directories a hub catalog is made of. `examples` is fetched so the shipped
/// `example-csv` template runs offline from a remote hub too.
const HUB_DIRS: &[&str] = &["source-templates", "sink-templates", "examples"];
const FETCH_CONCURRENCY: usize = 8;
const COMPLETE_MARKER: &str = ".complete";
/// Written into every snapshot so a later `@version` lookup knows which remote
/// to fetch an older catalog commit from.
const LOCATION_MARKER: &str = ".hub-location";
const CURRENT_FILE: &str = "current";
/// Set to any non-empty value to skip the network and use the cache only.
pub const OFFLINE_ENV: &str = "FAUCET_HUB_OFFLINE";
/// Override the cache root (default `$XDG_CACHE_HOME/faucet/hub`, else
/// `%LOCALAPPDATA%/faucet/hub` on Windows, else `~/.cache/faucet/hub`).
pub const CACHE_ENV: &str = "FAUCET_HUB_CACHE";

/// Where remote hubs are cached.
pub fn cache_root() -> PathBuf {
    cache_root_from(|k| std::env::var(k).ok(), cfg!(windows))
}

fn cache_root_from(var: impl Fn(&str) -> Option<String>, windows: bool) -> PathBuf {
    let set = |k: &str| var(k).filter(|v| !v.trim().is_empty());
    if let Some(p) = set(CACHE_ENV) {
        return PathBuf::from(p);
    }
    if let Some(x) = set("XDG_CACHE_HOME") {
        return PathBuf::from(x).join("faucet").join("hub");
    }
    if windows && let Some(local) = set("LOCALAPPDATA") {
        return PathBuf::from(local).join("faucet").join("hub");
    }
    let home = set("HOME")
        .or_else(|| if windows { set("USERPROFILE") } else { None })
        .unwrap_or_else(|| ".".into());
    PathBuf::from(home)
        .join(".cache")
        .join("faucet")
        .join("hub")
}

/// GitHub contents-API client for one hub location.
pub struct GithubHub {
    client: reqwest::Client,
    api_base: String,
    repo: String,
    r#ref: String,
    path: String,
    token: Option<String>,
    /// The commit [`Self::head_sha`] resolved; listings read it once set.
    at: std::sync::OnceLock<String>,
}

#[derive(Debug, serde::Deserialize)]
struct Entry {
    name: String,
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    download_url: Option<String>,
}

fn net_err(what: &str, e: impl std::fmt::Display) -> CliError {
    CliError::Config(format!("hub github: {what}: {e}"))
}

/// A catalog's `index.json` at the hub root: version history, `stable`, trust.
const INDEX_FILE: &str = "index.json";

/// Whether `commit` is a full git commit id. A catalog's `index.json` is
/// remote data and its commits become cache paths, so nothing else is used.
pub fn is_commit_id(commit: &str) -> bool {
    commit.len() == 40 && commit.chars().all(|c| c.is_ascii_hexdigit())
}

/// The first seven characters of a commit id, for messages.
pub fn short_commit(commit: &str) -> String {
    commit.chars().take(7).collect()
}

/// The environment variable holding a token for one owner's repos (#696):
/// `FAUCET_GITHUB_TOKEN_<OWNER>`, the owner upper-cased with `-` and `.` as `_`.
pub fn owner_token_var(repo: &str) -> String {
    let owner = repo.split('/').next().unwrap_or(repo);
    let owner: String = owner
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect();
    format!("FAUCET_GITHUB_TOKEN_{owner}")
}

/// The token for `repo`: its owner's own variable first, so a private hub in
/// one org and the public hub can be read in one invocation, then the global
/// `FAUCET_GITHUB_TOKEN` / `GITHUB_TOKEN`.
pub fn github_token(repo: &str) -> Option<String> {
    [
        owner_token_var(repo),
        "FAUCET_GITHUB_TOKEN".into(),
        "GITHUB_TOKEN".into(),
    ]
    .into_iter()
    .find_map(|var| std::env::var(var).ok().filter(|t| !t.trim().is_empty()))
}

impl GithubHub {
    /// `api_base` is `https://api.github.com` in production and a mock server
    /// in tests. A `GITHUB_TOKEN` in the environment is used when present, so a
    /// private catalog works and the unauthenticated rate limit is avoided.
    pub fn new(loc: &HubLocation, api_base: &str) -> CliResult<Self> {
        let HubLocation::Github { repo, r#ref, path } = loc else {
            return Err(CliError::Internal(
                "GithubHub::new called with a local hub location".into(),
            ));
        };
        let client = reqwest::Client::builder()
            .user_agent(concat!("faucet-stream/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|e| net_err("building HTTP client", e))?;
        let token = github_token(repo);
        Ok(Self {
            client,
            api_base: api_base.trim_end_matches('/').to_string(),
            repo: repo.clone(),
            r#ref: r#ref.clone(),
            path: path.trim_matches('/').to_string(),
            token,
            at: std::sync::OnceLock::new(),
        })
    }

    fn get(&self, url: &str, accept: &str) -> reqwest::RequestBuilder {
        let mut r = self
            .client
            .get(url)
            .header("Accept", accept)
            .header("X-GitHub-Api-Version", "2022-11-28");
        if let Some(t) = &self.token {
            r = r.bearer_auth(t);
        }
        r
    }

    async fn send(&self, req: reqwest::RequestBuilder, what: &str) -> CliResult<reqwest::Response> {
        let resp = req.send().await.map_err(|e| net_err(what, e))?;
        self.send_checked(resp, what).await
    }

    async fn send_checked(
        &self,
        resp: reqwest::Response,
        what: &str,
    ) -> CliResult<reqwest::Response> {
        let status = resp.status();
        if status.is_success() {
            return Ok(resp);
        }
        let body = resp.text().await.unwrap_or_default();
        let hint = match status.as_u16() {
            401 | 403 => {
                " (set GITHUB_TOKEN — the anonymous API limit is 60 requests/hour, and a private repo needs a token)"
            }
            404 => " (check the repo, ref and path)",
            _ => "",
        };
        Err(net_err(
            what,
            format!(
                "HTTP {status}{hint}: {}",
                body.chars().take(200).collect::<String>()
            ),
        ))
    }

    /// The commit the ref points at — one cheap request that decides whether
    /// the cache is current.
    pub async fn head_sha(&self) -> CliResult<String> {
        let url = format!(
            "{}/repos/{}/commits/{}",
            self.api_base, self.repo, self.r#ref
        );
        let resp = self
            .send(
                self.get(&url, "application/vnd.github.sha"),
                "resolving ref",
            )
            .await?;
        let sha = resp
            .text()
            .await
            .map_err(|e| net_err("resolving ref", e))?
            .trim()
            .to_string();
        if sha.len() < 7 || !sha.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(net_err(
                "resolving ref",
                format!("unexpected commit id '{sha}'"),
            ));
        }
        // Every later listing and download reads this commit, so a push
        // during the download cannot mix two revisions into one snapshot.
        let _ = self.at.set(sha.clone());
        Ok(sha)
    }

    fn contents_url(&self, rel: &str) -> String {
        let mut p = self.path.clone();
        if !rel.is_empty() {
            if !p.is_empty() {
                p.push('/');
            }
            p.push_str(rel);
        }
        format!(
            "{}/repos/{}/contents/{p}?ref={}",
            self.api_base,
            self.repo,
            self.at.get().unwrap_or(&self.r#ref)
        )
    }

    /// List a directory; `Ok(None)` when it does not exist (a hub may have no
    /// `examples/`).
    async fn list_dir(&self, rel: &str) -> CliResult<Option<Vec<Entry>>> {
        let url = self.contents_url(rel);
        let resp = self
            .get(&url, "application/vnd.github+json")
            .send()
            .await
            .map_err(|e| net_err(&format!("listing {rel}"), e))?;
        if resp.status().as_u16() == 404 {
            return Ok(None);
        }
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(net_err(
                &format!("listing {rel}"),
                format!(
                    "HTTP {status}: {}",
                    body.chars().take(200).collect::<String>()
                ),
            ));
        }
        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| net_err(&format!("listing {rel}"), e))?;
        match body {
            serde_json::Value::Array(_) => Ok(Some(
                serde_json::from_value(body).map_err(|e| net_err(&format!("listing {rel}"), e))?,
            )),
            _ => Err(CliError::Config(format!(
                "hub github: '{}' in {} is a file, not a directory",
                self.path, self.repo
            ))),
        }
    }

    /// Every file under the hub's directories (one level of subdirectories,
    /// enough for `examples/data/`), with its path relative to the hub root.
    async fn walk(&self) -> CliResult<Vec<(String, String)>> {
        let mut files: Vec<(String, String)> = Vec::new();
        let mut found_any = false;
        for dir in HUB_DIRS {
            let Some(entries) = self.list_dir(dir).await? else {
                continue;
            };
            found_any = true;
            let mut pending: Vec<(String, Entry)> = entries
                .into_iter()
                .map(|e| ((*dir).to_string(), e))
                .collect();
            while let Some((parent, e)) = pending.pop() {
                let rel = format!("{parent}/{}", e.name);
                match e.kind.as_str() {
                    "file" => {
                        if let Some(url) = e.download_url {
                            files.push((rel, url));
                        }
                    }
                    "dir" if parent.matches('/').count() < 2 => {
                        let sub_rel = rel.clone();
                        if let Some(sub) = self.list_dir(&sub_rel).await? {
                            pending.extend(sub.into_iter().map(|s| (sub_rel.clone(), s)));
                        }
                    }
                    _ => {}
                }
            }
        }
        if !found_any {
            return Err(CliError::Config(format!(
                "hub github: {}@{}{} has no source-templates/ or sink-templates/ directory — is this a hub catalog?",
                self.repo,
                self.r#ref,
                if self.path.is_empty() {
                    String::new()
                } else {
                    format!("/{}", self.path)
                }
            )));
        }
        Ok(files)
    }

    /// A file at the hub root, `None` when the catalog has none.
    async fn fetch_optional(&self, rel: &str) -> CliResult<Option<Vec<u8>>> {
        let resp = self
            .get(&self.contents_url(rel), "application/vnd.github.raw+json")
            .send()
            .await
            .map_err(|e| net_err(&format!("fetching {rel}"), e))?;
        if resp.status().as_u16() == 404 {
            return Ok(None);
        }
        let resp = self.send_checked(resp, &format!("fetching {rel}")).await?;
        resp.bytes()
            .await
            .map(|b| Some(b.to_vec()))
            .map_err(|e| net_err(&format!("fetching {rel}"), e))
    }

    /// Download a complete snapshot of the hub into `dest` (created),
    /// including the root `index.json` when the catalog has one — it holds the
    /// launched (`stable`) version of each template and the commits of older
    /// ones, without which an unpinned run would take the newest body.
    pub async fn download(&self, dest: &Path) -> CliResult<()> {
        let files = self.walk().await?;
        std::fs::create_dir_all(dest)
            .map_err(|e| net_err(&format!("creating {}", dest.display()), e))?;
        let results: Vec<CliResult<()>> = futures::stream::iter(files)
            .map(|(rel, url)| async move {
                let resp = self
                    .send(
                        self.get(&url, "application/octet-stream"),
                        &format!("downloading {rel}"),
                    )
                    .await?;
                let bytes = resp
                    .bytes()
                    .await
                    .map_err(|e| net_err(&format!("downloading {rel}"), e))?;
                let target = dest.join(&rel);
                if let Some(parent) = target.parent() {
                    std::fs::create_dir_all(parent)
                        .map_err(|e| net_err(&format!("creating {}", parent.display()), e))?;
                }
                std::fs::write(&target, &bytes)
                    .map_err(|e| net_err(&format!("writing {}", target.display()), e))?;
                Ok(())
            })
            .buffer_unordered(FETCH_CONCURRENCY)
            .collect()
            .await;
        results.into_iter().collect::<CliResult<Vec<()>>>()?;
        if let Some(index) = self.fetch_optional(INDEX_FILE).await? {
            std::fs::write(dest.join(INDEX_FILE), index)
                .map_err(|e| net_err(&format!("writing {INDEX_FILE}"), e))?;
        }
        std::fs::write(
            dest.join(LOCATION_MARKER),
            format!(
                "github:{}@{}{}",
                self.repo,
                self.r#ref,
                if self.path.is_empty() {
                    String::new()
                } else {
                    format!("/{}", self.path)
                }
            ),
        )
        .map_err(|e| net_err("writing location marker", e))?;
        std::fs::write(dest.join(COMPLETE_MARKER), b"")
            .map_err(|e| net_err("writing cache marker", e))?;
        Ok(())
    }

    /// Read one file at an exact commit through the contents API (raw body).
    pub async fn read_file_at(&self, commit: &str, rel: &str) -> CliResult<Vec<u8>> {
        let mut p = self.path.clone();
        if !p.is_empty() {
            p.push('/');
        }
        p.push_str(rel.trim_matches('/'));
        let url = format!(
            "{}/repos/{}/contents/{p}?ref={commit}",
            self.api_base, self.repo
        );
        let resp = self
            .send(
                self.get(&url, "application/vnd.github.raw+json"),
                &format!("fetching {rel} @ {}", short_commit(commit)),
            )
            .await?;
        resp.bytes()
            .await
            .map(|b| b.to_vec())
            .map_err(|e| net_err(&format!("fetching {rel}"), e))
    }
}

/// The remote a cached snapshot was fetched from, when `dir` is one.
pub fn snapshot_location(dir: &Path) -> Option<HubLocation> {
    let text = std::fs::read_to_string(dir.join(LOCATION_MARKER)).ok()?;
    HubLocation::parse(text.trim())
        .ok()
        .filter(HubLocation::is_remote)
}

/// Fetch one catalog file at an exact commit into the cache
/// (`<key>/files/<commit>/<rel>`) and return its path; a cached copy is reused.
pub async fn fetch_file_at(
    loc: &HubLocation,
    cache_root: &Path,
    api_base: &str,
    commit: &str,
    rel: &str,
) -> CliResult<PathBuf> {
    if !is_commit_id(commit) {
        return Err(CliError::Config(format!(
            "hub {}: the catalog index names commit {commit:?} for {rel}, which is not a full \
             40-character commit id",
            loc.describe()
        )));
    }
    let target = cache_root
        .join(loc.cache_key())
        .join("files")
        .join(commit)
        .join(rel.trim_matches('/'));
    if target.is_file() {
        return Ok(target);
    }
    let hub = GithubHub::new(loc, api_base)?;
    let bytes = hub.read_file_at(commit, rel).await?;
    if let Some(parent) = target.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| net_err(&format!("creating {}", parent.display()), e))?;
    }
    let tmp = target.with_extension(format!("tmp-{}", std::process::id()));
    std::fs::write(&tmp, &bytes).map_err(|e| net_err(&format!("writing {}", tmp.display()), e))?;
    std::fs::rename(&tmp, &target)
        .map_err(|e| net_err(&format!("installing {}", target.display()), e))?;
    Ok(target)
}

fn is_complete(dir: &Path) -> bool {
    dir.join(COMPLETE_MARKER).is_file()
}

fn current_snapshot(key_dir: &Path) -> Option<PathBuf> {
    let sha = std::fs::read_to_string(key_dir.join(CURRENT_FILE)).ok()?;
    let dir = key_dir.join(sha.trim());
    is_complete(&dir).then_some(dir)
}

fn offline_requested() -> bool {
    std::env::var(OFFLINE_ENV).is_ok_and(|v| !v.trim().is_empty())
}

/// Resolve a remote hub to a local directory: the cached snapshot of the
/// ref's current commit, fetched when missing. Falls back to the last
/// complete snapshot (with a warning) when the network is unavailable or
/// `FAUCET_HUB_OFFLINE` is set.
pub async fn fetch_cached(
    loc: &HubLocation,
    cache_root: &Path,
    api_base: &str,
) -> CliResult<PathBuf> {
    let key_dir = cache_root.join(loc.cache_key());
    let cached = current_snapshot(&key_dir);

    if offline_requested() {
        return cached.ok_or_else(|| {
            CliError::Config(format!(
                "{OFFLINE_ENV} is set but {} has never been fetched (nothing under {})",
                loc.describe(),
                key_dir.display()
            ))
        });
    }

    let hub = GithubHub::new(loc, api_base)?;
    let sha = match hub.head_sha().await {
        Ok(sha) => sha,
        Err(e) => {
            return match cached {
                Some(dir) => {
                    tracing::warn!(hub = %loc.describe(), error = %e, "hub unreachable — using the cached snapshot");
                    eprintln!(
                        "warning: {} unreachable ({e}); using the cached snapshot at {}",
                        loc.describe(),
                        dir.display()
                    );
                    Ok(dir)
                }
                None => Err(CliError::Config(format!(
                    "{e}\n  no cached copy of {} exists yet — connect once, or pass --hub <local directory>",
                    loc.describe()
                ))),
            };
        }
    };

    let snapshot = key_dir.join(&sha);
    if !is_complete(&snapshot) {
        let tmp = key_dir.join(format!(".tmp-{sha}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        hub.download(&tmp).await?;
        install_snapshot(&tmp, &snapshot)?;
        eprintln!(
            "fetched {} @ {} → {}",
            loc.describe(),
            short_commit(&sha),
            snapshot.display()
        );
    }
    let previous = std::fs::read_to_string(key_dir.join(CURRENT_FILE)).ok();
    std::fs::write(key_dir.join(CURRENT_FILE), &sha)
        .map_err(|e| net_err("recording current snapshot", e))?;
    prune_old(&key_dir, &sha, previous.as_deref().map(str::trim));
    Ok(snapshot)
}

/// Move a downloaded snapshot into place. A complete snapshot another
/// process installed meanwhile is never replaced — it may be in use.
fn install_snapshot(tmp: &Path, snapshot: &Path) -> CliResult<()> {
    if !is_complete(snapshot) {
        let _ = std::fs::remove_dir_all(snapshot);
        if let Err(e) = std::fs::rename(tmp, snapshot)
            && !is_complete(snapshot)
        {
            return Err(net_err(
                &format!("installing snapshot {}", snapshot.display()),
                e,
            ));
        }
    }
    let _ = std::fs::remove_dir_all(tmp);
    Ok(())
}

/// Whether a directory entry was modified within [`TMP_GRACE`] (or its age
/// cannot be read — then it is left alone).
fn is_recent(e: &std::fs::DirEntry) -> bool {
    e.metadata()
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.elapsed().ok())
        .is_none_or(|age| age < TMP_GRACE)
}

/// How long another process's download directory is left alone.
const TMP_GRACE: std::time::Duration = std::time::Duration::from_secs(3600);

/// Drop snapshots older than the current and the one it replaced (a run that
/// started just before the switch may still read it), keeping the pinned
/// `files/` cache and other processes' recent downloads.
fn prune_old(key_dir: &Path, keep: &str, previous: Option<&str>) {
    let Ok(rd) = std::fs::read_dir(key_dir) else {
        return;
    };
    for e in rd.flatten() {
        let name = e.file_name();
        let name = name.to_string_lossy();
        let spared = name == keep
            || name == CURRENT_FILE
            || name == "files"
            || Some(&*name) == previous
            || (name.starts_with(".tmp-") && is_recent(&e));
        if spared {
            continue;
        }
        if e.path().is_dir() {
            let _ = std::fs::remove_dir_all(e.path());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use wiremock::matchers::{header, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn loc() -> HubLocation {
        HubLocation::Github {
            repo: "acme/hub".into(),
            r#ref: "main".into(),
            path: String::new(),
        }
    }

    fn entry(name: &str, kind: &str, parent: &str, server: &str) -> serde_json::Value {
        json!({
            "name": name, "type": kind, "path": format!("{parent}/{name}"),
            "download_url": if kind == "file" { Some(format!("{server}/raw/{parent}/{name}")) } else { None }
        })
    }

    async fn mock_hub(server: &MockServer, sha: &str) {
        let base = server.uri();
        Mock::given(method("GET"))
            .and(path("/repos/acme/hub/commits/main"))
            .and(header("Accept", "application/vnd.github.sha"))
            .respond_with(ResponseTemplate::new(200).set_body_string(sha))
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .and(path("/repos/acme/hub/contents/source-templates"))
            .and(query_param("ref", sha))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([
                entry("acme.yaml", "file", "source-templates", &base),
                entry(".keep", "file", "source-templates", &base),
            ])))
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .and(path("/repos/acme/hub/contents/sink-templates"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([entry(
                "files.yaml",
                "file",
                "sink-templates",
                &base
            ),])))
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .and(path("/repos/acme/hub/contents/examples"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!([entry("data", "dir", "examples", &base),])),
            )
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .and(path("/repos/acme/hub/contents/examples/data"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([entry(
                "t.csv",
                "file",
                "examples/data",
                &base
            ),])))
            .mount(server)
            .await;
        for (p, body) in [
            (
                "/raw/source-templates/acme.yaml",
                "kind: source-template\nname: acme\nsource: {type: rest, config: {base_url: \"https://a\", path: /}}\nstreams: [{name: t}]\n",
            ),
            ("/raw/source-templates/.keep", ""),
            (
                "/raw/sink-templates/files.yaml",
                "kind: sink-template\nname: files\nsink: {type: jsonl, config: {}}\nper_stream: {path: \"./out/${stream}.jsonl\"}\n",
            ),
            ("/raw/examples/data/t.csv", "id\n1\n"),
        ] {
            Mock::given(method("GET"))
                .and(path(p))
                .respond_with(ResponseTemplate::new(200).set_body_string(body))
                .mount(server)
                .await;
        }
    }

    #[tokio::test]
    #[serial_test::serial(hub_env)]
    async fn fetches_caches_and_reuses_a_snapshot() {
        let server = MockServer::start().await;
        mock_hub(&server, "0123456789abcdef").await;
        let cache = tempfile::tempdir().unwrap();
        // SAFETY: tests in this module never run the offline path concurrently
        // with a different value; the variable is cleared right after.
        unsafe { std::env::remove_var(OFFLINE_ENV) };

        let dir = fetch_cached(&loc(), cache.path(), &server.uri())
            .await
            .expect("fetch");
        assert!(dir.join("source-templates/acme.yaml").is_file());
        assert!(dir.join("sink-templates/files.yaml").is_file());
        assert!(
            dir.join("examples/data/t.csv").is_file(),
            "one level of subdirectories is walked"
        );
        assert!(dir.join(".complete").is_file());
        assert!(dir.ends_with("0123456789abcdef"));
        // The snapshot is a real hub: the catalog loads and composes.
        let cat = crate::hub::Catalog::load(&dir).expect("catalog");
        assert_eq!(cat.sources.len(), 1);
        assert_eq!(cat.sinks.len(), 1);

        // Same sha → the cache is reused: only the ref lookup hits the server.
        let before = server.received_requests().await.unwrap().len();
        let again = fetch_cached(&loc(), cache.path(), &server.uri())
            .await
            .expect("cached");
        assert_eq!(again, dir);
        let after = server.received_requests().await.unwrap().len();
        assert_eq!(after - before, 1, "one request: the commit lookup");

        // A new commit becomes current; the one it replaced is kept for a
        // run that started before the switch (#789 CLI-104) and pruned on the
        // next advance.
        server.reset().await;
        mock_hub(&server, "fedcba9876543210").await;
        let newer = fetch_cached(&loc(), cache.path(), &server.uri())
            .await
            .expect("refetch");
        assert!(newer.ends_with("fedcba9876543210"));
        assert!(dir.exists(), "the previous snapshot is kept one generation");
        assert_eq!(
            std::fs::read_to_string(cache.path().join(loc().cache_key()).join("current")).unwrap(),
            "fedcba9876543210"
        );
        server.reset().await;
        mock_hub(&server, "aaaabbbbccccdddd").await;
        fetch_cached(&loc(), cache.path(), &server.uri())
            .await
            .expect("third fetch");
        assert!(!dir.exists(), "two generations back is pruned");
        assert!(newer.exists());
    }

    #[tokio::test]
    #[serial_test::serial(hub_env)]
    async fn a_remote_snapshot_carries_the_index_and_an_unpinned_run_takes_stable() {
        let server = MockServer::start().await;
        mock_hub(&server, "0123456789abcdef").await;
        let stable_commit = "1".repeat(40);
        let index = json!({
            "commit": "2".repeat(40),
            "sources": [{"id": "acme", "name": "acme", "newest": 2, "stable": 1,
                "versions": [{"version": 1, "commit": stable_commit}, {"version": 2, "commit": "2".repeat(40)}]}],
            "sinks": []
        });
        Mock::given(method("GET"))
            .and(path("/repos/acme/hub/contents/index.json"))
            .and(header("Accept", "application/vnd.github.raw+json"))
            .respond_with(ResponseTemplate::new(200).set_body_string(index.to_string()))
            .mount(&server)
            .await;
        let cache = tempfile::tempdir().unwrap();
        // SAFETY: serial(hub_env); restored below.
        unsafe {
            std::env::remove_var(OFFLINE_ENV);
            std::env::set_var(CACHE_ENV, cache.path());
        }
        let dir = fetch_cached(&loc(), cache.path(), &server.uri())
            .await
            .expect("fetch");
        assert!(
            dir.join("index.json").is_file(),
            "index.json is part of the snapshot"
        );

        // The launched version lives at another commit; seed the pinned-file
        // cache so resolving it needs no network.
        let pinned = cache
            .path()
            .join(loc().cache_key())
            .join("files")
            .join(&stable_commit)
            .join("source-templates/acme.yaml");
        std::fs::create_dir_all(pinned.parent().unwrap()).unwrap();
        std::fs::write(&pinned, "kind: source-template\nname: acme\n").unwrap();
        let got = crate::hub::locate("acme", &dir, crate::hub::catalog::SOURCE_DIR).await;
        unsafe { std::env::remove_var(CACHE_ENV) };
        assert_eq!(
            got.expect("locate"),
            pinned,
            "unpinned resolves to stable, not newest"
        );
    }

    #[tokio::test]
    #[serial_test::serial(hub_env)]
    async fn a_catalog_without_an_index_downloads_cleanly() {
        let server = MockServer::start().await;
        mock_hub(&server, "0123456789abcdef").await;
        let cache = tempfile::tempdir().unwrap();
        let dir = fetch_cached(&loc(), cache.path(), &server.uri())
            .await
            .expect("fetch");
        assert!(!dir.join("index.json").exists());
        assert!(dir.join(".complete").is_file());
    }

    #[tokio::test]
    async fn an_index_commit_must_be_a_full_commit_id() {
        let cache = tempfile::tempdir().unwrap();
        for bad in ["/etc", "../x", "abc", "é".repeat(40).as_str()] {
            let err = fetch_file_at(&loc(), cache.path(), "http://127.0.0.1:1", bad, "a.yaml")
                .await
                .unwrap_err()
                .to_string();
            assert!(err.contains("40-character"), "{bad}: {err}");
        }
        assert!(is_commit_id(&"a".repeat(40)));
        assert!(!is_commit_id(&"g".repeat(40)));
        assert_eq!(short_commit("é".repeat(10).as_str()), "é".repeat(7));
    }

    #[test]
    fn install_never_replaces_a_complete_snapshot_and_prune_spares_pins_and_downloads() {
        let root = tempfile::tempdir().unwrap();
        let snap = root.path().join("abc");
        std::fs::create_dir_all(&snap).unwrap();
        std::fs::write(snap.join(".complete"), b"").unwrap();
        std::fs::write(snap.join("marker"), b"theirs").unwrap();
        let tmp = root.path().join(".tmp-abc-1");
        std::fs::create_dir_all(&tmp).unwrap();
        std::fs::write(tmp.join("marker"), b"ours").unwrap();
        install_snapshot(&tmp, &snap).unwrap();
        assert_eq!(std::fs::read(snap.join("marker")).unwrap(), b"theirs");
        assert!(!tmp.exists());

        let fresh = root.path().join("def");
        let tmp = root.path().join(".tmp-def-1");
        std::fs::create_dir_all(&tmp).unwrap();
        std::fs::write(tmp.join(".complete"), b"").unwrap();
        install_snapshot(&tmp, &fresh).unwrap();
        assert!(is_complete(&fresh));
        let err = install_snapshot(&root.path().join(".tmp-missing"), &root.path().join("ghi"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("installing snapshot"), "{err}");

        for d in ["files/x", "old", "prev", ".tmp-live-2"] {
            std::fs::create_dir_all(root.path().join(d)).unwrap();
        }
        prune_old(root.path(), "def", Some("prev"));
        assert!(root.path().join("files/x").exists());
        assert!(root.path().join("prev").exists());
        assert!(
            root.path().join(".tmp-live-2").exists(),
            "a recent download is spared"
        );
        assert!(!root.path().join("old").exists());
        assert!(!root.path().join("abc").exists());
    }

    #[tokio::test]
    #[serial_test::serial(hub_env)]
    async fn unreachable_falls_back_to_the_cache_or_errors_clearly() {
        let server = MockServer::start().await;
        mock_hub(&server, "0123456789abcdef").await;
        let cache = tempfile::tempdir().unwrap();
        let dir = fetch_cached(&loc(), cache.path(), &server.uri())
            .await
            .expect("fetch");
        // A port nothing listens on: a dropped mock server's port can be
        // reused by a sibling test's server that mocks the same paths.
        let dead = "http://127.0.0.1:1".to_string();
        drop(server);

        let fallback = fetch_cached(&loc(), cache.path(), &dead)
            .await
            .expect("cached fallback");
        assert_eq!(fallback, dir);

        let empty = tempfile::tempdir().unwrap();
        let err = fetch_cached(&loc(), empty.path(), &dead)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("no cached copy"), "{err}");
        assert!(err.contains("--hub <local directory>"), "{err}");
    }

    #[tokio::test]
    #[serial_test::serial(hub_env)]
    async fn a_repo_without_hub_directories_is_refused() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/repos/acme/hub/commits/main"))
            .respond_with(ResponseTemplate::new(200).set_body_string("0123456789abcdef"))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(404).set_body_string("Not Found"))
            .mount(&server)
            .await;
        let cache = tempfile::tempdir().unwrap();
        let err = fetch_cached(&loc(), cache.path(), &server.uri())
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("no source-templates/ or sink-templates/"),
            "{err}"
        );
        assert!(
            !cache
                .path()
                .join(loc().cache_key())
                .join("current")
                .exists(),
            "nothing recorded"
        );
    }

    #[tokio::test]
    #[serial_test::serial(hub_env)]
    async fn api_errors_carry_a_hint() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/repos/acme/hub/commits/main"))
            .respond_with(ResponseTemplate::new(403).set_body_string("rate limit"))
            .mount(&server)
            .await;
        let hub = GithubHub::new(&loc(), &server.uri()).unwrap();
        let err = hub.head_sha().await.unwrap_err().to_string();
        assert!(err.contains("GITHUB_TOKEN"), "{err}");
        assert!(err.contains("HTTP 403"), "{err}");

        let server2 = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string("not-a-sha!"))
            .mount(&server2)
            .await;
        let hub = GithubHub::new(&loc(), &server2.uri()).unwrap();
        assert!(
            hub.head_sha()
                .await
                .unwrap_err()
                .to_string()
                .contains("unexpected commit id")
        );
    }

    #[test]
    fn an_owners_own_token_wins_over_the_global_one() {
        assert_eq!(
            owner_token_var("acme-corp/hub"),
            "FAUCET_GITHUB_TOKEN_ACME_CORP"
        );
        assert_eq!(owner_token_var("My.Org/x"), "FAUCET_GITHUB_TOKEN_MY_ORG");
        // A unique owner, so no other test's environment can interfere.
        let var = owner_token_var("zz-owner-696/hub");
        // SAFETY: the variable name is unique to this test.
        unsafe { std::env::set_var(&var, "owner-tok") };
        assert_eq!(
            github_token("zz-owner-696/hub").as_deref(),
            Some("owner-tok")
        );
        unsafe { std::env::set_var(&var, "  ") };
        assert_ne!(
            github_token("zz-owner-696/hub").as_deref(),
            Some("  "),
            "blank is unset"
        );
        unsafe { std::env::remove_var(&var) };
    }

    #[tokio::test]
    #[serial_test::serial(hub_env)]
    async fn listing_errors_and_file_bodies_are_reported_and_a_token_is_sent() {
        // A local location is a programming error, not a network one.
        let err = match GithubHub::new(&HubLocation::Dir(PathBuf::from("/x")), "http://127.0.0.1:1")
        {
            Ok(_) => panic!("a directory is not a GitHub hub"),
            Err(e) => e,
        };
        assert!(matches!(err, CliError::Internal(_)), "{err}");

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/repos/acme/hub/commits/main"))
            .and(header("Authorization", "Bearer t0k"))
            .respond_with(ResponseTemplate::new(200).set_body_string("0123456789abcdef"))
            .mount(&server)
            .await;
        // source-templates: a server error; sink-templates: a file, not a dir.
        Mock::given(method("GET"))
            .and(path("/repos/acme/hub/contents/source-templates"))
            .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/repos/acme/hub/contents/sink-templates"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"name": "sink-templates", "type": "file"})),
            )
            .mount(&server)
            .await;
        // SAFETY (test): env var private to this test binary; cleared below.
        unsafe { std::env::set_var("FAUCET_GITHUB_TOKEN", "t0k") };
        let hub = GithubHub::new(&loc(), &server.uri()).unwrap();
        unsafe { std::env::remove_var("FAUCET_GITHUB_TOKEN") };
        assert_eq!(
            hub.head_sha().await.unwrap(),
            "0123456789abcdef",
            "token header accepted by the mock"
        );
        let err = hub
            .list_dir("source-templates")
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("listing source-templates") && err.contains("HTTP 500"),
            "{err}"
        );
        let err = hub
            .list_dir("sink-templates")
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("is a file, not a directory"), "{err}");

        // 404 on the ref itself carries the repo/ref hint.
        let server2 = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(404).set_body_string("Not Found"))
            .mount(&server2)
            .await;
        let hub = GithubHub::new(&loc(), &server2.uri()).unwrap();
        let err = hub.head_sha().await.unwrap_err().to_string();
        assert!(err.contains("check the repo, ref and path"), "{err}");

        // A path inside the repo is part of every contents URL.
        let nested = HubLocation::Github {
            repo: "acme/hub".into(),
            r#ref: "v1".into(),
            path: "catalog/hub".into(),
        };
        let hub = GithubHub::new(&nested, "https://api.example").unwrap();
        assert_eq!(
            hub.contents_url("source-templates"),
            "https://api.example/repos/acme/hub/contents/catalog/hub/source-templates?ref=v1"
        );
        assert_eq!(
            hub.contents_url(""),
            "https://api.example/repos/acme/hub/contents/catalog/hub?ref=v1"
        );
    }

    #[tokio::test]
    #[serial_test::serial(hub_env)]
    async fn snapshots_remember_their_remote_and_old_versions_fetch_by_commit() {
        let server = MockServer::start().await;
        mock_hub(&server, "0123456789abcdef").await;
        Mock::given(method("GET"))
            .and(path("/repos/acme/hub/contents/source-templates/acme.yaml"))
            .and(query_param(
                "ref",
                "aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111",
            ))
            .and(header("Accept", "application/vnd.github.raw+json"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string("kind: source-template\nname: acme\n# v1\n"),
            )
            .mount(&server)
            .await;
        let cache = tempfile::tempdir().unwrap();
        let dir = fetch_cached(&loc(), cache.path(), &server.uri())
            .await
            .expect("fetch");
        assert_eq!(
            snapshot_location(&dir),
            Some(loc()),
            "the snapshot names its remote"
        );
        assert_eq!(snapshot_location(cache.path()), None);

        let f = fetch_file_at(
            &loc(),
            cache.path(),
            &server.uri(),
            "aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111",
            "source-templates/acme.yaml",
        )
        .await
        .expect("fetch at commit");
        assert!(f.ends_with(
            "files/aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111/source-templates/acme.yaml"
        ));
        assert!(std::fs::read_to_string(&f).unwrap().contains("# v1"));
        let before = server.received_requests().await.unwrap().len();
        let again = fetch_file_at(
            &loc(),
            cache.path(),
            &server.uri(),
            "aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111",
            "source-templates/acme.yaml",
        )
        .await
        .unwrap();
        assert_eq!(again, f);
        assert_eq!(
            server.received_requests().await.unwrap().len(),
            before,
            "cached: no request"
        );
        let err = fetch_file_at(
            &loc(),
            cache.path(),
            &server.uri(),
            "bbbb2222bbbb2222bbbb2222bbbb2222bbbb2222",
            "source-templates/acme.yaml",
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("fetching source-templates/acme.yaml @ bbbb222"),
            "{err}"
        );
    }

    #[tokio::test]
    #[serial_test::serial(hub_env)]
    async fn offline_mode_uses_the_cache_and_never_the_network() {
        let server = MockServer::start().await;
        mock_hub(&server, "0123456789abcdef").await;
        let cache = tempfile::tempdir().unwrap();
        let dir = fetch_cached(&loc(), cache.path(), &server.uri())
            .await
            .expect("fetch");
        let before = server.received_requests().await.unwrap().len();
        // SAFETY (test): env var private to this test binary; cleared below.
        unsafe { std::env::set_var(OFFLINE_ENV, "1") };
        let offline = fetch_cached(&loc(), cache.path(), &server.uri()).await;
        let empty = tempfile::tempdir().unwrap();
        let missing = fetch_cached(&loc(), empty.path(), &server.uri()).await;
        unsafe { std::env::remove_var(OFFLINE_ENV) };
        assert_eq!(offline.expect("cached"), dir);
        assert_eq!(
            server.received_requests().await.unwrap().len(),
            before,
            "no request went out"
        );
        let err = missing.unwrap_err().to_string();
        assert!(
            err.contains(OFFLINE_ENV) && err.contains("never been fetched"),
            "{err}"
        );
    }

    #[test]
    #[serial_test::serial(hub_env)]
    fn cache_root_honours_overrides() {
        let env = |pairs: &'static [(&'static str, &'static str)]| {
            move |k: &str| {
                pairs
                    .iter()
                    .find(|(n, _)| *n == k)
                    .map(|(_, v)| v.to_string())
            }
        };
        let hub = |p: &str| PathBuf::from(p).join("faucet").join("hub");
        assert_eq!(
            cache_root_from(env(&[(CACHE_ENV, "/c"), ("XDG_CACHE_HOME", "/x")]), false),
            PathBuf::from("/c")
        );
        assert_eq!(
            cache_root_from(env(&[(CACHE_ENV, " "), ("XDG_CACHE_HOME", "/x")]), true),
            hub("/x")
        );
        assert_eq!(
            cache_root_from(env(&[("LOCALAPPDATA", "C:/L"), ("HOME", "/h")]), true),
            hub("C:/L")
        );
        assert_eq!(
            cache_root_from(env(&[("LOCALAPPDATA", "C:/L"), ("HOME", "/h")]), false),
            PathBuf::from("/h")
                .join(".cache")
                .join("faucet")
                .join("hub")
        );
        assert_eq!(
            cache_root_from(env(&[("USERPROFILE", "C:/U")]), true),
            PathBuf::from("C:/U")
                .join(".cache")
                .join("faucet")
                .join("hub")
        );
        assert_eq!(
            cache_root_from(env(&[("USERPROFILE", "C:/U")]), false),
            PathBuf::from(".").join(".cache").join("faucet").join("hub")
        );
        assert!(!cache_root().as_os_str().is_empty());
    }
}
