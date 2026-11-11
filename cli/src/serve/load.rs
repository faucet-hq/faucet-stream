//! Turn a submitted config body into expanded nodes, applying the workspace
//! `--default-config` base. Mirrors `PipelineConfig::from_path_async` but merges
//! a base `Value` and uses `from_value`. All `${env}`/`${file}`/`${secret}` and
//! `${vault:…}`-style directives resolve against the *server's* environment and
//! credentials (the documented privilege surface — spec §13).

use crate::config::{PipelineConfig, TransformSpec};
use crate::expand::{ExpandedNode, expand};
use crate::serve::error::ServeError;
use faucet_core::resolve::{ReferencePolicy, ResourceKind};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Ceiling on the discovery fan-out's describe call during submission — long
/// enough for a slow metadata endpoint, short enough that a hung upstream
/// can't tie up submit handlers indefinitely.
const SUBMIT_DISCOVERY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Who wrote a config body the server is about to load.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodyOrigin {
    /// The server's operator or an admin wrote it: a registered template, or
    /// a run already admitted when it was submitted.
    Trusted,
    /// The request carried it. Subprocess connectors run only when the server
    /// was started with `--allow-subprocess-connectors`.
    Caller { allow_subprocess: bool },
    /// The server's operator wrote it in a file the server reads (the
    /// triggers file): trusted, and its SQL connectors keep host access.
    ServerFile,
}

impl BodyOrigin {
    /// Whether SQL connectors in a body of this origin run sandboxed.
    pub fn confines_sql(self) -> bool {
        !matches!(self, BodyOrigin::ServerFile)
    }
}

/// The sandbox switches (fields annotated `x-faucet-sandbox`, see
/// [`faucet_core::resolve::sandbox_fields`]) a connector's config schema
/// declares — empty for a connector that has none or an unknown kind. Read
/// from the same schema `faucet schema` prints, so a plugin connector that
/// declares one is confined like a built-in.
pub fn sandbox_fields(is_source: bool, kind: &str) -> std::sync::Arc<[String]> {
    type Cache =
        std::sync::RwLock<std::collections::HashMap<(bool, String), std::sync::Arc<[String]>>>;
    static CACHE: std::sync::OnceLock<Cache> = std::sync::OnceLock::new();
    let cache = CACHE.get_or_init(Default::default);
    let key = (is_source, kind.to_string());
    if let Some(hit) = cache.read().ok().and_then(|c| c.get(&key).cloned()) {
        return hit;
    }
    let schema = if is_source {
        crate::registry::source_schema(kind)
    } else {
        crate::registry::sink_schema(kind)
    };
    let Ok(schema) = schema else {
        return std::sync::Arc::from(Vec::new());
    };
    let fields: std::sync::Arc<[String]> = faucet_core::resolve::sandbox_fields(&schema).into();
    if let Ok(mut c) = cache.write() {
        c.insert(key, std::sync::Arc::clone(&fields));
    }
    fields
}

/// Whether `faucet serve` confines a source of this kind.
pub fn source_is_sandboxed(kind: &str) -> bool {
    !sandbox_fields(true, kind).is_empty()
}

/// Whether `faucet serve` confines a sink of this kind.
pub fn sink_is_sandboxed(kind: &str) -> bool {
    !sandbox_fields(false, kind).is_empty()
}

fn force_fields(config: &mut Value, fields: &[String]) {
    if let Value::Object(map) = config {
        for f in fields {
            map.insert(f.clone(), Value::Bool(true));
        }
    }
}

fn force_source(config: &mut Value, kind: &str) {
    force_fields(config, &sandbox_fields(true, kind));
}

fn force_sink(config: &mut Value, kind: &str) {
    force_fields(config, &sandbox_fields(false, kind));
}

fn force_override(config: &mut Option<Value>, is_source: bool, kind: &str) {
    let fields = sandbox_fields(is_source, kind);
    if !fields.is_empty() {
        force_fields(
            config.get_or_insert_with(|| Value::Object(Default::default())),
            &fields,
        );
    }
}

fn template_kind<'a>(
    templates: &'a std::collections::HashMap<String, crate::config::ConnectorSpec>,
    legacy: &'a Option<crate::config::ConnectorSpec>,
    name: Option<&str>,
) -> Option<&'a str> {
    let name = name.unwrap_or("default");
    templates
        .get(name)
        .or(if name == "default" {
            legacy.as_ref()
        } else {
            None
        })
        .map(|t| t.kind.as_str())
}

/// Force `sandbox: true` on every SQL connector the config can build, so a
/// query loaded by `faucet serve` cannot read or attach host files (#862 H-04).
/// Templates, row overrides, topology nodes, fan-out sources, the mirror
/// snapshot, partition bound probes, `verify.destination` and DLQ sinks are
/// all covered; a submitted `sandbox: false` is overwritten.
pub fn confine_sql(cfg: &mut PipelineConfig) {
    let p = &mut cfg.pipeline;
    for spec in p.source.iter_mut().chain(p.sources.values_mut()) {
        force_source(&mut spec.config, &spec.kind);
    }
    for spec in p.sink.iter_mut().chain(p.sinks.values_mut()) {
        force_sink(&mut spec.config, &spec.kind);
    }
    if let Some(d) = p.dlq.as_mut() {
        force_sink(&mut d.sink.config, &d.sink.kind);
    }
    let (sources, source, sinks, sink) = (&p.sources, &p.source, &p.sinks, &p.sink);
    for node in p.nodes.values_mut() {
        match node {
            crate::config::NodeSpec::Source {
                template,
                kind,
                config,
            } => {
                if let Some(k) = kind
                    .as_deref()
                    .or_else(|| template_kind(sources, source, template.as_deref()))
                {
                    force_override(config, true, k);
                }
            }
            crate::config::NodeSpec::Sink {
                template,
                kind,
                config,
            } => {
                if let Some(k) = kind
                    .as_deref()
                    .or_else(|| template_kind(sinks, sink, template.as_deref()))
                {
                    force_override(config, false, k);
                }
            }
            _ => {}
        }
    }
    for row in &mut cfg.matrix {
        let rows = [
            (row.source.as_mut(), sources, source, true),
            (
                row.discover.as_mut().map(|d| &mut d.source),
                sources,
                source,
                true,
            ),
            (row.sink.as_mut(), sinks, sink, false),
        ];
        for (over, templates, legacy, is_source) in rows {
            let Some(over) = over else { continue };
            if let Some(k) = over
                .kind
                .as_deref()
                .or_else(|| template_kind(templates, legacy, over.r#ref.as_deref()))
            {
                force_override(&mut over.config, is_source, k);
            }
        }
        if let Some(Some(d)) = row.dlq.as_mut() {
            force_sink(&mut d.sink.config, &d.sink.kind);
        }
    }
    if let Some(r) = cfg.replication.as_mut() {
        force_source(&mut r.snapshot.source.config, &r.snapshot.source.kind);
    }
    let probes = cfg
        .partition
        .iter_mut()
        .chain(cfg.matrix.iter_mut().filter_map(|r| r.partition.as_mut()))
        .filter_map(crate::partition::PartitionSpec::probe_mut);
    for probe in probes {
        force_source(&mut probe.from_source.config, &probe.from_source.kind);
    }
    if let Some(dest) = cfg.verify.as_mut().and_then(|v| v.destination.as_mut()) {
        force_source(&mut dest.config, &dest.kind);
    }
    if let Some(r) = cfg.reconcile.as_mut() {
        force_source(&mut r.count.config, &r.count.kind);
    }
    let p = &mut cfg.pipeline;
    bound_sql_transforms(&mut p.transforms);
    for spec in p.source.iter_mut().chain(p.sources.values_mut()) {
        bound_sql_transforms(spec.transforms.iter_mut().flatten());
    }
    for node in p.nodes.values_mut() {
        if let crate::config::NodeSpec::Transform { transforms } = node {
            bound_sql_transforms(transforms);
        }
    }
    for row in &mut cfg.matrix {
        bound_sql_transforms(row.transforms.iter_mut().flatten());
    }
}

/// The DuckDB `memory_limit` a `sql` transform in a request's config gets when
/// it sets none.
pub const SQL_TRANSFORM_MEMORY_LIMIT: &str = "1GB";

/// The most DuckDB threads a `sql` transform in a request's config gets when
/// it sets none.
pub const SQL_TRANSFORM_MAX_THREADS: usize = 4;

/// Give each `sql` transform that leaves them unset a bounded `memory_limit`
/// and `threads` (DuckDB's own defaults take most of the machine).
fn bound_sql_transforms<'a>(transforms: impl IntoIterator<Item = &'a mut TransformSpec>) {
    for t in transforms.into_iter().filter(|t| t.kind == "sql") {
        let Some(map) = t.config.as_object_mut() else {
            continue;
        };
        if map.get("memory_limit").is_none_or(Value::is_null) {
            map.insert("memory_limit".into(), SQL_TRANSFORM_MEMORY_LIMIT.into());
        }
        if map.get("threads").is_none_or(Value::is_null) {
            let threads = std::thread::available_parallelism()
                .map_or(1, usize::from)
                .min(SQL_TRANSFORM_MAX_THREADS);
            map.insert("threads".into(), threads.into());
        }
    }
}

/// [`confine_sql`] over expanded rows: the merged configs each row builds.
pub fn confine_sql_nodes(nodes: &mut [ExpandedNode]) {
    for n in nodes {
        bound_sql_transforms(&mut n.transforms);
        force_source(&mut n.source.config, &n.source.kind);
        force_sink(&mut n.sink.config, &n.sink.kind);
        if let Some(d) = n.dlq.as_mut() {
            force_sink(&mut d.sink.config, &d.sink.kind);
        }
    }
}

/// Environment variables `faucet serve` reads its own credentials from. A
/// loaded config can never resolve them through `${env:}` / `${secret:}`.
pub const SERVER_SECRET_ENV: &[&str] = &[
    "FAUCET_VAULT_KEY",
    "FAUCET_VAULT_PREVIOUS_KEYS",
    "FAUCET_SERVE_AUTH_TOKEN",
    "FAUCET_SERVE_READ_TOKEN",
    "FAUCET_SERVE_WRITE_TOKEN",
    "FAUCET_SERVE_ADMIN_TOKEN",
    "FAUCET_SERVE_HISTORY",
    "FAUCET_TEMPLATE_STORE",
    "FAUCET_SNAPSHOT_KEY",
];

/// System trees no config loaded by `faucet serve` may read or write.
const SYSTEM_DIRS: &[&str] = &["/proc", "/sys", "/dev"];

/// Connector / state / transport kinds whose config names local files.
pub const LOCAL_PATH_KINDS: &[&str] = &[
    "file", "csv", "jsonl", "parquet", "sqlite", "duckdb", "delta", "iceberg",
];

/// Paths are kept both as given (made absolute) and as they resolved when
/// protected; the given form is resolved again for checks (at most
/// [`RESOLVED_TTL`] apart), so a path whose ancestor symlink was swapped since
/// (a mounted config volume) still matches.
#[derive(Default)]
struct Protected {
    env: std::collections::BTreeSet<String>,
    files: std::collections::BTreeSet<std::path::PathBuf>,
    dirs: std::collections::BTreeSet<std::path::PathBuf>,
    /// Directories in which every environment file name is protected,
    /// whether the file exists yet or not.
    env_dirs: std::collections::BTreeSet<std::path::PathBuf>,
    own_state: std::collections::BTreeSet<std::path::PathBuf>,
    secrets: std::collections::BTreeSet<(String, String)>,
}

fn protected() -> &'static std::sync::RwLock<Protected> {
    static P: std::sync::OnceLock<std::sync::RwLock<Protected>> = std::sync::OnceLock::new();
    P.get_or_init(Default::default)
}

/// Bumped by every write to [`protected`], so a cached [`Resolved`] snapshot
/// taken before it is never reused.
static PROTECTED_GENERATION: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// A write guard over [`protected`] that invalidates the [`resolved`]
/// snapshot when dropped.
struct ProtectedWrite(std::sync::RwLockWriteGuard<'static, Protected>);

impl std::ops::Deref for ProtectedWrite {
    type Target = Protected;
    fn deref(&self) -> &Protected {
        &self.0
    }
}

impl std::ops::DerefMut for ProtectedWrite {
    fn deref_mut(&mut self) -> &mut Protected {
        &mut self.0
    }
}

impl Drop for ProtectedWrite {
    fn drop(&mut self) {
        PROTECTED_GENERATION.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

fn protected_write() -> Option<ProtectedWrite> {
    protected().write().ok().map(ProtectedWrite)
}

/// Mark an environment variable the server read a credential from (an
/// `--auth-config` token) so no loaded config can resolve it.
pub fn protect_env(name: &str) {
    if let Some(mut p) = protected_write() {
        p.env.insert(name.to_string());
    }
}

/// Mark a file the server read a credential from, as [`protect_env`].
pub fn protect_file(path: &std::path::Path) {
    let forms = path_forms(path);
    if let Some(mut p) = protected_write() {
        p.files.extend(forms);
    }
}

/// Mark a directory the server keeps its own files in (a file state store) so
/// no loaded config reads or writes anything under it.
pub fn protect_dir(path: &std::path::Path) {
    let forms = path_forms(path);
    if let Some(mut p) = protected_write() {
        p.dirs.extend(forms);
    }
}

/// `path` made absolute without resolving anything, and resolved.
fn path_forms(path: &std::path::Path) -> [std::path::PathBuf; 2] {
    let absolute = match std::path::absolute(path) {
        Ok(a) => lexical_normalize(&a),
        Err(_) => lexical_normalize(path),
    };
    [absolute, normalize_path(path)]
}

/// Protect the file state store a `--default-config` keeps its bookmarks in:
/// no loaded config reads or writes under it as data, while a run that keeps
/// that same store as its own state (the value it inherits from the default
/// config) still may.
pub fn protect_default_state(base: &Value) {
    let state = &base["pipeline"]["state"];
    if state["type"] == "file"
        && let Some(dir) = state["config"]["path"].as_str()
    {
        let forms = path_forms(std::path::Path::new(dir));
        if let Some(mut p) = protected_write() {
            p.own_state.extend(forms.iter().cloned());
            p.dirs.extend(forms);
        }
    }
}

/// Whether a `kind` state store with `config` is the server's own default
/// state store.
fn is_default_state(kind: &str, config: &Value) -> bool {
    kind == "file"
        && config["path"].as_str().is_some_and(|dir| {
            let dir = normalize_path(std::path::Path::new(dir));
            let own = protected()
                .read()
                .map(|p| p.own_state.clone())
                .unwrap_or_default();
            own.iter().any(|o| *o == dir || normalize_path(o) == dir)
        })
}

/// Protect the server's environment files: the one it loaded, and every file
/// named like one (`.env`, `.env.local`, `.envrc`, …) in its working directory or beside
/// the loaded file, including files created after the server started.
pub fn protect_env_files(cwd: &std::path::Path, loaded: Option<&std::path::Path>) {
    let mut dirs = vec![cwd.to_path_buf()];
    if let Some(p) = loaded {
        protect_file(p);
        if let Some(parent) = std::path::absolute(p)
            .ok()
            .and_then(|a| a.parent().map(Into::into))
        {
            dirs.push(parent);
        }
    }
    let forms: Vec<_> = dirs.iter().flat_map(|d| path_forms(d)).collect();
    if let Some(mut p) = protected_write() {
        p.env_dirs.extend(forms);
    }
}

/// Whether a file name is an environment file's (`.env`, `.env.local`,
/// `.envrc`, …), compared without case so a case-insensitive filesystem's
/// spellings match too.
fn is_env_file_name(name: &std::ffi::OsStr) -> bool {
    name.to_string_lossy()
        .to_ascii_lowercase()
        .starts_with(".env")
}

/// Environment file names a glob is tried against: the ones tools write.
const ENV_FILE_NAMES: &[&str] = &[
    ".env",
    ".envrc",
    ".env.local",
    ".env.development",
    ".env.development.local",
    ".env.production",
    ".env.production.local",
    ".env.test",
    ".env.test.local",
    ".env.staging",
    ".env.staging.local",
];

/// Mark a secret-manager reference (`${vault:…}`, …) the server resolved for
/// its own use, so no loaded config can resolve it (or another `#field` of the
/// same secret).
pub fn protect_secret_ref(scheme: &str, reference: &str) {
    if let Some(mut p) = protected_write() {
        p.secrets
            .insert((scheme.to_string(), secret_base(reference).to_string()));
    }
}

/// Protect every environment variable, file and secret-manager reference a
/// server input file (`--auth-config`, `--connect-providers`, …) reads.
pub fn protect_references(doc: &Value) {
    use crate::interpolate::{Directive, iter_directives};
    for_each_string(doc, &mut |s| {
        for (_, dir) in iter_directives(s) {
            if let Directive::LoadTime { prefix, body } = dir {
                match prefix {
                    "env" | "secret" => protect_env(body),
                    "file" => protect_file(std::path::Path::new(body)),
                    p if crate::secrets::SECRET_SCHEMES.contains(&p) => protect_secret_ref(p, body),
                    _ => {}
                }
            }
        }
    });
}

/// Protect a server input file and everything it reads.
pub fn protect_input(path: &std::path::Path, doc: &Value) {
    protect_file(path);
    protect_references(doc);
}

/// Protect a SQLite database the server keeps (run history), with its
/// journal files.
pub fn protect_sqlite_url(url: &str) {
    if let Some(path) = sqlite_path(url).as_deref() {
        for suffix in ["", "-wal", "-shm", "-journal"] {
            protect_file(std::path::Path::new(&format!("{path}{suffix}")));
        }
    }
}

fn sqlite_path(url: &str) -> Option<std::borrow::Cow<'_, str>> {
    if !url.starts_with("sqlite:") {
        return None;
    }
    local_path(url)
}

fn secret_base(reference: &str) -> &str {
    reference.split('#').next().unwrap_or(reference)
}

fn secret_is_protected(scheme: &str, reference: &str) -> bool {
    protected()
        .read()
        .map(|p| {
            p.secrets
                .contains(&(scheme.to_string(), secret_base(reference).to_string()))
        })
        .unwrap_or(true)
}

/// The check a secret-manager reference passes right before it is fetched:
/// never one of the server's own, and allowed by `policy` unless the
/// operator's `--default-config` base wrote it as is.
fn secret_screen(
    policy: &ReferencePolicy,
    operator_tokens: &std::collections::BTreeSet<String>,
) -> impl Fn(&str, &str) -> Result<(), String> + Send + Sync + use<> {
    let policy = policy.clone();
    let operator_tokens = operator_tokens.clone();
    move |scheme: &str, reference: &str| {
        if secret_is_protected(scheme, reference) {
            return Err("it is one of the server's own credentials".into());
        }
        if operator_tokens.contains(&format!("${{{scheme}:{reference}}}")) {
            return Ok(());
        }
        policy.check_secret(scheme, reference).map_err(|e| e.reason)
    }
}

fn normalize_path(path: &std::path::Path) -> std::path::PathBuf {
    /// Dangling symlinks followed before the rest of a path is taken as is.
    const MAX_LINK_HOPS: usize = 40;
    let mut path = if path.is_relative() {
        std::env::current_dir()
            .map(|d| d.join(path))
            .unwrap_or_else(|_| path.to_path_buf())
    } else {
        path.to_path_buf()
    };
    'hops: for _ in 0..MAX_LINK_HOPS {
        let mut existing = path.as_path();
        let mut rest = Vec::new();
        loop {
            if let Ok(real) = std::fs::canonicalize(existing) {
                return lexical_normalize(&rest.iter().rev().fold(real, |p, c| p.join(c)));
            }
            // A link whose target does not exist yet: judge the path by
            // where it points.
            if let Ok(target) = std::fs::read_link(existing) {
                let base = existing.parent().unwrap_or(std::path::Path::new("/"));
                path = rest.iter().rev().fold(base.join(target), |p, c| p.join(c));
                continue 'hops;
            }
            match (existing.parent(), existing.components().next_back()) {
                (Some(parent), Some(last)) => {
                    rest.push(last.as_os_str().to_os_string());
                    existing = parent;
                }
                _ => return lexical_normalize(&path),
            }
        }
    }
    lexical_normalize(&path)
}

/// Resolve `.` and `..` without touching the filesystem, for a path that does
/// not exist (so `/tmp/../proc/x` is still recognised as `/proc/x`).
fn lexical_normalize(path: &std::path::Path) -> std::path::PathBuf {
    use std::path::Component;
    let mut out = std::path::PathBuf::new();
    for c in path.components() {
        match c {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other),
        }
    }
    out
}

fn env_is_protected(name: &str) -> bool {
    SERVER_SECRET_ENV.contains(&name)
        || protected()
            .read()
            .map(|p| p.env.contains(name))
            .unwrap_or(true)
}

/// Whether `p` lies in a system tree, read either the way the OS resolves it
/// or lexically (the two differ when a `..` follows a symlink).
fn in_system_dir(p: &std::path::Path) -> bool {
    let lexical = lexical_normalize(p);
    [p, lexical.as_path()].iter().any(|p| {
        *p != std::path::Path::new("/dev/null") && SYSTEM_DIRS.iter().any(|d| p.starts_with(d))
    })
}

fn file_is_protected(path: &str) -> bool {
    let p = std::path::Path::new(path);
    if in_system_dir(p) {
        return true;
    }
    let normalized = normalize_path(p);
    if in_system_dir(&normalized) {
        return true;
    }
    resolved().is_none_or(|r| r.holds(&normalized))
}

/// Why a concrete local path a connector is about to list, open or replace is
/// out of reach; `None` when it is not. What `faucet serve` installs as the
/// [`faucet_core::path_guard`].
pub fn server_file_refusal(path: &std::path::Path) -> Option<String> {
    let normalized = normalize_path(path);
    let refused = in_system_dir(path)
        || in_system_dir(&normalized)
        || resolved().is_none_or(|r| r.holds(&normalized));
    refused.then(|| "is one of the server's own files".to_string())
}

/// How a connector touches a local path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Access {
    /// Reads it: a file, a directory's files (all of them when `recursive`),
    /// or a glob's matches.
    Read { recursive: bool },
    /// Writes the file it names (a `{…}` segment is a template).
    Write,
}

/// Upper bound on the existing glob matches checked for a symlink into a
/// protected location when a config is loaded.
const GLOB_CHECK_LIMIT: usize = 10_000;

/// Whether reading or writing `path` — a file, a directory, or a glob (a
/// `${…}` token counts as `*`) — would reach a system tree or a protected
/// file.
fn data_path_is_protected(path: &str, access: Access) -> bool {
    let pattern = match access {
        Access::Read { .. } => replace_tokens(path),
        Access::Write => replace_templates(&replace_tokens(path)),
    };
    if !pattern.contains(['*', '?', '[']) {
        let raw = std::path::Path::new(path);
        let normalized = normalize_path(raw);
        if in_system_dir(raw) || in_system_dir(&normalized) {
            return true;
        }
        let Some(r) = resolved() else {
            return true;
        };
        if r.holds(&normalized) {
            return true;
        }
        return match access {
            Access::Write => false,
            Access::Read { recursive } => {
                r.files.iter().any(|f| {
                    f.parent() == Some(normalized.as_path())
                        || (recursive && f.starts_with(&normalized))
                }) || (recursive && r.dirs.iter().any(|d| d.starts_with(&normalized)))
                    || r.is_env_dir(&normalized)
                    || (recursive && r.env_dirs.iter().any(|d| d.starts_with(&normalized)))
            }
        };
    }
    glob_is_protected(&pattern, access)
}

fn glob_is_protected(pattern: &str, access: Access) -> bool {
    let glob_at = pattern.find(['*', '?', '[']).unwrap_or(pattern.len());
    let (dir, rest) = match pattern[..glob_at].rfind('/') {
        Some(slash) => (&pattern[..slash.max(1)], &pattern[slash + 1..]),
        None => (".", pattern),
    };
    if rest.split('/').any(|c| c == "..") {
        return true;
    }
    let dir = normalize_path(std::path::Path::new(dir));
    if in_system_dir(&dir) {
        return true;
    }
    let full = format!("{}/{rest}", dir.display().to_string().trim_end_matches('/'));
    if pattern_can_reach_system_dir(&full)
        || (pattern.starts_with('/') && pattern_can_reach_system_dir(pattern))
    {
        return true;
    }
    let Some(r) = resolved() else {
        return true;
    };
    let hit = match glob::Pattern::new(&full) {
        Ok(p) => r.files.iter().any(|f| p.matches_path(f)),
        Err(_) => r.files.iter().any(|f| f.starts_with(&dir)),
    } || r
        .dirs
        .iter()
        .any(|d| dir.starts_with(d) || d.starts_with(&dir))
        || glob_reaches_env_file(&r, &full);
    hit || (matches!(access, Access::Read { .. }) && glob_match_escapes(&r, &full))
}

/// Whether an absolute glob pattern could match a path in a system tree,
/// compared segment by segment (`**` spans any number of segments).
fn pattern_can_reach_system_dir(pattern: &str) -> bool {
    let segments: Vec<&str> = pattern.split('/').filter(|s| !s.is_empty()).collect();
    SYSTEM_DIRS.iter().any(|d| {
        let target: Vec<&str> = d.split('/').filter(|s| !s.is_empty()).collect();
        segments_can_reach(&segments, &target)
    })
}

fn segments_can_reach(pattern: &[&str], target: &[&str]) -> bool {
    let Some((first, tail)) = target.split_first() else {
        return true;
    };
    let Some((p, rest)) = pattern.split_first() else {
        return false;
    };
    if *p == "**" {
        return segments_can_reach(rest, target) || segments_can_reach(pattern, tail);
    }
    let matches = glob::Pattern::new(p).map_or(true, |g| g.matches(first));
    matches && segments_can_reach(rest, tail)
}

/// Whether an existing match of `pattern` resolves (through a symlink) into a
/// system tree or onto a protected file.
fn glob_match_escapes(r: &Resolved, pattern: &str) -> bool {
    let Ok(matches) = glob::glob(pattern) else {
        return false;
    };
    matches
        .take(GLOB_CHECK_LIMIT)
        .filter_map(Result::ok)
        .filter_map(|p| std::fs::canonicalize(p).ok())
        .any(|real| in_system_dir(&real) || r.holds(&real))
}

/// Whether a glob could reach an environment file (or a directory named like
/// one) directly in a protected directory (see [`segment_can_be_env_name`]). Segments are matched the way a listing
/// walks them (`*` stays within a segment, `**` spans any number). A glob that
/// matches only by a leading wildcard (`*.csv` against a later `.env.csv`) is
/// left to [`faucet_core::path_guard`], which checks each file listed.
fn glob_reaches_env_file(r: &Resolved, full: &str) -> bool {
    let pattern: Vec<&str> = full.split('/').filter(|s| !s.is_empty()).collect();
    r.env_dirs.iter().any(|d| {
        let dir: Vec<String> = d
            .components()
            .filter_map(|c| match c {
                std::path::Component::Normal(n) => Some(n.to_string_lossy().into_owned()),
                _ => None,
            })
            .collect();
        let dir: Vec<&str> = dir.iter().map(String::as_str).collect();
        segments_reach_env_name(&pattern, &dir)
    })
}

fn segments_reach_env_name(pattern: &[&str], dir: &[&str]) -> bool {
    let Some((p, rest)) = pattern.split_first() else {
        return false;
    };
    if *p == "**" {
        return segments_reach_env_name(rest, dir)
            || (!dir.is_empty() && segments_reach_env_name(pattern, &dir[1..]));
    }
    match dir.split_first() {
        None => segment_can_be_env_name(p, rest.is_empty()),
        Some((d, dir_rest)) => {
            glob::Pattern::new(p).map_or(true, |g| g.matches_with(d, glob_options()))
                && segments_reach_env_name(rest, dir_rest)
        }
    }
}

/// How a local file listing compares names: `*` never crosses `/`, a leading
/// `.` needs no literal, and case is ignored (a case-insensitive filesystem
/// opens either spelling).
fn glob_options() -> glob::MatchOptions {
    glob::MatchOptions {
        case_sensitive: false,
        require_literal_separator: true,
        require_literal_leading_dot: false,
    }
}

/// Whether a glob segment can name an environment file: one of the names
/// tools write (for the last segment — a directory of that name is unusual,
/// and a `${now.date}/` segment must stay usable), or any name starting
/// `.env` when its first element can match a `.`.
fn segment_can_be_env_name(segment: &str, last: bool) -> bool {
    let conventional = last
        && glob::Pattern::new(segment).map_or(true, |g| {
            ENV_FILE_NAMES
                .iter()
                .any(|n| g.matches_with(n, glob_options()))
        });
    let dot_first = glob_tokens(segment)
        .first()
        .is_some_and(|t| !matches!(t, GlobToken::Star) && t.matches('.'));
    conventional || (dot_first && segment_can_start_with(segment, ".env"))
}

/// Whether a glob segment (`*`, `?`, `[…]`, literals; no `/`) matches some
/// name that starts with `prefix`, ignoring ASCII case.
fn segment_can_start_with(segment: &str, prefix: &str) -> bool {
    let tokens = glob_tokens(segment);
    let mut states = std::collections::BTreeSet::from([0usize]);
    let close = |states: &mut std::collections::BTreeSet<usize>| {
        let mut queue: Vec<usize> = states.iter().copied().collect();
        while let Some(i) = queue.pop() {
            if matches!(tokens.get(i), Some(GlobToken::Star)) && states.insert(i + 1) {
                queue.push(i + 1);
            }
        }
    };
    close(&mut states);
    for c in prefix.chars() {
        let mut next = std::collections::BTreeSet::new();
        for &i in &states {
            match tokens.get(i) {
                Some(GlobToken::Star) => {
                    next.insert(i);
                }
                Some(t) if t.matches(c) => {
                    next.insert(i + 1);
                }
                _ => {}
            }
        }
        close(&mut next);
        if next.is_empty() {
            return false;
        }
        states = next;
    }
    true
}

enum GlobToken {
    Star,
    Any,
    Literal(char),
    Class {
        negated: bool,
        items: Vec<(char, char)>,
    },
}

impl GlobToken {
    fn matches(&self, c: char) -> bool {
        let lower = c.to_ascii_lowercase();
        let upper = c.to_ascii_uppercase();
        match self {
            GlobToken::Star | GlobToken::Any => true,
            GlobToken::Literal(l) => l.eq_ignore_ascii_case(&c),
            GlobToken::Class { negated, items } => {
                let has = |c: char| items.iter().any(|(lo, hi)| (*lo..=*hi).contains(&c));
                if *negated {
                    !has(lower) || !has(upper)
                } else {
                    has(lower) || has(upper)
                }
            }
        }
    }
}

/// The tokens of one glob segment, read the way the `glob` crate reads them
/// (`[!…]` negates, `]` first in a class is literal); an unterminated `[` is a
/// literal.
fn glob_tokens(segment: &str) -> Vec<GlobToken> {
    let chars: Vec<char> = segment.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        match chars[i] {
            '*' => {
                if !matches!(out.last(), Some(GlobToken::Star)) {
                    out.push(GlobToken::Star);
                }
                i += 1;
            }
            '?' => {
                out.push(GlobToken::Any);
                i += 1;
            }
            '[' => {
                let mut j = i + 1;
                let negated = chars.get(j) == Some(&'!');
                if negated {
                    j += 1;
                }
                let start = j;
                let mut items = Vec::new();
                while j < chars.len() && (chars[j] != ']' || j == start) {
                    if j + 2 < chars.len() && chars[j + 1] == '-' && chars[j + 2] != ']' {
                        items.push((chars[j], chars[j + 2]));
                        j += 3;
                    } else {
                        items.push((chars[j], chars[j]));
                        j += 1;
                    }
                }
                if j < chars.len() {
                    out.push(GlobToken::Class { negated, items });
                    i = j + 1;
                } else {
                    out.push(GlobToken::Literal('['));
                    i += 1;
                }
            }
            c => {
                out.push(GlobToken::Literal(c));
                i += 1;
            }
        }
    }
    out
}

/// Replace `{…}` path-template segments (a file sink's `{part}`) with `*`.
fn replace_templates(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    let mut rest = path;
    while let Some(start) = rest.find('{') {
        out.push_str(&rest[..start]);
        out.push('*');
        rest = match rest[start..].find('}') {
            Some(end) => &rest[start + end + 1..],
            None => "",
        };
    }
    out.push_str(rest);
    out
}

fn replace_tokens(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    let mut rest = path;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        match rest[start..].find('}') {
            Some(end) => {
                out.push('*');
                rest = &rest[start + end + 1..];
            }
            None => {
                out.push('*');
                rest = "";
            }
        }
    }
    out.push_str(rest);
    out
}

/// The protected paths, each as stored and as it resolves now.
struct Resolved {
    files: Vec<std::path::PathBuf>,
    dirs: Vec<std::path::PathBuf>,
    env_dirs: Vec<std::path::PathBuf>,
    /// The env dirs' filesystem identities, so another spelling of the same
    /// directory (a bind mount) matches too.
    env_ids: Vec<(u64, u64)>,
}

impl Resolved {
    /// Whether `path` (resolved) is a protected file, lies under a protected
    /// directory, or passes through an environment-file name directly inside
    /// a protected environment directory.
    fn holds(&self, path: &std::path::Path) -> bool {
        self.files.iter().any(|f| f == path)
            || self.dirs.iter().any(|d| path.starts_with(d))
            || self.under_env_name(path)
    }

    fn under_env_name(&self, path: &std::path::Path) -> bool {
        let mut prefix = std::path::PathBuf::new();
        for c in path.components() {
            if let std::path::Component::Normal(name) = c
                && is_env_file_name(name)
                && self.is_env_dir(&prefix)
            {
                return true;
            }
            prefix.push(c);
        }
        false
    }

    fn is_env_dir(&self, dir: &std::path::Path) -> bool {
        self.env_dirs.iter().any(|d| d == dir)
            || dir_id(dir).is_some_and(|id| self.env_ids.contains(&id))
    }
}

#[cfg(unix)]
fn dir_id(path: &std::path::Path) -> Option<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(path)
        .ok()
        .filter(std::fs::Metadata::is_dir)
        .map(|m| (m.dev(), m.ino()))
}

#[cfg(not(unix))]
fn dir_id(_path: &std::path::Path) -> Option<(u64, u64)> {
    None
}

/// How long a [`Resolved`] snapshot is reused before the protected paths are
/// resolved again (a symlink swapped since then is seen after at most this).
const RESOLVED_TTL: std::time::Duration = std::time::Duration::from_secs(1);

/// [`Resolved`] now; `None` when the set can't be read (callers treat that as
/// protected). A snapshot is reused for [`RESOLVED_TTL`] unless a path was
/// protected since.
fn resolved() -> Option<std::sync::Arc<Resolved>> {
    type Cached = Option<(std::time::Instant, u64, std::sync::Arc<Resolved>)>;
    static CACHE: std::sync::Mutex<Cached> = std::sync::Mutex::new(None);
    let generation = PROTECTED_GENERATION.load(std::sync::atomic::Ordering::SeqCst);
    if let Ok(cache) = CACHE.lock()
        && let Some((at, g, r)) = cache.as_ref()
        && *g == generation
        && at.elapsed() < RESOLVED_TTL
    {
        return Some(r.clone());
    }
    let fresh = std::sync::Arc::new(resolve_protected()?);
    if let Ok(mut cache) = CACHE.lock() {
        *cache = Some((std::time::Instant::now(), generation, fresh.clone()));
    }
    Some(fresh)
}

/// The protected sets resolved now. They are copied out so no lock is held
/// across filesystem calls.
fn resolve_protected() -> Option<Resolved> {
    let (files, dirs, env_dirs) = protected()
        .read()
        .map(|g| (g.files.clone(), g.dirs.clone(), g.env_dirs.clone()))
        .ok()?;
    let expand = |set: std::collections::BTreeSet<std::path::PathBuf>| {
        let mut out: Vec<_> = set.iter().map(|p| normalize_path(p)).collect();
        out.extend(set);
        out.sort();
        out.dedup();
        out
    };
    let env_dirs = expand(env_dirs);
    let env_ids = env_dirs.iter().filter_map(|d| dir_id(d)).collect();
    Some(Resolved {
        files: expand(files),
        dirs: expand(dirs),
        env_dirs,
        env_ids,
    })
}

fn for_each_string(value: &Value, f: &mut impl FnMut(&str)) {
    match value {
        Value::String(s) => f(s),
        Value::Array(items) => items.iter().for_each(|v| for_each_string(v, f)),
        Value::Object(map) => {
            for (k, v) in map {
                f(k);
                for_each_string(v, f);
            }
        }
        _ => {}
    }
}

/// The `${env:}` / `${secret:}` / `${file:}` references in `doc` that point
/// at the server's own credentials.
pub fn server_secret_refs(doc: &Value) -> Vec<String> {
    use crate::interpolate::{Directive, iter_directives};
    let mut found = Vec::new();
    for_each_string(doc, &mut |s| {
        for (token, dir) in iter_directives(s) {
            if let Directive::LoadTime { prefix, body } = dir {
                let hit = match prefix {
                    "env" | "secret" => env_is_protected(body),
                    "file" => file_is_protected(body),
                    p if crate::secrets::SECRET_SCHEMES.contains(&p) => {
                        secret_is_protected(p, body)
                    }
                    _ => false,
                };
                if hit {
                    found.push(token.to_string());
                }
            }
        }
    });
    found
}

fn is_path_key(key: &str) -> bool {
    matches!(
        key,
        "path"
            | "paths"
            | "dir"
            | "directory"
            | "file"
            | "files"
            | "database"
            | "database_url"
            | "table_uri"
            | "warehouse"
            | "location"
    ) || key.ends_with("_path")
        || key.ends_with("_dir")
        || key.ends_with("_file")
}

/// The local filesystem path a config value names, if it names one: read as
/// the connector reads it ([`faucet_core::resolve::local_path`]).
fn local_path(value: &str) -> Option<std::borrow::Cow<'_, str>> {
    faucet_core::resolve::local_path(value).filter(|p| !p.is_empty() && p != ":memory:")
}

/// Runtime tokens a local path may carry in a caller-supplied config: their
/// values come from the clock or the config itself, never from records.
const STATIC_PATH_TOKENS: &[&str] = &["now", "backfill", "partition"];

/// Why a screening pass refused a value.
#[derive(Debug, Clone)]
enum Why {
    /// A local path that takes a value from records.
    RecordPath,
    /// A system tree or a file holding the server's own credentials.
    Protected,
    /// A URL host that takes a value from records.
    RecordHost,
    /// A URL host taken from a token.
    TokenHost,
    /// A connector type that runs a program.
    Program,
    /// A local file an auth provider a caller configures names.
    ProviderFile,
    /// The reference policy's reason.
    Policy(String),
}

/// One refused value. `text` is how a refusal of a whole submitted config
/// lists it; `caller` is set when only a caller's template params were
/// screened (#989).
#[derive(Debug, Clone)]
struct Finding {
    why: Why,
    text: String,
    caller: Option<CallerFinding>,
}

/// What a refusal of a caller's template param may say about a finding:
/// which names wrote the value, where, and only the caller's own text.
#[derive(Debug, Clone)]
struct CallerFinding {
    names: std::collections::BTreeSet<String>,
    field: String,
    shown: Option<String>,
    reason: Option<String>,
}

/// What a screening pass refused: paths that reach the server's credentials
/// or a system tree, and resources the reference policy does not allow.
#[derive(Default)]
struct Findings {
    protected: Vec<String>,
    policy: Vec<String>,
    items: Vec<Finding>,
}

impl Findings {
    /// Record a refusal of `value` at `ptr` (relative to `base`).
    fn add(
        &mut self,
        scope: &Scope<'_>,
        base: &str,
        ptr: &str,
        value: &str,
        why: Why,
        text: String,
    ) {
        let value = faucet_core::util::redact_uri_credentials(value);
        let value = value.as_str();
        let text = faucet_core::util::redact_uri_credentials(&text);
        match why {
            Why::RecordPath | Why::Protected => self.protected.push(text.clone()),
            Why::RecordHost | Why::TokenHost | Why::Policy(_) => self.policy.push(text.clone()),
            Why::Program | Why::ProviderFile => {}
        }
        let caller = scope.only.map(|f| CallerFinding {
            names: f.params_at(ptr),
            field: format!("{base}{ptr}"),
            shown: f.shown(ptr, value),
            reason: match &why {
                Why::Policy(r) => Some(f.shown_reason(ptr, r)),
                _ => None,
            },
        });
        self.items.push(Finding { why, text, caller });
    }
}

/// What one screening pass checks: a whole caller config (`caller`), a
/// trusted one, or — with `only` — just the values a caller's template
/// params wrote (#989), as pointers relative to the value being checked.
#[derive(Clone, Copy)]
struct Scope<'a> {
    caller: bool,
    policy: Option<&'a ReferencePolicy>,
    only: Option<&'a crate::params::CallerFields>,
}

impl<'a> Scope<'a> {
    /// Whether the value at `ptr` is screened.
    fn wants(&self, ptr: &str) -> bool {
        self.only.is_none_or(|f| f.covers(ptr))
    }

    /// This scope over a value whose own caller fields are `only`.
    fn within<'b>(&self, only: Option<&'b crate::params::CallerFields>) -> Scope<'b>
    where
        'a: 'b,
    {
        Scope {
            caller: self.caller,
            policy: self.policy,
            only,
        }
    }
}

/// The dotted name of the field at `ptr` below `base`, as a refusal names it
/// in place of a value that may carry credentials.
fn field_name(base: &str, ptr: &str) -> String {
    crate::params::caller::display(&format!("{base}{ptr}"))
}

/// Whether a local path a caller supplied takes a value from records.
fn takes_record_value(path: &str) -> bool {
    use crate::interpolate::{Directive, iter_directives};
    iter_directives(path).any(|(_, d)| match d {
        Directive::Deferred { id, .. } => !STATIC_PATH_TOKENS.contains(&id),
        Directive::LoadTime { .. } => false,
    })
}

const RECORD_PATH: &str = "(a local path in a submitted config may not take a value from records)";

/// The access a read of `config` implies (a `recursive: true` directory
/// read reaches every file below it).
fn read_access(config: &Value, access: Access) -> Access {
    match access {
        Access::Read { .. } => Access::Read {
            recursive: config.get("recursive").and_then(Value::as_bool) == Some(true),
        },
        Access::Write => Access::Write,
    }
}

/// Check one local path `s` at `ptr` (relative to `base`).
fn check_path_value(
    s: &str,
    base: &str,
    ptr: &str,
    access: Access,
    scope: &Scope<'_>,
    found: &mut Findings,
) {
    let Some(path) = local_path(s) else {
        return;
    };
    let path = path.as_ref();
    if scope.caller && takes_record_value(path) {
        found.add(
            scope,
            base,
            ptr,
            s,
            Why::RecordPath,
            format!("{s} {RECORD_PATH}"),
        );
    } else if data_path_is_protected(path, access) {
        found.add(scope, base, ptr, s, Why::Protected, s.to_string());
    } else if let Some(Err(e)) = scope.policy.map(|p| p.check_path(path)) {
        let text = format!("{s} ({})", e.reason);
        found.add(scope, base, ptr, s, Why::Policy(e.reason), text);
    }
}

fn check_local_paths(
    config: &Value,
    base: &str,
    access: Access,
    scope: &Scope<'_>,
    found: &mut Findings,
) {
    fn walk(
        v: &Value,
        ptr: &str,
        under_path_key: bool,
        cx: (&str, Access, &Scope<'_>),
        found: &mut Findings,
    ) {
        let (base, access, scope) = cx;
        match v {
            Value::String(s) if under_path_key => {
                if scope.wants(ptr) {
                    check_path_value(s, base, ptr, access, scope, found);
                }
            }
            Value::Array(items) => items
                .iter()
                .enumerate()
                .for_each(|(i, x)| walk(x, &format!("{ptr}/{i}"), under_path_key, cx, found)),
            Value::Object(map) => {
                for (k, child) in map {
                    let at = crate::params::caller::child(ptr, k);
                    walk(child, &at, is_path_key(k), cx, found);
                }
            }
            _ => {}
        }
    }
    walk(
        config,
        "",
        false,
        (base, read_access(config, access), scope),
        found,
    );
}

/// Check every resource a `kind` connector's `config` (at `base`) names
/// through its schema's `x-faucet-resource` annotations (#881): a local path
/// against the server's credential files and system trees, and every
/// resource against the reference policy.
fn check_connector(
    kind: &str,
    config: &Value,
    base: &str,
    access: Access,
    scope: &Scope<'_>,
    found: &mut Findings,
) {
    if scope.only.is_some_and(|f| f.is_empty()) {
        return;
    }
    if LOCAL_PATH_KINDS.contains(&kind) {
        check_local_paths(config, base, access, scope, found);
    }
    let access = read_access(config, access);
    let azure_hosts = azure_blob_hosts(kind, config);
    for r in crate::serve::references::connector_resources(kind, config) {
        if !scope.wants(&r.pointer) {
            continue;
        }
        let ptr = r.pointer.as_str();
        if r.kind == ResourceKind::Path
            && let Some(path) = local_path(&r.value)
        {
            let path = path.as_ref();
            if scope.caller && takes_record_value(path) {
                let text = format!("{} {RECORD_PATH}", r.value);
                found.add(scope, base, ptr, &r.value, Why::RecordPath, text);
                continue;
            }
            if data_path_is_protected(path, access) {
                found.add(scope, base, ptr, &r.value, Why::Protected, r.value.clone());
                continue;
            }
        }
        if scope.caller
            && (r.kind == ResourceKind::Url
                || (r.kind == ResourceKind::Path && local_path(&r.value).is_none()))
            && faucet_core::resolve::url_hosts_as(&r.value, r.syntax)
                .iter()
                .any(|h| takes_record_value(h))
        {
            let text = format!(
                "{kind}{}: `{}` (a URL host in a submitted config may not take a value from \
                 records — it cannot be checked)",
                r.pointer,
                field_name(base, ptr)
            );
            found.add(scope, base, ptr, &r.value, Why::RecordHost, text);
            continue;
        }
        let checked = match (scope.policy, azure_hosts.as_deref()) {
            (Some(_), Some(_))
                if ptr == AZURE_BLOB_URLS[0]
                    && config.pointer(AZURE_BLOB_URLS[1]).is_some()
                    && scope.wants(AZURE_BLOB_URLS[1]) =>
            {
                None
            }
            (Some(p), Some(hosts)) if AZURE_BLOB_URLS.contains(&ptr) => {
                let string = config
                    .pointer(AZURE_BLOB_URLS[1])
                    .and_then(Value::as_str)
                    .unwrap_or("");
                Some(p.check_azure_conn_with_hosts(string, hosts))
            }
            (policy, _) => policy.map(|p| p.check(&r)),
        };
        if let Some(Err(e)) = checked {
            let text = match r.kind {
                ResourceKind::Url => format!("`{}` ({})", field_name(base, ptr), e.reason),
                _ => format!("{} ({})", r.value, e.reason),
            };
            found.add(scope, base, ptr, &r.value, Why::Policy(e.reason), text);
        }
    }
}

/// The fields of an Azure Blob connector's config whose hosts the client
/// reads together with the rest of the connection: the custom endpoint and
/// the connection string.
const AZURE_BLOB_URLS: [&str; 2] = ["/endpoint", "/auth/config/connection_string"];

/// The hosts an `azure-blob` connector's `config` connects to, read from the
/// whole connection (account, endpoint, emulator, connection string and the
/// server's environment) the way its client builds it; `None` for any other
/// kind or a config that is not one.
fn azure_blob_hosts(kind: &str, config: &Value) -> Option<Vec<String>> {
    #[cfg(any(feature = "source-azure-blob", feature = "sink-azure-blob"))]
    if kind == "azure-blob" {
        let conn: faucet_common_azure::AzureConnection =
            serde_json::from_value(config.clone()).ok()?;
        return Some(faucet_common_azure::effective_hosts(&conn));
    }
    let _ = (kind, config);
    None
}

/// The access a document position implies: under a `sink` / `sinks` / `dlq` /
/// `state` key (or a topology node of `kind: sink`) a connector writes;
/// anywhere else it reads.
fn access_under(key: &str, inherited: Access) -> Access {
    match key {
        "sink" | "sinks" | "dlq" | "state" => Access::Write,
        "source" | "sources" | "snapshot" | "from_source" | "destination" | "relations" => {
            Access::Read { recursive: false }
        }
        _ => inherited,
    }
}

/// Local paths in `doc` — any `{type, config}` of a [`LOCAL_PATH_KINDS`] kind,
/// and the file-backed relations of a `sql` transform — that would read or
/// write a system tree or a server credential file.
pub fn server_data_paths(doc: &Value, caller: bool) -> Vec<String> {
    let scope = Scope {
        caller,
        policy: None,
        only: None,
    };
    data_path_findings(doc, &scope).protected
}

/// Every `{type, config}` in `doc` (pointers from its root) checked under
/// `scope`.
fn data_path_findings(doc: &Value, scope: &Scope<'_>) -> Findings {
    fn walk(v: &Value, ptr: &str, access: Access, scope: &Scope<'_>, found: &mut Findings) {
        match v {
            Value::Object(map) => {
                let access = match map.get("kind").and_then(Value::as_str) {
                    Some("sink") => Access::Write,
                    Some("source") => Access::Read { recursive: false },
                    _ => access,
                };
                if let Some(t) = map.get("type").and_then(Value::as_str) {
                    // A caller's template param naming a connector that runs
                    // a program (a whole caller config is screened for that
                    // before it is parsed).
                    let at = format!("{ptr}/type");
                    if scope.only.is_some_and(|f| f.covers(&at))
                        && crate::serve::references::runs_program(t)
                        && scope.policy.is_some_and(|p| !p.allows_subprocess())
                    {
                        found.add(scope, "", &at, t, Why::Program, t.to_string());
                    }
                    let (base, config) = match map.get("config") {
                        Some(cfg) => (format!("{ptr}/config"), Some(cfg.clone())),
                        None if matches!(t, "csv" | "jsonl") => (
                            ptr.to_string(),
                            map.get("path").map(|p| serde_json::json!({ "path": p })),
                        ),
                        None => (String::new(), None),
                    };
                    let own_state = ptr.ends_with("/state")
                        && config.as_ref().is_some_and(|c| is_default_state(t, c));
                    if let Some(config) = config.filter(|_| !own_state) {
                        let rel = scope.only.map(|f| f.under(&base));
                        let inner = scope.within(rel.as_ref());
                        if map.contains_key("config") {
                            check_connector(t, &config, &base, access, &inner, found);
                        } else if !inner.only.is_some_and(|f| f.is_empty()) {
                            check_local_paths(&config, &base, access, &inner, found);
                        }
                    }
                }
                for (k, c) in map {
                    let at = crate::params::caller::child(ptr, k);
                    walk(c, &at, access_under(k, access), scope, found);
                }
            }
            Value::Array(items) => items
                .iter()
                .enumerate()
                .for_each(|(i, c)| walk(c, &format!("{ptr}/{i}"), access, scope, found)),
            _ => {}
        }
    }
    let mut found = Findings::default();
    walk(
        doc,
        "",
        Access::Read { recursive: false },
        scope,
        &mut found,
    );
    for list in [&mut found.protected, &mut found.policy] {
        let mut seen = std::collections::HashSet::new();
        list.retain(|f| seen.insert(f.clone()));
    }
    found
}

/// A connector template and the pointer it is declared at.
fn template_spec<'a>(
    templates: &'a std::collections::HashMap<String, crate::config::ConnectorSpec>,
    legacy: &'a Option<crate::config::ConnectorSpec>,
    plural: &str,
    name: Option<&str>,
) -> Option<(&'a crate::config::ConnectorSpec, String)> {
    let name = name.unwrap_or("default");
    if let Some(t) = templates.get(name) {
        return Some((
            t,
            format!("/pipeline/{plural}/{}", crate::params::caller::escape(name)),
        ));
    }
    let singular = plural.trim_end_matches('s');
    legacy
        .as_ref()
        .filter(|_| name == "default")
        .map(|t| (t, format!("/pipeline/{singular}")))
}

/// A connector a config builds from a template override (a matrix-row
/// `source` / `sink` / `fan_out.source`, a topology `source` / `sink` node):
/// its kind, the template config with the override merged over it, and
/// where both are.
struct OverrideConnector {
    kind: String,
    config: Value,
    access: Access,
    /// Where the override is (its config is at `{at}/config`).
    at: String,
    /// The template's config and where it is declared.
    template: Option<(Value, String)>,
    over: Value,
}

fn resolve_override(
    templates: &std::collections::HashMap<String, crate::config::ConnectorSpec>,
    legacy: &Option<crate::config::ConnectorSpec>,
    plural: &str,
    over: (Option<&str>, Option<&str>, Option<&Value>),
    at: String,
    access: Access,
) -> Option<OverrideConnector> {
    let (name, kind, config) = over;
    let template = template_spec(templates, legacy, plural, name);
    let kind = kind
        .or(template.as_ref().map(|(t, _)| t.kind.as_str()))?
        .to_string();
    let template = template.map(|(t, tat)| (t.config.clone(), format!("{tat}/config")));
    let over = config
        .cloned()
        .unwrap_or_else(|| Value::Object(Default::default()));
    let mut merged = template
        .as_ref()
        .map(|(c, _)| c.clone())
        .unwrap_or_else(|| Value::Object(Default::default()));
    crate::merge::merge_value(&mut merged, over.clone());
    Some(OverrideConnector {
        kind,
        config: merged,
        access,
        at,
        template,
        over,
    })
}

/// The connectors a config builds from a template override, resolved
/// against their template, with the access each implies.
fn override_connectors(cfg: &PipelineConfig) -> Vec<OverrideConnector> {
    let p = &cfg.pipeline;
    let read = Access::Read { recursive: false };
    let mut out = Vec::new();
    for (id, node) in &p.nodes {
        let at = format!("/pipeline/nodes/{}", crate::params::caller::escape(id));
        let resolved = match node {
            crate::config::NodeSpec::Source {
                template,
                kind,
                config,
            } => resolve_override(
                &p.sources,
                &p.source,
                "sources",
                (template.as_deref(), kind.as_deref(), config.as_ref()),
                at,
                read,
            ),
            crate::config::NodeSpec::Sink {
                template,
                kind,
                config,
            } => resolve_override(
                &p.sinks,
                &p.sink,
                "sinks",
                (template.as_deref(), kind.as_deref(), config.as_ref()),
                at,
                Access::Write,
            ),
            _ => None,
        };
        out.extend(resolved);
    }
    let no_templates = Default::default();
    for (i, row) in cfg.matrix.iter().enumerate() {
        let discover = row.discover.as_ref().map(|d| &d.source);
        let discover_templates = match discover {
            Some(d) if d.r#ref.is_none() => &no_templates,
            _ => &p.sources,
        };
        let overrides = [
            (
                row.source.as_ref(),
                &p.sources,
                &p.source,
                "sources",
                "source",
                read,
            ),
            (
                discover,
                discover_templates,
                &None,
                "sources",
                "fan_out/source",
                read,
            ),
            (
                row.sink.as_ref(),
                &p.sinks,
                &p.sink,
                "sinks",
                "sink",
                Access::Write,
            ),
        ];
        for (over, templates, legacy, plural, part, access) in overrides {
            let Some(over) = over else { continue };
            out.extend(resolve_override(
                templates,
                legacy,
                plural,
                (
                    over.r#ref.as_deref(),
                    over.kind.as_deref(),
                    over.config.as_ref(),
                ),
                format!("/matrix/{i}/{part}"),
                access,
            ));
        }
    }
    out
}

/// Check every template-override connector of `cfg` under `scope`; with
/// `scope.only`, a value the override inherits from its template is the
/// caller's where the template's was.
fn override_findings(cfg: &PipelineConfig, scope: &Scope<'_>, found: &mut Findings) {
    for o in override_connectors(cfg) {
        let base = format!("{}/config", o.at);
        let merged = scope.only.map(|f| {
            let empty = Value::Object(Default::default());
            let (tpl_cfg, tpl_fields) = match &o.template {
                Some((c, tat)) => (c, f.under(tat)),
                None => (&empty, f.empty_like()),
            };
            merged_caller_fields(tpl_cfg, &tpl_fields, &o.over, &f.under(&base))
        });
        let inner = scope.within(merged.as_ref());
        check_connector(&o.kind, &o.config, &base, o.access, &inner, found);
    }
}

// ── Caller-supplied template params (#989) ─────────────────────────────────

/// The caller's side of a trusted body: the values a caller supplied for a
/// registered template's params (#989). The body loads trusted; these values
/// are held to the caller reference policy, like a config the caller sent.
#[derive(Debug, Clone, Copy)]
pub struct CallerParams<'a> {
    pub fields: &'a crate::params::CallerFields,
    /// The caller's origin (whether the server allows subprocess connectors).
    pub origin: BodyOrigin,
}

/// How a refusal of a caller-supplied param value starts.
pub const CALLER_PARAM_REFUSAL: &str = "a value a caller supplied for a template param";

/// How a refusal of a value an event's sender placed in a trigger's config
/// starts.
pub const TRIGGER_VALUE_REFUSAL: &str = "a value an event's sender supplied to a trigger";

/// Whether `message` is a refusal of caller-supplied values
/// ([`CALLER_PARAM_REFUSAL`] / [`TRIGGER_VALUE_REFUSAL`]), and the audit
/// target that names what was refused.
pub fn caller_refusal_target(message: &str) -> Option<&'static str> {
    if message.starts_with(TRIGGER_VALUE_REFUSAL) {
        Some("trigger values")
    } else if message.starts_with(CALLER_PARAM_REFUSAL) {
        Some("template params")
    } else {
        None
    }
}

/// `fields` extended through the config's own references: a string holding
/// `${vars.X}` / `${sources.N.PATH}` / `${sinks.N.PATH}` that names a
/// caller-supplied value carries it too. Also recorded under the canonical
/// spelling of an aliased key (`replication:` → `mirror:`, a row's
/// `discover:` → `fan_out:`), as the parsed config serialises it.
fn propagate_caller_fields(
    doc: &Value,
    fields: &crate::params::CallerFields,
) -> crate::params::CallerFields {
    let out = crate::params::caller::through_refs(doc, fields);
    let mut out = out.with_alias(0, "replication", "mirror");
    for (ptr, params) in out.clone().iter() {
        let segs: Vec<&str> = ptr.split('/').skip(1).collect();
        if segs.first() == Some(&"matrix") && segs.get(2) == Some(&"discover") {
            let mut c = segs.clone();
            c[2] = "fan_out";
            out.insert_all(&format!("/{}", c.join("/")), params);
        }
    }
    out
}

/// The caller part of a template override merged over its template: an
/// entry of the template survives unless the override replaces it (the
/// [`crate::merge::merge_value`] rule — objects merge, anything else
/// replaces).
fn merged_caller_fields(
    template: &Value,
    template_fields: &crate::params::CallerFields,
    over: &Value,
    over_fields: &crate::params::CallerFields,
) -> crate::params::CallerFields {
    let replaced = |ptr: &str| -> bool {
        let (mut t, mut o) = (Some(template), over);
        for seg in ptr.split('/').skip(1) {
            let seg = seg.replace("~1", "/").replace("~0", "~");
            let Some(next) = o.as_object().and_then(|m| m.get(&seg)) else {
                return false;
            };
            let tnext = t.and_then(|v| v.as_object()).and_then(|m| m.get(&seg));
            if !(next.is_object() && tnext.is_some_and(Value::is_object)) {
                return true;
            }
            o = next;
            t = tnext;
        }
        false
    };
    let mut out = over_fields.clone();
    for ptr in template_fields.iter().map(|(p, _)| p.clone()) {
        if !replaced(&ptr) {
            out.copy_entry(template_fields, &ptr, &ptr);
        }
    }
    out
}

/// Refuse a trusted template body (#989) in which a value a caller supplied
/// for a param (or an `env` override) reaches a path, URL or command the
/// caller reference `policy` does not allow: the screening a caller's own
/// config gets — every connector field annotated `x-faucet-resource` (and
/// every path of a local-file connector, state store, DLQ or lineage
/// transport), every auth provider, every absolute URL, and a connector type
/// that runs a program — over just the values `fields` names. `doc` is the
/// body as loaded, before it was parsed into `cfg`.
pub fn refuse_caller_params(
    doc: &Value,
    cfg: &PipelineConfig,
    fields: &crate::params::CallerFields,
    policy: &ReferencePolicy,
    for_tenant: bool,
) -> Result<(), ServeError> {
    if fields.is_empty() {
        return Ok(());
    }
    let fields = propagate_caller_fields(doc, fields);
    let view = serde_json::to_value(cfg)
        .map_err(|e| ServeError::Internal(format!("serializing config: {e}")))?;
    let scope = Scope {
        caller: true,
        policy: Some(policy),
        only: Some(&fields),
    };
    let mut found = data_path_findings(&view, &scope);
    override_findings(cfg, &scope, &mut found);
    url_findings(&view, &scope, &mut found);
    for (name, spec) in cfg.auth.iter().flatten() {
        let base = format!("/auth/{}/config", crate::params::caller::escape(name));
        let here = fields.under(&base);
        provider_findings(
            spec,
            (&format!("auth.{name}"), &base),
            &scope.within(Some(&here)),
            &mut found,
        );
    }
    caller_params_error(found, for_tenant, fields.only_trigger_values())
}

/// The refusal [`refuse_caller_params`]'s findings amount to, if any: each
/// names the params (and `env` overrides) that wrote the value, the field,
/// and only the text the caller sent.
fn caller_params_error(
    found: Findings,
    for_tenant: bool,
    triggered: bool,
) -> Result<(), ServeError> {
    let mut lines: Vec<String> = found
        .items
        .into_iter()
        .filter_map(|f| {
            let c = f.caller?;
            let reason = match f.why {
                Why::RecordPath => {
                    "a path a caller chooses may not also take a value from records".to_string()
                }
                Why::Protected => format!(
                    "it reaches a system tree ({}) or a file holding the server's own credentials",
                    SYSTEM_DIRS.join(", ")
                ),
                Why::RecordHost => {
                    "a URL host a caller chooses may not also take a value from records".into()
                }
                Why::TokenHost => {
                    "a URL host a caller chooses may not also come from a token".into()
                }
                Why::Program => "connectors that run a program are not allowed".into(),
                Why::ProviderFile => "a provider a caller configures may not name a local file \
                                      — pass a key inline as `key_json`"
                    .into(),
                Why::Policy(r) => c.reason.unwrap_or(r),
            };
            let names = c
                .names
                .iter()
                .map(|n| crate::params::caller::describe(n))
                .collect::<Vec<_>>()
                .join(", ");
            let to = c.shown.map(|v| format!(" to {v}")).unwrap_or_default();
            Some(format!(
                "{names} sets `{}`{to} ({reason})",
                crate::params::caller::display(&c.field),
            ))
        })
        .collect();
    if lines.is_empty() {
        return Ok(());
    }
    lines.sort();
    lines.dedup();
    let tenant_note = if for_tenant {
        " (a tenant is confined to --tenant-path-root/<tenant>)"
    } else {
        ""
    };
    let message = if triggered {
        format!(
            "{TRIGGER_VALUE_REFUSAL} reaches what the server's reference policy does not allow a \
             caller: {}; the event was not run. The operator can allow it with \
             --allow-path-root / --allow-url-host / --allow-url-scheme{tenant_note}",
            lines.join("; "),
        )
    } else {
        format!(
            "{CALLER_PARAM_REFUSAL} reaches what the server's reference policy does not allow a \
             caller: {}; the operator can allow it with --allow-path-root / --allow-url-host / \
             --allow-url-scheme{tenant_note}, or the template's author can declare the param \
             `trusted: true`",
            lines.join("; "),
        )
    };
    Err(ServeError::Forbidden(
        crate::secrets::registry::redact_message(&message),
    ))
}

/// Refuse a server-local path a request names outside a config (a DLQ
/// location): like a connector path in a submitted config, it may not reach a
/// system tree or the server's credentials, and must pass the caller
/// reference policy — for a tenant, confined to the tenant's path root.
/// `write` is a path the request writes, archives or deletes.
pub fn refuse_request_path(
    field: &str,
    path: &str,
    write: bool,
    tenant: Option<&str>,
) -> Result<(), ServeError> {
    let policy = crate::serve::references::installed().for_origin(
        BodyOrigin::Caller {
            allow_subprocess: false,
        },
        tenant,
    );
    refuse_request_path_with(field, path, write, tenant.is_some(), &policy)
}

/// [`refuse_request_path`] under an explicit caller policy.
pub fn refuse_request_path_with(
    field: &str,
    path: &str,
    write: bool,
    for_tenant: bool,
    policy: &ReferencePolicy,
) -> Result<(), ServeError> {
    let local = local_path(path).unwrap_or(std::borrow::Cow::Borrowed(path));
    let local = local.as_ref();
    let protected = data_path_is_protected(local, Access::Read { recursive: true })
        || (write && data_path_is_protected(local, Access::Write));
    if protected {
        return Err(ServeError::Forbidden(format!(
            "`{field}` ({path}) reaches a location a request to `faucet serve` may not touch — \
             a system tree ({}) or a file holding the server's own credentials",
            SYSTEM_DIRS.join(", ")
        )));
    }
    policy.check_path(path).map_err(|e| {
        ServeError::Forbidden(format!(
            "`{field}` ({path}) is not allowed under the server's reference policy: {}; the \
             operator can allow it with --allow-path-root{}",
            e.reason,
            if for_tenant {
                " (a tenant is confined to --tenant-path-root/<tenant>)"
            } else {
                ""
            }
        ))
    })
}

/// Refuse a config whose local-file connectors, state store or DLQ point at
/// a system tree or a file the server keeps its credentials in. Run it before
/// anything in the config executes (bound probes, discovery) and again over
/// the expanded rows.
pub fn refuse_server_paths(
    cfg: &PipelineConfig,
    nodes: &[ExpandedNode],
    origin: BodyOrigin,
) -> Result<(), ServeError> {
    let policy = crate::serve::references::installed().for_origin(origin, None);
    refuse_server_paths_with(cfg, nodes, origin, &policy)
}

/// [`refuse_server_paths`] under an explicit reference policy (#881): every
/// path, URL and command a connector names through an annotated schema field
/// must also be allowed by `policy`.
pub fn refuse_server_paths_with(
    cfg: &PipelineConfig,
    nodes: &[ExpandedNode],
    origin: BodyOrigin,
    policy: &ReferencePolicy,
) -> Result<(), ServeError> {
    let scope = Scope {
        caller: matches!(origin, BodyOrigin::Caller { .. }),
        policy: Some(policy),
        only: None,
    };
    let mut found = match serde_json::to_value(cfg) {
        Ok(doc) => data_path_findings(&doc, &scope),
        Err(e) => return Err(ServeError::Internal(format!("serializing config: {e}"))),
    };
    override_findings(cfg, &scope, &mut found);
    let read = Access::Read { recursive: false };
    for n in nodes {
        let mut check = |kind: &str, config: &Value, access: Access| {
            check_connector(kind, config, "", access, &scope, &mut found);
        };
        check(&n.source.kind, &n.source.config, read);
        check(&n.sink.kind, &n.sink.config, Access::Write);
        if let Some(st) = n
            .state
            .as_ref()
            .filter(|st| !is_default_state(&st.kind, &st.config))
        {
            check(&st.kind, &st.config, Access::Write);
        }
        if let Some(d) = &n.dlq {
            check(&d.sink.kind, &d.sink.config, Access::Write);
        }
    }
    findings_error(found)
}

/// The refusal a screening pass's findings amount to, if any.
fn findings_error(mut found: Findings) -> Result<(), ServeError> {
    for list in [&mut found.protected, &mut found.policy] {
        list.sort();
        list.dedup();
    }
    if !found.protected.is_empty() {
        return Err(ServeError::Forbidden(format!(
            "the config reads or writes local paths a config loaded by `faucet serve` may not \
             touch — system trees ({}) or files holding the server's own credentials: {}",
            SYSTEM_DIRS.join(", "),
            found.protected.join(", ")
        )));
    }
    if !found.policy.is_empty() {
        return Err(ServeError::Forbidden(format!(
            "the config names resources a submitted config may not reach under the server's \
             reference policy: {}; the operator can allow them with --allow-path-root / \
             --allow-url-host / --allow-url-scheme / --allow-unverified-tls, or register the \
             pipeline as a template",
            found.policy.join(", ")
        )));
    }
    Ok(())
}

/// Whether `value` is an absolute URL (`scheme://…`).
fn is_absolute_url(value: &str) -> bool {
    value.find("://").is_some_and(|i| {
        i > 0
            && value[..i]
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
            && value.as_bytes()[0].is_ascii_alphabetic()
    })
}

/// Every absolute URL anywhere in `doc` (the ones `scope` wants) checked
/// against its policy; a URL whose host comes from a `${…}` token cannot be
/// checked and is refused.
fn url_findings(doc: &Value, scope: &Scope<'_>, found: &mut Findings) {
    fn urls(v: &Value, ptr: &str, out: &mut Vec<(String, String)>) {
        match v {
            Value::String(s) if is_absolute_url(s) => out.push((ptr.to_string(), s.clone())),
            Value::Array(a) => a
                .iter()
                .enumerate()
                .for_each(|(i, x)| urls(x, &format!("{ptr}/{i}"), out)),
            Value::Object(o) => o
                .iter()
                .for_each(|(k, x)| urls(x, &crate::params::caller::child(ptr, k), out)),
            _ => {}
        }
    }
    let Some(policy) = scope.policy else {
        return;
    };
    let mut all = Vec::new();
    urls(doc, "", &mut all);
    for (ptr, s) in all {
        if !scope.wants(&ptr) {
            continue;
        }
        if faucet_core::resolve::url_hosts(&s)
            .iter()
            .any(|h| h.contains("${"))
        {
            let text = format!(
                "`{}` (a URL host in a submitted document may not come from a token — it \
                 cannot be checked)",
                field_name("", &ptr)
            );
            found.add(scope, "", &ptr, &s, Why::TokenHost, text);
        } else if let Err(e) = policy.check_url(&s) {
            let text = format!("`{}` ({})", field_name("", &ptr), e.reason);
            found.add(scope, "", &ptr, &s, Why::Policy(e.reason), text);
        }
    }
}

/// Screen a document fragment a request supplied for a run whose body is
/// otherwise the operator's (an inline deployment overlay merged into a
/// registered template): the fragment is caller input, so it is held to
/// everything a caller-submitted config is — its subprocess connectors, its
/// `${env:}` / `${file:}` / `${secret:}` / secret-manager references, every
/// local path (system trees, the server's credential files, the caller path
/// roots) and every URL it names — under the caller `policy`.
pub fn screen_caller_fragment(doc: &Value, policy: &ReferencePolicy) -> Result<(), ServeError> {
    refuse_bound_tokens(doc)?;
    screen_subprocess(doc, policy, false)?;
    screen_references(doc, policy)?;
    let scope = Scope {
        caller: true,
        policy: Some(policy),
        only: None,
    };
    let mut found = data_path_findings(doc, &scope);
    url_findings(doc, &scope, &mut found);
    findings_error(found)
}

/// Refuse a caller fragment that references a value bound after screening
/// (`${param.*}`, `${vars.*}`, a template or row token): the fragment is
/// screened as written, so a value substituted into it later could name any
/// URL, path or host. Only the clock / backfill / partition tokens are kept.
fn refuse_bound_tokens(doc: &Value) -> Result<(), ServeError> {
    use crate::interpolate::{Directive, iter_directives};
    let mut refused = std::collections::BTreeSet::new();
    for_each_string(doc, &mut |s| {
        for (token, dir) in iter_directives(s) {
            if let Directive::Deferred { id, .. } = dir
                && !STATIC_PATH_TOKENS.contains(&id)
            {
                refused.insert(token.to_string());
            }
        }
    });
    if refused.is_empty() {
        return Ok(());
    }
    Err(ServeError::Forbidden(format!(
        "the document references values bound after it is checked ({}); a request-supplied \
         document may use only literal values and the `${{now.*}}`, `${{backfill.*}}` and \
         `${{partition.*}}` tokens — register it as a deployment overlay to use params",
        refused.into_iter().collect::<Vec<_>>().join(", ")
    )))
}

/// The connector kinds `doc` names anywhere as a `type:` whose schema declares
/// a `command` field (`x-faucet-resource: command`): they run a program.
pub fn subprocess_kinds_in(doc: &Value) -> std::collections::BTreeSet<String> {
    fn walk(v: &Value, out: &mut std::collections::BTreeSet<String>) {
        match v {
            Value::Object(map) => {
                if let Some(Value::String(t)) = map.get("type")
                    && crate::serve::references::runs_program(t)
                {
                    out.insert(t.clone());
                }
                map.values().for_each(|c| walk(c, out));
            }
            Value::Array(items) => items.iter().for_each(|c| walk(c, out)),
            _ => {}
        }
    }
    let mut out = std::collections::BTreeSet::new();
    walk(doc, &mut out);
    out
}

/// Refuse a submitted document that reads the server's own credentials.
/// The reference resolver under an open policy: only the server's own
/// credentials are out of reach.
pub fn refuse_server_secrets(doc: &Value) -> Result<(), ServeError> {
    screen_references(doc, &ReferencePolicy::open())
}

/// Refuse a document whose `${env:}` / `${secret:}` / `${file:}` /
/// secret-manager references read the server's own credentials, or reach
/// anything `policy` does not allow (#881).
pub fn screen_references(doc: &Value, policy: &ReferencePolicy) -> Result<(), ServeError> {
    screen_references_except(doc, policy, &std::collections::BTreeSet::new())
}

/// The `${…}` load-time references in `doc`, as written.
fn load_time_tokens(doc: &Value) -> std::collections::BTreeSet<String> {
    use crate::interpolate::{Directive, iter_directives};
    let mut out = std::collections::BTreeSet::new();
    for_each_string(doc, &mut |s| {
        for (token, dir) in iter_directives(s) {
            if matches!(dir, Directive::LoadTime { .. }) {
                out.insert(token.to_string());
            }
        }
    });
    out
}

fn screen_references_except(
    doc: &Value,
    policy: &ReferencePolicy,
    skip: &std::collections::BTreeSet<String>,
) -> Result<(), ServeError> {
    use crate::interpolate::{Directive, iter_directives};
    let refs = server_secret_refs(doc);
    if !refs.is_empty() {
        return Err(ServeError::Forbidden(format!(
            "the config references the server's own credentials ({}); a config loaded by \
             `faucet serve` may not read them",
            refs.join(", ")
        )));
    }
    let mut refused = std::collections::BTreeSet::new();
    for_each_string(doc, &mut |s| {
        for (token, dir) in iter_directives(s) {
            let Directive::LoadTime { prefix, body } = dir else {
                continue;
            };
            if skip.contains(token) {
                continue;
            }
            let checked = match prefix {
                "env" | "secret" => policy.check_env(body),
                "file" => policy.check_file(body),
                p if crate::secrets::SECRET_SCHEMES.contains(&p) => policy.check_secret(p, body),
                _ => Ok(()),
            };
            if let Err(e) = checked {
                refused.insert(format!("{token}: {}", e.reason));
            }
        }
    });
    if refused.is_empty() {
        return Ok(());
    }
    Err(ServeError::Forbidden(format!(
        "the config references what a submitted config may not read under the server's \
         reference policy ({}); the operator can allow them with --allow-env / \
         --allow-path-root / --allow-secret-scheme, or register the pipeline as a template",
        refused.into_iter().collect::<Vec<_>>().join("; ")
    )))
}

/// Screen a caller-supplied document the way the loader does: its
/// references and its subprocess connectors, under the installed reference
/// policy for `origin`.
pub fn screen_document(doc: &Value, origin: BodyOrigin) -> Result<(), ServeError> {
    let policy = crate::serve::references::installed().for_origin(origin, None);
    screen_subprocess(doc, &policy, false)?;
    screen_references(doc, &policy)
}

/// Refuse subprocess connectors in a document the caller supplied, unless the
/// server allows them and the run is not for a tenant.
pub fn refuse_subprocess(
    doc: &Value,
    origin: BodyOrigin,
    for_tenant: bool,
) -> Result<(), ServeError> {
    let policy = crate::serve::references::installed().for_origin(origin, for_tenant.then_some(""));
    screen_subprocess(doc, &policy, for_tenant)
}

/// Refuse a shared auth provider (`auth:` catalog) whose token endpoint the
/// reference policy does not allow, or that reaches a system tree or a
/// server credential file. Every endpoint and file a provider names is found
/// by [`faucet_auth::provider_resources`].
pub fn screen_auth_catalog(
    cfg: &PipelineConfig,
    policy: &ReferencePolicy,
) -> Result<(), ServeError> {
    screen_auth_providers(
        cfg.auth
            .iter()
            .flatten()
            .map(|(n, s)| (format!("auth.{n}"), s)),
        policy,
        false,
    )
}

/// Check every endpoint and file an auth provider `spec` names
/// ([`faucet_auth::provider_resources`]); `at` is how a message names it
/// and the pointer of its `config`. With `scope.caller` the provider came
/// from a caller and may not name a local file at all.
fn provider_findings(spec: &Value, at: (&str, &str), scope: &Scope<'_>, found: &mut Findings) {
    let (name, base) = at;
    if scope.only.is_some_and(|f| f.is_empty()) {
        return;
    }
    for r in faucet_auth::provider_resources(spec) {
        if !scope.wants(&r.pointer) {
            continue;
        }
        let at = format!("{name}.config{}", r.pointer.replace('/', "."));
        let ptr = r.pointer.as_str();
        match r.kind {
            ResourceKind::Path => {
                let path = local_path(&r.value).unwrap_or(std::borrow::Cow::Borrowed(&r.value));
                let path = path.as_ref();
                let access = if r.writes {
                    Access::Write
                } else {
                    Access::Read { recursive: false }
                };
                if scope.caller {
                    let text = format!("{at}: {}", r.value);
                    found.add(scope, base, ptr, &r.value, Why::ProviderFile, text);
                } else if data_path_is_protected(path, access) {
                    let text = format!(
                        "{at}: {} (a system tree or a server credential file)",
                        r.value
                    );
                    found.add(scope, base, ptr, &r.value, Why::Protected, text);
                }
            }
            _ => {
                if let Some(Err(e)) = scope.policy.map(|p| p.check(&r.to_resource_ref())) {
                    let text = format!("`{at}` ({})", e.reason);
                    found.add(scope, base, ptr, &r.value, Why::Policy(e.reason), text);
                }
            }
        }
    }
}

/// [`screen_auth_catalog`] over any named provider specs (a config's `auth:`
/// catalog, a tenant's connections). With `caller`, the specs came from a
/// request: a provider may not name a local file at all — a key file is
/// passed as `key_json`, and a refresh token is persisted by the server (a
/// tenant connection persists to the vault), never to a caller-chosen
/// directory.
pub fn screen_auth_providers<'a>(
    providers: impl IntoIterator<Item = (String, &'a Value)>,
    policy: &ReferencePolicy,
    caller: bool,
) -> Result<(), ServeError> {
    let scope = Scope {
        caller,
        policy: Some(policy),
        only: None,
    };
    let mut found = Findings::default();
    for (name, spec) in providers {
        provider_findings(spec, (&name, ""), &scope, &mut found);
    }
    let (mut files, mut refused) = (Vec::new(), Vec::new());
    for f in found.items {
        match f.why {
            Why::Policy(_) => refused.push(f.text),
            _ => files.push(f.text),
        }
    }
    if !files.is_empty() {
        files.sort();
        return Err(ServeError::Forbidden(format!(
            "the config's auth providers name local files a config loaded by `faucet serve` \
             may not use: {}; pass a service-account key inline as `key_json`, and drop \
             `persist` (a submitted config may not choose where the server writes tokens)",
            files.join(", ")
        )));
    }
    if refused.is_empty() {
        return Ok(());
    }
    refused.sort();
    Err(ServeError::Forbidden(format!(
        "the config's auth providers call endpoints a submitted config may not reach under \
         the server's reference policy: {}; the operator can allow them with \
         --allow-url-host / --allow-url-scheme",
        refused.join(", ")
    )))
}

/// Refuse a notification channel whose URL the reference policy does not
/// allow: a submitted config's `notifications:` are requests the server makes.
#[cfg(feature = "notify")]
pub fn screen_notifications(
    cfg: &PipelineConfig,
    policy: &ReferencePolicy,
) -> Result<(), ServeError> {
    let mut refused: Vec<String> = cfg
        .notifications
        .iter()
        .filter_map(|n| {
            let url = n.channel.target_url()?;
            let e = policy.check_url(url).err()?;
            Some(format!(
                "notifications `{}` ({}): {}",
                n.name,
                n.channel.kind(),
                e.reason
            ))
        })
        .collect();
    if refused.is_empty() {
        return Ok(());
    }
    refused.sort();
    Err(ServeError::Forbidden(format!(
        "the config's notification channels send to URLs a submitted config may not reach \
         under the server's reference policy: {}; the operator can allow them with \
         --allow-url-host / --allow-url-scheme",
        refused.join(", ")
    )))
}

/// Refuse the connectors in `doc` that run a program (#881: a `command`
/// field in their schema) unless `policy` allows it.
pub fn screen_subprocess(
    doc: &Value,
    policy: &ReferencePolicy,
    for_tenant: bool,
) -> Result<(), ServeError> {
    if policy.allows_subprocess() {
        return Ok(());
    }
    let kinds = subprocess_kinds_in(doc);
    if kinds.is_empty() {
        return Ok(());
    }
    let kinds = kinds.into_iter().collect::<Vec<_>>().join(", ");
    let message = if for_tenant {
        format!(
            "connector type(s) {kinds} run a program on the server and are never allowed in a \
             config submitted for a tenant; register the pipeline as a template instead"
        )
    } else {
        format!(
            "connector type(s) {kinds} run a program on the server; a submitted config may use \
             them only when the server was started with --allow-subprocess-connectors (or \
             register the pipeline as a template)"
        )
    };
    Err(ServeError::Unprocessable {
        message,
        details: None,
    })
}

/// Map a failure to resolve a `${file:}` / secret-manager reference: an I/O,
/// network or credential failure may pass on retry (`503`), anything else is
/// the config's fault (`400`).
fn resolution_error(e: crate::error::CliError) -> ServeError {
    use crate::error::CliError;
    match e {
        CliError::ReadInterpolatedFile { .. }
        | CliError::SecretFetchFailed { .. }
        | CliError::SecretAuthFailed { .. } => ServeError::Unavailable(e.to_string()),
        CliError::SecretRefused { .. } => ServeError::Forbidden(e.to_string()),
        other => ServeError::BadConfig(other.to_string()),
    }
}

/// Wire format of a submitted config body.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConfigFormat {
    #[default]
    Yaml,
    Json,
}

/// A loaded submission: the merged/resolved config and its expanded nodes.
#[derive(Debug)]
pub struct LoadedSubmission {
    pub cfg: PipelineConfig,
    pub nodes: Vec<ExpandedNode>,
    /// The tenant this submission runs for (#709), with what it needs at run
    /// time. `None` for an ordinary run.
    pub tenant: Option<std::sync::Arc<TenantScope>>,
    /// The stored data-flow policy versions merged into `cfg.policy`
    /// (#1043); recorded on the run so every replica enforces the same ones.
    /// `None` in a build without the `policy` feature.
    pub policy_pin: Option<crate::serve::history::policies::PolicyPin>,
}

impl LoadedSubmission {
    /// Whether the submission is a topology config (`pipeline.nodes`). Such a
    /// submission carries no expanded rows: it runs as a node graph.
    pub fn is_topology(&self) -> bool {
        crate::topology::is_topology(&self.cfg)
    }

    /// Refuse a topology config on a path that works on matrix rows (#789
    /// CLI-17), instead of acting on an empty row set as if it had none.
    pub fn require_matrix(&self) -> Result<(), ServeError> {
        if self.is_topology() {
            return Err(ServeError::Unprocessable {
                message: crate::error::CliError::TopologyNotSupported.to_string(),
                details: None,
            });
        }
        Ok(())
    }

    /// The shared auth-provider catalog for this run: the config's own
    /// `auth:` block, plus — for a tenant run — the tenant's connections with
    /// their refresh tokens persisted back to the vault.
    pub fn auth_catalog(&self) -> crate::error::CliResult<crate::auth_catalog::AuthCatalog> {
        build_catalog(self.tenant.as_deref(), &self.cfg)
    }
}

fn build_catalog(
    tenant: Option<&TenantScope>,
    cfg: &PipelineConfig,
) -> crate::error::CliResult<crate::auth_catalog::AuthCatalog> {
    match tenant {
        Some(t) => (t.build_catalog)(cfg.auth.as_ref()),
        None => crate::auth_catalog::build_auth_catalog(cfg.auth.as_ref()),
    }
}

/// Builds the auth catalog for a tenant run from the (connection-injected)
/// `auth:` block.
pub type CatalogBuilder = std::sync::Arc<
    dyn Fn(
            Option<&std::collections::HashMap<String, Value>>,
        ) -> crate::error::CliResult<crate::auth_catalog::AuthCatalog>
        + Send
        + Sync,
>;

/// Everything a run started for a tenant needs (#709), assembled by the
/// `tenants` module and carried from submission to execution.
pub struct TenantScope {
    /// What `${tenant.*}` tokens read.
    pub values: crate::tenant_tokens::TenantValues,
    /// The tenant's usable connections as `{type, config}` provider specs,
    /// unsealed; injected into the config's `auth:` catalog (a connection
    /// shadows a catalog entry of the same name).
    pub connections: std::collections::BTreeMap<String, Value>,
    /// Connections whose endpoint the server's `--connect-providers` file sets
    /// (a hosted OAuth connection): screened under the operator's policy,
    /// not the tenant's, because the tenant did not choose its URL.
    pub operator_connections: std::collections::BTreeSet<String>,
    /// Connections that need re-authorization, with why. A run referencing
    /// one is refused.
    pub blocked: std::collections::BTreeMap<String, String>,
    /// Per-run ceilings from the tenant's limits.
    pub budget: Option<faucet_core::BudgetSpec>,
    pub build_catalog: CatalogBuilder,
    /// Records each state key the run uses, for tenant deletion.
    pub on_state_key: Option<crate::executor::StateKeyHook>,
}

impl std::fmt::Debug for TenantScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TenantScope")
            .field("tenant", &self.values.id)
            .field("connections", &self.connections.keys().collect::<Vec<_>>())
            .field("blocked", &self.blocked)
            .finish()
    }
}

impl TenantScope {
    /// The executor state scope for this tenant's runs.
    pub fn state_scope(&self) -> crate::executor::StateScope {
        crate::executor::StateScope {
            namespace: Some(self.values.id.clone()),
            on_key: self.on_state_key.clone(),
        }
    }

    /// Refuse a submission whose rows reference a connection that needs
    /// re-authorization, or a provider neither the tenant nor the config has.
    fn check_refs(&self, cfg: &PipelineConfig, nodes: &[ExpandedNode]) -> Result<(), ServeError> {
        for node in nodes {
            for config in [&node.source.config, &node.sink.config] {
                let Some(name) = crate::auth_catalog::auth_ref(config) else {
                    continue;
                };
                if let Some(why) = self.blocked.get(&name) {
                    return Err(ServeError::Conflict(format!(
                        "connection '{name}' of tenant '{}' needs re-authorization ({why}); \
                         reconnect it before running",
                        self.values.id
                    )));
                }
                if !cfg.auth.as_ref().is_some_and(|a| a.contains_key(&name)) {
                    return Err(ServeError::Conflict(format!(
                        "tenant '{}' has no connection '{name}' (row '{}' references it); \
                         create it with POST /v1/tenants/{}/connections or a connect flow",
                        self.values.id, node.id, self.values.id
                    )));
                }
            }
        }
        Ok(())
    }
}

/// Load + merge + expand a submitted config body.
/// The server-wide data-flow policy type (#702) — the real spec on a `policy`
/// build, a unit placeholder otherwise so the loader's signature is stable.
#[cfg(feature = "policy")]
pub type ServerPolicy = faucet_core::PolicySpec;
#[cfg(not(feature = "policy"))]
pub type ServerPolicy = ();

pub async fn load_submission(
    body: &str,
    format: ConfigFormat,
    default_base: Option<&Value>,
    policy: Option<&ServerPolicy>,
    origin: BodyOrigin,
) -> Result<LoadedSubmission, ServeError> {
    load_submission_scoped(body, format, default_base, policy, origin, None).await
}

/// [`load_submission`] for a run started for a tenant (#709): binds
/// `${tenant.*}`, injects the tenant's connections into the `auth:` catalog,
/// and refuses a row that references a connection needing re-authorization.
pub async fn load_submission_scoped(
    body: &str,
    format: ConfigFormat,
    default_base: Option<&Value>,
    policy: Option<&ServerPolicy>,
    origin: BodyOrigin,
    tenant: Option<std::sync::Arc<TenantScope>>,
) -> Result<LoadedSubmission, ServeError> {
    let references = crate::serve::references::installed();
    load_submission_with(
        body,
        format,
        default_base,
        policy,
        origin,
        tenant,
        &references,
    )
    .await
}

/// [`load_submission_scoped`] under an explicit reference policy (#881):
/// every `${env:}` / `${file:}` / `${secret:}` / secret-manager reference and
/// every path, URL and command a connector names is resolved through
/// `references` for the body's origin and tenant.
pub async fn load_submission_with(
    body: &str,
    format: ConfigFormat,
    default_base: Option<&Value>,
    policy: Option<&ServerPolicy>,
    origin: BodyOrigin,
    tenant: Option<std::sync::Arc<TenantScope>>,
    references: &crate::serve::references::ServeReferencePolicy,
) -> Result<LoadedSubmission, ServeError> {
    load_submission_inputs(
        body,
        format,
        default_base,
        policy,
        origin,
        tenant,
        references,
        None,
    )
    .await
}

/// [`load_submission_with`] for a trusted template body whose param values a
/// caller supplied (#989): after the trusted load, and before anything in the
/// config runs, the values `caller` names are held to the caller reference
/// policy ([`refuse_caller_params`]).
#[allow(clippy::too_many_arguments)]
pub async fn load_submission_inputs(
    body: &str,
    format: ConfigFormat,
    default_base: Option<&Value>,
    policy: Option<&ServerPolicy>,
    origin: BodyOrigin,
    tenant: Option<std::sync::Arc<TenantScope>>,
    references: &crate::serve::references::ServeReferencePolicy,
    caller: Option<CallerParams<'_>>,
) -> Result<LoadedSubmission, ServeError> {
    let caller_params = caller.filter(|c| !c.fields.is_empty());
    let refs = references.for_origin(origin, tenant.as_deref().map(|t| t.values.id.as_str()));
    // 1. Parse to a Value per the declared format.
    let mut submitted: Value = match format {
        ConfigFormat::Yaml => crate::yaml::from_str(body)
            .map_err(|e| ServeError::BadConfig(format!("invalid YAML: {e}")))?,
        ConfigFormat::Json => serde_json::from_str(body)
            .map_err(|e| ServeError::BadConfig(format!("invalid JSON: {e}")))?,
    };

    screen_references(&submitted, &refs)?;
    let caller_auth: std::collections::BTreeSet<String> = submitted
        .get("auth")
        .and_then(Value::as_object)
        .map(|m| m.keys().cloned().collect())
        .unwrap_or_default();

    // 2. ${env}/${file}/${secret} interpolation against the server's env/fs,
    // resolved INTO the parsed tree (post-parse) so a resolved value can never
    // alter the submitted document's structure (F43).
    crate::interpolate::interpolate_value(&mut submitted).map_err(resolution_error)?;

    // 3. Merge onto the workspace default (submitted wins; see merge.rs semantics).
    let mut merged = match default_base {
        Some(base) => {
            let mut m = base.clone();
            crate::merge::merge_value(&mut m, submitted);
            m
        }
        None => submitted,
    };

    // 3a. Bind `${tenant.*}` (#709) — only a tenant run has values for them.
    crate::tenant_tokens::bind_document(&mut merged, tenant.as_deref().map(|t| &t.values))
        .map_err(|message| ServeError::Unprocessable {
            message,
            details: None,
        })?;

    // 3b. Bind `${param.*}` against the config's own `params:` defaults (#444).
    // A body materialized by the template registry has no `params:` block left,
    // so this is a no-op for template-triggered runs; for a directly-submitted
    // parameterized config it applies the declared defaults and rejects a
    // `required` param with no value — which is the honest answer, since
    // `POST /v1/runs` has no param channel (register a template to get one).
    // A caller value a `params:` default holds lands wherever `${param.*}`
    // places it: follow it before binding removes the tokens.
    let param_fields =
        caller_params.map(|c| crate::params::caller::through_refs(&merged, c.fields));
    crate::params::bind_document(
        &mut merged,
        &Default::default(),
        crate::params::BindMode::Strict,
    )
    .map_err(|e| ServeError::Unprocessable {
        message: e.to_string(),
        details: None,
    })?;

    screen_subprocess(&merged, &refs, tenant.is_some())?;
    let operator_tokens = default_base.map(load_time_tokens).unwrap_or_default();
    screen_references_except(&merged, &refs, &operator_tokens)?;

    // The document the caller's param pointers address (#989).
    let caller_doc = caller_params.map(|_| merged.clone());

    // 4. Version gate + structural-ref resolution.
    let mut cfg = PipelineConfig::from_value(merged).map_err(|e| ServeError::Unprocessable {
        message: e.to_string(),
        details: None,
    })?;

    if origin.confines_sql() {
        confine_sql(&mut cfg);
    }

    // serve runs once per submission; a schedule: block is a category error.
    #[cfg(feature = "schedule")]
    if cfg.schedule.is_some() {
        return Err(ServeError::BadConfig(
            "submitted config contains a `schedule:` block — serve runs once per \
             submission; use `faucet schedule` for cron scheduling"
                .into(),
        ));
    }

    // 5. Secret-manager directives (${vault:…} etc.) with the server's creds.
    // The server-wide data-flow policy (#702) merges over the config's own
    // `policy:` block, so the static gate and the runtime backstop see one.
    #[cfg(feature = "policy")]
    if let Some(server_policy) = policy {
        cfg.policy = Some(match cfg.policy.take() {
            Some(own) => own
                .merge(server_policy.clone())
                .map_err(|e| ServeError::BadConfig(format!("policy: {e}")))?,
            None => server_policy.clone(),
        });
    }
    #[cfg(not(feature = "policy"))]
    let _ = policy;

    // Each reference is held to the policy again as it is about to be read:
    // it may have taken its final form only after `${vars.*}` / template
    // references were substituted.
    let screen = secret_screen(&refs, &operator_tokens);
    crate::secrets::resolve_secrets_screened(&mut cfg, &screen)
        .await
        .map_err(resolution_error)?;

    // A provider the request wrote is held to the caller rules; one only the
    // operator's `--default-config` base declares is not.
    let caller = matches!(origin, BodyOrigin::Caller { .. });
    let written = |n: &str| caller && caller_auth.contains(n);
    for by_caller in [true, false] {
        screen_auth_providers(
            cfg.auth
                .iter()
                .flatten()
                .filter(|(n, _)| written(n) == by_caller)
                .map(|(n, spec)| (format!("auth.{n}"), spec)),
            &refs,
            by_caller,
        )?;
    }

    // Notification channels send to URLs from the body too.
    #[cfg(feature = "notify")]
    screen_notifications(&cfg, &refs)?;

    // 5a. A tenant's connections join the auth catalog (#709), after secrets
    // resolution so an unsealed credential is never scanned for directives.
    // A connection is the tenant's own input: it is held to the tenant's
    // caller policy whatever the origin of the body that references it.
    if let Some(t) = &tenant {
        let tenant_refs = references.for_origin(
            BodyOrigin::Caller {
                allow_subprocess: false,
            },
            Some(t.values.id.as_str()),
        );
        let (hosted, written): (Vec<_>, Vec<_>) = t
            .connections
            .iter()
            .partition(|(n, _)| t.operator_connections.contains(*n));
        screen_auth_providers(
            written
                .into_iter()
                .map(|(n, spec)| (format!("connection {n}"), spec)),
            &tenant_refs,
            true,
        )?;
        screen_auth_providers(
            hosted
                .into_iter()
                .map(|(n, spec)| (format!("connection {n}"), spec)),
            &ReferencePolicy::open(),
            true,
        )?;
        let catalog = cfg.auth.get_or_insert_with(Default::default);
        for (name, spec) in &t.connections {
            catalog.insert(name.clone(), spec.clone());
        }
    }

    // 5a'. The values a caller supplied for a template's params (#989), with
    // their final text, before anything in the config runs.
    if let (Some(c), Some(doc)) = (caller_params, caller_doc.as_ref()) {
        let tenant_id = tenant.as_deref().map(|t| t.values.id.as_str());
        let caller_refs = references.for_origin(c.origin, tenant_id);
        let fields = param_fields.as_ref().unwrap_or(c.fields);
        refuse_caller_params(doc, &cfg, fields, &caller_refs, tenant_id.is_some())?;
    }

    // 5b. Discovery-driven matrix fan-out (#647): a source whose `discovery:` /
    // `odata:` block sets `fan_out` discovers its objects live and generates
    // the matrix before expansion, so a generic template's `objects` param
    // materializes into one row per object at trigger time. This is the one
    // network call in the submit path, so it runs under a hard timeout — a
    // hung upstream describe endpoint must not wedge `POST /v1/runs`.
    refuse_server_paths_with(&cfg, &[], origin, &refs)?;
    let auth =
        build_catalog(tenant.as_deref(), &cfg).map_err(|e| ServeError::BadConfig(e.to_string()))?;
    // The submit-time network calls (bound probes, discovery, the auth logins
    // they trigger) run under the body's reference policy, as the run will.
    tokio::time::timeout(
        SUBMIT_DISCOVERY_TIMEOUT,
        faucet_core::http::with_reference_policy(
            std::sync::Arc::new(refs.clone()),
            crate::partition::resolve_runtime_with(&mut cfg, &auth),
        ),
    )
    .await
    .map_err(|_| ServeError::Unprocessable {
        message: format!(
            "partition bound probes / discovery fan-out did not complete within {}s — the \
             source is slow or unreachable",
            SUBMIT_DISCOVERY_TIMEOUT.as_secs()
        ),
        details: None,
    })?
    .map_err(|e| ServeError::Unprocessable {
        message: e.to_string(),
        details: None,
    })?;

    // A topology config runs as a node graph, not as matrix rows (#789 CLI-17):
    // validate the graph here and carry no rows. A tenant run is refused — its
    // state is namespaced per matrix row, which a graph's sink nodes are not.
    if crate::topology::is_topology(&cfg) {
        if tenant.is_some() {
            return Err(ServeError::Unprocessable {
                message: "a tenant run cannot use a topology config (`pipeline.nodes`): \
                          tenant state is namespaced per matrix row, and a node graph has \
                          none"
                    .into(),
                details: None,
            });
        }
        crate::topology::validate_topology_spec(&cfg).map_err(|e| ServeError::Unprocessable {
            message: e.to_string(),
            details: None,
        })?;
        refuse_server_paths_with(&cfg, &[], origin, &refs)?;
        return Ok(LoadedSubmission {
            cfg,
            nodes: Vec::new(),
            tenant,
            policy_pin: None,
        });
    }

    // 6. Expand the matrix.
    let mut nodes = expand(&cfg).map_err(|e| ServeError::Unprocessable {
        message: e.to_string(),
        details: None,
    })?;
    if origin.confines_sql() {
        confine_sql_nodes(&mut nodes);
    }

    refuse_server_paths_with(&cfg, &nodes, origin, &refs)?;

    if let Some(t) = &tenant {
        t.check_refs(&cfg, &nodes)?;
    }

    Ok(LoadedSubmission {
        cfg,
        nodes,
        tenant,
        policy_pin: None,
    })
}

#[cfg(test)]
mod tests;
