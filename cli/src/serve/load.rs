//! Turn a submitted config body into expanded nodes, applying the workspace
//! `--default-config` base. Mirrors `PipelineConfig::from_path_async` but merges
//! a base `Value` and uses `from_value`. All `${env}`/`${file}`/`${secret}` and
//! `${vault:…}`-style directives resolve against the *server's* environment and
//! credentials (the documented privilege surface — spec §13).

use crate::config::PipelineConfig;
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
}

/// [`confine_sql`] over expanded rows: the merged configs each row builds.
pub fn confine_sql_nodes(nodes: &mut [ExpandedNode]) {
    for n in nodes {
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

#[derive(Default)]
struct Protected {
    env: std::collections::BTreeSet<String>,
    files: std::collections::BTreeSet<std::path::PathBuf>,
    secrets: std::collections::BTreeSet<(String, String)>,
}

fn protected() -> &'static std::sync::RwLock<Protected> {
    static P: std::sync::OnceLock<std::sync::RwLock<Protected>> = std::sync::OnceLock::new();
    P.get_or_init(Default::default)
}

/// Mark an environment variable the server read a credential from (an
/// `--auth-config` token) so no loaded config can resolve it.
pub fn protect_env(name: &str) {
    if let Ok(mut p) = protected().write() {
        p.env.insert(name.to_string());
    }
}

/// Mark a file the server read a credential from, as [`protect_env`].
pub fn protect_file(path: &std::path::Path) {
    if let Ok(mut p) = protected().write() {
        p.files.insert(normalize_path(path));
    }
}

/// Mark a secret-manager reference (`${vault:…}`, …) the server resolved for
/// its own use, so no loaded config can resolve it (or another `#field` of the
/// same secret).
pub fn protect_secret_ref(scheme: &str, reference: &str) {
    if let Ok(mut p) = protected().write() {
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
    if let Some(path) = sqlite_path(url) {
        for suffix in ["", "-wal", "-shm", "-journal"] {
            protect_file(std::path::Path::new(&format!("{path}{suffix}")));
        }
    }
}

fn sqlite_path(url: &str) -> Option<&str> {
    let rest = url.strip_prefix("sqlite:")?;
    let rest = rest.strip_prefix("//").unwrap_or(rest);
    let rest = rest.split('?').next().unwrap_or(rest);
    (!rest.is_empty() && rest != ":memory:").then_some(rest)
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

fn normalize_path(path: &std::path::Path) -> std::path::PathBuf {
    let absolute;
    let path = if path.is_relative() {
        absolute = std::env::current_dir()
            .map(|d| d.join(path))
            .unwrap_or_else(|_| path.to_path_buf());
        absolute.as_path()
    } else {
        path
    };
    let mut existing = path;
    let mut rest = Vec::new();
    loop {
        if let Ok(real) = std::fs::canonicalize(existing) {
            return lexical_normalize(&rest.iter().rev().fold(real, |p, c| p.join(c)));
        }
        match (existing.parent(), existing.components().next_back()) {
            (Some(parent), Some(last)) => {
                rest.push(last.as_os_str().to_os_string());
                existing = parent;
            }
            _ => return lexical_normalize(path),
        }
    }
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
    protected()
        .read()
        .map(|g| g.files.contains(&normalized))
        .unwrap_or(true)
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
        return protected_files(|f| match access {
            Access::Write => f == normalized,
            Access::Read { recursive } => {
                f == normalized
                    || f.parent() == Some(normalized.as_path())
                    || (recursive && f.starts_with(&normalized))
            }
        });
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
    let hit = match glob::Pattern::new(&full) {
        Ok(p) => protected_files(|f| p.matches_path(f)),
        Err(_) => protected_files(|f| f.starts_with(&dir)),
    };
    hit || (matches!(access, Access::Read { .. }) && glob_match_escapes(&full))
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
fn glob_match_escapes(pattern: &str) -> bool {
    let Ok(matches) = glob::glob(pattern) else {
        return false;
    };
    matches
        .take(GLOB_CHECK_LIMIT)
        .filter_map(Result::ok)
        .filter_map(|p| std::fs::canonicalize(p).ok())
        .any(|real| in_system_dir(&real) || protected_files(|f| f == real))
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

fn protected_files(pred: impl Fn(&std::path::Path) -> bool) -> bool {
    protected()
        .read()
        .map(|g| g.files.iter().any(|f| pred(f)))
        .unwrap_or(true)
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

/// The local filesystem path a config value names, if it names one.
fn local_path(value: &str) -> Option<&str> {
    if let Some(p) = sqlite_path(value) {
        return Some(p);
    }
    if let Some(p) = value.strip_prefix("file://") {
        return Some(p);
    }
    if value.contains("://") {
        return None;
    }
    let p = value.strip_prefix("file:").unwrap_or(value);
    (!p.is_empty() && p != ":memory:").then_some(p)
}

/// Runtime tokens a local path may carry in a caller-supplied config: their
/// values come from the clock or the config itself, never from records.
const STATIC_PATH_TOKENS: &[&str] = &["now", "backfill", "partition"];

/// What a screening pass refused: paths that reach the server's credentials
/// or a system tree, and resources the reference policy does not allow.
#[derive(Default)]
struct Findings {
    protected: Vec<String>,
    policy: Vec<String>,
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

fn check_local_paths(
    config: &Value,
    access: Access,
    caller: bool,
    policy: Option<&ReferencePolicy>,
    found: &mut Findings,
) {
    fn walk(
        v: &Value,
        under_path_key: bool,
        access: Access,
        caller: bool,
        policy: Option<&ReferencePolicy>,
        found: &mut Findings,
    ) {
        match v {
            Value::String(s) if under_path_key => {
                let Some(path) = local_path(s) else {
                    return;
                };
                if caller && takes_record_value(path) {
                    found.protected.push(format!("{s} {RECORD_PATH}"));
                    return;
                }
                if data_path_is_protected(path, access) {
                    found.protected.push(s.clone());
                } else if let Some(Err(e)) = policy.map(|p| p.check_path(path)) {
                    found.policy.push(format!("{s} ({})", e.reason));
                }
            }
            Value::Array(items) => items
                .iter()
                .for_each(|i| walk(i, under_path_key, access, caller, policy, found)),
            Value::Object(map) => {
                for (k, child) in map {
                    walk(child, is_path_key(k), access, caller, policy, found);
                }
            }
            _ => {}
        }
    }
    let access = match access {
        Access::Read { .. } => Access::Read {
            recursive: config.get("recursive").and_then(Value::as_bool) == Some(true),
        },
        Access::Write => Access::Write,
    };
    walk(config, false, access, caller, policy, found);
}

/// Check every resource a `kind` connector's `config` names through its
/// schema's `x-faucet-resource` annotations (#881): a local path against the
/// server's credential files and system trees, and every resource against the
/// reference policy.
fn check_connector(
    kind: &str,
    config: &Value,
    access: Access,
    caller: bool,
    policy: Option<&ReferencePolicy>,
    found: &mut Findings,
) {
    if LOCAL_PATH_KINDS.contains(&kind) {
        check_local_paths(config, access, caller, policy, found);
    }
    for r in crate::serve::references::connector_resources(kind, config) {
        if r.kind == ResourceKind::Path
            && let Some(path) = local_path(&r.value)
        {
            if caller && takes_record_value(path) {
                found.protected.push(format!("{} {RECORD_PATH}", r.value));
                continue;
            }
            if data_path_is_protected(path, access) {
                found.protected.push(r.value.clone());
                continue;
            }
        }
        if caller
            && (r.kind == ResourceKind::Url
                || (r.kind == ResourceKind::Path && local_path(&r.value).is_none()))
            && faucet_core::resolve::url_hosts(&r.value)
                .iter()
                .any(|h| takes_record_value(h))
        {
            found.policy.push(format!(
                "{kind}{}: {} (a URL host in a submitted config may not take a value from \
                 records — it cannot be checked)",
                r.pointer, r.value
            ));
            continue;
        }
        if let Some(Err(e)) = policy.map(|p| p.check(&r)) {
            found.policy.push(format!("{} ({})", r.value, e.reason));
        }
    }
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
    data_path_findings(doc, caller, None).protected
}

fn data_path_findings(doc: &Value, caller: bool, policy: Option<&ReferencePolicy>) -> Findings {
    fn walk(
        v: &Value,
        access: Access,
        caller: bool,
        policy: Option<&ReferencePolicy>,
        found: &mut Findings,
    ) {
        match v {
            Value::Object(map) => {
                let access = match map.get("kind").and_then(Value::as_str) {
                    Some("sink") => Access::Write,
                    Some("source") => Access::Read { recursive: false },
                    _ => access,
                };
                match (map.get("type").and_then(Value::as_str), map.get("config")) {
                    (Some(t), Some(cfg)) => check_connector(t, cfg, access, caller, policy, found),
                    (Some("csv" | "jsonl"), None) => {
                        if let Some(path) = map.get("path") {
                            let wrapped = serde_json::json!({ "path": path });
                            check_local_paths(&wrapped, access, caller, policy, found);
                        }
                    }
                    _ => {}
                }
                for (k, c) in map {
                    walk(c, access_under(k, access), caller, policy, found);
                }
            }
            Value::Array(items) => items
                .iter()
                .for_each(|c| walk(c, access, caller, policy, found)),
            _ => {}
        }
    }
    let mut found = Findings::default();
    walk(
        doc,
        Access::Read { recursive: false },
        caller,
        policy,
        &mut found,
    );
    for list in [&mut found.protected, &mut found.policy] {
        let mut seen = std::collections::HashSet::new();
        list.retain(|f| seen.insert(f.clone()));
    }
    found
}

fn template_spec<'a>(
    templates: &'a std::collections::HashMap<String, crate::config::ConnectorSpec>,
    legacy: &'a Option<crate::config::ConnectorSpec>,
    name: Option<&str>,
) -> Option<&'a crate::config::ConnectorSpec> {
    let name = name.unwrap_or("default");
    templates.get(name).or(if name == "default" {
        legacy.as_ref()
    } else {
        None
    })
}

/// The connector a template override builds: its kind (the override's, else
/// the template's) and the template config with the override merged over it.
fn resolve_override(
    templates: &std::collections::HashMap<String, crate::config::ConnectorSpec>,
    legacy: &Option<crate::config::ConnectorSpec>,
    name: Option<&str>,
    kind: Option<&str>,
    config: Option<&Value>,
) -> Option<(String, Value)> {
    let template = template_spec(templates, legacy, name);
    let kind = kind.or(template.map(|t| t.kind.as_str()))?.to_string();
    let mut merged = template
        .map(|t| t.config.clone())
        .unwrap_or_else(|| Value::Object(Default::default()));
    if let Some(over) = config {
        crate::merge::merge_value(&mut merged, over.clone());
    }
    Some((kind, merged))
}

/// The connectors a config builds from a template override — matrix-row
/// `source` / `sink` / `fan_out.source` and topology `source` / `sink`
/// nodes — resolved against their template, with the access each implies.
fn override_connectors(cfg: &PipelineConfig) -> Vec<(String, Value, Access)> {
    let p = &cfg.pipeline;
    let read = Access::Read { recursive: false };
    let mut out = Vec::new();
    for node in p.nodes.values() {
        let resolved = match node {
            crate::config::NodeSpec::Source {
                template,
                kind,
                config,
            } => resolve_override(
                &p.sources,
                &p.source,
                template.as_deref(),
                kind.as_deref(),
                config.as_ref(),
            )
            .map(|(k, c)| (k, c, read)),
            crate::config::NodeSpec::Sink {
                template,
                kind,
                config,
            } => resolve_override(
                &p.sinks,
                &p.sink,
                template.as_deref(),
                kind.as_deref(),
                config.as_ref(),
            )
            .map(|(k, c)| (k, c, Access::Write)),
            _ => None,
        };
        out.extend(resolved);
    }
    for row in &cfg.matrix {
        let no_templates = Default::default();
        let discover = row.discover.as_ref().map(|d| &d.source);
        let discover_templates = match discover {
            Some(d) if d.r#ref.is_none() => &no_templates,
            _ => &p.sources,
        };
        let overrides = [
            (row.source.as_ref(), &p.sources, &p.source, read),
            (discover, discover_templates, &None, read),
            (row.sink.as_ref(), &p.sinks, &p.sink, Access::Write),
        ];
        for (over, templates, legacy, access) in overrides {
            let Some(over) = over else { continue };
            out.extend(
                resolve_override(
                    templates,
                    legacy,
                    over.r#ref.as_deref(),
                    over.kind.as_deref(),
                    over.config.as_ref(),
                )
                .map(|(k, c)| (k, c, access)),
            );
        }
    }
    out
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
    let local = local_path(path).unwrap_or(path);
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
    let caller = matches!(origin, BodyOrigin::Caller { .. });
    let policy = Some(policy);
    let mut found = match serde_json::to_value(cfg) {
        Ok(doc) => data_path_findings(&doc, caller, policy),
        Err(e) => return Err(ServeError::Internal(format!("serializing config: {e}"))),
    };
    let mut check = |kind: &str, config: &Value, access: Access| {
        check_connector(kind, config, access, caller, policy, &mut found);
    };
    let read = Access::Read { recursive: false };
    for (kind, config, access) in override_connectors(cfg) {
        check(&kind, &config, access);
    }
    for n in nodes {
        check(&n.source.kind, &n.source.config, read);
        check(&n.sink.kind, &n.sink.config, Access::Write);
        if let Some(st) = &n.state {
            check(&st.kind, &st.config, Access::Write);
        }
        if let Some(d) = &n.dlq {
            check(&d.sink.kind, &d.sink.config, Access::Write);
        }
    }
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
             --allow-url-host / --allow-url-scheme, or register the pipeline as a template",
            found.policy.join(", ")
        )));
    }
    Ok(())
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

/// Keys of a shared auth provider's config that name an endpoint it calls.
const AUTH_URL_KEYS: &[&str] = &["token_url", "url", "authorization_url"];

/// Refuse a shared auth provider (`auth:` catalog) whose token endpoint the
/// reference policy does not allow.
pub fn screen_auth_catalog(
    cfg: &PipelineConfig,
    policy: &ReferencePolicy,
) -> Result<(), ServeError> {
    let mut refused = Vec::new();
    for (name, spec) in cfg.auth.iter().flatten() {
        let Some(config) = spec.get("config") else {
            continue;
        };
        for key in AUTH_URL_KEYS {
            if let Some(Value::String(url)) = config.get(*key)
                && let Err(e) = policy.check_url(url)
            {
                refused.push(format!("auth.{name}.config.{key}: {url} ({})", e.reason));
            }
        }
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
    let refs = references.for_origin(origin, tenant.as_deref().map(|t| t.values.id.as_str()));
    // 1. Parse to a Value per the declared format.
    let mut submitted: Value = match format {
        ConfigFormat::Yaml => serde_yaml::from_str(body)
            .map_err(|e| ServeError::BadConfig(format!("invalid YAML: {e}")))?,
        ConfigFormat::Json => serde_json::from_str(body)
            .map_err(|e| ServeError::BadConfig(format!("invalid JSON: {e}")))?,
    };

    screen_references(&submitted, &refs)?;

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

    crate::secrets::resolve_secrets(&mut cfg)
        .await
        .map_err(resolution_error)?;

    screen_auth_catalog(&cfg, &refs)?;

    // 5a. A tenant's connections join the auth catalog (#709), after secrets
    // resolution so an unsealed credential is never scanned for directives.
    if let Some(t) = &tenant {
        let catalog = cfg.auth.get_or_insert_with(Default::default);
        for (name, spec) in &t.connections {
            catalog.insert(name.clone(), spec.clone());
        }
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
    tokio::time::timeout(
        SUBMIT_DISCOVERY_TIMEOUT,
        crate::partition::resolve_runtime_with(&mut cfg, &auth),
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

    Ok(LoadedSubmission { cfg, nodes, tenant })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A path a request names directly (a DLQ location) is held to the
    /// caller policy, the system trees and a tenant's root.
    #[test]
    fn a_request_path_is_screened_like_a_connector_path() {
        let base = tempfile::tempdir().unwrap();
        let tenants = base.path().join("tenants");
        std::fs::create_dir_all(tenants.join("acme")).unwrap();
        std::fs::create_dir_all(tenants.join("other")).unwrap();
        let args = crate::cli::ReferenceArgs {
            allow_path_root: vec![base.path().to_path_buf()],
            tenant_path_root: Some(tenants.clone()),
            ..Default::default()
        };
        let refs = crate::serve::references::ServeReferencePolicy::from_args(&args).unwrap();
        let caller = BodyOrigin::Caller {
            allow_subprocess: false,
        };
        let acme = refs.for_origin(caller, Some("acme"));
        let own = tenants.join("acme/dlq.jsonl").display().to_string();
        let theirs = tenants.join("other/dlq.jsonl").display().to_string();
        assert!(refuse_request_path_with("from", &own, true, true, &acme).is_ok());
        let err = refuse_request_path_with("from", &theirs, false, true, &acme).unwrap_err();
        assert!(
            matches!(&err, ServeError::Forbidden(m) if m.contains("`from`") && m.contains("--tenant-path-root")),
            "{err:?}"
        );
        let open = ReferencePolicy::open();
        for bad in ["/proc/self/environ", "/sys/kernel", "/dev/sda"] {
            let err = refuse_request_path_with("location", bad, false, false, &open).unwrap_err();
            assert!(
                matches!(&err, ServeError::Forbidden(m) if m.contains("system tree")),
                "{bad}"
            );
        }
        let operator = refs.for_origin(caller, None);
        let outside = "/var/tmp-faucet-not-a-root/dlq.jsonl";
        let err =
            refuse_request_path_with("location", outside, true, false, &operator).unwrap_err();
        assert!(
            matches!(&err, ServeError::Forbidden(m) if m.contains("--allow-path-root") && !m.contains("tenant")),
            "{err:?}"
        );
        assert!(refuse_request_path("location", "/etc/shadow", true, None).is_err());
    }

    /// The sandbox switches come from the connector's schema (#876).
    #[cfg(all(feature = "source-sqlite", feature = "sink-sqlite"))]
    #[test]
    fn sandboxed_kinds_are_read_from_the_schema() {
        assert_eq!(&*sandbox_fields(true, "sqlite"), &["sandbox".to_string()]);
        assert!(source_is_sandboxed("sqlite"));
        assert!(
            !sink_is_sandboxed("sqlite"),
            "the sqlite sink runs no caller SQL"
        );
        assert!(!source_is_sandboxed("no-such-kind"));
        let mut cfg = json!({});
        force_override_value(&mut cfg);
        assert!(cfg.as_object().unwrap().is_empty());
    }

    fn force_override_value(cfg: &mut Value) {
        let mut opt = Some(cfg.clone());
        force_override(&mut opt, true, "no-such-kind");
        *cfg = opt.unwrap();
        let mut none = None;
        force_override(&mut none, true, "no-such-kind");
        assert!(none.is_none(), "an unconfined kind gets no override block");
    }

    #[cfg(all(
        feature = "source-duckdb",
        feature = "sink-duckdb",
        feature = "source-sqlite"
    ))]
    #[test]
    fn every_sql_connector_position_is_sandboxed() {
        let doc = json!({
            "version": 1,
            "pipeline": {
                "source": { "type": "duckdb", "config": { "database": ":memory:", "query": "SELECT 1", "sandbox": false } },
                "sources": {
                    "lite": { "type": "sqlite", "config": { "database_url": "sqlite::memory:", "query": "SELECT 1" } },
                    "csv": { "type": "csv", "config": { "path": "a.csv" } }
                },
                "sink": { "type": "duckdb", "config": { "database": ":memory:", "table_name": "t" } },
                "sinks": { "lite": { "type": "sqlite", "config": { "database_url": "sqlite::memory:", "table": "t" } } },
                "dlq": { "sink": { "type": "duckdb", "config": { "database": ":memory:", "table_name": "d" } } }
            },
            "matrix": [
                { "id": "a", "source": { "ref": "lite", "config": { "sandbox": false } } },
                { "id": "b", "source": { "ref": "csv" }, "sink": { "type": "duckdb", "config": { "database": ":memory:", "table_name": "u" } },
                  "dlq": { "sink": { "type": "duckdb", "config": { "database": ":memory:", "table_name": "e" } } } },
                { "id": "c", "source": { "ref": "csv" }, "sink": { "ref": "lite" } }
            ]
        });
        let mut cfg = PipelineConfig::from_value(doc).unwrap();
        confine_sql(&mut cfg);
        let on = |v: &Value| v.get("sandbox") == Some(&Value::Bool(true));
        let p = &cfg.pipeline;
        assert!(on(&p.source.as_ref().unwrap().config));
        assert!(on(&p.sources["lite"].config));
        assert!(!on(&p.sources["csv"].config));
        assert!(on(&p.sink.as_ref().unwrap().config));
        assert!(
            !on(&p.sinks["lite"].config),
            "the sqlite sink runs no caller SQL"
        );
        assert!(on(&p.dlq.as_ref().unwrap().sink.config));
        assert!(on(cfg.matrix[0]
            .source
            .as_ref()
            .unwrap()
            .config
            .as_ref()
            .unwrap()));
        assert!(on(cfg.matrix[1]
            .sink
            .as_ref()
            .unwrap()
            .config
            .as_ref()
            .unwrap()));
        assert!(on(&cfg.matrix[1]
            .dlq
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap()
            .sink
            .config));
        assert!(cfg.matrix[2].sink.as_ref().unwrap().config.is_none());

        let mut nodes = expand(&cfg).unwrap();
        for n in &mut nodes {
            if let Some(m) = n.source.config.as_object_mut() {
                m.insert("sandbox".into(), Value::Bool(false));
            }
        }
        confine_sql_nodes(&mut nodes);
        let a = nodes.iter().find(|n| n.id == "a").unwrap();
        assert!(on(&a.source.config));
        let b = nodes.iter().find(|n| n.id == "b").unwrap();
        assert!(!on(&b.source.config));
        assert!(on(&b.sink.config));
        assert!(on(&b.dlq.as_ref().unwrap().sink.config));
    }

    #[cfg(all(
        feature = "source-duckdb",
        feature = "sink-duckdb",
        feature = "source-sqlite"
    ))]
    #[test]
    fn topology_nodes_fan_out_sources_and_the_mirror_snapshot_are_sandboxed() {
        let topo = json!({
            "version": 1,
            "pipeline": {
                "sources": { "d": { "type": "duckdb", "config": { "database": ":memory:", "query": "SELECT 1" } } },
                "sinks": { "default": { "type": "duckdb", "config": { "database": ":memory:", "table_name": "t" } } },
                "nodes": {
                    "src": { "kind": "source", "ref": "d", "config": { "sandbox": false } },
                    "lite": { "kind": "source", "type": "sqlite", "config": { "database_url": "sqlite::memory:", "query": "SELECT 1" } },
                    "csv": { "kind": "source", "type": "csv", "config": { "path": "a.csv" } },
                    "out": { "kind": "sink" },
                    "m": { "kind": "merge" }
                },
                "edges": []
            }
        });
        let mut cfg: PipelineConfig = serde_json::from_value(topo).unwrap();
        confine_sql(&mut cfg);
        let node_cfg = |id: &str| match &cfg.pipeline.nodes[id] {
            crate::config::NodeSpec::Source { config, .. }
            | crate::config::NodeSpec::Sink { config, .. } => config.clone(),
            _ => None,
        };
        let on =
            |v: Option<Value>| v.and_then(|v| v.get("sandbox").cloned()) == Some(Value::Bool(true));
        assert!(on(node_cfg("src")));
        assert!(on(node_cfg("lite")));
        assert!(!on(node_cfg("csv")));
        assert!(on(node_cfg("out")));

        let fan = json!({
            "version": 1,
            "pipeline": {
                "sources": { "lite": { "type": "sqlite", "config": { "database_url": "sqlite::memory:", "query": "SELECT 1" } } },
                "sink": { "type": "jsonl", "config": { "path": "o.jsonl" } }
            },
            "matrix": [ { "id": "r", "source": { "ref": "lite" },
                "fan_out": { "source": { "type": "sqlite", "config": { "database_url": "sqlite::memory:", "query": "SELECT 1 AS id" } }, "select": "$.id", "as": "id" } } ]
        });
        let mut cfg: PipelineConfig = serde_json::from_value(fan).unwrap();
        confine_sql(&mut cfg);
        let d = cfg.matrix[0].discover.as_ref().unwrap();
        assert_eq!(d.source.config.as_ref().unwrap()["sandbox"], true);

        let mirror = json!({
            "version": 1,
            "pipeline": {
                "source": { "type": "postgres-cdc", "config": {} },
                "sink": { "type": "jsonl", "config": { "path": "o.jsonl" } }
            },
            "mirror": { "mode": "snapshot_then_cdc",
                "snapshot": { "source": { "type": "sqlite", "config": { "database_url": "sqlite::memory:", "query": "SELECT 1" } } } }
        });
        let mut cfg: PipelineConfig = serde_json::from_value(mirror).unwrap();
        confine_sql(&mut cfg);
        assert_eq!(
            cfg.replication.as_ref().unwrap().snapshot.source.config["sandbox"],
            true
        );
    }

    #[test]
    fn only_a_server_file_keeps_host_access() {
        assert!(BodyOrigin::Trusted.confines_sql());
        assert!(
            BodyOrigin::Caller {
                allow_subprocess: true
            }
            .confines_sql()
        );
        assert!(!BodyOrigin::ServerFile.confines_sql());
    }

    #[cfg(all(
        feature = "source-duckdb",
        feature = "sink-duckdb",
        feature = "source-sqlite"
    ))]
    #[tokio::test]
    async fn a_loaded_submission_is_sandboxed_unless_it_came_from_a_server_file() {
        let body = json!({
            "version": 1,
            "pipeline": {
                "source": { "type": "duckdb", "config": { "database": ":memory:", "query": "SELECT 1", "sandbox": false } },
                "sink": { "type": "jsonl", "config": { "path": "o.jsonl" } }
            }
        })
        .to_string();
        let caller = load_submission(&body, ConfigFormat::Json, None, None, BodyOrigin::Trusted)
            .await
            .unwrap();
        assert_eq!(caller.nodes[0].source.config["sandbox"], true);
        let file = load_submission(
            &body,
            ConfigFormat::Json,
            None,
            None,
            BodyOrigin::ServerFile,
        )
        .await
        .unwrap();
        assert_eq!(file.nodes[0].source.config["sandbox"], false);
    }

    #[test]
    fn a_path_that_normalizes_into_proc_is_protected() {
        assert!(file_is_protected("/proc/self/environ"));
        assert!(file_is_protected("/tmp/../proc/self/environ"));
        assert!(file_is_protected("/nonexistent/./../proc/1/environ"));
        assert_eq!(
            lexical_normalize(std::path::Path::new("/a/./b/../c")),
            std::path::PathBuf::from("/a/c")
        );
    }
    use serde_json::json;

    fn base() -> Value {
        json!({
            "version": 1,
            "pipeline": {
                "source": { "type": "csv", "config": { "path": "DEFAULT.csv" } },
                "sink": { "type": "jsonl", "config": { "path": "out.jsonl" } }
            }
        })
    }

    #[tokio::test]
    async fn submitted_overrides_default() {
        let body = r#"{ "pipeline": { "source": { "config": { "path": "OVERRIDE.csv" } } } }"#;
        let loaded = load_submission(
            body,
            ConfigFormat::Json,
            Some(&base()),
            None,
            BodyOrigin::Trusted,
        )
        .await
        .unwrap();
        // The override wins; the default sink survives the merge.
        let node = &loaded.nodes[0];
        assert_eq!(node.source.config["path"], "OVERRIDE.csv");
        assert_eq!(node.sink.config["path"], "out.jsonl");
    }

    #[tokio::test]
    async fn missing_version_without_base_is_unprocessable() {
        // version defaults to 1 via serde, so this exercises the expand/validation
        // failure path (pipeline with no source/sink). Accept either layer's error.
        let body = r#"{ "pipeline": {} }"#;
        let err = load_submission(body, ConfigFormat::Json, None, None, BodyOrigin::Trusted)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            ServeError::Unprocessable { .. } | ServeError::BadConfig(_)
        ));
    }

    #[cfg(feature = "schedule")]
    #[tokio::test]
    async fn schedule_block_is_rejected() {
        let body = r#"
version: 1
pipeline:
  source: { type: csv, config: { path: x.csv } }
  sink: { type: jsonl, config: { path: out.jsonl } }
schedule:
  cron: "0 * * * *"
  timezone: UTC
"#;
        let err = load_submission(body, ConfigFormat::Yaml, None, None, BodyOrigin::Trusted)
            .await
            .unwrap_err();
        match err {
            ServeError::BadConfig(m) => assert!(m.contains("schedule:")),
            other => panic!("expected BadConfig, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn invalid_yaml_is_bad_config() {
        let err = load_submission("{[bad", ConfigFormat::Yaml, None, None, BodyOrigin::Trusted)
            .await
            .unwrap_err();
        assert!(matches!(err, ServeError::BadConfig(_)));
    }

    #[tokio::test]
    async fn submitted_extends_is_rejected_with_composition_hint() {
        // Composition must NOT run for HTTP-submitted bodies — otherwise a client
        // could read arbitrary server files via `extends`. `deny_unknown_fields`
        // rejects the key during `from_value` (no I/O), and `friendly_parse_error`
        // attaches the composition hint.
        let body = "version: 1\nextends: /etc/passwd\npipeline:\n  source: { type: csv, config: { path: x.csv } }\n  sink: { type: jsonl, config: { path: o.jsonl } }\n";
        let err = load_submission(body, ConfigFormat::Yaml, None, None, BodyOrigin::Trusted)
            .await
            .unwrap_err();
        // ServeError doesn't implement Display; pull the inner message directly.
        let msg = match &err {
            ServeError::Unprocessable { message, .. } => message.clone(),
            ServeError::BadConfig(m) => m.clone(),
            other => format!("{other:?}"),
        };
        // Assert the hint fired (not merely that serde named the field) so a
        // regression in `friendly_parse_error` is caught.
        assert!(
            msg.contains("composition"),
            "submitted extends must be rejected with the composition hint, got: {msg}"
        );
    }

    fn csv_body(path: &str) -> String {
        format!(
            "version: 1\npipeline:\n  source: {{ type: csv, config: {{ path: \"{path}\" }} }}\n  sink: {{ type: jsonl, config: {{ path: out.jsonl }} }}\n"
        )
    }

    const SINGER: &str = "version: 1\npipeline:\n  source: { type: singer, config: { executable: /bin/sh, stream: x } }\n  sink: { type: jsonl, config: { path: out.jsonl } }\n";

    fn message(e: &ServeError) -> String {
        e.api_error().error.message
    }

    #[test]
    fn server_secret_references_are_found() {
        let doc = json!({
            "a": "${env:FAUCET_VAULT_KEY}",
            "b": ["x ${secret:FAUCET_SERVE_ADMIN_TOKEN} y"],
            "c": {"${file:/proc/1/environ}": "${file:/proc/self/cmdline}"},
            "ok": "${env:HOME} ${file:./data.csv} ${vault:kv/x} ${users.id}"
        });
        let refs = server_secret_refs(&doc);
        assert_eq!(refs.len(), 4, "{refs:?}");
        assert!(refs.iter().all(|r| !r.contains("HOME")));

        protect_env("FAUCET_LOAD_TEST_PRINCIPAL");
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("tok");
        std::fs::write(&file, "x").unwrap();
        protect_file(&file);
        let doc = json!([
            "${env:FAUCET_LOAD_TEST_PRINCIPAL}",
            format!("${{file:{}}}", file.display()),
            format!("${{file:{}/./tok}}", dir.path().display()),
        ]);
        assert_eq!(server_secret_refs(&doc).len(), 3);
        assert!(refuse_server_secrets(&json!({"x": "${env:HOME}"})).is_ok());
        let err = refuse_server_secrets(&doc).unwrap_err();
        assert!(matches!(err, ServeError::Forbidden(_)));
    }

    #[test]
    fn secret_manager_references_the_server_resolved_are_protected() {
        protect_secret_ref("vault", "secret/load-test#admin");
        let refs = server_secret_refs(&json!([
            "${vault:secret/load-test}",
            "${vault:secret/load-test#other}",
            "${aws-sm:secret/load-test}",
            "${vault:secret/load-test-2}",
        ]));
        assert_eq!(
            refs,
            vec![
                "${vault:secret/load-test}".to_string(),
                "${vault:secret/load-test#other}".to_string()
            ]
        );
    }

    #[test]
    fn protect_input_covers_the_file_and_everything_it_reads() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("providers.yaml");
        std::fs::write(&input, "x").unwrap();
        let read = dir.path().join("client-secret");
        protect_input(
            &input,
            &json!({"a": "${env:LOAD_TEST_INPUT_VAR}", "b": format!("${{file:{}}}", read.display()),
                    "c": "${gcp-sm:projects/p/secrets/load-test/versions/1}"}),
        );
        let refs = server_secret_refs(&json!([
            format!("${{file:{}}}", input.display()),
            format!("${{file:{}}}", read.display()),
            "${env:LOAD_TEST_INPUT_VAR}",
            "${secret:LOAD_TEST_INPUT_VAR}",
            "${gcp-sm:projects/p/secrets/load-test/versions/1}",
        ]));
        assert_eq!(refs.len(), 5, "{refs:?}");
        for name in [
            "FAUCET_SERVE_HISTORY",
            "FAUCET_VAULT_PREVIOUS_KEYS",
            "FAUCET_SNAPSHOT_KEY",
        ] {
            assert!(env_is_protected(name), "{name}");
        }
    }

    #[test]
    fn sqlite_urls_name_their_file() {
        assert_eq!(sqlite_path("sqlite:/var/h.db"), Some("/var/h.db"));
        assert_eq!(
            sqlite_path("sqlite:///var/h.db?mode=rwc"),
            Some("/var/h.db")
        );
        assert_eq!(sqlite_path("sqlite::memory:"), None);
        assert_eq!(sqlite_path("postgres://x"), None);
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("history.db");
        protect_sqlite_url(&format!("sqlite:{}", db.display()));
        assert!(file_is_protected(db.to_str().unwrap()));
        assert!(file_is_protected(&format!("{}-wal", db.display())));
    }

    fn read_protected(path: &str) -> bool {
        data_path_is_protected(path, Access::Read { recursive: false })
    }

    #[test]
    fn local_paths_and_globs_that_reach_protected_files_are_found() {
        let dir = tempfile::tempdir().unwrap();
        let secrets = dir.path().join("etc");
        std::fs::create_dir(&secrets).unwrap();
        let auth = secrets.join("auth.yaml");
        std::fs::write(&auth, "x").unwrap();
        protect_file(&auth);
        let data = dir.path().join("data");
        std::fs::create_dir(&data).unwrap();
        let d = dir.path().display();

        assert!(read_protected("/proc/self/environ"));
        assert!(read_protected("/sys/kernel"));
        assert!(read_protected("/dev/stdin"));
        assert!(!read_protected("/dev/null"));
        assert!(read_protected(&auth.display().to_string()));
        assert!(read_protected(&format!("{d}/data/../etc/auth.yaml")));
        assert!(
            read_protected(&format!("{d}/etc")),
            "a directory holding one"
        );
        assert!(read_protected(&format!("{d}/etc/*.yaml")));
        assert!(
            read_protected(&format!("{d}/*")),
            "a glob that recurses into it"
        );
        assert!(read_protected(&format!("{d}/data/*/../../etc/auth.yaml")));
        assert!(read_protected(&format!("{d}/etc/${{now.date}}.yaml")));
        assert!(!read_protected(&format!("{d}/data/in.csv")));
        assert!(!read_protected(&format!("{d}/data/*.csv")));
        assert!(!read_protected(&format!("{d}/etc/*.csv")));
        assert!(!read_protected(&format!(
            "{d}/data/dt=${{now.date}}/x.jsonl"
        )));

        assert_eq!(local_path("sqlite:/x.db"), Some("/x.db"));
        assert_eq!(local_path("file:///x"), Some("/x"));
        assert_eq!(local_path("file:./x"), Some("./x"));
        assert_eq!(local_path("s3://b/k"), None);
        assert_eq!(local_path(":memory:"), None);
        assert_eq!(replace_tokens("a/${now.date}/b${x"), "a/*/b*");
    }

    #[test]
    fn data_paths_are_checked_only_in_local_kinds_and_path_keys() {
        let doc = json!({
            "pipeline": {
                "source": {"type": "file", "config": {"path": "/proc/self/environ", "format": "raw_text"}},
                "sink": {"type": "rest", "config": {"path": "/proc/x"}},
                "sources": {"q": {"type": "duckdb", "config": {"database": "/dev/zero", "query": "/proc"}}}
            },
            "state": {"type": "file", "config": {"dir": "/sys/x"}},
            "lineage": {"transport": {"type": "file", "config": {"path": "out.jsonl"}}}
        });
        let found = server_data_paths(&doc, false);
        assert_eq!(found.len(), 3, "{found:?}");
        assert!(found.iter().all(|f| !f.contains("/proc/x")));

        let caller = json!({"type": "jsonl", "config": {"path": "out/${parent.name}.jsonl"}});
        assert_eq!(server_data_paths(&caller, true).len(), 1);
        assert!(server_data_paths(&caller, false).is_empty());
        let clock = json!({"type": "jsonl", "config": {"path": "out/${now.date}.jsonl"}});
        assert!(server_data_paths(&clock, true).is_empty());
    }

    #[test]
    fn a_wildcard_in_any_segment_that_can_reach_a_system_tree_is_refused() {
        for glob in [
            "/p*/self/environ",
            "/*/self/environ",
            "/**/environ",
            "/[p]roc/self/environ",
            "/pro?/self/environ",
            "/[/x",
            "/${x}/self/environ",
        ] {
            assert!(read_protected(glob), "{glob}");
        }
        assert!(segments_can_reach(&["**", "x"], &["proc"]));
        assert!(segments_can_reach(&["proc"], &["proc"]));
        assert!(!segments_can_reach(&[], &["proc"]));
        assert!(!segments_can_reach(&["data", "*"], &["proc"]));
        assert!(!pattern_can_reach_system_dir("/data/*/x.csv"));
        assert!(!read_protected("/no-such-faucet-dir/*/x.csv"));
    }

    #[cfg(unix)]
    #[test]
    fn a_glob_match_that_is_a_symlink_into_a_system_tree_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink("/dev/zero", dir.path().join("link")).unwrap();
        std::fs::write(dir.path().join("plain.csv"), "a\n").unwrap();
        let d = dir.path().display();
        assert!(read_protected(&format!("{d}/l*")));
        assert!(!read_protected(&format!("{d}/*.csv")));
        assert!(
            !data_path_is_protected(&format!("{d}/l*"), Access::Write),
            "a sink path is never expanded"
        );
    }

    #[test]
    fn writing_beside_a_protected_file_is_allowed_but_reading_it_is_not() {
        let dir = tempfile::tempdir().unwrap();
        let deep = dir.path().join("a").join("b");
        std::fs::create_dir_all(&deep).unwrap();
        let db = deep.join("history.db");
        std::fs::write(&db, "x").unwrap();
        protect_file(&db);
        let d = dir.path().display();
        let b = deep.display();
        let write = |p: &str| data_path_is_protected(p, Access::Write);
        assert!(write(&db.display().to_string()));
        assert!(
            write(&format!("{b}/{{part}}.db")),
            "a template that can name it"
        );
        assert!(!write(&b.to_string()), "a state store directory holding it");
        assert!(!write(&format!("{b}/out.jsonl")));
        assert!(read_protected(&b.to_string()), "a listing of its directory");
        assert!(!read_protected(&format!("{d}/a")), "a listing one level up");
        assert!(data_path_is_protected(
            &format!("{d}/a"),
            Access::Read { recursive: true }
        ));
        assert_eq!(replace_templates("x/{part}/y{z"), "x/*/y*");

        let state =
            json!({"pipeline": {"state": {"type": "file", "config": {"path": b.to_string()}}}});
        assert!(server_data_paths(&state, true).is_empty());
        let recursive = json!({"pipeline": {"source": {"type": "file", "config": {"path": format!("{d}/a"), "recursive": true}}}});
        assert_eq!(server_data_paths(&recursive, true).len(), 1);
        let node = json!({"pipeline": {"nodes": {"out": {"kind": "sink", "type": "file", "config": {"path": b.to_string()}}}}});
        assert!(server_data_paths(&node, true).is_empty());
    }

    #[test]
    fn sql_transform_relations_are_path_checked() {
        let doc = json!({"pipeline": {"transforms": [{"type": "sql", "config": {
        "query": "SELECT * FROM batch, env",
        "relations": [
            {"name": "env", "source": {"type": "csv", "path": "/proc/self/environ", "has_header": false}},
            {"name": "j", "source": {"type": "jsonl", "path": "/sys/x"}},
            {"name": "v", "source": {"type": "values", "columns": ["a"], "rows": [[1]]}}
        ]}}]}});
        let found = server_data_paths(&doc, true);
        assert_eq!(found, vec!["/proc/self/environ", "/sys/x"]);
    }

    fn cfg_of(yaml: &str) -> PipelineConfig {
        PipelineConfig::from_value(serde_yaml::from_str(yaml).unwrap()).unwrap()
    }

    #[test]
    fn template_overrides_without_a_type_are_checked_against_the_template_kind() {
        let caller = BodyOrigin::Caller {
            allow_subprocess: false,
        };
        let topology = cfg_of(
            "version: 1\npipeline:\n  sources:\n    x: { type: csv, config: { path: in.csv } }\n  sinks:\n    o: { type: jsonl, config: { path: out.jsonl } }\n  nodes:\n    src: { kind: source, ref: x, config: { path: /proc/self/environ } }\n    out: { kind: sink, ref: o }\n  edges:\n    - [src, out]\n",
        );
        let err = refuse_server_paths(&topology, &[], caller).unwrap_err();
        assert!(message(&err).contains("/proc/self/environ"), "{err:?}");

        let fan_out = cfg_of(
            "version: 1\npipeline:\n  sources:\n    x: { type: csv, config: { path: in.csv } }\n  sink: { type: stdout, config: {} }\nmatrix:\n  - id: r\n    fan_out:\n      source: { ref: x, config: { path: /sys/kernel } }\n      select: \"$.id\"\n      as: v\n  - id: standalone\n    fan_out:\n      source: { type: file, config: { path: /dev/stdin } }\n      select: \"$.id\"\n      as: w\n",
        );
        let err = refuse_server_paths(&fan_out, &[], caller).unwrap_err();
        assert!(message(&err).contains("/sys/kernel"), "{err:?}");
        assert!(message(&err).contains("/dev/stdin"), "{err:?}");

        let retyped = cfg_of(
            "version: 1\npipeline:\n  sources:\n    x: { type: rest, config: { path: /proc/self/environ } }\n  sink: { type: stdout, config: {} }\nmatrix:\n  - id: r\n    source: { ref: x, type: file }\n",
        );
        assert!(refuse_server_paths(&retyped, &[], caller).is_err());

        let sink = cfg_of(
            "version: 1\npipeline:\n  source: { type: csv, config: { path: in.csv } }\n  sinks:\n    o: { type: jsonl, config: { path: out.jsonl } }\nmatrix:\n  - id: r\n    sink: { ref: o, config: { path: /dev/stdout } }\n",
        );
        assert!(refuse_server_paths(&sink, &[], caller).is_err());

        let fine = cfg_of(
            "version: 1\npipeline:\n  sources:\n    x: { type: rest, config: { url: \"http://h\", path: /users } }\n  sink: { type: stdout, config: {} }\nmatrix:\n  - id: r\n    source: { ref: x, config: { path: \"/users/${now.date}\" } }\n  - id: missing\n    source: { ref: nope, config: { path: /proc/x } }\n",
        );
        assert!(refuse_server_paths(&fine, &[], caller).is_ok());
    }

    #[cfg(all(
        feature = "source-duckdb",
        feature = "sink-duckdb",
        feature = "source-sqlite"
    ))]
    #[test]
    fn bound_probes_and_the_verify_destination_are_sandboxed() {
        let mut cfg = cfg_of(
            "version: 1\npipeline:\n  source: { type: csv, config: { path: in.csv } }\n  sink: { type: stdout, config: {} }\npartition:\n  kind: integer\n  from: 0\n  chunk_size: 10\n  bounds: inclusive\n  to: { from_source: { type: duckdb, config: { database: \":memory:\", query: \"SELECT 1 AS m\" } }, value_path: \"$.m\" }\nverify:\n  key: [id]\n  destination: { type: sqlite, config: { database: x.db, query: \"SELECT 1\" } }\nmatrix:\n  - id: r\n    partition:\n      kind: offset\n      chunk_size: 10\n      total: { from_source: { type: sqlite, config: { database: x.db, query: \"SELECT 1 AS n\" } }, value_path: \"$.n\" }\n  - id: plain\n    partition: { kind: integer, from: 0, to: 5, chunk_size: 1, bounds: inclusive }\n",
        );
        confine_sql(&mut cfg);
        let top = cfg.partition.as_mut().and_then(|p| p.probe_mut()).unwrap();
        assert_eq!(top.from_source.config["sandbox"], true);
        let row = cfg.matrix[0]
            .partition
            .as_mut()
            .and_then(|p| p.probe_mut())
            .unwrap();
        assert_eq!(row.from_source.config["sandbox"], true);
        assert!(
            cfg.matrix[1]
                .partition
                .as_mut()
                .unwrap()
                .probe_mut()
                .is_none()
        );
        let dest = cfg.verify.as_ref().unwrap().destination.as_ref().unwrap();
        assert_eq!(dest.config["sandbox"], true);
    }

    #[tokio::test]
    async fn a_probe_reading_a_protected_path_is_refused_before_it_runs() {
        let caller = BodyOrigin::Caller {
            allow_subprocess: false,
        };
        let body = "version: 1\npipeline:\n  source: { type: csv, config: { path: in.csv } }\n  sink: { type: stdout, config: {} }\npartition:\n  kind: integer\n  from: 0\n  chunk_size: 10\n  bounds: inclusive\n  to: { from_source: { type: file, config: { path: /proc/self/environ, format: raw_text } }, value_path: \"$.nope\" }\n";
        let err = load_submission(body, ConfigFormat::Yaml, None, None, caller)
            .await
            .unwrap_err();
        assert!(matches!(err, ServeError::Forbidden(_)), "{err:?}");
    }

    #[tokio::test]
    async fn loading_refuses_local_paths_to_server_files_even_through_a_row_override() {
        let caller = BodyOrigin::Caller {
            allow_subprocess: false,
        };
        let direct = "version: 1\npipeline:\n  source: { type: file, config: { path: /proc/self/environ, format: raw_text } }\n  sink: { type: stdout, config: {} }\n";
        let err = load_submission(direct, ConfigFormat::Yaml, None, None, caller)
            .await
            .unwrap_err();
        assert!(matches!(err, ServeError::Forbidden(_)), "{err:?}");
        assert!(message(&err).contains("/proc/self/environ"));

        let via_row = "version: 1\npipeline:\n  sources:\n    x: { type: csv, config: { path: in.csv } }\n  sink: { type: stdout, config: {} }\nmatrix:\n  - id: r\n    source: { ref: x, config: { path: /proc/self/environ } }\n";
        let err = load_submission(via_row, ConfigFormat::Yaml, None, None, caller)
            .await
            .unwrap_err();
        assert!(matches!(err, ServeError::Forbidden(_)), "{err:?}");
    }

    #[tokio::test]
    async fn a_secret_reference_arriving_through_an_env_value_is_refused() {
        protect_secret_ref("vault", "secret/load-test-env");
        // SAFETY: test-only variable, read by this test alone.
        unsafe { std::env::set_var("LOAD_TEST_INDIRECT", "${vault:secret/load-test-env}") };
        let err = load_submission(
            &csv_body("${env:LOAD_TEST_INDIRECT}"),
            ConfigFormat::Yaml,
            None,
            None,
            BodyOrigin::Trusted,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, ServeError::Forbidden(_)), "{err:?}");
    }

    #[test]
    fn resolution_failures_that_may_pass_on_retry_are_unavailable() {
        use crate::error::CliError;
        let io = CliError::ReadInterpolatedFile {
            path: "/x".into(),
            source: std::io::Error::new(std::io::ErrorKind::NotFound, "gone"),
        };
        assert!(matches!(resolution_error(io), ServeError::Unavailable(_)));
        let fetch = CliError::SecretFetchFailed {
            scheme: "vault".into(),
            reference: "kv/x".into(),
            source: "503".into(),
        };
        assert!(matches!(
            resolution_error(fetch),
            ServeError::Unavailable(_)
        ));
        let auth = CliError::SecretAuthFailed {
            scheme: "vault".into(),
            hint: "token expired".into(),
        };
        assert!(matches!(resolution_error(auth), ServeError::Unavailable(_)));
        let missing = CliError::MissingEnvVar {
            var: "X".into(),
            location: "a".into(),
        };
        assert!(matches!(
            resolution_error(missing),
            ServeError::BadConfig(_)
        ));
        let absent = CliError::SecretNotFound {
            scheme: "vault".into(),
            reference: "kv/x".into(),
        };
        assert!(matches!(resolution_error(absent), ServeError::BadConfig(_)));
    }

    #[cfg(any(feature = "source-singer", feature = "sink-singer"))]
    #[test]
    fn subprocess_kinds_are_found_anywhere() {
        let doc = json!({
            "pipeline": {"sources": {"a": {"type": "singer"}}, "sink": {"type": "jsonl"}},
            "matrix": [{"sink": {"type": "singer"}}],
            "transforms": [{"type": "flatten"}]
        });
        assert_eq!(
            subprocess_kinds_in(&doc).into_iter().collect::<Vec<_>>(),
            vec!["singer".to_string()]
        );
        let caller = BodyOrigin::Caller {
            allow_subprocess: false,
        };
        let allowed = BodyOrigin::Caller {
            allow_subprocess: true,
        };
        assert!(refuse_subprocess(&doc, BodyOrigin::Trusted, true).is_ok());
        assert!(refuse_subprocess(&doc, allowed, false).is_ok());
        assert!(refuse_subprocess(&json!({"type": "csv"}), caller, false).is_ok());
        let err = refuse_subprocess(&doc, caller, false).unwrap_err();
        assert!(message(&err).contains("--allow-subprocess-connectors"));
        let err = refuse_subprocess(&doc, allowed, true).unwrap_err();
        assert!(message(&err).contains("tenant"));
    }

    #[tokio::test]
    async fn loading_refuses_server_secrets_and_unallowed_subprocesses() {
        let err = load_submission(
            &csv_body("${env:FAUCET_SERVE_AUTH_TOKEN}"),
            ConfigFormat::Yaml,
            None,
            None,
            BodyOrigin::Trusted,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, ServeError::Forbidden(_)), "{err:?}");
        let err = load_submission(
            SINGER,
            ConfigFormat::Yaml,
            None,
            None,
            BodyOrigin::Caller {
                allow_subprocess: false,
            },
        )
        .await
        .unwrap_err();
        assert!(message(&err).contains("--allow-subprocess-connectors"));
        for origin in [
            BodyOrigin::Trusted,
            BodyOrigin::Caller {
                allow_subprocess: true,
            },
        ] {
            if let Err(e) = load_submission(SINGER, ConfigFormat::Yaml, None, None, origin).await {
                assert!(
                    !message(&e).contains("--allow-subprocess-connectors"),
                    "{e:?}"
                );
            }
        }
    }

    /// A `${scheme:…}` that is no credential reference is not screened, and a
    /// load-time directive in a local path takes no value from records.
    #[test]
    fn unknown_schemes_pass_and_load_time_paths_are_static() {
        let deny = ReferencePolicy::deny_all();
        screen_references(&serde_json::json!({"a": "${nope:x}"}), &deny).unwrap();
        assert!(!takes_record_value("/data/${env:HOME}/x.jsonl"));
        assert!(!takes_record_value("/data/${now.date}/x.jsonl"));
        assert!(takes_record_value("/data/${parent.id}/x.jsonl"));
    }

    #[test]
    fn references_go_through_the_policy_after_the_credential_overlay() {
        let deny = ReferencePolicy::deny_all();
        let doc = serde_json::json!({"a": "${env:SOME_VAR}", "b": "$${env:ESCAPED}"});
        let err = screen_references(&doc, &deny).unwrap_err().to_string();
        assert!(
            err.contains("${env:SOME_VAR}") && err.contains("--allow-env"),
            "{err}"
        );
        assert!(!err.contains("ESCAPED"), "{err}");
        screen_references(&doc, &ReferencePolicy::open()).unwrap();
        let skip: std::collections::BTreeSet<String> =
            ["${env:SOME_VAR}".to_string()].into_iter().collect();
        screen_references_except(&doc, &deny, &skip).unwrap();
        let err = screen_references(
            &serde_json::json!(["${file:/etc/hosts}", "${vault:a/b}"]),
            &deny,
        )
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("${file:/etc/hosts}") && err.contains("${vault:a/b}"),
            "{err}"
        );
        let overlay = screen_references(
            &serde_json::json!("${env:FAUCET_VAULT_KEY}"),
            &ReferencePolicy::open(),
        )
        .unwrap_err()
        .to_string();
        assert!(overlay.contains("server's own credentials"), "{overlay}");
        refuse_server_secrets(&serde_json::json!("${env:ANY}")).unwrap();
        assert_eq!(
            load_time_tokens(&serde_json::json!({"x": "${env:A} ${row.b}"})),
            ["${env:A}".to_string()].into_iter().collect()
        );
    }

    #[test]
    fn auth_catalog_endpoints_follow_the_url_policy() {
        let mut cfg = PipelineConfig::from_value(serde_json::json!({
            "version": 1,
            "auth": {
                "idp": {"type": "oauth2", "config": {"token_url": "https://idp.example.com/t"}},
                "meta": {"type": "token_endpoint", "config": {"url": "http://169.254.169.254/x"}},
                "plain": {"type": "static"}
            },
            "pipeline": {"source": {"type": "rest", "config": {}}, "sink": {"type": "stdout", "config": {}}}
        }))
        .unwrap();
        let open = ReferencePolicy::open();
        let err = screen_auth_catalog(&cfg, &open).unwrap_err().to_string();
        assert!(
            err.contains("auth.meta.config.url") && !err.contains("idp"),
            "{err}"
        );
        cfg.auth.as_mut().unwrap().remove("meta");
        screen_auth_catalog(&cfg, &open).unwrap();
        let hosts = ReferencePolicy::open().with_url_hosts(["other.example.com"]);
        assert!(screen_auth_catalog(&cfg, &hosts).is_err());
    }

    #[test]
    fn subprocess_screening_follows_origin_and_tenant() {
        let doc = serde_json::json!({"pipeline": {"source": {"type": "singer", "config": {}}}});
        refuse_subprocess(&doc, BodyOrigin::Trusted, true).unwrap();
        let allowed = BodyOrigin::Caller {
            allow_subprocess: true,
        };
        refuse_subprocess(&doc, allowed, false).unwrap();
        let err = refuse_subprocess(&doc, allowed, true)
            .unwrap_err()
            .to_string();
        assert!(err.contains("tenant"), "{err}");
        let err = screen_document(
            &doc,
            BodyOrigin::Caller {
                allow_subprocess: false,
            },
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("--allow-subprocess-connectors"), "{err}");
        screen_document(&serde_json::json!({}), allowed).unwrap();
    }

    #[test]
    fn policy_refusals_name_the_flags() {
        let cfg = PipelineConfig::from_value(serde_json::json!({
            "version": 1,
            "pipeline": {
                "source": {"type": "csv", "config": {"path": "/srv/elsewhere/in.csv"}},
                "sink": {"type": "jsonl", "config": {"path": "/srv/elsewhere/out.jsonl"}},
                "state": {"type": "file", "config": {"path": "/srv/elsewhere/state"}}
            }
        }))
        .unwrap();
        let caller = BodyOrigin::Caller {
            allow_subprocess: false,
        };
        let policy = ReferencePolicy::deny_all().with_path_roots(["/srv/data"]);
        let err = refuse_server_paths_with(&cfg, &[], caller, &policy)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("/srv/elsewhere/state") && err.contains("--allow-path-root"),
            "{err}"
        );
        refuse_server_paths_with(&cfg, &[], BodyOrigin::Trusted, &ReferencePolicy::open()).unwrap();
        let rows = PipelineConfig::from_value(serde_json::json!({
            "version": 1,
            "pipeline": {
                "source": {"type": "csv", "config": {"path": "/srv/data/${row.id}.csv"}},
                "sink": {"type": "jsonl", "config": {"path": "/srv/data/out.jsonl"}}
            }
        }))
        .unwrap();
        let err = refuse_server_paths_with(&rows, &[], caller, &policy)
            .unwrap_err()
            .to_string();
        assert!(err.contains("from records"), "{err}");
    }

    /// A URL host a submitted config takes from records cannot be checked
    /// against any list, so it is refused for a caller even when the host
    /// policy is open; a host fixed in the config, or a token in the path,
    /// still passes.
    #[cfg(all(feature = "source-rest", feature = "sink-stdout"))]
    #[test]
    fn a_caller_url_host_may_not_come_from_records() {
        let cfg = |base: &str| {
            PipelineConfig::from_value(serde_json::json!({
                "version": 1,
                "pipeline": {
                    "source": {"type": "rest", "config": {"base_url": base}},
                    "sink": {"type": "stdout", "config": {}}
                }
            }))
            .unwrap()
        };
        let caller = BodyOrigin::Caller {
            allow_subprocess: false,
        };
        let open = ReferencePolicy::open();
        for bad in ["https://${parent.host}/v1", "${parent.next_url}"] {
            let err = refuse_server_paths_with(&cfg(bad), &[], caller, &open)
                .unwrap_err()
                .to_string();
            assert!(
                err.contains("rest/base_url") && err.contains("may not take a value from records"),
                "{bad}: {err}"
            );
            refuse_server_paths_with(&cfg(bad), &[], BodyOrigin::Trusted, &open).unwrap();
        }
        for ok in [
            "https://api.example.com/items/${parent.id}",
            "https://api.example.com/${now.date}",
        ] {
            refuse_server_paths_with(&cfg(ok), &[], caller, &open).unwrap();
        }
    }
}
