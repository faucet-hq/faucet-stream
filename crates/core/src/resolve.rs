//! Allow-list resolution of the external resources a config names (#881,
//! RFC 0013).
//!
//! A config can reach outside the process in a handful of ways: a local path,
//! a URL, a command to run, an environment variable, a `${file:}` read, or a
//! secret-manager reference. A [`ReferencePolicy`] lists what a config of a
//! given origin may reach, and everything it does not list is refused.
//!
//! Connector config fields that name such a resource carry the
//! [`RESOURCE_ANNOTATION`] (`x-faucet-resource: path | url | command`) in their
//! JSON Schema, so a host can check any connector — including one it has never
//! seen — by walking the schema against the config ([`annotated_resources`]).
//! In a connector crate:
//!
//! ```ignore
//! #[derive(serde::Deserialize, schemars::JsonSchema)]
//! pub struct MyConfig {
//!     #[schemars(extend("x-faucet-resource" = "path"))]
//!     pub cert_file: Option<String>,
//! }
//! ```

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;
use std::path::{Component, Path, PathBuf};

/// The JSON Schema keyword marking a config field that names an external
/// resource. Its value is a [`ResourceKind`] (`"path"`, `"url"`, `"command"`).
pub const RESOURCE_ANNOTATION: &str = "x-faucet-resource";

/// The JSON Schema keyword on a free-form string map (object-store options,
/// driver properties) naming which of its keys hold a resource: an object of
/// key pattern → [`ResourceKind`]. A pattern is matched against the key
/// lower-cased: exact, `PREFIX*`, `*SUFFIX` or `*INFIX*`.
///
/// ```ignore
/// #[schemars(extend("x-faucet-resource-keys" = {"*endpoint*": "url", "*_file": "path"}))]
/// pub storage_options: HashMap<String, String>,
/// ```
pub const RESOURCE_KEYS_ANNOTATION: &str = "x-faucet-resource-keys";

/// Whether map key `key` (any case) matches a [`RESOURCE_KEYS_ANNOTATION`]
/// pattern.
fn key_pattern_matches(pattern: &str, key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    let pattern = pattern.to_ascii_lowercase();
    match (pattern.strip_prefix('*'), pattern.strip_suffix('*')) {
        (Some(rest), _) if rest.ends_with('*') && rest.len() > 1 => {
            key.contains(&rest[..rest.len() - 1])
        }
        (Some(suffix), _) => key.ends_with(suffix),
        (None, Some(prefix)) => key.starts_with(prefix),
        (None, None) => key == pattern,
    }
}

/// The kind a [`RESOURCE_KEYS_ANNOTATION`] on `node` gives map key `key`.
fn keyed_annotation(node: &Value, key: &str) -> Option<ResourceKind> {
    node.get(RESOURCE_KEYS_ANNOTATION)?
        .as_object()?
        .iter()
        .find(|(pattern, _)| key_pattern_matches(pattern, key))
        .and_then(|(_, kind)| kind.as_str().and_then(ResourceKind::parse))
}

/// What kind of external resource a config field names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum ResourceKind {
    /// A local file or directory (or a remote object location when the value
    /// carries a scheme such as `s3://`).
    Path,
    /// A network endpoint or connection URL.
    Url,
    /// A program the connector runs.
    Command,
}

impl ResourceKind {
    /// The annotation value for this kind.
    pub fn as_str(self) -> &'static str {
        match self {
            ResourceKind::Path => "path",
            ResourceKind::Url => "url",
            ResourceKind::Command => "command",
        }
    }

    /// Parse an annotation value.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "path" => Some(ResourceKind::Path),
            "url" => Some(ResourceKind::Url),
            "command" => Some(ResourceKind::Command),
            _ => None,
        }
    }
}

impl std::fmt::Display for ResourceKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One resource a config names, found through a schema annotation.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ResourceRef {
    /// JSON pointer of the value inside the config (`/tls/ca_path`).
    pub pointer: String,
    pub kind: ResourceKind,
    pub value: String,
}

impl ResourceRef {
    pub fn new(pointer: impl Into<String>, kind: ResourceKind, value: impl Into<String>) -> Self {
        Self {
            pointer: pointer.into(),
            kind,
            value: value.into(),
        }
    }
}

/// What a refused reference was.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RefusalKind {
    /// An `${env:}` / `${secret:}` variable.
    Env,
    /// A `${file:}` read.
    File,
    /// A secret-manager reference (`${vault:…}`, …).
    Secret,
    /// A local path (or remote location) a connector reads or writes.
    Path,
    /// A URL a connector connects to.
    Url,
    /// A program a connector runs.
    Command,
}

/// Why a reference was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Refusal {
    pub kind: RefusalKind,
    /// The reference as written (a variable name, a path, a URL, …).
    pub subject: String,
    pub reason: String,
}

impl Refusal {
    pub fn new(kind: RefusalKind, subject: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            kind,
            subject: subject.into(),
            reason: reason.into(),
        }
    }
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.subject, self.reason)
    }
}

impl std::error::Error for Refusal {}

/// The external resources a config of one origin may reach.
///
/// Every dimension is either open (`None` — anything) or an allow-list
/// (`Some(list)`; an empty list allows nothing). [`ReferencePolicy::default`]
/// is [`ReferencePolicy::deny_all`]: a policy only allows what it is told to.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ReferencePolicy {
    path_roots: Option<Vec<PathBuf>>,
    url_schemes: Option<BTreeSet<String>>,
    url_hosts: Option<Vec<String>>,
    env: Option<Vec<String>>,
    secret_schemes: Option<BTreeSet<String>>,
    subprocess: bool,
}

impl Default for ReferencePolicy {
    fn default() -> Self {
        Self::deny_all()
    }
}

/// Hosts a URL may name only when a policy lists them explicitly: link-local
/// and cloud instance-metadata endpoints. `host` is compared in its canonical
/// form ([`canonical_host`]), so `2852039166`, `0xa9fea9fe` or a trailing dot
/// name the same address as `169.254.169.254`.
pub fn is_metadata_host(host: &str) -> bool {
    let canonical = canonical_host(host);
    let h = canonical.trim_start_matches('[').trim_end_matches(']');
    if let Ok(ip) = h.parse::<std::net::IpAddr>() {
        return is_metadata_ip(ip);
    }
    matches!(h, "metadata.google.internal" | "metadata")
}

/// Whether an address is link-local or a cloud instance-metadata endpoint.
pub fn is_metadata_ip(ip: std::net::IpAddr) -> bool {
    let v4_meta =
        |v4: std::net::Ipv4Addr| v4.is_link_local() || v4.octets() == [100, 100, 100, 200];
    match ip {
        std::net::IpAddr::V4(v4) => v4_meta(v4),
        std::net::IpAddr::V6(v6) => {
            (v6.segments()[0] & 0xffc0) == 0xfe80
                || v6 == std::net::Ipv6Addr::new(0xfd00, 0xec2, 0, 0, 0, 0, 0, 0x254)
                || v6.to_ipv4_mapped().is_some_and(v4_meta)
        }
    }
}

/// The addresses `host` resolved to that a connection may use: every one
/// that is not link-local / cloud metadata ([`is_metadata_ip`]), unless
/// `policy` names the host exactly ([`ReferencePolicy::lists_host_exactly`]).
/// An error (the reason) when nothing is left — the name points only at a
/// metadata endpoint. This is the connection-time half of the metadata rule:
/// [`is_metadata_host`] can only judge a host by how it is written.
pub fn screen_resolved<I>(
    host: &str,
    addrs: I,
    policy: Option<&ReferencePolicy>,
) -> Result<Vec<std::net::SocketAddr>, String>
where
    I: IntoIterator<Item = std::net::SocketAddr>,
{
    let all: Vec<std::net::SocketAddr> = addrs.into_iter().collect();
    if policy.is_some_and(|p| p.lists_host_exactly(host)) {
        return Ok(all);
    }
    let kept: Vec<std::net::SocketAddr> = all
        .iter()
        .copied()
        .filter(|a| !is_metadata_ip(a.ip()))
        .collect();
    if kept.is_empty() && !all.is_empty() {
        return Err(format!(
            "host `{}` resolves to a link-local / metadata endpoint",
            canonical_host(host)
        ));
    }
    Ok(kept)
}

/// The canonical, lower-case form of a URL host as a URL parser (and the
/// system resolver) reads it: numeric IPv4 forms (`2852039166`, `0xa9fea9fe`,
/// `0251.0376.0251.0376`, `169.254.43518`) become dotted quads, IPv6 is
/// bracketed and compressed, and a domain loses its trailing dots. A host the
/// parser rejects (a token, a pattern) is returned lower-cased as written.
pub fn canonical_host(host: &str) -> String {
    let raw = host.to_ascii_lowercase();
    if raw.is_empty() || raw.contains(['$', '{', '}', '*', '/', '?', '#', '@']) {
        return raw;
    }
    match url::Url::parse(&format!("http://{raw}/"))
        .ok()
        .and_then(|u| u.host().map(|h| h.to_owned()))
    {
        Some(url::Host::Ipv4(v4)) => v4.to_string(),
        Some(url::Host::Ipv6(v6)) => format!("[{v6}]"),
        Some(url::Host::Domain(d)) => d.trim_end_matches('.').to_string(),
        None => raw.trim_end_matches('.').to_string(),
    }
}

/// Whether `name` matches `pattern`: exact, `*` (anything) or `PREFIX*`.
fn name_matches(pattern: &str, name: &str) -> bool {
    match pattern.strip_suffix('*') {
        Some(prefix) => name.starts_with(prefix),
        None => pattern == name,
    }
}

/// Whether `host` matches a host pattern: exact, `*` or `*.suffix`.
fn host_matches(pattern: &str, host: &str) -> bool {
    let pattern = pattern.to_ascii_lowercase();
    if pattern == "*" {
        return true;
    }
    match pattern.strip_prefix("*.") {
        Some(suffix) => host.len() > suffix.len() + 1 && host.ends_with(&format!(".{suffix}")),
        None => canonical_host(&pattern) == host,
    }
}

/// Absolute, symlink-resolved form of `path`: the longest existing prefix of
/// the path as written is canonicalized by the filesystem (so a `..` after a
/// symlink climbs out of the link's target, as the kernel would), and the
/// components after it are appended. `None` when a component after that
/// prefix is `..` — where it lands depends on directories that do not exist
/// yet, so it cannot be checked.
fn normalize(path: &Path) -> Option<PathBuf> {
    let absolute = if path.is_relative() {
        std::env::current_dir().ok()?.join(path)
    } else {
        path.to_path_buf()
    };
    let components: Vec<Component<'_>> = absolute.components().collect();
    for split in (1..=components.len()).rev() {
        let prefix: PathBuf = components[..split].iter().collect();
        let Ok(mut out) = std::fs::canonicalize(&prefix) else {
            continue;
        };
        for c in &components[split..] {
            match c {
                Component::CurDir => {}
                Component::Normal(name) => out.push(name),
                _ => return None,
            }
        }
        return Some(out);
    }
    None
}

/// The scheme of `value` when it is written `scheme://…` or `scheme:…`, lower
/// case. A Windows drive letter is not a scheme.
fn scheme_of(value: &str) -> Option<String> {
    let colon = value.find(':')?;
    let scheme = &value[..colon];
    let valid = scheme.len() > 1
        && scheme.starts_with(|c: char| c.is_ascii_alphabetic())
        && scheme
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'));
    let rest = value[colon + 1..]
        .split(['/', '?', '#', ','])
        .next()
        .unwrap_or("");
    let port = !rest.is_empty() && rest.chars().all(|c| c.is_ascii_digit());
    (valid && !port).then(|| scheme.to_ascii_lowercase())
}

/// A local path a value names behind a `file:` / `sqlite:` prefix, or the
/// value itself when it carries no scheme. `None` for a remote location.
fn local_part(value: &str) -> Option<&str> {
    match scheme_of(value).as_deref() {
        Some("file") => {
            let rest = &value[5..];
            let Some(after) = rest.strip_prefix("//") else {
                return Some(rest);
            };
            // `file://host/path` (RFC 8089): an empty authority or `localhost`
            // names the local absolute path; any other host is not local.
            let (authority, path) = match after.find('/') {
                Some(i) => (&after[..i], &after[i..]),
                None => (after, ""),
            };
            if authority.is_empty() || authority.eq_ignore_ascii_case("localhost") {
                Some(path)
            } else if authority == "." || authority == ".." {
                Some(after)
            } else {
                None
            }
        }
        Some("sqlite") => {
            let rest = &value[7..];
            let rest = rest.strip_prefix("//").unwrap_or(rest);
            Some(rest.split('?').next().unwrap_or(rest))
        }
        Some(_) => None,
        None => Some(value),
    }
}

/// Whether a scheme-less value is a relative path (`jobs/${id}`) rather than
/// `host[:port][/…]`: its first segment is neither dotted, ported nor
/// `localhost`.
fn is_relative_path(value: &str) -> bool {
    match value.split_once('/') {
        Some((first, _)) => {
            !first.contains(['.', ':', '[']) && !first.eq_ignore_ascii_case("localhost")
        }
        None => false,
    }
}

/// Schemes a WHATWG URL parser (the `url` crate behind reqwest, object_store
/// and most HTTP clients) treats as special: `\\` ends the authority like `/`.
const SPECIAL_SCHEMES: &[&str] = &["http", "https", "ws", "wss", "ftp", "file"];

/// Keys of an ADO.NET / ODBC style connection string that name the server.
const CONN_HOST_KEYS: &[&str] = &[
    "server",
    "data source",
    "datasource",
    "address",
    "addr",
    "network address",
    "host",
    "hostname",
    "hostaddr",
];

/// Keys of an Azure Storage connection string whose value is an endpoint URL.
const CONN_ENDPOINT_KEYS: &[&str] = &[
    "blobendpoint",
    "queueendpoint",
    "tableendpoint",
    "fileendpoint",
    "dfsendpoint",
];

/// The hosts a `key=value;…` connection string (ADO.NET, ODBC, Azure
/// Storage) connects to, or `None` when `value` is not one. A string naming
/// no server connects to the local machine.
fn connection_string_hosts(value: &str) -> Option<Vec<String>> {
    let (first_key, _) = value.split(';').next()?.split_once('=')?;
    let first_key = first_key.trim();
    if first_key.is_empty()
        || !first_key
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, ' ' | '_' | '-'))
    {
        return None;
    }
    let unquote = |v: &str| {
        let v = v.trim();
        v.strip_prefix('{')
            .and_then(|v| v.strip_suffix('}'))
            .or_else(|| v.strip_prefix('"').and_then(|v| v.strip_suffix('"')))
            .or_else(|| v.strip_prefix('\'').and_then(|v| v.strip_suffix('\'')))
            .unwrap_or(v)
            .to_string()
    };
    let libpq_style = !value.contains(';')
        && value.split_whitespace().count() > 1
        && value.split_whitespace().all(|kv| kv.contains('='));
    let separators: &[char] = if libpq_style {
        &[' ', '\t', '\n', '\r']
    } else {
        &[';']
    };
    let pairs: Vec<(String, String)> = value
        .split(separators)
        .filter_map(|kv| kv.split_once('='))
        .map(|(k, v)| (k.trim().to_ascii_lowercase(), unquote(v)))
        .collect();
    let get = |key: &str| {
        pairs
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    };
    let mut hosts = Vec::new();
    for (k, v) in &pairs {
        if CONN_HOST_KEYS.contains(&k.as_str()) {
            let lower = v.to_ascii_lowercase();
            let bare = ["tcp:", "np:", "lpc:", "admin:"]
                .iter()
                .find_map(|p| lower.starts_with(p).then(|| &v[p.len()..]))
                .unwrap_or(v);
            let bare = bare.split(',').next().unwrap_or(bare);
            let bare = bare.split('\\').next().unwrap_or(bare);
            let host = if bare.contains("://") {
                url_hosts(bare)
            } else {
                url_hosts(&format!("tcp://{bare}"))
            };
            hosts.extend(host);
        } else if CONN_ENDPOINT_KEYS.contains(&k.as_str()) {
            hosts.extend(url_hosts(v));
        }
    }
    if let Some(account) = get("accountname")
        && !pairs.iter().any(|(k, _)| k == "blobendpoint")
    {
        let suffix = get("endpointsuffix").unwrap_or("core.windows.net");
        hosts.push(canonical_host(&format!("{account}.blob.{suffix}")));
    }
    if get("usedevelopmentstorage").is_some_and(|v| v.eq_ignore_ascii_case("true")) {
        hosts.push("127.0.0.1".into());
    }
    if hosts.is_empty() {
        hosts.push("localhost".into());
    }
    hosts.retain(|h| !h.is_empty());
    Some(hosts)
}

/// The hosts a URL's authority names (several for `h1,h2` connection
/// strings and broker lists, and for the servers a `key=value;…` connection
/// string names), in canonical form ([`canonical_host`]), without userinfo or
/// port. A URL with a special scheme (`http`, `https`, …) is read by the same
/// parser HTTP clients use, so `\\` ends its authority as `/` does. A host
/// taken from a `${…}` token is returned lower-cased as written.
pub fn url_hosts(value: &str) -> Vec<String> {
    let value = value.trim();
    if !value.contains("://")
        && let Some(hosts) = connection_string_hosts(value)
    {
        return hosts;
    }
    let scheme = scheme_of(value);
    let special = scheme
        .as_deref()
        .is_none_or(|s| SPECIAL_SCHEMES.contains(&s));
    if scheme.is_some()
        && special
        && !value.contains("${")
        && let Ok(parsed) = url::Url::parse(value)
    {
        return parsed
            .host_str()
            .map(canonical_host)
            .into_iter()
            .filter(|h| !h.is_empty())
            .collect();
    }
    let after = match value.find("://") {
        Some(i) => &value[i + 3..],
        None => match &scheme {
            Some(s) => &value[s.len() + 1..],
            None => value,
        },
    };
    let authority = if special {
        after.split(['/', '?', '#', '\\']).next()
    } else {
        after.split(['/', '?', '#']).next()
    }
    .unwrap_or("");
    let hosts = match authority.rfind('@') {
        Some(at) => &authority[at + 1..],
        None => authority,
    };
    let hosts = hosts.split(';').next().unwrap_or(hosts);
    hosts
        .split(',')
        .map(|h| {
            let h = if h.starts_with('[') {
                h.find(']').map_or(h, |end| &h[..=end])
            } else {
                h.rsplit_once(':')
                    .filter(|(_, port)| port.chars().all(|c| c.is_ascii_digit()))
                    .map_or(h, |(host, _)| host)
            };
            canonical_host(h)
        })
        .filter(|h| !h.is_empty())
        .collect()
}

impl ReferencePolicy {
    /// Everything allowed: any path, URL, variable and secret, and programs.
    pub fn open() -> Self {
        Self {
            path_roots: None,
            url_schemes: None,
            url_hosts: None,
            env: None,
            secret_schemes: None,
            subprocess: true,
        }
    }

    /// Nothing allowed.
    pub fn deny_all() -> Self {
        Self {
            path_roots: Some(Vec::new()),
            url_schemes: Some(BTreeSet::new()),
            url_hosts: Some(Vec::new()),
            env: Some(Vec::new()),
            secret_schemes: Some(BTreeSet::new()),
            subprocess: false,
        }
    }

    /// Allow local paths only under these directories.
    pub fn with_path_roots<I, P>(mut self, roots: I) -> Self
    where
        I: IntoIterator<Item = P>,
        P: Into<PathBuf>,
    {
        self.path_roots = Some(roots.into_iter().map(Into::into).collect());
        self
    }

    /// Allow any local path.
    pub fn any_path(mut self) -> Self {
        self.path_roots = None;
        self
    }

    /// Allow URLs only with these schemes (`https`, `postgres`, …).
    pub fn with_url_schemes<I, S>(mut self, schemes: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.url_schemes = Some(
            schemes
                .into_iter()
                .map(|s| s.as_ref().to_ascii_lowercase())
                .collect(),
        );
        self
    }

    /// Allow URLs of any scheme.
    pub fn any_url_scheme(mut self) -> Self {
        self.url_schemes = None;
        self
    }

    /// Allow URLs only to these hosts (`api.example.com`, `*.example.com`,
    /// `10.0.0.5`). A link-local or metadata host is reachable only when listed.
    pub fn with_url_hosts<I, S>(mut self, hosts: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.url_hosts = Some(hosts.into_iter().map(Into::into).collect());
        self
    }

    /// Allow URLs to any host except link-local and metadata endpoints.
    pub fn any_url_host(mut self) -> Self {
        self.url_hosts = None;
        self
    }

    /// Allow only these environment variables (`NAME` or `PREFIX*`).
    pub fn with_env<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.env = Some(names.into_iter().map(Into::into).collect());
        self
    }

    /// Allow any environment variable.
    pub fn any_env(mut self) -> Self {
        self.env = None;
        self
    }

    /// Allow only these secret-manager schemes (`vault`, `aws-sm`, …).
    pub fn with_secret_schemes<I, S>(mut self, schemes: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.secret_schemes = Some(
            schemes
                .into_iter()
                .map(|s| s.as_ref().to_ascii_lowercase())
                .collect(),
        );
        self
    }

    /// Allow any secret-manager scheme.
    pub fn any_secret_scheme(mut self) -> Self {
        self.secret_schemes = None;
        self
    }

    /// Whether connectors that run a program are allowed.
    pub fn with_subprocess(mut self, allowed: bool) -> Self {
        self.subprocess = allowed;
        self
    }

    /// The allowed path roots, `None` when any path is allowed.
    pub fn path_roots(&self) -> Option<&[PathBuf]> {
        self.path_roots.as_deref()
    }

    /// The allowed URL schemes, `None` when any scheme is allowed.
    pub fn url_schemes(&self) -> Option<&BTreeSet<String>> {
        self.url_schemes.as_ref()
    }

    /// The allowed URL hosts, `None` when any host is allowed.
    pub fn url_host_patterns(&self) -> Option<&[String]> {
        self.url_hosts.as_deref()
    }

    /// The allowed environment variables, `None` when any is allowed.
    pub fn env_patterns(&self) -> Option<&[String]> {
        self.env.as_deref()
    }

    /// The allowed secret-manager schemes, `None` when any is allowed.
    pub fn secret_schemes(&self) -> Option<&BTreeSet<String>> {
        self.secret_schemes.as_ref()
    }

    /// Whether connectors that run a program are allowed.
    pub fn allows_subprocess(&self) -> bool {
        self.subprocess
    }

    /// Check an `${env:NAME}` / `${secret:NAME}` reference.
    pub fn check_env(&self, name: &str) -> Result<(), Refusal> {
        match &self.env {
            None => Ok(()),
            Some(list) if list.iter().any(|p| name_matches(p, name)) => Ok(()),
            Some(_) => Err(Refusal::new(
                RefusalKind::Env,
                name,
                "environment variable not in the allow-list",
            )),
        }
    }

    /// Check a `${file:PATH}` reference.
    pub fn check_file(&self, path: &str) -> Result<(), Refusal> {
        self.check_local(path)
            .map_err(|reason| Refusal::new(RefusalKind::File, path, reason))
    }

    /// Check a secret-manager reference (`${vault:…}`, …).
    pub fn check_secret(&self, scheme: &str, reference: &str) -> Result<(), Refusal> {
        match &self.secret_schemes {
            None => Ok(()),
            Some(set) if set.contains(&scheme.to_ascii_lowercase()) => Ok(()),
            Some(_) => Err(Refusal::new(
                RefusalKind::Secret,
                format!("{scheme}:{reference}"),
                "secret-manager scheme not in the allow-list",
            )),
        }
    }

    /// Check a path a connector reads or writes. A value with a remote scheme
    /// (`s3://…`) is checked as a URL.
    pub fn check_path(&self, value: &str) -> Result<(), Refusal> {
        match local_part(value) {
            Some(local) => self
                .check_local(local)
                .map_err(|reason| Refusal::new(RefusalKind::Path, value, reason)),
            None => self.check_url(value),
        }
    }

    fn check_local(&self, path: &str) -> Result<(), String> {
        let Some(roots) = &self.path_roots else {
            return Ok(());
        };
        if path.is_empty() || path == ":memory:" || path == "/dev/null" {
            return Ok(());
        }
        let dynamic = path.find(['*', '?', '[', '{']).unwrap_or(path.len());
        let (base, rest) = match path[..dynamic].rfind('/') {
            Some(slash) if dynamic < path.len() => (&path[..slash.max(1)], &path[slash + 1..]),
            None if dynamic < path.len() => (".", path),
            _ => (path, ""),
        };
        if rest.split('/').any(|seg| seg == "..") {
            return Err("a pattern or token segment may not contain `..`".into());
        }
        let Some(target) = normalize(Path::new(base)) else {
            return Err("a `..` below a directory that does not exist cannot be checked".into());
        };
        if roots
            .iter()
            .filter_map(|r| normalize(r))
            .any(|root| target.starts_with(root))
        {
            return Ok(());
        }
        Err(if roots.is_empty() {
            "local paths are not allowed".into()
        } else {
            format!(
                "outside the allowed path roots ({})",
                roots
                    .iter()
                    .map(|r| r.display().to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })
    }

    /// Check a URL a connector connects to. `file:` / `sqlite:` URLs are
    /// checked as paths, a scheme-less `host[:port]` as a host, and a relative
    /// reference (`/jobs`, `./x`) names no host of its own and passes.
    pub fn check_url(&self, value: &str) -> Result<(), Refusal> {
        let refuse = |reason: String| Refusal::new(RefusalKind::Url, value, reason);
        let scheme = scheme_of(value).filter(|_| !value.starts_with('/'));
        match scheme.as_deref() {
            Some("file") | Some("sqlite") => {
                return match local_part(value) {
                    Some(local) => self
                        .check_local(local)
                        .map_err(|reason| Refusal::new(RefusalKind::Path, value, reason)),
                    None if self.path_roots.is_none() => Ok(()),
                    None => Err(refuse(
                        "a `file:` URL that names a remote host is not a local path".into(),
                    )),
                };
            }
            None if value.starts_with(['/', '.', '?', '#']) => return Ok(()),
            None if is_relative_path(value) => return Ok(()),
            _ => {}
        }
        if let (Some(scheme), Some(allowed)) = (&scheme, &self.url_schemes)
            && !allowed.contains(scheme)
        {
            return Err(refuse(format!(
                "URL scheme `{scheme}` not in the allow-list"
            )));
        }
        for host in url_hosts(value) {
            if host.contains("${") || host.contains('*') {
                if self.url_hosts.is_some() {
                    return Err(refuse(
                        "a host taken from a token cannot be checked against the host allow-list"
                            .into(),
                    ));
                }
                continue;
            }
            match &self.url_hosts {
                Some(list) if list.iter().any(|p| host_matches(p, &host)) => {}
                Some(_) => return Err(refuse(format!("host `{host}` not in the allow-list"))),
                None if is_metadata_host(&host) => {
                    return Err(refuse(format!(
                        "host `{host}` is a link-local / metadata endpoint"
                    )));
                }
                None => {}
            }
        }
        Ok(())
    }

    /// Whether the host allow-list names `host` itself — an exact entry, not
    /// `*` or a `*.suffix` pattern. Only such an entry lets a name reach a
    /// link-local / metadata address ([`screen_resolved`]).
    pub fn lists_host_exactly(&self, host: &str) -> bool {
        let host = canonical_host(host);
        self.url_hosts.as_ref().is_some_and(|list| {
            list.iter()
                .any(|p| !p.contains('*') && canonical_host(p) == host)
        })
    }

    /// [`check_url`](Self::check_url), then resolve every host the value names
    /// that is not an IP literal and refuse one whose addresses are all
    /// link-local / metadata endpoints ([`screen_resolved`]). For a client
    /// that does not go through [`crate::http::Client`] (whose resolver
    /// applies the same rule at connection time); a name that fails to resolve
    /// passes, since the connection fails too. Resolution blocks — call it off
    /// the async runtime's worker threads, or accept a short stall. A name
    /// re-pointed between this check and the client's own lookup is not
    /// caught here.
    pub fn check_url_resolved(&self, value: &str) -> Result<(), Refusal> {
        self.check_url(value)?;
        if scheme_of(value).is_some_and(|s| s == "file" || s == "sqlite") {
            return Ok(());
        }
        for host in url_hosts(value) {
            let bare = host.trim_start_matches('[').trim_end_matches(']');
            if host.contains("${") || host.contains('*') || bare.parse::<std::net::IpAddr>().is_ok()
            {
                continue;
            }
            let Ok(addrs) = std::net::ToSocketAddrs::to_socket_addrs(&(bare, 0)) else {
                continue;
            };
            screen_resolved(&host, addrs, Some(self))
                .map_err(|reason| Refusal::new(RefusalKind::Url, value, reason))?;
        }
        Ok(())
    }

    /// [`check`](Self::check), resolving URL hosts ([`check_url_resolved`](Self::check_url_resolved)).
    pub fn check_resolved(&self, r: &ResourceRef) -> Result<(), Refusal> {
        match r.kind {
            ResourceKind::Url => self.check_url_resolved(&r.value),
            ResourceKind::Path if local_part(&r.value).is_none() => {
                self.check_url_resolved(&r.value)
            }
            _ => self.check(r),
        }
    }

    /// Check a program a connector runs.
    pub fn check_command(&self, value: &str) -> Result<(), Refusal> {
        if self.subprocess {
            Ok(())
        } else {
            Err(Refusal::new(
                RefusalKind::Command,
                value,
                "connectors that run a program are not allowed",
            ))
        }
    }

    /// Check one annotated resource.
    pub fn check(&self, r: &ResourceRef) -> Result<(), Refusal> {
        match r.kind {
            ResourceKind::Path => self.check_path(&r.value),
            ResourceKind::Url => self.check_url(&r.value),
            ResourceKind::Command => self.check_command(&r.value),
        }
    }
}

const MAX_DEPTH: usize = 64;

fn deref<'a>(root: &'a Value, node: &'a Value) -> &'a Value {
    let mut cur = node;
    for _ in 0..MAX_DEPTH {
        let Some(r) = cur.get("$ref").and_then(Value::as_str) else {
            return cur;
        };
        let Some(pointer) = r.strip_prefix('#') else {
            return cur;
        };
        match root.pointer(pointer) {
            Some(next) => cur = next,
            None => return cur,
        }
    }
    cur
}

fn annotation_of(node: &Value) -> Option<ResourceKind> {
    node.get(RESOURCE_ANNOTATION)
        .and_then(Value::as_str)
        .and_then(ResourceKind::parse)
}

/// Every schema node `node` stands for: itself (dereferenced) and the
/// branches of its `oneOf` / `anyOf` / `allOf`, recursively.
fn alternatives<'a>(root: &'a Value, node: &'a Value, out: &mut Vec<&'a Value>, depth: usize) {
    if depth > MAX_DEPTH {
        return;
    }
    out.push(node);
    let node = deref(root, node);
    if !std::ptr::eq(node, *out.last().expect("pushed")) {
        out.push(node);
    }
    for key in ["oneOf", "anyOf", "allOf"] {
        if let Some(Value::Array(branches)) = node.get(key) {
            for b in branches {
                alternatives(root, b, out, depth + 1);
            }
        }
    }
}

fn escape_pointer(key: &str) -> String {
    key.replace('~', "~0").replace('/', "~1")
}

fn visit(
    root: &Value,
    node: &Value,
    value: &Value,
    pointer: &str,
    out: &mut Vec<ResourceRef>,
    depth: usize,
) {
    if depth > MAX_DEPTH {
        return;
    }
    let mut alts = Vec::new();
    alternatives(root, node, &mut alts, 0);
    let kind = alts.iter().find_map(|n| annotation_of(n));
    match value {
        Value::String(s) => {
            if let Some(kind) = kind {
                out.push(ResourceRef::new(pointer, kind, s.clone()));
            }
        }
        Value::Array(items) => {
            for (i, item) in items.iter().enumerate() {
                let p = format!("{pointer}/{i}");
                if let (Some(kind), Value::String(s)) = (kind, item) {
                    out.push(ResourceRef::new(p.clone(), kind, s.clone()));
                }
                for alt in &alts {
                    if let Some(items_schema) = alt.get("items").filter(|v| v.is_object()) {
                        visit(root, items_schema, item, &p, out, depth + 1);
                    }
                }
            }
        }
        Value::Object(map) => {
            for (k, child) in map {
                let p = format!("{pointer}/{}", escape_pointer(k));
                if let (Some(kind), Value::String(s)) =
                    (alts.iter().find_map(|n| keyed_annotation(n, k)), child)
                {
                    out.push(ResourceRef::new(p.clone(), kind, s.clone()));
                }
                for alt in &alts {
                    let declared = alt.get("properties").and_then(|props| props.get(k));
                    if let Some(s) = declared {
                        visit(root, s, child, &p, out, depth + 1);
                    } else if let Some(s) =
                        alt.get("additionalProperties").filter(|v| v.is_object())
                    {
                        visit(root, s, child, &p, out, depth + 1);
                    }
                }
            }
        }
        _ => {}
    }
}

/// The resources `config` names through fields `schema` annotates with
/// [`RESOURCE_ANNOTATION`]. `schema` is a connector's config schema (with its
/// own `$defs`); every `oneOf` / `anyOf` / `allOf` branch is considered, so a
/// tagged-enum field is found whichever variant the config picked.
pub fn annotated_resources(schema: &Value, config: &Value) -> Vec<ResourceRef> {
    let mut out = Vec::new();
    visit(schema, schema, config, "", &mut out, 0);
    let mut seen = BTreeSet::new();
    out.retain(|r| seen.insert((r.pointer.clone(), r.kind, r.value.clone())));
    out
}

fn collect_fields(
    root: &Value,
    node: &Value,
    path: &str,
    out: &mut BTreeSet<(String, ResourceKind)>,
    depth: usize,
) {
    if depth > 12 {
        return;
    }
    let mut alts = Vec::new();
    alternatives(root, node, &mut alts, 0);
    if let Some(kind) = alts.iter().find_map(|n| annotation_of(n))
        && !path.is_empty()
    {
        out.insert((path.to_string(), kind));
    }
    for alt in alts {
        if let Some(Value::Object(keys)) = alt.get(RESOURCE_KEYS_ANNOTATION) {
            for (pattern, kind) in keys {
                if let Some(kind) = kind.as_str().and_then(ResourceKind::parse) {
                    out.insert((format!("{path}{{{pattern}}}"), kind));
                }
            }
        }
        if let Some(Value::Object(props)) = alt.get("properties") {
            for (k, s) in props {
                let p = if path.is_empty() {
                    k.clone()
                } else {
                    format!("{path}.{k}")
                };
                collect_fields(root, s, &p, out, depth + 1);
            }
        }
        if let Some(items) = alt.get("items").filter(|v| v.is_object()) {
            collect_fields(root, items, &format!("{path}[]"), out, depth + 1);
        }
        if let Some(extra) = alt.get("additionalProperties").filter(|v| v.is_object()) {
            collect_fields(root, extra, &format!("{path}{{}}"), out, depth + 1);
        }
    }
}

/// Every annotated field a schema declares, as dotted paths (`tls.ca_path`;
/// `[]` marks an array element, `{}` a map value) with their kind.
pub fn annotated_fields(schema: &Value) -> Vec<(String, ResourceKind)> {
    let mut out = BTreeSet::new();
    collect_fields(schema, schema, "", &mut out, 0);
    out.into_iter().collect()
}

/// Whether a schema declares at least one field of `kind`.
pub fn declares(schema: &Value, kind: ResourceKind) -> bool {
    annotated_fields(schema).iter().any(|(_, k)| *k == kind)
}

/// The JSON Schema keyword (value `true`) on a top-level boolean config field
/// that, set to `true`, confines a connector whose own input can otherwise
/// reach host files or the network — a SQL query that can `ATTACH` or
/// `read_csv` — to the resources its configuration declares. A host loading a
/// config it does not trust forces every such field on:
///
/// ```ignore
/// #[schemars(extend("x-faucet-sandbox" = true))]
/// pub sandbox: bool,
/// ```
pub const SANDBOX_ANNOTATION: &str = "x-faucet-sandbox";

/// The top-level config fields `schema` marks with [`SANDBOX_ANNOTATION`],
/// across every `oneOf` / `anyOf` / `allOf` branch of the root.
pub fn sandbox_fields(schema: &Value) -> Vec<String> {
    let mut alts = Vec::new();
    alternatives(schema, schema, &mut alts, 0);
    let mut out = BTreeSet::new();
    for alt in alts {
        let Some(Value::Object(props)) = alt.get("properties") else {
            continue;
        };
        for (name, field) in props {
            let mut field_alts = Vec::new();
            alternatives(schema, field, &mut field_alts, 0);
            if field_alts
                .iter()
                .any(|n| n.get(SANDBOX_ANNOTATION) == Some(&Value::Bool(true)))
            {
                out.insert(name.clone());
            }
        }
    }
    out.into_iter().collect()
}

#[cfg(test)]
mod tests {
    #[test]
    fn local_part_reads_bare_and_relative_file_authorities() {
        assert_eq!(local_part("file://localhost"), Some(""));
        assert_eq!(local_part("file://"), Some(""));
        assert_eq!(local_part("file://./data/x"), Some("./data/x"));
        assert_eq!(local_part("file://../x"), Some("../x"));
        assert_eq!(local_part("file://host/x"), None);
    }
    use super::*;
    use serde_json::json;

    /// A schema that refers to itself through `oneOf`, and a config nested
    /// deeper than the walk follows, end the walk rather than recursing on.
    #[test]
    fn self_referential_schemas_and_deep_configs_stop_at_the_depth_cap() {
        let s = json!({"oneOf": [{"$ref": "#"}], "type": "object"});
        assert!(annotated_fields(&s).is_empty());
        let nested = json!({
            "type": "object",
            "properties": {"next": {"$ref": "#"}, "path": {"type": "string", "x-faucet-resource": "path"}}
        });
        let mut config = json!({"path": "/deepest"});
        for _ in 0..MAX_DEPTH + 2 {
            config = json!({"next": config});
        }
        let found = annotated_resources(&nested, &config);
        assert!(found.is_empty(), "{found:?}");
        let shallow = annotated_resources(&nested, &json!({"next": {"path": "/a"}}));
        assert_eq!(shallow.len(), 1);
    }

    #[test]
    fn a_scheme_without_slashes_names_its_host() {
        assert_eq!(url_hosts("mysql:db.internal:3306/app"), vec!["db.internal"]);
    }

    #[test]
    fn a_branch_without_properties_declares_no_sandbox_field() {
        assert!(sandbox_fields(&json!({"oneOf": [{"type": "string"}]})).is_empty());
    }

    #[test]
    fn sandbox_fields_are_read_from_the_root_and_its_branches() {
        assert!(sandbox_fields(&schema()).is_empty());
        let s = json!({
            "type": "object",
            "properties": {
                "sandbox": {"type": "boolean", "x-faucet-sandbox": true},
                "confine": {"$ref": "#/$defs/Flag"},
                "off": {"type": "boolean", "x-faucet-sandbox": false},
                "query": {"type": "string"}
            },
            "allOf": [{"properties": {"lock": {"anyOf": [{"type": "boolean", "x-faucet-sandbox": true}]}}}],
            "$defs": {"Flag": {"type": "boolean", "x-faucet-sandbox": true}}
        });
        assert_eq!(sandbox_fields(&s), vec!["confine", "lock", "sandbox"]);
    }

    fn schema() -> Value {
        json!({
            "type": "object",
            "properties": {
                "path": {"type": "string", "x-faucet-resource": "path"},
                "name": {"type": "string"},
                "files": {"type": "array", "items": {"type": "string"}, "x-faucet-resource": "path"},
                "tls": {"anyOf": [{"$ref": "#/$defs/Tls"}, {"type": "null"}]},
                "auth": {"oneOf": [
                    {"type": "object", "properties": {"type": {"const": "key"}, "config": {"$ref": "#/$defs/Key"}}},
                    {"type": "object", "properties": {"type": {"const": "none"}}}
                ]},
                "endpoints": {"type": "object", "additionalProperties": {"type": "string", "x-faucet-resource": "url"}},
                "cmd": {"$ref": "#/$defs/Cmd", "x-faucet-resource": "command"},
                "hops": {"type": "array", "items": {"$ref": "#/$defs/Hop"}}
            },
            "$defs": {
                "Tls": {"type": "object", "properties": {"ca_path": {"type": ["string", "null"], "x-faucet-resource": "path"}}},
                "Key": {"type": "object", "properties": {"key_file": {"type": "string", "x-faucet-resource": "path"}}},
                "Cmd": {"type": "string"},
                "Hop": {"type": "object", "properties": {"url": {"type": "string", "x-faucet-resource": "url"}}}
            }
        })
    }

    #[test]
    fn finds_every_annotated_value() {
        let config = json!({
            "path": "/data/in.csv",
            "name": "/etc/passwd",
            "files": ["/a", "/b"],
            "tls": {"ca_path": "/certs/ca.pem"},
            "auth": {"type": "key", "config": {"key_file": "/keys/k"}},
            "endpoints": {"a/b": "https://x.example"},
            "cmd": "/bin/tap",
            "hops": [{"url": "http://h1"}, {"url": 5}],
            "unknown": {"path": "/ignored"}
        });
        let found = annotated_resources(&schema(), &config);
        let mut got: Vec<(&str, ResourceKind, &str)> = found
            .iter()
            .map(|r| (r.pointer.as_str(), r.kind, r.value.as_str()))
            .collect();
        got.sort();
        assert_eq!(
            got,
            vec![
                ("/auth/config/key_file", ResourceKind::Path, "/keys/k"),
                ("/cmd", ResourceKind::Command, "/bin/tap"),
                ("/endpoints/a~1b", ResourceKind::Url, "https://x.example"),
                ("/files/0", ResourceKind::Path, "/a"),
                ("/files/1", ResourceKind::Path, "/b"),
                ("/hops/0/url", ResourceKind::Url, "http://h1"),
                ("/path", ResourceKind::Path, "/data/in.csv"),
                ("/tls/ca_path", ResourceKind::Path, "/certs/ca.pem"),
            ]
        );
    }

    #[test]
    fn lists_annotated_fields() {
        let fields = annotated_fields(&schema());
        let names: Vec<String> = fields.iter().map(|(p, k)| format!("{p}:{k}")).collect();
        assert_eq!(
            names,
            vec![
                "auth.config.key_file:path",
                "cmd:command",
                "endpoints{}:url",
                "files:path",
                "hops[].url:url",
                "path:path",
                "tls.ca_path:path",
            ]
        );
        assert!(declares(&schema(), ResourceKind::Command));
        assert!(!declares(&json!({"type": "object"}), ResourceKind::Path));
    }

    #[test]
    fn a_recursive_schema_terminates() {
        let s = json!({"$ref": "#/$defs/A", "$defs": {"A": {"$ref": "#/$defs/A"}}});
        assert!(annotated_resources(&s, &json!({"a": 1})).is_empty());
        let s = json!({"$defs": {"N": {"type": "object", "properties": {"n": {"$ref": "#/$defs/N"}, "p": {"type": "string", "x-faucet-resource": "path"}}}}, "$ref": "#/$defs/N"});
        let deepest = &annotated_fields(&s)[0].0;
        assert!(
            deepest.starts_with("n.n.") && deepest.ends_with(".p"),
            "{deepest}"
        );
        let deep = json!({"n": {"n": {"p": "/x"}}});
        assert_eq!(annotated_resources(&s, &deep)[0].pointer, "/n/n/p");
        assert!(annotated_resources(&json!({"$ref": "http://x"}), &json!("v")).is_empty());
        assert!(annotated_resources(&json!({"$ref": "#/missing"}), &json!("v")).is_empty());
    }

    #[test]
    fn the_derive_attribute_produces_the_annotation() {
        #[derive(schemars::JsonSchema)]
        #[allow(dead_code)]
        struct Tls {
            #[schemars(extend("x-faucet-resource" = "path"))]
            ca: Option<std::path::PathBuf>,
        }
        #[derive(schemars::JsonSchema)]
        #[allow(dead_code)]
        struct Cfg {
            #[schemars(extend("x-faucet-resource" = "url"))]
            url: String,
            #[schemars(extend("x-faucet-resource" = "command"))]
            program: Vec<String>,
            tls: Option<Tls>,
        }
        let schema = serde_json::to_value(schemars::schema_for!(Cfg)).unwrap();
        let fields = annotated_fields(&schema);
        assert_eq!(
            fields,
            vec![
                ("program".to_string(), ResourceKind::Command),
                ("tls.ca".to_string(), ResourceKind::Path),
                ("url".to_string(), ResourceKind::Url),
            ]
        );
        let found = annotated_resources(
            &schema,
            &json!({"url": "https://h", "program": ["tap"], "tls": {"ca": "/c"}}),
        );
        assert_eq!(found.len(), 3);
    }

    #[test]
    fn kinds_round_trip() {
        for k in [ResourceKind::Path, ResourceKind::Url, ResourceKind::Command] {
            assert_eq!(ResourceKind::parse(k.as_str()), Some(k));
            assert_eq!(serde_json::to_value(k).unwrap(), json!(k.as_str()));
        }
        assert_eq!(ResourceKind::parse("other"), None);
    }

    #[test]
    fn the_default_denies_everything() {
        let p = ReferencePolicy::default();
        assert_eq!(p, ReferencePolicy::deny_all());
        assert!(p.check_env("HOME").is_err());
        assert!(p.check_file("/tmp/x").is_err());
        assert!(p.check_secret("vault", "a/b").is_err());
        assert!(p.check_path("/tmp/x").is_err());
        assert!(p.check_url("https://example.com").is_err());
        assert!(p.check_command("/bin/sh").is_err());
        assert!(!p.allows_subprocess());
        let e = p.check_path("/tmp/x").unwrap_err();
        assert_eq!(e.kind, RefusalKind::Path);
        assert!(e.to_string().contains("not allowed"), "{e}");
    }

    #[test]
    fn the_open_policy_allows_everything_but_metadata_hosts() {
        let p = ReferencePolicy::open();
        assert!(p.check_env("HOME").is_ok());
        assert!(p.check_file("/etc/passwd").is_ok());
        assert!(p.check_secret("vault", "a").is_ok());
        assert!(p.check_path("/etc/passwd").is_ok());
        assert!(p.check_url("https://example.com").is_ok());
        assert!(p.check_command("/bin/sh").is_ok());
        for bad in [
            "http://169.254.169.254/latest",
            "http://[fe80::1]/",
            "http://[fd00:ec2::254]/",
            "http://[::ffff:169.254.169.254]/",
            "http://metadata.google.internal/x",
            "http://100.100.100.200/",
        ] {
            assert!(p.check_url(bad).is_err(), "{bad}");
        }
        assert!(p.check_url("http://[::1]:80/").is_ok());
        let listed = ReferencePolicy::open().with_url_hosts(["169.254.169.254"]);
        assert!(listed.check_url("http://169.254.169.254/").is_ok());
    }

    #[test]
    fn env_and_secret_allow_lists() {
        let p = ReferencePolicy::deny_all()
            .with_env(["APP_*", "TOKEN"])
            .with_secret_schemes(["Vault"]);
        assert!(p.check_env("APP_KEY").is_ok());
        assert!(p.check_env("TOKEN").is_ok());
        assert!(p.check_env("TOKEN2").is_err());
        assert!(p.check_secret("vault", "x").is_ok());
        assert_eq!(
            p.check_secret("aws-sm", "x").unwrap_err().subject,
            "aws-sm:x"
        );
        assert_eq!(p.env_patterns().unwrap().len(), 2);
        assert!(p.secret_schemes().unwrap().contains("vault"));
        assert!(ReferencePolicy::deny_all().any_env().check_env("X").is_ok());
        assert!(
            ReferencePolicy::deny_all()
                .any_secret_scheme()
                .check_secret("x", "y")
                .is_ok()
        );
    }

    #[test]
    fn paths_must_sit_under_a_root() {
        let dir = std::env::temp_dir().join(format!("faucet-resolve-{}", std::process::id()));
        std::fs::create_dir_all(dir.join("sub")).unwrap();
        let p = ReferencePolicy::deny_all().with_path_roots([&dir]);
        assert_eq!(p.path_roots().unwrap().len(), 1);
        let d = dir.display();
        for ok in [
            format!("{d}/in.csv"),
            format!("{d}/sub/new/out.jsonl"),
            format!("{d}/*.csv"),
            format!("{d}/sub/part-{{part}}.jsonl"),
            format!("{d}/x/${{now.date}}.csv"),
            format!("file://{d}/a"),
            format!("file://localhost{d}/a"),
            format!("sqlite:{d}/db.sqlite?mode=rwc"),
            format!("sqlite://{d}/db.sqlite"),
            ":memory:".to_string(),
            "/dev/null".to_string(),
            String::new(),
        ] {
            assert!(p.check_path(&ok).is_ok(), "{ok}");
        }
        for bad in [
            "/etc/passwd".to_string(),
            "file://localhost/etc/passwd".to_string(),
            "file://LOCALHOST/etc/passwd".to_string(),
            "file://elsewhere/etc/passwd".to_string(),
            format!("{d}/../escape"),
            format!("{d}/*/../../etc/*"),
            format!("{d}x/sibling"),
            "relative/elsewhere".to_string(),
        ] {
            assert!(p.check_path(&bad).is_err(), "{bad}");
        }
        assert!(p.check_file(&format!("{d}/a")).is_ok());
        assert_eq!(
            p.check_file("/etc/hosts").unwrap_err().kind,
            RefusalKind::File
        );
        #[cfg(unix)]
        {
            let link = dir.join("link");
            let _ = std::fs::remove_file(&link);
            std::os::unix::fs::symlink("/etc", &link).unwrap();
            assert!(p.check_path(&format!("{d}/link/passwd")).is_err());
        }
        assert!(p.check_path(&format!("{d}/sub/../in.csv")).is_ok());
        assert!(p.check_path(&format!("{d}/missing/../in.csv")).is_err());
        let cwd = ReferencePolicy::deny_all().with_path_roots(["."]);
        assert!(cwd.check_path("data/in.csv").is_ok());
        assert!(cwd.check_path("*.csv").is_ok());
        assert!(
            ReferencePolicy::deny_all()
                .any_path()
                .check_path("/x")
                .is_ok()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_remote_location_in_a_path_field_is_a_url() {
        let p = ReferencePolicy::open().with_url_hosts(["bucket"]);
        assert!(p.check_path("s3://bucket/key").is_ok());
        assert_eq!(
            p.check_path("s3://other/key").unwrap_err().kind,
            RefusalKind::Url
        );
        assert!(p.check_path("C:/data/x").is_ok());
    }

    #[test]
    fn urls_are_checked_by_scheme_and_host() {
        let p = ReferencePolicy::open()
            .with_url_schemes(["https", "postgres"])
            .with_url_hosts(["api.example.com", "*.internal", "db"]);
        assert!(p.url_schemes().unwrap().contains("https"));
        assert_eq!(p.url_host_patterns().unwrap().len(), 3);
        for ok in [
            "https://api.example.com/v1",
            "https://user:p@ss@api.example.com:8443/x?q#f",
            "https://a.b.internal/",
            "postgres://u:p@db:5432/x",
            "postgres://db,db/x",
            "db:5432",
            "/tmp/local",
        ] {
            assert!(p.check_url(ok).is_ok(), "{ok}");
        }
        for bad in [
            "http://api.example.com/",
            "https://evil.example.com/",
            "https://internal/",
            "postgres://db,evil/x",
            "https://${parent.host}/x",
            "evil:5432",
        ] {
            assert!(p.check_url(bad).is_err(), "{bad}");
        }
        let any_host = ReferencePolicy::open();
        assert!(any_host.check_url("https://${parent.host}/x").is_ok());
        assert!(any_host.check_url("unix:///var/run/x.sock").is_ok());
        let star = ReferencePolicy::open().with_url_hosts(["*"]);
        assert!(star.check_url("https://anything/").is_ok());
        let paths = ReferencePolicy::open()
            .with_path_roots(Vec::<PathBuf>::new())
            .with_url_hosts(Vec::<String>::new());
        assert!(paths.check_url("file:///etc/passwd").is_err());
        assert!(paths.check_url("sqlite:/etc/x.db").is_err());
        assert!(paths.check_url("file:./rel").is_err());
        assert!(paths.check_url("/jobs/1").is_ok());
        assert!(paths.check_url("jobs/${job_id}").is_ok());
        assert!(paths.check_url("localhost/x").is_err());
        assert!(paths.check_url("api.example.com/x").is_err());
        assert!(
            ReferencePolicy::deny_all()
                .any_url_scheme()
                .any_url_host()
                .check_url("ftp://x/")
                .is_ok()
        );
    }

    #[test]
    fn commands_follow_the_subprocess_switch() {
        let r = ResourceRef::new("/executable", ResourceKind::Command, "/bin/tap");
        assert!(ReferencePolicy::deny_all().check(&r).is_err());
        assert!(
            ReferencePolicy::deny_all()
                .with_subprocess(true)
                .check(&r)
                .is_ok()
        );
        let url = ResourceRef::new("/u", ResourceKind::Url, "https://x");
        assert!(ReferencePolicy::open().check(&url).is_ok());
        let path = ResourceRef::new("/p", ResourceKind::Path, "/x");
        assert!(ReferencePolicy::open().check(&path).is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn a_parent_step_after_a_symlink_follows_the_link_target() {
        let base = std::env::temp_dir().join(format!("faucet-resolve-ln-{}", std::process::id()));
        let root = base.join("root");
        let outside = base.join("outside");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(outside.join("inner")).unwrap();
        std::fs::write(outside.join("secret"), "x").unwrap();
        std::os::unix::fs::symlink(outside.join("inner"), root.join("l")).unwrap();
        let p = ReferencePolicy::deny_all().with_path_roots([&root]);
        let r = root.display();
        assert!(p.check_path(&format!("{r}/l/../secret")).is_err());
        assert!(p.check_path(&format!("{r}/l/../new.csv")).is_err());
        assert!(p.check_path(&format!("{r}/l/../*.csv")).is_err());
        assert!(p.check_path(&format!("{r}/ok.csv")).is_ok());
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn numeric_and_dotted_forms_of_a_metadata_host_are_refused() {
        let p = ReferencePolicy::open();
        for bad in [
            "http://2852039166/latest",
            "http://0xa9fea9fe/latest",
            "http://0xA9.0xFE.0xA9.0xFE/",
            "http://0251.0376.0251.0376/",
            "http://169.254.43518/",
            "http://169.254.169.254./latest",
            "http://metadata.google.internal./x",
            "http://METADATA.google.internal/x",
            "http://[0:0:0:0:0:ffff:a9fe:a9fe]/",
            "postgres://u@db,2852039166:5432/x",
        ] {
            assert!(p.check_url(bad).is_err(), "{bad}");
        }
        assert!(is_metadata_host("2852039166"));
        assert!(!is_metadata_host("example.com."));
        assert_eq!(canonical_host("Example.COM."), "example.com");
        assert_eq!(canonical_host("${host}"), "${host}");
        assert_eq!(canonical_host("0x7f000001"), "127.0.0.1");
        let listed = ReferencePolicy::open().with_url_hosts(["api.example.com"]);
        assert!(listed.check_url("https://API.example.com./v1").is_ok());
        assert!(listed.check_url("https://0x7f000001/").is_err());
    }

    #[test]
    fn a_backslash_ends_the_authority_of_a_special_url() {
        assert_eq!(
            url_hosts("http://169.254.169.254\\@api.example.com/x"),
            vec!["169.254.169.254"]
        );
        assert_eq!(
            url_hosts("https://user:pw@API.example.com:8443/x"),
            vec!["api.example.com"]
        );
        assert_eq!(url_hosts("http://${host}\\@a.com/"), vec!["${host}"]);
        // A non-special scheme keeps `\\` in its userinfo, as its parser does.
        assert_eq!(url_hosts("postgres://u\\@db/x"), vec!["db"]);
        assert_eq!(url_hosts("postgres://u:a;b@db;x=1/x"), vec!["db"]);
        let listed = ReferencePolicy::open().with_url_hosts(["api.example.com"]);
        assert!(
            listed
                .check_url("http://169.254.169.254\\@api.example.com/latest/meta-data/")
                .is_err()
        );
        let open = ReferencePolicy::open();
        assert!(
            open.check_url("http://169.254.169.254\\@api.example.com/")
                .is_err()
        );
    }

    #[test]
    fn broker_lists_and_connection_strings_name_their_hosts() {
        assert_eq!(url_hosts("h1:9092,h2:9092"), vec!["h1", "h2"]);
        assert_eq!(scheme_of("h1:9092,h2:9092"), None);
        assert_eq!(
            url_hosts("Server=tcp:10.0.0.9,1433;Database=x;User Id=u;Password=p"),
            vec!["10.0.0.9"]
        );
        assert_eq!(
            url_hosts("Data Source={SQL01\\INST};Initial Catalog=x"),
            vec!["sql01"]
        );
        assert_eq!(url_hosts("Database=x"), vec!["localhost"]);
        assert_eq!(url_hosts("host=db.internal port=5432"), vec!["db.internal"]);
        assert_eq!(
            url_hosts(
                "DefaultEndpointsProtocol=https;AccountName=acct;AccountKey=k==;\
                 EndpointSuffix=core.chinacloudapi.cn"
            ),
            vec!["acct.blob.core.chinacloudapi.cn"]
        );
        assert_eq!(
            url_hosts("AccountName=a;AccountKey=k;BlobEndpoint=http://169.254.169.254/"),
            vec!["169.254.169.254"]
        );
        assert_eq!(url_hosts("UseDevelopmentStorage=true"), vec!["127.0.0.1"]);
        assert_eq!(url_hosts("Server=nats://n1:4222"), vec!["n1"]);
        let p = ReferencePolicy::open().with_url_hosts(["db.internal"]);
        assert!(p.check_url("Server=tcp:10.0.0.9,1433;Database=x").is_err());
        assert!(p.check_url("Server=db.internal;Database=x").is_ok());
        assert!(p.check_url("db.internal:9092,evil:9092").is_err());
        assert!(
            ReferencePolicy::open()
                .check_url("AccountName=a;BlobEndpoint=http://169.254.169.254/")
                .is_err()
        );
    }

    #[test]
    fn resolved_metadata_addresses_are_dropped_unless_the_name_is_listed() {
        let meta: std::net::SocketAddr = "169.254.169.254:0".parse().unwrap();
        let v6: std::net::SocketAddr = "[fd00:ec2::254]:0".parse().unwrap();
        let ok: std::net::SocketAddr = "10.0.0.1:0".parse().unwrap();
        assert_eq!(screen_resolved("a.test", [meta, ok], None), Ok(vec![ok]));
        let err = screen_resolved("A.test.", [meta, v6], None).unwrap_err();
        assert_eq!(
            err,
            "host `a.test` resolves to a link-local / metadata endpoint"
        );
        assert_eq!(screen_resolved("a.test", [], None), Ok(vec![]));
        let listed = ReferencePolicy::open().with_url_hosts(["a.test", "*.wild"]);
        assert!(listed.lists_host_exactly("A.TEST."));
        assert!(!listed.lists_host_exactly("x.wild"));
        assert!(!ReferencePolicy::open().lists_host_exactly("a.test"));
        assert_eq!(
            screen_resolved("a.test", [meta], Some(&listed)),
            Ok(vec![meta])
        );
        assert!(screen_resolved("x.wild", [meta], Some(&listed)).is_err());
    }

    #[test]
    fn a_resolved_check_judges_names_by_their_addresses() {
        let open = ReferencePolicy::open();
        assert!(open.check_url_resolved("http://localhost:1/x").is_ok());
        assert!(open.check_url_resolved("http://127.0.0.1/x").is_ok());
        assert!(open.check_url_resolved("https://${h}/x").is_ok());
        assert!(open.check_url_resolved("file:///tmp/x").is_ok());
        assert!(
            open.check_url_resolved("http://faucet-no-such-host.invalid/x")
                .is_ok()
        );
        assert!(open.check_url_resolved("http://169.254.169.254/").is_err());
        let r = ResourceRef::new("/u", ResourceKind::Url, "http://localhost/");
        assert!(open.check_resolved(&r).is_ok());
        let r = ResourceRef::new("/p", ResourceKind::Path, "s3://localhost/x");
        assert!(open.check_resolved(&r).is_ok());
        let r = ResourceRef::new("/p", ResourceKind::Path, "/tmp/x");
        assert!(open.check_resolved(&r).is_ok());
        let r = ResourceRef::new("/c", ResourceKind::Command, "x");
        assert!(ReferencePolicy::deny_all().check_resolved(&r).is_err());
    }

    #[test]
    fn a_keyed_map_annotation_names_resources_by_key() {
        let schema = json!({
            "type": "object",
            "properties": {
                "opts": {
                    "type": "object",
                    "additionalProperties": {"type": "string"},
                    "x-faucet-resource-keys": {
                        "*endpoint*": "url",
                        "*_file": "path",
                        "proxy*": "url",
                        "google_service_account": "path",
                        "ignored": "bogus"
                    }
                }
            }
        });
        let config = json!({"opts": {
            "AWS_ENDPOINT_URL": "http://169.254.169.254",
            "aws_web_identity_token_file": "/etc/token",
            "Proxy_Url": "http://p",
            "GOOGLE_SERVICE_ACCOUNT": "/k.json",
            "aws_allow_http": "true",
            "ignored": "x",
            "endpoint_n": 3
        }});
        let found = annotated_resources(&schema, &config);
        let mut got: Vec<(&str, ResourceKind)> =
            found.iter().map(|r| (r.pointer.as_str(), r.kind)).collect();
        got.sort();
        assert_eq!(
            got,
            vec![
                ("/opts/AWS_ENDPOINT_URL", ResourceKind::Url),
                ("/opts/GOOGLE_SERVICE_ACCOUNT", ResourceKind::Path),
                ("/opts/Proxy_Url", ResourceKind::Url),
                ("/opts/aws_web_identity_token_file", ResourceKind::Path),
            ]
        );
        let endpoint = found
            .iter()
            .find(|r| r.pointer == "/opts/AWS_ENDPOINT_URL")
            .unwrap();
        assert!(ReferencePolicy::open().check(endpoint).is_err());
        assert!(declares(&schema, ResourceKind::Path));
        assert!(
            annotated_fields(&schema).contains(&("opts{*endpoint*}".into(), ResourceKind::Url))
        );
        assert!(key_pattern_matches("*mid*", "A_MID_B"));
        assert!(key_pattern_matches("*", "x"));
    }

    #[test]
    fn helpers() {
        assert_eq!(scheme_of("C:/x"), None);
        assert_eq!(scheme_of("HTTPS://x").as_deref(), Some("https"));
        assert_eq!(scheme_of("no-colon"), None);
        assert_eq!(scheme_of("bad scheme:x"), None);
        assert_eq!(url_hosts("https://[::1]:8080/x"), vec!["[::1]".to_string()]);
        assert_eq!(url_hosts("h:notaport"), vec!["h:notaport".to_string()]);
        assert!(!host_matches("*.example.com", "example.com"));
        assert_eq!(
            normalize(Path::new("/./faucet-no-such-dir/./x")),
            Some(PathBuf::from("/faucet-no-such-dir/x"))
        );
        assert_eq!(normalize(Path::new("/faucet-no-such-dir/../etc")), None);
        let e = Refusal::new(RefusalKind::Command, "x", "why");
        assert_eq!(e.to_string(), "x (why)");
        assert_eq!(ResourceKind::Url.to_string(), "url");
    }
}
