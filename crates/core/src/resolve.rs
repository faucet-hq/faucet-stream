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

pub use crate::host_credentials::{
    deny_unscoped_host_credentials, host_credentials_allowed, require_host_credentials,
    scoped_host_credentials, unscoped_host_credentials_denied, with_host_credentials,
};
pub use crate::store_options::{StoreKind, store_options_host_identity};

mod azure;
pub use azure::{AzureConnKey, AzureConnSettings, is_valid_azure_account_name};

/// The JSON Schema keyword marking a config field that names an external
/// resource. Its value is a [`ResourceKind`] (`"path"`, `"url"`, `"command"`).
pub const RESOURCE_ANNOTATION: &str = "x-faucet-resource";

/// The JSON Schema keyword on a free-form string map (object-store options,
/// driver properties) naming which of its keys hold a resource: an object of
/// key pattern → [`ResourceKind`], or → `{"kind": …, "syntax": …}` to also
/// name the value's grammar ([`UrlSyntax`]). A pattern is matched against the
/// key lower-cased: exact, `PREFIX*`, `*SUFFIX` or `*INFIX*`; an exact
/// pattern wins over the others.
///
/// ```ignore
/// #[schemars(extend("x-faucet-resource-keys" = {
///     "*endpoint*": "url",
///     "*_file": "path",
///     "bootstrap.servers": {"kind": "url", "syntax": "url-list"}
/// }))]
/// pub storage_options: HashMap<String, String>,
/// ```
pub const RESOURCE_KEYS_ANNOTATION: &str = "x-faucet-resource-keys";

/// The JSON Schema keyword marking a config setting that turns off
/// verification of the peer a connector connects to (TLS certificate or SSH
/// host key). [`annotated_resources`] reports a config that turns it on as a
/// [`ResourceKind::Unverified`] reference. Its value is one of:
///
/// - `true` on a boolean field: the field set to `true`;
/// - `true` on an object schema (an enum variant): a config object whose
///   `const` properties all match the variant's;
/// - an array of strings on a string field: one of those values (any case);
/// - an object of key → array of strings on a string map: an entry whose
///   value is one of those listed for its key (any case).
///
/// ```ignore
/// #[schemars(extend("x-faucet-unverified" = true))]
/// pub insecure_skip_verify: bool,
/// ```
pub const UNVERIFIED_ANNOTATION: &str = "x-faucet-unverified";

/// The JSON Schema keyword marking a config setting that authenticates with
/// credentials held by the host running faucet (OS authentication, a local
/// wallet) rather than ones the config names. Takes the same values as
/// [`UNVERIFIED_ANNOTATION`]; a config that turns it on is reported as a
/// [`ResourceKind::HostCredentials`] reference.
pub const HOST_CREDENTIALS_ANNOTATION: &str = "x-faucet-host-credentials";

/// The JSON Schema keyword on a [`ResourceKind::Url`] field naming the grammar
/// its value is written in, when that is not a URL: a [`UrlSyntax`]
/// (`"ado"`, `"oracle"`, `"azure-conn"`, `"url-list"`). The value is then read
/// the way the connector's client reads it.
///
/// ```ignore
/// #[schemars(extend("x-faucet-resource" = "url", "x-faucet-url-syntax" = "ado"))]
/// pub connection_string: Option<String>,
/// ```
pub const URL_SYNTAX_ANNOTATION: &str = "x-faucet-url-syntax";

/// The grammar of a [`ResourceKind::Url`] value ([`URL_SYNTAX_ANNOTATION`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum UrlSyntax {
    /// An ADO.NET connection string (`Server=tcp:host,1433;…`), read with
    /// [`crate::ado::parse`].
    Ado,
    /// An Oracle connect string: Easy Connect (Plus), a connect descriptor, a
    /// net service alias or directory naming.
    Oracle,
    /// An Azure Storage connection string (`AccountName=…;BlobEndpoint=…`).
    AzureConn,
    /// URLs or `host:port` addresses separated by `,` or whitespace (a broker
    /// or server list).
    UrlList,
}

impl UrlSyntax {
    /// The annotation value for this grammar.
    pub fn as_str(self) -> &'static str {
        match self {
            UrlSyntax::Ado => "ado",
            UrlSyntax::Oracle => "oracle",
            UrlSyntax::AzureConn => "azure-conn",
            UrlSyntax::UrlList => "url-list",
        }
    }

    /// Parse an annotation value.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "ado" => Some(UrlSyntax::Ado),
            "oracle" => Some(UrlSyntax::Oracle),
            "azure-conn" => Some(UrlSyntax::AzureConn),
            "url-list" => Some(UrlSyntax::UrlList),
            _ => None,
        }
    }
}

impl std::fmt::Display for UrlSyntax {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

fn syntax_of(node: &Value) -> Option<UrlSyntax> {
    node.get(URL_SYNTAX_ANNOTATION)
        .and_then(Value::as_str)
        .and_then(UrlSyntax::parse)
}

/// The longest value a policy with path roots (one a host applies to configs
/// it does not trust) reads; a longer one is refused.
pub const MAX_SCREENED_LEN: usize = 64 * 1024;

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

/// The kind (and grammar) one [`RESOURCE_KEYS_ANNOTATION`] entry gives: a
/// kind string (`"url"`), or an object `{"kind": "url", "syntax": "url-list"}`
/// naming the value's grammar too ([`URL_SYNTAX_ANNOTATION`]).
fn keyed_entry(entry: &Value) -> Option<(ResourceKind, Option<UrlSyntax>)> {
    match entry {
        Value::String(kind) => ResourceKind::parse(kind).map(|k| (k, None)),
        Value::Object(o) => {
            let kind = o.get("kind")?.as_str().and_then(ResourceKind::parse)?;
            let syntax = o
                .get("syntax")
                .and_then(Value::as_str)
                .and_then(UrlSyntax::parse);
            Some((kind, syntax))
        }
        _ => None,
    }
}

/// The kind a [`RESOURCE_KEYS_ANNOTATION`] on `node` gives map key `key`, and
/// its grammar. An exact pattern wins over a wildcard one.
fn keyed_annotation(node: &Value, key: &str) -> Option<(ResourceKind, Option<UrlSyntax>)> {
    let patterns = node.get(RESOURCE_KEYS_ANNOTATION)?.as_object()?;
    let exact = patterns
        .iter()
        .find(|(p, _)| !p.contains('*') && key_pattern_matches(p, key));
    exact
        .or_else(|| {
            patterns
                .iter()
                .find(|(p, _)| p.contains('*') && key_pattern_matches(p, key))
        })
        .and_then(|(_, entry)| keyed_entry(entry))
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
    /// A setting that turns off peer verification ([`UNVERIFIED_ANNOTATION`]);
    /// the value is the setting as configured.
    Unverified,
    /// A setting that authenticates with the host's own credentials
    /// ([`HOST_CREDENTIALS_ANNOTATION`]); the value is the setting as configured.
    HostCredentials,
}

impl ResourceKind {
    /// The annotation value for this kind.
    pub fn as_str(self) -> &'static str {
        match self {
            ResourceKind::Path => "path",
            ResourceKind::Url => "url",
            ResourceKind::Command => "command",
            ResourceKind::Unverified => "unverified",
            ResourceKind::HostCredentials => "host_credentials",
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
    /// The grammar of a URL value that is not a URL ([`URL_SYNTAX_ANNOTATION`]).
    pub syntax: Option<UrlSyntax>,
}

impl ResourceRef {
    pub fn new(pointer: impl Into<String>, kind: ResourceKind, value: impl Into<String>) -> Self {
        Self {
            pointer: pointer.into(),
            kind,
            value: value.into(),
            syntax: None,
        }
    }

    /// This reference, its value written in `syntax`.
    pub fn with_syntax(mut self, syntax: Option<UrlSyntax>) -> Self {
        self.syntax = syntax;
        self
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
    /// A setting that turns off peer verification.
    Unverified,
    /// A setting that authenticates with the host's own credentials.
    HostCredentials,
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
    unverified: bool,
    host_credentials: bool,
    pin_servers: bool,
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

/// IPv6 instance-metadata endpoints outside link-local space (AWS, GCP, OCI).
const METADATA_V6: [std::net::Ipv6Addr; 3] = [
    std::net::Ipv6Addr::new(0xfd00, 0xec2, 0, 0, 0, 0, 0, 0x254),
    std::net::Ipv6Addr::new(0xfd20, 0xce, 0, 0, 0, 0, 0, 0x254),
    std::net::Ipv6Addr::new(0xfd00, 0xc1, 0, 0, 0, 0, 0xa9fe, 0xa9fe),
];

/// Whether an address is link-local or a cloud instance-metadata endpoint.
pub fn is_metadata_ip(ip: std::net::IpAddr) -> bool {
    let v4_meta =
        |v4: std::net::Ipv4Addr| v4.is_link_local() || v4.octets() == [100, 100, 100, 200];
    match ip {
        std::net::IpAddr::V4(v4) => v4_meta(v4),
        std::net::IpAddr::V6(v6) => {
            (v6.segments()[0] & 0xffc0) == 0xfe80
                || METADATA_V6.contains(&v6)
                || v6.to_ipv4_mapped().is_some_and(v4_meta)
                || embedded_ipv4(v6).is_some_and(v4_meta)
        }
    }
}

/// The IPv4 address carried in an IPv4-compatible (`::a.b.c.d`) or NAT64
/// well-known-prefix (`64:ff9b::a.b.c.d`) IPv6 address.
fn embedded_ipv4(v6: std::net::Ipv6Addr) -> Option<std::net::Ipv4Addr> {
    let seg = v6.segments();
    let prefix_ok = seg[..6] == [0, 0, 0, 0, 0, 0] || seg[..6] == [0x64, 0xff9b, 0, 0, 0, 0];
    let o = v6.octets();
    prefix_ok.then(|| std::net::Ipv4Addr::new(o[12], o[13], o[14], o[15]))
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

/// Schemes a path field opens as a remote location (object stores, HTTP,
/// SFTP) when written `scheme://…`. Any other prefix in a path field is a
/// local path.
const REMOTE_PATH_SCHEMES: &[&str] = &[
    "http", "https", "s3", "s3a", "s3n", "gs", "gcs", "az", "azure", "abfs", "abfss", "adl",
    "wasb", "wasbs", "oss", "sftp", "ftp", "hdfs", "memory",
];

/// What a value in a path field names.
enum Location<'a> {
    /// A local path, with any `file:` / `sqlite:` / `duckdb:` prefix removed
    /// and decoded the way its consumer decodes it.
    Local(std::borrow::Cow<'a, str>),
    /// A remote location (`s3://…`), checked as a URL.
    Remote,
    /// A local path whose meaning cannot be checked.
    Refused(&'static str),
}

/// Percent-decode `s` (`None` when the bytes are not UTF-8).
fn percent_decoded(s: &str) -> Option<std::borrow::Cow<'_, str>> {
    percent_encoding::percent_decode_str(s).decode_utf8().ok()
}

/// The file a `sqlite:` connection URL opens, read exactly as sqlx reads it:
/// the `sqlite://` then `sqlite:` prefixes removed (case-sensitively, each as
/// often as it repeats), the part before the first `?`, percent-decoded.
fn sqlite_file(value: &str) -> Location<'_> {
    let rest = value
        .trim_start_matches("sqlite://")
        .trim_start_matches("sqlite:");
    let database = rest.split('?').next().unwrap_or(rest);
    match percent_decoded(database) {
        // SQLite opens a name starting `file:` as a URI and decodes it again.
        Some(name) if name.len() >= 5 && name[..5].eq_ignore_ascii_case("file:") => {
            Location::Refused(
                "a SQLite database name that is itself a `file:` URI cannot be checked",
            )
        }
        Some(name) => Location::Local(name),
        None => Location::Refused("a percent-encoded path that is not UTF-8 cannot be checked"),
    }
}

fn locate(value: &str) -> Location<'_> {
    use std::borrow::Cow;
    match scheme_of(value).as_deref() {
        Some("file") => {
            let rest = &value[5..];
            let local = match rest.strip_prefix("//") {
                None => rest,
                Some(after) => {
                    // `file://host/path` (RFC 8089): an empty authority or
                    // `localhost` names the local absolute path; any other
                    // host is not local.
                    let (authority, path) = match after.find('/') {
                        Some(i) => (&after[..i], &after[i..]),
                        None => (after, ""),
                    };
                    if authority.is_empty() || authority.eq_ignore_ascii_case("localhost") {
                        path
                    } else if authority == "." || authority == ".." {
                        after
                    } else {
                        return Location::Remote;
                    }
                }
            };
            match percent_decoded(local) {
                Some(p) => Location::Local(p),
                None => {
                    Location::Refused("a percent-encoded path that is not UTF-8 cannot be checked")
                }
            }
        }
        Some("sqlite") if value.starts_with("sqlite:") => sqlite_file(value),
        Some("duckdb") if value.starts_with("duckdb:") => Location::Local(Cow::Borrowed(
            value
                .trim_start_matches("duckdb://")
                .trim_start_matches("duckdb:"),
        )),
        Some(s) if REMOTE_PATH_SCHEMES.contains(&s) && value[s.len() + 1..].starts_with("//") => {
            Location::Remote
        }
        _ => Location::Local(Cow::Borrowed(value)),
    }
}

/// A local path a value names behind a `file:` / `sqlite:` / `duckdb:`
/// prefix, or the value itself. `None` for a remote location.
fn local_part(value: &str) -> Option<std::borrow::Cow<'_, str>> {
    match locate(value) {
        Location::Local(p) => Some(p),
        Location::Remote => None,
        Location::Refused(_) => Some(std::borrow::Cow::Borrowed(value)),
    }
}

/// The local filesystem path a path-field value opens: a `file:` URL's path,
/// the file a `sqlite:` URL opens (decoded as sqlx decodes it), a `duckdb:`
/// path without its prefix, or the value itself. `None` for a remote location
/// (`s3://…`, `https://…`). A value whose local meaning cannot be checked is
/// returned as written, and [`ReferencePolicy::check_path`] refuses it.
pub fn local_path(value: &str) -> Option<std::borrow::Cow<'_, str>> {
    local_part(value)
}

/// Whether a remote location's path climbs with a `..` segment (written
/// plainly or percent-encoded), which a client may resolve against a local
/// directory or another prefix.
fn has_parent_segment(value: &str) -> bool {
    let after = value.split_once("://").map_or(value, |(_, a)| a);
    after
        .split(['/', '\\'])
        .any(|seg| percent_decoded(seg).is_some_and(|d| d.trim() == "..") || seg == "..")
}

/// Whether a relative path's first segment carries a `:` (other than a
/// Windows drive letter), which reads as a URL scheme to some consumers and
/// as a directory name to others.
fn ambiguous_first_segment(path: &str) -> bool {
    if path.starts_with('/') {
        return false;
    }
    let first = path.split(['/', '\\']).next().unwrap_or(path);
    let drive = first.len() == 2 && first.as_bytes()[0].is_ascii_alphabetic();
    first.contains(':') && !drive
}

/// Whether a scheme-less value is a relative path (`jobs/${id}`) rather than
/// `host[:port][/…]`: its first segment is neither dotted, ported nor
/// `localhost`, and does not read as an IP address (`2852039166`, `0x7f000001`).
fn is_relative_path(value: &str) -> bool {
    match value.split_once('/') {
        Some((first, _)) => {
            !first.contains(['.', ':', '['])
                && !first.eq_ignore_ascii_case("localhost")
                && canonical_host(first).parse::<std::net::IpAddr>().is_err()
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
    let pairs = connection_string_pairs(value)?;
    let get = |key: &str| {
        pairs
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    };
    let mut hosts = Vec::new();
    for (k, v) in &pairs {
        if CONN_HOST_KEYS.contains(&k.as_str()) {
            hosts.extend(server_value_hosts(v));
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

/// The innermost `(KEY=value)` pairs of a parenthesised connect descriptor
/// (Oracle Net), keys lower-cased.
fn descriptor_pairs(value: &str) -> Vec<(String, String)> {
    value
        .split('(')
        .filter_map(|part| {
            let (k, v) = part.split_once('=')?;
            let v = v.split(')').next().unwrap_or(v).trim();
            (!v.is_empty() && !v.starts_with('(')).then(|| {
                (
                    k.trim().to_ascii_lowercase(),
                    v.trim_matches(|c| c == '"' || c == '\'').to_string(),
                )
            })
        })
        .collect()
}

/// The `key=value` pairs of a `key=value;…` (or libpq `key=value key=value`)
/// connection string, keys lower-cased and values unquoted, or `None` when
/// `value` is not one.
fn connection_string_pairs(value: &str) -> Option<Vec<(String, String)>> {
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
    Some(
        value
            .split(separators)
            .filter_map(|kv| kv.split_once('='))
            .map(|(k, v)| (k.trim().to_ascii_lowercase(), unquote(v)))
            .collect(),
    )
}

/// The hosts a URL's authority names (several for `h1,h2` connection
/// strings and broker lists, and for the servers a `key=value;…` connection
/// string names), in canonical form ([`canonical_host`]), without userinfo or
/// port. A URL with a special scheme (`http`, `https`, …) is read by the same
/// parser HTTP clients use, so `\\` ends its authority as `/` does. A host
/// taken from a `${…}` token is returned lower-cased as written.
pub fn url_hosts(value: &str) -> Vec<String> {
    let value = value.trim();
    if let Some(read) = MongoRead::read(value) {
        return read.tcp_hosts();
    }
    if let Some(ip) = ip_literal(value) {
        return vec![ip];
    }
    if let Some(items) = scheme_list(value) {
        return items.into_iter().flat_map(url_hosts).collect();
    }
    if value.starts_with('(') {
        let hosts: Vec<String> = descriptor_pairs(value)
            .into_iter()
            .filter(|(k, _)| k == "host")
            .map(|(_, v)| canonical_host(&v))
            .filter(|h| !h.is_empty())
            .collect();
        return if hosts.is_empty() {
            vec!["localhost".into()]
        } else {
            hosts
        };
    }
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
        _ if value.starts_with("//") => &value[2..],
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
    let mut out: Vec<String> = hosts
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
        .collect();
    for (key, v) in database_url_params(value) {
        if DB_HOST_PARAMS.contains(&key.as_str()) && !v.starts_with('/') {
            out.extend(
                v.split(',')
                    .map(|h| canonical_host(h.trim()))
                    .filter(|h| !h.is_empty()),
            );
        }
    }
    out
}

/// The canonical host of a value that is, as a whole, an IP address
/// (`169.254.169.254`, `fd00:ec2::254`, `[::1]`).
fn ip_literal(value: &str) -> Option<String> {
    let bare = value
        .strip_prefix('[')
        .and_then(|v| v.strip_suffix(']'))
        .unwrap_or(value);
    match bare.parse::<std::net::IpAddr>().ok()? {
        std::net::IpAddr::V4(v4) => Some(v4.to_string()),
        std::net::IpAddr::V6(v6) => Some(format!("[{v6}]")),
    }
}

/// The elements of a `,`-separated list whose elements after the first carry
/// their own `scheme://` (`nats://a:4222,nats://b:4222`), or `None`.
fn scheme_list(value: &str) -> Option<Vec<&str>> {
    let parts: Vec<&str> = value.split(',').map(str::trim).collect();
    let later_scheme = parts
        .iter()
        .skip(1)
        .any(|p| scheme_of(p).is_some_and(|s| p[s.len()..].starts_with("://")));
    (parts.len() > 1 && later_scheme).then(|| parts.into_iter().filter(|p| !p.is_empty()).collect())
}

/// The elements of a [`UrlSyntax::UrlList`] value.
fn list_items(value: &str) -> impl Iterator<Item = &str> {
    value
        .split(|c: char| c == ',' || c.is_ascii_whitespace())
        .filter(|p| !p.is_empty())
}

/// The host of one `host[:port]` address (`[v6]:port` kept bracketed).
fn address_host(address: &str) -> String {
    let address = address.trim();
    let host = if address.starts_with('[') {
        address.find(']').map_or(address, |end| &address[..=end])
    } else if let Some(ip) = ip_literal(address) {
        return ip;
    } else {
        address
            .rsplit_once(':')
            .filter(|(_, port)| port.chars().all(|c| c.is_ascii_digit()))
            .map_or(address, |(host, _)| host)
    };
    canonical_host(host)
}

/// The hosts one server value of a connection string names (`tcp:host,1433`,
/// `host\instance`, `(local)`, a URL): any `tcp:` / `np:` / `lpc:` / `admin:`
/// prefix (any case) removed, port and instance dropped. A value starting
/// with `/` names no host.
fn server_value_hosts(v: &str) -> Vec<String> {
    if v.contains("://") {
        return url_hosts(v);
    }
    let lower = v.to_ascii_lowercase();
    let bare = ["tcp:", "np:", "lpc:", "admin:"]
        .iter()
        .find_map(|p| lower.starts_with(p).then(|| &v[p.len()..]))
        .unwrap_or(v);
    if bare.contains("://") {
        return url_hosts(bare);
    }
    if bare.starts_with('/') {
        return Vec::new();
    }
    url_hosts(&format!("tcp://{}", crate::ado::host_of_server(bare)))
}

/// The hosts an ADO.NET connection string connects to: the driver's own
/// reading ([`crate::ado::server_host`]) and every server-like key read more
/// broadly ([`server_value_hosts`]).
fn ado_hosts(pairs: &[(String, String)]) -> Vec<String> {
    let mut hosts = vec![canonical_host(&crate::ado::server_host(pairs))];
    for (k, v) in pairs {
        if CONN_HOST_KEYS.contains(&k.as_str()) {
            hosts.extend(server_value_hosts(v));
        }
    }
    hosts.retain(|h| !h.is_empty());
    hosts.sort();
    hosts.dedup();
    hosts
}

/// Whether a MongoDB URI authenticates with credentials the host itself
/// holds: `authMechanism=MONGODB-AWS` or `GSSAPI`, or an
/// `authMechanismProperties` `ENVIRONMENT` (a cloud or Kubernetes identity).
pub fn mongodb_uri_uses_host_identity(uri: &str) -> bool {
    MongoRead::read(uri).is_some_and(|r| r.host_credentials())
}

/// Whether an Oracle connect string authenticates with the host's identity
/// or token files: a `TOKEN_AUTH` other than `OAUTH` with an explicit
/// `TOKEN_LOCATION`.
pub fn oracle_connect_uses_host_identity(value: &str) -> bool {
    let read = read_oracle(value);
    let get = |key: &str| {
        read.params
            .iter()
            .filter(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
            .collect::<Vec<_>>()
    };
    get("token_auth")
        .last()
        .is_some_and(|mode| !mode.eq_ignore_ascii_case("oauth") || get("token_location").is_empty())
}

/// How an Oracle connect string names its database.
#[derive(Debug, PartialEq, Eq)]
enum OracleForm {
    Empty,
    /// `(DESCRIPTION=…)`.
    Descriptor,
    /// `[protocol://][//]host[:port][/service][?params]`.
    EasyConnect,
    /// A bare net service name, resolved through the host's Oracle
    /// configuration (`tnsnames.ora`).
    Alias,
    /// `ldap:` naming, resolved through the host's Oracle configuration.
    Directory,
}

/// An Oracle connect string as the Oracle client reads it.
struct OracleRead {
    form: OracleForm,
    /// The Easy Connect protocol (`tcps`), lower case.
    scheme: Option<String>,
    /// Every server and proxy host, canonical.
    hosts: Vec<String>,
    /// Descriptor `(KEY=value)` pairs or Easy Connect Plus parameters, keys
    /// lower-cased and values unquoted.
    params: Vec<(String, String)>,
}

fn unquote(v: &str) -> String {
    let v = v.trim();
    v.strip_prefix('"')
        .and_then(|v| v.strip_suffix('"'))
        .or_else(|| v.strip_prefix('\'').and_then(|v| v.strip_suffix('\'')))
        .unwrap_or(v)
        .to_string()
}

fn read_oracle(value: &str) -> OracleRead {
    let value = value.trim();
    let mut read = OracleRead {
        form: OracleForm::EasyConnect,
        scheme: None,
        hosts: Vec::new(),
        params: Vec::new(),
    };
    let lower = value.to_ascii_lowercase();
    if value.is_empty() {
        read.form = OracleForm::Empty;
    } else if lower.starts_with("ldap:") || lower.starts_with("ldaps:") {
        read.form = OracleForm::Directory;
    } else if value.starts_with('(') {
        read.form = OracleForm::Descriptor;
        read.params = descriptor_pairs(value);
        for (k, v) in &read.params {
            if k == "host" {
                read.hosts.extend(v.split(',').map(address_host));
            }
        }
    } else if !value.contains([':', '/', '(', ',']) {
        read.form = OracleForm::Alias;
    } else {
        let mut rest = value;
        if let Some(i) = rest.find("://") {
            let s = &rest[..i];
            if !s.is_empty()
                && s.chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
            {
                read.scheme = Some(s.to_ascii_lowercase());
                rest = &rest[i + 3..];
            }
        }
        if read.scheme.is_none() {
            rest = rest.strip_prefix("//").unwrap_or(rest);
        }
        let (main, query) = rest.split_once('?').unwrap_or((rest, ""));
        let addresses = main.split('/').next().unwrap_or(main);
        read.hosts
            .extend(addresses.split([';', ',']).map(address_host));
        read.params = query
            .split('&')
            .filter_map(|kv| kv.split_once('='))
            .map(|(k, v)| (k.trim().to_ascii_lowercase(), unquote(v)))
            .collect();
    }
    for (k, v) in &read.params {
        if k == "https_proxy" {
            read.hosts.push(address_host(v));
        }
    }
    read.hosts.retain(|h| !h.is_empty());
    read
}

/// The `key=value` pairs of an Azure Storage connection string, keys
/// lower-cased.
fn azure_pairs(value: &str) -> Vec<(String, String)> {
    value
        .split(';')
        .filter_map(|seg| seg.trim().split_once('='))
        .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
        .filter(|(_, v)| !v.is_empty())
        .collect()
}

/// The URLs an Azure Storage connection string names, with the key each
/// came from: every `…Endpoint` key in any case (wider than the client,
/// which reads only `BlobEndpoint`), then — without a `BlobEndpoint` — the
/// account endpoint the client builds ([`AzureConnSettings::account_url`]).
fn azure_conn_urls(value: &str) -> Vec<(String, String)> {
    let mut urls: Vec<(String, String)> = azure_pairs(value)
        .into_iter()
        .filter(|(k, _)| k.ends_with("endpoint"))
        .collect();
    let settings = AzureConnSettings::read(value);
    if settings.get(AzureConnKey::BlobEndpoint).is_none()
        && let Some(url) = settings.account_url()
    {
        urls.push(("accountname".into(), url));
    }
    urls
}

/// The hosts a value written in `syntax` connects to, in canonical form; the
/// same as [`url_hosts`] for `None`.
pub fn url_hosts_as(value: &str, syntax: Option<UrlSyntax>) -> Vec<String> {
    let raw = value;
    let value = value.trim();
    match syntax {
        None => url_hosts(value),
        Some(UrlSyntax::Ado) => crate::ado::parse(raw)
            .map(|pairs| ado_hosts(&pairs))
            .unwrap_or_default(),
        Some(UrlSyntax::Oracle) => read_oracle(value).hosts,
        Some(UrlSyntax::AzureConn) => {
            let mut hosts: Vec<String> = azure_conn_urls(value)
                .iter()
                .flat_map(|(_, u)| url_hosts(u))
                .collect();
            if AzureConnSettings::read(value).use_development_storage() {
                hosts.push("127.0.0.1".into());
            }
            hosts
        }
        Some(UrlSyntax::UrlList) => list_items(value).flat_map(url_hosts).collect(),
    }
}

/// The socket path a `unix:` / `…+unix:` URL (`redis+unix:///run/redis.sock`)
/// opens, percent-decoded (`Some(None)` when it is not UTF-8); `None` for any
/// other scheme.
fn unix_socket_path(scheme: &str, value: &str) -> Option<Option<String>> {
    if scheme != "unix" && !scheme.ends_with("+unix") {
        return None;
    }
    let rest = &value[scheme.len() + 1..];
    let rest = rest.strip_prefix("//").unwrap_or(rest);
    let path = rest.split(['?', '#']).next().unwrap_or(rest);
    Some(percent_decoded(path).map(|p| p.into_owned()))
}

/// One server address of a MongoDB URI, read as `ServerAddress::parse` reads
/// it.
#[derive(Debug, Clone, PartialEq, Eq)]
enum MongoHost {
    /// A TCP host, canonical.
    Tcp(String),
    /// A Unix socket path (a segment ending in `.sock`), percent-decoded;
    /// `None` when it is not UTF-8.
    Socket(Option<String>),
    /// A segment the driver refuses.
    Unreadable,
}

/// A `mongodb://` / `mongodb+srv://` URI read as the MongoDB driver reads it:
/// options after the first `?`, user info up to the last `@`, hosts up to the
/// first `/`, split on `,` (`#` and `;` are host characters); options split on
/// `&` and the first `=`, keys folded with Unicode lower-casing and not
/// decoded, values percent-decoded (lossy, no `+` → space).
#[derive(Debug, Clone, PartialEq, Eq)]
struct MongoRead {
    srv: bool,
    hosts: Vec<MongoHost>,
    options: Vec<(String, String)>,
}

impl MongoRead {
    fn read(value: &str) -> Option<Self> {
        let (scheme, after) = value.split_once("://")?;
        let srv = if scheme.eq_ignore_ascii_case("mongodb") {
            false
        } else if scheme.eq_ignore_ascii_case("mongodb+srv") {
            true
        } else {
            return None;
        };
        let (pre, query) = match after.split_once('?') {
            Some((pre, q)) => (pre, Some(q)),
            None => (after, None),
        };
        let hosts_and_db = pre.rsplit_once('@').map_or(pre, |(_, h)| h);
        let hosts = hosts_and_db
            .split_once('/')
            .map_or(hosts_and_db, |(h, _)| h);
        let hosts = hosts.split(',').map(mongo_host).collect();
        let options = query
            .into_iter()
            .flat_map(|q| q.split('&'))
            .filter_map(|pair| pair.split_once('='))
            .map(|(k, v)| {
                let v = percent_encoding::percent_decode_str(v).decode_utf8_lossy();
                (k.to_lowercase(), v.into_owned())
            })
            .collect();
        Some(Self {
            srv,
            hosts,
            options,
        })
    }

    fn values(&self, key: &str) -> impl Iterator<Item = &str> {
        self.options
            .iter()
            .filter(move |(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    fn is_true(&self, key: &str) -> bool {
        self.values(key).any(|v| v == "true")
    }

    /// Whether the client stays on the server it names rather than learning
    /// others from it.
    fn pinned(&self) -> bool {
        self.is_true("loadbalanced") || (!self.srv && self.is_true("directconnection"))
    }

    /// Whether the URI authenticates with credentials the host itself holds
    /// (cloud or Kubernetes identity, Kerberos).
    fn host_credentials(&self) -> bool {
        let mechanism = self.values("authmechanism").any(|m| {
            ["MONGODB-AWS", "GSSAPI"]
                .iter()
                .any(|x| m.eq_ignore_ascii_case(x))
        });
        let environment = self.values("authmechanismproperties").any(|props| {
            props.split(',').any(|p| {
                p.split_once(':')
                    .is_some_and(|(k, _)| k.trim().eq_ignore_ascii_case("environment"))
            })
        });
        mechanism || environment
    }

    /// The TCP hosts the client connects to, the proxy included.
    fn tcp_hosts(&self) -> Vec<String> {
        let mut out: Vec<String> = self
            .hosts
            .iter()
            .filter_map(|h| match h {
                MongoHost::Tcp(h) => Some(h.clone()),
                _ => None,
            })
            .collect();
        out.extend(
            self.values("proxyhost")
                .map(canonical_host)
                .filter(|h| !h.is_empty()),
        );
        out
    }
}

/// The MongoDB options that name a local file.
const MONGO_PATH_OPTIONS: &[&str] = &["tlscafile", "tlscertificatekeyfile"];

/// The MongoDB options that turn off peer verification unless `false`.
const MONGO_UNVERIFIED_OPTIONS: &[&str] = &[
    "tlsinsecure",
    "tlsallowinvalidcertificates",
    "tlsallowinvalidhostnames",
];

fn mongo_host(segment: &str) -> MongoHost {
    if segment.ends_with(".sock") {
        return MongoHost::Socket(percent_decoded(segment).map(|p| p.into_owned()));
    }
    let host = match segment.strip_prefix('[') {
        Some(rest) => match rest.split_once(']') {
            Some((ip, port))
                if ip.parse::<std::net::Ipv6Addr>().is_ok()
                    && (port.is_empty() || port.starts_with(':')) =>
            {
                return MongoHost::Tcp(canonical_host(&format!("[{ip}]")));
            }
            _ => return MongoHost::Unreadable,
        },
        None => segment.split_once(':').map_or(segment, |(h, _)| h),
    };
    match canonical_host(host) {
        h if h.is_empty() => MongoHost::Unreadable,
        h => MongoHost::Tcp(h),
    }
}

/// The domain every host a `mongodb+srv` lookup of `name` returns must end
/// with: `name` without its first label when it has three or more.
fn srv_parent(name: &str) -> &str {
    match name.split_once('.') {
        Some((_, rest)) if rest.contains('.') => rest,
        _ => name,
    }
}

/// Connect-string parameters (Easy Connect Plus or descriptor) naming a local
/// file or directory the Oracle client reads.
const ORACLE_PATH_PARAMS: &[&str] = &[
    "wallet_location",
    "my_wallet_directory",
    "token_location",
    "oci_config_file",
    "directory",
];

/// Connect-string parameters naming a URL the Oracle client calls.
const ORACLE_URL_PARAMS: &[&str] = &["oci_iam_url", "azure_db_app_id_uri"];

/// Schemes whose drivers read connection settings from the URL query.
const DB_URL_SCHEMES: &[&str] = &[
    "postgres",
    "postgresql",
    "redshift",
    "mysql",
    "mariadb",
    "tcp",
    "tcps",
];

/// Query parameters of a database URL that name a host (or, starting with
/// `/`, a Unix socket directory).
const DB_HOST_PARAMS: &[&str] = &["host", "hostaddr", "https_proxy"];

/// Query parameters of a database URL that name a local file.
const DB_PATH_PARAMS: &[&str] = &[
    "socket",
    "passfile",
    "sslrootcert",
    "ssl-root-cert",
    "sslca",
    "ssl-ca",
    "sslcert",
    "ssl-cert",
    "sslkey",
    "ssl-key",
    "sslcrl",
    "sslcrldir",
    "trustservercertificateca",
    "token_location",
    "oci_config_file",
    "wallet_location",
    "my_wallet_directory",
    "directory",
];

/// Query parameters of a database URL that turn off peer verification, with
/// the values that do.
const DB_UNVERIFIED_PARAMS: &[(&str, &[&str])] = &[
    ("verify_ca", &["false", "0"]),
    ("verify_identity", &["false", "0"]),
    ("trustservercertificate", &["true", "yes"]),
    ("ssl_server_dn_match", &["false", "no", "off"]),
];

/// The query parameters of a database connection URL, or the pairs of a
/// `key=value` connection string (lower-cased keys); empty for any other
/// value.
fn database_url_params(value: &str) -> Vec<(String, String)> {
    let value = value.trim();
    if value.starts_with('(') {
        return descriptor_pairs(value);
    }
    if !value.contains("://") {
        return connection_string_pairs(value).unwrap_or_default();
    }
    let is_db = scheme_of(value).is_some_and(|s| DB_URL_SCHEMES.contains(&s.as_str()));
    if !is_db {
        return Vec::new();
    }
    let query = value
        .split_once('?')
        .map(|(_, q)| q.split('#').next().unwrap_or(q))
        .unwrap_or("");
    url::form_urlencoded::parse(query.as_bytes())
        .map(|(k, v)| (k.to_ascii_lowercase(), v.into_owned()))
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
            unverified: true,
            host_credentials: true,

            pin_servers: false,
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
            unverified: false,
            host_credentials: false,

            pin_servers: false,
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

    /// Whether settings that turn off peer verification are allowed.
    pub fn with_unverified(mut self, allowed: bool) -> Self {
        self.unverified = allowed;
        self
    }

    /// Whether settings that authenticate with the host's own credentials are
    /// allowed.
    pub fn with_host_credentials(mut self, allowed: bool) -> Self {
        self.host_credentials = allowed;
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

    /// Require a client that learns further servers from the one it reaches
    /// (a MongoDB replica set or `mongodb+srv` lookup) to stay on the server
    /// the value names (`directConnection=true` or `loadBalanced=true`).
    pub fn with_pinned_servers(mut self, pinned: bool) -> Self {
        self.pin_servers = pinned;
        self
    }

    /// Whether clients must stay on the servers a value names
    /// ([`with_pinned_servers`](Self::with_pinned_servers)).
    pub fn pins_servers(&self) -> bool {
        self.pin_servers
    }

    /// Whether settings that turn off peer verification are allowed.
    pub fn allows_unverified(&self) -> bool {
        self.unverified
    }

    /// Whether settings that authenticate with the host's own credentials are
    /// allowed.
    pub fn allows_host_credentials(&self) -> bool {
        self.host_credentials
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
        let refuse = |reason: String| Refusal::new(RefusalKind::Path, value, reason);
        match locate(value) {
            Location::Local(local) => self.check_local(&local).map_err(refuse),
            Location::Remote if self.path_roots.is_some() && has_parent_segment(value) => Err(
                refuse("a remote location in a path field may not contain a `..` segment".into()),
            ),
            Location::Remote => self.check_url(value),
            Location::Refused(reason) if self.path_roots.is_some() => Err(refuse(reason.into())),
            Location::Refused(_) => Ok(()),
        }
    }

    fn check_local(&self, path: &str) -> Result<(), String> {
        let Some(roots) = &self.path_roots else {
            return Ok(());
        };
        if path.is_empty() || path == ":memory:" || path == "/dev/null" {
            return Ok(());
        }
        if ambiguous_first_segment(path) {
            return Err(
                "a relative path whose first segment contains `:` reads as a URL scheme; \
                 write it as `./…` or use an absolute path"
                    .into(),
            );
        }
        if path.starts_with('~') {
            return Err(
                "a path starting with `~` may be expanded to a home directory; \
                 use an absolute path"
                    .into(),
            );
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

    /// Check a URL a connector connects to. `file:` / `sqlite:` / `unix:` URLs
    /// are checked as paths, a scheme-less `host[:port]` or IP address as a
    /// host, a `//host/…` network-path reference by its host, and a relative
    /// reference (`/jobs`, `./x`) names no host of its own and passes. Under a
    /// policy with path roots (one for configs the host does not trust), a
    /// value from which no server can be read is refused.
    pub fn check_url(&self, value: &str) -> Result<(), Refusal> {
        self.check_url_as(value, None)
    }

    /// [`check_url`](Self::check_url) for a value written in `syntax`
    /// ([`URL_SYNTAX_ANNOTATION`]), read the way the connector's client reads
    /// it. Under a policy with path roots, a value the client's grammar cannot
    /// read is refused.
    pub fn check_url_as(&self, value: &str, syntax: Option<UrlSyntax>) -> Result<(), Refusal> {
        let raw = value;
        let value = value.trim_matches(|c: char| c.is_ascii_whitespace());
        if self.path_roots.is_some() && value.len() > MAX_SCREENED_LEN {
            return Err(Refusal::new(
                RefusalKind::Url,
                &value[..value.floor_char_boundary(64)],
                format!("a value longer than {MAX_SCREENED_LEN} bytes is not read"),
            ));
        }
        match syntax {
            None => self.check_plain_url(value),
            Some(UrlSyntax::Ado) => self.check_ado(raw),
            Some(UrlSyntax::Oracle) => self.check_oracle(value),
            Some(UrlSyntax::AzureConn) => self.check_azure_conn(value),
            Some(UrlSyntax::UrlList) => {
                for item in list_items(value) {
                    self.check_plain_url(item)
                        .map_err(|e| Refusal::new(e.kind, value, e.reason))?;
                }
                Ok(())
            }
        }
    }

    /// Check one host a value names against the host allow-list, or the
    /// link-local / metadata rule.
    fn check_host(&self, subject: &str, host: &str) -> Result<(), Refusal> {
        let refuse = |reason: String| Refusal::new(RefusalKind::Url, subject, reason);
        if host.contains("${") || host.contains('*') {
            if self.url_hosts.is_some() {
                return Err(refuse(
                    "a host taken from a token cannot be checked against the host allow-list"
                        .into(),
                ));
            }
            return Ok(());
        }
        match &self.url_hosts {
            Some(list) if list.iter().any(|p| host_matches(p, host)) => Ok(()),
            Some(_) => Err(refuse(format!("host `{host}` not in the allow-list"))),
            None if is_metadata_host(host) => Err(refuse(format!(
                "host `{host}` is a link-local / metadata endpoint"
            ))),
            None => Ok(()),
        }
    }

    /// Check a URL scheme against the scheme allow-list.
    fn check_scheme(&self, subject: &str, scheme: &str) -> Result<(), Refusal> {
        match &self.url_schemes {
            Some(allowed) if !allowed.contains(scheme) => Err(Refusal::new(
                RefusalKind::Url,
                subject,
                format!("URL scheme `{scheme}` not in the allow-list"),
            )),
            _ => Ok(()),
        }
    }

    /// Check the local file a connection setting `key` names.
    fn check_setting_path(&self, subject: &str, key: &str, path: &str) -> Result<(), Refusal> {
        self.check_local(path).map_err(|reason| {
            Refusal::new(RefusalKind::Path, subject, format!("`{key}`: {reason}"))
        })
    }

    /// Refuse a connection setting that turns off peer verification.
    fn check_setting_verification(&self, subject: &str, key: &str, v: &str) -> Result<(), Refusal> {
        let unverified = DB_UNVERIFIED_PARAMS
            .iter()
            .any(|(k, vals)| *k == key && vals.iter().any(|x| x.eq_ignore_ascii_case(v.trim())));
        if unverified && !self.unverified {
            return Err(Refusal::new(
                RefusalKind::Url,
                subject,
                format!("`{key}={}` turns off certificate verification", v.trim()),
            ));
        }
        Ok(())
    }

    /// Check an ADO.NET connection string, read as the SQL Server driver
    /// reads it ([`crate::ado::parse`]).
    fn check_ado(&self, value: &str) -> Result<(), Refusal> {
        let pairs = match crate::ado::parse(value) {
            Ok(pairs) => pairs,
            Err(e) if self.path_roots.is_some() => {
                return Err(Refusal::new(
                    RefusalKind::Url,
                    value,
                    format!(
                        "the connection string cannot be read the way the driver reads it ({e})"
                    ),
                ));
            }
            Err(_) => return Ok(()),
        };
        for host in ado_hosts(&pairs) {
            self.check_host(value, &host)?;
        }
        for (k, v) in &pairs {
            if DB_PATH_PARAMS.contains(&k.as_str()) && !v.is_empty() {
                self.check_setting_path(value, k, v)?;
            }
            self.check_setting_verification(value, k, v)?;
        }
        Ok(())
    }

    /// Check an Oracle connect string, read as the Oracle client reads it.
    fn check_oracle(&self, value: &str) -> Result<(), Refusal> {
        let confined = self.path_roots.is_some();
        let refuse = |kind: RefusalKind, reason: String| Refusal::new(kind, value, reason);
        let read = read_oracle(value);
        match read.form {
            OracleForm::Empty => return Ok(()),
            OracleForm::Alias if confined => {
                return Err(refuse(
                    RefusalKind::Url,
                    "a net service name is resolved through the server's own Oracle \
                     configuration; write an Easy Connect string or a connect descriptor"
                        .into(),
                ));
            }
            OracleForm::Directory if confined => {
                return Err(refuse(
                    RefusalKind::Url,
                    "directory naming is resolved through the server's own Oracle \
                     configuration; write an Easy Connect string or a connect descriptor"
                        .into(),
                ));
            }
            OracleForm::Alias | OracleForm::Directory => return Ok(()),
            OracleForm::Descriptor | OracleForm::EasyConnect => {}
        }
        let get = |key: &str| {
            read.params
                .iter()
                .filter(|(k, _)| k == key)
                .map(|(_, v)| v.as_str())
                .collect::<Vec<_>>()
        };
        let runs_program = get("protocol")
            .iter()
            .any(|p| p.eq_ignore_ascii_case("beq"))
            || !get("program").is_empty();
        if runs_program && !self.subprocess {
            return Err(refuse(
                RefusalKind::Command,
                "a bequeath connection (`PROTOCOL=BEQ` / `PROGRAM=`) runs a program".into(),
            ));
        }
        if confined
            && get("protocol")
                .iter()
                .any(|p| p.eq_ignore_ascii_case("ipc"))
        {
            return Err(refuse(
                RefusalKind::Url,
                "an IPC connection (`PROTOCOL=IPC`) reaches a local listener by name".into(),
            ));
        }
        if let Some(scheme) = &read.scheme {
            self.check_scheme(value, scheme)?;
        }
        if confined && !runs_program {
            if read.form == OracleForm::Descriptor && get("host").is_empty() {
                return Err(refuse(
                    RefusalKind::Url,
                    "the connect descriptor names no `HOST`; name the server explicitly".into(),
                ));
            }
            if read.hosts.is_empty() {
                return Err(refuse(
                    RefusalKind::Url,
                    "no server could be read from this value; name the server explicitly".into(),
                ));
            }
        }
        for host in &read.hosts {
            self.check_host(value, host)?;
        }
        let token_auth = get("token_auth");
        if let Some(mode) = token_auth.last() {
            let own_identity =
                !mode.eq_ignore_ascii_case("oauth") || get("token_location").is_empty();
            if own_identity && !self.host_credentials {
                return Err(refuse(
                    RefusalKind::HostCredentials,
                    format!(
                        "`token_auth={mode}` authenticates with the server's own identity or \
                         token files; only `OAUTH` with an explicit `token_location` is allowed"
                    ),
                ));
            }
        }
        for (k, v) in &read.params {
            if ORACLE_PATH_PARAMS.contains(&k.as_str()) && !v.is_empty() {
                self.check_setting_path(value, k, v)?;
            }
            if ORACLE_URL_PARAMS.contains(&k.as_str()) && !v.is_empty() {
                self.check_plain_url(v)
                    .map_err(|e| refuse(e.kind, format!("`{k}`: {}", e.reason)))?;
            }
            self.check_setting_verification(value, k, v)?;
        }
        Ok(())
    }

    /// Check an Azure Storage connection string, read as the client reads it
    /// ([`AzureConnSettings`]): every `…Endpoint=` URL, the account endpoint
    /// built from `AccountName` / `EndpointSuffix`, the emulator's loopback
    /// host, the account-name and protocol rules, and — under a policy with
    /// path roots — no key in another case than the one the client reads.
    fn check_azure_conn(&self, value: &str) -> Result<(), Refusal> {
        self.check_azure_conn_hosts(value, None)
    }

    /// [`check_url_as`](Self::check_url_as) for an Azure Storage connection
    /// string whose blob hosts are `hosts` — what the client connects to once
    /// the rest of the connection (account, endpoint, emulator) is applied —
    /// instead of the ones the string alone names. The string's other
    /// endpoints and its rules are checked as usual.
    pub fn check_azure_conn_with_hosts(
        &self,
        value: &str,
        hosts: &[String],
    ) -> Result<(), Refusal> {
        let raw = value;
        let value = value.trim_matches(|c: char| c.is_ascii_whitespace());
        if self.path_roots.is_some() && value.len() > MAX_SCREENED_LEN {
            return self.check_url_as(raw, Some(UrlSyntax::AzureConn));
        }
        self.check_azure_conn_hosts(value, Some(hosts))
    }

    fn check_azure_conn_hosts(&self, value: &str, hosts: Option<&[String]>) -> Result<(), Refusal> {
        let settings = AzureConnSettings::read(value);
        for (k, url) in azure_conn_urls(value) {
            if hosts.is_some() && matches!(k.as_str(), "blobendpoint" | "accountname") {
                continue;
            }
            self.check_plain_url(&url)
                .map_err(|e| Refusal::new(e.kind, value, format!("`{k}`: {}", e.reason)))?;
        }
        match hosts {
            Some(hosts) => {
                for host in hosts {
                    self.check_host(value, host)?;
                }
            }
            None if settings.use_development_storage() => self.check_host(value, "127.0.0.1")?,
            None => {}
        }
        if let Some(problem) = settings.problem() {
            return Err(Refusal::new(RefusalKind::Url, value, problem));
        }
        if self.path_roots.is_some()
            && let Some(key) = settings.other_case_keys().first()
        {
            let canonical = AzureConnKey::ALL
                .iter()
                .find(|k| k.as_str().eq_ignore_ascii_case(key))
                .map_or("", |k| k.as_str());
            return Err(Refusal::new(
                RefusalKind::Url,
                value,
                format!(
                    "`{key}` is not read by the client, which reads `{canonical}`; write the key \
                     in that case"
                ),
            ));
        }
        Ok(())
    }

    /// Check a MongoDB URI, read as the driver reads it ([`MongoRead`]).
    fn check_mongo(&self, value: &str, read: &MongoRead) -> Result<(), Refusal> {
        let confined = self.path_roots.is_some();
        let refuse = |kind: RefusalKind, reason: String| Refusal::new(kind, value, reason);
        self.check_scheme(value, if read.srv { "mongodb+srv" } else { "mongodb" })?;
        let mut server = false;
        for host in &read.hosts {
            match host {
                MongoHost::Tcp(h) => {
                    self.check_host(value, h)?;
                    if read.srv && self.url_hosts.is_some() {
                        let parent = srv_parent(h);
                        self.check_host(value, &format!("x.{parent}"))
                            .map_err(|_| {
                                refuse(
                                    RefusalKind::Url,
                                    format!(
                                        "a `mongodb+srv` lookup of `{h}` may return any host under \
                                     `{parent}`; the host allow-list must admit `*.{parent}`"
                                    ),
                                )
                            })?;
                    }
                    server = true;
                }
                MongoHost::Socket(Some(p)) if confined && !p.starts_with('/') => {
                    return Err(refuse(
                        RefusalKind::Path,
                        "a relative Unix socket path cannot be checked; use an absolute path"
                            .into(),
                    ));
                }
                MongoHost::Socket(Some(p)) => {
                    self.check_setting_path(value, "unix socket", p)?;
                    server = true;
                }
                MongoHost::Socket(None) if confined => {
                    return Err(refuse(
                        RefusalKind::Path,
                        "a percent-encoded path that is not UTF-8 cannot be checked".into(),
                    ));
                }
                MongoHost::Socket(None) | MongoHost::Unreadable => {}
            }
        }
        if confined && !server {
            return Err(refuse(
                RefusalKind::Url,
                "no server could be read from this value; name the server explicitly".into(),
            ));
        }
        for (key, v) in &read.options {
            if MONGO_PATH_OPTIONS.contains(&key.as_str()) {
                self.check_setting_path(value, key, v)?;
            }
            if MONGO_UNVERIFIED_OPTIONS.contains(&key.as_str()) && v != "false" && !self.unverified
            {
                return Err(refuse(
                    RefusalKind::Url,
                    format!("`{key}` turns off certificate verification"),
                ));
            }
            if key == "proxyhost" {
                self.check_host(value, &canonical_host(v))?;
            }
        }
        if read.host_credentials() && !self.host_credentials {
            return Err(refuse(
                RefusalKind::HostCredentials,
                "`authMechanism` / `authMechanismProperties` authenticate with the server's own \
                 identity (cloud, Kubernetes or Kerberos credentials)"
                    .into(),
            ));
        }
        if self.pin_servers && !read.pinned() {
            return Err(refuse(
                RefusalKind::Url,
                "a MongoDB client connects to further servers the one it reaches names; add \
                 `directConnection=true` to use one server (or `loadBalanced=true` behind a load \
                 balancer), or have the hosts it may reach listed in the host allow-list"
                    .into(),
            ));
        }
        Ok(())
    }

    /// Check a value with no declared grammar.
    fn check_plain_url(&self, value: &str) -> Result<(), Refusal> {
        let value = value.trim_matches(|c: char| c.is_ascii_whitespace());
        if let Some(read) = MongoRead::read(value) {
            return self.check_mongo(value, &read);
        }
        if let Some(items) = scheme_list(value) {
            for item in items {
                self.check_plain_url(item)
                    .map_err(|e| Refusal::new(e.kind, value, e.reason))?;
            }
            return Ok(());
        }
        let refuse = |reason: String| Refusal::new(RefusalKind::Url, value, reason);
        let ip = ip_literal(value);
        let scheme = scheme_of(value).filter(|_| !value.starts_with('/') && ip.is_none());
        if let Some(path) = scheme.as_deref().and_then(|s| unix_socket_path(s, value)) {
            return match path {
                Some(path) => self.check_setting_path(value, "unix socket", &path),
                None if self.path_roots.is_some() => Err(Refusal::new(
                    RefusalKind::Path,
                    value,
                    "a percent-encoded path that is not UTF-8 cannot be checked",
                )),
                None => Ok(()),
            };
        }
        match scheme.as_deref() {
            Some("file") | Some("sqlite") => {
                return match locate(value) {
                    Location::Local(local) => self
                        .check_local(&local)
                        .map_err(|reason| Refusal::new(RefusalKind::Path, value, reason)),
                    _ if self.path_roots.is_none() => Ok(()),
                    Location::Refused(reason) => {
                        Err(Refusal::new(RefusalKind::Path, value, reason))
                    }
                    Location::Remote => Err(refuse(
                        "a `file:` URL that names a remote host is not a local path".into(),
                    )),
                };
            }
            None if value.starts_with("//") || ip.is_some() => {}
            None if value.starts_with(['/', '.', '?', '#']) => return Ok(()),
            None if is_relative_path(value)
                && !value.starts_with('(')
                && connection_string_pairs(value).is_none() =>
            {
                return Ok(());
            }
            _ => {}
        }
        if let Some(scheme) = &scheme {
            self.check_scheme(value, scheme)?;
        }
        let hosts = url_hosts(value);
        for host in &hosts {
            self.check_host(value, host)?;
        }
        let params = database_url_params(value);
        let socket_dir = params
            .iter()
            .any(|(k, v)| DB_HOST_PARAMS.contains(&k.as_str()) && v.starts_with('/'));
        if self.path_roots.is_some()
            && hosts.is_empty()
            && !value.is_empty()
            && !socket_dir
            && scheme.as_deref() != Some("memory")
        {
            return Err(refuse(
                "no server could be read from this value; name the server explicitly".into(),
            ));
        }
        for (key, v) in params {
            let names_path = DB_PATH_PARAMS.contains(&key.as_str())
                || (DB_HOST_PARAMS.contains(&key.as_str()) && v.starts_with('/'));
            if names_path {
                self.check_setting_path(value, &key, &v)?;
            }
            self.check_setting_verification(value, &key, &v)?;
        }
        if !self.unverified
            && matches!(scheme.as_deref(), Some("rediss" | "valkeys"))
            && value
                .split_once('#')
                .is_some_and(|(_, f)| f.eq_ignore_ascii_case("insecure"))
        {
            return Err(refuse(
                "the `#insecure` fragment turns off certificate verification".into(),
            ));
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
        self.check_url_resolved_as(value, None)
    }

    /// [`check_url_resolved`](Self::check_url_resolved) for a value written in
    /// `syntax` ([`check_url_as`](Self::check_url_as)).
    pub fn check_url_resolved_as(
        &self,
        value: &str,
        syntax: Option<UrlSyntax>,
    ) -> Result<(), Refusal> {
        self.check_url_as(value, syntax)?;
        if syntax.is_none() && scheme_of(value).is_some_and(|s| s == "file" || s == "sqlite") {
            return Ok(());
        }
        for host in url_hosts_as(value, syntax) {
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
            ResourceKind::Url => self.check_url_resolved_as(&r.value, r.syntax),
            ResourceKind::Path if matches!(locate(&r.value), Location::Remote) => {
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
            ResourceKind::Url => self.check_url_as(&r.value, r.syntax),
            ResourceKind::Command => self.check_command(&r.value),
            ResourceKind::Unverified if self.unverified => Ok(()),
            ResourceKind::Unverified => Err(Refusal::new(
                RefusalKind::Unverified,
                r.value.clone(),
                "settings that turn off certificate or host-key verification are not allowed",
            )),
            ResourceKind::HostCredentials if self.host_credentials => Ok(()),
            ResourceKind::HostCredentials => Err(Refusal::new(
                RefusalKind::HostCredentials,
                r.value.clone(),
                "settings that authenticate with the server's own credentials are not allowed",
            )),
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

/// The key below `value` (empty for `value` itself) and the setting, when
/// `value` turns on what `node`'s `annotation` ([`UNVERIFIED_ANNOTATION`] or
/// [`HOST_CREDENTIALS_ANNOTATION`]) marks.
fn flagged_setting(node: &Value, value: &Value, annotation: &str) -> Option<(String, String)> {
    let in_list = |list: &Value, s: &str| {
        list.as_array().is_some_and(|l| {
            l.iter()
                .filter_map(Value::as_str)
                .any(|v| v.eq_ignore_ascii_case(s.trim()))
        })
    };
    match (node.get(annotation)?, value) {
        (Value::Bool(true), Value::Bool(true)) => Some((String::new(), "true".into())),
        (Value::Bool(true), Value::Object(map)) => {
            let Some(Value::Object(props)) = node.get("properties") else {
                return None;
            };
            let consts: Vec<(&String, &Value)> = props
                .iter()
                .filter_map(|(k, p)| p.get("const").map(|c| (k, c)))
                .collect();
            let (key, tag) = consts.first()?;
            consts
                .iter()
                .all(|(k, c)| map.get(*k) == Some(*c))
                .then(|| ((*key).clone(), tag.as_str().unwrap_or_default().to_string()))
        }
        (list @ Value::Array(_), Value::String(s)) => {
            in_list(list, s).then(|| (String::new(), s.clone()))
        }
        (Value::Object(keys), Value::Object(map)) => map.iter().find_map(|(k, v)| {
            let list = keys
                .iter()
                .find(|(key, _)| key.eq_ignore_ascii_case(k))
                .map(|(_, l)| l)?;
            let s = v.as_str()?;
            in_list(list, s).then(|| (k.clone(), s.to_string()))
        }),
        _ => None,
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
    for (annotation, flagged) in [
        (UNVERIFIED_ANNOTATION, ResourceKind::Unverified),
        (HOST_CREDENTIALS_ANNOTATION, ResourceKind::HostCredentials),
    ] {
        if let Some((key, setting)) = alts
            .iter()
            .find_map(|n| flagged_setting(n, value, annotation))
        {
            let mut field = pointer.trim_start_matches('/').replace('/', ".");
            if !key.is_empty() {
                field = if field.is_empty() {
                    key
                } else {
                    format!("{field}.{key}")
                };
            }
            let text = format!("{field}: {setting}");
            out.push(ResourceRef::new(pointer, flagged, text));
        }
    }
    let kind = alts.iter().find_map(|n| annotation_of(n));
    let syntax = alts.iter().find_map(|n| syntax_of(n));
    match value {
        Value::String(s) => {
            if let Some(kind) = kind {
                out.push(ResourceRef::new(pointer, kind, s.clone()).with_syntax(syntax));
            }
        }
        Value::Array(items) => {
            for (i, item) in items.iter().enumerate() {
                let p = format!("{pointer}/{i}");
                if let (Some(kind), Value::String(s)) = (kind, item) {
                    out.push(ResourceRef::new(p.clone(), kind, s.clone()).with_syntax(syntax));
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
                if let (Some((kind, syntax)), Value::String(s)) =
                    (alts.iter().find_map(|n| keyed_annotation(n, k)), child)
                {
                    out.push(ResourceRef::new(p.clone(), kind, s.clone()).with_syntax(syntax));
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
            for (pattern, entry) in keys {
                if let Some((kind, _)) = keyed_entry(entry) {
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

/// Every field a schema marks with [`URL_SYNTAX_ANNOTATION`], as dotted paths
/// (the same form as [`annotated_fields`]) with their grammar.
pub fn url_syntax_fields(schema: &Value) -> Vec<(String, UrlSyntax)> {
    fn walk(
        root: &Value,
        node: &Value,
        path: &str,
        out: &mut BTreeSet<(String, UrlSyntax)>,
        depth: usize,
    ) {
        if depth > 12 {
            return;
        }
        let mut alts = Vec::new();
        alternatives(root, node, &mut alts, 0);
        if let Some(syntax) = alts.iter().find_map(|n| syntax_of(n))
            && !path.is_empty()
        {
            out.insert((path.to_string(), syntax));
        }
        for alt in alts {
            if let Some(Value::Object(props)) = alt.get("properties") {
                for (k, s) in props {
                    let p = if path.is_empty() {
                        k.clone()
                    } else {
                        format!("{path}.{k}")
                    };
                    walk(root, s, &p, out, depth + 1);
                }
            }
            if let Some(items) = alt.get("items").filter(|v| v.is_object()) {
                walk(root, items, &format!("{path}[]"), out, depth + 1);
            }
        }
    }
    let mut out = BTreeSet::new();
    walk(schema, schema, "", &mut out, 0);
    out.into_iter().collect()
}

/// Whether a schema declares at least one field of `kind`. For
/// [`ResourceKind::Command`] (a connector that runs a program), a key pattern
/// of a free-form map ([`RESOURCE_KEYS_ANNOTATION`]) does not count: such a
/// key runs a program only when a config sets it, which
/// [`annotated_resources`] reports.
pub fn declares(schema: &Value, kind: ResourceKind) -> bool {
    annotated_fields(schema).iter().any(|(path, k)| {
        *k == kind
            && (kind != ResourceKind::Command || !path.ends_with('}') || path.ends_with("{}"))
    })
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
    flagged_fields(schema, SANDBOX_ANNOTATION)
}

/// The JSON Schema keyword (value `true`) on a top-level string config field
/// whose value is a SQL statement the connector runs as written (a source's
/// `query`). A host that places values it did not author into a config uses
/// it to tell a SQL statement from a field that only shares its name (a
/// GraphQL document, a search or document-store filter):
///
/// ```ignore
/// #[schemars(extend("x-faucet-sql" = true))]
/// pub query: String,
/// ```
pub const SQL_ANNOTATION: &str = "x-faucet-sql";

/// The top-level config fields `schema` marks with [`SQL_ANNOTATION`],
/// across every `oneOf` / `anyOf` / `allOf` branch of the root.
pub fn sql_fields(schema: &Value) -> Vec<String> {
    flagged_fields(schema, SQL_ANNOTATION)
}

/// The top-level config fields `schema` marks with the boolean `annotation`.
fn flagged_fields(schema: &Value, annotation: &str) -> Vec<String> {
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
                .any(|n| n.get(annotation) == Some(&Value::Bool(true)))
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
        assert_eq!(local_part("file://localhost").as_deref(), Some(""));
        assert_eq!(local_part("file://").as_deref(), Some(""));
        assert_eq!(local_part("file://./data/x").as_deref(), Some("./data/x"));
        assert_eq!(local_part("file://../x").as_deref(), Some("../x"));
        assert_eq!(local_part("file://host/x"), None);
    }
    use super::*;

    #[test]
    fn mongodb_and_oracle_values_name_the_hosts_identity() {
        assert!(mongodb_uri_uses_host_identity(
            "mongodb://db.example/?authMechanism=MONGODB-AWS"
        ));
        assert!(mongodb_uri_uses_host_identity(
            "mongodb+srv://c.example/?authMechanism=MONGODB-OIDC&authMechanismProperties=ENVIRONMENT:k8s"
        ));
        assert!(!mongodb_uri_uses_host_identity("mongodb://u:p@db.example/"));
        assert!(!mongodb_uri_uses_host_identity("not a uri"));
        assert!(oracle_connect_uses_host_identity(
            "tcps://db.example:2484/svc?token_auth=OCI_IAM"
        ));
        assert!(oracle_connect_uses_host_identity(
            "tcps://db.example:2484/svc?token_auth=OAUTH"
        ));
        assert!(!oracle_connect_uses_host_identity(
            "tcps://db.example:2484/svc?token_auth=OAUTH&token_location=/t"
        ));
        assert!(!oracle_connect_uses_host_identity("db.example:1521/svc"));
    }
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
    fn database_url_query_parameters_are_screened() {
        assert_eq!(
            url_hosts("postgres://u@db.example.com/x?host=169.254.169.254,b.example.com"),
            vec!["db.example.com", "169.254.169.254", "b.example.com"]
        );
        assert_eq!(
            url_hosts("postgres:///x?host=/var/run/postgresql"),
            Vec::<String>::new()
        );
        assert_eq!(
            url_hosts("https://api.example.com/x?host=169.254.169.254"),
            vec!["api.example.com"]
        );
        let caller = ReferencePolicy::deny_all()
            .with_path_roots(["/srv/data"])
            .any_url_scheme()
            .any_url_host();
        for url in [
            "postgres://u@db.example.com/x?hostaddr=169.254.169.254",
            "postgresql://u@db.example.com/x?sslkey=/etc/ssl/private/k.pem",
            "postgres://u@db.example.com/x?SSLROOTCERT=%2Fetc%2Fca.pem",
            "postgres:///x?host=/etc",
            "mysql://u@db.example.com/x?socket=/var/run/other.sock",
            "mysql://u@db.example.com/x?ssl-ca=/etc/ca.pem",
            "mysql://u@db.example.com/x?require_ssl=true&verify_ca=false",
            "mysql://u@db.example.com/x?verify_identity=0",
            "rediss://cache.example.com:6380/0#insecure",
        ] {
            assert!(caller.check_url(url).is_err(), "{url}");
            assert_eq!(
                ReferencePolicy::open().check_url(url).is_ok(),
                !url.contains("169.254"),
                "open policy: {url}"
            );
        }
        for url in [
            "postgres://u@db.example.com/x?sslmode=verify-full&sslrootcert=/srv/data/ca.pem",
            "postgres://u@db.example.com/x?host=db2.example.com",
            "mysql://u@db.example.com/x?require_ssl=true&verify_ca=true",
            "https://api.example.com/x?sslkey=/etc/k.pem",
            "rediss://cache.example.com:6380/0",
        ] {
            assert!(caller.check_url(url).is_ok(), "{url}");
        }
        for cs in [
            "Server=169.254.169.254;Database=x;Note=a/b",
            "Server=db.example.com;TrustServerCertificateCA=/etc/ssl/ca.pem",
            "Server=db.example.com;TrustServerCertificate=yes",
            "host=db.example.com sslkey=/etc/k.pem",
        ] {
            assert!(caller.check_url(cs).is_err(), "{cs}");
        }
        assert!(
            caller
                .check_url("Server=db.example.com;TrustServerCertificateCA=/srv/data/ca.pem")
                .is_ok()
        );
        assert!(caller.check_url("data/in.csv").is_ok());
        let descriptor = "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=db.example.com)(PORT=2484))\
             (CONNECT_DATA=(SERVICE_NAME=svc)))";
        assert_eq!(url_hosts(descriptor), vec!["db.example.com"]);
        assert!(caller.check_url(descriptor).is_ok());
        for bad in [
            "(DESCRIPTION=(ADDRESS=(HOST=169.254.169.254)(PORT=1521)))",
            "(DESCRIPTION=(ADDRESS=(HOST=metadata))(SECURITY=(MY_WALLET_DIRECTORY=/srv/data)))",
            "(DESCRIPTION=(ADDRESS=(HOST=db.example.com))(SECURITY=(MY_WALLET_DIRECTORY=/etc/w)))",
            "(DESCRIPTION=(ADDRESS=(HOST=db.example.com))(SECURITY=(SSL_SERVER_DN_MATCH=FALSE)))",
            "tcps://db.example.com:2484/svc?wallet_location=/etc/w",
            "tcps://db.example.com:2484/svc?ssl_server_dn_match=off",
        ] {
            assert!(caller.check_url(bad).is_err(), "{bad}");
        }
        let lenient = caller.clone().with_unverified(true);
        assert!(
            lenient
                .check_url("Server=db.example.com;TrustServerCertificate=true")
                .is_ok()
        );
        assert!(
            lenient
                .check_url("mysql://u@h.example.com/x?verify_ca=false")
                .is_ok()
        );
        assert!(
            lenient
                .check_url("rediss://h.example.com/0#insecure")
                .is_ok()
        );
        let err = caller
            .check_url("postgres://u@h.example.com/x?sslkey=/etc/k.pem")
            .unwrap_err();
        assert_eq!(err.kind, RefusalKind::Path);
        assert!(err.reason.starts_with("`sslkey`"), "{err}");
    }

    #[test]
    fn settings_that_turn_off_verification_are_reported_and_follow_the_policy() {
        let s = json!({
            "type": "object",
            "properties": {
                "tls": {"$ref": "#/$defs/Tls"},
                "mode": {"type": "string", "x-faucet-unverified": ["trust_server_certificate"]},
                "host_key": {"oneOf": [
                    {"type": "object", "properties": {"mode": {"const": "strict"}}},
                    {"type": "object", "properties": {"mode": {"const": "insecure"}},
                     "x-faucet-unverified": true}
                ]},
                "extra": {"type": "object", "additionalProperties": {"type": "string"},
                          "x-faucet-unverified": {"enable.ssl.certificate.verification": ["false"]}}
            },
            "$defs": {"Tls": {"type": "object", "properties": {
                "insecure_skip_verify": {"type": "boolean", "x-faucet-unverified": true}
            }}}
        });
        let quiet = json!({
            "tls": {"insecure_skip_verify": false},
            "mode": "require",
            "host_key": {"mode": "strict"},
            "extra": {"enable.ssl.certificate.verification": "true"}
        });
        assert!(annotated_resources(&s, &quiet).is_empty());
        let loud = json!({
            "tls": {"insecure_skip_verify": true},
            "mode": "Trust_Server_Certificate",
            "host_key": {"mode": "insecure"},
            "extra": {"Enable.SSL.Certificate.Verification": "FALSE"}
        });
        let found = annotated_resources(&s, &loud);
        let mut found = found;
        found.sort_by(|a, b| a.pointer.cmp(&b.pointer));
        let got: Vec<(&str, &str)> = found
            .iter()
            .map(|r| (r.pointer.as_str(), r.value.as_str()))
            .collect();
        assert_eq!(
            got,
            vec![
                ("/extra", "extra.Enable.SSL.Certificate.Verification: FALSE"),
                ("/host_key", "host_key.mode: insecure"),
                ("/mode", "mode: Trust_Server_Certificate"),
                (
                    "/tls/insecure_skip_verify",
                    "tls.insecure_skip_verify: true"
                ),
            ]
        );
        assert!(found.iter().all(|r| r.kind == ResourceKind::Unverified));
        assert_eq!(ResourceKind::Unverified.to_string(), "unverified");
        let r = &found[0];
        assert!(ReferencePolicy::open().check(r).is_ok());
        assert!(ReferencePolicy::open().allows_unverified());
        let refused = ReferencePolicy::deny_all().check(r).unwrap_err();
        assert_eq!(refused.kind, RefusalKind::Unverified);
        assert!(refused.subject.starts_with("extra."), "{refused}");
        assert!(
            ReferencePolicy::deny_all()
                .with_unverified(true)
                .check_resolved(r)
                .is_ok()
        );
        let os = json!({"properties": {
            "external_auth": {"type": "boolean", "x-faucet-host-credentials": true}
        }});
        assert!(annotated_resources(&os, &json!({"external_auth": false})).is_empty());
        let found = annotated_resources(&os, &json!({"external_auth": true}));
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].kind, ResourceKind::HostCredentials);
        assert_eq!(found[0].value, "external_auth: true");
        assert_eq!(ResourceKind::HostCredentials.as_str(), "host_credentials");
        let refused = ReferencePolicy::deny_all().check(&found[0]).unwrap_err();
        assert_eq!(refused.kind, RefusalKind::HostCredentials);
        assert!(ReferencePolicy::open().check(&found[0]).is_ok());
        assert!(ReferencePolicy::open().allows_host_credentials());
        assert!(
            ReferencePolicy::deny_all()
                .with_host_credentials(true)
                .check(&found[0])
                .is_ok()
        );
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
        assert!(sql_fields(&s).is_empty());
    }

    #[test]
    fn sql_fields_are_read_from_the_root_and_its_branches() {
        let s = json!({
            "type": "object",
            "properties": {
                "query": {"type": "string", "x-faucet-sql": true},
                "filter": {"type": "string"},
                "sandbox": {"type": "boolean", "x-faucet-sandbox": true}
            },
            "oneOf": [{"properties": {"sql": {"$ref": "#/$defs/Sql"}}}],
            "$defs": {"Sql": {"type": "string", "x-faucet-sql": true}}
        });
        assert_eq!(sql_fields(&s), vec!["query", "sql"]);
        assert_eq!(sandbox_fields(&s), vec!["sandbox"]);
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
            "http://[fd20:ce::254]/",
            "http://[fd00:c1::a9fe:a9fe]/",
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
    fn a_scheme_a_path_field_does_not_open_remotely_is_a_path() {
        let dir = std::env::temp_dir().join(format!("faucet-scheme-{}", std::process::id()));
        std::fs::create_dir_all(dir.join("zz:")).unwrap();
        let p = ReferencePolicy::deny_all()
            .any_url_scheme()
            .any_url_host()
            .with_path_roots([&dir]);
        for bad in [
            "zz:/x.jsonl",
            "zz:/../../escaped.jsonl",
            "zz:/../../../../etc/hosts",
            "zz://../../x.jsonl",
            "Http:x",
            "s3:/bucket/key",
            "~/x.csv",
            "~",
        ] {
            let e = p.check_path(bad).unwrap_err();
            assert_eq!(e.kind, RefusalKind::Path, "{bad}");
        }
        for bad in [
            "s3://bucket/../x",
            "s3://bucket/%2e%2E/x",
            "https://h/a/..\\b",
        ] {
            assert!(p.check_path(bad).is_err(), "{bad}");
        }
        let d = dir.display();
        assert!(p.check_path(&format!("{d}/zz:/x.jsonl")).is_ok());
        assert!(p.check_path("s3://bucket/key").is_ok());
        assert!(p.check_path("https://h/a/b.csv").is_ok());
        assert!(
            p.check_path(&format!("{d}/a/2026-01-01T00:00:00.jsonl"))
                .is_ok()
        );
        assert!(!ambiguous_first_segment("C:/data/x"));
        assert!(!ambiguous_first_segment("/abs/zz:/x"));
        assert!(ReferencePolicy::open().check_path("zz:/x").is_ok());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn duckdb_and_sqlite_prefixes_are_read_as_their_drivers_read_them() {
        let dir = std::env::temp_dir().join(format!("faucet-sqlite-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = ReferencePolicy::deny_all().with_path_roots([&dir]);
        let d = dir.display().to_string();
        let enc = d.replace('/', "%2F");
        for ok in [
            format!("sqlite://{enc}%2Fx.db"),
            format!("sqlite:{d}/x%20y.db?mode=rwc"),
            format!("duckdb://{d}/x.duckdb"),
            format!("duckdb:{d}/x.duckdb"),
            "duckdb::memory:".to_string(),
            "sqlite::memory:".to_string(),
            "sqlite://?mode=memory".to_string(),
        ] {
            assert!(p.check_path(&ok).is_ok(), "{ok}");
        }
        for bad in [
            "sqlite://%2Fetc%2Fx.db".to_string(),
            format!("sqlite://{d}/%2e%2e/x.db"),
            format!("sqlite://{d}/%2E%2E%2F%2E%2E%2Fx.db"),
            format!("sqlite://file:{d}/x.db"),
            format!("sqlite:FILE:{enc}%2Fx.db"),
            "sqlite://%FF.db".to_string(),
            "SQLITE:///etc/x.db".to_string(),
            "duckdb:/etc/x.duckdb".to_string(),
            "duckdb://~/x.duckdb".to_string(),
            "DUCKDB:x.duckdb".to_string(),
            format!("file://{enc}%2F..%2F..%2Fetc"),
            "file:///%FF".to_string(),
        ] {
            assert!(p.check_path(&bad).is_err(), "{bad}");
        }
        assert!(p.check_url(&format!("sqlite://file:{d}/x.db")).is_err());
        assert!(
            ReferencePolicy::open()
                .check_url("sqlite://file:x.db")
                .is_ok()
        );
        assert!(
            ReferencePolicy::open()
                .check_path("sqlite://%FF.db")
                .is_ok()
        );
        assert_eq!(
            local_path("sqlite://%2Ftmp%2Fx.db?mode=ro").as_deref(),
            Some("/tmp/x.db")
        );
        assert_eq!(local_path("s3://b/k"), None);
        assert_eq!(local_path("duckdb://x.duckdb").as_deref(), Some("x.duckdb"));
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
            "http://[::169.254.169.254]/latest",
            "http://[64:ff9b::a9fe:a9fe]/latest",
            "postgres://u@db,2852039166:5432/x",
            "2852039166/latest/meta-data",
            "0xa9fea9fe/latest",
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
        assert!(listed.check_url("2130706433/x").is_err());
        assert!(listed.check_url("jobs/123").is_ok());
        assert!(p.check_url("jobs/123").is_ok());
        assert!(!is_metadata_ip("::1".parse().unwrap()));
        assert!(!is_metadata_ip("64:ff9b::808:808".parse().unwrap()));
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

    fn confined() -> ReferencePolicy {
        ReferencePolicy::deny_all()
            .with_path_roots(["/srv/data"])
            .any_url_scheme()
            .any_url_host()
    }

    fn refused(syntax: Option<UrlSyntax>, value: &str) -> Refusal {
        confined()
            .check_url_as(value, syntax)
            .expect_err(&format!("{value} should be refused"))
    }

    fn accepted(syntax: Option<UrlSyntax>, value: &str) {
        if let Err(e) = confined().check_url_as(value, syntax) {
            panic!("{value} should be accepted: {}", e.reason);
        }
    }

    const ADO: Option<UrlSyntax> = Some(UrlSyntax::Ado);
    const ORACLE: Option<UrlSyntax> = Some(UrlSyntax::Oracle);
    const AZURE: Option<UrlSyntax> = Some(UrlSyntax::AzureConn);
    const LIST: Option<UrlSyntax> = Some(UrlSyntax::UrlList);

    #[test]
    fn url_syntax_round_trips_and_is_read_from_the_schema() {
        for s in [
            UrlSyntax::Ado,
            UrlSyntax::Oracle,
            UrlSyntax::AzureConn,
            UrlSyntax::UrlList,
        ] {
            assert_eq!(UrlSyntax::parse(s.as_str()), Some(s));
            assert_eq!(s.to_string(), s.as_str());
        }
        assert_eq!(UrlSyntax::parse("jdbc"), None);
        let schema = serde_json::json!({
            "type": "object",
            "properties": {
                "connection_string": {
                    "type": "string",
                    "x-faucet-resource": "url",
                    "x-faucet-url-syntax": "ado"
                },
                "servers": {
                    "type": "array",
                    "items": {"type": "string"},
                    "x-faucet-resource": "url",
                    "x-faucet-url-syntax": "url-list"
                },
                "plain": {"type": "string", "x-faucet-resource": "url"}
            }
        });
        assert_eq!(
            url_syntax_fields(&schema),
            vec![
                ("connection_string".to_string(), UrlSyntax::Ado),
                ("servers".to_string(), UrlSyntax::UrlList),
            ]
        );
        let config = serde_json::json!({
            "connection_string": ";Server=169.254.169.254",
            "servers": ["nats://ok.example.com,nats://169.254.169.254"],
            "plain": "https://api.example.com"
        });
        let refs = annotated_resources(&schema, &config);
        let syntax = |p: &str| refs.iter().find(|r| r.pointer == p).unwrap().syntax;
        assert_eq!(syntax("/connection_string"), ADO);
        assert_eq!(syntax("/servers/0"), LIST);
        assert_eq!(syntax("/plain"), None);
        for r in &refs {
            let plain = r.pointer == "/plain";
            assert_eq!(confined().check(r).is_ok(), plain, "{}", r.pointer);
            if !plain {
                assert!(ReferencePolicy::open().check_resolved(r).is_err());
            }
        }
    }

    #[test]
    fn ado_strings_are_read_untrimmed_like_the_driver() {
        assert_eq!(url_hosts_as("\u{b}=;;", ADO), vec!["localhost".to_string()]);
        assert_eq!(
            url_hosts_as("\u{b}Server=169.254.169.254;", ADO),
            vec!["localhost".to_string()]
        );
        let caller = ReferencePolicy::deny_all()
            .with_path_roots(["/srv/data"])
            .any_url_scheme()
            .any_url_host();
        assert!(caller.check_url_as("\u{b}=;;", ADO).is_ok());
        assert!(
            caller
                .check_url_as(" Server=169.254.169.254;", ADO)
                .is_err()
        );
    }

    #[test]
    fn ado_strings_with_leading_separators_name_their_server() {
        for cs in [
            ";Server=tcp:169.254.169.254,1433;User Id=sa;Password=x",
            " ; Server=169.254.169.254;User Id=sa",
            ";;Data Source='169.254.169.254';User Id=sa",
            "Data Source={169.254.169.254\\inst,1433}",
        ] {
            assert!(refused(ADO, cs).reason.contains("169.254.169.254"), "{cs}");
            assert!(
                ReferencePolicy::open().check_url_as(cs, ADO).is_err(),
                "{cs}"
            );
            assert!(
                url_hosts_as(cs, ADO).contains(&"169.254.169.254".to_string()),
                "{cs}"
            );
        }
        accepted(
            ADO,
            "Server=tcp:db.example.com,1433;Database=x;User Id=sa;Password='p;w'",
        );
        accepted(
            ADO,
            ";Server=db.example.com;TrustServerCertificateCA=/srv/data/ca.pem",
        );
    }

    #[test]
    fn ado_strings_with_any_first_key_name_their_files() {
        for cs in [
            "App.Name=x;Server=db.example.com;TrustServerCertificateCA=/etc/x.pem",
            "a==b=1;Server=db.example.com;TrustServerCertificateCA=/etc/x.pem",
            "Server=db.example.com;TrustServerCertificateCA={/etc/x.pem}",
            "Server=db.example.com; TrustServerCertificateCA = '/etc/x.pem' ",
            "Server=db.example.com;TrustServerCertificateCA=\"/etc/x\"\"y.pem\"",
        ] {
            let e = refused(ADO, cs);
            assert_eq!(e.kind, RefusalKind::Path, "{cs}");
            assert!(e.reason.contains("trustservercertificateca"), "{cs}");
        }
        let e = refused(ADO, "Server=db.example.com;TrustServerCertificate= yes");
        assert!(e.reason.contains("turns off certificate verification"));
        assert!(
            confined()
                .with_unverified(true)
                .check_url_as("Server=db.example.com;TrustServerCertificate=true", ADO)
                .is_ok()
        );
    }

    #[test]
    fn ado_strings_the_driver_cannot_read_are_refused_for_callers() {
        for cs in ["Server=h;junk", "Server='open", "Server=hé", "k==v"] {
            assert!(
                refused(ADO, cs)
                    .reason
                    .contains("the way the driver reads it")
            );
            assert!(
                ReferencePolicy::open().check_url_as(cs, ADO).is_ok(),
                "{cs}"
            );
            assert!(url_hosts_as(cs, ADO).is_empty());
        }
        accepted(ADO, "Database=x");
        accepted(ADO, "");
    }

    #[test]
    fn easy_connect_network_path_names_its_host() {
        for v in [
            "//169.254.169.254:1521/svc",
            "  //169.254.169.254:1521/svc  ",
            "169.254.169.254/svc",
            "tcps://169.254.169.254:2484/svc",
        ] {
            assert!(refused(ORACLE, v).reason.contains("169.254.169.254"), "{v}");
        }
        assert!(
            refused(None, "//169.254.169.254:1521/svc")
                .reason
                .contains("169.254")
        );
        assert_eq!(
            url_hosts("//db.example.com:1521/svc"),
            vec!["db.example.com"]
        );
        accepted(ORACLE, "//db.example.com:1521/FREEPDB1");
        accepted(ORACLE, "db.example.com:1521/FREEPDB1");
        accepted(
            ORACLE,
            "tcps://db.example.com:2484/svc?ssl_server_dn_match=on",
        );
        accepted(None, "/jobs/1");
        let tcps = ReferencePolicy::open().with_url_schemes(["tcp"]);
        assert!(
            tcps.check_url_as("tcps://db.example.com:2484/s", ORACLE)
                .is_err()
        );
    }

    #[test]
    fn easy_connect_plus_parameters_without_scheme_are_read() {
        let e = refused(
            ORACLE,
            "db.example.com:2484/svc?wallet_location=/etc/oracle/wallet",
        );
        assert_eq!(e.kind, RefusalKind::Path);
        assert!(e.reason.contains("wallet_location"));
        assert!(
            refused(ORACLE, "db.example.com/svc?WALLET_LOCATION=\"/etc/w\"")
                .reason
                .contains("wallet_location")
        );
        assert!(
            refused(ORACLE, "db.example.com/svc?oci_config_file=/etc/oci/config")
                .reason
                .contains("oci_config_file")
        );
        assert!(
            refused(
                ORACLE,
                "db.example.com/svc?oci_iam_url=http://169.254.169.254/x"
            )
            .reason
            .contains("oci_iam_url")
        );
        assert!(
            refused(ORACLE, "db.example.com/svc?ssl_server_dn_match=off")
                .reason
                .contains("turns off")
        );
        accepted(
            ORACLE,
            "db.example.com:2484/svc?wallet_location=/srv/data/wallet&retry_count=3",
        );
    }

    #[test]
    fn easy_connect_address_lists_name_every_host() {
        for v in [
            "db.example.com:1521;169.254.169.254:1521/svc",
            "db.example.com,169.254.169.254:1521/svc",
            "[fe80::1]:1521/svc",
        ] {
            assert!(refused(ORACLE, v).reason.contains("link-local"), "{v}");
        }
        assert_eq!(
            url_hosts_as("a.example.com,b.example.com:1521;c.example.com/s", ORACLE),
            vec!["a.example.com", "b.example.com", "c.example.com"]
        );
        accepted(ORACLE, "a.example.com:1521;b.example.com:1521/svc");
    }

    #[test]
    fn descriptor_proxy_is_a_host() {
        let v = "(DESCRIPTION=(ADDRESS=(HTTPS_PROXY=169.254.169.254)(HTTPS_PROXY_PORT=80)\
                 (PROTOCOL=TCPS)(HOST=db.example.com)(PORT=443)))";
        assert!(refused(ORACLE, v).reason.contains("169.254.169.254"));
        assert!(
            refused(ORACLE, "h:1/s?https_proxy=169.254.169.254")
                .reason
                .contains("169")
        );
        assert!(
            refused(
                ORACLE,
                "( description = ( address = ( host = 169.254.169.254 )( port = 1521 ) ) )"
            )
            .reason
            .contains("169.254.169.254")
        );
        assert!(
            refused(
                ORACLE,
                "(DESCRIPTION=(ADDRESS=(HOST=db.example.com,169.254.169.254)))"
            )
            .reason
            .contains("169.254.169.254")
        );
        assert!(
            refused(
                None,
                "tcps://db.example.com:2484/s?https_proxy=169.254.169.254"
            )
            .reason
            .contains("169.254.169.254")
        );
        let ok = "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=db.example.com)(PORT=2484))\
                  (CONNECT_DATA=(SERVICE_NAME=s))(SECURITY=(MY_WALLET_DIRECTORY=/srv/data/w)))";
        accepted(ORACLE, ok);
        let e = refused(ORACLE, "(DESCRIPTION=(CONNECT_DATA=(SERVICE_NAME=s)))");
        assert!(e.reason.contains("names no `HOST`"));
        assert_eq!(
            url_hosts_as("(DESCRIPTION=(ADDRESS=(HOST=fe80::1)))", ORACLE),
            vec!["[fe80::1]"]
        );
        assert_eq!(
            url_hosts_as("https://api.example.com/x", None),
            vec!["api.example.com"]
        );
    }

    #[test]
    fn token_location_is_a_path() {
        let v = "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=db.example.com)(PORT=2484))\
                 (SECURITY=(TOKEN_AUTH=OAUTH)(TOKEN_LOCATION=/etc/oci/tok)))";
        let e = refused(ORACLE, v);
        assert_eq!(e.kind, RefusalKind::Path);
        assert!(e.reason.contains("token_location"));
        assert!(
            refused(
                None,
                "tcps://db.example.com:2484/s?token_location=/etc/oci/token"
            )
            .reason
            .contains("token_location")
        );
        accepted(
            ORACLE,
            "db.example.com:2484/s?token_auth=OAUTH&token_location=/srv/data/token",
        );
    }

    #[test]
    fn token_auth_uses_host_credentials() {
        for v in [
            "db.example.com:2484/s?token_auth=OCI_INSTANCE_PRINCIPAL",
            "db.example.com:2484/s?token_auth=AZURE_MANAGED_IDENTITY",
            "db.example.com:2484/s?token_auth=OAUTH",
            "(DESCRIPTION=(ADDRESS=(HOST=db.example.com))(SECURITY=(TOKEN_AUTH=OCI_TOKEN)))",
        ] {
            assert_eq!(refused(ORACLE, v).kind, RefusalKind::HostCredentials, "{v}");
            assert!(
                confined()
                    .with_host_credentials(true)
                    .check_url_as(v, ORACLE)
                    .is_ok(),
                "{v}"
            );
        }
    }

    #[test]
    fn bequeath_runs_a_program() {
        let beq = "(DESCRIPTION=(ADDRESS=(PROTOCOL=BEQ)(PROGRAM=/bin/sh)(ARGV0=sh))\
                   (CONNECT_DATA=(SID=x)))";
        assert_eq!(refused(ORACLE, beq).kind, RefusalKind::Command);
        assert_eq!(
            refused(ORACLE, "(DESCRIPTION=(ADDRESS=(PROTOCOL=beq)))").kind,
            RefusalKind::Command
        );
        assert!(
            confined()
                .with_subprocess(true)
                .check_url_as(beq, ORACLE)
                .is_ok()
        );
        assert!(ReferencePolicy::open().check_url_as(beq, ORACLE).is_ok());
        let ipc = "(DESCRIPTION=(ADDRESS=(PROTOCOL=IPC)(KEY=x)))";
        assert!(refused(ORACLE, ipc).reason.contains("IPC"));
        assert!(ReferencePolicy::open().check_url_as(ipc, ORACLE).is_ok());
    }

    #[test]
    fn names_resolved_by_the_servers_oracle_configuration_are_refused_for_callers() {
        for v in [
            "orclpdb_alias",
            "ldap://ldap.example.com:389/cn=orcl",
            "LDAP:orcl",
        ] {
            assert!(
                refused(ORACLE, v).reason.contains("Oracle configuration"),
                "{v}"
            );
            assert!(
                ReferencePolicy::open().check_url_as(v, ORACLE).is_ok(),
                "{v}"
            );
        }
        accepted(ORACLE, "   ");
        assert!(refused(ORACLE, ":1521/svc").reason.contains("no server"));
    }

    #[test]
    fn azure_connection_strings_check_every_endpoint() {
        for v in [
            "TableEndpoint=https://ok.example.com/;BlobEndpoint=http://169.254.169.254/",
            "AccountName=a;AccountKey=k==;QueueEndpoint=http://169.254.169.254/",
        ] {
            assert!(refused(AZURE, v).reason.contains("endpoint`: host"), "{v}");
        }
        let https_only = ReferencePolicy::open().with_url_schemes(["https"]);
        assert!(
            https_only
                .check_url_as("AccountName=a;DefaultEndpointsProtocol=http", AZURE)
                .is_err()
        );
        assert_eq!(
            url_hosts_as(
                "TableEndpoint=https://ok.example.com/;BlobEndpoint=http://169.254.169.254/",
                AZURE
            ),
            vec!["ok.example.com", "169.254.169.254"]
        );
        accepted(
            AZURE,
            "DefaultEndpointsProtocol=https;AccountName=acct;AccountKey=k==;\
             EndpointSuffix=core.windows.net",
        );
        accepted(AZURE, "UseDevelopmentStorage=true");
        accepted(AZURE, "SharedAccessSignature=sv=2024&sig=x");
        let only = ReferencePolicy::open().with_url_hosts(["acct.blob.core.windows.net"]);
        assert!(
            only.check_url_as("UseDevelopmentStorage=true", AZURE)
                .is_err()
        );
        assert!(only.check_url_as("AccountName=acct", AZURE).is_ok());
    }

    #[test]
    fn azure_string_hosts_follow_the_client_reading() {
        let v = "AccountName=acct;EndpointSuffix=evil.example;AccountKey=k==;\
                 accountname=ok;endpointsuffix=core.windows.net";
        assert_eq!(url_hosts_as(v, AZURE), vec!["acct.blob.evil.example"]);
        let only = ReferencePolicy::open().with_url_hosts(["*.blob.core.windows.net"]);
        assert!(only.check_url_as(v, AZURE).is_err());
        let suffix = "AccountName=acct;EndpointSuffix=x@evil.example";
        assert_eq!(url_hosts_as(suffix, AZURE), vec!["evil.example"]);
        assert_eq!(
            url_hosts_as("UseDevelopmentStorage=true", AZURE),
            vec!["127.0.0.1"]
        );
        assert_eq!(
            url_hosts_as("AccountName=acct;DefaultEndpointsProtocol=http", AZURE),
            vec!["acct.blob.core.windows.net"]
        );
        for bad in [
            "AccountName=evil.example#;AccountKey=k",
            "AccountName=acct;EndpointSuffix=core.windows.net;\
             DefaultEndpointsProtocol=https://evil.example/?",
        ] {
            let e = ReferencePolicy::open()
                .check_url_as(bad, AZURE)
                .expect_err(bad);
            assert!(!e.reason.contains("evil"), "{}", e.reason);
        }
    }

    #[test]
    fn azure_key_in_another_case_is_refused_for_callers() {
        let v = "AccountName=acct;accountname=other;AccountKey=k==";
        let e = refused(AZURE, v);
        assert!(e.reason.contains("`accountname`"), "{}", e.reason);
        assert!(e.reason.contains("`AccountName`"), "{}", e.reason);
        assert!(!e.reason.contains("other"), "{}", e.reason);
        assert!(ReferencePolicy::open().check_url_as(v, AZURE).is_ok());
        accepted(AZURE, "AccountName=acct;AccountKey=k==");
    }

    #[test]
    fn azure_blob_endpoint_replaces_the_account_host() {
        let v = "AccountName=acct;EndpointSuffix=core.windows.net;BlobEndpoint=https://b.example/";
        assert_eq!(url_hosts_as(v, AZURE), vec!["b.example"]);
        let only = ReferencePolicy::open().with_url_hosts(["b.example"]);
        assert!(only.check_url_as(v, AZURE).is_ok());
        let effective = vec!["10.1.2.3".to_string()];
        let private = ReferencePolicy::open().with_url_hosts(["10.1.2.3"]);
        assert!(
            private
                .check_azure_conn_with_hosts("AccountName=acct;AccountKey=k==", &effective)
                .is_ok()
        );
        assert!(
            private
                .check_url_as("AccountName=acct;AccountKey=k==", AZURE)
                .is_err()
        );
        assert!(
            private
                .check_azure_conn_with_hosts(
                    "AccountName=acct;QueueEndpoint=https://q.example/",
                    &effective
                )
                .is_err()
        );
        assert!(
            private
                .check_azure_conn_with_hosts("AccountName=a#;AccountKey=k==", &effective)
                .is_err()
        );
        let long = "a".repeat(MAX_SCREENED_LEN + 1);
        assert!(confined().check_azure_conn_with_hosts(&long, &[]).is_err());
    }

    #[test]
    fn server_values_read_alike_in_ado_and_plain_strings() {
        for (server, host) in [
            ("tcp:db.example.com,1433", "db.example.com"),
            ("TCP:db.example.com", "db.example.com"),
            ("np:db\\inst", "db"),
            ("(local)", "localhost"),
        ] {
            let plain = url_hosts(&format!("Server={server};Database=x"));
            let ado = url_hosts_as(&format!("Server={server};Database=x"), ADO);
            assert!(plain.contains(&host.to_string()), "{server}: {plain:?}");
            assert!(ado.contains(&host.to_string()), "{server}: {ado:?}");
        }
        assert!(server_value_hosts("/var/run/sock").is_empty());
        assert!(server_value_hosts("np:/pipe").is_empty());
        assert_eq!(
            server_value_hosts("tcp://db.example.com:1433"),
            vec!["db.example.com"]
        );
    }

    #[test]
    fn keyed_entries_can_name_a_grammar() {
        let schema = json!({
            "type": "object",
            "properties": {"extra": {
                "type": "object",
                "additionalProperties": {"type": "string"},
                "x-faucet-resource-keys": {
                    "bootstrap.servers": {"kind": "url", "syntax": "url-list"},
                    "*.location": "path",
                    "ssl.engine.location": "command",
                    "*.cmd": "command",
                    "bad": {"syntax": "url-list"}
                }
            }}
        });
        let config = json!({"extra": {
            "bootstrap.servers": "ok.example.com:9092,169.254.169.254:9092",
            "ssl.engine.location": "/lib/x.so",
            "ssl.ca.location": "/etc/ca.pem",
            "bad": "x"
        }});
        let found = annotated_resources(&schema, &config);
        let find = |p: &str| found.iter().find(|r| r.pointer == p).cloned();
        let brokers = find("/extra/bootstrap.servers").unwrap();
        assert_eq!(brokers.syntax, Some(UrlSyntax::UrlList));
        assert!(ReferencePolicy::open().check(&brokers).is_err());
        assert_eq!(
            find("/extra/ssl.engine.location").unwrap().kind,
            ResourceKind::Command
        );
        assert_eq!(
            find("/extra/ssl.ca.location").unwrap().kind,
            ResourceKind::Path
        );
        assert!(find("/extra/bad").is_none());
        assert!(!declares(&schema, ResourceKind::Command));
        assert!(declares(&schema, ResourceKind::Url));
        let typed =
            json!({"type": "object", "properties": {"cmd": {"x-faucet-resource": "command"}}});
        assert!(declares(&typed, ResourceKind::Command));
        assert!(keyed_entry(&json!(3)).is_none());
    }

    #[test]
    fn url_lists_and_comma_lists_check_every_element() {
        for (syntax, v) in [
            (LIST, "b1.example.com:9092,169.254.169.254:9092"),
            (LIST, "b1.example.com:9092 SSL://169.254.169.254:9093"),
            (
                None,
                "nats://ok.example.com:4222,nats://169.254.169.254:4222",
            ),
            (
                LIST,
                "nats://ok.example.com:4222,nats://169.254.169.254:4222",
            ),
        ] {
            assert!(refused(syntax, v).reason.contains("169.254.169.254"), "{v}");
        }
        assert_eq!(
            url_hosts("nats://a.example.com,nats://b.example.com"),
            vec!["a.example.com", "b.example.com"]
        );
        assert_eq!(
            url_hosts_as("a.example.com:9092, b.example.com:9092", LIST),
            vec!["a.example.com", "b.example.com"]
        );
        accepted(LIST, "b1.example.com:9092,b2.example.com:9092");
        accepted(LIST, "localhost:9092");
        accepted(None, "postgres://u@h1.example.com,h2.example.com/db");
    }

    #[test]
    fn whole_ip_addresses_are_hosts() {
        for v in [
            "fd00:ec2::254",
            "[fd00:ec2::254]",
            " 169.254.169.254 ",
            "fe80::1",
        ] {
            assert!(refused(None, v).reason.contains("link-local"), "{v}");
        }
        assert_eq!(url_hosts("fd00:ec2::254"), vec!["[fd00:ec2::254]"]);
        accepted(None, "10.0.0.5");
        accepted(None, "::1");
    }

    #[test]
    fn unreadable_connection_values_are_refused_for_callers() {
        for v in [
            "postgres:///db",
            "redis://",
            "http://",
            "mongodb://:27017/x",
        ] {
            assert!(refused(None, v).reason.contains("no server"), "{v}");
            assert!(ReferencePolicy::open().check_url(v).is_ok(), "{v}");
        }
        accepted(None, "");
        accepted(None, "postgres:///db?host=/srv/data/pg");
        accepted(None, "redis+unix:///srv/data/redis.sock?db=1");
        accepted(None, "unix:/srv/data/redis.sock");
        accepted(None, "memory://");
        assert_eq!(
            refused(None, "redis+unix:///var/run/redis.sock").kind,
            RefusalKind::Path
        );
        assert_eq!(refused(None, "unix:///x%FF").kind, RefusalKind::Path);
        assert!(ReferencePolicy::open().check_url("unix:///x%FF").is_ok());
        let long = format!("https://h.example.com/{}", "a".repeat(MAX_SCREENED_LEN));
        assert!(refused(None, &long).reason.contains("longer than"));
        assert!(ReferencePolicy::open().check_url(&long).is_ok());
    }

    #[test]
    fn realistic_values_of_every_url_field_are_accepted() {
        for v in [
            "https://api.example.com/v1",
            "http://localhost:8080",
            "postgres://u:p@db.example.com:5432/app?sslmode=require",
            "postgresql://u@db.example.com/app",
            "mysql://u:p@db.example.com:3306/app",
            "mssql://sa:p@db.example.com:1433/app",
            "mongodb+srv://u:p@cluster0.example.net/app?retryWrites=true",
            "mongodb://h1.example.com:27017,h2.example.com:27017/?replicaSet=rs0",
            "redis://cache.example.com:6379/0",
            "rediss://cache.example.com:6380/0",
            "amqp://u:p@rabbit.example.com:5672/%2f",
            "nats://127.0.0.1:4222",
            "rabbit.example.com",
            "db.example.com",
            "localhost:9010",
            "broker1:9092,broker2:9092",
            "http://localhost:8123",
            "https://sqs.us-east-1.amazonaws.com/123456789012/q",
            "http://localhost:4566",
            "https://acct.snowflakecomputing.com",
            "https://dbc-123.cloud.databricks.com",
            "grpc.example.com:443",
            "wss://stream.example.com/feed",
            "https://login.microsoftonline.us",
            "http://127.0.0.1:10000/devstoreaccount1",
            "https://storage.googleapis.com",
            "http://localhost:4443",
            "https://catalog.example.com/api/catalog",
            "thrift://hms.example.com:9083",
            "s3://bucket/prefix/",
            "gs://bucket/prefix",
            "abfss://container@acct.dfs.core.windows.net/path",
            "https://schema-registry.example.com:8081",
            "https://es.example.com:9200",
            "${parent.next_url}",
        ] {
            accepted(None, v);
        }
    }

    #[test]
    fn mongodb_tls_files_follow_the_path_roots() {
        for v in [
            "mongodb://db.example.com/?tlsCAFile=/etc/ssl/private/ca.pem",
            "mongodb://db.example.com/?TLSCERTIFICATEKEYFILE=%2Fetc%2Fpasswd",
            "mongodb://db.example.com/?tls=true&tlsCertificateKeyFile=/srv/data/../../etc/passwd",
            "mongodb://db.example.com/?tlsCertificate\u{212A}eyFile=/etc/passwd",
            "mongodb://db.example.com/?appName=a,b://c&tlsCertificateKeyFile=/etc/passwd",
            "mongodb+srv://cluster0.example.net/?tlsCAFile=/etc/passwd",
        ] {
            let e = refused(None, v);
            assert_eq!(e.kind, RefusalKind::Path, "{v}");
            assert!(!e.reason.contains("/etc"), "{v}: {}", e.reason);
        }
        accepted(None, "mongodb://db.example.com/?tlsCAFile=/srv/data/ca.pem");
        accepted(
            None,
            "mongodb://db.example.com/?tlsCAFile=%2Fsrv%2Fdata%2Fca.pem",
        );
        assert!(
            ReferencePolicy::open()
                .check_url("mongodb://db.example.com/?tlsCAFile=/etc/ssl/ca.pem")
                .is_ok()
        );
    }

    #[test]
    fn mongodb_unverified_tls_needs_the_flag() {
        for v in [
            "mongodb://db.example.com/?tlsInsecure=true",
            "mongodb://db.example.com/?tlsAllowInvalidCertificates=true",
            "mongodb://db.example.com/?TLSALLOWINVALIDHOSTNAMES=yes",
            "mongodb://db.example.com/?tlsinsecure=%74rue",
        ] {
            let e = refused(None, v);
            assert!(e.reason.contains("verification"), "{v}: {}", e.reason);
            assert!(confined().with_unverified(true).check_url(v).is_ok(), "{v}");
        }
        accepted(None, "mongodb://db.example.com/?tlsInsecure=false&tls=true");
    }

    #[test]
    fn mongodb_ambient_auth_needs_host_credentials() {
        for v in [
            "mongodb://db.example.com/?authMechanism=MONGODB-OIDC&authMechanismProperties=ENVIRONMENT:k8s",
            "mongodb://db.example.com/?authMechanism=MONGODB-OIDC&authMechanismProperties=TOKEN_RESOURCE:x,ENVIRONMENT%3Aazure",
            "mongodb://db.example.com/?authMechanism=MONGODB-AWS",
            "mongodb://db.example.com/?authmechanism=gssapi",
            "mongodb+srv://cluster0.example.net/?authMechanism=MONGODB%2DOIDC&authMechanismProperties=ENVIRONMENT:gcp",
        ] {
            let e = refused(None, v);
            assert_eq!(e.kind, RefusalKind::HostCredentials, "{v}");
            assert!(
                confined().with_host_credentials(true).check_url(v).is_ok(),
                "{v}"
            );
        }
        accepted(
            None,
            "mongodb://u:p@db.example.com/?authMechanism=SCRAM-SHA-256",
        );
        accepted(
            None,
            "mongodb://db.example.com/?authMechanism=MONGODB-X509&tlsCertificateKeyFile=/srv/data/c.pem",
        );
    }

    #[test]
    fn mongodb_socket_hosts_are_paths() {
        for v in [
            "mongodb://%2Fvar%2Frun%2Fmongodb-27017.sock/",
            "mongodb://db.example.com,%2Ftmp%2F..%2Fetc%2Fm.sock/db",
            "mongodb://m.sock/",
            "mongodb://%2Fsrv%2Fdata%2F%FF.sock/",
        ] {
            assert_eq!(refused(None, v).kind, RefusalKind::Path, "{v}");
        }
        accepted(None, "mongodb://%2Fsrv%2Fdata%2Fmongodb.sock/");
        assert_eq!(
            url_hosts("mongodb://%2Fsrv%2Fdata%2Fm.sock,db.example.com/"),
            vec!["db.example.com"]
        );
        assert_eq!(url_hosts("mongodb://evil.SOCK/"), vec!["evil.sock"]);
        assert!(
            ReferencePolicy::open()
                .check_url("mongodb://%2Fvar%2Frun%2Fm.sock/")
                .is_ok()
        );
    }

    #[test]
    fn mongodb_hosts_are_read_like_the_driver() {
        for v in [
            "mongodb://ok.example.com#,169.254.169.254:80/",
            "mongodb://ok.example.com;x,169.254.169.254/",
            "mongodb://u:p@x@ok.example.com,169.254.169.254/?appName=a",
            "mongodb://ok.example.com/?proxyHost=169.254.169.254",
            "mongodb://ok.example.com/?appName=x,nats://ok.example.com&proxyHost=169.254.169.254",
        ] {
            assert!(refused(None, v).reason.contains("169.254.169.254"), "{v}");
        }
        assert_eq!(
            url_hosts("mongodb://a.example.com#,B.example.com:27018/?x=1"),
            vec!["a.example.com#", "b.example.com"]
        );
    }

    #[test]
    fn mongodb_srv_needs_the_parent_domain_listed() {
        let exact = confined().with_url_hosts(["cluster0.example.net"]);
        let parent = confined().with_url_hosts(["*.example.net"]);
        let v = "mongodb+srv://cluster0.example.net/app";
        assert!(exact.check_url(v).is_err());
        assert!(parent.check_url(v).is_ok());
        assert!(
            exact
                .check_url("mongodb://cluster0.example.net/app")
                .is_ok()
        );
        accepted(None, v);
    }

    #[test]
    fn mongodb_values_keep_plus_signs() {
        let read = MongoRead::read(
            "mongodb://u:p@db.example.com/app?tlsCAFile=/srv/data/a+b%20c.pem&appName=x%FF",
        )
        .unwrap();
        assert_eq!(
            read.values("tlscafile").collect::<Vec<_>>(),
            vec!["/srv/data/a+b c.pem"]
        );
        assert_eq!(
            read.values("appname").collect::<Vec<_>>(),
            vec!["x\u{FFFD}"]
        );
        let read = MongoRead::read("MONGODB://h/?tlsCertificate\u{212A}eyFile=/x&bad&k=").unwrap();
        assert_eq!(
            read.options,
            vec![
                ("tlscertificatekeyfile".to_string(), "/x".to_string()),
                ("k".to_string(), String::new())
            ]
        );
        assert!(MongoRead::read("mongodb:/h").is_none());
        assert!(MongoRead::read("postgres://h").is_none());
    }

    #[test]
    fn mongodb_server_addresses_are_read_like_the_driver() {
        let read = MongoRead::read(
            "mongodb+srv://a@b@[::1]:27017,[::1]x,[zz],:1,h:x:y,%2Fs.sock,%FF.sock/db,x?y",
        )
        .unwrap();
        assert!(read.srv);
        assert_eq!(
            read.hosts,
            vec![
                MongoHost::Tcp("[::1]".into()),
                MongoHost::Unreadable,
                MongoHost::Unreadable,
                MongoHost::Unreadable,
                MongoHost::Tcp("h".into()),
                MongoHost::Socket(Some("/s.sock".into())),
                MongoHost::Socket(None),
            ]
        );
        assert_eq!(srv_parent("cluster0.example.net"), "example.net");
        assert_eq!(srv_parent("example.net"), "example.net");
        assert_eq!(srv_parent("localhost"), "localhost");
    }

    #[test]
    fn mongodb_clients_stay_on_the_named_server_when_pinned() {
        let pinned = confined().with_pinned_servers(true);
        assert!(pinned.pins_servers());
        assert!(!confined().pins_servers());
        for v in [
            "mongodb://db.example.com/app",
            "mongodb://a.example.com,b.example.com/?replicaSet=rs0",
            "mongodb://db.example.com/?directConnection=false",
            "mongodb+srv://cluster0.example.net/",
            "mongodb+srv://cluster0.example.net/?directConnection=true",
        ] {
            let e = pinned.check_url(v).expect_err(v);
            assert!(e.reason.contains("directConnection=true"), "{v}");
        }
        for v in [
            "mongodb://db.example.com/app?directConnection=true",
            "mongodb://db.example.com/?replicaSet=rs0&directConnection=true",
            "mongodb://lb.example.com/?loadBalanced=true",
            "mongodb+srv://cluster0.example.net/?loadBalanced=true",
        ] {
            assert!(pinned.check_url(v).is_ok(), "{v}");
        }
    }

    #[test]
    fn mongodb_urls_are_checked_by_scheme() {
        let only_https = confined().with_url_schemes(["https"]);
        assert!(
            only_https
                .check_url("mongodb://db.example.com/")
                .unwrap_err()
                .reason
                .contains("mongodb")
        );
        assert!(
            only_https
                .check_url("mongodb+srv://c.example.net/")
                .unwrap_err()
                .reason
                .contains("mongodb+srv")
        );
        assert_eq!(
            url_hosts("mongodb://a.example.com/?proxyHost=Proxy.example.com"),
            vec!["a.example.com", "proxy.example.com"]
        );
        assert!(
            ReferencePolicy::open()
                .check_url("mongodb://:1,%FF.sock/")
                .is_ok()
        );
    }
}
