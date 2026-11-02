//! Which `Host` and `Origin` headers the server answers, and the response
//! headers every API response carries.
//!
//! - `Host`: when enforced, a request must name one of the allowed hosts. A
//!   `--no-auth` server on loopback enforces it by default (a page served from
//!   another name that resolves to loopback cannot reach it); any server
//!   enforces it once `--allowed-host` is given. The health and metrics
//!   probes are exempt.
//! - `Origin`: a state-changing request (`POST`, `PUT`, `PATCH`, `DELETE`)
//!   that carries an `Origin` must come from the server's own origin, a
//!   `--cors-origin` or an `--allowed-origin`.

use crate::serve::error::ServeError;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, Method, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

/// The validated `--allowed-host` / `--allowed-origin` settings.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GuardSettings {
    /// Host names (lower-case, no port); `*` accepts any.
    pub allowed_hosts: Vec<String>,
    /// Origins (lower-case `scheme://host[:port]`).
    pub allowed_origins: Vec<String>,
}

impl GuardSettings {
    /// Validate the flag values.
    pub fn from_args(hosts: &[String], origins: &[String]) -> Result<Self, String> {
        let mut allowed_hosts = Vec::new();
        for h in hosts.iter().map(|h| h.trim()).filter(|h| !h.is_empty()) {
            if h != "*" && (h.contains("://") || h.contains('/')) {
                return Err(format!(
                    "--allowed-host `{h}` must be a host name or address, not a URL"
                ));
            }
            let name = if h == "*" {
                h.to_string()
            } else {
                split_host_port(h).0
            };
            allowed_hosts.push(name);
        }
        let mut allowed_origins = Vec::new();
        for o in origins.iter().map(|o| o.trim()).filter(|o| !o.is_empty()) {
            let parsed = parse_origin(o).ok_or_else(|| {
                format!("--allowed-origin `{o}` must be an origin like https://app.example")
            })?;
            allowed_origins.push(parsed.render());
        }
        Ok(Self {
            allowed_hosts,
            allowed_origins,
        })
    }
}

/// The checks one server applies.
#[derive(Debug, Clone)]
pub struct RequestGuard {
    enforce_host: bool,
    hosts: Vec<String>,
    origins: Vec<String>,
}

impl RequestGuard {
    /// The guard for a server listening on `listen`. `no_auth`: the server
    /// was started with `--no-auth`. `cors_origins`: the `--cors-origin` list.
    pub fn new(
        settings: &GuardSettings,
        listen: SocketAddr,
        no_auth: bool,
        cors_origins: &[String],
    ) -> Self {
        let enforce_host = !settings.allowed_hosts.is_empty()
            || (no_auth && super::config::is_loopback(listen.ip()));
        let mut hosts: Vec<String> = ["localhost", "127.0.0.1", "::1"]
            .iter()
            .map(|h| h.to_string())
            .collect();
        if !listen.ip().is_unspecified() {
            hosts.push(listen.ip().to_string());
        }
        hosts.extend(settings.allowed_hosts.iter().cloned());
        let mut origins = settings.allowed_origins.clone();
        origins.extend(
            cors_origins
                .iter()
                .filter_map(|o| parse_origin(o.trim()))
                .map(|o| o.render()),
        );
        Self {
            enforce_host: enforce_host && !settings.allowed_hosts.iter().any(|h| h == "*"),
            hosts,
            origins,
        }
    }

    /// Whether the request's `Host` names an allowed host.
    fn host_allowed(&self, host: Option<&str>) -> bool {
        if !self.enforce_host {
            return true;
        }
        let Some(host) = host else {
            return true;
        };
        let (name, _) = split_host_port(host);
        self.hosts.contains(&name)
    }

    /// Whether a state-changing request's `Origin` is acceptable.
    fn origin_allowed(&self, headers: &HeaderMap) -> bool {
        let Some(raw) = headers.get(header::ORIGIN) else {
            return true;
        };
        let Some(origin) = raw.to_str().ok().and_then(|o| parse_origin(o.trim())) else {
            return false;
        };
        if self.origins.contains(&origin.render()) {
            return true;
        }
        let forwarded = headers
            .get("x-forwarded-host")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.split(',').next())
            .map(str::trim);
        [request_host(headers), forwarded]
            .into_iter()
            .flatten()
            .any(|h| origin.same_authority(h))
    }
}

/// A parsed `scheme://host[:port]`.
struct Origin {
    scheme: String,
    host: String,
    port: Option<u16>,
}

impl Origin {
    fn default_port(&self) -> Option<u16> {
        match self.scheme.as_str() {
            "http" => Some(80),
            "https" => Some(443),
            _ => None,
        }
    }

    fn render(&self) -> String {
        match self.port {
            Some(p) if Some(p) != self.default_port() => {
                format!("{}://{}:{p}", self.scheme, bracket(&self.host))
            }
            _ => format!("{}://{}", self.scheme, bracket(&self.host)),
        }
    }

    /// Whether a `Host` header value names this origin's host and port.
    fn same_authority(&self, host: &str) -> bool {
        let (name, port) = split_host_port(host);
        let ours = self.port.or(self.default_port());
        name == self.host && port.or(self.default_port()) == ours
    }
}

fn bracket(host: &str) -> String {
    if host.contains(':') {
        format!("[{host}]")
    } else {
        host.to_string()
    }
}

fn parse_origin(s: &str) -> Option<Origin> {
    let (scheme, rest) = s.split_once("://")?;
    let scheme = scheme.to_ascii_lowercase();
    if scheme.is_empty() || rest.is_empty() || rest.contains(['/', '?', '#', '@']) {
        return None;
    }
    let (host, port) = split_host_port(rest);
    if host.is_empty() {
        return None;
    }
    Some(Origin { scheme, host, port })
}

/// `host[:port]` → lower-case host without brackets, and the port.
fn split_host_port(s: &str) -> (String, Option<u16>) {
    let s = s.trim();
    if let Some(rest) = s.strip_prefix('[') {
        let (host, tail) = rest.split_once(']').unwrap_or((rest, ""));
        let port = tail.strip_prefix(':').and_then(|p| p.parse().ok());
        return (host.to_ascii_lowercase(), port);
    }
    if s.parse::<IpAddr>().is_ok() {
        return (s.to_ascii_lowercase(), None);
    }
    match s.rsplit_once(':') {
        Some((h, p)) if p.chars().all(|c| c.is_ascii_digit()) && !p.is_empty() => {
            (h.trim_end_matches('.').to_ascii_lowercase(), p.parse().ok())
        }
        _ => (s.trim_end_matches('.').to_ascii_lowercase(), None),
    }
}

fn request_host(headers: &HeaderMap) -> Option<&str> {
    headers.get(header::HOST).and_then(|v| v.to_str().ok())
}

fn changes_state(method: &Method) -> bool {
    matches!(
        *method,
        Method::POST | Method::PUT | Method::PATCH | Method::DELETE
    )
}

/// Paths a probe reaches by address (a load balancer, a Kubernetes kubelet).
fn is_probe_path(path: &str) -> bool {
    matches!(path, "/healthz" | "/readyz" | "/metrics")
}

/// Whether `path` is an API route (JSON responses).
fn is_api_path(path: &str) -> bool {
    path == "/mcp" || path == "/v1" || path.starts_with("/v1/")
}

/// The middleware: refuse a disallowed `Host` or `Origin`, and stamp the
/// response headers.
pub async fn guard(State(guard): State<Arc<RequestGuard>>, req: Request, next: Next) -> Response {
    let path = req.uri().path().to_string();
    let host = request_host(req.headers())
        .map(str::to_string)
        .or_else(|| req.uri().authority().map(|a| a.to_string()));
    let refusal = if !is_probe_path(&path) && !guard.host_allowed(host.as_deref()) {
        Some(ServeError::Forbidden(
            "this server does not answer requests for that host name; start it with \
             --allowed-host to add one"
                .into(),
        ))
    } else if changes_state(req.method()) && !guard.origin_allowed(req.headers()) {
        Some(ServeError::Forbidden(
            "a request from another origin may not change this server's state; list the \
             origin with --allowed-origin or --cors-origin"
                .into(),
        ))
    } else {
        None
    };
    let mut resp = match refusal {
        Some(e) => e.into_response(),
        None => next.run(req).await,
    };
    let headers = resp.headers_mut();
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    if is_api_path(&path) {
        headers.insert(
            header::HeaderName::from_static("cross-origin-resource-policy"),
            HeaderValue::from_static("same-origin"),
        );
    }
    resp
}

/// Whether a request's `Content-Type` is one of `accepted` (`*` or `*/*`
/// accepts any, `type/*` any subtype). A request without one is accepted
/// only when its body is empty.
pub fn content_type_accepted(headers: &HeaderMap, body_empty: bool, accepted: &[String]) -> bool {
    let Some(raw) = headers.get(header::CONTENT_TYPE) else {
        return body_empty;
    };
    let Ok(raw) = raw.to_str() else {
        return false;
    };
    let media = raw
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    accepted.iter().any(|a| {
        let a = a.trim().to_ascii_lowercase();
        a == "*"
            || a == "*/*"
            || a == media
            || a.strip_suffix("/*")
                .is_some_and(|t| media.split('/').next() == Some(t))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(
                header::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                HeaderValue::from_str(v).unwrap(),
            );
        }
        h
    }

    fn guard(no_auth: bool, listen: &str, hosts: &[&str], origins: &[&str]) -> RequestGuard {
        let s = GuardSettings::from_args(
            &hosts.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            &origins.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
        )
        .unwrap();
        RequestGuard::new(&s, listen.parse().unwrap(), no_auth, &[])
    }

    #[test]
    fn a_no_auth_loopback_server_answers_only_local_names() {
        let g = guard(true, "127.0.0.1:8080", &[], &[]);
        for ok in [
            "localhost:8080",
            "127.0.0.1:8080",
            "[::1]:8080",
            "LOCALHOST",
        ] {
            assert!(g.host_allowed(Some(ok)), "{ok}");
        }
        assert!(!g.host_allowed(Some("other.example:8080")));
        assert!(!g.host_allowed(Some("127.0.0.1.other.example")));
        assert!(g.host_allowed(None));
    }

    #[test]
    fn an_authenticating_server_checks_hosts_only_when_listed() {
        let g = guard(false, "0.0.0.0:8080", &[], &[]);
        assert!(g.host_allowed(Some("faucet.internal")));
        let g = guard(false, "0.0.0.0:8080", &["faucet.example.com"], &[]);
        assert!(g.host_allowed(Some("faucet.example.com:443")));
        assert!(!g.host_allowed(Some("other.example")));
        let g = guard(true, "0.0.0.0:8080", &[], &[]);
        assert!(
            g.host_allowed(Some("10.0.0.5:8080")),
            "network no-auth is opt-in"
        );
        let g = guard(true, "127.0.0.1:8080", &["*"], &[]);
        assert!(g.host_allowed(Some("anything.example")));
    }

    #[test]
    fn state_changes_need_the_servers_own_or_a_listed_origin() {
        let g = guard(true, "127.0.0.1:8080", &[], &["https://app.example"]);
        assert!(g.origin_allowed(&headers(&[])));
        assert!(g.origin_allowed(&headers(&[
            ("origin", "http://localhost:8080"),
            ("host", "localhost:8080")
        ])));
        assert!(!g.origin_allowed(&headers(&[
            ("origin", "https://other.example"),
            ("host", "localhost:8080")
        ])));
        assert!(!g.origin_allowed(&headers(&[("origin", "null"), ("host", "localhost:8080")])));
        assert!(g.origin_allowed(&headers(&[
            ("origin", "https://APP.example:443"),
            ("host", "localhost:8080")
        ])));
        assert!(g.origin_allowed(&headers(&[
            ("origin", "https://faucet.example"),
            ("host", "10.0.0.1:8080"),
            ("x-forwarded-host", "faucet.example")
        ])));
        assert!(!g.origin_allowed(&headers(&[
            ("origin", "http://localhost:9999"),
            ("host", "localhost:8080")
        ])));
    }

    #[test]
    fn cors_origins_may_change_state() {
        let s = GuardSettings::default();
        let g = RequestGuard::new(
            &s,
            "127.0.0.1:8080".parse().unwrap(),
            false,
            &["https://console.example".into()],
        );
        assert!(g.origin_allowed(&headers(&[
            ("origin", "https://console.example"),
            ("host", "localhost:8080")
        ])));
    }

    #[test]
    fn flag_values_are_validated() {
        assert!(GuardSettings::from_args(&["https://x.example".into()], &[]).is_err());
        assert!(GuardSettings::from_args(&[], &["x.example".into()]).is_err());
        assert!(GuardSettings::from_args(&[], &["https://x.example/path".into()]).is_err());
        let s = GuardSettings::from_args(&["Faucet.Example:8443".into()], &[]).unwrap();
        assert_eq!(s.allowed_hosts, vec!["faucet.example".to_string()]);
    }

    #[test]
    fn content_types_match_by_media_type() {
        let json = vec!["application/json".to_string()];
        let h = headers(&[("content-type", "application/json; charset=utf-8")]);
        assert!(content_type_accepted(&h, false, &json));
        let h = headers(&[("content-type", "text/plain")]);
        assert!(!content_type_accepted(&h, false, &json));
        assert!(content_type_accepted(&HeaderMap::new(), true, &json));
        assert!(!content_type_accepted(&HeaderMap::new(), false, &json));
        assert!(content_type_accepted(&h, false, &["*".to_string()]));
        assert!(content_type_accepted(&h, false, &["text/*".to_string()]));
        assert!(!content_type_accepted(
            &h,
            false,
            &["application/*".to_string()]
        ));
    }
}
