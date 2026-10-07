//! RFC 3986 reference resolution for server-provided links (#750).

use faucet_core::FaucetError;
use reqwest::Url;

/// Response-header name under which the REST source records the URL of the
/// request that produced a page, so a relative next-page link resolves against
/// it. Never sent to a server.
pub const REQUEST_URL_HEADER: &str = "x-faucet-request-url";

/// Resolve `link` against `base` per RFC 3986 §5 (`Url::join`): absolute links
/// pass through, and root-relative (`/a`), path-relative (`page2`),
/// protocol-relative (`//host/a`) and query-only (`?p=2`) links resolve
/// against the base.
pub fn resolve_link(base: &Url, link: &str) -> Result<Url, FaucetError> {
    base.join(link.trim()).map_err(|e| {
        FaucetError::Source(format!(
            "cannot resolve link '{link}' against '{base}': {e}"
        ))
    })
}

/// [`resolve_link`] over string inputs, returning the resolved URL as a string.
pub fn resolve_link_str(base: &str, link: &str) -> Result<String, FaucetError> {
    let base = Url::parse(base)
        .map_err(|e| FaucetError::Source(format!("invalid base URL '{base}': {e}")))?;
    let resolved = resolve_link(&base, link)?;
    if resolved.host_str() != base.host_str() {
        tracing::debug!(
            from = base.host_str().unwrap_or_default(),
            to = resolved.host_str().unwrap_or_default(),
            "following a next-page link to a different host"
        );
    }
    Ok(resolved.to_string())
}

/// Whether a request to `target` may carry the credentials configured for
/// `base`: true for the same scheme, host and port, or a host in
/// `trusted_hosts` (exact, or `*.suffix`); false for any other host, so the
/// request goes out without them. A plain-`http` target from an `https` base is
/// refused outright — credentials or data would cross the wire in clear.
pub fn credentials_allowed(
    base: &str,
    target: &str,
    trusted_hosts: &[String],
) -> Result<bool, FaucetError> {
    let (Ok(base), Ok(target)) = (Url::parse(base), Url::parse(target)) else {
        return Ok(false);
    };
    if base.scheme() == "https" && target.scheme() != "https" {
        return Err(FaucetError::Source(format!(
            "refusing to follow a server-given URL from https to {}://{}: it would downgrade \
             the connection to cleartext",
            target.scheme(),
            target.host_str().unwrap_or_default()
        )));
    }
    if base.scheme() == target.scheme()
        && base.host_str() == target.host_str()
        && base.port_or_known_default() == target.port_or_known_default()
    {
        return Ok(true);
    }
    let Some(host) = target.host_str() else {
        return Ok(false);
    };
    let host = host.to_ascii_lowercase();
    Ok(trusted_hosts.iter().any(|t| {
        let t = t.trim().to_ascii_lowercase();
        match t.strip_prefix("*.") {
            Some(suffix) => host.len() > suffix.len() && host.ends_with(&format!(".{suffix}")),
            None => host == t,
        }
    }))
}

/// Header names that carry credentials even when configured as static
/// headers; dropped from a request to a host that may not receive them.
pub const CREDENTIAL_HEADERS: [&str; 3] = ["authorization", "cookie", "proxy-authorization"];

/// Resolve an async-job URL against the configured `base_url`: absolute URLs
/// pass through; a root-relative URL whose path already starts with the
/// base's path prefix is resolved at the origin (so
/// `https://h/services/data/v60.0` + `/services/data/v60.0/jobs/1` does not
/// duplicate the prefix); any other relative URL is appended under the base.
pub fn resolve_job_url(base_url: &str, url: &str) -> String {
    if url.starts_with("http://") || url.starts_with("https://") {
        return url.to_string();
    }
    if let Ok(base) = Url::parse(base_url) {
        let prefix = base.path().trim_end_matches('/');
        if url.starts_with('/')
            && !prefix.is_empty()
            && (url == prefix
                || url.starts_with(&format!("{prefix}/"))
                || url.starts_with(&format!("{prefix}?")))
            && let Ok(joined) = base.join(url)
        {
            return joined.to_string();
        }
    }
    format!(
        "{}/{}",
        base_url.trim_end_matches('/'),
        url.trim_start_matches('/')
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_go_only_to_the_base_origin_or_a_trusted_host() {
        let base = "https://api.example.com/v2";
        let none: [String; 0] = [];
        assert!(credentials_allowed(base, "https://api.example.com/v2/p?2", &none).unwrap());
        assert!(credentials_allowed(base, "https://api.example.com:443/x", &none).unwrap());
        assert!(!credentials_allowed(base, "https://cdn.example.net/x", &none).unwrap());
        assert!(!credentials_allowed(base, "https://api.example.com:8443/x", &none).unwrap());
        assert!(!credentials_allowed(base, "not a url", &none).unwrap());
        let trusted = [
            "files.example.net".to_string(),
            "*.blob.example.org".to_string(),
        ];
        assert!(credentials_allowed(base, "https://files.example.net/x", &trusted).unwrap());
        assert!(credentials_allowed(base, "https://a.blob.example.org/x", &trusted).unwrap());
        assert!(!credentials_allowed(base, "https://blob.example.org/x", &trusted).unwrap());
        let err = credentials_allowed(base, "http://api.example.com/v2", &trusted)
            .unwrap_err()
            .to_string();
        assert!(err.contains("downgrade"), "{err}");
        assert!(credentials_allowed("http://h/x", "http://h/y", &none).unwrap());
        assert!(!credentials_allowed("http://h/x", "https://other/y", &none).unwrap());
    }

    fn r(base: &str, link: &str) -> String {
        resolve_link_str(base, link).unwrap()
    }

    #[test]
    fn resolves_every_reference_form() {
        let base = "https://api.example.com/services/data/v60.0/query?q=SELECT";
        assert_eq!(
            r(base, "/services/data/v60.0/query/01g-2000"),
            "https://api.example.com/services/data/v60.0/query/01g-2000"
        );
        assert_eq!(
            r(base, "query/next"),
            "https://api.example.com/services/data/v60.0/query/next"
        );
        assert_eq!(
            r(base, "//cdn.example.com/p2"),
            "https://cdn.example.com/p2"
        );
        assert_eq!(
            r(base, "https://other.example.com/a?b=1"),
            "https://other.example.com/a?b=1"
        );
        assert_eq!(
            r(base, "?page=2"),
            "https://api.example.com/services/data/v60.0/query?page=2"
        );
        assert_eq!(r("https://h/a/b", " /c "), "https://h/c");
    }

    #[test]
    fn unresolvable_inputs_error() {
        assert!(resolve_link_str("not a url", "/a").is_err());
        assert!(resolve_link_str("https://h/", "http://[::1").is_err());
    }

    #[test]
    fn job_urls_do_not_duplicate_the_base_path() {
        let base = "https://api.example.com/services/data/v60.0";
        assert_eq!(
            resolve_job_url(base, "/services/data/v60.0/jobs/query/750"),
            "https://api.example.com/services/data/v60.0/jobs/query/750"
        );
        assert_eq!(
            resolve_job_url(base, "/jobs/query"),
            "https://api.example.com/services/data/v60.0/jobs/query"
        );
        assert_eq!(
            resolve_job_url(base, "jobs/query"),
            "https://api.example.com/services/data/v60.0/jobs/query"
        );
        assert_eq!(resolve_job_url(base, "https://o/x"), "https://o/x");
        assert_eq!(resolve_job_url("https://h", "/jobs"), "https://h/jobs");
        assert_eq!(resolve_job_url("https://h/", "jobs"), "https://h/jobs");
        assert_eq!(resolve_job_url("::bad", "/jobs"), "::bad/jobs");
        assert_eq!(
            resolve_job_url(base, "/services/data/v60.0?x=1"),
            "https://api.example.com/services/data/v60.0?x=1"
        );
    }
}
