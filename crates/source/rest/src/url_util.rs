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

    fn r(base: &str, link: &str) -> String {
        resolve_link_str(base, link).unwrap()
    }

    #[test]
    fn resolves_every_reference_form() {
        let base = "https://x.my.salesforce.com/services/data/v60.0/query?q=SELECT";
        assert_eq!(
            r(base, "/services/data/v60.0/query/01g-2000"),
            "https://x.my.salesforce.com/services/data/v60.0/query/01g-2000"
        );
        assert_eq!(
            r(base, "query/next"),
            "https://x.my.salesforce.com/services/data/v60.0/query/next"
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
            "https://x.my.salesforce.com/services/data/v60.0/query?page=2"
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
        let base = "https://x.my.salesforce.com/services/data/v60.0";
        assert_eq!(
            resolve_job_url(base, "/services/data/v60.0/jobs/query/750"),
            "https://x.my.salesforce.com/services/data/v60.0/jobs/query/750"
        );
        assert_eq!(
            resolve_job_url(base, "/jobs/query"),
            "https://x.my.salesforce.com/services/data/v60.0/jobs/query"
        );
        assert_eq!(
            resolve_job_url(base, "jobs/query"),
            "https://x.my.salesforce.com/services/data/v60.0/jobs/query"
        );
        assert_eq!(resolve_job_url(base, "https://o/x"), "https://o/x");
        assert_eq!(resolve_job_url("https://h", "/jobs"), "https://h/jobs");
        assert_eq!(resolve_job_url("https://h/", "jobs"), "https://h/jobs");
        assert_eq!(resolve_job_url("::bad", "/jobs"), "::bad/jobs");
        assert_eq!(
            resolve_job_url(base, "/services/data/v60.0?x=1"),
            "https://x.my.salesforce.com/services/data/v60.0?x=1"
        );
    }
}
