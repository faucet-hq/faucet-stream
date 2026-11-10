//! Embedded web-console assets (serve-ui feature). The static shell is PUBLIC;
//! all data stays behind the bearer-gated `/v1` API. Assets are embedded at
//! compile time from `src/serve/ui/` via `rust-embed`.

use crate::serve::error::ServeError;
use axum::extract::Path;
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};

#[derive(rust_embed::RustEmbed)]
#[folder = "src/serve/ui/"]
struct UiAssets;

/// The console's content policy: its own scripts only, no framing, and
/// inline `style` attributes (the views set them) but no inline scripts.
pub(crate) const CONTENT_SECURITY_POLICY: &str = "default-src 'self'; script-src 'self'; \
     style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; \
     object-src 'none'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'";

/// Send embedded asset bytes with the right content-type and `no-cache`
/// (assets are tiny + embedded; correctness over caching — see spec §11).
fn serve_asset(path: &str) -> Response {
    if !plain_asset_path(path) {
        return ServeError::NotFound.into_response();
    }
    #[cfg(debug_assertions)]
    if crosses_link(std::path::Path::new(UI_DIR), path) {
        return ServeError::NotFound.into_response();
    }
    match UiAssets::get(path) {
        Some(file) => {
            let mime = file.metadata.mimetype();
            (
                StatusCode::OK,
                [
                    (header::CONTENT_TYPE, mime.to_string()),
                    (header::CACHE_CONTROL, "no-cache".to_string()),
                    (
                        header::CONTENT_SECURITY_POLICY,
                        CONTENT_SECURITY_POLICY.to_string(),
                    ),
                    (header::X_CONTENT_TYPE_OPTIONS, "nosniff".to_string()),
                    (header::X_FRAME_OPTIONS, "DENY".to_string()),
                    (header::REFERRER_POLICY, "no-referrer".to_string()),
                ],
                file.data.into_owned(),
            )
                .into_response()
        }
        None => ServeError::NotFound.into_response(),
    }
}

/// Asset paths must be plain relative names: no empty, `.` or `..` segment,
/// no leading `/`, no backslash, and nothing but normal components.
fn plain_asset_path(path: &str) -> bool {
    !path.is_empty()
        && !path.contains('\\')
        && path
            .split('/')
            .all(|segment| !matches!(segment, "" | "." | ".."))
        && std::path::Path::new(path)
            .components()
            .all(|c| matches!(c, std::path::Component::Normal(_)))
}

/// In a debug build assets are read from the source folder, which follows
/// symbolic links; an asset reached through one is not served.
#[cfg(debug_assertions)]
const UI_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src/serve/ui");

#[cfg(debug_assertions)]
fn crosses_link(root: &std::path::Path, path: &str) -> bool {
    let mut at = root.to_path_buf();
    path.split('/').any(|segment| {
        at.push(segment);
        std::fs::symlink_metadata(&at).is_ok_and(|m| m.file_type().is_symlink())
    })
}

/// `GET /` → the SPA shell.
pub async fn index() -> Response {
    serve_asset("index.html")
}

/// `GET /assets/{*path}` → an embedded asset by relative path.
pub async fn asset(Path(path): Path<String>) -> Response {
    serve_asset(&path)
}

/// True when the request prefers an HTML document (so a deep-link / refresh of a
/// client-side route should receive the SPA shell rather than a JSON 404).
pub(crate) fn wants_html(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .map(|a| a.contains("text/html"))
        .unwrap_or(false)
}

/// Router fallback: an HTML-accepting GET to an unmatched path returns the SPA
/// shell (enables hash-route deep links); everything else gets the standard JSON
/// 404 so the API's 404 shape is preserved.
pub async fn spa_fallback(method: Method, headers: HeaderMap) -> Response {
    if method == Method::GET && wants_html(&headers) {
        index().await
    } else {
        ServeError::NotFound.into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[tokio::test]
    async fn the_shell_and_assets_carry_the_content_policy_headers() {
        for resp in [index().await, asset(Path("app.js".into())).await] {
            assert_eq!(resp.status(), StatusCode::OK);
            let h = resp.headers();
            let csp = h[header::CONTENT_SECURITY_POLICY].to_str().unwrap();
            assert!(csp.contains("frame-ancestors 'none'"), "{csp}");
            assert!(csp.contains("script-src 'self'"), "{csp}");
            assert_eq!(h[header::X_CONTENT_TYPE_OPTIONS], "nosniff");
            assert_eq!(h[header::X_FRAME_OPTIONS], "DENY");
            assert_eq!(h[header::REFERRER_POLICY], "no-referrer");
        }
    }

    #[test]
    fn wants_html_true_for_browser_accept() {
        let mut h = HeaderMap::new();
        h.insert(
            header::ACCEPT,
            HeaderValue::from_static("text/html,application/xhtml+xml"),
        );
        assert!(wants_html(&h));
    }

    #[test]
    fn wants_html_false_for_json_or_missing() {
        let mut h = HeaderMap::new();
        h.insert(header::ACCEPT, HeaderValue::from_static("application/json"));
        assert!(!wants_html(&h));
        assert!(!wants_html(&HeaderMap::new()));
    }

    #[test]
    fn index_asset_is_embedded_and_html() {
        let resp = serve_asset("index.html");
        assert_eq!(resp.status(), StatusCode::OK);
        let ct = resp.headers().get(header::CONTENT_TYPE).unwrap();
        assert!(ct.to_str().unwrap().contains("text/html"));
    }

    #[test]
    fn asset_paths_must_be_plain_relative_names() {
        for good in ["app.js", "views/runs.js", "mark.svg"] {
            assert!(plain_asset_path(good), "{good}");
        }
        for bad in [
            "",
            "/app.js",
            "./app.js",
            "views/./runs.js",
            "../ui_assets.rs",
            "views/../app.js",
            "views//runs.js",
            "views/",
            "views\\runs.js",
            "..\\ui_assets.rs",
        ] {
            assert!(!plain_asset_path(bad), "{bad:?}");
            assert_eq!(serve_asset(bad).status(), StatusCode::NOT_FOUND, "{bad:?}");
        }
    }

    #[tokio::test]
    async fn asset_paths_with_dot_segments_are_not_found() {
        use tower::ServiceExt;
        let app = axum::Router::new().route("/assets/{*path}", axum::routing::get(asset));
        let up = "..%2F".repeat(30);
        for uri in [
            format!("/assets/{up}etc%2Flocaltime"),
            format!("/assets/{}etc/localtime", "../".repeat(30)),
            "/assets/%2E%2E/ui_assets.rs".to_string(),
            "/assets/%2E%2E%2Fui_assets.rs".to_string(),
            "/assets/views%5C..%5C..%5Cui_assets.rs".to_string(),
            "/assets/%252F..%252Fui_assets.rs".to_string(),
            "/assets//app.js".to_string(),
            "/assets/views/./runs.js".to_string(),
        ] {
            let resp = app
                .clone()
                .oneshot(
                    axum::http::Request::get(&uri)
                        .body(axum::body::Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::NOT_FOUND, "{uri}");
        }
        let resp = app
            .oneshot(
                axum::http::Request::get("/assets/views/runs.js")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[cfg(all(unix, debug_assertions))]
    #[test]
    fn asset_symlink_inside_the_folder_is_not_served_in_debug() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("app.js"), b"x").unwrap();
        std::fs::create_dir(root.path().join("views")).unwrap();
        std::fs::write(root.path().join("views/runs.js"), b"x").unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("secret"), b"x").unwrap();
        std::os::unix::fs::symlink(outside.path().join("secret"), root.path().join("leaf.js"))
            .unwrap();
        std::os::unix::fs::symlink(outside.path(), root.path().join("linked")).unwrap();
        assert!(!crosses_link(root.path(), "app.js"));
        assert!(!crosses_link(root.path(), "views/runs.js"));
        assert!(!crosses_link(root.path(), "missing.js"));
        assert!(crosses_link(root.path(), "leaf.js"));
        assert!(crosses_link(root.path(), "linked/secret"));
        assert!(!crosses_link(std::path::Path::new(UI_DIR), "app.js"));
    }

    #[test]
    fn missing_asset_is_not_found() {
        let resp = serve_asset("does-not-exist.xyz");
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}
