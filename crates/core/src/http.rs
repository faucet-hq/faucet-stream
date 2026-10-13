//! The HTTP client every HTTP connector uses (RFC 0013, #880).
//!
//! [`Client`] wraps a `reqwest::Client` and adds what each HTTP connector
//! used to write for itself, sometimes wrongly:
//!
//! - **Redirects that drop credentials.** Redirects are followed here, not by
//!   reqwest, so a hop to another origin (scheme, host or port) drops the
//!   `Authorization`, `Cookie` and `Proxy-Authorization` headers, every header
//!   marked sensitive, and every header named with
//!   [`ClientBuilder::sensitive_header`] (an API-key header). reqwest's own
//!   policy only knows the standard headers.
//! - **Bounded response reads.** [`Client::bytes`] / [`text`](Client::text) /
//!   [`json`](Client::json) refuse a body above
//!   [`max_response_bytes`](ClientBuilder::max_response_bytes) instead of
//!   buffering it whole.
//! - **One error shape.** A non-2xx response becomes
//!   [`FaucetError::HttpStatus`] (body truncated, URL without credentials), a
//!   `429` / `503` with `Retry-After` becomes [`FaucetError::RateLimited`], and
//!   a transport error keeps reqwest's timeout / connect classification with
//!   the credentials removed from its URL.
//! - **Retries through [`CallContext`].** [`Client::execute`] runs a request
//!   under a context's timeout, deadline, retry policy, cancellation and
//!   metering.
//!
//! ```no_run
//! # async fn ex() -> Result<(), faucet_core::FaucetError> {
//! use faucet_core::http::Client;
//! use faucet_core::io::CallContext;
//! use std::time::Duration;
//!
//! let client = Client::builder()
//!     .timeout(Duration::from_secs(30))
//!     .sensitive_header("x-api-key")
//!     .build()?;
//! let ctx = CallContext::new().with_retry(faucet_core::RetryPolicy::default());
//! let resp = client
//!     .execute(&ctx, "page", || client.get("https://api.example.com/items"))
//!     .await?;
//! let body: serde_json::Value = client.json(resp).await?;
//! # Ok(())
//! # }
//! ```
//!
//! reqwest options this builder does not name (compression, mutual TLS, root
//! certificates) are set through [`ClientBuilder::configure`].

use crate::error::FaucetError;
use crate::io::CallContext;
use reqwest::header::{self, HeaderMap, HeaderName};
use reqwest::{Method, Request, RequestBuilder, Response, StatusCode, Url};
use serde::de::DeserializeOwned;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

/// The largest body [`Client::bytes`] buffers by default: 1 GiB.
pub const DEFAULT_MAX_RESPONSE_BYTES: u64 = 1 << 30;

/// The most redirects one request follows by default.
pub const DEFAULT_MAX_REDIRECTS: usize = 10;

/// A response body was larger than the client's
/// [`max_response_bytes`](ClientBuilder::max_response_bytes). Carried in
/// [`FaucetError::Custom`]; downcast to read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResponseTooLarge {
    /// The response URL, without credentials.
    pub url: String,
    /// The limit that was exceeded, in bytes.
    pub limit: u64,
}

impl fmt::Display for ResponseTooLarge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "response from {} is larger than the {}-byte limit",
            self.url, self.limit
        )
    }
}

impl std::error::Error for ResponseTooLarge {}

/// Builds a [`Client`].
#[must_use]
pub struct ClientBuilder {
    inner: reqwest::ClientBuilder,
    max_response_bytes: Option<u64>,
    max_redirects: usize,
    sensitive: Vec<HeaderName>,
    error: Option<FaucetError>,
}

impl fmt::Debug for ClientBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientBuilder")
            .field("max_response_bytes", &self.max_response_bytes)
            .field("max_redirects", &self.max_redirects)
            .field("sensitive", &self.sensitive)
            .finish_non_exhaustive()
    }
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientBuilder {
    /// A builder with the defaults: no client-level timeout, a
    /// [`DEFAULT_MAX_RESPONSE_BYTES`] read limit, [`DEFAULT_MAX_REDIRECTS`].
    #[allow(clippy::disallowed_methods)] // the one sanctioned reqwest client constructor
    pub fn new() -> Self {
        Self {
            inner: reqwest::Client::builder(),
            max_response_bytes: Some(DEFAULT_MAX_RESPONSE_BYTES),
            max_redirects: DEFAULT_MAX_REDIRECTS,
            sensitive: Vec::new(),
            error: None,
        }
    }

    /// Bound a whole request, body included.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.inner = self.inner.timeout(timeout);
        self
    }

    /// Bound connection setup.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.inner = self.inner.connect_timeout(timeout);
        self
    }

    /// Bound each idle read, so a long streamed body is not cut while it keeps
    /// arriving.
    pub fn read_timeout(mut self, timeout: Duration) -> Self {
        self.inner = self.inner.read_timeout(timeout);
        self
    }

    /// Send `user_agent` on every request.
    pub fn user_agent(mut self, user_agent: &str) -> Self {
        self.inner = self.inner.user_agent(user_agent.to_string());
        self
    }

    /// Send `headers` on every request.
    pub fn default_headers(mut self, headers: HeaderMap) -> Self {
        self.inner = self.inner.default_headers(headers);
        self
    }

    /// Route every request through the proxy at `url`. A malformed URL fails
    /// [`build`](Self::build).
    pub fn proxy(mut self, url: &str) -> Self {
        match reqwest::Proxy::all(url) {
            Ok(p) => self.inner = self.inner.proxy(p),
            Err(e) => {
                self.error = Some(FaucetError::Config(format!(
                    "invalid proxy URL {}: {e}",
                    crate::util::redact_uri_credentials(url)
                )))
            }
        }
        self
    }

    /// Ignore proxy environment variables.
    pub fn no_proxy(mut self) -> Self {
        self.inner = self.inner.no_proxy();
        self
    }

    /// Follow at most `n` redirects (`0` returns the redirect response).
    pub fn max_redirects(mut self, n: usize) -> Self {
        self.max_redirects = n;
        self
    }

    /// Refuse a buffered body above `limit` bytes; `None` lifts the limit.
    pub fn max_response_bytes(mut self, limit: Option<u64>) -> Self {
        self.max_response_bytes = limit;
        self
    }

    /// Drop the header `name` on a redirect to another origin, like
    /// `Authorization`. Name every header that carries a credential (an API
    /// key, a session id). An invalid name fails [`build`](Self::build).
    pub fn sensitive_header(mut self, name: &str) -> Self {
        match HeaderName::from_bytes(name.as_bytes()) {
            Ok(n) => self.sensitive.push(n),
            Err(_) => {
                self.error = Some(FaucetError::Config(format!(
                    "invalid sensitive header name {name:?}"
                )))
            }
        }
        self
    }

    /// Set any other reqwest option: `|b| b.gzip(true)`, an identity, root
    /// certificates. Don't set a redirect policy here — redirects are followed
    /// by [`Client::send`], which strips credentials on a cross-origin hop.
    pub fn configure(
        mut self,
        f: impl FnOnce(reqwest::ClientBuilder) -> reqwest::ClientBuilder,
    ) -> Self {
        self.inner = f(self.inner);
        self
    }

    /// Like [`configure`](Self::configure) for a step that can fail.
    pub fn try_configure(
        mut self,
        f: impl FnOnce(reqwest::ClientBuilder) -> Result<reqwest::ClientBuilder, FaucetError>,
    ) -> Result<Self, FaucetError> {
        self.inner = f(self.inner)?;
        Ok(self)
    }

    /// Build the client.
    pub fn build(self) -> Result<Client, FaucetError> {
        if let Some(e) = self.error {
            return Err(e);
        }
        Ok(Client {
            inner: self
                .inner
                .redirect(same_origin_redirects(self.max_redirects))
                .build()?,
            max_response_bytes: self.max_response_bytes,
            max_redirects: self.max_redirects,
            sensitive: self.sensitive.into(),
        })
    }
}

/// reqwest's own redirect policy for a [`Client`]: follow a same-origin hop
/// up to `max`, stop at a hop to another origin. [`Client::send`] follows that
/// one with the credentials stripped; a request sent straight from a
/// [`RequestBuilder`] gets the 3xx back, so nothing leaks either way.
fn same_origin_redirects(max: usize) -> reqwest::redirect::Policy {
    reqwest::redirect::Policy::custom(move |attempt| {
        let cross = attempt
            .previous()
            .last()
            .is_some_and(|prev| is_cross_origin(prev, attempt.url()));
        if cross || attempt.previous().len() > max {
            attempt.stop()
        } else {
            attempt.follow()
        }
    })
}

/// An HTTP client for connectors. Cheap to clone; build one in `new()` and
/// reuse it. See the [module docs](self).
#[derive(Clone)]
pub struct Client {
    inner: reqwest::Client,
    max_response_bytes: Option<u64>,
    max_redirects: usize,
    sensitive: Arc<[HeaderName]>,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("max_response_bytes", &self.max_response_bytes)
            .field("max_redirects", &self.max_redirects)
            .field("sensitive", &self.sensitive)
            .finish_non_exhaustive()
    }
}

impl Client {
    /// A client with the [`ClientBuilder`] defaults.
    pub fn new() -> Result<Self, FaucetError> {
        ClientBuilder::new().build()
    }

    /// A builder.
    pub fn builder() -> ClientBuilder {
        ClientBuilder::new()
    }

    /// The underlying reqwest client, for request shapes this type does not
    /// cover. It follows same-origin redirects only: a request sent straight
    /// from it gets a cross-origin 3xx back instead of following it. Send
    /// through [`send`](Self::send) to follow those with credentials stripped.
    pub fn inner(&self) -> &reqwest::Client {
        &self.inner
    }

    /// Start a request.
    pub fn request<U: reqwest::IntoUrl>(&self, method: Method, url: U) -> RequestBuilder {
        self.inner.request(method, url)
    }

    /// Start a `GET`.
    pub fn get<U: reqwest::IntoUrl>(&self, url: U) -> RequestBuilder {
        self.inner.get(url)
    }

    /// Start a `POST`.
    pub fn post<U: reqwest::IntoUrl>(&self, url: U) -> RequestBuilder {
        self.inner.post(url)
    }

    /// Start a `PUT`.
    pub fn put<U: reqwest::IntoUrl>(&self, url: U) -> RequestBuilder {
        self.inner.put(url)
    }

    /// Start a `DELETE`.
    pub fn delete<U: reqwest::IntoUrl>(&self, url: U) -> RequestBuilder {
        self.inner.delete(url)
    }

    /// Start a `HEAD`.
    pub fn head<U: reqwest::IntoUrl>(&self, url: U) -> RequestBuilder {
        self.inner.head(url)
    }

    /// The buffered-read limit.
    pub fn max_response_bytes(&self) -> Option<u64> {
        self.max_response_bytes
    }

    /// Send one attempt and follow its redirects. Any status comes back as a
    /// response; only a transport failure is an error.
    pub async fn send(&self, request: RequestBuilder) -> Result<Response, FaucetError> {
        let request = request.build().map_err(transport_error)?;
        self.send_request(request).await
    }

    /// [`send`](Self::send) for an already-built request.
    pub async fn send_request(&self, request: Request) -> Result<Response, FaucetError> {
        let mut current = request;
        let mut hops = 0usize;
        loop {
            let template = current.try_clone();
            let method = current.method().clone();
            let url = current.url().clone();
            let headers = current.headers().clone();
            let resp = self
                .inner
                .execute(current)
                .await
                .map_err(transport_error)?;
            let status = resp.status();
            if !status.is_redirection() || self.max_redirects == 0 {
                return Ok(resp);
            }
            let Some(next_url) = resp
                .headers()
                .get(header::LOCATION)
                .and_then(|v| v.to_str().ok())
                .and_then(|loc| url.join(loc).ok())
            else {
                return Ok(resp);
            };
            let from = resp.url().clone();
            // reqwest follows same-origin hops itself and stops only at the
            // limit or on a body it cannot replay.
            let same_origin = !is_cross_origin(&from, &next_url);
            let rewritten = from != url && !matches!(method, Method::GET | Method::HEAD);
            if (same_origin && template.is_none()) || (!same_origin && rewritten) {
                return Ok(resp);
            }
            if same_origin || hops >= self.max_redirects {
                return Err(FaucetError::HttpStatus {
                    status: status.as_u16(),
                    url: crate::util::redact_uri_credentials(url.as_str()),
                    body: format!("too many redirects (more than {})", self.max_redirects),
                });
            }
            let Some(next) = redirect_request(
                status,
                method,
                &from,
                next_url,
                headers,
                template,
                &self.sensitive,
            ) else {
                return Ok(resp);
            };
            hops += 1;
            current = next;
        }
    }

    /// Run a request as one backend call under `ctx` ([`CallContext::call`]):
    /// `build` is invoked once per attempt, a non-2xx response is an error
    /// ([`status_error`]) so a `5xx` / `429` is retried by the context's
    /// policy, and the 2xx response is returned.
    pub async fn execute<F>(
        &self,
        ctx: &CallContext,
        op: &'static str,
        mut build: F,
    ) -> Result<Response, FaucetError>
    where
        F: FnMut() -> RequestBuilder,
    {
        ctx.call(op, || {
            let request = build();
            async move {
                let resp = self.send(request).await?;
                if resp.status().is_success() {
                    Ok(resp)
                } else {
                    Err(status_error(resp).await)
                }
            }
        })
        .await
    }

    /// Read the whole body, refusing one above the read limit.
    pub async fn bytes(&self, resp: Response) -> Result<Vec<u8>, FaucetError> {
        read_limited(resp, self.max_response_bytes).await
    }

    /// Read the whole body as text (invalid UTF-8 replaced).
    pub async fn text(&self, resp: Response) -> Result<String, FaucetError> {
        let bytes = self.bytes(resp).await?;
        Ok(match String::from_utf8(bytes) {
            Ok(s) => s,
            Err(e) => String::from_utf8_lossy(e.as_bytes()).into_owned(),
        })
    }

    /// Read the whole body as JSON.
    pub async fn json<T: DeserializeOwned>(&self, resp: Response) -> Result<T, FaucetError> {
        let bytes = self.bytes(resp).await?;
        Ok(serde_json::from_slice(&bytes)?)
    }
}

/// The request for the next redirect hop, or `None` when the hop is not
/// followed: a streamed body cannot be replayed, and a body is never replayed
/// to another origin (it may carry credentials, like a token form).
fn redirect_request(
    status: StatusCode,
    method: Method,
    from: &Url,
    to: Url,
    mut headers: HeaderMap,
    template: Option<Request>,
    sensitive: &[HeaderName],
) -> Option<Request> {
    let to_get = status == StatusCode::SEE_OTHER
        || ((status == StatusCode::MOVED_PERMANENTLY || status == StatusCode::FOUND)
            && method == Method::POST);
    let cross = is_cross_origin(from, &to);
    if cross {
        strip_credentials(&mut headers, sensitive);
    }
    let mut next = if to_get {
        for h in [
            header::CONTENT_TYPE,
            header::CONTENT_LENGTH,
            header::CONTENT_ENCODING,
            header::TRANSFER_ENCODING,
        ] {
            headers.remove(h);
        }
        let method = if method == Method::HEAD {
            Method::HEAD
        } else {
            Method::GET
        };
        Request::new(method, to)
    } else {
        let mut next = template?;
        if cross && next.body().is_some() {
            return None;
        }
        *next.url_mut() = to;
        next
    };
    *next.headers_mut() = headers;
    Some(next)
}

/// Whether `b` is a different origin from `a`: another scheme, host or port.
pub fn is_cross_origin(a: &Url, b: &Url) -> bool {
    a.scheme() != b.scheme()
        || a.host_str() != b.host_str()
        || a.port_or_known_default() != b.port_or_known_default()
}

/// Remove every credential-bearing header: the standard ones, every value
/// marked sensitive, and the `extra` names.
pub fn strip_credentials(headers: &mut HeaderMap, extra: &[HeaderName]) {
    for h in [
        header::AUTHORIZATION,
        header::COOKIE,
        header::PROXY_AUTHORIZATION,
        header::WWW_AUTHENTICATE,
    ] {
        headers.remove(h);
    }
    for h in extra {
        headers.remove(h);
    }
    let marked: Vec<HeaderName> = headers
        .iter()
        .filter(|(_, v)| v.is_sensitive())
        .map(|(k, _)| k.clone())
        .collect();
    for h in marked {
        headers.remove(h);
    }
}

/// A transport error with the credentials removed from its URL.
pub fn transport_error(e: reqwest::Error) -> FaucetError {
    FaucetError::Http(crate::io::redact_reqwest_url(e, crate::redact::redact))
}

/// The error for a non-2xx response: [`FaucetError::RateLimited`] for a `429`
/// (or a `503` with `Retry-After`) that states its wait, otherwise
/// [`FaucetError::HttpStatus`] with the body truncated to
/// [`DEFAULT_ERROR_BODY_MAX_LEN`](crate::util::DEFAULT_ERROR_BODY_MAX_LEN)
/// bytes and the URL without credentials. Consumes the response.
pub async fn status_error(resp: Response) -> FaucetError {
    let status = resp.status().as_u16();
    let url = resp.url().to_string();
    match crate::util::check_http_response_rate_limited(
        resp,
        crate::util::DEFAULT_ERROR_BODY_MAX_LEN,
    )
    .await
    {
        Err(FaucetError::HttpStatus { status, url, body }) => FaucetError::HttpStatus {
            status,
            url: crate::util::redact_uri_credentials(&url),
            body,
        },
        Err(e) => e,
        Ok(_) => FaucetError::HttpStatus {
            status,
            url: crate::util::redact_uri_credentials(&url),
            body: String::new(),
        },
    }
}

async fn read_limited(mut resp: Response, limit: Option<u64>) -> Result<Vec<u8>, FaucetError> {
    let too_large = |resp: &Response, limit: u64| {
        FaucetError::Custom(Box::new(ResponseTooLarge {
            url: crate::util::redact_uri_credentials(resp.url().as_str()),
            limit,
        }))
    };
    if let (Some(limit), Some(len)) = (limit, resp.content_length())
        && len > limit
    {
        return Err(too_large(&resp, limit));
    }
    let capacity = resp
        .content_length()
        .map_or(0, |n| usize::try_from(n).unwrap_or(0).min(64 << 20));
    let mut out = Vec::with_capacity(capacity);
    while let Some(chunk) = resp.chunk().await.map_err(transport_error)? {
        out.extend_from_slice(&chunk);
        if let Some(limit) = limit
            && out.len() as u64 > limit
        {
            return Err(too_large(&resp, limit));
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resilience::{BackoffKind, RetryPolicy};
    use axum::Router;
    use axum::http::{HeaderMap as AxHeaders, StatusCode as AxStatus};
    use axum::response::IntoResponse;
    use axum::routing::{any, get};
    use std::sync::Mutex;

    type Seen = Arc<Mutex<Vec<(String, Vec<(String, String)>)>>>;

    async fn serve(router: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        format!("http://{addr}")
    }

    fn recorder(
        seen: Seen,
        name: &'static str,
    ) -> impl Fn(AxHeaders) -> std::future::Ready<&'static str> + Clone + Send + Sync + 'static {
        move |h: AxHeaders| {
            let pairs = h
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
                .collect();
            seen.lock().unwrap().push((name.to_string(), pairs));
            std::future::ready("ok")
        }
    }

    fn has(seen: &Seen, hop: &str, header: &str) -> bool {
        seen.lock()
            .unwrap()
            .iter()
            .filter(|(n, _)| n == hop)
            .any(|(_, h)| h.iter().any(|(k, _)| k == header))
    }

    #[tokio::test]
    async fn a_cross_origin_redirect_drops_credentials() {
        let seen: Seen = Arc::default();
        let other = serve(Router::new().route("/land", get(recorder(seen.clone(), "other")))).await;
        let to = format!("{other}/land");
        let keep_to = to.clone();
        let same_seen = seen.clone();
        let origin = serve(
            Router::new()
                .route(
                    "/start",
                    get(move || {
                        let to = to.clone();
                        async move { (AxStatus::FOUND, [("location", to)]).into_response() }
                    }),
                )
                .route(
                    "/keep",
                    any(move || {
                        let to = keep_to.clone();
                        async move {
                            (AxStatus::TEMPORARY_REDIRECT, [("location", to)]).into_response()
                        }
                    }),
                )
                .route(
                    "/same",
                    get(|| async { (AxStatus::FOUND, [("location", "/here")]).into_response() }),
                )
                .route("/here", get(recorder(same_seen, "same"))),
        )
        .await;
        let client = Client::builder()
            .sensitive_header("x-api-key")
            .build()
            .unwrap();
        let mut secret = reqwest::header::HeaderValue::from_static("s3cr3t");
        secret.set_sensitive(true);
        let req = |path: &str| {
            client
                .get(format!("{origin}{path}"))
                .bearer_auth("tok")
                .header("x-api-key", "k")
                .header("x-session", secret.clone())
                .header("cookie", "a=b")
                .header("x-trace", "t")
        };
        let resp = client.send(req("/start")).await.unwrap();
        assert_eq!(resp.status(), 200);
        assert!(resp.url().as_str().ends_with("/land"));
        for h in ["authorization", "x-api-key", "x-session", "cookie"] {
            assert!(!has(&seen, "other", h), "{h} leaked cross-origin");
        }
        assert!(has(&seen, "other", "x-trace"));
        let resp = client
            .send(client.post(format!("{origin}/keep")).body("client_secret=s"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 307, "a body is not replayed to another origin");
        client.send(req("/same")).await.unwrap();
        for h in ["authorization", "x-api-key", "x-session", "cookie"] {
            assert!(has(&seen, "same", h), "{h} kept on a same-origin hop");
        }
    }

    #[tokio::test]
    async fn redirects_rewrite_methods_and_respect_the_limit() {
        let seen: Arc<Mutex<Vec<String>>> = Arc::default();
        let s = seen.clone();
        let base = serve(
            Router::new()
                .route(
                    "/see-other",
                    any(|| async { (AxStatus::SEE_OTHER, [("location", "/sink")]).into_response() }),
                )
                .route(
                    "/temp",
                    any(|| async {
                        (AxStatus::TEMPORARY_REDIRECT, [("location", "/sink")]).into_response()
                    }),
                )
                .route(
                    "/loop",
                    any(|| async { (AxStatus::FOUND, [("location", "/loop")]).into_response() }),
                )
                .route(
                    "/nowhere",
                    any(|| async { AxStatus::MOVED_PERMANENTLY.into_response() }),
                )
                .route(
                    "/sink",
                    any(move |m: axum::http::Method, body: String| {
                        s.lock().unwrap().push(format!("{m} {body}"));
                        async { "done" }
                    }),
                ),
        )
        .await;
        let client = Client::builder().max_redirects(3).build().unwrap();
        client
            .send(client.post(format!("{base}/see-other")).body("payload"))
            .await
            .unwrap();
        client
            .send(client.post(format!("{base}/temp")).body("payload"))
            .await
            .unwrap();
        assert_eq!(*seen.lock().unwrap(), vec!["GET ", "POST payload"]);
        let err = client
            .send(client.get(format!("{base}/loop")))
            .await
            .unwrap_err();
        assert!(
            matches!(&err, FaucetError::HttpStatus { status: 302, body, .. } if body.contains("too many")),
            "{err:?}"
        );
        let resp = client
            .send(client.get(format!("{base}/nowhere")))
            .await
            .unwrap();
        assert_eq!(resp.status(), 301, "no Location: returned as is");
        let none = Client::builder().max_redirects(0).build().unwrap();
        let resp = none.send(none.get(format!("{base}/loop"))).await.unwrap();
        assert_eq!(resp.status(), 302);
        let stream = reqwest::Body::wrap("x".to_string());
        let resp = client
            .send(client.post(format!("{base}/temp")).body(stream))
            .await
            .unwrap();
        assert_eq!(resp.status(), 307, "a streamed body cannot be replayed");
        let resp = client
            .send(client.head(format!("{base}/see-other")))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }

    #[tokio::test]
    async fn reads_are_bounded() {
        let base = serve(
            Router::new()
                .route("/big", get(|| async { "0123456789" }))
                .route(
                    "/chunked",
                    get(|| async {
                        axum::body::Body::from_stream(futures::stream::iter(vec![
                            Ok::<_, std::io::Error>("01234"),
                            Ok("56789"),
                        ]))
                    }),
                )
                .route("/json", get(|| async { r#"{"a":1}"# }))
                .route("/latin", get(|| async { vec![0xffu8, b'a'] })),
        )
        .await;
        let small = Client::builder()
            .max_response_bytes(Some(4))
            .build()
            .unwrap();
        for path in ["/big", "/chunked"] {
            let resp = small.send(small.get(format!("{base}{path}"))).await.unwrap();
            let err = small.bytes(resp).await.unwrap_err();
            let FaucetError::Custom(inner) = &err else {
                panic!("{err:?}")
            };
            let tl = inner.downcast_ref::<ResponseTooLarge>().unwrap();
            assert_eq!(tl.limit, 4);
            assert!(tl.to_string().contains("4-byte limit"));
        }
        let client = Client::new().unwrap();
        assert_eq!(client.max_response_bytes(), Some(DEFAULT_MAX_RESPONSE_BYTES));
        let resp = client.send(client.get(format!("{base}/json"))).await.unwrap();
        let v: serde_json::Value = client.json(resp).await.unwrap();
        assert_eq!(v["a"], 1);
        let resp = client.send(client.get(format!("{base}/latin"))).await.unwrap();
        assert_eq!(client.text(resp).await.unwrap(), "\u{fffd}a");
        let unbounded = Client::builder().max_response_bytes(None).build().unwrap();
        let resp = unbounded
            .send(unbounded.get(format!("{base}/chunked")))
            .await
            .unwrap();
        assert_eq!(unbounded.text(resp).await.unwrap(), "0123456789");
    }

    #[tokio::test]
    async fn execute_retries_and_classifies_statuses() {
        let hits = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let h = hits.clone();
        let base = serve(
            Router::new()
                .route(
                    "/flaky",
                    get(move || {
                        let n = h.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        async move {
                            if n == 0 {
                                AxStatus::BAD_GATEWAY.into_response()
                            } else {
                                "fine".into_response()
                            }
                        }
                    }),
                )
                .route(
                    "/limited",
                    get(|| async {
                        (AxStatus::TOO_MANY_REQUESTS, [("retry-after", "3")]).into_response()
                    }),
                )
                .route(
                    "/missing",
                    get(|| async { (AxStatus::NOT_FOUND, "nope").into_response() }),
                ),
        )
        .await;
        let client = Client::new().unwrap();
        let ctx = CallContext::new().with_retry(RetryPolicy {
            max_attempts: 3,
            backoff: BackoffKind::None,
            jitter: false,
            ..RetryPolicy::default()
        });
        let resp = client
            .execute(&ctx, "get", || client.get(format!("{base}/flaky")))
            .await
            .unwrap();
        assert_eq!(client.text(resp).await.unwrap(), "fine");
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 2);
        let err = client
            .execute(&CallContext::new(), "get", || {
                client.get(format!("{base}/limited"))
            })
            .await
            .unwrap_err();
        assert!(
            matches!(err, FaucetError::RateLimited(d) if d == Duration::from_secs(3)),
            "{err:?}"
        );
        let err = client
            .execute(&ctx, "get", || {
                client.get(format!("http://u:pw@{}/missing", &base[7..]))
            })
            .await
            .unwrap_err();
        match err {
            FaucetError::HttpStatus { status, url, body } => {
                assert_eq!(status, 404);
                assert!(!url.contains("pw"), "{url}");
                assert_eq!(body, "nope");
            }
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn transport_errors_keep_their_class() {
        let client = Client::builder()
            .timeout(Duration::from_millis(50))
            .connect_timeout(Duration::from_millis(50))
            .read_timeout(Duration::from_millis(50))
            .user_agent("faucet-test")
            .default_headers(HeaderMap::new())
            .no_proxy()
            .configure(|b| b.pool_max_idle_per_host(1))
            .try_configure(Ok)
            .unwrap()
            .build()
            .unwrap();
        let base = serve(Router::new().route(
            "/slow",
            get(|| async {
                #[allow(clippy::disallowed_methods)]
                tokio::time::sleep(Duration::from_secs(5)).await;
                "late"
            }),
        ))
        .await;
        let err = client
            .send(client.get(format!("{base}/slow")))
            .await
            .unwrap_err();
        assert_eq!(
            crate::resilience::classify(&err),
            Some(crate::RetryClass::Timeout),
            "{err:?}"
        );
        let err = client
            .send(client.get("http://127.0.0.1:1/"))
            .await
            .unwrap_err();
        assert!(crate::resilience::classify(&err).is_some(), "{err:?}");
        let err = client.send(client.get("not a url")).await.unwrap_err();
        assert!(matches!(err, FaucetError::Http(_)));
        for m in [client.put("http://h/"), client.delete("http://h/")] {
            assert!(m.build().is_ok());
        }
        assert!(client.request(Method::PATCH, "http://h/").build().is_ok());
        assert!(format!("{client:?}").contains("Client"));
        assert!(format!("{:?}", Client::builder()).contains("ClientBuilder"));
        assert!(format!("{:?}", ClientBuilder::default()).contains("max_redirects"));
        let _ = client.inner();
    }

    #[test]
    fn invalid_builder_input_fails_build() {
        assert!(matches!(
            Client::builder().proxy("::not a url::").build(),
            Err(FaucetError::Config(_))
        ));
        assert!(matches!(
            Client::builder().sensitive_header("bad header").build(),
            Err(FaucetError::Config(_))
        ));
        assert!(Client::builder().proxy("http://proxy.local:3128").build().is_ok());
        assert!(
            Client::builder()
                .try_configure(|_| Err(FaucetError::Config("no".into())))
                .is_err()
        );
    }

    #[test]
    fn origins_compare_scheme_host_and_port() {
        let u = |s: &str| Url::parse(s).unwrap();
        assert!(!is_cross_origin(&u("https://a/x"), &u("https://a:443/y")));
        assert!(is_cross_origin(&u("https://a/x"), &u("http://a/x")));
        assert!(is_cross_origin(&u("https://a/x"), &u("https://b/x")));
        assert!(is_cross_origin(&u("https://a/x"), &u("https://a:8443/x")));
    }

    #[tokio::test]
    async fn status_error_without_a_failure_status() {
        let base = serve(Router::new().route("/ok", get(|| async { "fine" }))).await;
        let client = Client::new().unwrap();
        let resp = client.send(client.get(format!("{base}/ok"))).await.unwrap();
        assert!(matches!(
            status_error(resp).await,
            FaucetError::HttpStatus { status: 200, .. }
        ));
    }
}
