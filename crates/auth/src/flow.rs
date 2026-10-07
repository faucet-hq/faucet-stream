//! Composable multi-step auth **flow** provider (#511).
//!
//! Turns `AuthProvider` from a single-step credential source into a small
//! declarative program: an optional login / pre-flight request chain whose
//! responses are captured (by JSONPath into a JSON body, an XML dot-path, a
//! response header, or a `Set-Cookie` value — #542), arbitrary credential
//! *placement* (header / query / cookie / body), a pluggable HMAC request
//! *signer* usable on both data requests **and** the login steps themselves
//! (#541), and a dynamic per-session base-URL — on top of the existing
//! single-flight machinery.
//!
//! ```yaml
//! auth:
//!   login_chain:
//!     type: flow
//!     config:
//!       steps:
//!         - request: { method: POST, url: "https://auth/oauth/token",
//!                      form: { grant_type: refresh_token, refresh_token: "..." } }
//!           capture: { access_token: "$.access_token" }
//!         - request: { method: GET, url: "https://login/rest/login",
//!                      query: { access_token: "${access_token}" } }
//!           capture: { bh_rest_token: "$.BhRestToken", base_url: "$.restUrl" }
//!       apply:
//!         - { into: query, name: BhRestToken, value: "${bh_rest_token}" }
//!       base_url_from: "${base_url}"
//!       ttl_secs: 86400
//!       reauth_on: [401]
//! ```

use crate::auth_http_client;
use async_trait::async_trait;
use base64::Engine;
use faucet_core::{AuthProvider, Credential, CredentialPlacement, FaucetError, RequestAuth};
use hmac::{Hmac, Mac};
use jsonpath_rust::JsonPath;
use serde::Deserialize;
use serde_json::Value;
use sha2::Sha256;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant};

type HmacSha256 = Hmac<Sha256>;

// ── Config ──────────────────────────────────────────────────────────────────

/// One HTTP request in the login / pre-flight chain.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct FlowRequest {
    #[serde(default = "default_method")]
    method: String,
    url: String,
    #[serde(default)]
    headers: HashMap<String, String>,
    #[serde(default)]
    query: HashMap<String, String>,
    /// `application/x-www-form-urlencoded` body.
    #[serde(default)]
    form: Option<HashMap<String, String>>,
    /// JSON body (mutually exclusive with `form`).
    #[serde(default)]
    json: Option<Value>,
    /// Optional HMAC signature computed over `template` and attached to this
    /// login/pre-flight request itself (#541). Uses a fresh `${ts}`/`${nonce}`
    /// clock per step, the same semantics as an `apply` signer. On the **first**
    /// step nothing has been captured yet, so its `template` may reference only
    /// `${param.*}` / `${env:*}` / `${ts}` / `${nonce}` (later steps also see
    /// values captured by earlier steps via `${name}`).
    #[serde(default)]
    sign: Option<SignSpec>,
}

fn default_method() -> String {
    "GET".to_owned()
}

/// Where a login value is captured from (#542). Defaults to `json` for
/// back-compat: `capture: { name: "$.jsonpath" }` (the bare-string form) still
/// parses as a JSONPath into the JSON response body.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CaptureFrom {
    /// JSONPath into the JSON response body.
    #[default]
    Json,
    /// Dot-path into an XML response body (element local names; namespace
    /// prefixes are ignored — `ns:tag` matches `tag`).
    Xml,
    /// A response header value (looked up case-insensitively).
    Header,
    /// A specific `Set-Cookie` value, selected by cookie name.
    SetCookie,
}

/// The structured (non-string) capture form: `{ from, name | path }`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct CaptureSource {
    from: CaptureFrom,
    /// Header / cookie name (`from: header | set_cookie`).
    #[serde(default)]
    name: Option<String>,
    /// JSONPath (`from: json`) or XML dot-path (`from: xml`).
    #[serde(default)]
    path: Option<String>,
}

/// How one login value is captured. Untagged so the historical bare-string
/// JSONPath form stays valid (`{ name: "$.path" }` ⇒ `from: json`), while a
/// struct form selects a richer source (#542).
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum CaptureSpec {
    /// `"$.jsonpath"` — JSONPath into the JSON response body (back-compat).
    Json(String),
    /// `{ from: json|xml|header|set_cookie, name|path }`.
    Source(CaptureSource),
}

impl CaptureSpec {
    fn kind(&self) -> CaptureFrom {
        match self {
            CaptureSpec::Json(_) => CaptureFrom::Json,
            CaptureSpec::Source(s) => s.from,
        }
    }

    /// Validate that the required selector field is present for the source.
    fn validate(&self) -> Result<(), &'static str> {
        match self {
            CaptureSpec::Json(p) if p.trim().is_empty() => Err("empty JSONPath"),
            CaptureSpec::Json(_) => Ok(()),
            CaptureSpec::Source(s) => match s.from {
                CaptureFrom::Json | CaptureFrom::Xml => match s.path.as_deref().map(str::trim) {
                    Some(p) if !p.is_empty() => Ok(()),
                    _ => Err("`from: json|xml` requires a non-empty `path`"),
                },
                CaptureFrom::Header | CaptureFrom::SetCookie => {
                    match s.name.as_deref().map(str::trim) {
                        Some(n) if !n.is_empty() => Ok(()),
                        _ => Err("`from: header|set_cookie` requires a non-empty `name`"),
                    }
                }
            },
        }
    }
}

/// A login step: a request plus the values to capture from its response.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct FlowStep {
    request: FlowRequest,
    /// Captured-name → capture source (JSONPath string for back-compat, or a
    /// `{ from, name|path }` struct for header / Set-Cookie / XML sources).
    #[serde(default)]
    capture: HashMap<String, CaptureSpec>,
}

/// Where a captured credential is placed into data requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PlaceTarget {
    Header,
    Query,
    Cookie,
    Body,
}

/// HMAC algorithm for the request signer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum SignAlg {
    HmacSha256,
}

/// Signature encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum SigEncoding {
    #[default]
    Hex,
    Base64,
}

/// Where a computed signature is placed (a header, with a value template).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SignInto {
    header: String,
    /// Value template; `${sig}` is the computed signature. Defaults to `${sig}`.
    #[serde(default = "default_sig_format")]
    format: String,
}

fn default_sig_format() -> String {
    "${sig}".to_owned()
}

/// A pluggable HMAC request signer.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SignSpec {
    alg: SignAlg,
    /// HMAC key (resolve via `${env:...}` / `${vault:...}` in the catalog).
    key: String,
    /// The signature base string; `${captured}`, `${ts}`, `${nonce}` are
    /// substituted before signing.
    template: String,
    #[serde(default)]
    encoding: SigEncoding,
    into: SignInto,
}

/// One `apply` entry: either a static placement or a computed signature.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
enum ApplySpec {
    /// `{ into, name, value }` — place a (templated) value.
    Place {
        into: PlaceTarget,
        name: String,
        /// Value template; `${captured}` substituted.
        value: String,
    },
    /// `{ sign: { ... } }` — compute and place an HMAC signature.
    Sign { sign: SignSpec },
}

/// The `type: flow` provider config.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct FlowConfig {
    /// Login / pre-flight chain, run in order; each captures values for later
    /// steps and for `apply`.
    #[serde(default)]
    steps: Vec<FlowStep>,
    /// Credential placements applied to every data request.
    #[serde(default)]
    apply: Vec<ApplySpec>,
    /// Template (over captured values) yielding a per-session base-URL that
    /// overrides the connector's configured `base_url`.
    #[serde(default)]
    base_url_from: Option<String>,
    /// Re-run the login chain after this many seconds.
    #[serde(default)]
    ttl_secs: Option<u64>,
    /// HTTP statuses that trigger a re-login (via `invalidate`). Advisory —
    /// stored for connectors that wire status-based reauth.
    #[serde(default)]
    reauth_on: Vec<u16>,
    // Future work (#542): an optional `cookie_jar: true` that shares a cookie
    // store between the login client and the connector's HTTP client, so
    // `Set-Cookie`s from login `steps` forward to data requests automatically.
    // Out of scope here — the `capture: { from: set_cookie }` → `apply:
    // { into: cookie }` path already expresses the same case (a cookie-session login).
}

impl FlowConfig {
    fn validate(&self) -> Result<(), FaucetError> {
        if self.steps.is_empty() && self.apply.is_empty() {
            return Err(FaucetError::Config(
                "flow auth: at least one of `steps` or `apply` is required".to_owned(),
            ));
        }
        for (i, step) in self.steps.iter().enumerate() {
            if step.request.url.trim().is_empty() {
                return Err(FaucetError::Config(format!(
                    "flow auth: step {i} has an empty `url`"
                )));
            }
            if step.request.form.is_some() && step.request.json.is_some() {
                return Err(FaucetError::Config(format!(
                    "flow auth: step {i} sets both `form` and `json`; pick one"
                )));
            }
            if let Some(sign) = &step.request.sign
                && sign.into.header.trim().is_empty()
            {
                return Err(FaucetError::Config(format!(
                    "flow auth: step {i} sign.into.header must not be empty"
                )));
            }
            for (name, cap) in &step.capture {
                if let Err(msg) = cap.validate() {
                    return Err(FaucetError::Config(format!(
                        "flow auth: step {i} capture '{name}': {msg}"
                    )));
                }
            }
        }
        for (i, a) in self.apply.iter().enumerate() {
            match a {
                ApplySpec::Place { name, .. } if name.trim().is_empty() => {
                    return Err(FaucetError::Config(format!(
                        "flow auth: apply[{i}] has an empty `name`"
                    )));
                }
                ApplySpec::Sign { sign } if sign.into.header.trim().is_empty() => {
                    return Err(FaucetError::Config(format!(
                        "flow auth: apply[{i}].sign.into.header must not be empty"
                    )));
                }
                _ => {}
            }
        }
        Ok(())
    }
}

// ── Template rendering ───────────────────────────────────────────────────────

/// Substitute `${key}` tokens from `ctx`. Unknown tokens are left verbatim.
/// Single-pass — a substituted value is never re-scanned.
fn render(template: &str, ctx: &HashMap<String, String>) -> String {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        if let Some(end) = after.find('}') {
            let key = &after[..end];
            match ctx.get(key) {
                Some(v) => out.push_str(v),
                None => {
                    out.push_str("${");
                    out.push_str(key);
                    out.push('}');
                }
            }
            rest = &after[end + 1..];
        } else {
            out.push_str(&rest[start..]);
            rest = "";
        }
    }
    out.push_str(rest);
    out
}

/// A captured JSON value rendered to its string form for templating.
fn value_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn jsonpath_first(body: &Value, path: &str) -> Option<Value> {
    let results = body.query(path).ok()?;
    results.first().map(|v| (*v).clone())
}

fn to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn hmac_sign(key: &str, message: &str, encoding: SigEncoding) -> String {
    let mut mac =
        HmacSha256::new_from_slice(key.as_bytes()).expect("HMAC accepts a key of any length");
    mac.update(message.as_bytes());
    let bytes = mac.finalize().into_bytes();
    match encoding {
        SigEncoding::Hex => to_hex(&bytes),
        SigEncoding::Base64 => base64::engine::general_purpose::STANDARD.encode(bytes),
    }
}

/// Compute the `(header-name, header-value)` for a signer given the render
/// context. `${captured}`, `${ts}`, `${nonce}` in `template` are substituted
/// before signing; `${sig}` in `into.format` becomes the computed signature.
fn sign_header(sign: &SignSpec, ctx: &HashMap<String, String>) -> (String, String) {
    let base = render(&sign.template, ctx);
    let sig = match sign.alg {
        SignAlg::HmacSha256 => hmac_sign(&sign.key, &base, sign.encoding),
    };
    let mut sig_ctx = ctx.clone();
    sig_ctx.insert("sig".to_owned(), sig);
    let value = render(&sign.into.format, &sig_ctx);
    (sign.into.header.clone(), value)
}

/// First response-header value (case-insensitive), as a string.
fn header_value(headers: &reqwest::header::HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_owned())
}

/// The value of a specific `Set-Cookie` cookie, selected by cookie name.
fn set_cookie_value(headers: &reqwest::header::HeaderMap, name: &str) -> Option<String> {
    for v in headers.get_all("set-cookie") {
        let Ok(s) = v.to_str() else { continue };
        // A Set-Cookie value is `name=value; attr; attr…`; the cookie pair is
        // the first `;`-delimited segment.
        let pair = s.split(';').next().unwrap_or("");
        if let Some((k, val)) = pair.split_once('=')
            && k.trim() == name
        {
            return Some(val.trim().to_owned());
        }
    }
    None
}

// ── Minimal XML dot-path extraction (#542) ───────────────────────────────────
//
// A tiny, dependency-free XML walk: enough to pull a scalar (e.g. a session id)
// out of a login response by element local name. Not a general XML parser — it
// ignores attributes, namespace prefixes (`ns:tag` matches `tag`), and PIs, and
// decodes only the five predefined entities. Deliberately kept in-crate rather
// than depending on the XML *source* connector.

#[derive(Debug)]
struct XmlNode {
    tag: String,
    text: String,
    children: Vec<XmlNode>,
}

enum XmlToken {
    Start(String),
    End,
    SelfClose(String),
    Text(String),
}

/// Local element name: strip a namespace prefix and any attributes.
fn xml_local_name(raw: &str) -> String {
    let name = raw.split_whitespace().next().unwrap_or("");
    match name.split_once(':') {
        Some((_, local)) => local.to_owned(),
        None => name.to_owned(),
    }
}

fn xml_unescape(s: &str) -> String {
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

fn xml_tokenize(input: &str) -> Vec<XmlToken> {
    let mut out = Vec::new();
    let mut rest = input;
    while let Some(lt) = rest.find('<') {
        let text = &rest[..lt];
        if !text.trim().is_empty() {
            out.push(XmlToken::Text(xml_unescape(text)));
        }
        let after = &rest[lt..];
        // CDATA: take its content verbatim as text.
        if let Some(cdata) = after.strip_prefix("<![CDATA[") {
            if let Some(end) = cdata.find("]]>") {
                let content = &cdata[..end];
                if !content.trim().is_empty() {
                    out.push(XmlToken::Text(content.to_owned()));
                }
                rest = &cdata[end + 3..];
                continue;
            }
            break;
        }
        let Some(gt_rel) = after.find('>') else { break };
        let inner = &after[1..gt_rel]; // between '<' and '>'
        rest = &after[gt_rel + 1..];
        if inner.starts_with('?') || inner.starts_with('!') {
            // XML declaration, comment, or doctype — skip.
            continue;
        }
        if let Some(close) = inner.strip_prefix('/') {
            let _ = close;
            out.push(XmlToken::End);
        } else if let Some(sc) = inner.strip_suffix('/') {
            out.push(XmlToken::SelfClose(xml_local_name(sc)));
        } else {
            out.push(XmlToken::Start(xml_local_name(inner)));
        }
    }
    out
}

fn xml_build_tree(tokens: Vec<XmlToken>) -> XmlNode {
    let mut stack: Vec<XmlNode> = vec![XmlNode {
        tag: String::new(),
        text: String::new(),
        children: Vec::new(),
    }];
    for tok in tokens {
        match tok {
            XmlToken::Start(tag) => stack.push(XmlNode {
                tag,
                text: String::new(),
                children: Vec::new(),
            }),
            XmlToken::SelfClose(tag) => {
                if let Some(parent) = stack.last_mut() {
                    parent.children.push(XmlNode {
                        tag,
                        text: String::new(),
                        children: Vec::new(),
                    });
                }
            }
            XmlToken::Text(t) => {
                if let Some(node) = stack.last_mut() {
                    node.text.push_str(&t);
                }
            }
            XmlToken::End => {
                if stack.len() > 1 {
                    let node = stack.pop().unwrap();
                    stack.last_mut().unwrap().children.push(node);
                }
            }
        }
    }
    // Unwind any unclosed elements into the root.
    while stack.len() > 1 {
        let node = stack.pop().unwrap();
        stack.last_mut().unwrap().children.push(node);
    }
    stack.pop().unwrap()
}

/// Walk a dot-path of element local names, returning the trimmed text of the
/// first matching element. `path` = `a.b.c`; namespace prefixes are ignored.
fn xml_dot_path(xml: &str, path: &str) -> Option<String> {
    let root = xml_build_tree(xml_tokenize(xml));
    let mut current = &root;
    for seg in path.split('.') {
        let seg = xml_local_name(seg.trim());
        if seg.is_empty() {
            continue;
        }
        current = current.children.iter().find(|c| c.tag == seg)?;
    }
    Some(current.text.trim().to_owned())
}

/// Build the placements + base-URL for one request from the captured context.
/// Pure given `captured` (and the clock values injected by the caller).
fn build_request_auth(
    apply: &[ApplySpec],
    base_url_from: &Option<String>,
    ctx: &HashMap<String, String>,
) -> RequestAuth {
    let mut out = RequestAuth::new();
    for a in apply {
        match a {
            ApplySpec::Place { into, name, value } => {
                let value = render(value, ctx);
                let name = name.clone();
                let placement = match into {
                    PlaceTarget::Header => CredentialPlacement::Header { name, value },
                    PlaceTarget::Query => CredentialPlacement::Query { name, value },
                    PlaceTarget::Cookie => CredentialPlacement::Cookie { name, value },
                    PlaceTarget::Body => CredentialPlacement::BodyField { name, value },
                };
                out = out.with_placement(placement);
            }
            ApplySpec::Sign { sign } => {
                let (name, value) = sign_header(sign, ctx);
                out = out.with_placement(CredentialPlacement::Header { name, value });
            }
        }
    }
    if let Some(tmpl) = base_url_from {
        let rendered = render(tmpl, ctx);
        if !rendered.is_empty() {
            out = out.with_base_url(rendered);
        }
    }
    // Expose the captured context so a connector can substitute `${name}` into a
    // raw body/header string (an XML/SOAP `sessionid`, #567) — the same names the
    // `apply` placements above draw from.
    out = out.with_captured(ctx.iter().map(|(k, v)| (k.clone(), v.clone())).collect());
    out
}

// ── Provider ────────────────────────────────────────────────────────────────

struct Session {
    /// Captured values (as strings, ready for templating) plus a fresh clock.
    ctx: HashMap<String, String>,
    expires_at: Option<Instant>,
}

impl Session {
    fn valid(&self) -> bool {
        match self.expires_at {
            Some(exp) => Instant::now() < exp,
            None => true,
        }
    }
}

/// A composable multi-step auth flow provider.
pub struct FlowProvider {
    http: reqwest::Client,
    config: FlowConfig,
    state: Mutex<Option<Session>>,
}

impl std::fmt::Debug for FlowProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlowProvider")
            .field("steps", &self.config.steps.len())
            .field("apply", &self.config.apply.len())
            .finish()
    }
}

impl FlowProvider {
    /// Build a flow provider from its `config` block.
    pub fn from_config(config: &Value) -> Result<Self, FaucetError> {
        let config: FlowConfig = serde_json::from_value(config.clone())
            .map_err(|e| FaucetError::Config(format!("flow auth: invalid config: {e}")))?;
        config.validate()?;
        Ok(Self {
            http: auth_http_client(),
            config,
            state: Mutex::new(None),
        })
    }

    /// A fresh clock context (`ts`, `nonce`) for signing.
    fn clock_ctx() -> HashMap<String, String> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0);
        let mut ctx = HashMap::new();
        ctx.insert("ts".to_owned(), now.to_string());
        ctx.insert("nonce".to_owned(), format!("{now}{nanos}"));
        ctx
    }

    /// Run the login chain, returning the captured context.
    async fn run_login(&self) -> Result<HashMap<String, String>, FaucetError> {
        let mut ctx: HashMap<String, String> = HashMap::new();
        for (i, step) in self.config.steps.iter().enumerate() {
            let req = &step.request;
            let method = reqwest::Method::from_bytes(req.method.to_uppercase().as_bytes())
                .map_err(|_| {
                    FaucetError::Config(format!(
                        "flow auth: step {i} has an invalid HTTP method '{}'",
                        req.method
                    ))
                })?;
            let url = render(&req.url, &ctx);
            let mut builder = self.http.request(method, &url);
            for (k, v) in &req.headers {
                builder = builder.header(k.as_str(), render(v, &ctx));
            }
            if !req.query.is_empty() {
                let q: Vec<(String, String)> = req
                    .query
                    .iter()
                    .map(|(k, v)| (k.clone(), render(v, &ctx)))
                    .collect();
                builder = builder.query(&q);
            }
            if let Some(form) = &req.form {
                let f: Vec<(String, String)> = form
                    .iter()
                    .map(|(k, v)| (k.clone(), render(v, &ctx)))
                    .collect();
                builder = builder.form(&f);
            } else if let Some(json) = &req.json {
                let rendered = render(&serde_json::to_string(json).unwrap_or_default(), &ctx);
                let body: Value = serde_json::from_str(&rendered).unwrap_or_else(|_| json.clone());
                builder = builder.json(&body);
            }
            // Sign this login/pre-flight step itself (#541): a fresh clock,
            // captured values so far, then a header placement.
            if let Some(sign) = &req.sign {
                let mut sign_ctx = ctx.clone();
                sign_ctx.extend(Self::clock_ctx());
                let (name, value) = sign_header(sign, &sign_ctx);
                builder = builder.header(name.as_str(), value);
            }
            let resp = builder.send().await.map_err(|e| {
                FaucetError::Auth(format!("flow auth: step {i} request failed: {e}"))
            })?;
            let status = resp.status();
            if !status.is_success() {
                return Err(FaucetError::Auth(format!(
                    "flow auth: step {i} returned HTTP {}",
                    status.as_u16()
                )));
            }
            // Decide which parts of the response the captures need. Headers are
            // read first (they borrow), then the body is consumed at most once.
            let headers = resp.headers().clone();
            let needs_json = step.capture.values().any(|c| c.kind() == CaptureFrom::Json);
            let needs_xml = step.capture.values().any(|c| c.kind() == CaptureFrom::Xml);
            let text: Option<String> = if needs_json || needs_xml {
                Some(resp.text().await.map_err(|e| {
                    FaucetError::Auth(format!(
                        "flow auth: step {i} failed to read response body: {e}"
                    ))
                })?)
            } else {
                None
            };
            let json_body: Option<Value> = if needs_json {
                Some(
                    serde_json::from_str(text.as_deref().unwrap_or("")).map_err(|e| {
                        FaucetError::Auth(format!(
                            "flow auth: step {i} response was not valid JSON: {e}"
                        ))
                    })?,
                )
            } else {
                None
            };
            for (name, cap) in &step.capture {
                let extracted = match cap {
                    CaptureSpec::Json(path) => json_body
                        .as_ref()
                        .and_then(|b| jsonpath_first(b, path))
                        .map(|v| value_to_string(&v)),
                    CaptureSpec::Source(s) => match s.from {
                        CaptureFrom::Json => json_body
                            .as_ref()
                            .and_then(|b| jsonpath_first(b, s.path.as_deref().unwrap_or("")))
                            .map(|v| value_to_string(&v)),
                        CaptureFrom::Xml => text
                            .as_deref()
                            .and_then(|t| xml_dot_path(t, s.path.as_deref().unwrap_or(""))),
                        CaptureFrom::Header => {
                            header_value(&headers, s.name.as_deref().unwrap_or(""))
                        }
                        CaptureFrom::SetCookie => {
                            set_cookie_value(&headers, s.name.as_deref().unwrap_or(""))
                        }
                    },
                };
                match extracted {
                    Some(v) => {
                        ctx.insert(name.clone(), v);
                    }
                    None => {
                        return Err(FaucetError::Auth(format!(
                            "flow auth: step {i} capture '{name}' matched nothing"
                        )));
                    }
                }
            }
        }
        Ok(ctx)
    }

    /// Ensure a valid session, returning its captured context (single-flight:
    /// the lock is held across the login network calls).
    async fn ensure_ctx(&self) -> Result<HashMap<String, String>, FaucetError> {
        let mut guard = self.state.lock().await;
        if let Some(s) = guard.as_ref()
            && s.valid()
        {
            return Ok(s.ctx.clone());
        }
        let ctx = self.run_login().await?;
        let expires_at = self
            .config
            .ttl_secs
            .and_then(|s| Instant::now().checked_add(Duration::from_secs(s)));
        *guard = Some(Session {
            ctx: ctx.clone(),
            expires_at,
        });
        Ok(ctx)
    }

    fn request_auth_from_ctx(&self, captured: &HashMap<String, String>) -> RequestAuth {
        let mut ctx = captured.clone();
        ctx.extend(Self::clock_ctx());
        build_request_auth(&self.config.apply, &self.config.base_url_from, &ctx)
    }
}

#[async_trait]
impl AuthProvider for FlowProvider {
    async fn credential(&self) -> Result<Credential, FaucetError> {
        let ctx = self.ensure_ctx().await?;
        let auth = self.request_auth_from_ctx(&ctx);
        // Header/bearer fallback for connectors that only consume `credential()`
        // (xml, graphql): return the first header placement as a header
        // credential. Query/cookie/body placements need `request_auth`.
        for p in &auth.placements {
            if let CredentialPlacement::Header { name, value } = p {
                return Ok(Credential::Header {
                    name: name.clone(),
                    value: value.clone(),
                });
            }
        }
        Err(FaucetError::Auth(
            "flow auth: no header credential to apply via credential(); this flow places its \
             credential in a query/cookie/body, which requires a connector that consumes \
             request_auth() (e.g. the REST source)"
                .to_owned(),
        ))
    }

    async fn invalidate(&self, _stale: &Credential) -> Result<Credential, FaucetError> {
        // Force a re-login on the next ensure_ctx.
        *self.state.lock().await = None;
        self.credential().await
    }

    async fn request_auth(
        &self,
        _method: &str,
        _url: &str,
        _query: &std::collections::BTreeMap<String, String>,
    ) -> Result<RequestAuth, FaucetError> {
        let ctx = self.ensure_ctx().await?;
        Ok(self.request_auth_from_ctx(&ctx))
    }

    fn reauth_statuses(&self) -> &[u16] {
        &self.config.reauth_on
    }

    fn provider_name(&self) -> &'static str {
        "flow"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ctx(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn render_substitutes_known_and_leaves_unknown() {
        let c = ctx(&[("token", "abc"), ("region", "eu")]);
        assert_eq!(render("Bearer ${token}", &c), "Bearer abc");
        assert_eq!(render("https://${region}.api", &c), "https://eu.api");
        assert_eq!(render("${missing}", &c), "${missing}");
        assert_eq!(render("no tokens", &c), "no tokens");
        assert_eq!(render("${token}${region}", &c), "abceu");
    }

    #[test]
    fn render_handles_unterminated_and_utf8() {
        let c = ctx(&[("x", "✓")]);
        assert_eq!(render("prefix ${x} ünïcode", &c), "prefix ✓ ünïcode");
        assert_eq!(render("dangling ${x", &c), "dangling ${x");
    }

    #[test]
    fn value_to_string_covers_kinds() {
        assert_eq!(value_to_string(&json!("s")), "s");
        assert_eq!(value_to_string(&json!(7)), "7");
        assert_eq!(value_to_string(&json!(true)), "true");
        assert_eq!(value_to_string(&json!(null)), "");
        assert_eq!(value_to_string(&json!([1, 2])), "[1,2]");
    }

    #[test]
    fn jsonpath_first_extracts_scalar() {
        let body = json!({"data": {"BhRestToken": "tok", "restUrl": "https://host/x"}});
        assert_eq!(
            jsonpath_first(&body, "$.data.BhRestToken"),
            Some(json!("tok"))
        );
        assert_eq!(
            jsonpath_first(&body, "$.data.restUrl"),
            Some(json!("https://host/x"))
        );
        assert_eq!(jsonpath_first(&body, "$.data.missing"), None);
    }

    #[test]
    fn hmac_hex_and_base64_are_deterministic() {
        let hex = hmac_sign("key", "message", SigEncoding::Hex);
        // Known HMAC-SHA256("key","message") hex vector.
        assert_eq!(
            hex,
            "6e9ef29b75fffc5b7abae527d58fdadb2fe42e7219011976917343065f58ed4a"
        );
        let b64 = hmac_sign("key", "message", SigEncoding::Base64);
        assert_eq!(b64, "bp7ym3X//Ft6uuUn1Y/a2y/kLnIZARl2kXNDBl9Y7Uo=");
    }

    #[test]
    fn build_request_auth_places_header_query_cookie_body() {
        let apply: Vec<ApplySpec> = serde_json::from_value(json!([
            { "into": "header", "name": "X-Tok", "value": "${tok}" },
            { "into": "query",  "name": "access_token", "value": "${tok}" },
            { "into": "cookie", "name": "sid", "value": "${sid}" },
            { "into": "body",   "name": "auth", "value": "${tok}" }
        ]))
        .unwrap();
        let c = ctx(&[("tok", "T"), ("sid", "S")]);
        let ra = build_request_auth(&apply, &None, &c);
        assert_eq!(ra.placements.len(), 4);
        assert!(
            matches!(&ra.placements[0], CredentialPlacement::Header { name, value } if name=="X-Tok" && value=="T")
        );
        assert!(
            matches!(&ra.placements[1], CredentialPlacement::Query { name, value } if name=="access_token" && value=="T")
        );
        assert!(
            matches!(&ra.placements[2], CredentialPlacement::Cookie { name, value } if name=="sid" && value=="S")
        );
        assert!(
            matches!(&ra.placements[3], CredentialPlacement::BodyField { name, value } if name=="auth" && value=="T")
        );
        assert!(ra.base_url.is_none());
        // #567: the captured context is exposed for `${name}` body/header
        // substitution by connectors (e.g. an XML `sessionid`).
        assert_eq!(ra.captured.get("tok").map(String::as_str), Some("T"));
        assert_eq!(ra.captured.get("sid").map(String::as_str), Some("S"));
    }

    #[test]
    fn build_request_auth_signs_and_sets_base_url() {
        let apply: Vec<ApplySpec> = serde_json::from_value(json!([
            { "sign": { "alg": "hmac_sha256", "key": "secret", "template": "${client}:${ts}",
                        "encoding": "hex", "into": { "header": "Authorization", "format": "SS ${sig}" } } }
        ]))
        .unwrap();
        let mut c = ctx(&[
            ("client", "abc"),
            ("ts", "100"),
            ("base_url", "https://eu.host"),
        ]);
        c.insert("base_url".to_owned(), "https://eu.host".to_owned());
        let ra = build_request_auth(&apply, &Some("${base_url}".to_owned()), &c);
        assert_eq!(ra.placements.len(), 1);
        let expected = format!("SS {}", hmac_sign("secret", "abc:100", SigEncoding::Hex));
        assert!(
            matches!(&ra.placements[0], CredentialPlacement::Header { name, value } if name=="Authorization" && *value==expected)
        );
        assert_eq!(ra.base_url.as_deref(), Some("https://eu.host"));
    }

    #[test]
    fn base_url_from_empty_render_is_ignored() {
        let ra = build_request_auth(&[], &Some("${missing_and_stripped}".to_owned()), &ctx(&[]));
        // The unknown token renders verbatim (non-empty), so it is set; an
        // actually-empty render (empty template) is ignored.
        assert!(ra.base_url.is_some());
        let ra2 = build_request_auth(&[], &Some("${e}".to_owned()), &ctx(&[("e", "")]));
        assert!(ra2.base_url.is_none());
    }

    #[test]
    fn config_validate_requires_steps_or_apply() {
        let empty: FlowConfig = serde_json::from_value(json!({})).unwrap();
        assert!(empty.validate().is_err());
    }

    #[test]
    fn config_rejects_form_and_json_together() {
        let cfg: FlowConfig = serde_json::from_value(json!({
            "steps": [ { "request": { "url": "https://x", "form": {"a":"b"}, "json": {"c":"d"} } } ]
        }))
        .unwrap();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn config_rejects_empty_place_name_and_sign_header() {
        let bad_place: FlowConfig = serde_json::from_value(json!({
            "apply": [ { "into": "query", "name": "", "value": "${x}" } ]
        }))
        .unwrap();
        assert!(bad_place.validate().is_err());

        let bad_sign: FlowConfig = serde_json::from_value(json!({
            "apply": [ { "sign": { "alg": "hmac_sha256", "key": "k", "template": "${ts}",
                                   "into": { "header": "" } } } ]
        }))
        .unwrap();
        assert!(bad_sign.validate().is_err());
    }

    #[test]
    fn from_config_rejects_unknown_field() {
        assert!(FlowProvider::from_config(&json!({ "bogus": 1 })).is_err());
    }

    #[test]
    fn build_provider_dispatches_flow() {
        let p = crate::build_provider(&json!({
            "type": "flow",
            "config": { "apply": [ { "into": "header", "name": "X", "value": "v" } ] }
        }))
        .unwrap();
        assert_eq!(p.provider_name(), "flow");
    }

    #[test]
    fn debug_redacts_and_summarizes() {
        let p = FlowProvider::from_config(&json!({
            "apply": [ { "into": "header", "name": "X", "value": "${t}" } ]
        }))
        .unwrap();
        let s = format!("{p:?}");
        assert!(s.contains("FlowProvider"));
        assert!(s.contains("apply"));
    }

    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use wiremock::matchers::{body_json, header, method, path};
    use wiremock::{Mock, MockServer, Respond, ResponseTemplate};

    #[tokio::test]
    async fn login_chain_captures_placements_and_base_url() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"access_token": "AT"})))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/login"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(
                    json!({"BhRestToken": "BRT", "restUrl": "https://data.example"}),
                ),
            )
            .mount(&server)
            .await;

        let cfg = json!({
            "steps": [
                { "request": { "method": "POST", "url": format!("{}/token", server.uri()),
                               "form": {"grant_type": "refresh_token"} },
                  "capture": { "access_token": "$.access_token" } },
                { "request": { "method": "GET", "url": format!("{}/login", server.uri()),
                               "query": {"access_token": "${access_token}"} },
                  "capture": { "bh_rest_token": "$.BhRestToken", "base_url": "$.restUrl" } }
            ],
            "apply": [ { "into": "query", "name": "BhRestToken", "value": "${bh_rest_token}" } ],
            "base_url_from": "${base_url}",
            "reauth_on": [401]
        });
        let p = FlowProvider::from_config(&cfg).unwrap();
        let ra = p
            .request_auth("GET", "https://data.example/x", &BTreeMap::new())
            .await
            .unwrap();
        assert_eq!(ra.base_url.as_deref(), Some("https://data.example"));
        assert!(
            matches!(&ra.placements[0], CredentialPlacement::Query { name, value } if name == "BhRestToken" && value == "BRT")
        );
        assert_eq!(p.reauth_statuses(), &[401]);
        // A query placement has no header credential to apply via credential().
        assert!(p.credential().await.is_err());
    }

    struct CountingLogin(Arc<AtomicUsize>);
    impl Respond for CountingLogin {
        fn respond(&self, _: &wiremock::Request) -> ResponseTemplate {
            let n = self.0.fetch_add(1, Ordering::SeqCst) + 1;
            ResponseTemplate::new(200).set_body_json(json!({ "sid": format!("S{n}") }))
        }
    }

    #[tokio::test]
    async fn header_credential_caches_then_reloads_on_invalidate() {
        let server = MockServer::start().await;
        let hits = Arc::new(AtomicUsize::new(0));
        Mock::given(method("POST"))
            .and(path("/login"))
            .respond_with(CountingLogin(hits.clone()))
            .mount(&server)
            .await;
        let cfg = json!({
            "steps": [ { "request": { "method": "POST", "url": format!("{}/login", server.uri()) },
                         "capture": { "sid": "$.sid" } } ],
            "apply": [ { "into": "header", "name": "X-Session", "value": "${sid}" } ]
        });
        let p = FlowProvider::from_config(&cfg).unwrap();

        let c1 = p.credential().await.unwrap();
        assert!(
            matches!(&c1, Credential::Header { name, value } if name == "X-Session" && value == "S1")
        );
        // Cached: no ttl → second call does not re-login.
        let _ = p.credential().await.unwrap();
        assert_eq!(hits.load(Ordering::SeqCst), 1);
        // invalidate forces a fresh login.
        let c2 = p.invalidate(&c1).await.unwrap();
        assert!(matches!(&c2, Credential::Header { value, .. } if value == "S2"));
        assert_eq!(hits.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn login_capture_miss_is_an_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/x"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"other": 1})))
            .mount(&server)
            .await;
        let cfg = json!({
            "steps": [ { "request": { "url": format!("{}/x", server.uri()) },
                         "capture": { "tok": "$.access_token" } } ],
            "apply": [ { "into": "header", "name": "X", "value": "${tok}" } ]
        });
        let p = FlowProvider::from_config(&cfg).unwrap();
        assert!(p.credential().await.is_err());
    }

    #[tokio::test]
    async fn login_non_success_is_an_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/x"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;
        let cfg = json!({
            "steps": [ { "request": { "url": format!("{}/x", server.uri()) } } ],
            "apply": [ { "into": "header", "name": "X", "value": "static" } ]
        });
        let p = FlowProvider::from_config(&cfg).unwrap();
        assert!(
            p.request_auth("GET", "https://x", &BTreeMap::new())
                .await
                .is_err()
        );
    }

    #[test]
    fn config_rejects_empty_step_url() {
        let cfg: FlowConfig = serde_json::from_value(json!({
            "steps": [ { "request": { "url": "  " } } ]
        }))
        .unwrap();
        assert!(cfg.validate().is_err());
    }

    #[tokio::test]
    async fn session_ttl_caches_within_window() {
        let server = MockServer::start().await;
        let hits = Arc::new(AtomicUsize::new(0));
        Mock::given(method("POST"))
            .and(path("/login"))
            .respond_with(CountingLogin(hits.clone()))
            .mount(&server)
            .await;
        let cfg = json!({
            "steps": [ { "request": { "method": "POST", "url": format!("{}/login", server.uri()) },
                         "capture": { "sid": "$.sid" } } ],
            "apply": [ { "into": "header", "name": "X", "value": "${sid}" } ],
            "ttl_secs": 3600
        });
        let p = FlowProvider::from_config(&cfg).unwrap();
        let _ = p.credential().await.unwrap();
        // Within the TTL window the session is reused (Session::valid == true).
        let _ = p.credential().await.unwrap();
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn an_absurd_ttl_does_not_panic() {
        let server = MockServer::start().await;
        let hits = Arc::new(AtomicUsize::new(0));
        Mock::given(method("POST"))
            .and(path("/login"))
            .respond_with(CountingLogin(hits.clone()))
            .mount(&server)
            .await;
        let cfg = json!({
            "steps": [ { "request": { "method": "POST", "url": format!("{}/login", server.uri()) },
                         "capture": { "sid": "$.sid" } } ],
            "apply": [ { "into": "header", "name": "X", "value": "${sid}" } ],
            "ttl_secs": u64::MAX
        });
        let p = FlowProvider::from_config(&cfg).unwrap();
        let _ = p.credential().await.unwrap();
        let _ = p.credential().await.unwrap();
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn login_sends_headers_and_json_body() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/login"))
            .and(header("x-tenant", "acme"))
            .and(body_json(json!({"scope": "read"})))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"tok": "T"})))
            .mount(&server)
            .await;
        let cfg = json!({
            "steps": [ { "request": { "method": "POST", "url": format!("{}/login", server.uri()),
                         "headers": {"X-Tenant": "acme"}, "json": {"scope": "read"} },
                         "capture": { "t": "$.tok" } } ],
            "apply": [ { "into": "header", "name": "Authorization", "value": "Bearer ${t}" } ]
        });
        let p = FlowProvider::from_config(&cfg).unwrap();
        assert_eq!(p.provider_name(), "flow");
        let c = p.credential().await.unwrap();
        assert!(
            matches!(&c, Credential::Header { name, value } if name == "Authorization" && value == "Bearer T")
        );
    }

    #[tokio::test]
    async fn login_invalid_method_errors() {
        let cfg = json!({
            "steps": [ { "request": { "method": "BAD METHOD", "url": "https://x/login" } } ],
            "apply": [ { "into": "header", "name": "X", "value": "static" } ]
        });
        let p = FlowProvider::from_config(&cfg).unwrap();
        assert!(
            p.request_auth("GET", "https://x", &BTreeMap::new())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn login_send_failure_errors() {
        // Port 1 is unassignable → the send fails at the transport layer.
        let cfg = json!({
            "steps": [ { "request": { "url": "http://127.0.0.1:1/x" }, "capture": { "t": "$.t" } } ],
            "apply": [ { "into": "header", "name": "X", "value": "${t}" } ]
        });
        let p = FlowProvider::from_config(&cfg).unwrap();
        assert!(
            p.request_auth("GET", "https://x", &BTreeMap::new())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn login_non_json_response_errors() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/x"))
            .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
            .mount(&server)
            .await;
        let cfg = json!({
            "steps": [ { "request": { "url": format!("{}/x", server.uri()) }, "capture": { "t": "$.t" } } ],
            "apply": [ { "into": "header", "name": "X", "value": "${t}" } ]
        });
        let p = FlowProvider::from_config(&cfg).unwrap();
        assert!(
            p.request_auth("GET", "https://x", &BTreeMap::new())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn login_step_with_empty_capture_succeeds() {
        // A pre-flight step that captures nothing (e.g. establishes a cookie);
        // its non-JSON body is never parsed.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/ping"))
            .respond_with(ResponseTemplate::new(200).set_body_string("ok"))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"t": "T"})))
            .mount(&server)
            .await;
        let cfg = json!({
            "steps": [
                { "request": { "url": format!("{}/ping", server.uri()) } },
                { "request": { "url": format!("{}/token", server.uri()) }, "capture": { "t": "$.t" } }
            ],
            "apply": [ { "into": "header", "name": "X", "value": "${t}" } ]
        });
        let p = FlowProvider::from_config(&cfg).unwrap();
        let c = p.credential().await.unwrap();
        assert!(matches!(&c, Credential::Header { value, .. } if value == "T"));
    }

    #[tokio::test]
    async fn no_steps_apply_only_flow_works() {
        // A flow with only `apply` (a static signer/placement, no login) needs
        // no network.
        let cfg = json!({
            "apply": [ { "sign": { "alg": "hmac_sha256", "key": "k", "template": "msg",
                                   "into": { "header": "Authorization", "format": "HMAC ${sig}" } } } ]
        });
        let p = FlowProvider::from_config(&cfg).unwrap();
        let c = p.credential().await.unwrap();
        let expected = format!("HMAC {}", hmac_sign("k", "msg", SigEncoding::Hex));
        assert!(
            matches!(&c, Credential::Header { name, value } if name == "Authorization" && *value == expected)
        );
    }

    // ── #541: HMAC sign on a login/pre-flight step ───────────────────────────

    #[test]
    fn sign_header_computes_name_and_value_with_clock() {
        let mut c = ctx(&[("client", "abc")]);
        c.insert("ts".to_owned(), "999".to_owned());
        let sign: SignSpec = serde_json::from_value(json!({
            "alg": "hmac_sha256", "key": "k", "template": "${client}:${ts}",
            "encoding": "hex", "into": { "header": "Authorization", "format": "SS ${sig}" }
        }))
        .unwrap();
        let (name, value) = sign_header(&sign, &c);
        assert_eq!(name, "Authorization");
        assert_eq!(
            value,
            format!("SS {}", hmac_sign("k", "abc:999", SigEncoding::Hex))
        );
    }

    #[tokio::test]
    async fn signed_login_step_sends_computed_signature_header() {
        let server = MockServer::start().await;
        let key = "secret_key";
        // Deterministic template (no ${ts}) so the header is assertable.
        let expected = format!(
            "SS access:{}",
            hmac_sign(key, "client:secret", SigEncoding::Base64)
        );
        Mock::given(method("POST"))
            .and(path("/auth/login"))
            .and(header("authorization", expected.as_str()))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({"session": {"token": "SESS"}})),
            )
            .mount(&server)
            .await;
        let cfg = json!({
            "steps": [ {
                "request": {
                    "method": "POST",
                    "url": format!("{}/auth/login", server.uri()),
                    "json": {"clientId": "client"},
                    "sign": { "alg": "hmac_sha256", "key": key, "template": "client:secret",
                              "encoding": "base64",
                              "into": { "header": "Authorization", "format": "SS access:${sig}" } }
                },
                "capture": { "session": "$.session.token" }
            } ],
            "apply": [ { "into": "header", "name": "Session", "value": "${session}" } ]
        });
        let p = FlowProvider::from_config(&cfg).unwrap();
        // Success only if the login request carried the exact signed header
        // (otherwise wiremock returns 404 and the step fails).
        let c = p.credential().await.unwrap();
        assert!(
            matches!(&c, Credential::Header { name, value } if name == "Session" && value == "SESS")
        );
    }

    #[tokio::test]
    async fn signed_login_then_signed_data_requests() {
        let server = MockServer::start().await;
        let key = "base64secret";
        let login_sig = format!(
            "SS access:{}",
            hmac_sign(key, "cid:csec", SigEncoding::Base64)
        );
        Mock::given(method("POST"))
            .and(path("/auth/login"))
            .and(header("authorization", login_sig.as_str()))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"session": {"token": "SESSION-TOK"}})),
            )
            .mount(&server)
            .await;
        let signer = json!({ "alg": "hmac_sha256", "key": key, "template": "cid:csec",
                             "encoding": "base64",
                             "into": { "header": "Authorization", "format": "SS access:${sig}" } });
        let cfg = json!({
            "steps": [ {
                "request": { "method": "POST", "url": format!("{}/auth/login", server.uri()),
                             "json": {"clientId": "cid"}, "sign": signer },
                "capture": { "session": "$.session.token" }
            } ],
            "apply": [
                { "sign": signer },
                { "into": "header", "name": "Session", "value": "${session}" }
            ]
        });
        let p = FlowProvider::from_config(&cfg).unwrap();
        let ra = p
            .request_auth("GET", "https://data", &BTreeMap::new())
            .await
            .unwrap();
        let has_auth = ra.placements.iter().any(|pl| {
            matches!(pl, CredentialPlacement::Header { name, value } if name == "Authorization" && *value == login_sig)
        });
        let has_sess = ra.placements.iter().any(|pl| {
            matches!(pl, CredentialPlacement::Header { name, value } if name == "Session" && value == "SESSION-TOK")
        });
        assert!(has_auth, "data request carries the signer header");
        assert!(has_sess, "data request carries the captured Session header");
    }

    #[test]
    fn config_rejects_empty_step_sign_header() {
        let cfg: FlowConfig = serde_json::from_value(json!({
            "steps": [ { "request": { "url": "https://x",
                "sign": { "alg": "hmac_sha256", "key": "k", "template": "${ts}",
                          "into": { "header": "" } } } } ]
        }))
        .unwrap();
        assert!(cfg.validate().is_err());
    }

    // ── #542: capture from header / Set-Cookie / XML ─────────────────────────

    #[test]
    fn xml_dot_path_extracts_nested_text() {
        let xml = "<operation><result><data><api><sessionid>ABC123</sessionid></api></data></result></operation>";
        assert_eq!(
            xml_dot_path(xml, "operation.result.data.api.sessionid").as_deref(),
            Some("ABC123")
        );
        assert_eq!(xml_dot_path(xml, "operation.result.missing"), None);
    }

    #[test]
    fn xml_dot_path_ignores_declaration_attrs_and_namespaces() {
        let xml = r#"<?xml version="1.0"?><ns:root xmlns:ns="urn:x"><ns:child id="1">  hi  </ns:child></ns:root>"#;
        assert_eq!(xml_dot_path(xml, "root.child").as_deref(), Some("hi"));
        // A path segment may itself carry a prefix — it is stripped too.
        assert_eq!(xml_dot_path(xml, "ns:root.ns:child").as_deref(), Some("hi"));
    }

    #[test]
    fn xml_dot_path_handles_cdata_and_entities() {
        let xml = "<r><a><![CDATA[a&b]]></a><b>x &amp; y</b></r>";
        assert_eq!(xml_dot_path(xml, "r.a").as_deref(), Some("a&b"));
        assert_eq!(xml_dot_path(xml, "r.b").as_deref(), Some("x & y"));
    }

    #[test]
    fn set_cookie_and_header_helpers_select_by_name() {
        let mut h = reqwest::header::HeaderMap::new();
        h.append("set-cookie", "a=1; path=/".parse().unwrap());
        h.append(
            "set-cookie",
            "ASP.NET_SessionId=SID; path=/; HttpOnly".parse().unwrap(),
        );
        h.insert("location", "https://x/next".parse().unwrap());
        assert_eq!(
            set_cookie_value(&h, "ASP.NET_SessionId").as_deref(),
            Some("SID")
        );
        assert_eq!(set_cookie_value(&h, "missing"), None);
        assert_eq!(
            header_value(&h, "Location").as_deref(),
            Some("https://x/next")
        );
        assert_eq!(header_value(&h, "absent"), None);
    }

    #[test]
    fn capture_backcompat_string_form_parses_as_json() {
        let step: FlowStep = serde_json::from_value(json!({
            "request": { "url": "https://x" },
            "capture": { "tok": "$.access_token" }
        }))
        .unwrap();
        assert_eq!(step.capture["tok"].kind(), CaptureFrom::Json);
        assert!(matches!(&step.capture["tok"], CaptureSpec::Json(p) if p == "$.access_token"));
    }

    #[test]
    fn capture_struct_forms_parse_kinds() {
        let step: FlowStep = serde_json::from_value(json!({
            "request": { "url": "https://x" },
            "capture": {
                "h": { "from": "header", "name": "Location" },
                "c": { "from": "set_cookie", "name": "sid" },
                "x": { "from": "xml", "path": "a.b" },
                "j": { "from": "json", "path": "$.tok" }
            }
        }))
        .unwrap();
        assert_eq!(step.capture["h"].kind(), CaptureFrom::Header);
        assert_eq!(step.capture["c"].kind(), CaptureFrom::SetCookie);
        assert_eq!(step.capture["x"].kind(), CaptureFrom::Xml);
        assert_eq!(step.capture["j"].kind(), CaptureFrom::Json);
    }

    #[test]
    fn config_rejects_capture_missing_selector() {
        let cfg: FlowConfig = serde_json::from_value(json!({
            "steps": [ { "request": { "url": "https://x" }, "capture": { "h": { "from": "header" } } } ]
        }))
        .unwrap();
        assert!(cfg.validate().is_err());

        let cfg2: FlowConfig = serde_json::from_value(json!({
            "steps": [ { "request": { "url": "https://x" }, "capture": { "x": { "from": "xml" } } } ]
        }))
        .unwrap();
        assert!(cfg2.validate().is_err());
    }

    #[tokio::test]
    async fn capture_from_header_works() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/login"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Location", "https://redirect.example/next"),
            )
            .mount(&server)
            .await;
        let cfg = json!({
            "steps": [ { "request": { "url": format!("{}/login", server.uri()) },
                         "capture": { "loc": { "from": "header", "name": "Location" } } } ],
            "apply": [ { "into": "header", "name": "X-Loc", "value": "${loc}" } ]
        });
        let p = FlowProvider::from_config(&cfg).unwrap();
        let c = p.credential().await.unwrap();
        assert!(
            matches!(&c, Credential::Header { name, value } if name == "X-Loc" && value == "https://redirect.example/next")
        );
    }

    #[tokio::test]
    async fn capture_from_set_cookie_applies_as_cookie() {
        // Cookie-session login: POST /login → 204 empty body + Set-Cookie session.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/entity/auth/login"))
            .respond_with(
                ResponseTemplate::new(204)
                    .append_header("Set-Cookie", "other=nope; path=/")
                    .append_header("Set-Cookie", "ASP.NET_SessionId=SID123; path=/; HttpOnly"),
            )
            .mount(&server)
            .await;
        let cfg = json!({
            "steps": [ { "request": { "method": "POST",
                           "url": format!("{}/entity/auth/login", server.uri()),
                           "json": {"name": "u", "password": "p"} },
                         "capture": { "session_cookie": { "from": "set_cookie", "name": "ASP.NET_SessionId" } } } ],
            "apply": [ { "into": "cookie", "name": "ASP.NET_SessionId", "value": "${session_cookie}" } ]
        });
        let p = FlowProvider::from_config(&cfg).unwrap();
        let ra = p
            .request_auth("GET", "https://data", &BTreeMap::new())
            .await
            .unwrap();
        assert!(
            matches!(&ra.placements[0], CredentialPlacement::Cookie { name, value } if name == "ASP.NET_SessionId" && value == "SID123")
        );
    }

    #[tokio::test]
    async fn capture_from_xml_body_works() {
        // XML-gateway style: session id lives in an XML response body.
        let server = MockServer::start().await;
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?><response><operation><result><data><api><sessionid>XYZ-SESSION</sessionid></api></data></result></operation></response>"#;
        Mock::given(method("POST"))
            .and(path("/xml/xmlgw.phtml"))
            .respond_with(ResponseTemplate::new(200).set_body_string(xml))
            .mount(&server)
            .await;
        let cfg = json!({
            "steps": [ { "request": { "method": "POST",
                           "url": format!("{}/xml/xmlgw.phtml", server.uri()) },
                         "capture": { "sess_id": { "from": "xml",
                             "path": "response.operation.result.data.api.sessionid" } } } ],
            "apply": [ { "into": "header", "name": "X-Session", "value": "${sess_id}" } ]
        });
        let p = FlowProvider::from_config(&cfg).unwrap();
        let c = p.credential().await.unwrap();
        assert!(matches!(&c, Credential::Header { value, .. } if value == "XYZ-SESSION"));
    }

    #[tokio::test]
    async fn capture_from_xml_miss_is_an_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/x"))
            .respond_with(ResponseTemplate::new(200).set_body_string("<r><a>1</a></r>"))
            .mount(&server)
            .await;
        let cfg = json!({
            "steps": [ { "request": { "method": "POST", "url": format!("{}/x", server.uri()) },
                         "capture": { "s": { "from": "xml", "path": "r.missing" } } } ],
            "apply": [ { "into": "header", "name": "X", "value": "${s}" } ]
        });
        let p = FlowProvider::from_config(&cfg).unwrap();
        assert!(p.credential().await.is_err());
    }
}
