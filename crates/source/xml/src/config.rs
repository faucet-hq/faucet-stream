//! XML source configuration.

use crate::decode::DecodeStep;
use faucet_core::{AuthSpec, DEFAULT_BATCH_SIZE, FaucetError, TlsClientConfig};
use reqwest::header::HeaderMap;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Authentication for XML API endpoints.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "config", rename_all = "snake_case")]
pub enum XmlAuth {
    /// No authentication.
    None,
    /// Bearer token.
    Bearer { token: String },
    /// Basic authentication.
    Basic { username: String, password: String },
    /// Custom headers (e.g. SOAP action headers, API keys).
    Custom { headers: HashMap<String, String> },
}

fn default_true() -> bool {
    true
}

/// SOAP protocol version. Controls the envelope namespace and the HTTP
/// header shape used to carry the SOAP action.
///
/// Deserializes from the wire strings `"1.1"` / `"1.2"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
pub enum SoapVersion {
    /// SOAP 1.1 — envelope namespace `http://schemas.xmlsoap.org/soap/envelope/`,
    /// action carried in a separate `SOAPAction` header.
    #[default]
    #[serde(rename = "1.1")]
    Soap11,
    /// SOAP 1.2 — envelope namespace `http://www.w3.org/2003/05/soap-envelope`,
    /// action carried as a `Content-Type` parameter (no `SOAPAction` header).
    #[serde(rename = "1.2")]
    Soap12,
}

impl SoapVersion {
    /// The SOAP envelope namespace URI for this version.
    pub fn namespace(self) -> &'static str {
        match self {
            SoapVersion::Soap11 => "http://schemas.xmlsoap.org/soap/envelope/",
            SoapVersion::Soap12 => "http://www.w3.org/2003/05/soap-envelope",
        }
    }
}

/// First-class SOAP ergonomics for the XML source.
///
/// This is **sugar** over the existing XML-over-HTTP request/response path —
/// not a WSDL client. When present, the source assembles a SOAP envelope for
/// the request body, injects the version-appropriate headers, and (by default)
/// resolves [`XmlStreamConfig::records_element_path`] relative to
/// `Envelope.Body` and surfaces SOAP `<Fault>` responses as errors.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SoapConfig {
    /// SOAP protocol version (default `1.1`).
    #[serde(default)]
    pub version: SoapVersion,
    /// The SOAP action. For 1.1 it becomes the `SOAPAction` header; for 1.2 it
    /// is carried as the `action` parameter of the `Content-Type` header.
    /// Optional — omit for actionless operations.
    pub action: Option<String>,
    /// The XML fragment placed inside `<soap:Body>` — typically the operation
    /// element, e.g. `<GetUsers xmlns="urn:example"/>`. Mutually exclusive with
    /// the top-level [`XmlStreamConfig::body`].
    pub body_inner: Option<String>,
    /// Extra namespace declarations (prefix → URI) added to the envelope
    /// element. The `soap` prefix is reserved for the envelope namespace and
    /// any entry using it is ignored.
    #[serde(default)]
    pub namespaces: HashMap<String, String>,
    /// When `true` (default), [`XmlStreamConfig::records_element_path`] is
    /// resolved relative to `Envelope.Body` — i.e. `Envelope.Body.` is
    /// auto-prepended, so you write `GetUsersResponse.Users.User`. Set `false`
    /// to supply the fully-qualified path from the document root.
    #[serde(default = "default_true")]
    pub path_relative_to_body: bool,
    /// When `true` (default), a SOAP `<Fault>` in the response raises
    /// [`FaucetError::Source`]. When `false`,
    /// a fault yields zero records (logged once).
    #[serde(default = "default_true")]
    pub fault_as_error: bool,
}

impl Default for SoapConfig {
    fn default() -> Self {
        Self {
            version: SoapVersion::default(),
            action: None,
            body_inner: None,
            namespaces: HashMap::new(),
            path_relative_to_body: true,
            fault_as_error: true,
        }
    }
}

impl SoapConfig {
    /// Assemble the SOAP request envelope wrapping `body_inner` inside
    /// `<soap:Body>`, declaring the version namespace plus any user-declared
    /// prefixes on the envelope element.
    ///
    /// Prefixes are emitted in a deterministic (sorted) order so the assembled
    /// body is stable across runs.
    pub fn build_envelope(&self, body_inner: &str) -> String {
        let mut attrs = format!(" xmlns:soap=\"{}\"", self.version.namespace());
        let mut prefixes: Vec<(&String, &String)> = self
            .namespaces
            .iter()
            // The `soap` prefix is reserved for the envelope namespace.
            .filter(|(prefix, _)| prefix.as_str() != "soap")
            .collect();
        prefixes.sort_by(|a, b| a.0.cmp(b.0));
        for (prefix, uri) in prefixes {
            attrs.push_str(&format!(" xmlns:{prefix}=\"{uri}\""));
        }
        format!(
            "<?xml version=\"1.0\" encoding=\"utf-8\"?>\
             <soap:Envelope{attrs}><soap:Body>{body_inner}</soap:Body></soap:Envelope>"
        )
    }

    /// The `Content-Type` header value for a request of this SOAP version.
    ///
    /// For 1.2 the action (when set) is carried as a `Content-Type` parameter.
    pub fn content_type(&self) -> String {
        match self.version {
            SoapVersion::Soap11 => "text/xml; charset=utf-8".to_string(),
            SoapVersion::Soap12 => match &self.action {
                Some(action) => {
                    format!("application/soap+xml; charset=utf-8; action=\"{action}\"")
                }
                None => "application/soap+xml; charset=utf-8".to_string(),
            },
        }
    }

    /// The `SOAPAction` header value (quoted) for SOAP 1.1. Always `None` for
    /// SOAP 1.2, which carries the action inside `Content-Type` instead.
    pub fn soap_action_header(&self) -> Option<String> {
        match self.version {
            SoapVersion::Soap11 => self.action.as_ref().map(|action| format!("\"{action}\"")),
            SoapVersion::Soap12 => None,
        }
    }
}

/// Pagination configuration for XML APIs.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type")]
pub enum XmlPagination {
    /// Page-number pagination with a query parameter.
    PageNumber {
        param_name: String,
        start_page: usize,
        page_size: Option<usize>,
        page_size_param: Option<String>,
    },
    /// Offset/limit pagination.
    Offset {
        offset_param: String,
        limit_param: String,
        limit: usize,
    },
    /// Body-cursor pagination (#544): read a continuation token from the
    /// response via a dot-path (namespace-insensitive, trailing-match), and on
    /// each subsequent request replace the request body with `next_body` (with
    /// `${next_token}` substituted). Stops when the token is absent/empty or
    /// repeats (loop guard), honouring `max_pages`. For stateful XML/SOAP APIs
    /// that page with a `readMore`/`resultId` handle (e.g. Sage Intacct).
    BodyCursor {
        /// Dot-path to the continuation-token element in the response.
        next_token_path: String,
        /// Request-body template for pages after the first; `${next_token}` is
        /// substituted with the captured token.
        next_body: String,
    },
}

/// Configuration for the XML source.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct XmlStreamConfig {
    /// Base URL of the API.
    pub base_url: String,
    /// Request path (appended to base_url).
    pub path: String,
    /// HTTP method (GET or POST for SOAP).
    #[serde(with = "crate::serde_helpers::http_method")]
    #[schemars(with = "String")]
    pub method: reqwest::Method,
    /// Authentication: either inline (`{ type, config }`) or a `{ ref: <name> }`
    /// pointer to a shared provider in the CLI's top-level `auth:` catalog.
    pub auth: AuthSpec<XmlAuth>,
    /// Additional request headers.
    #[serde(skip, default)]
    pub headers: HeaderMap,
    /// Optional request body (e.g. a raw SOAP envelope). Mutually exclusive
    /// with [`SoapConfig::body_inner`] when a [`soap`](Self::soap) block is set.
    pub body: Option<String>,
    /// Optional first-class SOAP ergonomics block. When present, the source
    /// assembles the SOAP envelope, injects the version-appropriate headers,
    /// and (by default) resolves `records_element_path` relative to
    /// `Envelope.Body`. Sugar over the raw `body` path — see [`SoapConfig`].
    #[serde(default)]
    pub soap: Option<SoapConfig>,
    /// Dot-separated path to the repeating element in the XML response
    /// (e.g. `"Envelope.Body.GetUsersResponse.Users.User"`).
    pub records_element_path: Option<String>,
    /// Pagination configuration.
    pub pagination: Option<XmlPagination>,
    /// Maximum number of pages to fetch.
    pub max_pages: Option<usize>,
    /// Query parameters to include in every request. Empty by default.
    ///
    /// The `#[serde(default)]` is load-bearing: without it the field was
    /// **required**, so any config omitting it failed to deserialize — the same
    /// oversight the REST source carried, and invisible until `faucet validate`
    /// started deserializing connector configs (#609).
    #[serde(default)]
    pub query_params: std::collections::HashMap<String, String>,
    /// Response-decode pipeline (#540): a declarative chain applied to the raw
    /// response body before record extraction (`extract` an element's text →
    /// `base64`/`gunzip`/`unzip` → `parse` csv/xlsx/xml/json). When non-empty,
    /// records come from the decoded output instead of `records_element_path`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub decode: Vec<DecodeStep>,
    /// Records per emitted [`StreamPage`](faucet_core::StreamPage). The
    /// event-driven XML parser accumulates matched subtrees into a buffer
    /// and yields whenever the buffer reaches this size. Defaults to
    /// [`DEFAULT_BATCH_SIZE`].
    ///
    /// `batch_size = 0` is the "no batching" sentinel: the document is
    /// drained end-to-end and the entire result set is emitted in a single
    /// page. Useful for small lookup payloads or for sinks (e.g. SQL `COPY`,
    /// BigQuery load jobs) that prefer one large request to many small ones.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Optional client-certificate (mutual TLS) config. When set, the source
    /// presents a client certificate on every request (data + inline auth token
    /// request). Requires the crate's `mtls` feature.
    #[serde(default)]
    pub tls: Option<TlsClientConfig>,
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

impl XmlStreamConfig {
    /// Create a new config with required fields.
    pub fn new(base_url: impl Into<String>, path: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            path: path.into(),
            method: reqwest::Method::GET,
            auth: AuthSpec::Inline(XmlAuth::None),
            headers: HeaderMap::new(),
            body: None,
            soap: None,
            records_element_path: None,
            pagination: None,
            max_pages: None,
            query_params: std::collections::HashMap::new(),
            decode: Vec::new(),
            batch_size: DEFAULT_BATCH_SIZE,
            tls: None,
        }
    }

    /// Set the response-decode pipeline (#540).
    pub fn decode(mut self, steps: Vec<DecodeStep>) -> Self {
        self.decode = steps;
        self
    }

    /// Attach a mutual-TLS client identity (requires the `mtls` feature at build
    /// time; otherwise [`XmlStream::try_new`](crate::XmlStream::try_new) errors).
    pub fn tls(mut self, tls: TlsClientConfig) -> Self {
        self.tls = Some(tls);
        self
    }

    /// Set the HTTP method (default: GET).
    pub fn method(mut self, method: reqwest::Method) -> Self {
        self.method = method;
        self
    }

    /// Set the authentication method.
    pub fn auth(mut self, auth: XmlAuth) -> Self {
        self.auth = AuthSpec::Inline(auth);
        self
    }

    /// Set additional headers.
    pub fn headers(mut self, headers: HeaderMap) -> Self {
        self.headers = headers;
        self
    }

    /// Set a raw SOAP or XML request body.
    pub fn body(mut self, body: impl Into<String>) -> Self {
        self.body = Some(body.into());
        self
    }

    /// Attach a first-class [`SoapConfig`] ergonomics block.
    pub fn with_soap(mut self, soap: SoapConfig) -> Self {
        self.soap = Some(soap);
        self
    }

    /// Set the dot-separated path to the repeating element.
    pub fn records_element_path(mut self, path: impl Into<String>) -> Self {
        self.records_element_path = Some(path.into());
        self
    }

    /// Set pagination configuration.
    pub fn pagination(mut self, pagination: XmlPagination) -> Self {
        self.pagination = Some(pagination);
        self
    }

    /// Set the maximum number of pages.
    pub fn max_pages(mut self, max: usize) -> Self {
        self.max_pages = Some(max);
        self
    }

    /// Add a query parameter.
    pub fn query_param(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.query_params.insert(key.into(), value.into());
        self
    }

    /// Set the per-page record count for
    /// [`Source::stream_pages`](faucet_core::Source::stream_pages).
    ///
    /// Pass `0` to opt out of batching — the entire document is drained and
    /// emitted in a single [`StreamPage`](faucet_core::StreamPage).
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Validate the configuration, surfacing SOAP-specific conflicts as
    /// [`FaucetError::Config`] before any request is made.
    ///
    /// Rules:
    /// - both the top-level [`body`](Self::body) and [`SoapConfig::body_inner`]
    ///   set is ambiguous;
    /// - a [`soap`](Self::soap) block requires `method: POST` (SOAP is a POST
    ///   protocol).
    ///
    /// A no-op (always `Ok`) when no `soap` block is present, so non-SOAP
    /// configs are unaffected.
    pub fn validate(&self) -> Result<(), FaucetError> {
        if let Some(soap) = &self.soap {
            if self.body.is_some() && soap.body_inner.is_some() {
                return Err(FaucetError::Config(
                    "xml: set either the top-level `body` or `soap.body_inner`, not both \
                     (ambiguous request body)"
                        .into(),
                ));
            }
            if self.method == reqwest::Method::GET {
                return Err(FaucetError::Config(
                    "xml: a `soap` block requires `method: POST` — SOAP is a POST protocol".into(),
                ));
            }
        }
        if let Some(tls) = &self.tls {
            tls.validate()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = XmlStreamConfig::new("https://api.example.com", "/users");
        assert_eq!(config.base_url, "https://api.example.com");
        assert_eq!(config.path, "/users");
        assert_eq!(config.method, reqwest::Method::GET);
        assert!(config.records_element_path.is_none());
    }

    #[test]
    fn soap_config() {
        let config = XmlStreamConfig::new("https://api.example.com", "/soap")
            .method(reqwest::Method::POST)
            .body("<Envelope><Body><GetUsers/></Body></Envelope>")
            .records_element_path("Envelope.Body.GetUsersResponse.Users.User");
        assert_eq!(config.method, reqwest::Method::POST);
        assert!(config.body.is_some());
        assert_eq!(
            config.records_element_path.unwrap(),
            "Envelope.Body.GetUsersResponse.Users.User"
        );
    }

    #[test]
    fn batch_size_defaults_to_default_batch_size() {
        let config = XmlStreamConfig::new("https://api.example.com", "/users");
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn with_batch_size_overrides_default() {
        let config = XmlStreamConfig::new("https://api.example.com", "/users").with_batch_size(500);
        assert_eq!(config.batch_size, 500);
    }

    #[test]
    fn batch_size_zero_is_accepted_as_no_batching_sentinel() {
        let config = XmlStreamConfig::new("https://api.example.com", "/users").with_batch_size(0);
        assert_eq!(config.batch_size, 0);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_ok());
    }

    #[test]
    fn batch_size_above_max_is_rejected_by_validate_batch_size() {
        let config = XmlStreamConfig::new("https://api.example.com", "/users")
            .with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_err());
    }

    #[test]
    fn batch_size_deserializes_from_json() {
        let json = r#"{
            "base_url": "https://api.example.com",
            "path": "/users.xml",
            "method": "GET",
            "auth": { "type": "none" },
            "body": null,
            "records_element_path": "root.user",
            "pagination": null,
            "max_pages": null,
            "query_params": {},
            "batch_size": 250
        }"#;
        let config: XmlStreamConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, 250);
    }

    #[test]
    fn soap_version_deserializes_from_wire_strings() {
        assert_eq!(
            serde_json::from_str::<SoapVersion>("\"1.1\"").unwrap(),
            SoapVersion::Soap11
        );
        assert_eq!(
            serde_json::from_str::<SoapVersion>("\"1.2\"").unwrap(),
            SoapVersion::Soap12
        );
        assert_eq!(SoapVersion::default(), SoapVersion::Soap11);
    }

    #[test]
    fn soap_version_serializes_to_wire_strings() {
        assert_eq!(
            serde_json::to_string(&SoapVersion::Soap11).unwrap(),
            "\"1.1\""
        );
        assert_eq!(
            serde_json::to_string(&SoapVersion::Soap12).unwrap(),
            "\"1.2\""
        );
    }

    #[test]
    fn soap_version_namespaces() {
        assert_eq!(
            SoapVersion::Soap11.namespace(),
            "http://schemas.xmlsoap.org/soap/envelope/"
        );
        assert_eq!(
            SoapVersion::Soap12.namespace(),
            "http://www.w3.org/2003/05/soap-envelope"
        );
    }

    #[test]
    fn soap_config_defaults_are_body_relative_and_fault_as_error() {
        let soap = SoapConfig::default();
        assert_eq!(soap.version, SoapVersion::Soap11);
        assert!(soap.path_relative_to_body);
        assert!(soap.fault_as_error);
        assert!(soap.action.is_none());
        assert!(soap.body_inner.is_none());
    }

    #[test]
    fn soap_config_deserializes_defaults_from_minimal_json() {
        let soap: SoapConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(soap.version, SoapVersion::Soap11);
        assert!(soap.path_relative_to_body);
        assert!(soap.fault_as_error);
    }

    #[test]
    fn build_envelope_soap11() {
        let soap = SoapConfig {
            version: SoapVersion::Soap11,
            body_inner: Some("<GetUsers xmlns=\"urn:example\"/>".into()),
            ..Default::default()
        };
        let env = soap.build_envelope(soap.body_inner.as_deref().unwrap());
        assert!(
            env.contains("xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\""),
            "got {env}"
        );
        assert!(env.contains("<soap:Envelope"));
        assert!(env.contains("<soap:Body><GetUsers xmlns=\"urn:example\"/></soap:Body>"));
        assert!(env.trim_end().ends_with("</soap:Envelope>"));
    }

    #[test]
    fn build_envelope_soap12() {
        let soap = SoapConfig {
            version: SoapVersion::Soap12,
            ..Default::default()
        };
        let env = soap.build_envelope("<Op/>");
        assert!(
            env.contains("xmlns:soap=\"http://www.w3.org/2003/05/soap-envelope\""),
            "got {env}"
        );
        assert!(env.contains("<soap:Body><Op/></soap:Body>"));
    }

    #[test]
    fn build_envelope_declares_extra_namespaces_sorted() {
        let mut namespaces = HashMap::new();
        namespaces.insert("b".to_string(), "urn:b".to_string());
        namespaces.insert("a".to_string(), "urn:a".to_string());
        // A `soap` prefix is reserved and must be dropped.
        namespaces.insert("soap".to_string(), "urn:should-be-ignored".to_string());
        let soap = SoapConfig {
            namespaces,
            ..Default::default()
        };
        let env = soap.build_envelope("<Op/>");
        // Deterministic sorted order: soap (envelope), then a, then b.
        let idx_soap = env.find("xmlns:soap=").unwrap();
        let idx_a = env.find("xmlns:a=\"urn:a\"").unwrap();
        let idx_b = env.find("xmlns:b=\"urn:b\"").unwrap();
        assert!(idx_soap < idx_a && idx_a < idx_b, "got {env}");
        assert!(!env.contains("urn:should-be-ignored"), "got {env}");
    }

    #[test]
    fn soap11_content_type_and_action_header() {
        let soap = SoapConfig {
            version: SoapVersion::Soap11,
            action: Some("urn:GetUsers".into()),
            ..Default::default()
        };
        assert_eq!(soap.content_type(), "text/xml; charset=utf-8");
        assert_eq!(
            soap.soap_action_header().as_deref(),
            Some("\"urn:GetUsers\"")
        );
    }

    #[test]
    fn soap11_without_action_has_no_soap_action_header() {
        let soap = SoapConfig {
            version: SoapVersion::Soap11,
            action: None,
            ..Default::default()
        };
        assert_eq!(soap.content_type(), "text/xml; charset=utf-8");
        assert!(soap.soap_action_header().is_none());
    }

    #[test]
    fn soap12_content_type_carries_action_and_has_no_soap_action_header() {
        let soap = SoapConfig {
            version: SoapVersion::Soap12,
            action: Some("urn:GetUsers".into()),
            ..Default::default()
        };
        assert_eq!(
            soap.content_type(),
            "application/soap+xml; charset=utf-8; action=\"urn:GetUsers\""
        );
        assert!(
            soap.soap_action_header().is_none(),
            "SOAP 1.2 never sets a SOAPAction header"
        );
    }

    #[test]
    fn soap12_content_type_without_action() {
        let soap = SoapConfig {
            version: SoapVersion::Soap12,
            action: None,
            ..Default::default()
        };
        assert_eq!(soap.content_type(), "application/soap+xml; charset=utf-8");
    }

    #[test]
    fn validate_ok_without_soap_block() {
        let config = XmlStreamConfig::new("https://api.example.com", "/svc");
        assert!(config.validate().is_ok());
    }

    #[test]
    fn validate_rejects_body_and_body_inner_both_set() {
        let config = XmlStreamConfig::new("https://api.example.com", "/svc")
            .method(reqwest::Method::POST)
            .body("<Envelope/>")
            .with_soap(SoapConfig {
                body_inner: Some("<Op/>".into()),
                ..Default::default()
            });
        let err = config.validate().unwrap_err();
        assert!(
            matches!(&err, FaucetError::Config(m) if m.contains("not both")),
            "got {err:?}"
        );
    }

    #[test]
    fn validate_rejects_soap_with_get_method() {
        // `new` defaults method to GET; a soap block requires POST.
        let config = XmlStreamConfig::new("https://api.example.com", "/svc")
            .with_soap(SoapConfig::default());
        let err = config.validate().unwrap_err();
        assert!(
            matches!(&err, FaucetError::Config(m) if m.contains("POST")),
            "got {err:?}"
        );
    }

    #[test]
    fn validate_ok_with_soap_and_post() {
        let config = XmlStreamConfig::new("https://api.example.com", "/svc")
            .method(reqwest::Method::POST)
            .with_soap(SoapConfig {
                body_inner: Some("<Op/>".into()),
                ..Default::default()
            });
        assert!(config.validate().is_ok());
    }

    #[test]
    fn with_soap_sets_the_block() {
        let config = XmlStreamConfig::new("https://api.example.com", "/svc")
            .method(reqwest::Method::POST)
            .with_soap(SoapConfig {
                action: Some("urn:Op".into()),
                ..Default::default()
            });
        assert_eq!(config.soap.unwrap().action.as_deref(), Some("urn:Op"));
    }

    #[test]
    fn soap_absent_by_default_and_deserializes_from_config_without_soap() {
        // Backward-compat: a config JSON with no `soap` key deserializes to
        // `soap: None`, leaving every legacy field untouched.
        let json = r#"{
            "base_url": "https://api.example.com",
            "path": "/users.xml",
            "method": "GET",
            "auth": { "type": "none" },
            "body": null,
            "records_element_path": "root.user",
            "pagination": null,
            "max_pages": null,
            "query_params": {}
        }"#;
        let config: XmlStreamConfig = serde_json::from_str(json).unwrap();
        assert!(config.soap.is_none());
    }

    #[test]
    fn batch_size_defaults_when_missing_from_json() {
        // The `#[serde(default = "default_batch_size")]` attribute is the
        // user-facing contract — older configs without `batch_size` must
        // continue to deserialize and adopt the library default.
        let json = r#"{
            "base_url": "https://api.example.com",
            "path": "/users.xml",
            "method": "GET",
            "auth": { "type": "none" },
            "body": null,
            "records_element_path": null,
            "pagination": null,
            "max_pages": null,
            "query_params": {}
        }"#;
        let config: XmlStreamConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }
}
