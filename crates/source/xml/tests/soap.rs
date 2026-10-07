//! Integration tests for the first-class `soap:` ergonomics block against a
//! wiremock server: request-header injection (SOAPAction / Content-Type) per
//! SOAP version, the assembled envelope body the server receives,
//! `Envelope.Body.`-relative record extraction (including a namespace-prefixed
//! record element), SOAP `<Fault>` handling (error vs zero-records), and a
//! regression asserting the raw-`body` path is unchanged when `soap` is absent.

use faucet_core::FaucetError;
use faucet_source_xml::{SoapConfig, SoapVersion, XmlAuth, XmlStream, XmlStreamConfig};
use reqwest::Method;
use wiremock::matchers::{body_string_contains, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// A default-namespaced SOAP success envelope wrapping the given `Users` markup.
fn soap_users_response(users: &str) -> String {
    format!(
        "<Envelope xmlns=\"http://schemas.xmlsoap.org/soap/envelope/\"><Body>\
         <GetUsersResponse xmlns=\"urn:example\"><Users>{users}</Users></GetUsersResponse>\
         </Body></Envelope>"
    )
}

#[tokio::test]
async fn soap11_injects_soapaction_header_and_text_xml_content_type() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/ws"))
        .and(header("soapaction", "\"urn:example:GetUsers\""))
        .and(header("content-type", "text/xml; charset=utf-8"))
        // The assembled envelope wraps the body_inner inside <soap:Body>.
        .and(body_string_contains(
            "<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\">",
        ))
        .and(body_string_contains(
            "<soap:Body><GetUsers xmlns=\"urn:example\"/></soap:Body>",
        ))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "text/xml")
                .set_body_string(soap_users_response(
                    "<User><Name>Alice</Name></User><User><Name>Bob</Name></User>",
                )),
        )
        .expect(1)
        .mount(&server)
        .await;

    let config = XmlStreamConfig::new(server.uri(), "/ws")
        .method(Method::POST)
        .records_element_path("GetUsersResponse.Users.User")
        .with_soap(SoapConfig {
            version: SoapVersion::Soap11,
            action: Some("urn:example:GetUsers".into()),
            body_inner: Some("<GetUsers xmlns=\"urn:example\"/>".into()),
            ..Default::default()
        });
    let records = XmlStream::new(config).fetch_all().await.unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["Name"], "Alice");
    assert_eq!(records[1]["Name"], "Bob");
}

#[tokio::test]
async fn soap12_carries_action_in_content_type_and_sends_no_soapaction_header() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/ws"))
        .and(header(
            "content-type",
            "application/soap+xml; charset=utf-8; action=\"urn:example:GetUsers\"",
        ))
        .and(body_string_contains(
            "<soap:Envelope xmlns:soap=\"http://www.w3.org/2003/05/soap-envelope\">",
        ))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "application/soap+xml")
                .set_body_string(soap_users_response("<User><Name>Carol</Name></User>")),
        )
        .expect(1)
        .mount(&server)
        .await;

    let config = XmlStreamConfig::new(server.uri(), "/ws")
        .method(Method::POST)
        .records_element_path("GetUsersResponse.Users.User")
        .with_soap(SoapConfig {
            version: SoapVersion::Soap12,
            action: Some("urn:example:GetUsers".into()),
            body_inner: Some("<GetUsers xmlns=\"urn:example\"/>".into()),
            ..Default::default()
        });
    let records = XmlStream::new(config).fetch_all().await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["Name"], "Carol");
}

#[tokio::test]
async fn soap_namespace_prefixed_record_element_is_extracted() {
    // The response uses a `usr:User` prefixed record element; the relative path
    // must name it verbatim (prefixes are preserved by XML→JSON conversion).
    let server = MockServer::start().await;
    let response = "<Envelope xmlns=\"http://schemas.xmlsoap.org/soap/envelope/\"><Body>\
         <GetUsersResponse xmlns:usr=\"urn:example:users\"><Users>\
         <usr:User><usr:Name>Dana</usr:Name></usr:User>\
         <usr:User><usr:Name>Erin</usr:Name></usr:User>\
         </Users></GetUsersResponse></Body></Envelope>";
    Mock::given(method("POST"))
        .and(path("/ws"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "text/xml")
                .set_body_string(response),
        )
        .expect(1)
        .mount(&server)
        .await;

    let config = XmlStreamConfig::new(server.uri(), "/ws")
        .method(Method::POST)
        .records_element_path("GetUsersResponse.Users.usr:User")
        .with_soap(SoapConfig {
            body_inner: Some("<GetUsers/>".into()),
            ..Default::default()
        });
    let records = XmlStream::new(config).fetch_all().await.unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["usr:Name"], "Dana");
    assert_eq!(records[1]["usr:Name"], "Erin");
}

#[tokio::test]
async fn soap_absolute_path_override_bypasses_body_prepend() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/ws"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "text/xml")
                .set_body_string(soap_users_response("<User><Name>Fred</Name></User>")),
        )
        .expect(1)
        .mount(&server)
        .await;

    let config = XmlStreamConfig::new(server.uri(), "/ws")
        .method(Method::POST)
        .records_element_path("Envelope.Body.GetUsersResponse.Users.User")
        .with_soap(SoapConfig {
            body_inner: Some("<GetUsers/>".into()),
            path_relative_to_body: false,
            ..Default::default()
        });
    let records = XmlStream::new(config).fetch_all().await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["Name"], "Fred");
}

#[tokio::test]
async fn soap_fault_as_error_surfaces_source_error() {
    let server = MockServer::start().await;
    let fault = "<Envelope xmlns=\"http://schemas.xmlsoap.org/soap/envelope/\"><Body>\
         <Fault><faultcode>Server</faultcode>\
         <faultstring>Account not found</faultstring></Fault></Body></Envelope>";
    Mock::given(method("POST"))
        .and(path("/ws"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "text/xml")
                .set_body_string(fault),
        )
        .mount(&server)
        .await;

    let config = XmlStreamConfig::new(server.uri(), "/ws")
        .method(Method::POST)
        .records_element_path("GetUsersResponse.Users.User")
        .with_soap(SoapConfig {
            body_inner: Some("<GetUsers/>".into()),
            ..Default::default()
        });
    let err = XmlStream::new(config).fetch_all().await.unwrap_err();
    assert!(
        matches!(&err, FaucetError::Source(m) if m.contains("SOAP fault") && m.contains("Account not found")),
        "got {err:?}"
    );
}

#[tokio::test]
async fn soap_fault_not_error_yields_zero_records() {
    let server = MockServer::start().await;
    let fault = "<Envelope xmlns=\"http://schemas.xmlsoap.org/soap/envelope/\"><Body>\
         <Fault><faultstring>transient</faultstring></Fault></Body></Envelope>";
    Mock::given(method("POST"))
        .and(path("/ws"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "text/xml")
                .set_body_string(fault),
        )
        .mount(&server)
        .await;

    let config = XmlStreamConfig::new(server.uri(), "/ws")
        .method(Method::POST)
        .records_element_path("GetUsersResponse.Users.User")
        .with_soap(SoapConfig {
            body_inner: Some("<GetUsers/>".into()),
            fault_as_error: false,
            ..Default::default()
        });
    let records = XmlStream::new(config).fetch_all().await.unwrap();
    assert!(records.is_empty());
}

#[tokio::test]
async fn soap_headers_injected_alongside_real_bearer_auth() {
    // The SOAP headers are set regardless of the auth variant, so genuine
    // bearer auth is applied in addition to SOAPAction / Content-Type.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/ws"))
        .and(header("authorization", "Bearer tok-123"))
        .and(header("soapaction", "\"urn:Op\""))
        .and(header("content-type", "text/xml; charset=utf-8"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "text/xml")
                .set_body_string(soap_users_response("<User><Name>Gwen</Name></User>")),
        )
        .expect(1)
        .mount(&server)
        .await;

    let config = XmlStreamConfig::new(server.uri(), "/ws")
        .method(Method::POST)
        .auth(XmlAuth::Bearer {
            token: "tok-123".into(),
        })
        .records_element_path("GetUsersResponse.Users.User")
        .with_soap(SoapConfig {
            action: Some("urn:Op".into()),
            body_inner: Some("<Op/>".into()),
            ..Default::default()
        });
    let records = XmlStream::new(config).fetch_all().await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["Name"], "Gwen");
}

#[tokio::test]
async fn raw_body_path_is_unchanged_when_soap_absent() {
    // Regression: with no `soap` block, the request is byte-for-byte the legacy
    // raw-`body` path — the body is sent verbatim, Content-Type is text/xml,
    // and no SOAPAction header is added by the source.
    let server = MockServer::start().await;
    let raw_envelope =
        "<soap:Envelope><soap:Body><GetUsers/></soap:Body></soap:Envelope>".to_string();
    Mock::given(method("POST"))
        .and(path("/ws"))
        .and(header("content-type", "text/xml; charset=utf-8"))
        .and(body_string_contains(&raw_envelope))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "text/xml")
                .set_body_string(
                    "<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\">\
                     <soap:Body><GetUsersResponse><User><Name>Ivy</Name></User>\
                     </GetUsersResponse></soap:Body></soap:Envelope>",
                ),
        )
        .expect(1)
        .mount(&server)
        .await;

    let config = XmlStreamConfig::new(server.uri(), "/ws")
        .method(Method::POST)
        .body(raw_envelope.clone())
        .records_element_path("soap:Envelope.soap:Body.GetUsersResponse.User");
    let records = XmlStream::new(config).fetch_all().await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["Name"], "Ivy");
}

/// The normal case for a real SOAP server: `soap:`-prefixed `Envelope`/`Body`
/// and a prefixed response wrapper. The default body-relative path must still
/// find the records (#789 API-08).
#[tokio::test]
async fn a_prefixed_envelope_resolves_the_body_relative_path() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/ws"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            "<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\">\
             <soap:Body><ns1:GetUsersResponse xmlns:ns1=\"urn:example\"><ns1:Users>\
             <ns1:User><Name>Alice</Name></ns1:User><ns1:User><Name>Bob</Name></ns1:User>\
             </ns1:Users></ns1:GetUsersResponse></soap:Body></soap:Envelope>",
        ))
        .mount(&server)
        .await;

    let config = XmlStreamConfig::new(server.uri(), "/ws")
        .method(Method::POST)
        .records_element_path("GetUsersResponse.Users.User")
        .with_soap(SoapConfig {
            body_inner: Some("<GetUsers xmlns=\"urn:example\"/>".into()),
            ..Default::default()
        });
    let records = XmlStream::new(config).fetch_all().await.unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["Name"], "Alice");
}

/// A success response whose body lacks the configured response element is a
/// configuration mismatch, not an empty result.
#[tokio::test]
async fn a_missing_response_element_fails_instead_of_returning_zero_rows() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/ws"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            "<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\">\
             <soap:Body><OtherResponse/></soap:Body></soap:Envelope>",
        ))
        .mount(&server)
        .await;

    let config = XmlStreamConfig::new(server.uri(), "/ws")
        .method(Method::POST)
        .records_element_path("GetUsersResponse.Users.User")
        .with_soap(SoapConfig {
            body_inner: Some("<GetUsers xmlns=\"urn:example\"/>".into()),
            ..Default::default()
        });
    let err = XmlStream::new(config).fetch_all().await.unwrap_err();
    assert!(matches!(err, FaucetError::Source(_)), "{err}");
    assert!(err.to_string().contains("GetUsersResponse"), "{err}");
}

#[tokio::test]
async fn a_soap_fault_on_http_500_is_handled_once_not_retried() {
    // API-46: a compliant SOAP 1.1 server answers a fault with 500; it used to
    // be re-POSTed as a transient error and never reached fault handling.
    let server = MockServer::start().await;
    let fault = "<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\"><soap:Body>\
         <soap:Fault><faultcode>soap:Server</faultcode>\
         <faultstring>Account &amp; region not found</faultstring></soap:Fault></soap:Body></soap:Envelope>";
    Mock::given(method("POST"))
        .and(path("/ws"))
        .respond_with(ResponseTemplate::new(500).set_body_string(fault))
        .expect(2)
        .mount(&server)
        .await;
    let config = |fault_as_error| {
        XmlStreamConfig::new(server.uri(), "/ws")
            .method(Method::POST)
            .records_element_path("GetUsersResponse.Users.User")
            .with_soap(SoapConfig {
                body_inner: Some("<GetUsers/>".into()),
                fault_as_error,
                ..Default::default()
            })
    };
    let err = XmlStream::new(config(true)).fetch_all().await.unwrap_err();
    assert!(
        matches!(&err, FaucetError::Source(m) if m.contains("Account & region not found")),
        "got {err:?}"
    );
    assert!(XmlStream::new(config(false)).fetch_all().await.unwrap().is_empty());
}

#[tokio::test]
async fn a_non_fault_http_500_is_still_retried() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/ws"))
        .respond_with(ResponseTemplate::new(500).set_body_string("<html>oops</html>"))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/ws"))
        .respond_with(ResponseTemplate::new(200).set_body_string(soap_users_response(
            "<User><Name>Ann</Name></User>",
        )))
        .mount(&server)
        .await;
    let config = XmlStreamConfig::new(server.uri(), "/ws")
        .method(Method::POST)
        .records_element_path("GetUsersResponse.Users.User")
        .with_soap(SoapConfig {
            body_inner: Some("<GetUsers/>".into()),
            ..Default::default()
        });
    let records = XmlStream::new(config).fetch_all().await.unwrap();
    assert_eq!(records[0]["Name"], "Ann");
}
