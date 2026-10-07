//! Integration tests for `XmlStream`'s HTTP fetch path against a wiremock
//! server: auth header application, pagination (page-number / offset),
//! `max_pages` capping, the identical-page loop guard, SOAP POST bodies,
//! and error paths (non-2xx status, malformed XML).

use faucet_core::FaucetError;
use faucet_source_xml::{XmlAuth, XmlPagination, XmlStream, XmlStreamConfig};
use reqwest::Method;
use std::collections::HashMap;
use wiremock::matchers::{body_string_contains, header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// `<root><item><id>i</id></item>...</root>` with `n` items.
fn items_doc(start: usize, n: usize) -> String {
    let mut s = String::from("<root>");
    for i in start..start + n {
        s.push_str(&format!("<item><id>{i}</id></item>"));
    }
    s.push_str("</root>");
    s
}

#[tokio::test]
async fn basic_auth_header_is_sent() {
    let server = MockServer::start().await;
    // Basic dXNlcjpwYXNz == base64("user:pass").
    Mock::given(method("GET"))
        .and(path("/feed.xml"))
        .and(header("authorization", "Basic dXNlcjpwYXNz"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "application/xml")
                .set_body_string(items_doc(0, 2)),
        )
        .expect(1)
        .mount(&server)
        .await;

    let config = XmlStreamConfig::new(server.uri(), "/feed.xml")
        .records_element_path("root.item")
        .auth(XmlAuth::Basic {
            username: "user".into(),
            password: "pass".into(),
        });
    let records = XmlStream::new(config).fetch_all().await.unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["id"], "0");
}

#[tokio::test]
async fn custom_auth_headers_are_sent() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/soap"))
        .and(header("soapaction", "urn:GetUsers"))
        .and(header("x-api-key", "secret-key"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "text/xml")
                .set_body_string(items_doc(0, 1)),
        )
        .expect(1)
        .mount(&server)
        .await;

    let mut headers = HashMap::new();
    headers.insert("SOAPAction".to_string(), "urn:GetUsers".to_string());
    headers.insert("X-API-Key".to_string(), "secret-key".to_string());

    let config = XmlStreamConfig::new(server.uri(), "/soap")
        .method(Method::POST)
        .records_element_path("root.item")
        .auth(XmlAuth::Custom { headers });
    let records = XmlStream::new(config).fetch_all().await.unwrap();
    assert_eq!(records.len(), 1);
}

#[tokio::test]
async fn custom_auth_invalid_header_name_errors() {
    // An illegal HTTP header name must surface as FaucetError::Auth before
    // any request is sent.
    let server = MockServer::start().await;
    let mut headers = HashMap::new();
    headers.insert("Invalid Header Name".to_string(), "v".to_string());
    let config = XmlStreamConfig::new(server.uri(), "/feed.xml")
        .records_element_path("root.item")
        .auth(XmlAuth::Custom { headers });
    let err = XmlStream::new(config).fetch_all().await.unwrap_err();
    assert!(matches!(err, FaucetError::Auth(_)), "got {err:?}");
}

#[tokio::test]
async fn soap_post_body_is_sent_and_response_extracted() {
    let server = MockServer::start().await;
    let soap_response = r#"<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
        <soap:Body>
            <GetUsersResponse>
                <User><Name>Alice</Name></User>
                <User><Name>Bob</Name></User>
            </GetUsersResponse>
        </soap:Body>
    </soap:Envelope>"#;
    Mock::given(method("POST"))
        .and(path("/soap"))
        .and(body_string_contains("GetUsers"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "text/xml")
                .set_body_string(soap_response),
        )
        .expect(1)
        .mount(&server)
        .await;

    let config = XmlStreamConfig::new(server.uri(), "/soap")
        .method(Method::POST)
        .body("<soap:Envelope><soap:Body><GetUsers/></soap:Body></soap:Envelope>")
        .records_element_path("soap:Envelope.soap:Body.GetUsersResponse.User");
    let records = XmlStream::new(config).fetch_all().await.unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["Name"], "Alice");
    assert_eq!(records[1]["Name"], "Bob");
}

#[tokio::test]
async fn query_params_are_sent() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/feed.xml"))
        .and(query_param("format", "xml"))
        .and(query_param("v", "2"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "application/xml")
                .set_body_string(items_doc(0, 1)),
        )
        .expect(1)
        .mount(&server)
        .await;

    let config = XmlStreamConfig::new(server.uri(), "/feed.xml")
        .records_element_path("root.item")
        .query_param("format", "xml")
        .query_param("v", "2");
    let records = XmlStream::new(config).fetch_all().await.unwrap();
    assert_eq!(records.len(), 1);
}

#[tokio::test]
async fn page_number_pagination_walks_pages_until_empty() {
    let server = MockServer::start().await;
    // page=1 -> 2 items, page=2 -> 2 items, page=3 -> empty (stops).
    Mock::given(method("GET"))
        .and(path("/feed.xml"))
        .and(query_param("page", "1"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "application/xml")
                .set_body_string(items_doc(0, 2)),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/feed.xml"))
        .and(query_param("page", "2"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "application/xml")
                .set_body_string(items_doc(2, 2)),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/feed.xml"))
        .and(query_param("page", "3"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "application/xml")
                .set_body_string("<root></root>"),
        )
        .mount(&server)
        .await;

    let config = XmlStreamConfig::new(server.uri(), "/feed.xml")
        .records_element_path("root.item")
        .pagination(XmlPagination::PageNumber {
            param_name: "page".into(),
            start_page: 1,
            page_size: None,
            page_size_param: None,
        });
    let records = XmlStream::new(config).fetch_all().await.unwrap();
    assert_eq!(records.len(), 4);
    assert_eq!(records[0]["id"], "0");
    assert_eq!(records[3]["id"], "3");
}

#[tokio::test]
async fn page_number_pagination_continues_past_a_short_page_until_empty() {
    let server = MockServer::start().await;
    // page_size=3 but the server clamps: page 2 is short (1), yet only the
    // empty page 3 ends paging (API-15).
    Mock::given(method("GET"))
        .and(path("/feed.xml"))
        .and(query_param("page", "1"))
        .and(query_param("size", "3"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "application/xml")
                .set_body_string(items_doc(0, 3)),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/feed.xml"))
        .and(query_param("page", "2"))
        .and(query_param("size", "3"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "application/xml")
                .set_body_string(items_doc(3, 1)),
        )
        .mount(&server)
        .await;

    let config = XmlStreamConfig::new(server.uri(), "/feed.xml")
        .records_element_path("root.item")
        .pagination(XmlPagination::PageNumber {
            param_name: "page".into(),
            start_page: 1,
            page_size: Some(3),
            page_size_param: Some("size".into()),
        });
    Mock::given(method("GET"))
        .and(path("/feed.xml"))
        .and(query_param("page", "3"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "application/xml")
                .set_body_string(items_doc(4, 0)),
        )
        .expect(1)
        .mount(&server)
        .await;
    let records = XmlStream::new(config).fetch_all().await.unwrap();
    assert_eq!(records.len(), 4, "3 full + 1 short page, then the empty page stops");
}

#[tokio::test]
async fn offset_pagination_walks_until_an_empty_page() {
    let server = MockServer::start().await;
    // limit=2: offset 0 -> 2, 2 -> 2, 4 -> 1 (short), 5 -> 0 (empty) -> stop.
    // A short page alone does not end paging: the server may clamp `limit`.
    Mock::given(method("GET"))
        .and(path("/feed.xml"))
        .and(query_param("offset", "0"))
        .and(query_param("limit", "2"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "application/xml")
                .set_body_string(items_doc(0, 2)),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/feed.xml"))
        .and(query_param("offset", "2"))
        .and(query_param("limit", "2"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "application/xml")
                .set_body_string(items_doc(2, 2)),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/feed.xml"))
        .and(query_param("offset", "4"))
        .and(query_param("limit", "2"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "application/xml")
                .set_body_string(items_doc(4, 1)),
        )
        .mount(&server)
        .await;

    let config = XmlStreamConfig::new(server.uri(), "/feed.xml")
        .records_element_path("root.item")
        .pagination(XmlPagination::Offset {
            offset_param: "offset".into(),
            limit_param: "limit".into(),
            limit: 2,
        });
    Mock::given(method("GET"))
        .and(path("/feed.xml"))
        .and(query_param("offset", "5"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "application/xml")
                .set_body_string(items_doc(5, 0)),
        )
        .expect(1)
        .mount(&server)
        .await;
    let records = XmlStream::new(config).fetch_all().await.unwrap();
    assert_eq!(records.len(), 5);
    assert_eq!(records[4]["id"], "4");
}

#[tokio::test]
async fn max_pages_caps_fetch() {
    let server = MockServer::start().await;
    // Every page is full (2 items), but max_pages=2 caps it at 4 records.
    // Use distinct page bodies so the loop guard does not trip first.
    Mock::given(method("GET"))
        .and(path("/feed.xml"))
        .and(query_param("page", "1"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "application/xml")
                .set_body_string(items_doc(0, 2)),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/feed.xml"))
        .and(query_param("page", "2"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "application/xml")
                .set_body_string(items_doc(2, 2)),
        )
        .mount(&server)
        .await;

    let config = XmlStreamConfig::new(server.uri(), "/feed.xml")
        .records_element_path("root.item")
        .max_pages(2)
        .pagination(XmlPagination::PageNumber {
            param_name: "page".into(),
            start_page: 1,
            page_size: None,
            page_size_param: None,
        });
    let records = XmlStream::new(config).fetch_all().await.unwrap();
    assert_eq!(records.len(), 4, "max_pages=2 -> at most 4 records");
}

#[tokio::test]
async fn identical_page_loop_guard_stops_fetch() {
    // A server that ignores the page param and returns the same non-empty
    // body forever must stop after two identical pages (audit #146 H4/H5).
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/feed.xml"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "application/xml")
                .set_body_string(items_doc(0, 2)),
        )
        .mount(&server)
        .await;

    let config = XmlStreamConfig::new(server.uri(), "/feed.xml")
        .records_element_path("root.item")
        .pagination(XmlPagination::PageNumber {
            param_name: "page".into(),
            start_page: 1,
            page_size: None,
            page_size_param: None,
        });
    let records = XmlStream::new(config).fetch_all().await.unwrap();
    // #321 M4: only page 1 is emitted (2 records). The identical page 2 trips
    // the stagnation guard and is DROPPED rather than emitted a second time
    // (previously it leaked 4 records = the duplicate page appended).
    assert_eq!(records.len(), 2);
}

#[tokio::test]
async fn non_2xx_status_returns_error() {
    // A persistent 404 is non-retriable and must surface as an error.
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/feed.xml"))
        .respond_with(ResponseTemplate::new(404).set_body_string("not found"))
        .mount(&server)
        .await;

    let config = XmlStreamConfig::new(server.uri(), "/feed.xml").records_element_path("root.item");
    let err = XmlStream::new(config).fetch_all().await.unwrap_err();
    assert!(
        matches!(err, FaucetError::HttpStatus { status, .. } if status == 404),
        "got {err:?}"
    );
}

#[tokio::test]
async fn malformed_xml_response_returns_transform_error() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/feed.xml"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "application/xml")
                .set_body_string("<root><a></b></root>"),
        )
        .mount(&server)
        .await;

    let config = XmlStreamConfig::new(server.uri(), "/feed.xml").records_element_path("root.a");
    let err = XmlStream::new(config).fetch_all().await.unwrap_err();
    assert!(
        matches!(&err, FaucetError::Transform(m) if m.contains("XML parse error")),
        "got {err:?}"
    );
}

#[tokio::test]
async fn no_records_path_returns_whole_document() {
    // With records_element_path = None, fetch_all returns the full doc as a
    // single record.
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/feed.xml"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "application/xml")
                .set_body_string("<root><name>Z</name></root>"),
        )
        .mount(&server)
        .await;

    let config = XmlStreamConfig::new(server.uri(), "/feed.xml");
    let records = XmlStream::new(config).fetch_all().await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["root"]["name"], "Z");
}
