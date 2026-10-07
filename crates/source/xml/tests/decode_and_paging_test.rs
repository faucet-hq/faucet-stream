//! Integration tests for the XML source's response-decode pipeline (#540) and
//! body-cursor pagination (#544), driven through the real HTTP path via wiremock.

use base64::Engine;
use faucet_core::Source;
use faucet_source_xml::config::XmlPagination;
use faucet_source_xml::decode::{DecodeStep, ParseFormat, ParseSpec, SimpleStep};
use faucet_source_xml::{XmlStream, XmlStreamConfig};
use std::collections::HashMap;
use wiremock::matchers::{body_string_contains, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// #540: a SOAP response carrying a base64-encoded CSV inside `<reportBytes>` is
/// decoded (extract → base64 → parse csv) into records through the HTTP path.
#[tokio::test]
async fn decode_pipeline_base64_csv_over_http() {
    let server = MockServer::start().await;
    let csv = b"id,name\n1,alice\n2,bob\n";
    let b64 = base64::engine::general_purpose::STANDARD.encode(csv);
    let body = format!(
        "<soap:Envelope><soap:Body><runReportResponse><runReportReturn>\
         <reportBytes>{b64}</reportBytes></runReportReturn></runReportResponse>\
         </soap:Body></soap:Envelope>"
    );
    Mock::given(method("POST"))
        .and(path("/report"))
        .respond_with(ResponseTemplate::new(200).set_body_string(body))
        .mount(&server)
        .await;

    let mut config = XmlStreamConfig::new(server.uri(), "/report")
        .method(reqwest::Method::POST)
        .body("<runReport/>")
        .decode(vec![
            DecodeStep::Extract {
                extract: "runReportResponse.runReportReturn.reportBytes".into(),
            },
            DecodeStep::Simple(SimpleStep::Base64),
            DecodeStep::Parse {
                parse: ParseSpec {
                    format: ParseFormat::Csv,
                    records_path: None,
                    delimiter: None,
                    has_headers: true,
                    sheet: None,
                    header_row: 0,
                },
            },
        ]);
    config.batch_size = 0;
    let stream = XmlStream::new(config);

    let records = stream.fetch_all().await.unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["id"], "1");
    assert_eq!(records[0]["name"], "alice");
    assert_eq!(records[1]["name"], "bob");
}

/// #544: body-cursor pagination reads a `resultId` from the response and re-POSTs
/// a `readMore` body carrying it. The second page's mock only matches when the
/// token was injected, so a green test proves the token round-tripped.
#[tokio::test]
async fn body_cursor_pagination_walks_two_pages() {
    let server = MockServer::start().await;

    // Page 1: initial readByQuery → one row + a continuation token.
    Mock::given(method("POST"))
        .and(path("/gw"))
        .and(body_string_contains("readByQuery"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            "<response><data><row><id>1</id></row></data><resultId>TOK-42</resultId></response>",
        ))
        .mount(&server)
        .await;

    // Page 2: only matches if the request body carried the token (readMore + TOK-42).
    Mock::given(method("POST"))
        .and(path("/gw"))
        .and(body_string_contains("readMore"))
        .and(body_string_contains("TOK-42"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string("<response><data><row><id>2</id></row></data></response>"),
        )
        .mount(&server)
        .await;

    let config = XmlStreamConfig::new(server.uri(), "/gw")
        .method(reqwest::Method::POST)
        .body("<readByQuery><object>GLDETAIL</object></readByQuery>")
        .records_element_path("response.data.row")
        .pagination(XmlPagination::BodyCursor {
            next_token_path: "resultId".into(),
            next_body: "<readMore><resultId>${next_token}</resultId></readMore>".into(),
        });
    let stream = XmlStream::new(config);

    let records = stream.fetch_all().await.unwrap();
    assert_eq!(records.len(), 2, "both pages should be fetched");
    assert_eq!(records[0]["id"], "1");
    assert_eq!(records[1]["id"], "2");
}

/// #544 termination: an absent token on the first page stops after one page.
#[tokio::test]
async fn body_cursor_stops_when_no_token() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/gw"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string("<response><data><row><id>1</id></row></data></response>"),
        )
        .mount(&server)
        .await;

    let config = XmlStreamConfig::new(server.uri(), "/gw")
        .method(reqwest::Method::POST)
        .body("<readByQuery/>")
        .records_element_path("response.data.row")
        .pagination(XmlPagination::BodyCursor {
            next_token_path: "resultId".into(),
            next_body: "<readMore><resultId>${next_token}</resultId></readMore>".into(),
        });
    let records = XmlStream::new(config)
        .fetch_with_context(&HashMap::new())
        .await
        .unwrap();
    assert_eq!(records.len(), 1);
}

/// An XML document inside the response keeps its attributes when parsed.
#[tokio::test]
async fn decode_pipeline_parses_xml_with_attributes() {
    let server = MockServer::start().await;
    let inner = r#"<rows><row id="1" kind="a"/><row id="2" kind="b"/></rows>"#;
    let b64 = base64::engine::general_purpose::STANDARD.encode(inner);
    Mock::given(method("POST"))
        .and(path("/report"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(format!("<r><payload>{b64}</payload></r>")),
        )
        .mount(&server)
        .await;
    let mut config = XmlStreamConfig::new(server.uri(), "/report")
        .method(reqwest::Method::POST)
        .body("<q/>")
        .decode(vec![
            DecodeStep::Extract {
                extract: "payload".into(),
            },
            DecodeStep::Simple(SimpleStep::Base64),
            DecodeStep::Parse {
                parse: ParseSpec {
                    format: ParseFormat::Xml,
                    records_path: None,
                    delimiter: None,
                    has_headers: true,
                    sheet: None,
                    header_row: 0,
                },
            },
        ]);
    config.batch_size = 0;
    let records = XmlStream::new(config).fetch_all().await.unwrap();
    let text = serde_json::to_string(&records).unwrap();
    for needle in ["\"@id\":\"1\"", "\"@kind\":\"b\""] {
        assert!(text.contains(needle), "{needle} in {text}");
    }
}

/// API-25: parent-record values and the body-cursor token are XML-escaped in
/// the request body, so `A&B Ltd` and markup-bearing values stay data.
#[tokio::test]
async fn body_values_are_xml_escaped() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/gw"))
        .and(body_string_contains("<name>A&amp;B Ltd</name>"))
        .and(body_string_contains("<f>&lt;/f&gt;&lt;x&gt;{id}</f>"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            "<response><data><row><id>1</id></row></data><resultId>a&amp;b&lt;</resultId></response>",
        ))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/gw"))
        .and(body_string_contains("<resultId>a&amp;b&lt;</resultId>"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string("<response><data><row><id>2</id></row></data></response>"),
        )
        .mount(&server)
        .await;
    let config = XmlStreamConfig::new(server.uri(), "/gw")
        .method(reqwest::Method::POST)
        .body("<q><name>{company}</name><f>{filter}</f></q>")
        .records_element_path("response.data.row")
        .pagination(XmlPagination::BodyCursor {
            next_token_path: "resultId".into(),
            next_body: "<readMore><resultId>${next_token}</resultId></readMore>".into(),
        });
    let ctx: HashMap<String, serde_json::Value> = [
        ("company".to_string(), serde_json::json!("A&B Ltd")),
        ("filter".to_string(), serde_json::json!("</f><x>{id}")),
        ("id".to_string(), serde_json::json!("SHOULD-NOT-APPEAR")),
    ]
    .into_iter()
    .collect();
    let records = XmlStream::new(config)
        .fetch_with_context(&ctx)
        .await
        .unwrap();
    assert_eq!(records.len(), 2);
}

/// API-31: body-cursor paging yields each HTTP page as it arrives instead of
/// buffering the whole run first.
#[tokio::test]
async fn body_cursor_pages_stream_before_the_next_request() {
    use futures::StreamExt;
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/gw"))
        .and(body_string_contains("readByQuery"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            "<response><data><row><id>1</id></row></data><resultId>T1</resultId></response>",
        ))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/gw"))
        .and(body_string_contains("readMore"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string("<response><data><row><id>2</id></row></data></response>"),
        )
        .mount(&server)
        .await;
    let mut config = XmlStreamConfig::new(server.uri(), "/gw")
        .method(reqwest::Method::POST)
        .body("<readByQuery/>")
        .records_element_path("response.data.row")
        .pagination(XmlPagination::BodyCursor {
            next_token_path: "resultId".into(),
            next_body: "<readMore>${next_token}</readMore>".into(),
        });
    config.batch_size = 1;
    let stream = XmlStream::new(config);
    let ctx = HashMap::new();
    let mut pages = stream.stream_pages(&ctx, 1);
    let first = pages.next().await.unwrap().unwrap();
    assert_eq!(first.records[0]["id"], "1");
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
    let second = pages.next().await.unwrap().unwrap();
    assert_eq!(second.records[0]["id"], "2");
    assert!(pages.next().await.is_none());
}
