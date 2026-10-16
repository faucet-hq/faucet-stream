//! Integration tests for the response-decode pipeline (#515) through the REST
//! source end-to-end.

use faucet_source_rest::{
    DecodeStep, PaginationStyle, ParseFormat, ParseSpec, RestStream, RestStreamConfig, SimpleStep,
    UnzipSpec,
};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn parse(format: ParseFormat) -> ParseSpec {
    ParseSpec {
        format,
        records_path: None,
        delimiter: None,
        has_headers: true,
        sheet: None,
        header_row: 0,
    }
}

/// A JSON envelope holding a base64-encoded, gzipped CSV — the shape of a
/// SOAP/JSON "reportBytes" export.
#[tokio::test]
async fn decodes_base64_gzip_csv_from_a_json_envelope() {
    use base64::Engine;
    use flate2::{Compression, write::GzEncoder};
    use std::io::Write;

    let csv = b"id,name\n1,alice\n2,bob\n";
    let mut enc = GzEncoder::new(Vec::new(), Compression::default());
    enc.write_all(csv).unwrap();
    let gz = enc.finish().unwrap();
    let b64 = base64::engine::general_purpose::STANDARD.encode(&gz);

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/report"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "d": { "reportBytes": b64 }
        })))
        .mount(&server)
        .await;

    // decode: extract the field → base64 → gunzip → parse CSV.
    let stream = RestStream::new(RestStreamConfig::new(&server.uri(), "/report").decode(vec![
        DecodeStep::Extract {
            extract: "$.d.reportBytes".into(),
        },
        DecodeStep::Simple(SimpleStep::Base64),
        DecodeStep::Simple(SimpleStep::Gunzip),
        DecodeStep::Parse {
            parse: parse(ParseFormat::Csv),
        },
    ]))
    .unwrap();

    let records = stream.fetch_all().await.unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["id"], "1");
    assert_eq!(records[1]["name"], "bob");
}

/// A zip export downloaded over HTTP: the member is picked by glob and parsed.
#[tokio::test]
async fn unzips_the_matching_member_of_a_downloaded_archive() {
    use std::io::Write;
    use zip::write::SimpleFileOptions;

    let mut cur = std::io::Cursor::new(Vec::new());
    {
        let mut zw = zip::ZipWriter::new(&mut cur);
        let opts = SimpleFileOptions::default();
        zw.start_file("README.txt", opts).unwrap();
        zw.write_all(b"not data").unwrap();
        zw.start_file("export/users.csv", opts).unwrap();
        zw.write_all(b"id,name\n1,alice\n2,bob\n").unwrap();
        zw.finish().unwrap();
    }

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/export.zip"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(cur.into_inner()))
        .mount(&server)
        .await;

    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/export.zip").decode(vec![
            DecodeStep::Unzip {
                unzip: UnzipSpec {
                    member: Some("*.csv".into()),
                },
            },
            DecodeStep::Parse {
                parse: parse(ParseFormat::Csv),
            },
        ]),
    )
    .unwrap();

    let records = stream.fetch_all().await.unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[1]["name"], "bob");
}

/// A `decode:` pipeline requires `pagination: none`.
#[test]
fn decode_with_pagination_is_rejected() {
    let cfg = RestStreamConfig::new("https://x", "/y")
        .pagination(PaginationStyle::PageNumber {
            param_name: "page".into(),
            start_page: 1,
            page_size: None,
            page_size_param: None,
        })
        .decode(vec![DecodeStep::Parse {
            parse: parse(ParseFormat::Json),
        }]);
    assert!(RestStream::new(cfg).is_err());
}
