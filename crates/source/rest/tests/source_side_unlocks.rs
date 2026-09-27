//! Integration tests for the source-side unlocks: OData mode (#512),
//! server-side incremental push-down (#513), and flow-provider request-auth
//! placement / dynamic base-URL (#511).

use std::collections::BTreeMap;
use std::sync::Arc;

use faucet_core::{
    AuthProvider, Credential, CredentialPlacement, FaucetError, ReplicationBind, ReplicationMethod,
    RequestAuth, SharedAuthProvider, Source,
};
use faucet_source_rest::{ODataConfig, ODataVersion, RestStream, RestStreamConfig};
use serde_json::json;
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

// ── #512 OData ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn odata_follows_nextlink_and_unwraps_value_envelope() {
    let server = MockServer::start().await;
    let page2 = format!("{}/Orders?page=2", server.uri());
    // Page 1: `$.value` records + a `@odata.nextLink` to page 2, and the derived
    // `$select` + `Prefer` should be present.
    Mock::given(method("GET"))
        .and(path("/Orders"))
        .and(query_param("$select", "DocEntry,DocDate"))
        .and(header("prefer", "odata.maxpagesize=2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "value": [ {"DocEntry": 1}, {"DocEntry": 2} ],
            "@odata.nextLink": page2,
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/Orders"))
        .and(query_param("page", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "value": [ {"DocEntry": 3} ],
        })))
        .mount(&server)
        .await;

    let stream = RestStream::new(RestStreamConfig::new(&server.uri(), "").odata(ODataConfig {
        version: ODataVersion::V4,
        entity: Some("Orders".to_owned()),
        select: vec!["DocEntry".to_owned(), "DocDate".to_owned()],
        page_size: Some(2),
        ..Default::default()
    }))
    .unwrap();

    let records = stream.fetch_all().await.unwrap();
    assert_eq!(records.len(), 3);
    assert_eq!(records[2]["DocEntry"], 3);
}

#[tokio::test]
async fn odata_discover_parses_metadata() {
    let server = MockServer::start().await;
    let edmx = r#"<edmx:Edmx xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
      <Schema Namespace="S">
        <EntityType Name="Order">
          <Key><PropertyRef Name="DocEntry"/></Key>
          <Property Name="DocEntry" Type="Edm.Int32" Nullable="false"/>
          <Property Name="DocDate" Type="Edm.DateTimeOffset"/>
        </EntityType>
        <EntityContainer Name="C">
          <EntitySet Name="Orders" EntityType="S.Order"/>
        </EntityContainer>
      </Schema>
    </edmx:Edmx>"#;
    Mock::given(method("GET"))
        .and(path("/$metadata"))
        .respond_with(ResponseTemplate::new(200).set_body_string(edmx))
        .mount(&server)
        .await;

    let stream = RestStream::new(RestStreamConfig::new(&server.uri(), "").odata(ODataConfig {
        entity: Some("Orders".to_owned()),
        ..Default::default()
    }))
    .unwrap();

    assert!(stream.supports_discover());
    let datasets = stream.discover().await.unwrap();
    assert_eq!(datasets.len(), 1);
    assert_eq!(datasets[0].name, "Orders");
    assert_eq!(
        datasets[0].config_patch,
        json!({"odata": {"entity": "Orders"}})
    );
    assert_eq!(
        datasets[0].schema.as_ref().unwrap()["properties"]["DocEntry"]["type"],
        "integer"
    );
}

// ── #513 server-side incremental push-down ─────────────────────────────────────

#[tokio::test]
async fn bind_pushes_bookmark_into_query_and_advances() {
    let server = MockServer::start().await;
    // The mock only matches when the rendered bookmark is pushed down as
    // `?updated_after=gte|2024-06-01T00:00:00Z`.
    Mock::given(method("GET"))
        .and(path("/events"))
        .and(query_param("updated_after", "gte|2024-06-01T00:00:00Z"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [ {"id": 1, "updated_at": "2024-07-01T00:00:00Z"} ],
            "max_updated": "2024-07-15T00:00:00Z",
        })))
        .mount(&server)
        .await;

    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/events")
            .records_path("$.data[*]")
            .replication_method(ReplicationMethod::Incremental)
            .replication_key("updated_at")
            .start_replication_value(json!("2024-06-01"))
            .replication_bind(ReplicationBind {
                into: faucet_core::BindTarget::Query,
                name: "updated_after".to_owned(),
                template: "gte|${bookmark}".to_owned(),
                format: faucet_core::BindFormat::Iso8601,
                advance_from: Some("$.max_updated".to_owned()),
                path: None,
                value_type: Default::default(),
            }),
    )
    .unwrap();

    let ctx = std::collections::HashMap::new();
    let mut pages = Vec::new();
    {
        use futures::StreamExt;
        let mut s = Source::stream_pages(&stream, &ctx, 0);
        while let Some(p) = s.next().await {
            pages.push(p.unwrap());
        }
    }
    let records: Vec<_> = pages.iter().flat_map(|p| p.records.clone()).collect();
    assert_eq!(records.len(), 1);
    // advance_from pulled the next bookmark from `$.max_updated`, not max(record).
    let bookmark = pages.iter().rev().find_map(|p| p.bookmark.clone()).unwrap();
    assert_eq!(bookmark, json!("2024-07-15T00:00:00Z"));
}

// ── #511 flow-provider request-auth placement + dynamic base-URL ────────────────

#[derive(Debug)]
struct MockFlow {
    base_url: String,
}

#[async_trait::async_trait]
impl AuthProvider for MockFlow {
    async fn credential(&self) -> Result<Credential, FaucetError> {
        Err(FaucetError::Auth("query-placed; use request_auth".into()))
    }
    async fn request_auth(
        &self,
        _method: &str,
        _url: &str,
        _query: &BTreeMap<String, String>,
    ) -> Result<RequestAuth, FaucetError> {
        Ok(RequestAuth::new()
            .with_placement(CredentialPlacement::Query {
                name: "BhRestToken".to_owned(),
                value: "SESSION".to_owned(),
            })
            .with_base_url(self.base_url.clone()))
    }
    fn provider_name(&self) -> &'static str {
        "mock-flow"
    }
}

#[tokio::test]
async fn flow_provider_places_query_token_and_overrides_base_url() {
    // The data server is the base-URL the provider captures; the config points at
    // a decoy base-URL to prove the dynamic override wins.
    let data = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/orders"))
        .and(query_param("BhRestToken", "SESSION"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"value": [{"id": 7}]})))
        .mount(&data)
        .await;

    let provider: SharedAuthProvider = Arc::new(MockFlow {
        base_url: data.uri(),
    });
    let stream = RestStream::new(
        RestStreamConfig::new("https://decoy.invalid", "/orders").records_path("$.value[*]"),
    )
    .unwrap()
    .with_auth_provider(provider);

    let records = stream.fetch_all().await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["id"], 7);
}

#[derive(Debug)]
struct MockFlowHeaderCookie;

#[async_trait::async_trait]
impl AuthProvider for MockFlowHeaderCookie {
    async fn credential(&self) -> Result<Credential, FaucetError> {
        Err(FaucetError::Auth("use request_auth".into()))
    }
    async fn request_auth(
        &self,
        _method: &str,
        _url: &str,
        _query: &BTreeMap<String, String>,
    ) -> Result<RequestAuth, FaucetError> {
        Ok(RequestAuth::new()
            .with_placement(CredentialPlacement::Header {
                name: "X-Session".to_owned(),
                value: "SID".to_owned(),
            })
            .with_placement(CredentialPlacement::Cookie {
                name: "sid".to_owned(),
                value: "abc".to_owned(),
            }))
    }
    fn provider_name(&self) -> &'static str {
        "mock-flow-hc"
    }
}

#[tokio::test]
async fn flow_provider_places_header_and_cookie() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/data"))
        .and(header("X-Session", "SID"))
        .and(header("Cookie", "sid=abc"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"value": [{"id": 1}]})))
        .mount(&server)
        .await;
    let provider: SharedAuthProvider = Arc::new(MockFlowHeaderCookie);
    let stream =
        RestStream::new(RestStreamConfig::new(&server.uri(), "/data").records_path("$.value[*]"))
            .unwrap()
            .with_auth_provider(provider);
    let records = stream.fetch_all().await.unwrap();
    assert_eq!(records.len(), 1);
}

#[tokio::test]
async fn replication_bind_pushes_bookmark_into_a_header() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/events"))
        .and(header("If-Modified-Since", "2024-06-01T00:00:00Z"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"data": [{"id": 1, "updated_at": "2024-07-01T00:00:00Z"}]})),
        )
        .mount(&server)
        .await;
    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/events")
            .records_path("$.data[*]")
            .replication_method(ReplicationMethod::Incremental)
            .replication_key("updated_at")
            .start_replication_value(json!("2024-06-01"))
            .replication_bind(ReplicationBind {
                into: faucet_core::BindTarget::Header,
                name: "If-Modified-Since".to_owned(),
                template: "${bookmark}".to_owned(),
                format: faucet_core::BindFormat::Iso8601,
                advance_from: None,
                path: None,
                value_type: Default::default(),
            }),
    )
    .unwrap();
    let records = stream.fetch_all().await.unwrap();
    assert_eq!(records.len(), 1);
}

// ── #527 in-run datetime window slicing ─────────────────────────────────────────

use faucet_source_rest::WindowSpec;

/// Drain a windowed stream into (all records, last non-null bookmark).
async fn drain_windowed(
    stream: &RestStream,
) -> (Vec<serde_json::Value>, Option<serde_json::Value>) {
    use futures::StreamExt;
    let ctx = std::collections::HashMap::new();
    let mut records = Vec::new();
    let mut bookmark = None;
    let mut s = Source::stream_pages(stream, &ctx, 0);
    while let Some(p) = s.next().await {
        let p = p.unwrap();
        records.extend(p.records);
        if let Some(bm) = p.bookmark {
            bookmark = Some(bm);
        }
    }
    (records, bookmark)
}

fn window_query(name_lower: &str, name_upper: &str) -> WindowSpec {
    serde_json::from_value(json!({
        "step": "1d",
        "lower": {"into": "query", "name": name_lower, "format": "date"},
        "upper": {"into": "query", "name": name_upper, "format": "date"},
    }))
    .unwrap()
}

#[tokio::test]
async fn window_slices_span_into_bounded_requests_and_advances_to_now() {
    let server = MockServer::start().await;
    // Three 1-day windows over [2024-01-01, 2024-01-04); each request carries the
    // window's start_date/end_date, and each window returns one record.
    for (day, id) in [("2024-01-01", 1), ("2024-01-02", 2), ("2024-01-03", 3)] {
        Mock::given(method("GET"))
            .and(path("/report"))
            .and(query_param("start_date", day))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [ {"id": id, "updated_at": format!("{day}T12:00:00Z")} ],
            })))
            .mount(&server)
            .await;
    }

    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/report")
            .records_path("$.data[*]")
            .replication_method(ReplicationMethod::Incremental)
            .replication_key("updated_at")
            .start_replication_value(json!("2024-01-01"))
            .window(window_query("start_date", "end_date")),
    )
    .unwrap()
    .with_now_override_rfc3339("2024-01-04T00:00:00Z");

    let (records, bookmark) = drain_windowed(&stream).await;
    assert_eq!(records.len(), 3, "one record per window");
    let mut ids: Vec<i64> = records.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    ids.sort();
    assert_eq!(ids, vec![1, 2, 3]);
    // The persisted bookmark is the last window's end (== now), so the next run
    // resumes from there with no gap and no overlap.
    let bm = bookmark.expect("a window sweep persists its boundary");
    assert!(
        bm.as_str().unwrap().starts_with("2024-01-04T00:00:00"),
        "bookmark should be the final window end (now), got {bm}"
    );
}

#[tokio::test]
async fn window_upper_bound_is_sent_on_each_request() {
    let server = MockServer::start().await;
    // A single window [2024-01-01, 2024-01-02): assert BOTH bounds land as query
    // params (start_date AND end_date), proving the request is bounded on both
    // sides — the whole point versus a single lower-bound `replication_bind`.
    Mock::given(method("GET"))
        .and(path("/report"))
        .and(query_param("start_date", "2024-01-01"))
        .and(query_param("end_date", "2024-01-02"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [ {"id": 1, "updated_at": "2024-01-01T09:00:00Z"} ],
        })))
        .mount(&server)
        .await;

    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/report")
            .records_path("$.data[*]")
            .replication_method(ReplicationMethod::Incremental)
            .replication_key("updated_at")
            .start_replication_value(json!("2024-01-01"))
            .window(window_query("start_date", "end_date")),
    )
    .unwrap()
    .with_now_override_rfc3339("2024-01-02T00:00:00Z");

    let (records, _) = drain_windowed(&stream).await;
    assert_eq!(records.len(), 1);
}

#[tokio::test]
async fn window_lookback_extends_the_first_window_backwards() {
    let server = MockServer::start().await;
    // start=2024-01-02, lookback=1d → the first window starts 2024-01-01.
    for day in ["2024-01-01", "2024-01-02"] {
        Mock::given(method("GET"))
            .and(path("/report"))
            .and(query_param("start_date", day))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [ {"id": 1, "updated_at": format!("{day}T00:00:00Z")} ],
            })))
            .mount(&server)
            .await;
    }
    let spec: WindowSpec = serde_json::from_value(json!({
        "step": "1d",
        "lower": {"into": "query", "name": "start_date", "format": "date"},
        "upper": {"into": "query", "name": "end_date", "format": "date"},
        "lookback": "1d",
    }))
    .unwrap();

    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/report")
            .records_path("$.data[*]")
            .replication_method(ReplicationMethod::Incremental)
            .replication_key("updated_at")
            .start_replication_value(json!("2024-01-02"))
            .window(spec),
    )
    .unwrap()
    .with_now_override_rfc3339("2024-01-03T00:00:00Z");

    // Both the lookback window (start 2024-01-01) and the current window must be
    // fetched — 2 records. If lookback were ignored, the 2024-01-01 mock would
    // never be hit and we'd get only 1.
    let (records, _) = drain_windowed(&stream).await;
    assert_eq!(
        records.len(),
        2,
        "lookback must fetch the day before the bookmark"
    );
}

#[tokio::test]
async fn window_granularity_makes_upper_inclusive_but_bookmark_is_true_boundary() {
    let server = MockServer::start().await;
    // step=1d, granularity=1d → for window [2024-01-01, 2024-01-02) the rendered
    // upper is end - granularity = 2024-01-01 (inclusive-inclusive API), but the
    // persisted bookmark stays the true half-open end 2024-01-02.
    Mock::given(method("GET"))
        .and(path("/report"))
        .and(query_param("start_date", "2024-01-01"))
        .and(query_param("end_date", "2024-01-01"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [ {"id": 1, "updated_at": "2024-01-01T06:00:00Z"} ],
        })))
        .mount(&server)
        .await;
    let spec: WindowSpec = serde_json::from_value(json!({
        "step": "1d",
        "granularity": "1d",
        "lower": {"into": "query", "name": "start_date", "format": "date"},
        "upper": {"into": "query", "name": "end_date", "format": "date"},
    }))
    .unwrap();

    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/report")
            .records_path("$.data[*]")
            .replication_method(ReplicationMethod::Incremental)
            .replication_key("updated_at")
            .start_replication_value(json!("2024-01-01"))
            .window(spec),
    )
    .unwrap()
    .with_now_override_rfc3339("2024-01-02T00:00:00Z");

    let (records, bookmark) = drain_windowed(&stream).await;
    assert_eq!(records.len(), 1);
    let bm = bookmark.unwrap();
    assert!(
        bm.as_str().unwrap().starts_with("2024-01-02T00:00:00"),
        "bookmark must be the true half-open boundary, not the granularity-adjusted upper: {bm}"
    );
}

#[tokio::test]
async fn window_empty_when_bookmark_at_or_after_now() {
    let server = MockServer::start().await;
    // No mock mounted: if any request fired, wiremock would 404 and the run would
    // error. Bookmark == now → zero windows → no requests → a clean no-op.
    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/report")
            .records_path("$.data[*]")
            .replication_method(ReplicationMethod::Incremental)
            .replication_key("updated_at")
            .start_replication_value(json!("2024-01-04T00:00:00Z"))
            .window(window_query("start_date", "end_date")),
    )
    .unwrap()
    .with_now_override_rfc3339("2024-01-04T00:00:00Z");

    let (records, bookmark) = drain_windowed(&stream).await;
    assert!(records.is_empty());
    assert!(bookmark.is_none(), "a no-op sweep persists no bookmark");
}

#[test]
fn window_requires_incremental_and_replication_key() {
    // Missing replication_key. `.map(|_| ())` drops the non-`Debug` `RestStream`
    // so `unwrap_err` can format the `Ok` side.
    let err = RestStream::new(
        RestStreamConfig::new("https://x", "/r")
            .records_path("$.data[*]")
            .replication_method(ReplicationMethod::Incremental)
            .window(window_query("start_date", "end_date")),
    )
    .map(|_| ())
    .unwrap_err();
    assert!(matches!(err, FaucetError::Config(m) if m.contains("replication_key")));

    // Not incremental.
    let err = RestStream::new(
        RestStreamConfig::new("https://x", "/r")
            .records_path("$.data[*]")
            .replication_key("updated_at")
            .window(window_query("start_date", "end_date")),
    )
    .map(|_| ())
    .unwrap_err();
    assert!(matches!(err, FaucetError::Config(m) if m.contains("incremental")));
}

#[tokio::test]
async fn window_without_start_bookmark_errors() {
    let server = MockServer::start().await;
    // No start_replication_value and no state → the window sweep can't anchor.
    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/report")
            .records_path("$.data[*]")
            .replication_method(ReplicationMethod::Incremental)
            .replication_key("date")
            .window(window_query("start_date", "end_date")),
    )
    .unwrap()
    .with_now_override_rfc3339("2024-01-02T00:00:00Z");

    use futures::StreamExt;
    let ctx = std::collections::HashMap::new();
    let mut s = Source::stream_pages(&stream, &ctx, 0);
    let first = s.next().await.expect("a stream item");
    let err = first.expect_err("window slicing without a start bookmark must error");
    assert!(matches!(err, FaucetError::Config(m) if m.contains("start bookmark")));
}

#[tokio::test]
async fn window_max_windows_truncates_the_sweep() {
    let server = MockServer::start().await;
    // A 3-day span at 1-day step would be 3 windows, but max_windows=1 truncates
    // to the first — the next run resumes from that window's end.
    Mock::given(method("GET"))
        .and(path("/report"))
        .and(query_param("start_date", "2024-01-01"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [ {"id": 1, "updated_at": "2024-01-01T00:00:00Z"} ],
        })))
        .mount(&server)
        .await;
    let spec: WindowSpec = serde_json::from_value(json!({
        "step": "1d",
        "max_windows": 1,
        "lower": {"into": "query", "name": "start_date", "format": "date"},
        "upper": {"into": "query", "name": "end_date", "format": "date"},
    }))
    .unwrap();

    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/report")
            .records_path("$.data[*]")
            .replication_method(ReplicationMethod::Incremental)
            .replication_key("updated_at")
            .start_replication_value(json!("2024-01-01"))
            .window(spec),
    )
    .unwrap()
    .with_now_override_rfc3339("2024-01-04T00:00:00Z");

    let (records, bookmark) = drain_windowed(&stream).await;
    // Only the first window ran (no mock for 2024-01-02/03 → they'd 404 if hit).
    assert_eq!(records.len(), 1);
    // Bookmark is the first window's end, so the next run resumes there.
    let bm = bookmark.unwrap();
    assert!(
        bm.as_str().unwrap().starts_with("2024-01-02T00:00:00"),
        "got {bm}"
    );
}
