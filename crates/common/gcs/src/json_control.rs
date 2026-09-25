//! Control-plane calls over the GCS JSON API, for plaintext emulator endpoints.
//!
//! The SDK's `StorageControl` client talks gRPC, which needs HTTP/2. An emulator
//! served over plain `http://` (fake-gcs-server) speaks HTTP/2 only behind TLS,
//! so a gRPC call against it fails at the transport. The data-plane `Storage`
//! client already uses the JSON API, so routing the two control-plane calls
//! faucet makes, `list_objects` and `get_object`, through the same API makes an
//! `http://` storage host work end to end.

use google_cloud_gax::error::Error;
use google_cloud_gax::options::RequestOptions;
use google_cloud_gax::response::Response;
use google_cloud_storage::model::{
    GetObjectRequest, ListObjectsRequest, ListObjectsResponse, Object,
};
use serde::Deserialize;

const BUCKET_PREFIX: &str = "projects/_/buckets/";

/// Whether a storage-host override points at a plaintext endpoint.
pub(crate) fn is_plaintext_endpoint(host: &str) -> bool {
    host.get(..7)
        .is_some_and(|scheme| scheme.eq_ignore_ascii_case("http://"))
}

/// A `StorageControl` stub whose `list_objects` and `get_object` call the JSON API.
#[derive(Debug, Clone)]
pub(crate) struct JsonApiControl {
    endpoint: String,
    http: reqwest::Client,
}

impl JsonApiControl {
    pub(crate) fn new(endpoint: &str) -> Self {
        Self {
            endpoint: endpoint.trim_end_matches('/').to_string(),
            http: reqwest::Client::new(),
        }
    }

    fn list_url(&self, req: &ListObjectsRequest) -> Result<String, Error> {
        let bucket = bucket_from_parent(&req.parent)?;
        let mut url = format!(
            "{}/storage/v1/b/{}/o",
            self.endpoint,
            urlencoding::encode(bucket)
        );
        let query = list_query(req);
        if !query.is_empty() {
            url.push('?');
            url.push_str(&query);
        }
        Ok(url)
    }

    fn get_url(&self, req: &GetObjectRequest) -> Result<String, Error> {
        let bucket = bucket_from_parent(&req.bucket)?;
        if req.object.is_empty() {
            return Err(Error::binding("get_object needs an object name"));
        }
        Ok(format!(
            "{}/storage/v1/b/{}/o/{}",
            self.endpoint,
            urlencoding::encode(bucket),
            urlencoding::encode(&req.object)
        ))
    }

    async fn get_json<T: serde::de::DeserializeOwned>(&self, url: &str) -> Result<T, Error> {
        let resp = self.http.get(url).send().await.map_err(Error::io)?;
        let status = resp.status();
        let headers = resp.headers().clone();
        let body = resp.bytes().await.map_err(Error::io)?;
        if !status.is_success() {
            return Err(Error::http(status.as_u16(), headers, body));
        }
        serde_json::from_slice(&body).map_err(Error::deser)
    }
}

impl google_cloud_storage::stub::StorageControl for JsonApiControl {
    async fn list_objects(
        &self,
        req: ListObjectsRequest,
        _options: RequestOptions,
    ) -> google_cloud_gax::Result<Response<ListObjectsResponse>> {
        let page: JsonListPage = self.get_json(&self.list_url(&req)?).await?;
        Ok(Response::from(
            page.into_response(bucket_from_parent(&req.parent)?),
        ))
    }

    async fn get_object(
        &self,
        req: GetObjectRequest,
        _options: RequestOptions,
    ) -> google_cloud_gax::Result<Response<Object>> {
        let object: JsonObject = self.get_json(&self.get_url(&req)?).await?;
        Ok(Response::from(
            object.into_model(bucket_from_parent(&req.bucket)?),
        ))
    }
}

fn bucket_from_parent(parent: &str) -> Result<&str, Error> {
    parent
        .strip_prefix(BUCKET_PREFIX)
        .filter(|b| !b.is_empty() && !b.contains('/'))
        .ok_or_else(|| {
            Error::binding(format!(
                "bucket must be `{BUCKET_PREFIX}<bucket>`, got `{parent}`"
            ))
        })
}

fn list_query(req: &ListObjectsRequest) -> String {
    let mut pairs: Vec<(&str, String)> = Vec::new();
    if !req.prefix.is_empty() {
        pairs.push(("prefix", req.prefix.clone()));
    }
    if !req.delimiter.is_empty() {
        pairs.push(("delimiter", req.delimiter.clone()));
    }
    if req.include_trailing_delimiter {
        pairs.push(("includeTrailingDelimiter", "true".into()));
    }
    if req.versions {
        pairs.push(("versions", "true".into()));
    }
    if req.page_size > 0 {
        pairs.push(("maxResults", req.page_size.to_string()));
    }
    if !req.page_token.is_empty() {
        pairs.push(("pageToken", req.page_token.clone()));
    }
    pairs
        .into_iter()
        .map(|(k, v)| format!("{k}={}", urlencoding::encode(&v)))
        .collect::<Vec<_>>()
        .join("&")
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonListPage {
    #[serde(default)]
    items: Vec<JsonObject>,
    #[serde(default)]
    prefixes: Vec<String>,
    #[serde(default)]
    next_page_token: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonObject {
    name: String,
    #[serde(default)]
    size: Option<String>,
    #[serde(default)]
    content_type: Option<String>,
    #[serde(default)]
    content_encoding: Option<String>,
}

impl JsonObject {
    fn into_model(self, bucket: &str) -> Object {
        let mut obj = Object::new()
            .set_name(self.name)
            .set_bucket(format!("{BUCKET_PREFIX}{bucket}"));
        if let Some(size) = self.size.and_then(|s| s.parse::<i64>().ok()) {
            obj = obj.set_size(size);
        }
        if let Some(ct) = self.content_type {
            obj = obj.set_content_type(ct);
        }
        if let Some(ce) = self.content_encoding {
            obj = obj.set_content_encoding(ce);
        }
        obj
    }
}

impl JsonListPage {
    fn into_response(self, bucket: &str) -> ListObjectsResponse {
        let objects = self
            .items
            .into_iter()
            .map(|o| o.into_model(bucket))
            .collect::<Vec<_>>();
        ListObjectsResponse::new()
            .set_objects(objects)
            .set_prefixes(self.prefixes)
            .set_next_page_token(self.next_page_token.unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use google_cloud_storage::stub::StorageControl as _;

    fn req(parent: &str) -> ListObjectsRequest {
        ListObjectsRequest::new().set_parent(parent)
    }

    #[test]
    fn plaintext_detection_is_scheme_only_and_case_insensitive() {
        assert!(is_plaintext_endpoint("http://localhost:4443"));
        assert!(is_plaintext_endpoint("HTTP://emulator"));
        assert!(!is_plaintext_endpoint("https://storage.googleapis.com"));
        assert!(!is_plaintext_endpoint("localhost:4443"));
        assert!(!is_plaintext_endpoint("http"));
    }

    #[test]
    fn bucket_is_parsed_from_the_resource_path() {
        assert_eq!(
            bucket_from_parent("projects/_/buckets/data").unwrap(),
            "data"
        );
        for bad in [
            "",
            "projects/_/buckets/",
            "buckets/data",
            "projects/_/buckets/a/b",
        ] {
            let err = bucket_from_parent(bad).unwrap_err();
            assert!(err.is_binding(), "{bad}: {err}");
        }
    }

    #[test]
    fn query_carries_every_set_field_encoded() {
        let r = req("projects/_/buckets/b")
            .set_prefix("raw data/")
            .set_delimiter("/")
            .set_include_trailing_delimiter(true)
            .set_versions(true)
            .set_page_size(50)
            .set_page_token("tok&1");
        assert_eq!(
            list_query(&r),
            "prefix=raw%20data%2F&delimiter=%2F&includeTrailingDelimiter=true&versions=true&maxResults=50&pageToken=tok%261"
        );
        assert_eq!(list_query(&req("projects/_/buckets/b")), "");
    }

    #[test]
    fn url_joins_endpoint_bucket_and_query() {
        let l = JsonApiControl::new("http://h:1/");
        assert_eq!(
            l.list_url(&req("projects/_/buckets/my bucket").set_prefix("p/"))
                .unwrap(),
            "http://h:1/storage/v1/b/my%20bucket/o?prefix=p%2F"
        );
        assert_eq!(
            l.list_url(&req("projects/_/buckets/b")).unwrap(),
            "http://h:1/storage/v1/b/b/o"
        );
        assert!(l.list_url(&req("nope")).is_err());
    }

    #[test]
    fn page_maps_objects_prefixes_and_token() {
        let page: JsonListPage = serde_json::from_value(serde_json::json!({
            "items": [
                {"name": "a.jsonl", "size": "12", "contentType": "application/x-ndjson"},
                {"name": "b.csv", "size": "not-a-number"}
            ],
            "prefixes": ["raw/"],
            "nextPageToken": "next"
        }))
        .unwrap();
        let resp = page.into_response("bkt");
        assert_eq!(resp.objects.len(), 2);
        assert_eq!(resp.objects[0].name, "a.jsonl");
        assert_eq!(resp.objects[0].size, 12);
        assert_eq!(resp.objects[0].content_type, "application/x-ndjson");
        assert_eq!(resp.objects[0].bucket, "projects/_/buckets/bkt");
        assert_eq!(resp.objects[1].size, 0);
        assert_eq!(resp.prefixes, vec!["raw/".to_string()]);
        assert_eq!(resp.next_page_token, "next");

        let empty = JsonListPage::default().into_response("bkt");
        assert!(empty.objects.is_empty() && empty.next_page_token.is_empty());
    }

    #[tokio::test]
    async fn list_reports_a_bad_parent_without_a_request() {
        let err = JsonApiControl::new("http://127.0.0.1:9")
            .list_objects(req("bad"), RequestOptions::default())
            .await
            .unwrap_err();
        assert!(err.is_binding());
    }

    #[tokio::test]
    async fn list_reports_an_unreachable_endpoint_as_io() {
        let err = JsonApiControl::new("http://127.0.0.1:9")
            .list_objects(req("projects/_/buckets/b"), RequestOptions::default())
            .await
            .unwrap_err();
        assert!(err.is_io(), "{err}");
    }

    #[tokio::test]
    async fn client_pages_through_the_json_api() {
        use wiremock::matchers::{method, path, query_param, query_param_is_missing};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/storage/v1/b/bkt/o"))
            .and(query_param("prefix", "raw/"))
            .and(query_param_is_missing("pageToken"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "items": [{"name": "raw/a.jsonl", "size": "3"}],
                "nextPageToken": "p2"
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/storage/v1/b/bkt/o"))
            .and(query_param("pageToken", "p2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "items": [{"name": "raw/b.jsonl"}]
            })))
            .mount(&server)
            .await;

        let control =
            crate::build_storage_control(&crate::GcsCredentials::Anonymous, Some(&server.uri()))
                .await
                .unwrap();
        let mut items = control
            .list_objects()
            .set_parent("projects/_/buckets/bkt")
            .set_prefix("raw/")
            .by_item();
        let mut names = Vec::new();
        use google_cloud_gax::paginator::ItemPaginator as _;
        while let Some(o) = items.next().await {
            names.push(o.unwrap().name);
        }
        assert_eq!(names, vec!["raw/a.jsonl", "raw/b.jsonl"]);
    }

    #[tokio::test]
    async fn non_success_status_and_bad_body_surface_as_errors() {
        use wiremock::matchers::path;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(path("/storage/v1/b/missing/o"))
            .respond_with(ResponseTemplate::new(404).set_body_string("no such bucket"))
            .mount(&server)
            .await;
        Mock::given(path("/storage/v1/b/garbled/o"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{not json"))
            .mount(&server)
            .await;
        let listing = JsonApiControl::new(&server.uri());

        let err = listing
            .list_objects(req("projects/_/buckets/missing"), RequestOptions::default())
            .await
            .unwrap_err();
        assert_eq!(err.http_status_code(), Some(404));

        let err = listing
            .list_objects(req("projects/_/buckets/garbled"), RequestOptions::default())
            .await
            .unwrap_err();
        assert!(err.is_deserialization(), "{err}");
    }

    #[test]
    fn get_url_encodes_the_object_name() {
        let c = JsonApiControl::new("http://h:1");
        let r = GetObjectRequest::new()
            .set_bucket("projects/_/buckets/b")
            .set_object("dir/a b.parquet");
        assert_eq!(
            c.get_url(&r).unwrap(),
            "http://h:1/storage/v1/b/b/o/dir%2Fa%20b.parquet"
        );
        let missing = GetObjectRequest::new().set_bucket("projects/_/buckets/b");
        assert!(c.get_url(&missing).unwrap_err().is_binding());
        let bad = GetObjectRequest::new().set_bucket("b").set_object("x");
        assert!(c.get_url(&bad).unwrap_err().is_binding());
    }

    #[tokio::test]
    async fn get_object_returns_size_and_encoding() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/storage/v1/b/bkt/o/data%2Fx.parquet"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "name": "data/x.parquet", "size": "4096", "contentEncoding": "gzip"
            })))
            .mount(&server)
            .await;
        let control =
            crate::build_storage_control(&crate::GcsCredentials::Anonymous, Some(&server.uri()))
                .await
                .unwrap();
        let obj = control
            .get_object()
            .set_bucket("projects/_/buckets/bkt")
            .set_object("data/x.parquet")
            .send()
            .await
            .unwrap();
        assert_eq!(obj.name, "data/x.parquet");
        assert_eq!(obj.size, 4096);
        assert_eq!(obj.content_encoding, "gzip");
        assert_eq!(obj.bucket, "projects/_/buckets/bkt");
    }
}
