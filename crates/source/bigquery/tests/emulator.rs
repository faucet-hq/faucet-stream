//! The BigQuery source against `ghcr.io/goccy/bigquery-emulator` (#967): the
//! REST query path (`jobs.query` + `getQueryResults`) and the Storage Read API
//! (gRPC, Arrow) over the same seeded table. The emulator does not check
//! tokens; the OAuth exchange runs against a wiremock token endpoint.
#![cfg(feature = "arrow")]

use std::collections::HashMap;

use faucet_core::Source;
use faucet_source_bigquery::{BigQueryCredentials, BigQuerySource, BigQuerySourceConfig};
use futures::StreamExt;
use gcp_bigquery_client::client_builder::ClientBuilder;
use serde_json::{Value, json};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const IMAGE: (&str, &str) = ("ghcr.io/goccy/bigquery-emulator", "0.8.0");
const HTTP_PORT: u16 = 9050;
const GRPC_PORT: u16 = 9060;

const DATA: &str = r#"
projects:
- id: p
  datasets:
  - id: ds
    tables:
    - id: people
      columns:
      - name: id
        type: INTEGER
      - name: name
        type: STRING
      - name: score
        type: FLOAT
      data:
      - id: 1
        name: ada
        score: 1.5
      - id: 2
        name: grace
        score: 2.25
      - id: 3
        name: linus
        score: -4.0
"#;

struct Emulator {
    _container: ContainerAsync<GenericImage>,
    http: String,
    grpc: String,
}

async fn emulator() -> Emulator {
    let container = faucet_conformance::containers::start(|| {
        GenericImage::new(IMAGE.0, IMAGE.1)
            .with_exposed_port(HTTP_PORT.tcp())
            .with_exposed_port(GRPC_PORT.tcp())
            .with_wait_for(WaitFor::seconds(2))
            .with_copy_to("/data.yaml", DATA.as_bytes().to_vec())
            .with_cmd(["--project=p", "--data-from-yaml=/data.yaml"])
    })
    .await;
    let http = container.get_host_port_ipv4(HTTP_PORT).await.unwrap();
    let grpc = container.get_host_port_ipv4(GRPC_PORT).await.unwrap();
    Emulator {
        _container: container,
        http: format!("http://127.0.0.1:{http}"),
        grpc: format!("127.0.0.1:{grpc}"),
    }
}

async fn source(
    emu: &Emulator,
    oauth: &MockServer,
    config: BigQuerySourceConfig,
) -> (BigQuerySource, tempfile::NamedTempFile) {
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token": "t", "token_type": "bearer", "expires_in": 3600
        })))
        .mount(oauth)
        .await;
    let sa = json!({
        "type": "service_account",
        "project_id": "p",
        "private_key_id": "k",
        "private_key": include_str!("fixtures/bigquery_test_key.pem"),
        "client_email": "faucet@p.iam.gserviceaccount.com",
        "client_id": "c",
        "token_uri": format!("{}/token", oauth.uri()),
    });
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(file.path(), sa.to_string()).unwrap();
    let tokens =
        faucet_common_bigquery::token_provider(&BigQueryCredentials::ServiceAccountKeyPath {
            path: file.path().to_str().unwrap().into(),
        })
        .await
        .unwrap();
    let client = ClientBuilder::new()
        .with_v2_base_url(format!("{}/bigquery/v2", emu.http))
        .build_from_authenticator(tokens.clone())
        .await
        .unwrap();
    let mut config = config;
    config.api_host = Some(emu.http.clone());
    config.storage_host = Some(emu.grpc.clone());
    (
        BigQuerySource::from_parts_with_tokens(config, client, tokens),
        file,
    )
}

fn creds() -> BigQueryCredentials {
    BigQueryCredentials::ApplicationDefault
}

fn expected() -> Vec<Value> {
    vec![
        json!({"id": 1, "name": "ada", "score": 1.5}),
        json!({"id": 2, "name": "grace", "score": 2.25}),
        json!({"id": 3, "name": "linus", "score": -4.0}),
    ]
}

async fn rows(src: &BigQuerySource) -> Vec<Value> {
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = src.stream_pages(&ctx, 0);
    let mut out = Vec::new();
    while let Some(page) = pages.next().await {
        out.extend(page.expect("page").records);
    }
    out.sort_by_key(|r| r["id"].as_i64());
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn the_query_path_reads_the_seeded_table() {
    let emu = emulator().await;
    let oauth = MockServer::start().await;
    let config = BigQuerySourceConfig::new(
        "p",
        creds(),
        "SELECT id, name, score FROM ds.people ORDER BY id",
    )
    .with_max_results_per_page(2);
    let (src, _key) = source(&emu, &oauth, config).await;
    assert_eq!(rows(&src).await, expected());
}

#[tokio::test(flavor = "multi_thread")]
async fn the_storage_read_api_streams_rows_and_arrow_batches() {
    let emu = emulator().await;
    let oauth = MockServer::start().await;
    let config = BigQuerySourceConfig::new("p", creds(), "").with_read_api("ds.people");
    let (src, _key) = source(&emu, &oauth, config).await;
    assert!(src.supports_columnar());
    assert_eq!(rows(&src).await, expected());

    let ctx: HashMap<String, Value> = HashMap::new();
    let mut batches = src.stream_batches(&ctx, 0);
    let mut total = 0;
    let mut names = Vec::new();
    while let Some(batch) = batches.next().await {
        let batch = batch.expect("batch").batch;
        total += batch.num_rows();
        let schema = batch.schema();
        names = schema.fields().iter().map(|f| f.name().clone()).collect();
    }
    assert_eq!(total, 3);
    assert_eq!(names, ["id", "name", "score"]);
}
