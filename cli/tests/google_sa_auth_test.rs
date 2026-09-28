//! A `google_service_account` catalog provider used by a REST source through
//! `auth: { ref }`, via the real CLI run path (#755).
#![cfg(feature = "google-sa")]

use wiremock::matchers::{body_string_contains, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const KEY: &str = include_str!("../../crates/auth/tests/fixtures/google_sa_test_key.pem");

#[tokio::test]
async fn rest_source_authenticates_with_a_service_account_ref() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .and(body_string_contains(
            "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": "ya29.sa",
            "expires_in": 3600
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1beta/properties/1:runReport"))
        .and(header("authorization", "Bearer ya29.sa"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "rows": [{"date": "2024-06-01"}]
        })))
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let key_file = dir.path().join("sa.json");
    std::fs::write(
        &key_file,
        serde_json::json!({
            "type": "service_account",
            "client_email": "etl@proj.iam.gserviceaccount.com",
            "private_key": KEY,
            "private_key_id": "kid",
            "token_uri": format!("{}/token", server.uri())
        })
        .to_string(),
    )
    .unwrap();
    let out = dir.path().join("out.jsonl");
    let yaml = format!(
        r#"
version: 1
name: ga4
auth:
  google:
    type: google_service_account
    config:
      key_file: "{key}"
      scopes: ["https://www.googleapis.com/auth/analytics.readonly"]
pipeline:
  source:
    type: rest
    config:
      base_url: "{base}"
      path: "/v1beta/properties/1:runReport"
      method: POST
      body: {{ metrics: [] }}
      auth: {{ ref: google }}
      records_path: "$.rows[*]"
      pagination: {{ type: None }}
  sink:
    type: jsonl
    config:
      path: "{out}"
"#,
        key = key_file.display(),
        base = server.uri(),
        out = out.display(),
    );
    faucet_cli::run_from_yaml_str(&yaml)
        .await
        .expect("run succeeds");
    let written = std::fs::read_to_string(&out).unwrap();
    assert!(written.contains("2024-06-01"), "{written}");
}
