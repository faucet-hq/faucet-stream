//! Azure credential variants (#967). `connection_string` and `sas_token`
//! read from Azurite (the SAS is a real account SAS signed with the emulator
//! key, and a wrongly signed one is refused). `managed_identity` reads from a
//! wiremock fake of IMDS (`IDENTITY_ENDPOINT`) plus the Blob list and get
//! calls, which only answer the token IMDS issued.

use base64::Engine as _;
use faucet_core::Source;
use faucet_source_azure_blob::{AzureBlobSource, AzureBlobSourceConfig, AzureCredentials};
use hmac::{Hmac, Mac};
use object_store::azure::MicrosoftAzureBuilder;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStoreExt, PutPayload};
use serde_json::{Value, json};
use testcontainers_modules::azurite::{Azurite, BLOB_PORT};
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

const ACCOUNT: &str = "devstoreaccount1";
const KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
const CONTAINER: &str = "creds";
const ROWS: &[u8] = b"{\"id\":1}\n{\"id\":2}\n";

fn rows() -> Vec<Value> {
    vec![json!({"id": 1}), json!({"id": 2})]
}

async fn read(config: AzureBlobSourceConfig) -> Result<Vec<Value>, String> {
    let source = AzureBlobSource::new(config.prefix("data/"))
        .await
        .map_err(|e| e.to_string())?;
    source.fetch_all().await.map_err(|e| e.to_string())
}

/// An account SAS (`sv=2019-12-12`) for blob read + list, signed with `key`.
fn account_sas(key: &str) -> String {
    let (sp, ss, srt, se, spr, sv) = (
        "rl",
        "b",
        "sco",
        "2099-01-01T00:00:00Z",
        "https,http",
        "2019-12-12",
    );
    let to_sign = format!("{ACCOUNT}\n{sp}\n{ss}\n{srt}\n\n{se}\n\n{spr}\n{sv}\n");
    let b64 = base64::engine::general_purpose::STANDARD;
    let mut mac = Hmac::<sha2::Sha256>::new_from_slice(&b64.decode(key).unwrap()).unwrap();
    mac.update(to_sign.as_bytes());
    let sig = b64.encode(mac.finalize().into_bytes());
    let sig: String = sig
        .chars()
        .map(|c| match c {
            '+' => "%2B".to_string(),
            '/' => "%2F".to_string(),
            '=' => "%3D".to_string(),
            c => c.to_string(),
        })
        .collect();
    format!("sv={sv}&ss={ss}&srt={srt}&sp={sp}&se={se}&spr={spr}&sig={sig}")
}

#[tokio::test(flavor = "multi_thread")]
async fn connection_string_and_sas_token_read_from_azurite() {
    let container = faucet_conformance::containers::start(|| {
        testcontainers::ImageExt::with_cmd(
            Azurite::default(),
            [
                "azurite",
                "--blobHost",
                "0.0.0.0",
                "--queueHost",
                "0.0.0.0",
                "--tableHost",
                "0.0.0.0",
                "--inMemoryPersistence",
            ],
        )
    })
    .await;
    let port = container.get_host_port_ipv4(BLOB_PORT).await.unwrap();
    let endpoint = format!("http://127.0.0.1:{port}/{ACCOUNT}");
    faucet_common_azure::testing::create_container(&endpoint, ACCOUNT, KEY, CONTAINER)
        .await
        .unwrap();
    let seed = MicrosoftAzureBuilder::new()
        .with_account(ACCOUNT)
        .with_access_key(KEY)
        .with_container_name(CONTAINER)
        .with_endpoint(endpoint.clone())
        .with_allow_http(true)
        .build()
        .unwrap();
    seed.put(
        &ObjPath::from("data/rows.jsonl"),
        PutPayload::from(ROWS.to_vec()),
    )
    .await
    .unwrap();

    let connection_string = format!(
        "DefaultEndpointsProtocol=http;AccountName={ACCOUNT};AccountKey={KEY};BlobEndpoint={endpoint};"
    );
    let by_string = AzureBlobSourceConfig::new(CONTAINER)
        .auth(AzureCredentials::ConnectionString { connection_string });
    assert_eq!(read(by_string).await.unwrap(), rows());

    let sas = |sas_token: String| {
        AzureBlobSourceConfig::new(CONTAINER)
            .account(ACCOUNT)
            .auth(AzureCredentials::SasToken { sas_token })
            .endpoint(endpoint.clone())
            .allow_http(true)
    };
    assert_eq!(read(sas(account_sas(KEY))).await.unwrap(), rows());
    let other_key = base64::engine::general_purpose::STANDARD.encode([7u8; 64]);
    assert!(
        read(sas(account_sas(&other_key))).await.is_err(),
        "a forged SAS is refused"
    );
}

/// A Blob endpoint on `server` that lists and serves `data/rows.jsonl` only to
/// `Authorization: Bearer <token>`.
async fn fake_blob(server: &MockServer, token: &str) {
    let list = format!(
        "<?xml version=\"1.0\" encoding=\"utf-8\"?><EnumerationResults ContainerName=\"{CONTAINER}\">\
         <Prefix>data/</Prefix><Blobs><Blob><Name>data/rows.jsonl</Name><Properties>\
         <Last-Modified>Mon, 01 Jan 2024 00:00:00 GMT</Last-Modified><Etag>0x8D1</Etag>\
         <Content-Length>{}</Content-Length><Content-Type>application/x-ndjson</Content-Type>\
         <BlobType>BlockBlob</BlobType></Properties></Blob></Blobs><NextMarker /></EnumerationResults>",
        ROWS.len()
    );
    let bearer = format!("Bearer {token}");
    Mock::given(method("GET"))
        .and(path(format!("/{ACCOUNT}/{CONTAINER}")))
        .and(query_param("comp", "list"))
        .and(header("authorization", bearer.as_str()))
        .respond_with(ResponseTemplate::new(200).set_body_raw(list, "application/xml"))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/{ACCOUNT}/{CONTAINER}/data/rows.jsonl")))
        .and(header("authorization", bearer.as_str()))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Last-Modified", "Mon, 01 Jan 2024 00:00:00 GMT")
                .insert_header("ETag", "0x8D1")
                .insert_header("x-ms-blob-type", "BlockBlob")
                .set_body_bytes(ROWS.to_vec()),
        )
        .mount(server)
        .await;
}

fn fake_config(server: &MockServer, auth: AzureCredentials) -> AzureBlobSourceConfig {
    AzureBlobSourceConfig::new(CONTAINER)
        .account(ACCOUNT)
        .auth(auth)
        .endpoint(format!("{}/{ACCOUNT}", server.uri()))
        .allow_http(true)
}

#[tokio::test(flavor = "multi_thread")]
async fn managed_identity_sends_the_token_imds_issued() {
    let server = MockServer::start().await;
    let expires = (std::time::SystemTime::now() + std::time::Duration::from_secs(3600))
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    Mock::given(method("GET"))
        .and(path("/msi/token"))
        .and(query_param("client_id", "mi-1"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(
                json!({"access_token": "mi-token", "expires_on": expires.to_string()}),
            ),
        )
        .mount(&server)
        .await;
    fake_blob(&server, "mi-token").await;

    // SAFETY: the only test in this binary that touches the environment, and it
    // sets the variable before building any client.
    unsafe { std::env::set_var("IDENTITY_ENDPOINT", format!("{}/msi/token", server.uri())) };
    let mi = AzureCredentials::ManagedIdentity {
        client_id: Some("mi-1".into()),
    };
    assert_eq!(read(fake_config(&server, mi)).await.unwrap(), rows());
    let other = AzureCredentials::ManagedIdentity {
        client_id: Some("mi-2".into()),
    };
    assert!(
        read(fake_config(&server, other)).await.is_err(),
        "IMDS issues no token for mi-2"
    );
}
