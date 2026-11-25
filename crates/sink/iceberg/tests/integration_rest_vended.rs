//! Integration test: `IcebergSink` against a REST catalog that vends storage
//! credentials (Lakekeeper with an STS-enabled warehouse on MinIO, #1110,
//! #1108). Requires Docker; `catalog-rest` is a default feature:
//!   cargo test -p faucet-sink-iceberg --test integration_rest_vended
//!
//! `catalog.properties` names no storage keys: every write is signed with the
//! credentials the catalog hands out with the table.

#![cfg(feature = "catalog-rest")]

use std::time::Duration;

use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::{Client, Config as S3Config};
use faucet_core::{FaucetError, Sink};
use faucet_sink_iceberg::{IcebergSink, IcebergSinkConfig};
use serde_json::{Value, json};
use testcontainers::core::wait::ExitWaitStrategy;
use testcontainers::core::{IntoContainerPort, Mount, WaitFor};
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

const MINIO_IMAGE: (&str, &str) = ("cgr.dev/chainguard/minio", "latest");
const POSTGRES_IMAGE: (&str, &str) = ("postgres", "17");
const CATALOG_IMAGE: (&str, &str) = ("quay.io/lakekeeper/catalog", "v0.10.2");
const ACCESS_KEY: &str = "minioadmin";
const SECRET_KEY: &str = "minioadmin";
const REGION: &str = "us-east-1";
const BUCKET: &str = "faucet-vended";
const WAREHOUSE: &str = "vended";

struct Stack {
    _minio: ContainerAsync<GenericImage>,
    _postgres: ContainerAsync<GenericImage>,
    _catalog: ContainerAsync<GenericImage>,
    s3_endpoint: String,
    catalog_uri: String,
}

fn catalog_image(cmd: &'static str) -> GenericImage {
    let image = GenericImage::new(CATALOG_IMAGE.0, CATALOG_IMAGE.1);
    match cmd {
        "migrate" => image.with_wait_for(WaitFor::exit(ExitWaitStrategy::new().with_exit_code(0))),
        _ => image.with_exposed_port(8181.tcp()),
    }
}

async fn start_stack() -> Stack {
    let suffix = std::process::id();
    let network = format!("faucet-iceberg-vended-{suffix}");
    let minio_name = format!("faucet-vended-minio-{suffix}");
    let pg_name = format!("faucet-vended-pg-{suffix}");

    let minio = faucet_conformance::containers::start(|| {
        GenericImage::new(MINIO_IMAGE.0, MINIO_IMAGE.1)
            .with_exposed_port(9000.tcp())
            .with_wait_for(WaitFor::message_on_stderr("API:"))
            .with_network(network.clone())
            .with_container_name(minio_name.clone())
            .with_mount(Mount::tmpfs_mount("/data"))
            .with_env_var("MINIO_ROOT_USER", ACCESS_KEY)
            .with_env_var("MINIO_ROOT_PASSWORD", SECRET_KEY)
            .with_cmd(["server", "/data"])
    })
    .await;
    let s3_port = minio.get_host_port_ipv4(9000).await.expect("minio port");
    let s3_endpoint = format!("http://127.0.0.1:{s3_port}");
    create_bucket(&s3_endpoint).await;

    let postgres = faucet_conformance::containers::start(|| {
        GenericImage::new(POSTGRES_IMAGE.0, POSTGRES_IMAGE.1)
            .with_wait_for(WaitFor::message_on_stderr(
                "database system is ready to accept connections",
            ))
            .with_network(network.clone())
            .with_container_name(pg_name.clone())
            .with_env_var("POSTGRES_PASSWORD", "pw")
            .with_env_var("POSTGRES_DB", "catalog")
    })
    .await;

    let db = format!("postgres://postgres:pw@{pg_name}:5432/catalog");
    let env = |image: GenericImage| {
        image
            .with_network(network.clone())
            .with_env_var("LAKEKEEPER__PG_DATABASE_URL_READ", db.clone())
            .with_env_var("LAKEKEEPER__PG_DATABASE_URL_WRITE", db.clone())
            .with_env_var(
                "LAKEKEEPER__PG_ENCRYPTION_KEY",
                "faucet-test-encryption-key",
            )
            .with_env_var("LAKEKEEPER__AUTHZ_BACKEND", "allowall")
    };
    let migrated = faucet_conformance::containers::start(|| {
        env(catalog_image("migrate")).with_cmd(["migrate"])
    })
    .await;
    drop(migrated);
    let catalog =
        faucet_conformance::containers::start(|| env(catalog_image("serve")).with_cmd(["serve"]))
            .await;
    let port = catalog
        .get_host_port_ipv4(8181)
        .await
        .expect("catalog port");
    let base = format!("http://127.0.0.1:{port}");
    bootstrap(&base, &minio_name).await;

    Stack {
        _minio: minio,
        _postgres: postgres,
        _catalog: catalog,
        s3_endpoint,
        catalog_uri: format!("{base}/catalog"),
    }
}

async fn post(
    client: &faucet_core::http::Client,
    url: &str,
    body: &Value,
) -> Result<(), FaucetError> {
    let body = body.to_string();
    client
        .execute(&faucet_core::CallContext::new(), "setup", || {
            client
                .post(url)
                .header("content-type", "application/json")
                .body(body.clone())
        })
        .await
        .map(|_| ())
}

async fn bootstrap(base: &str, minio_name: &str) {
    let client = faucet_core::http::Client::builder().build().unwrap();
    let wait = faucet_core::CallContext::new();
    let mut tries = 0;
    loop {
        match post(
            &client,
            &format!("{base}/management/v1/bootstrap"),
            &json!({"accept-terms-of-use": true}),
        )
        .await
        {
            Ok(()) => break,
            Err(_) if tries < 60 => {
                tries += 1;
                wait.wait(Duration::from_millis(500)).await.unwrap();
            }
            Err(e) => panic!("catalog bootstrap: {e}"),
        }
    }
    post(
        &client,
        &format!("{base}/management/v1/warehouse"),
        &json!({
            "warehouse-name": WAREHOUSE,
            "storage-profile": {
                "type": "s3",
                "bucket": BUCKET,
                "region": REGION,
                "sts-enabled": true,
                "flavor": "s3-compat",
                "endpoint": format!("http://{minio_name}:9000"),
                "path-style-access": true,
                "key-prefix": "wh"
            },
            "storage-credential": {
                "type": "s3",
                "credential-type": "access-key",
                "aws-access-key-id": ACCESS_KEY,
                "aws-secret-access-key": SECRET_KEY
            }
        }),
    )
    .await
    .expect("create warehouse");
}

async fn create_bucket(endpoint: &str) {
    let creds = Credentials::new(ACCESS_KEY, SECRET_KEY, None, None, "test");
    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .region(aws_config::Region::new(REGION))
        .endpoint_url(endpoint)
        .credentials_provider(creds)
        .load()
        .await;
    let s3_config = S3Config::from(&sdk_config)
        .to_builder()
        .force_path_style(true)
        .build();
    Client::from_conf(s3_config)
        .create_bucket()
        .bucket(BUCKET)
        .send()
        .await
        .expect("create bucket");
}

fn sink_config(stack: &Stack, table: &str, extra: Value) -> IcebergSinkConfig {
    let mut properties = json!({
        "s3.endpoint": stack.s3_endpoint,
        "s3.region": REGION,
        "s3.path-style-access": "true",
        "s3.disable-config-load": "true",
        "s3.disable-ec2-metadata": "true"
    });
    for (k, v) in extra.as_object().unwrap() {
        properties[k] = v.clone();
    }
    serde_json::from_value(json!({
        "catalog": {
            "type": "rest",
            "uri": stack.catalog_uri,
            "warehouse": WAREHOUSE,
            "access_delegation": "vended_credentials",
            "properties": properties
        },
        "namespace": ["vended"],
        "table": table
    }))
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_vending_catalog_commits_every_flush_and_refuses_static_keys_with_its_token() {
    let stack = start_stack().await;

    let sink = IcebergSink::new(sink_config(&stack, "events", json!({})))
        .await
        .expect("IcebergSink::new");
    for page in 0..3u64 {
        let records: Vec<Value> = (0..5)
            .map(|i| json!({"id": page * 5 + i, "page": page}))
            .collect();
        assert_eq!(sink.write_batch(&records).await.expect("write_batch"), 5);
        sink.flush()
            .await
            .unwrap_or_else(|e| panic!("flush {page}: {e}"));
    }

    let mixed = IcebergSink::new(sink_config(
        &stack,
        "events",
        json!({"s3.access-key-id": ACCESS_KEY, "s3.secret-access-key": SECRET_KEY}),
    ))
    .await
    .expect("IcebergSink::new");
    let err = mixed
        .write_batch(&[json!({"id": 99, "page": 9})])
        .await
        .expect_err("static keys with the vended session token are refused");
    assert!(err.to_string().contains("Remove the static keys"), "{err}");
}
