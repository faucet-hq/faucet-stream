//! Object-store emulators for connector integration tests: MinIO (S3),
//! Azurite (Azure Blob) and fake-gcs-server (GCS), started through
//! [`containers::start`](crate::containers::start) with pinned images.
//!
//! Each helper returns the running container with the endpoint the host
//! reaches it on. Creating an S3 bucket or an Azure container needs a signed
//! request, so that stays with the test's own SDK; a GCS bucket needs none and
//! [`fake_gcs`] creates the ones it is given.

use testcontainers::core::{IntoContainerPort, Mount, WaitFor};
use testcontainers::{ContainerAsync, ContainerRequest, GenericImage, ImageExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::containers::{ReadyProbe, StartOptions, start, start_with};

/// MinIO image (Chainguard's maintained build; upstream images are no longer
/// pullable, #694).
pub const MINIO_IMAGE: (&str, &str) = ("cgr.dev/chainguard/minio", "latest");
/// MinIO root user.
pub const MINIO_ACCESS_KEY: &str = "minioadmin";
/// MinIO root password.
pub const MINIO_SECRET_KEY: &str = "minioadmin";
/// Region every MinIO client in the tests signs for.
pub const MINIO_REGION: &str = "us-east-1";
/// Port MinIO serves S3 on inside the container.
pub const MINIO_PORT: u16 = 9000;

/// Azurite image.
pub const AZURITE_IMAGE: (&str, &str) = ("mcr.microsoft.com/azure-storage/azurite", "3.35.0");
/// Azurite's well-known development account.
pub const AZURITE_ACCOUNT: &str = "devstoreaccount1";
/// Azurite's well-known development account key.
pub const AZURITE_KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
/// Port Azurite serves Blob on inside the container.
pub const AZURITE_BLOB_PORT: u16 = 10000;

/// fake-gcs-server image.
pub const FAKE_GCS_IMAGE: (&str, &str) = ("fsouza/fake-gcs-server", "1.56.1");
/// fake-gcs-server fork with XML API writes, pinned by digest (amd64 only).
pub const FAKE_GCS_XML_IMAGE: (&str, &str) = (
    "tustvold/fake-gcs-server",
    "latest@sha256:dcd3aeacc07c731f1336e90c2889be2af8626ae993ee1fe2c0ba042ebbeb5a04",
);
/// Port fake-gcs-server listens on inside the container.
pub const FAKE_GCS_PORT: u16 = 4443;

/// A running emulator and the base URL the host reaches it on.
pub struct Store {
    /// The container; dropping it stops the emulator.
    pub container: ContainerAsync<GenericImage>,
    /// `http://127.0.0.1:<port>` (Azurite: with `/<account>` appended).
    pub endpoint: String,
}

/// The MinIO request, in memory, for callers that add a network or a name.
pub fn minio_request() -> ContainerRequest<GenericImage> {
    GenericImage::new(MINIO_IMAGE.0, MINIO_IMAGE.1)
        .with_exposed_port(MINIO_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stderr("API:"))
        .with_mount(Mount::tmpfs_mount("/data"))
        .with_env_var("MINIO_ROOT_USER", MINIO_ACCESS_KEY)
        .with_env_var("MINIO_ROOT_PASSWORD", MINIO_SECRET_KEY)
        .with_cmd(["server", "/data"])
}

/// Start MinIO; `customize` adjusts the request (network, container name).
pub async fn minio_with(
    customize: impl Fn(ContainerRequest<GenericImage>) -> ContainerRequest<GenericImage>,
) -> Store {
    let container = start(|| customize(minio_request())).await;
    let port = container
        .get_host_port_ipv4(MINIO_PORT)
        .await
        .expect("minio port");
    Store {
        container,
        endpoint: format!("http://127.0.0.1:{port}"),
    }
}

/// Start MinIO.
pub async fn minio() -> Store {
    minio_with(|r| r).await
}

/// Start Azurite (Blob only, in memory). The endpoint names the account.
pub async fn azurite() -> Store {
    let container = start(|| {
        GenericImage::new(AZURITE_IMAGE.0, AZURITE_IMAGE.1)
            .with_exposed_port(AZURITE_BLOB_PORT.tcp())
            .with_wait_for(WaitFor::message_on_stdout(
                "Azurite Blob service successfully listens on",
            ))
            .with_cmd([
                "azurite-blob",
                "--blobHost",
                "0.0.0.0",
                "--inMemoryPersistence",
                "--skipApiVersionCheck",
                "--loose",
            ])
    })
    .await;
    let port = container
        .get_host_port_ipv4(AZURITE_BLOB_PORT)
        .await
        .expect("azurite port");
    Store {
        container,
        endpoint: format!("http://127.0.0.1:{port}/{AZURITE_ACCOUNT}"),
    }
}

/// Start fake-gcs-server (in memory, plain HTTP) and create `buckets`. It
/// serves the JSON API that the GCS connectors and OpenDAL use.
pub async fn fake_gcs(buckets: &[&str]) -> Store {
    fake_gcs_on(FAKE_GCS_IMAGE, buckets).await
}

/// Start the fake-gcs-server build that also serves the XML API writes
/// `object_store` sends (upstream answers them 400), and create `buckets`.
/// It is the image `object_store`'s own CI tests against.
pub async fn fake_gcs_xml(buckets: &[&str]) -> Store {
    fake_gcs_on(FAKE_GCS_XML_IMAGE, buckets).await
}

async fn fake_gcs_on(image: (&str, &str), buckets: &[&str]) -> Store {
    // Public host 127.0.0.1: the XML API answers path-style requests whose
    // Host matches it, on any port.
    let container = start_with(
        || {
            GenericImage::new(image.0, image.1)
                .with_exposed_port(FAKE_GCS_PORT.tcp())
                .with_wait_for(WaitFor::message_on_stderr("server started at"))
                .with_cmd(["-scheme=http", "-backend=memory", "-public-host=127.0.0.1"])
        },
        &StartOptions::default().ready(ReadyProbe::http(FAKE_GCS_PORT, "/storage/v1/b")),
    )
    .await;
    let port = container
        .get_host_port_ipv4(FAKE_GCS_PORT)
        .await
        .expect("fake-gcs port");
    for bucket in buckets {
        create_gcs_bucket(port, bucket)
            .await
            .unwrap_or_else(|e| panic!("create fake-gcs bucket {bucket}: {e}"));
    }
    Store {
        container,
        endpoint: format!("http://127.0.0.1:{port}"),
    }
}

/// A GCS service-account key that sends every request to `endpoint` without
/// OAuth, the form `object_store` accepts for an emulator.
pub fn fake_gcs_service_account_key(endpoint: &str) -> String {
    serde_json::json!({
        "private_key": "unused",
        "private_key_id": "unused",
        "client_email": "faucet-tests@example.invalid",
        "gcs_base_url": endpoint,
        "disable_oauth": true
    })
    .to_string()
}

async fn create_gcs_bucket(port: u16, bucket: &str) -> Result<(), String> {
    let body = serde_json::json!({ "name": bucket }).to_string();
    let request = format!(
        "POST /storage/v1/b HTTP/1.0\r\nHost: 127.0.0.1:{port}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .map_err(|e| e.to_string())?;
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|e| e.to_string())?;
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .map_err(|e| e.to_string())?;
    let head = String::from_utf8_lossy(&response);
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or_else(|| format!("no status line: {head}"))?;
    if (200..300).contains(&status) || status == 409 {
        Ok(())
    } else {
        Err(format!("status {status}: {head}"))
    }
}
