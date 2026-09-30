//! The shared format × option matrix (#777) against fake-gcs-server, through
//! the GCS sink. Requires Docker.
#![cfg(all(
    not(target_os = "windows"),
    feature = "file-formats",
    feature = "arrow",
    feature = "compression",
    feature = "encryption"
))]

mod remote_matrix;

use faucet_core::{FaucetError, Sink};
use remote_matrix::{BoxFut, Remote, run_matrix};
use serde_json::{Value, json};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};

const BUCKET: &str = "faucet-matrix";

struct GcsRemote {
    host: String,
    http: reqwest::Client,
    _c: ContainerAsync<GenericImage>,
}

impl Remote for GcsRemote {
    fn config(&self, prefix: &str, mut fields: Value) -> Value {
        fields["bucket"] = BUCKET.into();
        fields["prefix"] = prefix.into();
        fields["storage_host"] = self.host.clone().into();
        fields["auth"] = json!({"type": "anonymous"});
        fields
    }

    fn sink(&self, cfg: Value) -> BoxFut<'_, Result<Box<dyn Sink>, FaucetError>> {
        Box::pin(async move {
            let cfg = serde_json::from_value(cfg).map_err(FaucetError::Json)?;
            Ok(Box::new(faucet_sink_gcs::GcsSink::new(cfg).await?) as Box<dyn Sink>)
        })
    }

    fn objects(&self, prefix: &str) -> BoxFut<'_, Vec<(String, Vec<u8>)>> {
        let prefix = prefix.to_string();
        Box::pin(async move {
            let listed: Value = self
                .http
                .get(format!(
                    "{}/storage/v1/b/{BUCKET}/o?prefix={}",
                    self.host,
                    urlencoding::encode(&prefix)
                ))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            let mut out = Vec::new();
            for item in listed["items"].as_array().cloned().unwrap_or_default() {
                let name = item["name"].as_str().unwrap().to_string();
                let body = self
                    .http
                    .get(format!(
                        "{}/storage/v1/b/{BUCKET}/o/{}?alt=media",
                        self.host,
                        urlencoding::encode(&name)
                    ))
                    .send()
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap();
                out.push((name[prefix.len()..].to_string(), body.to_vec()));
            }
            out
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn every_writable_format_takes_every_option_on_gcs() {
    let image = GenericImage::new("fsouza/fake-gcs-server", "latest")
        .with_exposed_port(4443.tcp())
        .with_wait_for(WaitFor::message_on_stderr("server started at"))
        .with_cmd(vec![
            "-scheme=http".to_string(),
            "-public-host=0.0.0.0:4443".to_string(),
        ]);
    let container = match image.start().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping: Docker not available ({e})");
            return;
        }
    };
    let port = container.get_host_port_ipv4(4443).await.expect("port");
    let host = format!("http://127.0.0.1:{port}");
    let http = reqwest::Client::new();
    http.post(format!("{host}/storage/v1/b"))
        .json(&json!({"name": BUCKET}))
        .send()
        .await
        .expect("create bucket");
    run_matrix(
        GcsRemote {
            host,
            http,
            _c: container,
        },
        true,
    )
    .await;
}
