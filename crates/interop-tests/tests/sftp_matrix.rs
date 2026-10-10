//! The shared format × option matrix (#777) against an SFTP server, through
//! the SFTP sink. A covering subset by default (every sink instance opens its
//! own SSH session); `FAUCET_MATRIX_FULL=1` runs every combination. Requires
//! Docker.
#![cfg(all(
    not(target_os = "windows"),
    feature = "file-formats",
    feature = "arrow",
    feature = "compression",
    feature = "encryption"
))]

mod remote_matrix;

use faucet_common_sftp::{SftpConnectionConfig, SftpSession, connect};
use faucet_core::{FaucetError, Sink};
use remote_matrix::{BoxFut, Remote, run_matrix_with};
use serde_json::{Value, json};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::io::AsyncReadExt;

const USER: &str = "faucet";
const PASS: &str = "secret";

struct SftpRemote {
    port: u16,
    _c: ContainerAsync<GenericImage>,
}

impl SftpRemote {
    async fn walk(
        &self,
        session: &SftpSession,
        dir: &str,
        rel: &str,
        out: &mut Vec<(String, Vec<u8>)>,
    ) {
        let Ok(entries) = session.read_dir(dir).await else {
            return;
        };
        for e in entries {
            let name = e.file_name();
            if name == "." || name == ".." {
                continue;
            }
            let path = format!("{dir}/{name}");
            let rel = format!("{rel}{name}");
            if e.file_type().is_dir() {
                Box::pin(self.walk(session, &path, &format!("{rel}/"), out)).await;
            } else {
                let mut f = session.open(path).await.expect("open");
                let mut body = Vec::new();
                f.read_to_end(&mut body).await.expect("read");
                out.push((rel, body));
            }
        }
    }
}

impl Remote for SftpRemote {
    fn config(&self, prefix: &str, mut fields: Value) -> Value {
        let obj = fields.as_object_mut().expect("object");
        if let Some(p) = obj.remove("path") {
            obj.insert("file_name".into(), p);
        }
        obj.insert("path".into(), format!("/data/{prefix}").into());
        obj.insert("host".into(), "127.0.0.1".into());
        obj.insert("port".into(), self.port.into());
        obj.insert("username".into(), USER.into());
        obj.insert("type".into(), "password".into());
        obj.insert("config".into(), json!({"password": PASS}));
        fields
    }

    fn sink(&self, cfg: Value) -> BoxFut<'_, Result<Box<dyn Sink>, FaucetError>> {
        Box::pin(async move {
            let cfg = serde_json::from_value(cfg).map_err(FaucetError::Json)?;
            Ok(Box::new(faucet_sink_sftp::SftpSink::new(cfg)?) as Box<dyn Sink>)
        })
    }

    fn objects(&self, prefix: &str) -> BoxFut<'_, Vec<(String, Vec<u8>)>> {
        let dir = format!("/data/{}", prefix.trim_end_matches('/'));
        Box::pin(async move {
            let conn = SftpConnectionConfig::with_password("127.0.0.1", USER, PASS).port(self.port);
            let session = connect(&conn).await.expect("verify session");
            let mut out = Vec::new();
            self.walk(&session, &dir, "", &mut out).await;
            out
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn every_writable_format_takes_every_option_on_sftp() {
    let Some(container) = faucet_conformance::containers::start_or_skip(
        || {
            GenericImage::new("atmoz/sftp", "alpine")
                .with_exposed_port(22.tcp())
                .with_wait_for(WaitFor::message_on_stderr("Server listening on"))
                .with_cmd(vec![format!("{USER}:{PASS}:::data")])
        },
        &Default::default(),
    )
    .await
    else {
        return;
    };
    let port = container.get_host_port_ipv4(22).await.expect("port");
    let full = std::env::var("FAUCET_MATRIX_FULL").is_ok();
    run_matrix_with(
        SftpRemote {
            port,
            _c: container,
        },
        full,
        3,
    )
    .await;
}
