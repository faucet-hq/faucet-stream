//! SFTP `private_key` auth (#967) against `atmoz/sftp` with the account's
//! password login unusable: an unencrypted ed25519 key and a passphrase-
//! protected one each read a file; a key the server does not trust, and the
//! encrypted key without its passphrase, are refused. Keys come from the
//! host's `ssh-keygen`.
#![cfg(not(target_os = "windows"))]

use faucet_common_sftp::{SftpAuth, SftpConnectionConfig};
use faucet_core::Source;
use faucet_source_sftp::{SftpFormat, SftpSource, SftpSourceConfig};
use serde_json::json;
use std::path::{Path, PathBuf};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::{GenericImage, ImageExt};

const USER: &str = "faucet";

fn keygen(dir: &Path, name: &str, passphrase: &str) -> (PathBuf, String) {
    let path = dir.join(name);
    let status = std::process::Command::new("ssh-keygen")
        .args(["-q", "-t", "ed25519", "-N", passphrase, "-C", name, "-f"])
        .arg(&path)
        .status()
        .expect("ssh-keygen");
    assert!(status.success());
    let public = std::fs::read_to_string(path.with_extension("pub")).unwrap();
    (path, public)
}

fn config(port: u16, path: &Path, passphrase: Option<&str>) -> SftpSourceConfig {
    let mut conn = SftpConnectionConfig::with_password("127.0.0.1", USER, "unused").port(port);
    conn.auth = SftpAuth::PrivateKey {
        path: path.display().to_string(),
        passphrase: passphrase.map(str::to_string),
    };
    SftpSourceConfig::new(conn, "/data").format(SftpFormat::Jsonl)
}

#[tokio::test(flavor = "multi_thread")]
async fn private_keys_authenticate_and_untrusted_keys_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let (plain, plain_pub) = keygen(dir.path(), "plain", "");
    let (locked, locked_pub) = keygen(dir.path(), "locked", "open sesame");
    let (stranger, _) = keygen(dir.path(), "stranger", "");

    let container = faucet_conformance::containers::start(|| {
        GenericImage::new(
            "atmoz/sftp",
            "alpine@sha256:43dd1c023cf1bae487127d4b54660b0aee6e94c188a6599be36fb89ba115f961",
        )
        .with_exposed_port(22.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Server listening on"))
        // No password: only the keys under `.ssh/keys` can log in.
        .with_cmd(vec![format!("{USER}::::data")])
        .with_copy_to(
            format!("/home/{USER}/.ssh/keys/plain.pub"),
            plain_pub.clone().into_bytes(),
        )
        .with_copy_to(
            format!("/home/{USER}/.ssh/keys/locked.pub"),
            locked_pub.clone().into_bytes(),
        )
        .with_copy_to(
            format!("/home/{USER}/data/rows.jsonl"),
            b"{\"id\":1}\n{\"id\":2}\n".to_vec(),
        )
    })
    .await;
    let port = container.get_host_port_ipv4(22).await.unwrap();

    for (key, passphrase) in [(&plain, None), (&locked, Some("open sesame"))] {
        let source = SftpSource::new(config(port, key, passphrase)).unwrap();
        let records = source
            .fetch_all()
            .await
            .unwrap_or_else(|e| panic!("{}: {e}", key.display()));
        assert_eq!(records, vec![json!({"id": 1}), json!({"id": 2})]);
    }

    let refused = SftpSource::new(config(port, &stranger, None))
        .unwrap()
        .fetch_all()
        .await;
    assert!(refused.is_err(), "an untrusted key must be refused");
    let err = SftpSource::new(config(port, &locked, None))
        .unwrap()
        .fetch_all()
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("failed to load SFTP private key"), "{err}");
}
