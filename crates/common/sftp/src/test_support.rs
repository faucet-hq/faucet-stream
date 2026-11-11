//! SFTP servers for the crate's tests.

use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

use crate::SftpConnectionConfig;

pub(crate) type Server = (ContainerAsync<GenericImage>, u16);

const IMAGE_TAG: &str =
    "alpine@sha256:43dd1c023cf1bae487127d4b54660b0aee6e94c188a6599be36fb89ba115f961";

/// The image's sshd configuration with one session per connection.
const ONE_SESSION_SSHD_CONFIG: &str = "Protocol 2
HostKey /etc/ssh/ssh_host_ed25519_key
HostKey /etc/ssh/ssh_host_rsa_key
UseDNS no
PermitRootLogin no
X11Forwarding no
AllowTcpForwarding no
Subsystem sftp internal-sftp
ForceCommand internal-sftp
ChrootDirectory %h
MaxSessions 1
";

/// A password made fresh for each test process.
fn password() -> &'static str {
    static PASSWORD: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    PASSWORD.get_or_init(|| {
        use std::hash::{BuildHasher, Hasher};
        let mut h = std::collections::hash_map::RandomState::new().build_hasher();
        h.write_u128(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos()),
        );
        format!("p{:016x}", h.finish())
    })
}

async fn start(one_session: bool) -> Option<Server> {
    let started = faucet_conformance::containers::start_or_skip(
        || {
            let image = GenericImage::new("atmoz/sftp", IMAGE_TAG)
                .with_exposed_port(22.tcp())
                .with_wait_for(WaitFor::message_on_stderr("Server listening on"))
                .with_cmd(vec![format!("faucet:{}:::data", password())]);
            if one_session {
                image.with_copy_to(
                    "/etc/ssh/sshd_config",
                    ONE_SESSION_SSHD_CONFIG.as_bytes().to_vec(),
                )
            } else {
                image
            }
        },
        &Default::default(),
    )
    .await;
    let Some(c) = started else {
        faucet_conformance::containers::backend_missing("sftp: the test backend is unavailable");
        return None;
    };
    let port = c.get_host_port_ipv4(22).await.expect("port");
    Some((c, port))
}

/// An SFTP server allowing the default ten sessions per connection.
pub(crate) async fn server() -> Option<Server> {
    start(false).await
}

/// An SFTP server allowing one session per connection.
pub(crate) async fn one_session_server() -> Option<Server> {
    start(true).await
}

pub(crate) fn config(port: u16) -> SftpConnectionConfig {
    SftpConnectionConfig::with_password("127.0.0.1", "faucet", password())
        .port(port)
        .known_hosts(crate::HostKeyPolicy::Insecure)
}

/// SSH connections the server holds open, read from inside its container.
pub(crate) async fn open_connections(c: &ContainerAsync<GenericImage>) -> usize {
    let mut out = c
        .exec(testcontainers::core::ExecCommand::new([
            "sh",
            "-c",
            "netstat -tn | grep ':22 ' | grep -c ESTABLISHED || true",
        ]))
        .await
        .expect("exec");
    let stdout = out.stdout_to_vec().await.expect("stdout");
    String::from_utf8_lossy(&stdout).trim().parse().unwrap_or(0)
}

/// Wait until the server holds exactly `n` connections.
pub(crate) async fn wait_for_connections(c: &ContainerAsync<GenericImage>, n: usize) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        let open = open_connections(c).await;
        if open == n {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the server still holds {open} connections, expected {n}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}
