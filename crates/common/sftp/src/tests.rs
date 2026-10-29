
use super::*;

#[test]
fn default_port_is_22() {
    let json = r#"{
            "host": "example.com",
            "username": "user",
            "type": "password",
            "config": { "password": "secret" }
        }"#;
    let cfg: SftpConnectionConfig = serde_json::from_str(json).unwrap();
    assert_eq!(cfg.port, DEFAULT_PORT);
}

#[test]
fn default_host_key_policy_is_accept_new() {
    let json = r#"{
            "host": "example.com",
            "username": "user",
            "type": "password",
            "config": { "password": "secret" }
        }"#;
    let cfg: SftpConnectionConfig = serde_json::from_str(json).unwrap();
    assert!(matches!(cfg.known_hosts, HostKeyPolicy::AcceptNew));
}

#[test]
fn password_auth_round_trips() {
    let json = r#"{
            "host": "h",
            "port": 2222,
            "username": "u",
            "type": "password",
            "config": { "password": "p" }
        }"#;
    let cfg: SftpConnectionConfig = serde_json::from_str(json).unwrap();
    assert_eq!(cfg.port, 2222);
    match &cfg.auth {
        SftpAuth::Password { password } => assert_eq!(password, "p"),
        other => panic!("expected password auth, got {other:?}"),
    }
    // Serialize back and confirm the adjacently-tagged shape survives.
    let value = serde_json::to_value(&cfg).unwrap();
    assert_eq!(value["type"], "password");
    assert_eq!(value["config"]["password"], "p");
}

#[test]
fn private_key_auth_round_trips() {
    let json = r#"{
            "host": "h",
            "username": "u",
            "type": "private_key",
            "config": { "path": "/home/u/.ssh/id_ed25519" }
        }"#;
    let cfg: SftpConnectionConfig = serde_json::from_str(json).unwrap();
    match &cfg.auth {
        SftpAuth::PrivateKey { path, passphrase } => {
            assert_eq!(path, "/home/u/.ssh/id_ed25519");
            assert!(passphrase.is_none());
        }
        other => panic!("expected private-key auth, got {other:?}"),
    }
}

#[test]
fn strict_policy_round_trips_with_path() {
    let json = r#"{
            "host": "h",
            "username": "u",
            "type": "password",
            "config": { "password": "p" },
            "known_hosts": { "mode": "strict", "known_hosts_path": "/etc/known_hosts" }
        }"#;
    let cfg: SftpConnectionConfig = serde_json::from_str(json).unwrap();
    match &cfg.known_hosts {
        HostKeyPolicy::Strict { known_hosts_path } => {
            assert_eq!(known_hosts_path.as_deref(), Some("/etc/known_hosts"));
        }
        other => panic!("expected strict policy, got {other:?}"),
    }
}

#[test]
fn insecure_policy_round_trips() {
    let json = r#"{
            "host": "h",
            "username": "u",
            "type": "password",
            "config": { "password": "p" },
            "known_hosts": { "mode": "insecure" }
        }"#;
    let cfg: SftpConnectionConfig = serde_json::from_str(json).unwrap();
    assert!(matches!(cfg.known_hosts, HostKeyPolicy::Insecure));
}

#[test]
fn debug_redacts_password() {
    let cfg = SftpConnectionConfig::with_password("h", "u", "hunter2");
    let dbg = format!("{cfg:?}");
    assert!(!dbg.contains("hunter2"), "password leaked in Debug: {dbg}");
    assert!(dbg.contains("<redacted>"));
}

#[test]
fn debug_redacts_passphrase() {
    let auth = SftpAuth::PrivateKey {
        path: "/k".into(),
        passphrase: Some("topsecret".into()),
    };
    let dbg = format!("{auth:?}");
    assert!(!dbg.contains("topsecret"), "passphrase leaked: {dbg}");
    assert!(dbg.contains("/k"), "path should still be visible");
}

const ED25519_A: &str =
    "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAILozywbDgXaQa69Wbtm7wCUIQgWrpRikYZPGSeRm8ULm";
const ED25519_B: &str =
    "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIDOQ3AnmbLNmWYPOodPL6rtC++IrO7oB/wdCcz7TWAyL";
const ECDSA: &str = "ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBFGy7eFxcAApM1fBomZnvTZ4B3+AK5KUhUhMuAFVffBj5C0RPOb9PI2vnBw6MmnPZfn94Dwi/z+mjg50EI7rNpk=";

fn key(s: &str) -> russh::keys::PublicKey {
    russh::keys::PublicKey::from_openssh(s).unwrap()
}

#[test]
fn accept_new_decision_pins_any_recorded_key_regardless_of_algorithm() {
    let (a, b, ec) = (key(ED25519_A), key(ED25519_B), key(ECDSA));
    assert_eq!(accept_new_decision(&[], &a), AcceptNewDecision::Learn);
    assert_eq!(
        accept_new_decision(std::slice::from_ref(&a), &a),
        AcceptNewDecision::Known
    );
    assert_eq!(
        accept_new_decision(std::slice::from_ref(&a), &b),
        AcceptNewDecision::Changed
    );
    assert_eq!(
        accept_new_decision(std::slice::from_ref(&a), &ec),
        AcceptNewDecision::Changed,
        "a different algorithm must not be learned as a new key"
    );
    assert_eq!(
        accept_new_decision(&[a.clone(), ec.clone()], &ec),
        AcceptNewDecision::Known
    );
}

#[test]
fn accept_new_learns_once_then_rejects_a_key_of_another_algorithm() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ssh").join("known_hosts");
    let (a, ec) = (key(ED25519_A), key(ECDSA));

    assert!(accept_new_at("sftp.example", 2222, &a, &path).unwrap());
    let recorded = std::fs::read_to_string(&path).unwrap();
    assert!(
        recorded.contains("[sftp.example]:2222 ssh-ed25519 "),
        "{recorded}"
    );

    assert!(accept_new_at("sftp.example", 2222, &a, &path).unwrap());
    let err = accept_new_at("sftp.example", 2222, &ec, &path).unwrap_err();
    assert!(err.to_string().contains("changed"), "{err}");
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        recorded,
        "nothing learned"
    );

    assert!(accept_new_at("other.example", 22, &ec, &path).unwrap());
}

#[test]
fn accept_new_surfaces_an_unreadable_known_hosts_entry() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("known_hosts");
    std::fs::write(&path, "sftp.example ssh-ed25519 not-base64!!\n").unwrap();
    let err = accept_new_at("sftp.example", 22, &key(ED25519_A), &path).unwrap_err();
    assert!(
        err.to_string().contains("known_hosts lookup failed"),
        "{err}"
    );
}

#[test]
fn default_known_hosts_path_is_under_home() {
    assert!(default_known_hosts_path().is_none_or(|p| p.ends_with(".ssh/known_hosts")));
}

#[test]
fn accept_new_needs_a_known_hosts_file_and_reports_a_failed_write() {
    let err = accept_new_in("sftp.example", 22, &key(ED25519_A), None).unwrap_err();
    assert!(err.to_string().contains("no home directory"), "{err}");
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("known_hosts");
    assert!(accept_new_in("sftp.example", 22, &key(ED25519_A), Some(path.clone())).unwrap());
    assert!(path.is_file(), "the first key is learned");
}

#[cfg(unix)]
#[test]
fn a_known_hosts_file_that_cannot_be_written_is_an_error() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let locked = dir.path().join("locked");
    std::fs::create_dir(&locked).unwrap();
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o500)).unwrap();
    let result = accept_new_at(
        "sftp.example",
        22,
        &key(ED25519_A),
        &locked.join("known_hosts"),
    );
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o700)).unwrap();
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("failed to record new host key"),
        "{err}"
    );
}

fn body(s: &str) -> &str {
    s.split_whitespace().nth(1).unwrap()
}

#[test]
fn known_hosts_markers_and_wildcards_are_honoured() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("known_hosts");
    let (a, b) = (key(ED25519_A), key(ED25519_B));

    std::fs::write(
        &path,
        format!("@revoked sftp.example ssh-ed25519 {}\n", body(ED25519_A)),
    )
    .unwrap();
    let err = accept_new_at("sftp.example", 22, &a, &path).unwrap_err();
    assert!(err.to_string().contains("@revoked"), "{err}");

    std::fs::write(
        &path,
        format!(
            "# pinned\n*.example,!skip.example ssh-ed25519 {}\n",
            body(ED25519_B)
        ),
    )
    .unwrap();
    assert!(accept_new_at("sftp.example", 22, &b, &path).unwrap());
    let err = accept_new_at("sftp.example", 22, &a, &path).unwrap_err();
    assert!(err.to_string().contains("changed"), "{err}");
    assert!(
        accept_new_at("skip.example", 22, &a, &path).unwrap(),
        "negated → learned"
    );

    std::fs::write(
        &path,
        format!(
            "@cert-authority *.example ssh-ed25519 {}\n",
            body(ED25519_B)
        ),
    )
    .unwrap();
    let err = accept_new_at("sftp.example", 22, &a, &path).unwrap_err();
    assert!(err.to_string().contains("cert-authority"), "{err}");
    assert!(
        !std::fs::read_to_string(&path)
            .unwrap()
            .contains(body(ED25519_A))
    );

    assert!(hosts_match("[h?st]:2222", "[host]:2222"));
    assert!(!hosts_match("other", "host"));
    assert!(wildmatch("SFTP.*", "sftp.example"));
    assert!(!wildmatch("a?", "a"));
    let extras = scan_known_hosts("@revoked\nhost ssh-ed25519\n@bogus host t k\n", "host", 22);
    assert!(extras.revoked.is_empty() && !extras.cert_authority);
}

/// #862 L-160: a revocation written against a hashed host is enforced.
#[test]
fn hashed_host_entries_match_their_host() {
    use base64::Engine as _;
    use hmac::Mac as _;
    let b64 = base64::engine::general_purpose::STANDARD;
    let salt = b"0123456789abcdefghij";
    let hash = |name: &str| {
        let mac = hmac::Hmac::<sha1::Sha1>::new_from_slice(salt)
            .unwrap()
            .chain_update(name.as_bytes());
        format!(
            "|1|{}|{}",
            b64.encode(salt),
            b64.encode(mac.finalize().into_bytes())
        )
    };
    assert!(hosts_match(&hash("sftp.example"), "sftp.example"));
    assert!(!hosts_match(&hash("sftp.example"), "other.example"));
    assert!(hosts_match(&hash("[h]:2222"), "[h]:2222"));
    assert!(!hosts_match(&format!("*,!{}", hash("h")), "h"));
    assert!(!hosts_match("|1|nosep", "h"));
    assert!(!hosts_match("|1|%%%|%%%", "h"));
    let extras = scan_known_hosts(
        &format!(
            "@revoked {} ssh-ed25519 {}\n",
            hash("sftp.example"),
            body(ED25519_A)
        ),
        "sftp.example",
        22,
    );
    assert_eq!(extras.revoked.len(), 1);
}

#[tokio::test]
async fn strict_policy_rejects_revoked_and_accepts_wildcard_keys() {
    use russh::client::Handler as _;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("known_hosts");
    std::fs::write(
            &path,
            format!(
                "sftp.example ssh-ed25519 {a}\n@revoked sftp.example ssh-ed25519 {a}\n*.wild ssh-ed25519 {b}\n@cert-authority ca.host ssh-ed25519 {b}\n",
                a = body(ED25519_A),
                b = body(ED25519_B)
            ),
        )
        .unwrap();
    let handler = |host: &str| ClientHandler {
        policy: HostKeyPolicy::Strict {
            known_hosts_path: Some(path.to_string_lossy().into_owned()),
        },
        host: host.into(),
        port: 22,
    };
    let err = handler("sftp.example")
        .check_server_key(&key(ED25519_A).into())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("@revoked"), "{err}");
    assert!(
        handler("a.wild")
            .check_server_key(&key(ED25519_B).into())
            .await
            .unwrap()
    );
    let err = handler("ca.host")
        .check_server_key(&key(ED25519_A).into())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("cert-authority"), "{err}");
}

#[tokio::test]
async fn a_host_certificate_is_refused_even_under_the_insecure_policy() {
    use russh::client::Handler as _;
    let cert = russh::keys::Certificate::from_openssh("ssh-ed25519-cert-v01@openssh.com AAAAIHNzaC1lZDI1NTE5LWNlcnQtdjAxQG9wZW5zc2guY29tAAAAIPPHg8GFbDtPpUu2SCXhgTm/VGQz8sHi9G5P/PxHP8IrAAAAIBBZHy7KX9Js/UVJnZI6Qq/2LZYr3QmsTnIgymh23IoZAAAAAAAAAAAAAAACAAAAC2ZhdWNldC10ZXN0AAAAEAAAAAxzZnRwLmV4YW1wbGUAAAAAAAAAAP//////////AAAAAAAAAAAAAAAAAAAAMwAAAAtzc2gtZWQyNTUxOQAAACDhCJxYDNpmpUchKQ9llIrqqP/aedWCKvssa3Yf/7rhJAAAAFMAAAALc3NoLWVkMjU1MTkAAABAO3iEpOQePt/XhpKxtojofp5jkeHS86KUN3YMBJ7tI066KJ7ylkthqVXez6Q7zp2mjBxqHQHPwz1uX4neP9kYBg==").unwrap();
    let mut handler = ClientHandler {
        policy: HostKeyPolicy::Insecure,
        host: "sftp.example".into(),
        port: 22,
    };
    let err = handler.check_server_key(&cert.into()).await.unwrap_err();
    assert!(
        err.to_string()
            .contains("host certificates are not supported"),
        "{err}"
    );
}

#[tokio::test]
async fn a_server_that_never_answers_times_out() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let hold = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        drop(socket);
    });
    let mut cfg = SftpConnectionConfig::with_password("127.0.0.1", "u", "p").port(port);
    cfg.connect_timeout_secs = 1;
    let started = std::time::Instant::now();
    let err = connect(&cfg).await.err().expect("a timeout");
    assert!(err.to_string().contains("connect_timeout_secs"), "{err}");
    assert!(started.elapsed() < std::time::Duration::from_secs(10));
    hold.abort();
    let ssh = cfg.ssh_config();
    assert_eq!(
        ssh.keepalive_interval,
        Some(std::time::Duration::from_secs(15))
    );
    cfg.keepalive_interval_secs = 0;
    assert_eq!(cfg.ssh_config().keepalive_interval, None);
}

#[test]
fn config_schema_is_object() {
    let schema = serde_json::to_value(schemars::schema_for!(SftpConnectionConfig)).unwrap();
    assert!(schema.is_object());
}
