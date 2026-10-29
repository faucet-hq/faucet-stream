#![cfg_attr(docsrs, feature(doc_cfg))]
//! Shared Redis pieces for the faucet-stream Redis source
//! (`faucet-source-redis`), sink (`faucet-sink-redis`) and state store
//! (`faucet-state-redis`): the `tls:` block ([`RedisTls`]) and the one
//! function that turns a URL plus that block into a [`redis::Client`].
//!
//! ```
//! use faucet_common_redis::{RedisTls, build_client};
//!
//! let client = build_client("redis://127.0.0.1:6379", &RedisTls::default()).unwrap();
//! # drop(client);
//! ```

use faucet_core::{FaucetError, LoadedTls};
use redis::{ClientTlsConfig, ConnectionAddr, IntoConnectionInfo, TlsCertificates};

/// The `tls:` block of the Redis source, sink and state store: a private CA
/// (`ca_cert_path` / `ca_cert_pem`, used in place of the bundled web PKI
/// roots), a client certificate for mutual TLS, and `insecure_skip_verify`.
/// Applies to `rediss://` URLs only.
pub type RedisTls = faucet_core::TlsOptions;

/// Build a client for `url` with `tls` applied.
///
/// Every failure is a [`FaucetError::Config`]: an invalid URL, `tls`
/// settings on a non-`rediss://` URL, or an unreadable certificate file.
/// Opens no connection.
pub fn build_client(url: &str, tls: &RedisTls) -> Result<redis::Client, FaucetError> {
    faucet_core::ensure_crypto_provider();
    let info = url
        .into_connection_info()
        .map_err(|e| FaucetError::Config(format!("invalid Redis URL: {e}")))?;
    let url_insecure = matches!(info.addr(), ConnectionAddr::TcpTls { insecure: true, .. });
    if url_insecure && !tls.insecure_skip_verify {
        tracing::warn!("redis: the URL's #insecure fragment disables certificate verification");
    }
    if !tls.is_set() {
        return redis::Client::open(info)
            .map_err(|e| FaucetError::Config(format!("invalid Redis URL: {e}")));
    }
    if !matches!(info.addr(), ConnectionAddr::TcpTls { .. }) {
        return Err(FaucetError::Config(
            "redis: `tls` settings need a `rediss://` URL".into(),
        ));
    }
    let loaded = tls.load("redis")?;
    let info = if loaded.insecure_skip_verify {
        match info.addr().clone() {
            ConnectionAddr::TcpTls {
                host,
                port,
                tls_params,
                ..
            } => info.set_addr(ConnectionAddr::TcpTls {
                host,
                port,
                insecure: true,
                tls_params,
            }),
            _ => info,
        }
    } else {
        info
    };
    match certificates(&loaded) {
        Some(certs) => redis::Client::build_with_tls(info, certs)
            .map_err(|e| FaucetError::Config(format!("redis: invalid `tls` settings: {e}"))),
        None => redis::Client::open(info)
            .map_err(|e| FaucetError::Config(format!("invalid Redis URL: {e}"))),
    }
}

fn certificates(loaded: &LoadedTls) -> Option<TlsCertificates> {
    let root_cert = if loaded.insecure_skip_verify {
        None
    } else {
        loaded.ca_pem.clone()
    };
    let client_tls = loaded.client.as_ref().map(|c| ClientTlsConfig {
        client_cert: c.cert.clone(),
        client_key: c.key.clone(),
    });
    if root_cert.is_none() && client_tls.is_none() {
        return None;
    }
    Some(TlsCertificates {
        client_tls,
        root_cert,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_conformance::tls::TlsFixture;

    fn insecure(client: &redis::Client) -> bool {
        matches!(
            client.get_connection_info().addr(),
            ConnectionAddr::TcpTls { insecure: true, .. }
        )
    }

    #[test]
    fn plain_urls_build_without_tls() {
        let c = build_client("redis://127.0.0.1:6379", &RedisTls::default()).unwrap();
        assert!(matches!(
            c.get_connection_info().addr(),
            ConnectionAddr::Tcp(..)
        ));
        assert!(insecure(
            &build_client("rediss://h:1/#insecure", &RedisTls::default()).unwrap()
        ));
    }

    #[test]
    fn invalid_url_is_a_config_error() {
        match build_client("not a url", &RedisTls::default()) {
            Err(FaucetError::Config(m)) => assert!(m.contains("invalid Redis URL"), "{m}"),
            other => panic!("{:?}", other.map(|_| ())),
        }
    }

    #[test]
    fn tls_settings_need_rediss() {
        let tls = RedisTls {
            insecure_skip_verify: true,
            ..Default::default()
        };
        match build_client("redis://127.0.0.1:6379", &tls) {
            Err(FaucetError::Config(m)) => assert!(m.contains("rediss://"), "{m}"),
            other => panic!("{:?}", other.map(|_| ())),
        }
    }

    #[test]
    fn insecure_skip_verify_marks_the_address() {
        let tls = RedisTls {
            insecure_skip_verify: true,
            ..Default::default()
        };
        assert!(insecure(
            &build_client("rediss://localhost:6380", &tls).unwrap()
        ));
    }

    #[test]
    fn ca_and_client_certificates_are_applied() {
        let fx = TlsFixture::generate();
        let tls = RedisTls {
            ca_cert_path: Some(fx.ca_path()),
            client_cert_path: Some(fx.client_cert_path()),
            client_key_path: Some(fx.client_key_path()),
            ..Default::default()
        };
        let c = build_client("rediss://localhost:6380", &tls).unwrap();
        assert!(!insecure(&c));
        let inline = RedisTls {
            ca_cert_pem: Some(fx.ca_pem().to_string()),
            ..Default::default()
        };
        assert!(build_client("rediss://localhost:6380", &inline).is_ok());
    }

    #[test]
    fn unreadable_or_bad_material_is_a_config_error() {
        let missing = RedisTls {
            ca_cert_path: Some("/nonexistent/ca.pem".into()),
            ..Default::default()
        };
        assert!(matches!(
            build_client("rediss://localhost:1", &missing),
            Err(FaucetError::Config(_))
        ));
        let dir = tempfile::tempdir().unwrap();
        let bad = dir.path().join("bad.pem");
        std::fs::write(
            &bad,
            "-----BEGIN CERTIFICATE-----\n!!\n-----END CERTIFICATE-----\n",
        )
        .unwrap();
        let tls = RedisTls {
            client_cert_path: Some(bad.clone()),
            client_key_path: Some(bad),
            ..Default::default()
        };
        match build_client("rediss://localhost:1", &tls) {
            Err(FaucetError::Config(m)) => assert!(m.contains("invalid `tls`"), "{m}"),
            other => panic!("{:?}", other.map(|_| ())),
        }
    }

    #[test]
    fn insecure_ignores_the_ca() {
        let loaded = LoadedTls {
            ca_pem: Some(b"ca".to_vec()),
            client: None,
            insecure_skip_verify: true,
        };
        assert!(certificates(&loaded).is_none());
    }
}
