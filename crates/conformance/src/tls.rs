//! A throwaway certificate set for TLS integration tests (#967).
//!
//! [`TlsFixture::generate`] makes, at test time, a CA, a server certificate
//! signed by it (SANs `localhost` and `127.0.0.1` by default), a client
//! certificate for mutual TLS, and an unrelated second CA a client can trust
//! to prove that verification really fails. Everything is written as PEM to a
//! temporary directory that is removed when the fixture drops, and
//! [`TlsFixture::mount`] copies the files into a container request.
//!
//! ```no_run
//! use faucet_conformance::tls::TlsFixture;
//! use testcontainers::GenericImage;
//!
//! let tls = TlsFixture::generate();
//! let request = tls.mount(GenericImage::new("redis", "7.4"), "/certs");
//! let ca_for_the_client = tls.ca_path();
//! # drop((request, ca_for_the_client));
//! ```
//!
//! Keys are ECDSA P-256 in PKCS#8, which every server image these tests use
//! (PostgreSQL, MySQL, Redis, MongoDB, NATS, Elasticsearch, Kafka) accepts.

pub mod images;

use std::path::{Path, PathBuf};

use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, date_time_ymd,
};
use testcontainers::CopyTargetOptions;
use testcontainers::{ContainerRequest, Image, ImageExt};

/// File names inside [`TlsFixture::dir`] and inside a mounted directory.
pub mod files {
    /// The CA that signed the server and client certificates.
    pub const CA: &str = "ca.pem";
    /// The server certificate.
    pub const SERVER_CERT: &str = "server.pem";
    /// The server private key (PKCS#8 PEM).
    pub const SERVER_KEY: &str = "server.key";
    /// Server key, server certificate and CA in one file (a PEM keystore).
    pub const SERVER_BUNDLE: &str = "server-bundle.pem";
    /// Server certificate followed by its key (`tlsCertificateKeyFile` style).
    pub const SERVER_CERT_KEY: &str = "server-cert-key.pem";
    /// The client certificate.
    pub const CLIENT_CERT: &str = "client.pem";
    /// The client private key (PKCS#8 PEM).
    pub const CLIENT_KEY: &str = "client.key";
    /// Client certificate followed by its key.
    pub const CLIENT_CERT_KEY: &str = "client-cert-key.pem";
    /// A CA that signed nothing here: trusting it must fail verification.
    pub const OTHER_CA: &str = "other-ca.pem";
}

/// A generated CA, server and client certificate set on disk.
pub struct TlsFixture {
    dir: tempfile::TempDir,
    ca_pem: String,
    server_cert_pem: String,
    server_key_pem: String,
    client_cert_pem: String,
    client_key_pem: String,
    other_ca_pem: String,
}

impl std::fmt::Debug for TlsFixture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsFixture")
            .field("dir", &self.dir.path())
            .finish_non_exhaustive()
    }
}

fn ca(name: &str) -> (String, Issuer<'static, KeyPair>) {
    let mut params = CertificateParams::new(Vec::<String>::new()).expect("no SANs");
    params.distinguished_name.push(DnType::CommonName, name);
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    params.not_before = date_time_ymd(2024, 1, 1);
    params.not_after = date_time_ymd(2099, 1, 1);
    let key = KeyPair::generate().expect("generate CA key");
    let cert = params.self_signed(&key).expect("self-sign CA");
    (cert.pem(), Issuer::new(params, key))
}

fn leaf(
    issuer: &Issuer<'static, KeyPair>,
    common_name: &str,
    sans: &[&str],
    purpose: ExtendedKeyUsagePurpose,
) -> (String, String) {
    let names: Vec<String> = sans.iter().map(|s| (*s).to_string()).collect();
    let mut params = CertificateParams::new(names).expect("valid SANs");
    params
        .distinguished_name
        .push(DnType::CommonName, common_name);
    params.is_ca = IsCa::ExplicitNoCa;
    params.use_authority_key_identifier_extension = true;
    params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyEncipherment,
    ];
    params.extended_key_usages = vec![purpose];
    params.not_before = date_time_ymd(2024, 1, 1);
    params.not_after = date_time_ymd(2099, 1, 1);
    let key = KeyPair::generate().expect("generate leaf key");
    let cert = params.signed_by(&key, issuer).expect("sign leaf");
    (cert.pem(), key.serialize_pem())
}

impl TlsFixture {
    /// A fresh set whose server certificate names `localhost` and `127.0.0.1`.
    pub fn generate() -> Self {
        Self::for_names(&["localhost", "127.0.0.1"])
    }

    /// A fresh set whose server certificate carries exactly `server_names` as
    /// subject alternative names (DNS names or IP literals). Leaving
    /// `127.0.0.1` out lets a test reach the same server under a name the
    /// certificate does not cover, to prove hostname verification.
    pub fn for_names(server_names: &[&str]) -> Self {
        let (ca_pem, issuer) = ca("faucet-test-ca");
        let (server_cert_pem, server_key_pem) = leaf(
            &issuer,
            server_names.first().copied().unwrap_or("localhost"),
            server_names,
            ExtendedKeyUsagePurpose::ServerAuth,
        );
        let (client_cert_pem, client_key_pem) = leaf(
            &issuer,
            "faucet-test-client",
            &["faucet-test-client"],
            ExtendedKeyUsagePurpose::ClientAuth,
        );
        let (other_ca_pem, _) = ca("faucet-unrelated-ca");
        let dir = tempfile::Builder::new()
            .prefix("faucet-tls-")
            .tempdir()
            .expect("create TLS fixture dir");
        let fixture = TlsFixture {
            dir,
            ca_pem,
            server_cert_pem,
            server_key_pem,
            client_cert_pem,
            client_key_pem,
            other_ca_pem,
        };
        for (name, body) in fixture.contents() {
            std::fs::write(fixture.dir.path().join(name), body).expect("write TLS fixture file");
        }
        fixture
    }

    fn contents(&self) -> Vec<(&'static str, String)> {
        vec![
            (files::CA, self.ca_pem.clone()),
            (files::SERVER_CERT, self.server_cert_pem.clone()),
            (files::SERVER_KEY, self.server_key_pem.clone()),
            (files::SERVER_BUNDLE, self.server_bundle_pem()),
            (
                files::SERVER_CERT_KEY,
                format!("{}{}", self.server_cert_pem, self.server_key_pem),
            ),
            (files::CLIENT_CERT, self.client_cert_pem.clone()),
            (files::CLIENT_KEY, self.client_key_pem.clone()),
            (
                files::CLIENT_CERT_KEY,
                format!("{}{}", self.client_cert_pem, self.client_key_pem),
            ),
            (files::OTHER_CA, self.other_ca_pem.clone()),
        ]
    }

    /// The directory holding every file named in [`files`].
    pub fn dir(&self) -> &Path {
        self.dir.path()
    }

    /// Path of one fixture file.
    pub fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    /// Path of the CA certificate.
    pub fn ca_path(&self) -> PathBuf {
        self.path(files::CA)
    }

    /// Path of the unrelated CA.
    pub fn other_ca_path(&self) -> PathBuf {
        self.path(files::OTHER_CA)
    }

    /// Path of the client certificate.
    pub fn client_cert_path(&self) -> PathBuf {
        self.path(files::CLIENT_CERT)
    }

    /// Path of the client key.
    pub fn client_key_path(&self) -> PathBuf {
        self.path(files::CLIENT_KEY)
    }

    /// The CA certificate as PEM.
    pub fn ca_pem(&self) -> &str {
        &self.ca_pem
    }

    /// The unrelated CA as PEM.
    pub fn other_ca_pem(&self) -> &str {
        &self.other_ca_pem
    }

    /// The server certificate as PEM.
    pub fn server_cert_pem(&self) -> &str {
        &self.server_cert_pem
    }

    /// The server key as PKCS#8 PEM.
    pub fn server_key_pem(&self) -> &str {
        &self.server_key_pem
    }

    /// The client certificate as PEM.
    pub fn client_cert_pem(&self) -> &str {
        &self.client_cert_pem
    }

    /// The client key as PKCS#8 PEM.
    pub fn client_key_pem(&self) -> &str {
        &self.client_key_pem
    }

    /// Server key, then server certificate and the CA: a PEM keystore.
    pub fn server_bundle_pem(&self) -> String {
        format!(
            "{}{}{}",
            self.server_key_pem, self.server_cert_pem, self.ca_pem
        )
    }

    /// Copy every fixture file into `container_dir` of the request, readable
    /// by any user (mode `0644`). A server that insists on a private key it
    /// owns with a stricter mode copies the key itself at start-up.
    pub fn mount<I: Image>(
        &self,
        request: impl Into<ContainerRequest<I>>,
        container_dir: &str,
    ) -> ContainerRequest<I> {
        let base = container_dir.trim_end_matches('/');
        self.contents()
            .into_iter()
            .fold(request.into(), |req, (name, body)| {
                req.with_copy_to(
                    CopyTargetOptions::new(format!("{base}/{name}")).with_mode(0o644),
                    body.into_bytes(),
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_every_file_and_removes_them_on_drop() {
        let tls = TlsFixture::generate();
        let dir = tls.dir().to_path_buf();
        for name in [
            files::CA,
            files::SERVER_CERT,
            files::SERVER_KEY,
            files::SERVER_BUNDLE,
            files::SERVER_CERT_KEY,
            files::CLIENT_CERT,
            files::CLIENT_KEY,
            files::CLIENT_CERT_KEY,
            files::OTHER_CA,
        ] {
            let body = std::fs::read_to_string(dir.join(name)).unwrap();
            assert!(body.contains("-----BEGIN"), "{name} is PEM");
        }
        assert!(tls.server_key_pem().contains("PRIVATE KEY"));
        assert!(tls.server_bundle_pem().starts_with(tls.server_key_pem()));
        assert_ne!(tls.ca_pem(), tls.other_ca_pem());
        assert_eq!(tls.ca_path(), dir.join(files::CA));
        assert_eq!(tls.other_ca_path(), dir.join(files::OTHER_CA));
        assert_eq!(tls.client_cert_path(), dir.join(files::CLIENT_CERT));
        assert_eq!(tls.client_key_path(), dir.join(files::CLIENT_KEY));
        assert!(tls.client_cert_pem().contains("CERTIFICATE"));
        assert!(tls.client_key_pem().contains("PRIVATE KEY"));
        assert!(tls.server_cert_pem().contains("CERTIFICATE"));
        assert!(format!("{tls:?}").contains("TlsFixture"));
        drop(tls);
        assert!(!dir.exists());
    }

    #[test]
    fn mount_copies_each_file_under_the_directory() {
        let tls = TlsFixture::for_names(&["localhost"]);
        let req = tls.mount(testcontainers::GenericImage::new("busybox", "1"), "/certs/");
        assert_eq!(req.copy_to_sources().count(), tls.contents().len());
    }
}
