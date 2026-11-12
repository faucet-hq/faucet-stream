//! The reference screen reads a MongoDB URI the way the driver does: for every
//! URI the driver accepts, the hosts, files, verification switches, identity
//! source and server pinning the screen sees are the driver's own.

use faucet_core::resolve::{ReferencePolicy, is_plain_host, literal_host, url_hosts};
use mongodb::options::{AuthMechanism, ConnectionString, HostInfo, ServerAddress, Tls};

#[derive(Debug, Default)]
struct Reading {
    hosts: Vec<String>,
    paths: Vec<String>,
    unverified: bool,
    host_credentials: bool,
    pinned: bool,
}

fn driver(uri: &str) -> Option<Reading> {
    let cs = ConnectionString::parse(uri).ok()?;
    let mut r = Reading::default();
    let srv = matches!(cs.host_info, HostInfo::DnsRecord(_));
    match &cs.host_info {
        HostInfo::HostIdentifiers(addrs) => {
            for a in addrs {
                match a {
                    ServerAddress::Tcp { host, .. } => r.hosts.push(match host.parse() {
                        Ok(std::net::IpAddr::V6(v6)) => format!("[{v6}]"),
                        _ => literal_host(host),
                    }),
                    ServerAddress::Unix { path } => r.paths.push(path.display().to_string()),
                    _ => {}
                }
            }
        }
        HostInfo::DnsRecord(name) => r.hosts.push(literal_host(name)),
        _ => {}
    }
    if let Some(Tls::Enabled(t)) = &cs.tls {
        r.paths
            .extend(t.ca_file_path.iter().map(|p| p.display().to_string()));
        r.paths
            .extend(t.cert_key_file_path.iter().map(|p| p.display().to_string()));
        r.unverified = t.allow_invalid_certificates == Some(true);
    }
    if let Some(c) = &cs.credential {
        let environment = c
            .mechanism_properties
            .as_ref()
            .is_some_and(|p| p.contains_key("ENVIRONMENT"));
        r.host_credentials = matches!(c.mechanism, Some(AuthMechanism::MongoDbOidc)) && environment;
    }
    r.pinned = cs.load_balanced == Some(true) || (!srv && cs.direct_connection == Some(true));
    Some(r)
}

const ROOT: &str = "/srv/data";

fn roots() -> ReferencePolicy {
    ReferencePolicy::deny_all()
        .with_path_roots([ROOT])
        .any_url_scheme()
        .any_url_host()
        .with_unverified(true)
        .with_host_credentials(true)
}

const CORPUS: &[&str] = &[
    "mongodb://db.example.com/app",
    "mongodb://u:p@db.example.com:27017/app?authSource=admin",
    "mongodb://u%40x:p@db.example.com/app",
    "mongodb://db.example.com/?tlsCAFile=/etc/ssl/ca.pem",
    "mongodb://db.example.com/?TLSCAFILE=%2Fsrv%2Fdata%2Fca.pem",
    "mongodb://db.example.com/?tlsCertificate\u{212A}eyFile=/etc/passwd",
    "mongodb://db.example.com/?tlsCAFile=/srv/data/a+b.pem",
    "mongodb://db.example.com/?tls=true&tlsCertificateKeyFile=%2Fsrv%2Fdata%2Fc.pem",
    "mongodb://ok.example.com/?appName=a,b://c&tlsCertificateKeyFile=/etc/passwd",
    "mongodb://ok.example.com#,other.example.com:27018/",
    "mongodb://ok.example.com;x,10.0.0.5/",
    "mongodb://%2Fsrv%2Fdata%2Fm.sock/",
    "mongodb://%2Ftmp%2Fm.sock,db.example.com/",
    "mongodb://evil.SOCK/",
    "mongodb://[::1]:27017,127.0.0.1,DB.Example.COM/",
    "mongodb+srv://cluster0.example.net/?authSource=admin",
    "mongodb+srv://cluster0.example.net/?loadBalanced=true",
    "mongodb://db.example.com/?tlsInsecure=true",
    "mongodb://db.example.com/?tlsAllowInvalidCertificates=true",
    "mongodb://db.example.com/?tlsInsecure=false",
    "mongodb://db.example.com/?authMechanism=MONGODB-OIDC&authMechanismProperties=ENVIRONMENT:k8s",
    "mongodb://db.example.com/?authMechanism=MONGODB-OIDC\
     &authMechanismProperties=ENVIRONMENT:azure,TOKEN_RESOURCE:api%3A%2F%2Fx",
    "mongodb://u@db.example.com/?authMechanism=SCRAM-SHA-256",
    "mongodb://db.example.com/?directConnection=true",
    "mongodb://db.example.com/?directConnection=false",
    "mongodb://a.example.com,b.example.com/?replicaSet=rs0",
    "mongodb://a.example.com/?replicaSet=rs0&directConnection=true",
    "mongodb://lb.example.com/?loadBalanced=true",
    "mongodb://7\r\rcccc6\u{19D}&.7",
    "mongodb://a\u{AD}b.example.com,B\u{212A}.example.com/?directConnection=true",
];

fn check(uri: &str, policy: &ReferencePolicy, refused: bool) {
    assert_eq!(
        policy.check_url(uri).is_err(),
        refused,
        "{uri}: {:?}",
        policy.check_url(uri)
    );
}

#[test]
fn screened_mongodb_options_match_the_driver_reading() {
    for uri in CORPUS {
        let d = driver(uri).unwrap_or_else(|| panic!("the driver refuses {uri}"));
        let mut screened = url_hosts(uri);
        let mut hosts = d.hosts.clone();
        screened.sort();
        hosts.sort();
        let odd = hosts.iter().chain(&screened).any(|h| !is_plain_host(h));
        if !odd {
            assert_eq!(screened, hosts, "{uri}");
        }
        let outside = d.paths.iter().any(|p| !p.starts_with(&format!("{ROOT}/")));
        check(uri, &roots(), outside || odd);
        check(
            uri,
            &ReferencePolicy::open().with_unverified(false),
            d.unverified,
        );
        check(
            uri,
            &ReferencePolicy::open().with_host_credentials(false),
            d.host_credentials,
        );
        check(
            uri,
            &ReferencePolicy::open().with_pinned_servers(true),
            !d.pinned,
        );
    }
}

#[test]
fn uris_the_driver_refuses_are_harmless_to_accept() {
    for uri in [
        "mongodb://db.example.com/?authMechanism=MONGODB-AWS",
        "mongodb://db.example.com/?authMechanism=GSSAPI",
        "mongodb://db.example.com/?proxyHost=proxy.example.com",
        "mongodb://db.example.com/?tlsAllowInvalidHostnames=true",
        "mongodb+srv://cluster0.example.net/?directConnection=true",
        "mongodb://:27017/",
        "mongodb://u:p@x@db.example.com/app",
    ] {
        assert!(driver(uri).is_none(), "the driver accepts {uri}");
    }
}

/// A URI the driver's own parser cannot read (it panics on a `%` followed by
/// a non-ASCII character) is refused with an error before the driver sees it.
#[tokio::test]
async fn a_uri_the_driver_cannot_parse_is_an_error() {
    let config: faucet_source_mongodb::MongoSourceConfig =
        faucet_core::serde_json::from_value(faucet_core::serde_json::json!({
            "connection_uri": "mongodb://u%a\u{E9}@db.example.com/",
            "database": "app",
            "collection": "c"
        }))
        .unwrap();
    let err = faucet_source_mongodb::MongoSource::new(config)
        .await
        .err()
        .expect("refused");
    assert!(err.to_string().contains("hexadecimal"), "{err}");
}
