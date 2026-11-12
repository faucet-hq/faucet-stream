#![no_main]

use faucet_core::resolve::{ReferencePolicy, literal_host, mongodb_uri_problem, url_hosts};
use libfuzzer_sys::fuzz_target;
use mongodb::options::{AuthMechanism, ConnectionString, HostInfo, ServerAddress, Tls};

// Whether a host as the driver dials it holds characters no host name can.
fn odd(host: &str) -> bool {
    !host.contains("${")
        && !host.chars().all(|c| {
            c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_' | '[' | ']' | ':' | '*')
        })
}

// The screen and the MongoDB driver must read a URI alike, and the screen
// refuses a host the driver would dial with characters no host name holds.
fuzz_target!(|s: &str| {
    let confined = ReferencePolicy::deny_all()
        .with_path_roots(["/srv/data"])
        .any_url_scheme()
        .any_url_host();
    let verdict = confined.check_url(s);
    // The driver's parser panics on a `%` followed by a non-ASCII character;
    // the screen and the connectors refuse those before it sees them.
    if mongodb_uri_problem(s).is_some() {
        let lower = s.to_ascii_lowercase();
        if lower.starts_with("mongodb://") || lower.starts_with("mongodb+srv://") {
            assert!(verdict.is_err(), "{s:?} accepted");
        }
        return;
    }
    let Ok(cs) = ConnectionString::parse(s) else {
        return;
    };
    let screened = url_hosts(s);
    let mut paths = Vec::new();
    match &cs.host_info {
        HostInfo::HostIdentifiers(addrs) => {
            for a in addrs {
                match a {
                    ServerAddress::Tcp { host, .. } => {
                        let host = match host.parse() {
                            Ok(std::net::IpAddr::V6(v6)) => format!("[{v6}]"),
                            _ => literal_host(host),
                        };
                        assert!(!odd(&host) || verdict.is_err(), "{s:?}: {host:?} accepted");
                        assert!(
                            host.is_empty() || screened.contains(&host),
                            "{s:?}: {host} not in {screened:?}"
                        );
                    }
                    ServerAddress::Unix { path } => paths.push(path.clone()),
                    _ => {}
                }
            }
        }
        HostInfo::DnsRecord(name) => {
            let name = literal_host(name);
            assert!(!odd(&name) || verdict.is_err(), "{s:?}: {name:?} accepted");
            assert!(
                name.is_empty() || screened.contains(&name),
                "{s:?}: {name} not in {screened:?}"
            );
        }
        _ => {}
    }
    let mut risky = false;
    if let Some(Tls::Enabled(t)) = &cs.tls {
        paths.extend(t.ca_file_path.iter().cloned());
        paths.extend(t.cert_key_file_path.iter().cloned());
        risky |= t.allow_invalid_certificates == Some(true);
    }
    if let Some(c) = &cs.credential {
        let environment = c
            .mechanism_properties
            .as_ref()
            .is_some_and(|p| p.contains_key("ENVIRONMENT"));
        risky |= matches!(c.mechanism, Some(AuthMechanism::MongoDbOidc)) && environment;
    }
    // An empty name, `:memory:` and `/dev/null` are names the path check lets through.
    risky |= paths
        .iter()
        .filter(|p| !matches!(p.to_str(), Some("" | ":memory:" | "/dev/null")))
        .any(|p| {
            !p.is_absolute() || !p.starts_with("/srv/data") || p.to_string_lossy().contains("..")
        });
    if risky {
        assert!(verdict.is_err(), "{s:?} accepted");
    }
});
