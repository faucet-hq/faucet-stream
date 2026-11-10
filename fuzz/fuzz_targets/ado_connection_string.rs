#![no_main]

use faucet_core::ado;
use faucet_core::resolve::{ReferencePolicy, UrlSyntax, canonical_host, url_hosts_as};
use libfuzzer_sys::fuzz_target;

// The screen and the SQL Server driver must read a connection string alike:
// whatever the driver accepts, the screen parses, and the host the driver
// connects to is one the screen checks.
fuzz_target!(|s: &str| {
    let screened = ado::parse(s);
    let policy = ReferencePolicy::deny_all()
        .with_path_roots(["/srv/data"])
        .any_url_scheme()
        .any_url_host();
    let _ = policy.check_url_as(s, Some(UrlSyntax::Ado));
    let Ok(config) = tiberius::Config::from_ado_string(s) else {
        return;
    };
    let pairs = screened.unwrap_or_else(|e| panic!("driver accepted {s:?}, screen refused: {e}"));
    let addr = config.get_addr();
    let host = addr.rsplit_once(':').map_or(addr.as_str(), |(h, _)| h);
    assert_eq!(ado::server_host(&pairs), host, "{s:?}");
    let hosts = url_hosts_as(s, Some(UrlSyntax::Ado));
    let host = canonical_host(host);
    assert!(host.is_empty() || hosts.contains(&host), "{s:?}: {host} not in {hosts:?}");
});
