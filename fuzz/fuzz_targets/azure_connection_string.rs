#![no_main]

use faucet_common_azure::{
    AzureConnection, AzureCredentials, builder_settings_with, effective_settings_with,
};
use faucet_core::resolve::{ReferencePolicy, UrlSyntax, url_hosts_as};
use libfuzzer_sys::fuzz_target;

// The screen and the Azure client must read a connection string alike: the
// settings the client is built with are the ones the screen derives, and
// every host the client connects to is one the screen checks.
fuzz_target!(|s: &str| {
    let policy = ReferencePolicy::deny_all()
        .with_path_roots(["/srv/data"])
        .any_url_scheme()
        .any_url_host();
    let screened = policy.check_url_as(s, Some(UrlSyntax::AzureConn));
    let conn = AzureConnection::new("c").auth(AzureCredentials::ConnectionString {
        connection_string: s.to_string(),
    });
    let ours = effective_settings_with(&conn, Vec::new());
    let Ok(client) = builder_settings_with(&conn, Vec::new()) else {
        return;
    };
    assert_eq!(ours, client, "{s:?}");
    if screened.is_err() {
        return;
    }
    let hosts = url_hosts_as(s, Some(UrlSyntax::AzureConn));
    for host in client.hosts() {
        assert!(hosts.contains(&host), "{s:?}: {host} not in {hosts:?}");
    }
});
