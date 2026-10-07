#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-common-nats
//!
//! Shared configuration types for the [`faucet-stream`](https://crates.io/crates/faucet-stream)
//! NATS source and sink connectors.
//!
//! - [`NatsAuth`] — authentication modes (None, Token, UserPassword, NKey,
//!   CredsFile) with a secret-safe [`std::fmt::Debug`].
//! - [`NatsConnectionConfig`] — the connection surface (`servers`, `auth`,
//!   `tls`, `name`) that both connectors `#[serde(flatten)]` into their config.
//! - [`connect`] — the single client builder both connectors use.
//!
//! All types derive `Serialize`, `Deserialize`, and `JsonSchema` so they
//! round-trip through YAML/JSON configs and CLI introspection.

pub mod auth;
pub mod connection;

pub use auth::NatsAuth;
pub use connection::{NatsConnectionConfig, connect};

/// The lineage dataset URI for a subject on the first configured server:
/// `nats://host:port?subject=…`, with any credentials in the server URL
/// removed (#789 MSG-39).
pub fn dataset_uri(servers: &[String], subject: &str) -> String {
    let server = servers.first().map(String::as_str).unwrap_or("unknown");
    let server = if server.contains("://") {
        server.to_string()
    } else {
        format!("nats://{server}")
    };
    let server = faucet_core::util::redact_uri_credentials(&server);
    format!("{}?subject={subject}", server.trim_end_matches('/'))
}

#[cfg(test)]
mod tests {
    #[test]
    fn dataset_uri_strips_credentials_and_keeps_one_scheme() {
        let uri = |s: &str| super::dataset_uri(&[s.to_string()], "a.b");
        assert_eq!(uri("nats://user:pw@h:4222"), "nats://h:4222?subject=a.b");
        assert_eq!(uri("tls://h:4222/"), "tls://h:4222?subject=a.b");
        assert_eq!(uri("tok@h:4222"), "nats://h:4222?subject=a.b");
        assert_eq!(super::dataset_uri(&[], "x"), "nats://unknown?subject=x");
    }
}
