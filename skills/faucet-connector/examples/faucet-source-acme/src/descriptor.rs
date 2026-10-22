//! The connector's static descriptor: its capabilities, answered from config
//! alone. A custom `faucet` binary registers it with
//! `PluginRegistry::register_source_described`, so load-time checks see the
//! same capabilities the built source reports.

use faucet_core::{ConnectorDescriptor, capabilities};

/// The acme source's descriptor: a plain, resumable read (no exactly-once,
/// discovery or sharding).
pub const DESCRIPTOR: ConnectorDescriptor =
    ConnectorDescriptor::new("acme", capabilities::plain_source);
