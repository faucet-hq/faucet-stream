//! The connector's static descriptor: its capabilities, answered from config
//! alone. A custom `faucet` binary registers it with
//! `PluginRegistry::register_sink_described`; without it the binary treats the
//! sink as append-only and refuses `write_mode: upsert`.

use faucet_core::capabilities::config::dedups_by_key;
use faucet_core::{Capabilities, ConnectorDescriptor, Value, WriteMode};

/// The acme sink's descriptor: appends and keyed upserts.
pub const DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor::new("acme", capabilities);

fn capabilities(config: &Value) -> Capabilities {
    Capabilities::builder()
        .write_modes([WriteMode::Append, WriteMode::Upsert])
        .keyed_upsert(true)
        .keyed_dedup(!config.is_null() && dedups_by_key(config))
        .build()
}
