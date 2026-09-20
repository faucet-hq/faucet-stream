#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-sink-sftp
//!
//! SFTP sink connector for the faucet-stream ecosystem.
//!
//! Writes `serde_json::Value` records to an SFTP server as JSON Lines objects
//! under a remote directory. Writes are **atomic** — each object is uploaded to
//! a temporary name and renamed into place, so consumers never observe a
//! partial file. Append-only. Connection, authentication, and host-key
//! verification come from [`faucet-common-sftp`](https://docs.rs/faucet-common-sftp).

pub mod config;
pub mod sink;

pub use faucet_core::{FaucetError, Sink};

pub use config::{SftpSinkConfig, SftpSinkFormat};
pub use faucet_common_sftp::{HostKeyPolicy, SftpAuth, SftpConnectionConfig};
pub use sink::SftpSink;
