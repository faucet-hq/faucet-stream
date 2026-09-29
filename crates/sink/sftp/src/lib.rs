#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-sink-sftp
//!
//! SFTP sink connector for the faucet-stream ecosystem.
//!
//! Writes records to files under a remote directory on an SFTP server, in
//! every format the local file sink writes — JSON Lines, JSON, CSV, XML,
//! Excel, Avro, Parquet and raw text — through the shared file writer (#777).
//! Each file is uploaded to a temporary name and renamed into place, so
//! consumers never observe a partial file. Connection, authentication, and
//! host-key verification come from
//! [`faucet-common-sftp`](https://docs.rs/faucet-common-sftp).

pub mod config;
mod object;
pub mod sink;

pub use faucet_core::{FaucetError, Sink};

pub use config::{SftpSinkConfig, SftpSinkFormat};
pub use faucet_common_sftp::{HostKeyPolicy, SftpAuth, SftpConnectionConfig};
pub use sink::SftpSink;
