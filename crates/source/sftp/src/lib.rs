#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-source-sftp
//!
//! SFTP source connector for the faucet-stream ecosystem.
//!
//! Lists a remote directory (or reads a single file) over SFTP and streams the
//! files as JSON Lines, JSON arrays, or raw text — with bounded memory for the
//! line- and record-oriented formats. Connection, authentication, and host-key
//! verification come from [`faucet-common-sftp`](https://docs.rs/faucet-common-sftp).

mod descriptor;
pub use descriptor::DESCRIPTOR;

pub mod config;
pub mod stream;

pub use faucet_core::{FaucetError, Source};

pub use config::{SftpFormat, SftpSourceConfig};
pub use faucet_common_sftp::{HostKeyPolicy, SftpAuth, SftpConnectionConfig};
pub use stream::SftpSource;
