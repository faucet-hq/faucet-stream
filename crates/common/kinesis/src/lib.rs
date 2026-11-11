#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-common-kinesis
//!
//! Shared configuration types for the faucet-stream AWS Kinesis Data Streams
//! source (`faucet-source-kinesis`) and sink (`faucet-sink-kinesis`)
//! connectors: the [`KinesisCredentials`] auth enum and the
//! [`build_client`] helper that assembles an `aws_sdk_kinesis::Client` from
//! region / endpoint / credential settings. Both connector crates re-export
//! these so end-user imports do not change.

mod auth;

pub use auth::{
    KinesisCredentials, build_client, host_identity, host_identity_at, region_from_env,
};
