#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-source-grpc
//!
//! A config-driven gRPC source that uses protobuf reflection to call
//! any gRPC service dynamically and return records as JSON.
//!
//! Supports both [`RpcKind::Unary`] (one request → one response) and
//! [`RpcKind::ServerStreaming`] (one request → server-driven stream of
//! responses) — see the crate README for the differences in batching and
//! reconnect behaviour.
//!
//! Requires a compiled `FileDescriptorSet` (produced by
//! `protoc --descriptor_set_out=descriptor.bin`).

pub mod config;
pub mod stream;

pub use faucet_core::{AuthSpec, FaucetError, SharedAuthProvider, Source};

pub use config::{GrpcAuth, GrpcStreamConfig, MetadataEntry, RpcKind};
pub use stream::GrpcStream;

/// The connector's static descriptor (RFC 0013): its capabilities, answered from config alone.
pub const DESCRIPTOR: faucet_core::ConnectorDescriptor =
    faucet_core::ConnectorDescriptor::new("grpc", faucet_core::capabilities::plain_source);
