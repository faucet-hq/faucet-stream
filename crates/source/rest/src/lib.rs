#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-source-rest
//!
//! A declarative, config-driven REST API client with pluggable authentication,
//! pagination, schema inference, and incremental replication.

pub mod async_job;
pub mod auth;
pub mod config;
pub mod decode;
pub mod discovery;
pub mod extract;
pub mod format;
pub mod odata;
pub mod pagination;
pub mod retry;
pub mod route;
pub mod serde_helpers;
pub mod stream;
pub mod url_util;

// Re-export core types so users don't need a separate faucet-core dependency.
pub use faucet_core::{
    FaucetError, RecordTransform, ReplicationMethod, Sink, Source, WindowBind, WindowSpec,
    replication, schema, transform,
};

pub use async_job::{
    AsyncJobConfig, AsyncJobIncremental, IncrementalInject, InjectMode, JobRequest, JobStatus,
    PollSpec, SubmitErrors,
};
pub use auth::oauth2::DEFAULT_EXPIRY_RATIO;
pub use auth::token_endpoint::DEFAULT_TOKEN_ENDPOINT_EXPIRY_RATIO;
pub use auth::{Auth, ResponseValidator, fetch_oauth2_token, fetch_token_from_endpoint};
pub use config::{
    ODataConfig, ODataVersion, RecordsMultiSpec, ResponseFormat, RestStreamConfig, RetryMatcher,
    TlsClientConfig,
};
pub use decode::{DecodeStep, ParseFormat, ParseSpec, SimpleStep, UnzipSpec};
pub use pagination::PaginationStyle;
pub use route::{RecordsRoute, RouteBy, RouteTarget};
pub use stream::RestStream;
