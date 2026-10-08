#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-source-xml
//!
//! A config-driven XML/SOAP API source with automatic XML-to-JSON conversion,
//! element-path record extraction, and pluggable authentication.

pub mod config;
pub mod convert;
pub mod decode;
mod format;
pub mod serde_helpers;
pub mod stream;
mod xmltext;

pub use faucet_core::{FaucetError, Source, TlsClientConfig};

pub use config::{SoapConfig, SoapVersion, XmlAuth, XmlPagination, XmlStreamConfig};
pub use decode::{DecodeStep, ParseFormat, ParseSpec, run_decode};
pub use stream::XmlStream;
