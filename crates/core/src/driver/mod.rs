//! Family driver traits (RFC 0014, #895).
//!
//! A driver is a connector's I/O and nothing else. The engine — this module
//! for the family-neutral contract, `faucet-common-sql` and
//! `faucet-common-file` for the SQL and object-store families — drives every
//! lifecycle from the driver's primitives: overwrite staging, the
//! exactly-once commit, scoped cleanup, rollback, per-row outcomes and the row
//! report. A connector built on a driver cannot get one of those contracts
//! wrong by forgetting a hook, because it does not implement the hook.
//!
//! | Layer | Trait | Adapter to [`Sink`](crate::Sink) / [`Source`](crate::Source) |
//! |---|---|---|
//! | any sink | [`SinkDriver`] (+ [`StagingDriver`], [`CommitTokenDriver`], [`CleanupDriver`], [`SchemaDriver`], [`RollbackDriver`]) | [`DriverSink`] |
//! | any source | [`SourceDriver`] | [`DriverSource`] |
//! | messaging, produce | [`MessageDriver`] (+ [`MessageTransactions`]) | [`MessageSinkDriver`] → [`DriverSink`] |
//! | messaging, consume | [`MessageConsumer`] | [`MessageSourceDriver`] → [`DriverSource`] |
//! | HTTP | [`HttpDriver`] | [`HttpSinkDriver`] → [`DriverSink`] |
//! | SQL | `faucet_common_sql::SqlDriver` | `faucet_common_sql::SqlSinkDriver` → [`DriverSink`] |
//! | object storage | `faucet_common_file::ObjectStoreDriver` | `faucet_common_file::FileSink` (a [`DriverSink`] inside) |
//!
//! The SQL and object-store traits live in their family crates because they
//! are built on those crates' engines (`SqlSinkEngine` / `SqlDialect`,
//! `FileWriter` / `StorageBackend`), and `faucet-core` depends on no
//! connector-family crate.
//!
//! **Values.** Drivers exchange JSON [`Value`](crate::Value) records under the
//! value-fidelity rules (#885); a destination that writes Arrow directly adds
//! a `ColumnarDriver` (feature `arrow`) and receives the
//! `RecordBatch` unconverted.
//!
//! **Calls.** Every async primitive receives the run's
//! [`CallSlot`](crate::CallSlot); the adapter installs the run's context into
//! it. Make each backend request through it.
//!
//! **Migrating a connector.** Keep the public type and config; hold the
//! adapter and forward:
//!
//! ```
//! use faucet_core::driver::{DriverSink, PageWrite, RowPolicy, SinkDriver};
//! use faucet_core::{CallSlot, FaucetError, Sink, Value, async_trait, forward_sink};
//!
//! /// The connector's I/O.
//! struct AcmeDriver;
//!
//! #[async_trait]
//! impl SinkDriver for AcmeDriver {
//!     fn connector_name(&self) -> &'static str {
//!         "acme"
//!     }
//!     async fn write(
//!         &self,
//!         calls: &CallSlot,
//!         rows: &[Value],
//!         _policy: RowPolicy,
//!     ) -> Result<PageWrite, FaucetError> {
//!         let n = rows.len();
//!         calls
//!             .call_once("insert", async move { Ok::<_, FaucetError>(n) })
//!             .await?;
//!         Ok(PageWrite::accepted(n))
//!     }
//! }
//!
//! /// The public connector type, unchanged for its users.
//! pub struct AcmeSink {
//!     inner: DriverSink<AcmeDriver>,
//! }
//!
//! #[async_trait]
//! impl Sink for AcmeSink {
//!     forward_sink!(inner);
//! }
//! ```
//!
//! See RFC 0014 for the deprecated hooks these traits replace and the
//! migration guide.

mod http;
mod message;
mod sink;
mod source;

pub use http::{HttpBatching, HttpDriver, HttpSinkDriver};
pub use message::{
    AckToken, InboundMessage, MessageConsumer, MessageDriver, MessageSinkDriver,
    MessageSourceDriver, MessageTransactions, OutboundMessage,
};
#[cfg(feature = "arrow")]
pub use sink::ColumnarDriver;
pub use sink::{
    CleanupDriver, CommitTokenDriver, DriverSink, PageWrite, RollbackDriver, RowPolicy,
    SchemaDriver, SinkDriver, StagingDriver,
};
#[cfg(feature = "arrow")]
pub use source::ColumnarStream;
pub use source::{DriverSource, NativeStream, PageStream, SourceDriver, pages_of};

#[cfg(test)]
mod tests;
