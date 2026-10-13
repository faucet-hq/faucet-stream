//! Forwarding by construction for [`Source`](crate::Source) and
//! [`Sink`](crate::Sink) wrappers (RFC 0013, #877).
//!
//! A wrapper (a decorator that holds an inner source or sink) must forward
//! every defaulted hook, or the hook silently reverts to its trait default for
//! everything behind the wrapper — exactly-once, upsert, the columnar fast
//! path, multi-table routing. [`forward_source!`](crate::forward_source) and
//! [`forward_sink!`](crate::forward_sink) generate the forwarding body for
//! every hook, so a wrapper writes only the methods it changes and a hook
//! added to the trait later reaches the inner connector without touching the
//! wrapper.
//!
//! Both macros are invoked inside the wrapper's `impl` block (with or without
//! `#[async_trait]`):
//!
//! ```text
//! forward_sink!(inner);                                  // forward everything
//! forward_sink!(inner; except write_batch, flush);       // the wrapper defines these
//! ```
//!
//! `inner` names the field holding the wrapped connector: any type whose
//! methods resolve to a `Source`/`Sink` (a `Box<dyn Sink>`, an `Arc<S>`, a
//! `&'a S`, a concrete connector), or a tuple index such as `0`.
//!
//! Every hook listed after `except` is left out: the wrapper either defines it
//! itself or deliberately takes the trait default. A name that is not a hook
//! of the trait is a compile error, and defining a hook without listing it is
//! a duplicate-definition error, so the list stays exact.
//!
//! Forwarding is verbatim, including the convenience hooks whose default
//! delegates to another hook (`fetch_all`, `stream_pages`, `check`,
//! `write_batch_partial`, `write_batch_idempotent`, …). A wrapper that changes
//! a hook must also list the hooks that should go through its change rather
//! than straight to the inner connector — a sink that rewrites records in
//! `write_batch` lists `write_batch_partial`, `write_batch_idempotent`,
//! `write_batch_columnar` and `load_native` too. `faucet-conformance`'s
//! `assert_source_forwards_every_hook` / `assert_sink_forwards_every_hook`
//! check the result.

/// Paths the forwarding macros expand to. Not part of the public API.
#[doc(hidden)]
pub mod __forward {
    #[cfg(feature = "arrow")]
    pub use arrow;
    pub use chrono;
    /// Whether this build of `faucet-core` has the `arrow` hooks.
    pub const ARROW: bool = cfg!(feature = "arrow");

    /// The future an `#[async_trait]` hook returns.
    pub type Fut<'a, T> = ::core::pin::Pin<Box<dyn ::core::future::Future<Output = T> + Send + 'a>>;
    /// A hook's fallible result.
    pub type Res<T> = Result<T, crate::FaucetError>;
    /// The parent-record context a fetch receives.
    pub type Ctx = std::collections::HashMap<String, crate::Value>;
    /// A page stream a source returns.
    pub type Pages<'a, T> = ::core::pin::Pin<Box<dyn crate::Stream<Item = Res<T>> + Send + 'a>>;
}

/// Emits its input only when `faucet-core` was built with `arrow`.
#[cfg(feature = "arrow")]
#[doc(hidden)]
#[macro_export]
macro_rules! __faucet_if_arrow {
    ($($t:tt)*) => { $($t)* };
}

/// Emits its input only when `faucet-core` was built with `arrow`.
#[cfg(not(feature = "arrow"))]
#[doc(hidden)]
#[macro_export]
macro_rules! __faucet_if_arrow {
    ($($t:tt)*) => {};
}

/// Generate the forwarding body of every [`Source`](crate::Source) hook
/// inside a wrapper's `impl Source for …` block.
///
/// `forward_source!(field)` forwards every hook to `self.field`;
/// `forward_source!(field; except a, b)` leaves out the hooks the wrapper
/// defines itself (or deliberately leaves at the trait default). See the
/// [module docs](crate::forward) for the rules.
///
/// ```
/// use faucet_core::{FaucetError, Source, Value, async_trait, forward_source};
/// use std::collections::HashMap;
///
/// /// Stamps a fixed state key on any source; everything else reaches `inner`.
/// struct Keyed {
///     inner: Box<dyn Source>,
///     key: String,
/// }
///
/// #[async_trait]
/// impl Source for Keyed {
///     fn state_key(&self) -> Option<String> {
///         Some(self.key.clone())
///     }
///     forward_source!(inner; except state_key);
/// }
/// # struct Empty;
/// # #[async_trait]
/// # impl Source for Empty {
/// #     async fn fetch_with_context(
/// #         &self,
/// #         _: &HashMap<String, Value>,
/// #     ) -> Result<Vec<Value>, FaucetError> {
/// #         Ok(vec![])
/// #     }
/// # }
/// # let s = Keyed { inner: Box::new(Empty), key: "k".into() };
/// # assert_eq!(s.state_key().as_deref(), Some("k"));
/// ```
#[macro_export]
macro_rules! forward_source {
    ($inner:tt) => { $crate::forward_source!($inner; except); };
    ($inner:tt; except $($skip:ident),* $(,)?) => {
        $crate::__faucet_forward_known!(@source [$($skip)*]);
        $crate::__faucet_forward_filter!(fetch_with_context [$($skip)*] {
            fn fetch_with_context<'life0, 'life1, 'async_trait>(
                &'life0 self,
                context: &'life1 $crate::forward::__forward::Ctx,
            ) -> $crate::forward::__forward::Fut<'async_trait, $crate::forward::__forward::Res<::std::vec::Vec<$crate::Value>>>
            where
                'life0: 'async_trait,
                'life1: 'async_trait,
                Self: 'async_trait,
            {
                #[allow(unused_imports)]
                use $crate::Source as _;
                self.$inner.fetch_with_context(context)
            }
        });
        $crate::__faucet_forward_filter!(fetch_all [$($skip)*] {
            fn fetch_all<'life0, 'async_trait>(
                &'life0 self,
            ) -> $crate::forward::__forward::Fut<'async_trait, $crate::forward::__forward::Res<::std::vec::Vec<$crate::Value>>>
            where
                'life0: 'async_trait,
                Self: 'async_trait,
            {
                #[allow(unused_imports)]
                use $crate::Source as _;
                self.$inner.fetch_all()
            }
        });
        $crate::__faucet_forward_filter!(fetch_with_context_incremental [$($skip)*] {
            fn fetch_with_context_incremental<'life0, 'life1, 'async_trait>(
                &'life0 self,
                context: &'life1 $crate::forward::__forward::Ctx,
            ) -> $crate::forward::__forward::Fut<'async_trait, $crate::forward::__forward::Res<(::std::vec::Vec<$crate::Value>, ::core::option::Option<$crate::Value>)>>
            where
                'life0: 'async_trait,
                'life1: 'async_trait,
                Self: 'async_trait,
            {
                #[allow(unused_imports)]
                use $crate::Source as _;
                self.$inner.fetch_with_context_incremental(context)
            }
        });
        $crate::__faucet_forward_filter!(fetch_all_incremental [$($skip)*] {
            fn fetch_all_incremental<'life0, 'async_trait>(
                &'life0 self,
            ) -> $crate::forward::__forward::Fut<'async_trait, $crate::forward::__forward::Res<(::std::vec::Vec<$crate::Value>, ::core::option::Option<$crate::Value>)>>
            where
                'life0: 'async_trait,
                Self: 'async_trait,
            {
                #[allow(unused_imports)]
                use $crate::Source as _;
                self.$inner.fetch_all_incremental()
            }
        });
        $crate::__faucet_forward_filter!(stream_pages [$($skip)*] {
            fn stream_pages<'a>(
                &'a self,
                context: &'a $crate::forward::__forward::Ctx,
                batch_size: usize,
            ) -> $crate::forward::__forward::Pages<'a, $crate::pipeline::StreamPage>
            {
                #[allow(unused_imports)]
                use $crate::Source as _;
                self.$inner.stream_pages(context, batch_size)
            }
        });
        $crate::__faucet_if_arrow! {
            $crate::__faucet_forward_filter!(supports_columnar [$($skip)*] {
                fn supports_columnar(&self) -> bool
                {
                    #[allow(unused_imports)]
                    use $crate::Source as _;
                    self.$inner.supports_columnar()
                }
            });
        }
        $crate::__faucet_if_arrow! {
            $crate::__faucet_forward_filter!(stream_batches [$($skip)*] {
                fn stream_batches<'a>(
                    &'a self,
                    context: &'a $crate::forward::__forward::Ctx,
                    batch_size: usize,
                ) -> $crate::forward::__forward::Pages<'a, $crate::columnar::ColumnarPage>
                {
                    #[allow(unused_imports)]
                    use $crate::Source as _;
                    self.$inner.stream_batches(context, batch_size)
                }
            });
        }
        $crate::__faucet_forward_filter!(native_output_formats [$($skip)*] {
            fn native_output_formats(&self) -> &'static [$crate::native::NativeFormat]
            {
                #[allow(unused_imports)]
                use $crate::Source as _;
                self.$inner.native_output_formats()
            }
        });
        $crate::__faucet_forward_filter!(stream_native [$($skip)*] {
            fn stream_native<'a>(
                &'a self,
                context: &'a $crate::forward::__forward::Ctx,
                format: $crate::native::NativeFormat,
                batch_size: usize,
            ) -> $crate::forward::__forward::Pages<'a, $crate::native::NativeBatch>
            {
                #[allow(unused_imports)]
                use $crate::Source as _;
                self.$inner.stream_native(context, format, batch_size)
            }
        });
        $crate::__faucet_forward_filter!(config_schema [$($skip)*] {
            fn config_schema(&self) -> $crate::Value
            {
                #[allow(unused_imports)]
                use $crate::Source as _;
                self.$inner.config_schema()
            }
        });
        $crate::__faucet_forward_filter!(state_key [$($skip)*] {
            fn state_key(&self) -> ::core::option::Option<::std::string::String>
            {
                #[allow(unused_imports)]
                use $crate::Source as _;
                self.$inner.state_key()
            }
        });
        $crate::__faucet_forward_filter!(apply_start_bookmark [$($skip)*] {
            fn apply_start_bookmark<'life0, 'async_trait>(
                &'life0 self,
                bookmark: $crate::Value,
            ) -> $crate::forward::__forward::Fut<'async_trait, $crate::forward::__forward::Res<()>>
            where
                'life0: 'async_trait,
                Self: 'async_trait,
            {
                #[allow(unused_imports)]
                use $crate::Source as _;
                self.$inner.apply_start_bookmark(bookmark)
            }
        });
        $crate::__faucet_forward_filter!(capture_resume_position [$($skip)*] {
            fn capture_resume_position<'life0, 'async_trait>(
                &'life0 self,
            ) -> $crate::forward::__forward::Fut<'async_trait, $crate::forward::__forward::Res<::core::option::Option<$crate::Value>>>
            where
                'life0: 'async_trait,
                Self: 'async_trait,
            {
                #[allow(unused_imports)]
                use $crate::Source as _;
                self.$inner.capture_resume_position()
            }
        });
        $crate::__faucet_forward_filter!(lag [$($skip)*] {
            fn lag<'life0, 'async_trait>(
                &'life0 self,
            ) -> $crate::forward::__forward::Fut<'async_trait, $crate::forward::__forward::Res<::core::option::Option<$crate::lag::SourceLag>>>
            where
                'life0: 'async_trait,
                Self: 'async_trait,
            {
                #[allow(unused_imports)]
                use $crate::Source as _;
                self.$inner.lag()
            }
        });
        $crate::__faucet_forward_filter!(state_schema [$($skip)*] {
            fn state_schema(&self) -> u32
            {
                #[allow(unused_imports)]
                use $crate::Source as _;
                self.$inner.state_schema()
            }
        });
        $crate::__faucet_forward_filter!(migrate_state [$($skip)*] {
            fn migrate_state(
                &self,
                from: u32,
                data: $crate::Value,
            ) -> $crate::forward::__forward::Res<$crate::Value>
            {
                #[allow(unused_imports)]
                use $crate::Source as _;
                self.$inner.migrate_state(from, data)
            }
        });
        $crate::__faucet_forward_filter!(supports_exactly_once [$($skip)*] {
            fn supports_exactly_once(&self) -> bool
            {
                #[allow(unused_imports)]
                use $crate::Source as _;
                self.$inner.supports_exactly_once()
            }
        });
        $crate::__faucet_forward_filter!(consumes_destructively [$($skip)*] {
            fn consumes_destructively(&self) -> bool
            {
                #[allow(unused_imports)]
                use $crate::Source as _;
                self.$inner.consumes_destructively()
            }
        });
        $crate::__faucet_forward_filter!(replay_guarantee [$($skip)*] {
            fn replay_guarantee(&self) -> $crate::idempotency::ReplayGuarantee
            {
                #[allow(unused_imports)]
                use $crate::Source as _;
                self.$inner.replay_guarantee()
            }
        });
        $crate::__faucet_forward_filter!(capabilities [$($skip)*] {
            fn capabilities(&self) -> $crate::capabilities::Capabilities
            {
                #[allow(unused_imports)]
                use $crate::Source as _;
                $crate::capabilities::Capabilities::decorated_source(
                    self,
                    &self.$inner.capabilities(),
                )
            }
        });
        $crate::__faucet_forward_filter!(is_shardable [$($skip)*] {
            fn is_shardable(&self) -> bool
            {
                #[allow(unused_imports)]
                use $crate::Source as _;
                self.$inner.is_shardable()
            }
        });
        $crate::__faucet_forward_filter!(enumerate_shards [$($skip)*] {
            fn enumerate_shards<'life0, 'async_trait>(
                &'life0 self,
                target: usize,
            ) -> $crate::forward::__forward::Fut<'async_trait, $crate::forward::__forward::Res<::std::vec::Vec<$crate::shard::ShardSpec>>>
            where
                'life0: 'async_trait,
                Self: 'async_trait,
            {
                #[allow(unused_imports)]
                use $crate::Source as _;
                self.$inner.enumerate_shards(target)
            }
        });
        $crate::__faucet_forward_filter!(apply_shard [$($skip)*] {
            fn apply_shard<'life0, 'life1, 'async_trait>(
                &'life0 self,
                shard: &'life1 $crate::shard::ShardSpec,
            ) -> $crate::forward::__forward::Fut<'async_trait, $crate::forward::__forward::Res<()>>
            where
                'life0: 'async_trait,
                'life1: 'async_trait,
                Self: 'async_trait,
            {
                #[allow(unused_imports)]
                use $crate::Source as _;
                self.$inner.apply_shard(shard)
            }
        });
        $crate::__faucet_forward_filter!(range_digest [$($skip)*] {
            fn range_digest<'life0, 'life1, 'life2, 'life3, 'async_trait>(
                &'life0 self,
                range: &'life1 $crate::diff::KeyRange,
                key: &'life2 str,
                columns: &'life3 [::std::string::String],
            ) -> $crate::forward::__forward::Fut<'async_trait, $crate::forward::__forward::Res<::core::option::Option<$crate::diff::ServerDigest>>>
            where
                'life0: 'async_trait,
                'life1: 'async_trait,
                'life2: 'async_trait,
                'life3: 'async_trait,
                Self: 'async_trait,
            {
                #[allow(unused_imports)]
                use $crate::Source as _;
                self.$inner.range_digest(range, key, columns)
            }
        });
        $crate::__faucet_forward_filter!(supports_discover [$($skip)*] {
            fn supports_discover(&self) -> bool
            {
                #[allow(unused_imports)]
                use $crate::Source as _;
                self.$inner.supports_discover()
            }
        });
        $crate::__faucet_forward_filter!(discover [$($skip)*] {
            fn discover<'life0, 'async_trait>(
                &'life0 self,
            ) -> $crate::forward::__forward::Fut<'async_trait, $crate::forward::__forward::Res<::std::vec::Vec<$crate::discover::DatasetDescriptor>>>
            where
                'life0: 'async_trait,
                Self: 'async_trait,
            {
                #[allow(unused_imports)]
                use $crate::Source as _;
                self.$inner.discover()
            }
        });
        $crate::__faucet_forward_filter!(record_table [$($skip)*] {
            fn record_table(
                &self,
                record: &$crate::Value,
            ) -> ::core::option::Option<::std::string::String>
            {
                #[allow(unused_imports)]
                use $crate::Source as _;
                self.$inner.record_table(record)
            }
        });
        $crate::__faucet_forward_filter!(position_le [$($skip)*] {
            fn position_le(
                &self,
                a: &$crate::Value,
                b: &$crate::Value,
            ) -> ::core::option::Option<bool>
            {
                #[allow(unused_imports)]
                use $crate::Source as _;
                self.$inner.position_le(a, b)
            }
        });
        $crate::__faucet_forward_filter!(record_covered [$($skip)*] {
            fn record_covered(
                &self,
                record: &$crate::Value,
                position: &$crate::Value,
            ) -> ::core::option::Option<bool>
            {
                #[allow(unused_imports)]
                use $crate::Source as _;
                self.$inner.record_covered(record, position)
            }
        });
        $crate::__faucet_forward_filter!(position_min [$($skip)*] {
            fn position_min(
                &self,
                positions: &[$crate::Value],
            ) -> ::core::option::Option<$crate::Value>
            {
                #[allow(unused_imports)]
                use $crate::Source as _;
                self.$inner.position_min(positions)
            }
        });
        $crate::__faucet_forward_filter!(connector_name [$($skip)*] {
            fn connector_name(&self) -> &'static str
            {
                #[allow(unused_imports)]
                use $crate::Source as _;
                self.$inner.connector_name()
            }
        });
        $crate::__faucet_forward_filter!(set_roundtrip_recorder [$($skip)*] {
            fn set_roundtrip_recorder(
                &self,
                recorder: ::std::sync::Arc<$crate::observability::RoundtripRecorder>,
            ) -> ()
            {
                #[allow(unused_imports)]
                use $crate::Source as _;
                self.$inner.set_roundtrip_recorder(recorder)
            }
        });
        $crate::__faucet_forward_filter!(set_call_context [$($skip)*] {
            fn set_call_context(&self, ctx: $crate::io::CallContext) -> ()
            {
                #[allow(unused_imports)]
                use $crate::Source as _;
                self.$inner.set_call_context(ctx)
            }
        });
        $crate::__faucet_forward_filter!(set_run_clock [$($skip)*] {
            fn set_run_clock(
                &self,
                now: $crate::forward::__forward::chrono::DateTime<$crate::forward::__forward::chrono::Utc>,
            ) -> ()
            {
                #[allow(unused_imports)]
                use $crate::Source as _;
                self.$inner.set_run_clock(now)
            }
        });
        $crate::__faucet_forward_filter!(set_cancel [$($skip)*] {
            fn set_cancel(&self, cancel: $crate::CancellationToken) -> bool
            {
                #[allow(unused_imports)]
                use $crate::Source as _;
                self.$inner.set_cancel(cancel)
            }
        });
        $crate::__faucet_forward_filter!(dataset_uri [$($skip)*] {
            fn dataset_uri(&self) -> ::std::string::String
            {
                #[allow(unused_imports)]
                use $crate::Source as _;
                self.$inner.dataset_uri()
            }
        });
        $crate::__faucet_forward_filter!(check [$($skip)*] {
            fn check<'life0, 'life1, 'async_trait>(
                &'life0 self,
                ctx: &'life1 $crate::check::CheckContext,
            ) -> $crate::forward::__forward::Fut<'async_trait, $crate::forward::__forward::Res<$crate::check::CheckReport>>
            where
                'life0: 'async_trait,
                'life1: 'async_trait,
                Self: 'async_trait,
            {
                #[allow(unused_imports)]
                use $crate::Source as _;
                self.$inner.check(ctx)
            }
        });
    };
}

/// Generate the forwarding body of every [`Sink`](crate::Sink) hook inside a
/// wrapper's `impl Sink for …` block.
///
/// `forward_sink!(field)` forwards every hook to `self.field`;
/// `forward_sink!(field; except a, b)` leaves out the hooks the wrapper
/// defines itself (or deliberately leaves at the trait default). See the
/// [module docs](crate::forward) for the rules.
///
/// ```
/// use faucet_core::{FaucetError, Sink, Value, async_trait, forward_sink};
/// use std::sync::atomic::{AtomicUsize, Ordering};
///
/// /// Counts the rows written through it.
/// struct Counted<S> {
///     inner: S,
///     rows: AtomicUsize,
/// }
///
/// #[async_trait]
/// impl<S: Sink> Sink for Counted<S> {
///     async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
///         let n = self.inner.write_batch(records).await?;
///         self.rows.fetch_add(n, Ordering::Relaxed);
///         Ok(n)
///     }
///     // The other write paths must count too, so they take the trait
///     // default (which calls `write_batch` above) instead of forwarding.
///     forward_sink!(inner; except write_batch, write_batch_partial,
///         write_batch_idempotent, supports_idempotent_writes,
///         supports_columnar, write_batch_columnar,
///         native_load_capabilities, load_native);
/// }
/// ```
///
/// A name in `except` that is not a `Sink` hook does not compile:
///
/// ```compile_fail
/// use faucet_core::{Sink, async_trait, forward_sink};
///
/// struct Wrapper {
///     inner: Box<dyn Sink>,
/// }
///
/// #[async_trait]
/// impl Sink for Wrapper {
///     forward_sink!(inner; except write_btach);
/// }
/// ```
#[macro_export]
macro_rules! forward_sink {
    ($inner:tt) => { $crate::forward_sink!($inner; except); };
    ($inner:tt; except $($skip:ident),* $(,)?) => {
        $crate::__faucet_forward_known!(@sink [$($skip)*]);
        $crate::__faucet_forward_filter!(write_batch [$($skip)*] {
            fn write_batch<'life0, 'life1, 'async_trait>(
                &'life0 self,
                records: &'life1 [$crate::Value],
            ) -> $crate::forward::__forward::Fut<'async_trait, $crate::forward::__forward::Res<usize>>
            where
                'life0: 'async_trait,
                'life1: 'async_trait,
                Self: 'async_trait,
            {
                #[allow(unused_imports)]
                use $crate::Sink as _;
                self.$inner.write_batch(records)
            }
        });
        $crate::__faucet_forward_filter!(write_batch_reported [$($skip)*] {
            fn write_batch_reported<'life0, 'life1, 'async_trait>(
                &'life0 self,
                records: &'life1 [$crate::Value],
            ) -> $crate::forward::__forward::Fut<'async_trait, $crate::forward::__forward::Res<$crate::accounting::WriteReport>>
            where
                'life0: 'async_trait,
                'life1: 'async_trait,
                Self: 'async_trait,
            {
                #[allow(unused_imports)]
                use $crate::Sink as _;
                self.$inner.write_batch_reported(records)
            }
        });
        $crate::__faucet_forward_filter!(flush [$($skip)*] {
            fn flush<'life0, 'async_trait>(
                &'life0 self,
            ) -> $crate::forward::__forward::Fut<'async_trait, $crate::forward::__forward::Res<()>>
            where
                'life0: 'async_trait,
                Self: 'async_trait,
            {
                #[allow(unused_imports)]
                use $crate::Sink as _;
                self.$inner.flush()
            }
        });
        $crate::__faucet_forward_filter!(write_batch_partial [$($skip)*] {
            fn write_batch_partial<'life0, 'life1, 'async_trait>(
                &'life0 self,
                records: &'life1 [$crate::Value],
            ) -> $crate::forward::__forward::Fut<'async_trait, $crate::forward::__forward::Res<::std::vec::Vec<$crate::traits::RowOutcome>>>
            where
                'life0: 'async_trait,
                'life1: 'async_trait,
                Self: 'async_trait,
            {
                #[allow(unused_imports)]
                use $crate::Sink as _;
                self.$inner.write_batch_partial(records)
            }
        });
        $crate::__faucet_forward_filter!(batch_atomicity [$($skip)*] {
            fn batch_atomicity(&self) -> $crate::dlq::BatchAtomicity
            {
                #[allow(unused_imports)]
                use $crate::Sink as _;
                self.$inner.batch_atomicity()
            }
        });
        $crate::__faucet_forward_filter!(admit_page [$($skip)*] {
            fn admit_page<'life0, 'life1, 'async_trait>(
                &'life0 self,
                records: &'life1 [$crate::Value],
            ) -> $crate::forward::__forward::Fut<'async_trait, $crate::forward::__forward::Res<()>>
            where
                'life0: 'async_trait,
                'life1: 'async_trait,
                Self: 'async_trait,
            {
                #[allow(unused_imports)]
                use $crate::Sink as _;
                self.$inner.admit_page(records)
            }
        });
        $crate::__faucet_if_arrow! {
            $crate::__faucet_forward_filter!(supports_columnar [$($skip)*] {
                fn supports_columnar(&self) -> bool
                {
                    #[allow(unused_imports)]
                    use $crate::Sink as _;
                    self.$inner.supports_columnar()
                }
            });
        }
        $crate::__faucet_if_arrow! {
            $crate::__faucet_forward_filter!(write_batch_columnar [$($skip)*] {
                fn write_batch_columnar<'life0, 'life1, 'async_trait>(
                    &'life0 self,
                    batch: &'life1 $crate::forward::__forward::arrow::array::RecordBatch,
                ) -> $crate::forward::__forward::Fut<'async_trait, $crate::forward::__forward::Res<usize>>
                where
                    'life0: 'async_trait,
                    'life1: 'async_trait,
                    Self: 'async_trait,
                {
                    #[allow(unused_imports)]
                    use $crate::Sink as _;
                    self.$inner.write_batch_columnar(batch)
                }
            });
        }
        $crate::__faucet_forward_filter!(native_load_capabilities [$($skip)*] {
            fn native_load_capabilities(
                &self,
            ) -> ::std::vec::Vec<$crate::native::NativeLoadCapability>
            {
                #[allow(unused_imports)]
                use $crate::Sink as _;
                self.$inner.native_load_capabilities()
            }
        });
        $crate::__faucet_forward_filter!(load_native [$($skip)*] {
            fn load_native<'life0, 'life1, 'async_trait>(
                &'life0 self,
                batch: $crate::native::NativeBatch,
                scope: &'life1 str,
                ctx: $crate::native::NativeLoadContext,
            ) -> $crate::forward::__forward::Fut<'async_trait, $crate::forward::__forward::Res<usize>>
            where
                'life0: 'async_trait,
                'life1: 'async_trait,
                Self: 'async_trait,
            {
                #[allow(unused_imports)]
                use $crate::Sink as _;
                self.$inner.load_native(batch, scope, ctx)
            }
        });
        $crate::__faucet_forward_filter!(supports_idempotent_writes [$($skip)*] {
            fn supports_idempotent_writes(&self) -> bool
            {
                #[allow(unused_imports)]
                use $crate::Sink as _;
                self.$inner.supports_idempotent_writes()
            }
        });
        $crate::__faucet_forward_filter!(sink_guarantee [$($skip)*] {
            fn sink_guarantee(&self) -> $crate::idempotency::SinkGuarantee
            {
                #[allow(unused_imports)]
                use $crate::Sink as _;
                self.$inner.sink_guarantee()
            }
        });
        $crate::__faucet_forward_filter!(capabilities [$($skip)*] {
            fn capabilities(&self) -> $crate::capabilities::Capabilities
            {
                #[allow(unused_imports)]
                use $crate::Sink as _;
                $crate::capabilities::Capabilities::decorated_sink(
                    self,
                    &self.$inner.capabilities(),
                )
            }
        });
        $crate::__faucet_forward_filter!(write_batch_is_replay_safe [$($skip)*] {
            fn write_batch_is_replay_safe(&self) -> bool
            {
                #[allow(unused_imports)]
                use $crate::Sink as _;
                self.$inner.write_batch_is_replay_safe()
            }
        });
        $crate::__faucet_forward_filter!(dedups_by_key [$($skip)*] {
            fn dedups_by_key(&self) -> bool
            {
                #[allow(unused_imports)]
                use $crate::Sink as _;
                self.$inner.dedups_by_key()
            }
        });
        $crate::__faucet_forward_filter!(supported_write_modes [$($skip)*] {
            fn supported_write_modes(&self) -> &'static [$crate::write_mode::WriteMode]
            {
                #[allow(unused_imports)]
                use $crate::Sink as _;
                self.$inner.supported_write_modes()
            }
        });
        $crate::__faucet_forward_filter!(current_schema [$($skip)*] {
            fn current_schema<'life0, 'async_trait>(
                &'life0 self,
            ) -> $crate::forward::__forward::Fut<'async_trait, $crate::forward::__forward::Res<::core::option::Option<$crate::Value>>>
            where
                'life0: 'async_trait,
                Self: 'async_trait,
            {
                #[allow(unused_imports)]
                use $crate::Sink as _;
                self.$inner.current_schema()
            }
        });
        $crate::__faucet_forward_filter!(supports_schema_evolution [$($skip)*] {
            fn supports_schema_evolution(&self) -> bool
            {
                #[allow(unused_imports)]
                use $crate::Sink as _;
                self.$inner.supports_schema_evolution()
            }
        });
        $crate::__faucet_forward_filter!(evolve_schema [$($skip)*] {
            fn evolve_schema<'life0, 'life1, 'async_trait>(
                &'life0 self,
                evolution: &'life1 $crate::drift::SchemaEvolution,
            ) -> $crate::forward::__forward::Fut<'async_trait, $crate::forward::__forward::Res<()>>
            where
                'life0: 'async_trait,
                'life1: 'async_trait,
                Self: 'async_trait,
            {
                #[allow(unused_imports)]
                use $crate::Sink as _;
                self.$inner.evolve_schema(evolution)
            }
        });
        $crate::__faucet_forward_filter!(supports_cleanup [$($skip)*] {
            fn supports_cleanup(&self) -> bool
            {
                #[allow(unused_imports)]
                use $crate::Sink as _;
                self.$inner.supports_cleanup()
            }
        });
        $crate::__faucet_forward_filter!(supports_staged_load [$($skip)*] {
            fn supports_staged_load(&self) -> bool
            {
                #[allow(unused_imports)]
                use $crate::Sink as _;
                self.$inner.supports_staged_load()
            }
        });
        $crate::__faucet_forward_filter!(cleanup_scope [$($skip)*] {
            fn cleanup_scope<'life0, 'life1, 'life2, 'async_trait>(
                &'life0 self,
                scope: &'life1 ::std::collections::BTreeMap<::std::string::String, $crate::Value>,
                seen: &'life2 $crate::cleanup::SeenKeys,
            ) -> $crate::forward::__forward::Fut<'async_trait, $crate::forward::__forward::Res<u64>>
            where
                'life0: 'async_trait,
                'life1: 'async_trait,
                'life2: 'async_trait,
                Self: 'async_trait,
            {
                #[allow(unused_imports)]
                use $crate::Sink as _;
                self.$inner.cleanup_scope(scope, seen)
            }
        });
        $crate::__faucet_forward_filter!(write_batch_idempotent [$($skip)*] {
            fn write_batch_idempotent<'life0, 'life1, 'life2, 'life3, 'async_trait>(
                &'life0 self,
                records: &'life1 [$crate::Value],
                scope: &'life2 str,
                token: &'life3 str,
            ) -> $crate::forward::__forward::Fut<'async_trait, $crate::forward::__forward::Res<usize>>
            where
                'life0: 'async_trait,
                'life1: 'async_trait,
                'life2: 'async_trait,
                'life3: 'async_trait,
                Self: 'async_trait,
            {
                #[allow(unused_imports)]
                use $crate::Sink as _;
                self.$inner.write_batch_idempotent(records, scope, token)
            }
        });
        $crate::__faucet_forward_filter!(write_batch_idempotent_reported [$($skip)*] {
            fn write_batch_idempotent_reported<'life0, 'life1, 'life2, 'life3, 'async_trait>(
                &'life0 self,
                records: &'life1 [$crate::Value],
                scope: &'life2 str,
                token: &'life3 str,
            ) -> $crate::forward::__forward::Fut<'async_trait, $crate::forward::__forward::Res<$crate::accounting::WriteReport>>
            where
                'life0: 'async_trait,
                'life1: 'async_trait,
                'life2: 'async_trait,
                'life3: 'async_trait,
                Self: 'async_trait,
            {
                #[allow(unused_imports)]
                use $crate::Sink as _;
                self.$inner.write_batch_idempotent_reported(records, scope, token)
            }
        });
        $crate::__faucet_forward_filter!(last_committed_token [$($skip)*] {
            fn last_committed_token<'life0, 'life1, 'async_trait>(
                &'life0 self,
                scope: &'life1 str,
            ) -> $crate::forward::__forward::Fut<'async_trait, $crate::forward::__forward::Res<::core::option::Option<::std::string::String>>>
            where
                'life0: 'async_trait,
                'life1: 'async_trait,
                Self: 'async_trait,
            {
                #[allow(unused_imports)]
                use $crate::Sink as _;
                self.$inner.last_committed_token(scope)
            }
        });
        $crate::__faucet_forward_filter!(is_overwrite [$($skip)*] {
            fn is_overwrite(&self) -> bool
            {
                #[allow(unused_imports)]
                use $crate::Sink as _;
                self.$inner.is_overwrite()
            }
        });
        $crate::__faucet_forward_filter!(begin_overwrite [$($skip)*] {
            fn begin_overwrite<'life0, 'async_trait>(
                &'life0 self,
            ) -> $crate::forward::__forward::Fut<'async_trait, $crate::forward::__forward::Res<()>>
            where
                'life0: 'async_trait,
                Self: 'async_trait,
            {
                #[allow(unused_imports)]
                use $crate::Sink as _;
                self.$inner.begin_overwrite()
            }
        });
        $crate::__faucet_forward_filter!(commit_overwrite [$($skip)*] {
            fn commit_overwrite<'life0, 'async_trait>(
                &'life0 self,
            ) -> $crate::forward::__forward::Fut<'async_trait, $crate::forward::__forward::Res<()>>
            where
                'life0: 'async_trait,
                Self: 'async_trait,
            {
                #[allow(unused_imports)]
                use $crate::Sink as _;
                self.$inner.commit_overwrite()
            }
        });
        $crate::__faucet_forward_filter!(supports_rollback [$($skip)*] {
            fn supports_rollback(&self) -> bool
            {
                #[allow(unused_imports)]
                use $crate::Sink as _;
                self.$inner.supports_rollback()
            }
        });
        $crate::__faucet_forward_filter!(rollback_run [$($skip)*] {
            fn rollback_run<'life0, 'life1, 'life2, 'async_trait>(
                &'life0 self,
                run_id: &'life1 str,
                opts: &'life2 $crate::rollback::RollbackOptions,
            ) -> $crate::forward::__forward::Fut<'async_trait, $crate::forward::__forward::Res<$crate::rollback::RollbackOutcome>>
            where
                'life0: 'async_trait,
                'life1: 'async_trait,
                'life2: 'async_trait,
                Self: 'async_trait,
            {
                #[allow(unused_imports)]
                use $crate::Sink as _;
                self.$inner.rollback_run(run_id, opts)
            }
        });
        $crate::__faucet_forward_filter!(forget_run [$($skip)*] {
            fn forget_run<'life0, 'life1, 'async_trait>(
                &'life0 self,
                run_id: &'life1 str,
            ) -> $crate::forward::__forward::Fut<'async_trait, $crate::forward::__forward::Res<()>>
            where
                'life0: 'async_trait,
                'life1: 'async_trait,
                Self: 'async_trait,
            {
                #[allow(unused_imports)]
                use $crate::Sink as _;
                self.$inner.forget_run(run_id)
            }
        });
        $crate::__faucet_forward_filter!(rewind_commit_token [$($skip)*] {
            fn rewind_commit_token<'life0, 'life1, 'life2, 'async_trait>(
                &'life0 self,
                scope: &'life1 str,
                token: ::core::option::Option<&'life2 str>,
            ) -> $crate::forward::__forward::Fut<'async_trait, $crate::forward::__forward::Res<()>>
            where
                'life0: 'async_trait,
                'life1: 'async_trait,
                'life2: 'async_trait,
                Self: 'async_trait,
            {
                #[allow(unused_imports)]
                use $crate::Sink as _;
                self.$inner.rewind_commit_token(scope, token)
            }
        });
        $crate::__faucet_forward_filter!(readback_source [$($skip)*] {
            fn readback_source(
                &self,
            ) -> ::core::option::Option<(::std::string::String, $crate::Value)>
            {
                #[allow(unused_imports)]
                use $crate::Sink as _;
                self.$inner.readback_source()
            }
        });
        $crate::__faucet_forward_filter!(abort_overwrite [$($skip)*] {
            fn abort_overwrite<'life0, 'async_trait>(
                &'life0 self,
            ) -> $crate::forward::__forward::Fut<'async_trait, $crate::forward::__forward::Res<()>>
            where
                'life0: 'async_trait,
                Self: 'async_trait,
            {
                #[allow(unused_imports)]
                use $crate::Sink as _;
                self.$inner.abort_overwrite()
            }
        });
        $crate::__faucet_forward_filter!(complete_run [$($skip)*] {
            fn complete_run<'life0, 'async_trait>(
                &'life0 self,
            ) -> $crate::forward::__forward::Fut<'async_trait, $crate::forward::__forward::Res<()>>
            where
                'life0: 'async_trait,
                Self: 'async_trait,
            {
                #[allow(unused_imports)]
                use $crate::Sink as _;
                self.$inner.complete_run()
            }
        });
        $crate::__faucet_forward_filter!(overwrite_staging_exists [$($skip)*] {
            fn overwrite_staging_exists<'life0, 'async_trait>(
                &'life0 self,
            ) -> $crate::forward::__forward::Fut<'async_trait, $crate::forward::__forward::Res<::core::option::Option<bool>>>
            where
                'life0: 'async_trait,
                Self: 'async_trait,
            {
                #[allow(unused_imports)]
                use $crate::Sink as _;
                self.$inner.overwrite_staging_exists()
            }
        });
        $crate::__faucet_forward_filter!(config_schema [$($skip)*] {
            fn config_schema(&self) -> $crate::Value
            {
                #[allow(unused_imports)]
                use $crate::Sink as _;
                self.$inner.config_schema()
            }
        });
        $crate::__faucet_forward_filter!(connector_name [$($skip)*] {
            fn connector_name(&self) -> &'static str
            {
                #[allow(unused_imports)]
                use $crate::Sink as _;
                self.$inner.connector_name()
            }
        });
        $crate::__faucet_forward_filter!(set_roundtrip_recorder [$($skip)*] {
            fn set_roundtrip_recorder(
                &self,
                recorder: ::std::sync::Arc<$crate::observability::RoundtripRecorder>,
            ) -> ()
            {
                #[allow(unused_imports)]
                use $crate::Sink as _;
                self.$inner.set_roundtrip_recorder(recorder)
            }
        });
        $crate::__faucet_forward_filter!(set_call_context [$($skip)*] {
            fn set_call_context(&self, ctx: $crate::io::CallContext) -> ()
            {
                #[allow(unused_imports)]
                use $crate::Sink as _;
                self.$inner.set_call_context(ctx)
            }
        });
        $crate::__faucet_forward_filter!(dataset_uri [$($skip)*] {
            fn dataset_uri(&self) -> ::std::string::String
            {
                #[allow(unused_imports)]
                use $crate::Sink as _;
                self.$inner.dataset_uri()
            }
        });
        $crate::__faucet_forward_filter!(local_outputs [$($skip)*] {
            fn local_outputs<'life0, 'async_trait>(
                &'life0 self,
            ) -> $crate::forward::__forward::Fut<'async_trait, ::std::vec::Vec<$crate::local_outputs::LocalOutput>>
            where
                'life0: 'async_trait,
                Self: 'async_trait,
            {
                #[allow(unused_imports)]
                use $crate::Sink as _;
                self.$inner.local_outputs()
            }
        });
        $crate::__faucet_forward_filter!(check [$($skip)*] {
            fn check<'life0, 'life1, 'async_trait>(
                &'life0 self,
                ctx: &'life1 $crate::check::CheckContext,
            ) -> $crate::forward::__forward::Fut<'async_trait, $crate::forward::__forward::Res<$crate::check::CheckReport>>
            where
                'life0: 'async_trait,
                'life1: 'async_trait,
                Self: 'async_trait,
            {
                #[allow(unused_imports)]
                use $crate::Sink as _;
                self.$inner.check(ctx)
            }
        });
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __faucet_forward_filter {
    (fetch_with_context [fetch_with_context $($rest:ident)*] $item:tt) => {};
    (fetch_all [fetch_all $($rest:ident)*] $item:tt) => {};
    (fetch_with_context_incremental [fetch_with_context_incremental $($rest:ident)*] $item:tt) => {};
    (fetch_all_incremental [fetch_all_incremental $($rest:ident)*] $item:tt) => {};
    (stream_pages [stream_pages $($rest:ident)*] $item:tt) => {};
    (supports_columnar [supports_columnar $($rest:ident)*] $item:tt) => {};
    (stream_batches [stream_batches $($rest:ident)*] $item:tt) => {};
    (native_output_formats [native_output_formats $($rest:ident)*] $item:tt) => {};
    (stream_native [stream_native $($rest:ident)*] $item:tt) => {};
    (config_schema [config_schema $($rest:ident)*] $item:tt) => {};
    (state_key [state_key $($rest:ident)*] $item:tt) => {};
    (apply_start_bookmark [apply_start_bookmark $($rest:ident)*] $item:tt) => {};
    (capture_resume_position [capture_resume_position $($rest:ident)*] $item:tt) => {};
    (lag [lag $($rest:ident)*] $item:tt) => {};
    (state_schema [state_schema $($rest:ident)*] $item:tt) => {};
    (migrate_state [migrate_state $($rest:ident)*] $item:tt) => {};
    (supports_exactly_once [supports_exactly_once $($rest:ident)*] $item:tt) => {};
    (consumes_destructively [consumes_destructively $($rest:ident)*] $item:tt) => {};
    (replay_guarantee [replay_guarantee $($rest:ident)*] $item:tt) => {};
    (capabilities [capabilities $($rest:ident)*] $item:tt) => {};
    (is_shardable [is_shardable $($rest:ident)*] $item:tt) => {};
    (enumerate_shards [enumerate_shards $($rest:ident)*] $item:tt) => {};
    (apply_shard [apply_shard $($rest:ident)*] $item:tt) => {};
    (range_digest [range_digest $($rest:ident)*] $item:tt) => {};
    (supports_discover [supports_discover $($rest:ident)*] $item:tt) => {};
    (discover [discover $($rest:ident)*] $item:tt) => {};
    (record_table [record_table $($rest:ident)*] $item:tt) => {};
    (position_le [position_le $($rest:ident)*] $item:tt) => {};
    (record_covered [record_covered $($rest:ident)*] $item:tt) => {};
    (position_min [position_min $($rest:ident)*] $item:tt) => {};
    (connector_name [connector_name $($rest:ident)*] $item:tt) => {};
    (set_roundtrip_recorder [set_roundtrip_recorder $($rest:ident)*] $item:tt) => {};
    (set_call_context [set_call_context $($rest:ident)*] $item:tt) => {};
    (set_run_clock [set_run_clock $($rest:ident)*] $item:tt) => {};
    (set_cancel [set_cancel $($rest:ident)*] $item:tt) => {};
    (dataset_uri [dataset_uri $($rest:ident)*] $item:tt) => {};
    (check [check $($rest:ident)*] $item:tt) => {};
    (write_batch [write_batch $($rest:ident)*] $item:tt) => {};
    (write_batch_reported [write_batch_reported $($rest:ident)*] $item:tt) => {};
    // A wrapper that changes `write_batch` takes the trait default for the
    // reported write, which goes through its own `write_batch` (#879).
    (write_batch_reported [write_batch $($rest:ident)*] $item:tt) => {};
    (flush [flush $($rest:ident)*] $item:tt) => {};
    (write_batch_partial [write_batch_partial $($rest:ident)*] $item:tt) => {};
    (batch_atomicity [batch_atomicity $($rest:ident)*] $item:tt) => {};
    (admit_page [admit_page $($rest:ident)*] $item:tt) => {};
    (write_batch_columnar [write_batch_columnar $($rest:ident)*] $item:tt) => {};
    (native_load_capabilities [native_load_capabilities $($rest:ident)*] $item:tt) => {};
    (load_native [load_native $($rest:ident)*] $item:tt) => {};
    (supports_idempotent_writes [supports_idempotent_writes $($rest:ident)*] $item:tt) => {};
    (sink_guarantee [sink_guarantee $($rest:ident)*] $item:tt) => {};
    (write_batch_is_replay_safe [write_batch_is_replay_safe $($rest:ident)*] $item:tt) => {};
    (dedups_by_key [dedups_by_key $($rest:ident)*] $item:tt) => {};
    (supported_write_modes [supported_write_modes $($rest:ident)*] $item:tt) => {};
    (current_schema [current_schema $($rest:ident)*] $item:tt) => {};
    (supports_schema_evolution [supports_schema_evolution $($rest:ident)*] $item:tt) => {};
    (evolve_schema [evolve_schema $($rest:ident)*] $item:tt) => {};
    (supports_cleanup [supports_cleanup $($rest:ident)*] $item:tt) => {};
    (supports_staged_load [supports_staged_load $($rest:ident)*] $item:tt) => {};
    (cleanup_scope [cleanup_scope $($rest:ident)*] $item:tt) => {};
    (write_batch_idempotent [write_batch_idempotent $($rest:ident)*] $item:tt) => {};
    (write_batch_idempotent_reported [write_batch_idempotent_reported $($rest:ident)*] $item:tt) => {};
    // A wrapper that changes `write_batch_idempotent` takes the trait default
    // for the reported write, which goes through its own change (#879).
    (write_batch_idempotent_reported [write_batch_idempotent $($rest:ident)*] $item:tt) => {};
    (last_committed_token [last_committed_token $($rest:ident)*] $item:tt) => {};
    (is_overwrite [is_overwrite $($rest:ident)*] $item:tt) => {};
    (begin_overwrite [begin_overwrite $($rest:ident)*] $item:tt) => {};
    (commit_overwrite [commit_overwrite $($rest:ident)*] $item:tt) => {};
    (supports_rollback [supports_rollback $($rest:ident)*] $item:tt) => {};
    (rollback_run [rollback_run $($rest:ident)*] $item:tt) => {};
    (forget_run [forget_run $($rest:ident)*] $item:tt) => {};
    (rewind_commit_token [rewind_commit_token $($rest:ident)*] $item:tt) => {};
    (readback_source [readback_source $($rest:ident)*] $item:tt) => {};
    (abort_overwrite [abort_overwrite $($rest:ident)*] $item:tt) => {};
    (complete_run [complete_run $($rest:ident)*] $item:tt) => {};
    (overwrite_staging_exists [overwrite_staging_exists $($rest:ident)*] $item:tt) => {};
    (local_outputs [local_outputs $($rest:ident)*] $item:tt) => {};
    ($hook:ident [] { $($item:tt)* }) => { $($item)* };
    ($hook:ident [$other:ident $($rest:ident)*] $item:tt) => {
        $crate::__faucet_forward_filter!($hook [$($rest)*] $item);
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __faucet_forward_known {
    (@source [fetch_with_context $($rest:ident)*]) => { $crate::__faucet_forward_known!(@source [$($rest)*]); };
    (@source [fetch_all $($rest:ident)*]) => { $crate::__faucet_forward_known!(@source [$($rest)*]); };
    (@source [fetch_with_context_incremental $($rest:ident)*]) => { $crate::__faucet_forward_known!(@source [$($rest)*]); };
    (@source [fetch_all_incremental $($rest:ident)*]) => { $crate::__faucet_forward_known!(@source [$($rest)*]); };
    (@source [stream_pages $($rest:ident)*]) => { $crate::__faucet_forward_known!(@source [$($rest)*]); };
    (@source [supports_columnar $($rest:ident)*]) => { $crate::__faucet_forward_known!(@source [$($rest)*]); };
    (@source [stream_batches $($rest:ident)*]) => { $crate::__faucet_forward_known!(@source [$($rest)*]); };
    (@source [native_output_formats $($rest:ident)*]) => { $crate::__faucet_forward_known!(@source [$($rest)*]); };
    (@source [stream_native $($rest:ident)*]) => { $crate::__faucet_forward_known!(@source [$($rest)*]); };
    (@source [config_schema $($rest:ident)*]) => { $crate::__faucet_forward_known!(@source [$($rest)*]); };
    (@source [state_key $($rest:ident)*]) => { $crate::__faucet_forward_known!(@source [$($rest)*]); };
    (@source [apply_start_bookmark $($rest:ident)*]) => { $crate::__faucet_forward_known!(@source [$($rest)*]); };
    (@source [capture_resume_position $($rest:ident)*]) => { $crate::__faucet_forward_known!(@source [$($rest)*]); };
    (@source [lag $($rest:ident)*]) => { $crate::__faucet_forward_known!(@source [$($rest)*]); };
    (@source [state_schema $($rest:ident)*]) => { $crate::__faucet_forward_known!(@source [$($rest)*]); };
    (@source [migrate_state $($rest:ident)*]) => { $crate::__faucet_forward_known!(@source [$($rest)*]); };
    (@source [supports_exactly_once $($rest:ident)*]) => { $crate::__faucet_forward_known!(@source [$($rest)*]); };
    (@source [consumes_destructively $($rest:ident)*]) => { $crate::__faucet_forward_known!(@source [$($rest)*]); };
    (@source [replay_guarantee $($rest:ident)*]) => { $crate::__faucet_forward_known!(@source [$($rest)*]); };
    (@source [capabilities $($rest:ident)*]) => { $crate::__faucet_forward_known!(@source [$($rest)*]); };
    (@source [is_shardable $($rest:ident)*]) => { $crate::__faucet_forward_known!(@source [$($rest)*]); };
    (@source [enumerate_shards $($rest:ident)*]) => { $crate::__faucet_forward_known!(@source [$($rest)*]); };
    (@source [apply_shard $($rest:ident)*]) => { $crate::__faucet_forward_known!(@source [$($rest)*]); };
    (@source [range_digest $($rest:ident)*]) => { $crate::__faucet_forward_known!(@source [$($rest)*]); };
    (@source [supports_discover $($rest:ident)*]) => { $crate::__faucet_forward_known!(@source [$($rest)*]); };
    (@source [discover $($rest:ident)*]) => { $crate::__faucet_forward_known!(@source [$($rest)*]); };
    (@source [record_table $($rest:ident)*]) => { $crate::__faucet_forward_known!(@source [$($rest)*]); };
    (@source [position_le $($rest:ident)*]) => { $crate::__faucet_forward_known!(@source [$($rest)*]); };
    (@source [record_covered $($rest:ident)*]) => { $crate::__faucet_forward_known!(@source [$($rest)*]); };
    (@source [position_min $($rest:ident)*]) => { $crate::__faucet_forward_known!(@source [$($rest)*]); };
    (@source [connector_name $($rest:ident)*]) => { $crate::__faucet_forward_known!(@source [$($rest)*]); };
    (@source [set_roundtrip_recorder $($rest:ident)*]) => { $crate::__faucet_forward_known!(@source [$($rest)*]); };
    (@source [set_call_context $($rest:ident)*]) => { $crate::__faucet_forward_known!(@source [$($rest)*]); };
    (@source [set_run_clock $($rest:ident)*]) => { $crate::__faucet_forward_known!(@source [$($rest)*]); };
    (@source [set_cancel $($rest:ident)*]) => { $crate::__faucet_forward_known!(@source [$($rest)*]); };
    (@source [dataset_uri $($rest:ident)*]) => { $crate::__faucet_forward_known!(@source [$($rest)*]); };
    (@source [check $($rest:ident)*]) => { $crate::__faucet_forward_known!(@source [$($rest)*]); };
    (@sink [write_batch $($rest:ident)*]) => { $crate::__faucet_forward_known!(@sink [$($rest)*]); };
    (@sink [write_batch_reported $($rest:ident)*]) => { $crate::__faucet_forward_known!(@sink [$($rest)*]); };
    (@sink [flush $($rest:ident)*]) => { $crate::__faucet_forward_known!(@sink [$($rest)*]); };
    (@sink [write_batch_partial $($rest:ident)*]) => { $crate::__faucet_forward_known!(@sink [$($rest)*]); };
    (@sink [batch_atomicity $($rest:ident)*]) => { $crate::__faucet_forward_known!(@sink [$($rest)*]); };
    (@sink [admit_page $($rest:ident)*]) => { $crate::__faucet_forward_known!(@sink [$($rest)*]); };
    (@sink [supports_columnar $($rest:ident)*]) => { $crate::__faucet_forward_known!(@sink [$($rest)*]); };
    (@sink [write_batch_columnar $($rest:ident)*]) => { $crate::__faucet_forward_known!(@sink [$($rest)*]); };
    (@sink [native_load_capabilities $($rest:ident)*]) => { $crate::__faucet_forward_known!(@sink [$($rest)*]); };
    (@sink [load_native $($rest:ident)*]) => { $crate::__faucet_forward_known!(@sink [$($rest)*]); };
    (@sink [supports_idempotent_writes $($rest:ident)*]) => { $crate::__faucet_forward_known!(@sink [$($rest)*]); };
    (@sink [sink_guarantee $($rest:ident)*]) => { $crate::__faucet_forward_known!(@sink [$($rest)*]); };
    (@sink [capabilities $($rest:ident)*]) => { $crate::__faucet_forward_known!(@sink [$($rest)*]); };
    (@sink [write_batch_is_replay_safe $($rest:ident)*]) => { $crate::__faucet_forward_known!(@sink [$($rest)*]); };
    (@sink [dedups_by_key $($rest:ident)*]) => { $crate::__faucet_forward_known!(@sink [$($rest)*]); };
    (@sink [supported_write_modes $($rest:ident)*]) => { $crate::__faucet_forward_known!(@sink [$($rest)*]); };
    (@sink [current_schema $($rest:ident)*]) => { $crate::__faucet_forward_known!(@sink [$($rest)*]); };
    (@sink [supports_schema_evolution $($rest:ident)*]) => { $crate::__faucet_forward_known!(@sink [$($rest)*]); };
    (@sink [evolve_schema $($rest:ident)*]) => { $crate::__faucet_forward_known!(@sink [$($rest)*]); };
    (@sink [supports_cleanup $($rest:ident)*]) => { $crate::__faucet_forward_known!(@sink [$($rest)*]); };
    (@sink [supports_staged_load $($rest:ident)*]) => { $crate::__faucet_forward_known!(@sink [$($rest)*]); };
    (@sink [cleanup_scope $($rest:ident)*]) => { $crate::__faucet_forward_known!(@sink [$($rest)*]); };
    (@sink [write_batch_idempotent $($rest:ident)*]) => { $crate::__faucet_forward_known!(@sink [$($rest)*]); };
    (@sink [write_batch_idempotent_reported $($rest:ident)*]) => { $crate::__faucet_forward_known!(@sink [$($rest)*]); };
    (@sink [last_committed_token $($rest:ident)*]) => { $crate::__faucet_forward_known!(@sink [$($rest)*]); };
    (@sink [is_overwrite $($rest:ident)*]) => { $crate::__faucet_forward_known!(@sink [$($rest)*]); };
    (@sink [begin_overwrite $($rest:ident)*]) => { $crate::__faucet_forward_known!(@sink [$($rest)*]); };
    (@sink [commit_overwrite $($rest:ident)*]) => { $crate::__faucet_forward_known!(@sink [$($rest)*]); };
    (@sink [supports_rollback $($rest:ident)*]) => { $crate::__faucet_forward_known!(@sink [$($rest)*]); };
    (@sink [rollback_run $($rest:ident)*]) => { $crate::__faucet_forward_known!(@sink [$($rest)*]); };
    (@sink [forget_run $($rest:ident)*]) => { $crate::__faucet_forward_known!(@sink [$($rest)*]); };
    (@sink [rewind_commit_token $($rest:ident)*]) => { $crate::__faucet_forward_known!(@sink [$($rest)*]); };
    (@sink [readback_source $($rest:ident)*]) => { $crate::__faucet_forward_known!(@sink [$($rest)*]); };
    (@sink [abort_overwrite $($rest:ident)*]) => { $crate::__faucet_forward_known!(@sink [$($rest)*]); };
    (@sink [complete_run $($rest:ident)*]) => { $crate::__faucet_forward_known!(@sink [$($rest)*]); };
    (@sink [overwrite_staging_exists $($rest:ident)*]) => { $crate::__faucet_forward_known!(@sink [$($rest)*]); };
    (@sink [config_schema $($rest:ident)*]) => { $crate::__faucet_forward_known!(@sink [$($rest)*]); };
    (@sink [connector_name $($rest:ident)*]) => { $crate::__faucet_forward_known!(@sink [$($rest)*]); };
    (@sink [set_roundtrip_recorder $($rest:ident)*]) => { $crate::__faucet_forward_known!(@sink [$($rest)*]); };
    (@sink [set_call_context $($rest:ident)*]) => { $crate::__faucet_forward_known!(@sink [$($rest)*]); };
    (@sink [dataset_uri $($rest:ident)*]) => { $crate::__faucet_forward_known!(@sink [$($rest)*]); };
    (@sink [local_outputs $($rest:ident)*]) => { $crate::__faucet_forward_known!(@sink [$($rest)*]); };
    (@sink [check $($rest:ident)*]) => { $crate::__faucet_forward_known!(@sink [$($rest)*]); };
    (@$tr:ident []) => {};
    (@source [$other:ident $($rest:ident)*]) => {
        ::core::compile_error!(::core::concat!(
            "forward_source!: `", ::core::stringify!($other), "` is not a Source hook"
        ));
    };
    (@sink [$other:ident $($rest:ident)*]) => {
        ::core::compile_error!(::core::concat!(
            "forward_sink!: `", ::core::stringify!($other), "` is not a Sink hook"
        ));
    };
}
