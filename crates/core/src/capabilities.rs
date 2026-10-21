//! Declared connector capabilities (RFC 0013, #876).
//!
//! A connector says what it can do through [`Capabilities`]: the write modes it
//! applies, whether it commits a watermark atomically, whether it can clean up,
//! roll back, evolve a schema, report lag, digest a key range, and so on. Two
//! places carry the declaration:
//!
//! - [`Source::capabilities`] /
//!   [`Sink::capabilities`] on a built connector.
//!   The default derives every field from the older per-capability trait
//!   methods, so a connector that overrides only those keeps working.
//! - A static [`ConnectorDescriptor`] registered with the connector's factory.
//!   Its `fn(&Value) -> Capabilities` answers from the connector's config alone
//!   — pure, no I/O — so load-time gates (`faucet validate`, expand) can check
//!   a config before anything connects.
//!
//! The two must agree for every config: `faucet-conformance`'s
//! `assert_descriptor_matches_*` checks it, and every built-in connector calls
//! it from its `tests/conformance.rs`.
//!
//! **Config-dependent capabilities.** A capability that depends on the config
//! (a SQL sink's cleanup needs column mapping, a sink only dedups by key under
//! `write_mode: upsert` with a `key`) is answered by the descriptor function
//! reading the relevant fields of the raw config value. Descriptors read field
//! by field and fall back to the connector's defaults for anything missing or
//! not yet interpolated, so a config that has not been resolved yet still gets
//! an answer; they never fail. Called with `Value::Null` instead of a config
//! object, a descriptor answers the kind-level view
//! ([`ConnectorDescriptor::kind_capabilities`]): every capability some valid
//! config enables (config-only facts such as `keyed_dedup` stay off).

use crate::idempotency::{ReplayGuarantee, SinkGuarantee};
use crate::traits::{Sink, Source};
use crate::write_mode::{WriteMode, WriteSpec};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// How a sink treats a record field its destination has no column for — the
/// fact the schema-drift policy needs to decide whether it can act (#883).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum SchemaReport {
    /// The sink has not declared it.
    #[default]
    Undeclared,
    /// [`Sink::current_schema`] reports the live
    /// destination schema, so the drift pass sees every column change.
    Reported,
    /// The sink reports no schema and writes every field it is given.
    Schemaless,
    /// The sink reports no schema and silently drops fields its destination
    /// schema lacks. A drift policy is blind against it, so `on_drift: fail`
    /// is refused at load time rather than silently inert.
    DropsUnknownFields,
}

impl SchemaReport {
    /// Wire name, for messages and docs.
    pub fn as_str(&self) -> &'static str {
        match self {
            SchemaReport::Undeclared => "undeclared",
            SchemaReport::Reported => "reported",
            SchemaReport::Schemaless => "schemaless",
            SchemaReport::DropsUnknownFields => "drops_unknown_fields",
        }
    }
}

/// What a connector can do, as declared by the connector itself.
///
/// Build one with [`Capabilities::builder`]; read it through the accessor
/// methods. Source-side and sink-side fields live in one type so a registry
/// can treat both alike; the fields that do not apply to a role stay at their
/// defaults.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct Capabilities {
    write_modes: Vec<WriteMode>,
    idempotent_writes: bool,
    keyed_upsert: bool,
    keyed_dedup: bool,
    cleanup: bool,
    rollback: bool,
    schema_evolution: bool,
    schema_report: SchemaReport,
    scoped_overwrite: bool,
    direct_overwrite: bool,
    staged_load: bool,
    exactly_once: bool,
    discover: bool,
    shardable: bool,
    range_read: bool,
    range_digest: bool,
    reports_lag: bool,
    columnar: bool,
    native: bool,
    table_routing: bool,
    read_side_effects: bool,
    complete_reads: bool,
}

const MODE_ORDER: [WriteMode; 4] = [
    WriteMode::Append,
    WriteMode::Upsert,
    WriteMode::Delete,
    WriteMode::Overwrite,
];

fn mode_rank(m: &WriteMode) -> usize {
    MODE_ORDER
        .iter()
        .position(|o| o == m)
        .unwrap_or(MODE_ORDER.len())
}

impl Capabilities {
    /// A builder with every capability off and no write modes.
    pub fn builder() -> CapabilitiesBuilder {
        CapabilitiesBuilder(Capabilities::default())
    }

    /// A builder starting from these capabilities, to add or remove one.
    pub fn to_builder(&self) -> CapabilitiesBuilder {
        CapabilitiesBuilder(self.clone())
    }

    /// What a plain source declares: nothing.
    pub fn plain_source() -> Self {
        Self::default()
    }

    /// What a plain sink declares: append only.
    pub fn append_only_sink() -> Self {
        Self::builder().write_modes([WriteMode::Append]).build()
    }

    /// Derive a source's capabilities from its per-capability trait methods.
    /// The default [`Source::capabilities`]; an
    /// override that adds a capability no trait method expresses starts here
    /// (`Capabilities::of_source(self).to_builder().range_digest(true).build()`).
    pub fn of_source<S: Source + ?Sized>(source: &S) -> Self {
        #[cfg(feature = "arrow")]
        let columnar = source.supports_columnar();
        #[cfg(not(feature = "arrow"))]
        let columnar = false;
        Self::builder()
            .exactly_once(source.replay_guarantee() == ReplayGuarantee::Deterministic)
            .discover(source.supports_discover())
            .shardable(source.is_shardable())
            .columnar(columnar)
            .native(!source.native_output_formats().is_empty())
            .build()
    }

    /// Derive a sink's capabilities from its per-capability trait methods.
    /// The default [`Sink::capabilities`].
    pub fn of_sink<S: Sink + ?Sized>(sink: &S) -> Self {
        #[cfg(feature = "arrow")]
        let columnar = sink.supports_columnar();
        #[cfg(not(feature = "arrow"))]
        let columnar = false;
        let modes = sink.supported_write_modes();
        let schema_evolution = sink.supports_schema_evolution();
        Self::builder()
            .write_modes(modes.iter().copied())
            .idempotent_writes(sink.supports_idempotent_writes())
            .keyed_upsert(
                modes.contains(&WriteMode::Upsert)
                    && sink.sink_guarantee() != SinkGuarantee::AtLeastOnce,
            )
            .keyed_dedup(sink.dedups_by_key())
            .cleanup(sink.supports_cleanup())
            .rollback(sink.supports_rollback())
            .schema_evolution(schema_evolution)
            .schema_report(if schema_evolution {
                SchemaReport::Reported
            } else {
                SchemaReport::Undeclared
            })
            .staged_load(sink.supports_staged_load())
            .columnar(columnar)
            .native(!sink.native_load_capabilities().is_empty())
            .build()
    }

    /// A decorator's capabilities: every field a trait method expresses is
    /// derived from the decorator (which forwards or deliberately withholds
    /// it), the rest is taken from the wrapped source's `inner` declaration.
    pub fn decorated_source<S: Source + ?Sized>(outer: &S, inner: &Capabilities) -> Self {
        Self::of_source(outer)
            .to_builder()
            .range_read(inner.range_read)
            .range_digest(inner.range_digest)
            .reports_lag(inner.reports_lag)
            .table_routing(inner.table_routing)
            .read_side_effects(inner.read_side_effects)
            .complete_reads(inner.complete_reads)
            .build()
    }

    /// [`decorated_source`](Self::decorated_source) for sinks.
    pub fn decorated_sink<S: Sink + ?Sized>(outer: &S, inner: &Capabilities) -> Self {
        Self::of_sink(outer)
            .to_builder()
            .schema_report(inner.schema_report)
            .scoped_overwrite(inner.scoped_overwrite)
            .direct_overwrite(inner.direct_overwrite)
            .build()
    }

    /// Write modes the sink applies, in canonical order.
    pub fn write_modes(&self) -> &[WriteMode] {
        &self.write_modes
    }

    /// Whether the sink applies `mode`.
    pub fn supports_write_mode(&self, mode: WriteMode) -> bool {
        self.write_modes.contains(&mode)
    }

    /// `write_mode: overwrite` (atomic full-destination replacement).
    pub fn overwrite(&self) -> bool {
        self.supports_write_mode(WriteMode::Overwrite)
    }

    /// Commits a page's rows and a commit token atomically.
    pub fn idempotent_writes(&self) -> bool {
        self.idempotent_writes
    }

    /// Converges by key under `write_mode: upsert` — the keyed-upsert
    /// effectively-once mechanism. A sink that forwards `key` to a target it
    /// cannot vouch for accepts upsert without this.
    pub fn keyed_upsert(&self) -> bool {
        self.keyed_upsert
    }

    /// This config dedups by key (`write_mode: upsert|delete` with a `key`).
    pub fn keyed_dedup(&self) -> bool {
        self.keyed_dedup
    }

    /// Scoped cleanup of rows the run did not write (#478).
    pub fn cleanup(&self) -> bool {
        self.cleanup
    }

    /// Undoing a run it wrote (#706).
    pub fn rollback(&self) -> bool {
        self.rollback
    }

    /// Additive / widening DDL (`on_drift: evolve`).
    pub fn schema_evolution(&self) -> bool {
        self.schema_evolution
    }

    /// How the sink treats fields its destination lacks.
    pub fn schema_report(&self) -> SchemaReport {
        self.schema_report
    }

    /// A windowed `overwrite.scope` (#518).
    pub fn scoped_overwrite(&self) -> bool {
        self.scoped_overwrite
    }

    /// A staging-free overwrite that understands the grouped staging signal.
    pub fn direct_overwrite(&self) -> bool {
        self.direct_overwrite
    }

    /// Bulk load through an object-store stage (#528).
    pub fn staged_load(&self) -> bool {
        self.staged_load
    }

    /// Deterministic replay from a bookmark (exactly-once source).
    pub fn exactly_once(&self) -> bool {
        self.exactly_once
    }

    /// Dataset discovery (`faucet discover`).
    pub fn discover(&self) -> bool {
        self.discover
    }

    /// Splits into shards for clustered execution.
    pub fn shardable(&self) -> bool {
        self.shardable
    }

    /// Reads one integer-key range through `apply_shard` without `shard:`
    /// config (`faucet verify` range mode).
    pub fn range_read(&self) -> bool {
        self.range_read
    }

    /// Computes a server-side digest of a key range (`Source::range_digest`).
    pub fn range_digest(&self) -> bool {
        self.range_digest
    }

    /// Reports how far behind its head it is (`Source::lag`).
    pub fn reports_lag(&self) -> bool {
        self.reports_lag
    }

    /// Speaks Arrow on the columnar path.
    pub fn columnar(&self) -> bool {
        self.columnar
    }

    /// Takes part in the native byte-passthrough path.
    pub fn native(&self) -> bool {
        self.native
    }

    /// Names the table each record belongs to (`Source::record_table`), so
    /// one change stream can drive a multi-table mirror.
    pub fn table_routing(&self) -> bool {
        self.table_routing
    }

    /// Reading changes state outside the pipeline — creates or advances a
    /// replication slot or capture position, joins a consumer group, opens a
    /// listener or a live session — so a read-only preview must not read it.
    pub fn read_side_effects(&self) -> bool {
        self.read_side_effects
    }

    /// A run can read the whole dataset (`Source::read_completeness`), after
    /// which destination rows it did not write are deleted — so the sink must
    /// support keyed cleanup.
    pub fn complete_reads(&self) -> bool {
        self.complete_reads
    }

    /// The source's typed replay guarantee.
    pub fn replay_guarantee(&self) -> ReplayGuarantee {
        if self.exactly_once {
            ReplayGuarantee::Deterministic
        } else {
            ReplayGuarantee::NonDeterministic
        }
    }

    /// The strongest delivery guarantee the sink can uphold.
    pub fn sink_guarantee(&self) -> SinkGuarantee {
        if self.idempotent_writes {
            SinkGuarantee::AtomicWatermark
        } else if self.keyed_upsert {
            SinkGuarantee::KeyedUpsert
        } else {
            SinkGuarantee::AtLeastOnce
        }
    }
}

/// Builder for [`Capabilities`].
#[derive(Debug, Clone, Default)]
pub struct CapabilitiesBuilder(Capabilities);

macro_rules! flag {
    ($($name:ident),* $(,)?) => {
        $(
            #[doc = concat!("Set `", stringify!($name), "`.")]
            #[must_use]
            pub fn $name(mut self, on: bool) -> Self {
                self.0.$name = on;
                self
            }
        )*
    };
}

impl CapabilitiesBuilder {
    /// Set the write modes (deduplicated, canonical order).
    #[must_use]
    pub fn write_modes(mut self, modes: impl IntoIterator<Item = WriteMode>) -> Self {
        let mut v: Vec<WriteMode> = Vec::new();
        for m in modes {
            if !v.contains(&m) {
                v.push(m);
            }
        }
        v.sort_by_key(mode_rank);
        self.0.write_modes = v;
        self
    }

    /// Set the schema report.
    #[must_use]
    pub fn schema_report(mut self, report: SchemaReport) -> Self {
        self.0.schema_report = report;
        self
    }

    flag!(
        idempotent_writes,
        keyed_upsert,
        keyed_dedup,
        cleanup,
        rollback,
        schema_evolution,
        scoped_overwrite,
        direct_overwrite,
        staged_load,
        exactly_once,
        discover,
        shardable,
        range_read,
        range_digest,
        reports_lag,
        columnar,
        native,
        table_routing,
        read_side_effects,
        complete_reads,
    );

    /// Finish.
    pub fn build(self) -> Capabilities {
        self.0
    }
}

/// A connector's static description: its `type:` name and a pure function
/// from its config to its [`Capabilities`]. Registered with the connector's
/// factory so load-time gates can read capabilities without building it.
#[derive(Clone, Copy)]
#[non_exhaustive]
pub struct ConnectorDescriptor {
    kind: &'static str,
    capabilities: fn(&Value) -> Capabilities,
}

impl ConnectorDescriptor {
    /// Describe connector `kind` by `capabilities`, which must be pure (no
    /// I/O) and must agree with the built connector's `capabilities()`.
    pub const fn new(kind: &'static str, capabilities: fn(&Value) -> Capabilities) -> Self {
        Self { kind, capabilities }
    }

    /// The connector's `type:` name.
    pub fn kind(&self) -> &'static str {
        self.kind
    }

    /// The capabilities the connector has under `config` (a config object).
    pub fn capabilities(&self, config: &Value) -> Capabilities {
        (self.capabilities)(config)
    }

    /// The kind-level view: every capability some config of this connector
    /// enables — what listings and "supported: …" messages show. The
    /// descriptor function answers it when called with `Value::Null`.
    pub fn kind_capabilities(&self) -> Capabilities {
        (self.capabilities)(&Value::Null)
    }
}

impl std::fmt::Debug for ConnectorDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectorDescriptor")
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

/// A descriptor function for a source with no capabilities.
pub fn plain_source(_config: &Value) -> Capabilities {
    Capabilities::plain_source()
}

/// A descriptor function for an append-only sink.
pub fn append_only_sink(_config: &Value) -> Capabilities {
    Capabilities::append_only_sink()
}

/// Helpers descriptor functions use to read a raw config value leniently.
pub mod config {
    use super::*;

    /// The flattened [`WriteSpec`] of a sink config, or the default when the
    /// fields are absent or not yet interpolated.
    pub fn write_spec(config: &Value) -> WriteSpec {
        let mut spec = WriteSpec::default();
        if let Some(mode) = config.get("write_mode")
            && let Ok(m) = serde_json::from_value::<WriteMode>(mode.clone())
        {
            spec.write_mode = m;
        }
        if let Some(Value::Array(keys)) = config.get("key") {
            spec.key = keys
                .iter()
                .filter_map(|k| k.as_str().map(str::to_string))
                .collect();
        }
        spec
    }

    /// Whether the config dedups by key.
    pub fn dedups_by_key(config: &Value) -> bool {
        write_spec(config).dedups_by_key()
    }

    /// A string field, when present.
    pub fn str_field<'a>(config: &'a Value, field: &str) -> Option<&'a str> {
        config.get(field).and_then(Value::as_str)
    }

    /// A boolean field, or `default` when absent or not a boolean.
    pub fn bool_field(config: &Value, field: &str, default: bool) -> bool {
        config
            .get(field)
            .and_then(Value::as_bool)
            .unwrap_or(default)
    }

    /// Whether `field` is present and not null.
    pub fn is_set(config: &Value, field: &str) -> bool {
        config.get(field).is_some_and(|v| !v.is_null())
    }

    /// The tag of an enum field serialized as a string or as `{ "type": … }`
    /// / a single-key object (`{"auto_map": {…}}`).
    pub fn tag<'a>(config: &'a Value, field: &str) -> Option<&'a str> {
        match config.get(field)? {
            Value::String(s) => Some(s),
            Value::Object(o) => o
                .get("type")
                .and_then(Value::as_str)
                .or_else(|| (o.len() == 1).then(|| o.keys().next().map(String::as_str))?),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FaucetError;
    use async_trait::async_trait;
    use serde_json::json;

    struct Plain;
    #[async_trait]
    impl Source for Plain {
        async fn fetch_with_context(
            &self,
            _: &std::collections::HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(vec![])
        }
    }
    #[async_trait]
    impl Sink for Plain {
        async fn write_batch(&self, r: &[Value]) -> Result<usize, FaucetError> {
            Ok(r.len())
        }
    }

    struct Rich;
    #[async_trait]
    impl Source for Rich {
        async fn fetch_with_context(
            &self,
            _: &std::collections::HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(vec![])
        }
        fn supports_exactly_once(&self) -> bool {
            true
        }
        fn supports_discover(&self) -> bool {
            true
        }
        fn is_shardable(&self) -> bool {
            true
        }
        fn native_output_formats(&self) -> &'static [crate::native::NativeFormat] {
            &[crate::native::NativeFormat::NdJson]
        }
    }
    #[async_trait]
    impl Sink for Rich {
        async fn write_batch(&self, r: &[Value]) -> Result<usize, FaucetError> {
            Ok(r.len())
        }
        fn supported_write_modes(&self) -> &'static [WriteMode] {
            &[WriteMode::Overwrite, WriteMode::Upsert, WriteMode::Append]
        }
        fn supports_idempotent_writes(&self) -> bool {
            true
        }
        fn dedups_by_key(&self) -> bool {
            true
        }
        fn supports_cleanup(&self) -> bool {
            true
        }
        fn supports_rollback(&self) -> bool {
            true
        }
        fn supports_schema_evolution(&self) -> bool {
            true
        }
        fn supports_staged_load(&self) -> bool {
            true
        }
    }

    /// A descriptor built at run time answers like a `const` one, and the
    /// test connectors' read and write paths behave like real ones.
    #[tokio::test]
    async fn descriptors_built_at_run_time_and_the_test_connectors() {
        let d = ConnectorDescriptor::new("rich", |_| Sink::capabilities(&Rich));
        assert_eq!(d.kind(), "rich");
        assert!(d.kind_capabilities().idempotent_writes());
        let rows = [json!({"a": 1})];
        for sink in [&Plain as &dyn Sink, &Rich] {
            assert_eq!(sink.write_batch(&rows).await.unwrap(), 1);
        }
        for source in [&Plain as &dyn Source, &Rich] {
            assert!(source.fetch_all().await.unwrap().is_empty());
        }
    }

    #[test]
    fn plain_connectors_derive_nothing() {
        assert_eq!(Source::capabilities(&Plain), Capabilities::plain_source());
        assert_eq!(Sink::capabilities(&Plain), Capabilities::append_only_sink());
        assert_eq!(plain_source(&Value::Null), Capabilities::plain_source());
        assert_eq!(
            append_only_sink(&Value::Null),
            Capabilities::append_only_sink()
        );
        let s = Capabilities::append_only_sink();
        assert_eq!(s.sink_guarantee(), SinkGuarantee::AtLeastOnce);
        assert_eq!(s.replay_guarantee(), ReplayGuarantee::NonDeterministic);
        assert!(!s.overwrite());
        assert_eq!(s.schema_report(), SchemaReport::Undeclared);
    }

    #[test]
    fn derivation_reads_every_trait_method() {
        let src = Source::capabilities(&Rich);
        assert!(src.exactly_once() && src.discover() && src.shardable() && src.native());
        assert!(!src.columnar() && !src.range_read() && !src.range_digest());
        assert!(!src.reports_lag());
        assert_eq!(src.replay_guarantee(), ReplayGuarantee::Deterministic);

        let sink = Sink::capabilities(&Rich);
        assert_eq!(
            sink.write_modes(),
            &[WriteMode::Append, WriteMode::Upsert, WriteMode::Overwrite]
        );
        assert!(sink.idempotent_writes() && sink.keyed_upsert() && sink.keyed_dedup());
        assert!(sink.cleanup() && sink.rollback() && sink.schema_evolution());
        assert!(sink.staged_load() && sink.overwrite());
        assert!(!sink.scoped_overwrite() && !sink.direct_overwrite() && !sink.native());
        assert_eq!(sink.schema_report(), SchemaReport::Reported);
        assert_eq!(sink.sink_guarantee(), SinkGuarantee::AtomicWatermark);
    }

    #[test]
    fn decorated_keeps_inner_declarations_and_outer_derivations() {
        let inner = Capabilities::builder()
            .range_read(true)
            .range_digest(true)
            .reports_lag(true)
            .table_routing(true)
            .read_side_effects(true)
            .complete_reads(true)
            .schema_report(SchemaReport::DropsUnknownFields)
            .scoped_overwrite(true)
            .direct_overwrite(true)
            .discover(true)
            .build();
        let src = Capabilities::decorated_source(&Plain, &inner);
        assert!(src.range_read() && src.range_digest() && src.reports_lag());
        assert!(src.table_routing() && src.read_side_effects() && src.complete_reads());
        assert!(!src.discover(), "derived from the outer connector");
        let sink = Capabilities::decorated_sink(&Plain, &inner);
        assert_eq!(sink.schema_report(), SchemaReport::DropsUnknownFields);
        assert!(sink.scoped_overwrite() && sink.direct_overwrite());
        assert_eq!(sink.write_modes(), &[WriteMode::Append]);
    }

    #[test]
    fn builder_sets_every_flag_and_round_trips() {
        let c = Capabilities::builder()
            .write_modes([WriteMode::Delete, WriteMode::Append, WriteMode::Delete])
            .keyed_upsert(true)
            .scoped_overwrite(true)
            .direct_overwrite(true)
            .range_read(true)
            .range_digest(true)
            .reports_lag(true)
            .columnar(true)
            .schema_report(SchemaReport::DropsUnknownFields)
            .build();
        assert_eq!(c.write_modes(), &[WriteMode::Append, WriteMode::Delete]);
        assert!(c.supports_write_mode(WriteMode::Delete));
        assert!(c.scoped_overwrite() && c.direct_overwrite());
        assert!(c.range_read() && c.range_digest() && c.reports_lag() && c.columnar());
        assert_eq!(c.sink_guarantee(), SinkGuarantee::KeyedUpsert);
        assert_eq!(c.to_builder().build(), c);
        let off = c.to_builder().columnar(false).build();
        assert!(!off.columnar());
        assert!(!c.table_routing() && !c.read_side_effects());
        let cdc = c
            .to_builder()
            .table_routing(true)
            .read_side_effects(true)
            .build();
        assert!(cdc.table_routing() && cdc.read_side_effects());
        let v = serde_json::to_value(&c).unwrap();
        assert_eq!(v["schema_report"], "drops_unknown_fields");
        assert_eq!(v["write_modes"], json!(["append", "delete"]));
    }

    #[test]
    fn schema_report_names() {
        for (r, n) in [
            (SchemaReport::Undeclared, "undeclared"),
            (SchemaReport::Reported, "reported"),
            (SchemaReport::Schemaless, "schemaless"),
            (SchemaReport::DropsUnknownFields, "drops_unknown_fields"),
        ] {
            assert_eq!(r.as_str(), n);
            assert_eq!(serde_json::to_value(r).unwrap(), json!(n));
        }
    }

    fn rich(c: &Value) -> Capabilities {
        Capabilities::append_only_sink()
            .to_builder()
            .keyed_dedup(config::dedups_by_key(c))
            .build()
    }

    #[test]
    fn descriptor_answers_from_config() {
        const D: ConnectorDescriptor = ConnectorDescriptor::new("rich", rich);
        assert_eq!(D.kind(), "rich");
        assert_eq!(D.kind_capabilities(), Capabilities::append_only_sink());
        assert!(
            D.capabilities(&json!({"write_mode": "upsert", "key": ["id"]}))
                .keyed_dedup()
        );
        assert!(
            !D.capabilities(&json!({"write_mode": "upsert"}))
                .keyed_dedup()
        );
        assert!(!D.capabilities(&json!({"write_mode": "${x}"})).keyed_dedup());
        assert!(format!("{D:?}").contains("rich"));
    }

    #[test]
    fn config_helpers_are_lenient() {
        let c = json!({
            "write_mode": "delete", "key": ["a", 1], "s": "x", "b": true,
            "n": null, "m": {"auto_map": {}}, "t": {"type": "jsonb"}, "e": "plain"
        });
        let spec = config::write_spec(&c);
        assert_eq!(spec.write_mode, WriteMode::Delete);
        assert_eq!(spec.key, vec!["a".to_string()]);
        assert_eq!(config::str_field(&c, "s"), Some("x"));
        assert!(config::bool_field(&c, "b", false));
        assert!(config::bool_field(&c, "missing", true));
        assert!(!config::is_set(&c, "n") && config::is_set(&c, "s"));
        assert_eq!(config::tag(&c, "m"), Some("auto_map"));
        assert_eq!(config::tag(&c, "t"), Some("jsonb"));
        assert_eq!(config::tag(&c, "e"), Some("plain"));
        assert_eq!(config::tag(&c, "b"), None);
        assert_eq!(config::tag(&c, "missing"), None);
        assert_eq!(
            config::write_spec(&Value::Null).write_mode,
            WriteMode::Append
        );
    }
}
