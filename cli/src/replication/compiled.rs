//! Load-time validation of the `replication:` block against the pipeline.

use crate::config::{ConnectorSpec, PipelineConfig};
use crate::error::{CliError, CliResult};
use crate::replication::spec::{ReplicationSpec, TableOverride, TablesSpec};
use std::collections::BTreeMap;

const CDC_SOURCES: &str =
    "postgres-cdc / mysql-cdc / mssql-cdc / mongodb-cdc / oracle-cdc / dynamodb in `mode: streams`";

/// How a mirror's CDC source replays after the snapshot handoff.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CdcReplay {
    /// Replays from an exact position (exactly-once-capable source).
    Deterministic,
    /// Replays a retained window; converges only through a keyed upsert sink.
    Keyed,
}

fn cdc_replay(spec: &ConnectorSpec) -> Option<CdcReplay> {
    if crate::registry::source_supports_exactly_once(&spec.kind) {
        return Some(CdcReplay::Deterministic);
    }
    let streams = spec.config.get("mode").and_then(|m| m.as_str()) == Some("streams");
    (spec.kind == "dynamodb" && streams).then_some(CdcReplay::Keyed)
}

/// Validated replication config, ready for the orchestrator.
#[derive(Debug, Clone)]
pub struct CompiledReplication {
    /// The bulk-read snapshot source (phase 1).
    pub snapshot_source: ConnectorSpec,
    /// Keep streaming CDC after the snapshot completes.
    pub continuous: bool,
    /// Multi-table mode (#731), when `mirror.tables` is set.
    pub tables: Option<CompiledTables>,
}

/// The validated `mirror.tables` / `mirror.per_table` / snapshot fan-out.
#[derive(Debug, Clone)]
pub struct CompiledTables {
    pub spec: TablesSpec,
    pub per_table: BTreeMap<String, TableOverride>,
    /// Tables snapshotted in parallel.
    pub concurrency: usize,
    /// Primary-key ranges per table snapshot (`<= 1` = whole table).
    pub shards: usize,
    /// The CDC source kind.
    pub cdc_kind: String,
}

fn compile_tables(
    spec: &ReplicationSpec,
    cdc: &ConnectorSpec,
    sink: &ConnectorSpec,
) -> CliResult<Option<CompiledTables>> {
    let snapshot = &spec.snapshot;
    let Some(tables) = spec.tables.as_ref() else {
        if !spec.per_table.is_empty() {
            return Err(CliError::Config(
                "mirror.per_table needs mirror.tables (a multi-table mirror)".into(),
            ));
        }
        if snapshot.shards > 1 {
            return Err(CliError::Config(
                "mirror.snapshot.shards applies to a multi-table mirror (mirror.tables)".into(),
            ));
        }
        return Ok(None);
    };
    if !crate::replication::tables::MULTI_TABLE_CDC_KINDS.contains(&cdc.kind.as_str()) {
        return Err(CliError::Config(format!(
            "a multi-table mirror needs a CDC source that routes records to tables ({}); got '{}'",
            crate::replication::tables::MULTI_TABLE_CDC_KINDS.join(" / "),
            cdc.kind
        )));
    }
    if !crate::registry::source_supports_discover(&snapshot.source.kind) {
        return Err(CliError::Config(format!(
            "mirror.snapshot.source '{}' cannot discover tables; a multi-table mirror \
             resolves its table set from the snapshot source's discovery",
            snapshot.source.kind
        )));
    }
    if cdc.config.get("batch_size").and_then(|v| v.as_u64()) == Some(0) {
        return Err(CliError::Config(
            "a multi-table mirror needs per-transaction CDC pages — remove \
             `batch_size: 0` from pipeline.source"
                .into(),
        ));
    }
    if cdc.kind == "mongodb-cdc" {
        mongodb_scope_matches_snapshot(&cdc.config, &snapshot.source)?;
    }
    if tables.include.is_empty() {
        return Err(CliError::Config(
            "mirror.tables.include must name at least one glob".into(),
        ));
    }
    if snapshot.concurrency == 0 {
        return Err(CliError::Config(
            "mirror.snapshot.concurrency must be at least 1".into(),
        ));
    }
    if tables.destination.is_none()
        && crate::replication::tables::default_destination(&sink.kind).is_none()
    {
        return Err(CliError::Config(format!(
            "sink '{}' has no default per-table destination — set mirror.tables.destination \
             (string values may use {{table}}, {{table_name}}, {{schema}})",
            sink.kind
        )));
    }
    if let Some(d) = &tables.destination
        && !d.is_object()
    {
        return Err(CliError::Config(
            "mirror.tables.destination must be a mapping of sink-config fields".into(),
        ));
    }
    Ok(Some(CompiledTables {
        spec: tables.clone(),
        per_table: spec.per_table.clone(),
        concurrency: snapshot.concurrency,
        shards: snapshot.shards,
        cdc_kind: cdc.kind.clone(),
    }))
}

impl CompiledReplication {
    /// Validate every replication-specific requirement up front so
    /// `faucet validate` / `faucet replicate` fail fast with a clear message.
    /// The generic per-row gates (exactly-once, write_mode×sink) are enforced
    /// separately by [`crate::expand::expand`].
    pub fn compile(spec: &ReplicationSpec, cfg: &PipelineConfig) -> CliResult<Self> {
        // No matrix fan-out in v1 — replication is a single pipeline.
        if !cfg.matrix.is_empty() {
            return Err(CliError::Config(
                "mirror does not support a `matrix:` — define a single CDC \
                 pipeline (pipeline.source + pipeline.sink) plus replication.snapshot"
                    .into(),
            ));
        }
        // The main pipeline.source must be a capture-capable CDC source.
        let cdc = cfg.pipeline.source.as_ref().ok_or_else(|| {
            CliError::Config(format!(
                "mirror requires `pipeline.source` to be the CDC source ({CDC_SOURCES})"
            ))
        })?;
        let Some(replay) = cdc_replay(cdc) else {
            return Err(CliError::Config(format!(
                "mirror `pipeline.source` must be a CDC source ({CDC_SOURCES}); got '{}'",
                cdc.kind
            )));
        };
        // The snapshot source must be a non-CDC bulk reader, and must exist.
        let snap = &spec.snapshot.source;
        if cdc_replay(snap).is_some() {
            return Err(CliError::Config(format!(
                "mirror.snapshot.source must be a non-CDC bulk source \
                 (e.g. postgres / mysql / mongodb / oracle / dynamodb scan); got CDC source '{}'",
                snap.kind
            )));
        }
        crate::registry::source_schema(&snap.kind)?; // typed UnknownConnector if absent
        // A destination sink is required.
        let sink = cfg.pipeline.sink.as_ref().ok_or_else(|| {
            CliError::Config("mirror requires `pipeline.sink` (the destination)".into())
        })?;
        // A durable, shared state backend is required: the orchestrator seeds
        // the CDC bookmark and persists the phase marker, and the executor must
        // read them back. `memory` is per-instance (not shared) and would also
        // lose the phase marker on restart, defeating resumability.
        let state = cfg.pipeline.state.as_ref().ok_or_else(|| {
            CliError::Config("mirror requires a `state:` store (for the phase + bookmark)".into())
        })?;
        if state.kind == "memory" {
            return Err(CliError::Config(
                "mirror requires a durable state backend (file / redis / postgres), \
                 not `memory` — the snapshot→CDC handoff and resume depend on it"
                    .into(),
            ));
        }
        // Recommend upsert for a true mirror; warn (don't fail) otherwise.
        let write_mode = sink
            .config
            .get("write_mode")
            .and_then(|v| v.as_str())
            .unwrap_or("append");
        let tables = compile_tables(spec, cdc, sink)?;
        if replay == CdcReplay::Keyed && tables.is_none() {
            let has_key = sink
                .config
                .get("key")
                .and_then(|v| v.as_array())
                .is_some_and(|k| !k.is_empty());
            if !matches!(write_mode, "upsert" | "delete") || !has_key {
                return Err(CliError::Config(format!(
                    "mirror from '{}' requires a keyed sink (`write_mode: upsert` with a \
                     non-empty `key`): its change stream replays a retained window rather \
                     than a deterministic position, so only a keyed upsert converges",
                    cdc.kind
                )));
            }
        }
        if write_mode != "upsert" && tables.is_none() {
            tracing::warn!(
                write_mode,
                "mirror sink is not in upsert mode — the snapshot↔CDC boundary may \
                 produce duplicate rows; use write_mode: upsert (with a key) for a true mirror"
            );
        }
        Ok(Self {
            snapshot_source: snap.clone(),
            continuous: spec.continuous,
            tables,
        })
    }
}

/// A multi-table mongodb-cdc mirror must watch exactly the database the
/// snapshot discovers: its change records name tables by bare collection only
/// at `scope.type: database`, and only then do they match the snapshot's table
/// set. Any other scope routes every change to no table while the positions
/// still advance, so the changes would be lost.
fn mongodb_scope_matches_snapshot(
    cdc: &serde_json::Value,
    snapshot: &ConnectorSpec,
) -> CliResult<()> {
    let scope = cdc.pointer("/scope/type").and_then(|v| v.as_str());
    if scope != Some("database") {
        return Err(CliError::Config(format!(
            "a multi-table mongodb-cdc mirror watches one database: set \
             pipeline.source.config.scope to {{ type: database, database: <name> }} (got {})",
            scope.map_or("the default cluster scope".to_string(), |s| format!("scope.type: {s}"))
        )));
    }
    let watched = cdc.pointer("/scope/database").and_then(|v| v.as_str());
    let snapshotted = snapshot.config.get("database").and_then(|v| v.as_str());
    if snapshot.kind == "mongodb" && watched != snapshotted {
        return Err(CliError::Config(format!(
            "the mongodb-cdc scope watches database '{}' but mirror.snapshot.source reads \
             '{}'; both must name the same database",
            watched.unwrap_or(""),
            snapshotted.unwrap_or("")
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::parse_with_extension;

    fn cfg(yaml: &str) -> PipelineConfig {
        parse_with_extension(yaml, "yaml").unwrap()
    }

    const GOOD: &str = r#"
version: 1
name: mirror
pipeline:
  source: { type: postgres-cdc, config: { connection_url: "postgres://x", slot_name: s, publication_name: p } }
  sink:   { type: postgres, config: { connection_url: "postgres://y", table_name: t, column_mapping: auto_map, write_mode: upsert, key: [id] } }
  state:  { type: file, config: { path: ./st } }
replication:
  mode: snapshot_then_cdc
  snapshot:
    source: { type: postgres, config: { connection_url: "postgres://x", query: "SELECT * FROM t" } }
"#;

    #[test]
    fn accepts_valid_config() {
        let c = cfg(GOOD);
        let r = CompiledReplication::compile(c.replication.as_ref().unwrap(), &c).unwrap();
        assert_eq!(r.snapshot_source.kind, "postgres");
        assert!(r.continuous);
    }

    #[test]
    fn rejects_non_cdc_pipeline_source() {
        let c = cfg(&GOOD.replace("postgres-cdc", "postgres"));
        let err = CompiledReplication::compile(c.replication.as_ref().unwrap(), &c).unwrap_err();
        assert!(format!("{err}").contains("CDC source"), "{err}");
    }

    #[test]
    fn rejects_memory_state() {
        let c = cfg(&GOOD.replace(
            "type: file, config: { path: ./st }",
            "type: memory, config: {}",
        ));
        let err = CompiledReplication::compile(c.replication.as_ref().unwrap(), &c).unwrap_err();
        assert!(format!("{err}").contains("durable state"), "{err}");
    }

    #[test]
    fn rejects_cdc_snapshot_source() {
        let bad = GOOD.replace(
            "source: { type: postgres, config: { connection_url: \"postgres://x\", query: \"SELECT * FROM t\" } }",
            "source: { type: postgres-cdc, config: {} }",
        );
        let c = cfg(&bad);
        let err = CompiledReplication::compile(c.replication.as_ref().unwrap(), &c).unwrap_err();
        assert!(format!("{err}").contains("non-CDC"), "{err}");
    }

    #[test]
    fn rejects_matrix() {
        let bad = format!("{GOOD}matrix:\n  - id: a\n");
        let c = cfg(&bad);
        let err = CompiledReplication::compile(c.replication.as_ref().unwrap(), &c).unwrap_err();
        assert!(format!("{err}").contains("matrix"), "{err}");
    }

    #[test]
    fn rejects_missing_sink() {
        // Drop the `sink:` line entirely — `pipeline.sink` is then `None`.
        let bad = GOOD
            .lines()
            .filter(|l| !l.trim_start().starts_with("sink:"))
            .collect::<Vec<_>>()
            .join("\n");
        let c = cfg(&bad);
        let err = CompiledReplication::compile(c.replication.as_ref().unwrap(), &c).unwrap_err();
        assert!(format!("{err}").contains("sink"), "{err}");
    }

    #[test]
    fn rejects_missing_source() {
        // Drop the `source:` line — `pipeline.source` is then `None`.
        let bad = GOOD
            .lines()
            .filter(|l| !l.trim_start().starts_with("source: { type: postgres-cdc"))
            .collect::<Vec<_>>()
            .join("\n");
        let c = cfg(&bad);
        let err = CompiledReplication::compile(c.replication.as_ref().unwrap(), &c).unwrap_err();
        // The CDC-source error message names the CDC source requirement.
        assert!(format!("{err}").contains("CDC source"), "{err}");
    }

    #[test]
    fn rejects_unknown_snapshot_source_kind() {
        // A snapshot source kind that isn't a registered connector is rejected by
        // `registry::source_schema` (typed UnknownConnector).
        let bad = GOOD.replace(
            "source: { type: postgres, config: { connection_url: \"postgres://x\", query: \"SELECT * FROM t\" } }",
            "source: { type: not_a_source, config: {} }",
        );
        let c = cfg(&bad);
        let err = CompiledReplication::compile(c.replication.as_ref().unwrap(), &c).unwrap_err();
        assert!(format!("{err}").contains("not_a_source"), "{err}");
    }

    #[test]
    fn rejects_missing_state() {
        // Drop the `state:` line — `pipeline.state` is then `None`.
        let bad = GOOD
            .lines()
            .filter(|l| !l.trim_start().starts_with("state:"))
            .collect::<Vec<_>>()
            .join("\n");
        let c = cfg(&bad);
        let err = CompiledReplication::compile(c.replication.as_ref().unwrap(), &c).unwrap_err();
        assert!(format!("{err}").contains("state"), "{err}");
    }

    const DYNAMO: &str = r#"
version: 1
name: mirror
pipeline:
  source: { type: dynamodb, config: { table_name: orders, mode: streams, idle_termination_secs: 5 } }
  sink:   { type: postgres, config: { connection_url: "postgres://y", table_name: t, column_mapping: auto_map, write_mode: upsert, key: [id] } }
  state:  { type: file, config: { path: ./st } }
replication:
  mode: snapshot_then_cdc
  snapshot:
    source: { type: dynamodb, config: { table_name: orders, mode: scan } }
"#;

    #[test]
    fn accepts_dynamodb_streams_with_keyed_upsert_sink() {
        let c = cfg(DYNAMO);
        let r = CompiledReplication::compile(c.replication.as_ref().unwrap(), &c).unwrap();
        assert_eq!(r.snapshot_source.kind, "dynamodb");
    }

    #[test]
    fn rejects_dynamodb_streams_without_keyed_sink() {
        for bad in [
            DYNAMO.replace(", write_mode: upsert, key: [id]", ""),
            DYNAMO.replace("key: [id]", "key: []"),
        ] {
            let c = cfg(&bad);
            let err =
                CompiledReplication::compile(c.replication.as_ref().unwrap(), &c).unwrap_err();
            assert!(format!("{err}").contains("keyed sink"), "{err}");
        }
    }

    #[test]
    fn rejects_dynamodb_scan_as_cdc_source() {
        let bad = DYNAMO.replacen("mode: streams, idle_termination_secs: 5", "mode: scan", 1);
        let c = cfg(&bad);
        let err = CompiledReplication::compile(c.replication.as_ref().unwrap(), &c).unwrap_err();
        assert!(format!("{err}").contains("CDC source"), "{err}");
    }

    #[test]
    fn rejects_dynamodb_streams_snapshot_source() {
        let bad = DYNAMO.replace(
            "{ table_name: orders, mode: scan }",
            "{ table_name: orders, mode: streams }",
        );
        let c = cfg(&bad);
        let err = CompiledReplication::compile(c.replication.as_ref().unwrap(), &c).unwrap_err();
        assert!(format!("{err}").contains("non-CDC"), "{err}");
    }

    const ORACLE: &str = r#"
version: 1
name: mirror
pipeline:
  source: { type: oracle-cdc, config: { host: db, service_name: ORCLPDB1, username: u, password: p, tables: [APP.ORDERS] } }
  sink:   { type: postgres, config: { connection_url: "postgres://y", table_name: t, column_mapping: auto_map, write_mode: upsert, key: [ID] } }
  state:  { type: file, config: { path: ./st } }
replication:
  mode: snapshot_then_cdc
  snapshot:
    source: { type: oracle, config: { host: db, service_name: ORCLPDB1, username: u, password: p, query: "SELECT * FROM APP.ORDERS" } }
"#;

    #[cfg(feature = "source-oracle")]
    #[test]
    fn accepts_oracle_cdc_with_oracle_snapshot() {
        let c = cfg(ORACLE);
        let r = CompiledReplication::compile(c.replication.as_ref().unwrap(), &c).unwrap();
        assert_eq!(r.snapshot_source.kind, "oracle");
    }

    #[test]
    fn rejects_oracle_cdc_as_snapshot_and_names_it_in_errors() {
        let bad = ORACLE.replace("source: { type: oracle,", "source: { type: oracle-cdc,");
        let c = cfg(&bad);
        let err = CompiledReplication::compile(c.replication.as_ref().unwrap(), &c).unwrap_err();
        assert!(format!("{err}").contains("non-CDC"), "{err}");

        let c = cfg(&GOOD.replace("postgres-cdc", "postgres"));
        let err = CompiledReplication::compile(c.replication.as_ref().unwrap(), &c).unwrap_err();
        assert!(format!("{err}").contains("oracle-cdc"), "{err}");
    }

    const TABLES: &str = r#"
version: 1
name: shop
pipeline:
  source: { type: postgres-cdc, config: { connection_url: "postgres://x", slot_name: s, publication_name: p } }
  sink:   { type: postgres, config: { connection_url: "postgres://y", column_mapping: auto_map } }
  state:  { type: file, config: { path: ./st } }
mirror:
  mode: snapshot_then_cdc
  snapshot:
    source: { type: postgres, config: { connection_url: "postgres://x", query: "SELECT 1" } }
    concurrency: 2
    shards: 4
  tables:
    include: ["public.*"]
  per_table:
    public.orders: { key: [order_id] }
"#;

    fn compile_err(yaml: &str) -> String {
        let c = cfg(yaml);
        CompiledReplication::compile(c.replication.as_ref().unwrap(), &c)
            .unwrap_err()
            .to_string()
    }

    #[test]
    fn accepts_a_multi_table_mirror() {
        let c = cfg(TABLES);
        let r = CompiledReplication::compile(c.replication.as_ref().unwrap(), &c).unwrap();
        let t = r.tables.expect("tables mode");
        assert_eq!(t.concurrency, 2);
        assert_eq!(t.shards, 4);
        assert_eq!(t.cdc_kind, "postgres-cdc");
        assert!(t.per_table.contains_key("public.orders"));
        assert!(
            CompiledReplication::compile(cfg(GOOD).replication.as_ref().unwrap(), &cfg(GOOD))
                .unwrap()
                .tables
                .is_none()
        );
    }

    #[test]
    fn multi_table_gates() {
        let e = compile_err(&TABLES.replace("  tables:\n    include: [\"public.*\"]\n", ""));
        assert!(e.contains("per_table needs mirror.tables"), "{e}");
        let e = compile_err(
            &TABLES
                .replace("  tables:\n    include: [\"public.*\"]\n", "")
                .replace("  per_table:\n    public.orders: { key: [order_id] }\n", ""),
        );
        assert!(e.contains("shards applies"), "{e}");
        let e = compile_err(&TABLES.replace(
            "type: postgres, config: { connection_url: \"postgres://x\", query",
            "type: rest, config: { url: \"https://a\", query",
        ));
        assert!(e.contains("cannot discover"), "{e}");
        let e = compile_err(&TABLES.replace(
            "publication_name: p }",
            "publication_name: p, batch_size: 0 }",
        ));
        assert!(e.contains("batch_size: 0"), "{e}");
        let e = compile_err(&TABLES.replace("include: [\"public.*\"]", "include: []"));
        assert!(e.contains("at least one glob"), "{e}");
        let e = compile_err(&TABLES.replace("concurrency: 2", "concurrency: 0"));
        assert!(e.contains("concurrency"), "{e}");
        let e = compile_err(&TABLES.replace(
            "sink:   { type: postgres, config: { connection_url: \"postgres://y\", column_mapping: auto_map } }",
            "sink:   { type: jsonl, config: { path: out.jsonl } }",
        ));
        assert!(e.contains("no default per-table destination"), "{e}");
        let e = compile_err(&TABLES.replace(
            "include: [\"public.*\"]",
            "include: [\"public.*\"]\n    destination: nope",
        ));
        assert!(e.contains("mapping"), "{e}");
    }

    #[test]
    fn multi_table_cdc_sources_must_route_by_table() {
        let kafka = TABLES.replace(
            "{ type: postgres-cdc, config: { connection_url: \"postgres://x\", slot_name: s, publication_name: p } }",
            "{ type: kafka, config: { brokers: \"b:9092\", topics: [t], group_id: g } }",
        );
        let e = compile_err(&kafka);
        assert!(e.contains("routes records to tables"), "{e}");
        let mongo = TABLES.replace(
            "{ type: postgres-cdc, config: { connection_url: \"postgres://x\", slot_name: s, publication_name: p } }",
            "{ type: mongodb-cdc, config: { connection_uri: \"mongodb://m\", scope: { type: collection, database: d, collection: c } } }",
        );
        let e = compile_err(&mongo);
        assert!(e.contains("watches one database"), "{e}");
    }

    #[test]
    fn a_mongodb_cdc_mirror_needs_the_snapshot_database_scope() {
        let mongo = |scope: &str, db: &str| {
            TABLES
                .replace(
                    "{ type: postgres-cdc, config: { connection_url: \"postgres://x\", slot_name: s, publication_name: p } }",
                    &format!("{{ type: mongodb-cdc, config: {{ connection_uri: \"mongodb://m\"{scope} }} }}"),
                )
                .replace(
                    "{ type: postgres, config: { connection_url: \"postgres://x\", query: \"SELECT 1\" } }",
                    &format!("{{ type: mongodb, config: {{ connection_uri: \"mongodb://m\", database: {db}, collection: c }} }}"),
                )
        };
        let e = compile_err(&mongo("", "shop"));
        assert!(e.contains("the default cluster scope"), "{e}");
        let e = compile_err(&mongo(", scope: { type: cluster }", "shop"));
        assert!(e.contains("scope.type: cluster"), "{e}");
        let e = compile_err(&mongo(", scope: { type: database, database: crm }", "shop"));
        assert!(e.contains("'crm'") && e.contains("'shop'"), "{e}");
        let ok = cfg(&mongo(", scope: { type: database, database: shop }", "shop"));
        let r = CompiledReplication::compile(ok.replication.as_ref().unwrap(), &ok).unwrap();
        assert_eq!(r.tables.expect("tables mode").cdc_kind, "mongodb-cdc");
    }

    #[test]
    fn non_upsert_sink_compiles_ok_with_warning() {
        // A sink with `write_mode: append` (or no write_mode) still compiles —
        // `compile` only warns (it does not fail) so a non-mirror replication is
        // allowed. This exercises the warn branch.
        let appendish = GOOD.replace(", write_mode: upsert, key: [id]", "");
        let c = cfg(&appendish);
        let r = CompiledReplication::compile(c.replication.as_ref().unwrap(), &c)
            .expect("non-upsert sink should still compile (warn, not fail)");
        assert_eq!(r.snapshot_source.kind, "postgres");
    }
}
