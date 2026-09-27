//! #465 Part 1 — cross-check the registry capability *allowlists* against the
//! trait methods on a real, built connector, so the two can never silently
//! drift.
//!
//! The gates in `expand.rs` / `topology.rs` trust the hand-maintained allowlists
//! in `registry.rs` (`IDEMPOTENT_SINK_KINDS`, `UPSERT_SINK_KINDS`,
//! `SCHEMA_EVOLUTION_SINK_KINDS`, `EXACTLY_ONCE_SOURCE_KINDS`,
//! `DISCOVER_SOURCE_KINDS`). The pipeline and each connector trust the trait
//! methods (`Sink::supports_idempotent_writes` / `supported_write_modes` /
//! `supports_schema_evolution`; `Source::supports_exactly_once` /
//! `supports_discover`). If those two ever disagree the worst case is a run that
//! passes the exactly-once gate but silently loses the guarantee — the worst bug
//! class in this project. This test builds each connector and asserts
//! allowlist == trait on every dimension.
//!
//! **Coverage model.** A connector whose `new()` opens a live connection eagerly
//! (the sqlx `.connect()` sinks, tiberius, Spanner, the Iceberg catalog, the CDC
//! sources) cannot be constructed in an offline unit test, so it is *skipped*
//! rather than failed. A `new()` that *panics* on the synthesized config (e.g.
//! a reqwest client hitting rustls' "no process-level CryptoProvider" when
//! `--all-features` pulls two crypto backends) is likewise isolated in a task
//! and treated as a skip. To guarantee the check never silently degrades to
//! probing nothing, [`MUST_CHECK_SINKS`] / [`MUST_CHECK_SOURCES`] name the
//! local-only connectors that build identically under every feature set (incl.
//! `sqlite`, which alone exercises all three sink capability dimensions) and the
//! test fails if any is *not* probed. Networked connectors are checked
//! best-effort. In CI the whole suite runs under `--all-features`, so every
//! compiled connector that can be built offline is exercised.

use std::collections::{HashMap, HashSet};

use faucet_cli::auth_catalog::AuthCatalog;
use faucet_cli::registry;
use serde_json::{Map, Value, json};
use std::time::Duration;

/// A connector whose `new()` tries to reach a live endpoint could block on a
/// connection attempt with the synthesized placeholder config. Bound every build
/// so such a connector is skipped (treated as un-probeable offline) instead of
/// stalling the suite under `--all-features`.
const BUILD_TIMEOUT: Duration = Duration::from_secs(4);

/// Sinks that MUST be probeable offline — the test fails if any is not probed,
/// so the parity check can never silently degrade to checking nothing.
///
/// Deliberately restricted to connectors with **no network/TLS client at all**
/// (local files / in-memory SQLite), so they build identically under every
/// feature set. A networked client (redis/mongodb/elasticsearch/…) can behave
/// differently under `--all-features` — e.g. a crypto backend pulled in by
/// another crate makes rustls panic on client construction, or an eager
/// connect fails without a server — so those are checked **best-effort** (when
/// they build) rather than required. `sqlite` alone exercises all three sink
/// capability dimensions (idempotent + upsert + schema-evolution), so the
/// tier-1 allowlist cross-check is still guaranteed.
const MUST_CHECK_SINKS: &[&str] = &["jsonl", "csv", "stdout", "sqlite"];

/// Sources that MUST be probeable offline (local-only — see [`MUST_CHECK_SINKS`]).
const MUST_CHECK_SOURCES: &[&str] = &["csv", "sqlite"];

/// Partial config overrides, deep-merged onto the schema-synthesized config
/// where the generic placeholder is not accepted by a connector's `new()` (a
/// scheme-specific URL, an enum variant the synthesizer can't guess). Keyed by
/// `"source:<kind>"` / `"sink:<kind>"`. Only the offending fields need setting.
fn config_override(key: &str) -> Option<Value> {
    let v = match key {
        "sink:sqlite" => json!({ "database_url": "sqlite::memory:" }),
        "source:sqlite" => json!({ "database_url": "sqlite::memory:", "query": "SELECT 1" }),
        "sink:mongodb" | "source:mongodb" => {
            json!({ "connection_uri": "mongodb://localhost:27017" })
        }
        "sink:redis" | "source:redis" => json!({ "url": "redis://localhost:6379" }),
        _ => return None,
    };
    Some(v)
}

/// Recursive deep-merge: object values merge key-by-key, everything else in
/// `overlay` replaces `base`.
fn deep_merge(base: &mut Value, overlay: Value) {
    match (base, overlay) {
        (Value::Object(b), Value::Object(o)) => {
            for (k, v) in o {
                deep_merge(b.entry(k).or_insert(Value::Null), v);
            }
        }
        (b, o) => *b = o,
    }
}

/// Synthesize a minimal, type-valid config instance from a connector's JSON
/// Schema: resolve `$ref`, take the first `enum`/`oneOf` variant, and fill every
/// `required` field with a field-name-aware placeholder. Enough to satisfy
/// `serde` + a lazily-constructing `new()`.
fn synthesize(schema: &Value, defs: &Map<String, Value>, field: &str, depth: u32) -> Value {
    if depth > 12 {
        return Value::Null;
    }
    if let Some(r) = schema.get("$ref").and_then(Value::as_str) {
        let name = r.rsplit('/').next().unwrap_or(r);
        return match defs.get(name) {
            Some(target) => synthesize(target, defs, field, depth + 1),
            None => Value::Null,
        };
    }
    if let Some(c) = schema.get("const") {
        return c.clone();
    }
    if let Some(first) = schema
        .get("enum")
        .and_then(Value::as_array)
        .and_then(|a| a.first())
    {
        return first.clone();
    }
    for combinator in ["oneOf", "anyOf", "allOf"] {
        if let Some(first) = schema
            .get(combinator)
            .and_then(Value::as_array)
            .and_then(|a| a.first())
        {
            return synthesize(first, defs, field, depth + 1);
        }
    }

    let ty = schema.get("type").and_then(|t| match t {
        Value::String(s) => Some(s.clone()),
        Value::Array(a) => a
            .iter()
            .filter_map(Value::as_str)
            .find(|s| *s != "null")
            .map(String::from),
        _ => None,
    });

    let is_object =
        ty.as_deref() == Some("object") || (ty.is_none() && schema.get("properties").is_some());
    if is_object {
        let props = schema
            .get("properties")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        let required: Vec<String> = schema
            .get("required")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .map(String::from)
                    .collect()
            })
            .unwrap_or_default();
        let mut obj = Map::new();
        for name in &required {
            if let Some(prop) = props.get(name) {
                obj.insert(name.clone(), synthesize(prop, defs, name, depth + 1));
            }
        }
        return Value::Object(obj);
    }

    match ty.as_deref() {
        Some("string") => Value::String(placeholder_string(field)),
        Some("integer") | Some("number") => json!(1),
        Some("boolean") => json!(false),
        Some("array") => json!([]),
        _ => Value::Null,
    }
}

/// A plausible placeholder for a required string field, keyed off its name so a
/// lazily-validating `new()` (URL / host parsing) still accepts it.
fn placeholder_string(field: &str) -> String {
    let f = field.to_ascii_lowercase();
    if f.contains("broker") || f.contains("bootstrap") {
        "localhost:9092".into()
    } else if f.contains("url") || f.contains("uri") || f.contains("endpoint") {
        "http://localhost:1/".into()
    } else if f.contains("host") {
        "localhost".into()
    } else if f.contains("path") || f.contains("file") {
        "/tmp/faucet_parity_probe".into()
    } else if f.contains("region") {
        "us-east-1".into()
    } else {
        "x".into()
    }
}

fn minimal_config(kind_key: &str, schema: &Value) -> Value {
    let empty = Map::new();
    let defs = schema
        .get("$defs")
        .or_else(|| schema.get("definitions"))
        .and_then(Value::as_object)
        .unwrap_or(&empty);
    let mut cfg = synthesize(schema, defs, "", 0);
    if let Some(overlay) = config_override(kind_key) {
        deep_merge(&mut cfg, overlay);
    }
    cfg
}

/// Try to build a sink and, if it builds, assert allowlist == trait on all three
/// sink dimensions. Returns whether it was actually probed.
async fn probe_sink(kind: &'static str) -> bool {
    let Ok(schema) = registry::sink_schema(kind) else {
        return false;
    };
    let cfg = minimal_config(&format!("sink:{kind}"), &schema);
    let offline_atomicity = registry::sink_batch_atomicity(kind, &cfg);
    // Isolate construction in a task: a `new()` that *panics* (e.g. a reqwest
    // client hitting rustls' "no process-level CryptoProvider" when
    // `--all-features` pulls in two crypto backends) surfaces as a `JoinError`
    // and is treated as "un-probeable offline" (skip), not a test failure. The
    // capability assertions run OUTSIDE the task, so a genuine allowlist-vs-trait
    // drift still panics the test as it should.
    let build = tokio::spawn(async move {
        let auth: AuthCatalog = HashMap::new();
        registry::build_sink(kind, cfg, &auth).await
    });
    let sink = match tokio::time::timeout(BUILD_TIMEOUT, build).await {
        Ok(Ok(Ok(s))) => s,
        _ => return false,
    };

    assert_eq!(
        registry::sink_supports_idempotent_writes(kind),
        sink.supports_idempotent_writes(),
        "{kind}: IDEMPOTENT_SINK_KINDS ({}) disagrees with Sink::supports_idempotent_writes ({})",
        registry::sink_supports_idempotent_writes(kind),
        sink.supports_idempotent_writes(),
    );
    assert_eq!(
        registry::sink_supports_schema_evolution(kind),
        sink.supports_schema_evolution(),
        "{kind}: SCHEMA_EVOLUTION_SINK_KINDS disagrees with Sink::supports_schema_evolution",
    );
    assert_eq!(
        registry::sink_supported_write_modes(kind),
        sink.supported_write_modes(),
        "{kind}: sink_supported_write_modes() disagrees with Sink::supported_write_modes()",
    );
    // #737: `faucet validate` decides `dlq_all` safety from the offline answer.
    assert_eq!(
        offline_atomicity,
        Some(sink.batch_atomicity()),
        "{kind}: registry::sink_batch_atomicity disagrees with Sink::batch_atomicity",
    );
    true
}

/// Every compiled sink kind answers `sink_batch_atomicity` from its typed
/// config without being built (#737) — the answer `faucet validate` uses to
/// refuse `on_batch_error: dlq_all` on a sink that can land part of a batch.
#[test]
fn every_sink_kind_declares_batch_atomicity_offline() {
    let mut answered = Vec::new();
    for kind in registry::sink_kinds() {
        let schema = registry::sink_schema(kind).expect("schema");
        let cfg = minimal_config(&format!("sink:{kind}"), &schema);
        if registry::sink_batch_atomicity(kind, &cfg).is_some() {
            answered.push(kind);
        }
    }
    for must in MUST_CHECK_SINKS {
        if registry::sink_kinds().contains(must) {
            assert!(answered.contains(must), "{must} must answer offline");
        }
    }
    assert_eq!(
        registry::sink_batch_atomicity("no-such-sink", &json!({})),
        None
    );
}

/// Try to build a source and, if it builds, assert allowlist == trait on both
/// source dimensions. Returns whether it was actually probed.
async fn probe_source(kind: &'static str) -> bool {
    let Ok(schema) = registry::source_schema(kind) else {
        return false;
    };
    let cfg = minimal_config(&format!("source:{kind}"), &schema);
    // See `probe_sink` — construction is isolated so a panic in `new()` skips
    // rather than fails; drift assertions stay outside the task.
    let build = tokio::spawn(async move {
        let auth: AuthCatalog = HashMap::new();
        registry::build_source(kind, cfg, &auth, None).await
    });
    let source = match tokio::time::timeout(BUILD_TIMEOUT, build).await {
        Ok(Ok(Ok(s))) => s,
        _ => return false,
    };

    assert_eq!(
        registry::source_supports_exactly_once(kind),
        source.supports_exactly_once(),
        "{kind}: EXACTLY_ONCE_SOURCE_KINDS disagrees with Source::supports_exactly_once",
    );
    assert_eq!(
        registry::source_supports_discover(kind),
        source.supports_discover(),
        "{kind}: DISCOVER_SOURCE_KINDS disagrees with Source::supports_discover",
    );
    true
}

#[tokio::test(flavor = "multi_thread")]
async fn sink_allowlists_match_trait_methods() {
    let mut checked: HashSet<&str> = HashSet::new();
    for kind in registry::sink_kinds() {
        if probe_sink(kind).await {
            checked.insert(kind);
        }
    }
    assert!(
        !checked.is_empty(),
        "no sinks were probed — the synthesizer is broken"
    );
    let compiled: HashSet<&str> = registry::sink_kinds().into_iter().collect();
    for must in MUST_CHECK_SINKS {
        if compiled.contains(must) {
            assert!(
                checked.contains(must),
                "sink '{must}' is compiled and must be probeable offline, but was not \
                 checked — its config synthesizer/override regressed (offline build broke)"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn source_allowlists_match_trait_methods() {
    let mut checked: HashSet<&str> = HashSet::new();
    for kind in registry::source_kinds() {
        if probe_source(kind).await {
            checked.insert(kind);
        }
    }
    assert!(
        !checked.is_empty(),
        "no sources were probed — the synthesizer is broken"
    );
    let compiled: HashSet<&str> = registry::source_kinds().into_iter().collect();
    for must in MUST_CHECK_SOURCES {
        if compiled.contains(must) {
            assert!(
                checked.contains(must),
                "source '{must}' is compiled and must be probeable offline, but was not \
                 checked — its config synthesizer/override regressed (offline build broke)"
            );
        }
    }
}

/// The derived capability helpers must agree with their base allowlists for
/// every allowlist member — a pure, instance-free consistency check that also
/// covers the connect-required connectors the offline probes skip.
#[test]
fn derived_capability_helpers_agree_with_allowlists() {
    for kind in registry::IDEMPOTENT_SINK_KINDS {
        assert!(registry::sink_supports_idempotent_writes(kind), "{kind}");
        assert_eq!(
            registry::sink_guarantee(kind),
            faucet_core::SinkGuarantee::AtomicWatermark,
            "{kind}: idempotent sink must derive AtomicWatermark"
        );
    }
    for kind in registry::UPSERT_SINK_KINDS {
        assert!(
            registry::sink_supported_write_modes(kind).contains(&faucet_core::WriteMode::Upsert),
            "{kind}: UPSERT_SINK_KINDS member must advertise Upsert"
        );
    }
    for kind in registry::EXACTLY_ONCE_SOURCE_KINDS {
        assert!(registry::source_supports_exactly_once(kind), "{kind}");
    }
    for kind in registry::DISCOVER_SOURCE_KINDS {
        assert!(registry::source_supports_discover(kind), "{kind}");
    }
}
