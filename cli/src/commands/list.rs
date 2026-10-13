//! `faucet list` — show every compiled-in source, sink, transform, and
//! state-store backend so users can discover what their binary supports.
//! `--available` instead lists every connector in the registry index (#208),
//! marking which are compiled into this binary.

use crate::cli::ListArgs;
use crate::conformance::tier_for;
use crate::error::CliResult;
use crate::registry::{sink_descriptions, sink_exists, source_descriptions, source_exists};
use crate::registry_index::RegistryIndex;
use crate::state::available_state_kinds;
#[cfg(feature = "quality")]
use crate::transforms::quality_descriptions;
use crate::transforms::transform_descriptions;

/// Execute the `list` subcommand.
pub async fn run(args: ListArgs) -> CliResult<()> {
    if args.available {
        return list_available(args);
    }
    if args.json {
        return run_json();
    }
    println!("Sources:");
    print_connectors(&source_descriptions(), true);
    println!();
    println!("Sinks:");
    print_connectors(&sink_descriptions(), false);
    println!();
    println!("Transforms:");
    print_two_column(&transform_descriptions());
    println!();
    #[cfg(feature = "quality")]
    {
        println!("Quality checks:");
        print_two_column(&quality_descriptions());
        println!();
    }
    println!("State stores: {}", available_state_kinds().join(", "));
    #[cfg(feature = "schedule")]
    println!("Scheduler:    compiled in (run `faucet schedule --help`, `faucet schema schedule`)");
    Ok(())
}

/// `faucet list --json` — the compiled-in connectors/transforms/state stores as
/// a single JSON object, so tooling and CI can consume the listing directly.
fn run_json() -> CliResult<()> {
    let out = build_list_json();
    println!(
        "{}",
        serde_json::to_string_pretty(&out).unwrap_or_else(|_| out.to_string())
    );
    Ok(())
}

/// Pure builder for the `faucet list --json` document (no I/O), so its shape can
/// be unit-tested.
fn build_list_json() -> serde_json::Value {
    let to_entries = |entries: &[(&'static str, &'static str)], is_source: Option<bool>| {
        entries
            .iter()
            .map(|(name, desc)| {
                let mut obj = serde_json::json!({ "name": name, "description": desc });
                if let Some(is_source) = is_source {
                    obj["tier"] = serde_json::json!(tier_for(name, is_source).label());
                    let caps = if is_source {
                        crate::registry::source_kind_capabilities(name)
                    } else {
                        crate::registry::sink_kind_capabilities(name)
                    };
                    obj["capabilities"] = serde_json::to_value(caps).unwrap_or_default();
                }
                obj
            })
            .collect::<Vec<_>>()
    };
    #[allow(unused_mut)]
    let mut out = serde_json::json!({
        "sources": to_entries(&source_descriptions(), Some(true)),
        "sinks": to_entries(&sink_descriptions(), Some(false)),
        "transforms": to_entries(&transform_descriptions(), None),
        "state_stores": available_state_kinds(),
    });
    #[cfg(feature = "quality")]
    {
        out["quality_checks"] = serde_json::json!(to_entries(&quality_descriptions(), None));
    }
    out
}

/// `faucet list --available` — every connector in the registry index, with a
/// marker for those already compiled into this binary.
fn list_available(args: ListArgs) -> CliResult<()> {
    let idx = RegistryIndex::load(args.index.as_deref())?;
    let mut connectors: Vec<_> = idx.connectors.iter().collect();
    connectors.sort_by(|a, b| {
        (a.kind.as_str(), a.name.as_str()).cmp(&(b.kind.as_str(), b.name.as_str()))
    });
    let compiled_for = |c: &crate::registry_index::ConnectorEntry| match c.kind.as_str() {
        "source" => source_exists(&c.name),
        "sink" => sink_exists(&c.name),
        _ => false,
    };
    if args.json {
        let rows: Vec<_> = connectors
            .iter()
            .map(|c| {
                serde_json::json!({
                    "kind": c.kind,
                    "name": c.name,
                    "description": c.description,
                    "tier": c.tier,
                    "verified": c.verified,
                    "compiled": compiled_for(c),
                })
            })
            .collect();
        let out = serde_json::json!({ "connectors": rows });
        println!(
            "{}",
            serde_json::to_string_pretty(&out).unwrap_or_else(|_| out.to_string())
        );
        return Ok(());
    }
    println!(
        "Registry connectors ({} total). ● = compiled into this binary, ○ = available via `faucet install`:\n",
        connectors.len()
    );
    for c in connectors {
        let mark = if compiled_for(c) { '●' } else { '○' };
        let badge = if c.verified { "verified" } else { "community" };
        let tier = c.tier.as_deref().unwrap_or("-");
        println!(
            "  {mark} {kind:<6} {name:<14} {tier:<12} {desc}  [{badge}]",
            kind = c.kind,
            name = c.name,
            desc = c.description
        );
    }
    Ok(())
}

fn print_two_column(entries: &[(&'static str, &'static str)]) {
    if entries.is_empty() {
        println!("  (none — rebuild faucet-cli with the relevant features enabled)");
        return;
    }
    let width = entries.iter().map(|(n, _)| n.len()).max().unwrap_or(0);
    for (name, desc) in entries {
        println!("  {name:<width$}  {desc}", width = width);
    }
}

/// Like [`print_two_column`] but prefixes each connector with its conformance
/// maturity tier badge (`faucet conformance` for the full scorecards).
fn print_connectors(entries: &[(&'static str, &'static str)], is_source: bool) {
    if entries.is_empty() {
        println!("  (none — rebuild faucet-cli with the relevant features enabled)");
        return;
    }
    let width = entries.iter().map(|(n, _)| n.len()).max().unwrap_or(0);
    for (name, desc) in entries {
        let tier = tier_for(name, is_source);
        println!(
            "  {badge} {name:<width$}  {tier:<12} {desc}",
            badge = tier.badge(),
            tier = tier.label(),
            width = width,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::build_list_json;

    #[test]
    fn list_json_has_expected_sections_and_builtins() {
        let v = build_list_json();
        // The four documented sections are present and are arrays.
        for key in ["sources", "sinks", "transforms", "state_stores"] {
            assert!(v[key].is_array(), "section `{key}` missing or not an array");
        }
        // Each connector entry carries name/description; sources/sinks add tier.
        let names = |key: &str| -> Vec<String> {
            v[key]
                .as_array()
                .unwrap()
                .iter()
                .map(|e| e["name"].as_str().unwrap_or_default().to_string())
                .collect()
        };
        // Default CLI build compiles in the `rest` source and `jsonl` sink.
        assert!(
            names("sources").iter().any(|n| n == "rest"),
            "sources: {:?}",
            names("sources")
        );
        assert!(
            names("sinks").iter().any(|n| n == "jsonl"),
            "sinks: {:?}",
            names("sinks")
        );
        assert!(
            v["sources"][0].get("tier").is_some(),
            "source entries should carry a tier"
        );
        // Every connector entry carries its declared capabilities (#876).
        let jsonl = v["sinks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["name"] == "jsonl")
            .unwrap();
        assert_eq!(
            jsonl["capabilities"]["write_modes"],
            serde_json::json!(["append"])
        );
        let rest = v["sources"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["name"] == "rest")
            .unwrap();
        assert_eq!(rest["capabilities"]["exactly_once"], false);
        // State stores always include the built-in memory + file backends.
        let stores: Vec<String> = v["state_stores"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s.as_str().unwrap_or_default().to_string())
            .collect();
        assert!(
            stores.iter().any(|s| s == "memory"),
            "state_stores: {stores:?}"
        );
    }
}
