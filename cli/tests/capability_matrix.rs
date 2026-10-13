//! #465 Part 3 / #876 — the docs-site connector capability matrix is
//! *generated* from the capabilities each connector declares
//! (`faucet conformance --matrix`), not hand-maintained. This test fails if the
//! committed copy drifts from what the generator emits, so a change to a
//! connector's declared capabilities forces regenerating the doc in the same PR.

use faucet_cli::registry;
use faucet_cli::registry_index::RegistryIndex;

const COMMITTED: &str = include_str!("../../docs/book/src/reference/capability-matrix.md");

fn every_builtin_compiled() -> bool {
    RegistryIndex::embedded()
        .connectors
        .iter()
        .filter(|c| c.verified)
        .all(|c| match c.kind.as_str() {
            "source" => registry::source_exists(&c.name),
            "sink" => registry::sink_exists(&c.name),
            _ => true,
        })
}

/// With every built-in compiled in (CI's `--all-features`), the committed
/// matrix must be byte-identical to the generator's output. A slimmer build
/// renders a subset: each of its per-connector rows must appear verbatim.
#[test]
fn committed_matrix_matches_generated() {
    let generated = faucet_cli::conformance::capability_matrix_markdown();
    let hint = "docs/book/src/reference/capability-matrix.md is stale — regenerate it:\n  \
         cargo run -p faucet-cli --all-features -- conformance --matrix > \
         docs/book/src/reference/capability-matrix.md";
    if every_builtin_compiled() {
        assert_eq!(COMMITTED, generated, "{hint}");
        return;
    }
    let per_connector = generated
        .split("## Exactly-once delivery")
        .next()
        .unwrap_or_default();
    for line in per_connector.lines().filter(|l| l.starts_with("| `")) {
        assert!(COMMITTED.contains(line), "{hint}\nmissing row: {line}");
    }
}
