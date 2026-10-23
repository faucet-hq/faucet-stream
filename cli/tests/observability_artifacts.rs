//! Metric-name lint for the shipped observability artifacts (issue #200).
//!
//! Every `faucet_*` metric referenced by the Grafana dashboards
//! (`observability/grafana/*.json`) and the Prometheus alert rules
//! (`observability/prometheus/alerts.yml`) must exist in the source tree —
//! a metric rename that misses the dashboards fails CI here instead of
//! silently blanking panels.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cli/ has a parent")
        .to_path_buf()
}

/// Collect `faucet_[a-z0-9_]+` tokens from a string.
fn metric_tokens(text: &str) -> BTreeSet<String> {
    let bytes = text.as_bytes();
    let mut out = BTreeSet::new();
    let needle = b"faucet_";
    let mut i = 0;
    while i + needle.len() <= bytes.len() {
        if &bytes[i..i + needle.len()] == needle
            && (i == 0 || !(bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_'))
        {
            let mut j = i + needle.len();
            while j < bytes.len()
                && (bytes[j].is_ascii_lowercase() || bytes[j].is_ascii_digit() || bytes[j] == b'_')
            {
                j += 1;
            }
            if j > i + needle.len() {
                out.insert(text[i..j].to_string());
            }
            i = j;
        } else {
            i += 1;
        }
    }
    out
}

/// Strip Prometheus histogram-exposition suffixes so `_bucket`/`_sum`/`_count`
/// references resolve to the base histogram name.
fn strip_histogram_suffix(name: &str) -> String {
    for suffix in ["_bucket", "_sum", "_count"] {
        if let Some(base) = name.strip_suffix(suffix) {
            return base.to_string();
        }
    }
    name.to_string()
}

/// Every metric name defined anywhere in the Rust source tree.
fn defined_metrics(root: &Path) -> BTreeSet<String> {
    let mut defined = BTreeSet::new();
    let dirs = [
        root.join("crates/core/src"),
        root.join("crates/lineage/src"),
        root.join("cli/src"),
    ];
    fn walk(dir: &Path, defined: &mut BTreeSet<String>) {
        for entry in std::fs::read_dir(dir).expect("read source dir") {
            let path = entry.expect("dir entry").path();
            if path.is_dir() {
                walk(&path, defined);
            } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
                let text = std::fs::read_to_string(&path).expect("read source file");
                defined.extend(metric_tokens(&text));
            }
        }
    }
    for dir in dirs {
        walk(&dir, &mut defined);
    }
    defined
}

/// Metric names referenced by an artifact file.
fn referenced_metrics(path: &Path) -> BTreeSet<String> {
    let text =
        std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    metric_tokens(&text)
        .into_iter()
        .map(|m| strip_histogram_suffix(&m))
        .collect()
}

#[test]
fn artifacts_exist_and_parse() {
    let root = repo_root();
    let alerts = root.join("observability/prometheus/alerts.yml");
    let parsed: serde_yaml_bw::Value =
        serde_yaml_bw::from_str(&std::fs::read_to_string(&alerts).expect("alerts.yml exists"))
            .expect("alerts.yml parses as YAML");
    let groups = parsed["groups"].as_sequence().expect("alert groups");
    assert!(groups.len() >= 3, "alert groups present");

    let dashboards: Vec<PathBuf> = std::fs::read_dir(root.join("observability/grafana"))
        .expect("grafana dir")
        .map(|e| e.expect("entry").path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("json"))
        .collect();
    assert!(dashboards.len() >= 4, "four dashboards ship");
    for d in &dashboards {
        let parsed: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(d).expect("dashboard readable"))
                .unwrap_or_else(|e| panic!("{} is not valid JSON: {e}", d.display()));
        assert!(
            parsed["uid"]
                .as_str()
                .unwrap_or_default()
                .starts_with("faucet-"),
            "{}: dashboard uid must be faucet-prefixed",
            d.display()
        );
        assert!(
            !parsed["panels"]
                .as_array()
                .unwrap_or(&Vec::new())
                .is_empty(),
            "{}: dashboard has panels",
            d.display()
        );
    }
}

#[test]
fn every_referenced_metric_exists_in_the_codebase() {
    let root = repo_root();
    let defined = defined_metrics(&root);
    assert!(
        defined.contains("faucet_pipeline_runs_total"),
        "sanity: definition scan found the core metrics"
    );

    let mut artifacts: Vec<PathBuf> = vec![root.join("observability/prometheus/alerts.yml")];
    artifacts.extend(
        std::fs::read_dir(root.join("observability/grafana"))
            .expect("grafana dir")
            .map(|e| e.expect("entry").path())
            .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("json")),
    );

    let mut unknown: Vec<String> = Vec::new();
    for artifact in &artifacts {
        for metric in referenced_metrics(artifact) {
            if !defined.contains(&metric) {
                unknown.push(format!(
                    "{} references unknown metric `{}`",
                    artifact.strip_prefix(&root).unwrap_or(artifact).display(),
                    metric
                ));
            }
        }
    }
    assert!(
        unknown.is_empty(),
        "observability artifacts reference metrics that do not exist in the codebase \
         (renamed without updating the dashboards/alerts?):\n  {}",
        unknown.join("\n  ")
    );
}

#[test]
fn histogram_suffixes_strip_correctly() {
    assert_eq!(
        strip_histogram_suffix("faucet_pipeline_run_duration_seconds_bucket"),
        "faucet_pipeline_run_duration_seconds"
    );
    assert_eq!(strip_histogram_suffix("faucet_x_sum"), "faucet_x");
    assert_eq!(strip_histogram_suffix("faucet_x_count"), "faucet_x");
    assert_eq!(
        strip_histogram_suffix("faucet_pipeline_runs_total"),
        "faucet_pipeline_runs_total"
    );
    // Token extraction respects word boundaries.
    let toks = metric_tokens(r#"rate(faucet_a_total[5m]) + prefaucet_b faucet_c"#);
    assert!(toks.contains("faucet_a_total"));
    assert!(toks.contains("faucet_c"));
    assert!(!toks.contains("faucet_b"), "prefixed token not a match");
}
