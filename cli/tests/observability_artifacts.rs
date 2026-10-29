//! Metric-name lint for the shipped observability artifacts (issue #200).
//!
//! Every `faucet_*` metric referenced by the Grafana dashboards
//! (`observability/grafana/*.json`) and the Prometheus alert rules
//! (`observability/prometheus/alerts.yml`) must exist in the source tree —
//! a metric rename that misses the dashboards fails CI here instead of
//! silently blanking panels.
//!
//! The reverse holds too (#1004): every metric the code emits appears on a
//! dashboard, or in `observability/metrics-not-charted.txt` with a reason.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

#[path = "support/rust_source.rs"]
mod rust_source;

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

/// Every metric name defined in the Rust source tree: a `faucet_*` name in a
/// string literal of non-test code. Comments, doc comments and test modules
/// do not count, so a renamed metric whose old name survives in a comment
/// ("renamed from faucet_x") is not mistaken for a live one.
fn defined_metrics(root: &Path) -> BTreeSet<String> {
    let mut defined = BTreeSet::new();
    for_each_source(root, &mut |text| {
        for (_, lit) in rust_source::literals(rust_source::non_test(text)) {
            defined.extend(metric_tokens(lit));
        }
    });
    defined
}

/// The crates whose source emits metrics.
const METRIC_SOURCE_DIRS: [&str; 4] = [
    "crates/core/src",
    "crates/lineage/src",
    "crates/transform-wasm/src",
    "cli/src",
];

fn for_each_source(root: &Path, f: &mut dyn FnMut(&str)) {
    fn walk(dir: &Path, f: &mut dyn FnMut(&str)) {
        for entry in std::fs::read_dir(dir).expect("read source dir") {
            let path = entry.expect("dir entry").path();
            if path.is_dir() {
                walk(&path, f);
            } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
                f(&std::fs::read_to_string(&path).expect("read source file"));
            }
        }
    }
    for dir in METRIC_SOURCE_DIRS {
        walk(&root.join(dir), f);
    }
}

const METRIC_MACROS: [&str; 6] = [
    "describe_counter!",
    "describe_gauge!",
    "describe_histogram!",
    "counter!",
    "gauge!",
    "histogram!",
];

const METRIC_SUFFIXES: [&str; 6] = ["_total", "_seconds", "_bytes", "_ratio", "_ms", "_info"];

/// Byte ranges `(open, close)` of every metric-macro argument list in `src`,
/// skipping parentheses inside string literals.
fn macro_arg_spans(src: &str) -> Vec<(usize, usize)> {
    let strings: BTreeMap<usize, usize> = rust_source::literal_spans(src, true)
        .into_iter()
        .map(|(at, lit)| {
            let open = src[at..].find('"').map_or(at, |q| at + q);
            let start = lit.as_ptr() as usize - src.as_ptr() as usize;
            (open, start + lit.len())
        })
        .collect();
    let b = src.as_bytes();
    let mut spans = Vec::new();
    for name in METRIC_MACROS {
        let mut from = 0;
        while let Some(k) = src[from..].find(name) {
            let at = from + k;
            from = at + name.len();
            let ident_before = at > 0 && (b[at - 1].is_ascii_alphanumeric() || b[at - 1] == b'_');
            let rest = &src[from..];
            let Some(open) = rest
                .find('(')
                .filter(|&o| rest[..o].trim().is_empty())
                .map(|o| from + o)
            else {
                continue;
            };
            if ident_before {
                continue;
            }
            let mut depth = 0usize;
            let mut i = open;
            while i < b.len() {
                if let Some(&end) = strings.get(&i) {
                    i = end + 1;
                    continue;
                }
                match b[i] {
                    b'(' => depth += 1,
                    b')' => {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                    }
                    _ => {}
                }
                i += 1;
            }
            spans.push((open, i));
        }
    }
    spans
}

/// Metric names emitted by one source file: a `faucet_*` string literal in
/// non-test code that is an argument of a metric macro, or is named like a
/// metric (a unit suffix, for names passed through a constant or a helper).
/// SQL table and index names are neither.
fn emitted_metrics_in(text: &str) -> BTreeSet<String> {
    let src = rust_source::non_test(text);
    let spans = macro_arg_spans(src);
    let mut out = BTreeSet::new();
    for (at, lit) in rust_source::literal_spans(src, false) {
        let in_macro = spans.iter().any(|&(o, c)| at > o && at < c);
        for token in metric_tokens(lit) {
            let name = strip_histogram_suffix(&token);
            if in_macro || METRIC_SUFFIXES.iter().any(|s| name.ends_with(s)) {
                out.insert(name);
            }
        }
    }
    out
}

fn emitted_metrics(root: &Path) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for_each_source(root, &mut |text| out.extend(emitted_metrics_in(text)));
    out
}

fn dashboard_paths(root: &Path) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = std::fs::read_dir(root.join("observability/grafana"))
        .expect("grafana dir")
        .map(|e| e.expect("entry").path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("json"))
        .collect();
    out.sort();
    out
}

/// Parse `observability/metrics-not-charted.txt`: one `<metric>: <reason>`
/// per line; blank lines and `#` comments are ignored.
fn parse_exemptions(text: &str) -> Result<BTreeMap<String, String>, Vec<String>> {
    let mut out = BTreeMap::new();
    let mut errors = Vec::new();
    for (n, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((metric, reason)) = line.split_once(':') else {
            errors.push(format!("line {}: expected `<metric>: <reason>`", n + 1));
            continue;
        };
        let (metric, reason) = (metric.trim(), reason.trim());
        if metric_tokens(metric).first().map(String::as_str) != Some(metric) {
            errors.push(format!("line {}: `{metric}` is not a metric name", n + 1));
        } else if reason.is_empty() {
            errors.push(format!("line {}: `{metric}` has no reason", n + 1));
        } else if out.insert(metric.to_string(), reason.to_string()).is_some() {
            errors.push(format!("line {}: `{metric}` is listed twice", n + 1));
        }
    }
    if errors.is_empty() {
        Ok(out)
    } else {
        Err(errors)
    }
}

/// Every way the emitted, charted and exempt sets disagree.
fn coverage_problems(
    emitted: &BTreeSet<String>,
    charted: &BTreeSet<String>,
    exempt: &BTreeMap<String, String>,
) -> Vec<String> {
    let mut problems = Vec::new();
    for m in emitted {
        if !charted.contains(m) && !exempt.contains_key(m) {
            problems.push(format!(
                "`{m}` is emitted but on no dashboard: chart it, or list it in \
                 observability/metrics-not-charted.txt with a reason"
            ));
        }
    }
    for m in exempt.keys() {
        if !emitted.contains(m) {
            problems.push(format!(
                "`{m}` is listed as not charted but the code no longer emits it"
            ));
        } else if charted.contains(m) {
            problems.push(format!(
                "`{m}` is listed as not charted but a dashboard shows it"
            ));
        }
    }
    problems
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

/// A metric name that survives only in a comment, a doc comment or a test
/// module is not "defined"; one in a string literal of live code is.
#[test]
fn only_live_string_literals_define_a_metric() {
    let src = concat!(
        "//! renamed from faucet_old_doc_total\n",
        "// was faucet_old_comment_total\n",
        "/* faucet_old_block_total */\n",
        "fn f() { counter!(\"faucet_live_total\"); }\n",
        "#[cfg(test)]\nmod tests { const X: &str = \"faucet_test_only_total\"; }\n",
    );
    let found: BTreeSet<String> = rust_source::literals(rust_source::non_test(src))
        .into_iter()
        .flat_map(|(_, l)| metric_tokens(l))
        .collect();
    assert_eq!(found, BTreeSet::from(["faucet_live_total".to_string()]));
}

#[test]
fn artifacts_exist_and_parse() {
    let root = repo_root();
    let alerts = root.join("observability/prometheus/alerts.yml");
    let parsed: faucet_cli::yaml::Value =
        faucet_cli::yaml::from_str(&std::fs::read_to_string(&alerts).expect("alerts.yml exists"))
            .expect("alerts.yml parses as YAML");
    let groups = parsed["groups"].as_sequence().expect("alert groups");
    assert!(groups.len() >= 3, "alert groups present");

    let dashboards = dashboard_paths(&root);
    assert!(dashboards.len() >= 6, "six dashboards ship");
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
    artifacts.extend(dashboard_paths(&root));

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

#[test]
fn every_emitted_metric_is_charted_or_exempt() {
    let root = repo_root();
    let emitted = emitted_metrics(&root);
    assert!(
        emitted.contains("faucet_pipeline_runs_total")
            && emitted.contains("faucet_wasm_invocations_total")
            && emitted.contains("faucet_serve_template_sync_runs_total"),
        "sanity: the emission scan found literal, crate-external and constant-held names"
    );
    let charted: BTreeSet<String> = dashboard_paths(&root)
        .iter()
        .flat_map(|d| referenced_metrics(d))
        .collect();
    let path = root.join("observability/metrics-not-charted.txt");
    let text = std::fs::read_to_string(&path).expect("metrics-not-charted.txt exists");
    let exempt = parse_exemptions(&text).unwrap_or_else(|errors| {
        panic!(
            "observability/metrics-not-charted.txt is malformed:\n  {}",
            errors.join("\n  ")
        )
    });
    let problems = coverage_problems(&emitted, &charted, &exempt);
    assert!(
        problems.is_empty(),
        "dashboard coverage is out of date:\n  {}",
        problems.join("\n  ")
    );
}

#[test]
fn emission_scan_finds_metrics_and_skips_other_identifiers() {
    let src = concat!(
        "const SYNC: &str = \"faucet_sync_runs_total\";\n",
        "fn f() {\n",
        "    counter!(\"faucet_plain_gauge_like\", \"k\" => \"(v\").increment(1);\n",
        "    metrics::gauge!(\n        \"faucet_pending\"\n    ).set(1.0);\n",
        "    histogram!(self.name(), labels);\n",
        "    let ddl = \"CREATE TABLE faucet_serve_runs (id TEXT)\";\n",
        "    let t = \"faucet_idx_bucket\";\n",
        "    let d = \"faucet_x_seconds_bucket\";\n",
        "}\n",
        "#[cfg(test)]\n#[allow(dead_code)] // test helpers\nmod tests {\n",
        "    fn g() { counter!(\"faucet_test_only_total\"); }\n}\n",
    );
    let found = emitted_metrics_in(src);
    let want: BTreeSet<String> = [
        "faucet_sync_runs_total",
        "faucet_plain_gauge_like",
        "faucet_pending",
        "faucet_x_seconds",
    ]
    .into_iter()
    .map(String::from)
    .collect();
    assert_eq!(found, want);
}

#[test]
fn coverage_check_reports_every_disagreement() {
    let set = |v: &[&str]| -> BTreeSet<String> { v.iter().map(|s| s.to_string()).collect() };
    let emitted = set(&["faucet_a_total", "faucet_b_total", "faucet_c_total"]);
    let charted = set(&["faucet_a_total", "faucet_c_total"]);
    let exempt = parse_exemptions(
        "# comment\n\nfaucet_c_total: shown elsewhere\nfaucet_gone_total: renamed\n",
    )
    .expect("well-formed");
    let problems = coverage_problems(&emitted, &charted, &exempt);
    assert_eq!(problems.len(), 3, "{problems:#?}");
    assert!(problems[0].contains("`faucet_b_total` is emitted but on no dashboard"));
    assert!(
        problems[1].contains("`faucet_c_total`") && problems[1].contains("a dashboard shows it")
    );
    assert!(problems[2].contains("`faucet_gone_total`") && problems[2].contains("no longer emits"));

    let clean = parse_exemptions("faucet_b_total: per-run detail only\n").expect("well-formed");
    assert!(coverage_problems(&emitted, &charted, &clean).is_empty());
}

#[test]
fn exemption_file_lines_need_a_metric_and_a_reason() {
    let errors = parse_exemptions(
        "faucet_a_total\nfaucet_b_total:\nnot_a_metric: why\nfaucet_c_total: x\nfaucet_c_total: y\n",
    )
    .expect_err("malformed");
    assert_eq!(errors.len(), 4, "{errors:#?}");
    assert!(errors[0].starts_with("line 1:"));
    assert!(errors[1].contains("no reason"));
    assert!(errors[2].contains("not a metric name"));
    assert!(errors[3].contains("listed twice"));
}
