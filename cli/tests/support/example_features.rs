//! Which shipped examples a test binary can load: an example that uses a
//! config key or transform gated on a feature this build lacks is skipped.
//! Shared by every test that walks `cli/examples/`, so they skip alike.

/// The feature `yaml` needs that this test binary was built without, if any.
pub fn missing_feature(yaml: &str) -> Option<&'static str> {
    let top =
        |key: &str| yaml.starts_with(&format!("{key}:")) || yaml.contains(&format!("\n{key}:"));
    let format = |f: &str| yaml.contains(&format!("format: {f}"));
    let checks: [(&str, bool, bool); 14] = [
        ("schedule", cfg!(feature = "schedule"), top("schedule")),
        (
            "masking",
            cfg!(feature = "masking"),
            top("masking") || yaml.contains("\n  masking:"),
        ),
        ("policy", cfg!(feature = "policy"), top("policy")),
        ("notify", cfg!(feature = "notify"), top("notifications")),
        ("catalog", cfg!(feature = "catalog"), top("catalog")),
        (
            "transform-sql",
            cfg!(feature = "transform-sql"),
            yaml.contains("type: sql"),
        ),
        (
            "transform-wasm",
            cfg!(feature = "transform-wasm"),
            yaml.contains("type: wasm"),
        ),
        ("lineage", cfg!(feature = "lineage"), top("lineage")),
        // Oracle loads Instant Client at runtime, so it is in `full` only.
        (
            "source-oracle / sink-oracle",
            cfg!(all(feature = "source-oracle", feature = "sink-oracle")),
            yaml.contains("type: oracle\n") || yaml.contains("type: oracle }"),
        ),
        (
            "file-format-excel",
            cfg!(feature = "file-format-excel"),
            format("xlsx"),
        ),
        (
            "file-format-xml",
            cfg!(feature = "file-format-xml"),
            format("xml"),
        ),
        (
            "file-format-avro",
            cfg!(feature = "file-format-avro"),
            format("avro"),
        ),
        (
            "file-format-orc",
            cfg!(feature = "file-format-orc"),
            format("orc"),
        ),
        (
            "source-oracle-cdc",
            cfg!(feature = "source-oracle-cdc"),
            yaml.contains("type: oracle-cdc"),
        ),
    ];
    checks
        .into_iter()
        .find(|(_, built, used)| !built && *used)
        .map(|(feature, _, _)| feature)
}
