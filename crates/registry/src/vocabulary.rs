//! Deprecated connector kinds and what replaces each (#779).

/// Per deprecated kind: what it reads or writes, and what replaces it. The one
/// source of the deprecation wording on every surface.
const FILE_KIND_REPLACEMENTS: &[(&str, &str, &str)] = &[
    (
        "csv",
        "CSV file",
        "`type: file` with `format: csv` (dialect options under `csv:`)",
    ),
    (
        "jsonl",
        "JSON Lines file",
        "`type: file` with a `.jsonl` path (or `format: json_lines`)",
    ),
    (
        "parquet",
        "Parquet file",
        "`type: file` with `format: parquet`, or `type: s3` for an S3 location",
    ),
];

/// What replaces a deprecated file kind, or `None` for any other kind.
pub fn file_kind_replacement(kind: &str) -> Option<&'static str> {
    FILE_KIND_REPLACEMENTS
        .iter()
        .find(|(k, _, _)| *k == kind)
        .map(|(_, _, r)| *r)
}

/// The listing / schema-catalog description of a deprecated file kind.
pub fn deprecated_kind_description(kind: &str) -> Option<&'static str> {
    static LABELS: std::sync::LazyLock<Vec<(&'static str, String)>> =
        std::sync::LazyLock::new(|| {
            FILE_KIND_REPLACEMENTS
                .iter()
                .map(|(k, what, r)| (*k, format!("Deprecated {what} connector: use {r}.")))
                .collect()
        });
    LABELS
        .iter()
        .find(|(k, _)| *k == kind)
        .map(|(_, l)| l.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_deprecated_kind_has_a_replacement_and_a_description() {
        for kind in ["csv", "jsonl", "parquet"] {
            let r = file_kind_replacement(kind).unwrap();
            let d = deprecated_kind_description(kind).unwrap();
            assert!(d.starts_with("Deprecated ") && d.contains(r), "{d}");
        }
        assert_eq!(file_kind_replacement("file"), None);
        assert_eq!(deprecated_kind_description("file"), None);
    }
}
