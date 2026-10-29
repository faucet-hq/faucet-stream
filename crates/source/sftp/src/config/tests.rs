
#[test]
fn glob_wildcards_match_names_holding_wildcard_characters() {
    assert!(glob_match("*.jsonl", "*daily.jsonl"));
    assert!(glob_match("*.jsonl", "a*b.jsonl"));
    assert!(glob_match("?x", "?x"));
    assert!(glob_match("a*", "a*"));
    assert!(!glob_match("*.jsonl", "*daily.csv"));
}

use super::*;
use faucet_common_sftp::SftpConnectionConfig;

fn conn() -> SftpConnectionConfig {
    SftpConnectionConfig::with_password("h", "u", "p")
}

#[test]
fn defaults() {
    let cfg = SftpSourceConfig::new(conn(), "/data");
    assert_eq!(cfg.path, "/data");
    assert!(cfg.glob.is_none());
    assert_eq!(cfg.format, SftpFormat::Jsonl);
    assert_eq!(cfg.batch_size, DEFAULT_BATCH_SIZE);
    assert_eq!(cfg.concurrency, 4);
}

#[test]
fn concurrency_is_settable_and_defaults_when_omitted() {
    // The field is new (#619); an existing config that does not mention it
    // must keep working and pick up the default rather than landing at 0,
    // which would mean "no reads in flight".
    let cfg = SftpSourceConfig::new(conn(), "/data").concurrency(12);
    assert_eq!(cfg.concurrency, 12);

    let parsed: SftpSourceConfig = serde_json::from_str(
        r#"{"host":"h","username":"u","type":"password",
                "config":{"password":"p"},"path":"/data"}"#,
    )
    .expect("a config without `concurrency` must still deserialize");
    assert_eq!(parsed.concurrency, 4);
}

#[test]
fn format_default_is_jsonl() {
    assert_eq!(SftpFormat::default(), SftpFormat::Jsonl);
}

#[test]
fn deserializes_flat_shape() {
    let json = r#"{
            "host": "sftp.example.com",
            "port": 2222,
            "username": "user",
            "type": "password",
            "config": { "password": "secret" },
            "path": "/incoming",
            "glob": "*.jsonl",
            "format": "jsonl",
            "batch_size": 250
        }"#;
    let cfg: SftpSourceConfig = serde_json::from_str(json).unwrap();
    assert_eq!(cfg.connection.host, "sftp.example.com");
    assert_eq!(cfg.connection.port, 2222);
    assert_eq!(cfg.path, "/incoming");
    assert_eq!(cfg.glob.as_deref(), Some("*.jsonl"));
    assert_eq!(cfg.batch_size, 250);
}

#[test]
fn batch_size_zero_is_valid_sentinel() {
    let cfg = SftpSourceConfig::new(conn(), "/d").with_batch_size(0);
    assert_eq!(cfg.batch_size, 0);
    assert!(faucet_core::validate_batch_size(cfg.batch_size).is_ok());
}

#[test]
fn format_parses_all_variants() {
    for (s, want) in [
        ("jsonl", SftpFormat::Jsonl),
        ("json_array", SftpFormat::JsonArray),
        ("raw_text", SftpFormat::RawText),
    ] {
        let got: SftpFormat = serde_json::from_str(&format!("\"{s}\"")).unwrap();
        assert_eq!(got, want);
    }
}

#[test]
fn glob_literal_and_wildcards() {
    assert!(glob_match("*.jsonl", "orders.jsonl"));
    assert!(glob_match("*.jsonl", ".jsonl"));
    assert!(!glob_match("*.jsonl", "orders.json"));
    assert!(glob_match("data-?.csv", "data-1.csv"));
    assert!(!glob_match("data-?.csv", "data-12.csv"));
    assert!(glob_match("*", "anything"));
    assert!(glob_match("exact.txt", "exact.txt"));
    assert!(!glob_match("exact.txt", "other.txt"));
    assert!(glob_match("a*b*c", "axxbyyc"));
    assert!(!glob_match("a*b*c", "axxbyy"));
}

/// Off-by-one boundary cases a backtracking matcher classically gets wrong
/// (empty inputs, exhausted input against a trailing `?`, pattern longer
/// than the name). Pure test hardening — the current matcher already passes.
#[test]
fn glob_boundary_cases() {
    assert!(glob_match("", ""), "empty pattern matches empty name");
    assert!(
        !glob_match("", "x"),
        "empty pattern must not match a non-empty name"
    );
    assert!(!glob_match("a?", "a"), "trailing `?` needs one more char");
    assert!(
        !glob_match("abc", "ab"),
        "pattern longer than name, no star"
    );
    assert!(glob_match("*", ""), "a lone star matches the empty name");
    assert!(glob_match("a*", "a"), "trailing star may match nothing");
    assert!(
        glob_match("**", ""),
        "consecutive stars collapse and match empty"
    );
    assert!(
        glob_match("?", "x"),
        "a single `?` matches exactly one char"
    );
    assert!(!glob_match("?", ""), "a single `?` needs a char");
}

// ── file formats (#604) ───────────────────────────────────────────────

/// Each variant maps onto the one shared format every other file
/// connector uses for the same bytes. The connector-owned shapes map to
/// `None` so they are never routed through the shared decoder — raw text
/// keeps this connector's own envelope.
#[cfg(any(
    feature = "file-format-csv",
    feature = "file-format-xml",
    feature = "file-format-excel",
    feature = "file-format-avro",
    feature = "file-format-orc"
))]
#[test]
fn formats_map_onto_the_shared_vocabulary_or_opt_out() {
    assert_eq!(
        SftpFormat::Jsonl.shared(),
        Some(faucet_core::FileFormat::JsonLines)
    );
    assert_eq!(
        SftpFormat::JsonArray.shared(),
        Some(faucet_core::FileFormat::JsonArray)
    );
    assert_eq!(SftpFormat::RawText.shared(), None);
    #[cfg(feature = "file-format-csv")]
    assert_eq!(SftpFormat::Csv.shared(), Some(faucet_core::FileFormat::Csv));
    #[cfg(feature = "file-format-xml")]
    assert_eq!(SftpFormat::Xml.shared(), Some(faucet_core::FileFormat::Xml));
    #[cfg(feature = "file-format-excel")]
    assert_eq!(
        SftpFormat::Xlsx.shared(),
        Some(faucet_core::FileFormat::Xlsx)
    );
    #[cfg(feature = "file-format-avro")]
    assert_eq!(
        SftpFormat::Avro.shared(),
        Some(faucet_core::FileFormat::Avro)
    );
    #[cfg(feature = "file-format-orc")]
    assert_eq!(SftpFormat::Orc.shared(), Some(faucet_core::FileFormat::Orc));
}

#[cfg(any(
    feature = "file-format-csv",
    feature = "file-format-xml",
    feature = "file-format-excel",
    feature = "file-format-avro",
    feature = "file-format-orc"
))]
#[test]
fn the_format_option_blocks_reach_the_decoder() {
    let mut cfg = SftpSourceConfig::new(SftpConnectionConfig::with_password("h", "u", "p"), "/p");
    cfg.csv = faucet_core::CsvOptions {
        delimiter: "\\t".into(),
        has_headers: false,
        ..Default::default()
    };
    cfg.excel = faucet_core::ExcelOptions {
        sheet: Some("Q3".into()),
        header_row: 1,
    };
    cfg.xml = faucet_core::XmlOptions {
        record_element: "order".into(),
        root_element: "orders".into(),
    };
    let opts = cfg.format_options();
    assert_eq!(opts.csv.delimiter_byte().expect("tab"), b'\t');
    assert!(!opts.csv.has_headers);
    assert_eq!(opts.excel.sheet.as_deref(), Some("Q3"));
    assert_eq!(opts.excel.header_row, 1);
    assert_eq!(opts.xml.record_element, "order");
}
