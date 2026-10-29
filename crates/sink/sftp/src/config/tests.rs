
use super::*;

#[test]
fn shared_writer_settings_follow_path_and_mode_rules() {
    let mut c = SftpSinkConfig::new(
        faucet_common_sftp::SftpConnectionConfig::with_password("h", "u", "p"),
        "/out",
    );
    let s = c.settings().unwrap();
    assert_eq!(s.format, faucet_core::FileFormat::JsonLines);
    assert!(s.object_per_flush);
    c.file_name = Some("d/part-{part}.txt".into());
    c.format = SftpSinkFormat::Auto;
    c.max_records_per_file = Some(7);
    let s = c.settings().unwrap();
    assert_eq!(s.format, faucet_core::FileFormat::RawText);
    assert_eq!(s.max_records_per_file, Some(7));
    assert!(c.validate().is_ok());
    c.file_name = Some("{part}-{part}.jsonl".into());
    assert!(
        c.validate()
            .unwrap_err()
            .to_string()
            .contains("more than one")
    );
    c.file_name = Some("x.unknownext".into());
    assert!(c.settings().is_err());
    c.file_name = None;
    c.format = SftpSinkFormat::JsonLines;
    c.write_mode = faucet_common_file::write::FileWriteMode::Overwrite;
    assert!(c.settings().unwrap_err().to_string().contains("need"));
    c.write_mode = faucet_common_file::write::FileWriteMode::Append;
    c.mode = faucet_common_file::write::IfExists::Append;
    assert!(c.validate().is_err());
    c.mode = faucet_common_file::write::IfExists::Replace;
    c.batch_size = 0;
    c.max_records_per_file = None;
    assert_eq!(c.settings().unwrap().max_records_per_file, None);
    assert_eq!(c.batch_atomicity(), faucet_core::BatchAtomicity::Atomic);
    c.max_records_per_file = Some(3);
    assert_eq!(c.batch_atomicity(), faucet_core::BatchAtomicity::BestEffort);
    let v: serde_json::Value = serde_json::to_value(&c).unwrap();
    assert!(v.get("parquet").is_some() && v.get("json_lines").is_some());
}
use faucet_common_sftp::SftpConnectionConfig;

fn conn() -> SftpConnectionConfig {
    SftpConnectionConfig::with_password("h", "u", "p")
}

#[test]
fn defaults() {
    let cfg = SftpSinkConfig::new(conn(), "/out");
    assert_eq!(cfg.path, "/out");
    assert_eq!(cfg.file_extension, ".jsonl");
    assert_eq!(cfg.batch_size, DEFAULT_BATCH_SIZE);
}

#[test]
fn deserializes_flat_shape() {
    let json = r#"{
            "host": "sftp.example.com",
            "username": "user",
            "type": "password",
            "config": { "password": "secret" },
            "path": "/upload",
            "batch_size": 0
        }"#;
    let cfg: SftpSinkConfig = serde_json::from_str(json).unwrap();
    assert_eq!(cfg.connection.host, "sftp.example.com");
    assert_eq!(cfg.path, "/upload");
    assert_eq!(cfg.batch_size, 0);
    assert_eq!(cfg.file_extension, ".jsonl");
}

#[test]
fn batch_size_zero_is_valid_sentinel() {
    let cfg = SftpSinkConfig::new(conn(), "/o").with_batch_size(0);
    assert!(faucet_core::validate_batch_size(cfg.batch_size).is_ok());
}

// ── file formats (#604) ───────────────────────────────────────────────

/// Every variant maps onto exactly one shared format, so what this sink
/// writes is what the file sources read back.
#[test]
fn every_format_maps_onto_the_shared_vocabulary() {
    assert_eq!(
        SftpSinkFormat::JsonLines.shared(),
        faucet_core::FileFormat::JsonLines
    );
    assert_eq!(
        SftpSinkFormat::JsonArray.shared(),
        faucet_core::FileFormat::JsonArray
    );
    #[cfg(feature = "file-format-csv")]
    assert_eq!(SftpSinkFormat::Csv.shared(), faucet_core::FileFormat::Csv);
    #[cfg(feature = "file-format-xml")]
    assert_eq!(SftpSinkFormat::Xml.shared(), faucet_core::FileFormat::Xml);
    #[cfg(feature = "file-format-excel")]
    assert_eq!(SftpSinkFormat::Xlsx.shared(), faucet_core::FileFormat::Xlsx);
}

#[test]
fn the_format_option_blocks_survive_the_builders() {
    let cfg = SftpSinkConfig::new(SftpConnectionConfig::with_password("h", "u", "p"), "/p")
        .format(SftpSinkFormat::JsonArray)
        .csv(faucet_core::CsvOptions {
            delimiter: ";".into(),
            has_headers: false,
            ..Default::default()
        })
        .excel(faucet_core::ExcelOptions {
            sheet: Some("Data".into()),
            header_row: 2,
        })
        .xml(faucet_core::XmlOptions {
            record_element: "row".into(),
            root_element: "rows".into(),
        });
    assert_eq!(cfg.format, SftpSinkFormat::JsonArray);
    let opts = cfg.format_options();
    assert_eq!(opts.csv.delimiter, ";");
    assert!(!opts.csv.has_headers);
    assert_eq!(opts.excel.sheet.as_deref(), Some("Data"));
    assert_eq!(opts.excel.header_row, 2);
    assert_eq!(opts.xml.record_element, "row");
    assert_eq!(opts.xml.root_element, "rows");
}

/// Both rollover caps are opt-in and independent (#618): setting one must
/// not disturb the other, or an operator asking for a byte cap silently
/// gets a record cap too.
#[test]
fn the_rollover_caps_are_independent_and_default_to_unset() {
    let base = SftpSinkConfig::new(conn(), "/out");
    assert_eq!(base.max_records_per_file, None);
    assert_eq!(base.max_bytes_per_file, None);

    let records_only = SftpSinkConfig::new(conn(), "/out").max_records_per_file(10);
    assert_eq!(records_only.max_records_per_file, Some(10));
    assert_eq!(records_only.max_bytes_per_file, None);

    let both = SftpSinkConfig::new(conn(), "/out")
        .max_records_per_file(10)
        .max_bytes_per_file(4096)
        .file_extension(".csv");
    assert_eq!(both.max_records_per_file, Some(10));
    assert_eq!(both.max_bytes_per_file, Some(4096));
    assert_eq!(both.file_extension, ".csv");
}

#[test]
fn batch_atomicity_matches_the_write_path() {
    let c: SftpSinkConfig = serde_json::from_value(serde_json::json!({"host": "sftp.example.com", "username": "user", "type": "password", "config": {"password": "secret"}, "path": "/upload"})).unwrap();
    assert_eq!(c.batch_atomicity(), faucet_core::BatchAtomicity::BestEffort);
}

#[cfg(feature = "file-format-avro")]
#[test]
fn the_avro_block_reaches_the_encoder() {
    let cfg = SftpSinkConfig::new(
        faucet_common_sftp::SftpConnectionConfig::with_password("h", "u", "p"),
        "/d",
    )
    .format(SftpSinkFormat::Avro)
    .avro(faucet_core::AvroOptions {
        schema: None,
        codec: faucet_core::AvroCodec::Snappy,
    });
    assert_eq!(cfg.format.shared(), faucet_core::FileFormat::Avro);
    assert_eq!(
        cfg.format_options().avro.codec,
        faucet_core::AvroCodec::Snappy
    );
}
