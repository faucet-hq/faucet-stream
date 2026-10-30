use super::*;
use faucet_common_sftp::SftpConnectionConfig;
use faucet_core::{Sink, WriteMode};

fn cfg() -> SftpSinkConfig {
    SftpSinkConfig::new(SftpConnectionConfig::with_password("h", "u", "p"), "/out")
}

#[test]
fn new_is_lazy_and_validates_batch_size() {
    assert!(SftpSink::new(cfg()).is_ok());
    let bad = cfg().with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
    assert!(matches!(SftpSink::new(bad), Err(FaucetError::Config(_))));
}

#[test]
fn files_land_under_the_directory() {
    let sink = SftpSink::new(cfg()).unwrap();
    let key = sink
        .inner
        .writer()
        .backend()
        .describe(faucet_common_file::write::Area::Destination, "f.jsonl");
    assert_eq!(key, "sftp://h:22/out/f.jsonl");
    let mut c = cfg();
    c.file_name = Some("dt/part-{part}.csv".into());
    c.format = crate::config::SftpSinkFormat::Auto;
    #[cfg(feature = "file-format-csv")]
    {
        let sink = SftpSink::new(c).unwrap();
        assert_eq!(sink.format(), FileFormat::Csv);
        let d = sink
            .inner
            .writer()
            .backend()
            .describe(faucet_common_file::write::Area::Destination, "x");
        assert_eq!(d, "sftp://h:22/out/dt/x");
    }
    #[cfg(not(feature = "file-format-csv"))]
    assert!(SftpSink::new(c).is_err());
}

#[test]
fn connector_name_is_sftp() {
    let sink = SftpSink::new(cfg()).unwrap();
    assert_eq!(sink.connector_name(), "sftp");
}

#[test]
fn dataset_uri_has_no_credentials() {
    let sink = SftpSink::new(cfg()).unwrap();
    assert_eq!(sink.dataset_uri(), "sftp://h:22/out");
    assert_eq!(sink.inner.backend().connection().host, "h");
    assert!(sink.config_schema().is_object());
}

#[test]
fn write_modes_and_capabilities() {
    let sink = SftpSink::new(cfg()).unwrap();
    assert!(!sink.supports_idempotent_writes());
    assert!(!sink.dedups_by_key());
    assert!(sink.supported_write_modes().contains(&WriteMode::Overwrite));
    assert!(!sink.is_overwrite());
}
