use super::*;
use faucet_common_sftp::SftpConnectionConfig;
use faucet_core::Source;

#[test]
fn a_file_over_max_object_bytes_is_refused_by_name() {
    assert!(check_file_size(64, 64, "/d/a.json").is_ok());
    let err = check_file_size(65, 64, "/d/a.json")
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("/d/a.json") && err.contains("max_object_bytes"),
        "{err}"
    );
}

#[test]
fn max_object_bytes_defaults_to_the_shared_ceiling() {
    assert_eq!(
        cfg().max_object_bytes,
        faucet_core::file_format::DEFAULT_MAX_OBJECT_BYTES
    );
    let mut doc = serde_json::to_value(cfg()).unwrap();
    doc.as_object_mut().unwrap().remove("max_object_bytes");
    let parsed: SftpSourceConfig = serde_json::from_value(doc.clone()).unwrap();
    assert_eq!(
        parsed.max_object_bytes,
        faucet_core::file_format::DEFAULT_MAX_OBJECT_BYTES
    );
    doc["max_object_bytes"] = serde_json::json!(10);
    let parsed: SftpSourceConfig = serde_json::from_value(doc).unwrap();
    assert_eq!(parsed.max_object_bytes, 10);
}

fn cfg() -> SftpSourceConfig {
    SftpSourceConfig::new(SftpConnectionConfig::with_password("h", "u", "p"), "/data")
}

#[test]
fn new_is_lazy_and_validates_batch_size() {
    // Valid config constructs with no I/O.
    assert!(SftpSource::new(cfg()).is_ok());
    // Out-of-range batch size is rejected up front.
    let bad = cfg().with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
    assert!(matches!(SftpSource::new(bad), Err(FaucetError::Config(_))));
    let mut empty = cfg();
    empty.parquet.columns = Some(vec![]);
    match SftpSource::new(empty) {
        Err(FaucetError::Config(m)) => assert!(m.contains("parquet.columns"), "{m}"),
        Err(e) => panic!("expected a Config error, got {e}"),
        Ok(_) => panic!("an empty projection must be refused"),
    }
}

#[test]
fn connector_name_is_sftp() {
    let src = SftpSource::new(cfg()).unwrap();
    assert_eq!(src.connector_name(), "sftp");
}

#[test]
fn dataset_uri_has_no_credentials() {
    let src = SftpSource::new(cfg()).unwrap();
    let uri = src.dataset_uri();
    assert_eq!(uri, "sftp://h:22/data");
    assert!(!uri.contains('p') || !uri.contains("password"));
}

#[test]
fn config_schema_is_valid_object() {
    let src = SftpSource::new(cfg()).unwrap();
    let schema = src.config_schema();
    assert!(schema.is_object());
    assert!(schema.get("properties").is_some());
}

#[test]
fn listing_limits_default_and_refuse_zero_entries() {
    let mut doc = serde_json::to_value(cfg()).unwrap();
    doc.as_object_mut().unwrap().remove("max_listed_entries");
    doc.as_object_mut().unwrap().remove("list_timeout_secs");
    let parsed: SftpSourceConfig = serde_json::from_value(doc).unwrap();
    assert_eq!(parsed.max_listed_entries, 1_000_000);
    assert_eq!(parsed.list_timeout_secs, 300);

    let mut zero = cfg();
    zero.max_listed_entries = 0;
    match SftpSource::new(zero) {
        Err(FaucetError::Config(m)) => assert!(m.contains("max_listed_entries"), "{m}"),
        Err(e) => panic!("expected a Config error, got {e}"),
        Ok(_) => panic!("a zero entry limit must be refused"),
    }
}

#[test]
fn a_listing_that_runs_out_of_time_names_list_timeout_secs() {
    let limit = std::time::Duration::from_secs(7);
    let timed_out = FaucetError::Timeout {
        operation: "list call".into(),
        after: limit,
    };
    let err = listing_timeout(timed_out, "/data", limit).to_string();
    assert!(
        err.contains("'/data'") && err.contains("7s") && err.contains("list_timeout_secs"),
        "{err}"
    );
    let other = listing_timeout(FaucetError::Source("dummy".into()), "/data", limit);
    assert!(matches!(other, FaucetError::Source(m) if m == "dummy"));
}

#[test]
fn listing_errors_become_source_errors() {
    let err = source_err(faucet_common_sftp::ListError::TooManyEntries {
        path: "/data".into(),
        limit: 2,
    });
    assert!(matches!(&err, FaucetError::Source(m) if m.contains("max_listed_entries")));
}

#[cfg(feature = "arrow")]
#[test]
fn ranged_reads_grow_with_the_bytes_received() {
    assert_eq!(initial_range_capacity(0), 0);
    assert_eq!(initial_range_capacity(1_000), 1_000);
    assert_eq!(initial_range_capacity(u64::MAX), 8 * 1024 * 1024);
}

/// A fake SFTP server serving one file, answering each `READ` with at most
/// `cap` bytes and recording every requested read length.
#[cfg(feature = "arrow")]
async fn serve_file(
    mut io: tokio::io::DuplexStream,
    file: Vec<u8>,
    cap: usize,
    reads: std::sync::Arc<std::sync::Mutex<Vec<u32>>>,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    fn frame(body: Vec<u8>) -> Vec<u8> {
        let mut out = (body.len() as u32).to_be_bytes().to_vec();
        out.extend(body);
        out
    }
    fn status(id: &[u8], code: u32) -> Vec<u8> {
        let mut b = vec![101];
        b.extend_from_slice(id);
        b.extend_from_slice(&code.to_be_bytes());
        b.extend_from_slice(&[0; 8]);
        frame(b)
    }
    let u32_at = |b: &[u8], at: usize| u32::from_be_bytes(b[at..at + 4].try_into().unwrap());
    while let Ok(len) = io.read_u32().await {
        let mut body = vec![0u8; len as usize];
        if io.read_exact(&mut body).await.is_err() {
            return;
        }
        let id = body[1..5].to_vec();
        let reply = match body[0] {
            1 => frame(vec![2, 0, 0, 0, 3]),
            3 => {
                let mut b = vec![102];
                b.extend_from_slice(&id);
                b.extend_from_slice(&1u32.to_be_bytes());
                b.push(b'h');
                frame(b)
            }
            5 => {
                let handle_len = u32_at(&body, 5) as usize;
                let at = 9 + handle_len;
                let offset = u64::from_be_bytes(body[at..at + 8].try_into().unwrap()) as usize;
                let want = u32_at(&body, at + 8);
                reads.lock().unwrap().push(want);
                if offset >= file.len() {
                    status(&id, 1)
                } else {
                    let end = file.len().min(offset + (want as usize).min(cap));
                    let mut b = vec![103];
                    b.extend_from_slice(&id);
                    b.extend_from_slice(&((end - offset) as u32).to_be_bytes());
                    b.extend_from_slice(&file[offset..end]);
                    frame(b)
                }
            }
            _ => status(&id, 0),
        };
        if io.write_all(&reply).await.is_err() {
            return;
        }
    }
}

#[cfg(feature = "arrow")]
#[tokio::test]
async fn ranged_reads_after_the_footer_use_full_size_reads() {
    use faucet_core::file_format::parquet_io::RangeRead;
    let file: Vec<u8> = (0..600_000u32).map(|i| i as u8).collect();
    let reads = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let (client, server) = tokio::io::duplex(1 << 20);
    tokio::spawn(serve_file(server, file.clone(), 64 * 1024, reads.clone()));
    let sftp = russh_sftp::client::SftpSession::new(client).await.unwrap();
    let mut range = FileRange {
        file: sftp.open("/f").await.unwrap(),
        path: "/f".into(),
        calls: faucet_core::CallContext::new(),
        primed: false,
    };
    let len = file.len() as u64;
    let footer = range.read_range(len - 8..len).await.unwrap();
    assert_eq!(&footer[..], &file[file.len() - 8..]);
    reads.lock().unwrap().clear();
    let body = range.read_range(1_000..401_000).await.unwrap();
    assert_eq!(&body[..], &file[1_000..401_000]);
    let reads = reads.lock().unwrap().clone();
    assert!(
        reads.iter().all(|&n| n >= 64 * 1024),
        "every read asks for a full reply: {reads:?}"
    );
    assert!(reads.len() <= 32, "{} reads for 400 KB", reads.len());
}
