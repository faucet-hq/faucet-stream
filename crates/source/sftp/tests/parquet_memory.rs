//! Parquet files are read over byte ranges, one row group at a time (#783):
//! peak memory while streaming a file stays far below the file's size on
//! both the row and the columnar path. Requires Docker (an SFTP server).
//!
//! `FAUCET_PARQUET_MEMORY_ROWS` sets the row count (default 400 000, about
//! 45 MiB); each row is ~110 bytes of poorly-compressible text.
#![cfg(all(not(target_os = "windows"), feature = "arrow"))]

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use faucet_common_sftp::{OpenFlags, SftpConnectionConfig, connect};
use faucet_core::Source;
use faucet_source_sftp::{SftpFormat, SftpSource, SftpSourceConfig};
use futures::StreamExt;
use serde_json::Value;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use tokio::io::AsyncWriteExt;

struct Counting;
static CURRENT: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            let now = CURRENT.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(now, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        CURRENT.fetch_sub(layout.size(), Ordering::Relaxed);
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

fn reset_peak() -> usize {
    let now = CURRENT.load(Ordering::Relaxed);
    PEAK.store(now, Ordering::Relaxed);
    now
}

/// Write `rows` rows to a local Parquet file in 64 Ki-row groups, never
/// holding more than one batch.
fn write_parquet(path: &std::path::Path, rows: usize) {
    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let props = parquet::file::properties::WriterProperties::builder()
        .set_max_row_group_row_count(Some(65_536))
        .set_dictionary_enabled(false)
        .build();
    let file = std::fs::File::create(path).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();
    let mut seed = 0x9E37_79B9_7F4A_7C15_u64;
    let mut written = 0;
    while written < rows {
        let n = (rows - written).min(8192);
        let ids: Vec<i64> = (written..written + n).map(|i| i as i64).collect();
        let payloads: Vec<String> = (0..n)
            .map(|_| {
                (0..100)
                    .map(|_| {
                        seed ^= seed << 13;
                        seed ^= seed >> 7;
                        seed ^= seed << 17;
                        (b'a' + (seed % 26) as u8) as char
                    })
                    .collect()
            })
            .collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(payloads)),
            ],
        )
        .unwrap();
        w.write(&batch).unwrap();
        written += n;
    }
    w.close().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_parquet_file_streams_in_bounded_memory() {
    let rows: usize = std::env::var("FAUCET_PARQUET_MEMORY_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(400_000);
    let image = GenericImage::new("atmoz/sftp", "alpine")
        .with_exposed_port(22.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Server listening on"))
        .with_cmd(vec!["faucet:secret:::data".to_string()]);
    let container = match image.start().await {
        Ok(c) => c,
        Err(e) => {
            assert!(
                std::env::var_os("FAUCET_REQUIRE_BACKENDS").is_none(),
                "Docker unavailable and FAUCET_REQUIRE_BACKENDS is set: {e}"
            );
            eprintln!("Skipping: Docker not available ({e})");
            return;
        }
    };
    let port = container.get_host_port_ipv4(22).await.unwrap();
    let conn = SftpConnectionConfig::with_password("127.0.0.1", "faucet", "secret").port(port);

    let local = std::env::temp_dir().join(format!("faucet-pq-mem-{}.parquet", std::process::id()));
    write_parquet(&local, rows);
    let size = std::fs::metadata(&local).unwrap().len() as usize;
    {
        let sftp = connect(&conn).await.unwrap();
        let mut remote = sftp
            .open_with_flags(
                "/data/big.parquet",
                OpenFlags::CREATE | OpenFlags::WRITE | OpenFlags::TRUNCATE,
            )
            .await
            .unwrap();
        let file = tokio::fs::File::open(&local).await.unwrap();
        let mut file = tokio::io::BufReader::with_capacity(1 << 20, file);
        tokio::io::copy_buf(&mut file, &mut remote).await.unwrap();
        remote.shutdown().await.unwrap();
    }
    let _ = std::fs::remove_file(&local);

    let cfg = SftpSourceConfig::new(conn, "/data/big.parquet")
        .format(SftpFormat::Parquet)
        .with_batch_size(1000);
    let src = SftpSource::new(cfg).unwrap();
    let ctx: HashMap<String, Value> = HashMap::new();

    let base = reset_peak();
    let mut seen = 0;
    let mut pages = src.stream_pages(&ctx, 1000);
    while let Some(page) = pages.next().await {
        let page = page.unwrap();
        assert!(page.records.len() <= 1000);
        seen += page.records.len();
    }
    drop(pages);
    let row_peak = PEAK.load(Ordering::Relaxed) - base;
    assert_eq!(seen, rows);

    let base = reset_peak();
    let mut seen = 0;
    let mut batches = src.stream_batches(&ctx, 1000);
    while let Some(b) = batches.next().await {
        let b = b.unwrap();
        assert!(b.batch.num_rows() <= 1000);
        seen += b.batch.num_rows();
    }
    drop(batches);
    let columnar_peak = PEAK.load(Ordering::Relaxed) - base;
    assert_eq!(seen, rows);

    eprintln!(
        "sftp parquet memory: file {} MiB, row-path peak {} MiB, columnar peak {} MiB",
        size >> 20,
        row_peak >> 20,
        columnar_peak >> 20
    );
    assert!(
        row_peak < size / 2,
        "row path peaked at {row_peak} for a {size}-byte file"
    );
    assert!(
        columnar_peak < size / 2,
        "columnar path peaked at {columnar_peak} for a {size}-byte file"
    );
}
