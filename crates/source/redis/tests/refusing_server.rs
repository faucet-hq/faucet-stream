//! The Redis source against a server that refuses every command: the
//! failure names the command and the pattern.

use faucet_core::Source;
use faucet_source_redis::{RedisSource, RedisSourceConfig, RedisSourceType};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

/// A RESP endpoint answering every command with an error.
async fn refusing_server() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            tokio::spawn(async move {
                let (read, mut write) = socket.into_split();
                let mut lines = BufReader::new(read).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    // A command is `*N` followed by N `$len` / value pairs.
                    let Some(n) = line.strip_prefix('*') else {
                        continue;
                    };
                    for _ in 0..2 * n.parse::<usize>().unwrap_or(0) {
                        if lines.next_line().await.ok().flatten().is_none() {
                            return;
                        }
                    }
                    if write.write_all(b"-ERR refused\r\n").await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    format!("redis://{addr}")
}

#[tokio::test]
async fn a_refused_scan_names_its_pattern() {
    let url = refusing_server().await;
    let source = RedisSource::new(RedisSourceConfig::new(
        &url,
        RedisSourceType::Keys {
            pattern: "user:*".into(),
        },
    ))
    .unwrap();
    source.set_roundtrip_recorder(std::sync::Arc::new(
        faucet_core::observability::RoundtripRecorder::new(
            faucet_core::observability::RoundtripSide::Source,
            "p",
            "r",
            "redis",
        ),
    ));
    let err = source.fetch_all().await.unwrap_err().to_string();
    assert!(err.contains("SCAN failed with pattern 'user:*'"), "{err}");
}
