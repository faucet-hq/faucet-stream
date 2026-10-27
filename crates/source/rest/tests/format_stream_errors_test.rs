//! Streamed CSV / JSON Lines bodies report read and parse failures with the
//! line cap and the format named.

use faucet_core::file_format::line_capped;
use faucet_source_rest::format::{
    csv_reader_to_ndjson_stream_with_options, csv_reader_to_value_pages,
    jsonl_reader_to_value_pages,
};
use futures::StreamExt as _;
use std::io::Cursor;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};

/// A body that fails mid-read with a transport error.
struct Broken;

impl AsyncRead for Broken {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Err(std::io::Error::other("connection reset")))
    }
}

fn capped(body: &str) -> impl AsyncRead + Unpin + Send + 'static {
    line_capped(Cursor::new(body.as_bytes().to_vec()), 16)
}

async fn first_error<T, S>(stream: S) -> String
where
    S: futures::Stream<Item = Result<T, faucet_core::FaucetError>>,
{
    let mut stream = Box::pin(stream);
    while let Some(item) = stream.next().await {
        if let Err(e) = item {
            return e.to_string();
        }
    }
    panic!("the stream ended without an error")
}

const LONG: &str = "a,b\n1,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const NOT_UTF8: &[u8] = b"a,b\n1,\xff\xfe\n";

#[tokio::test]
async fn csv_ndjson_stream_names_the_line_cap_the_read_error_and_bad_text() {
    let err = first_error(csv_reader_to_ndjson_stream_with_options(
        capped(LONG),
        b',',
        b'"',
        true,
        Vec::new(),
    ))
    .await;
    assert!(err.contains("a CSV line is longer than 16 bytes"), "{err}");

    let err = first_error(csv_reader_to_ndjson_stream_with_options(
        Broken,
        b',',
        b'"',
        true,
        Vec::new(),
    ))
    .await;
    assert!(err.contains("CSV read error: connection reset"), "{err}");

    let err = first_error(csv_reader_to_ndjson_stream_with_options(
        Cursor::new(NOT_UTF8.to_vec()),
        b',',
        b'"',
        true,
        Vec::new(),
    ))
    .await;
    assert!(err.contains("CSV parse error"), "{err}");
}

#[tokio::test]
async fn csv_value_pages_name_the_line_cap_and_bad_text() {
    let err = first_error(csv_reader_to_value_pages(capped(LONG), b',', true, 10)).await;
    assert!(err.contains("a CSV line is longer than 16 bytes"), "{err}");

    let err = first_error(csv_reader_to_value_pages(
        Cursor::new(NOT_UTF8.to_vec()),
        b',',
        true,
        10,
    ))
    .await;
    assert!(err.contains("CSV parse error"), "{err}");
}

#[tokio::test]
async fn jsonl_pages_name_the_line_cap_and_the_read_error() {
    let err = first_error(jsonl_reader_to_value_pages(
        capped("{\"a\":\"aaaaaaaaaaaaaaaaaaaaaaaa\"}"),
        10,
    ))
    .await;
    assert!(
        err.contains("a JSONL line is longer than 16 bytes"),
        "{err}"
    );

    let err = first_error(jsonl_reader_to_value_pages(Broken, 10)).await;
    assert!(err.contains("JSONL read error: connection reset"), "{err}");
}
