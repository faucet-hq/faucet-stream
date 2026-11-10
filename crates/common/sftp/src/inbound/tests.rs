use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Hands out its bytes `step` at a time, so a packet's length prefix and body
/// arrive split across reads.
struct Trickle {
    data: Vec<u8>,
    at: usize,
    step: usize,
}

impl AsyncRead for Trickle {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let n = this
            .step
            .min(buf.remaining())
            .min(this.data.len() - this.at);
        buf.put_slice(&this.data[this.at..this.at + n]);
        this.at += n;
        Poll::Ready(Ok(()))
    }
}

fn packet(body: &[u8]) -> Vec<u8> {
    let mut out = (body.len() as u32).to_be_bytes().to_vec();
    out.extend_from_slice(body);
    out
}

#[tokio::test]
async fn packets_under_the_limit_pass_through_unchanged() {
    let mut data = packet(b"hello");
    data.extend(packet(b""));
    data.extend(packet(&[7u8; 300]));
    for step in [1, 3, 4096] {
        let guard = InboundGuard::new();
        let mut stream = LimitedStream::new(
            Trickle {
                data: data.clone(),
                at: 0,
                step,
            },
            300,
            guard.clone(),
        );
        let mut out = Vec::new();
        stream.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, data, "step {step}");
        assert!(guard.refusal().is_none());
    }
}

#[tokio::test]
async fn a_packet_over_the_limit_is_refused_before_its_body_is_read() {
    let mut data = packet(b"ok");
    data.extend(packet(&[1u8; 11]));
    let guard = InboundGuard::new();
    let mut stream = LimitedStream::new(
        Trickle {
            data,
            at: 0,
            step: 2,
        },
        10,
        guard.clone(),
    );
    let mut first = [0u8; 6];
    stream.read_exact(&mut first).await.unwrap();
    assert_eq!(&first, &packet(b"ok")[..]);
    let err = stream.read(&mut [0u8; 64]).await.unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert!(err.to_string().contains("11-byte packet"), "{err}");
    assert_eq!(
        stream.read(&mut [0u8; 64]).await.unwrap(),
        0,
        "then end of stream"
    );
    let refusal = guard.refusal().expect("recorded");
    assert!(
        refusal.contains(&MAX_INBOUND_PACKET.to_string()),
        "{refusal}"
    );
}

#[tokio::test]
async fn a_stream_that_ends_mid_packet_reports_end_of_stream() {
    let mut data = packet(b"abcdef");
    data.truncate(7);
    let mut stream = LimitedStream::new(
        Trickle {
            data: data[..2].to_vec(),
            at: 0,
            step: 1,
        },
        100,
        InboundGuard::new(),
    );
    let mut out = Vec::new();
    stream.read_to_end(&mut out).await.unwrap();
    assert!(out.is_empty(), "a partial length prefix is not handed on");

    let mut stream = LimitedStream::new(
        Trickle {
            data: data.clone(),
            at: 0,
            step: 64,
        },
        100,
        InboundGuard::new(),
    );
    let mut out = Vec::new();
    stream.read_to_end(&mut out).await.unwrap();
    assert_eq!(out, data);
    assert_eq!(stream.read(&mut []).await.unwrap(), 0);
}

#[tokio::test]
async fn writes_pass_through() {
    let (client, mut server) = tokio::io::duplex(64);
    let mut stream = LimitedStream::new(client, 10, InboundGuard::new());
    stream.write_all(b"ping").await.unwrap();
    stream.flush().await.unwrap();
    stream.shutdown().await.unwrap();
    let mut got = Vec::new();
    server.read_to_end(&mut got).await.unwrap();
    assert_eq!(got, b"ping");
}

#[tokio::test]
async fn an_inbound_packet_over_the_limit_is_refused() {
    let (client, mut server) = tokio::io::duplex(1024);
    let guard = InboundGuard::new();
    let raw = russh_sftp::client::RawSftpSession::new(LimitedStream::new(
        client,
        MAX_INBOUND_PACKET,
        guard.clone(),
    ));
    raw.set_timeout(1);
    let fake_server = tokio::spawn(async move {
        let mut init = [0u8; 9];
        server.read_exact(&mut init).await.unwrap();
        server
            .write_all(&(MAX_INBOUND_PACKET + 1).to_be_bytes())
            .await
            .unwrap();
        server.write_all(&[0u8; 512]).await.unwrap();
        server
    });
    assert!(raw.init().await.is_err(), "the version reply is refused");
    let refusal = guard.refusal().expect("the refusal is recorded");
    assert!(
        refusal.contains(&(MAX_INBOUND_PACKET + 1).to_string()),
        "{refusal}"
    );
    drop(fake_server.await.unwrap());
}

#[tokio::test]
async fn chunked_reads_ask_for_at_most_one_chunk() {
    let data = vec![3u8; MAX_READ_CHUNK + 10];
    let mut reader = ChunkedReader::new(Trickle {
        data: data.clone(),
        at: 0,
        step: usize::MAX,
    });
    let mut buf = vec![0u8; MAX_READ_CHUNK * 2];
    let n = reader.read(&mut buf).await.unwrap();
    assert_eq!(n, MAX_READ_CHUNK);
    let n = reader.read(&mut buf).await.unwrap();
    assert_eq!(n, 10);
    let mut small = [0u8; 4];
    assert_eq!(reader.read(&mut small).await.unwrap(), 0);
    let inner = reader.into_inner();
    assert_eq!(inner.at, data.len());
}
