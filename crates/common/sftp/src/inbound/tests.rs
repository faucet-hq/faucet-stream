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

fn observed(data: &[u8], step: usize, max: u32) -> (Observed<Trickle>, Arc<InboundGuard>) {
    let guard = InboundGuard::new();
    let stream = Observed::new(
        Trickle {
            data: data.to_vec(),
            at: 0,
            step,
        },
        max,
        guard.clone(),
    );
    (stream, guard)
}

#[tokio::test]
async fn every_byte_passes_through_unchanged() {
    let mut data = packet(b"hello");
    data.extend(packet(b""));
    data.extend(packet(&[7u8; 300]));
    data.extend(packet(&[9u8; 301]));
    data.extend(packet(b"after"));
    for step in [1, 3, 4, 5, 4096] {
        let (mut stream, guard) = observed(&data, step, 300);
        let mut out = Vec::new();
        stream.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, data, "step {step}");
        let refusal = guard.refusal().expect("the 301-byte packet is recorded");
        assert!(refusal.contains("301-byte packet"), "{refusal}");
        assert_eq!(guard.refusals(), 1, "recorded once, step {step}");
    }
}

#[tokio::test]
async fn packets_within_the_limit_record_nothing() {
    let mut data = packet(&[1u8; 10]);
    data.extend(packet(&[2u8; 10]));
    for step in [1, 2, 64] {
        let (mut stream, guard) = observed(&data, step, 10);
        let mut out = Vec::new();
        stream.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, data);
        assert!(guard.refusal().is_none());
        assert_eq!(guard.refusals(), 0);
    }
}

#[tokio::test]
async fn a_stream_that_ends_mid_packet_records_nothing() {
    let data = packet(b"abcdef");
    for cut in [2, 7] {
        let (mut stream, guard) = observed(&data[..cut], 1, 100);
        let mut out = Vec::new();
        stream.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, &data[..cut]);
        assert!(guard.refusal().is_none());
    }
}

#[test]
fn framing_skips_bodies_and_reads_each_prefix() {
    let mut framing = Framing::new(8);
    assert_eq!(framing.advance(&packet(&[0xffu8; 8])), None);
    let over = packet(&[0u8; 9]);
    assert_eq!(framing.advance(&over[..3]), None);
    assert_eq!(framing.advance(&over[3..]), Some(9));
    assert_eq!(framing.advance(&packet(&[0u8; 20])), None, "only the first");
}

#[tokio::test]
async fn writes_pass_through() {
    let (client, mut server) = tokio::io::duplex(64);
    let mut stream = Observed::new(client, 10, InboundGuard::new());
    stream.write_all(b"ping").await.unwrap();
    stream.flush().await.unwrap();
    stream.shutdown().await.unwrap();
    let mut got = Vec::new();
    server.read_to_end(&mut got).await.unwrap();
    assert_eq!(got, b"ping");
}

async fn read_request(server: &mut tokio::io::DuplexStream) -> Vec<u8> {
    let len = server.read_u32().await.unwrap();
    let mut body = vec![0u8; len as usize];
    server.read_exact(&mut body).await.unwrap();
    body
}

async fn reply_version(server: &mut tokio::io::DuplexStream) {
    read_request(server).await;
    server.write_all(&packet(&[2, 0, 0, 0, 3])).await.unwrap();
}

fn session(
    client: tokio::io::DuplexStream,
) -> (russh_sftp::client::RawSftpSession, Arc<InboundGuard>) {
    let guard = InboundGuard::new();
    let raw = russh_sftp::client::RawSftpSession::new_with_config(
        Observed::new(client, MAX_INBOUND_PACKET, guard.clone()),
        client_config(),
    );
    raw.set_timeout(5);
    (raw, guard)
}

#[tokio::test]
async fn an_inbound_packet_over_the_limit_is_refused_with_its_length() {
    let (client, mut server) = tokio::io::duplex(64 * 1024);
    let (raw, guard) = session(client);
    let fake_server = tokio::spawn(async move {
        read_request(&mut server).await;
        server
            .write_all(&(MAX_INBOUND_PACKET + 1).to_be_bytes())
            .await
            .unwrap();
        let _ = server.write_all(&[0u8; 512]).await;
        server
    });
    let init = tokio::time::timeout(std::time::Duration::from_secs(2), raw.init())
        .await
        .expect("refused at the length prefix, not after a timeout");
    assert!(init.is_err(), "the version reply is refused");
    let refusal = guard.refusal().expect("the refusal is recorded");
    assert!(
        refusal.contains(&(MAX_INBOUND_PACKET + 1).to_string())
            && refusal.contains(&MAX_INBOUND_PACKET.to_string()),
        "{refusal}"
    );
    assert!(raw.stat("/x").await.is_err(), "the channel is closed");
    drop(fake_server.await.unwrap());
}

#[tokio::test]
async fn a_reply_at_the_limit_is_read() {
    let (client, mut server) = tokio::io::duplex(64 * 1024);
    let (raw, guard) = session(client);
    let fake_server = tokio::spawn(async move {
        reply_version(&mut server).await;
        let request = read_request(&mut server).await;
        let id = &request[1..5];
        let n = MAX_INBOUND_PACKET - 9;
        let mut reply = MAX_INBOUND_PACKET.to_be_bytes().to_vec();
        reply.push(103);
        reply.extend_from_slice(id);
        reply.extend_from_slice(&n.to_be_bytes());
        reply.resize(4 + MAX_INBOUND_PACKET as usize, 5);
        server.write_all(&reply).await.unwrap();
        server
    });
    raw.init().await.unwrap();
    let data = raw.read("h", 0, MAX_READ_CHUNK as u32).await.unwrap();
    assert_eq!(data.data.len(), (MAX_INBOUND_PACKET - 9) as usize);
    assert!(guard.refusal().is_none());
    drop(fake_server.await.unwrap());
}

#[tokio::test]
async fn the_high_level_session_refuses_an_oversized_reply() {
    let (client, mut server) = tokio::io::duplex(64 * 1024);
    let guard = InboundGuard::new();
    let fake_server = tokio::spawn(async move {
        reply_version(&mut server).await;
        read_request(&mut server).await;
        server
            .write_all(&(MAX_INBOUND_PACKET + 7).to_be_bytes())
            .await
            .unwrap();
        server
    });
    let sftp = russh_sftp::client::SftpSession::new_with_config(
        Observed::new(client, MAX_INBOUND_PACKET, guard.clone()),
        client_config(),
    )
    .await
    .unwrap();
    assert!(sftp.metadata("/x").await.is_err());
    let refusal = guard.refusal().expect("recorded");
    assert!(
        refusal.contains(&(MAX_INBOUND_PACKET + 7).to_string()),
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

#[test]
fn every_session_bounds_packets_and_reads_in_flight() {
    let config = client_config();
    assert_eq!(config.max_packet_len, MAX_INBOUND_PACKET);
    assert_eq!(config.max_write_packet_len, 256 * 1024);
    assert_eq!(config.max_concurrent_reads, 4);
}
