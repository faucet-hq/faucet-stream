//! A scripted TDS endpoint for exercising a SQL Server connector's I/O paths
//! without a server.
//!
//! [`TdsServer`] speaks just enough of the TDS protocol for a client with
//! encryption disabled: it answers the pre-login and login exchange, then
//! answers every request with the first [`Reply`] whose needle occurs in the
//! request text (statement text and string parameters, decoded as UTF-16).
//! A request no rule matches gets an empty `DONE`. Every request text is
//! recorded, so a test can assert what the connector sent.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

/// A column type the server can describe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TdsType {
    /// `TINYINT`.
    TinyInt,
    /// `SMALLINT`.
    SmallInt,
    /// `INT`.
    Int,
    /// `BIGINT`.
    BigInt,
    /// `NVARCHAR(4000)`.
    NVarChar,
    /// `BIT`.
    Bit,
    /// `FLOAT`.
    Float,
    /// `REAL` (a single-precision float), for a `Float` value.
    Real,
    /// `DECIMAL(38, scale)`, for a `Decimal` value.
    Decimal {
        /// Digits after the decimal point.
        scale: u8,
    },
}

/// One cell of a result row.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum TdsValue {
    /// SQL `NULL`.
    Null,
    /// An integer, for an `Int` or `BigInt` column.
    Int(i64),
    /// A string, for an `NVarChar` column.
    Str(String),
    /// A boolean, for a `Bit` column.
    Bool(bool),
    /// A float, for a `Float` or `Real` column.
    Float(f64),
    /// An unscaled decimal (`value × 10^-scale`), for a `Decimal` column.
    Decimal(i128),
}

impl From<i64> for TdsValue {
    fn from(v: i64) -> Self {
        Self::Int(v)
    }
}

impl From<&str> for TdsValue {
    fn from(v: &str) -> Self {
        Self::Str(v.to_string())
    }
}

impl From<bool> for TdsValue {
    fn from(v: bool) -> Self {
        Self::Bool(v)
    }
}

/// How the server answers a matching request.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Reply {
    /// One result set.
    Rows {
        /// Column names and types.
        columns: Vec<(String, TdsType)>,
        /// Row values, one per column.
        rows: Vec<Vec<TdsValue>>,
    },
    /// No result set; `DONE` reporting this many affected rows.
    Done(u64),
    /// A server error.
    Error {
        /// The error number (`2627` a key violation, `1205` a deadlock victim).
        code: u32,
        /// The message.
        message: String,
    },
    /// Never answer; the connection stays open.
    Hang,
    /// Send this reply's tokens without ending the message, then stop
    /// answering: a result set that stalls mid-stream.
    Stall(Box<Reply>),
    /// Close the connection without answering.
    Close,
}

impl Reply {
    /// A result set from `(name, type)` columns and rows.
    pub fn rows(columns: &[(&str, TdsType)], rows: Vec<Vec<TdsValue>>) -> Self {
        Self::Rows {
            columns: columns.iter().map(|(n, t)| (n.to_string(), *t)).collect(),
            rows,
        }
    }

    /// A server error with a user-defined error number.
    pub fn error(message: &str) -> Self {
        Self::error_code(50000, message)
    }

    /// A server error with this error number.
    pub fn error_code(code: u32, message: &str) -> Self {
        Self::Error {
            code,
            message: message.to_string(),
        }
    }
}

#[derive(Debug)]
struct Rule {
    needle: String,
    reply: Reply,
    remaining: Option<usize>,
}

#[derive(Debug, Default)]
struct Script {
    rules: Vec<Rule>,
    requests: Vec<String>,
    refuse_login: bool,
}

/// A TDS endpoint on `127.0.0.1` answering from a script. Dropping it closes
/// the listener and every connection.
#[derive(Debug)]
pub struct TdsServer {
    addr: SocketAddr,
    script: Arc<Mutex<Script>>,
    task: JoinHandle<()>,
}

impl TdsServer {
    /// Bind an ephemeral port and start answering.
    pub async fn start() -> std::io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let script = Arc::new(Mutex::new(Script::default()));
        let shared = script.clone();
        let task = tokio::spawn(async move {
            let mut conns = Vec::new();
            while let Ok((socket, _)) = listener.accept().await {
                conns.push(tokio::spawn(serve(socket, shared.clone())));
            }
        });
        Ok(Self { addr, script, task })
    }

    /// The bound port.
    pub fn port(&self) -> u16 {
        self.addr.port()
    }

    /// `mssql://sa:pw@127.0.0.1:<port>/db`.
    pub fn url(&self) -> String {
        format!("mssql://sa:pw@127.0.0.1:{}/db", self.port())
    }

    /// Answer every request containing `needle` with `reply`. Later rules
    /// take precedence over earlier ones.
    pub fn on(&self, needle: &str, reply: Reply) -> &Self {
        self.push(needle, reply, None)
    }

    /// Answer the next `times` requests containing `needle` with `reply`,
    /// ahead of every other rule.
    pub fn on_times(&self, needle: &str, reply: Reply, times: usize) -> &Self {
        self.push(needle, reply, Some(times))
    }

    fn push(&self, needle: &str, reply: Reply, remaining: Option<usize>) -> &Self {
        self.lock().rules.insert(
            0,
            Rule {
                needle: needle.to_string(),
                reply,
                remaining,
            },
        );
        self
    }

    /// Refuse every later login with a server error.
    pub fn refuse_logins(&self) {
        self.lock().refuse_login = true;
    }

    /// The text of every request answered so far, in order.
    pub fn requests(&self) -> Vec<String> {
        self.lock().requests.clone()
    }

    /// Whether any request so far contained `needle`.
    pub fn saw(&self, needle: &str) -> bool {
        self.lock().requests.iter().any(|r| r.contains(needle))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Script> {
        self.script.lock().unwrap_or_else(|p| p.into_inner())
    }
}

impl Drop for TdsServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

const PRELOGIN: u8 = 0x12;
const LOGIN: u8 = 0x10;
const TABULAR: u8 = 0x04;

async fn read_message(socket: &mut TcpStream) -> Option<(u8, Vec<u8>)> {
    let mut payload = Vec::new();
    loop {
        let mut header = [0u8; 8];
        socket.read_exact(&mut header).await.ok()?;
        let len = u16::from_be_bytes([header[2], header[3]]) as usize;
        let mut body = vec![0u8; len.checked_sub(8)?];
        socket.read_exact(&mut body).await.ok()?;
        payload.extend_from_slice(&body);
        if header[1] & 0x01 == 0x01 {
            return Some((header[0], payload));
        }
    }
}

async fn write_message(socket: &mut TcpStream, payload: &[u8], end: bool) {
    let chunks: Vec<&[u8]> = payload.chunks(4000).collect();
    let last = chunks.len() - 1;
    for (i, chunk) in chunks.into_iter().enumerate() {
        let status = u8::from(end && i == last);
        let mut packet = vec![TABULAR, status, 0, 0, 0, 0, (i + 1) as u8, 0];
        packet[2..4].copy_from_slice(&((chunk.len() + 8) as u16).to_be_bytes());
        packet.extend_from_slice(chunk);
        // A failed write surfaces as the next read failing.
        let _ = socket.write_all(&packet).await;
    }
}

async fn serve(mut socket: TcpStream, script: Arc<Mutex<Script>>) {
    while let Some((kind, payload)) = read_message(&mut socket).await {
        let lock = || script.lock().unwrap_or_else(|p| p.into_inner());
        let reply = match kind {
            PRELOGIN => prelogin_response(),
            LOGIN if lock().refuse_login => {
                let mut out = error_token(18456, "Login failed for user 'sa'.");
                out.extend(done(0x02, 0));
                out
            }
            LOGIN => done(0, 0),
            _ => {
                let text = request_text(&payload);
                let reply = {
                    let mut s = lock();
                    s.requests.push(text.clone());
                    pick(&mut s.rules, &text)
                };
                match reply {
                    Reply::Hang => return drain(socket).await,
                    Reply::Stall(inner) => {
                        let mut tokens = encode_reply(&inner);
                        tokens.truncate(tokens.len() - 13);
                        write_message(&mut socket, &tokens, false).await;
                        return drain(socket).await;
                    }
                    Reply::Close => return,
                    other => encode_reply(&other),
                }
            }
        };
        write_message(&mut socket, &reply, true).await;
    }
}

async fn drain(mut socket: TcpStream) {
    let mut sink = [0u8; 1024];
    while socket.read(&mut sink).await.is_ok_and(|n| n > 0) {}
}

fn pick(rules: &mut Vec<Rule>, text: &str) -> Reply {
    let Some(i) = rules.iter().position(|r| text.contains(&r.needle)) else {
        return Reply::Done(0);
    };
    let reply = rules[i].reply.clone();
    if let Some(n) = rules[i].remaining.as_mut() {
        *n -= 1;
        if *n == 0 {
            rules.remove(i);
        }
    }
    reply
}

fn request_text(payload: &[u8]) -> String {
    let decode = |bytes: &[u8]| {
        let units: Vec<u16> = bytes
            .chunks_exact(2)
            .map(|c| u16::from_le_bytes([c[0], c[1]]))
            .collect();
        String::from_utf16_lossy(&units)
    };
    let odd = payload.get(1..).unwrap_or_default();
    format!("{}\n{}", decode(payload), decode(odd))
}

fn prelogin_response() -> Vec<u8> {
    let mut out = vec![0x00, 0, 11, 0, 6, 0x01, 0, 17, 0, 1, 0xFF];
    out.extend_from_slice(&[16, 0, 0, 0, 0, 0]);
    out.push(0x02);
    out
}

fn ucs2(s: &str) -> Vec<u8> {
    s.encode_utf16().flat_map(u16::to_le_bytes).collect()
}

fn b_varchar(s: &str) -> Vec<u8> {
    let mut out = vec![s.encode_utf16().count() as u8];
    out.extend(ucs2(s));
    out
}

fn done(status: u16, rows: u64) -> Vec<u8> {
    let mut out = vec![0xFD];
    out.extend_from_slice(&status.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes());
    out.extend_from_slice(&rows.to_le_bytes());
    out
}

fn error_token(code: u32, message: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&code.to_le_bytes());
    body.extend_from_slice(&[1, 16]);
    body.extend_from_slice(&(message.encode_utf16().count() as u16).to_le_bytes());
    body.extend(ucs2(message));
    body.extend(b_varchar("fake"));
    body.extend(b_varchar(""));
    body.extend_from_slice(&1u32.to_le_bytes());
    let mut out = vec![0xAA];
    out.extend_from_slice(&(body.len() as u16).to_le_bytes());
    out.extend(body);
    out
}

fn type_info(t: TdsType) -> Vec<u8> {
    match t {
        TdsType::TinyInt => vec![0x26, 1],
        TdsType::SmallInt => vec![0x26, 2],
        TdsType::Int => vec![0x26, 4],
        TdsType::BigInt => vec![0x26, 8],
        TdsType::Bit => vec![0x68, 1],
        TdsType::Float => vec![0x6D, 8],
        TdsType::Real => vec![0x6D, 4],
        TdsType::Decimal { scale } => vec![0x6C, 17, 38, scale],
        TdsType::NVarChar => vec![0xE7, 0x40, 0x1F, 0x09, 0x04, 0xD0, 0x00, 0x34],
    }
}

fn cell(t: TdsType, v: &TdsValue) -> Vec<u8> {
    match (t, v) {
        (TdsType::NVarChar, TdsValue::Null) => vec![0xFF, 0xFF],
        (_, TdsValue::Null) => vec![0],
        (TdsType::TinyInt, TdsValue::Int(i)) => vec![1, *i as u8],
        (TdsType::SmallInt, TdsValue::Int(i)) => {
            let mut out = vec![2];
            out.extend_from_slice(&(*i as i16).to_le_bytes());
            out
        }
        (TdsType::Int, TdsValue::Int(i)) => {
            let mut out = vec![4];
            out.extend_from_slice(&(*i as i32).to_le_bytes());
            out
        }
        (TdsType::BigInt, TdsValue::Int(i)) => {
            let mut out = vec![8];
            out.extend_from_slice(&i.to_le_bytes());
            out
        }
        (TdsType::Bit, TdsValue::Bool(b)) => vec![1, u8::from(*b)],
        (TdsType::Float, TdsValue::Float(f)) => {
            let mut out = vec![8];
            out.extend_from_slice(&f.to_le_bytes());
            out
        }
        (TdsType::Real, TdsValue::Float(f)) => {
            let mut out = vec![4];
            out.extend_from_slice(&(*f as f32).to_le_bytes());
            out
        }
        (TdsType::Decimal { .. }, TdsValue::Decimal(d)) => {
            let mut out = vec![17, u8::from(*d >= 0)];
            out.extend_from_slice(&d.unsigned_abs().to_le_bytes());
            out
        }
        (TdsType::NVarChar, TdsValue::Str(s)) => {
            let bytes = ucs2(s);
            let mut out = (bytes.len() as u16).to_le_bytes().to_vec();
            out.extend(bytes);
            out
        }
        (t, v) => panic!("tds: a {v:?} cell does not fit a {t:?} column"),
    }
}

fn encode_reply(reply: &Reply) -> Vec<u8> {
    match reply {
        Reply::Rows { columns, rows } => {
            let mut out = vec![0x81];
            out.extend_from_slice(&(columns.len() as u16).to_le_bytes());
            for (name, t) in columns {
                out.extend_from_slice(&0u32.to_le_bytes());
                out.extend_from_slice(&0x0001u16.to_le_bytes());
                out.extend(type_info(*t));
                out.extend(b_varchar(name));
            }
            for row in rows {
                out.push(0xD1);
                for ((_, t), v) in columns.iter().zip(row) {
                    out.extend(cell(*t, v));
                }
            }
            out.extend(done(0x10, rows.len() as u64));
            out
        }
        Reply::Done(n) => done(0x10, *n),
        Reply::Error { code, message } => {
            let mut out = error_token(*code, message);
            out.extend(done(0x02, 0));
            out
        }
        Reply::Hang | Reply::Close | Reply::Stall(_) => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rules_match_newest_first_and_expire() {
        let mut rules = Vec::new();
        for (needle, n, remaining) in [("SELECT", 1, None), ("SELECT", 2, Some(1))] {
            rules.insert(
                0,
                Rule {
                    needle: needle.into(),
                    reply: Reply::Done(n),
                    remaining,
                },
            );
        }
        assert!(matches!(pick(&mut rules, "SELECT 1"), Reply::Done(2)));
        assert!(matches!(pick(&mut rules, "SELECT 1"), Reply::Done(1)));
        assert!(matches!(pick(&mut rules, "other"), Reply::Done(0)));
    }

    #[test]
    fn request_text_decodes_either_alignment() {
        let mut payload = vec![0u8];
        payload.extend(ucs2("SELECT 1"));
        assert!(request_text(&payload).contains("SELECT 1"));
        assert!(request_text(&ucs2("SELECT 2")).contains("SELECT 2"));
        assert_eq!(request_text(&[]), "\n");
    }

    #[test]
    #[should_panic(expected = "does not fit")]
    fn a_mistyped_cell_is_a_test_bug() {
        cell(TdsType::Int, &TdsValue::Str("x".into()));
    }

    #[test]
    fn cells_encode_every_type() {
        assert_eq!(cell(TdsType::NVarChar, &TdsValue::Null), vec![0xFF, 0xFF]);
        assert_eq!(cell(TdsType::Int, &TdsValue::Null), vec![0]);
        assert_eq!(cell(TdsType::Bit, &true.into()), vec![1, 1]);
        assert_eq!(cell(TdsType::Float, &TdsValue::Float(1.0)).len(), 9);
        assert_eq!(cell(TdsType::BigInt, &5i64.into()).len(), 9);
        assert_eq!(cell(TdsType::SmallInt, &5i64.into()), vec![2, 5, 0]);
        assert_eq!(cell(TdsType::TinyInt, &5i64.into()), vec![1, 5]);
        assert_eq!(cell(TdsType::Int, &5i64.into()), vec![4, 5, 0, 0, 0]);
        assert_eq!(type_info(TdsType::Int), vec![0x26, 4]);
        assert_eq!(type_info(TdsType::Bit), vec![0x68, 1]);
        assert_eq!(type_info(TdsType::Float), vec![0x6D, 8]);
        assert_eq!(type_info(TdsType::Real), vec![0x6D, 4]);
        assert_eq!(
            type_info(TdsType::Decimal { scale: 2 }),
            vec![0x6C, 17, 38, 2]
        );
        assert_eq!(
            cell(TdsType::Real, &TdsValue::Float(1.0)),
            vec![4, 0, 0, 0x80, 0x3F]
        );
        let neg = cell(TdsType::Decimal { scale: 2 }, &TdsValue::Decimal(-5));
        assert_eq!((neg.len(), neg[0], neg[1], neg[2]), (18, 17, 0, 5));
        assert!(encode_reply(&Reply::Hang).is_empty());
        assert_eq!(type_info(TdsType::TinyInt), vec![0x26, 1]);
        assert_eq!(type_info(TdsType::SmallInt), vec![0x26, 2]);
        assert_eq!(
            cell(TdsType::NVarChar, &"ab".into()),
            vec![4, 0, b'a', 0, b'b', 0]
        );
        assert!(
            matches!(Reply::error("x"), Reply::Error { code: 50000, message } if message == "x")
        );
    }

    #[tokio::test]
    async fn the_listener_records_requests_and_hangs_or_closes() {
        let server = TdsServer::start().await.unwrap();
        assert!(server.url().ends_with(&format!(":{}/db", server.port())));
        server
            .on("SELECT", Reply::Close)
            .on_times("WAIT", Reply::Hang, 1);
        let mut socket = TcpStream::connect(("127.0.0.1", server.port()))
            .await
            .unwrap();
        let mut packet = vec![0x01, 0x00, 0, 10, 0, 0, 1, 0, b'x', 0];
        packet.extend([0x01, 0x01, 0, 0, 0, 0, 2, 0]);
        packet.extend(ucs2("WAIT"));
        packet[12..14].copy_from_slice(&16u16.to_be_bytes());
        socket.write_all(&packet).await.unwrap();
        let read = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            socket.read(&mut [0u8; 8]),
        )
        .await;
        assert!(read.is_err(), "a hung request gets no answer");
        drop(socket);
        let mut socket = TcpStream::connect(("127.0.0.1", server.port()))
            .await
            .unwrap();
        let mut packet = vec![0x01, 0x01, 0, 0, 0, 0, 1, 0];
        packet.extend(ucs2("SELECT"));
        let len = packet.len() as u16;
        packet[2..4].copy_from_slice(&len.to_be_bytes());
        socket.write_all(&packet).await.unwrap();
        assert_eq!(socket.read(&mut [0u8; 8]).await.unwrap(), 0);
        assert!(server.saw("WAIT") && server.saw("SELECT"));
        assert_eq!(server.requests().len(), 2);
    }
}
