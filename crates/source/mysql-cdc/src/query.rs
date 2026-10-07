//! Classification of binlog `QueryEvent` statements and XA identifiers (pure).
//!
//! Only real DDL is an implicit commit. `SAVEPOINT` / `ROLLBACK TO` /
//! `RELEASE SAVEPOINT` and `XA END` are logged *inside* a transaction and must
//! not split it; `XA START` opens one; an XA transaction's rows become final
//! only at `XA COMMIT` (or vanish at `XA ROLLBACK`); `TRUNCATE` is a change to
//! a table the stream carries as a `truncate` record.

/// An XA transaction identifier: `gtrid`, `bqual`, `formatID`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct XaId {
    pub format_id: i64,
    pub gtrid: Vec<u8>,
    pub bqual: Vec<u8>,
}

/// What a `QueryEvent` statement means for the transaction stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum QueryKind {
    /// `BEGIN` / `XA START` — a transaction opens.
    Begin,
    /// `COMMIT` / `ROLLBACK` of a transaction that touched non-transactional
    /// tables — the buffered rows are final.
    Commit,
    /// A statement logged inside a transaction that does not end it.
    InTransaction,
    /// `XA COMMIT <xid>` (`commit: true`) or `XA ROLLBACK <xid>`: the outcome
    /// of a prepared XA transaction.
    XaOutcome { xid: XaId, commit: bool },
    /// `TRUNCATE [TABLE] [db.]table`.
    Truncate { schema: String, table: String },
    /// Anything else: DDL, which commits implicitly.
    Ddl,
}

/// Strip leading `/* … */` comments and whitespace.
fn strip_leading_comments(mut s: &str) -> &str {
    loop {
        s = s.trim_start();
        match s
            .strip_prefix("/*")
            .and_then(|r| r.find("*/").map(|i| &r[i + 2..]))
        {
            Some(rest) => s = rest,
            None => return s,
        }
    }
}

/// Split off the leading run of `n` whitespace-separated words, upper-cased.
fn leading_words(s: &str, n: usize) -> (Vec<String>, &str) {
    let mut rest = s;
    let mut words = Vec::with_capacity(n);
    for _ in 0..n {
        rest = rest.trim_start();
        let end = rest
            .find(|c: char| c.is_whitespace() || c == ';')
            .unwrap_or(rest.len());
        if end == 0 {
            break;
        }
        words.push(rest[..end].to_ascii_uppercase());
        rest = &rest[end..];
    }
    (words, rest)
}

/// Classify one `QueryEvent` statement. `default_schema` is the event's
/// current database, used for an unqualified `TRUNCATE`.
pub(crate) fn classify_query(query: &str, default_schema: &str) -> QueryKind {
    let q = strip_leading_comments(query);
    let (w, rest) = leading_words(q, 2);
    let first = w.first().map(String::as_str).unwrap_or("");
    let second = w.get(1).map(String::as_str).unwrap_or("");
    match (first, second) {
        ("BEGIN", _) => QueryKind::Begin,
        ("COMMIT", _) => QueryKind::Commit,
        ("SAVEPOINT", _) => QueryKind::InTransaction,
        ("RELEASE", "SAVEPOINT") => QueryKind::InTransaction,
        ("ROLLBACK", "TO") => QueryKind::InTransaction,
        ("ROLLBACK", "WORK") | ("ROLLBACK", "") => QueryKind::Commit,
        ("XA", "START" | "BEGIN") => QueryKind::Begin,
        ("XA", "END") => QueryKind::InTransaction,
        ("XA", "COMMIT") => match parse_query_xid(rest) {
            Some(xid) => QueryKind::XaOutcome { xid, commit: true },
            None => QueryKind::Ddl,
        },
        ("XA", "ROLLBACK") => match parse_query_xid(rest) {
            Some(xid) => QueryKind::XaOutcome { xid, commit: false },
            None => QueryKind::Ddl,
        },
        ("TRUNCATE", _) => {
            let target = if second == "TABLE" {
                rest
            } else {
                &q["TRUNCATE".len()..]
            };
            match parse_table_name(target) {
                Some((schema, table)) => QueryKind::Truncate {
                    schema: schema.unwrap_or_else(|| default_schema.to_string()),
                    table,
                },
                None => QueryKind::Ddl,
            }
        }
        _ => QueryKind::Ddl,
    }
}

/// One identifier, back-quoted or bare.
fn parse_ident(s: &str) -> Option<(String, &str)> {
    let s = s.trim_start();
    if let Some(rest) = s.strip_prefix('`') {
        let mut out = String::new();
        let mut chars = rest.char_indices();
        while let Some((i, c)) = chars.next() {
            if c == '`' {
                if rest[i + 1..].starts_with('`') {
                    out.push('`');
                    chars.next();
                } else {
                    return Some((out, &rest[i + 1..]));
                }
            } else {
                out.push(c);
            }
        }
        None
    } else {
        let end = s
            .find(|c: char| !(c.is_alphanumeric() || c == '_' || c == '$'))
            .unwrap_or(s.len());
        (end > 0).then(|| (s[..end].to_string(), &s[end..]))
    }
}

/// `[db.]table` at the start of `s`.
fn parse_table_name(s: &str) -> Option<(Option<String>, String)> {
    let (first, rest) = parse_ident(s)?;
    match rest.strip_prefix('.') {
        Some(rest) => {
            let (table, _) = parse_ident(rest)?;
            Some((Some(first), table))
        }
        None => Some((None, first)),
    }
}

/// One xid component: `X'hex'`, `'text'` (with `''` / `\'` escapes) or
/// `0x…`.
fn parse_xid_part(s: &str) -> Option<(Vec<u8>, &str)> {
    let s = s.trim_start();
    if let Some(rest) = s.strip_prefix("X'").or_else(|| s.strip_prefix("x'")) {
        let end = rest.find('\'')?;
        return Some((decode_hex(&rest[..end])?, &rest[end + 1..]));
    }
    if let Some(rest) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        let end = rest
            .find(|c: char| !c.is_ascii_hexdigit())
            .unwrap_or(rest.len());
        return Some((decode_hex(&rest[..end])?, &rest[end..]));
    }
    let quote = s.chars().next().filter(|c| *c == '\'' || *c == '"')?;
    let rest = &s[1..];
    let mut out = Vec::new();
    let mut chars = rest.char_indices();
    while let Some((i, c)) = chars.next() {
        if c == '\\' {
            let (_, e) = chars.next()?;
            let mut buf = [0u8; 4];
            out.extend_from_slice(e.encode_utf8(&mut buf).as_bytes());
        } else if c == quote {
            if rest[i + 1..].starts_with(quote) {
                out.push(quote as u8);
                chars.next();
            } else {
                return Some((out, &rest[i + 1..]));
            }
        } else {
            let mut buf = [0u8; 4];
            out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
        }
    }
    None
}

fn decode_hex(h: &str) -> Option<Vec<u8>> {
    if !h.len().is_multiple_of(2) {
        return None;
    }
    (0..h.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&h[i..i + 2], 16).ok())
        .collect()
}

/// The xid after `XA COMMIT` / `XA ROLLBACK`: `gtrid [, bqual [, formatID]]`
/// (`bqual` defaults to empty, `formatID` to 1).
pub(crate) fn parse_query_xid(s: &str) -> Option<XaId> {
    let (gtrid, mut rest) = parse_xid_part(s)?;
    let mut bqual = Vec::new();
    let mut format_id = 1i64;
    if let Some(r) = rest.trim_start().strip_prefix(',') {
        let (b, r) = parse_xid_part(r)?;
        bqual = b;
        rest = r;
        if let Some(r) = rest.trim_start().strip_prefix(',') {
            let r = r.trim_start();
            let end = r
                .find(|c: char| !(c.is_ascii_digit() || c == '-'))
                .unwrap_or(r.len());
            format_id = r[..end].parse().ok()?;
        }
    }
    Some(XaId {
        format_id,
        gtrid,
        bqual,
    })
}

/// Decode an `XA_PREPARE_LOG_EVENT` body: `one_phase` (1 byte), `formatID`
/// (4, LE), `gtrid_length` (4, LE), `bqual_length` (4, LE), then the data.
pub(crate) fn parse_xa_prepare(body: &[u8]) -> Option<(bool, XaId)> {
    let one_phase = *body.first()? != 0;
    let word = |at: usize| -> Option<u32> {
        Some(u32::from_le_bytes(body.get(at..at + 4)?.try_into().ok()?))
    };
    let format_id = i64::from(word(1)? as i32);
    let gtrid_len = word(5)? as usize;
    let bqual_len = word(9)? as usize;
    let data = body.get(13..13 + gtrid_len + bqual_len)?;
    Some((
        one_phase,
        XaId {
            format_id,
            gtrid: data[..gtrid_len].to_vec(),
            bqual: data[gtrid_len..].to_vec(),
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(g: &[u8], b: &[u8], f: i64) -> XaId {
        XaId {
            format_id: f,
            gtrid: g.to_vec(),
            bqual: b.to_vec(),
        }
    }

    #[test]
    fn transaction_boundaries() {
        assert_eq!(classify_query("BEGIN", "d"), QueryKind::Begin);
        assert_eq!(classify_query("  /* app */ COMMIT", "d"), QueryKind::Commit);
        assert_eq!(classify_query("ROLLBACK", "d"), QueryKind::Commit);
        assert_eq!(classify_query("rollback work", "d"), QueryKind::Commit);
        assert_eq!(
            classify_query("SAVEPOINT `s1`", "d"),
            QueryKind::InTransaction
        );
        assert_eq!(
            classify_query("ROLLBACK TO SAVEPOINT s1", "d"),
            QueryKind::InTransaction
        );
        assert_eq!(
            classify_query("ROLLBACK TO s1", "d"),
            QueryKind::InTransaction
        );
        assert_eq!(
            classify_query("RELEASE SAVEPOINT s1", "d"),
            QueryKind::InTransaction
        );
        assert_eq!(
            classify_query("XA START X'61',X'',1", "d"),
            QueryKind::Begin
        );
        assert_eq!(
            classify_query("XA END X'61',X'',1", "d"),
            QueryKind::InTransaction
        );
        assert_eq!(
            classify_query("ALTER TABLE t ADD c INT", "d"),
            QueryKind::Ddl
        );
        assert_eq!(classify_query("", "d"), QueryKind::Ddl);
        assert_eq!(classify_query("/* unterminated", "d"), QueryKind::Ddl);
    }

    #[test]
    fn xa_outcomes_carry_their_xid() {
        assert_eq!(
            classify_query("XA COMMIT X'6774',X'62',7", "d"),
            QueryKind::XaOutcome {
                xid: id(b"gt", b"b", 7),
                commit: true
            }
        );
        assert_eq!(
            classify_query("XA ROLLBACK 'gt'", "d"),
            QueryKind::XaOutcome {
                xid: id(b"gt", b"", 1),
                commit: false
            }
        );
        assert_eq!(
            parse_query_xid(" 'a''b', \"c\\\"d\", -3"),
            Some(id(b"a'b", b"c\"d", -3))
        );
        assert_eq!(parse_query_xid("0x6162"), Some(id(b"ab", b"", 1)));
        assert_eq!(classify_query("XA COMMIT", "d"), QueryKind::Ddl);
        assert_eq!(classify_query("XA ROLLBACK X'6'", "d"), QueryKind::Ddl);
        assert_eq!(parse_query_xid("'x', 'y', z"), None);
        assert_eq!(parse_query_xid("'open"), None);
        assert_eq!(parse_query_xid("'esc\\"), None);
        assert_eq!(parse_query_xid("X'6"), None);
    }

    #[test]
    fn truncate_targets() {
        assert_eq!(
            classify_query("TRUNCATE TABLE `shop`.`order``s`", "d"),
            QueryKind::Truncate {
                schema: "shop".into(),
                table: "order`s".into()
            }
        );
        assert_eq!(
            classify_query("truncate items", "inventory"),
            QueryKind::Truncate {
                schema: "inventory".into(),
                table: "items".into()
            }
        );
        assert_eq!(classify_query("TRUNCATE TABLE `bad", "d"), QueryKind::Ddl);
        assert_eq!(classify_query("TRUNCATE TABLE db.`x", "d"), QueryKind::Ddl);
        assert_eq!(classify_query("TRUNCATE TABLE ", "d"), QueryKind::Ddl);
    }

    #[test]
    fn xa_prepare_event_body() {
        let mut body = vec![0u8];
        body.extend_from_slice(&7i32.to_le_bytes());
        body.extend_from_slice(&2u32.to_le_bytes());
        body.extend_from_slice(&1u32.to_le_bytes());
        body.extend_from_slice(b"gtb");
        assert_eq!(parse_xa_prepare(&body), Some((false, id(b"gt", b"b", 7))));
        body[0] = 1;
        assert!(parse_xa_prepare(&body).unwrap().0);
        assert_eq!(parse_xa_prepare(&body[..10]), None);
        assert_eq!(parse_xa_prepare(&[]), None);
    }
}
