//! ADO.NET-style connection strings (`Server=tcp:host,1433;User Id=sa;…`),
//! read with the grammar the SQL Server driver (`tiberius` 0.13) uses, so a
//! host screening a string and the driver opening it agree on every key.
//!
//! - pairs are separated by `;`; empty pairs and whitespace between pairs are
//!   skipped;
//! - a key runs to the first single `=` (`==` is a literal `=`), is trimmed and
//!   lower-cased, and may hold any other character;
//! - an unquoted value runs to the next `;`, trimmed;
//! - a value may be quoted with `'…'` or `"…"` (a doubled quote embeds one) or
//!   braced with `{…}` (running to the first `}`); only whitespace may follow
//!   the closing quote or brace;
//! - only ASCII input is accepted.

use std::fmt;

/// Why a string is not an ADO.NET connection string the driver would accept.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdoParseError(&'static str);

impl AdoParseError {
    /// The reason, in a few words.
    pub fn reason(&self) -> &'static str {
        self.0
    }
}

impl fmt::Display for AdoParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for AdoParseError {}

/// The `(key, value)` pairs of `input` in the order written, keys lower-cased
/// and values unquoted. A key may repeat; the driver uses the last one
/// ([`last`]).
pub fn parse(input: &str) -> Result<Vec<(String, String)>, AdoParseError> {
    if !input.is_ascii() {
        return Err(AdoParseError(
            "the connection string contains non-ASCII characters",
        ));
    }
    let b = input.as_bytes();
    let n = b.len();
    let mut i = 0;
    let mut pairs = Vec::new();
    while i < n {
        while i < n && (b[i] == b';' || b[i].is_ascii_whitespace()) {
            i += 1;
        }
        if i >= n {
            break;
        }
        let mut key = String::new();
        loop {
            if i >= n || b[i] == b';' {
                return Err(AdoParseError("key-value pairs must be joined by a `=`"));
            }
            if b[i] == b'=' {
                if i + 1 < n && b[i + 1] == b'=' {
                    key.push('=');
                    i += 2;
                    continue;
                }
                i += 1;
                break;
            }
            key.push(b[i] as char);
            i += 1;
        }
        let key = key
            .trim_matches(|c: char| c.is_ascii_whitespace())
            .to_ascii_lowercase();
        if key.is_empty() {
            return Err(AdoParseError("a key in the connection string is empty"));
        }
        while i < n && b[i].is_ascii_whitespace() {
            i += 1;
        }
        let value = if i < n && (b[i] == b'\'' || b[i] == b'"') {
            let quote = b[i];
            i += 1;
            let mut v = String::new();
            loop {
                if i >= n {
                    return Err(AdoParseError("the connection string has an unclosed quote"));
                }
                if b[i] == quote {
                    if i + 1 < n && b[i + 1] == quote {
                        v.push(quote as char);
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                v.push(b[i] as char);
                i += 1;
            }
            skip_trailing(b, &mut i)?;
            v
        } else if i < n && b[i] == b'{' {
            i += 1;
            let mut v = String::new();
            loop {
                if i >= n {
                    return Err(AdoParseError("the connection string has an unclosed brace"));
                }
                if b[i] == b'}' {
                    i += 1;
                    break;
                }
                v.push(b[i] as char);
                i += 1;
            }
            skip_trailing(b, &mut i)?;
            v
        } else {
            let start = i;
            while i < n && b[i] != b';' {
                i += 1;
            }
            input[start..i]
                .trim_end_matches(|c: char| c.is_ascii_whitespace())
                .to_string()
        };
        pairs.push((key, value));
        if i < n && b[i] == b';' {
            i += 1;
        }
    }
    Ok(pairs)
}

fn skip_trailing(b: &[u8], i: &mut usize) -> Result<(), AdoParseError> {
    while *i < b.len() && b[*i] != b';' {
        if !b[*i].is_ascii_whitespace() {
            return Err(AdoParseError(
                "unexpected characters after a quoted or braced value",
            ));
        }
        *i += 1;
    }
    Ok(())
}

/// The value the driver reads for `key` (lower case): the last pair with it.
pub fn last<'a>(pairs: &'a [(String, String)], key: &str) -> Option<&'a str> {
    pairs
        .iter()
        .rev()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
}

/// The host the driver connects to: the `server` value (else `data source`)
/// without a `tcp:` prefix, port or `\instance`, with `(local)` read as
/// `localhost`; `localhost` when neither key is set.
pub fn server_host(pairs: &[(String, String)]) -> String {
    match last(pairs, "server").or_else(|| last(pairs, "data source")) {
        Some(server) => host_of_server(server),
        None => "localhost".to_string(),
    }
}

/// The host part of an ADO.NET `Server=` value (`tcp:host\instance,port`),
/// read as the driver reads it: only a lower-case `tcp:` prefix is removed.
pub fn host_of_server(server: &str) -> String {
    let bare = server.strip_prefix("tcp:").unwrap_or(server);
    let bare = bare.split(',').next().unwrap_or(bare);
    let bare = bare.split('\\').next().unwrap_or(bare);
    bare.replace("(local)", "localhost")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn value(s: &str, key: &str) -> Option<String> {
        last(&parse(s).unwrap(), key).map(str::to_string)
    }

    #[test]
    fn reads_pairs_keys_and_separators_as_the_driver_does() {
        assert_eq!(
            value("Server=host;Database=db", "server").as_deref(),
            Some("host")
        );
        assert_eq!(value("  SeRVeR  =host", "server").as_deref(), Some("host"));
        assert_eq!(value(";;Server=host;;", "server").as_deref(), Some("host"));
        assert_eq!(value(" ; Server=host", "server").as_deref(), Some("host"));
        assert_eq!(
            value("App.Name=x;Server=h", "app.name").as_deref(),
            Some("x")
        );
        assert_eq!(value("a==b=c", "a=b").as_deref(), Some("c"));
        assert_eq!(
            value("Password=ab=cd", "password").as_deref(),
            Some("ab=cd")
        );
        assert_eq!(value("k=   a b   ", "k").as_deref(), Some("a b"));
        assert_eq!(value("k=", "k").as_deref(), Some(""));
        assert_eq!(value("k=a;k=b", "k").as_deref(), Some("b"));
        assert!(parse("").unwrap().is_empty());
        assert!(parse(" ;; ").unwrap().is_empty());
    }

    #[test]
    fn reads_quoted_and_braced_values() {
        assert_eq!(value("k=' a;b '", "k").as_deref(), Some(" a;b "));
        assert_eq!(value("k=\"a\"\"b\"", "k").as_deref(), Some("a\"b"));
        assert_eq!(value("k='a''b'", "k").as_deref(), Some("a'b"));
        assert_eq!(value("k={a;b} ;j=1", "k").as_deref(), Some("a;b"));
        assert_eq!(value("k='=v'", "k").as_deref(), Some("=v"));
    }

    #[test]
    fn refuses_what_the_driver_refuses() {
        for bad in [
            "k==v",
            "Server=h;junk",
            "=v",
            "k='open",
            "k=\"open",
            "k={open",
            "k='a'b",
            "k={a}b",
            "Server=hé",
        ] {
            let e = parse(bad).unwrap_err();
            assert!(!e.reason().is_empty());
            assert_eq!(e.to_string(), e.reason());
        }
    }

    #[test]
    fn names_the_server_host() {
        let host = |s: &str| server_host(&parse(s).unwrap());
        assert_eq!(host("Server=tcp:db.example.com,1433"), "db.example.com");
        assert_eq!(host("Data Source=db\\inst,1433"), "db");
        assert_eq!(host("Server=(local)"), "localhost");
        assert_eq!(host("Database=x"), "localhost");
        assert_eq!(host("Data Source=a;Server=b"), "b");
        assert_eq!(host("Server=TCP:h"), "TCP:h");
    }
}
