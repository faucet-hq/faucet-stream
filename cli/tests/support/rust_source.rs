//! A minimal Rust-source reader for source-scanning tests: string literals
//! (comments, char literals and raw strings skipped) and the part of a file
//! before its `#[cfg(test)]` module.
#![allow(dead_code)]

/// Every non-raw string literal in `src` as `(line, contents)`.
pub fn literals(src: &str) -> Vec<(usize, &str)> {
    literal_spans(src, false)
        .into_iter()
        .map(|(at, lit)| (src[..at].matches('\n').count() + 1, lit))
        .collect()
}

/// Every string literal in `src` as `(byte offset of its opening, contents)`;
/// raw strings (`r"…"`, `r#"…"#`) are included when `raw` is set.
pub fn literal_spans(src: &str, raw: bool) -> Vec<(usize, &str)> {
    let b = src.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        if src[i..].starts_with("//") {
            i = src[i..].find('\n').map_or(b.len(), |j| i + j);
            continue;
        }
        if src[i..].starts_with("/*") {
            i = src[i + 2..].find("*/").map_or(b.len(), |j| i + 2 + j + 2);
            continue;
        }
        let ident_before = i > 0 && (b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_');
        if b[i] == b'r' && !ident_before && matches!(b.get(i + 1), Some(b'"' | b'#')) {
            let mut j = i + 1;
            let mut hashes = 0;
            while b.get(j) == Some(&b'#') {
                hashes += 1;
                j += 1;
            }
            if b.get(j) == Some(&b'"') {
                let close = format!("\"{}", "#".repeat(hashes));
                let end = src[j + 1..].find(&close).map_or(b.len(), |k| j + 1 + k);
                if raw {
                    out.push((i, &src[j + 1..end]));
                }
                i = (end + close.len()).min(b.len());
                continue;
            }
        }
        if b[i] == b'\'' {
            // A char literal (`'"'`, `'\''`) or a lifetime.
            if b.get(i + 1) == Some(&b'\\') {
                i = src[i + 2..].find('\'').map_or(b.len(), |k| i + 2 + k + 1);
            } else if let Some(c) = src[i + 1..].chars().next()
                && src[i + 1 + c.len_utf8()..].starts_with('\'')
            {
                i += 1 + c.len_utf8() + 1;
            } else {
                i += 1;
            }
            continue;
        }
        if b[i] == b'"' {
            let start = i + 1;
            let mut j = start;
            while j < b.len() && b[j] != b'"' {
                j += if b[j] == b'\\' { 2 } else { 1 };
            }
            let end = j.min(b.len());
            out.push((i, &src[start..end]));
            i = end + 1;
            continue;
        }
        i += 1;
    }
    out
}

/// The part of a source file before its first `#[cfg(test)]` module.
pub fn non_test(src: &str) -> &str {
    const ATTR: &str = "#[cfg(test)]";
    let mut from = 0;
    while let Some(k) = src[from..].find(ATTR) {
        let at = from + k;
        let mut rest = src[at + ATTR.len()..].trim_start();
        // Further attributes and line comments may sit between `#[cfg(test)]` and `mod`.
        loop {
            if rest.starts_with("#[") {
                rest = rest.find(']').map_or("", |e| rest[e + 1..].trim_start());
            } else if rest.starts_with("//") {
                rest = rest.find('\n').map_or("", |e| rest[e + 1..].trim_start());
            } else {
                break;
            }
        }
        if rest.starts_with("mod ") {
            return &src[..at];
        }
        from = at + ATTR.len();
    }
    src
}
