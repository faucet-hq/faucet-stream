//! JSON Lines sealed per line, with whole-file integrity (#789 FILE-24).
//!
//! A file opens with a sealed header line naming a random file id, every
//! record line is sealed on its own (so the file stays appendable), and a
//! sealed trailer line closes it with the record count and a SHA-256 digest
//! of every record line in order. A reader requires the trailer, so a line
//! dropped, duplicated, reordered or moved in from another file, and a file
//! cut short at a line boundary, all fail to open. Files written before the
//! header existed open line by line, with a warning that they carry no
//! whole-file check.

use base64::Engine as _;
use faucet_core::encryption::{
    SEALED_LINE_CONTEXT, SEALED_LINES_HEADER_CONTEXT, SEALED_LINES_TRAILER_CONTEXT,
};
use faucet_core::{CompiledEncryption, FaucetError, SealedLine};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

const VERSION: u32 = 2;

#[derive(Serialize, Deserialize)]
struct Header {
    faucet_sealed_lines: u32,
    file: String,
}

#[derive(Serialize, Deserialize)]
struct Trailer {
    file: String,
    count: u64,
    sha256: String,
}

fn b64(sealed: &[u8]) -> Vec<u8> {
    let mut line = base64::engine::general_purpose::STANDARD
        .encode(sealed)
        .into_bytes();
    line.push(b'\n');
    line
}

/// The running state of one sealed file being written.
#[derive(Clone)]
pub struct LineSeal {
    file: String,
    count: u64,
    digest: Sha256,
}

impl std::fmt::Debug for LineSeal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LineSeal")
            .field("file", &self.file)
            .field("count", &self.count)
            .finish()
    }
}

impl LineSeal {
    /// A new file with a fresh id.
    pub fn new() -> Self {
        Self {
            file: uuid::Uuid::new_v4().to_string(),
            count: 0,
            digest: Sha256::new(),
        }
    }

    /// Records sealed so far.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// The header line, newline-terminated.
    pub fn header(&self, enc: &CompiledEncryption) -> Vec<u8> {
        let body = serde_json::to_vec(&Header {
            faucet_sealed_lines: VERSION,
            file: self.file.clone(),
        })
        .expect("a header serializes");
        b64(&enc.encrypt_bound(&body, SEALED_LINES_HEADER_CONTEXT))
    }

    /// Seal one record's line, newline-terminated, and fold it into the digest.
    pub fn seal(&mut self, enc: &CompiledEncryption, plain: &[u8]) -> Vec<u8> {
        let line = b64(&enc.encrypt_bound(plain, SEALED_LINE_CONTEXT));
        self.digest.update(&line[..line.len() - 1]);
        self.digest.update(b"\n");
        self.count += 1;
        line
    }

    /// The trailer line, newline-terminated.
    pub fn trailer(&self, enc: &CompiledEncryption) -> Vec<u8> {
        let body = serde_json::to_vec(&Trailer {
            file: self.file.clone(),
            count: self.count,
            sha256: hex(&self.digest.clone().finalize()),
        })
        .expect("a trailer serializes");
        b64(&enc.encrypt_bound(&body, SEALED_LINES_TRAILER_CONTEXT))
    }
}

impl Default for LineSeal {
    fn default() -> Self {
        Self::new()
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// An opened per-line sealed file.
#[derive(Debug)]
pub enum Opened {
    /// A file with a header and trailer, verified whole. `body_len` is the
    /// byte length before the trailer line, where a writer continues.
    Sealed {
        /// The records' plaintext lines, each newline-terminated.
        plain: Vec<u8>,
        /// The state to continue writing the file with.
        seal: LineSeal,
        /// Bytes before the trailer line.
        body_len: usize,
    },
    /// A file written before whole-file integrity (no header): each line was
    /// opened on its own.
    Legacy {
        /// The records' plaintext lines, each newline-terminated.
        plain: Vec<u8>,
    },
    /// No lines at all.
    Empty,
}

fn sealed_bytes(line: &str, number: usize) -> Result<Vec<u8>, FaucetError> {
    base64::engine::general_purpose::STANDARD
        .decode(line)
        .ok()
        .filter(|b| faucet_core::encryption::is_encrypted(b))
        .ok_or_else(|| {
            FaucetError::Source(format!(
                "line {number} is not encrypted, but `encryption` is set — refusing to read \
                 plaintext as if it were authenticated"
            ))
        })
}

fn damaged(what: &str) -> FaucetError {
    FaucetError::Source(format!(
        "encrypted JSON Lines file failed its integrity check: {what}"
    ))
}

/// Open a per-line sealed file, verifying it whole when it has a header.
pub fn open(raw: &[u8], enc: &CompiledEncryption) -> Result<Opened, FaucetError> {
    let text = std::str::from_utf8(raw)
        .map_err(|e| FaucetError::Source(format!("encrypted lines are not UTF-8: {e}")))?;
    let mut offset = 0usize;
    let mut lines = Vec::new();
    for (i, line) in text.split_inclusive('\n').enumerate() {
        let start = offset;
        offset += line.len();
        let trimmed = line.trim();
        if !trimmed.is_empty() {
            lines.push((i + 1, start, trimmed));
        }
    }
    let Some(&(first_no, _, first)) = lines.first() else {
        return Ok(Opened::Empty);
    };
    let header = match enc.open_line(&sealed_bytes(first, first_no)?) {
        Ok(SealedLine::Header(body)) => serde_json::from_slice::<Header>(&body)
            .ok()
            .filter(|h| h.faucet_sealed_lines == VERSION)
            .ok_or_else(|| damaged("its header is unreadable"))?,
        Ok(SealedLine::Trailer(_)) => return Err(damaged("it starts with its trailer")),
        _ => return open_legacy(&lines, enc),
    };

    let mut seal = LineSeal {
        file: header.file,
        count: 0,
        digest: Sha256::new(),
    };
    let mut plain = Vec::new();
    for (k, &(number, start, line)) in lines.iter().enumerate().skip(1) {
        match enc.open_line(&sealed_bytes(line, number)?)? {
            SealedLine::Data(body) => {
                seal.digest.update(line.as_bytes());
                seal.digest.update(b"\n");
                seal.count += 1;
                plain.extend_from_slice(&body);
                plain.push(b'\n');
            }
            SealedLine::Header(_) => {
                return Err(damaged(&format!("line {number} is a second header")));
            }
            SealedLine::Trailer(body) => {
                if k + 1 != lines.len() {
                    return Err(damaged(&format!(
                        "line {number} is the trailer, but lines follow it"
                    )));
                }
                let t: Trailer = serde_json::from_slice(&body)
                    .map_err(|_| damaged("its trailer is unreadable"))?;
                if t.file != seal.file {
                    return Err(damaged("its trailer belongs to another file"));
                }
                if t.count != seal.count || t.sha256 != hex(&seal.digest.clone().finalize()) {
                    return Err(damaged(&format!(
                        "the trailer records {} line(s), the file holds {} — lines were \
                         dropped, added, reordered or altered",
                        t.count, seal.count
                    )));
                }
                return Ok(Opened::Sealed {
                    plain,
                    seal,
                    body_len: start,
                });
            }
        }
    }
    Err(damaged(
        "it ends without its trailer — the file was cut short",
    ))
}

fn open_legacy(
    lines: &[(usize, usize, &str)],
    enc: &CompiledEncryption,
) -> Result<Opened, FaucetError> {
    let mut plain = Vec::new();
    for &(number, _, line) in lines {
        plain.extend_from_slice(&enc.decrypt(&sealed_bytes(line, number)?)?);
        plain.push(b'\n');
    }
    tracing::warn!(
        "reading an encrypted JSON Lines file written before whole-file integrity checks: \
         each line is authenticated, but dropped or reordered lines can not be detected"
    );
    Ok(Opened::Legacy { plain })
}

/// Every record's plaintext line of a per-line sealed file, verified whole.
pub fn plaintext(raw: &[u8], enc: &CompiledEncryption) -> Result<Vec<u8>, FaucetError> {
    Ok(match open(raw, enc)? {
        Opened::Sealed { plain, .. } | Opened::Legacy { plain } => plain,
        Opened::Empty => Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enc() -> CompiledEncryption {
        let spec: faucet_core::EncryptionSpec =
            serde_json::from_value(serde_json::json!({"key": "k"})).unwrap();
        CompiledEncryption::compile(&spec).unwrap()
    }

    fn file(enc: &CompiledEncryption, rows: &[&str]) -> (Vec<Vec<u8>>, LineSeal) {
        let mut seal = LineSeal::new();
        let mut lines = vec![seal.header(enc)];
        for r in rows {
            lines.push(seal.seal(enc, r.as_bytes()));
        }
        lines.push(seal.trailer(enc));
        (lines, seal)
    }

    #[test]
    fn a_whole_file_opens_and_resumes_before_its_trailer() {
        let enc = enc();
        let (lines, seal) = file(&enc, &["{\"a\":1}", "{\"a\":2}"]);
        let raw = lines.concat();
        let Opened::Sealed {
            plain,
            seal: resumed,
            body_len,
        } = open(&raw, &enc).unwrap()
        else {
            panic!("sealed")
        };
        assert_eq!(plain, b"{\"a\":1}\n{\"a\":2}\n");
        assert_eq!(resumed.count(), 2);
        assert_eq!(body_len, raw.len() - lines.last().unwrap().len());
        assert_eq!(resumed.trailer(&enc).len(), seal.trailer(&enc).len());
        let mut more = raw[..body_len].to_vec();
        let mut resumed = resumed;
        more.extend(resumed.seal(&enc, b"{\"a\":3}"));
        more.extend(resumed.trailer(&enc));
        assert_eq!(
            plaintext(&more, &enc).unwrap(),
            b"{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n"
        );
        assert!(matches!(open(b"\n \n", &enc).unwrap(), Opened::Empty));
    }

    #[test]
    fn dropped_reordered_duplicated_or_cut_lines_are_refused() {
        let enc = enc();
        let (lines, _) = file(&enc, &["1", "2", "3"]);
        let cases: Vec<(&str, Vec<Vec<u8>>)> = vec![
            (
                "dropped",
                vec![
                    lines[0].clone(),
                    lines[1].clone(),
                    lines[3].clone(),
                    lines[4].clone(),
                ],
            ),
            (
                "reordered",
                vec![
                    lines[0].clone(),
                    lines[2].clone(),
                    lines[1].clone(),
                    lines[3].clone(),
                    lines[4].clone(),
                ],
            ),
            (
                "duplicated",
                vec![
                    lines[0].clone(),
                    lines[1].clone(),
                    lines[1].clone(),
                    lines[2].clone(),
                    lines[3].clone(),
                    lines[4].clone(),
                ],
            ),
            ("cut", lines[..3].to_vec()),
            (
                "after trailer",
                [lines.clone(), vec![lines[1].clone()]].concat(),
            ),
            (
                "second header",
                [vec![lines[0].clone()], lines.clone()].concat(),
            ),
            ("trailer first", vec![lines[4].clone()]),
        ];
        for (what, raw) in cases {
            let err = open(&raw.concat(), &enc).unwrap_err();
            assert!(err.to_string().contains("integrity"), "{what}: {err}");
        }
        let (other, _) = file(&enc, &["1", "2", "3"]);
        let transplanted = [&lines[..4], &other[4..]].concat();
        assert!(open(&transplanted.concat(), &enc).is_err());
        assert!(open(b"{\"plain\":1}\n", &enc).is_err());
        assert!(open(&[0xff], &enc).is_err());
    }

    #[test]
    fn legacy_files_open_line_by_line_and_refuse_bound_lines() {
        let enc = enc();
        let legacy = [
            b64(&enc.encrypt(b"{\"a\":1}")),
            b64(&enc.encrypt(b"{\"a\":2}")),
        ]
        .concat();
        assert_eq!(plaintext(&legacy, &enc).unwrap(), b"{\"a\":1}\n{\"a\":2}\n");
        let (lines, _) = file(&enc, &["1"]);
        let stripped = [b64(&enc.encrypt(b"x")), lines[1].clone()].concat();
        assert!(
            open(&stripped, &enc).is_err(),
            "a header-less file can not smuggle bound lines"
        );
    }
}
