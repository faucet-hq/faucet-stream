//! Parser for LogMiner `SQL_REDO` text (mined with `NO_ROWID_IN_STMT` +
//! `NO_SQL_DELIMITER`). Pure — every shape here was captured from a real
//! Oracle 23ai redo stream.
//!
//! ```text
//! insert into "APP"."T"("ID","NAME") values ('1','it''s')
//! update "APP"."T" set "NAME" = 'bob', "AMT" = NULL where "ID" = '1' and "AMT" IS NULL
//! delete from "APP"."T" where "ID" = '1' and "D" = TO_DATE('2024-01-02 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
//! DECLARE ... BEGIN select "NOTE" into loc_c from "APP"."T" where "ID" = '1' for update;
//!   buf_c := 'aaa'; dbms_lob.write(loc_c, 3, 1, buf_c); END;
//! ```

use faucet_common_oracle::hex_to_bytes;

/// A literal from redo text, before typing against the column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resolved {
    /// SQL `NULL`.
    Null,
    /// Text of a quoted literal, number or `TO_*(...)` call.
    Text(String),
    /// Bytes of a `HEXTORAW(...)`.
    Bytes(Vec<u8>),
    /// `EMPTY_CLOB()` / `EMPTY_BLOB()` — content follows as LOB writes.
    EmptyLob,
}

/// Ordered `column → value` pairs.
pub type Image = Vec<(String, Resolved)>;

/// The DML statement kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DmlKind {
    /// `insert into … values …`
    Insert,
    /// `update … set … where …`
    Update,
    /// `delete from … where …`
    Delete,
}

/// A parsed DML statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Dml {
    /// Statement kind.
    pub kind: DmlKind,
    /// Owner.
    pub owner: String,
    /// Table.
    pub table: String,
    /// Inserted values (`insert`) or assignments (`update`).
    pub set: Image,
    /// `where` equalities / `IS NULL` tests — the before image.
    pub conditions: Image,
}

/// One `dbms_lob.write` applied to a LOB column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LobPiece {
    /// 1-based offset (characters for CLOB, bytes for BLOB).
    pub offset: u64,
    /// The written data.
    pub data: Resolved,
}

/// A parsed LOB change block (`LOB_WRITE` / `LOB_TRIM`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LobChange {
    /// Owner.
    pub owner: String,
    /// Table.
    pub table: String,
    /// The LOB column.
    pub column: String,
    /// Row identity from the locator `select … where …`.
    pub conditions: Image,
    /// Writes, in order.
    pub writes: Vec<LobPiece>,
    /// Final length after a `dbms_lob.trim`.
    pub trim: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Tok {
    Ident(String),
    Str(String),
    Word(String),
    Sym(&'static str),
}

fn tokenize(s: &str) -> Result<Vec<Tok>, String> {
    let chars: Vec<char> = s.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match c {
            c if c.is_whitespace() => i += 1,
            '"' => {
                let end = chars[i + 1..]
                    .iter()
                    .position(|&ch| ch == '"')
                    .ok_or("unterminated quoted identifier")?;
                out.push(Tok::Ident(chars[i + 1..i + 1 + end].iter().collect()));
                i += end + 2;
            }
            '\'' => {
                let mut text = String::new();
                i += 1;
                loop {
                    match chars.get(i) {
                        None => return Err("unterminated string literal".into()),
                        Some('\'') if chars.get(i + 1) == Some(&'\'') => {
                            text.push('\'');
                            i += 2;
                        }
                        Some('\'') => {
                            i += 1;
                            break;
                        }
                        Some(&ch) => {
                            text.push(ch);
                            i += 1;
                        }
                    }
                }
                out.push(Tok::Str(text));
            }
            '(' | ')' | ',' | '=' | '.' | ';' => {
                out.push(Tok::Sym(match c {
                    '(' => "(",
                    ')' => ")",
                    ',' => ",",
                    '=' => "=",
                    '.' => ".",
                    _ => ";",
                }));
                i += 1;
            }
            '|' if chars.get(i + 1) == Some(&'|') => {
                out.push(Tok::Sym("||"));
                i += 2;
            }
            ':' if chars.get(i + 1) == Some(&'=') => {
                out.push(Tok::Sym(":="));
                i += 2;
            }
            c if c.is_ascii_digit()
                || ((c == '-' || c == '+')
                    && chars
                        .get(i + 1)
                        .is_some_and(|d| d.is_ascii_digit() || *d == '.')) =>
            {
                let start = i;
                i += 1;
                while i < chars.len() {
                    let ch = chars[i];
                    let exp_sign = (ch == '+' || ch == '-') && matches!(chars[i - 1], 'e' | 'E');
                    if ch.is_ascii_digit() || ch == '.' || ch == 'e' || ch == 'E' || exp_sign {
                        i += 1;
                    } else {
                        break;
                    }
                }
                out.push(Tok::Word(chars[start..i].iter().collect()));
            }
            c if c.is_alphanumeric() || c == '_' || c == '$' || c == '#' => {
                let start = i;
                while i < chars.len()
                    && (chars[i].is_alphanumeric() || matches!(chars[i], '_' | '$' | '#'))
                {
                    i += 1;
                }
                out.push(Tok::Word(chars[start..i].iter().collect()));
            }
            other => return Err(format!("unexpected character {other:?} in redo")),
        }
    }
    Ok(out)
}

struct Cursor {
    toks: Vec<Tok>,
    pos: usize,
}

impl Cursor {
    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos)
    }

    fn next(&mut self) -> Option<Tok> {
        let t = self.toks.get(self.pos).cloned();
        self.pos += 1;
        t
    }

    fn at_word(&self, w: &str) -> bool {
        matches!(self.peek(), Some(Tok::Word(x)) if x.eq_ignore_ascii_case(w))
    }

    fn at_sym(&self, s: &str) -> bool {
        matches!(self.peek(), Some(Tok::Sym(x)) if *x == s)
    }

    fn word(&mut self, w: &str) -> Result<(), String> {
        if self.at_word(w) {
            self.pos += 1;
            Ok(())
        } else {
            Err(format!("expected `{w}`, found {:?}", self.peek()))
        }
    }

    fn sym(&mut self, s: &str) -> Result<(), String> {
        if self.at_sym(s) {
            self.pos += 1;
            Ok(())
        } else {
            Err(format!("expected `{s}`, found {:?}", self.peek()))
        }
    }

    fn ident(&mut self) -> Result<String, String> {
        match self.next() {
            Some(Tok::Ident(s)) => Ok(s),
            other => Err(format!("expected a quoted identifier, found {other:?}")),
        }
    }

    fn table_ref(&mut self) -> Result<(String, String), String> {
        let first = self.ident()?;
        if self.at_sym(".") {
            self.pos += 1;
            Ok((first, self.ident()?))
        } else {
            Ok((String::new(), first))
        }
    }

    fn value(&mut self) -> Result<Resolved, String> {
        let mut v = self.term()?;
        while self.at_sym("||") {
            self.pos += 1;
            let rhs = self.term()?;
            v = concat(v, rhs)?;
        }
        Ok(v)
    }

    fn term(&mut self) -> Result<Resolved, String> {
        match self.next() {
            Some(Tok::Str(s)) => Ok(Resolved::Text(s)),
            Some(Tok::Word(w)) if w.eq_ignore_ascii_case("NULL") => Ok(Resolved::Null),
            Some(Tok::Word(w)) if self.at_sym("(") => {
                self.pos += 1;
                let mut args = Vec::new();
                if !self.at_sym(")") {
                    args.push(self.value()?);
                    while self.at_sym(",") {
                        self.pos += 1;
                        args.push(self.value()?);
                    }
                }
                self.sym(")")?;
                apply_function(&w, args)
            }
            Some(Tok::Word(w))
                if w.starts_with(|c: char| c.is_ascii_digit() || c == '-' || c == '+') =>
            {
                Ok(Resolved::Text(w))
            }
            other => Err(format!("unexpected value token {other:?}")),
        }
    }

    /// `"C" = v [and "D" IS NULL]…`, stopping at the end or at `stop`.
    fn conditions(&mut self, stop: Option<&str>) -> Result<Image, String> {
        let mut out = Vec::new();
        loop {
            if self.peek().is_none() || self.at_sym(";") || stop.is_some_and(|s| self.at_word(s)) {
                break;
            }
            if self.at_word("ROWID") {
                self.pos += 1;
                self.sym("=")?;
                self.value()?;
            } else {
                let col = self.ident()?;
                if self.at_word("IS") {
                    self.pos += 1;
                    self.word("NULL")?;
                    out.push((col, Resolved::Null));
                } else {
                    self.sym("=")?;
                    out.push((col, self.value()?));
                }
            }
            if self.at_word("and") {
                self.pos += 1;
            } else {
                break;
            }
        }
        Ok(out)
    }

    fn expect_end(&mut self) -> Result<(), String> {
        while self.at_sym(";") {
            self.pos += 1;
        }
        match self.peek() {
            None => Ok(()),
            Some(t) => Err(format!("unexpected trailing token {t:?}")),
        }
    }
}

fn concat(a: Resolved, b: Resolved) -> Result<Resolved, String> {
    let text = |r: Resolved| match r {
        Resolved::Null | Resolved::EmptyLob => Ok(String::new()),
        Resolved::Text(s) => Ok(s),
        Resolved::Bytes(_) => Err("cannot concatenate a RAW value".to_string()),
    };
    Ok(Resolved::Text(text(a)? + &text(b)?))
}

fn apply_function(name: &str, args: Vec<Resolved>) -> Result<Resolved, String> {
    let first = || args.first().cloned().unwrap_or(Resolved::Null);
    match name.to_ascii_uppercase().as_str() {
        "TO_DATE" | "TO_TIMESTAMP" | "TO_TIMESTAMP_TZ" | "TO_DSINTERVAL" | "TO_YMINTERVAL"
        | "TO_NUMBER" | "TO_BINARY_DOUBLE" | "TO_BINARY_FLOAT" | "TO_CHAR" | "TO_NCHAR"
        | "TO_CLOB" | "TO_NCLOB" => Ok(first()),
        "HEXTORAW" => match first() {
            Resolved::Text(h) => hex_to_bytes(&h)
                .map(Resolved::Bytes)
                .ok_or_else(|| format!("invalid HEXTORAW literal {h:?}")),
            other => Ok(other),
        },
        "UNISTR" => match first() {
            Resolved::Text(s) => decode_unistr(&s).map(Resolved::Text),
            other => Ok(other),
        },
        "EMPTY_CLOB" | "EMPTY_BLOB" => Ok(Resolved::EmptyLob),
        other => Err(format!("unsupported function {other}() in redo")),
    }
}

/// Decode `UNISTR` escapes: `\XXXX` UTF-16 code units, `\\` a backslash.
pub fn decode_unistr(s: &str) -> Result<String, String> {
    let mut units: Vec<u16> = Vec::new();
    let mut out = String::new();
    let flush = |units: &mut Vec<u16>, out: &mut String| -> Result<(), String> {
        if !units.is_empty() {
            out.push_str(&String::from_utf16(units).map_err(|e| format!("bad UNISTR: {e}"))?);
            units.clear();
        }
        Ok(())
    };
    let chars: Vec<char> = s.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '\\' {
            if chars.get(i + 1) == Some(&'\\') {
                flush(&mut units, &mut out)?;
                out.push('\\');
                i += 2;
                continue;
            }
            let hex: String = chars
                .get(i + 1..i + 5)
                .ok_or("truncated UNISTR escape")?
                .iter()
                .collect();
            units.push(
                u16::from_str_radix(&hex, 16).map_err(|_| format!("bad UNISTR escape {hex}"))?,
            );
            i += 5;
        } else {
            flush(&mut units, &mut out)?;
            out.push(chars[i]);
            i += 1;
        }
    }
    flush(&mut units, &mut out)?;
    Ok(out)
}

/// Parse an `insert` / `update` / `delete` redo statement.
pub fn parse_dml(sql: &str) -> Result<Dml, String> {
    let mut c = Cursor {
        toks: tokenize(sql)?,
        pos: 0,
    };
    let dml = if c.at_word("insert") {
        c.pos += 1;
        c.word("into")?;
        let (owner, table) = c.table_ref()?;
        c.sym("(")?;
        let mut cols = vec![c.ident()?];
        while c.at_sym(",") {
            c.pos += 1;
            cols.push(c.ident()?);
        }
        c.sym(")")?;
        c.word("values")?;
        c.sym("(")?;
        let mut vals = vec![c.value()?];
        while c.at_sym(",") {
            c.pos += 1;
            vals.push(c.value()?);
        }
        c.sym(")")?;
        if cols.len() != vals.len() {
            return Err(format!("{} columns but {} values", cols.len(), vals.len()));
        }
        Dml {
            kind: DmlKind::Insert,
            owner,
            table,
            set: cols.into_iter().zip(vals).collect(),
            conditions: Vec::new(),
        }
    } else if c.at_word("update") {
        c.pos += 1;
        let (owner, table) = c.table_ref()?;
        c.word("set")?;
        let mut set = Vec::new();
        loop {
            let col = c.ident()?;
            c.sym("=")?;
            set.push((col, c.value()?));
            if c.at_sym(",") {
                c.pos += 1;
            } else {
                break;
            }
        }
        let conditions = if c.at_word("where") {
            c.pos += 1;
            c.conditions(None)?
        } else {
            Vec::new()
        };
        Dml {
            kind: DmlKind::Update,
            owner,
            table,
            set,
            conditions,
        }
    } else if c.at_word("delete") {
        c.pos += 1;
        c.word("from")?;
        let (owner, table) = c.table_ref()?;
        let conditions = if c.at_word("where") {
            c.pos += 1;
            c.conditions(None)?
        } else {
            Vec::new()
        };
        Dml {
            kind: DmlKind::Delete,
            owner,
            table,
            set: Vec::new(),
            conditions,
        }
    } else {
        return Err(format!("not a DML statement: {}", truncate(sql)));
    };
    c.expect_end()?;
    Ok(dml)
}

/// Parse a `LOB_WRITE` / `LOB_TRIM` PL/SQL block.
pub fn parse_lob_change(sql: &str) -> Result<LobChange, String> {
    let mut c = Cursor {
        toks: tokenize(sql)?,
        pos: 0,
    };
    let mut target: Option<(String, String, String, Image)> = None;
    let mut buffer: Option<Resolved> = None;
    let mut writes = Vec::new();
    let mut trim = None;
    while let Some(tok) = c.next() {
        let Tok::Word(w) = tok else { continue };
        if w.eq_ignore_ascii_case("select") && matches!(c.peek(), Some(Tok::Ident(_))) {
            let column = c.ident()?;
            c.word("into")?;
            c.next();
            c.word("from")?;
            let (owner, table) = c.table_ref()?;
            let conditions = if c.at_word("where") {
                c.pos += 1;
                c.conditions(Some("for"))?
            } else {
                Vec::new()
            };
            target = Some((owner, table, column, conditions));
        } else if w.to_ascii_lowercase().starts_with("buf_") && c.at_sym(":=") {
            c.pos += 1;
            buffer = Some(c.value()?);
        } else if w.eq_ignore_ascii_case("dbms_lob") && c.at_sym(".") {
            c.pos += 1;
            let op = match c.next() {
                Some(Tok::Word(op)) => op.to_ascii_lowercase(),
                other => return Err(format!("expected a dbms_lob call, found {other:?}")),
            };
            c.sym("(")?;
            let mut args = Vec::new();
            while !c.at_sym(")") {
                match c.next() {
                    Some(Tok::Word(a)) => args.push(a),
                    Some(Tok::Sym(",")) => {}
                    other => return Err(format!("unexpected dbms_lob argument {other:?}")),
                }
            }
            c.sym(")")?;
            let num = |i: usize| -> Result<u64, String> {
                args.get(i)
                    .and_then(|a| a.parse::<u64>().ok())
                    .ok_or_else(|| format!("dbms_lob.{op}: bad argument {i}"))
            };
            match op.as_str() {
                "write" => writes.push(LobPiece {
                    offset: num(2)?.max(1),
                    data: buffer.clone().ok_or("dbms_lob.write before any buffer")?,
                }),
                "trim" => trim = Some(num(1)?),
                other => return Err(format!("unsupported dbms_lob.{other}")),
            }
        }
    }
    let (owner, table, column, conditions) = target.ok_or("LOB change without a locator select")?;
    Ok(LobChange {
        owner,
        table,
        column,
        conditions,
        writes,
        trim,
    })
}

/// Apply LOB pieces to a column's current value.
pub fn apply_lob(current: &Resolved, change: &LobChange) -> Resolved {
    let binary = matches!(current, Resolved::Bytes(_))
        || change
            .writes
            .iter()
            .any(|w| matches!(w.data, Resolved::Bytes(_)));
    if binary {
        let mut bytes = match current {
            Resolved::Bytes(b) => b.clone(),
            _ => Vec::new(),
        };
        for w in &change.writes {
            let data = match &w.data {
                Resolved::Bytes(b) => b.clone(),
                Resolved::Text(t) => t.as_bytes().to_vec(),
                _ => Vec::new(),
            };
            let at = (w.offset - 1) as usize;
            if bytes.len() < at + data.len() {
                bytes.resize(at + data.len(), 0);
            }
            bytes[at..at + data.len()].copy_from_slice(&data);
        }
        if let Some(n) = change.trim {
            bytes.truncate(n as usize);
        }
        return Resolved::Bytes(bytes);
    }
    let mut chars: Vec<char> = match current {
        Resolved::Text(t) => t.chars().collect(),
        _ => Vec::new(),
    };
    for w in &change.writes {
        let data: Vec<char> = match &w.data {
            Resolved::Text(t) => t.chars().collect(),
            _ => Vec::new(),
        };
        let at = (w.offset - 1) as usize;
        if chars.len() < at + data.len() {
            chars.resize(at + data.len(), ' ');
        }
        chars[at..at + data.len()].copy_from_slice(&data);
    }
    if let Some(n) = change.trim {
        chars.truncate(n as usize);
    }
    Resolved::Text(chars.into_iter().collect())
}

fn truncate(s: &str) -> String {
    s.chars().take(120).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(s: &str) -> Resolved {
        Resolved::Text(s.into())
    }

    #[test]
    fn insert_with_every_literal_shape() {
        let sql = r#"insert into "FAUCET"."CDC_U"("ID","NAME","AMT","D","TS","TSZ","BIN","NV","NOTE","IDS","IYM","F") values ('1','it''s','12.5',TO_DATE('2024-01-02 00:00:00', 'YYYY-MM-DD HH24:MI:SS'),TO_TIMESTAMP('2024-01-02 03:04:05.500000000'),TO_TIMESTAMP_TZ('2024-01-02 03:04:05.000000000 +05:30'),HEXTORAW('0102'),UNISTR('h\00E9llo'),EMPTY_CLOB(),TO_DSINTERVAL('+01 02:03:04.500000'),TO_YMINTERVAL('+01-02'),'2.5E+000')"#;
        let d = parse_dml(sql).unwrap();
        assert_eq!(d.kind, DmlKind::Insert);
        assert_eq!((d.owner.as_str(), d.table.as_str()), ("FAUCET", "CDC_U"));
        let get = |c: &str| d.set.iter().find(|(k, _)| k == c).unwrap().1.clone();
        assert_eq!(get("ID"), t("1"));
        assert_eq!(get("NAME"), t("it's"));
        assert_eq!(get("D"), t("2024-01-02 00:00:00"));
        assert_eq!(get("TSZ"), t("2024-01-02 03:04:05.000000000 +05:30"));
        assert_eq!(get("BIN"), Resolved::Bytes(vec![1, 2]));
        assert_eq!(get("NV"), t("héllo"));
        assert_eq!(get("NOTE"), Resolved::EmptyLob);
        assert_eq!(get("IDS"), t("+01 02:03:04.500000"));
        assert_eq!(get("F"), t("2.5E+000"));
        assert!(d.conditions.is_empty());
    }

    #[test]
    fn update_and_delete_conditions() {
        let d = parse_dml(
            r#"update "A"."T" set "NAME" = 'bob', "AMT" = NULL where "ID" = '1' and "AMT" IS NULL and ROWID = 'AAAR5J'"#,
        )
        .unwrap();
        assert_eq!(d.kind, DmlKind::Update);
        assert_eq!(
            d.set,
            vec![("NAME".into(), t("bob")), ("AMT".into(), Resolved::Null)]
        );
        assert_eq!(
            d.conditions,
            vec![("ID".into(), t("1")), ("AMT".into(), Resolved::Null)]
        );

        let d = parse_dml(r#"delete from "A"."T" where "ID" = 12.5 and "N" = -3;"#).unwrap();
        assert_eq!(d.kind, DmlKind::Delete);
        assert_eq!(
            d.conditions,
            vec![("ID".into(), t("12.5")), ("N".into(), t("-3"))]
        );

        let d = parse_dml(r#"delete from "T""#).unwrap();
        assert_eq!(d.owner, "");
        assert!(d.conditions.is_empty());
        let d = parse_dml(r#"update "T" set "A" = 'x'"#).unwrap();
        assert!(d.conditions.is_empty());
    }

    #[test]
    fn concatenation_and_numbers() {
        let d = parse_dml(r#"insert into "A"."T"("S","E","P") values ('ab' || NULL || UNISTR('\0063'), 1.5E+10, +4)"#)
            .unwrap();
        assert_eq!(d.set[0].1, t("abc"));
        assert_eq!(d.set[1].1, t("1.5E+10"));
        assert_eq!(d.set[2].1, t("+4"));
    }

    #[test]
    fn malformed_redo_is_an_error() {
        for bad in [
            "set transaction read write",
            r#"insert into "A"."T"("X") values ('1', '2')"#,
            r#"insert into "A"."T"("X") values (FOO('1'))"#,
            r#"insert into "A"."T"("X") values (HEXTORAW('zz'))"#,
            r#"insert into "A"."T"("X") values ('1') extra"#,
            r#"insert into "A"."T"("X) values ('1')"#,
            r#"insert into "A"."T"("X") values ('1)"#,
            r#"insert into "A"."T"("X") values ('a' || HEXTORAW('01'))"#,
            r#"update "A"."T" set "X" = 'a' where "Y" IS 'b'"#,
            r#"update "A"."T" set "X" 'a'"#,
            r#"update A set "X" = 'a'"#,
            r#"delete "A"."T""#,
            "insert into \"A\".\"T\"(\"X\") values (@)",
            r#"insert into "A"."T"("X") values (INTO)"#,
        ] {
            assert!(parse_dml(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn unistr_decoding() {
        assert_eq!(decode_unistr(r"h\00E9llo").unwrap(), "héllo");
        assert_eq!(decode_unistr(r"\D83D\DE00!").unwrap(), "😀!");
        assert_eq!(decode_unistr(r"a\\b").unwrap(), r"a\b");
        assert!(decode_unistr(r"\00").is_err());
        assert!(decode_unistr(r"\ZZZZ").is_err());
        assert!(decode_unistr(r"\D83D").is_err());
        assert_eq!(
            apply_function("UNISTR", vec![Resolved::Null]).unwrap(),
            Resolved::Null
        );
        assert_eq!(apply_function("HEXTORAW", vec![]).unwrap(), Resolved::Null);
        assert_eq!(apply_function("TO_DATE", vec![]).unwrap(), Resolved::Null);
    }

    const LOB_WRITE: &str = "DECLARE\n loc_c CLOB;\n buf_c VARCHAR2(6156);\n loc_b BLOB;\n buf_b RAW(6156);\n loc_nc NCLOB;\n buf_nc NVARCHAR2(6156);\n e_len NUMBER;\nBEGIN\n select \"NOTE\" into loc_c from \"FAUCET\".\"LOB_V\" where \"ID\" = '1' for update;\n\n buf_c := 'aa''b';\n  dbms_lob.write(loc_c, 4, 3, buf_c);\nEND;";

    #[test]
    fn lob_write_block() {
        let c = parse_lob_change(LOB_WRITE).unwrap();
        assert_eq!(
            (c.owner.as_str(), c.table.as_str(), c.column.as_str()),
            ("FAUCET", "LOB_V", "NOTE")
        );
        assert_eq!(c.conditions, vec![("ID".into(), t("1"))]);
        assert_eq!(
            c.writes,
            vec![LobPiece {
                offset: 3,
                data: t("aa'b")
            }]
        );
        assert_eq!(apply_lob(&t("xy"), &c), t("xyaa'b"));
        assert_eq!(apply_lob(&Resolved::EmptyLob, &c), t("  aa'b"));
        assert_eq!(apply_lob(&t("0123456789"), &c), t("01aa'b6789"));
    }

    #[test]
    fn blob_write_and_trim() {
        let sql = "DECLARE loc_b BLOB; buf_b RAW(10); BEGIN select \"B\" into loc_b from \"A\".\"T\" where \"ID\" = '2' for update; buf_b := HEXTORAW('ABCD'); dbms_lob.write(loc_b, 2, 0, buf_b); dbms_lob.trim(loc_b, 1); END;";
        let c = parse_lob_change(sql).unwrap();
        assert_eq!(c.writes[0].offset, 1);
        assert_eq!(c.trim, Some(1));
        assert_eq!(apply_lob(&Resolved::Null, &c), Resolved::Bytes(vec![0xAB]));
        assert_eq!(
            apply_lob(&Resolved::Bytes(vec![1, 2, 3]), &c),
            Resolved::Bytes(vec![0xAB])
        );
        let trim_only = "DECLARE loc_c CLOB; BEGIN select \"N\" into loc_c from \"A\".\"T\" where \"ID\" = '2' for update; dbms_lob.trim(loc_c, 2); END;";
        let c = parse_lob_change(trim_only).unwrap();
        assert_eq!(apply_lob(&t("hello"), &c), t("he"));
        let mut mixed = c.clone();
        mixed.writes.push(LobPiece {
            offset: 1,
            data: t("Z"),
        });
        mixed.trim = None;
        assert_eq!(
            apply_lob(&Resolved::Bytes(vec![1]), &mixed),
            Resolved::Bytes(b"Z".to_vec())
        );
        mixed.writes[0].data = Resolved::Null;
        assert_eq!(apply_lob(&t("q"), &mixed), t("q"));
        assert_eq!(
            apply_lob(&Resolved::Bytes(vec![9]), &mixed),
            Resolved::Bytes(vec![9])
        );
    }

    #[test]
    fn lob_block_errors() {
        assert!(parse_lob_change("BEGIN dbms_lob.write(loc_c, 1, 1, buf_c); END;").is_err());
        assert!(parse_lob_change("BEGIN END;").is_err());
        let no_select = "BEGIN buf_c := 'a'; END;";
        assert!(parse_lob_change(no_select).is_err());
        let erase = "BEGIN select \"N\" into loc_c from \"A\".\"T\" where \"ID\" = '1' for update; dbms_lob.erase(loc_c, 1, 1); END;";
        assert!(parse_lob_change(erase).is_err());
        let bad_arg = "BEGIN select \"N\" into loc_c from \"A\".\"T\" for update; buf_c := 'a'; dbms_lob.write(loc_c, 1, x, buf_c); END;";
        assert!(parse_lob_change(bad_arg).is_err());
        let str_arg = "BEGIN select \"N\" into loc_c from \"A\".\"T\" for update; buf_c := 'a'; dbms_lob.write(loc_c, 'x'); END;";
        assert!(parse_lob_change(str_arg).is_err());
        let not_call =
            "BEGIN select \"N\" into loc_c from \"A\".\"T\" for update; dbms_lob.(x); END;";
        assert!(parse_lob_change(not_call).is_err());
        let no_where = "BEGIN select \"N\" into loc_c from \"A\".\"T\" for update; END;";
        assert!(parse_lob_change(no_where).unwrap().conditions.is_empty());
    }
}
