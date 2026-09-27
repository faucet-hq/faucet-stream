//! The `filter:` predicate DSL: parsing into an [`Expr`] tree and typing it
//! against a table schema as an Iceberg [`Predicate`] for scan pushdown.

use faucet_core::FaucetError;
use iceberg::expr::{Predicate, Reference};
use iceberg::spec::{Datum, PrimitiveType, Schema, Type};

/// A comparison operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    /// `=` / `==`
    Eq,
    /// `!=` / `<>`
    Ne,
    /// `<`
    Lt,
    /// `<=`
    Le,
    /// `>`
    Gt,
    /// `>=`
    Ge,
}

/// An untyped literal as written in the filter.
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    /// An integer (`42`, `-7`).
    Int(i64),
    /// A number with a fraction or exponent (`1.5`, `2e3`).
    Float(f64),
    /// A quoted string (`'x'` / `"x"`).
    Str(String),
    /// `true` / `false`.
    Bool(bool),
}

/// A parsed filter expression.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// `a and b`
    And(Box<Expr>, Box<Expr>),
    /// `a or b`
    Or(Box<Expr>, Box<Expr>),
    /// `not a`
    Not(Box<Expr>),
    /// `column <op> literal`
    Cmp {
        /// Column name (dot path for nested fields).
        column: String,
        /// Operator.
        op: CmpOp,
        /// Right-hand literal.
        value: Literal,
    },
    /// `column [not] in (literal, …)`
    In {
        /// Column name.
        column: String,
        /// `not in`.
        negated: bool,
        /// Candidate values.
        values: Vec<Literal>,
    },
    /// `column is [not] null`
    IsNull {
        /// Column name.
        column: String,
        /// `is not null`.
        negated: bool,
    },
    /// `column [not] starts_with 'prefix'`
    StartsWith {
        /// Column name.
        column: String,
        /// `not starts_with`.
        negated: bool,
        /// Prefix.
        prefix: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Ident(String),
    Str(String),
    Num(String),
    Op(&'static str),
    LParen,
    RParen,
    Comma,
}

fn err(msg: impl std::fmt::Display) -> FaucetError {
    FaucetError::Config(format!("iceberg: invalid `filter`: {msg}"))
}

fn tokenize(input: &str) -> Result<Vec<Token>, FaucetError> {
    let chars: Vec<char> = input.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match c {
            c if c.is_whitespace() => i += 1,
            '(' => {
                out.push(Token::LParen);
                i += 1;
            }
            ')' => {
                out.push(Token::RParen);
                i += 1;
            }
            ',' => {
                out.push(Token::Comma);
                i += 1;
            }
            '\'' | '"' => {
                let quote = c;
                let mut s = String::new();
                i += 1;
                loop {
                    match chars.get(i) {
                        None => return Err(err("unterminated string literal")),
                        Some(&ch) if ch == quote => {
                            if chars.get(i + 1) == Some(&quote) {
                                s.push(quote);
                                i += 2;
                            } else {
                                i += 1;
                                break;
                            }
                        }
                        Some(&ch) => {
                            s.push(ch);
                            i += 1;
                        }
                    }
                }
                out.push(Token::Str(s));
            }
            '`' => {
                let start = i + 1;
                let end = chars[start..]
                    .iter()
                    .position(|&ch| ch == '`')
                    .ok_or_else(|| err("unterminated `quoted` column name"))?;
                out.push(Token::Ident(chars[start..start + end].iter().collect()));
                i = start + end + 1;
            }
            '=' => {
                i += if chars.get(i + 1) == Some(&'=') { 2 } else { 1 };
                out.push(Token::Op("="));
            }
            '!' => {
                if chars.get(i + 1) != Some(&'=') {
                    return Err(err("expected `!=`"));
                }
                out.push(Token::Op("!="));
                i += 2;
            }
            '<' => match chars.get(i + 1) {
                Some('=') => {
                    out.push(Token::Op("<="));
                    i += 2;
                }
                Some('>') => {
                    out.push(Token::Op("!="));
                    i += 2;
                }
                _ => {
                    out.push(Token::Op("<"));
                    i += 1;
                }
            },
            '>' => {
                if chars.get(i + 1) == Some(&'=') {
                    out.push(Token::Op(">="));
                    i += 2;
                } else {
                    out.push(Token::Op(">"));
                    i += 1;
                }
            }
            c if c.is_ascii_digit() || c == '-' || c == '+' || c == '.' => {
                let start = i;
                i += 1;
                while i < chars.len() {
                    let ch = chars[i];
                    let exp_sign = (ch == '-' || ch == '+')
                        && matches!(chars.get(i - 1), Some('e') | Some('E'));
                    if ch.is_ascii_digit() || ch == '.' || ch == 'e' || ch == 'E' || exp_sign {
                        i += 1;
                    } else {
                        break;
                    }
                }
                out.push(Token::Num(chars[start..i].iter().collect()));
            }
            c if c.is_alphabetic() || c == '_' => {
                let start = i;
                while i < chars.len()
                    && (chars[i].is_alphanumeric() || chars[i] == '_' || chars[i] == '.')
                {
                    i += 1;
                }
                out.push(Token::Ident(chars[start..i].iter().collect()));
            }
            other => return Err(err(format!("unexpected character {other:?}"))),
        }
    }
    Ok(out)
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn next(&mut self) -> Option<Token> {
        let t = self.tokens.get(self.pos).cloned();
        self.pos += 1;
        t
    }

    fn keyword(&self, kw: &str) -> bool {
        matches!(self.peek(), Some(Token::Ident(s)) if s.eq_ignore_ascii_case(kw))
    }

    fn eat_keyword(&mut self, kw: &str) -> bool {
        if self.keyword(kw) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn or(&mut self) -> Result<Expr, FaucetError> {
        let mut left = self.and()?;
        while self.eat_keyword("or") {
            left = Expr::Or(Box::new(left), Box::new(self.and()?));
        }
        Ok(left)
    }

    fn and(&mut self) -> Result<Expr, FaucetError> {
        let mut left = self.unary()?;
        while self.eat_keyword("and") {
            left = Expr::And(Box::new(left), Box::new(self.unary()?));
        }
        Ok(left)
    }

    fn unary(&mut self) -> Result<Expr, FaucetError> {
        if self.eat_keyword("not") {
            return Ok(Expr::Not(Box::new(self.unary()?)));
        }
        if self.peek() == Some(&Token::LParen) {
            self.pos += 1;
            let e = self.or()?;
            if self.next() != Some(Token::RParen) {
                return Err(err("expected `)`"));
            }
            return Ok(e);
        }
        self.comparison()
    }

    fn literal(&mut self) -> Result<Literal, FaucetError> {
        match self.next() {
            Some(Token::Str(s)) => Ok(Literal::Str(s)),
            Some(Token::Num(n)) => parse_number(&n),
            Some(Token::Ident(s)) if s.eq_ignore_ascii_case("true") => Ok(Literal::Bool(true)),
            Some(Token::Ident(s)) if s.eq_ignore_ascii_case("false") => Ok(Literal::Bool(false)),
            Some(t) => Err(err(format!("expected a literal, found {t:?}"))),
            None => Err(err("expected a literal, found end of input")),
        }
    }

    fn comparison(&mut self) -> Result<Expr, FaucetError> {
        let column = match self.next() {
            Some(Token::Ident(s)) => s,
            Some(t) => return Err(err(format!("expected a column name, found {t:?}"))),
            None => return Err(err("expected a column name, found end of input")),
        };
        if self.eat_keyword("is") {
            let negated = self.eat_keyword("not");
            if !self.eat_keyword("null") {
                return Err(err(format!("expected `null` after `{column} is`")));
            }
            return Ok(Expr::IsNull { column, negated });
        }
        let negated = self.eat_keyword("not");
        if self.eat_keyword("in") {
            if self.next() != Some(Token::LParen) {
                return Err(err("expected `(` after `in`"));
            }
            let mut values = vec![self.literal()?];
            loop {
                match self.next() {
                    Some(Token::Comma) => values.push(self.literal()?),
                    Some(Token::RParen) => break,
                    _ => return Err(err("expected `,` or `)` in `in (…)` list")),
                }
            }
            return Ok(Expr::In {
                column,
                negated,
                values,
            });
        }
        if self.eat_keyword("starts_with") {
            return match self.literal()? {
                Literal::Str(prefix) => Ok(Expr::StartsWith {
                    column,
                    negated,
                    prefix,
                }),
                _ => Err(err("`starts_with` takes a string literal")),
            };
        }
        if negated {
            return Err(err(format!(
                "expected `in` or `starts_with` after `{column} not`"
            )));
        }
        let op = match self.next() {
            Some(Token::Op("=")) => CmpOp::Eq,
            Some(Token::Op("!=")) => CmpOp::Ne,
            Some(Token::Op("<")) => CmpOp::Lt,
            Some(Token::Op("<=")) => CmpOp::Le,
            Some(Token::Op(">")) => CmpOp::Gt,
            Some(Token::Op(">=")) => CmpOp::Ge,
            _ => return Err(err(format!("expected an operator after `{column}`"))),
        };
        let value = self.literal()?;
        Ok(Expr::Cmp { column, op, value })
    }
}

fn parse_number(n: &str) -> Result<Literal, FaucetError> {
    if let Ok(i) = n.parse::<i64>() {
        return Ok(Literal::Int(i));
    }
    n.parse::<f64>()
        .ok()
        .filter(|f| f.is_finite())
        .map(Literal::Float)
        .ok_or_else(|| err(format!("{n:?} is not a number")))
}

/// Parse a filter expression.
pub fn parse(input: &str) -> Result<Expr, FaucetError> {
    let tokens = tokenize(input)?;
    if tokens.is_empty() {
        return Err(err("expression is empty"));
    }
    let mut p = Parser { tokens, pos: 0 };
    let e = p.or()?;
    if let Some(t) = p.peek() {
        return Err(err(format!("unexpected trailing token {t:?}")));
    }
    Ok(e)
}

fn primitive<'a>(schema: &'a Schema, column: &str) -> Result<&'a PrimitiveType, FaucetError> {
    let field = schema.field_by_name(column).ok_or_else(|| {
        FaucetError::Config(format!(
            "iceberg: `filter` references column {column:?}, which is not in the table schema"
        ))
    })?;
    match field.field_type.as_ref() {
        Type::Primitive(p) => Ok(p),
        other => Err(FaucetError::Config(format!(
            "iceberg: `filter` column {column:?} has non-primitive type {other} and cannot be compared"
        ))),
    }
}

/// Render `raw` with exactly `scale` fractional digits; refuses a lossy value.
pub fn rescale_decimal(raw: &str, scale: u32) -> Option<String> {
    let raw = raw.trim();
    let (neg, digits) = match raw.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, raw.strip_prefix('+').unwrap_or(raw)),
    };
    let (int, frac) = digits.split_once('.').unwrap_or((digits, ""));
    if (int.is_empty() && frac.is_empty())
        || !int.chars().all(|c| c.is_ascii_digit())
        || !frac.chars().all(|c| c.is_ascii_digit())
    {
        return None;
    }
    let scale = scale as usize;
    let frac = if frac.len() > scale {
        let (keep, extra) = frac.split_at(scale);
        if extra.chars().any(|c| c != '0') {
            return None;
        }
        keep.to_string()
    } else {
        format!("{frac:0<scale$}")
    };
    let int = if int.is_empty() { "0" } else { int };
    let sign = if neg { "-" } else { "" };
    Some(if scale == 0 {
        format!("{sign}{int}")
    } else {
        format!("{sign}{int}.{frac}")
    })
}

fn literal_text(lit: &Literal) -> String {
    match lit {
        Literal::Int(i) => i.to_string(),
        Literal::Float(f) => f.to_string(),
        Literal::Str(s) => s.clone(),
        Literal::Bool(b) => b.to_string(),
    }
}

/// Type `lit` as a [`Datum`] of the column's primitive type.
pub fn datum_for(column: &str, lit: &Literal, ty: &PrimitiveType) -> Result<Datum, FaucetError> {
    let mismatch = || {
        FaucetError::Config(format!(
            "iceberg: `filter` literal {} cannot be compared with column {column:?} of type {ty}",
            match lit {
                Literal::Str(s) => format!("'{s}'"),
                other => literal_text(other),
            }
        ))
    };
    let parsed = |r: iceberg::Result<Datum>| r.map_err(|_| mismatch());
    match (ty, lit) {
        (PrimitiveType::Boolean, Literal::Bool(b)) => Ok(Datum::bool(*b)),
        (PrimitiveType::Int, Literal::Int(i)) => {
            i32::try_from(*i).map(Datum::int).map_err(|_| mismatch())
        }
        (PrimitiveType::Long, Literal::Int(i)) => Ok(Datum::long(*i)),
        (PrimitiveType::Float, Literal::Int(i)) => Ok(Datum::float(*i as f32)),
        (PrimitiveType::Float, Literal::Float(f)) => Ok(Datum::float(*f as f32)),
        (PrimitiveType::Double, Literal::Int(i)) => Ok(Datum::double(*i as f64)),
        (PrimitiveType::Double, Literal::Float(f)) => Ok(Datum::double(*f)),
        (
            PrimitiveType::Decimal { scale, .. },
            Literal::Int(_) | Literal::Float(_) | Literal::Str(_),
        ) => {
            let text = rescale_decimal(&literal_text(lit), *scale).ok_or_else(mismatch)?;
            parsed(Datum::decimal_from_str(text).and_then(|d| d.to(&Type::Primitive(ty.clone()))))
        }
        (PrimitiveType::Date, Literal::Str(s)) => parsed(Datum::date_from_str(s)),
        (PrimitiveType::Time, Literal::Str(s)) => parsed(Datum::time_from_str(s)),
        (PrimitiveType::Timestamp, Literal::Str(s)) => parsed(Datum::timestamp_from_str(s)),
        (PrimitiveType::Timestamp, Literal::Int(i)) => Ok(Datum::timestamp_micros(*i)),
        (PrimitiveType::Timestamptz, Literal::Str(s)) => parsed(Datum::timestamptz_from_str(s)),
        (PrimitiveType::Timestamptz, Literal::Int(i)) => Ok(Datum::timestamptz_micros(*i)),
        (PrimitiveType::String, Literal::Str(s)) => Ok(Datum::string(s)),
        (PrimitiveType::Uuid, Literal::Str(s)) => parsed(Datum::uuid_from_str(s)),
        _ => Err(mismatch()),
    }
}

/// Type a parsed filter against `schema` as an Iceberg [`Predicate`].
pub fn to_predicate(expr: &Expr, schema: &Schema) -> Result<Predicate, FaucetError> {
    Ok(match expr {
        Expr::And(a, b) => to_predicate(a, schema)?.and(to_predicate(b, schema)?),
        Expr::Or(a, b) => to_predicate(a, schema)?.or(to_predicate(b, schema)?),
        Expr::Not(a) => to_predicate(a, schema)?.negate(),
        Expr::IsNull { column, negated } => {
            primitive(schema, column)?;
            let r = Reference::new(column.clone());
            if *negated {
                r.is_not_null()
            } else {
                r.is_null()
            }
        }
        Expr::Cmp { column, op, value } => {
            let d = datum_for(column, value, primitive(schema, column)?)?;
            let r = Reference::new(column.clone());
            match op {
                CmpOp::Eq => r.equal_to(d),
                CmpOp::Ne => r.not_equal_to(d),
                CmpOp::Lt => r.less_than(d),
                CmpOp::Le => r.less_than_or_equal_to(d),
                CmpOp::Gt => r.greater_than(d),
                CmpOp::Ge => r.greater_than_or_equal_to(d),
            }
        }
        Expr::In {
            column,
            negated,
            values,
        } => {
            let ty = primitive(schema, column)?;
            let datums = values
                .iter()
                .map(|v| datum_for(column, v, ty))
                .collect::<Result<Vec<_>, _>>()?;
            let r = Reference::new(column.clone());
            if *negated {
                r.is_not_in(datums)
            } else {
                r.is_in(datums)
            }
        }
        Expr::StartsWith {
            column,
            negated,
            prefix,
        } => {
            let ty = primitive(schema, column)?;
            if *ty != PrimitiveType::String {
                return Err(FaucetError::Config(format!(
                    "iceberg: `starts_with` needs a string column, {column:?} is {ty}"
                )));
            }
            let r = Reference::new(column.clone());
            let d = Datum::string(prefix);
            if *negated {
                r.not_starts_with(d)
            } else {
                r.starts_with(d)
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::expr::Bind;
    use iceberg::spec::{NestedField, StructType};
    use std::sync::Arc;

    fn cmp(column: &str, op: CmpOp, value: Literal) -> Expr {
        Expr::Cmp {
            column: column.into(),
            op,
            value,
        }
    }

    #[test]
    fn parses_comparisons_and_precedence() {
        let e = parse("a = 1 or b > 2.5 and not c <= 'x'").unwrap();
        assert_eq!(
            e,
            Expr::Or(
                Box::new(cmp("a", CmpOp::Eq, Literal::Int(1))),
                Box::new(Expr::And(
                    Box::new(cmp("b", CmpOp::Gt, Literal::Float(2.5))),
                    Box::new(Expr::Not(Box::new(cmp(
                        "c",
                        CmpOp::Le,
                        Literal::Str("x".into())
                    ))))
                ))
            )
        );
    }

    #[test]
    fn parses_every_operator_spelling() {
        for (src, op) in [
            ("a == 1", CmpOp::Eq),
            ("a = 1", CmpOp::Eq),
            ("a != 1", CmpOp::Ne),
            ("a <> 1", CmpOp::Ne),
            ("a < 1", CmpOp::Lt),
            ("a <= 1", CmpOp::Le),
            ("a > 1", CmpOp::Gt),
            ("a >= 1", CmpOp::Ge),
        ] {
            assert_eq!(parse(src).unwrap(), cmp("a", op, Literal::Int(1)), "{src}");
        }
    }

    #[test]
    fn parses_in_null_starts_with_parens_and_literals() {
        assert_eq!(
            parse("x NOT IN (1, -2, 'z', TRUE, false, 1e3)").unwrap(),
            Expr::In {
                column: "x".into(),
                negated: true,
                values: vec![
                    Literal::Int(1),
                    Literal::Int(-2),
                    Literal::Str("z".into()),
                    Literal::Bool(true),
                    Literal::Bool(false),
                    Literal::Float(1000.0),
                ]
            }
        );
        assert_eq!(
            parse("(`weird col` is not null)").unwrap(),
            Expr::IsNull {
                column: "weird col".into(),
                negated: true
            }
        );
        assert_eq!(
            parse("a.b is null").unwrap(),
            Expr::IsNull {
                column: "a.b".into(),
                negated: false
            }
        );
        assert_eq!(
            parse("s not starts_with \"it's\"").unwrap(),
            Expr::StartsWith {
                column: "s".into(),
                negated: true,
                prefix: "it's".into()
            }
        );
        assert_eq!(
            parse("s starts_with 'a''b'").unwrap(),
            Expr::StartsWith {
                column: "s".into(),
                negated: false,
                prefix: "a'b".into()
            }
        );
        assert_eq!(
            parse("x in (1)").unwrap(),
            Expr::In {
                column: "x".into(),
                negated: false,
                values: vec![Literal::Int(1)]
            }
        );
        assert_eq!(
            parse("n > 2.5e-1").unwrap(),
            cmp("n", CmpOp::Gt, Literal::Float(0.25))
        );
    }

    #[test]
    fn rejects_malformed_input() {
        for bad in [
            "",
            "   ",
            "a = ",
            "a",
            "a = 1 extra",
            "(a = 1",
            "a ! 1",
            "a = 'open",
            "`open = 1",
            "a is maybe",
            "a not = 1",
            "a in 1",
            "a in (1 2)",
            "a starts_with 1",
            "= 1",
            "a = )",
            "a = 1..2",
            "a # 1",
        ] {
            let e = parse(bad).unwrap_err();
            assert!(e.to_string().contains("filter"), "{bad:?}: {e}");
        }
    }

    fn schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::optional(
                    1,
                    "b",
                    Type::Primitive(PrimitiveType::Boolean),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "i",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(NestedField::optional(
                    3,
                    "l",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    4,
                    "f",
                    Type::Primitive(PrimitiveType::Float),
                )),
                Arc::new(NestedField::optional(
                    5,
                    "d",
                    Type::Primitive(PrimitiveType::Double),
                )),
                Arc::new(NestedField::optional(
                    6,
                    "dec",
                    Type::Primitive(PrimitiveType::Decimal {
                        precision: 10,
                        scale: 2,
                    }),
                )),
                Arc::new(NestedField::optional(
                    7,
                    "dt",
                    Type::Primitive(PrimitiveType::Date),
                )),
                Arc::new(NestedField::optional(
                    8,
                    "tm",
                    Type::Primitive(PrimitiveType::Time),
                )),
                Arc::new(NestedField::optional(
                    9,
                    "ts",
                    Type::Primitive(PrimitiveType::Timestamp),
                )),
                Arc::new(NestedField::optional(
                    10,
                    "tz",
                    Type::Primitive(PrimitiveType::Timestamptz),
                )),
                Arc::new(NestedField::optional(
                    11,
                    "s",
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::optional(
                    12,
                    "u",
                    Type::Primitive(PrimitiveType::Uuid),
                )),
                Arc::new(NestedField::optional(
                    13,
                    "st",
                    Type::Struct(StructType::new(vec![Arc::new(NestedField::optional(
                        14,
                        "inner",
                        Type::Primitive(PrimitiveType::Long),
                    ))])),
                )),
                Arc::new(NestedField::optional(
                    15,
                    "bin",
                    Type::Primitive(PrimitiveType::Binary),
                )),
            ])
            .build()
            .unwrap()
    }

    fn pred(src: &str) -> Result<Predicate, FaucetError> {
        to_predicate(&parse(src).unwrap(), &schema())
    }

    #[test]
    fn types_literals_against_columns_and_binds() {
        let s = Arc::new(schema());
        for src in [
            "b = true",
            "i = 3",
            "l >= -9",
            "f < 1",
            "f < 1.5",
            "d != 2",
            "d > 2.25",
            "dec = 12.3",
            "dec = '1.50'",
            "dec = 7",
            "dt = '2026-01-31'",
            "tm = '10:11:12'",
            "ts = '2026-01-31T10:00:00'",
            "ts > 1000",
            "tz = '2026-01-31T10:00:00Z'",
            "tz < 5",
            "s = 'x'",
            "u = '0f8fad5b-d9cb-469f-a165-70867728950e'",
            "st.inner = 4",
            "i in (1, 2) and l not in (3)",
            "s is null or i is not null",
            "s starts_with 'ab' and s not starts_with 'abc'",
            "not (i < 1)",
        ] {
            let p = pred(src).unwrap_or_else(|e| panic!("{src}: {e}"));
            p.bind(s.clone(), true)
                .unwrap_or_else(|e| panic!("{src} should bind: {e}"));
        }
    }

    #[test]
    fn rejects_type_mismatches_and_unknown_columns() {
        for (src, needle) in [
            ("nope = 1", "not in the table schema"),
            ("st = 1", "non-primitive"),
            ("b = 1", "cannot be compared"),
            ("i = 3000000000", "cannot be compared"),
            ("i = 1.5", "cannot be compared"),
            ("s = 1", "cannot be compared"),
            ("dec = 1.234", "cannot be compared"),
            ("dec = 'abc'", "cannot be compared"),
            ("dt = 'not-a-date'", "cannot be compared"),
            ("bin = 'x'", "cannot be compared"),
            ("i starts_with 'x'", "string column"),
            ("i in (1, 'x')", "cannot be compared"),
            ("b = true and s = 2", "cannot be compared"),
            ("b = true or s = 2", "cannot be compared"),
            ("not s = 2", "cannot be compared"),
            ("zz is null", "not in the table schema"),
        ] {
            let e = pred(src).unwrap_err().to_string();
            assert!(e.contains(needle), "{src}: {e}");
        }
    }

    #[test]
    fn rescale_decimal_cases() {
        assert_eq!(rescale_decimal("12.3", 2).as_deref(), Some("12.30"));
        assert_eq!(rescale_decimal("-1.500", 2).as_deref(), Some("-1.50"));
        assert_eq!(rescale_decimal("+.5", 1).as_deref(), Some("0.5"));
        assert_eq!(rescale_decimal("7", 0).as_deref(), Some("7"));
        assert_eq!(rescale_decimal("7.0", 0).as_deref(), Some("7"));
        assert_eq!(rescale_decimal("1.234", 2), None);
        assert_eq!(rescale_decimal("1e3", 2), None);
        assert_eq!(rescale_decimal(".", 2), None);
        assert_eq!(rescale_decimal("1.x", 2), None);
    }
}
