//! Apache Avro Object Container Files (#719).
//!
//! # Reading
//!
//! An OCF embeds its writer schema, so a file decodes on its own. When a
//! *reader* schema is supplied (`avro.schema` on a source) every file is
//! resolved against it with Avro's schema-resolution rules — the way to
//! project a subset of fields, rename through aliases, or read files written
//! across a schema's evolution into one shape. [`ContainerDecoder`](super::container::ContainerDecoder)
//! adds the multi-file rule: without an explicit reader schema the first
//! file's writer schema becomes the reader schema for the rest, so a prefix
//! whose later files cannot be resolved against the first fails with an error
//! naming both files instead of producing rows of two shapes.
//!
//! # Logical types
//!
//! Mapped explicitly, never through a lossy numeric fallback:
//!
//! | Avro | JSON record | Arrow |
//! |---|---|---|
//! | `decimal(p, s)` | exact decimal string (`"12.30"`) | `Decimal128(p, s)` (`Decimal256` above 38 digits) |
//! | `big-decimal` | decimal string | `Utf8` |
//! | `date` | `"YYYY-MM-DD"` | `Date32` |
//! | `time-millis` / `time-micros` | `"HH:MM:SS.fff"` / `"HH:MM:SS.ffffff"` | `Time32(ms)` / `Time64(µs)` |
//! | `timestamp-{millis,micros,nanos}` | RFC 3339 UTC (`"…Z"`) at that precision | `Timestamp(unit, "UTC")` |
//! | `local-timestamp-*` | naive ISO 8601 | `Timestamp(unit, None)` |
//! | `uuid` | canonical string | `Utf8` |
//! | `duration` | `{months, days, millis}` | `Struct` |
//!
//! `bytes` and `fixed` are lowercase hex — the same encoding the Arrow JSON
//! writer uses for binary columns, so an Avro file and a Parquet file carrying
//! the same bytes produce the same records. `NaN` and the infinities, which
//! JSON cannot hold, become the strings `"NaN"`, `"Infinity"`, `"-Infinity"`.
//!
//! A union of `null` and one type is that type, nullable. A union of several
//! non-null types decodes to whichever branch the value holds; on the Arrow
//! path — where a column needs one type — it is a `Utf8` column holding the
//! value's JSON. A recursive record is likewise `Utf8` JSON on the Arrow path
//! (Arrow has no recursive types) and a nested object on the record path.
//!
//! # Writing
//!
//! With `avro.schema` set the records are encoded against it; logical types
//! are accepted in the shapes above (plus epoch integers). Without one the
//! schema is inferred from the records being written: `integer` → `long`,
//! `number` → `double`, objects → nested records, a field that is absent or
//! null anywhere → `["null", T]` with a `null` default, and a field whose
//! values mix types → `string`. Field names that are not valid Avro names are
//! sanitized (`first-name` → `first_name`, a leading digit gains `_`), and the
//! original name is kept as the field's `faucet.name` attribute.

use super::{AvroCodec, AvroOptions};
use crate::error::FaucetError;
use apache_avro::schema::{DecimalSchema, RecordField, UnionSchema};
use apache_avro::types::Value as Av;
use apache_avro::{Codec, Schema};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, SecondsFormat, Timelike, Utc};
use serde_json::{Map, Value, json};
use std::collections::{HashMap, HashSet};

/// Attribute holding a field's original name when it had to be sanitized.
pub const ORIGINAL_NAME_ATTR: &str = "faucet.name";

/// Name of the top-level record an inferred schema declares.
pub const INFERRED_RECORD_NAME: &str = "faucet_record";

/// The field a non-record top-level datum is wrapped in, so every record is an
/// object whatever the file's root type.
pub const VALUE_FIELD: &str = "value";

fn codec(c: AvroCodec) -> Codec {
    match c {
        AvroCodec::Null => Codec::Null,
        AvroCodec::Deflate => Codec::Deflate(Default::default()),
        AvroCodec::Snappy => Codec::Snappy,
        AvroCodec::Zstd => Codec::Zstandard(Default::default()),
    }
}

impl AvroOptions {
    /// The configured schema, parsed, if any.
    pub fn parsed_schema(&self) -> Result<Option<Schema>, FaucetError> {
        self.schema.as_ref().map(parse_schema).transpose()
    }
}

/// Parse an Avro schema from its JSON form.
pub fn parse_schema(v: &Value) -> Result<Schema, FaucetError> {
    Schema::parse(v)
        .map_err(|e| FaucetError::Config(format!("avro.schema is not a valid Avro schema: {e}")))
}

/// Decode a whole OCF into records, resolved against `opts.schema` when set.
pub fn decode(bytes: &[u8], opts: &AvroOptions) -> Result<Vec<Value>, FaucetError> {
    let reader_schema = opts.parsed_schema()?;
    let mut out = Vec::new();
    read_records(bytes, reader_schema.as_ref(), usize::MAX, &mut |chunk| {
        out.extend(chunk);
        Ok(())
    })
    .map_err(|e| FaucetError::Source(format!("avro: {e}")))?;
    Ok(out)
}

/// The writer schema an OCF declares, read from its header only.
pub fn writer_schema<R: std::io::Read>(reader: R) -> Result<Schema, FaucetError> {
    let r = apache_avro::Reader::new(reader)
        .map_err(|e| FaucetError::Source(format!("avro header: {e}")))?;
    Ok(r.writer_schema().clone())
}

/// Stream an OCF's records in chunks of `chunk` (at least 1), resolved against
/// `reader_schema` when given. Returns the schema the records are shaped by.
pub fn read_records<R: std::io::Read>(
    reader: R,
    reader_schema: Option<&Schema>,
    chunk: usize,
    f: &mut dyn FnMut(Vec<Value>) -> Result<(), FaucetError>,
) -> Result<Schema, FaucetError> {
    read_with(
        reader,
        reader_schema,
        chunk,
        Mode::Record,
        &mut |chunk, _| f(chunk),
    )
}

/// How a datum is rendered: plain JSON, or JSON the Arrow decoder can type
/// against [`arrow_schema`] (multi-branch unions and recursion as JSON text).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
    Record,
    Arrow,
}

/// Receives each decoded chunk with the schema it is shaped by.
pub(crate) type ChunkSink<'a> = dyn FnMut(Vec<Value>, &Schema) -> Result<(), FaucetError> + 'a;

pub(crate) fn read_with<R: std::io::Read>(
    reader: R,
    reader_schema: Option<&Schema>,
    chunk: usize,
    mode: Mode,
    f: &mut ChunkSink<'_>,
) -> Result<Schema, FaucetError> {
    let builder = apache_avro::Reader::builder(reader);
    let r = match reader_schema {
        Some(s) => builder.reader_schema(s).build(),
        None => builder.build(),
    }
    .map_err(|e| FaucetError::Source(format!("avro header: {e}")))?;
    let schema = reader_schema
        .cloned()
        .unwrap_or_else(|| r.writer_schema().clone());
    let names = named(&schema);
    let chunk = chunk.max(1);
    let mut buf = Vec::with_capacity(chunk.min(4096));
    for datum in r {
        let datum = datum.map_err(|e| FaucetError::Source(format!("avro datum: {e}")))?;
        buf.push(root_to_json(&datum, &schema, &names, mode)?);
        if buf.len() >= chunk {
            f(std::mem::take(&mut buf), &schema)?;
        }
    }
    if !buf.is_empty() {
        f(buf, &schema)?;
    }
    Ok(schema)
}

type Names<'a> = HashMap<String, &'a Schema>;

fn named(schema: &Schema) -> Names<'_> {
    let mut out = HashMap::new();
    collect_named(schema, &mut out);
    out
}

fn collect_named<'a>(s: &'a Schema, out: &mut Names<'a>) {
    match s {
        Schema::Record(r) => {
            out.insert(r.name.fullname(None), s);
            out.entry(r.name.name().to_string()).or_insert(s);
            for f in &r.fields {
                collect_named(&f.schema, out);
            }
        }
        Schema::Enum(e) => {
            out.insert(e.name.fullname(None), s);
            out.entry(e.name.name().to_string()).or_insert(s);
        }
        Schema::Fixed(x) | Schema::Duration(x) => {
            out.insert(x.name.fullname(None), s);
            out.entry(x.name.name().to_string()).or_insert(s);
        }
        Schema::Array(a) => collect_named(&a.items, out),
        Schema::Map(m) => collect_named(&m.types, out),
        Schema::Union(u) => u.variants().iter().for_each(|v| collect_named(v, out)),
        _ => {}
    }
}

fn resolve<'a>(s: &'a Schema, names: &Names<'a>) -> Result<&'a Schema, FaucetError> {
    match s {
        Schema::Ref { name } => names
            .get(&name.fullname(None))
            .or_else(|| names.get(name.name()))
            .copied()
            .ok_or_else(|| {
                FaucetError::Source(format!(
                    "avro: unresolved schema reference `{}`",
                    name.fullname(None)
                ))
            }),
        other => Ok(other),
    }
}

fn ref_name(s: &Schema) -> Option<String> {
    match s {
        Schema::Ref { name } => Some(name.fullname(None)),
        Schema::Record(r) => Some(r.name.fullname(None)),
        _ => None,
    }
}

/// A union needing one Arrow column for several value types.
pub(crate) fn is_complex_union(u: &UnionSchema) -> bool {
    u.variants()
        .iter()
        .filter(|v| !matches!(v, Schema::Null))
        .count()
        > 1
}

/// Convert one decoded Avro datum to JSON with the logical-type mapping in the
/// module docs (exact decimal strings, hex bytes, ISO temporals). Unlike the
/// file reader, a non-record datum is returned as-is rather than wrapped in
/// [`VALUE_FIELD`] — the shape a single-message codec (a Kafka value) wants.
pub fn datum_to_json(datum: &Av, schema: &Schema) -> Result<Value, FaucetError> {
    let names = named(schema);
    to_json(datum, schema, &names, Mode::Record, &mut Vec::new())
}

/// Build the Avro datum for `value` under `schema`, accepting the JSON shapes
/// [`datum_to_json`] produces (the inverse mapping). The error names the path
/// that did not fit.
pub fn datum_from_json(value: &Value, schema: &Schema) -> Result<Av, String> {
    let names = named(schema);
    from_json(value, schema, &names, "")
}

fn root_to_json(
    v: &Av,
    schema: &Schema,
    names: &Names<'_>,
    mode: Mode,
) -> Result<Value, FaucetError> {
    let mut stack = Vec::new();
    let resolved = resolve(schema, names)?;
    let j = to_json(v, schema, names, mode, &mut stack)?;
    Ok(match (resolved, j) {
        (Schema::Record(_), j @ Value::Object(_)) => j,
        (_, j) => {
            let mut m = Map::new();
            m.insert(VALUE_FIELD.into(), j);
            Value::Object(m)
        }
    })
}

fn to_json(
    v: &Av,
    schema: &Schema,
    names: &Names<'_>,
    mode: Mode,
    stack: &mut Vec<String>,
) -> Result<Value, FaucetError> {
    let s = resolve(schema, names)?;
    if mode == Mode::Arrow
        && let Some(n) = ref_name(schema)
        && stack.contains(&n)
    {
        let inner = to_json(v, s, names, Mode::Record, &mut Vec::new())?;
        return Ok(Value::String(inner.to_string()));
    }
    Ok(match (v, s) {
        (Av::Union(i, inner), Schema::Union(u)) => {
            let branch = u.variants().get(*i as usize).ok_or_else(|| {
                FaucetError::Source(format!("avro: union branch {i} out of range"))
            })?;
            if mode == Mode::Arrow && is_complex_union(u) {
                if matches!(**inner, Av::Null) {
                    return Ok(Value::Null);
                }
                let j = to_json(inner, branch, names, Mode::Record, &mut Vec::new())?;
                return Ok(Value::String(j.to_string()));
            }
            to_json(inner, branch, names, mode, stack)?
        }
        (Av::Union(_, inner), other) => to_json(inner, other, names, mode, stack)?,
        (Av::Null, _) => Value::Null,
        (Av::Boolean(b), _) => Value::Bool(*b),
        (Av::Int(i), _) => json!(i),
        (Av::Long(i), _) => json!(i),
        (Av::Float(f), _) => float_json(f64::from(*f)),
        (Av::Double(f), _) => float_json(*f),
        (Av::Bytes(b), _) | (Av::Fixed(_, b), _) => Value::String(hex(b)),
        (Av::String(t), _) => Value::String(t.clone()),
        (Av::Enum(_, sym), _) => Value::String(sym.clone()),
        (Av::Array(items), s) => {
            let item_schema = match s {
                Schema::Array(a) => &*a.items,
                _ => s,
            };
            Value::Array(
                items
                    .iter()
                    .map(|i| to_json(i, item_schema, names, mode, stack))
                    .collect::<Result<_, _>>()?,
            )
        }
        (Av::Map(m), s) => {
            let value_schema = match s {
                Schema::Map(m) => &*m.types,
                _ => s,
            };
            let mut keys: Vec<&String> = m.keys().collect();
            keys.sort();
            let mut out = Map::new();
            for k in keys {
                out.insert(k.clone(), to_json(&m[k], value_schema, names, mode, stack)?);
            }
            Value::Object(out)
        }
        (Av::Record(fields), s) => {
            let rec = match s {
                Schema::Record(r) => Some(r),
                _ => None,
            };
            let pushed = rec.map(|r| r.name.fullname(None));
            if let Some(n) = &pushed {
                stack.push(n.clone());
            }
            let mut out = Map::new();
            for (name, fv) in fields {
                let fs = rec
                    .and_then(|r| r.lookup.get(name).map(|i| &r.fields[*i].schema))
                    .unwrap_or(&Schema::Null);
                out.insert(name.clone(), to_json(fv, fs, names, mode, stack)?);
            }
            if pushed.is_some() {
                stack.pop();
            }
            Value::Object(out)
        }
        (Av::Date(d), _) => Value::String(date_string(*d)?),
        (Av::Decimal(d), s) => {
            let scale = match s {
                Schema::Decimal(DecimalSchema { scale, .. }) => *scale,
                _ => 0,
            };
            let bytes = <Vec<u8>>::try_from(d)
                .map_err(|e| FaucetError::Source(format!("avro decimal: {e}")))?;
            Value::String(decimal_to_string(&bytes, scale))
        }
        (Av::BigDecimal(d), _) => Value::String(d.to_string()),
        (Av::TimeMillis(ms), _) => Value::String(time_string(i64::from(*ms) * 1_000_000, 3)?),
        (Av::TimeMicros(us), _) => Value::String(time_string(us.saturating_mul(1_000), 6)?),
        (Av::TimestampMillis(t), _) => {
            Value::String(utc_string(*t, 1_000_000, SecondsFormat::Millis)?)
        }
        (Av::TimestampMicros(t), _) => Value::String(utc_string(*t, 1_000, SecondsFormat::Micros)?),
        (Av::TimestampNanos(t), _) => Value::String(utc_string(*t, 1, SecondsFormat::Nanos)?),
        (Av::LocalTimestampMillis(t), _) => Value::String(local_string(*t, 1_000_000, 3)?),
        (Av::LocalTimestampMicros(t), _) => Value::String(local_string(*t, 1_000, 6)?),
        (Av::LocalTimestampNanos(t), _) => Value::String(local_string(*t, 1, 9)?),
        (Av::Duration(d), _) => json!({
            "months": u32::from(d.months()),
            "days": u32::from(d.days()),
            "millis": u32::from(d.millis()),
        }),
        (Av::Uuid(u), _) => Value::String(u.to_string()),
    })
}

fn float_json(f: f64) -> Value {
    if f.is_nan() {
        Value::String("NaN".into())
    } else if f.is_infinite() {
        Value::String(if f > 0.0 { "Infinity" } else { "-Infinity" }.into())
    } else {
        json!(f)
    }
}

fn hex(b: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(b.len() * 2);
    for byte in b {
        s.push(DIGITS[(byte >> 4) as usize] as char);
        s.push(DIGITS[(byte & 0xf) as usize] as char);
    }
    s
}

fn unhex(s: &str) -> Result<Vec<u8>, String> {
    if !s.len().is_multiple_of(2) {
        return Err(format!("{s:?} is not hex (odd length)"));
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|_| format!("{s:?} is not hex")))
        .collect()
}

const EPOCH: NaiveDate = NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid epoch");

fn date_string(days: i32) -> Result<String, FaucetError> {
    EPOCH
        .checked_add_signed(chrono::Duration::days(i64::from(days)))
        .map(|d| d.format("%Y-%m-%d").to_string())
        .ok_or_else(|| FaucetError::Source(format!("avro date {days} is out of range")))
}

fn time_string(nanos: i64, digits: usize) -> Result<String, FaucetError> {
    let secs = u32::try_from(nanos.div_euclid(1_000_000_000)).ok();
    let frac = nanos.rem_euclid(1_000_000_000) as u32;
    let t = secs
        .and_then(|s| NaiveTime::from_num_seconds_from_midnight_opt(s, frac))
        .ok_or_else(|| FaucetError::Source(format!("avro time {nanos}ns is out of range")))?;
    Ok(format!(
        "{}.{:0w$}",
        t.format("%H:%M:%S"),
        t.nanosecond() / 10u32.pow(9 - digits as u32),
        w = digits
    ))
}

fn instant(t: i64, nanos_per_unit: i64) -> Option<DateTime<Utc>> {
    let n = i128::from(t) * i128::from(nanos_per_unit);
    let secs = i64::try_from(n.div_euclid(1_000_000_000)).ok()?;
    DateTime::from_timestamp(secs, n.rem_euclid(1_000_000_000) as u32)
}

fn utc_string(t: i64, nanos_per_unit: i64, fmt: SecondsFormat) -> Result<String, FaucetError> {
    instant(t, nanos_per_unit)
        .map(|d| d.to_rfc3339_opts(fmt, true))
        .ok_or_else(|| FaucetError::Source(format!("avro timestamp {t} is out of range")))
}

fn local_string(t: i64, nanos_per_unit: i64, digits: usize) -> Result<String, FaucetError> {
    let d = instant(t, nanos_per_unit)
        .ok_or_else(|| FaucetError::Source(format!("avro local timestamp {t} is out of range")))?
        .naive_utc();
    Ok(format!(
        "{}.{:0w$}",
        d.format("%Y-%m-%dT%H:%M:%S"),
        d.nanosecond() / 10u32.pow(9 - digits as u32),
        w = digits
    ))
}

/// Render a big-endian two's-complement unscaled integer at `scale`.
pub(crate) fn decimal_to_string(bytes: &[u8], scale: usize) -> String {
    let negative = bytes.first().is_some_and(|b| b & 0x80 != 0);
    let mut mag: Vec<u8> = if negative {
        negate(bytes)
    } else {
        bytes.to_vec()
    };
    let mut digits = Vec::new();
    while mag.iter().any(|b| *b != 0) {
        let mut rem: u32 = 0;
        for b in mag.iter_mut() {
            let cur = (rem << 8) | u32::from(*b);
            *b = (cur / 10) as u8;
            rem = cur % 10;
        }
        digits.push(b'0' + rem as u8);
    }
    while digits.len() <= scale {
        digits.push(b'0');
    }
    digits.reverse();
    let (int, frac) = digits.split_at(digits.len() - scale);
    let mut s = String::new();
    if negative {
        s.push('-');
    }
    s.push_str(std::str::from_utf8(int).expect("ascii digits"));
    if scale > 0 {
        s.push('.');
        s.push_str(std::str::from_utf8(frac).expect("ascii digits"));
    }
    s
}

fn negate(bytes: &[u8]) -> Vec<u8> {
    let mut out: Vec<u8> = bytes.iter().map(|b| !b).collect();
    for b in out.iter_mut().rev() {
        let (v, carry) = b.overflowing_add(1);
        *b = v;
        if !carry {
            break;
        }
    }
    out
}

/// Parse a decimal literal (optionally in exponent form) into the minimal
/// big-endian two's-complement unscaled integer at `scale`.
///
/// Refuses digits beyond `scale` that are not zero — rounding a money column
/// silently is not a formatting choice — and more than `precision` digits.
pub(crate) fn decimal_from_str(
    text: &str,
    scale: usize,
    precision: usize,
) -> Result<Vec<u8>, String> {
    let t = text.trim();
    let (negative, body) = match t.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, t.strip_prefix('+').unwrap_or(t)),
    };
    let (mantissa, exp) = match body.find(['e', 'E']) {
        Some(i) => (
            &body[..i],
            body[i + 1..]
                .parse::<i64>()
                .map_err(|_| format!("{text:?} is not a decimal"))?,
        ),
        None => (body, 0),
    };
    let (int, frac) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if int.is_empty() && frac.is_empty()
        || !int.bytes().chain(frac.bytes()).all(|b| b.is_ascii_digit())
    {
        return Err(format!("{text:?} is not a decimal"));
    }
    let mut digits: Vec<u8> = int.bytes().chain(frac.bytes()).map(|b| b - b'0').collect();
    let point = int.len() as i64 + exp;
    let wanted = point + scale as i64;
    if wanted < 0 {
        if digits.iter().any(|d| *d != 0) {
            return Err(format!("{text:?} has more than {scale} fractional digits"));
        }
        digits.clear();
    } else {
        let wanted = wanted as usize;
        if digits.len() > wanted {
            if digits[wanted..].iter().any(|d| *d != 0) {
                return Err(format!("{text:?} has more than {scale} fractional digits"));
            }
            digits.truncate(wanted);
        } else {
            digits.resize(wanted, 0);
        }
    }
    let first = digits.iter().position(|d| *d != 0).unwrap_or(digits.len());
    let digits = &digits[first..];
    if digits.len() > precision {
        return Err(format!(
            "{text:?} needs {} digits, more than the precision {precision}",
            digits.len()
        ));
    }
    let mut mag: Vec<u8> = vec![0];
    for d in digits {
        let mut carry = u32::from(*d);
        for b in mag.iter_mut().rev() {
            let cur = u32::from(*b) * 10 + carry;
            *b = (cur & 0xff) as u8;
            carry = cur >> 8;
        }
        while carry > 0 {
            mag.insert(0, (carry & 0xff) as u8);
            carry >>= 8;
        }
    }
    if mag[0] & 0x80 != 0 {
        mag.insert(0, 0);
    }
    while mag.len() > 1 && mag[0] == 0 && mag[1] & 0x80 == 0 {
        mag.remove(0);
    }
    Ok(if negative && mag.iter().any(|b| *b != 0) {
        negate(&mag)
    } else {
        mag
    })
}

fn sign_extend(bytes: Vec<u8>, size: usize) -> Result<Vec<u8>, String> {
    if bytes.len() > size {
        return Err(format!(
            "decimal needs {} bytes, the fixed holds {size}",
            bytes.len()
        ));
    }
    let fill = if bytes.first().is_some_and(|b| b & 0x80 != 0) {
        0xff
    } else {
        0
    };
    let mut out = vec![fill; size - bytes.len()];
    out.extend(bytes);
    Ok(out)
}

// ── Writing ──────────────────────────────────────────────────────────────────

/// Encode records into one OCF, against `opts.schema` or an inferred schema.
pub fn encode(records: &[Value], opts: &AvroOptions) -> Result<Vec<u8>, FaucetError> {
    let schema = match opts.parsed_schema()? {
        Some(s) => s,
        None => infer_schema(records)?,
    };
    let names = named(&schema);
    let mut w = apache_avro::Writer::with_codec(&schema, Vec::new(), codec(opts.codec))
        .map_err(|e| FaucetError::Sink(format!("avro writer: {e}")))?;
    for (i, r) in records.iter().enumerate() {
        let datum = root_from_json(r, &schema, &names)
            .map_err(|e| FaucetError::Sink(format!("avro: record {i}: {e}")))?;
        w.append_value_ref(&datum)
            .map_err(|e| FaucetError::Sink(format!("avro: record {i}: {e}")))?;
    }
    w.into_inner()
        .map_err(|e| FaucetError::Sink(format!("avro writer: {e}")))
}

fn root_from_json(v: &Value, schema: &Schema, names: &Names<'_>) -> Result<Av, String> {
    let resolved = resolve(schema, names).map_err(|e| e.to_string())?;
    match resolved {
        Schema::Record(_) => from_json(v, schema, names, ""),
        _ => {
            let inner = v.get(VALUE_FIELD).unwrap_or(v);
            from_json(inner, schema, names, "")
        }
    }
}

fn at(path: &str) -> String {
    if path.is_empty() {
        "the record".into()
    } else {
        format!("`{path}`")
    }
}

fn from_json(v: &Value, schema: &Schema, names: &Names<'_>, path: &str) -> Result<Av, String> {
    let s = resolve(schema, names).map_err(|e| e.to_string())?;
    let want = |what: &str| format!("{}: expected {what}, got {}", at(path), short(v));
    Ok(match s {
        Schema::Null => match v {
            Value::Null => Av::Null,
            _ => return Err(want("null")),
        },
        Schema::Boolean => Av::Boolean(v.as_bool().ok_or_else(|| want("a boolean"))?),
        Schema::Int => {
            let i = v.as_i64().ok_or_else(|| want("an integer"))?;
            Av::Int(
                i32::try_from(i)
                    .map_err(|_| format!("{}: {i} does not fit an Avro int", at(path)))?,
            )
        }
        Schema::Long => Av::Long(v.as_i64().ok_or_else(|| want("an integer"))?),
        Schema::Float => Av::Float(float_from(v).ok_or_else(|| want("a number"))? as f32),
        Schema::Double => Av::Double(float_from(v).ok_or_else(|| want("a number"))?),
        Schema::Bytes => Av::Bytes(bytes_from(v).map_err(|e| format!("{}: {e}", at(path)))?),
        Schema::String => Av::String(match v {
            Value::String(t) => t.clone(),
            Value::Null => return Err(want("a string")),
            other => super::cell_text(other),
        }),
        Schema::Array(a) => {
            let items = v.as_array().ok_or_else(|| want("an array"))?;
            Av::Array(
                items
                    .iter()
                    .enumerate()
                    .map(|(i, x)| from_json(x, &a.items, names, &format!("{path}[{i}]")))
                    .collect::<Result<_, _>>()?,
            )
        }
        Schema::Map(m) => {
            let obj = v.as_object().ok_or_else(|| want("an object"))?;
            Av::Map(
                obj.iter()
                    .map(|(k, x)| Ok((k.clone(), from_json(x, &m.types, names, &join(path, k))?)))
                    .collect::<Result<_, String>>()?,
            )
        }
        Schema::Record(r) => {
            let obj = v.as_object().ok_or_else(|| want("an object"))?;
            let mut fields = Vec::with_capacity(r.fields.len());
            for f in &r.fields {
                let key = source_name(f);
                let fv = obj.get(key).unwrap_or(&Value::Null);
                let datum = match (fv, accepts_null(&f.schema, names), &f.default) {
                    (Value::Null, false, Some(default)) => {
                        from_json(default, &f.schema, names, &join(path, key))?
                    }
                    (Value::Null, false, None) => {
                        return Err(format!(
                            "{}: required field is missing or null",
                            at(&join(path, key))
                        ));
                    }
                    _ => from_json(fv, &f.schema, names, &join(path, key))?,
                };
                fields.push((f.name.clone(), datum));
            }
            Av::Record(fields)
        }
        Schema::Union(u) => {
            let variants = u.variants();
            if v.is_null()
                && let Some(i) = variants.iter().position(|b| matches!(b, Schema::Null))
            {
                return Ok(Av::Union(i as u32, Box::new(Av::Null)));
            }
            let mut last = None;
            for (i, b) in variants.iter().enumerate() {
                if matches!(b, Schema::Null) {
                    continue;
                }
                match from_json(v, b, names, path) {
                    Ok(d) => return Ok(Av::Union(i as u32, Box::new(d))),
                    Err(e) => last = Some(e),
                }
            }
            return Err(last.unwrap_or_else(|| want("a value matching the union")));
        }
        Schema::Enum(e) => {
            let sym = v.as_str().ok_or_else(|| want("an enum symbol"))?;
            let i =
                e.symbols.iter().position(|x| x == sym).ok_or_else(|| {
                    format!("{}: {sym:?} is not one of {:?}", at(path), e.symbols)
                })?;
            Av::Enum(i as u32, sym.to_string())
        }
        Schema::Fixed(x) => {
            let b = bytes_from(v).map_err(|e| format!("{}: {e}", at(path)))?;
            if b.len() != x.size {
                return Err(format!(
                    "{}: fixed({}) got {} bytes",
                    at(path),
                    x.size,
                    b.len()
                ));
            }
            Av::Fixed(x.size, b)
        }
        Schema::Decimal(d) => {
            let text = match v {
                Value::String(t) => t.clone(),
                Value::Number(n) => n.to_string(),
                _ => return Err(want("a decimal string or number")),
            };
            let bytes = decimal_from_str(&text, d.scale, d.precision)
                .map_err(|e| format!("{}: {e}", at(path)))?;
            let bytes = match &d.inner {
                apache_avro::schema::InnerDecimalSchema::Fixed(f) => {
                    sign_extend(bytes, f.size).map_err(|e| format!("{}: {e}", at(path)))?
                }
                _ => bytes,
            };
            Av::Decimal(apache_avro::Decimal::from(bytes))
        }
        Schema::BigDecimal => {
            let text = match v {
                Value::String(t) => t.clone(),
                Value::Number(n) => n.to_string(),
                _ => return Err(want("a decimal string or number")),
            };
            Av::BigDecimal(
                text.parse::<apache_avro::BigDecimal>()
                    .map_err(|_| format!("{}: {text:?} is not a decimal", at(path)))?,
            )
        }
        Schema::Uuid(_) => {
            let t = v.as_str().ok_or_else(|| want("a UUID string"))?;
            Av::Uuid(
                apache_avro::Uuid::parse_str(t)
                    .map_err(|_| format!("{}: {t:?} is not a UUID", at(path)))?,
            )
        }
        Schema::Date => Av::Date(match v {
            Value::String(t) => {
                let d = NaiveDate::parse_from_str(t, "%Y-%m-%d")
                    .map_err(|_| format!("{}: {t:?} is not YYYY-MM-DD", at(path)))?;
                i32::try_from((d - EPOCH).num_days())
                    .map_err(|_| format!("{}: {t:?} is out of range", at(path)))?
            }
            _ => i32::try_from(v.as_i64().ok_or_else(|| want("a date"))?)
                .map_err(|_| format!("{}: date out of range", at(path)))?,
        }),
        Schema::TimeMillis => Av::TimeMillis(
            i32::try_from(time_from(v, 1_000_000).map_err(|e| format!("{}: {e}", at(path)))?)
                .map_err(|_| format!("{}: time out of range", at(path)))?,
        ),
        Schema::TimeMicros => {
            Av::TimeMicros(time_from(v, 1_000).map_err(|e| format!("{}: {e}", at(path)))?)
        }
        Schema::TimestampMillis => Av::TimestampMillis(
            ts_from(v, 1_000_000, true).map_err(|e| format!("{}: {e}", at(path)))?,
        ),
        Schema::TimestampMicros => {
            Av::TimestampMicros(ts_from(v, 1_000, true).map_err(|e| format!("{}: {e}", at(path)))?)
        }
        Schema::TimestampNanos => {
            Av::TimestampNanos(ts_from(v, 1, true).map_err(|e| format!("{}: {e}", at(path)))?)
        }
        Schema::LocalTimestampMillis => Av::LocalTimestampMillis(
            ts_from(v, 1_000_000, false).map_err(|e| format!("{}: {e}", at(path)))?,
        ),
        Schema::LocalTimestampMicros => Av::LocalTimestampMicros(
            ts_from(v, 1_000, false).map_err(|e| format!("{}: {e}", at(path)))?,
        ),
        Schema::LocalTimestampNanos => {
            Av::LocalTimestampNanos(ts_from(v, 1, false).map_err(|e| format!("{}: {e}", at(path)))?)
        }
        Schema::Duration(_) => {
            let part = |k: &str| -> Result<u32, String> {
                v.get(k)
                    .and_then(Value::as_u64)
                    .and_then(|n| u32::try_from(n).ok())
                    .ok_or_else(|| format!("{}: duration needs an unsigned `{k}`", at(path)))
            };
            Av::Duration(apache_avro::Duration::new(
                apache_avro::Months::new(part("months")?),
                apache_avro::Days::new(part("days")?),
                apache_avro::Millis::new(part("millis")?),
            ))
        }
        Schema::Ref { .. } => unreachable!("resolved above"),
    })
}

fn source_name(f: &RecordField) -> &str {
    f.custom_attributes
        .get(ORIGINAL_NAME_ATTR)
        .and_then(Value::as_str)
        .unwrap_or(&f.name)
}

fn accepts_null(s: &Schema, names: &Names<'_>) -> bool {
    match resolve(s, names) {
        Ok(Schema::Null) => true,
        Ok(Schema::Union(u)) => u.is_nullable(),
        _ => false,
    }
}

fn join(path: &str, k: &str) -> String {
    if path.is_empty() {
        k.to_string()
    } else {
        format!("{path}.{k}")
    }
}

fn short(v: &Value) -> String {
    let s = v.to_string();
    if s.len() > 60 {
        format!("{}…", &s[..s.floor_char_boundary(60)])
    } else {
        s
    }
}

fn float_from(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => match s.as_str() {
            "NaN" => Some(f64::NAN),
            "Infinity" => Some(f64::INFINITY),
            "-Infinity" => Some(f64::NEG_INFINITY),
            _ => None,
        },
        _ => None,
    }
}

fn bytes_from(v: &Value) -> Result<Vec<u8>, String> {
    match v {
        Value::String(s) => unhex(s),
        Value::Array(items) => items
            .iter()
            .map(|i| {
                i.as_u64()
                    .and_then(|n| u8::try_from(n).ok())
                    .ok_or_else(|| "byte array holds a non-byte".to_string())
            })
            .collect(),
        other => Err(format!("expected hex bytes, got {}", short(other))),
    }
}

fn time_from(v: &Value, nanos_per_unit: i64) -> Result<i64, String> {
    match v {
        Value::String(t) => {
            let parsed = NaiveTime::parse_from_str(t, "%H:%M:%S%.f")
                .map_err(|_| format!("{t:?} is not HH:MM:SS[.fff]"))?;
            let nanos = i64::from(parsed.num_seconds_from_midnight()) * 1_000_000_000
                + i64::from(parsed.nanosecond());
            Ok(nanos / nanos_per_unit)
        }
        other => other
            .as_i64()
            .ok_or_else(|| format!("expected a time, got {}", short(other))),
    }
}

fn ts_from(v: &Value, nanos_per_unit: i64, utc: bool) -> Result<i64, String> {
    let t = match v {
        Value::String(t) => t,
        other => {
            return other
                .as_i64()
                .ok_or_else(|| format!("expected a timestamp, got {}", short(other)));
        }
    };
    let dt: NaiveDateTime = match DateTime::parse_from_rfc3339(t) {
        Ok(d) if utc => d.with_timezone(&Utc).naive_utc(),
        Ok(d) => d.naive_local(),
        Err(_) => [
            "%Y-%m-%dT%H:%M:%S%.f",
            "%Y-%m-%d %H:%M:%S%.f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
        ]
        .iter()
        .find_map(|f| NaiveDateTime::parse_from_str(t, f).ok())
        .ok_or_else(|| format!("{t:?} is not an RFC 3339 / ISO 8601 timestamp"))?,
    };
    let nanos = dt
        .and_utc()
        .timestamp_nanos_opt()
        .map(i128::from)
        .unwrap_or_else(|| {
            i128::from(dt.and_utc().timestamp()) * 1_000_000_000
                + i128::from(dt.and_utc().timestamp_subsec_nanos())
        });
    i64::try_from(nanos / i128::from(nanos_per_unit)).map_err(|_| format!("{t:?} is out of range"))
}

// ── Schema inference ─────────────────────────────────────────────────────────

/// Infer an Avro record schema from records (see the module docs for rules).
pub fn infer_schema(records: &[Value]) -> Result<Schema, FaucetError> {
    let json_schema = crate::schema::infer_schema(records);
    let order = super::header_union(records);
    let avro =
        record_json(INFERRED_RECORD_NAME, &json_schema, Some(&order)).map_err(FaucetError::Sink)?;
    Schema::parse(&avro)
        .map_err(|e| FaucetError::Sink(format!("avro: inferred schema is invalid: {e}")))
}

fn record_json(name: &str, js: &Value, order: Option<&[String]>) -> Result<Value, String> {
    let props = js
        .get("properties")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let keys: Vec<String> = match order {
        Some(o) => o
            .iter()
            .filter(|k| props.contains_key(*k))
            .cloned()
            .collect(),
        None => {
            let mut k: Vec<String> = props.keys().cloned().collect();
            k.sort();
            k
        }
    };
    let mut seen: HashMap<String, String> = HashMap::new();
    let mut fields = Vec::with_capacity(keys.len());
    for key in keys {
        let clean = avro_name(&key);
        if let Some(prev) = seen.insert(clean.clone(), key.clone()) {
            return Err(format!(
                "avro: fields {prev:?} and {key:?} both sanitize to the Avro name {clean:?}"
            ));
        }
        let (ty, nullable) = type_json(&props[&key], &format!("{name}_{clean}"))?;
        let mut f = Map::new();
        f.insert("name".into(), Value::String(clean.clone()));
        if nullable {
            f.insert("type".into(), json!(["null", ty]));
            f.insert("default".into(), Value::Null);
        } else {
            f.insert("type".into(), ty);
        }
        if clean != key {
            f.insert(ORIGINAL_NAME_ATTR.into(), Value::String(key));
        }
        fields.push(Value::Object(f));
    }
    Ok(json!({"type": "record", "name": name, "fields": fields}))
}

fn type_json(js: &Value, name: &str) -> Result<(Value, bool), String> {
    let types: HashSet<&str> = match js.get("type") {
        Some(Value::String(t)) => std::iter::once(t.as_str()).collect(),
        Some(Value::Array(a)) => a.iter().filter_map(Value::as_str).collect(),
        _ => HashSet::new(),
    };
    let nullable = types.contains("null");
    let non_null: Vec<&str> = types.iter().copied().filter(|t| *t != "null").collect();
    let ty = match non_null.as_slice() {
        [] => return Ok((json!("null"), false)),
        ["boolean"] => json!("boolean"),
        ["integer"] => json!("long"),
        ["number"] => json!("double"),
        ["string"] => json!("string"),
        ["array"] => {
            let items = match js.get("items") {
                Some(i) => {
                    let (t, n) = type_json(i, &format!("{name}_item"))?;
                    if n { json!(["null", t]) } else { t }
                }
                None => json!(["null", "string"]),
            };
            json!({"type": "array", "items": items})
        }
        ["object"] => record_json(name, js, None)?,
        _ => json!("string"),
    };
    Ok((ty, nullable))
}

/// A valid Avro name for `s`: `[A-Za-z_][A-Za-z0-9_]*`.
pub fn avro_name(s: &str) -> String {
    let mut out: String = s
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if out.is_empty() || out.starts_with(|c: char| c.is_ascii_digit()) {
        out.insert(0, '_');
    }
    out
}

// ── Arrow ────────────────────────────────────────────────────────────────────

/// The Arrow schema the records of `schema` decode into (see the module docs).
#[cfg(feature = "arrow")]
pub fn arrow_schema(schema: &Schema) -> Result<arrow::datatypes::SchemaRef, FaucetError> {
    use arrow::datatypes::{Field, Schema as ArrowSchema};
    let names = named(schema);
    let mut stack = Vec::new();
    let root = resolve(schema, &names)?;
    let fields: Vec<Field> = match root {
        Schema::Record(r) => {
            stack.push(r.name.fullname(None));
            r.fields
                .iter()
                .map(|f| {
                    let (dt, nullable) = arrow_type(&f.schema, &names, &mut stack)?;
                    Ok(Field::new(&f.name, dt, nullable))
                })
                .collect::<Result<_, FaucetError>>()?
        }
        other => {
            let (dt, nullable) = arrow_type(other, &names, &mut stack)?;
            vec![Field::new(VALUE_FIELD, dt, nullable)]
        }
    };
    Ok(std::sync::Arc::new(ArrowSchema::new(fields)))
}

#[cfg(feature = "arrow")]
fn arrow_type(
    schema: &Schema,
    names: &Names<'_>,
    stack: &mut Vec<String>,
) -> Result<(arrow::datatypes::DataType, bool), FaucetError> {
    use arrow::datatypes::{DataType as D, Field, Fields, TimeUnit};
    use std::sync::Arc;
    if let Some(n) = ref_name(schema)
        && stack.contains(&n)
    {
        return Ok((D::Utf8, true));
    }
    let s = resolve(schema, names)?;
    let utc: Option<Arc<str>> = Some("UTC".into());
    Ok(match s {
        Schema::Null => (D::Null, true),
        Schema::Boolean => (D::Boolean, false),
        Schema::Int => (D::Int32, false),
        Schema::Long => (D::Int64, false),
        Schema::Float => (D::Float32, false),
        Schema::Double => (D::Float64, false),
        Schema::Bytes => (D::Binary, false),
        Schema::String | Schema::Enum(_) | Schema::Uuid(_) | Schema::BigDecimal => (D::Utf8, false),
        Schema::Fixed(f) => (
            D::FixedSizeBinary(i32::try_from(f.size).unwrap_or(i32::MAX)),
            false,
        ),
        Schema::Decimal(d) => match (u8::try_from(d.precision), i8::try_from(d.scale)) {
            (Ok(p), Ok(sc)) if p <= 38 => (D::Decimal128(p, sc), false),
            (Ok(p), Ok(sc)) if p <= 76 => (D::Decimal256(p, sc), false),
            _ => (D::Utf8, false),
        },
        Schema::Date => (D::Date32, false),
        Schema::TimeMillis => (D::Time32(TimeUnit::Millisecond), false),
        Schema::TimeMicros => (D::Time64(TimeUnit::Microsecond), false),
        Schema::TimestampMillis => (D::Timestamp(TimeUnit::Millisecond, utc), false),
        Schema::TimestampMicros => (D::Timestamp(TimeUnit::Microsecond, utc), false),
        Schema::TimestampNanos => (D::Timestamp(TimeUnit::Nanosecond, utc), false),
        Schema::LocalTimestampMillis => (D::Timestamp(TimeUnit::Millisecond, None), false),
        Schema::LocalTimestampMicros => (D::Timestamp(TimeUnit::Microsecond, None), false),
        Schema::LocalTimestampNanos => (D::Timestamp(TimeUnit::Nanosecond, None), false),
        Schema::Duration(_) => (
            D::Struct(Fields::from(vec![
                Field::new("months", D::Int64, false),
                Field::new("days", D::Int64, false),
                Field::new("millis", D::Int64, false),
            ])),
            false,
        ),
        Schema::Array(a) => {
            let (dt, n) = arrow_type(&a.items, names, stack)?;
            (D::List(Arc::new(Field::new("item", dt, n))), false)
        }
        Schema::Map(m) => {
            let (dt, n) = arrow_type(&m.types, names, stack)?;
            let entries = Field::new(
                "entries",
                D::Struct(Fields::from(vec![
                    Field::new("key", D::Utf8, false),
                    Field::new("value", dt, n),
                ])),
                false,
            );
            (D::Map(Arc::new(entries), false), false)
        }
        Schema::Record(r) => {
            stack.push(r.name.fullname(None));
            let fields = r
                .fields
                .iter()
                .map(|f| {
                    let (dt, n) = arrow_type(&f.schema, names, stack)?;
                    Ok(Field::new(&f.name, dt, n))
                })
                .collect::<Result<Vec<_>, FaucetError>>();
            stack.pop();
            (D::Struct(Fields::from(fields?)), false)
        }
        Schema::Union(u) => {
            let non_null: Vec<&Schema> = u
                .variants()
                .iter()
                .filter(|v| !matches!(v, Schema::Null))
                .collect();
            match non_null.as_slice() {
                [] => (D::Null, true),
                [one] => {
                    let (dt, _) = arrow_type(one, names, stack)?;
                    (dt, u.is_nullable())
                }
                _ => (D::Utf8, u.is_nullable()),
            }
        }
        Schema::Ref { .. } => unreachable!("resolved above"),
    })
}

/// Stream an OCF as Arrow batches of at most `batch_size` rows (0 = one batch).
#[cfg(feature = "arrow")]
pub fn read_batches<R: std::io::Read>(
    reader: R,
    reader_schema: Option<&Schema>,
    batch_size: usize,
    f: &mut dyn FnMut(arrow::array::RecordBatch) -> Result<(), FaucetError>,
) -> Result<(Schema, arrow::datatypes::SchemaRef), FaucetError> {
    let chunk = if batch_size == 0 {
        usize::MAX
    } else {
        batch_size
    };
    let mut arrow: Option<arrow::datatypes::SchemaRef> = None;
    let schema = read_with(
        reader,
        reader_schema,
        chunk,
        Mode::Arrow,
        &mut |rows, schema| {
            let target = match &arrow {
                Some(a) => a.clone(),
                None => {
                    let a = arrow_schema(schema)?;
                    arrow = Some(a.clone());
                    a
                }
            };
            f(crate::columnar::values_to_record_batch(&rows, target)?)
        },
    )?;
    let arrow = match arrow {
        Some(a) => a,
        None => arrow_schema(&schema)?,
    };
    Ok((schema, arrow))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datum_conversion_keeps_logical_types_exact_and_round_trips() {
        let schema = Schema::parse_str(
            r#"{"type":"record","name":"r","fields":[
                {"name":"amount","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}},
                {"name":"raw","type":"bytes"},
                {"name":"day","type":{"type":"int","logicalType":"date"}}
            ]}"#,
        )
        .unwrap();
        let v = json!({"amount": "12.34", "raw": "0aff", "day": "2026-10-07"});
        let datum = datum_from_json(&v, &schema).unwrap();
        assert_eq!(datum_to_json(&datum, &schema).unwrap(), v);
        assert_eq!(
            datum_to_json(&Av::Long(7), &Schema::Long).unwrap(),
            json!(7),
            "a non-record datum is not wrapped"
        );
        assert!(datum_from_json(&json!({"amount": 1}), &schema).is_err());
    }

    fn logical_schema() -> Value {
        json!({
            "type": "record",
            "name": "payment",
            "namespace": "test",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}},
                {"name": "fee", "type": {"type": "fixed", "name": "fee_t", "size": 8, "logicalType": "decimal", "precision": 12, "scale": 3}},
                {"name": "day", "type": {"type": "int", "logicalType": "date"}},
                {"name": "at", "type": {"type": "long", "logicalType": "timestamp-micros"}},
                {"name": "at_ms", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                {"name": "at_ns", "type": {"type": "long", "logicalType": "timestamp-nanos"}},
                {"name": "local", "type": {"type": "long", "logicalType": "local-timestamp-micros"}},
                {"name": "local_ms", "type": {"type": "long", "logicalType": "local-timestamp-millis"}},
                {"name": "local_ns", "type": {"type": "long", "logicalType": "local-timestamp-nanos"}},
                {"name": "t_ms", "type": {"type": "int", "logicalType": "time-millis"}},
                {"name": "t_us", "type": {"type": "long", "logicalType": "time-micros"}},
                {"name": "uid", "type": {"type": "string", "logicalType": "uuid"}},
                {"name": "span", "type": {"type": "fixed", "name": "dur", "size": 12, "logicalType": "duration"}},
                {"name": "note", "type": ["null", "string"], "default": null},
                {"name": "either", "type": ["null", "long", "string"], "default": null},
                {"name": "kind", "type": {"type": "enum", "name": "kind_t", "symbols": ["A", "B"]}},
                {"name": "raw", "type": "bytes"},
                {"name": "tags", "type": {"type": "array", "items": "string"}},
                {"name": "attrs", "type": {"type": "map", "values": "int"}},
                {"name": "ratio", "type": "float"},
                {"name": "score", "type": "double"},
                {"name": "ok", "type": "boolean"},
                {"name": "small", "type": "int"},
                {"name": "big", "type": {"type": "bytes", "logicalType": "big-decimal"}}
            ]
        })
    }

    fn logical_record() -> Value {
        json!({
            "id": 7,
            "amount": "-1234.50",
            "fee": "0.125",
            "day": "2024-02-29",
            "at": "2024-02-29T12:34:56.123456Z",
            "at_ms": "2024-02-29T12:34:56.123Z",
            "at_ns": "2024-02-29T12:34:56.123456789Z",
            "local": "2024-02-29T12:34:56.123456",
            "local_ms": "2024-02-29T12:34:56.123",
            "local_ns": "2024-02-29T12:34:56.123456789",
            "t_ms": "01:02:03.004",
            "t_us": "01:02:03.000004",
            "uid": "6f1c2b1e-4b8a-4b8e-9a38-3c3f3f0f9e11",
            "span": {"months": 1, "days": 2, "millis": 3},
            "note": null,
            "either": "text",
            "kind": "B",
            "raw": "00ff10",
            "tags": ["x", "y"],
            "attrs": {"a": 1, "b": 2},
            "ratio": 0.5,
            "score": "NaN",
            "ok": true,
            "small": 3,
            "big": "123456789012345678901234567890.5"
        })
    }

    #[test]
    fn logical_types_round_trip_through_an_ocf() {
        for codec in [
            AvroCodec::Null,
            AvroCodec::Deflate,
            AvroCodec::Snappy,
            AvroCodec::Zstd,
        ] {
            let opts = AvroOptions {
                schema: Some(logical_schema()),
                codec,
            };
            let bytes = encode(&[logical_record()], &opts).expect("encode");
            let back = decode(&bytes, &AvroOptions::default()).expect("decode");
            assert_eq!(back, vec![logical_record()], "{codec:?}");
        }
    }

    #[test]
    fn epoch_integers_are_accepted_for_temporal_types() {
        let schema = json!({"type": "record", "name": "r", "fields": [
            {"name": "d", "type": {"type": "int", "logicalType": "date"}},
            {"name": "t", "type": {"type": "long", "logicalType": "timestamp-millis"}},
            {"name": "tm", "type": {"type": "int", "logicalType": "time-millis"}}
        ]});
        let opts = AvroOptions {
            schema: Some(schema),
            ..Default::default()
        };
        let bytes = encode(&[json!({"d": 1, "t": 1000, "tm": 1500})], &opts).expect("encode");
        let back = decode(&bytes, &AvroOptions::default()).expect("decode");
        assert_eq!(
            back[0],
            json!({"d": "1970-01-02", "t": "1970-01-01T00:00:01.000Z", "tm": "00:00:01.500"})
        );
    }

    #[test]
    fn inferred_schema_widens_nullable_nested_and_mixed_fields() {
        let recs = vec![
            json!({"id": 1, "name": "a", "first-name": "x", "nested": {"k": 1.5}, "mixed": 1, "list": [1, 2], "empty": []}),
            json!({"id": 2, "name": null, "first-name": "y", "nested": {"k": 2}, "mixed": "two", "list": [], "empty": []}),
        ];
        let bytes = encode(&recs, &AvroOptions::default()).expect("encode");
        let schema = writer_schema(&bytes[..]).expect("schema");
        let text = serde_json::to_string(&schema).expect("json");
        assert!(text.contains("\"first_name\""), "{text}");
        assert!(text.contains(ORIGINAL_NAME_ATTR), "{text}");
        let back = decode(&bytes, &AvroOptions::default()).expect("decode");
        assert_eq!(back[0]["id"], json!(1));
        assert_eq!(back[1]["name"], Value::Null);
        assert_eq!(back[0]["first_name"], json!("x"));
        assert_eq!(back[1]["nested"], json!({"k": 2.0}));
        assert_eq!(back[0]["mixed"], json!("1"));
        assert_eq!(back[1]["mixed"], json!("two"));
        assert_eq!(back[0]["list"], json!([1, 2]));
        assert_eq!(back[0]["empty"], json!([]));
    }

    #[test]
    fn inference_refuses_colliding_sanitized_names() {
        let err =
            encode(&[json!({"a-b": 1, "a_b": 2})], &AvroOptions::default()).expect_err("collision");
        assert!(err.to_string().contains("sanitize"), "{err}");
    }

    #[test]
    fn avro_names_are_sanitized() {
        assert_eq!(avro_name("ok_1"), "ok_1");
        assert_eq!(avro_name("first name"), "first_name");
        assert_eq!(avro_name("1st"), "_1st");
        assert_eq!(avro_name(""), "_");
    }

    #[test]
    fn reader_schema_projects_and_defaults_added_fields() {
        let bytes = encode(&[json!({"a": 1, "b": "x"})], &AvroOptions::default()).expect("encode");
        let reader = json!({"type": "record", "name": INFERRED_RECORD_NAME, "fields": [
            {"name": "b", "type": "string"},
            {"name": "c", "type": "long", "default": 9}
        ]});
        let back = decode(
            &bytes,
            &AvroOptions {
                schema: Some(reader),
                ..Default::default()
            },
        )
        .expect("decode");
        assert_eq!(back, vec![json!({"b": "x", "c": 9})]);
    }

    #[test]
    fn a_non_record_root_is_wrapped_in_value() {
        let opts = AvroOptions {
            schema: Some(json!("long")),
            ..Default::default()
        };
        let bytes = encode(&[json!({"value": 5}), json!(6)], &opts).expect("encode");
        let back = decode(&bytes, &AvroOptions::default()).expect("decode");
        assert_eq!(back, vec![json!({"value": 5}), json!({"value": 6})]);
    }

    #[test]
    fn recursive_records_decode_as_nested_objects() {
        let schema = json!({"type": "record", "name": "node", "fields": [
            {"name": "v", "type": "long"},
            {"name": "next", "type": ["null", "node"], "default": null}
        ]});
        let rec = json!({"v": 1, "next": {"v": 2, "next": null}});
        let opts = AvroOptions {
            schema: Some(schema),
            ..Default::default()
        };
        let bytes = encode(std::slice::from_ref(&rec), &opts).expect("encode");
        assert_eq!(
            decode(&bytes, &AvroOptions::default()).expect("decode"),
            vec![rec]
        );
    }

    #[test]
    fn encoding_errors_name_the_field() {
        let schema = json!({"type": "record", "name": "r", "fields": [
            {"name": "n", "type": "int"},
            {"name": "req", "type": "string"}
        ]});
        let opts = AvroOptions {
            schema: Some(schema),
            ..Default::default()
        };
        let err = encode(&[json!({"n": "x", "req": "a"})], &opts).expect_err("type");
        assert!(err.to_string().contains("`n`"), "{err}");
        let err = encode(&[json!({"n": 1})], &opts).expect_err("missing");
        assert!(err.to_string().contains("required"), "{err}");
        let err = encode(&[json!({"n": 1i64 << 40, "req": "a"})], &opts).expect_err("range");
        assert!(err.to_string().contains("does not fit"), "{err}");
    }

    #[test]
    fn invalid_schemas_and_bodies_are_typed_errors() {
        let opts = AvroOptions {
            schema: Some(json!({"type": "nope"})),
            ..Default::default()
        };
        assert!(matches!(opts.parsed_schema(), Err(FaucetError::Config(_))));
        let err = decode(b"not avro", &AvroOptions::default()).expect_err("garbage");
        assert!(err.to_string().contains("avro"), "{err}");
    }

    #[test]
    fn decimals_render_and_parse_exactly() {
        let cases = [
            ("0", 0),
            ("-1", 0),
            ("12.30", 2),
            ("-0.05", 2),
            ("170141183460469231731687303715884105727", 0),
        ];
        for (text, scale) in cases {
            let bytes = decimal_from_str(text, scale, 60).expect(text);
            assert_eq!(decimal_to_string(&bytes, scale), text, "{text}");
        }
        assert_eq!(
            decimal_to_string(&decimal_from_str("1.5e1", 1, 5).expect("exp"), 1),
            "15.0"
        );
        assert_eq!(
            decimal_to_string(&decimal_from_str("1.230", 2, 5).expect("trailing zero"), 2),
            "1.23"
        );
        assert!(decimal_from_str("1.234", 2, 5).is_err());
        assert!(decimal_from_str("123456", 0, 5).is_err());
        assert!(decimal_from_str("abc", 0, 5).is_err());
        assert!(decimal_from_str("1e-9", 2, 5).is_err());
        assert_eq!(decimal_from_str("0e-9", 2, 5).expect("zero"), vec![0]);
        assert_eq!(
            sign_extend(vec![0xff], 3).expect("neg"),
            vec![0xff, 0xff, 0xff]
        );
        assert!(sign_extend(vec![1, 2, 3], 2).is_err());
    }

    #[test]
    fn value_coercions_and_their_refusals() {
        let names = HashMap::new();
        assert!(
            matches!(from_json(&json!(1), &Schema::String, &names, "s"), Ok(Av::String(s)) if s == "1")
        );
        assert!(from_json(&Value::Null, &Schema::String, &names, "s").is_err());
        assert!(from_json(&json!(1), &Schema::Null, &names, "s").is_err());
        assert!(
            matches!(from_json(&json!([1, 2]), &Schema::Bytes, &names, "b"), Ok(Av::Bytes(b)) if b == vec![1, 2])
        );
        assert!(from_json(&json!("abc"), &Schema::Bytes, &names, "b").is_err());
        assert!(from_json(&json!("zz"), &Schema::Bytes, &names, "b").is_err());
        assert!(from_json(&json!([300]), &Schema::Bytes, &names, "b").is_err());
        assert!(from_json(&json!(true), &Schema::Bytes, &names, "b").is_err());
        assert!(from_json(&json!("Infinity"), &Schema::Double, &names, "d").is_ok());
        assert!(from_json(&json!("-Infinity"), &Schema::Double, &names, "d").is_ok());
        assert!(from_json(&json!("x"), &Schema::Double, &names, "d").is_err());
        assert!(from_json(&json!("x"), &Schema::Date, &names, "d").is_err());
        assert!(from_json(&json!("x"), &Schema::TimestampMicros, &names, "d").is_err());
        assert!(
            from_json(
                &json!("2024-01-01 00:00:00"),
                &Schema::TimestampMicros,
                &names,
                "d"
            )
            .is_ok()
        );
        assert!(
            from_json(
                &json!("2024-01-01T00:00:00+02:00"),
                &Schema::LocalTimestampMicros,
                &names,
                "d"
            )
            .is_ok()
        );
        assert!(from_json(&json!(true), &Schema::TimeMicros, &names, "d").is_err());
        assert!(from_json(&json!("bad"), &Schema::TimeMicros, &names, "d").is_err());
        assert!(from_json(&json!(5), &Schema::TimeMicros, &names, "d").is_ok());
        assert!(from_json(&json!(true), &Schema::TimestampMicros, &names, "d").is_err());
        assert!(
            from_json(
                &json!({"months": 1}),
                &parse_schema(
                    &json!({"type": "fixed", "name": "d", "size": 12, "logicalType": "duration"})
                )
                .unwrap(),
                &names,
                "d"
            )
            .is_err()
        );
        let e = parse_schema(&json!({"type": "enum", "name": "e", "symbols": ["A"]})).unwrap();
        assert!(from_json(&json!("Z"), &e, &named(&e), "e").is_err());
        let f = parse_schema(&json!({"type": "fixed", "name": "f", "size": 2})).unwrap();
        assert!(from_json(&json!("00"), &f, &named(&f), "f").is_err());
        let u = parse_schema(&json!({"type": "string", "logicalType": "uuid"})).unwrap();
        assert!(from_json(&json!("nope"), &u, &names, "u").is_err());
        let bd = parse_schema(&json!({"type": "bytes", "logicalType": "big-decimal"})).unwrap();
        assert!(from_json(&json!(1.5), &bd, &names, "b").is_ok());
        assert!(from_json(&json!("x"), &bd, &names, "b").is_err());
        assert!(from_json(&json!(true), &bd, &names, "b").is_err());
        let dec = parse_schema(
            &json!({"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 1}),
        )
        .unwrap();
        assert!(from_json(&json!(1.5), &dec, &names, "d").is_ok());
        assert!(from_json(&json!(true), &dec, &names, "d").is_err());
        let un = parse_schema(&json!(["long", "boolean"])).unwrap();
        assert!(from_json(&json!("x"), &un, &names, "u").is_err());
        assert!(from_json(&Value::Null, &un, &names, "u").is_err());
        assert!(short(&json!("x".repeat(100))).ends_with('…'));
    }

    #[test]
    fn out_of_range_temporal_values_are_errors() {
        assert!(date_string(i32::MAX).is_err());
        assert!(time_string(-1, 3).is_err());
        assert!(utc_string(i64::MAX, 1_000_000, SecondsFormat::Millis).is_err());
        assert!(local_string(i64::MAX, 1_000_000, 3).is_err());
        assert_eq!(float_json(f64::NEG_INFINITY), json!("-Infinity"));
    }

    #[cfg(feature = "arrow")]
    #[test]
    fn arrow_batches_carry_logical_types() {
        use arrow::datatypes::{DataType as D, TimeUnit};
        let opts = AvroOptions {
            schema: Some(logical_schema()),
            ..Default::default()
        };
        let rec = logical_record();
        let bytes = encode(&[rec.clone(), rec], &opts).expect("encode");
        let mut batches = Vec::new();
        let (_, schema) = read_batches(&bytes[..], None, 1, &mut |b| {
            batches.push(b);
            Ok(())
        })
        .expect("batches");
        assert_eq!(batches.len(), 2);
        let ty = |n: &str| schema.field_with_name(n).expect(n).data_type().clone();
        assert_eq!(ty("amount"), D::Decimal128(10, 2));
        assert_eq!(ty("day"), D::Date32);
        assert_eq!(
            ty("at"),
            D::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(ty("local"), D::Timestamp(TimeUnit::Microsecond, None));
        assert_eq!(ty("t_ms"), D::Time32(TimeUnit::Millisecond));
        assert_eq!(ty("either"), D::Utf8);
        assert!(schema.field_with_name("note").unwrap().is_nullable());
        let back = crate::columnar::record_batch_to_values(&batches[0]).expect("values");
        let amount = batches[0]
            .column_by_name("amount")
            .and_then(|c| c.as_any().downcast_ref::<arrow::array::Decimal128Array>())
            .expect("decimal column");
        assert_eq!(amount.value(0), -123_450);
        assert_eq!(back[0]["day"], json!("2024-02-29"));
        assert_eq!(back[0]["either"], json!("\"text\""));
    }

    #[cfg(feature = "arrow")]
    #[test]
    fn arrow_schema_handles_roots_recursion_and_wide_decimals() {
        use arrow::datatypes::DataType as D;
        let s = parse_schema(&json!("long")).unwrap();
        assert_eq!(arrow_schema(&s).unwrap().field(0).name(), VALUE_FIELD);
        let rec = parse_schema(&json!({"type": "record", "name": "node", "fields": [
            {"name": "next", "type": ["null", "node"], "default": null},
            {"name": "wide", "type": {"type": "bytes", "logicalType": "decimal", "precision": 50, "scale": 2}},
            {"name": "huge", "type": {"type": "bytes", "logicalType": "decimal", "precision": 90, "scale": 2}},
            {"name": "only_null", "type": ["null"]},
            {"name": "n", "type": "null"}
        ]}))
        .unwrap();
        let a = arrow_schema(&rec).unwrap();
        assert_eq!(a.field(0).data_type(), &D::Utf8);
        assert_eq!(a.field(1).data_type(), &D::Decimal256(50, 2));
        assert_eq!(a.field(2).data_type(), &D::Utf8);
        assert_eq!(a.field(3).data_type(), &D::Null);
        let opts = AvroOptions {
            schema: Some(json!({"type": "record", "name": "node", "fields": [
                {"name": "v", "type": "long"},
                {"name": "next", "type": ["null", "node"], "default": null}
            ]})),
            ..Default::default()
        };
        let bytes = encode(&[json!({"v": 1, "next": {"v": 2, "next": null}})], &opts).unwrap();
        let mut rows = Vec::new();
        read_batches(&bytes[..], None, 0, &mut |b| {
            rows.extend(crate::columnar::record_batch_to_values(&b)?);
            Ok(())
        })
        .unwrap();
        let next: Value = serde_json::from_str(rows[0]["next"].as_str().unwrap()).unwrap();
        assert_eq!(next, json!({"v": 2, "next": null}));
    }
}
