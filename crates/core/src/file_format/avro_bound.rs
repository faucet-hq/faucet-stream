//! Bounds what one Avro datum may decode to before the Avro reader sees it.
//!
//! [`Bounded`] sits between the input and `apache_avro::Reader`. It reads the
//! container header and each block itself, decompresses the block, and walks
//! every datum against the writer schema, counting the values it would
//! produce. A datum whose values would pass `max_decoded_bytes` is refused
//! before it is decoded, which covers arrays of zero-width items (`null`, an
//! empty record) that take no input bytes. The reader then gets the same
//! file with each block already decompressed (the header's codec set to
//! `null`), so it decodes exactly as it would have.
//!
//! Input this module cannot make sense of is handed on unchanged, and the
//! reader reports it as before.

use super::budget;
use crate::error::FaucetError;
use apache_avro::schema::{
    InnerDecimalSchema, Name, NamespaceRef, RecordSchema, ResolvedSchema, UuidSchema,
};
use apache_avro::{Codec, Schema};
use std::cell::RefCell;
use std::collections::HashMap;
use std::io::{self, Read};
use std::rc::Rc;
use std::str::FromStr;

/// Charged per decoded value: the Avro value and the JSON value it becomes.
const NODE: u64 = (std::mem::size_of::<apache_avro::types::Value>()
    + std::mem::size_of::<serde_json::Value>()) as u64;

/// Deepest nesting a datum may reach.
pub(crate) const MAX_DEPTH: usize = 1024;

/// The largest header or block handled; the Avro reader refuses more.
const MAX_SECTION: u64 = apache_avro::util::DEFAULT_MAX_ALLOCATION_BYTES as u64;

const MAGIC: [u8; 4] = [b'O', b'b', b'j', 1];

/// Where a refusal is left for the caller, since the Avro reader wraps the
/// I/O error it surfaces as.
pub(crate) type Failure = Rc<RefCell<Option<FaucetError>>>;

enum State {
    Header,
    Blocks,
    Passthrough,
    Done,
}

pub(crate) struct Bounded<R> {
    inner: R,
    limit: u64,
    failure: Failure,
    state: State,
    out: Vec<u8>,
    pos: usize,
    recorded: Vec<u8>,
    sync: [u8; 16],
    codec: Codec,
    schema: Option<Schema>,
    names: HashMap<Name, Schema>,
    reader_schema: Option<Schema>,
}

impl<R: Read> Bounded<R> {
    pub(crate) fn new(
        inner: R,
        limit: u64,
        reader_schema: Option<&Schema>,
        failure: Failure,
    ) -> Self {
        Self {
            inner,
            limit,
            failure,
            state: State::Header,
            out: Vec::new(),
            pos: 0,
            recorded: Vec::new(),
            sync: [0; 16],
            codec: Codec::Null,
            schema: None,
            names: HashMap::new(),
            reader_schema: reader_schema.cloned(),
        }
    }

    fn fail(&self, e: FaucetError) -> io::Error {
        let msg = e.to_string();
        *self.failure.borrow_mut() = Some(e);
        io::Error::new(io::ErrorKind::InvalidData, msg)
    }

    /// Hand on everything read for the current section, then the rest.
    fn pass_through(&mut self) {
        self.out = std::mem::take(&mut self.recorded);
        self.pos = 0;
        self.state = State::Passthrough;
    }

    fn take_exact(&mut self, n: u64) -> io::Result<Option<Vec<u8>>> {
        if n > MAX_SECTION {
            return Ok(None);
        }
        let mut buf = Vec::new();
        (&mut self.inner).take(n).read_to_end(&mut buf)?;
        self.recorded.extend_from_slice(&buf);
        Ok((buf.len() as u64 == n).then_some(buf))
    }

    /// A zig-zag long; `Ok(None)` at a clean end of input or a malformed one.
    fn long(&mut self, eof_ok: &mut bool) -> io::Result<Option<i64>> {
        let mut value: u64 = 0;
        let mut shift = 0;
        loop {
            let mut b = [0u8; 1];
            if self.inner.read(&mut b)? == 0 {
                *eof_ok = shift == 0;
                return Ok(None);
            }
            self.recorded.push(b[0]);
            if shift >= 64 {
                return Ok(None);
            }
            value |= u64::from(b[0] & 0x7f) << shift;
            if b[0] & 0x80 == 0 {
                return Ok(Some(((value >> 1) as i64) ^ -((value & 1) as i64)));
            }
            shift += 7;
        }
    }

    fn header(&mut self) -> io::Result<()> {
        self.recorded.clear();
        let Some(magic) = self.take_exact(4)? else {
            self.pass_through();
            return Ok(());
        };
        if magic != MAGIC {
            self.pass_through();
            return Ok(());
        }
        let mut meta: Vec<(String, Vec<u8>)> = Vec::new();
        let mut eof = false;
        loop {
            let Some(count) = self.long(&mut eof)? else {
                self.pass_through();
                return Ok(());
            };
            if count == 0 {
                break;
            }
            if count < 0 && self.long(&mut eof)?.is_none() {
                self.pass_through();
                return Ok(());
            }
            for _ in 0..count.unsigned_abs() {
                let key = match self.long(&mut eof)? {
                    Some(n) if n >= 0 => self.take_exact(n as u64)?,
                    _ => None,
                };
                let value = match self.long(&mut eof)? {
                    Some(n) if n >= 0 => self.take_exact(n as u64)?,
                    _ => None,
                };
                let (Some(key), Some(value)) = (key, value) else {
                    self.pass_through();
                    return Ok(());
                };
                let Ok(key) = String::from_utf8(key) else {
                    self.pass_through();
                    return Ok(());
                };
                meta.push((key, value));
            }
        }
        let Some(sync) = self.take_exact(16)? else {
            self.pass_through();
            return Ok(());
        };
        let codec = match meta.iter().find(|(k, _)| k == "avro.codec") {
            None => Some(Codec::Null),
            Some((_, v)) => std::str::from_utf8(v)
                .ok()
                .and_then(|s| Codec::from_str(s).ok()),
        };
        let schema = meta
            .iter()
            .find(|(k, _)| k == "avro.schema")
            .and_then(|(_, v)| std::str::from_utf8(v).ok())
            .and_then(|json| self.writer_schema(json));
        if schema.is_none() && self.reader_schema.is_some() {
            return Err(self.fail(FaucetError::Source(
                "avro header: the writer schema does not resolve against the reader schema".into(),
            )));
        }
        let (Some(codec), Some((schema, names))) = (codec, schema) else {
            self.pass_through();
            return Ok(());
        };
        self.codec = codec;
        self.schema = Some(schema);
        self.names = names;
        self.sync.copy_from_slice(&sync);
        let kept: Vec<&(String, Vec<u8>)> = meta
            .iter()
            .filter(|(k, _)| k != "avro.codec" && k != "avro.codec.compression_level")
            .collect();
        let mut out = MAGIC.to_vec();
        put_long(&mut out, kept.len() as i64 + 1);
        for (k, v) in kept {
            put_bytes(&mut out, k.as_bytes());
            put_bytes(&mut out, v);
        }
        put_bytes(&mut out, b"avro.codec");
        put_bytes(&mut out, b"null");
        put_long(&mut out, 0);
        out.extend_from_slice(&self.sync);
        self.out = out;
        self.pos = 0;
        self.state = State::Blocks;
        Ok(())
    }

    /// The writer schema and every type it names, resolved the way the Avro
    /// reader resolves it (against the reader schema's names when needed).
    fn writer_schema(&self, json: &str) -> Option<(Schema, HashMap<Name, Schema>)> {
        let own = |schema: Schema| {
            let names = ResolvedSchema::try_from(&schema)
                .ok()?
                .get_names()
                .iter()
                .map(|(n, s)| (n.clone(), (*s).clone()))
                .collect();
            Some((schema, names))
        };
        if let Some(found) = Schema::parse_str(json).ok().and_then(own) {
            return Some(found);
        }
        let reader = self.reader_schema.as_ref()?;
        let reader_json = serde_json::to_string(reader).ok()?;
        let (schema, list) = Schema::parse_str_with_list(json, [reader_json]).ok()?;
        let mut schemata = vec![&schema];
        schemata.extend(list.iter());
        let names = ResolvedSchema::new_with_schemata(schemata)
            .ok()?
            .get_names()
            .iter()
            .map(|(n, s)| (n.clone(), (*s).clone()))
            .collect();
        Some((schema, names))
    }

    fn block(&mut self) -> io::Result<()> {
        self.recorded.clear();
        let mut eof = false;
        let count = match self.long(&mut eof)? {
            Some(n) if n >= 0 => n,
            None if eof => {
                self.state = State::Done;
                return Ok(());
            }
            _ => {
                self.pass_through();
                return Ok(());
            }
        };
        let data = match self.long(&mut eof)? {
            Some(n) if n >= 0 => self.take_exact(n as u64)?,
            _ => None,
        };
        let sync = self.take_exact(16)?;
        let (Some(mut data), Some(sync)) = (data, sync) else {
            self.pass_through();
            return Ok(());
        };
        if sync != self.sync {
            self.pass_through();
            return Ok(());
        }
        if let Err(e) = self.codec.decompress(&mut data) {
            return Err(self.fail(FaucetError::Source(format!("avro block: {e}"))));
        }
        let schema = self.schema.as_ref().expect("set with the header");
        let mut census = Census {
            names: &self.names,
            limit: self.limit,
            used: 0,
        };
        let mut input: &[u8] = &data;
        for _ in 0..count {
            let before = input.len();
            census.used = 0;
            match census.walk(schema, None, &mut input, 0) {
                Ok(()) => {}
                Err(Stop::Malformed) => break,
                Err(Stop::Over) => {
                    return Err(self.fail(budget::exceeded("an avro record", self.limit)));
                }
                Err(Stop::TooDeep) => {
                    return Err(self.fail(FaucetError::Source(format!(
                        "an avro record nests deeper than {MAX_DEPTH} levels"
                    ))));
                }
            }
            if input.len() == before {
                break;
            }
        }
        let mut out = Vec::with_capacity(data.len() + 36);
        put_long(&mut out, count);
        put_long(&mut out, data.len() as i64);
        out.extend_from_slice(&data);
        out.extend_from_slice(&self.sync);
        self.out = out;
        self.pos = 0;
        Ok(())
    }
}

impl<R: Read> Read for Bounded<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            if self.pos < self.out.len() {
                let n = buf.len().min(self.out.len() - self.pos);
                buf[..n].copy_from_slice(&self.out[self.pos..self.pos + n]);
                self.pos += n;
                return Ok(n);
            }
            match self.state {
                State::Header => self.header()?,
                State::Blocks => self.block()?,
                State::Passthrough => return self.inner.read(buf),
                State::Done => return Ok(0),
            }
        }
    }
}

fn put_long(out: &mut Vec<u8>, v: i64) {
    let mut z = ((v << 1) ^ (v >> 63)) as u64;
    loop {
        if z < 0x80 {
            out.push(z as u8);
            return;
        }
        out.push((z as u8 & 0x7f) | 0x80);
        z >>= 7;
    }
}

fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    put_long(out, b.len() as i64);
    out.extend_from_slice(b);
}

enum Stop {
    Over,
    TooDeep,
    Malformed,
}

/// Counts the values one datum decodes to.
struct Census<'a> {
    names: &'a HashMap<Name, Schema>,
    limit: u64,
    used: u64,
}

impl Census<'_> {
    fn charge(&mut self, n: u64) -> Result<(), Stop> {
        self.used = self.used.saturating_add(n);
        if self.used > self.limit {
            return Err(Stop::Over);
        }
        Ok(())
    }

    fn walk(
        &mut self,
        s: &Schema,
        ns: NamespaceRef,
        input: &mut &[u8],
        depth: usize,
    ) -> Result<(), Stop> {
        if depth > MAX_DEPTH {
            return Err(Stop::TooDeep);
        }
        match s {
            Schema::Null => self.charge(NODE),
            Schema::Boolean => {
                skip(input, 1)?;
                self.charge(NODE)
            }
            Schema::Int
            | Schema::Long
            | Schema::Date
            | Schema::TimeMillis
            | Schema::TimeMicros
            | Schema::TimestampMillis
            | Schema::TimestampMicros
            | Schema::TimestampNanos
            | Schema::LocalTimestampMillis
            | Schema::LocalTimestampMicros
            | Schema::LocalTimestampNanos
            | Schema::Enum(_) => {
                long(input)?;
                self.charge(NODE)
            }
            Schema::Float => {
                skip(input, 4)?;
                self.charge(NODE)
            }
            Schema::Double => {
                skip(input, 8)?;
                self.charge(NODE)
            }
            Schema::Duration(_) => {
                skip(input, 12)?;
                self.charge(NODE)
            }
            Schema::Bytes
            | Schema::String
            | Schema::BigDecimal
            | Schema::Uuid(UuidSchema::Bytes | UuidSchema::String)
            | Schema::Decimal(apache_avro::schema::DecimalSchema {
                inner: InnerDecimalSchema::Bytes,
                ..
            }) => {
                let n = long(input)?;
                let n = u64::try_from(n).map_err(|_| Stop::Malformed)?;
                skip(input, n)?;
                self.charge(NODE.saturating_add(n.saturating_mul(2)))
            }
            Schema::Fixed(f)
            | Schema::Uuid(UuidSchema::Fixed(f))
            | Schema::Decimal(apache_avro::schema::DecimalSchema {
                inner: InnerDecimalSchema::Fixed(f),
                ..
            }) => {
                skip(input, f.size as u64)?;
                self.charge(NODE.saturating_add((f.size as u64).saturating_mul(2)))
            }
            Schema::Array(a) => {
                self.charge(NODE)?;
                self.items(input, ns, |c, input, ns| {
                    c.walk(&a.items, ns, input, depth + 1)
                })
            }
            Schema::Map(m) => {
                self.charge(NODE)?;
                self.items(input, ns, |c, input, ns| {
                    c.walk(&Schema::String, ns, input, depth + 1)?;
                    c.walk(&m.types, ns, input, depth + 1)
                })
            }
            Schema::Union(u) => {
                let i = long(input)?;
                let variant = usize::try_from(i)
                    .ok()
                    .and_then(|i| u.variants().get(i))
                    .ok_or(Stop::Malformed)?;
                self.charge(NODE)?;
                self.walk(variant, ns, input, depth + 1)
            }
            Schema::Record(RecordSchema { name, fields, .. }) => {
                self.charge(NODE)?;
                let full = name.fully_qualified_name(ns);
                for field in fields {
                    self.charge(field.name.len() as u64)?;
                    self.walk(&field.schema, full.namespace(), input, depth + 1)?;
                }
                Ok(())
            }
            Schema::Ref { name } => {
                let full = name.fully_qualified_name(ns);
                let resolved = self.names.get(full.as_ref()).ok_or(Stop::Malformed)?;
                self.walk(resolved, full.namespace(), input, depth + 1)
            }
        }
    }

    /// The blocks of an array or map. A run of items that take no input
    /// bytes is charged in one step rather than walked item by item.
    fn items(
        &mut self,
        input: &mut &[u8],
        ns: NamespaceRef,
        mut item: impl FnMut(&mut Self, &mut &[u8], NamespaceRef) -> Result<(), Stop>,
    ) -> Result<(), Stop> {
        loop {
            let mut count = long(input)?;
            if count == 0 {
                return Ok(());
            }
            if count < 0 {
                count = count.checked_neg().ok_or(Stop::Malformed)?;
                long(input)?;
            }
            let count = count as u64;
            if self.used.saturating_add(count.saturating_mul(NODE)) > self.limit {
                return Err(Stop::Over);
            }
            let mut i = 0;
            while i < count {
                let (bytes, used) = (input.len(), self.used);
                item(self, input, ns)?;
                i += 1;
                if input.len() == bytes {
                    let each = self.used - used;
                    self.charge(each.saturating_mul(count - i))?;
                    break;
                }
            }
        }
    }
}

fn skip(input: &mut &[u8], n: u64) -> Result<(), Stop> {
    let n = usize::try_from(n).map_err(|_| Stop::Malformed)?;
    if input.len() < n {
        return Err(Stop::Malformed);
    }
    *input = &input[n..];
    Ok(())
}

fn long(input: &mut &[u8]) -> Result<i64, Stop> {
    let mut value: u64 = 0;
    for (i, b) in input.iter().enumerate().take(10) {
        value |= u64::from(b & 0x7f) << (7 * i);
        if b & 0x80 == 0 {
            *input = &input[i + 1..];
            return Ok(((value >> 1) as i64) ^ -((value & 1) as i64));
        }
    }
    Err(Stop::Malformed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_format::avro::read_records;
    use crate::file_format::{ContainerDecoder, FileFormat, FileInput, FormatOptions};
    use serde_json::{Value, json};

    const SYNC: [u8; 16] = [7; 16];

    fn ocf(schema: &str, blocks: &[(i64, Vec<u8>)]) -> Vec<u8> {
        let mut out = MAGIC.to_vec();
        put_long(&mut out, 2);
        put_bytes(&mut out, b"avro.schema");
        put_bytes(&mut out, schema.as_bytes());
        put_bytes(&mut out, b"avro.codec");
        put_bytes(&mut out, b"null");
        put_long(&mut out, 0);
        out.extend_from_slice(&SYNC);
        for (count, data) in blocks {
            put_long(&mut out, *count);
            put_long(&mut out, data.len() as i64);
            out.extend_from_slice(data);
            out.extend_from_slice(&SYNC);
        }
        out
    }

    fn longs(values: &[i64]) -> Vec<u8> {
        let mut out = Vec::new();
        for v in values {
            put_long(&mut out, *v);
        }
        out
    }

    fn read(bytes: &[u8], limit: u64) -> Result<Vec<Value>, FaucetError> {
        let mut out = Vec::new();
        let mut d = ContainerDecoder::new(FileFormat::Avro, &FormatOptions::default())?
            .with_max_decoded_bytes(limit);
        d.records("f.avro", FileInput::Bytes(bytes.to_vec()), 0, &mut |c| {
            out.extend(c);
            Ok(())
        })?;
        Ok(out)
    }

    const NULLS: &str = r#"{"type":"record","name":"r","fields":[{"name":"a","type":{"type":"array","items":"null"}}]}"#;

    #[test]
    fn a_huge_array_of_nulls_is_refused_before_it_is_decoded() {
        let bomb = ocf(NULLS, &[(1, longs(&[1 << 40, 0]))]);
        assert!(bomb.len() < 200, "{}", bomb.len());
        let err = read(&bomb, budget::MAX_DEFAULT_DECODED_BYTES).unwrap_err();
        assert!(err.to_string().contains("max_decoded_bytes"), "{err}");
        let err = read_records(&bomb[..], None, 10, &mut |_| Ok(())).unwrap_err();
        assert!(err.to_string().contains("max_decoded_bytes"), "{err}");
    }

    #[test]
    fn nested_arrays_of_nulls_are_refused_before_they_are_decoded() {
        let schema = r#"{"type":"record","name":"r","fields":[{"name":"a","type":{"type":"array","items":{"type":"array","items":"null"}}}]}"#;
        let mut datum = longs(&[1000]);
        for _ in 0..1000 {
            datum.extend(longs(&[4_000_000, 0]));
        }
        datum.extend(longs(&[0]));
        let bomb = ocf(schema, &[(1, datum)]);
        let err = read(&bomb, budget::MAX_DEFAULT_DECODED_BYTES).unwrap_err();
        assert!(err.to_string().contains("max_decoded_bytes"), "{err}");
    }

    #[test]
    fn arrays_of_empty_records_count_like_nulls() {
        let schema = r#"{"type":"record","name":"r","fields":[{"name":"a","type":{"type":"array","items":{"type":"record","name":"e","fields":[]}}}]}"#;
        let bomb = ocf(schema, &[(1, longs(&[-(1 << 40), 0, 0]))]);
        let err = read(&bomb, 1 << 20).unwrap_err();
        assert!(err.to_string().contains("max_decoded_bytes"), "{err}");
        let small = ocf(schema, &[(1, longs(&[3, 0]))]);
        assert_eq!(
            read(&small, 1 << 20).unwrap(),
            vec![json!({"a": [{}, {}, {}]})]
        );
    }

    #[test]
    fn zero_width_runs_are_charged_once_per_item() {
        let ok = ocf(NULLS, &[(1, longs(&[100, 0]))]);
        assert_eq!(
            read(&ok, 1 << 20).unwrap()[0]["a"]
                .as_array()
                .unwrap()
                .len(),
            100
        );
        let over = ocf(NULLS, &[(1, longs(&[10_000, 0]))]);
        let err = read(&over, 100 * NODE).unwrap_err();
        assert!(err.to_string().contains("max_decoded_bytes"), "{err}");
    }

    #[test]
    fn records_within_the_limit_decode_and_a_batch_over_it_is_refused() {
        let schema = r#"{"type":"record","name":"r","fields":[{"name":"s","type":"string"},{"name":"m","type":{"type":"map","values":"long"}},{"name":"u","type":["null","double"]},{"name":"f","type":{"type":"fixed","name":"x","size":2}}]}"#;
        let mut datum = longs(&[3]);
        datum.extend_from_slice(b"abc");
        datum.extend(longs(&[1, 1]));
        datum.push(b'k');
        datum.extend(longs(&[5, 0, 1]));
        datum.extend_from_slice(&1.5f64.to_le_bytes());
        datum.extend_from_slice(&[0, 1]);
        let file = ocf(schema, &[(2, [datum.clone(), datum].concat())]);
        let rows = read(&file, 1 << 20).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["s"], json!("abc"));
        assert_eq!(rows[0]["m"], json!({"k": 5}));
        let err = read(&file, 2 * NODE).unwrap_err();
        assert!(err.to_string().contains("max_decoded_bytes"), "{err}");
        assert!(err.to_string().contains("f.avro"), "{err}");
    }

    #[test]
    fn records_nested_past_the_depth_limit_are_refused() {
        let schema =
            r#"{"type":"record","name":"n","fields":[{"name":"next","type":["null","n"]}]}"#;
        let mut datum = longs(&vec![1; MAX_DEPTH + 10]);
        datum.extend(longs(&[0]));
        let err = read(&ocf(schema, &[(1, datum)]), 1 << 30).unwrap_err();
        assert!(err.to_string().contains("nests deeper"), "{err}");
    }

    #[test]
    fn compressed_blocks_are_bounded_too() {
        let schema = apache_avro::Schema::parse_str(NULLS).unwrap();
        let mut w = apache_avro::Writer::with_codec(
            &schema,
            Vec::new(),
            Codec::Deflate(Default::default()),
        )
        .unwrap();
        let mut rec = apache_avro::types::Record::new(&schema).unwrap();
        rec.put(
            "a",
            apache_avro::types::Value::Array(vec![apache_avro::types::Value::Null; 5000]),
        );
        w.append_value(rec).unwrap();
        let file = w.into_inner().unwrap();
        assert_eq!(
            read(&file, 1 << 30).unwrap()[0]["a"]
                .as_array()
                .unwrap()
                .len(),
            5000
        );
        let err = read(&file, 1000 * NODE).unwrap_err();
        assert!(err.to_string().contains("max_decoded_bytes"), "{err}");
    }

    #[test]
    fn input_it_cannot_read_reaches_the_avro_reader_unchanged() {
        let err = read(b"not avro at all", 1 << 20).unwrap_err();
        assert!(err.to_string().contains("avro header"), "{err}");
        let mut truncated = ocf(NULLS, &[(1, longs(&[2, 0]))]);
        truncated.truncate(truncated.len() - 4);
        assert!(read(&truncated, 1 << 20).is_err());
        let mut bad_sync = ocf(NULLS, &[(1, longs(&[2, 0]))]);
        let n = bad_sync.len();
        bad_sync[n - 1] = 0;
        assert!(read(&bad_sync, 1 << 20).is_err());
        let unknown = ocf(
            r#"{"type":"record","name":"r","fields":[{"name":"a","type":"missing"}]}"#,
            &[],
        );
        assert!(read(&unknown, 1 << 20).is_err());
        assert!(read(&ocf(NULLS, &[(-1, vec![])]), 1 << 20).is_err());
        let reader = apache_avro::Schema::parse_str(NULLS).unwrap();
        let err = read_records(&unknown[..], Some(&reader), 10, &mut |_| Ok(())).unwrap_err();
        assert!(err.to_string().contains("does not resolve"), "{err}");
        assert!(read(&ocf(NULLS, &[]), 1 << 20).unwrap().is_empty());
    }

    #[test]
    fn varints_round_trip() {
        for v in [0i64, 1, -1, 63, -64, 64, 1 << 40, i64::MAX, i64::MIN] {
            let bytes = longs(&[v]);
            let mut input: &[u8] = &bytes;
            assert_eq!(long(&mut input).ok(), Some(v));
            assert!(input.is_empty());
        }
        let mut input: &[u8] = &[0x80; 11];
        assert!(matches!(long(&mut input), Err(Stop::Malformed)));
    }
}
