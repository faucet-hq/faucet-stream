//! Opt-in Apache Arrow columnar record path (feature `arrow`, RFC 0002 / #375).
//!
//! This is the **escape hatch** described in #324: a columnar representation a
//! connector may produce or consume at the page boundary, *additive* to and
//! coexisting with the default `serde_json::Value` row model. It touches neither
//! [`StreamPage`](crate::StreamPage) nor any existing connector — an Arrow-native
//! source overrides [`Source::stream_batches`](crate::Source::stream_batches) and
//! an Arrow-native sink overrides
//! [`Sink::write_batch_columnar`](crate::Sink::write_batch_columnar); the pipeline
//! uses the columnar path only when **both** sides advertise support (and no
//! `Value`-shaped stage needs to observe the records), so a
//! `parquet → parquet` chain never materializes `Value`.
//!
//! The `RecordBatch ↔ Value` conversions here are the single source of truth for
//! the shim (they match `faucet-transform-sql`'s `shovel` byte-for-byte, incl.
//! `with_explicit_nulls(true)` so an explicit-null field round-trips as
//! `"key": null` rather than being silently dropped — audit #321 H6).

use crate::FaucetError;
use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use serde_json::Value;
use std::sync::Arc;

/// A page of records in **columnar** (Arrow) form, the columnar analogue of
/// [`StreamPage`](crate::StreamPage).
///
/// `bookmark` carries the exact same checkpoint semantics as `StreamPage`:
/// `Some` triggers flush + bookmark-persist after the batch is durably written;
/// most sources emit `Some` only on the final batch, CDC-style sources per
/// committed transaction.
#[derive(Debug, Clone)]
pub struct ColumnarPage {
    /// The record batch to write to the sink for this page.
    pub batch: RecordBatch,
    /// Optional bookmark to checkpoint after this batch is durably written.
    pub bookmark: Option<Value>,
}

impl ColumnarPage {
    /// Construct a columnar page from a batch and optional bookmark.
    pub fn new(batch: RecordBatch, bookmark: Option<Value>) -> Self {
        Self { batch, bookmark }
    }

    /// Number of rows in the batch.
    pub fn num_rows(&self) -> usize {
        self.batch.num_rows()
    }
}

/// Map any display-able error into a [`FaucetError::Transform`] with context.
fn te<E: std::fmt::Display>(ctx: &str, e: E) -> FaucetError {
    FaucetError::Transform(format!("columnar shim: {ctx}: {e}"))
}

/// Infer an Arrow [`Schema`] from a slice of JSON records (each a JSON object).
///
/// Returns the inferred schema in an [`Arc`]. An empty slice yields a schema
/// with no fields.
pub fn infer_arrow_schema(records: &[Value]) -> Result<SchemaRef, FaucetError> {
    let iter = records
        .iter()
        .map(|v| Ok::<_, arrow::error::ArrowError>(v.clone()));
    let schema = arrow_json::reader::infer_json_schema_from_iterator(iter)
        .map_err(|e| te("schema inference", e))?;
    refine_wide_integers(&schema, records).map(Arc::new)
}

/// Re-type fields that `arrow-json` widened to `Float64` purely because an
/// integer did not fit `i64`, and refuse the cases that cannot be re-typed.
///
/// `arrow-json` infers `Float64` for any integer outside `i64` range, so a `u64`
/// id such as `18446744073709551615` silently becomes `1.8446744073709552e19` —
/// exact value gone, `Ok` returned (#460). Where every observed value for such a
/// field is a non-negative integer, `UInt64` holds it exactly, so use that.
/// Where no integer type fits (values spanning negative *and* above `i64::MAX`)
/// or the value sits somewhere this pass does not re-type (inside a list), fail
/// with a typed error naming the path rather than approximating it.
///
/// Fields arrow-json typed `Float64` because a value genuinely *is* fractional
/// are left alone, as is a field mixing integers and floats — coercing those to
/// `Float64` is ordinary JSON-number behaviour, not loss of an exact integer.
fn refine_wide_integers(schema: &Schema, records: &[Value]) -> Result<Schema, FaucetError> {
    use arrow::datatypes::{DataType, Field};

    /// Values observed at one field across the page (nulls skipped).
    fn observed<'a>(records: &'a [Value], name: &str) -> Vec<&'a Value> {
        records
            .iter()
            .filter_map(|r| r.get(name))
            .filter(|v| !v.is_null())
            .collect()
    }

    /// Does this number need more than `i64` *and* is it an exact integer?
    fn is_wide_integer(v: &Value) -> bool {
        matches!(v, Value::Number(n) if !n.is_i64() && n.is_u64())
    }

    fn refine_field(field: &Field, values: Vec<&Value>) -> Result<Field, FaucetError> {
        match field.data_type() {
            DataType::Float64 if values.iter().any(|v| is_wide_integer(v)) => {
                // Every value integral and non-negative → UInt64 is exact.
                if values
                    .iter()
                    .all(|v| matches!(v, Value::Number(n) if n.is_u64()))
                {
                    Ok(field.clone().with_data_type(DataType::UInt64))
                } else {
                    Err(FaucetError::Transform(format!(
                        "columnar shim: field {:?} mixes an integer above i64::MAX with values \
                         no unsigned type can hold, so no exact Arrow type fits. Convert it to a \
                         string first (a `cast` transform) — it is not silently widened to a \
                         float because that loses the exact value",
                        field.name()
                    )))
                }
            }
            DataType::Struct(children) => {
                let refined = children
                    .iter()
                    .map(|child| {
                        let child_values = values
                            .iter()
                            .filter_map(|v| v.get(child.name()))
                            .filter(|v| !v.is_null())
                            .collect();
                        refine_field(child, child_values).map(Arc::new)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(field
                    .clone()
                    .with_data_type(DataType::Struct(refined.into())))
            }
            // A wide integer inside a list is not re-typed by this pass; refusing
            // beats writing an approximation nobody asked for.
            _ if contains_wide_integer_in_container(&values) => {
                Err(FaucetError::Transform(format!(
                    "columnar shim: field {:?} contains an integer above i64::MAX inside a list, \
                     which the columnar path cannot represent exactly. Convert those elements to \
                     strings first (a `cast` transform)",
                    field.name()
                )))
            }
            _ => Ok(field.clone()),
        }
    }

    /// Any wide integer nested inside an array (at any depth).
    fn contains_wide_integer_in_container(values: &[&Value]) -> bool {
        fn walk(v: &Value, in_list: bool) -> bool {
            match v {
                Value::Array(items) => items.iter().any(|i| walk(i, true)),
                Value::Object(map) => map.values().any(|i| walk(i, in_list)),
                other => in_list && is_wide_integer(other),
            }
        }
        values.iter().any(|v| walk(v, false))
    }

    let fields = schema
        .fields()
        .iter()
        .map(|f| refine_field(f, observed(records, f.name())).map(Arc::new))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Schema::new(fields).with_metadata(schema.metadata().clone()))
}

/// Top-level numeric, boolean or temporal columns of `schema` that a masking
/// pass re-types into strings: a name-based rule covers them (`rewrites`), or
/// a numeric/boolean column now holds a string in some record (temporal
/// columns always read back as strings, so only a name rule widens them).
#[cfg(feature = "masking")]
pub(crate) fn masked_string_columns(
    schema: &Schema,
    records: &[Value],
    rewrites: impl Fn(&str) -> bool,
) -> Vec<String> {
    schema
        .fields()
        .iter()
        .filter(|f| {
            let t = f.data_type();
            let scalar = t.is_numeric() || *t == DataType::Boolean;
            (rewrites(f.name()) && (scalar || t.is_temporal()))
                || (scalar
                    && records
                        .iter()
                        .any(|r| r.get(f.name()).is_some_and(Value::is_string)))
        })
        .map(|f| f.name().clone())
        .collect()
}

/// `schema` with the named top-level columns as nullable Utf8.
pub(crate) fn widen_to_utf8(
    schema: &SchemaRef,
    names: &std::collections::BTreeSet<String>,
) -> SchemaRef {
    if !schema.fields().iter().any(|f| names.contains(f.name())) {
        return Arc::clone(schema);
    }
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| {
            if names.contains(f.name()) {
                Field::new(f.name(), DataType::Utf8, true).with_metadata(f.metadata().clone())
            } else {
                f.as_ref().clone()
            }
        })
        .collect();
    Arc::new(Schema::new(fields).with_metadata(schema.metadata().clone()))
}

/// Encode a slice of JSON records into a single [`RecordBatch`] against `schema`.
///
/// Returns an empty batch if `records` is empty. A fractional number bound
/// for an integer column is an error (see [`check_integral`]).
pub fn values_to_record_batch(
    records: &[Value],
    schema: SchemaRef,
) -> Result<RecordBatch, FaucetError> {
    check_integral(records, &schema)?;
    let rehexed = base64_binary_to_hex(records, &schema)?;
    let records = rehexed.as_deref().unwrap_or(records);
    let mut decoder = arrow_json::ReaderBuilder::new(schema.clone())
        .build_decoder()
        .map_err(|e| te("decoder build", e))?;
    decoder.serialize(records).map_err(|e| te("encode", e))?;
    let mut batches = Vec::new();
    while let Some(b) = decoder.flush().map_err(|e| te("flush", e))? {
        batches.push(b);
    }
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }
    if batches.len() == 1 {
        return Ok(batches.pop().unwrap());
    }
    arrow::compute::concat_batches(&schema, &batches).map_err(|e| te("concat", e))
}

fn holds_binary(dt: &DataType) -> bool {
    match dt {
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => true,
        DataType::Struct(fields) => fields.iter().any(|f| holds_binary(f.data_type())),
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
            holds_binary(f.data_type())
        }
        _ => false,
    }
}

/// Binary values travel as base64 (the canonical rendering, [`crate::fidelity`])
/// but arrow-json reads hex: rewrite the strings bound for binary columns.
/// `None` when `schema` has none.
fn base64_binary_to_hex(
    records: &[Value],
    schema: &Schema,
) -> Result<Option<Vec<Value>>, FaucetError> {
    use base64::Engine as _;
    fn rewrite(value: &mut Value, dt: &DataType, path: &str) -> Result<(), FaucetError> {
        match (value, dt) {
            (Value::String(s), dt) if !dt.is_nested() && holds_binary(dt) => {
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(s.as_bytes())
                    .map_err(|_| {
                        FaucetError::Transform(format!(
                            "columnar shim: field '{path}' holds {s:?}, which is not base64"
                        ))
                    })?;
                *s = bytes.iter().map(|b| format!("{b:02x}")).collect();
                Ok(())
            }
            (Value::Object(map), DataType::Struct(fields)) => {
                for f in fields {
                    if let Some(v) = map.get_mut(f.name()) {
                        rewrite(v, f.data_type(), &format!("{path}.{}", f.name()))?;
                    }
                }
                Ok(())
            }
            (
                Value::Array(items),
                DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _),
            ) => {
                for item in items {
                    rewrite(item, f.data_type(), path)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
    let fields: Vec<&Field> = schema
        .fields()
        .iter()
        .map(|f| f.as_ref())
        .filter(|f| holds_binary(f.data_type()))
        .collect();
    if fields.is_empty() {
        return Ok(None);
    }
    let mut out = records.to_vec();
    for record in &mut out {
        let Value::Object(map) = record else { continue };
        for f in &fields {
            if let Some(v) = map.get_mut(f.name()) {
                rewrite(v, f.data_type(), f.name())?;
            }
        }
    }
    Ok(Some(out))
}

/// Refuse a fractional JSON number bound for an integer column of `schema`,
/// at any depth (struct fields, list items).
///
/// arrow-json converts a float into an integer column by truncating it
/// (`3.7` → `3`), so any JSON → Arrow decode against a declared or inferred
/// integer type should run this first. Whole floats (`3.0`) pass.
pub fn check_integral(records: &[Value], schema: &Schema) -> Result<(), FaucetError> {
    use arrow::datatypes::DataType;

    fn holds_integer(dt: &DataType) -> bool {
        match dt {
            DataType::Struct(fields) => fields.iter().any(|f| holds_integer(f.data_type())),
            DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
                holds_integer(f.data_type())
            }
            other => other.is_integer(),
        }
    }

    fn walk(value: &Value, dt: &DataType, path: &str) -> Result<(), FaucetError> {
        match (value, dt) {
            (Value::Number(n), dt) if dt.is_integer() => {
                if n.as_i64().is_none()
                    && n.as_u64().is_none()
                    && n.as_f64()
                        .is_none_or(|x| x.fract() != 0.0 || !x.is_finite())
                {
                    return Err(FaucetError::Transform(format!(
                        "field '{path}': {n} is not a whole number, but the column is {dt}"
                    )));
                }
                Ok(())
            }
            (Value::Object(map), DataType::Struct(fields)) => {
                for f in fields {
                    if let Some(child) = map.get(f.name()) {
                        walk(child, f.data_type(), &format!("{path}.{}", f.name()))?;
                    }
                }
                Ok(())
            }
            (
                Value::Array(items),
                DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _),
            ) => {
                for item in items {
                    walk(item, f.data_type(), &format!("{path}[]"))?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    let fields: Vec<_> = schema
        .fields()
        .iter()
        .filter(|f| holds_integer(f.data_type()))
        .collect();
    if fields.is_empty() {
        return Ok(());
    }
    for record in records {
        let Value::Object(map) = record else { continue };
        for f in &fields {
            if let Some(v) = map.get(f.name()) {
                walk(v, f.data_type(), f.name())?;
            }
        }
    }
    Ok(())
}

/// Convenience: infer the schema from `records` and encode them into a batch.
pub fn values_to_record_batch_inferred(records: &[Value]) -> Result<RecordBatch, FaucetError> {
    let schema = infer_arrow_schema(records)?;
    values_to_record_batch(records, schema)
}

/// Decode a [`RecordBatch`] into JSON objects (one per row).
///
/// Uses `arrow-json`'s array writer with **explicit nulls enabled**, so a
/// null-valued column is emitted as `"key": null` rather than omitted — without
/// this a `SELECT *`-style identity would silently delete every explicit-null
/// field (audit #321 H6). An empty batch returns an empty `Vec`.
///
/// Values follow the canonical renderings of [`crate::fidelity`]: decimals are
/// exact strings (`"12345678901234567890.12"`) rather than JSON numbers, which
/// `serde_json` would round through `f64`; non-finite floats become a counted
/// `null`; binary columns are base64; timestamps and times carry their unit's
/// native precision, and a zoned timestamp its offset.
pub fn record_batch_to_values(batch: &RecordBatch) -> Result<Vec<Value>, FaucetError> {
    let mut buf = Vec::new();
    {
        let mut writer = arrow_json::writer::WriterBuilder::new()
            .with_explicit_nulls(true)
            .with_encoder_factory(Arc::new(ExactEncoderFactory))
            .build::<_, arrow_json::writer::JsonArray>(&mut buf);
        writer.write(batch).map_err(|e| te("json write", e))?;
        writer.finish().map_err(|e| te("json finish", e))?;
    }
    serde_json::from_slice(&buf).map_err(|e| te("json parse", e))
}

#[derive(Debug)]
struct ExactEncoderFactory;

struct StringEncoder<F: FnMut(usize) -> String>(F);

impl<F: FnMut(usize) -> String> arrow_json::writer::Encoder for StringEncoder<F> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let text = (self.0)(idx);
        serde_json::to_writer(&mut *out, &text).expect("writing to a Vec cannot fail");
    }
}

struct ValueEncoder<F: FnMut(usize) -> Value>(F);

impl<F: FnMut(usize) -> Value> arrow_json::writer::Encoder for ValueEncoder<F> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        serde_json::to_writer(&mut *out, &(self.0)(idx)).expect("writing to a Vec cannot fail");
    }
}

fn arrow_precision(unit: &arrow::datatypes::TimeUnit) -> crate::fidelity::TimePrecision {
    use crate::fidelity::TimePrecision;
    use arrow::datatypes::TimeUnit;
    match unit {
        TimeUnit::Second => TimePrecision::Seconds,
        TimeUnit::Millisecond => TimePrecision::Millis,
        TimeUnit::Microsecond => TimePrecision::Micros,
        TimeUnit::Nanosecond => TimePrecision::Nanos,
    }
}

/// An epoch offset in `precision` units: an instant in `zone` when the column
/// has one, a local date-time otherwise.
fn timestamp_json(
    v: i64,
    precision: crate::fidelity::TimePrecision,
    zone: Option<&arrow::array::timezone::Tz>,
) -> Value {
    use crate::fidelity::TimePrecision;
    let utc = match precision {
        TimePrecision::Seconds => chrono::DateTime::from_timestamp(v, 0),
        TimePrecision::Millis => chrono::DateTime::from_timestamp_millis(v),
        TimePrecision::Micros => chrono::DateTime::from_timestamp_micros(v),
        _ => Some(chrono::DateTime::from_timestamp_nanos(v)),
    };
    match (utc, zone) {
        (Some(utc), Some(tz)) => crate::fidelity::timestamp(&utc.with_timezone(tz), precision),
        (Some(utc), None) => crate::fidelity::timestamp_naive(&utc.naive_utc(), precision),
        (None, _) => Value::from(v),
    }
}

/// A time of day `v` units after midnight.
fn time_json(v: i64, unit: &arrow::datatypes::TimeUnit) -> Value {
    use arrow::datatypes::TimeUnit;
    let (per_second, nanos_per_unit) = match unit {
        TimeUnit::Second => (1, 1_000_000_000),
        TimeUnit::Millisecond => (1_000, 1_000_000),
        TimeUnit::Microsecond => (1_000_000, 1_000),
        TimeUnit::Nanosecond => (1_000_000_000, 1),
    };
    u32::try_from(v.div_euclid(per_second))
        .ok()
        .zip(u32::try_from(v.rem_euclid(per_second) * nanos_per_unit).ok())
        .and_then(|(s, n)| chrono::NaiveTime::from_num_seconds_from_midnight_opt(s, n))
        .map_or_else(
            || Value::from(v),
            |t| crate::fidelity::time(&t, arrow_precision(unit)),
        )
}

fn nullable<'a>(
    encoder: impl arrow_json::writer::Encoder + 'a,
    array: &'a dyn arrow::array::Array,
) -> arrow_json::writer::NullableEncoder<'a> {
    arrow_json::writer::NullableEncoder::new(Box::new(encoder), array.nulls().cloned())
}

impl arrow_json::writer::EncoderFactory for ExactEncoderFactory {
    fn make_default_encoder<'a>(
        &self,
        _field: &'a arrow::datatypes::FieldRef,
        array: &'a dyn arrow::array::Array,
        _options: &'a arrow_json::writer::EncoderOptions,
    ) -> Result<Option<arrow_json::writer::NullableEncoder<'a>>, arrow::error::ArrowError> {
        use arrow::array::AsArray;
        use arrow::datatypes::{
            DataType, Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type, Float16Type,
            Float32Type, Float64Type, Time32MillisecondType, Time32SecondType,
            Time64MicrosecondType, Time64NanosecondType, TimeUnit, TimestampMicrosecondType,
            TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
        };
        let enc = match array.data_type() {
            DataType::Decimal32(..) => {
                let a = array.as_primitive::<Decimal32Type>();
                nullable(StringEncoder(move |i| a.value_as_string(i)), array)
            }
            DataType::Decimal64(..) => {
                let a = array.as_primitive::<Decimal64Type>();
                nullable(StringEncoder(move |i| a.value_as_string(i)), array)
            }
            DataType::Decimal128(..) => {
                let a = array.as_primitive::<Decimal128Type>();
                nullable(StringEncoder(move |i| a.value_as_string(i)), array)
            }
            DataType::Decimal256(..) => {
                let a = array.as_primitive::<Decimal256Type>();
                nullable(StringEncoder(move |i| a.value_as_string(i)), array)
            }
            DataType::Float64 => {
                let a = array.as_primitive::<Float64Type>();
                if a.values().iter().all(|v| v.is_finite()) {
                    return Ok(None);
                }
                nullable(
                    ValueEncoder(move |i| crate::fidelity::float64(a.value(i))),
                    array,
                )
            }
            DataType::Float32 => {
                let a = array.as_primitive::<Float32Type>();
                if a.values().iter().all(|v| v.is_finite()) {
                    return Ok(None);
                }
                nullable(
                    ValueEncoder(move |i| crate::fidelity::float32(a.value(i))),
                    array,
                )
            }
            DataType::Float16 => {
                let a = array.as_primitive::<Float16Type>();
                if a.values().iter().all(|v| v.is_finite()) {
                    return Ok(None);
                }
                nullable(
                    ValueEncoder(move |i| crate::fidelity::float32(f32::from(a.value(i)))),
                    array,
                )
            }
            DataType::Binary => {
                let a = array.as_binary::<i32>();
                nullable(
                    ValueEncoder(move |i| crate::fidelity::binary(a.value(i))),
                    array,
                )
            }
            DataType::LargeBinary => {
                let a = array.as_binary::<i64>();
                nullable(
                    ValueEncoder(move |i| crate::fidelity::binary(a.value(i))),
                    array,
                )
            }
            DataType::BinaryView => {
                let a = array.as_binary_view();
                nullable(
                    ValueEncoder(move |i| crate::fidelity::binary(a.value(i))),
                    array,
                )
            }
            DataType::FixedSizeBinary(_) => {
                let a = array.as_fixed_size_binary();
                nullable(
                    ValueEncoder(move |i| crate::fidelity::binary(a.value(i))),
                    array,
                )
            }
            DataType::Timestamp(unit, tz) => {
                let zone = match tz {
                    Some(name) => match name.parse::<arrow::array::timezone::Tz>() {
                        Ok(z) => Some(z),
                        Err(_) => return Ok(None),
                    },
                    None => None,
                };
                let precision = arrow_precision(unit);
                let values: Vec<i64> = match unit {
                    TimeUnit::Second => array
                        .as_primitive::<TimestampSecondType>()
                        .values()
                        .to_vec(),
                    TimeUnit::Millisecond => array
                        .as_primitive::<TimestampMillisecondType>()
                        .values()
                        .to_vec(),
                    TimeUnit::Microsecond => array
                        .as_primitive::<TimestampMicrosecondType>()
                        .values()
                        .to_vec(),
                    TimeUnit::Nanosecond => array
                        .as_primitive::<TimestampNanosecondType>()
                        .values()
                        .to_vec(),
                };
                nullable(
                    ValueEncoder(move |i| timestamp_json(values[i], precision, zone.as_ref())),
                    array,
                )
            }
            DataType::Time32(unit) => {
                let unit = *unit;
                let values: Vec<i64> = match unit {
                    TimeUnit::Second => array
                        .as_primitive::<Time32SecondType>()
                        .values()
                        .iter()
                        .map(|v| i64::from(*v))
                        .collect(),
                    _ => array
                        .as_primitive::<Time32MillisecondType>()
                        .values()
                        .iter()
                        .map(|v| i64::from(*v))
                        .collect(),
                };
                nullable(ValueEncoder(move |i| time_json(values[i], &unit)), array)
            }
            DataType::Time64(unit) => {
                let unit = *unit;
                let values: Vec<i64> = match unit {
                    TimeUnit::Nanosecond => array
                        .as_primitive::<Time64NanosecondType>()
                        .values()
                        .to_vec(),
                    _ => array
                        .as_primitive::<Time64MicrosecondType>()
                        .values()
                        .to_vec(),
                };
                nullable(ValueEncoder(move |i| time_json(values[i], &unit)), array)
            }
            _ => return Ok(None),
        };
        Ok(Some(enc))
    }
}

/// Bytes the rows of `batch` occupy — its own slice of each column, so a
/// sliced batch is not charged for the whole parent buffers.
pub(crate) fn batch_slice_bytes(batch: &RecordBatch) -> u64 {
    use arrow::array::Array;
    batch
        .columns()
        .iter()
        .map(|c| {
            c.to_data()
                .get_slice_memory_size()
                .unwrap_or_else(|_| c.get_array_memory_size()) as u64
        })
        .sum()
}

/// Compare two schemas for field-level equality (name + data-type + nullability).
pub fn schema_eq(a: &Schema, b: &Schema) -> bool {
    a.fields() == b.fields()
}

#[cfg(test)]
mod slice_bytes_tests {
    use super::*;
    use arrow::array::Int64Array;

    #[test]
    fn a_sliced_batch_is_charged_for_its_rows_only() {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let col = Arc::new(Int64Array::from_iter_values(0..1000));
        let batch = RecordBatch::try_new(schema, vec![col]).unwrap();
        assert_eq!(batch_slice_bytes(&batch), 8000);
        assert_eq!(batch_slice_bytes(&batch.slice(0, 10)), 80);
        assert!(batch.slice(0, 10).get_array_memory_size() >= 8000);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_fractional_number_in_an_integer_column_is_refused() {
        use arrow::datatypes::{DataType, Field, Fields};
        let schema = Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int64, true),
            Field::new(
                "s",
                DataType::Struct(Fields::from(vec![Field::new("m", DataType::Int32, true)])),
                true,
            ),
            Field::new(
                "l",
                DataType::List(Arc::new(Field::new("item", DataType::UInt8, true))),
                true,
            ),
            Field::new("f", DataType::Float64, true),
        ]));
        let ok = [
            json!({"n": 3.0, "s": {"m": 2}, "l": [1, 2], "f": 1.5}),
            json!("x"),
        ];
        let batch = values_to_record_batch(&ok[..1], schema.clone()).unwrap();
        assert_eq!(batch.num_rows(), 1);
        check_integral(&ok, &schema).unwrap();
        for (bad, field) in [
            (json!({"n": 3.7}), "n"),
            (json!({"s": {"m": 0.5}}), "s.m"),
            (json!({"l": [1, 2.25]}), "l[]"),
        ] {
            let err = values_to_record_batch(&[bad], schema.clone()).unwrap_err();
            assert!(err.to_string().contains(&format!("'{field}'")), "{err}");
        }
        let floats = Schema::new(vec![Field::new("f", DataType::Float64, true)]);
        check_integral(&[json!({"f": 0.5})], &floats).unwrap();
    }

    #[test]
    fn round_trip_scalars_nulls_nested() {
        let recs = vec![
            json!({"id": 1, "name": "a", "score": 1.5, "ok": true, "tags": ["x", "y"]}),
            json!({"id": 2, "name": null, "score": null, "ok": false, "tags": []}),
        ];
        let batch = values_to_record_batch_inferred(&recs).unwrap();
        assert_eq!(batch.num_rows(), 2);
        let back = record_batch_to_values(&batch).unwrap();
        assert_eq!(back[0]["id"], json!(1));
        assert_eq!(back[0]["tags"], json!(["x", "y"]));
        // #321 H6: an explicit-null field survives the round-trip.
        assert!(back[1].as_object().unwrap().contains_key("name"));
        assert_eq!(back[1]["name"], json!(null));
    }

    #[test]
    fn empty_records_yield_empty_batch_and_back() {
        let schema = infer_arrow_schema(&[json!({"a": 1})]).unwrap();
        let batch = values_to_record_batch(&[], schema).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert!(record_batch_to_values(&batch).unwrap().is_empty());
    }

    fn decimal_batch() -> RecordBatch {
        use arrow::array::Decimal128Array;
        use arrow::datatypes::{DataType, Field};
        let amounts = Decimal128Array::from(vec![
            Some(12_345_678_901_234_567_890_123_456_789_012_345_678_i128),
            None,
            Some(-1),
        ])
        .with_precision_and_scale(38, 2)
        .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(38, 2),
            true,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(amounts)]).unwrap()
    }

    #[test]
    fn decimals_survive_as_exact_strings_and_round_trip() {
        let batch = decimal_batch();
        let values = record_batch_to_values(&batch).unwrap();
        assert_eq!(
            values[0]["amount"],
            json!("123456789012345678901234567890123456.78")
        );
        assert_eq!(values[1]["amount"], json!(null));
        assert_eq!(values[2]["amount"], json!("-0.01"));
        let back = values_to_record_batch(&values, batch.schema()).unwrap();
        assert_eq!(back, batch);
    }

    #[test]
    fn nested_decimals_are_exact_too() {
        use arrow::array::{Decimal128Array, ListArray};
        use arrow::buffer::OffsetBuffer;
        use arrow::datatypes::{DataType, Field};
        let item = Arc::new(Field::new("item", DataType::Decimal128(38, 0), true));
        let values = Decimal128Array::from(vec![99_999_999_999_999_999_999_999_i128])
            .with_precision_and_scale(38, 0)
            .unwrap();
        let list = ListArray::new(
            item.clone(),
            OffsetBuffer::from_lengths([1]),
            Arc::new(values),
            None,
        );
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ids",
            DataType::List(item),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(list)]).unwrap();
        let rows = record_batch_to_values(&batch).unwrap();
        assert_eq!(rows[0]["ids"], json!(["99999999999999999999999"]));
    }

    #[test]
    fn non_finite_floats_become_counted_nulls() {
        use arrow::array::{Float32Array, Float64Array};
        use arrow::datatypes::{DataType, Field};
        let halves = arrow::compute::cast(
            &Float32Array::from(vec![Some(f32::INFINITY), Some(0.5), None, Some(f32::NAN)]),
            &DataType::Float16,
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("d", DataType::Float64, true),
            Field::new("f", DataType::Float32, true),
            Field::new("h", DataType::Float16, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float64Array::from(vec![
                    Some(f64::NAN),
                    Some(f64::INFINITY),
                    Some(1.5),
                    None,
                ])),
                Arc::new(Float32Array::from(vec![
                    Some(f32::NEG_INFINITY),
                    Some(0.1),
                    Some(f32::NAN),
                    Some(2.0),
                ])),
                halves,
            ],
        )
        .unwrap();
        let counter = crate::fidelity::NonFiniteCounter::new();
        let rows = counter.scope_sync(|| record_batch_to_values(&batch).unwrap());
        assert_eq!(rows[0]["d"], json!(null));
        assert_eq!(rows[1]["d"], json!(null));
        assert_eq!(rows[2]["d"], json!(1.5));
        assert_eq!(rows[3]["d"], json!(null));
        assert_eq!(rows[0]["f"], json!(null));
        assert_eq!(rows[1]["f"].to_string(), "0.1");
        assert_eq!(rows[2]["f"], json!(null));
        assert_eq!(rows[0]["h"], json!(null));
        assert_eq!(rows[1]["h"], json!(0.5));
        assert_eq!(rows[2]["h"], json!(null));
        assert_eq!(rows[3]["h"], json!(null));
        assert_eq!(counter.count(), 6, "every non-finite value is counted");

        let rows = vec![
            json!({"d": "NaN", "f": 1.0, "h": 1.0}),
            json!({"d": "Infinity", "f": 1.0, "h": 1.0}),
            json!({"d": 1.5, "f": 1.0, "h": 1.0}),
        ];
        let back = values_to_record_batch(&rows, schema).unwrap();
        let d = back
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(d.value(0).is_nan());
        assert_eq!(d.value(1), f64::INFINITY);
        assert_eq!(d.value(2), 1.5);
    }

    #[test]
    fn all_finite_floats_keep_the_default_encoding() {
        let batch = values_to_record_batch_inferred(&[json!({"x": 0.25})]).unwrap();
        assert_eq!(record_batch_to_values(&batch).unwrap()[0]["x"], json!(0.25));
    }

    #[test]
    fn columnar_page_reports_rows() {
        let batch = values_to_record_batch_inferred(&[json!({"a": 1}), json!({"a": 2})]).unwrap();
        let page = ColumnarPage::new(batch, Some(json!({"lsn": 42})));
        assert_eq!(page.num_rows(), 2);
        assert_eq!(page.bookmark, Some(json!({"lsn": 42})));
    }
}

#[cfg(test)]
mod wide_integer_tests {
    use super::*;
    use serde_json::json;

    /// #460: arrow-json types an integer above `i64::MAX` as `Float64`, so
    /// `18446744073709551615` used to come back as `1.8446744073709552e19` with
    /// `Ok`. It must now round-trip exactly.
    #[test]
    fn u64_above_i64_max_round_trips_exactly() {
        let recs = vec![json!({"id": u64::MAX})];
        let back =
            record_batch_to_values(&values_to_record_batch_inferred(&recs).unwrap()).unwrap();
        assert_eq!(back[0]["id"], json!(u64::MAX), "exact value must survive");
        assert!(back[0]["id"].is_u64(), "and stay an integer, not a float");
    }

    #[test]
    fn mixed_small_and_wide_integers_all_survive() {
        let recs = vec![
            json!({"id": 1}),
            json!({"id": u64::MAX}),
            json!({"id": null}),
        ];
        let back =
            record_batch_to_values(&values_to_record_batch_inferred(&recs).unwrap()).unwrap();
        assert_eq!(back[0]["id"], json!(1));
        assert_eq!(back[1]["id"], json!(u64::MAX));
        assert_eq!(back[2]["id"], json!(null));
    }

    #[test]
    fn wide_integer_nested_in_a_struct_survives() {
        let recs = vec![json!({"outer": {"id": u64::MAX, "n": 3}})];
        let back =
            record_batch_to_values(&values_to_record_batch_inferred(&recs).unwrap()).unwrap();
        assert_eq!(back[0]["outer"]["id"], json!(u64::MAX));
        assert_eq!(back[0]["outer"]["n"], json!(3));
    }

    /// Genuine floats, and integer/float mixes, keep their existing behaviour —
    /// coercing those to Float64 is ordinary JSON-number semantics, not loss of
    /// an exact integer.
    #[test]
    fn genuine_floats_are_untouched() {
        let recs = vec![json!({"a": 1.5}), json!({"a": 2})];
        let back =
            record_batch_to_values(&values_to_record_batch_inferred(&recs).unwrap()).unwrap();
        assert_eq!(back[0]["a"], json!(1.5));
        assert_eq!(back[1]["a"], json!(2.0));

        // i64 range is unaffected (2^53+1 already round-tripped before).
        let recs = vec![json!({"n": 9007199254740993i64, "m": i64::MIN})];
        let back =
            record_batch_to_values(&values_to_record_batch_inferred(&recs).unwrap()).unwrap();
        assert_eq!(back[0]["n"], json!(9007199254740993i64));
        assert_eq!(back[0]["m"], json!(i64::MIN));
    }

    /// No exact type fits a field spanning negatives and above-i64::MAX, so it
    /// must fail loudly rather than approximate.
    #[test]
    fn unrepresentable_mix_is_refused() {
        let recs = vec![json!({"v": -1}), json!({"v": u64::MAX})];
        let err = match values_to_record_batch_inferred(&recs) {
            Err(e) => e.to_string(),
            Ok(b) => panic!("must refuse; got {:?}", record_batch_to_values(&b).unwrap()),
        };
        assert!(err.contains("\"v\""), "{err}");
        assert!(err.contains("cast"), "points at the workaround: {err}");
    }

    /// A wide integer inside a list is not re-typed by this pass, so it is
    /// refused rather than silently written as a float.
    #[test]
    fn wide_integer_inside_a_list_is_refused() {
        let recs = vec![json!({"ids": [1, u64::MAX]})];
        let err = match values_to_record_batch_inferred(&recs) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("must refuse a wide integer inside a list"),
        };
        assert!(err.contains("\"ids\""), "{err}");
        assert!(err.contains("list"), "{err}");
    }
}

#[cfg(test)]
mod fidelity_rendering_tests {
    use super::*;
    use arrow::array::{
        BinaryArray, BinaryViewArray, FixedSizeBinaryArray, LargeBinaryArray,
        Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray,
    };
    use arrow::datatypes::TimeUnit;
    use serde_json::json;

    fn one_column(name: &str, array: arrow::array::ArrayRef) -> Vec<Value> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            name,
            array.data_type().clone(),
            true,
        )]));
        record_batch_to_values(&RecordBatch::try_new(schema, vec![array]).unwrap()).unwrap()
    }

    #[test]
    fn binary_columns_render_as_base64() {
        let bytes: &[u8] = &[0, 1, 2, 255];
        for array in [
            Arc::new(BinaryArray::from(vec![Some(bytes), None])) as arrow::array::ArrayRef,
            Arc::new(LargeBinaryArray::from(vec![Some(bytes), None])),
            Arc::new(BinaryViewArray::from(vec![Some(bytes), None])),
            Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    vec![Some(bytes), None].into_iter(),
                    4,
                )
                .unwrap(),
            ),
        ] {
            let rows = one_column("b", array);
            assert_eq!(rows[0]["b"], json!("AAEC/w=="));
            assert_eq!(rows[1]["b"], json!(null));
        }
    }

    #[test]
    fn timestamps_keep_their_unit_and_zone() {
        let micros = 1_758_198_896_123_000i64;
        let rows = one_column(
            "t",
            Arc::new(TimestampMicrosecondArray::from(vec![Some(micros), None])),
        );
        assert_eq!(rows[0]["t"], json!("2025-09-18T12:34:56.123000"));
        assert_eq!(rows[1]["t"], json!(null));
        let rows = one_column(
            "t",
            Arc::new(TimestampMicrosecondArray::from(vec![micros]).with_timezone("+05:30")),
        );
        assert_eq!(rows[0]["t"], json!("2025-09-18T18:04:56.123000+05:30"));
        let rows = one_column(
            "t",
            Arc::new(TimestampMillisecondArray::from(vec![1_500]).with_timezone("+00:00")),
        );
        assert_eq!(rows[0]["t"], json!("1970-01-01T00:00:01.500Z"));
        let rows = one_column("t", Arc::new(TimestampSecondArray::from(vec![-1])));
        assert_eq!(rows[0]["t"], json!("1969-12-31T23:59:59"));
        let rows = one_column("t", Arc::new(TimestampNanosecondArray::from(vec![1])));
        assert_eq!(rows[0]["t"], json!("1970-01-01T00:00:00.000000001"));
        let rows = one_column("t", Arc::new(TimestampSecondArray::from(vec![i64::MAX])));
        assert_eq!(
            rows[0]["t"],
            json!(i64::MAX),
            "out of range keeps the raw value"
        );
    }

    #[test]
    fn an_unparseable_zone_falls_back_to_the_default_encoder() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "t",
            DataType::Timestamp(TimeUnit::Second, Some("Not/AZone".into())),
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(
                TimestampSecondArray::from(vec![0]).with_timezone("Not/AZone"),
            )],
        )
        .unwrap();
        assert!(record_batch_to_values(&batch).is_err());
    }

    #[test]
    fn times_keep_their_unit() {
        let rows = one_column("t", Arc::new(Time32SecondArray::from(vec![3661])));
        assert_eq!(rows[0]["t"], json!("01:01:01"));
        let rows = one_column("t", Arc::new(Time32MillisecondArray::from(vec![1_500])));
        assert_eq!(rows[0]["t"], json!("00:00:01.500"));
        let rows = one_column("t", Arc::new(Time64MicrosecondArray::from(vec![1])));
        assert_eq!(rows[0]["t"], json!("00:00:00.000001"));
        let rows = one_column("t", Arc::new(Time64NanosecondArray::from(vec![7, -1])));
        assert_eq!(rows[0]["t"], json!("00:00:00.000000007"));
        assert_eq!(rows[1]["t"], json!(-1), "out of range keeps the raw value");
    }
}

#[cfg(test)]
mod binary_round_trip_tests {
    use super::*;
    use arrow::datatypes::Fields;
    use serde_json::json;

    #[test]
    fn base64_binary_round_trips_through_a_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Binary, true),
            Field::new("f", DataType::FixedSizeBinary(2), true),
            Field::new(
                "s",
                DataType::Struct(Fields::from(vec![Field::new(
                    "x",
                    DataType::LargeBinary,
                    true,
                )])),
                true,
            ),
            Field::new(
                "l",
                DataType::List(Arc::new(Field::new("item", DataType::Binary, true))),
                true,
            ),
            Field::new("n", DataType::Int64, true),
        ]));
        let rows = vec![
            json!({"b": "AAEC/w==", "f": "AP8=", "s": {"x": "aGk="}, "l": ["AQ==", null], "n": 1}),
            json!({"b": null, "f": null, "s": null, "l": null, "n": null}),
            json!("not an object"),
        ];
        let batch = values_to_record_batch(&rows[..2], schema.clone()).unwrap();
        assert_eq!(record_batch_to_values(&batch).unwrap(), rows[..2].to_vec());
        assert!(base64_binary_to_hex(&rows, &schema).unwrap().is_some());
        let err = values_to_record_batch(&[json!({"b": "zz!"})], schema).unwrap_err();
        assert!(err.to_string().contains("not base64"), "{err}");
        let plain = Schema::new(vec![Field::new("n", DataType::Int64, true)]);
        assert!(base64_binary_to_hex(&rows, &plain).unwrap().is_none());
    }
}
