//! Builds `Value`s straight from Arrow arrays, for the types whose rendering
//! is proven identical to the arrow-json hop; anything else is left to it.

use super::{arrow_precision, time_json, timestamp_json};
use crate::fidelity::{self, TimePrecision};
use arrow::array::timezone::Tz;
use arrow::array::{
    Array, AsArray, BinaryViewArray, BooleanArray, FixedSizeBinaryArray, GenericBinaryArray,
    GenericStringArray, PrimitiveArray, RecordBatch, StringViewArray,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{
    ArrowDictionaryKeyType, ArrowNativeType, ArrowPrimitiveType, DataType, Decimal32Type,
    Decimal64Type, Decimal128Type, Decimal256Type, Float16Type, Float32Type, Float64Type, Int8Type,
    Int16Type, Int32Type, Int64Type, Time32MillisecondType, Time32SecondType,
    Time64MicrosecondType, Time64NanosecondType, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use arrow::util::display::{ArrayFormatter, FormatOptions};
use serde_json::{Map, Value};
use std::collections::HashSet;
use std::fmt::Write as _;

pub(super) const MAX_DEPTH: usize = 32;

pub(super) fn convert(batch: &RecordBatch) -> Option<Vec<Value>> {
    if batch.num_rows() == 0 {
        return Some(Vec::new());
    }
    let schema = batch.schema_ref();
    if !unique(schema.fields().iter().map(|f| f.name().as_str())) {
        return None;
    }
    let columns = schema
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(field, array)| Some((field.name().as_str(), plan(array.as_ref(), 1)?)))
        .collect::<Option<Vec<_>>>()?;
    Some(
        (0..batch.num_rows())
            .map(|row| Value::Object(object(&columns, row)))
            .collect(),
    )
}

fn unique<'a>(names: impl Iterator<Item = &'a str>) -> bool {
    let mut seen = HashSet::new();
    names.into_iter().all(|name| seen.insert(name))
}

fn object(fields: &[(&str, Cell<'_>)], row: usize) -> Map<String, Value> {
    let mut map = Map::with_capacity(fields.len());
    for (name, cell) in fields {
        map.insert((*name).to_owned(), cell.get(row));
    }
    map
}

pub(super) type KeyAt<'a> = Box<dyn Fn(usize) -> usize + 'a>;

struct Cell<'a> {
    nulls: Option<&'a NullBuffer>,
    kind: Kind<'a>,
}

enum Text<'a> {
    Utf8(&'a GenericStringArray<i32>),
    Large(&'a GenericStringArray<i64>),
    View(&'a StringViewArray),
}

impl<'a> Text<'a> {
    fn of(array: &'a dyn Array) -> Option<Self> {
        match array.data_type() {
            DataType::Utf8 => Some(Self::Utf8(array.as_string_opt()?)),
            DataType::LargeUtf8 => Some(Self::Large(array.as_string_opt()?)),
            DataType::Utf8View => Some(Self::View(array.as_string_view_opt()?)),
            _ => None,
        }
    }

    fn at(&self, i: usize) -> &'a str {
        match self {
            Self::Utf8(a) => a.value(i),
            Self::Large(a) => a.value(i),
            Self::View(a) => a.value(i),
        }
    }
}

enum Ranges<'a> {
    Small(&'a [i32]),
    Large(&'a [i64]),
    Fixed(usize),
}

impl Ranges<'_> {
    fn at(&self, i: usize) -> std::ops::Range<usize> {
        match self {
            Self::Small(offsets) => offsets[i].as_usize()..offsets[i + 1].as_usize(),
            Self::Large(offsets) => offsets[i].as_usize()..offsets[i + 1].as_usize(),
            Self::Fixed(len) => i * len..i * len + len,
        }
    }
}

enum Kind<'a> {
    Null,
    Bool(&'a BooleanArray),
    Int8(&'a [i8]),
    Int16(&'a [i16]),
    Int32(&'a [i32]),
    Int64(&'a [i64]),
    UInt8(&'a [u8]),
    UInt16(&'a [u16]),
    UInt32(&'a [u32]),
    UInt64(&'a [u64]),
    Float64(&'a [f64]),
    Float32(&'a [f32]),
    Float16(&'a [<Float16Type as ArrowPrimitiveType>::Native]),
    Text(Text<'a>),
    Binary(&'a GenericBinaryArray<i32>),
    LargeBinary(&'a GenericBinaryArray<i64>),
    BinaryView(&'a BinaryViewArray),
    FixedBinary(&'a FixedSizeBinaryArray),
    Decimal32(&'a PrimitiveArray<Decimal32Type>),
    Decimal64(&'a PrimitiveArray<Decimal64Type>),
    Decimal128(&'a PrimitiveArray<Decimal128Type>),
    Decimal256(&'a PrimitiveArray<Decimal256Type>),
    Timestamp {
        values: &'a [i64],
        precision: TimePrecision,
        zone: Option<Tz>,
    },
    Time32 {
        values: &'a [i32],
        unit: TimeUnit,
    },
    Time64 {
        values: &'a [i64],
        unit: TimeUnit,
    },
    Date(ArrayFormatter<'a>),
    Struct(Vec<(&'a str, Cell<'a>)>),
    List {
        ranges: Ranges<'a>,
        items: Box<Cell<'a>>,
    },
    Map {
        offsets: &'a [i32],
        keys: Text<'a>,
        values: Box<Cell<'a>>,
    },
    Dictionary {
        key: KeyAt<'a>,
        values: Text<'a>,
        value_nulls: Option<&'a NullBuffer>,
    },
}

impl Cell<'_> {
    fn get(&self, i: usize) -> Value {
        if self.nulls.is_some_and(|nulls| nulls.is_null(i)) {
            return Value::Null;
        }
        match &self.kind {
            Kind::Null => Value::Null,
            Kind::Bool(a) => Value::Bool(a.value(i)),
            Kind::Int8(v) => Value::from(v[i]),
            Kind::Int16(v) => Value::from(v[i]),
            Kind::Int32(v) => Value::from(v[i]),
            Kind::Int64(v) => Value::from(v[i]),
            Kind::UInt8(v) => Value::from(v[i]),
            Kind::UInt16(v) => Value::from(v[i]),
            Kind::UInt32(v) => Value::from(v[i]),
            Kind::UInt64(v) => Value::from(v[i]),
            Kind::Float64(v) => fidelity::float64(v[i]),
            Kind::Float32(v) => fidelity::float32(v[i]),
            Kind::Float16(v) => fidelity::float32(f32::from(v[i])),
            Kind::Text(text) => Value::String(text.at(i).to_owned()),
            Kind::Binary(a) => fidelity::binary(a.value(i)),
            Kind::LargeBinary(a) => fidelity::binary(a.value(i)),
            Kind::BinaryView(a) => fidelity::binary(a.value(i)),
            Kind::FixedBinary(a) => fidelity::binary(a.value(i)),
            Kind::Decimal32(a) => Value::String(a.value_as_string(i)),
            Kind::Decimal64(a) => Value::String(a.value_as_string(i)),
            Kind::Decimal128(a) => Value::String(a.value_as_string(i)),
            Kind::Decimal256(a) => Value::String(a.value_as_string(i)),
            Kind::Timestamp {
                values,
                precision,
                zone,
            } => timestamp_json(values[i], *precision, zone.as_ref()),
            Kind::Time32 { values, unit } => time_json(i64::from(values[i]), unit),
            Kind::Time64 { values, unit } => time_json(values[i], unit),
            Kind::Date(formatter) => {
                let mut text = String::new();
                let _ = write!(text, "{}", formatter.value(i));
                Value::String(text)
            }
            Kind::Struct(fields) => Value::Object(object(fields, i)),
            Kind::List { ranges, items } => {
                Value::Array(ranges.at(i).map(|j| items.get(j)).collect())
            }
            Kind::Map {
                offsets,
                keys,
                values,
            } => {
                let entries = offsets[i].as_usize()..offsets[i + 1].as_usize();
                let mut map = Map::with_capacity(entries.len());
                for j in entries {
                    map.insert(keys.at(j).to_owned(), values.get(j));
                }
                Value::Object(map)
            }
            Kind::Dictionary {
                key,
                values,
                value_nulls,
            } => {
                let k = key(i);
                if value_nulls.is_some_and(|nulls| nulls.is_null(k)) {
                    Value::Null
                } else {
                    Value::String(values.at(k).to_owned())
                }
            }
        }
    }
}

fn dictionary_key<'a, K: ArrowDictionaryKeyType>(
    array: &'a dyn Array,
) -> Option<(KeyAt<'a>, &'a dyn Array)> {
    let dictionary = array.as_dictionary_opt::<K>()?;
    let keys: &'a [K::Native] = dictionary.keys().values();
    Some((
        Box::new(move |i| keys[i].as_usize()),
        dictionary.values().as_ref(),
    ))
}

pub(super) fn dictionary_parts(array: &dyn Array) -> Option<(KeyAt<'_>, &dyn Array)> {
    dictionary_key::<Int8Type>(array)
        .or_else(|| dictionary_key::<Int16Type>(array))
        .or_else(|| dictionary_key::<Int32Type>(array))
        .or_else(|| dictionary_key::<Int64Type>(array))
        .or_else(|| dictionary_key::<UInt8Type>(array))
        .or_else(|| dictionary_key::<UInt16Type>(array))
        .or_else(|| dictionary_key::<UInt32Type>(array))
        .or_else(|| dictionary_key::<UInt64Type>(array))
}

fn dictionary(array: &dyn Array) -> Option<Kind<'_>> {
    let (key, values) = dictionary_parts(array)?;
    if matches!(values.data_type(), DataType::Utf8View) {
        return None;
    }
    Some(Kind::Dictionary {
        key,
        values: Text::of(values)?,
        value_nulls: values.nulls(),
    })
}

fn plan(array: &dyn Array, depth: usize) -> Option<Cell<'_>> {
    if depth > MAX_DEPTH {
        return None;
    }
    let kind = match array.data_type() {
        DataType::Null => Kind::Null,
        DataType::Boolean => Kind::Bool(array.as_boolean_opt()?),
        DataType::Int8 => Kind::Int8(array.as_primitive_opt::<Int8Type>()?.values()),
        DataType::Int16 => Kind::Int16(array.as_primitive_opt::<Int16Type>()?.values()),
        DataType::Int32 => Kind::Int32(array.as_primitive_opt::<Int32Type>()?.values()),
        DataType::Int64 => Kind::Int64(array.as_primitive_opt::<Int64Type>()?.values()),
        DataType::UInt8 => Kind::UInt8(array.as_primitive_opt::<UInt8Type>()?.values()),
        DataType::UInt16 => Kind::UInt16(array.as_primitive_opt::<UInt16Type>()?.values()),
        DataType::UInt32 => Kind::UInt32(array.as_primitive_opt::<UInt32Type>()?.values()),
        DataType::UInt64 => Kind::UInt64(array.as_primitive_opt::<UInt64Type>()?.values()),
        DataType::Float64 => Kind::Float64(array.as_primitive_opt::<Float64Type>()?.values()),
        DataType::Float32 => Kind::Float32(array.as_primitive_opt::<Float32Type>()?.values()),
        DataType::Float16 => Kind::Float16(array.as_primitive_opt::<Float16Type>()?.values()),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Kind::Text(Text::of(array)?),
        DataType::Binary => Kind::Binary(array.as_binary_opt()?),
        DataType::LargeBinary => Kind::LargeBinary(array.as_binary_opt()?),
        DataType::BinaryView => Kind::BinaryView(array.as_binary_view_opt()?),
        DataType::FixedSizeBinary(_) => Kind::FixedBinary(array.as_fixed_size_binary_opt()?),
        DataType::Decimal32(..) => Kind::Decimal32(array.as_primitive_opt()?),
        DataType::Decimal64(..) => Kind::Decimal64(array.as_primitive_opt()?),
        DataType::Decimal128(..) => Kind::Decimal128(array.as_primitive_opt()?),
        DataType::Decimal256(..) => Kind::Decimal256(array.as_primitive_opt()?),
        DataType::Timestamp(unit, zone) => {
            let values: &[i64] = match unit {
                TimeUnit::Second => array.as_primitive_opt::<TimestampSecondType>()?.values(),
                TimeUnit::Millisecond => array
                    .as_primitive_opt::<TimestampMillisecondType>()?
                    .values(),
                TimeUnit::Microsecond => array
                    .as_primitive_opt::<TimestampMicrosecondType>()?
                    .values(),
                TimeUnit::Nanosecond => array
                    .as_primitive_opt::<TimestampNanosecondType>()?
                    .values(),
            };
            let zone = match zone {
                Some(name) => Some(name.parse::<Tz>().ok()?),
                None => None,
            };
            Kind::Timestamp {
                values,
                precision: arrow_precision(unit),
                zone,
            }
        }
        DataType::Time32(unit) => {
            let values: &[i32] = match unit {
                TimeUnit::Second => array.as_primitive_opt::<Time32SecondType>()?.values(),
                _ => array.as_primitive_opt::<Time32MillisecondType>()?.values(),
            };
            Kind::Time32 {
                values,
                unit: *unit,
            }
        }
        DataType::Time64(unit) => {
            let values: &[i64] = match unit {
                TimeUnit::Nanosecond => array.as_primitive_opt::<Time64NanosecondType>()?.values(),
                _ => array.as_primitive_opt::<Time64MicrosecondType>()?.values(),
            };
            Kind::Time64 {
                values,
                unit: *unit,
            }
        }
        DataType::Date32 | DataType::Date64 => Kind::Date(date_formatter(array)?),
        DataType::Struct(fields) => {
            let array = array.as_struct_opt()?;
            if !unique(fields.iter().map(|f| f.name().as_str())) {
                return None;
            }
            Kind::Struct(
                fields
                    .iter()
                    .zip(array.columns())
                    .map(|(field, child)| {
                        Some((field.name().as_str(), plan(child.as_ref(), depth + 1)?))
                    })
                    .collect::<Option<Vec<_>>>()?,
            )
        }
        DataType::List(_) => {
            let array = array.as_list_opt::<i32>()?;
            Kind::List {
                ranges: Ranges::Small(array.value_offsets()),
                items: Box::new(plan(array.values().as_ref(), depth + 1)?),
            }
        }
        DataType::LargeList(_) => {
            let array = array.as_list_opt::<i64>()?;
            Kind::List {
                ranges: Ranges::Large(array.value_offsets()),
                items: Box::new(plan(array.values().as_ref(), depth + 1)?),
            }
        }
        DataType::FixedSizeList(..) => {
            let array = array.as_fixed_size_list_opt()?;
            Kind::List {
                ranges: Ranges::Fixed(array.value_length().as_usize()),
                items: Box::new(plan(array.values().as_ref(), depth + 1)?),
            }
        }
        DataType::Map(..) => {
            let array = array.as_map_opt()?;
            if array.keys().null_count() != 0 || array.entries().null_count() != 0 {
                return None;
            }
            Kind::Map {
                offsets: array.value_offsets(),
                keys: Text::of(array.keys().as_ref())?,
                values: Box::new(plan(array.values().as_ref(), depth + 1)?),
            }
        }
        DataType::Dictionary(..) => dictionary(array)?,
        _ => return None,
    };
    Some(Cell {
        nulls: array.nulls(),
        kind,
    })
}

fn date_formatter(array: &dyn Array) -> Option<ArrayFormatter<'_>> {
    ArrayFormatter::try_new(array, &FormatOptions::new().with_display_error(true)).ok()
}

#[cfg(test)]
mod tests;
