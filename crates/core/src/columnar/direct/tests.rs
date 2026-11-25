use super::super::{record_batch_to_values, record_batch_to_values_json};
use super::{MAX_DEPTH, convert};
use crate::fidelity::NonFiniteCounter;
use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array,
    Decimal32Array, Decimal64Array, Decimal128Array, Decimal256Array, DictionaryArray,
    DurationSecondArray, FixedSizeBinaryArray, FixedSizeListArray, Float16Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, IntervalYearMonthArray, LargeBinaryArray,
    LargeListArray, LargeListViewArray, LargeStringArray, ListArray, ListViewArray, MapArray,
    NullArray, PrimitiveArray, RecordBatch, RecordBatchOptions, RunArray, StringArray,
    StringViewArray, StructArray, Time32MillisecondArray, Time32SecondArray,
    Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array, UnionArray, make_array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{
    ArrowDictionaryKeyType, ArrowPrimitiveType, DataType, Field, Fields, Float16Type, Float32Type,
    Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, Schema, TimeUnit, UInt8Type,
    UInt16Type, UInt32Type, UInt64Type, UnionFields, i256,
};
use serde_json::Value;
use std::sync::Arc;

type F16 = <Float16Type as ArrowPrimitiveType>::Native;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

fn batch_of(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
        .collect();
    let arrays = columns.into_iter().map(|(_, array)| array).collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
}

fn counted<R>(f: impl FnOnce() -> R) -> (R, u64) {
    let counter = NonFiniteCounter::new();
    let out = counter.scope_sync(f);
    (out, counter.count())
}

fn numbers(value: &Value, out: &mut Vec<(u8, u64)>) {
    match value {
        Value::Number(n) if n.is_u64() => out.push((0, n.as_u64().unwrap())),
        Value::Number(n) if n.is_i64() => out.push((1, n.as_i64().unwrap() as u64)),
        Value::Number(n) => out.push((2, n.as_f64().unwrap().to_bits())),
        Value::Array(items) => items.iter().for_each(|v| numbers(v, out)),
        Value::Object(map) => map.values().for_each(|v| numbers(v, out)),
        _ => {}
    }
}

fn number_bits(values: &[Value]) -> Vec<(u8, u64)> {
    let mut out = Vec::new();
    values.iter().for_each(|v| numbers(v, &mut out));
    out
}

fn assert_one(batch: &RecordBatch) -> u64 {
    let (direct, direct_count) = counted(|| convert(batch));
    let (json, json_count) = counted(|| record_batch_to_values_json(batch));
    let direct = direct.expect("the direct builder must take this batch");
    let json = json.expect("the JSON hop converts this batch");
    assert_eq!(
        serde_json::to_string(&direct).unwrap(),
        serde_json::to_string(&json).unwrap()
    );
    assert_eq!(direct, json);
    assert_eq!(number_bits(&direct), number_bits(&json));
    assert_eq!(direct_count, json_count);
    let (public, public_count) = counted(|| record_batch_to_values(batch));
    assert_eq!(public.unwrap(), json);
    assert_eq!(public_count, json_count);
    json_count
}

fn assert_same(batch: &RecordBatch) -> u64 {
    let n = batch.num_rows();
    for (start, len) in [(1, n.saturating_sub(1)), (n / 2, n - n / 2), (n / 3, n / 3)] {
        if start + len <= n {
            assert_one(&batch.slice(start, len));
        }
    }
    assert_one(&batch.slice(0, 0));
    assert_one(batch)
}

fn assert_falls_back(batch: &RecordBatch) {
    let (direct, rendered) = counted(|| convert(batch));
    assert!(direct.is_none(), "the direct builder must leave this batch");
    assert_eq!(rendered, 0, "planning renders nothing");
    let (public, public_count) =
        counted(|| record_batch_to_values(batch).map_err(|e| e.to_string()));
    let (json, json_count) =
        counted(|| record_batch_to_values_json(batch).map_err(|e| e.to_string()));
    assert_eq!(public, json);
    assert_eq!(public_count, json_count);
}

fn masked(array: &ArrayRef, null_at: impl Fn(usize) -> bool) -> ArrayRef {
    let valid: Vec<bool> = (0..array.len()).map(|i| !null_at(i)).collect();
    let data = array
        .to_data()
        .into_builder()
        .nulls(Some(NullBuffer::from(valid)))
        .build()
        .unwrap();
    make_array(data)
}

fn offsets(len: usize, start: usize) -> Vec<usize> {
    let mut out = vec![start];
    let mut pos = start;
    for step in [2usize, 0, 3, 1].iter().cycle() {
        if pos + step > len {
            break;
        }
        pos += step;
        out.push(pos);
    }
    out
}

fn item(child: &ArrayRef) -> Arc<Field> {
    Arc::new(Field::new("item", child.data_type().clone(), true))
}

fn list_from(child: &ArrayRef, start: usize) -> ArrayRef {
    let offsets: Vec<i32> = offsets(child.len(), start)
        .into_iter()
        .map(|o| o as i32)
        .collect();
    Arc::new(ListArray::new(
        item(child),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        child.clone(),
        None,
    ))
}

fn list_of(child: &ArrayRef) -> ArrayRef {
    list_from(child, 0)
}

fn large_list_of(child: &ArrayRef) -> ArrayRef {
    let offsets: Vec<i64> = offsets(child.len(), 0)
        .into_iter()
        .map(|o| o as i64)
        .collect();
    Arc::new(LargeListArray::new(
        item(child),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        child.clone(),
        None,
    ))
}

fn fixed_list_of(child: &ArrayRef, size: usize) -> ArrayRef {
    let rows = child.len() / size;
    Arc::new(FixedSizeListArray::new(
        item(child),
        size as i32,
        child.slice(0, rows * size),
        None,
    ))
}

fn struct_of(children: Vec<(&str, ArrayRef)>) -> ArrayRef {
    let fields: Fields = children
        .iter()
        .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
        .collect();
    let arrays = children.into_iter().map(|(_, array)| array).collect();
    Arc::new(StructArray::new(fields, arrays, None))
}

fn map_with(keys: ArrayRef, values: &ArrayRef, key_nullable: bool, offsets: Vec<i32>) -> MapArray {
    let fields = Fields::from(vec![
        Field::new("keys", keys.data_type().clone(), key_nullable),
        Field::new("values", values.data_type().clone(), true),
    ]);
    let entries = StructArray::new(fields, vec![keys, values.clone()], None);
    MapArray::try_new(
        Arc::new(Field::new("entries", entries.data_type().clone(), false)),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        entries,
        None,
        false,
    )
    .unwrap()
}

fn map_of(values: &ArrayRef) -> ArrayRef {
    let keys: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..values.len()).map(|i| format!("k{}", i % 3)),
    ));
    let offsets = offsets(values.len(), 0)
        .into_iter()
        .map(|o| o as i32)
        .collect();
    Arc::new(map_with(keys, values, false, offsets))
}

fn assert_leaf(array: ArrayRef) -> u64 {
    let some = masked(&array, |i| i % 3 == 1);
    let all = masked(&array, |_| true);
    let count = assert_same(&batch_of(vec![
        ("v", array.clone()),
        ("some", some.clone()),
        ("all", all.clone()),
    ]));
    for nested in [
        list_of(&array),
        list_of(&some),
        list_from(&some, 1),
        masked(&list_of(&array), |i| i % 2 == 0),
        large_list_of(&some),
        fixed_list_of(&some, 2),
        struct_of(vec![("a", array.clone()), ("b", some.clone())]),
        masked(&struct_of(vec![("a", array.clone())]), |i| i % 2 == 0),
        map_of(&array),
        map_of(&some),
        list_of(&struct_of(vec![("a", some.clone()), ("m", all.clone())])),
        struct_of(vec![("l", fixed_list_of(&some, 1)), ("x", array.clone())]),
    ] {
        assert_same(&batch_of(vec![("n", nested)]));
    }
    count
}

fn primitive<T: ArrowPrimitiveType>(values: Vec<T::Native>) -> ArrayRef {
    Arc::new(PrimitiveArray::<T>::from_iter_values(values))
}

#[test]
fn integers_and_booleans_match_the_json_hop() {
    let mut rng = Rng(1);
    let mut random = move || (0..200).map(|_| rng.next()).collect::<Vec<u64>>();
    macro_rules! ints {
        ($($array:ident: $native:ty),*) => {$(
            let mut values: Vec<$native> = vec![<$native>::MIN, <$native>::MAX, 0, 1];
            values.push((0 as $native).wrapping_sub(1));
            values.extend(random().into_iter().map(|v| v as $native));
            assert_eq!(assert_leaf(Arc::new($array::from(values))), 0);
        )*};
    }
    ints!(
        Int8Array: i8,
        Int16Array: i16,
        Int32Array: i32,
        Int64Array: i64,
        UInt8Array: u8,
        UInt16Array: u16,
        UInt32Array: u32,
        UInt64Array: u64
    );
    assert_leaf(Arc::new(BooleanArray::from(vec![
        true, false, false, true, true, false, true,
    ])));
}

const F64_EDGES: &[f64] = &[
    0.0,
    -0.0,
    0.1,
    1.0,
    -1.0,
    1.5,
    1e-7,
    1e15,
    1e16,
    1e17,
    1e21,
    1e22,
    1e23,
    5e-324,
    f64::MIN_POSITIVE,
    2.225_073_858_507_201e-308,
    f64::MAX,
    f64::MIN,
    f64::EPSILON,
    9_007_199_254_740_991.0,
    9_007_199_254_740_992.0,
    9_007_199_254_740_993.0,
    -9_007_199_254_740_993.0,
    123_456_789_012_345_680.0,
    0.300_000_000_000_000_04,
    18_446_744_073_709_551_615.0,
    -9_223_372_036_854_775_808.0,
    1e300,
    1e-300,
];

const F32_EDGES: &[f32] = &[
    0.0,
    -0.0,
    0.1,
    1.0,
    -1.0,
    1e-7,
    1e21,
    3.0e38,
    1e-45,
    f32::MIN_POSITIVE,
    1.175_494_2e-38,
    f32::MAX,
    f32::MIN,
    f32::EPSILON,
    16_777_216.0,
    16_777_217.0,
    0.3,
    1e10,
    1e-10,
];

#[test]
fn float64_edges_match_the_json_hop() {
    assert_eq!(assert_leaf(primitive::<Float64Type>(F64_EDGES.to_vec())), 0);
    let mut with_non_finite = F64_EDGES.to_vec();
    with_non_finite.extend([f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -f64::NAN]);
    assert!(assert_leaf(primitive::<Float64Type>(with_non_finite)) > 0);
}

fn one_row_list(child: ArrayRef) -> RecordBatch {
    let len = child.len();
    let list = ListArray::new(
        item(&child),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, len as i32])),
        child,
        None,
    );
    batch_of(vec![("v", Arc::new(list))])
}

fn random_floats<T: ArrowPrimitiveType>(
    seed: u64,
    total: usize,
    from_bits: impl Fn(u64) -> T::Native,
    is_finite: impl Fn(&T::Native) -> bool,
    filler: T::Native,
) {
    const CHUNK: usize = 1 << 18;
    let mut rng = Rng(seed);
    let mut done = 0;
    let mut raw = true;
    while done < total {
        let len = CHUNK.min(total - done);
        let values: Vec<T::Native> = (0..len)
            .map(|_| {
                let v = from_bits(rng.next());
                if raw || is_finite(&v) { v } else { filler }
            })
            .collect();
        let non_finite = values.iter().filter(|v| !is_finite(v)).count() as u64;
        let array = primitive::<T>(values);
        if done < 2 * CHUNK {
            assert_same(&batch_of(vec![("v", array.slice(0, 1 << 12))]));
        }
        assert_eq!(assert_one(&one_row_list(array)), non_finite);
        done += len;
        raw = !raw;
    }
}

fn random_f64(seed: u64, total: usize) {
    random_floats::<Float64Type>(seed, total, f64::from_bits, |v| v.is_finite(), 1.0 / 3.0);
}

#[test]
fn random_float64_bit_patterns_match_the_json_hop() {
    random_f64(7, 1 << 20);
}

#[test]
#[ignore = "10M random f64 bit patterns; run in release"]
fn ten_million_random_float64_bit_patterns_match_the_json_hop() {
    random_f64(11, 10_000_000);
}

fn single_float_mismatches(values: &[f32]) -> u64 {
    let batch = one_row_list(primitive::<Float32Type>(values.to_vec()));
    let json = record_batch_to_values_json(&batch).unwrap();
    let rendered = json[0]["v"].as_array().unwrap();
    values
        .iter()
        .zip(rendered)
        .filter(|(v, json)| crate::fidelity::float32(**v) != **json)
        .count() as u64
}

#[test]
fn single_and_half_floats_take_the_json_hop() {
    let mut with_non_finite = F32_EDGES.to_vec();
    with_non_finite.extend([f32::NAN, f32::INFINITY, f32::NEG_INFINITY]);
    for values in [F32_EDGES.to_vec(), with_non_finite] {
        let array = primitive::<Float32Type>(values);
        assert_falls_back(&batch_of(vec![("v", array.clone())]));
        assert_falls_back(&batch_of(vec![("v", list_of(&array))]));
        assert_falls_back(&batch_of(vec![("v", struct_of(vec![("a", array)]))]));
    }
    let halves: Vec<F16> = [0.0f32, -0.0, 0.1, 1.5, 65504.0, f32::NAN, f32::INFINITY]
        .into_iter()
        .map(F16::from_f32)
        .collect();
    let halves: ArrayRef = Arc::new(Float16Array::from(halves));
    assert_falls_back(&batch_of(vec![("v", halves)]));
}

#[test]
fn the_json_hop_renders_some_single_floats_unlike_the_fidelity_rendering() {
    let witnesses = [f32::from_bits(0x3980_0000), f32::from_bits(0x4c52_56d4)];
    assert_eq!(single_float_mismatches(&witnesses), 2);
    let batch = batch_of(vec![("v", primitive::<Float32Type>(witnesses.to_vec()))]);
    let json = record_batch_to_values_json(&batch).unwrap();
    assert_eq!(
        serde_json::to_string(&json).unwrap(),
        r#"[{"v":0.00024414062},{"v":55139152.0}]"#
    );
    let fidelity: Vec<String> = witnesses
        .iter()
        .map(|v| crate::fidelity::float32(*v).to_string())
        .collect();
    assert_eq!(fidelity, ["0.00024414063", "55139150.0"]);
}

#[test]
#[ignore = "all 2^32 f32 bit patterns; run in release"]
fn count_single_floats_the_json_hop_renders_unlike_the_fidelity_rendering() {
    use std::sync::atomic::{AtomicU64, Ordering};
    const CHUNK: u64 = 1 << 20;
    const CHUNKS: u64 = (1 << 32) / CHUNK;
    let next = AtomicU64::new(0);
    let mismatches = AtomicU64::new(0);
    let finite_values = AtomicU64::new(0);
    let workers = std::thread::available_parallelism().map_or(4, usize::from);
    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| {
                loop {
                    let chunk = next.fetch_add(1, Ordering::Relaxed);
                    if chunk >= CHUNKS {
                        break;
                    }
                    let start = chunk * CHUNK;
                    let floats: Vec<f32> = (start..start + CHUNK)
                        .map(|bits| f32::from_bits(bits as u32))
                        .collect();
                    let finite = floats.iter().filter(|v| v.is_finite()).count() as u64;
                    assert!(finite == 0 || finite == CHUNK, "chunk {chunk} is mixed");
                    if finite == CHUNK {
                        mismatches.fetch_add(single_float_mismatches(&floats), Ordering::Relaxed);
                        finite_values.fetch_add(CHUNK, Ordering::Relaxed);
                    }
                }
            });
        }
    });
    let mismatches = mismatches.load(Ordering::Relaxed);
    let finite_values = finite_values.load(Ordering::Relaxed);
    eprintln!("{mismatches} of {finite_values} finite f32 bit patterns render differently");
    assert_eq!(finite_values, (1 << 32) - (1 << 24));
    assert!(mismatches > 0);
}

fn nasty_strings() -> Vec<String> {
    let controls: String = (0u8..0x20).map(char::from).chain(['\u{7f}']).collect();
    vec![
        String::new(),
        "plain".into(),
        "\"quoted\"".into(),
        "back\\slash \\n \\u0041".into(),
        controls,
        "\u{2028}\u{2029}\u{feff}".into(),
        "😀𝄞 é ñ 漢字".into(),
        "</script>".into(),
        "a".repeat(100_000),
        "{\"looks\":\"like json\"}".into(),
        "null".into(),
        "1e5".into(),
    ]
}

#[test]
fn strings_match_the_json_hop() {
    let strings = nasty_strings();
    assert_leaf(Arc::new(StringArray::from_iter_values(&strings)));
    assert_leaf(Arc::new(LargeStringArray::from_iter_values(&strings)));
    assert_leaf(Arc::new(StringViewArray::from_iter_values(&strings)));
}

#[test]
fn binary_matches_the_json_hop() {
    let every_byte: Vec<u8> = (0..=u8::MAX).collect();
    let values: Vec<&[u8]> = vec![b"", b"a", b"ab", b"abc", &every_byte, &[0, 0, 0], &[255]];
    assert_leaf(Arc::new(BinaryArray::from_iter_values(&values)));
    assert_leaf(Arc::new(LargeBinaryArray::from_iter_values(&values)));
    assert_leaf(Arc::new(BinaryViewArray::from_iter_values(&values)));
    let fixed: Vec<[u8; 4]> = vec![[0, 1, 2, 255], [255; 4], [0; 4], *b"abcd", *b"wxyz"];
    assert_leaf(Arc::new(
        FixedSizeBinaryArray::try_from_iter(fixed.into_iter()).unwrap(),
    ));
}

#[test]
fn decimals_match_the_json_hop() {
    for (precision, scale) in [(9, 2), (9, -3), (5, 5), (1, 0), (9, 9)] {
        let values = vec![
            999_999_999,
            -999_999_999,
            0,
            5,
            -5,
            12_345,
            i32::MAX,
            i32::MIN,
        ];
        assert_leaf(Arc::new(
            Decimal32Array::from(values)
                .with_precision_and_scale(precision, scale)
                .unwrap(),
        ));
    }
    for (precision, scale) in [(18, 4), (18, -2), (10, 10), (1, 0)] {
        let max = 999_999_999_999_999_999i64;
        let values = vec![max, -max, 0, 5, -5, 12_345, i64::MAX, i64::MIN];
        assert_leaf(Arc::new(
            Decimal64Array::from(values)
                .with_precision_and_scale(precision, scale)
                .unwrap(),
        ));
    }
    for (precision, scale) in [(38, 10), (38, -5), (38, 38), (1, 0), (20, 2)] {
        let max = 10i128.pow(38) - 1;
        let values = vec![
            max,
            -max,
            0,
            5,
            -5,
            1_234_567_890_123_456_789_012,
            i128::MAX,
            i128::MIN,
        ];
        assert_leaf(Arc::new(
            Decimal128Array::from(values)
                .with_precision_and_scale(precision, scale)
                .unwrap(),
        ));
    }
    for (precision, scale) in [(76, 20), (76, -10), (76, 76), (1, 0)] {
        let max = i256::from_string(&"9".repeat(76)).unwrap();
        let values = vec![
            max,
            max.wrapping_neg(),
            i256::ZERO,
            i256::from_i128(5),
            i256::from_i128(-5),
            i256::MAX,
            i256::MIN,
        ];
        assert_leaf(Arc::new(
            Decimal256Array::from(values)
                .with_precision_and_scale(precision, scale)
                .unwrap(),
        ));
    }
}

#[test]
fn timestamps_match_the_json_hop() {
    for zone in [
        None,
        Some("+05:30"),
        Some("UTC"),
        Some("America/New_York"),
        Some("-08:00"),
    ] {
        let zone = zone.map(Arc::<str>::from);
        for per_second in [1i64, 1_000, 1_000_000, 1_000_000_000] {
            let mut values = vec![i64::MIN, i64::MAX, 0, -1, 1, 999, -999, 1_700_000_000];
            for seconds in [
                1_700_000_000i64,
                253_402_300_800,
                -62_198_755_200,
                951_782_400,
            ] {
                let scaled = seconds.saturating_mul(per_second);
                values.push(scaled);
                values.push(scaled.saturating_add(123_456_789));
            }
            let array: ArrayRef = match per_second {
                1 => Arc::new(TimestampSecondArray::from(values).with_timezone_opt(zone.clone())),
                1_000 => Arc::new(
                    TimestampMillisecondArray::from(values).with_timezone_opt(zone.clone()),
                ),
                1_000_000 => Arc::new(
                    TimestampMicrosecondArray::from(values).with_timezone_opt(zone.clone()),
                ),
                _ => {
                    Arc::new(TimestampNanosecondArray::from(values).with_timezone_opt(zone.clone()))
                }
            };
            assert_leaf(array);
        }
    }
}

#[test]
fn times_and_dates_match_the_json_hop() {
    let small = vec![
        -1,
        0,
        1,
        59,
        86_399,
        86_400,
        86_401,
        100_000,
        i32::MIN,
        i32::MAX,
    ];
    assert_leaf(Arc::new(Time32SecondArray::from(small.clone())));
    let mut millis = small.clone();
    millis.extend([86_399_999, 86_400_000, 43_200_123]);
    assert_leaf(Arc::new(Time32MillisecondArray::from(millis)));
    let wide = vec![
        -1,
        0,
        1,
        86_399_999_999,
        86_400_000_000,
        86_399_999_999_999,
        86_400_000_000_000,
        43_200_123_456,
        i64::MIN,
        i64::MAX,
    ];
    assert_leaf(Arc::new(Time64MicrosecondArray::from(wide.clone())));
    assert_leaf(Arc::new(Time64NanosecondArray::from(wide)));
    let mut days = small;
    days.extend([
        19_723,
        -719_163,
        -719_529,
        2_932_896,
        2_932_897,
        -1_000_000_000,
    ]);
    assert_leaf(Arc::new(Date32Array::from(days)));
    assert_leaf(Arc::new(Date64Array::from(vec![
        i64::MIN,
        i64::MAX,
        0,
        -1,
        1,
        86_400_000,
        86_399_999,
        1_700_000_000_123,
        253_402_300_800_000,
        -62_198_755_200_000,
        -62_198_755_200_001,
    ])));
}

fn dictionaries<K: ArrowDictionaryKeyType>(keys: Vec<Option<K::Native>>) -> Vec<ArrayRef> {
    let words = ["", "a", "b \"quoted\"", "😀"];
    let keys: PrimitiveArray<K> = keys.into_iter().collect();
    vec![
        Arc::new(DictionaryArray::new(
            keys.clone(),
            Arc::new(StringArray::from_iter_values(words)),
        )),
        Arc::new(DictionaryArray::new(
            keys,
            Arc::new(LargeStringArray::from_iter_values(words)),
        )),
    ]
}

#[test]
fn dictionaries_match_the_json_hop() {
    macro_rules! keyed {
        ($($key:ty),*) => {$(
            for array in dictionaries::<$key>(vec![
                Some(0), None, Some(3), Some(1), Some(2), Some(3), Some(3), None, Some(0),
            ]) {
                assert_leaf(array);
            }
        )*};
    }
    keyed!(
        Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type
    );
}

#[test]
fn null_columns_match_the_json_hop() {
    let nulls: ArrayRef = Arc::new(NullArray::new(7));
    assert_same(&batch_of(vec![("n", nulls.clone())]));
    for nested in [
        list_of(&nulls),
        fixed_list_of(&nulls, 2),
        struct_of(vec![("a", nulls.clone())]),
        map_of(&nulls),
    ] {
        assert_same(&batch_of(vec![("n", nested)]));
    }
}

fn mixed(rows: usize) -> RecordBatch {
    let ints: ArrayRef = Arc::new(Int64Array::from_iter_values(
        (0..rows as i64).map(|i| i * 7),
    ));
    let floats: ArrayRef = Arc::new(Float64Array::from_iter_values(
        (0..rows).map(|i| i as f64 * 0.37 - 5.0),
    ));
    let text: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..rows).map(|i| format!("customer-{i}")),
    ));
    let flags: ArrayRef = Arc::new(BooleanArray::from_iter((0..rows).map(|i| Some(i % 3 == 0))));
    let stamps: ArrayRef = Arc::new(
        TimestampMicrosecondArray::from_iter_values(
            (0..rows as i64).map(|i| 1_700_000_000_000_000 + i * 1_000_003),
        )
        .with_timezone("UTC"),
    );
    let tags: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..rows * 2).map(|i| format!("tag{}", i % 17)),
    ));
    let tag_lists: ArrayRef = Arc::new(ListArray::new(
        item(&tags),
        OffsetBuffer::from_lengths(std::iter::repeat_n(2, rows)),
        tags,
        None,
    ));
    let owner = struct_of(vec![
        ("id", masked(&ints, |i| i % 11 == 0)),
        ("name", text.clone()),
    ]);
    batch_of(vec![
        ("id", ints),
        ("amount", masked(&floats, |i| i % 13 == 0)),
        ("name", text),
        ("active", flags),
        ("at", stamps),
        ("tags", tag_lists),
        ("owner", owner),
    ])
}

#[test]
fn nested_shapes_match_the_json_hop() {
    assert_same(&mixed(257));
    let floats: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(1.5),
        None,
        Some(f64::NAN),
        Some(-0.0),
        Some(2.5),
        Some(f64::INFINITY),
        Some(7.0),
        None,
        Some(9.25),
        Some(0.1),
        Some(3.0),
        Some(4.0),
    ]));
    let inner = struct_of(vec![("l", list_of(&floats)), ("m", map_of(&floats))]);
    let inner = masked(&inner, |i| i == 2);
    let outer = list_of(&inner);
    let deep = map_of(&large_list_of(&struct_of(vec![
        ("x", outer.clone()),
        ("y", masked(&outer, |i| i == 1)),
    ])));
    for array in [inner, outer, deep] {
        assert_same(&batch_of(vec![("n", array)]));
    }
}

#[test]
fn non_finite_values_are_counted_only_where_rendered() {
    let floats: ArrayRef = Arc::new(Float64Array::from(vec![f64::NAN, 1.0, 2.0]));
    let under_null_struct = masked(&struct_of(vec![("f", floats.clone())]), |i| i == 0);
    assert_eq!(assert_same(&batch_of(vec![("s", under_null_struct)])), 0);

    let under_null_value = masked(&floats, |i| i == 0);
    assert_eq!(assert_same(&batch_of(vec![("f", under_null_value)])), 0);

    let under_null_list: ArrayRef = Arc::new(ListArray::new(
        item(&floats),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 2, 3])),
        floats.clone(),
        Some(NullBuffer::from(vec![false, true, true])),
    ));
    assert_eq!(assert_same(&batch_of(vec![("l", under_null_list)])), 0);

    let unreferenced: ArrayRef = Arc::new(ListArray::new(
        item(&floats),
        OffsetBuffer::new(ScalarBuffer::from(vec![1, 2, 3])),
        floats.clone(),
        None,
    ));
    assert_eq!(assert_same(&batch_of(vec![("l", unreferenced)])), 0);

    assert_eq!(assert_same(&batch_of(vec![("l", list_of(&floats))])), 1);

    let values: ArrayRef = Arc::new(Float64Array::from(vec![f64::NAN, 1.0, f64::INFINITY, 2.0]));
    let keys: ArrayRef = Arc::new(StringArray::from(vec!["a", "a", "b", "a"]));
    let overwritten: ArrayRef = Arc::new(map_with(keys, &values, false, vec![0, 2, 4]));
    assert_eq!(assert_same(&batch_of(vec![("m", overwritten)])), 2);
}

#[test]
fn a_non_finite_dictionary_value_read_by_several_rows_takes_the_json_hop() {
    let shared: ArrayRef = Arc::new(DictionaryArray::new(
        Int32Array::from(vec![0, 0, 1, 0]),
        Arc::new(Float64Array::from(vec![f64::NAN, 1.0])),
    ));
    let floats: ArrayRef = Arc::new(Float64Array::from(vec![f64::NAN, 1.0, 2.0, 3.0]));
    let batch = batch_of(vec![("f", floats), ("d", shared)]);
    assert_falls_back(&batch);
    let (values, count) = counted(|| record_batch_to_values(&batch).unwrap());
    assert_eq!(values.len(), 4);
    assert_eq!(count, 4);
}

#[test]
fn empty_batches_match_the_json_hop() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("d", DataType::Duration(TimeUnit::Second), true),
    ]));
    let empty = RecordBatch::new_empty(schema);
    assert_eq!(convert(&empty), Some(Vec::new()));
    assert_eq!(
        record_batch_to_values_json(&empty).unwrap(),
        Vec::<Value>::new()
    );

    let no_columns = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(3)),
    )
    .unwrap();
    assert_same(&no_columns);
    assert_eq!(convert(&no_columns).unwrap().len(), 3);
}

fn nested_lists(levels: usize) -> ArrayRef {
    let mut array: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
    for _ in 0..levels {
        let len = array.len() as i32;
        array = Arc::new(ListArray::new(
            item(&array),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, len])),
            array,
            None,
        ));
    }
    array
}

#[test]
fn nesting_past_the_limit_takes_the_json_hop() {
    assert_same(&batch_of(vec![("n", nested_lists(MAX_DEPTH - 1))]));
    assert_falls_back(&batch_of(vec![("n", nested_lists(MAX_DEPTH))]));
}

#[test]
fn unsupported_types_take_the_json_hop() {
    let ints: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));
    let durations: ArrayRef = Arc::new(DurationSecondArray::from(vec![1, 2, 3, 4]));
    let run_ends = RunArray::try_new(
        &Int32Array::from(vec![2, 4]),
        &StringArray::from(vec!["a", "b"]),
    )
    .unwrap();
    let union = UnionArray::try_new(
        UnionFields::try_new(vec![0], vec![Field::new("i", DataType::Int32, true)]).unwrap(),
        ScalarBuffer::from(vec![0i8, 0, 0, 0]),
        None,
        vec![ints.clone()],
    )
    .unwrap();
    let list_view = ListViewArray::new(
        item(&ints),
        ScalarBuffer::from(vec![0, 1, 2, 3]),
        ScalarBuffer::from(vec![1, 1, 1, 1]),
        ints.clone(),
        None,
    );
    let large_list_view = LargeListViewArray::new(
        item(&ints),
        ScalarBuffer::from(vec![0i64, 1, 2, 3]),
        ScalarBuffer::from(vec![1i64, 1, 1, 1]),
        ints.clone(),
        None,
    );
    let dictionary_of_ints =
        DictionaryArray::new(Int8Array::from(vec![0, 1, 0, 1]), ints.slice(0, 2));
    let dictionary_of_views = DictionaryArray::new(
        Int8Array::from(vec![0, 1, 0, 1]),
        Arc::new(StringViewArray::from(vec!["a", "b"])),
    );
    let dictionary_with_null_values = DictionaryArray::new(
        Int8Array::from(vec![0, 1, 0, 1]),
        Arc::new(StringArray::from(vec![Some("a"), None])),
    );
    let bad_zone = TimestampSecondArray::from(vec![1, 2, 3, 4]).with_timezone("Not/AZone");
    let integer_keys = map_with(ints.clone(), &ints, false, vec![0, 1, 2, 3, 4]);
    let null_keys = map_with(
        Arc::new(StringArray::from(vec![
            Some("a"),
            None,
            Some("c"),
            Some("d"),
        ])),
        &ints,
        true,
        vec![0, 1, 2, 3, 4],
    );
    let duplicate_fields = StructArray::new(
        Fields::from(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("a", DataType::Int32, true),
        ]),
        vec![ints.clone(), ints.clone()],
        None,
    );
    let unsupported: Vec<ArrayRef> = vec![
        durations.clone(),
        Arc::new(IntervalYearMonthArray::from(vec![1, 2, 3, 4])),
        Arc::new(run_ends),
        Arc::new(union),
        Arc::new(list_view),
        Arc::new(large_list_view),
        Arc::new(dictionary_of_ints),
        Arc::new(dictionary_of_views),
        Arc::new(dictionary_with_null_values),
        Arc::new(bad_zone),
        Arc::new(integer_keys),
        Arc::new(null_keys),
        Arc::new(duplicate_fields),
        struct_of(vec![("a", ints.clone()), ("d", durations.clone())]),
        list_of(&durations),
        large_list_of(&durations),
        fixed_list_of(&durations, 2),
        map_of(&durations),
    ];
    for array in unsupported {
        let floats: ArrayRef = Arc::new(Float64Array::from(vec![f64::NAN; array.len()]));
        assert_falls_back(&batch_of(vec![("f", floats), ("u", array)]));
    }
}

#[test]
fn duplicate_root_columns_take_the_json_hop() {
    let ints: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
    let other: ArrayRef = Arc::new(Int32Array::from(vec![3, 4]));
    assert_falls_back(&batch_of(vec![("a", ints), ("a", other)]));
}

#[test]
#[ignore = "timing; run in release with --nocapture"]
fn timing_of_both_paths() {
    let batch = mixed(100_000);
    let median = |f: &dyn Fn() -> usize| {
        let mut runs: Vec<std::time::Duration> = (0..5)
            .map(|_| {
                let start = std::time::Instant::now();
                assert_eq!(f(), 100_000);
                start.elapsed()
            })
            .collect();
        runs.sort();
        runs[2]
    };
    let json = median(&|| record_batch_to_values_json(&batch).unwrap().len());
    let direct = median(&|| convert(&batch).unwrap().len());
    eprintln!(
        "100k rows: json hop {json:?}, direct {direct:?}, ratio {:.2}x",
        json.as_secs_f64() / direct.as_secs_f64()
    );
}
