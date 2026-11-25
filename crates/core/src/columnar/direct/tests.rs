use super::super::{record_batch_to_values, record_batch_to_values_json};
use super::{MAX_DEPTH, convert};
use crate::fidelity::NonFiniteCounter;
use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array,
    Decimal32Array, Decimal64Array, Decimal128Array, Decimal256Array, DictionaryArray,
    DurationSecondArray, FixedSizeBinaryArray, FixedSizeListArray, Float16Array, Float32Array,
    Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, IntervalYearMonthArray,
    LargeBinaryArray, LargeListArray, LargeListViewArray, LargeStringArray, ListArray,
    ListViewArray, MapArray, NullArray, PrimitiveArray, RecordBatch, RecordBatchOptions, RunArray,
    StringArray, StringViewArray, StructArray, Time32MillisecondArray, Time32SecondArray,
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

fn half_set() -> Vec<F16> {
    [
        0.0f32,
        -0.0,
        0.1,
        1.5,
        65504.0,
        -65504.0,
        6.0e-8,
        f32::NAN,
        f32::INFINITY,
    ]
    .into_iter()
    .map(F16::from_f32)
    .collect()
}

#[test]
fn single_and_half_floats_match_the_json_hop() {
    assert_eq!(assert_leaf(primitive::<Float32Type>(F32_EDGES.to_vec())), 0);
    let mut with_non_finite = F32_EDGES.to_vec();
    with_non_finite.extend([f32::NAN, f32::INFINITY, f32::NEG_INFINITY]);
    assert!(assert_leaf(primitive::<Float32Type>(with_non_finite)) > 0);
    let finite_halves: Vec<F16> = half_set().into_iter().filter(|h| h.is_finite()).collect();
    assert_eq!(assert_leaf(Arc::new(Float16Array::from(finite_halves))), 0);
    assert!(assert_leaf(Arc::new(Float16Array::from(half_set()))) > 0);
}

#[test]
fn single_floats_render_their_shortest_form_with_or_without_a_non_finite_neighbour() {
    let witnesses = vec![f32::from_bits(0x3980_0000), f32::from_bits(0x4c52_56d4)];
    let expected = r#"[{"v":0.00024414063},{"v":55139150.0}]"#;
    let plain = batch_of(vec![("v", primitive::<Float32Type>(witnesses.clone()))]);
    for values in [
        record_batch_to_values(&plain).unwrap(),
        record_batch_to_values_json(&plain).unwrap(),
    ] {
        assert_eq!(serde_json::to_string(&values).unwrap(), expected);
    }
    let mut with_nan = witnesses.clone();
    with_nan.push(f32::NAN);
    let in_array = batch_of(vec![("v", primitive::<Float32Type>(with_nan))]);
    let sibling: ArrayRef = Arc::new(Float32Array::from(vec![f32::NAN, 1.0]));
    let beside = batch_of(vec![
        ("v", primitive::<Float32Type>(witnesses)),
        ("n", sibling),
    ]);
    for render in [record_batch_to_values, record_batch_to_values_json] {
        assert_eq!(
            serde_json::to_string(&render(&in_array).unwrap()).unwrap(),
            r#"[{"v":0.00024414063},{"v":55139150.0},{"v":null}]"#
        );
        assert_eq!(
            render(&beside).unwrap(),
            serde_json::from_str::<Vec<Value>>(
                r#"[{"v":0.00024414063,"n":null},{"v":55139150.0,"n":1.0}]"#
            )
            .unwrap()
        );
    }
}

fn random_f32(seed: u64, total: usize) {
    random_floats::<Float32Type>(
        seed,
        total,
        |bits| f32::from_bits(bits as u32),
        |v| v.is_finite(),
        1.0 / 3.0,
    );
}

#[test]
fn random_float32_bit_patterns_match_the_json_hop() {
    random_f32(13, 1 << 20);
}

fn rendered_numbers(values: &[Value]) -> (Vec<f64>, Vec<String>) {
    let items = values[0]["v"].as_array().unwrap();
    let numbers = items.iter().map(|v| v.as_f64().unwrap()).collect();
    let text = serde_json::to_string(&values[0]["v"]).unwrap();
    let texts = text[1..text.len() - 1]
        .split(',')
        .map(str::to_owned)
        .collect();
    (numbers, texts)
}

fn mix(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[test]
fn every_half_float_renders_its_widened_value_on_both_paths() {
    let halves: Vec<F16> = (0..=u16::MAX)
        .map(F16::from_bits)
        .filter(|h| h.is_finite())
        .collect();
    assert_eq!(halves.len(), 65_536 - 2 * 1024);
    let batch = one_row_list(Arc::new(Float16Array::from(halves.clone())));
    assert_eq!(assert_one(&batch), 0);
    let (numbers, texts) = rendered_numbers(&record_batch_to_values(&batch).unwrap());
    for (i, h) in halves.iter().enumerate() {
        let widened = f32::from(*h);
        assert_eq!((numbers[i] as f32).to_bits(), widened.to_bits(), "{h:?}");
        assert_eq!(
            texts[i].parse::<f32>().unwrap().to_bits(),
            widened.to_bits()
        );
        assert_eq!(F16::from_f32(numbers[i] as f32).to_bits(), h.to_bits());
        let previous = halves.get(i.wrapping_sub(1));
        if let Some(p) = previous.filter(|p| p.to_bits() >> 15 == h.to_bits() >> 15) {
            let ordered = if h.to_bits() >> 15 == 0 {
                numbers[i - 1] < numbers[i]
            } else {
                numbers[i - 1] > numbers[i]
            };
            assert!(
                ordered || (f32::from(*p) == 0.0 && widened != 0.0),
                "{p:?} {h:?}"
            );
        }
    }
}

const D3_EXCEPTIONS: [u32; 2] = [0x15ae_43fd, 0x95ae_43fd];

#[test]
#[ignore = "all 2^32 f32 bit patterns; run in release"]
fn every_single_float_renders_one_canonical_value_on_every_path() {
    use arrow::array::AsArray;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, Ordering};
    const CHUNK: u64 = 1 << 20;
    const CHUNKS: u64 = (1 << 32) / CHUNK;
    let next = AtomicU64::new(0);
    let finite_values = AtomicU64::new(0);
    let checksum = AtomicU64::new(0);
    let unlike_display = Mutex::new(Vec::new());
    let list_schema = Arc::new(Schema::new(vec![Field::new(
        "v",
        DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
        true,
    )]));
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
                    let first = f32::from_bits(start as u32);
                    if !first.is_finite() {
                        assert!(
                            (start..start + CHUNK).all(|b| !f32::from_bits(b as u32).is_finite())
                        );
                        continue;
                    }
                    let end = start + CHUNK;
                    let extra = end < (1 << 32) && f32::from_bits(end as u32).is_finite();
                    let last = if extra { end + 1 } else { end };
                    let floats: Vec<f32> =
                        (start..last).map(|b| f32::from_bits(b as u32)).collect();
                    assert!(
                        floats.iter().all(|v| v.is_finite()),
                        "chunk {chunk} is mixed"
                    );
                    let batch = one_row_list(primitive::<Float32Type>(floats.clone()));
                    let direct = convert(&batch).expect("single floats are built directly");
                    let hop = record_batch_to_values_json(&batch).unwrap();
                    assert_eq!(number_bits(&direct), number_bits(&hop), "chunk {chunk}");
                    let (numbers, texts) = rendered_numbers(&direct);
                    let read_back =
                        crate::columnar::values_to_record_batch(&direct, list_schema.clone())
                            .unwrap();
                    let read_back = read_back.column(0).as_list::<i32>().values().clone();
                    let read_back = read_back.as_primitive::<Float32Type>();
                    let mut sum = 0u64;
                    for i in 0..(end - start) as usize {
                        let bits = floats[i].to_bits();
                        let r = numbers[i];
                        assert_eq!((r as f32).to_bits(), bits, "cast {bits:#010x}");
                        assert_eq!(
                            texts[i].parse::<f32>().unwrap().to_bits(),
                            bits,
                            "text {bits:#010x}"
                        );
                        assert_eq!(read_back.value(i).to_bits(), bits, "reader {bits:#010x}");
                        let next_r = numbers.get(i + 1).copied();
                        if let Some(n) = next_r {
                            let ordered = if bits >> 31 == 0 { n > r } else { n < r };
                            assert!(ordered || (bits & 0x7fff_ffff == 0), "order {bits:#010x}");
                        }
                        let display: f64 = floats[i].to_string().parse().unwrap();
                        if display.to_bits() != r.to_bits() {
                            unlike_display.lock().unwrap().push(bits);
                        }
                        sum = sum.wrapping_add(mix(r.to_bits() ^ u64::from(bits).rotate_left(29)));
                    }
                    checksum.fetch_add(sum, Ordering::Relaxed);
                    finite_values.fetch_add(end - start, Ordering::Relaxed);
                }
            });
        }
    });
    let mut unlike_display = unlike_display.into_inner().unwrap();
    unlike_display.sort_unstable();
    let checksum = checksum.load(Ordering::Relaxed);
    eprintln!("checksum {checksum:#018x}; unlike Display: {unlike_display:#010x?}");
    assert_eq!(finite_values.load(Ordering::Relaxed), (1 << 32) - (1 << 24));
    assert_eq!(unlike_display, D3_EXCEPTIONS);
    assert_eq!(checksum, EXHAUSTIVE_CHECKSUM);
}

const EXHAUSTIVE_CHECKSUM: u64 = 0xa8a7_2795_24ce_e260;

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

fn unpacked(array: &ArrayRef) -> ArrayRef {
    let DataType::Dictionary(_, value_type) = array.data_type() else {
        panic!("not a dictionary: {}", array.data_type());
    };
    arrow::compute::cast(array, value_type).unwrap()
}

fn assert_unpacks_like_its_values(array: &ArrayRef) {
    let plain = unpacked(array);
    let n = array.len();
    for (dictionary, values) in [
        (array.clone(), plain.clone()),
        (array.slice(1, n - 1), plain.slice(1, n - 1)),
        (list_of(array), list_of(&plain)),
        (
            struct_of(vec![("d", array.clone())]),
            struct_of(vec![("d", plain.clone())]),
        ),
        (map_of(array), map_of(&plain)),
    ] {
        let dictionary = batch_of(vec![("v", dictionary)]);
        let (want, want_count) =
            counted(|| record_batch_to_values(&batch_of(vec![("v", values)])).unwrap());
        for render in [record_batch_to_values, record_batch_to_values_json] {
            let (got, got_count) = counted(|| render(&dictionary).unwrap());
            assert_eq!(
                serde_json::to_string(&got).unwrap(),
                serde_json::to_string(&want).unwrap()
            );
            assert_eq!(number_bits(&got), number_bits(&want));
            assert_eq!(got_count, want_count);
        }
    }
}

fn text_values_with_nulls() -> Vec<ArrayRef> {
    let words = [None, Some("a"), Some(""), Some("😀")];
    let sliced: ArrayRef = Arc::new(StringArray::from(vec![
        Some("skipped"),
        None,
        Some("a"),
        Some(""),
        Some("😀"),
    ]));
    vec![
        Arc::new(StringArray::from(words.to_vec())),
        Arc::new(LargeStringArray::from(words.to_vec())),
        sliced.slice(1, 4),
    ]
}

#[test]
fn null_dictionary_values_render_null() {
    macro_rules! keyed {
        ($($key:ty),*) => {$(
            for values in text_values_with_nulls() {
                let keys: PrimitiveArray<$key> =
                    vec![Some(0), Some(1), None, Some(1), Some(3), Some(0), Some(2)]
                        .into_iter()
                        .collect();
                let array: ArrayRef = Arc::new(DictionaryArray::new(keys, values));
                let batch = batch_of(vec![("v", array.clone())]);
                let rows = convert(&batch).expect("text dictionaries are built directly");
                assert_eq!(
                    serde_json::to_string(&rows).unwrap(),
                    r#"[{"v":null},{"v":"a"},{"v":null},{"v":"a"},{"v":"😀"},{"v":null},{"v":""}]"#
                );
                assert_unpacks_like_its_values(&array);
            }
        )*};
    }
    keyed!(
        Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type
    );
}

#[test]
fn a_key_under_a_null_slot_is_never_read() {
    let values: ArrayRef = Arc::new(StringArray::from(vec![None, Some("a")]));
    let signed = DictionaryArray::try_new(
        Int32Array::from_iter_values_with_nulls(
            [0, -5, 1, 99],
            Some(NullBuffer::from(vec![true, false, true, false])),
        ),
        values.clone(),
    )
    .unwrap();
    let unsigned = DictionaryArray::try_new(
        UInt8Array::from_iter_values_with_nulls(
            [200, 1, 255],
            Some(NullBuffer::from(vec![false, true, false])),
        ),
        values,
    )
    .unwrap();
    for (array, expected) in [
        (
            Arc::new(signed) as ArrayRef,
            r#"[{"v":null},{"v":null},{"v":"a"},{"v":null}]"#,
        ),
        (
            Arc::new(unsigned) as ArrayRef,
            r#"[{"v":null},{"v":"a"},{"v":null}]"#,
        ),
    ] {
        let rows = convert(&batch_of(vec![("v", array.clone())])).unwrap();
        assert_eq!(serde_json::to_string(&rows).unwrap(), expected);
        assert_eq!(
            serde_json::to_string(&convert(&batch_of(vec![("l", list_of(&array))])).unwrap())
                .unwrap(),
            serde_json::to_string(
                &convert(&batch_of(vec![("l", list_of(&unpacked(&array)))])).unwrap()
            )
            .unwrap()
        );
    }
}

#[test]
fn dictionaries_of_every_value_type_render_like_their_unpacked_values() {
    let values: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec!["x", "y", "z"])),
        Arc::new(LargeStringArray::from(vec!["x", "y", "z"])),
        Arc::new(StringViewArray::from(vec!["x", "y", "z"])),
        Arc::new(
            Decimal128Array::from(vec![12_345, -1, 0])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        ),
        Arc::new(BooleanArray::from(vec![true, false, true])),
        Arc::new(Int32Array::from(vec![-1, 0, i32::MAX])),
        Arc::new(Int64Array::from(vec![i64::MIN, 0, i64::MAX])),
        Arc::new(UInt64Array::from(vec![0, 1, u64::MAX])),
        Arc::new(Float64Array::from(vec![0.1, f64::NAN, -0.0])),
        Arc::new(Float32Array::from(vec![
            f32::from_bits(0x4c52_56d4),
            f32::NAN,
            0.7,
        ])),
        Arc::new(Float16Array::from(vec![
            F16::from_f32(0.1),
            F16::from_f32(f32::INFINITY),
            F16::from_f32(1.5),
        ])),
        Arc::new(BinaryArray::from(vec![&b"\x00\xff"[..], b"", b"abc"])),
        Arc::new(Date32Array::from(vec![0, 19_000, -1])),
        Arc::new(
            TimestampMillisecondArray::from(vec![0, 1_700_000_000_123, -1]).with_timezone("+05:30"),
        ),
    ];
    for values in values {
        for values in [
            values.clone(),
            masked(&values, |i| i == 0),
            values.slice(1, 2),
        ] {
            let keys = Int16Array::from(vec![Some(1), Some(0), None, Some(1), Some(0), Some(1)]);
            assert_unpacks_like_its_values(
                &(Arc::new(DictionaryArray::new(keys, values)) as ArrayRef),
            );
        }
    }
}

#[test]
fn null_dictionary_values_render_null_on_every_path() {
    let keys = || Int32Array::from(vec![0, 1, 0]);
    let durations: ArrayRef = Arc::new(DurationSecondArray::from(vec![1, 2, 3]));
    let dictionaries: Vec<(ArrayRef, &str)> = vec![
        (
            Arc::new(DictionaryArray::new(
                keys(),
                Arc::new(Int32Array::from(vec![None, Some(7)])),
            )),
            r#"[null,7,null]"#,
        ),
        (
            Arc::new(DictionaryArray::new(
                keys(),
                Arc::new(StringArray::from(vec![None, Some("a")])),
            )),
            r#"[null,"a",null]"#,
        ),
        (
            Arc::new(DictionaryArray::new(
                keys(),
                Arc::new(StringViewArray::from(vec![None, Some("a")])),
            )),
            r#"[null,"a",null]"#,
        ),
        (
            Arc::new(DictionaryArray::new(
                keys(),
                Arc::new(Float32Array::from_iter_values_with_nulls(
                    [f32::NAN, 1.5],
                    Some(NullBuffer::from(vec![false, true])),
                )),
            )),
            r#"[null,1.5,null]"#,
        ),
    ];
    for (array, expected) in dictionaries {
        for batch in [
            batch_of(vec![("v", array.clone())]),
            batch_of(vec![("v", array.clone()), ("d", durations.clone())]),
        ] {
            for render in [record_batch_to_values, record_batch_to_values_json] {
                let (rows, count) = counted(|| render(&batch).unwrap());
                let column: Vec<Value> = rows.iter().map(|r| r["v"].clone()).collect();
                assert_eq!(serde_json::to_string(&column).unwrap(), expected);
                assert_eq!(count, 0, "a NaN under a null value slot is not counted");
            }
        }
    }
}

#[test]
fn nested_dictionaries_render_null_values_as_null() {
    let inner: ArrayRef = Arc::new(DictionaryArray::new(
        Int8Array::from(vec![0, 1, 2]),
        Arc::new(StringArray::from(vec![Some("x"), None, Some("z")])),
    ));
    let outer: ArrayRef = Arc::new(DictionaryArray::new(
        Int16Array::from(vec![Some(2), Some(1), None, Some(0), Some(1)]),
        inner,
    ));
    let expected = r#"["z",null,null,"x",null]"#;
    let ree: ArrayRef = Arc::new(
        RunArray::try_new(
            &Int32Array::from(vec![2, 3, 5]),
            &DictionaryArray::new(
                Int32Array::from(vec![Some(1), Some(0), None]),
                Arc::new(Int32Array::from(vec![None, Some(9)])),
            ),
        )
        .unwrap(),
    );
    for (array, expected) in [(outer, expected), (ree, "[9,9,null,null,null]")] {
        for render in [record_batch_to_values, record_batch_to_values_json] {
            let rows = render(&batch_of(vec![("v", array.clone())])).unwrap();
            let column: Vec<Value> = rows.iter().map(|r| r["v"].clone()).collect();
            assert_eq!(serde_json::to_string(&column).unwrap(), expected);
            let rows = render(&one_row_list(array.clone())).unwrap();
            assert_eq!(serde_json::to_string(&rows[0]["v"]).unwrap(), expected);
        }
    }
}

#[test]
fn a_map_with_dictionary_keys_is_refused_whole() {
    let keys: ArrayRef = Arc::new(DictionaryArray::new(
        Int32Array::from(vec![0, 1, 0]),
        Arc::new(StringArray::from(vec![Some("k"), None])),
    ));
    let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let map: ArrayRef = Arc::new(map_with(keys, &values, true, vec![0, 1, 3]));
    let batch = batch_of(vec![("m", map)]);
    assert!(convert(&batch).is_none());
    let err = record_batch_to_values(&batch).unwrap_err().to_string();
    assert!(err.contains("Only UTF8 keys"), "{err}");
}

#[test]
fn plain_arrow_json_still_renders_a_null_dictionary_value_as_its_slot() {
    let array: ArrayRef = Arc::new(DictionaryArray::new(
        Int32Array::from(vec![0, 1]),
        Arc::new(StringArray::from(vec![None, Some("a")])),
    ));
    let batch = batch_of(vec![("v", array)]);
    let mut out = Vec::new();
    let mut writer = arrow_json::writer::WriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, arrow_json::writer::JsonArray>(&mut out);
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    assert_eq!(
        String::from_utf8(out).unwrap(),
        r#"[{"v":""},{"v":"a"}]"#,
        "arrow-json now renders a null dictionary value as null (apache/arrow-rs#12259): \
         delete the Dictionary arm and DictionaryValues encoder in ExactEncoderFactory (#1115)"
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
    let dictionary_of_ints_with_null_values = DictionaryArray::new(
        Int8Array::from(vec![0, 1, 0, 1]),
        Arc::new(Int32Array::from(vec![Some(1), None])),
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
        Arc::new(dictionary_of_ints_with_null_values),
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

#[test]
#[ignore = "timing; run in release with --nocapture"]
fn timing_of_single_floats() {
    let mut rng = Rng(17);
    let column = |rng: &mut Rng| -> ArrayRef {
        primitive::<Float32Type>(
            (0..100_000)
                .map(|_| ((rng.next() >> 11) as f64 / (1u64 << 53) as f64 * 2000.0 - 1000.0) as f32)
                .collect(),
        )
    };
    let batch = batch_of(vec![
        ("a", column(&mut rng)),
        ("b", column(&mut rng)),
        ("c", column(&mut rng)),
        ("d", column(&mut rng)),
    ]);
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
    let public = median(&|| record_batch_to_values(&batch).unwrap().len());
    let direct = convert(&batch).map(|_| median(&|| convert(&batch).unwrap().len()));
    eprintln!("100k rows x 4 Float32: json hop {json:?}, public {public:?}, direct {direct:?}");
}

#[test]
fn run_end_encoded_null_runs_render_null() {
    let ends = Int32Array::from(vec![2, 3, 5]);
    let cases: Vec<(ArrayRef, &str, u64)> = vec![
        (
            Arc::new(
                RunArray::try_new(&ends, &Int32Array::from(vec![Some(4), None, Some(6)])).unwrap(),
            ),
            "[4,4,null,6,6]",
            0,
        ),
        (
            Arc::new(
                RunArray::try_new(&ends, &StringArray::from(vec![None, Some("b"), Some("c")]))
                    .unwrap(),
            ),
            r#"[null,null,"b","c","c"]"#,
            0,
        ),
        (
            Arc::new(
                RunArray::try_new(
                    &ends,
                    &Float64Array::from_iter_values_with_nulls(
                        [1.5, f64::NAN, 2.5],
                        Some(NullBuffer::from(vec![true, false, true])),
                    ),
                )
                .unwrap(),
            ),
            "[1.5,1.5,null,2.5,2.5]",
            0,
        ),
    ];
    for (array, expected, non_finite) in cases {
        let sliced = array.slice(1, 3);
        let full: Vec<Value> = serde_json::from_str(expected).unwrap();
        let sliced_expected = serde_json::to_string(&full[1..4]).unwrap();
        for (array, expected) in [(array, expected.to_owned()), (sliced, sliced_expected)] {
            let batch = batch_of(vec![("v", array)]);
            assert!(convert(&batch).is_none(), "run-end-encoded takes the hop");
            for render in [record_batch_to_values, record_batch_to_values_json] {
                let (rows, count) = counted(|| render(&batch).unwrap());
                let column: Vec<Value> = rows.iter().map(|r| r["v"].clone()).collect();
                assert_eq!(serde_json::to_string(&column).unwrap(), expected);
                assert_eq!(count, non_finite, "a NaN under a null run is not counted");
            }
        }
    }
}
