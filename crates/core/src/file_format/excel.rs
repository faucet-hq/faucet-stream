//! Excel (`.xlsx`) read/write for the file connectors (#604).
//!
//! **Excel is not streamable.** A workbook is a zip container whose central
//! directory sits at the end, and `calamine` parses a whole worksheet into a
//! range before the first cell is readable. So both directions hold the whole
//! object in memory, and callers must size `batch_size` accordingly — the one
//! format in this module where peak memory is the file rather than the page.
//! [`FileFormat::requires_whole_object`](super::FileFormat::requires_whole_object)
//! reports this so a connector can pick its read strategy without a match on
//! the format.

use crate::error::FaucetError;
use serde_json::{Map, Value};

/// The worksheet a written workbook gets when the config names none.
pub const DEFAULT_SHEET: &str = "Sheet1";

/// Parse a workbook into records.
///
/// `sheet` is a worksheet name, or an index as a string; `header_row` is the
/// 0-based row supplying field names. Cells keep their Excel type — a number
/// stays a JSON number — rather than being stringified, because a spreadsheet
/// is one of the few sources that actually knows its types.
pub fn decode(
    bytes: &[u8],
    sheet: Option<&str>,
    header_row: usize,
) -> Result<Vec<Value>, FaucetError> {
    use calamine::{Reader, Xlsx};
    // Borrow the body rather than copying it: `&[u8]` is `Read + Seek`, so a
    // `to_vec()` here would be a second whole-file allocation on top of the one
    // calamine makes.
    let mut wb: Xlsx<_> = calamine::open_workbook_from_rs(std::io::Cursor::new(bytes))
        .map_err(|e| FaucetError::Source(format!("xlsx: opening workbook: {e}")))?;
    let names = wb.sheet_names().to_vec();
    let name = resolve_sheet(&names, sheet)?;
    let range = wb
        .worksheet_range(&name)
        .map_err(|e| FaucetError::Source(format!("xlsx: reading sheet '{name}': {e}")))?;
    // An empty sheet is zero records, not an error: a sink that wrote an empty
    // page produces exactly this, and failing the read would turn "no data"
    // into a pipeline failure.
    let total_rows = range.rows().count();
    if total_rows == 0 {
        return Ok(Vec::new());
    }
    let headers: Vec<String> = range
        .rows()
        .nth(header_row)
        .ok_or_else(|| {
            FaucetError::Source(format!(
                "xlsx: header_row {header_row} is beyond the sheet ({total_rows} rows)"
            ))
        })?
        .iter()
        .map(cell_to_string)
        .collect();
    let mut out = Vec::new();
    for row in range.rows().skip(header_row + 1) {
        let mut obj = Map::new();
        for (i, cell) in row.iter().enumerate() {
            let key = headers
                .get(i)
                .cloned()
                .filter(|k| !k.is_empty())
                .unwrap_or_else(|| format!("column_{i}"));
            obj.insert(key, cell_to_value(cell));
        }
        out.push(Value::Object(obj));
    }
    Ok(out)
}

/// A name, an index-as-string, or the first sheet.
///
/// A name that looks like a number is tried as a name **first**, so a worksheet
/// genuinely called "2024" is not read as sheet index 2024 (or, worse, as a
/// different sheet that happens to be at that index).
fn resolve_sheet(names: &[String], sheet: Option<&str>) -> Result<String, FaucetError> {
    match sheet {
        Some(s) if names.iter().any(|n| n == s) => Ok(s.to_string()),
        Some(s) => match s.parse::<usize>() {
            Ok(idx) => names.get(idx).cloned().ok_or_else(|| {
                FaucetError::Source(format!(
                    "xlsx: sheet index {idx} out of range ({} sheets)",
                    names.len()
                ))
            }),
            Err(_) => Err(FaucetError::Source(format!(
                "xlsx: sheet '{s}' not found (available: {})",
                names.join(", ")
            ))),
        },
        None => names
            .first()
            .cloned()
            .ok_or_else(|| FaucetError::Source("xlsx: workbook has no worksheets".into())),
    }
}

/// Write records as a single-worksheet workbook.
///
/// Columns are the first-seen union of every record's keys, so a record that
/// gains a field mid-page widens the sheet rather than losing the field.
/// Numbers and booleans are written as their Excel types; everything else is
/// text, with nested structure re-serialized as JSON rather than dropped.
pub fn encode(records: &[Value], sheet: Option<&str>) -> Result<Vec<u8>, FaucetError> {
    use rust_xlsxwriter::{Workbook, Worksheet};
    let headers = super::header_union(records);
    let mut book = Workbook::new();
    let mut ws = Worksheet::new();
    ws.set_name(sheet.unwrap_or(DEFAULT_SHEET))
        .map_err(|e| FaucetError::Sink(format!("xlsx: naming the worksheet: {e}")))?;

    let err = |e: rust_xlsxwriter::XlsxError| FaucetError::Sink(format!("xlsx: writing: {e}"));
    for (col, h) in headers.iter().enumerate() {
        ws.write_string(0, col as u16, h.as_str()).map_err(err)?;
    }
    for (row, r) in records.iter().enumerate() {
        let row = (row + 1) as u32;
        for (col, h) in headers.iter().enumerate() {
            let col = col as u16;
            match r.get(h) {
                None | Some(Value::Null) => {}
                Some(Value::Bool(b)) => {
                    ws.write_boolean(row, col, *b).map_err(err)?;
                }
                Some(Value::Number(n)) => match n.as_f64() {
                    Some(f) => {
                        ws.write_number(row, col, f).map_err(err)?;
                    }
                    // An integer too large for f64 would lose precision as a
                    // number; write the exact digits as text instead.
                    None => {
                        ws.write_string(row, col, n.to_string().as_str())
                            .map_err(err)?;
                    }
                },
                Some(other) => {
                    ws.write_string(row, col, super::cell_text(other).as_str())
                        .map_err(err)?;
                }
            }
        }
    }
    book.push_worksheet(ws);
    book.save_to_buffer()
        .map_err(|e| FaucetError::Sink(format!("xlsx: finishing the workbook: {e}")))
}

/// An integral double becomes an integer; anything else stays a float.
fn float_to_value(f: f64) -> Value {
    if f.fract() == 0.0 && f >= i64::MIN as f64 && f <= i64::MAX as f64 {
        return Value::from(f as i64);
    }
    serde_json::Number::from_f64(f)
        .map(Value::Number)
        .unwrap_or(Value::Null)
}

fn cell_to_string(cell: &calamine::Data) -> String {
    use calamine::Data;
    match cell {
        Data::String(s) => s.clone(),
        Data::Empty => String::new(),
        other => other.to_string(),
    }
}

fn cell_to_value(cell: &calamine::Data) -> Value {
    use calamine::Data;
    match cell {
        Data::Empty => Value::Null,
        Data::String(s) => Value::String(s.clone()),
        Data::Bool(b) => Value::Bool(*b),
        Data::Int(i) => Value::from(*i),
        // xlsx stores every number as a double, so an integer and its float
        // form are the same cell. Mapping an integral double back to an
        // integer is what makes a write→read round trip faithful; without it
        // every count comes back as `1.0`.
        Data::Float(f) => float_to_value(*f),
        Data::DateTime(dt) => Value::String(dt.to_string()),
        Data::DateTimeIso(s) | Data::DurationIso(s) => Value::String(s.clone()),
        Data::Error(e) => Value::String(format!("{e:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_workbook_round_trips_with_its_types() {
        let recs = vec![
            json!({"id": 1, "name": "ada", "ok": true}),
            json!({"id": 2, "name": "grace", "ok": false}),
        ];
        let bytes = encode(&recs, None).expect("encode");
        let back = decode(&bytes, None, 0).expect("decode");
        assert_eq!(back, recs, "numbers and booleans survive as themselves");
    }

    #[test]
    fn columns_are_the_union_and_a_missing_cell_is_null() {
        let bytes = encode(&[json!({"a": 1}), json!({"b": 2})], None).expect("encode");
        let back = decode(&bytes, None, 0).expect("decode");
        assert_eq!(
            back,
            vec![json!({"a": 1, "b": null}), json!({"a": null, "b": 2})]
        );
    }

    #[test]
    fn a_named_sheet_is_written_and_selected_back() {
        let bytes = encode(&[json!({"a": 1})], Some("Data")).expect("encode");
        assert!(decode(&bytes, Some("Data"), 0).is_ok());
        // An index selects it too.
        assert!(decode(&bytes, Some("0"), 0).is_ok());
        let err = decode(&bytes, Some("Nope"), 0).expect_err("missing sheet");
        assert!(err.to_string().contains("available: Data"), "{err}");
    }

    #[test]
    fn an_empty_workbook_reads_back_as_zero_records_not_an_error() {
        // A sink that wrote an empty page produces exactly this; failing here
        // would turn "no data" into a pipeline failure.
        let bytes = encode(&[], None).expect("encode");
        assert!(decode(&bytes, None, 0).expect("decode").is_empty());
    }

    #[test]
    fn a_header_row_beyond_the_sheet_is_an_error_naming_the_row() {
        let bytes = encode(&[json!({"a": 1})], None).expect("encode");
        let err = decode(&bytes, None, 99).expect_err("out of range");
        assert!(err.to_string().contains("header_row 99"), "{err}");
    }

    #[test]
    fn nested_structure_is_kept_as_json_text_rather_than_dropped() {
        let bytes = encode(&[json!({"a": {"k": 1}})], None).expect("encode");
        let back = decode(&bytes, None, 0).expect("decode");
        assert_eq!(back, vec![json!({"a": r#"{"k":1}"#})]);
    }

    #[test]
    fn a_sheet_index_out_of_range_names_the_count() {
        let bytes = encode(&[json!({"a": 1})], None).expect("encode");
        let err = decode(&bytes, Some("7"), 0).expect_err("index 7");
        assert!(err.to_string().contains("out of range"), "{err}");
    }

    #[test]
    fn a_non_string_header_cell_is_stringified_rather_than_dropped() {
        // A worksheet whose header row holds numbers still names its columns;
        // dropping them would silently shift every value.
        let bytes = encode(&[json!({"2024": "q1", "flag": true})], None).expect("encode");
        let back = decode(&bytes, None, 0).expect("decode");
        assert_eq!(back, vec![json!({"2024": "q1", "flag": true})]);
    }

    #[test]
    fn a_blank_cell_reads_back_as_null() {
        let bytes = encode(&[json!({"a": 1, "b": null})], None).expect("encode");
        let back = decode(&bytes, None, 0).expect("decode");
        assert_eq!(back, vec![json!({"a": 1, "b": null})]);
    }

    #[test]
    fn not_a_workbook_is_an_error_not_a_panic() {
        let err = decode(b"id,name\n1,ada\n", None, 0).expect_err("csv is not xlsx");
        assert!(err.to_string().contains("opening workbook"), "{err}");
    }

    #[test]
    fn an_integral_double_reads_back_as_an_integer() {
        // xlsx has only doubles, so without this a round-tripped count comes
        // back as `1.0` and every downstream equality check breaks.
        assert_eq!(float_to_value(1.0), json!(1));
        assert_eq!(float_to_value(-3.0), json!(-3));
        assert_eq!(float_to_value(1.5), json!(1.5));
        assert_eq!(float_to_value(f64::NAN), Value::Null);
    }

    #[test]
    fn a_sheet_named_like_a_number_is_matched_by_name_first() {
        let bytes = encode(&[json!({"a": 1})], Some("2024")).expect("encode");
        // Without the name-first rule this would be read as index 2024.
        assert!(decode(&bytes, Some("2024"), 0).is_ok());
    }

    /// Every `calamine::Data` variant maps to a JSON value. xlsx has no type
    /// system of its own beyond these, so an unmapped variant would silently
    /// become the wrong JSON type on read-back.
    #[test]
    fn every_cell_variant_decodes_to_its_json_shape() {
        use calamine::{CellErrorType, Data, ExcelDateTime, ExcelDateTimeType};
        assert_eq!(cell_to_value(&Data::Empty), Value::Null);
        assert_eq!(cell_to_value(&Data::String("s".into())), json!("s"));
        assert_eq!(cell_to_value(&Data::Bool(true)), json!(true));
        assert_eq!(cell_to_value(&Data::Int(-7)), json!(-7));
        assert_eq!(cell_to_value(&Data::Float(2.5)), json!(2.5));
        // An integral double comes back as an integer, not `3.0`.
        assert_eq!(cell_to_value(&Data::Float(3.0)), json!(3));
        assert_eq!(
            cell_to_value(&Data::DateTimeIso("2026-01-02T03:04:05".into())),
            json!("2026-01-02T03:04:05")
        );
        assert_eq!(
            cell_to_value(&Data::DurationIso("PT1H".into())),
            json!("PT1H")
        );
        // An error cell becomes text rather than being dropped, so a bad cell
        // is visible downstream instead of vanishing.
        assert_eq!(
            cell_to_value(&Data::Error(CellErrorType::Div0)),
            json!("Div0")
        );
        // A datetime cell becomes a non-empty string.
        let dt = Data::DateTime(ExcelDateTime::new(
            45000.5,
            ExcelDateTimeType::DateTime,
            false,
        ));
        assert!(matches!(cell_to_value(&dt), Value::String(s) if !s.is_empty()));
    }

    /// Header cells are read as text. An empty header must be the empty
    /// string, not the word "Empty" — that string becomes a record key.
    #[test]
    fn header_cells_stringify_without_leaking_the_debug_form() {
        use calamine::Data;
        assert_eq!(cell_to_string(&Data::String("name".into())), "name");
        assert_eq!(cell_to_string(&Data::Empty), "");
        assert_eq!(cell_to_string(&Data::Int(3)), "3");
        assert_eq!(cell_to_string(&Data::Bool(true)), "true");
    }

    /// `float_to_value` is what keeps a write→read round trip faithful; a
    /// non-finite double has no JSON number form and must not panic.
    #[test]
    fn a_non_finite_double_becomes_null_rather_than_panicking() {
        assert_eq!(float_to_value(f64::NAN), Value::Null);
        assert_eq!(float_to_value(f64::INFINITY), Value::Null);
        assert_eq!(float_to_value(-0.0), json!(0));
    }
}
