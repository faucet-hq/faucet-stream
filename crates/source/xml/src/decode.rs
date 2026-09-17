//! Response-decode pipeline (#515).
//!
//! A declarative chain applied to the raw response **body** before record
//! extraction, so a source can consume payloads that aren't plain JSON —
//! including files embedded in a JSON/SOAP envelope:
//!
//! ```yaml
//! decode:
//!   - extract: "runReportResponse.runReportReturn.reportBytes"  # element text out of the SOAP body
//!   - base64                               # base64 → bytes
//!   - unzip: { member: "*.csv" }           # or `gunzip`
//!   - parse: { format: xlsx, header_row: 4 }
//! ```
//!
//! Steps compose left-to-right over a byte buffer; the terminal `parse` step
//! turns the bytes into records. Without an explicit `parse`, the bytes are
//! parsed as JSON.
//!
//! Note: unlike the REST source's decode pipeline, the XML source's `extract`
//! step selects an **element's text** by a dot-path (namespace-insensitive,
//! trailing-match) rather than a JSONPath — the body here is XML/SOAP.

use base64::Engine;
use faucet_core::FaucetError;
use jsonpath_rust::JsonPath;
use quick_xml::events::Event;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::io::{Cursor, Read};

/// A byte-chain step with no parameters (`- base64`, `- gunzip`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SimpleStep {
    /// Base64-decode the (UTF-8 text) buffer into bytes.
    Base64,
    /// Gzip-decompress the buffer.
    Gunzip,
}

/// Select a member of a zip archive.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields)]
pub struct UnzipSpec {
    /// Glob (`*.csv`, `prefix*`, `*mid*`, or an exact name) selecting the member.
    /// When omitted, the first file entry is used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub member: Option<String>,
}

/// Final-parse format for a decode chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum ParseFormat {
    /// JSON (default).
    #[default]
    Json,
    /// Delimited text.
    Csv,
    /// Excel workbook (requires the crate's `excel` feature).
    Xlsx,
    /// XML → JSON.
    Xml,
}

fn default_has_headers() -> bool {
    true
}

/// Parse the decoded bytes into records.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ParseSpec {
    /// Output format.
    pub format: ParseFormat,
    /// JSONPath selecting the record array (json/xml). When omitted, an array
    /// body becomes the records and an object becomes a single record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub records_path: Option<String>,
    /// CSV delimiter byte (default `,`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delimiter: Option<u8>,
    /// Whether the first CSV row is a header (default `true`).
    #[serde(default = "default_has_headers")]
    pub has_headers: bool,
    /// Excel worksheet name / index-as-string (default: first).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sheet: Option<String>,
    /// 0-based Excel header row (default `0`).
    #[serde(default)]
    pub header_row: usize,
}

/// One step of the decode chain.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum DecodeStep {
    /// Parameterless byte step (`base64` / `gunzip`).
    Simple(SimpleStep),
    /// Pull a (string) field out of a JSON body first.
    Extract {
        /// JSONPath to a string field whose value becomes the buffer.
        extract: String,
    },
    /// Select a member from a zip archive.
    Unzip {
        /// Member selector.
        unzip: UnzipSpec,
    },
    /// Terminal parse into records.
    Parse {
        /// Parse options.
        parse: ParseSpec,
    },
}

/// Run the decode chain over the response body, returning records.
pub async fn run_decode(body: &[u8], steps: &[DecodeStep]) -> Result<Vec<Value>, FaucetError> {
    let mut buf = body.to_vec();
    for step in steps {
        match step {
            DecodeStep::Extract { extract } => {
                // XML source variant: `extract` is a dot-path to an element whose
                // text content becomes the buffer (e.g. pull a base64 file blob
                // out of a SOAP `<reportBytes>` element). The path is matched by
                // its trailing element names, namespace-prefix-insensitive.
                let s = xml_extract_text(&buf, extract).ok_or_else(|| {
                    FaucetError::Source(format!(
                        "decode `extract`: '{extract}' matched no element text in the XML body"
                    ))
                })?;
                buf = s.into_bytes();
            }
            DecodeStep::Simple(SimpleStep::Base64) => {
                let text = std::str::from_utf8(&buf).map_err(|e| {
                    FaucetError::Source(format!("decode `base64`: buffer is not UTF-8 text: {e}"))
                })?;
                buf = base64::engine::general_purpose::STANDARD
                    .decode(text.trim())
                    .map_err(|e| FaucetError::Source(format!("decode `base64`: {e}")))?;
            }
            DecodeStep::Simple(SimpleStep::Gunzip) => {
                let mut out = Vec::new();
                flate2::read::GzDecoder::new(Cursor::new(&buf))
                    .read_to_end(&mut out)
                    .map_err(|e| FaucetError::Source(format!("decode `gunzip`: {e}")))?;
                buf = out;
            }
            DecodeStep::Unzip { unzip } => {
                buf = unzip_member(&buf, unzip.member.as_deref())?;
            }
            DecodeStep::Parse { parse } => {
                return parse_records(&buf, parse).await;
            }
        }
    }
    // No explicit `parse` → default to JSON.
    parse_records(
        &buf,
        &ParseSpec {
            format: ParseFormat::Json,
            records_path: None,
            delimiter: None,
            has_headers: true,
            sheet: None,
            header_row: 0,
        },
    )
    .await
}

/// Concatenated text of the first element whose ancestor stack ends with the
/// dot-path segments (namespace-prefix-insensitive). Used by the XML `extract`
/// decode step to pull a blob (e.g. a base64-encoded file) out of a SOAP body —
/// e.g. `runReportResponse.runReportReturn.reportBytes`.
pub(crate) fn xml_extract_text(bytes: &[u8], dot_path: &str) -> Option<String> {
    let want: Vec<String> = dot_path
        .split('.')
        .filter(|s| !s.is_empty())
        .map(|s| s.rsplit(':').next().unwrap_or(s).to_string())
        .collect();
    if want.is_empty() {
        return None;
    }
    let text = std::str::from_utf8(bytes).ok()?;
    let mut reader = quick_xml::Reader::from_str(text);
    let local = |name: &[u8]| -> String {
        String::from_utf8_lossy(name)
            .rsplit(':')
            .next()
            .unwrap_or_default()
            .to_string()
    };
    let ends_with = |stack: &[String]| -> bool {
        stack.len() >= want.len() && stack[stack.len() - want.len()..] == want[..]
    };
    let mut stack: Vec<String> = Vec::new();
    let mut capture_depth: Option<usize> = None;
    let mut out = String::new();
    loop {
        match reader.read_event().ok()? {
            Event::Eof => break,
            Event::Start(e) => {
                stack.push(local(e.name().as_ref()));
                if capture_depth.is_none() && ends_with(&stack) {
                    capture_depth = Some(stack.len());
                    out.clear();
                }
            }
            Event::Empty(e) => {
                stack.push(local(e.name().as_ref()));
                if capture_depth.is_none() && ends_with(&stack) {
                    return Some(String::new());
                }
                stack.pop();
            }
            Event::End(_) => {
                if capture_depth == Some(stack.len()) {
                    return Some(out);
                }
                stack.pop();
            }
            Event::Text(t) if capture_depth.is_some() => {
                if let Ok(s) = t.unescape() {
                    out.push_str(&s);
                }
            }
            Event::CData(t) if capture_depth.is_some() => {
                out.push_str(&String::from_utf8_lossy(&t));
            }
            _ => {}
        }
    }
    None
}

/// Minimal glob: `*` matches any run. Supports `*.ext`, `prefix*`, `*mid*`,
/// `*a*b*`, and exact names.
fn glob_match(pattern: &str, name: &str) -> bool {
    if !pattern.contains('*') {
        return pattern == name;
    }
    let parts: Vec<&str> = pattern.split('*').collect();
    let mut pos = 0usize;
    for (i, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }
        if i == 0 {
            if !name[pos..].starts_with(part) {
                return false;
            }
            pos += part.len();
        } else if i == parts.len() - 1 {
            return name[pos..].ends_with(part);
        } else {
            match name[pos..].find(part) {
                Some(idx) => pos += idx + part.len(),
                None => return false,
            }
        }
    }
    true
}

fn unzip_member(bytes: &[u8], member: Option<&str>) -> Result<Vec<u8>, FaucetError> {
    let mut archive = zip::ZipArchive::new(Cursor::new(bytes))
        .map_err(|e| FaucetError::Source(format!("decode `unzip`: not a valid zip: {e}")))?;
    // Resolve the member index first (immutable name scan), then read it.
    let mut chosen: Option<usize> = None;
    for i in 0..archive.len() {
        let f = archive
            .by_index(i)
            .map_err(|e| FaucetError::Source(format!("decode `unzip`: {e}")))?;
        if !f.is_file() {
            continue;
        }
        let matches = match member {
            Some(pat) => glob_match(pat, f.name()),
            None => true, // first file
        };
        if matches {
            chosen = Some(i);
            break;
        }
    }
    let idx = chosen.ok_or_else(|| {
        FaucetError::Source(format!(
            "decode `unzip`: no member matched {}",
            member
                .map(|m| format!("'{m}'"))
                .unwrap_or_else(|| "any".into())
        ))
    })?;
    let mut f = archive
        .by_index(idx)
        .map_err(|e| FaucetError::Source(format!("decode `unzip`: {e}")))?;
    let mut out = Vec::new();
    f.read_to_end(&mut out)
        .map_err(|e| FaucetError::Source(format!("decode `unzip`: reading member: {e}")))?;
    Ok(out)
}

async fn parse_records(bytes: &[u8], spec: &ParseSpec) -> Result<Vec<Value>, FaucetError> {
    match spec.format {
        ParseFormat::Json => {
            let v: Value = serde_json::from_slice(bytes)
                .map_err(|e| FaucetError::Source(format!("decode `parse` json: {e}")))?;
            Ok(records_from_value(v, spec.records_path.as_deref()))
        }
        ParseFormat::Csv => {
            crate::format::parse_csv(bytes, spec.delimiter.unwrap_or(b','), spec.has_headers)
                .await
                .map_err(|e| FaucetError::Source(format!("decode `parse` csv: {e}")))
        }
        ParseFormat::Xlsx => {
            crate::format::parse_excel(bytes, spec.sheet.as_deref(), spec.header_row)
                .map_err(|e| FaucetError::Source(format!("decode `parse` xlsx: {e}")))
        }
        ParseFormat::Xml => {
            let v = xml_to_json(bytes)?;
            Ok(records_from_value(v, spec.records_path.as_deref()))
        }
    }
}

/// Turn a JSON value into records: apply `records_path` if given, else an array
/// becomes the records and any other value becomes a single record.
fn records_from_value(v: Value, records_path: Option<&str>) -> Vec<Value> {
    match records_path {
        Some(path) => v
            .query(path)
            .ok()
            .map(|ms| ms.into_iter().cloned().collect())
            .unwrap_or_default(),
        None => match v {
            Value::Array(a) => a,
            other => vec![other],
        },
    }
}

/// One open element in the compact XML decoder: its object and its accumulated
/// text. The stack is seeded with a synthetic document-root frame.
type XmlFrame = (Map<String, Value>, String);

/// Unbalanced-input error for the compact XML decoder.
///
/// The frame stack is seeded with one synthetic document-root frame, so
/// *balanced* input never empties it — but an unbalanced extra `</x>` pops the
/// root, and the frame access that follows used to `.expect()`, i.e. panic on
/// untrusted network input inside a live pipeline. `quick_xml`'s
/// `check_end_names` happens to reject the mismatch first, but that is a
/// dependency default, not an invariant this code owns (#654 L25) — so the
/// shortfall is reported as a typed error, matching `convert::xml_to_json`.
fn unbalanced_xml(detail: &str) -> FaucetError {
    FaucetError::Source(format!(
        "decode `parse` xml: malformed XML: {detail}; the document has more closing \
         than opening tags"
    ))
}

/// Borrow the innermost open frame, or report unbalanced input.
fn top_frame<'a>(stack: &'a mut [XmlFrame], detail: &str) -> Result<&'a mut XmlFrame, FaucetError> {
    stack.last_mut().ok_or_else(|| unbalanced_xml(detail))
}

/// Pop the innermost open frame, or report unbalanced input.
fn pop_frame(stack: &mut Vec<XmlFrame>, detail: &str) -> Result<XmlFrame, FaucetError> {
    stack.pop().ok_or_else(|| unbalanced_xml(detail))
}

/// Compact XML → JSON: each element becomes an object of its children; repeated
/// child tags become arrays; attributes are `@name`; text is `#text` (or the
/// value directly when an element has only text).
fn xml_to_json(bytes: &[u8]) -> Result<Value, FaucetError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|e| FaucetError::Source(format!("decode `parse` xml: not UTF-8: {e}")))?;
    let mut reader = quick_xml::Reader::from_str(text);
    // A stack of (object, text-accumulator) frames; index 0 is the document root.
    let mut stack: Vec<(Map<String, Value>, String)> = vec![(Map::new(), String::new())];

    fn attrs(e: &quick_xml::events::BytesStart) -> Map<String, Value> {
        let mut m = Map::new();
        for a in e.attributes().flatten() {
            let k = String::from_utf8_lossy(a.key.as_ref())
                .rsplit(':')
                .next()
                .unwrap_or_default()
                .to_string();
            if let Ok(v) = a.unescape_value() {
                m.insert(format!("@{k}"), Value::String(v.to_string()));
            }
        }
        m
    }
    fn local(name: &[u8]) -> String {
        String::from_utf8_lossy(name)
            .rsplit(':')
            .next()
            .unwrap_or_default()
            .to_string()
    }
    fn insert_child(parent: &mut Map<String, Value>, key: String, val: Value) {
        match parent.get_mut(&key) {
            Some(Value::Array(arr)) => arr.push(val),
            Some(existing) => {
                let prev = existing.take();
                parent.insert(key, Value::Array(vec![prev, val]));
            }
            None => {
                parent.insert(key, val);
            }
        }
    }
    fn finish(obj: Map<String, Value>, text: String) -> Value {
        let trimmed = text.trim();
        if obj.is_empty() {
            Value::String(trimmed.to_string())
        } else {
            let mut obj = obj;
            if !trimmed.is_empty() {
                obj.insert("#text".to_string(), Value::String(trimmed.to_string()));
            }
            Value::Object(obj)
        }
    }

    loop {
        match reader
            .read_event()
            .map_err(|e| FaucetError::Source(format!("decode `parse` xml: {e}")))?
        {
            Event::Eof => break,
            Event::Start(e) => stack.push((attrs(&e), String::new())),
            Event::Empty(e) => {
                let name = local(e.name().as_ref());
                let val = finish(attrs(&e), String::new());
                let top = top_frame(&mut stack, "element after the document root closed")?;
                insert_child(&mut top.0, name, val);
            }
            Event::End(e) => {
                let name = local(e.name().as_ref());
                let detail = format!("unexpected end tag `</{name}>`");
                let (obj, text) = pop_frame(&mut stack, &detail)?;
                let val = finish(obj, text);
                let top = top_frame(&mut stack, &detail)?;
                insert_child(&mut top.0, name, val);
            }
            Event::Text(t) => {
                if let Ok(s) = t.unescape() {
                    top_frame(&mut stack, "text after the document root closed")?
                        .1
                        .push_str(&s);
                }
            }
            Event::CData(t) => {
                top_frame(&mut stack, "CDATA after the document root closed")?
                    .1
                    .push_str(&String::from_utf8_lossy(&t));
            }
            _ => {}
        }
    }
    let (root, _) = stack.pop().unwrap_or_default();
    Ok(Value::Object(root))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn parse_json() -> ParseSpec {
        ParseSpec {
            format: ParseFormat::Json,
            records_path: None,
            delimiter: None,
            has_headers: true,
            sheet: None,
            header_row: 0,
        }
    }

    #[test]
    fn glob_matches_common_patterns() {
        assert!(glob_match("*.csv", "report.csv"));
        assert!(!glob_match("*.csv", "report.txt"));
        assert!(glob_match("data*", "data_2024.csv"));
        assert!(glob_match("*part*", "x_part_1"));
        assert!(glob_match("exact.csv", "exact.csv"));
        assert!(!glob_match("exact.csv", "other.csv"));
        assert!(glob_match("*a*b*", "xxaxxbxx"));
        assert!(!glob_match("*a*b*", "xxbxxaxx"));
    }

    #[tokio::test]
    async fn extract_xml_base64_json_chain() {
        // A SOAP-ish XML envelope holding a base64-encoded JSON array in an
        // element — the XML `extract` step pulls the element text out.
        let inner = br#"[{"id":1},{"id":2}]"#;
        let b64 = base64::engine::general_purpose::STANDARD.encode(inner);
        let body = format!("<Envelope><Body><d><payload>{b64}</payload></d></Body></Envelope>");
        let steps = vec![
            DecodeStep::Extract {
                extract: "d.payload".into(),
            },
            DecodeStep::Simple(SimpleStep::Base64),
            DecodeStep::Parse {
                parse: parse_json(),
            },
        ];
        let recs = run_decode(body.as_bytes(), &steps).await.unwrap();
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[1]["id"], 2);
    }

    #[tokio::test]
    async fn extract_xml_namespaced_trailing_match() {
        // Namespace prefixes are stripped and the path matches by trailing
        // segments, so a SOAP-qualified element is found by its local names.
        let body = "<soap:Envelope><soap:Body><ns:runReportResponse>\
            <ns:runReportReturn><ns:reportBytes>SGk=</ns:reportBytes>\
            </ns:runReportReturn></ns:runReportResponse></soap:Body></soap:Envelope>";
        // "SGk=" → "Hi"; parse the decoded text as a headerless CSV so the
        // single value lands as a field we can assert on.
        let steps = [
            DecodeStep::Extract {
                extract: "runReportResponse.runReportReturn.reportBytes".into(),
            },
            DecodeStep::Simple(SimpleStep::Base64),
            DecodeStep::Parse {
                parse: ParseSpec {
                    format: ParseFormat::Csv,
                    has_headers: false,
                    ..parse_json()
                },
            },
        ];
        let recs = run_decode(body.as_bytes(), &steps).await.unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0]["column_0"], "Hi");
    }

    #[tokio::test]
    async fn gunzip_csv_chain() {
        use flate2::{Compression, write::GzEncoder};
        use std::io::Write;
        let csv = b"a,b\n1,2\n3,4\n";
        let mut enc = GzEncoder::new(Vec::new(), Compression::default());
        enc.write_all(csv).unwrap();
        let gz = enc.finish().unwrap();
        let steps = vec![
            DecodeStep::Simple(SimpleStep::Gunzip),
            DecodeStep::Parse {
                parse: ParseSpec {
                    format: ParseFormat::Csv,
                    ..parse_json()
                },
            },
        ];
        let recs = run_decode(&gz, &steps).await.unwrap();
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0]["a"], "1");
        assert_eq!(recs[1]["b"], "4");
    }

    #[tokio::test]
    async fn unzip_member_glob_chain() {
        use std::io::Write;
        use zip::write::SimpleFileOptions;
        let mut cur = Cursor::new(Vec::new());
        {
            let mut zw = zip::ZipWriter::new(&mut cur);
            let opts = SimpleFileOptions::default();
            zw.start_file("notes.txt", opts).unwrap();
            zw.write_all(b"ignore").unwrap();
            zw.start_file("data.csv", opts).unwrap();
            zw.write_all(b"x\n9\n").unwrap();
            zw.finish().unwrap();
        }
        let zip_bytes = cur.into_inner();
        let steps = vec![
            DecodeStep::Unzip {
                unzip: UnzipSpec {
                    member: Some("*.csv".into()),
                },
            },
            DecodeStep::Parse {
                parse: ParseSpec {
                    format: ParseFormat::Csv,
                    ..parse_json()
                },
            },
        ];
        let recs = run_decode(&zip_bytes, &steps).await.unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0]["x"], "9");
    }

    #[tokio::test]
    async fn default_parse_is_json_and_records_path_works() {
        let body = br#"{"items":[{"n":1},{"n":2}]}"#;
        // No parse step → default json, whole object is one record.
        let recs = run_decode(body, &[]).await.unwrap();
        assert_eq!(recs.len(), 1);
        // With records_path → the array.
        let steps = vec![DecodeStep::Parse {
            parse: ParseSpec {
                records_path: Some("$.items[*]".into()),
                ..parse_json()
            },
        }];
        let recs = run_decode(body, &steps).await.unwrap();
        assert_eq!(recs.len(), 2);
    }

    #[test]
    fn xml_to_json_handles_repeated_elements_and_attrs() {
        let xml = br#"<root><row id="1"><n>a</n></row><row id="2"><n>b</n></row></root>"#;
        let v = xml_to_json(xml).unwrap();
        let rows = &v["root"]["row"];
        assert!(rows.is_array());
        assert_eq!(rows[0]["@id"], "1");
        assert_eq!(rows[0]["n"], "a");
        assert_eq!(rows[1]["n"], "b");
    }

    #[tokio::test]
    async fn xml_parse_with_records_path() {
        let xml = br#"<root><row><n>a</n></row><row><n>b</n></row></root>"#;
        let steps = vec![DecodeStep::Parse {
            parse: ParseSpec {
                format: ParseFormat::Xml,
                records_path: Some("$.root.row[*]".into()),
                ..parse_json()
            },
        }];
        let recs = run_decode(xml, &steps).await.unwrap();
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[1]["n"], "b");
    }

    #[tokio::test]
    async fn base64_on_non_utf8_errors() {
        let steps = vec![DecodeStep::Simple(SimpleStep::Base64)];
        assert!(run_decode(&[0xff, 0xfe], &steps).await.is_err());
    }

    #[tokio::test]
    async fn extract_missing_field_errors() {
        let steps = vec![DecodeStep::Extract {
            extract: "$.nope".into(),
        }];
        assert!(run_decode(br#"{"a":1}"#, &steps).await.is_err());
    }

    #[tokio::test]
    async fn unzip_no_match_errors() {
        use std::io::Write;
        use zip::write::SimpleFileOptions;
        let mut cur = Cursor::new(Vec::new());
        {
            let mut zw = zip::ZipWriter::new(&mut cur);
            zw.start_file("a.txt", SimpleFileOptions::default())
                .unwrap();
            zw.write_all(b"x").unwrap();
            zw.finish().unwrap();
        }
        let steps = vec![DecodeStep::Unzip {
            unzip: UnzipSpec {
                member: Some("*.csv".into()),
            },
        }];
        assert!(run_decode(&cur.into_inner(), &steps).await.is_err());
    }

    #[test]
    fn decode_step_deserializes_mixed_forms() {
        let steps: Vec<DecodeStep> = serde_json::from_value(json!([
            "base64",
            "gunzip",
            { "extract": "$.x" },
            { "unzip": { "member": "*.csv" } },
            { "parse": { "format": "csv" } }
        ]))
        .unwrap();
        assert_eq!(steps.len(), 5);
        assert!(matches!(steps[0], DecodeStep::Simple(SimpleStep::Base64)));
        assert!(matches!(steps[1], DecodeStep::Simple(SimpleStep::Gunzip)));
        assert!(matches!(steps[2], DecodeStep::Extract { .. }));
        assert!(matches!(steps[3], DecodeStep::Unzip { .. }));
        assert!(matches!(steps[4], DecodeStep::Parse { .. }));
    }

    // ─── Unbalanced-XML frame guards (#654 L25) ─────────────────────────────

    /// An unbalanced extra `</x>` drains the synthetic root frame. That must be
    /// a typed error, never a panic — a panic here would take down a live
    /// pipeline on attacker-controlled input.
    #[test]
    fn top_frame_on_empty_stack_is_a_typed_error() {
        let mut empty: Vec<XmlFrame> = Vec::new();
        let err = top_frame(&mut empty, "text after the document root closed")
            .expect_err("an empty frame stack must error, not panic");
        let FaucetError::Source(msg) = err else {
            panic!("unbalanced XML must be FaucetError::Source, got {err:?}");
        };
        assert_eq!(
            msg,
            "decode `parse` xml: malformed XML: text after the document root closed; \
             the document has more closing than opening tags"
        );
    }

    #[test]
    fn pop_frame_on_empty_stack_is_a_typed_error() {
        let mut empty: Vec<XmlFrame> = Vec::new();
        let err = pop_frame(&mut empty, "unexpected end tag `</a>`")
            .expect_err("an empty frame stack must error, not panic");
        let FaucetError::Source(msg) = err else {
            panic!("unbalanced XML must be FaucetError::Source, got {err:?}");
        };
        assert_eq!(
            msg,
            "decode `parse` xml: malformed XML: unexpected end tag `</a>`; \
             the document has more closing than opening tags"
        );
    }

    #[test]
    fn frame_helpers_return_the_innermost_frame_when_present() {
        let mut stack: Vec<XmlFrame> =
            vec![(Map::new(), "root".into()), (Map::new(), "inner".into())];
        assert_eq!(
            top_frame(&mut stack, "unused").expect("frame present").1,
            "inner"
        );
        assert_eq!(
            pop_frame(&mut stack, "unused").expect("frame present").1,
            "inner"
        );
        assert_eq!(
            top_frame(&mut stack, "unused").expect("frame present").1,
            "root",
            "popping must expose the enclosing frame"
        );
    }

    #[test]
    fn xml_to_json_rejects_unbalanced_end_tag_without_panicking() {
        // `quick_xml`'s `check_end_names` currently rejects this before the
        // frame stack can underflow; either way the contract is the same — a
        // typed `Source` error naming the malformed document, no panic.
        let err =
            xml_to_json(b"<a><b>1</b></a></a>").expect_err("an extra closing tag must be rejected");
        assert!(
            matches!(err, FaucetError::Source(_)),
            "malformed XML must surface as FaucetError::Source, got {err:?}"
        );
    }

    #[test]
    fn xml_to_json_keeps_the_balanced_happy_path() {
        // Byte-parity guard: the typed-error refactor must not shift decoding.
        let v = xml_to_json(b"<r><a x=\"1\">hi</a><a>there</a></r>").expect("balanced XML decodes");
        assert_eq!(
            v,
            json!({"r": {"a": [{"@x": "1", "#text": "hi"}, "there"]}})
        );
    }
}
