//! XML read/write for the file connectors (#604).
//!
//! The decoder is the compact element→object mapping the REST source has used
//! since #515: each element becomes an object of its children, repeated child
//! tags become arrays, attributes are `@name`, and text is `#text` (or the
//! value directly when an element has only text). Namespaces are stripped to
//! their local name.
//!
//! XML has **no canonical record boundary**, so one is declared
//! ([`XmlOptions::record_element`](super::XmlOptions)). Selecting the wrong
//! element yields no records rather than the wrong ones, which is why the
//! decoder reports the elements it did see.

use crate::error::FaucetError;
use quick_xml::events::{BytesEnd, BytesStart, BytesText, Event};
use serde_json::{Map, Value};

/// Parse XML bytes into records.
///
/// `record_element` names the repeated element. When no element of that name
/// exists, the document root's direct children are used instead — right for the
/// common `<rows><row/>…</rows>` shape without forcing every config to spell it
/// out, and reported in the error when neither yields anything.
pub fn decode(bytes: &[u8], record_element: &str) -> Result<Vec<Value>, FaucetError> {
    let doc = to_json(bytes)?;
    let mut out = Vec::new();
    collect(&doc, record_element, &mut out);
    if !out.is_empty() {
        return Ok(out);
    }
    // Fall back to the root's children.
    match root_children(&doc) {
        Some(children) => Ok(children),
        // An empty document element is zero records, not an error: a sink that
        // wrote an empty page produces exactly this, and failing the read
        // would turn "no data" into a pipeline failure.
        None if is_empty_document(&doc) => Ok(Vec::new()),
        None => Err(FaucetError::Source(format!(
            "xml: no <{record_element}> elements, and the document root has no repeated child to \
             use instead — set `xml.record_element` to the element that delimits one record"
        ))),
    }
}

/// Every value stored under `key`, at any depth, flattened out of arrays.
fn collect(v: &Value, key: &str, out: &mut Vec<Value>) {
    match v {
        Value::Object(map) => {
            for (k, child) in map {
                if k == key {
                    match child {
                        Value::Array(items) => out.extend(items.iter().cloned()),
                        other => out.push(other.clone()),
                    }
                } else {
                    collect(child, key, out);
                }
            }
        }
        Value::Array(items) => {
            for i in items {
                collect(i, key, out);
            }
        }
        _ => {}
    }
}

/// Whether the document is a single element with no children and no text.
fn is_empty_document(doc: &Value) -> bool {
    let Some(root) = doc.as_object().and_then(|m| m.values().next()) else {
        return true;
    };
    match root {
        Value::String(s) => s.trim().is_empty(),
        Value::Object(map) => map.is_empty(),
        _ => false,
    }
}

/// The document root's children, when the root wraps exactly one repeated
/// element. Returns `None` for a root with several differently-named children,
/// where "the records" would be a guess.
fn root_children(doc: &Value) -> Option<Vec<Value>> {
    let root = doc.as_object()?.values().next()?;
    let map = root.as_object()?;
    let real: Vec<(&String, &Value)> = map
        .iter()
        .filter(|(k, _)| !k.starts_with('@') && *k != "#text")
        .collect();
    match real.as_slice() {
        [(_, Value::Array(items))] => Some(items.to_vec()),
        [(_, single)] => Some(vec![(*single).clone()]),
        _ => None,
    }
}

/// Any writer failure, whatever `quick_xml` calls it in this version.
fn err<E: std::fmt::Display>(e: E) -> FaucetError {
    FaucetError::Sink(format!("xml: {e}"))
}

/// Write records as XML: `<root><record>…</record>…</root>`.
///
/// Scalars become text elements; a nested object becomes a nested element; an
/// array becomes a repeated element, which is the shape [`decode`] reads back.
pub fn encode(
    records: &[Value],
    root_element: &str,
    record_element: &str,
) -> Result<Vec<u8>, FaucetError> {
    let mut w = quick_xml::Writer::new(Vec::new());
    w.write_event(Event::Start(BytesStart::new(root_element)))
        .map_err(err)?;
    for r in records {
        w.write_event(Event::Start(BytesStart::new(record_element)))
            .map_err(err)?;
        write_value(&mut w, r)?;
        w.write_event(Event::End(BytesEnd::new(record_element)))
            .map_err(err)?;
    }
    w.write_event(Event::End(BytesEnd::new(root_element)))
        .map_err(err)?;
    Ok(w.into_inner())
}

fn write_value(w: &mut quick_xml::Writer<Vec<u8>>, v: &Value) -> Result<(), FaucetError> {
    match v {
        Value::Object(map) => {
            for (k, child) in map {
                let name = sanitize(k);
                match child {
                    Value::Array(items) => {
                        for item in items {
                            w.write_event(Event::Start(BytesStart::new(&name)))
                                .map_err(err)?;
                            write_value(w, item)?;
                            w.write_event(Event::End(BytesEnd::new(&name)))
                                .map_err(err)?;
                        }
                    }
                    other => {
                        w.write_event(Event::Start(BytesStart::new(&name)))
                            .map_err(err)?;
                        write_value(w, other)?;
                        w.write_event(Event::End(BytesEnd::new(&name)))
                            .map_err(err)?;
                    }
                }
            }
            Ok(())
        }
        Value::Null => Ok(()),
        other => w
            .write_event(Event::Text(BytesText::new(&super::cell_text(other))))
            .map_err(err),
    }
}

/// A field name that is a legal XML element name.
///
/// JSON keys can hold spaces, slashes and leading digits; an element name
/// cannot. Substituting `_` keeps the document well-formed and the mapping
/// obvious, which beats failing the write on a field the user cannot rename.
fn sanitize(key: &str) -> String {
    let mut out = String::with_capacity(key.len());
    for (i, c) in key.chars().enumerate() {
        let ok = c.is_alphanumeric() || c == '_' || c == '-' || c == '.';
        let ok = ok && !(i == 0 && (c.is_numeric() || c == '-' || c == '.'));
        out.push(if ok { c } else { '_' });
    }
    if out.is_empty() { "field".into() } else { out }
}

/// One open element: its object and its accumulated text.
type Frame = (Map<String, Value>, String);

fn unbalanced(detail: &str) -> FaucetError {
    FaucetError::Source(format!(
        "xml: malformed XML: {detail}; the document has more closing than opening tags"
    ))
}

fn top<'a>(stack: &'a mut [Frame], detail: &str) -> Result<&'a mut Frame, FaucetError> {
    stack.last_mut().ok_or_else(|| unbalanced(detail))
}

fn pop(stack: &mut Vec<Frame>, detail: &str) -> Result<Frame, FaucetError> {
    stack.pop().ok_or_else(|| unbalanced(detail))
}

fn attrs(e: &BytesStart) -> Map<String, Value> {
    let mut m = Map::new();
    for a in e.attributes().flatten() {
        let k = local(a.key.as_ref());
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

/// Compact XML → JSON.
pub fn to_json(bytes: &[u8]) -> Result<Value, FaucetError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|e| FaucetError::Source(format!("xml: not UTF-8: {e}")))?;
    let mut reader = quick_xml::Reader::from_str(text);
    let mut stack: Vec<Frame> = vec![(Map::new(), String::new())];
    let mut names: Vec<String> = Vec::new();

    loop {
        match reader
            .read_event()
            .map_err(|e| FaucetError::Source(format!("xml: {e}")))?
        {
            Event::Eof => break,
            Event::Start(e) => {
                names.push(local(e.name().as_ref()));
                stack.push((attrs(&e), String::new()));
            }
            Event::Empty(e) => {
                let name = local(e.name().as_ref());
                let val = finish(attrs(&e), String::new());
                let t = top(&mut stack, "element after the document root closed")?;
                insert_child(&mut t.0, name, val);
            }
            Event::Text(t) => {
                let s = t
                    .unescape()
                    .map_err(|e| FaucetError::Source(format!("xml: {e}")))?
                    .to_string();
                top(&mut stack, "text after the document root closed")?
                    .1
                    .push_str(&s);
            }
            Event::CData(t) => {
                let s = String::from_utf8_lossy(t.as_ref()).to_string();
                top(&mut stack, "CDATA after the document root closed")?
                    .1
                    .push_str(&s);
            }
            Event::End(_) => {
                let (obj, text) = pop(&mut stack, "unmatched closing tag")?;
                let name = names
                    .pop()
                    .ok_or_else(|| unbalanced("unmatched closing tag"))?;
                let val = finish(obj, text);
                let parent = top(&mut stack, "unmatched closing tag")?;
                insert_child(&mut parent.0, name, val);
            }
            _ => {}
        }
    }
    let (root, _) = stack
        .pop()
        .ok_or_else(|| unbalanced("document root closed twice"))?;
    Ok(Value::Object(root))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_named_record_element_selects_the_records() {
        let xml = br#"<rows><row><id>1</id><n>a</n></row><row><id>2</id><n>b</n></row></rows>"#;
        assert_eq!(
            decode(xml, "row").expect("decode"),
            vec![json!({"id": "1", "n": "a"}), json!({"id": "2", "n": "b"})]
        );
    }

    #[test]
    fn a_single_record_is_still_a_list_of_one() {
        let xml = br#"<rows><row><id>1</id></row></rows>"#;
        assert_eq!(
            decode(xml, "row").expect("decode"),
            vec![json!({"id": "1"})]
        );
    }

    #[test]
    fn the_record_element_is_found_at_any_depth() {
        let xml = br#"<env><body><rows><row><id>1</id></row></rows></body></env>"#;
        assert_eq!(
            decode(xml, "row").expect("decode"),
            vec![json!({"id": "1"})]
        );
    }

    #[test]
    fn without_a_match_the_roots_children_are_used() {
        // `<rows><row/>…</rows>` read with the default `record` name still
        // works, rather than silently returning nothing.
        let xml = br#"<rows><row><id>1</id></row><row><id>2</id></row></rows>"#;
        assert_eq!(decode(xml, "record").expect("decode").len(), 2);
    }

    #[test]
    fn a_self_closing_element_and_cdata_decode() {
        // `Event::Empty` and `Event::CData` are separate arms from Start/Text.
        let v = to_json(br#"<r><e/><c><![CDATA[raw <>&]]></c><!-- ignored --></r>"#).expect("json");
        assert_eq!(v["r"]["e"], json!(""));
        assert_eq!(v["r"]["c"], json!("raw <>&"));
    }

    #[test]
    fn three_repeated_elements_accumulate_into_one_array() {
        // The second occurrence promotes the value to an array; the third
        // takes the push branch.
        let recs = decode(br#"<rs><r>a</r><r>b</r><r>c</r></rs>"#, "r").expect("decode");
        assert_eq!(recs, vec![json!("a"), json!("b"), json!("c")]);
    }

    #[test]
    fn a_root_wrapping_a_single_child_still_yields_one_record() {
        // `root_children` has a distinct arm for a lone non-array child.
        let recs = decode(br#"<rows><row><id>1</id></row></rows>"#, "nope").expect("decode");
        assert_eq!(recs, vec![json!({"id": "1"})]);
    }

    #[test]
    fn emptiness_is_judged_on_the_root_shape() {
        assert!(is_empty_document(&json!({})));
        assert!(is_empty_document(&json!({"r": ""})));
        assert!(is_empty_document(&json!({"r": "   "})));
        assert!(is_empty_document(&json!({"r": {}})));
        assert!(!is_empty_document(&json!({"r": "text"})));
        assert!(!is_empty_document(&json!({"r": {"a": 1}})));
        assert!(!is_empty_document(&json!({"r": [1]})));
    }

    #[test]
    fn the_writer_error_helper_is_prefixed() {
        assert_eq!(err("boom").to_string(), "Sink error: xml: boom");
    }

    #[test]
    fn an_empty_document_reads_back_as_zero_records_not_an_error() {
        let bytes = encode(&[], "records", "record").expect("encode");
        assert_eq!(
            String::from_utf8(bytes.clone()).unwrap(),
            "<records></records>"
        );
        assert!(decode(&bytes, "record").expect("decode").is_empty());
    }

    #[test]
    fn an_ambiguous_root_is_an_error_naming_the_option() {
        let xml = br#"<doc><a>1</a><b>2</b></doc>"#;
        let err = decode(xml, "record").expect_err("ambiguous");
        assert!(err.to_string().contains("xml.record_element"), "{err}");
    }

    #[test]
    fn attributes_text_and_namespaces_map_compactly() {
        let xml = br#"<r><x ns:k="v">t</x></r>"#;
        let v = to_json(xml).expect("json");
        assert_eq!(v["r"]["x"]["@k"], json!("v"));
        assert_eq!(v["r"]["x"]["#text"], json!("t"));
    }

    #[test]
    fn unbalanced_input_is_a_typed_error_not_a_panic() {
        // Untrusted input must never panic a live pipeline. `quick_xml`'s own
        // `check_end_names` usually rejects the mismatch first, so the message
        // is normally its — the guard here is that the frame-stack accesses
        // report rather than `.expect()`, whichever layer catches it.
        let err = to_json(b"<a></a></a>").expect_err("unbalanced");
        assert!(matches!(err, FaucetError::Source(_)), "{err}");
        assert!(unbalanced("x").to_string().contains("malformed XML"));
    }

    #[test]
    fn encode_round_trips_through_decode() {
        let recs = vec![json!({"id": "1", "n": "a"}), json!({"id": "2", "n": "b"})];
        let bytes = encode(&recs, "records", "record").expect("encode");
        assert_eq!(
            String::from_utf8(bytes.clone()).unwrap(),
            "<records><record><id>1</id><n>a</n></record><record><id>2</id><n>b</n></record></records>"
        );
        assert_eq!(decode(&bytes, "record").expect("decode"), recs);
    }

    #[test]
    fn an_array_field_becomes_a_repeated_element() {
        let bytes = encode(&[json!({"t": ["a", "b"]})], "rs", "r").expect("encode");
        assert_eq!(
            String::from_utf8(bytes).unwrap(),
            "<rs><r><t>a</t><t>b</t></r></rs>"
        );
    }

    #[test]
    fn a_key_that_is_not_a_legal_element_name_is_substituted_not_rejected() {
        let bytes = encode(&[json!({"a b/c": 1, "2x": 2})], "rs", "r").expect("encode");
        let s = String::from_utf8(bytes).unwrap();
        assert!(s.contains("<a_b_c>1</a_b_c>"), "{s}");
        assert!(s.contains("<_x>2</_x>"), "{s}");
    }

    #[test]
    fn a_null_field_writes_an_empty_element() {
        let bytes = encode(&[json!({"a": null})], "rs", "r").expect("encode");
        assert_eq!(String::from_utf8(bytes).unwrap(), "<rs><r><a></a></r></rs>");
    }
}
