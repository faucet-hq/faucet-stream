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

/// Every value stored under `key`, at any depth, flattened out of arrays, in
/// document order.
fn collect(v: &Value, key: &str, out: &mut Vec<Value>) {
    enum Work<'a> {
        Visit(&'a Value),
        Emit(&'a Value),
    }
    let mut pending = vec![Work::Visit(v)];
    while let Some(work) = pending.pop() {
        match work {
            Work::Emit(Value::Array(items)) => out.extend(items.iter().cloned()),
            Work::Emit(other) => out.push(other.clone()),
            Work::Visit(Value::Object(map)) => {
                for (k, child) in map.iter().rev() {
                    pending.push(if k == key {
                        Work::Emit(child)
                    } else {
                        Work::Visit(child)
                    });
                }
            }
            Work::Visit(Value::Array(items)) => {
                pending.extend(items.iter().rev().map(Work::Visit));
            }
            Work::Visit(_) => {}
        }
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

fn attrs(e: &BytesStart) -> Result<Map<String, Value>, FaucetError> {
    let mut m = Map::new();
    for a in e.attributes() {
        let a = a.map_err(|e| FaucetError::Source(format!("xml: malformed attribute: {e}")))?;
        let v = a
            .normalized_value(quick_xml::XmlVersion::Implicit1_0)
            .map_err(|e| {
                FaucetError::Source(format!("xml: attribute `{}`: {e}", a.key.as_ref()))
            })?;
        m.insert(
            format!("@{}", local(a.key.as_ref())),
            Value::String(v.into_owned()),
        );
    }
    Ok(m)
}

fn local(name: &str) -> String {
    name.rsplit(':').next().unwrap_or_default().to_string()
}

/// Resolve `&name;` (the five XML entities) or a character reference.
fn reference(r: &quick_xml::events::BytesRef<'_>) -> Result<String, FaucetError> {
    if let Some(ch) = r.resolve_char_ref().map_err(|e| {
        FaucetError::Source(format!(
            "xml: invalid character reference `&{};`: {e}",
            &**r
        ))
    })? {
        return Ok(ch.to_string());
    }
    quick_xml::escape::resolve_predefined_entity(r)
        .map(str::to_string)
        .ok_or_else(|| FaucetError::Source(format!("xml: undefined entity reference `&{};`", &**r)))
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

/// The deepest element nesting [`to_json`] accepts. Deeper documents are
/// refused with a typed error: the resulting value is nested once per level,
/// and walking or dropping an unbounded one would overflow the stack.
pub const MAX_XML_DEPTH: usize = 256;

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
                if names.len() >= MAX_XML_DEPTH {
                    return Err(FaucetError::Source(format!(
                        "xml: elements are nested more than {MAX_XML_DEPTH} levels deep"
                    )));
                }
                names.push(local(e.name().as_ref()));
                stack.push((attrs(&e)?, String::new()));
            }
            Event::Empty(e) => {
                let name = local(e.name().as_ref());
                let val = finish(attrs(&e)?, String::new());
                let t = top(&mut stack, "element after the document root closed")?;
                insert_child(&mut t.0, name, val);
            }
            Event::Text(t) => {
                top(&mut stack, "text after the document root closed")?
                    .1
                    .push_str(&t.xml10_content());
            }
            Event::CData(t) => {
                top(&mut stack, "CDATA after the document root closed")?
                    .1
                    .push_str(&t.xml10_content());
            }
            Event::GeneralRef(r) => {
                let s = reference(&r)?;
                top(&mut stack, "text after the document root closed")?
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
    fn many_attributes_parse_and_a_duplicate_name_is_refused() {
        // RUSTSEC-2026-0194 is fixed in quick-xml 0.41+, so the duplicate-name
        // check is back on: a repeated attribute is malformed XML, not "last wins".
        let many: String = (0..50_000).map(|i| format!(" a{i}=\"{i}\"")).collect();
        let xml = format!("<rows><row{many} id=\"1\"/></rows>");
        let rows = decode(xml.as_bytes(), "row").expect("decode");
        assert_eq!(rows[0]["@a49999"], json!("49999"));
        let err = decode(br#"<rows><row id="1" id="2"/></rows>"#, "row").expect_err("dup");
        assert!(err.to_string().contains("malformed attribute"), "{err}");
    }

    #[test]
    fn references_resolve_and_undefined_entities_fail() {
        let v = to_json(br#"<r><v a="A &amp; B">x &lt; &#65;<![CDATA[&z]]></v></r>"#).unwrap();
        assert_eq!(v["r"]["v"]["@a"], json!("A & B"));
        assert_eq!(v["r"]["v"]["#text"], json!("x < A&z"));
        let err = to_json(b"<r>&nbsp;</r>").expect_err("undefined");
        assert!(err.to_string().contains("undefined entity"), "{err}");
        let err = to_json(b"<r>&#0;</r>").expect_err("bad char ref");
        assert!(
            err.to_string().contains("invalid character reference"),
            "{err}"
        );
        let err = to_json(br#"<r a="&nbsp;"/>"#).expect_err("attr");
        assert!(err.to_string().contains("attribute `a`"), "{err}");
    }

    fn nested(depth: usize) -> Vec<u8> {
        format!("{}x{}", "<a>".repeat(depth), "</a>".repeat(depth)).into_bytes()
    }

    #[test]
    fn a_hostile_nesting_depth_is_refused_rather_than_overflowing_the_stack() {
        let err = decode(&nested(250_000), "row").expect_err("too deep");
        assert!(err.to_string().contains("nested more than"), "{err}");
    }

    #[test]
    fn nesting_up_to_the_limit_still_decodes() {
        let v = to_json(&nested(MAX_XML_DEPTH)).expect("at the limit");
        let mut cur = &v;
        for _ in 0..MAX_XML_DEPTH {
            cur = &cur["a"];
        }
        assert_eq!(cur, &json!("x"));
        assert!(to_json(&nested(MAX_XML_DEPTH + 1)).is_err());
    }

    fn collect_recursive(v: &Value, key: &str, out: &mut Vec<Value>) {
        match v {
            Value::Object(map) => {
                for (k, child) in map {
                    if k == key {
                        match child {
                            Value::Array(items) => out.extend(items.iter().cloned()),
                            other => out.push(other.clone()),
                        }
                    } else {
                        collect_recursive(child, key, out);
                    }
                }
            }
            Value::Array(items) => items.iter().for_each(|i| collect_recursive(i, key, out)),
            _ => {}
        }
    }

    #[test]
    fn the_iterative_walk_matches_a_recursive_one() {
        let xml = br#"<r><row><id>1</id></row><g><row><id>2</id><row><id>2.1</id></row></row><h><row><id>3</id></row></h></g><row><id>4</id></row><z><row><id>5</id></row></z></r>"#;
        let doc = to_json(xml).expect("json");
        let mut iterative = Vec::new();
        collect(&doc, "row", &mut iterative);
        let mut recursive = Vec::new();
        collect_recursive(&doc, "row", &mut recursive);
        assert_eq!(iterative.len(), 5);
        assert_eq!(iterative, recursive);
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
