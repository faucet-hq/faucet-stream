//! XML to JSON conversion.
//!
//! Converts XML documents to `serde_json::Value` preserving the element
//! hierarchy. Attributes are prefixed with `@`, text content uses `#text`.

use faucet_core::FaucetError;
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use serde_json::{Map, Value, json};

/// Convert an XML string to a JSON value.
///
/// Elements become objects, repeated elements become arrays, attributes
/// are stored with `@` prefix, and text content uses `#text`.
pub fn xml_to_json(xml: &str) -> Result<Value, FaucetError> {
    let mut reader = Reader::from_str(xml);
    let mut stack: Vec<(String, Map<String, Value>)> = vec![("$root".into(), Map::new())];

    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).into_owned();
                let mut obj = Map::new();

                // Collect attributes.
                for attr in e.attributes().with_checks(false).flatten() {
                    let key = format!("@{}", String::from_utf8_lossy(attr.key.as_ref()));
                    let val = String::from_utf8_lossy(&attr.value).into_owned();
                    obj.insert(key, Value::String(val));
                }

                stack.push((name, obj));
            }
            Ok(Event::End(_)) => {
                let (name, obj) = stack.pop().ok_or_else(|| {
                    FaucetError::Transform("malformed XML: unexpected end tag".into())
                })?;

                let value = if obj.len() == 1 && obj.contains_key("#text") {
                    // Simplify: element with only text becomes a string.
                    obj.into_iter().next().unwrap().1
                } else {
                    Value::Object(obj)
                };

                let parent = stack.last_mut().ok_or_else(|| {
                    FaucetError::Transform("malformed XML: no parent element".into())
                })?;

                // If the key already exists, convert to array.
                match parent.1.get_mut(&name) {
                    Some(Value::Array(arr)) => arr.push(value),
                    Some(existing) => {
                        let prev = existing.clone();
                        *existing = Value::Array(vec![prev, value]);
                    }
                    None => {
                        parent.1.insert(name, value);
                    }
                }
            }
            Ok(Event::Text(e)) => {
                let text = e
                    .unescape()
                    .map_err(|err| FaucetError::Transform(format!("XML decode error: {err}")))?
                    .trim()
                    .to_string();

                if !text.is_empty()
                    && let Some(current) = stack.last_mut()
                {
                    match current.1.get_mut("#text") {
                        Some(Value::String(s)) => {
                            s.push(' ');
                            s.push_str(&text);
                        }
                        _ => {
                            current.1.insert("#text".into(), Value::String(text));
                        }
                    }
                }
            }
            Ok(Event::CData(e)) => {
                // CDATA is literal (un-escaped) text that quick_xml emits as a
                // separate event; without this arm the content was silently
                // dropped — data loss for SOAP / feed APIs that wrap markup in
                // CDATA (audit #146 H15). Decode and append to `#text` exactly
                // like Event::Text.
                let text = e
                    .decode()
                    .map_err(|err| {
                        FaucetError::Transform(format!("XML CDATA decode error: {err}"))
                    })?
                    .trim()
                    .to_string();

                if !text.is_empty()
                    && let Some(current) = stack.last_mut()
                {
                    match current.1.get_mut("#text") {
                        Some(Value::String(s)) => {
                            s.push(' ');
                            s.push_str(&text);
                        }
                        _ => {
                            current.1.insert("#text".into(), Value::String(text));
                        }
                    }
                }
            }
            Ok(Event::Empty(e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).into_owned();
                let mut obj = Map::new();
                for attr in e.attributes().with_checks(false).flatten() {
                    let key = format!("@{}", String::from_utf8_lossy(attr.key.as_ref()));
                    let val = String::from_utf8_lossy(&attr.value).into_owned();
                    obj.insert(key, Value::String(val));
                }
                let value = if obj.is_empty() {
                    json!(null)
                } else {
                    Value::Object(obj)
                };

                if let Some(parent) = stack.last_mut() {
                    match parent.1.get_mut(&name) {
                        Some(Value::Array(arr)) => arr.push(value),
                        Some(existing) => {
                            let prev = existing.clone();
                            *existing = Value::Array(vec![prev, value]);
                        }
                        None => {
                            parent.1.insert(name, value);
                        }
                    }
                }
            }
            Ok(Event::Eof) => break,
            Ok(_) => {} // Skip comments, processing instructions, etc.
            Err(e) => {
                return Err(FaucetError::Transform(format!("XML parse error: {e}")));
            }
        }
    }

    let (_, root) = stack
        .pop()
        .ok_or_else(|| FaucetError::Transform("empty XML document".into()))?;

    Ok(Value::Object(root))
}

/// Walk an XML document with `quick_xml::Reader::read_event` and invoke
/// `on_record` once per element whose path matches the dot-separated
/// `records_element_path` selector. Records are materialised as JSON values
/// in the same shape `xml_to_json` would produce — attributes become `@key`
/// entries, repeated children become arrays, and a single `#text` child is
/// flattened to a bare string.
///
/// When `records_element_path` is `None` the entire document is emitted as
/// a single record (matches the eager `xml_to_json` behaviour).
///
/// The key difference from `xml_to_json` is that subtree JSON values are
/// only materialised while inside a matched element — surrounding elements
/// are observed via the event stream but never accumulated, which bounds
/// memory to one matched element + the path stack regardless of total
/// document size. Combined with batched yielding in
/// [`crate::stream::XmlStream`]'s `stream_pages`, this keeps client-side
/// memory at `O(batch_size * record_size)` even for multi-gigabyte
/// payloads.
pub fn stream_extract<F: FnMut(Value)>(
    xml: &str,
    records_element_path: Option<&str>,
    mut on_record: F,
) -> Result<(), FaucetError> {
    let target_segments: Option<Vec<&str>> = records_element_path.map(|p| p.split('.').collect());

    let mut reader = Reader::from_str(xml);

    // Current element path: outer-most → inner-most element name.
    let mut path: Vec<String> = Vec::new();

    // When `Some(start_depth)`, we are currently building a subtree rooted
    // at the element opened at `path[start_depth]`. The subtree stack
    // mirrors `xml_to_json`'s stack but is rooted at the matched element
    // rather than the document.
    let mut start_depth: Option<usize> = None;
    let mut subtree: Vec<(String, Map<String, Value>)> = Vec::new();

    // When `records_element_path` is None, we eagerly build the whole
    // document and emit it as one record on EOF. This preserves the
    // historical "no path = full doc" behaviour.
    let mut full_doc: Option<Vec<(String, Map<String, Value>)>> = if target_segments.is_none() {
        Some(vec![("$root".into(), Map::new())])
    } else {
        None
    };

    /// Returns true when the current open-element path matches the target
    /// dot-path selector exactly (i.e. the element just opened is the
    /// repeating record element).
    fn path_matches(path: &[String], target: &[&str]) -> bool {
        path.len() == target.len() && path.iter().zip(target).all(|(a, b)| a.as_str() == *b)
    }

    /// Append a child value under `name` to the topmost frame, converting to
    /// an array on repetition (mirrors `xml_to_json`).
    fn append_child(parent: &mut Map<String, Value>, name: String, value: Value) {
        match parent.get_mut(&name) {
            Some(Value::Array(arr)) => arr.push(value),
            Some(existing) => {
                let prev = existing.clone();
                *existing = Value::Array(vec![prev, value]);
            }
            None => {
                parent.insert(name, value);
            }
        }
    }

    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).into_owned();
                let mut obj = Map::new();
                for attr in e.attributes().with_checks(false).flatten() {
                    let key = format!("@{}", String::from_utf8_lossy(attr.key.as_ref()));
                    let val = String::from_utf8_lossy(&attr.value).into_owned();
                    obj.insert(key, Value::String(val));
                }

                path.push(name.clone());

                if let Some(doc) = full_doc.as_mut() {
                    doc.push((name, obj));
                } else if let Some(target) = target_segments.as_deref() {
                    if start_depth.is_some() {
                        subtree.push((name, obj));
                    } else if path_matches(&path, target) {
                        // Opening the matched element itself — start a new
                        // subtree builder rooted at it.
                        start_depth = Some(path.len() - 1);
                        subtree.push((name, obj));
                    }
                    // Otherwise: outside any matched element — drop the
                    // event without materialising anything.
                }
            }
            Ok(Event::Empty(e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).into_owned();
                let mut obj = Map::new();
                for attr in e.attributes().with_checks(false).flatten() {
                    let key = format!("@{}", String::from_utf8_lossy(attr.key.as_ref()));
                    let val = String::from_utf8_lossy(&attr.value).into_owned();
                    obj.insert(key, Value::String(val));
                }
                let value = if obj.is_empty() {
                    json!(null)
                } else {
                    Value::Object(obj)
                };

                // Treat self-closing tag as a transient open+close at the
                // current depth.
                path.push(name.clone());
                let matches_target = target_segments
                    .as_deref()
                    .map(|t| path_matches(&path, t))
                    .unwrap_or(false);
                path.pop();

                if let Some(doc) = full_doc.as_mut() {
                    if let Some(parent) = doc.last_mut() {
                        append_child(&mut parent.1, name, value);
                    }
                } else if matches_target && start_depth.is_none() {
                    // Self-closing matched element: emit immediately.
                    on_record(value);
                } else if start_depth.is_some()
                    && let Some(parent) = subtree.last_mut()
                {
                    append_child(&mut parent.1, name, value);
                }
            }
            Ok(Event::End(_)) => {
                let name = path.pop().ok_or_else(|| {
                    FaucetError::Transform("malformed XML: unexpected end tag".into())
                })?;

                if let Some(doc) = full_doc.as_mut() {
                    let (popped_name, obj) = doc.pop().ok_or_else(|| {
                        FaucetError::Transform("malformed XML: no element on stack".into())
                    })?;
                    debug_assert_eq!(popped_name, name);
                    let value = if obj.len() == 1 && obj.contains_key("#text") {
                        obj.into_iter().next().unwrap().1
                    } else {
                        Value::Object(obj)
                    };
                    let parent = doc.last_mut().ok_or_else(|| {
                        FaucetError::Transform("malformed XML: no parent element".into())
                    })?;
                    append_child(&mut parent.1, popped_name, value);
                } else if let Some(depth) = start_depth {
                    let (popped_name, obj) = subtree.pop().ok_or_else(|| {
                        FaucetError::Transform("malformed XML: no element on subtree stack".into())
                    })?;
                    debug_assert_eq!(popped_name, name);
                    let value = if obj.len() == 1 && obj.contains_key("#text") {
                        obj.into_iter().next().unwrap().1
                    } else {
                        Value::Object(obj)
                    };

                    if subtree.is_empty() {
                        // We just closed the matched element itself —
                        // emit and reset.
                        debug_assert_eq!(path.len(), depth);
                        start_depth = None;
                        on_record(value);
                    } else if let Some(parent) = subtree.last_mut() {
                        append_child(&mut parent.1, popped_name, value);
                    }
                }
                // Outside any matched element and no full-doc mode: drop.
            }
            Ok(Event::Text(e)) => {
                let text = e
                    .unescape()
                    .map_err(|err| FaucetError::Transform(format!("XML decode error: {err}")))?
                    .trim()
                    .to_string();
                if text.is_empty() {
                    continue;
                }

                if let Some(doc) = full_doc.as_mut() {
                    if let Some(current) = doc.last_mut() {
                        match current.1.get_mut("#text") {
                            Some(Value::String(s)) => {
                                s.push(' ');
                                s.push_str(&text);
                            }
                            _ => {
                                current.1.insert("#text".into(), Value::String(text));
                            }
                        }
                    }
                } else if start_depth.is_some()
                    && let Some(current) = subtree.last_mut()
                {
                    match current.1.get_mut("#text") {
                        Some(Value::String(s)) => {
                            s.push(' ');
                            s.push_str(&text);
                        }
                        _ => {
                            current.1.insert("#text".into(), Value::String(text));
                        }
                    }
                }
            }
            Ok(Event::CData(e)) => {
                // CDATA is literal text emitted as its own event; capture it
                // instead of dropping it — data loss for CDATA-wrapped markup
                // (audit #146 H15). Decode and append to `#text` like Text.
                let text = e
                    .decode()
                    .map_err(|err| {
                        FaucetError::Transform(format!("XML CDATA decode error: {err}"))
                    })?
                    .trim()
                    .to_string();
                if text.is_empty() {
                    continue;
                }
                if let Some(doc) = full_doc.as_mut() {
                    if let Some(current) = doc.last_mut() {
                        match current.1.get_mut("#text") {
                            Some(Value::String(s)) => {
                                s.push(' ');
                                s.push_str(&text);
                            }
                            _ => {
                                current.1.insert("#text".into(), Value::String(text));
                            }
                        }
                    }
                } else if start_depth.is_some()
                    && let Some(current) = subtree.last_mut()
                {
                    match current.1.get_mut("#text") {
                        Some(Value::String(s)) => {
                            s.push(' ');
                            s.push_str(&text);
                        }
                        _ => {
                            current.1.insert("#text".into(), Value::String(text));
                        }
                    }
                }
            }
            Ok(Event::Eof) => break,
            Ok(_) => {} // Comments, PIs, etc.
            Err(e) => {
                return Err(FaucetError::Transform(format!("XML parse error: {e}")));
            }
        }
    }

    if let Some(mut doc) = full_doc {
        let (_, root) = doc
            .pop()
            .ok_or_else(|| FaucetError::Transform("empty XML document".into()))?;
        on_record(Value::Object(root));
    }

    Ok(())
}

/// Navigate into a JSON value using a dot-separated path and extract
/// matching records. If the final element is an array, its items are
/// returned individually.
pub fn extract_at_path(value: &Value, path: &str) -> Vec<Value> {
    let segments: Vec<&str> = path.split('.').collect();
    let mut current = value.clone();

    for seg in &segments {
        current = match current {
            Value::Object(ref map) => match map.get(*seg) {
                Some(v) => v.clone(),
                None => return vec![],
            },
            _ => return vec![],
        };
    }

    match current {
        Value::Array(arr) => arr,
        other => vec![other],
    }
}

/// [`extract_at_path`] where each segment matches its element exactly or, when
/// no key matches exactly, by local name — so `Envelope.Body.GetUsersResponse`
/// finds `soap:Envelope.soap:Body.ns1:GetUsersResponse` (#789 API-08).
pub fn extract_at_path_by_local(value: &Value, path: &str) -> Vec<Value> {
    let mut current = value;
    for seg in path.split('.') {
        current = match child_exact_or_local(current, seg) {
            Some(v) => v,
            None => return vec![],
        };
    }
    match current {
        Value::Array(arr) => arr.clone(),
        other => vec![other.clone()],
    }
}

fn child_exact_or_local<'a>(value: &'a Value, seg: &str) -> Option<&'a Value> {
    let map = value.as_object()?;
    map.get(seg)
        .or_else(|| find_child_by_local(value, local_name(seg)))
}

/// Extract the records of a SOAP response. With `relative_to_body` the path is
/// resolved under the envelope's `Body`, found by local name whatever its
/// namespace prefix. A response with no `Envelope`/`Body`, or whose body lacks
/// the path's first element (the operation's response wrapper, always present
/// on a successful reply), is an error rather than zero records; a deeper
/// element that is absent is an empty result.
pub fn extract_soap_records(
    doc: &Value,
    path: &str,
    relative_to_body: bool,
) -> Result<Vec<Value>, faucet_core::FaucetError> {
    if !relative_to_body {
        return Ok(extract_at_path_by_local(doc, path));
    }
    let body = find_child_by_local(doc, "Envelope")
        .and_then(|env| find_child_by_local(env, "Body"))
        .ok_or_else(|| {
            faucet_core::FaucetError::Source(
                "xml: the SOAP response has no Envelope/Body element".into(),
            )
        })?;
    let first = path.split('.').next().unwrap_or(path);
    if child_exact_or_local(body, first).is_none() {
        return Err(faucet_core::FaucetError::Source(format!(
            "xml: the SOAP body has no `{first}` element (records_element_path `{path}`); \
             check the response element name"
        )));
    }
    Ok(extract_at_path_by_local(body, path))
}

/// The local name of an element key — the part after the last `:`, so a
/// namespace-prefixed element like `soap:Body` matches on `Body`.
fn local_name(key: &str) -> &str {
    match key.rsplit_once(':') {
        Some((_, local)) => local,
        None => key,
    }
}

/// Find the first child of `value` whose element key has the given local name
/// (namespace-prefix-insensitive). Returns the child value.
fn find_child_by_local<'a>(value: &'a Value, local: &str) -> Option<&'a Value> {
    value
        .as_object()?
        .iter()
        .find_map(|(k, v)| (local_name(k) == local).then_some(v))
}

/// Best-effort human-readable text of a SOAP fault subtree: the SOAP 1.1
/// `faultstring`, else the SOAP 1.2 `Reason`/`Text`, else the whole subtree.
fn fault_message(fault: &Value) -> String {
    // A repeated <Fault> collapses to an array; inspect the first.
    let fault = match fault {
        Value::Array(items) => items.first().unwrap_or(fault),
        other => other,
    };
    // SOAP 1.1: <faultstring>msg</faultstring>.
    if let Some(fs) = find_child_by_local(fault, "faultstring") {
        return value_to_text(fs);
    }
    // SOAP 1.2: <Reason><Text>msg</Text></Reason>.
    if let Some(reason) = find_child_by_local(fault, "Reason") {
        if let Some(text) = find_child_by_local(reason, "Text") {
            return value_to_text(text);
        }
        return value_to_text(reason);
    }
    value_to_text(fault)
}

/// Flatten a converted-XML value to its text: a bare string, an element's
/// `#text`, else the serialized JSON.
fn value_to_text(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Object(map) => map
            .get("#text")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| value.to_string()),
        other => other.to_string(),
    }
}

/// Detect a SOAP `<Fault>` under `Envelope.Body` in a converted document and
/// return its message. Matching is namespace-prefix-insensitive (a fault under
/// `soap:Envelope.soap:Body.soap:Fault` and one under a default-namespaced
/// `Envelope.Body.Fault` are both detected). Returns `None` when there is no
/// fault (the normal success path).
pub fn detect_soap_fault(doc: &Value) -> Option<String> {
    let envelope = find_child_by_local(doc, "Envelope")?;
    let body = find_child_by_local(envelope, "Body")?;
    let fault = find_child_by_local(body, "Fault")?;
    Some(fault_message(fault))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_xml_to_json() {
        let xml = r#"<root><name>Alice</name><age>30</age></root>"#;
        let json = xml_to_json(xml).unwrap();
        assert_eq!(json["root"]["name"], "Alice");
        assert_eq!(json["root"]["age"], "30");
    }

    #[test]
    fn repeated_elements_become_array() {
        let xml = r#"<root><item>a</item><item>b</item><item>c</item></root>"#;
        let json = xml_to_json(xml).unwrap();
        let items = json["root"]["item"].as_array().unwrap();
        assert_eq!(items.len(), 3);
        assert_eq!(items[0], "a");
        assert_eq!(items[1], "b");
    }

    #[test]
    fn attributes_prefixed() {
        let xml = r#"<user id="42"><name>Bob</name></user>"#;
        let json = xml_to_json(xml).unwrap();
        assert_eq!(json["user"]["@id"], "42");
        assert_eq!(json["user"]["name"], "Bob");
    }

    #[test]
    fn nested_elements() {
        let xml = r#"<root><user><address><city>NYC</city></address></user></root>"#;
        let json = xml_to_json(xml).unwrap();
        assert_eq!(json["root"]["user"]["address"]["city"], "NYC");
    }

    #[test]
    fn cdata_content_is_captured_not_dropped() {
        // H15 (audit #146): quick_xml emits CDATA as a separate event; it must
        // be captured into #text, not silently dropped (it was, before the fix).
        let xml = r#"<root><body><![CDATA[<b>hi</b> & bye]]></body></root>"#;
        let json = xml_to_json(xml).unwrap();
        assert_eq!(json["root"]["body"], "<b>hi</b> & bye");
    }

    #[test]
    fn cdata_content_captured_in_streaming_path() {
        // H15: the streaming converter must also capture CDATA.
        let xml = r#"<feed><item><html><![CDATA[<p>x</p>]]></html></item></feed>"#;
        let recs = collect_stream_extract(xml, Some("feed.item"));
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0]["html"], "<p>x</p>");
    }

    #[test]
    fn empty_elements() {
        let xml = r#"<root><flag/></root>"#;
        let json = xml_to_json(xml).unwrap();
        assert!(json["root"]["flag"].is_null());
    }

    #[test]
    fn empty_element_with_attr() {
        let xml = r#"<root><flag enabled="true"/></root>"#;
        let json = xml_to_json(xml).unwrap();
        assert_eq!(json["root"]["flag"]["@enabled"], "true");
    }

    #[test]
    fn extract_at_path_nested() {
        let val = json!({"root": {"users": {"user": [{"id": 1}, {"id": 2}]}}});
        let records = extract_at_path(&val, "root.users.user");
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["id"], 1);
    }

    #[test]
    fn extract_at_path_single_element() {
        let val = json!({"root": {"user": {"id": 1}}});
        let records = extract_at_path(&val, "root.user");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["id"], 1);
    }

    #[test]
    fn extract_at_path_missing() {
        let val = json!({"root": {}});
        let records = extract_at_path(&val, "root.users.user");
        assert!(records.is_empty());
    }

    #[test]
    fn soap_envelope() {
        let xml = r#"
        <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
            <soap:Body>
                <GetUsersResponse>
                    <User><Name>Alice</Name></User>
                    <User><Name>Bob</Name></User>
                </GetUsersResponse>
            </soap:Body>
        </soap:Envelope>"#;
        let json = xml_to_json(xml).unwrap();
        let users = extract_at_path(&json, "soap:Envelope.soap:Body.GetUsersResponse.User");
        assert_eq!(users.len(), 2);
    }

    fn collect_stream_extract(xml: &str, path: Option<&str>) -> Vec<Value> {
        let mut out = Vec::new();
        stream_extract(xml, path, |v| out.push(v)).unwrap();
        out
    }

    #[test]
    fn stream_extract_matches_eager_path_extraction() {
        let xml = r#"<root>
            <user id="1"><name>Alice</name><age>30</age></user>
            <user id="2"><name>Bob</name><age>25</age></user>
            <user id="3"><name>Carol</name><age>40</age></user>
        </root>"#;
        let streamed = collect_stream_extract(xml, Some("root.user"));
        let eager = extract_at_path(&xml_to_json(xml).unwrap(), "root.user");
        assert_eq!(streamed, eager);
        assert_eq!(streamed.len(), 3);
        assert_eq!(streamed[0]["@id"], "1");
        assert_eq!(streamed[0]["name"], "Alice");
        assert_eq!(streamed[2]["name"], "Carol");
    }

    #[test]
    fn stream_extract_handles_nested_children_and_attrs() {
        let xml = r#"<root>
            <order id="A"><line><sku>X</sku><qty>2</qty></line><line><sku>Y</sku><qty>5</qty></line></order>
            <order id="B"><line><sku>Z</sku><qty>1</qty></line></order>
        </root>"#;
        let streamed = collect_stream_extract(xml, Some("root.order"));
        let eager = extract_at_path(&xml_to_json(xml).unwrap(), "root.order");
        assert_eq!(streamed, eager);
        assert_eq!(streamed.len(), 2);
        let lines = streamed[0]["line"].as_array().expect("repeated children");
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[1]["sku"], "Y");
    }

    #[test]
    fn stream_extract_no_path_returns_full_doc_once() {
        let xml = r#"<root><a>1</a><b>2</b></root>"#;
        let streamed = collect_stream_extract(xml, None);
        let eager = xml_to_json(xml).unwrap();
        assert_eq!(streamed.len(), 1);
        assert_eq!(streamed[0], eager);
    }

    #[test]
    fn stream_extract_no_matches_emits_nothing() {
        let xml = r#"<root><a>1</a></root>"#;
        let streamed = collect_stream_extract(xml, Some("root.missing"));
        assert!(streamed.is_empty());
    }

    #[test]
    fn stream_extract_self_closing_matched_element() {
        let xml = r#"<root><item id="1"/><item id="2"/><item id="3"/></root>"#;
        let streamed = collect_stream_extract(xml, Some("root.item"));
        assert_eq!(streamed.len(), 3);
        assert_eq!(streamed[0]["@id"], "1");
        assert_eq!(streamed[2]["@id"], "3");
    }

    #[test]
    fn stream_extract_preserves_soap_namespaces() {
        let xml = r#"
        <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
            <soap:Body>
                <GetUsersResponse>
                    <User><Name>Alice</Name></User>
                    <User><Name>Bob</Name></User>
                </GetUsersResponse>
            </soap:Body>
        </soap:Envelope>"#;
        let streamed =
            collect_stream_extract(xml, Some("soap:Envelope.soap:Body.GetUsersResponse.User"));
        let eager = extract_at_path(
            &xml_to_json(xml).unwrap(),
            "soap:Envelope.soap:Body.GetUsersResponse.User",
        );
        assert_eq!(streamed, eager);
        assert_eq!(streamed.len(), 2);
        assert_eq!(streamed[1]["Name"], "Bob");
    }

    #[test]
    fn xml_to_json_mixed_text_and_children_keeps_both() {
        // An element with both text and a child element is NOT flattened to a
        // bare string (obj.len() != 1), so #text and the child coexist.
        let xml = r#"<root><p>hello<b>bold</b></p></root>"#;
        let json = xml_to_json(xml).unwrap();
        assert_eq!(json["root"]["p"]["#text"], "hello");
        assert_eq!(json["root"]["p"]["b"], "bold");
    }

    #[test]
    fn xml_to_json_text_split_by_entity_is_concatenated() {
        // An entity reference (&amp;) splits the character data into two
        // Text events on the same element; the second event hits the
        // "#text already a String" branch and appends with a space.
        let xml = r#"<root><msg>foo &amp; bar</msg></root>"#;
        let json = xml_to_json(xml).unwrap();
        assert_eq!(json["root"]["msg"], "foo & bar");
    }

    #[test]
    fn xml_to_json_text_then_cdata_concatenated() {
        // Leading plain text followed by a CDATA block on the same element:
        // the CDATA arm appends to the existing #text String.
        let xml = r#"<root><note>before <![CDATA[<raw>]]></note></root>"#;
        let json = xml_to_json(xml).unwrap();
        assert_eq!(json["root"]["note"], "before <raw>");
    }

    #[test]
    fn xml_to_json_repeated_empty_elements_become_array() {
        // Two self-closing tags with the same name under one parent: the
        // second Empty event converts the first scalar value into an array.
        let xml = r#"<root><flag/><flag/></root>"#;
        let json = xml_to_json(xml).unwrap();
        let flags = json["root"]["flag"].as_array().expect("repeated empties");
        assert_eq!(flags.len(), 2);
        assert!(flags[0].is_null());
        assert!(flags[1].is_null());
    }

    #[test]
    fn xml_to_json_three_repeated_empty_elements_push_onto_array() {
        // A third same-named empty element pushes onto the already-array
        // value (the `Some(Value::Array(arr)) => arr.push(value)` arm).
        let xml = r#"<root><flag a="1"/><flag a="2"/><flag a="3"/></root>"#;
        let json = xml_to_json(xml).unwrap();
        let flags = json["root"]["flag"].as_array().expect("repeated empties");
        assert_eq!(flags.len(), 3);
        assert_eq!(flags[2]["@a"], "3");
    }

    #[test]
    fn xml_to_json_skips_comments_and_processing_instructions() {
        // Comments and PIs hit the `Ok(_) => {}` skip arm; the surrounding
        // data must still parse correctly.
        let xml = r#"<?xml version="1.0"?><root><!-- a comment --><name>X</name></root>"#;
        let json = xml_to_json(xml).unwrap();
        assert_eq!(json["root"]["name"], "X");
    }

    #[test]
    fn xml_to_json_malformed_returns_parse_error() {
        // A mismatched end tag is rejected by quick_xml's end-name check,
        // surfacing as FaucetError::Transform via the `Err(e)` arm.
        let xml = r#"<root><a></b></root>"#;
        let err = xml_to_json(xml).unwrap_err();
        assert!(
            matches!(&err, FaucetError::Transform(m) if m.contains("XML parse error")),
            "got {err:?}"
        );
    }

    #[test]
    fn extract_at_path_descending_into_scalar_returns_empty() {
        // The path tries to descend past a scalar leaf, hitting the
        // `_ => return vec![]` non-object arm.
        let val = json!({"root": {"name": "Alice"}});
        let records = extract_at_path(&val, "root.name.first");
        assert!(records.is_empty());
    }

    #[test]
    fn extract_at_path_scalar_root_returns_empty() {
        // The very first segment lookup is against a non-object value.
        let val = json!("just a string");
        let records = extract_at_path(&val, "anything");
        assert!(records.is_empty());
    }

    #[test]
    fn stream_extract_full_doc_mode_self_closing_and_repeats() {
        // No path => full_doc mode. Exercises the Empty arm under full_doc
        // (append_child into doc.last_mut), including repetition → array.
        let xml = r#"<root><flag/><flag/><name>Z</name></root>"#;
        let streamed = collect_stream_extract(xml, None);
        let eager = xml_to_json(xml).unwrap();
        assert_eq!(streamed.len(), 1);
        assert_eq!(streamed[0], eager);
        let flags = streamed[0]["root"]["flag"]
            .as_array()
            .expect("repeated empties in full-doc mode");
        assert_eq!(flags.len(), 2);
        assert_eq!(streamed[0]["root"]["name"], "Z");
    }

    #[test]
    fn stream_extract_full_doc_mode_mixed_text_and_cdata() {
        // full_doc mode: text then CDATA on the same element appends to the
        // existing #text String (the full_doc Text + CData arms).
        let xml = r#"<root><note>hi &amp; <![CDATA[<x>]]></note></root>"#;
        let streamed = collect_stream_extract(xml, None);
        let eager = xml_to_json(xml).unwrap();
        assert_eq!(streamed.len(), 1);
        assert_eq!(streamed[0], eager);
        assert_eq!(streamed[0]["root"]["note"], "hi & <x>");
    }

    #[test]
    fn stream_extract_subtree_self_closing_child_appends() {
        // A self-closing child inside a matched subtree exercises the
        // `start_depth.is_some()` Empty append_child branch.
        let xml = r#"<root>
            <user id="1"><active/><name>Alice</name></user>
            <user id="2"><active/><name>Bob</name></user>
        </root>"#;
        let streamed = collect_stream_extract(xml, Some("root.user"));
        let eager = extract_at_path(&xml_to_json(xml).unwrap(), "root.user");
        assert_eq!(streamed, eager);
        assert_eq!(streamed.len(), 2);
        assert!(streamed[0]["active"].is_null());
        assert_eq!(streamed[0]["name"], "Alice");
    }

    #[test]
    fn stream_extract_subtree_text_split_by_entity_concatenated() {
        // Entity-split text inside a matched subtree hits the subtree
        // "#text already a String" append branch.
        let xml = r#"<root><item><msg>a &amp; b</msg></item></root>"#;
        let streamed = collect_stream_extract(xml, Some("root.item"));
        assert_eq!(streamed.len(), 1);
        assert_eq!(streamed[0]["msg"], "a & b");
    }

    #[test]
    fn stream_extract_subtree_text_then_cdata_concatenated() {
        // Text then CDATA inside a matched subtree appends CDATA onto the
        // existing #text String (subtree CData "already a String" branch).
        let xml = r#"<root><item><note>start <![CDATA[<end>]]></note></item></root>"#;
        let streamed = collect_stream_extract(xml, Some("root.item"));
        assert_eq!(streamed.len(), 1);
        assert_eq!(streamed[0]["note"], "start <end>");
    }

    #[test]
    fn stream_extract_repeated_children_push_onto_existing_array() {
        // Three same-named children inside a matched element exercise the
        // append_child `Some(Value::Array(arr)) => arr.push` arm.
        let xml = r#"<root><order><line>a</line><line>b</line><line>c</line></order></root>"#;
        let streamed = collect_stream_extract(xml, Some("root.order"));
        assert_eq!(streamed.len(), 1);
        let lines = streamed[0]["line"].as_array().expect("repeated children");
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[2], "c");
    }

    #[test]
    fn stream_extract_malformed_returns_parse_error() {
        // A mismatched end tag surfaces via the streaming parser's Err arm.
        let xml = r#"<root><a></b></root>"#;
        let mut out = Vec::new();
        let err = stream_extract(xml, Some("root.a"), |v| out.push(v)).unwrap_err();
        assert!(
            matches!(&err, FaucetError::Transform(m) if m.contains("XML parse error")),
            "got {err:?}"
        );
    }

    #[test]
    fn soap_records_resolve_through_prefixes_and_fail_without_the_wrapper() {
        let doc = json!({"soap:Envelope": {"soap:Body": {"ns1:Resp": {"ns1:Item": [{"id": 1}, {"id": 2}]}}}});
        assert_eq!(
            extract_soap_records(&doc, "Resp.Item", true).unwrap().len(),
            2
        );
        assert_eq!(
            extract_soap_records(&doc, "Resp.Missing", true).unwrap(),
            Vec::<Value>::new(),
            "an absent inner element is an empty result"
        );
        let err = extract_soap_records(&doc, "Other.Item", true).unwrap_err();
        assert!(err.to_string().contains("`Other`"), "{err}");
        let err = extract_soap_records(&json!({"root": {}}), "Resp", true).unwrap_err();
        assert!(err.to_string().contains("Envelope/Body"), "{err}");
        assert_eq!(
            extract_soap_records(&doc, "Envelope.Body.Resp.Item", false)
                .unwrap()
                .len(),
            2,
            "an absolute path also matches prefixed segments"
        );
        let exact = json!({"a": {"x:b": 1, "b": 2}});
        assert_eq!(
            extract_at_path_by_local(&exact, "a.b"),
            vec![json!(2)],
            "exact key wins"
        );
    }

    #[test]
    fn local_name_strips_namespace_prefix() {
        assert_eq!(local_name("soap:Body"), "Body");
        assert_eq!(local_name("Body"), "Body");
        assert_eq!(local_name("a:b:c"), "c");
    }

    #[test]
    fn detect_soap_fault_soap11_prefixed() {
        let xml = r#"<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
            <soap:Body>
                <soap:Fault>
                    <faultcode>soap:Server</faultcode>
                    <faultstring>Something went wrong</faultstring>
                </soap:Fault>
            </soap:Body>
        </soap:Envelope>"#;
        let doc = xml_to_json(xml).unwrap();
        assert_eq!(
            detect_soap_fault(&doc).as_deref(),
            Some("Something went wrong")
        );
    }

    #[test]
    fn detect_soap_fault_soap11_default_namespace() {
        // A default-namespaced envelope (unprefixed element names).
        let xml = r#"<Envelope xmlns="http://schemas.xmlsoap.org/soap/envelope/">
            <Body>
                <Fault>
                    <faultcode>Server</faultcode>
                    <faultstring>boom</faultstring>
                </Fault>
            </Body>
        </Envelope>"#;
        let doc = xml_to_json(xml).unwrap();
        assert_eq!(detect_soap_fault(&doc).as_deref(), Some("boom"));
    }

    #[test]
    fn detect_soap_fault_soap12_reason_text() {
        let xml = r#"<env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope">
            <env:Body>
                <env:Fault>
                    <env:Code><env:Value>env:Receiver</env:Value></env:Code>
                    <env:Reason><env:Text xml:lang="en">server exploded</env:Text></env:Reason>
                </env:Fault>
            </env:Body>
        </env:Envelope>"#;
        let doc = xml_to_json(xml).unwrap();
        assert_eq!(detect_soap_fault(&doc).as_deref(), Some("server exploded"));
    }

    #[test]
    fn detect_soap_fault_returns_none_on_success_response() {
        let xml = r#"<Envelope><Body>
            <GetUsersResponse><User><Name>Alice</Name></User></GetUsersResponse>
        </Body></Envelope>"#;
        let doc = xml_to_json(xml).unwrap();
        assert!(detect_soap_fault(&doc).is_none());
    }

    #[test]
    fn detect_soap_fault_returns_none_when_no_envelope() {
        let doc = xml_to_json("<root><item>a</item></root>").unwrap();
        assert!(detect_soap_fault(&doc).is_none());
    }

    #[test]
    fn stream_extract_skips_comments_outside_match() {
        // Comments hit the `Ok(_) => {}` arm; surrounding records still emit.
        let xml = r#"<root><!-- c --><item><v>1</v></item><!-- d --><item><v>2</v></item></root>"#;
        let streamed = collect_stream_extract(xml, Some("root.item"));
        assert_eq!(streamed.len(), 2);
        assert_eq!(streamed[0]["v"], "1");
        assert_eq!(streamed[1]["v"], "2");
    }
}
