//! Shared quick-xml helpers: character data with references resolved, and
//! attribute values unescaped, each failing loudly instead of dropping text.

use quick_xml::XmlVersion;
use quick_xml::escape::resolve_predefined_entity;
use quick_xml::events::{BytesRef, BytesStart, Event};

/// The character data an event contributes to its element: text, CDATA, or a
/// resolved entity / character reference. `Ok(None)` for every other event.
pub(crate) fn event_text(event: &Event<'_>) -> Result<Option<String>, String> {
    match event {
        Event::Text(t) => Ok(Some(t.xml10_content().into_owned())),
        Event::CData(c) => Ok(Some(c.xml10_content().into_owned())),
        Event::GeneralRef(r) => resolve_reference(r).map(Some),
        _ => Ok(None),
    }
}

/// Resolve `&name;` (the five XML entities) or `&#N;` / `&#xN;`.
pub(crate) fn resolve_reference(r: &BytesRef<'_>) -> Result<String, String> {
    if let Some(ch) = r
        .resolve_char_ref()
        .map_err(|e| format!("invalid character reference `&{};`: {e}", &**r))?
    {
        return Ok(ch.to_string());
    }
    resolve_predefined_entity(r)
        .map(str::to_string)
        .ok_or_else(|| format!("undefined entity reference `&{};`", &**r))
}

/// Every attribute of a start tag as `(qualified name, unescaped value)`. A
/// duplicate name, a malformed attribute or an unknown entity is an error.
pub(crate) fn attributes(e: &BytesStart<'_>) -> Result<Vec<(String, String)>, String> {
    let mut out = Vec::new();
    for attr in e.attributes() {
        let attr = attr.map_err(|e| format!("malformed attribute: {e}"))?;
        let value = attr
            .normalized_value(XmlVersion::Implicit1_0)
            .map_err(|e| format!("attribute `{}`: {e}", attr.key.as_ref()))?;
        out.push((attr.key.as_ref().to_string(), value.into_owned()));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use quick_xml::Reader;

    fn texts(xml: &str) -> Result<String, String> {
        let mut reader = Reader::from_str(xml);
        let mut out = String::new();
        loop {
            let ev = reader.read_event().map_err(|e| e.to_string())?;
            if matches!(ev, Event::Eof) {
                return Ok(out);
            }
            if let Some(t) = event_text(&ev)? {
                out.push_str(&t);
            }
        }
    }

    #[test]
    fn references_resolve_and_cdata_is_literal() {
        assert_eq!(
            texts("<a>x &amp; y &#65;&#x42; <![CDATA[&lt;]]></a>").unwrap(),
            "x & y AB &lt;"
        );
    }

    #[test]
    fn undefined_entity_is_an_error_not_dropped_text() {
        let err = texts("<a>a&nbsp;b</a>").unwrap_err();
        assert!(err.contains("undefined entity reference `&nbsp;`"), "{err}");
    }

    #[test]
    fn invalid_char_reference_is_an_error() {
        let err = texts("<a>&#0;</a>").unwrap_err();
        assert!(err.contains("invalid character reference"), "{err}");
    }

    #[test]
    fn attributes_are_unescaped_and_duplicates_rejected() {
        let mut reader = Reader::from_str(r#"<a n="A &amp; B &quot;q&quot;" m="&#65;"/>"#);
        let Event::Empty(e) = reader.read_event().unwrap() else {
            panic!("expected empty element")
        };
        assert_eq!(
            attributes(&e).unwrap(),
            vec![
                ("n".to_string(), "A & B \"q\"".to_string()),
                ("m".to_string(), "A".to_string())
            ]
        );
        let mut reader = Reader::from_str(r#"<a n="1" n="2"/>"#);
        let Event::Empty(e) = reader.read_event().unwrap() else {
            panic!("expected empty element")
        };
        assert!(attributes(&e).unwrap_err().contains("malformed attribute"));
        let mut reader = Reader::from_str(r#"<a n="&bogus;"/>"#);
        let Event::Empty(e) = reader.read_event().unwrap() else {
            panic!("expected empty element")
        };
        assert!(attributes(&e).unwrap_err().contains("attribute `n`"));
    }
}
