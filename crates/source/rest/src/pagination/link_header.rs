//! Link header pagination (RFC 8288).

use reqwest::header::HeaderMap;

/// Extract the `rel="next"` URL from a `Link` header (RFC 8288).
///
/// Parses each link-value into its `<URI-Reference>` and parameters, splitting
/// on commas only *outside* the angle brackets (so a comma inside a URL does
/// not split a link), and matches the `rel` parameter case-insensitively
/// whether it is quoted (`rel="next"`) or unquoted (`rel=next`) and even when
/// it carries multiple space-separated relations (`rel="prev next"`) (#78 LOW).
///
/// Every `Link` header field is read: RFC 8288 makes several fields equivalent
/// to one comma-joined field, and servers commonly send `rel="first"` and
/// `rel="next"` on separate lines. A field holding non-visible-ASCII bytes is
/// read lossily rather than ending pagination.
pub fn extract_next_link(headers: &HeaderMap) -> Option<String> {
    for value in headers.get_all("link") {
        let link = String::from_utf8_lossy(value.as_bytes());
        for (uri, rel) in parse_link_header(&link) {
            if rel
                .split_whitespace()
                .any(|tok| tok.eq_ignore_ascii_case("next"))
            {
                return Some(uri);
            }
        }
    }
    None
}

/// Split a `Link` header into `(uri, rel)` pairs. Splits link-values on commas
/// that are not inside an angle-bracketed URI, then extracts the `<...>` URI
/// and the value of any `rel=` parameter (quoted or unquoted).
fn parse_link_header(header: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut depth = 0i32; // inside <...>
    let mut start = 0usize;
    let bytes = header.as_bytes();
    let mut segments: Vec<&str> = Vec::new();
    for (i, &b) in bytes.iter().enumerate() {
        match b {
            b'<' => depth += 1,
            b'>' => depth -= 1,
            b',' if depth <= 0 => {
                segments.push(&header[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    segments.push(&header[start..]);

    for seg in segments {
        let seg = seg.trim();
        let Some(lt) = seg.find('<') else { continue };
        let Some(gt) = seg[lt + 1..].find('>') else {
            continue;
        };
        let uri = seg[lt + 1..lt + 1 + gt].trim().to_string();
        // Parameters follow the `>`; find `rel=...`.
        let params = &seg[lt + 1 + gt + 1..];
        let mut rel = String::new();
        for param in params.split(';') {
            // Parameter names are case-insensitive (RFC 8288 §3).
            let Some((name, v)) = param.split_once('=') else {
                continue;
            };
            if name.trim().eq_ignore_ascii_case("rel") {
                rel = v.trim().trim_matches('"').to_string();
                break;
            }
        }
        out.push((uri, rel));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::{HeaderMap, HeaderValue};

    #[test]
    fn next_in_a_later_link_field_is_found() {
        let mut headers = HeaderMap::new();
        headers.append(
            "link",
            HeaderValue::from_static(r#"<https://api.example.com/items?page=1>; rel="first""#),
        );
        headers.append(
            "link",
            HeaderValue::from_static(r#"<https://api.example.com/items?page=2>; rel="next""#),
        );
        assert_eq!(
            extract_next_link(&headers),
            Some("https://api.example.com/items?page=2".to_string()),
        );
    }

    #[test]
    fn rel_parameter_name_is_case_insensitive() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "link",
            HeaderValue::from_static(r#"<https://api.example.com/items?page=2>; REL="next""#),
        );
        assert_eq!(
            extract_next_link(&headers),
            Some("https://api.example.com/items?page=2".to_string()),
        );
    }

    #[test]
    fn non_ascii_first_field_does_not_end_pagination() {
        let mut headers = HeaderMap::new();
        headers.append(
            "link",
            HeaderValue::from_bytes(b"<https://api.example.com/caf\xc3\xa9>; rel=\"about\"")
                .unwrap(),
        );
        headers.append(
            "link",
            HeaderValue::from_static(r#"<https://api.example.com/items?page=2>; rel="next""#),
        );
        assert_eq!(
            extract_next_link(&headers),
            Some("https://api.example.com/items?page=2".to_string()),
        );
    }

    #[test]
    fn test_extract_next_link() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "link",
            HeaderValue::from_static(
                r#"<https://api.example.com/items?page=2>; rel="next", <https://api.example.com/items?page=5>; rel="last""#
            ),
        );
        assert_eq!(
            extract_next_link(&headers),
            Some("https://api.example.com/items?page=2".to_string()),
        );
    }

    #[test]
    fn test_no_next_link() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "link",
            HeaderValue::from_static(r#"<https://api.example.com/items?page=1>; rel="prev""#),
        );
        assert_eq!(extract_next_link(&headers), None);
    }

    #[test]
    fn test_empty_headers() {
        let headers = HeaderMap::new();
        assert_eq!(extract_next_link(&headers), None);
    }

    #[test]
    fn unquoted_rel_is_supported() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "link",
            HeaderValue::from_static("<https://api.example.com/items?page=2>; rel=next"),
        );
        assert_eq!(
            extract_next_link(&headers),
            Some("https://api.example.com/items?page=2".to_string())
        );
    }

    #[test]
    fn multi_relation_rel_token_matches_next() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "link",
            HeaderValue::from_static(r#"<https://api.example.com/p3>; rel="prev next""#),
        );
        assert_eq!(
            extract_next_link(&headers),
            Some("https://api.example.com/p3".to_string())
        );
    }

    #[test]
    fn comma_inside_url_does_not_split_link() {
        // A query string containing a comma must not break link-value splitting.
        let mut headers = HeaderMap::new();
        headers.insert(
            "link",
            HeaderValue::from_static(
                r#"<https://api.example.com/items?ids=1,2,3&page=2>; rel="next""#,
            ),
        );
        assert_eq!(
            extract_next_link(&headers),
            Some("https://api.example.com/items?ids=1,2,3&page=2".to_string())
        );
    }

    #[test]
    fn rel_next_is_case_insensitive() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "link",
            HeaderValue::from_static(r#"<https://api.example.com/x>; rel="NEXT""#),
        );
        assert_eq!(
            extract_next_link(&headers),
            Some("https://api.example.com/x".to_string())
        );
    }
}
