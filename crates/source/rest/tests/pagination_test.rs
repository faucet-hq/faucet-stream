use faucet_source_rest::PaginationStyle;
use faucet_source_rest::pagination::PaginationState;
use reqwest::header::HeaderMap;
use serde_json::json;
use std::collections::HashMap;

/// Convenience: an empty HeaderMap for tests that don't need response headers.
fn no_headers() -> HeaderMap {
    HeaderMap::new()
}

#[test]
fn cursor_pagination_extracts_token() {
    let style = PaginationStyle::Cursor {
        next_token_path: "$.meta.cursor".into(),
        param_name: "cursor".into(),
    };

    let body = json!({"data": [], "meta": {"cursor": "abc123"}});
    let mut state = PaginationState::default();

    let has_next = style.advance(&body, &no_headers(), &mut state, 10).unwrap();
    assert!(has_next);
    assert_eq!(state.next_token, Some("abc123".into()));

    let mut params = HashMap::new();
    style.apply_params(&mut params, &state);
    assert_eq!(params.get("cursor").unwrap(), "abc123");
}

#[test]
fn cursor_pagination_stops_on_null() {
    let style = PaginationStyle::Cursor {
        next_token_path: "$.meta.cursor".into(),
        param_name: "cursor".into(),
    };

    let body = json!({"data": [], "meta": {"cursor": null}});
    let mut state = PaginationState::default();

    let has_next = style.advance(&body, &no_headers(), &mut state, 0).unwrap();
    assert!(!has_next);
    assert!(state.next_token.is_none());
}

#[test]
fn cursor_in_body_extracts_token_and_exposes_body_cursor() {
    let style = PaginationStyle::CursorInBody {
        next_token_path: "$.paging.next.after".into(),
        body_cursor_field: "after".into(),
    };

    // First page: no cursor extracted yet → nothing to inject.
    let mut state = PaginationState::default();
    assert!(style.body_cursor(&state).is_none());

    let body = json!({"results": [{"id": 1}], "paging": {"next": {"after": "c1"}}});
    let has_next = style.advance(&body, &no_headers(), &mut state, 1).unwrap();
    assert!(has_next);
    assert_eq!(state.next_token, Some("c1".into()));
    // The extracted cursor is exposed for injection into the next request body.
    assert_eq!(style.body_cursor(&state), Some(("after", "c1")));
}

#[test]
fn cursor_in_body_stops_when_cursor_absent() {
    let style = PaginationStyle::CursorInBody {
        next_token_path: "$.paging.next.after".into(),
        body_cursor_field: "after".into(),
    };
    let body = json!({"results": [{"id": 3}], "paging": {}});
    let mut state = PaginationState::default();
    let has_next = style.advance(&body, &no_headers(), &mut state, 1).unwrap();
    assert!(!has_next);
}

#[test]
fn cursor_in_body_stops_on_repeated_cursor() {
    // Loop guard: a server that keeps returning the same `after` must not loop.
    let style = PaginationStyle::CursorInBody {
        next_token_path: "$.paging.next.after".into(),
        body_cursor_field: "after".into(),
    };
    let body = json!({"results": [{"id": 1}], "paging": {"next": {"after": "same"}}});
    let mut state = PaginationState::default();
    assert!(style.advance(&body, &no_headers(), &mut state, 1).unwrap());
    // Same cursor again → stop rather than loop forever.
    assert!(!style.advance(&body, &no_headers(), &mut state, 1).unwrap());
}

#[test]
fn body_cursor_is_none_for_non_body_styles() {
    let style = PaginationStyle::Cursor {
        next_token_path: "$.c".into(),
        param_name: "cursor".into(),
    };
    let state = PaginationState {
        next_token: Some("x".into()),
        ..Default::default()
    };
    assert!(style.body_cursor(&state).is_none());
}

#[test]
fn page_number_increments() {
    let style = PaginationStyle::PageNumber {
        param_name: "page".into(),
        start_page: 1,
        page_size: Some(25),
        page_size_param: Some("per_page".into()),
    };

    let mut state = PaginationState::default();
    let body = json!({});

    let has_next = style.advance(&body, &no_headers(), &mut state, 25).unwrap();
    assert!(has_next);
    assert_eq!(state.page, 1);

    let mut params = HashMap::new();
    style.apply_params(&mut params, &state);
    assert_eq!(params.get("page").unwrap(), "2");
    assert_eq!(params.get("per_page").unwrap(), "25");
}

#[test]
fn page_number_stops_on_repeated_identical_page() {
    // Regression for #78/#15: an API that clamps an out-of-range page to the
    // last page and re-returns it (non-empty) must not loop forever.
    let style = PaginationStyle::PageNumber {
        param_name: "page".into(),
        start_page: 1,
        page_size: None,
        page_size_param: None,
    };
    let mut state = PaginationState::default();
    let body = json!({"items": [{"id": 1}, {"id": 2}]});

    // First page is new content → continue.
    assert!(style.advance(&body, &no_headers(), &mut state, 2).unwrap());
    // Identical page returned again → stagnation detected → stop.
    assert!(!style.advance(&body, &no_headers(), &mut state, 2).unwrap());
}

#[test]
fn page_number_continues_on_distinct_pages() {
    let style = PaginationStyle::PageNumber {
        param_name: "page".into(),
        start_page: 1,
        page_size: None,
        page_size_param: None,
    };
    let mut state = PaginationState::default();
    assert!(
        style
            .advance(&json!({"items": [{"id": 1}]}), &no_headers(), &mut state, 1)
            .unwrap()
    );
    // Different content → keep going.
    assert!(
        style
            .advance(&json!({"items": [{"id": 2}]}), &no_headers(), &mut state, 1)
            .unwrap()
    );
}

#[test]
fn page_number_stops_on_empty() {
    let style = PaginationStyle::PageNumber {
        param_name: "page".into(),
        start_page: 1,
        page_size: None,
        page_size_param: None,
    };

    let mut state = PaginationState::default();
    let body = json!({});

    let has_next = style.advance(&body, &no_headers(), &mut state, 0).unwrap();
    assert!(!has_next);
}

#[test]
fn offset_pagination_advances() {
    let style = PaginationStyle::Offset {
        offset_param: "offset".into(),
        limit_param: "limit".into(),
        limit: 50,
        total_path: Some("$.total".into()),
        rows_path: None,
    };

    let body = json!({"total": 120});
    let mut state = PaginationState::default();

    let has_next = style.advance(&body, &no_headers(), &mut state, 50).unwrap();
    assert!(has_next);
    assert_eq!(state.offset, 50);

    let has_next = style.advance(&body, &no_headers(), &mut state, 50).unwrap();
    assert!(has_next);
    assert_eq!(state.offset, 100);

    let has_next = style.advance(&body, &no_headers(), &mut state, 20).unwrap();
    assert!(!has_next);
    assert_eq!(state.offset, 120);
}

#[test]
fn offset_stops_on_repeated_identical_page_without_total_path() {
    // Regression for #264 F18: a server that ignores the `offset` parameter
    // returns the identical full page forever. With `total_path` omitted, the
    // record-count heuristic keeps `has_next` true on every full page, so the
    // run would loop until `max_pages` and duplicate records. The
    // content-stagnation guard must stop on the repeated page.
    let style = PaginationStyle::Offset {
        offset_param: "offset".into(),
        limit_param: "limit".into(),
        limit: 2,
        total_path: None,
        rows_path: None,
    };
    let mut state = PaginationState::default();
    let body = json!([{"id": 1}, {"id": 2}]);

    // First full page is new content → continue.
    assert!(style.advance(&body, &no_headers(), &mut state, 2).unwrap());
    assert_eq!(state.offset, 2);
    // Identical page returned again (server ignored offset) → stop.
    assert!(!style.advance(&body, &no_headers(), &mut state, 2).unwrap());
}

#[test]
fn offset_continues_on_distinct_pages_without_total_path() {
    // Legitimate offset pagination where each page differs must still
    // paginate to completion (guard must not fire on distinct content).
    let style = PaginationStyle::Offset {
        offset_param: "offset".into(),
        limit_param: "limit".into(),
        limit: 2,
        total_path: None,
        rows_path: None,
    };
    let mut state = PaginationState::default();

    assert!(
        style
            .advance(&json!([{"id": 1}, {"id": 2}]), &no_headers(), &mut state, 2)
            .unwrap()
    );
    assert!(
        style
            .advance(&json!([{"id": 3}, {"id": 4}]), &no_headers(), &mut state, 2)
            .unwrap()
    );
    // A short page no longer ends paging (the server may clamp `limit`,
    // API-15); the empty page after it does.
    assert!(
        style
            .advance(&json!([{"id": 5}]), &no_headers(), &mut state, 1)
            .unwrap()
    );
    assert_eq!(state.offset, 5);
    assert!(
        !style
            .advance(&json!([]), &no_headers(), &mut state, 0)
            .unwrap()
    );
}

#[test]
fn offset_repeated_metadata_body_with_total_path_does_not_false_stop() {
    // When `total_path` is set, a paging-metadata body that legitimately
    // repeats (`{"total": N}` echoed on every page) must NOT be mistaken for
    // stagnation — `offset::advance` has an authoritative stop condition.
    let style = PaginationStyle::Offset {
        offset_param: "offset".into(),
        limit_param: "limit".into(),
        limit: 50,
        total_path: Some("$.total".into()),
        rows_path: None,
    };
    let mut state = PaginationState::default();
    let body = json!({"total": 120});

    assert!(style.advance(&body, &no_headers(), &mut state, 50).unwrap());
    assert_eq!(state.offset, 50);
    // Identical metadata body, but pagination is legitimate (offset < total).
    assert!(style.advance(&body, &no_headers(), &mut state, 50).unwrap());
    assert_eq!(state.offset, 100);
    // Offset reaches total → stop.
    assert!(!style.advance(&body, &no_headers(), &mut state, 20).unwrap());
    assert_eq!(state.offset, 120);
}

#[test]
fn link_header_extracts_next_link() {
    use reqwest::header::HeaderValue;

    let style = PaginationStyle::LinkHeader;
    let body = json!({});
    let mut state = PaginationState::default();

    let mut headers = HeaderMap::new();
    headers.insert(
        "link",
        HeaderValue::from_static(
            r#"<https://api.example.com/items?page=2>; rel="next", <https://api.example.com/items?page=5>; rel="last""#,
        ),
    );

    let has_next = style.advance(&body, &headers, &mut state, 10).unwrap();
    assert!(has_next);
    assert_eq!(
        state.next_link,
        Some("https://api.example.com/items?page=2".into())
    );
}

#[test]
fn link_header_stops_when_no_next() {
    let style = PaginationStyle::LinkHeader;
    let body = json!({});
    let mut state = PaginationState::default();

    let has_next = style.advance(&body, &no_headers(), &mut state, 5).unwrap();
    assert!(!has_next);
    assert!(state.next_link.is_none());
}

#[test]
fn next_link_in_body_extracts_url() {
    let style = PaginationStyle::NextLinkInBody {
        next_link_path: "$.next_link".into(),
    };
    let body = json!({"results": [], "next_link": "https://api.example.com/workers?page=2"});
    let mut state = PaginationState::default();

    let has_next = style.advance(&body, &no_headers(), &mut state, 10).unwrap();
    assert!(has_next);
    assert_eq!(
        state.next_link,
        Some("https://api.example.com/workers?page=2".into())
    );
}

#[test]
fn next_link_in_body_stops_on_null() {
    let style = PaginationStyle::NextLinkInBody {
        next_link_path: "$.next_link".into(),
    };
    let body = json!({"results": [], "next_link": null});
    let mut state = PaginationState {
        next_link: Some("stale".into()),
        ..Default::default()
    };

    let has_next = style.advance(&body, &no_headers(), &mut state, 0).unwrap();
    assert!(!has_next);
    assert!(state.next_link.is_none());
}

#[test]
fn next_link_in_body_stops_when_field_absent() {
    let style = PaginationStyle::NextLinkInBody {
        next_link_path: "$.next_link".into(),
    };
    let body = json!({"results": []});
    let mut state = PaginationState::default();

    let has_next = style.advance(&body, &no_headers(), &mut state, 0).unwrap();
    assert!(!has_next);
    assert!(state.next_link.is_none());
}

// ── Loop detection ────────────────────────────────────────────────────────────

#[test]
fn cursor_loop_detection_stops_on_duplicate_token() {
    let style = PaginationStyle::Cursor {
        next_token_path: "$.cursor".into(),
        param_name: "cursor".into(),
    };

    let mut state = PaginationState::default();

    // First advance: cursor "abc" — should succeed.
    let body = json!({"cursor": "abc"});
    let has_next = style.advance(&body, &no_headers(), &mut state, 10).unwrap();
    assert!(has_next);
    assert_eq!(state.next_token, Some("abc".into()));

    // Second advance: same cursor "abc" — loop detected, should stop.
    let has_next = style.advance(&body, &no_headers(), &mut state, 10).unwrap();
    assert!(!has_next, "expected loop detection to stop pagination");
}

#[test]
fn cursor_loop_detection_allows_distinct_tokens() {
    let style = PaginationStyle::Cursor {
        next_token_path: "$.cursor".into(),
        param_name: "cursor".into(),
    };

    let mut state = PaginationState::default();

    let body1 = json!({"cursor": "page2"});
    assert!(
        style
            .advance(&body1, &no_headers(), &mut state, 10)
            .unwrap()
    );

    let body2 = json!({"cursor": "page3"});
    assert!(
        style
            .advance(&body2, &no_headers(), &mut state, 10)
            .unwrap()
    );

    let body3 = json!({"cursor": null});
    assert!(
        !style
            .advance(&body3, &no_headers(), &mut state, 10)
            .unwrap()
    );
}

#[test]
fn link_header_loop_detection_stops_on_duplicate() {
    use reqwest::header::HeaderValue;

    let style = PaginationStyle::LinkHeader;
    let body = json!({});
    let mut state = PaginationState::default();

    let mut headers = HeaderMap::new();
    headers.insert(
        "link",
        HeaderValue::from_static(r#"<https://api.example.com/items?page=2>; rel="next""#),
    );

    // First advance: new link — succeeds.
    let has_next = style.advance(&body, &headers, &mut state, 10).unwrap();
    assert!(has_next);

    // Second advance: same link — loop detected.
    let has_next = style.advance(&body, &headers, &mut state, 10).unwrap();
    assert!(!has_next, "expected loop detection to stop pagination");
}

#[test]
fn next_link_body_loop_detection_stops_on_duplicate() {
    let style = PaginationStyle::NextLinkInBody {
        next_link_path: "$.next_link".into(),
    };

    let mut state = PaginationState::default();
    let body = json!({"results": [], "next_link": "https://api.example.com/page=2"});

    // First advance: new link — succeeds.
    let has_next = style.advance(&body, &no_headers(), &mut state, 10).unwrap();
    assert!(has_next);

    // Second advance: same link — loop detected.
    let has_next = style.advance(&body, &no_headers(), &mut state, 10).unwrap();
    assert!(!has_next, "expected loop detection to stop pagination");
}

#[test]
fn offset_rows_path_advances_by_server_rows_not_child_records() {
    // API-36: three child records from two server rows must advance by 2.
    let style = PaginationStyle::Offset {
        offset_param: "offset".into(),
        limit_param: "limit".into(),
        limit: 2,
        total_path: None,
        rows_path: Some("$.orders".into()),
    };
    let mut state = PaginationState::default();
    let body = json!({"orders": [{"lines": [1, 2]}, {"lines": [3]}]});
    assert!(style.advance(&body, &no_headers(), &mut state, 3).unwrap());
    assert_eq!(state.offset, 2);
    assert!(
        !style
            .advance(&json!({"orders": []}), &no_headers(), &mut state, 0)
            .unwrap()
    );
}
