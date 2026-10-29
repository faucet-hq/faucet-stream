use super::*;
use faucet_core::Sink as _;

#[test]
fn dataset_uri_combines_base_url_and_index() {
    let config = ElasticsearchSinkConfig::new("http://localhost:9200", "my-index");
    let sink = ElasticsearchSink::new(config).unwrap();
    assert_eq!(sink.dataset_uri(), "http://localhost:9200/my-index");
}

#[test]
fn resume_dup_risk_only_when_multichunk_and_no_id_field() {
    // The duplication-on-resume risk exists only when a page splits into
    // >1 _bulk chunk AND documents get auto-generated IDs.
    assert!(
        resume_dup_risk(2, false),
        "multi-chunk + no id_field is risky"
    );
    assert!(
        !resume_dup_risk(1, false),
        "single chunk can't partially commit"
    );
    assert!(
        !resume_dup_risk(5, true),
        "id_field makes re-sends idempotent"
    );
    assert!(!resume_dup_risk(1, true));
}
use serde_json::json;

#[test]
fn item_failures_read_any_action_type_and_missing_items() {
    let ok = json!({"index": {"status": 201}});
    let bad = json!({"index": {"status": 400, "error": {"type": "mapper_parsing_exception"}}});
    let conflict = json!({"create": {"status": 409, "error": {"type": "version_conflict"}}});
    let exists =
        json!({"create": {"status": 409, "error": {"type": "version_conflict_engine_exception"}}});
    assert!(
        item_failure(&exists).is_none(),
        "create on an existing _id is idempotent"
    );
    let index_conflict =
        json!({"index": {"status": 409, "error": {"type": "version_conflict_engine_exception"}}});
    assert!(item_failure(&index_conflict).is_some());
    assert!(item_failure(&ok).is_none());
    assert!(
        item_failure(&bad)
            .unwrap()
            .contains("mapper_parsing_exception")
    );
    assert!(
        item_failure(&conflict)
            .unwrap()
            .contains("version_conflict")
    );
    assert!(item_failure(&Value::Null).unwrap().contains("truncated"));
    let err = first_failure(&[ok.clone(), bad, conflict]).unwrap_err();
    assert!(err.to_string().contains("had 2 errors"), "{err}");
    assert!(first_failure(&[ok]).is_ok());
}

#[test]
fn only_overload_rejections_are_retried() {
    let r429 = json!({"index": {"status": 429, "error": {"type": "x"}}});
    let r503 = json!({"index": {"status": 503, "error": {"type": "x"}}});
    let pool =
        json!({"index": {"status": 500, "error": {"type": "es_rejected_execution_exception"}}});
    let parse = json!({"index": {"status": 400, "error": {"type": "mapper_parsing_exception"}}});
    let ok = json!({"index": {"status": 201}});
    assert!(retriable_item(&r429) && retriable_item(&r503) && retriable_item(&pool));
    assert!(!retriable_item(&parse) && !retriable_item(&ok) && !retriable_item(&Value::Null));
    assert_eq!(item_retry_delay(1).as_millis(), 200);
    assert_eq!(item_retry_delay(3).as_millis(), 800);
    assert_eq!(item_retry_delay(30).as_millis(), 5_000);
}

#[test]
fn settings_copy_keeps_what_the_index_needs_and_drops_identity() {
    let got = copyable_settings(&json!({
        "number_of_shards": "3",
        "number_of_replicas": "1",
        "refresh_interval": "30s",
        "analysis": {"analyzer": {"a": {"type": "custom", "tokenizer": "standard"}}},
        "lifecycle": {"name": "p", "rollover_alias": "orders"},
        "routing": {"allocation": {
            "include": {"_tier_preference": "data_hot"},
            "initial_recovery": {"_id": "n1"}
        }},
        "auto_expand_replicas": "0-1",
        "hidden": "true",
        "uuid": "abc",
        "creation_date": "1",
        "provided_name": "orders-1",
        "version": {"created": "1"}
    }));
    assert_eq!(
        got,
        json!({
            "number_of_shards": "3",
            "number_of_replicas": "1",
            "refresh_interval": "30s",
            "analysis": {"analyzer": {"a": {"type": "custom", "tokenizer": "standard"}}},
            "lifecycle": {"name": "p"},
            "routing": {"allocation": {"include": {"_tier_preference": "data_hot"}}},
            "auto_expand_replicas": "0-1",
            "hidden": "true"
        })
    );
    assert_eq!(copyable_settings(&json!("x")), json!({}));
}

#[test]
fn a_retried_entry_takes_later_writes_to_its_id_along() {
    let e = |action: &str, id: &str, v: i32| {
        format!("{{\"{action}\":{{\"_index\":\"i\",\"_id\":\"{id}\"}}}}\n{{\"v\":{v}}}\n")
    };
    let entries = vec![
        e("index", "a", 1),
        e("index", "b", 1),
        e("index", "a", 2),
        e("delete", "b", 0),
        "{\"index\":{\"_index\":\"i\"}}\n{\"v\":9}\n".to_string(),
    ];
    assert_eq!(entry_doc_id(&entries[0]).as_deref(), Some("a"));
    assert_eq!(entry_doc_id(&entries[4]), None);
    let pending: Vec<usize> = (0..entries.len()).collect();
    assert_eq!(with_later_same_id(&entries, &pending, vec![0]), vec![0, 2]);
    assert_eq!(with_later_same_id(&entries, &pending, vec![2]), vec![2]);
    assert_eq!(
        with_later_same_id(&entries, &pending, vec![1, 4]),
        vec![1, 3, 4]
    );
}

#[test]
fn reported_fields_accept_arrays_and_still_widen() {
    let dest = json!({"type": "object", "properties": {
        "n": reported_field("integer"),
        "tags": reported_field("string"),
    }});
    let page = json!({"type": "object", "properties": {
        "n": {"type": "number"},
        "tags": {"type": "array"},
    }});
    let diff = faucet_core::drift::diff_schema(&dest, &page, true);
    assert_eq!(
        diff.widenings
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>(),
        vec!["n"]
    );
    assert!(diff.incompatible.is_empty(), "{diff:?}");
}

#[test]
fn cleanup_refuses_scope_values_past_ignore_above() {
    let props = json!({
        "tenant": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 4}}},
        "plain": {"type": "keyword"}
    });
    let (target, limit) = term_target(&props, "tenant").unwrap();
    assert_eq!((target.as_str(), limit), ("tenant.keyword", Some(4)));
    assert_eq!(term_target(&props, "plain").unwrap().1, None);
    assert!(check_ignore_above("tenant", &json!("abcd"), limit).is_ok());
    let err = check_ignore_above("tenant", &json!("abcde"), limit).unwrap_err();
    assert!(err.contains("ignore_above") && err.contains("(4)"), "{err}");
    assert!(check_ignore_above("tenant", &json!(123456), limit).is_ok());
    assert!(check_ignore_above("plain", &json!("long value"), None).is_ok());
}

fn term_field(properties: &Value, field: &str) -> Result<String, String> {
    term_target(properties, field).map(|(target, _)| target)
}

#[test]
fn cleanup_terms_target_keyword_sub_fields_of_text() {
    let props = json!({
        "tenant": {"type": "text", "fields": {"raw": {"type": "keyword"}}},
        "plain": {"type": "keyword"},
        "body": {"type": "text"},
        "owner": {"properties": {"name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}}}}
    });
    assert_eq!(term_field(&props, "tenant").unwrap(), "tenant.raw");
    assert_eq!(term_field(&props, "plain").unwrap(), "plain");
    assert_eq!(
        term_field(&props, "owner.name").unwrap(),
        "owner.name.keyword"
    );
    assert_eq!(term_field(&props, "unmapped").unwrap(), "unmapped");
    assert!(term_field(&props, "body").unwrap_err().contains("keyword"));
}

#[test]
fn evolved_mappings_use_item_types_and_bounded_keywords() {
    assert_eq!(
        field_mapping(&json!({"type": "array", "items": {"type": "integer"}})),
        json!({"type": "long"})
    );
    assert_eq!(
        field_mapping(&json!({"type": ["array", "null"], "items": {"type": "string"}})),
        json!({"type": "keyword", "ignore_above": 8191})
    );
    assert_eq!(
        field_mapping(&json!({"type": "string"})),
        json!({"type": "keyword", "ignore_above": 8191})
    );
    assert_eq!(
        field_mapping(&json!({"type": "boolean"})),
        json!({"type": "boolean"})
    );
    assert_eq!(
        field_mapping(&json!({"type": "array"})),
        json!({"type": "keyword", "ignore_above": 8191})
    );
}

#[test]
fn plan_dedups_by_document_id_in_page_order() {
    use faucet_core::{WriteMode, WriteSpec};
    let spec = WriteSpec {
        write_mode: WriteMode::Upsert,
        key: vec!["id".to_string()],
        delete_marker: Some(faucet_core::DeleteMarker {
            field: "__op".to_string(),
            values: vec!["d".to_string()],
        }),
        rollback: None,
    };
    let page = vec![
        json!({"id": 7, "v": 1}),
        json!({"id": 8, "v": 2}),
        json!({"id": "7", "__op": "d"}),
        json!({"v": 3}),
    ];
    let planned = plan_origins(&page, &spec);
    assert_eq!(planned.actions.len(), 2, "7 and \"7\" are one document");
    assert_eq!(planned.actions[0].id, "7");
    assert!(planned.actions[0].doc.is_none(), "the later delete wins");
    assert_eq!(planned.actions[0].origins, vec![0, 2]);
    assert_eq!((planned.upserts, planned.deletes), (1, 1));
    assert_eq!(planned.failed.len(), 1);
}

#[test]
fn new_rejects_oversized_batch_size() {
    // Regression for #78/#44.
    let config = ElasticsearchSinkConfig::new("http://localhost:9200", "idx")
        .with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
    assert!(ElasticsearchSink::new(config).is_err());
}

#[test]
fn bulk_body_without_id_field() {
    let config = ElasticsearchSinkConfig::new("http://localhost:9200", "test_idx");
    let sink = ElasticsearchSink::new(config).unwrap();

    let records = [
        json!({"name": "Alice", "age": 30}),
        json!({"name": "Bob", "age": 25}),
    ];

    let body: String = records
        .iter()
        .map(|r| sink.append_entry(r).unwrap())
        .collect();
    let lines: Vec<&str> = body.trim().split('\n').collect();

    // 2 records = 4 lines (action + data for each).
    assert_eq!(lines.len(), 4);

    // Verify action lines contain the index.
    let action: Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(action["index"]["_index"], "test_idx");
    assert!(action["index"].get("_id").is_none());

    // Verify data lines.
    let data: Value = serde_json::from_str(lines[1]).unwrap();
    assert_eq!(data["name"], "Alice");
}

#[test]
fn bulk_body_with_id_field() {
    let config =
        ElasticsearchSinkConfig::new("http://localhost:9200", "test_idx").id_field("doc_id");
    let sink = ElasticsearchSink::new(config).unwrap();

    let records = [
        json!({"doc_id": "abc-123", "name": "Alice"}),
        json!({"doc_id": 42, "name": "Bob"}),
        json!({"name": "Charlie"}), // missing id field
    ];

    let body: String = records
        .iter()
        .map(|r| sink.append_entry(r).unwrap())
        .collect();
    let lines: Vec<&str> = body.trim().split('\n').collect();
    assert_eq!(lines.len(), 6);

    // First record: string id.
    let action0: Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(action0["index"]["_id"], "abc-123");

    // Second record: numeric id serialized as string.
    let action1: Value = serde_json::from_str(lines[2]).unwrap();
    assert_eq!(action1["index"]["_id"], "42");

    // Third record: no id field, so no _id in action.
    let action2: Value = serde_json::from_str(lines[4]).unwrap();
    assert!(action2["index"].get("_id").is_none());

    // A null / array / object id is refused rather than rendered "null".
    for bad in [
        json!({"doc_id": null}),
        json!({"doc_id": [1]}),
        json!({"doc_id": {}}),
    ] {
        let err = sink.append_entry(&bad).unwrap_err();
        assert!(err.to_string().contains("not a string"), "{err}");
    }
}

#[test]
fn create_op_type_emits_create_actions() {
    let mut config = ElasticsearchSinkConfig::new("http://localhost:9200", "logs-app");
    config.op_type = crate::config::ElasticsearchOpType::Create;
    let sink = ElasticsearchSink::new(config).unwrap();
    let entry = sink.append_entry(&json!({"m": 1})).unwrap();
    let action: Value = serde_json::from_str(entry.lines().next().unwrap()).unwrap();
    assert_eq!(action["create"]["_index"], "logs-app");
}

#[test]
fn es_mapping_to_json_schema_types() {
    // Numeric families collapse to integer / number.
    assert_eq!(es_type_to_json("long"), "integer");
    assert_eq!(es_type_to_json("integer"), "integer");
    assert_eq!(es_type_to_json("short"), "integer");
    assert_eq!(es_type_to_json("byte"), "integer");
    assert_eq!(es_type_to_json("double"), "number");
    assert_eq!(es_type_to_json("float"), "number");
    assert_eq!(es_type_to_json("half_float"), "number");
    assert_eq!(es_type_to_json("scaled_float"), "number");
    // Boolean + object families.
    assert_eq!(es_type_to_json("boolean"), "boolean");
    assert_eq!(es_type_to_json("object"), "object");
    assert_eq!(es_type_to_json("nested"), "object");
    // Everything else → string.
    assert_eq!(es_type_to_json("keyword"), "string");
    assert_eq!(es_type_to_json("text"), "string");
    assert_eq!(es_type_to_json("date"), "string");
    assert_eq!(es_type_to_json("ip"), "string");
    assert_eq!(es_type_to_json("geo_point"), "string");
}

#[test]
fn base_to_es_types() {
    assert_eq!(base_to_es(SqlBaseType::Integer), "long");
    assert_eq!(base_to_es(SqlBaseType::Double), "double");
    assert_eq!(base_to_es(SqlBaseType::Boolean), "boolean");
    assert_eq!(base_to_es(SqlBaseType::Text), "keyword");
    assert_eq!(base_to_es(SqlBaseType::Json), "object");
}

#[test]
fn doc_id_composite_key_is_canonical_json_not_separator_join() {
    // F13: the core helper is now injective — a composite key is encoded as
    // a canonical JSON array, NOT a `:`-join, so the separator can't collide.
    let kt = faucet_core::KeyTuple(vec![
        ("tenant".to_string(), serde_json::json!("acme")),
        ("id".to_string(), serde_json::json!(7)),
    ]);
    assert_eq!(faucet_core::key_to_doc_id(&kt, ":"), "[\"acme\",7]");
}

#[test]
fn doc_id_from_row_uses_injective_core_encoding() {
    // Composite key: now canonical-JSON encoded (F13) — NOT a separator
    // join — so it is injective. In `key` declaration order.
    let row = json!({"id": 7, "tenant": "acme", "v": "x"});
    let key = vec!["tenant".to_string(), "id".to_string()];
    // Matches faucet_core::key_to_doc_id for the same KeyTuple.
    let kt = faucet_core::KeyTuple(vec![
        ("tenant".to_string(), json!("acme")),
        ("id".to_string(), json!(7)),
    ]);
    assert_eq!(
        doc_id_from_row(&row, &key),
        faucet_core::key_to_doc_id(&kt, ":")
    );

    // Single string key column → rendered plain (no separator possible).
    let row = json!({"id": "abc-123"});
    assert_eq!(doc_id_from_row(&row, &["id".to_string()]), "abc-123");
}

#[test]
fn doc_id_from_row_composite_is_injective_no_collision() {
    // F13 regression: two distinct composite keys that would collide under a
    // naive separator-join must now produce DISTINCT `_id`s.
    let key = vec!["a".to_string(), "b".to_string()];
    let id1 = doc_id_from_row(&json!({"a": "x_", "b": "y"}), &key);
    let id2 = doc_id_from_row(&json!({"a": "x", "b": "_y"}), &key);
    assert_ne!(
        id1, id2,
        "distinct composite keys must not collapse to the same _id"
    );
    // And both go through the injective core helper, not a `:`-join.
    assert!(!id1.contains("x_:y") && !id2.contains("x:_y"));
}

// -- scoped cleanup (#478) pure helpers ---------------------------------

fn kt(pairs: &[(&str, Value)]) -> faucet_core::KeyTuple {
    faucet_core::KeyTuple(
        pairs
            .iter()
            .map(|(c, v)| (c.to_string(), v.clone()))
            .collect(),
    )
}

fn scope_of(pairs: &[(&str, Value)]) -> std::collections::BTreeMap<String, Value> {
    pairs
        .iter()
        .map(|(c, v)| (c.to_string(), v.clone()))
        .collect()
}

#[test]
fn cleanup_query_filters_scope_and_excludes_written_ids() {
    let ids = cleanup_doc_ids(&[kt(&[("id", json!(1))]), kt(&[("id", json!("a"))])]);
    let q = build_cleanup_query(&scope_of(&[("contact_id", json!(7))]), &ids);
    assert_eq!(
        q,
        json!({"query": {"bool": {
            "filter": [{"term": {"contact_id": 7}}],
            "must_not": [{"ids": {"values": ["1", "a"]}}],
        }}})
    );
}

#[test]
fn cleanup_query_empty_seen_deletes_the_whole_scope() {
    // The motivating case: the source says "this scope is now empty", so the
    // query is the scope predicate alone — NOT a no-op, and no `must_not`.
    let q = build_cleanup_query(&scope_of(&[("contact_id", json!(7))]), &[]);
    assert_eq!(
        q,
        json!({"query": {"bool": {"filter": [{"term": {"contact_id": 7}}]}}})
    );
    assert!(
        q["query"]["bool"].get("must_not").is_none(),
        "an empty id list must omit must_not, not send an empty ids query"
    );
}

#[test]
fn cleanup_query_over_an_empty_scope_covers_the_whole_index() {
    let q = build_cleanup_query(&scope_of(&[]), &["1".to_string()]);
    assert_eq!(
        q,
        json!({"query": {"bool": {"must_not": [{"ids": {"values": ["1"]}}]}}})
    );
    assert_eq!(
        build_cleanup_query(&scope_of(&[]), &[]),
        json!({"query": {"match_all": {}}})
    );
}

#[test]
fn cleanup_query_one_term_per_scope_field() {
    let q = build_cleanup_query(
        &scope_of(&[("a", json!(1)), ("b", json!("x"))]),
        &["1".to_string()],
    );
    // BTreeMap ordering makes the scope clauses deterministic.
    assert_eq!(
        q["query"]["bool"]["filter"],
        json!([{"term": {"a": 1}}, {"term": {"b": "x"}}])
    );
}

#[test]
fn cleanup_ids_match_the_upsert_id_derivation() {
    // The cleanup only deletes what the upsert path did NOT write, so its
    // ids must be byte-identical to the ones `plan_entry` indexes under
    // — including for composite keys (canonical JSON, not a `:`-join).
    let row = json!({"tenant": "acme", "id": 7});
    let key = vec!["tenant".to_string(), "id".to_string()];
    let ids = cleanup_doc_ids(&[kt(&[("tenant", json!("acme")), ("id", json!(7))])]);
    assert_eq!(ids, vec![doc_id_from_row(&row, &key)]);
    assert_eq!(ids[0], "[\"acme\",7]");
}

#[test]
fn key_alignment_accepts_matching_tuples() {
    let key = vec!["tenant".to_string(), "id".to_string()];
    let seen = vec![kt(&[("tenant", json!("acme")), ("id", json!(1))])];
    assert!(check_key_alignment(&key, &seen).is_ok());
}

#[test]
fn key_alignment_rejects_a_reordered_or_renamed_tuple() {
    // Order matters: `key_to_doc_id` is order-sensitive, so a reordered
    // tuple derives a different `_id` and would delete a written document.
    let key = vec!["tenant".to_string(), "id".to_string()];
    let reordered = vec![kt(&[("id", json!(1)), ("tenant", json!("acme"))])];
    let err = check_key_alignment(&key, &reordered).expect_err("must refuse");
    assert!(err.to_string().contains("refusing to delete"), "{err}");

    let renamed = vec![kt(&[("tenant", json!("acme")), ("other", json!(1))])];
    assert!(check_key_alignment(&key, &renamed).is_err());
}

#[test]
fn id_count_within_the_ceiling_is_accepted() {
    assert!(check_cleanup_id_count(MAX_CLEANUP_IDS).is_ok());
}

#[test]
fn oversized_id_set_is_refused_with_the_bound_named() {
    // The id set cannot be split across queries, so an outsized one must be
    // refused outright rather than half-issued.
    let err = check_cleanup_id_count(MAX_CLEANUP_IDS + 1).expect_err("must refuse");
    let msg = err.to_string();
    assert!(msg.contains(&MAX_CLEANUP_IDS.to_string()), "{msg}");
    assert!(msg.contains("Nothing was deleted"), "{msg}");
}

#[test]
fn delete_by_query_reports_deleted_count() {
    let body = json!({"deleted": 3, "version_conflicts": 0, "failures": [], "timed_out": false});
    assert_eq!(deleted_from_delete_by_query(&body).unwrap(), 3);
}

#[test]
fn delete_by_query_failures_are_not_reported_as_success() {
    // A partial delete answers 200 OK; reporting its `deleted` count would
    // claim the scope is clean while stale documents remain.
    let body = json!({
        "deleted": 2,
        "failures": [{"index": "idx", "cause": {"type": "es_rejected_execution_exception"}}],
    });
    let err = deleted_from_delete_by_query(&body).expect_err("must surface the failure");
    let msg = err.to_string();
    assert!(msg.contains("deleting 2"), "{msg}");
    assert!(msg.contains("es_rejected_execution_exception"), "{msg}");
}

#[test]
fn delete_by_query_version_conflicts_are_surfaced() {
    let body = json!({"deleted": 5, "version_conflicts": 2, "failures": []});
    let err = deleted_from_delete_by_query(&body).expect_err("must surface the conflict");
    assert!(err.to_string().contains("version conflict"), "{err}");
}

#[test]
fn delete_by_query_timeout_is_surfaced() {
    let body = json!({"deleted": 1, "version_conflicts": 0, "failures": [], "timed_out": true});
    let err = deleted_from_delete_by_query(&body).expect_err("must surface the timeout");
    assert!(err.to_string().contains("timed out"), "{err}");
}

#[test]
fn delete_by_query_malformed_response_is_typed_error() {
    let err = deleted_from_delete_by_query(&json!({"acknowledged": true}))
        .expect_err("must refuse a body with no deleted count");
    assert!(err.to_string().contains("malformed"), "{err}");
}

#[test]
fn plan_body_upsert_uses_key_id_and_strips_marker() {
    use faucet_core::{DeleteMarker, WriteMode, WriteSpec};

    let config = ElasticsearchSinkConfig {
        id_field: Some("ignored".to_string()),
        write: WriteSpec {
            write_mode: WriteMode::Upsert,
            key: vec!["id".to_string()],
            delete_marker: Some(DeleteMarker {
                field: "__op".to_string(),
                values: vec!["d".to_string()],
            }),
            rollback: None,
        },
        ..ElasticsearchSinkConfig::new("http://localhost:9200", "idx")
    };
    let sink = ElasticsearchSink::new(config).unwrap();

    let records = vec![
        json!({"id": 1, "v": "a"}),
        json!({"id": 2, "v": "x", "__op": "d"}),
    ];
    let planned = plan_origins(&records, &sink.inner.driver().config.write);
    let body: String = planned
        .actions
        .iter()
        .map(|a| sink.plan_entry(a).unwrap())
        .collect();
    let lines: Vec<&str> = body.trim().split('\n').collect();

    // 1 upsert (action + doc) + 1 delete (action only) = 3 lines.
    assert_eq!(lines.len(), 3, "{lines:?}");

    // Upsert action: `_id` derived from the key (overrides id_field).
    let action0: Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(action0["index"]["_id"], "1");
    assert_eq!(action0["index"]["_index"], "idx");
    // Doc line: the marker field is stripped.
    let doc0: Value = serde_json::from_str(lines[1]).unwrap();
    assert_eq!(doc0["v"], "a");
    assert!(doc0.get("__op").is_none());

    // Delete action: key-derived `_id`, NO doc line follows.
    let action1: Value = serde_json::from_str(lines[2]).unwrap();
    assert_eq!(action1["delete"]["_id"], "2");
    assert_eq!(action1["delete"]["_index"], "idx");
}

#[test]
fn staging_index_name_is_prefixed_and_unique() {
    let a = staging_index_name("orders", 0x1a2b);
    assert!(a.starts_with("orders-faucet-ovw-"), "{a}");
    assert_ne!(a, staging_index_name("orders", 0x1a2c));
}

#[test]
fn alias_swap_actions_remove_all_previous_then_add_staging() {
    let body = build_alias_swap_actions(
        "orders",
        "orders-faucet-ovw-staging",
        "orders-faucet-ovw-1",
        &["orders-old-a".to_string(), "orders-old-b".to_string()],
    );
    let actions = body["actions"].as_array().unwrap();
    assert_eq!(
        actions.len(),
        4,
        "two removes + one add + the marker detach"
    );
    assert_eq!(actions[0]["remove"]["index"], "orders-old-a");
    assert_eq!(actions[0]["remove"]["alias"], "orders");
    assert_eq!(actions[1]["remove"]["index"], "orders-old-b");
    assert_eq!(actions[2]["add"]["index"], "orders-faucet-ovw-1");
    assert_eq!(actions[2]["add"]["alias"], "orders");
    assert_eq!(actions[3]["remove"]["index"], "orders-faucet-ovw-1");
    assert_eq!(actions[3]["remove"]["alias"], "orders-faucet-ovw-staging");
}

#[test]
fn a_tokened_run_has_its_own_marker_alias() {
    let t = "67000000abcdef";
    assert_eq!(
        run_staging_alias("orders", None),
        "orders-faucet-ovw-staging"
    );
    let m = run_staging_alias("orders", Some(t));
    assert_eq!(m, "orders-faucet-ovw-staging-67000000abcdef");
    assert_eq!(staging_alias_token("orders", &m).as_deref(), Some(t));
    for other in [
        "orders-faucet-ovw-staging",
        "orders-faucet-ovw-staging-nope",
        "orders2-faucet-ovw-staging-67000000abcdef",
    ] {
        assert_eq!(staging_alias_token("orders", other), None, "{other}");
    }
    let body = build_alias_swap_actions("orders", &m, "orders-faucet-ovw-1", &[]);
    assert_eq!(body["actions"][1]["remove"]["alias"], m.as_str());
}

#[test]
fn alias_swap_first_run_only_adds() {
    let body = build_alias_swap_actions(
        "orders",
        "orders-faucet-ovw-staging",
        "orders-faucet-ovw-1",
        &[],
    );
    let actions = body["actions"].as_array().unwrap();
    assert_eq!(actions.len(), 2);
    assert!(actions[0].get("add").is_some());
    assert_eq!(actions[1]["remove"]["alias"], "orders-faucet-ovw-staging");
}
