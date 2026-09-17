//! Integration tests for the compiled `SqlTransform`: per-page execution,
//! reference relations, schema drift, validation, and connection reuse.

use faucet_core::stage::{CompiledStage, apply_stages_to_page, compile_stage};
use faucet_transform_sql::{RelationSource, RelationSpec, SqlTransform, SqlTransformConfig};
use serde_json::{Value, json};

/// Compile a config into a runnable page stage.
fn stage(cfg: SqlTransformConfig) -> CompiledStage {
    compile_stage(&SqlTransform::compile(&cfg).unwrap().into_page_stage()).unwrap()
}

/// Convenience: compile a bare query (no relations) and run one page through it.
fn run(query: &str, page: Vec<Value>) -> Vec<Value> {
    let s = stage(SqlTransformConfig {
        query: query.into(),
        relations: vec![],
        memory_limit: None,
        threads: None,
    });
    apply_stages_to_page(page, &[s]).unwrap()
}

#[test]
fn select_passthrough_and_projection() {
    let out = run(
        "SELECT id, upper(name) AS name FROM batch WHERE id > 1",
        vec![json!({"id": 1, "name": "a"}), json!({"id": 2, "name": "b"})],
    );
    assert_eq!(out, vec![json!({"id": 2, "name": "B"})]);
}

#[test]
fn group_by_aggregation_within_page() {
    let out = run(
        "SELECT k, SUM(v) AS total FROM batch GROUP BY k ORDER BY k",
        vec![
            json!({"k": "a", "v": 1}),
            json!({"k": "a", "v": 2}),
            json!({"k": "b", "v": 5}),
        ],
    );
    assert_eq!(
        out,
        vec![json!({"k": "a", "total": 3}), json!({"k": "b", "total": 5})]
    );
}

#[test]
fn empty_page_returns_empty_without_error() {
    let out = run("SELECT * FROM batch", vec![]);
    assert!(out.is_empty());
}

#[test]
fn empty_result_set_is_valid() {
    let out = run("SELECT * FROM batch WHERE false", vec![json!({"a": 1})]);
    assert!(out.is_empty());
}

#[test]
fn schema_drift_recreates_batch() {
    let s = stage(SqlTransformConfig {
        query: "SELECT * FROM batch".into(),
        relations: vec![],
        memory_limit: None,
        threads: None,
    });
    let p1 = apply_stages_to_page(vec![json!({"a": 1})], std::slice::from_ref(&s)).unwrap();
    let p2 =
        apply_stages_to_page(vec![json!({"a": 2, "b": "x"})], std::slice::from_ref(&s)).unwrap();
    assert_eq!(p1[0]["a"], json!(1));
    assert_eq!(p2[0]["b"], json!("x"));
}

#[test]
fn join_to_csv_reference_relation() {
    let path = format!("{}/tests/data/countries.csv", env!("CARGO_MANIFEST_DIR"));
    let s = stage(SqlTransformConfig {
        query: "SELECT b.id, c.country FROM batch b LEFT JOIN countries c ON b.code = c.code ORDER BY b.id".into(),
        relations: vec![RelationSpec {
            name: "countries".into(),
            source: RelationSource::Csv { path, has_header: true },
            reload_on_change: false,
        }],
        memory_limit: None,
        threads: None,
    });
    let out = apply_stages_to_page(
        vec![
            json!({"id": 1, "code": "US"}),
            json!({"id": 2, "code": "IN"}),
        ],
        &[s],
    )
    .unwrap();
    assert_eq!(out[0]["country"], json!("United States"));
    assert_eq!(out[1]["country"], json!("India"));
}

#[test]
fn values_relation_join() {
    let s = stage(SqlTransformConfig {
        query: "SELECT b.id, t.label FROM batch b JOIN tiers t ON b.tier = t.id ORDER BY b.id"
            .into(),
        relations: vec![RelationSpec {
            name: "tiers".into(),
            source: RelationSource::Values {
                columns: vec!["id".into(), "label".into()],
                rows: vec![
                    vec![json!(1), json!("gold")],
                    vec![json!(2), json!("silver")],
                ],
            },
            reload_on_change: false,
        }],
        memory_limit: None,
        threads: None,
    });
    let out = apply_stages_to_page(vec![json!({"id": 9, "tier": 2})], &[s]).unwrap();
    assert_eq!(out[0]["label"], json!("silver"));
}

#[test]
fn bad_query_fails_at_compile_with_message() {
    let err = SqlTransform::compile(&SqlTransformConfig {
        query: "SELEKT * FROM batch".into(), // syntax error
        relations: vec![],
        memory_limit: None,
        threads: None,
    })
    .unwrap_err();
    let msg = format!("{err}").to_lowercase();
    assert!(msg.contains("sel") || msg.contains("syntax"), "got: {err}");
}

#[test]
fn query_referencing_only_batch_compiles() {
    // `batch` doesn't exist yet at compile — referencing only its columns must
    // be tolerated (the binder error is about `batch` missing).
    SqlTransform::compile(&SqlTransformConfig {
        query: "SELECT x, y FROM batch WHERE z > 1".into(),
        relations: vec![],
        memory_limit: None,
        threads: None,
    })
    .expect("references only batch columns -> tolerated at compile");
}

#[test]
fn reserved_relation_name_batch_rejected() {
    let err = SqlTransform::compile(&SqlTransformConfig {
        query: "SELECT * FROM batch".into(),
        relations: vec![RelationSpec {
            name: "batch".into(),
            source: RelationSource::Values {
                columns: vec!["a".into()],
                rows: vec![],
            },
            reload_on_change: false,
        }],
        memory_limit: None,
        threads: None,
    })
    .unwrap_err();
    assert!(format!("{err}").contains("batch"));
}

#[test]
fn missing_reference_file_fails_at_compile() {
    let err = SqlTransform::compile(&SqlTransformConfig {
        query: "SELECT * FROM batch JOIN nope USING (id)".into(),
        relations: vec![RelationSpec {
            name: "nope".into(),
            source: RelationSource::Csv {
                path: "/does/not/exist.csv".into(),
                has_header: true,
            },
            reload_on_change: false,
        }],
        memory_limit: None,
        threads: None,
    })
    .unwrap_err();
    let msg = format!("{err}").to_lowercase();
    assert!(
        msg.contains("exist") || msg.contains("no such") || msg.contains("nope"),
        "got: {err}"
    );
}

#[test]
fn connection_reused_across_pages() {
    let s = stage(SqlTransformConfig {
        query: "SELECT count(*) AS n FROM batch".into(),
        relations: vec![],
        memory_limit: None,
        threads: None,
    });
    for i in 1..=3 {
        let out = apply_stages_to_page(
            (0..i).map(|j| json!({"x": j})).collect(),
            std::slice::from_ref(&s),
        )
        .unwrap();
        assert_eq!(out[0]["n"], json!(i));
    }
}

#[test]
fn aggregation_is_per_page_over_two_pages() {
    let s = stage(SqlTransformConfig {
        query: "SELECT SUM(v) AS total FROM batch".into(),
        relations: vec![],
        memory_limit: None,
        threads: None,
    });
    let p1 = apply_stages_to_page(
        vec![json!({"v": 1}), json!({"v": 2})],
        std::slice::from_ref(&s),
    )
    .unwrap();
    let p2 = apply_stages_to_page(vec![json!({"v": 10})], std::slice::from_ref(&s)).unwrap();
    assert_eq!(p1[0]["total"], json!(3)); // per-page, not global
    assert_eq!(p2[0]["total"], json!(10));
}

/// `batch_size: 0` (single-page) gives global aggregation: the whole result set
/// arrives in one page, so the SUM is computed across every record at once.
#[test]
fn global_aggregation_single_page() {
    let out = run(
        "SELECT SUM(v) AS total FROM batch",
        vec![json!({"v": 1}), json!({"v": 2}), json!({"v": 10})],
    );
    assert_eq!(out, vec![json!({"total": 13})]);
}

#[test]
fn memory_limit_and_threads_pragmas_apply() {
    // Both pragmas must be accepted by DuckDB and the query still runs.
    let s = stage(SqlTransformConfig {
        query: "SELECT count(*) AS n FROM batch".into(),
        relations: vec![],
        memory_limit: Some("256MB".into()),
        threads: Some(2),
    });
    let out = apply_stages_to_page(vec![json!({"x": 1}), json!({"x": 2})], &[s]).unwrap();
    assert_eq!(out[0]["n"], json!(2));
}

#[test]
fn duplicate_relation_name_rejected() {
    let err = SqlTransform::compile(&SqlTransformConfig {
        query: "SELECT * FROM batch".into(),
        relations: vec![
            RelationSpec {
                name: "dup".into(),
                source: RelationSource::Values {
                    columns: vec!["a".into()],
                    rows: vec![vec![json!(1)]],
                },
                reload_on_change: false,
            },
            RelationSpec {
                name: "dup".into(),
                source: RelationSource::Values {
                    columns: vec!["b".into()],
                    rows: vec![vec![json!(2)]],
                },
                reload_on_change: false,
            },
        ],
        memory_limit: None,
        threads: None,
    })
    .unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("duplicate relation name 'dup'"), "got: {msg}");
}

#[test]
fn invalid_relation_name_rejected() {
    let err = SqlTransform::compile(&SqlTransformConfig {
        query: "SELECT * FROM batch".into(),
        relations: vec![RelationSpec {
            name: "1bad name".into(), // starts with a digit + has a space
            source: RelationSource::Values {
                columns: vec!["a".into()],
                rows: vec![vec![json!(1)]],
            },
            reload_on_change: false,
        }],
        memory_limit: None,
        threads: None,
    })
    .unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("invalid relation name '1bad name'"),
        "got: {msg}"
    );
}

#[test]
fn values_relation_with_no_columns_rejected() {
    let err = SqlTransform::compile(&SqlTransformConfig {
        query: "SELECT * FROM batch".into(),
        relations: vec![RelationSpec {
            name: "empty".into(),
            source: RelationSource::Values {
                columns: vec![],
                rows: vec![],
            },
            reload_on_change: false,
        }],
        memory_limit: None,
        threads: None,
    })
    .unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("values relation 'empty' has no columns"),
        "got: {msg}"
    );
}

#[test]
fn jsonl_reference_relation_join() {
    let path = format!("{}/tests/data/regions.jsonl", env!("CARGO_MANIFEST_DIR"));
    let s = stage(SqlTransformConfig {
        query: "SELECT b.id, r.region FROM batch b LEFT JOIN regions r ON b.code = r.code ORDER BY b.id".into(),
        relations: vec![RelationSpec {
            name: "regions".into(),
            source: RelationSource::Jsonl { path },
            reload_on_change: false,
        }],
        memory_limit: None,
        threads: None,
    });
    let out = apply_stages_to_page(
        vec![
            json!({"id": 1, "code": "US"}),
            json!({"id": 2, "code": "IN"}),
        ],
        &[s],
    )
    .unwrap();
    assert_eq!(out[0]["region"], json!("NA"));
    assert_eq!(out[1]["region"], json!("APAC"));
}

/// A query that fully validates at compile time (no `batch` reference at all)
/// takes the `Ok` arm of `validate_query` rather than the missing-`batch`
/// tolerance arm.
#[test]
fn query_without_batch_reference_compiles_and_runs() {
    let out = run("SELECT 1 AS one, 'hi' AS greeting", vec![json!({"x": 1})]);
    assert_eq!(out, vec![json!({"one": 1, "greeting": "hi"})]);
}

#[test]
fn debug_impl_exposes_query() {
    let t = SqlTransform::compile(&SqlTransformConfig {
        query: "SELECT * FROM batch".into(),
        relations: vec![],
        memory_limit: None,
        threads: None,
    })
    .unwrap();
    let dbg = format!("{t:?}");
    assert!(dbg.contains("SqlTransform"), "got: {dbg}");
    assert!(dbg.contains("SELECT * FROM batch"), "got: {dbg}");
}

/// A CSV reference relation with `reload_on_change: true` must pick up edits to
/// the file between pages (re-stat mtime → rebuild + swap the table).
#[test]
fn reload_on_change_csv_picks_up_edits_between_pages() {
    use std::io::Write;
    let dir = std::env::temp_dir();
    let path = dir.join(format!(
        "faucet_sql_reload_{}_{}.csv",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let path_str = path.to_string_lossy().to_string();

    std::fs::write(&path, "code,country\nUS,United States\n").unwrap();

    let s = stage(SqlTransformConfig {
        query: "SELECT b.id, c.country FROM batch b LEFT JOIN refs c ON b.code = c.code".into(),
        relations: vec![RelationSpec {
            name: "refs".into(),
            source: RelationSource::Csv {
                path: path_str.clone(),
                has_header: true,
            },
            reload_on_change: true,
        }],
        memory_limit: None,
        threads: None,
    });

    let p1 = apply_stages_to_page(
        vec![json!({"id": 1, "code": "US"})],
        std::slice::from_ref(&s),
    )
    .unwrap();
    assert_eq!(p1[0]["country"], json!("United States"));

    // Rewrite the file with a different value for US; sleep guards against the
    // filesystem mtime resolution coalescing the two writes.
    std::thread::sleep(std::time::Duration::from_millis(1100));
    {
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(b"code,country\nUS,Reloaded\n").unwrap();
        f.flush().unwrap();
    }

    let p2 = apply_stages_to_page(
        vec![json!({"id": 2, "code": "US"})],
        std::slice::from_ref(&s),
    )
    .unwrap();
    assert_eq!(p2[0]["country"], json!("Reloaded"));

    let _ = std::fs::remove_file(&path);
}

/// Same as above for a JSONL relation — exercises the non-CSV reload branch.
#[test]
fn reload_on_change_jsonl_picks_up_edits_between_pages() {
    use std::io::Write;
    let dir = std::env::temp_dir();
    let path = dir.join(format!(
        "faucet_sql_reload_{}_{}.jsonl",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let path_str = path.to_string_lossy().to_string();

    std::fs::write(&path, "{\"code\":\"US\",\"region\":\"NA\"}\n").unwrap();

    let s = stage(SqlTransformConfig {
        query: "SELECT b.id, r.region FROM batch b LEFT JOIN refs r ON b.code = r.code".into(),
        relations: vec![RelationSpec {
            name: "refs".into(),
            source: RelationSource::Jsonl {
                path: path_str.clone(),
            },
            reload_on_change: true,
        }],
        memory_limit: None,
        threads: None,
    });

    let p1 = apply_stages_to_page(
        vec![json!({"id": 1, "code": "US"})],
        std::slice::from_ref(&s),
    )
    .unwrap();
    assert_eq!(p1[0]["region"], json!("NA"));

    std::thread::sleep(std::time::Duration::from_millis(1100));
    {
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(b"{\"code\":\"US\",\"region\":\"EMEA\"}\n")
            .unwrap();
        f.flush().unwrap();
    }

    let p2 = apply_stages_to_page(
        vec![json!({"id": 2, "code": "US"})],
        std::slice::from_ref(&s),
    )
    .unwrap();
    assert_eq!(p2[0]["region"], json!("EMEA"));

    let _ = std::fs::remove_file(&path);
}

// ── Compile-time validation must fail CLOSED (#654 H7) ──────────────────────

/// Compile just the config (no page run), returning the validation result.
fn compile_only(query: &str) -> Result<(), String> {
    SqlTransform::compile(&SqlTransformConfig {
        query: query.into(),
        relations: vec![],
        memory_limit: None,
        threads: None,
    })
    .map(|_| ())
    .map_err(|e| e.to_string())
}

#[test]
fn validation_rejects_a_genuinely_missing_table() {
    // The audit's exact case: the old rule tolerated ANY error whose text
    // contained "batch", so a join against a table that does not exist was
    // accepted — the advertised compile-time gate passed and the pipeline
    // died on page 1 of a real run. The missing table's name deliberately
    // contains the word "batch".
    let err = compile_only("SELECT * FROM batch b JOIN daily_batches d USING (id)")
        .expect_err("a missing table must fail validation");
    assert!(
        err.contains("daily_batches"),
        "the error must name the missing table: {err}"
    );
}

#[test]
fn validation_rejects_syntax_errors_and_unknown_functions() {
    assert!(compile_only("SELECT * FROM").is_err(), "syntax error");
    assert!(
        compile_only("SELECT no_such_fn(id) FROM batch").is_err(),
        "unknown function"
    );
}

#[test]
fn validation_still_accepts_the_unknown_page_schema() {
    // `batch`'s columns only exist once the first page arrives, so selecting
    // them must NOT fail at config-load time — in either DuckDB wording
    // (unqualified, and qualified through an alias).
    compile_only("SELECT id, SUM(amount) AS total FROM batch GROUP BY id")
        .expect("unqualified batch columns are tolerated");
    compile_only("SELECT b.id, b.amount FROM batch b WHERE b.amount > 0")
        .expect("qualified batch columns are tolerated");
}

#[test]
fn the_validation_placeholder_never_reaches_output() {
    // Validation registers a zero-row `batch` placeholder; the real run must
    // replace it, so no placeholder column can appear in transformed records.
    let out = run("SELECT * FROM batch", vec![json!({"id": 1, "v": "a"})]);
    assert_eq!(out, vec![json!({"id": 1, "v": "a"})]);
}
