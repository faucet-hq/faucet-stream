use super::*;
use serde_json::json;

fn cols(names: &[&str]) -> Vec<String> {
    names.iter().map(|s| (*s).to_owned()).collect()
}

fn obj(v: Value) -> Map<String, Value> {
    v.as_object().unwrap().clone()
}

#[test]
fn column_for_field_per_rule() {
    let c = cols(&["id", "Email", "NAME", "Ab", "AB"]);
    assert_eq!(
        column_for_field("id", &c, ColumnMatching::Exact),
        Some("id")
    );
    assert_eq!(column_for_field("email", &c, ColumnMatching::Exact), None);
    assert_eq!(
        column_for_field("email", &c, ColumnMatching::CaseInsensitive),
        Some("Email")
    );
    assert_eq!(
        column_for_field("email", &c, ColumnMatching::FoldUnique),
        Some("Email")
    );
    assert_eq!(column_for_field("ab", &c, ColumnMatching::FoldUnique), None);
    assert_eq!(
        column_for_field("name", &c, ColumnMatching::UpperFold),
        Some("NAME")
    );
    assert_eq!(
        column_for_field("email", &c, ColumnMatching::UpperFold),
        None
    );
    assert_eq!(ColumnMatching::default(), ColumnMatching::Exact);
}

#[test]
fn the_exact_twin_is_written_and_the_other_is_shadowed() {
    let c = cols(&["email"]);
    let o = obj(json!({"Email": "a", "email": "b", "extra": 1}));
    let r: Vec<_> = resolve_fields(&o, &c, ColumnMatching::CaseInsensitive).collect();
    assert!(r.contains(&("email", &json!("b"), Resolution::Column("email"))));
    assert!(r.contains(&("Email", &json!("a"), Resolution::Shadowed("email"))));
    assert!(r.contains(&("extra", &json!(1), Resolution::Unmatched)));
    assert_eq!(
        canonical_record(&o, &c, ColumnMatching::CaseInsensitive),
        obj(json!({"email": "b", "extra": 1}))
    );
}

#[test]
fn canonical_record_uses_the_column_spelling() {
    let c = cols(&["EMAIL", "ID"]);
    let o = obj(json!({"email": "x", "id": 1}));
    assert_eq!(
        canonical_record(&o, &c, ColumnMatching::UpperFold),
        obj(json!({"EMAIL": "x", "ID": 1}))
    );
    assert_eq!(canonical_record(&o, &c, ColumnMatching::Exact), o);
}

#[test]
fn field_for_column_picks_like_the_write_path() {
    let c = cols(&["Email"]);
    let o = obj(json!({"EMAIL": 1, "email": 2}));
    let picked = field_for_column("Email", &c, &o, ColumnMatching::CaseInsensitive).unwrap();
    assert_eq!(
        picked.0,
        o.keys().find(|k| k.eq_ignore_ascii_case("Email")).unwrap()
    );
    assert!(field_for_column("Email", &c, &o, ColumnMatching::Exact).is_none());
    assert!(field_for_column("Email", &c, &o, ColumnMatching::UpperFold).is_none());
    let upper = cols(&["EMAIL"]);
    let o = obj(json!({"email": 2}));
    assert_eq!(
        field_for_column("EMAIL", &upper, &o, ColumnMatching::UpperFold),
        Some(("email", &json!(2)))
    );
}

const RULES: [ColumnMatching; 4] = [
    ColumnMatching::Exact,
    ColumnMatching::CaseInsensitive,
    ColumnMatching::FoldUnique,
    ColumnMatching::UpperFold,
];

fn column_sets() -> Vec<Vec<String>> {
    vec![
        cols(&[]),
        cols(&["id", "Email", "NAME", "Ab", "AB"]),
        cols(&["ID", "EMAIL", "name", "ÉCOLE", "straße"]),
        cols(&["id", "id", "Id", "value"]),
        cols(&["UserId", "userid", "USERID"]),
        cols(&["a", "B", "c_1", "C_1", "ünï"]),
    ]
}

fn field_names() -> Vec<&'static str> {
    vec![
        "id", "ID", "Id", "iD", "email", "Email", "EMAIL", "name", "NAME", "Name", "ab", "Ab",
        "AB", "aB", "école", "ÉCOLE", "strasse", "STRASSE", "straße", "value", "VALUE", "userid",
        "UserId", "USERID", "UserID", "a", "A", "b", "B", "c_1", "C_1", "ünï", "ÜNÏ", "extra", "",
    ]
}

fn objects() -> Vec<Map<String, Value>> {
    let names = field_names();
    let mut out = vec![Map::new()];
    for width in [1usize, 2, 3, 5] {
        for start in 0..names.len() {
            let mut o = Map::new();
            for (n, step) in (0..width).zip([1usize, 7, 3, 11, 5]) {
                let name = names[(start + n * step) % names.len()];
                o.insert(name.to_owned(), json!(format!("{name}-{n}")));
            }
            out.push(o);
        }
    }
    out
}

#[test]
fn the_resolver_answers_like_the_free_functions() {
    for columns in column_sets() {
        for matching in RULES {
            let resolver = ColumnResolver::new(columns.clone(), matching);
            assert_eq!(resolver.columns(), columns.as_slice());
            assert_eq!(resolver.matching(), matching);
            for field in field_names() {
                let expected = column_for_field(field, &columns, matching);
                assert_eq!(
                    resolver.column_for_field(field),
                    expected,
                    "{field:?} in {columns:?} under {matching:?}"
                );
                assert_eq!(
                    resolver.index_of(field).map(|i| columns[i].as_str()),
                    expected
                );
            }
            for obj in objects() {
                for column in columns.iter().map(String::as_str).chain(field_names()) {
                    assert_eq!(
                        resolver.field_for_column(column, &obj),
                        field_for_column(column, &columns, &obj, matching),
                        "{column:?} of {obj:?} in {columns:?} under {matching:?}"
                    );
                }
                let expected = canonical_record(&obj, &columns, matching);
                let got = resolver.canonical_record(&obj);
                assert_eq!(*got, expected, "{obj:?} in {columns:?} under {matching:?}");
                assert_eq!(
                    got.keys().collect::<Vec<_>>(),
                    expected.keys().collect::<Vec<_>>()
                );
                if matches!(got, Cow::Borrowed(_)) {
                    assert_eq!(expected, obj);
                }
            }
        }
    }
}

#[test]
fn a_record_already_keyed_by_its_columns_is_borrowed() {
    let resolver = ColumnResolver::new(cols(&["id", "Email"]), ColumnMatching::CaseInsensitive);
    let settled = obj(json!({"id": 1, "Email": "a", "extra": true}));
    assert!(matches!(
        resolver.canonical_record(&settled),
        Cow::Borrowed(_)
    ));
    let renamed = obj(json!({"ID": 1, "Email": "a"}));
    match resolver.canonical_record(&renamed) {
        Cow::Owned(out) => assert_eq!(out, obj(json!({"id": 1, "Email": "a"}))),
        Cow::Borrowed(_) => panic!("a renamed field must be rewritten"),
    }
}

fn ambiguous_reference<'r, 'c>(
    obj: &'r Map<String, Value>,
    columns: &'c [String],
) -> Option<AmbiguousFold<'r, 'c>> {
    for column in columns {
        if obj.contains_key(column) {
            continue;
        }
        let folded: Vec<&String> = obj
            .keys()
            .filter(|k| k.eq_ignore_ascii_case(column))
            .collect();
        if let [a, b, ..] = folded.as_slice() {
            return Some(AmbiguousFold {
                column,
                first: a,
                second: b,
            });
        }
    }
    None
}

#[test]
fn ambiguous_folds_are_found_like_a_scan_of_every_column() {
    let mut found = 0;
    for columns in column_sets() {
        let resolver = ColumnResolver::new(columns.clone(), ColumnMatching::CaseInsensitive);
        for obj in objects() {
            let expected = ambiguous_reference(&obj, &columns);
            found += usize::from(expected.is_some());
            assert_eq!(
                resolver.ambiguous_fold(&obj),
                expected,
                "{obj:?} in {columns:?}"
            );
        }
    }
    assert!(found > 0, "the corpus must hold ambiguous records");
    let resolver = ColumnResolver::new(cols(&["UserId", "Name"]), ColumnMatching::CaseInsensitive);
    let two = obj(json!({"userid": 1, "USERID": 2, "name": "x"}));
    let fold = resolver.ambiguous_fold(&two).unwrap();
    assert_eq!(fold.column, "UserId");
    assert_eq!(
        [fold.first, fold.second],
        [
            two.keys()
                .find(|k| k.eq_ignore_ascii_case("userid"))
                .unwrap(),
            two.keys()
                .rfind(|k| k.eq_ignore_ascii_case("userid"))
                .unwrap()
        ]
    );
    let exact = obj(json!({"UserId": 1, "userid": 2, "USERID": 3}));
    assert_eq!(resolver.ambiguous_fold(&exact), None);
}
