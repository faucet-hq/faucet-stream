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
