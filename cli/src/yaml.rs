//! The one place faucet reads and writes YAML (#929).
//!
//! Every YAML parse, render and tree in the CLI goes through this module, so
//! moving to another YAML library changes this file and nothing else. The
//! golden tests below pin the behaviour any backend must keep; a scan test
//! (`cli/tests/yaml_boundary.rs`) fails when the library is named elsewhere.

use std::fmt;

use serde::Serialize;
use serde::de::DeserializeOwned;

/// A YAML node tree: mappings keep their order and their non-string keys,
/// and tags (`!include`) survive.
pub type Value = yaml_serde::Value;

/// An ordered YAML mapping.
pub type Mapping = yaml_serde::Mapping;

/// A tagged YAML node (`!tag value`).
pub type TaggedValue = yaml_serde::value::TaggedValue;

/// A YAML parse or render failure. Its text names the line and column when
/// the parser reports one.
#[derive(Debug)]
pub struct Error(yaml_serde::Error);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

/// Deserialize `T` from YAML (or JSON, which is a subset of YAML). A mapping
/// that repeats a key is refused whatever `T` is: deserializing straight into
/// a map type would otherwise keep the last value silently.
pub fn from_str<T: DeserializeOwned>(text: &str) -> Result<T, Error> {
    yaml_serde::from_str::<Value>(text).map_err(Error)?;
    yaml_serde::from_str(text).map_err(Error)
}

/// Render `value` as a YAML document.
pub fn to_string<T: Serialize + ?Sized>(value: &T) -> Result<String, Error> {
    yaml_serde::to_string(value).map_err(Error)
}

/// Convert `value` into a YAML node tree.
pub fn to_value<T: Serialize>(value: T) -> Result<Value, Error> {
    yaml_serde::to_value(value).map_err(Error)
}

/// Deserialize `T` from a YAML node tree.
pub fn from_value<T: DeserializeOwned>(value: Value) -> Result<T, Error> {
    yaml_serde::from_value(value).map_err(Error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value as Json, json};

    fn json(text: &str) -> Json {
        from_str(text).unwrap()
    }

    #[test]
    fn scalars_resolve_as_yaml_1_2() {
        assert_eq!(
            json("a: yes\nb: no\nc: on\nd: true\ne: False\nf: ~\ng: null\nh: ''"),
            json!({"a": "yes", "b": "no", "c": "on", "d": true, "e": false,
                   "f": null, "g": null, "h": ""})
        );
    }

    #[test]
    fn number_forms_are_pinned() {
        assert_eq!(
            json(
                "a: 1\nb: -7\nc: 1.5\nd: 1e3\ne: 0x1F\nf: 0o17\ng: 010\nh: 1_000\ni: '1'\nj: 18446744073709551615"
            ),
            json!({"a": 1, "b": -7, "c": 1.5, "d": 1000.0, "e": 31, "f": 15,
                   "g": "010", "h": "1_000", "i": "1", "j": 18446744073709551615u64})
        );
        let v: Value = from_str("a: .inf\nb: .nan").unwrap();
        assert!(v["a"].as_f64().unwrap().is_infinite());
        assert!(v["b"].as_f64().unwrap().is_nan());
    }

    #[test]
    fn duplicate_keys_are_refused_for_every_target() {
        #[derive(serde::Deserialize, Debug)]
        #[allow(dead_code)]
        struct S {
            a: u8,
            b: u8,
        }
        let text = "a: 1\nb: 2\na: 3\n";
        for err in [
            from_str::<Json>(text).unwrap_err().to_string(),
            from_str::<Value>(text).unwrap_err().to_string(),
            from_str::<S>(text).unwrap_err().to_string(),
            from_str::<Json>("x:\n  a: 1\n  a: 2\n")
                .unwrap_err()
                .to_string(),
            from_str::<Json>(r#"{"a": 1, "a": 2}"#)
                .unwrap_err()
                .to_string(),
        ] {
            assert!(err.contains("duplicate"), "{err}");
        }
        assert_eq!(json("a: 1\nb: {a: 2}"), json!({"a": 1, "b": {"a": 2}}));
    }

    #[test]
    fn errors_name_the_line_and_column() {
        let err = from_str::<Json>("a: [1, 2\nb: 3\n").unwrap_err();
        let text = err.to_string();
        assert!(text.contains("line"), "{text}");
        assert!(std::error::Error::source(&err).is_none() || !text.is_empty());
        let err = from_str::<u8>("300").unwrap_err().to_string();
        assert!(err.contains("invalid value"), "{err}");
    }

    #[test]
    fn json_text_parses_as_yaml() {
        assert_eq!(
            json(r#"{"a": [1, "x"], "b": {"c": null}}"#),
            json!({"a": [1, "x"], "b": {"c": null}})
        );
    }

    #[test]
    fn render_layout_is_pinned() {
        let v = json!({"name": "p", "pipeline": {"source": {"type": "csv", "config": {"path": "a b.csv"}}},
                       "matrix": [{"id": "r1"}, {"id": "02"}], "empty": [], "none": null, "on": "yes"});
        assert_eq!(
            to_string(&v).unwrap(),
            "name: p\npipeline:\n  source:\n    type: csv\n    config:\n      path: a b.csv\nmatrix:\n- id: r1\n- id: '02'\nempty: []\nnone: null\non: yes\n"
        );
    }

    #[test]
    fn trees_keep_order_tags_and_round_trip() {
        let v: Value = from_str("z: 1\na: !include part.yaml\n1: x\n").unwrap();
        let keys: Vec<String> = v
            .as_mapping()
            .unwrap()
            .keys()
            .map(|k| to_string(k).unwrap().trim().to_string())
            .collect();
        assert_eq!(keys, ["z", "a", "1"]);
        match &v["a"] {
            Value::Tagged(t) => {
                let t: &TaggedValue = t;
                assert_eq!(t.tag, "!include");
                assert_eq!(t.value.as_str(), Some("part.yaml"));
            }
            other => panic!("expected a tagged node, got {other:?}"),
        }
        let text = to_string(&v).unwrap();
        assert_eq!(text, "z: 1\na: !include part.yaml\n1: x\n");
        assert_eq!(from_str::<Value>(&text).unwrap(), v);

        let mut m = Mapping::new();
        m.insert(Value::String("k".into()), to_value(vec![1, 2]).unwrap());
        let back: Json = from_value(Value::Mapping(m)).unwrap();
        assert_eq!(back, json!({"k": [1, 2]}));
        assert!(from_value::<u8>(Value::String("x".into())).is_err());
        assert!(to_value(f64::NAN).is_ok());
    }
}
