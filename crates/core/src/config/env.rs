//! A flat serde deserializer over prefixed environment variables.

use std::ffi::OsString;
use std::fmt;

use serde::de::{
    self, DeserializeOwned, DeserializeSeed, IntoDeserializer, MapAccess, SeqAccess, Visitor,
};

#[derive(Debug)]
pub(crate) struct Error(String);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

impl de::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Error(msg.to_string())
    }

    fn missing_field(field: &'static str) -> Self {
        Error(format!("missing value for field {field}"))
    }
}

/// Deserialize `T` from the variables starting with `prefix`; each key is the
/// rest of the name, lower-cased. Variables whose name or value is not UTF-8
/// are skipped.
pub(crate) fn from_vars<T, I>(prefix: &str, vars: I) -> Result<T, Error>
where
    T: DeserializeOwned,
    I: IntoIterator<Item = (OsString, OsString)>,
{
    let entries: Vec<(String, String)> = vars
        .into_iter()
        .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)))
        .filter_map(|(k, v)| {
            let rest = k.strip_prefix(prefix)?;
            (!rest.is_empty()).then(|| (rest.to_lowercase(), v))
        })
        .collect();
    T::deserialize(Vars {
        entries: entries.into_iter(),
        value: None,
    })
}

struct Vars<I> {
    entries: I,
    value: Option<String>,
}

impl<'de, I: Iterator<Item = (String, String)>> de::Deserializer<'de> for Vars<I> {
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        visitor.visit_map(self)
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map struct enum identifier ignored_any
    }
}

impl<'de, I: Iterator<Item = (String, String)>> MapAccess<'de> for Vars<I> {
    type Error = Error;

    fn next_key_seed<K: DeserializeSeed<'de>>(&mut self, seed: K) -> Result<Option<K::Value>, Error> {
        match self.entries.next() {
            Some((key, value)) => {
                self.value = Some(value);
                seed.deserialize(key.into_deserializer()).map(Some)
            }
            None => Ok(None),
        }
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(&mut self, seed: V) -> Result<V::Value, Error> {
        let value = self.value.take().expect("next_value_seed follows next_key_seed");
        seed.deserialize(Text(value))
    }
}

/// One variable's value, parsed as whatever type the field asks for.
struct Text(String);

impl Text {
    fn parse<T: std::str::FromStr>(&self, what: &str) -> Result<T, Error> {
        self.0
            .trim()
            .parse()
            .map_err(|_| Error(format!("{:?} is not a valid {what}", self.0)))
    }
}

macro_rules! parse_as {
    ($($method:ident => $visit:ident : $ty:ty),* $(,)?) => {
        $(
            fn $method<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
                visitor.$visit(self.parse::<$ty>(stringify!($ty))?)
            }
        )*
    };
}

impl<'de> de::Deserializer<'de> for Text {
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        visitor.visit_string(self.0)
    }

    parse_as! {
        deserialize_bool => visit_bool: bool,
        deserialize_i8 => visit_i8: i8,
        deserialize_i16 => visit_i16: i16,
        deserialize_i32 => visit_i32: i32,
        deserialize_i64 => visit_i64: i64,
        deserialize_i128 => visit_i128: i128,
        deserialize_u8 => visit_u8: u8,
        deserialize_u16 => visit_u16: u16,
        deserialize_u32 => visit_u32: u32,
        deserialize_u64 => visit_u64: u64,
        deserialize_u128 => visit_u128: u128,
        deserialize_f32 => visit_f32: f32,
        deserialize_f64 => visit_f64: f64,
        deserialize_char => visit_char: char,
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        visitor.visit_some(self)
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Error> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        let items: Vec<String> = if self.0.is_empty() {
            Vec::new()
        } else {
            self.0.split(',').map(str::to_string).collect()
        };
        visitor.visit_seq(Items(items.into_iter()))
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Error> {
        visitor.visit_enum(self.0.into_deserializer())
    }

    serde::forward_to_deserialize_any! {
        str string bytes byte_buf unit unit_struct tuple tuple_struct map struct
        identifier ignored_any
    }
}

struct Items(std::vec::IntoIter<String>);

impl<'de> SeqAccess<'de> for Items {
    type Error = Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, Error> {
        self.0.next().map(|item| seed.deserialize(Text(item))).transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    fn vars(pairs: &[(&str, &str)]) -> Vec<(OsString, OsString)> {
        pairs
            .iter()
            .map(|(k, v)| (OsString::from(k), OsString::from(v)))
            .collect()
    }

    #[derive(Debug, Deserialize, PartialEq)]
    #[serde(rename_all = "snake_case")]
    enum Mode {
        Fast,
        Safe,
    }

    #[derive(Debug, Deserialize, PartialEq)]
    struct Newtype(u16);

    #[derive(Debug, Deserialize, PartialEq)]
    struct Cfg {
        project_id: String,
        batch_size: usize,
        ratio: f64,
        offset: i64,
        enabled: bool,
        mode: Mode,
        tags: Vec<String>,
        ports: Vec<Newtype>,
        initial: char,
        #[serde(default)]
        missing: Option<u32>,
        present: Option<u32>,
        #[serde(default)]
        empty: Vec<String>,
        numeric_text: String,
    }

    #[test]
    fn reads_every_flat_field_type() {
        let got: Cfg = from_vars(
            "APP_",
            vars(&[
                ("APP_PROJECT_ID", "p-1"),
                ("APP_BATCH_SIZE", " 500 "),
                ("APP_RATIO", "0.5"),
                ("APP_OFFSET", "-3"),
                ("APP_ENABLED", "true"),
                ("APP_MODE", "safe"),
                ("APP_TAGS", "a,b"),
                ("APP_PORTS", "80,443"),
                ("APP_INITIAL", "x"),
                ("APP_PRESENT", "7"),
                ("APP_EMPTY", ""),
                ("APP_NUMERIC_TEXT", "0042"),
                ("APP_", "ignored"),
                ("OTHER_PROJECT_ID", "ignored"),
            ]),
        )
        .unwrap();
        assert_eq!(
            got,
            Cfg {
                project_id: "p-1".into(),
                batch_size: 500,
                ratio: 0.5,
                offset: -3,
                enabled: true,
                mode: Mode::Safe,
                tags: vec!["a".into(), "b".into()],
                ports: vec![Newtype(80), Newtype(443)],
                initial: 'x',
                missing: None,
                present: Some(7),
                empty: Vec::new(),
                numeric_text: "0042".into(),
            }
        );
        assert_ne!(got.mode, Mode::Fast);
    }

    #[test]
    fn names_the_bad_value_and_the_missing_field() {
        #[derive(Debug, Deserialize)]
        #[allow(dead_code)]
        struct Small {
            n: u8,
        }
        let err = from_vars::<Small, _>("P_", vars(&[("P_N", "300")])).unwrap_err();
        assert_eq!(err.to_string(), "\"300\" is not a valid u8");
        let err = from_vars::<Small, _>("P_", vars(&[])).unwrap_err();
        assert_eq!(err.to_string(), "missing value for field n");
        let err = from_vars::<Cfg, _>("P_", vars(&[("P_MODE", "slow")])).unwrap_err();
        assert!(err.to_string().contains("slow"), "{err}");
        let err = from_vars::<Small, _>("P_", vars(&[("P_N", "")])).unwrap_err();
        assert!(err.to_string().contains("u8"), "{err}");
    }

    #[cfg(unix)]
    #[test]
    fn skips_variables_that_are_not_utf8() {
        use std::os::unix::ffi::OsStringExt;
        #[derive(Debug, Deserialize, PartialEq)]
        struct One {
            a: String,
        }
        let mut v = vars(&[("Q_A", "ok")]);
        v.push((OsString::from("Q_B"), OsString::from_vec(vec![0xff])));
        assert_eq!(
            from_vars::<One, _>("Q_", v).unwrap(),
            One { a: "ok".into() }
        );
    }
}
