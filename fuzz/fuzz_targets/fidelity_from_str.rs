#![no_main]

use faucet_core::fidelity;
use libfuzzer_sys::fuzz_target;
use serde_json::Value;

fuzz_target!(|text: &str| {
    let plain: Result<Value, _> = serde_json::from_str(text);
    let faithful = fidelity::from_str(text);
    match (plain, faithful) {
        (Ok(plain), Ok(faithful)) => assert_same_shape(&plain, &faithful),
        (Ok(_), Err(e)) => panic!("serde_json accepts {text:?} but fidelity::from_str fails: {e}"),
        (Err(_), Ok(v)) => panic!("serde_json rejects {text:?} but fidelity::from_str returns {v}"),
        (Err(_), Err(_)) => {}
    }
    assert_eq!(
        fidelity::from_slice(text.as_bytes()).ok(),
        fidelity::from_str(text).ok()
    );
});

/// The faithful parse may only differ from serde_json's by spelling a number
/// as the string of its exact digits.
fn assert_same_shape(plain: &Value, faithful: &Value) {
    match (plain, faithful) {
        (Value::Number(n), Value::String(s)) => {
            let reparsed: f64 = s
                .parse()
                .unwrap_or_else(|_| panic!("{s:?} is not a number"));
            let expected = n.as_f64().expect("serde_json numbers are finite");
            assert!(
                reparsed == expected || (reparsed - expected).abs() <= expected.abs() * 1e-15,
                "{s:?} does not spell {n}"
            );
        }
        (Value::Array(a), Value::Array(b)) => {
            assert_eq!(a.len(), b.len());
            a.iter().zip(b).for_each(|(x, y)| assert_same_shape(x, y));
        }
        (Value::Object(a), Value::Object(b)) => {
            assert_eq!(a.len(), b.len());
            for (k, x) in a {
                assert_same_shape(x, b.get(k).unwrap_or_else(|| panic!("key {k:?} lost")));
            }
        }
        (a, b) => assert_eq!(a, b),
    }
}
