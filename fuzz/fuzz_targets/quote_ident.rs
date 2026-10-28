#![no_main]

use faucet_core::util::quote_ident;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|name: &str| {
    let quoted = quote_ident(name);
    let inner = quoted
        .strip_prefix('"')
        .and_then(|rest| rest.strip_suffix('"'))
        .expect("a quoted identifier is wrapped in double quotes");
    // Every embedded quote is doubled, so the identifier cannot end early.
    let mut chars = inner.chars();
    while let Some(c) = chars.next() {
        if c == '"' {
            assert_eq!(chars.next(), Some('"'), "lone quote in {quoted:?}");
        }
    }
    assert_eq!(inner.replace("\"\"", "\""), name);
});
