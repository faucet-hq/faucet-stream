//! Value-based PII detectors (issue #206). Every detector fully anchors its
//! match. The card detector additionally requires a card-network prefix and
//! length and a valid Luhn checksum; Luhn alone passes one number in ten, so
//! without the network table epoch-millisecond timestamps and long ids would be
//! rewritten. Numbers that begin like a card (ids starting 3, 4, 5, 6 or 8 at
//! card lengths) still match about one time in ten.

use super::config::Detector;
use regex::Regex;
use std::sync::OnceLock;

/// Return `true` if `value` looks like an instance of `detector`'s PII class.
pub fn detects(detector: Detector, value: &str) -> bool {
    match detector {
        Detector::Email => email_re().is_match(value),
        Detector::Ssn => is_ssn(value),
        Detector::Phone => is_phone(value),
        Detector::Ipv4 => is_ipv4(value),
        Detector::CreditCard => is_credit_card(value),
        Detector::PhoneLoose => is_phone_loose(value),
        Detector::CreditCardLoose => card_digits(value).is_some_and(|d| luhn_valid(&d)),
    }
}

/// [`detects`] over a JSON scalar. Strings go through every detector; a JSON
/// number only through the card detectors (an integer can hold a card number)
/// and through `phone_loose`, which opts into matching one,
/// while ids, epoch timestamps and amounts are numbers that look like phone
/// numbers); booleans and containers never match.
pub fn detects_value(detector: Detector, value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::String(s) => detects(detector, s),
        serde_json::Value::Number(n) if matches_numbers(detector) => n
            .as_u64()
            .is_some_and(|u| detects(detector, &u.to_string())),
        _ => false,
    }
}

/// Whether `detector` can match a JSON number, so a rule using it may turn a
/// numeric value into a string.
pub(crate) fn matches_numbers(detector: Detector) -> bool {
    matches!(
        detector,
        Detector::CreditCard | Detector::CreditCardLoose | Detector::PhoneLoose
    )
}

fn email_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    // Conservative single-@ RFC-5322 subset, fully anchored.
    RE.get_or_init(|| Regex::new(r"(?i)^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$").unwrap())
}

/// US SSN `NNN-NN-NNNN`. The `regex` crate has no look-around, so the shape is
/// matched with a plain regex and the never-issued area/group/serial groups
/// (000 / 666 / 9xx area, 00 group, 0000 serial) are excluded numerically —
/// which keeps false positives low.
fn is_ssn(value: &str) -> bool {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| Regex::new(r"^(\d{3})-(\d{2})-(\d{4})$").unwrap());
    let Some(caps) = re.captures(value) else {
        return false;
    };
    let area = &caps[1];
    let group = &caps[2];
    let serial = &caps[3];
    area != "000" && area != "666" && !area.starts_with('9') && group != "00" && serial != "0000"
}

/// A phone number with real phone structure: E.164 (`+` then 8–15 digits,
/// separators allowed), or a North-American number written with separators
/// (`415-555-2671`, `(415) 555-2671`, `1 415 555 2671`). A bare run of digits
/// is not a phone number — ids and epoch timestamps look exactly like one.
fn is_phone(value: &str) -> bool {
    if let Some(rest) = value.strip_prefix('+') {
        let mut digits = 0usize;
        for ch in rest.chars() {
            match ch {
                '0'..='9' => digits += 1,
                ' ' | '.' | '-' | '(' | ')' => {}
                _ => return false,
            }
        }
        return rest.starts_with(|c: char| ('1'..='9').contains(&c)) && (8..=15).contains(&digits);
    }
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"^(?:1[ .-]?)?(?:\(\d{3}\) ?|\d{3}[ .-])\d{3}[ .-]\d{4}$").unwrap()
    })
    .is_match(value)
}

/// The pre-1.13 `phone` matcher: digits and the usual separators only, `+`
/// only in front, 10–15 digits in all.
fn is_phone_loose(value: &str) -> bool {
    let mut digits = 0usize;
    for (i, ch) in value.chars().enumerate() {
        match ch {
            '0'..='9' => digits += 1,
            '+' if i == 0 => {}
            ' ' | '.' | '-' | '(' | ')' => {}
            _ => return false,
        }
    }
    (10..=15).contains(&digits)
}

fn is_ipv4(value: &str) -> bool {
    let mut octets = 0;
    for part in value.split('.') {
        octets += 1;
        if octets > 4 {
            return false;
        }
        // 1–3 digits, no leading zeros (except "0" itself), value ≤ 255.
        if part.is_empty() || part.len() > 3 || !part.bytes().all(|b| b.is_ascii_digit()) {
            return false;
        }
        if part.len() > 1 && part.starts_with('0') {
            return false;
        }
        if part.parse::<u16>().unwrap_or(256) > 255 {
            return false;
        }
    }
    octets == 4
}

/// A card network's issuer range: the first `prefix_len` digits fall in
/// `lo..=hi` and the number has one of `lengths` digits.
struct CardRange {
    lo: u32,
    hi: u32,
    prefix_len: usize,
    lengths: &'static [usize],
}

const fn range(lo: u32, hi: u32, prefix_len: usize, lengths: &'static [usize]) -> CardRange {
    CardRange {
        lo,
        hi,
        prefix_len,
        lengths,
    }
}

const L16_19: &[usize] = &[16, 17, 18, 19];
const L14_19: &[usize] = &[14, 15, 16, 17, 18, 19];
const L13_19: &[usize] = &[13, 14, 15, 16, 17, 18, 19];

/// Issuer ranges of the card networks: Visa, Mastercard, Amex, Discover, JCB,
/// Diners, UnionPay, Maestro, Mir, RuPay, Troy, Verve and UATP. Deliberately
/// broad, so a real card is not missed.
const CARD_NETWORKS: &[CardRange] = &[
    range(4, 4, 1, &[13, 16, 19]),
    range(51, 55, 2, &[16]),
    range(2221, 2720, 4, &[16]),
    range(34, 34, 2, &[15]),
    range(37, 37, 2, &[15]),
    range(6011, 6011, 4, L16_19),
    range(644, 649, 3, L16_19),
    range(65, 65, 2, L16_19),
    range(622_126, 622_925, 6, L16_19),
    range(3528, 3589, 4, L16_19),
    range(300, 305, 3, L14_19),
    range(3095, 3095, 4, L14_19),
    range(36, 36, 2, L14_19),
    range(38, 39, 2, L14_19),
    range(62, 62, 2, L16_19),
    range(81, 81, 2, L16_19),
    range(50, 50, 2, L13_19),
    range(56, 69, 2, L13_19),
    range(2200, 2204, 4, L16_19),
    range(60, 60, 2, &[16]),
    range(82, 82, 2, &[16]),
    range(508, 508, 3, &[16]),
    range(9792, 9792, 4, &[16]),
    range(506, 507, 3, L16_19),
    range(650, 650, 3, L16_19),
    range(1, 1, 1, &[15]),
];

fn issued_by_a_network(digits: &[u8]) -> bool {
    CARD_NETWORKS.iter().any(|n| {
        n.lengths.contains(&digits.len())
            && (n.lo..=n.hi).contains(
                &digits[..n.prefix_len]
                    .iter()
                    .fold(0u32, |acc, d| acc * 10 + u32::from(*d)),
            )
    })
}

/// A card number: 13–19 digits (single space/dash separators allowed) with a
/// card network's prefix and length, passing the Luhn checksum.
fn is_credit_card(value: &str) -> bool {
    card_digits(value).is_some_and(|d| issued_by_a_network(&d) && luhn_valid(&d))
}

/// The digits of a 13–19 digit candidate written with single space/dash
/// separators, or `None` when `value` is not shaped like one.
fn card_digits(value: &str) -> Option<Vec<u8>> {
    // Reject anything that isn't digits + single space/dash separators.
    let mut digits: Vec<u8> = Vec::with_capacity(19);
    let mut prev_sep = true; // disallow leading separator
    for ch in value.chars() {
        match ch {
            '0'..='9' => {
                digits.push(ch as u8 - b'0');
                prev_sep = false;
            }
            ' ' | '-' => {
                if prev_sep {
                    return None; // no leading / doubled separators
                }
                prev_sep = true;
            }
            _ => return None,
        }
    }
    if prev_sep && !digits.is_empty() && (value.ends_with(' ') || value.ends_with('-')) {
        return None; // trailing separator
    }
    (13..=19).contains(&digits.len()).then_some(digits)
}

/// Luhn (mod-10) checksum over already-parsed digits.
fn luhn_valid(digits: &[u8]) -> bool {
    let mut sum = 0u32;
    let mut double = false;
    for &d in digits.iter().rev() {
        let mut v = d as u32;
        if double {
            v *= 2;
            if v > 9 {
                v -= 9;
            }
        }
        sum += v;
        double = !double;
    }
    sum.is_multiple_of(10)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_number_matching_detectors_match_numbers() {
        let numbers = [
            serde_json::json!(4111111111111111u64),
            serde_json::json!(4155552671u64),
        ];
        for det in [
            Detector::Email,
            Detector::CreditCard,
            Detector::Ssn,
            Detector::Phone,
            Detector::Ipv4,
            Detector::PhoneLoose,
            Detector::CreditCardLoose,
        ] {
            let hit = numbers.iter().any(|n| detects_value(det, n));
            assert_eq!(hit, matches_numbers(det), "{det}");
        }
        assert!(!detects_value(Detector::CreditCard, &serde_json::json!(-4)));
        assert!(!detects_value(
            Detector::CreditCard,
            &serde_json::json!(1.5)
        ));
    }

    #[test]
    fn phone_loose_keeps_the_broad_matcher() {
        for v in [
            "4155552671",
            "14155552671",
            "020 7946 0958",
            "+44 20 7946 0958",
        ] {
            assert!(detects(Detector::PhoneLoose, v), "{v}");
        }
        assert!(!detects(Detector::Phone, "4155552671"));
        assert!(!detects(Detector::PhoneLoose, "12345"));
        assert!(!detects(Detector::PhoneLoose, "41a5552671"));
        assert!(detects_value(
            Detector::PhoneLoose,
            &serde_json::json!(4155552671u64)
        ));
        assert!(!detects_value(
            Detector::Phone,
            &serde_json::json!(4155552671u64)
        ));
        assert_eq!(Detector::PhoneLoose.to_string(), "phone_loose");
        let d: Detector = serde_json::from_value(serde_json::json!("phone_loose")).unwrap();
        assert_eq!(d, Detector::PhoneLoose);
    }

    #[test]
    fn email_detector() {
        assert!(detects(Detector::Email, "alice@example.com"));
        assert!(detects(Detector::Email, "a.b+tag@sub.domain.co"));
        assert!(!detects(Detector::Email, "not an email"));
        assert!(!detects(Detector::Email, "alice@localhost")); // no TLD
        assert!(!detects(Detector::Email, "prefix alice@x.com suffix")); // anchored
    }

    #[test]
    fn ssn_detector() {
        assert!(detects(Detector::Ssn, "123-45-6789"));
        assert!(!detects(Detector::Ssn, "000-45-6789")); // invalid area
        assert!(!detects(Detector::Ssn, "666-45-6789"));
        assert!(!detects(Detector::Ssn, "900-45-6789"));
        assert!(!detects(Detector::Ssn, "123-00-6789")); // invalid group
        assert!(!detects(Detector::Ssn, "123-45-0000")); // invalid serial
        assert!(!detects(Detector::Ssn, "123456789")); // needs dashes
    }

    #[test]
    fn credit_card_detector_uses_luhn() {
        assert!(detects(Detector::CreditCard, "4111111111111111")); // Visa test
        assert!(detects(Detector::CreditCard, "4111 1111 1111 1111"));
        assert!(detects(Detector::CreditCard, "4111-1111-1111-1111"));
        assert!(detects(Detector::CreditCard, "378282246310005")); // Amex 15
        assert!(!detects(Detector::CreditCard, "4111111111111112")); // bad luhn
        assert!(!detects(Detector::CreditCard, "1234567890")); // too short
        assert!(!detects(Detector::CreditCard, "4111 1111 1111 111a")); // non-digit
        assert!(!detects(Detector::CreditCard, "-4111111111111111")); // leading sep
    }

    /// A Luhn-valid number of `len` digits starting with `prefix`.
    fn luhn_number(prefix: &str, len: usize) -> String {
        let mut body: Vec<u8> = prefix.bytes().map(|b| b - b'0').collect();
        while body.len() < len - 1 {
            body.push(((body.len() * 7) % 10) as u8);
        }
        let check = (0..10u8)
            .find(|c| {
                let mut all = body.clone();
                all.push(*c);
                luhn_valid(&all)
            })
            .unwrap();
        body.push(check);
        body.iter().map(|d| char::from(b'0' + d)).collect()
    }

    #[test]
    fn credit_card_matches_one_number_per_network() {
        let published = [
            "4111111111111111", // Visa
            "4222222222222",    // Visa 13
            "5555555555554444", // Mastercard
            "2223003122003222", // Mastercard 2-series
            "378282246310005",  // Amex
            "371449635398431",  // Amex
            "6011111111111117", // Discover
            "3530111333300000", // JCB
            "30569309025904",   // Diners
            "6200000000000005", // UnionPay
            "6759649826438453", // Maestro
            "135410014004955",  // UATP
        ];
        let generated = [
            ("4", 19),
            ("2200", 16),
            ("2204", 19),
            ("644", 16),
            ("65", 19),
            ("622126", 16),
            ("3589", 19),
            ("3095", 14),
            ("36", 14),
            ("38", 16),
            ("81", 16),
            ("50", 13),
            ("60", 16),
            ("82", 16),
            ("508", 16),
            ("9792", 16),
            ("506", 16),
            ("650", 19),
            ("1", 15),
        ];
        let numbers = published
            .iter()
            .map(|s| (*s).to_owned())
            .chain(generated.iter().map(|(p, l)| luhn_number(p, *l)));
        for n in numbers {
            assert!(detects(Detector::CreditCard, &n), "{n}");
            let spaced: String = n
                .chars()
                .enumerate()
                .flat_map(|(i, c)| (i > 0 && i % 4 == 0).then_some(' ').into_iter().chain([c]))
                .collect();
            assert!(detects(Detector::CreditCard, &spaced), "{spaced}");
            let int: u64 = n.parse().unwrap();
            assert!(
                detects_value(Detector::CreditCard, &serde_json::json!(int)),
                "{n}"
            );
        }
    }

    #[test]
    fn credit_card_skips_epoch_millis_and_ids_outside_card_ranges() {
        let epoch_hits = (0..10_000u64)
            .map(|i| 1_731_974_400_000 + i)
            .filter(|ms| {
                detects(Detector::CreditCard, &ms.to_string())
                    || detects_value(Detector::CreditCard, &serde_json::json!(ms))
            })
            .count();
        assert_eq!(epoch_hits, 0);
        let loose_hits = (0..10_000u64)
            .map(|i| 1_731_974_400_000 + i)
            .filter(|ms| detects(Detector::CreditCardLoose, &ms.to_string()))
            .count();
        assert!(loose_hits > 500, "{loose_hits}");
        for (prefix, len) in [
            ("1", 19),
            ("1", 16),
            ("7", 16),
            ("9", 16),
            ("2999", 16),
            ("1", 13),
        ] {
            let id = luhn_number(prefix, len);
            assert!(!detects(Detector::CreditCard, &id), "{id}");
            assert!(detects(Detector::CreditCardLoose, &id), "{id}");
        }
        // A Luhn-valid 19-digit id starting with 4 begins like a Visa card and
        // still matches: the documented residual.
        assert!(detects(Detector::CreditCard, &luhn_number("4", 19)));
        // Wrong length for the network.
        assert!(!detects(Detector::CreditCard, &luhn_number("34", 16)));
        assert!(!detects(Detector::CreditCard, &luhn_number("4", 14)));
    }

    #[test]
    fn credit_card_loose_keeps_the_luhn_only_matcher() {
        assert!(detects(Detector::CreditCardLoose, "4111111111111111"));
        assert!(detects(Detector::CreditCardLoose, "4111 1111 1111 1111"));
        assert!(!detects(Detector::CreditCardLoose, "4111111111111112"));
        assert!(!detects(Detector::CreditCardLoose, "1234567890"));
        assert!(!detects(Detector::CreditCardLoose, "-4111111111111111"));
        assert!(detects_value(
            Detector::CreditCardLoose,
            &serde_json::json!(luhn_number("1", 19).parse::<u64>().unwrap())
        ));
        assert_eq!(Detector::CreditCardLoose.to_string(), "credit_card_loose");
        let d: Detector = serde_json::from_value(serde_json::json!("credit_card_loose")).unwrap();
        assert_eq!(d, Detector::CreditCardLoose);
        assert_eq!(
            serde_json::to_value(Detector::CreditCardLoose).unwrap(),
            serde_json::json!("credit_card_loose")
        );
    }

    #[test]
    fn phone_detector() {
        assert!(detects(Detector::Phone, "+14155552671"));
        assert!(detects(Detector::Phone, "+44 20 7946 0958"));
        assert!(detects(Detector::Phone, "415-555-2671"));
        assert!(detects(Detector::Phone, "(415) 555-2671"));
        assert!(detects(Detector::Phone, "415.555.2671"));
        assert!(detects(Detector::Phone, "1-415-555-2671"));
        assert!(!detects(Detector::Phone, "12345")); // too short
        assert!(!detects(Detector::Phone, "not a phone"));
        assert!(!detects(Detector::Phone, "1717200000000")); // epoch ms
        assert!(!detects(Detector::Phone, "4155552671")); // bare digits
        assert!(!detects(Detector::Phone, "+0123456789"));
        assert!(!detects(Detector::Phone, "+1234"));
        assert!(!detects(Detector::Phone, "+1415x5552671"));
    }

    #[test]
    fn numbers_only_go_through_the_card_detector() {
        use serde_json::json;
        assert!(detects_value(
            Detector::CreditCard,
            &json!(4111111111111111u64)
        ));
        assert!(!detects_value(
            Detector::CreditCard,
            &json!(4111111111111112u64)
        ));
        assert!(!detects_value(Detector::Phone, &json!(4155552671u64)));
        assert!(!detects_value(Detector::Phone, &json!(1717200000000u64)));
        assert!(!detects_value(Detector::CreditCard, &json!(true)));
        assert!(detects_value(Detector::Phone, &json!("415-555-2671")));
        assert!(!detects_value(Detector::Email, &json!(null)));
    }

    #[test]
    fn ipv4_detector() {
        assert!(detects(Detector::Ipv4, "192.168.0.1"));
        assert!(detects(Detector::Ipv4, "0.0.0.0"));
        assert!(detects(Detector::Ipv4, "255.255.255.255"));
        assert!(!detects(Detector::Ipv4, "256.0.0.1")); // > 255
        assert!(!detects(Detector::Ipv4, "192.168.01.1")); // leading zero
        assert!(!detects(Detector::Ipv4, "192.168.0")); // 3 octets
        assert!(!detects(Detector::Ipv4, "1.2.3.4.5")); // 5 octets
    }
}
