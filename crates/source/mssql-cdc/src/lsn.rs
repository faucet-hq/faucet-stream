//! SQL Server Log Sequence Number (LSN) — pure parse / format / compare logic.
//!
//! An LSN in SQL Server change data capture is a `binary(10)` value: a 10-byte,
//! big-endian, fixed-width integer whose lexicographic byte order is exactly its
//! numeric order. We keep it as `[u8; 10]` and serialise it to the state store as
//! a 20-character lowercase hex string (matching `CONVERT(VARCHAR(20), lsn, 2)`).
//!
//! All logic here is pure and unit-tested without a live server — the LSN is
//! load-bearing for resumability, so a subtle off-by-one in the increment or a
//! wrong comparison would silently drop or replay change rows.

use std::fmt;

use faucet_core::FaucetError;

/// Number of bytes in a SQL Server `binary(10)` LSN.
pub const LSN_BYTES: usize = 10;

/// A SQL Server change data capture Log Sequence Number.
///
/// Stored as a big-endian byte array; [`Ord`] is the derived lexicographic
/// comparison, which for big-endian bytes equals numeric order.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lsn([u8; LSN_BYTES]);

impl Lsn {
    /// The all-zero LSN (`0x0000...0000`), the minimum possible value.
    pub const ZERO: Lsn = Lsn([0u8; LSN_BYTES]);

    /// Wrap a raw 10-byte big-endian array.
    pub fn from_array(bytes: [u8; LSN_BYTES]) -> Self {
        Lsn(bytes)
    }

    /// Build an LSN from a byte slice. The slice must be exactly `LSN_BYTES`
    /// long; any other length is a typed error (SQL Server always returns
    /// `binary(10)`, so a different width means a decode bug, not a data value).
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, FaucetError> {
        if bytes.len() != LSN_BYTES {
            return Err(FaucetError::Source(format!(
                "mssql-cdc: LSN must be exactly {LSN_BYTES} bytes, got {}",
                bytes.len()
            )));
        }
        let mut arr = [0u8; LSN_BYTES];
        arr.copy_from_slice(bytes);
        Ok(Lsn(arr))
    }

    /// Parse an LSN from its hex string form (20 hex digits, case-insensitive, an
    /// optional `0x`/`0X` prefix tolerated). Rejects any other length or a
    /// non-hex digit with a typed error.
    pub fn from_hex(s: &str) -> Result<Self, FaucetError> {
        let hex = s
            .strip_prefix("0x")
            .or_else(|| s.strip_prefix("0X"))
            .unwrap_or(s);
        if hex.len() != LSN_BYTES * 2 {
            return Err(FaucetError::Source(format!(
                "mssql-cdc: LSN hex must be {} digits, got {} ({s:?})",
                LSN_BYTES * 2,
                hex.len()
            )));
        }
        let mut arr = [0u8; LSN_BYTES];
        for (i, byte) in arr.iter_mut().enumerate() {
            let pair = &hex[i * 2..i * 2 + 2];
            *byte = u8::from_str_radix(pair, 16).map_err(|e| {
                FaucetError::Source(format!("mssql-cdc: invalid LSN hex {s:?}: {e}"))
            })?;
        }
        Ok(Lsn(arr))
    }

    /// Render as a 20-character lowercase hex string (no `0x` prefix), matching
    /// `CONVERT(VARCHAR(20), lsn, 2)`.
    pub fn to_hex(&self) -> String {
        let mut out = String::with_capacity(LSN_BYTES * 2);
        for b in &self.0 {
            out.push_str(&format!("{b:02x}"));
        }
        out
    }

    /// The smallest LSN strictly greater than `self` — the client-side analogue
    /// of `sys.fn_cdc_increment_lsn`. Used to derive the **half-open** lower
    /// bound for the next poll from a durable bookmark, so a resumed run never
    /// re-reads the last already-committed change (the `fn_cdc_get_all_changes`
    /// range is inclusive of `from_lsn`).
    ///
    /// Returns `None` only when `self` is the all-`0xFF` maximum (unreachable in
    /// practice — the log would have to wrap the entire 80-bit LSN space).
    pub fn increment(&self) -> Option<Self> {
        let mut arr = self.0;
        for byte in arr.iter_mut().rev() {
            if *byte == u8::MAX {
                *byte = 0;
            } else {
                *byte += 1;
                return Some(Lsn(arr));
            }
        }
        None
    }

    /// The LSN immediately before this one, or `None` for the zero LSN.
    pub fn decrement(&self) -> Option<Self> {
        let mut arr = self.0;
        for byte in arr.iter_mut().rev() {
            if *byte == 0 {
                *byte = u8::MAX;
            } else {
                *byte -= 1;
                return Some(Lsn(arr));
            }
        }
        None
    }
}

impl fmt::Debug for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Lsn({})", self.to_hex())
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn decrement_borrows_and_stops_at_zero() {
        let l = Lsn::from_hex("00000000000000000100").unwrap();
        assert_eq!(l.decrement().unwrap().to_hex(), "000000000000000000ff");
        assert_eq!(l.decrement().unwrap().increment().unwrap(), l);
        assert_eq!(
            Lsn::from_hex("00000000000000000000").unwrap().decrement(),
            None
        );
    }

    use super::*;

    #[test]
    fn hex_round_trips() {
        let hex = "0000002a000000550003";
        let lsn = Lsn::from_hex(hex).unwrap();
        assert_eq!(lsn.to_hex(), hex);
    }

    #[test]
    fn hex_tolerates_0x_prefix_and_uppercase() {
        let a = Lsn::from_hex("0x0000002A000000550003").unwrap();
        let b = Lsn::from_hex("0000002a000000550003").unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn hex_rejects_wrong_length_and_non_hex() {
        assert!(Lsn::from_hex("00").is_err());
        assert!(Lsn::from_hex("").is_err());
        // 20 chars but a non-hex digit.
        assert!(Lsn::from_hex("0000002a000000550zzz").is_err());
    }

    #[test]
    fn from_bytes_requires_exact_width() {
        assert!(Lsn::from_bytes(&[0u8; 10]).is_ok());
        assert!(Lsn::from_bytes(&[0u8; 9]).is_err());
        assert!(Lsn::from_bytes(&[0u8; 11]).is_err());
    }

    #[test]
    fn ordering_is_numeric_big_endian() {
        let small = Lsn::from_hex("00000000000000000001").unwrap();
        let big = Lsn::from_hex("00000000000000000002").unwrap();
        let bigger_high_byte = Lsn::from_hex("01000000000000000000").unwrap();
        assert!(small < big);
        assert!(big < bigger_high_byte);
        assert_eq!(Lsn::ZERO, Lsn::from_hex("00000000000000000000").unwrap());
        assert!(Lsn::ZERO < small);
    }

    #[test]
    fn increment_is_the_next_value() {
        let lsn = Lsn::from_hex("000000000000000000ff").unwrap();
        let next = lsn.increment().unwrap();
        // 0x...00ff + 1 carries into the next byte -> 0x...0100.
        assert_eq!(next.to_hex(), "00000000000000000100");
        assert!(lsn < next);
        // No LSN sits strictly between lsn and its increment.
        assert!(Lsn::from_hex("00000000000000000100").unwrap() <= next);
    }

    #[test]
    fn increment_simple_low_byte() {
        let lsn = Lsn::from_hex("00000000000000000001").unwrap();
        assert_eq!(lsn.increment().unwrap().to_hex(), "00000000000000000002");
    }

    #[test]
    fn increment_max_saturates_to_none() {
        let max = Lsn::from_array([0xFF; LSN_BYTES]);
        assert!(max.increment().is_none());
    }

    #[test]
    fn increment_of_zero_is_one() {
        assert_eq!(
            Lsn::ZERO.increment().unwrap().to_hex(),
            "00000000000000000001"
        );
    }
}
