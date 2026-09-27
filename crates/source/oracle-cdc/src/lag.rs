//! Source lag (#733): how far the capture position trails the database's
//! current SCN, in seconds of commit time. SCN gaps are not event counts, so
//! only `seconds` is reported.

use faucet_core::SourceLag;

/// ORA-08181: the SCN is older than the SCN-to-time mapping retains.
pub(crate) const UNMAPPED_SCN: i32 = 8181;

/// What to ask the database for, given the capture position and the
/// current SCN. Pure.
#[derive(Debug, PartialEq)]
pub(crate) enum LagPlan {
    /// The position is at (or past) the head.
    CaughtUp,
    /// Measure the time between `position` and `current`.
    Measure { current: u64, position: u64 },
}

pub(crate) fn plan(position: u64, current: u64) -> LagPlan {
    if position >= current {
        LagPlan::CaughtUp
    } else {
        LagPlan::Measure { current, position }
    }
}

/// Turn the age query's outcome into lag: an unmapped SCN reports nothing,
/// any other database error is `Err(code)`. Pure.
pub(crate) fn from_age(age: Result<f64, Option<i32>>) -> Result<Option<SourceLag>, Option<i32>> {
    match age {
        Ok(s) => Ok(Some(SourceLag::seconds(s))),
        Err(Some(UNMAPPED_SCN)) => Ok(None),
        Err(code) => Err(code),
    }
}

/// The commit SCN a bookmark value carries, if any. Pure.
pub(crate) fn commit_scn(bookmark: &serde_json::Value) -> Option<u64> {
    bookmark
        .get("commit_scn")
        .and_then(serde_json::Value::as_u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn plans_and_interprets() {
        assert_eq!(plan(10, 10), LagPlan::CaughtUp);
        assert_eq!(plan(11, 10), LagPlan::CaughtUp);
        assert_eq!(
            plan(5, 10),
            LagPlan::Measure {
                current: 10,
                position: 5
            }
        );
        assert_eq!(from_age(Ok(12.5)), Ok(Some(SourceLag::seconds(12.5))));
        assert_eq!(from_age(Ok(-1.0)), Ok(Some(SourceLag::seconds(0.0))));
        assert_eq!(from_age(Err(Some(UNMAPPED_SCN))), Ok(None));
        assert_eq!(from_age(Err(Some(942))), Err(Some(942)));
        assert_eq!(from_age(Err(None)), Err(None));
        assert_eq!(commit_scn(&json!({"commit_scn": 7})), Some(7));
        assert_eq!(commit_scn(&json!({})), None);
    }
}
