//! Which conformance assertions a connector must run, given what it declares
//! (RFC 0013, #878).
//!
//! A declared capability is only worth what its test proves, so each row of
//! [`REQUIREMENTS`] names a capability, how a connector declares it, and the
//! assertions (any one of them) that prove it. `scripts/conformance-coverage.py`
//! reads this table (through the committed snapshot
//! `crates/conformance/declared-capabilities.json`, which
//! [`requirements_json`] renders into), reads each built-in connector's
//! declarations and its `tests/conformance.rs`, and fails CI when a declared
//! capability has no matching assertion call. A connector that cannot run an
//! assertion in CI is listed with a reason in
//! `crates/conformance/conformance-coverage-exempt.txt` (or, for the call
//! deadline, `call-deadline-exempt.txt`).
//!
//! A third-party connector can use the same table to pick the checks its own
//! `tests/conformance.rs` should call.

use faucet_core::Value;

/// Which side of the pipeline a requirement applies to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Role {
    /// A source connector.
    Source,
    /// A sink connector.
    Sink,
}

impl Role {
    /// `"source"` / `"sink"`.
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Source => "source",
            Role::Sink => "sink",
        }
    }
}

/// How a connector declares the capability a requirement covers. A
/// requirement applies when any of its triggers holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Trigger {
    /// Every connector of the role.
    Always,
    /// A boolean field of the connector's kind-level
    /// [`Capabilities`](faucet_core::Capabilities) (its descriptor called with
    /// no config) is on — the serialized field name, e.g. `idempotent_writes`.
    Capability(&'static str),
    /// The kind-level write modes include this mode (`upsert`, `overwrite`, …).
    WriteMode(&'static str),
    /// The connector's crate overrides this trait method.
    Overrides(&'static str),
    /// The connector's crate overrides `flush` with a body that does work (not
    /// just `Ok(())`), or delegates to the shared file writer
    /// (`faucet_common_file::delegate_sink!`): it buffers until flush.
    BufferedFlush,
    /// The connector's crate declares this `BatchAtomicity` variant
    /// (`Atomic`, `PerRow`) for some configuration.
    Declares(&'static str),
    /// The connector is built on a family driver (RFC 0014): its crate holds
    /// a `DriverSink` or implements one of the driver traits.
    Driver,
}

impl Trigger {
    fn to_json(self) -> Value {
        let (kind, arg) = match self {
            Trigger::Always => ("always", None),
            Trigger::Capability(f) => ("capability", Some(f)),
            Trigger::WriteMode(m) => ("write_mode", Some(m)),
            Trigger::Overrides(m) => ("overrides", Some(m)),
            Trigger::BufferedFlush => ("buffered_flush", None),
            Trigger::Declares(v) => ("declares", Some(v)),
            Trigger::Driver => ("driver", None),
        };
        match arg {
            Some(a) => serde_json::json!({ "kind": kind, "arg": a }),
            None => serde_json::json!({ "kind": kind }),
        }
    }
}

/// One row of the table: a capability, how it is declared, and the
/// assertions that prove it.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct Requirement {
    /// Stable id, used by the exemption file (`<crate> <id>: <reason>`).
    pub id: &'static str,
    /// The side it applies to.
    pub role: Role,
    /// The requirement applies when any trigger holds.
    pub triggers: &'static [Trigger],
    /// Calling any one of these in `tests/conformance.rs` satisfies it.
    pub assertions: &'static [&'static str],
    /// What the assertion proves.
    pub summary: &'static str,
}

impl Requirement {
    /// The row as JSON (what the committed snapshot holds).
    pub fn to_json(&self) -> Value {
        serde_json::json!({
            "id": self.id,
            "role": self.role.as_str(),
            "triggers": self.triggers.iter().map(|t| t.to_json()).collect::<Vec<_>>(),
            "assertions": self.assertions,
            "summary": self.summary,
        })
    }
}

/// The capability → required-assertion table.
pub const REQUIREMENTS: &[Requirement] = &[
    Requirement {
        id: "descriptor",
        role: Role::Sink,
        triggers: &[Trigger::Always],
        assertions: &["assert_descriptor_matches_sink"],
        summary: "the static descriptor answers what the built sink declares",
    },
    Requirement {
        id: "idempotent_writes",
        role: Role::Sink,
        triggers: &[Trigger::Capability("idempotent_writes")],
        assertions: &["assert_idempotent_replay", "assert_capabilities_truthful"],
        summary: "a committed page's token is durable, so a replay is skipped",
    },
    Requirement {
        id: "keyed_write_modes",
        role: Role::Sink,
        triggers: &[Trigger::WriteMode("upsert"), Trigger::WriteMode("delete")],
        assertions: &["assert_write_modes_truthful"],
        summary: "upsert converges by key, delete removes, a missing key fails the row",
    },
    Requirement {
        id: "replay_safe",
        role: Role::Sink,
        triggers: &[
            Trigger::Capability("keyed_upsert"),
            Trigger::Overrides("write_batch_is_replay_safe"),
        ],
        assertions: &["assert_replay_safe"],
        summary: "a plain write retried after a lost response does not duplicate rows",
    },
    Requirement {
        id: "overwrite",
        role: Role::Sink,
        triggers: &[Trigger::WriteMode("overwrite")],
        assertions: &["assert_overwrite_lifecycle"],
        summary: "staged rows stay invisible until commit; abort leaves the destination as it was",
    },
    Requirement {
        id: "schema_evolution",
        role: Role::Sink,
        triggers: &[Trigger::Capability("schema_evolution")],
        assertions: &["assert_schema_evolution_effective"],
        summary: "evolve_schema makes the added column appear in current_schema",
    },
    Requirement {
        id: "buffered_flush",
        role: Role::Sink,
        triggers: &[Trigger::BufferedFlush],
        assertions: &["assert_cancellation_flushes"],
        summary: "a cancelled run flushes, so buffered output is not lost",
    },
    Requirement {
        id: "partial_outcomes",
        role: Role::Sink,
        triggers: &[Trigger::Overrides("write_batch_partial")],
        assertions: &["assert_partial_outcomes_truthful"],
        summary: "one outcome per row, and an Ok row is in the destination",
    },
    Requirement {
        id: "row_accounting",
        role: Role::Sink,
        triggers: &[Trigger::Overrides("write_batch_reported")],
        assertions: &["assert_reported_rows_truthful"],
        summary: "the write report covers the page and accepted equals what landed",
    },
    Requirement {
        id: "atomic_batches",
        role: Role::Sink,
        triggers: &[Trigger::Declares("Atomic")],
        assertions: &["assert_batch_atomicity_truthful"],
        summary: "a failed write of a batch declared atomic lands no row",
    },
    Requirement {
        id: "call_deadline",
        role: Role::Sink,
        triggers: &[Trigger::Always],
        assertions: &["assert_sink_call_deadline"],
        summary: "a write to a hung backend ends at the call deadline and on cancel",
    },
    Requirement {
        id: "driver_declarations",
        role: Role::Sink,
        triggers: &[Trigger::Driver],
        assertions: &["assert_driver_declarations_truthful"],
        summary: "an undeclared lifecycle is refused by its hook and a declared one answers",
    },
    Requirement {
        id: "driver_row_paths",
        role: Role::Sink,
        triggers: &[Trigger::Driver],
        assertions: &["assert_driver_row_paths_agree"],
        summary: "the plain, reported, per-row and governed writes of a valid page agree",
    },
    Requirement {
        id: "descriptor",
        role: Role::Source,
        triggers: &[Trigger::Always],
        assertions: &["assert_descriptor_matches_source"],
        summary: "the static descriptor answers what the built source declares",
    },
    Requirement {
        id: "exactly_once",
        role: Role::Source,
        triggers: &[Trigger::Capability("exactly_once")],
        assertions: &["assert_bookmark_roundtrip"],
        summary: "a resumed run picks up after the emitted bookmark",
    },
    Requirement {
        id: "discover",
        role: Role::Source,
        triggers: &[Trigger::Capability("discover")],
        assertions: &["assert_discover_roundtrips"],
        summary: "every discovered dataset can be selected and read",
    },
    Requirement {
        id: "call_deadline",
        role: Role::Source,
        triggers: &[Trigger::Always],
        assertions: &["assert_source_call_deadline"],
        summary: "a read from a hung backend ends at the call deadline and on cancel",
    },
];

/// The requirements that apply to `role`.
pub fn requirements_for(role: Role) -> impl Iterator<Item = &'static Requirement> {
    REQUIREMENTS.iter().filter(move |r| r.role == role)
}

/// [`REQUIREMENTS`] as a JSON array.
pub fn requirements_json() -> Value {
    Value::Array(REQUIREMENTS.iter().map(Requirement::to_json).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIB: &str = include_str!("lib.rs");
    const CHECKS: &str = include_str!("checks.rs");
    const CALLS: &str = include_str!("calls.rs");

    #[test]
    fn every_named_assertion_exists() {
        for r in REQUIREMENTS {
            for a in r.assertions {
                let def = format!("pub async fn {a}");
                let def_sync = format!("pub fn {a}");
                assert!(
                    [LIB, CHECKS, CALLS]
                        .iter()
                        .any(|src| src.contains(&def) || src.contains(&def_sync)),
                    "requirement `{}` names `{a}`, which faucet-conformance does not define",
                    r.id
                );
            }
        }
    }

    #[test]
    fn ids_are_unique_per_role() {
        for role in [Role::Source, Role::Sink] {
            let mut ids: Vec<&str> = requirements_for(role).map(|r| r.id).collect();
            let n = ids.len();
            ids.sort_unstable();
            ids.dedup();
            assert_eq!(
                ids.len(),
                n,
                "duplicate requirement id for {}",
                role.as_str()
            );
        }
    }

    #[test]
    fn capability_triggers_name_real_capability_fields() {
        let fields = serde_json::to_value(faucet_core::Capabilities::default()).unwrap();
        for r in REQUIREMENTS {
            for t in r.triggers {
                if let Trigger::Capability(f) = t {
                    assert!(
                        fields.get(*f).is_some_and(Value::is_boolean),
                        "`{}` triggers on `{f}`, which is not a Capabilities flag",
                        r.id
                    );
                }
            }
        }
    }

    #[test]
    fn renders_every_trigger_kind() {
        let json = requirements_json();
        let text = json.to_string();
        for kind in [
            "always",
            "capability",
            "write_mode",
            "overrides",
            "buffered_flush",
            "declares",
            "driver",
        ] {
            assert!(text.contains(&format!("\"kind\":\"{kind}\"")), "{kind}");
        }
        assert_eq!(json.as_array().unwrap().len(), REQUIREMENTS.len());
        assert_eq!(Role::Source.as_str(), "source");
        assert_eq!(Role::Sink.as_str(), "sink");
    }
}
