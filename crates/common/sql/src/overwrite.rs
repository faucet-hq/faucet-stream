//! Shared overwrite (full-refresh) refusals of the SQL sinks.

use faucet_core::FaucetError;

/// The refusal for an overwrite of a table other tables reference by foreign
/// key. Every SQL sink's swap deletes and re-inserts the target's rows (or
/// renames it away), which would cascade into those tables (`ON DELETE
/// CASCADE` / `SET NULL`), leave their keys dangling, or fail only after the
/// whole staging load (`NO ACTION`). `sink` names the sink (`"mssql"`), and
/// `can_disable` adds the "disable" remedy for a backend that can disable a
/// single constraint.
pub fn referenced_overwrite_error(
    sink: &str,
    table: &str,
    referenced_by: &[String],
    can_disable: bool,
) -> FaucetError {
    let remedy = if can_disable {
        "drop or disable the foreign key"
    } else {
        "drop the foreign key"
    };
    FaucetError::Config(format!(
        "{sink} overwrite: table '{table}' is referenced by a foreign key from {}; replacing \
         its rows would cascade into, break or be blocked by those tables. Use write_mode: \
         upsert, or {remedy}",
        referenced_by.join(", ")
    ))
}

/// The refusal for an overwrite write whose staging relation is missing
/// although the target exists. Begin clones an existing target into staging,
/// so staging is gone only when another overwrite run of the table dropped it
/// (it judged this run abandoned) or the overwrite was not begun. Re-creating
/// staging from the page instead would let the commit swap in only the pages
/// written since, silently dropping the rest.
pub fn missing_overwrite_staging_error(sink: &str, staging: &str) -> FaucetError {
    FaucetError::Sink(format!(
        "{sink} sink: overwrite staging '{staging}' is missing although the target exists; \
         another overwrite run of this table may have dropped it, or the overwrite was not \
         begun. The destination is unchanged"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_staging_names_the_sink_and_the_staging() {
        let e = missing_overwrite_staging_error("postgres", "t__faucet_ovw_x");
        assert!(matches!(e, FaucetError::Sink(_)));
        let msg = e.to_string();
        assert!(msg.contains("postgres sink: overwrite staging 't__faucet_ovw_x' is missing"));
        assert!(msg.contains("The destination is unchanged"));
    }

    #[test]
    fn names_the_sink_table_referencing_tables_and_remedy() {
        let e = referenced_overwrite_error("mssql", "dbo.customers", &["dbo.orders".into()], true);
        let FaucetError::Config(msg) = e else {
            panic!("expected a config error")
        };
        assert_eq!(
            msg,
            "mssql overwrite: table 'dbo.customers' is referenced by a foreign key from \
             dbo.orders; replacing its rows would cascade into, break or be blocked by those \
             tables. Use write_mode: upsert, or drop or disable the foreign key"
        );
        let e = referenced_overwrite_error("mysql", "t", &["a".into(), "b".into()], false);
        assert!(e.to_string().ends_with("from a, b; replacing its rows would cascade into, break or be blocked by those tables. Use write_mode: upsert, or drop the foreign key"), "{e}");
    }
}
