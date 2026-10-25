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

#[cfg(test)]
mod tests {
    use super::*;

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
