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

/// `<table><suffix>` within a backend's identifier limit of `max` bytes. A
/// longer name is cut and given a hash of the full table name, so it stays
/// distinct from the table and from other derived names (a backend would
/// otherwise refuse it, or truncate it silently onto the target's name).
pub fn bounded_table_name(table: &str, suffix: &str, max: usize) -> String {
    if table.len() + suffix.len() <= max {
        return format!("{table}{suffix}");
    }
    let hash = table.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
    });
    let tag = format!("_{:08x}", hash as u32);
    let mut cut = max.saturating_sub(suffix.len() + tag.len());
    while cut > 0 && !table.is_char_boundary(cut) {
        cut -= 1;
    }
    format!("{}{tag}{suffix}", &table[..cut])
}

/// The staging table of overwrite run `token` within `max` bytes
/// (`<table>__faucet_ovw_<token>`, shortened by [`bounded_table_name`]); the
/// untokened name without one.
pub fn bounded_staging_table(table: &str, token: Option<&str>, max: usize) -> String {
    bounded_table_name(
        table,
        &faucet_core::idempotency::overwrite_staging_suffix(token),
        max,
    )
}

/// The table part every tokened staging name of `table` shares under a
/// `max`-byte limit (the `<base>` of `<base>__faucet_ovw_<token>`), for
/// recognising other runs' staging.
pub fn bounded_staging_base(table: &str, max: usize) -> String {
    let probe = "0".repeat(faucet_core::idempotency::OVERWRITE_RUN_TOKEN_LEN);
    let suffix = faucet_core::idempotency::overwrite_staging_suffix(Some(&probe));
    let name = bounded_table_name(table, &suffix, max);
    name.strip_suffix(suffix.as_str())
        .unwrap_or(&name)
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn staging_names_fit_the_limit_and_keep_a_shared_base() {
        use faucet_core::idempotency::{new_overwrite_run_token, overwrite_staging_token};
        let token = new_overwrite_run_token();
        // Short names are untouched.
        assert_eq!(
            bounded_staging_table("t", Some(&token), 60),
            format!("t__faucet_ovw_{token}")
        );
        assert_eq!(bounded_staging_base("t", 60), "t");
        let long = "customer_subscription_invoice_lines_v2"; // 38 chars
        let staging = bounded_staging_table(long, Some(&token), 60);
        assert!(staging.len() <= 60, "{staging}");
        let base = bounded_staging_base(long, 60);
        assert!(staging.starts_with(&base));
        assert_eq!(
            overwrite_staging_token(&base, &staging),
            Some(token.clone())
        );
        // Distinct tables stay distinct after the cut.
        let other = "customer_subscription_invoice_lines_v3";
        assert_ne!(
            bounded_staging_base(long, 60),
            bounded_staging_base(other, 60)
        );
        // A multi-byte table name is cut on a character boundary.
        let wide = "é".repeat(40);
        assert!(bounded_staging_table(&wide, Some(&token), 60).len() <= 60);
    }

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
