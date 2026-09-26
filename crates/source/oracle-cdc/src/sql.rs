//! Pure SQL builders and preflight verdicts for the LogMiner source.

use faucet_common_oracle::{quote_ident_oracle, quote_validated};

/// `(OWNER, TABLE_NAME) IN ((:a,:b), …)` with binds numbered from `first`.
pub(crate) fn table_filter(n_tables: usize, first: usize) -> String {
    let pairs: Vec<String> = (0..n_tables)
        .map(|i| format!("(:{}, :{})", first + 2 * i, first + 2 * i + 1))
        .collect();
    pairs.join(", ")
}

/// The `V$LOGMNR_CONTENTS` query for a window `(:1, :2]` over the captured
/// tables (bound from `:3`). Transaction control rows are kept for every
/// transaction, since commits decide what is emitted.
pub(crate) fn contents_sql(n_tables: usize) -> String {
    format!(
        "SELECT SCN, OPERATION_CODE, RAWTOHEX(XID), SEG_OWNER, TABLE_NAME, SQL_REDO, CSF, \
         ROW_ID, ROLLBACK, TO_CHAR(TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS'), INFO \
         FROM V$LOGMNR_CONTENTS \
         WHERE SCN > :1 AND SCN <= :2 AND (OPERATION_CODE IN (6, 7, 36) \
         OR ((SEG_OWNER, TABLE_NAME) IN ({}) AND OPERATION_CODE IN (1, 2, 3, 5, 10, 11, 255)))",
        table_filter(n_tables, 3)
    )
}

/// Column metadata for the captured tables (binds from `:1`).
pub(crate) fn columns_sql(n_tables: usize) -> String {
    format!(
        "SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_SCALE FROM ALL_TAB_COLS \
         WHERE HIDDEN_COLUMN = 'NO' AND (OWNER, TABLE_NAME) IN ({})",
        table_filter(n_tables, 1)
    )
}

/// Per-table key / all-column supplemental log groups (binds from `:1`).
pub(crate) fn log_groups_sql(n_tables: usize) -> String {
    format!(
        "SELECT OWNER, TABLE_NAME, LOG_GROUP_TYPE FROM ALL_LOG_GROUPS \
         WHERE LOG_GROUP_TYPE IN ('ALL COLUMN LOGGING', 'PRIMARY KEY LOGGING') \
         AND (OWNER, TABLE_NAME) IN ({})",
        table_filter(n_tables, 1)
    )
}

/// Archived logs overlapping `(:1, :2]`.
pub(crate) const ARCHIVED_LOGS_SQL: &str = "SELECT NAME, THREAD#, SEQUENCE#, FIRST_CHANGE#, \
    NEXT_CHANGE# FROM V$ARCHIVED_LOG WHERE NAME IS NOT NULL AND STANDBY_DEST = 'NO' \
    AND DELETED = 'NO' AND STATUS = 'A' AND NEXT_CHANGE# > :1 AND FIRST_CHANGE# <= :2";

/// Online logs overlapping `(:1, :2]` (one member per group is enough).
pub(crate) const ONLINE_LOGS_SQL: &str = "SELECT MIN(f.MEMBER), l.THREAD#, l.SEQUENCE#, \
    l.FIRST_CHANGE#, l.NEXT_CHANGE# FROM V$LOG l JOIN V$LOGFILE f ON f.GROUP# = l.GROUP# \
    WHERE l.NEXT_CHANGE# > :1 AND l.FIRST_CHANGE# <= :2 \
    GROUP BY l.THREAD#, l.SEQUENCE#, l.FIRST_CHANGE#, l.NEXT_CHANGE#";

/// Current SCN plus the oldest open transaction's start.
pub(crate) const POSITION_SQL: &str =
    "SELECT d.CURRENT_SCN, (SELECT MIN(t.START_SCN) FROM V$TRANSACTION t) FROM V$DATABASE d";

/// CDB / container detection.
pub(crate) const CONTAINER_SQL: &str =
    "SELECT d.CDB, SYS_CONTEXT('USERENV', 'CON_NAME') FROM V$DATABASE d";

/// Database-level supplemental logging.
pub(crate) const SUPPLEMENTAL_SQL: &str = "SELECT SUPPLEMENTAL_LOG_DATA_MIN, \
    SUPPLEMENTAL_LOG_DATA_PK, SUPPLEMENTAL_LOG_DATA_ALL FROM V$DATABASE";

/// Register one log file with the session (`:1` path, `:2` NEW=1 / ADDFILE=3).
pub(crate) const ADD_LOGFILE_SQL: &str =
    "BEGIN SYS.DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => :1, OPTIONS => :2); END;";

/// Start a LogMiner session over `[:1, :2]`.
pub(crate) const START_LOGMNR_SQL: &str = "BEGIN SYS.DBMS_LOGMNR.START_LOGMNR(STARTSCN => :1, \
    ENDSCN => :2, OPTIONS => SYS.DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + \
    SYS.DBMS_LOGMNR.NO_ROWID_IN_STMT + SYS.DBMS_LOGMNR.NO_SQL_DELIMITER); END;";

/// End the LogMiner session.
pub(crate) const END_LOGMNR_SQL: &str = "BEGIN SYS.DBMS_LOGMNR.END_LOGMNR; END;";

/// `DBMS_LOGMNR.ADD_LOGFILE` option for the first file of a session.
pub(crate) const LOGFILE_NEW: i64 = 1;
/// `DBMS_LOGMNR.ADD_LOGFILE` option for subsequent files.
pub(crate) const LOGFILE_ADD: i64 = 3;

/// Flush-table DDL (wrapped to ignore ORA-00955).
pub(crate) fn flush_table_ddl(table: &str) -> String {
    let q = quote_validated(table);
    format!(
        "BEGIN EXECUTE IMMEDIATE 'CREATE TABLE {} (SCN NUMBER(19))'; EXCEPTION WHEN OTHERS THEN \
         IF SQLCODE != -955 THEN RAISE; END IF; END;",
        q.replace('\'', "''")
    )
}

/// Record the window end in the flush table (commit follows).
pub(crate) fn flush_sql(table: &str) -> String {
    let q = quote_validated(table);
    format!(
        "MERGE INTO {q} f USING (SELECT 1 AS K FROM DUAL) s ON (1 = 1) \
         WHEN MATCHED THEN UPDATE SET f.SCN = :1 WHEN NOT MATCHED THEN INSERT (SCN) VALUES (:2)"
    )
}

/// Whether the session must register log files itself: a PDB mines per PDB
/// (21c+), locating logs automatically; a non-CDB or the root does not.
pub(crate) fn needs_logfiles(cdb: &str, container: &str) -> bool {
    !cdb.eq_ignore_ascii_case("YES") || container.eq_ignore_ascii_case("CDB$ROOT")
}

/// Database-level supplemental logging flags (`V$DATABASE`).
#[derive(Debug, Clone)]
pub(crate) struct DatabaseLogging {
    pub min: String,
    pub pk: String,
    pub all: String,
}

/// What supplemental logging allows: `fatal` problems block capture (no
/// minimal logging, or a table whose updates would carry no key); `partial`
/// lists the fixes for tables whose updates carry only key + changed columns.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct LoggingReport {
    pub fatal: Vec<String>,
    pub partial: Vec<String>,
}

fn alter_all_columns(owner: &str, table: &str) -> String {
    format!(
        "ALTER TABLE {}.{} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;",
        quote_ident_oracle(owner).unwrap_or_else(|_| owner.to_string()),
        quote_ident_oracle(table).unwrap_or_else(|_| table.to_string())
    )
}

/// Judge supplemental logging for the captured tables. `groups` are
/// `(owner, table, LOG_GROUP_TYPE)` rows.
pub(crate) fn logging_report(
    db: &DatabaseLogging,
    tables: &[(String, String)],
    groups: &[(String, String, String)],
) -> LoggingReport {
    let yes = |v: &str| !v.eq_ignore_ascii_case("NO");
    let mut report = LoggingReport::default();
    if !yes(&db.min) {
        report.fatal.push(
            "minimal supplemental logging is disabled, so LogMiner cannot reconstruct changes: \
             run `ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;` as SYSDBA (in the CDB root)"
                .to_string(),
        );
    }
    let has = |o: &str, t: &str, ty: &str| {
        groups
            .iter()
            .any(|(go, gt, gty)| go == o && gt == t && gty.eq_ignore_ascii_case(ty))
    };
    for (o, t) in tables {
        let all = yes(&db.all) || has(o, t, "ALL COLUMN LOGGING");
        let key = all || yes(&db.pk) || has(o, t, "PRIMARY KEY LOGGING");
        if !key {
            report.fatal.push(format!(
                "{o}.{t} logs no key columns, so its updates and deletes cannot be attributed \
                 to a row: run `{}`",
                alter_all_columns(o, t)
            ));
        } else if !all {
            report.partial.push(alter_all_columns(o, t));
        }
    }
    report
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filters_and_queries() {
        assert_eq!(table_filter(2, 3), "(:3, :4), (:5, :6)");
        let q = contents_sql(1);
        assert!(q.contains("SCN > :1 AND SCN <= :2"), "{q}");
        assert!(q.contains("IN ((:3, :4))"), "{q}");
        assert!(columns_sql(1).ends_with("IN ((:1, :2))"));
        assert!(log_groups_sql(2).contains("(:1, :2), (:3, :4)"));
        assert!(flush_table_ddl("F").contains("CREATE TABLE \"F\" (SCN NUMBER(19))"));
        assert!(flush_sql("F").starts_with("MERGE INTO \"F\" f"));
        for s in [
            ARCHIVED_LOGS_SQL,
            ONLINE_LOGS_SQL,
            POSITION_SQL,
            CONTAINER_SQL,
            SUPPLEMENTAL_SQL,
        ] {
            assert!(s.starts_with("SELECT"));
        }
        for s in [ADD_LOGFILE_SQL, START_LOGMNR_SQL, END_LOGMNR_SQL] {
            assert!(s.starts_with("BEGIN"));
        }
        assert_ne!(LOGFILE_NEW, LOGFILE_ADD);
    }

    #[test]
    fn container_modes() {
        assert!(!needs_logfiles("YES", "FREEPDB1"));
        assert!(needs_logfiles("YES", "CDB$ROOT"));
        assert!(needs_logfiles("NO", "ORCL"));
    }

    #[test]
    fn supplemental_verdicts() {
        let db = |min: &str, pk: &str, all: &str| DatabaseLogging {
            min: min.into(),
            pk: pk.into(),
            all: all.into(),
        };
        let tables = vec![
            ("A".to_string(), "T".to_string()),
            ("A".to_string(), "U".to_string()),
        ];
        let r = logging_report(&db("NO", "NO", "NO"), &tables, &[]);
        assert_eq!(r.fatal.len(), 3, "{r:?}");
        assert!(r.fatal[0].contains("ADD SUPPLEMENTAL LOG DATA;"));
        assert!(
            r.fatal[1].contains("ALTER TABLE \"A\".\"T\" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;")
        );
        assert_eq!(
            logging_report(&db("YES", "NO", "YES"), &tables, &[]),
            LoggingReport::default()
        );
        let r = logging_report(&db("IMPLICIT", "YES", "NO"), &tables, &[]);
        assert!(r.fatal.is_empty());
        assert_eq!(r.partial.len(), 2);
        let groups = vec![
            (
                "A".to_string(),
                "T".to_string(),
                "ALL COLUMN LOGGING".to_string(),
            ),
            (
                "A".to_string(),
                "U".to_string(),
                "PRIMARY KEY LOGGING".to_string(),
            ),
        ];
        let r = logging_report(&db("YES", "NO", "NO"), &tables, &groups);
        assert!(r.fatal.is_empty());
        assert_eq!(
            r.partial,
            vec!["ALTER TABLE \"A\".\"U\" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;"]
        );
        assert!(alter_all_columns("A\"", "T").contains("A\".\"T\""));
    }
}
