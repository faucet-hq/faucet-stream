//! Declared column types read from `sys.columns`, and what they mean for
//! binding (#789 SQL-34, SQL-38, SQL-39). Pure — no I/O.

/// One table column as SQL Server declares it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ColumnInfo {
    pub name: String,
    /// Base system type (`TYPE_NAME(system_type_id)`), lower case.
    pub type_name: String,
    /// `sys.columns.max_length` in bytes; `-1` for `(max)`.
    pub max_length: i16,
    pub precision: u8,
    pub scale: u8,
    pub collation: Option<String>,
    pub is_nullable: bool,
    pub is_identity: bool,
    /// A computed column (never writable).
    pub is_computed: bool,
}

impl ColumnInfo {
    /// The column's declared type, e.g. `decimal(38,10)`, `nvarchar(50)`,
    /// `datetime2(3)`. `None` for a type this sink cannot re-state (CLR types,
    /// `timestamp`, anything unrecognised).
    pub(crate) fn declared_type(&self) -> Option<String> {
        let t = self.type_name.as_str();
        let len = |bytes_per_char: i16| {
            if self.max_length == -1 {
                "max".to_string()
            } else {
                (self.max_length / bytes_per_char).to_string()
            }
        };
        Some(match t {
            "nvarchar" | "nchar" => format!("{t}({})", len(2)),
            "varchar" | "char" | "varbinary" | "binary" => format!("{t}({})", len(1)),
            "decimal" | "numeric" => format!("{t}({},{})", self.precision, self.scale),
            "datetime2" | "time" | "datetimeoffset" => format!("{t}({})", self.scale),
            "bigint" | "int" | "smallint" | "tinyint" | "bit" | "float" | "real" | "money"
            | "smallmoney" | "date" | "datetime" | "smalldatetime" | "uniqueidentifier" | "xml"
            | "text" | "ntext" | "image" | "sql_variant" => t.to_string(),
            _ => return None,
        })
    }

    /// The declared type plus `COLLATE` for character columns — what an
    /// `ALTER COLUMN` must re-state so it changes nothing but nullability.
    pub(crate) fn declared_with_collation(&self) -> Option<String> {
        let ty = self.declared_type()?;
        Some(match &self.collation {
            Some(c) if is_character(&self.type_name) => format!("{ty} COLLATE {c}"),
            _ => ty,
        })
    }

    /// What a bound parameter is `CAST` to inside a `VALUES` constructor, so
    /// every row of a column has one type: SQL Server types a multi-row `VALUES`
    /// column by data-type precedence, and an untyped NULL or a number in a text
    /// column otherwise wins and fails the statement (SQL-38, SQL-39).
    ///
    /// Character columns cast to `nvarchar(max)`, never their declared length:
    /// `CAST` truncates silently where the insert's own conversion raises
    /// "string or binary data would be truncated". Binary and CLR-typed columns
    /// are not cast (`CAST(nvarchar AS varbinary)` reinterprets the bytes).
    pub(crate) fn bind_cast(&self) -> Option<String> {
        let t = self.type_name.as_str();
        if is_character(t) || matches!(t, "text" | "ntext") {
            return Some("nvarchar(max)".into());
        }
        if is_binary(t) || matches!(t, "sql_variant") {
            return None;
        }
        self.declared_type()
    }

    /// Whether a NULL for this column must be bound as `varbinary`.
    pub(crate) fn is_binary(&self) -> bool {
        is_binary(&self.type_name)
    }
}

fn is_character(t: &str) -> bool {
    matches!(t, "nvarchar" | "nchar" | "varchar" | "char")
}

fn is_binary(t: &str) -> bool {
    matches!(t, "varbinary" | "binary" | "image")
}

/// The `sys.columns` query [`ColumnInfo`] is decoded from. `@P1` is the table
/// literal passed to `OBJECT_ID`.
pub(crate) const COLUMN_INFO_SQL: &str = "SELECT c.name AS name, \
     LOWER(TYPE_NAME(c.system_type_id)) AS type_name, c.max_length AS max_length, \
     c.precision AS precision, c.scale AS scale, c.collation_name AS collation, \
     c.is_nullable AS is_nullable, c.is_identity AS is_identity, c.is_computed AS is_computed \
     FROM sys.columns c WHERE c.object_id = OBJECT_ID(@P1) ORDER BY c.column_id";

/// The `CAST` targets for `columns`, looked up by name in `infos`. A column
/// missing from `infos` binds bare.
pub(crate) fn casts_for(infos: &[ColumnInfo], columns: &[String]) -> Vec<Option<String>> {
    columns
        .iter()
        .map(|c| {
            infos
                .iter()
                .find(|i| &i.name == c)
                .and_then(ColumnInfo::bind_cast)
        })
        .collect()
}

/// Which of `columns` are binary, in order, for NULL binding.
pub(crate) fn binary_flags(infos: &[ColumnInfo], columns: &[String]) -> Vec<bool> {
    columns
        .iter()
        .map(|c| infos.iter().any(|i| &i.name == c && i.is_binary()))
        .collect()
}

/// `@P{n}`, wrapped in `CAST(… AS <ty>)` when the column has a cast target.
pub(crate) fn placeholder(n: usize, cast: Option<&str>) -> String {
    match cast {
        Some(ty) => format!("CAST(@P{n} AS {ty})"),
        None => format!("@P{n}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(t: &str, max_length: i16, precision: u8, scale: u8) -> ColumnInfo {
        ColumnInfo {
            name: "c".into(),
            type_name: t.into(),
            max_length,
            precision,
            scale,
            collation: None,
            is_nullable: true,
            is_identity: false,
            is_computed: false,
        }
    }

    #[test]
    fn declared_type_restates_length_precision_and_scale() {
        assert_eq!(
            col("nvarchar", 100, 0, 0).declared_type().unwrap(),
            "nvarchar(50)"
        );
        assert_eq!(
            col("nvarchar", -1, 0, 0).declared_type().unwrap(),
            "nvarchar(max)"
        );
        assert_eq!(col("nchar", 20, 0, 0).declared_type().unwrap(), "nchar(10)");
        assert_eq!(
            col("varchar", 30, 0, 0).declared_type().unwrap(),
            "varchar(30)"
        );
        assert_eq!(
            col("varbinary", -1, 0, 0).declared_type().unwrap(),
            "varbinary(max)"
        );
        assert_eq!(
            col("binary", 16, 0, 0).declared_type().unwrap(),
            "binary(16)"
        );
        assert_eq!(
            col("decimal", 17, 38, 10).declared_type().unwrap(),
            "decimal(38,10)"
        );
        assert_eq!(
            col("numeric", 9, 10, 2).declared_type().unwrap(),
            "numeric(10,2)"
        );
        assert_eq!(
            col("datetime2", 8, 27, 7).declared_type().unwrap(),
            "datetime2(7)"
        );
        assert_eq!(col("time", 5, 16, 3).declared_type().unwrap(), "time(3)");
        assert_eq!(
            col("datetimeoffset", 10, 34, 7).declared_type().unwrap(),
            "datetimeoffset(7)"
        );
        for t in [
            "bigint",
            "int",
            "smallint",
            "tinyint",
            "bit",
            "float",
            "real",
            "money",
            "smallmoney",
            "date",
            "datetime",
            "smalldatetime",
            "uniqueidentifier",
            "xml",
            "text",
            "ntext",
            "image",
            "sql_variant",
        ] {
            assert_eq!(col(t, 8, 0, 0).declared_type().unwrap(), t);
        }
        assert_eq!(col("hierarchyid", 892, 0, 0).declared_type(), None);
        assert_eq!(col("timestamp", 8, 0, 0).declared_type(), None);
    }

    #[test]
    fn collation_is_restated_only_for_character_columns() {
        let mut c = col("varchar", 40, 0, 0);
        c.collation = Some("Latin1_General_CS_AS".into());
        assert_eq!(
            c.declared_with_collation().unwrap(),
            "varchar(40) COLLATE Latin1_General_CS_AS"
        );
        let mut d = col("decimal", 9, 10, 2);
        d.collation = Some("ignored".into());
        assert_eq!(d.declared_with_collation().unwrap(), "decimal(10,2)");
        assert_eq!(col("geography", -1, 0, 0).declared_with_collation(), None);
    }

    #[test]
    fn bind_cast_never_truncates_text_and_never_casts_binary() {
        assert_eq!(
            col("varchar", 10, 0, 0).bind_cast().unwrap(),
            "nvarchar(max)"
        );
        assert_eq!(col("nchar", 4, 0, 0).bind_cast().unwrap(), "nvarchar(max)");
        assert_eq!(col("ntext", 16, 0, 0).bind_cast().unwrap(), "nvarchar(max)");
        assert_eq!(
            col("datetime2", 8, 27, 3).bind_cast().unwrap(),
            "datetime2(3)"
        );
        assert_eq!(
            col("uniqueidentifier", 16, 0, 0).bind_cast().unwrap(),
            "uniqueidentifier"
        );
        assert_eq!(col("int", 4, 10, 0).bind_cast().unwrap(), "int");
        assert_eq!(col("varbinary", -1, 0, 0).bind_cast(), None);
        assert_eq!(col("image", 16, 0, 0).bind_cast(), None);
        assert_eq!(col("sql_variant", 8016, 0, 0).bind_cast(), None);
        assert_eq!(col("geometry", -1, 0, 0).bind_cast(), None);
        assert!(col("binary", 8, 0, 0).is_binary());
        assert!(!col("nvarchar", 8, 0, 0).is_binary());
    }

    #[test]
    fn casts_and_binary_flags_follow_the_column_order() {
        let mut a = col("int", 4, 10, 0);
        a.name = "a".into();
        let mut b = col("varbinary", 10, 0, 0);
        b.name = "b".into();
        let infos = vec![a, b];
        let cols = vec!["b".to_string(), "a".to_string(), "missing".to_string()];
        assert_eq!(
            casts_for(&infos, &cols),
            vec![None, Some("int".into()), None]
        );
        assert_eq!(binary_flags(&infos, &cols), vec![true, false, false]);
    }

    #[test]
    fn placeholder_wraps_only_when_a_cast_is_known() {
        assert_eq!(placeholder(3, Some("date")), "CAST(@P3 AS date)");
        assert_eq!(placeholder(3, None), "@P3");
    }
}
