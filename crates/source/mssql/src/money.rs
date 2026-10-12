//! Exact `MONEY` / `SMALLMONEY` reads.
//!
//! `tiberius` decodes `MONEY` into an `f64`, which is exact only below about
//! ±900 billion. Before streaming, the source asks SQL Server for the query's
//! result shape (`sp_describe_first_result_set`); when a column is `MONEY` or
//! `SMALLMONEY` it wraps the query so that column is read as
//! `CAST(… AS DECIMAL(19,4))`, which the driver decodes exactly. A query that
//! cannot be wrapped (a trailing `ORDER BY`, a CTE, a temp table) keeps the
//! float path, with a warning.

use std::sync::atomic::{AtomicBool, Ordering};

use futures::io::{AsyncRead, AsyncWrite};
use serde_json::Value;
use tiberius::{Client, ToSql};

/// Alias of the derived table the cast query selects from.
const ALIAS: &str = "_faucet_money";

/// One visible column of a described result set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DescribedColumn {
    pub name: Option<String>,
    pub type_name: Option<String>,
}

impl DescribedColumn {
    fn is_money(&self) -> bool {
        self.type_name
            .as_deref()
            .map(|t| t.trim().to_ascii_lowercase())
            .is_some_and(|t| t == "money" || t == "smallmoney")
    }
}

/// What the probe decided for one query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MoneyPlan {
    /// No `MONEY` column: run the query unchanged.
    Unchanged,
    /// Run this query instead; `MONEY` columns arrive as `DECIMAL(19,4)`.
    Cast(String),
    /// `MONEY` columns exist but the query cannot be wrapped; the reason is logged.
    Fallback(String),
}

/// The `@params` declaration `sp_describe_first_result_set` needs for the
/// positional `@Pn` markers, typed as the source binds them.
pub(crate) fn param_declarations(values: &[Value]) -> String {
    values
        .iter()
        .enumerate()
        .map(|(i, v)| {
            let ty = match v {
                Value::Number(n) if n.is_i64() => "bigint",
                Value::Number(n) if n.is_u64() => "nvarchar(max)",
                Value::Number(_) => "float",
                Value::Bool(_) => "bit",
                Value::Null => "int",
                _ => "nvarchar(max)",
            };
            format!("@P{} {ty}", i + 1)
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn bracket(name: &str) -> String {
    format!("[{}]", name.replace(']', "]]"))
}

/// Build the cast query for `query` from its described columns. `Ok(None)`
/// when no column is `MONEY`; `Err(reason)` when one is but the columns cannot
/// be selected by name from a derived table.
pub(crate) fn cast_query(
    query: &str,
    columns: &[DescribedColumn],
) -> Result<Option<String>, String> {
    if !columns.iter().any(DescribedColumn::is_money) {
        return Ok(None);
    }
    let mut seen = std::collections::HashSet::new();
    let mut select = Vec::with_capacity(columns.len());
    for col in columns {
        let name = match col.name.as_deref() {
            Some(n) if !n.is_empty() => n,
            _ => return Err("a result column has no name".to_string()),
        };
        if !seen.insert(name.to_lowercase()) {
            return Err(format!("two result columns are named {name:?}"));
        }
        let q = bracket(name);
        if col.is_money() {
            select.push(format!("CAST({ALIAS}.{q} AS DECIMAL(19,4)) AS {q}"));
        } else {
            select.push(format!("{ALIAS}.{q}"));
        }
    }
    Ok(Some(format!(
        "SELECT {} FROM ({query}) AS {ALIAS}",
        select.join(", ")
    )))
}

/// Describe the first result set of `tsql` (visible columns only, in order).
pub(crate) async fn describe<S>(
    client: &mut Client<S>,
    tsql: &str,
    declarations: &str,
) -> Result<Vec<DescribedColumn>, tiberius::error::Error>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let tsql = tsql.to_string();
    let decl = declarations.to_string();
    let (sql, params): (&str, Vec<&dyn ToSql>) = if decl.is_empty() {
        ("EXEC sp_describe_first_result_set @tsql = @P1", vec![&tsql])
    } else {
        (
            "EXEC sp_describe_first_result_set @tsql = @P1, @params = @P2",
            vec![&tsql, &decl],
        )
    };
    let rows = client
        .query(sql, &params)
        .await?
        .into_first_result()
        .await?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        if row.try_get::<bool, _>("is_hidden")?.unwrap_or(false) {
            continue;
        }
        out.push(DescribedColumn {
            name: row.try_get::<&str, _>("name")?.map(str::to_string),
            type_name: row
                .try_get::<&str, _>("system_type_name")?
                .map(str::to_string),
        });
    }
    Ok(out)
}

/// Decide how to read `query`: describe it, build the cast query when a
/// `MONEY` column is present, and confirm SQL Server accepts the wrapped form.
pub(crate) async fn plan<S>(client: &mut Client<S>, query: &str, values: &[Value]) -> MoneyPlan
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let decl = param_declarations(values);
    let columns = match describe(client, query, &decl).await {
        Ok(c) => c,
        Err(e) => {
            tracing::debug!(error = %e, "MSSQL result-shape probe failed; reading the query as written");
            return MoneyPlan::Unchanged;
        }
    };
    let cast = match cast_query(query, &columns) {
        Ok(None) => return MoneyPlan::Unchanged,
        Ok(Some(q)) => q,
        Err(reason) => return MoneyPlan::Fallback(reason),
    };
    match describe(client, &cast, &decl).await {
        Ok(_) => MoneyPlan::Cast(cast),
        Err(e) => MoneyPlan::Fallback(format!("the query cannot be used as a derived table ({e})")),
    }
}

/// The query to run for a probe outcome; warns once (per `warned`) when
/// `MONEY` columns stay on the driver's float path.
pub(crate) fn resolve(query: String, plan: MoneyPlan, warned: &AtomicBool) -> String {
    match plan {
        MoneyPlan::Unchanged => query,
        MoneyPlan::Cast(cast) => cast,
        MoneyPlan::Fallback(reason) => {
            if !warned.swap(true, Ordering::Relaxed) {
                tracing::warn!(
                    reason = %reason,
                    "MSSQL query returns MONEY/SMALLMONEY columns that cannot be read exactly: \
                     values beyond about ±900 billion may lose cents. CAST them to DECIMAL(19,4) \
                     in the query to read them exactly"
                );
            }
            query
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn col(name: Option<&str>, ty: &str) -> DescribedColumn {
        DescribedColumn {
            name: name.map(str::to_string),
            type_name: Some(ty.to_string()),
        }
    }

    #[test]
    fn resolve_picks_the_query_and_warns_once() {
        let warned = AtomicBool::new(false);
        assert_eq!(resolve("q".into(), MoneyPlan::Unchanged, &warned), "q");
        assert_eq!(
            resolve("q".into(), MoneyPlan::Cast("c".into()), &warned),
            "c"
        );
        assert!(!warned.load(Ordering::Relaxed));
        assert_eq!(
            resolve("q".into(), MoneyPlan::Fallback("why".into()), &warned),
            "q"
        );
        assert!(warned.load(Ordering::Relaxed));
        assert_eq!(
            resolve("q".into(), MoneyPlan::Fallback("why".into()), &warned),
            "q"
        );
    }

    #[test]
    fn declarations_follow_the_bind_types() {
        let values = [
            json!(7),
            json!(u64::MAX),
            json!(1.5),
            json!(true),
            Value::Null,
            json!("x"),
            json!({"a": 1}),
        ];
        assert_eq!(
            param_declarations(&values),
            "@P1 bigint, @P2 nvarchar(max), @P3 float, @P4 bit, @P5 int, @P6 nvarchar(max), @P7 nvarchar(max)"
        );
        assert_eq!(param_declarations(&[]), "");
    }

    #[test]
    fn no_money_column_leaves_the_query_alone() {
        let cols = [col(Some("id"), "int"), col(Some("d"), "decimal(19,4)")];
        assert_eq!(cast_query("SELECT * FROM t", &cols), Ok(None));
        let untyped = [DescribedColumn {
            name: Some("x".into()),
            type_name: None,
        }];
        assert_eq!(cast_query("SELECT * FROM t", &untyped), Ok(None));
    }

    #[test]
    fn money_columns_are_cast_in_place() {
        let cols = [
            col(Some("id"), "int"),
            col(Some("price"), "money"),
            col(Some("fee]x"), " SMALLMONEY "),
        ];
        assert_eq!(
            cast_query("SELECT * FROM t WHERE id > @P1", &cols)
                .unwrap()
                .unwrap(),
            "SELECT _faucet_money.[id], CAST(_faucet_money.[price] AS DECIMAL(19,4)) AS [price], \
             CAST(_faucet_money.[fee]]x] AS DECIMAL(19,4)) AS [fee]]x] \
             FROM (SELECT * FROM t WHERE id > @P1) AS _faucet_money"
        );
    }

    #[test]
    fn unnamed_or_duplicate_columns_cannot_be_wrapped() {
        let unnamed = [col(Some("m"), "money"), col(None, "int")];
        assert_eq!(
            cast_query("q", &unnamed),
            Err("a result column has no name".to_string())
        );
        let empty = [col(Some("m"), "money"), col(Some(""), "int")];
        assert!(cast_query("q", &empty).is_err());
        let dup = [col(Some("m"), "money"), col(Some("M"), "int")];
        assert_eq!(
            cast_query("q", &dup),
            Err("two result columns are named \"M\"".to_string())
        );
    }
}
