//! The Oracle [`Source`] — the only module that talks to the driver. Rows are
//! fetched on a blocking thread and handed to the async stream one page at a
//! time over a bounded channel, so memory stays at O(batch_size).

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use faucet_common_oracle::oracle::sql_type::ToSql;
use faucet_common_oracle::{
    OraclePool, Side, blocking, call_timeout, checkout, connect_pool, ora_err, quote_ident_oracle,
    row_to_json, shard_bounds_query, shard_wrap, trim_statement,
};
use faucet_core::check::{CheckContext, CheckReport, Probe};
use faucet_core::shard::{PkShardBounds, ShardSpec, parse_pk_shard, pk_shards_from_bounds};
use faucet_core::{FaucetError, Source, StreamPage};
use futures::Stream;
use serde_json::Value;

use crate::config::{OracleReplication, OracleSourceConfig};
use crate::query::{
    CatalogRow, OwnedBind, PlannedQuery, apply_incremental, default_state_key,
    descriptors_from_catalog, plan_query, resolve_binds,
};

/// Oracle Database query source.
pub struct OracleSource {
    config: OracleSourceConfig,
    pool: OraclePool,
    start_bookmark: Mutex<Option<Value>>,
    applied_shard: Mutex<Option<PkShardBounds>>,
}

static NULL_TEXT: Option<String> = None;

fn as_tosql(b: &OwnedBind) -> &dyn ToSql {
    match b {
        OwnedBind::Null => &NULL_TEXT,
        OwnedBind::Int(i) => i,
        OwnedBind::Float(f) => f,
        OwnedBind::Text(s) => s,
    }
}

/// Everything a blocking fetch needs, owned so it can cross into
/// `spawn_blocking`.
struct QueryJob {
    pool: OraclePool,
    sql: String,
    params: Vec<Value>,
    bookmark: Option<Value>,
    array_size: u32,
    chunk: usize,
    json_columns: Vec<String>,
    timeout: Option<Duration>,
}

/// Run a query, handing each full page to `emit`; stops early when `emit`
/// returns `false` (the consumer went away).
fn run_query(job: QueryJob, mut emit: impl FnMut(Vec<Value>) -> bool) -> Result<(), FaucetError> {
    let conn = checkout(&job.pool, Side::Source, job.timeout)?;
    let mut stmt = conn
        .statement(&job.sql)
        .fetch_array_size(job.array_size)
        .build()
        .map_err(|e| ora_err(Side::Source, "prepare", &e))?;
    let names: Vec<String> = stmt.bind_names().iter().map(|s| s.to_string()).collect();
    let binds = resolve_binds(&names, &job.params, job.bookmark.as_ref())?;
    let owned: Vec<(String, OwnedBind)> = binds
        .into_iter()
        .map(|(n, v)| (n, OwnedBind::from_value(&v)))
        .collect();
    let named: Vec<(&str, &dyn ToSql)> = owned
        .iter()
        .map(|(n, b)| (n.as_str(), as_tosql(b)))
        .collect();
    let rows = stmt
        .query_named(&named)
        .map_err(|e| query_error(&e.to_string()))?;
    let mut buf = Vec::with_capacity(job.chunk.min(job.array_size as usize));
    for row in rows {
        let row = row.map_err(|e| ora_err(Side::Source, "fetch", &e))?;
        buf.push(row_to_json(&row, &job.json_columns, Side::Source)?);
        if buf.len() >= job.chunk && !emit(std::mem::take(&mut buf)) {
            return Ok(());
        }
    }
    if !buf.is_empty() {
        emit(buf);
    }
    Ok(())
}

/// A query failure, with the fix spelled out when the driver refused a native
/// `JSON` column.
fn query_error(message: &str) -> FaucetError {
    if message.contains("unsupported Oracle type JSON") {
        FaucetError::Source(format!(
            "oracle query: {message}; native JSON columns cannot be fetched directly — select \
             JSON_SERIALIZE(<col> RETURNING CLOB) AS <col> and list the column under `json_columns`"
        ))
    } else {
        FaucetError::Source(format!("oracle query: {message}"))
    }
}

impl OracleSource {
    /// Validate the config and connect (fails fast on bad credentials).
    pub async fn new(config: OracleSourceConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let pool = connect_pool(&config.connection, config.max_connections).await?;
        Ok(Self {
            config,
            pool,
            start_bookmark: Mutex::new(None),
            applied_shard: Mutex::new(None),
        })
    }

    fn current_start(&self) -> Option<Value> {
        self.start_bookmark.lock().expect("bookmark mutex").clone()
    }

    fn job(&self, planned: &PlannedQuery) -> QueryJob {
        let sql = trim_statement(&planned.sql).to_string();
        let sql = match &*self.applied_shard.lock().expect("shard mutex") {
            Some(bounds) => shard_wrap(bounds, &sql),
            None => sql,
        };
        let batch = self.config.batch_size;
        QueryJob {
            pool: self.pool.clone(),
            sql,
            params: planned.params.clone(),
            bookmark: planned.bookmark.clone(),
            array_size: if batch == 0 {
                1000
            } else {
                batch.min(u32::MAX as usize) as u32
            },
            chunk: if batch == 0 { usize::MAX } else { batch },
            json_columns: self.config.json_columns.clone(),
            timeout: call_timeout(self.config.statement_timeout_secs),
        }
    }

    async fn collect_all(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
        let start = self.current_start();
        let planned = plan_query(&self.config, context, start.as_ref());
        let mut job = self.job(&planned);
        job.chunk = usize::MAX;
        let records = blocking(move || {
            let mut all = Vec::new();
            run_query(job, |page| {
                all.extend(page);
                true
            })?;
            Ok(all)
        })
        .await?;
        let mut running = None;
        let records = apply_incremental(records, planned.incremental.as_ref(), &mut running);
        let bookmark = planned.incremental.as_ref().and(running);
        Ok((records, bookmark))
    }
}

#[async_trait]
impl Source for OracleSource {
    async fn fetch_with_context(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        Ok(self.collect_all(context).await?.0)
    }

    async fn fetch_with_context_incremental(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
        self.collect_all(context).await
    }

    fn stream_pages<'a>(
        &'a self,
        context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let start = self.current_start();
        let planned = plan_query(&self.config, context, start.as_ref());
        let job = self.job(&planned);
        Box::pin(async_stream::try_stream! {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<Value>>(2);
            let handle = tokio::task::spawn_blocking(move || {
                run_query(job, |page| tx.blocking_send(page).is_ok())
            });
            let mut running = None;
            let mut total = 0usize;
            while let Some(page) = rx.recv().await {
                let kept = apply_incremental(page, planned.incremental.as_ref(), &mut running);
                total += kept.len();
                if !kept.is_empty() {
                    yield StreamPage { records: kept, bookmark: None };
                }
            }
            handle
                .await
                .map_err(|e| FaucetError::Source(format!("oracle fetch task failed: {e}")))??;
            let bookmark = planned.incremental.as_ref().and(running);
            if bookmark.is_some() {
                yield StreamPage { records: Vec::new(), bookmark };
            }
            tracing::info!(rows = total, query = %self.config.query, "oracle source stream complete");
        })
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(OracleSourceConfig))
            .expect("schema serialization")
    }

    fn connector_name(&self) -> &'static str {
        "oracle"
    }

    fn dataset_uri(&self) -> String {
        format!(
            "{}?query={}",
            self.config.connection.display_uri(),
            self.config.query
        )
    }

    fn state_key(&self) -> Option<String> {
        match &self.config.replication {
            OracleReplication::Full => None,
            OracleReplication::Incremental { .. } => Some(
                self.config
                    .state_key
                    .clone()
                    .unwrap_or_else(|| default_state_key(&self.config)),
            ),
        }
    }

    async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
        *self.start_bookmark.lock().expect("bookmark mutex") = Some(bookmark);
        Ok(())
    }

    async fn check(&self, ctx: &CheckContext) -> Result<CheckReport, FaucetError> {
        let started = std::time::Instant::now();
        let pool = self.pool.clone();
        let probe = blocking(move || {
            let conn = checkout(&pool, Side::Source, None)?;
            conn.query_row("SELECT 1 FROM DUAL", &[])
                .map_err(|e| ora_err(Side::Source, "probe", &e))?;
            Ok(())
        });
        let hint = "check the connection settings / credentials and that the listener is reachable";
        let probe = match tokio::time::timeout(ctx.timeout, probe).await {
            Ok(Ok(())) => Probe::pass("connect", started.elapsed()),
            Ok(Err(e)) => Probe::fail_hint("connect", started.elapsed(), e.to_string(), hint),
            Err(_) => Probe::fail_hint("connect", started.elapsed(), "timed out", hint),
        };
        Ok(CheckReport::single(probe))
    }

    fn supports_discover(&self) -> bool {
        true
    }

    /// Enumerate tables in non-Oracle-maintained schemas from `ALL_TAB_COLUMNS`,
    /// with row estimates from optimizer statistics (`ALL_TABLES.NUM_ROWS`).
    async fn discover(&self) -> Result<Vec<faucet_core::DatasetDescriptor>, FaucetError> {
        const SQL: &str = "SELECT c.OWNER, c.TABLE_NAME, c.COLUMN_NAME, c.DATA_TYPE, \
                c.DATA_SCALE, c.NULLABLE, t.NUM_ROWS \
           FROM ALL_TAB_COLUMNS c \
           JOIN ALL_TABLES t ON t.OWNER = c.OWNER AND t.TABLE_NAME = c.TABLE_NAME \
          WHERE c.OWNER IN (SELECT USERNAME FROM ALL_USERS WHERE ORACLE_MAINTAINED = 'N') \
            AND t.TEMPORARY = 'N' AND t.NESTED = 'NO' AND t.SECONDARY = 'N' \
          ORDER BY c.OWNER, c.TABLE_NAME, c.COLUMN_ID";
        let pool = self.pool.clone();
        let timeout = call_timeout(self.config.statement_timeout_secs);
        let rows = blocking(move || {
            let conn = checkout(&pool, Side::Source, timeout)?;
            let rs = conn
                .query_as::<(
                    String,
                    String,
                    String,
                    String,
                    Option<i64>,
                    String,
                    Option<i64>,
                )>(SQL, &[])
                .map_err(|e| ora_err(Side::Source, "catalog discovery", &e))?;
            let mut out = Vec::new();
            for r in rs {
                let (owner, table, column, data_type, scale, nullable, num_rows) =
                    r.map_err(|e| ora_err(Side::Source, "catalog discovery", &e))?;
                out.push(CatalogRow {
                    owner,
                    table,
                    column,
                    data_type,
                    scale,
                    nullable: nullable != "N",
                    num_rows,
                });
            }
            Ok(out)
        })
        .await?;
        descriptors_from_catalog(rows)
    }

    fn is_shardable(&self) -> bool {
        self.config.shard.is_some()
    }

    async fn enumerate_shards(&self, target: usize) -> Result<Vec<ShardSpec>, FaucetError> {
        let Some(shard_cfg) = self.config.shard.clone() else {
            return Ok(vec![ShardSpec::whole()]);
        };
        let start = self.current_start();
        let planned = plan_query(&self.config, &HashMap::new(), start.as_ref());
        let key = quote_ident_oracle(&shard_cfg.key)?;
        let sql = shard_bounds_query(&planned.sql, &key);
        let pool = self.pool.clone();
        let timeout = call_timeout(self.config.statement_timeout_secs);
        let (lo, hi) = blocking(move || {
            let conn = checkout(&pool, Side::Source, timeout)?;
            let stmt_err = |e: &faucet_common_oracle::oracle::Error| {
                FaucetError::Source(format!(
                    "oracle: shard bounds for key {:?} failed (it must be an integer-valued \
                     output column): {e}",
                    shard_cfg.key
                ))
            };
            let mut stmt = conn.statement(&sql).build().map_err(|e| stmt_err(&e))?;
            let names: Vec<String> = stmt.bind_names().iter().map(|s| s.to_string()).collect();
            let binds = resolve_binds(&names, &planned.params, planned.bookmark.as_ref())?;
            let owned: Vec<(String, OwnedBind)> = binds
                .into_iter()
                .map(|(n, v)| (n, OwnedBind::from_value(&v)))
                .collect();
            let named: Vec<(&str, &dyn ToSql)> = owned
                .iter()
                .map(|(n, b)| (n.as_str(), as_tosql(b)))
                .collect();
            stmt.query_row_as_named::<(Option<i64>, Option<i64>)>(&named)
                .map_err(|e| stmt_err(&e))
        })
        .await?;
        Ok(pk_shards_from_bounds(
            &self.config.shard.as_ref().expect("checked").key,
            lo,
            hi,
            target,
        ))
    }

    async fn apply_shard(&self, shard: &ShardSpec) -> Result<(), FaucetError> {
        let bounds = parse_pk_shard(shard, "oracle")?;
        if let Some(b) = &bounds {
            quote_ident_oracle(&b.key)?;
        }
        *self.applied_shard.lock().expect("shard mutex") = bounds;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_error_hints_native_json() {
        let e = query_error("DPI-xxxx: unsupported Oracle type JSON");
        assert!(e.to_string().contains("JSON_SERIALIZE"), "{e}");
        let e = query_error("ORA-00942: table or view does not exist");
        assert!(!e.to_string().contains("JSON_SERIALIZE"), "{e}");
    }

    #[test]
    fn owned_binds_convert_to_driver_values() {
        for b in [
            OwnedBind::Null,
            OwnedBind::Int(1),
            OwnedBind::Float(1.5),
            OwnedBind::Text("x".into()),
        ] {
            let _ = as_tosql(&b);
        }
    }
}
