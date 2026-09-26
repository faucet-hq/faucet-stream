//! The DynamoDB `Source` implementation: parallel scan / query with
//! per-segment cursors, and DynamoDB Streams CDC with lineage-ordered shard
//! workers, cumulative per-shard bookmarks, gap handling and idle /
//! max-messages termination.

use crate::config::{DynamoDbSourceConfig, OnGap, ReadMode};
use crate::envelope::snapshot_envelope;
use crate::lineage::{Planner, capture_bookmark, detect_gaps, ids_and_parents, start_for};
use crate::scan::{ScanEvent, expressions, run_segment};
use crate::state::{ScanBookmark, SegmentCursor, StreamBookmark, state_key};
use crate::streams::{IteratorOutcome, ShardEvent, acquire_iterator, describe_shards, run_shard};
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::TableDescription;
use aws_sdk_dynamodbstreams::Client as StreamsClient;
use faucet_common_dynamodb::{KeyAttribute, key_of, key_schema, sdk_error_parts};
use faucet_core::{FaucetError, ShardSpec, Stream, StreamPage};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

type PageStream<'a> = Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>>;

/// Amazon DynamoDB source. See the crate README for semantics.
pub struct DynamoDbSource {
    config: DynamoDbSourceConfig,
    client: Client,
    streams: StreamsClient,
    start_bookmark: Mutex<Option<Value>>,
    shard: Mutex<Option<(u32, u32)>>,
}

/// Parse a `{segment, total_segments}` shard descriptor. Pure.
pub(crate) fn parse_segment_shard(spec: &ShardSpec) -> Result<(u32, u32), FaucetError> {
    let get = |k: &str| {
        spec.descriptor
            .get(k)
            .and_then(Value::as_u64)
            .and_then(|n| u32::try_from(n).ok())
    };
    match (get("segment"), get("total_segments")) {
        (Some(s), Some(t)) if t > 0 && s < t => Ok((s, t)),
        _ => Err(FaucetError::Source(format!(
            "dynamodb: invalid scan shard descriptor {} (expected {{segment, total_segments}})",
            spec.descriptor
        ))),
    }
}

/// Plan `total` scan segments as shards. Pure.
pub(crate) fn plan_segment_shards(total: u32) -> Vec<ShardSpec> {
    (0..total)
        .map(|i| {
            ShardSpec::new(
                i.to_string(),
                json!({"segment": i, "total_segments": total}),
            )
        })
        .collect()
}

fn chunk_size(batch_size: usize) -> usize {
    if batch_size == 0 {
        usize::MAX
    } else {
        batch_size
    }
}

impl DynamoDbSource {
    /// Create a new DynamoDB source. Validates the config and builds the AWS
    /// clients; no network I/O until streaming.
    pub async fn new(config: DynamoDbSourceConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let region = config.region.as_deref();
        let endpoint = config.endpoint_url.as_deref();
        let client =
            faucet_common_dynamodb::build_client(region, endpoint, &config.credentials).await?;
        let streams =
            faucet_common_dynamodb::build_streams_client(region, endpoint, &config.credentials)
                .await?;
        Ok(Self {
            config,
            client,
            streams,
            start_bookmark: Mutex::new(None),
            shard: Mutex::new(None),
        })
    }

    async fn describe_table(&self) -> Result<TableDescription, FaucetError> {
        let out = self
            .client
            .describe_table()
            .table_name(&self.config.table_name)
            .send()
            .await
            .map_err(|e| {
                FaucetError::Source(format!(
                    "dynamodb: DescribeTable '{}' failed: {}",
                    self.config.table_name,
                    sdk_error_parts(&e).1
                ))
            })?;
        out.table().cloned().ok_or_else(|| {
            FaucetError::Source(format!(
                "dynamodb: DescribeTable '{}' returned no table",
                self.config.table_name
            ))
        })
    }

    async fn resolve_stream_arn(&self) -> Result<String, FaucetError> {
        if let Some(arn) = &self.config.stream_arn {
            return Ok(arn.clone());
        }
        let desc = self.describe_table().await?;
        desc.latest_stream_arn().map(str::to_string).ok_or_else(|| {
            FaucetError::Source(format!(
                "dynamodb: table '{}' has no stream — enable DynamoDB Streams \
                 (NEW_AND_OLD_IMAGES) or set stream_arn",
                self.config.table_name
            ))
        })
    }

    fn start_bookmark(&self) -> Option<Value> {
        self.start_bookmark
            .lock()
            .expect("bookmark mutex poisoned")
            .clone()
    }

    /// Segments this instance reads and the segment total.
    fn scan_targets(&self) -> (Vec<u32>, u32) {
        match *self.shard.lock().expect("shard mutex poisoned") {
            Some((segment, total)) => (vec![segment], total),
            None if self.config.mode == ReadMode::Scan => {
                ((0..self.config.segments).collect(), self.config.segments)
            }
            None => (vec![0], 1),
        }
    }

    /// Scan/query pages. `snapshot` wraps each item in an `op: "r"` envelope;
    /// `emit_cursors` puts the cumulative cursors on intermediate pages;
    /// `final_bookmark` goes on the last page.
    fn scan_stream<'a>(
        &'a self,
        targets: Vec<u32>,
        total: u32,
        resume: BTreeMap<u32, SegmentCursor>,
        snapshot: Option<Vec<KeyAttribute>>,
        emit_cursors: bool,
        final_bookmark: Value,
    ) -> PageStream<'a> {
        let chunk = chunk_size(self.config.batch_size);
        Box::pin(async_stream::try_stream! {
            let exprs = expressions(&self.config);
            let (tx, mut rx) = tokio::sync::mpsc::channel::<ScanEvent>(16);
            let semaphore = Arc::new(tokio::sync::Semaphore::new(self.config.max_concurrency));
            let mut cursors: BTreeMap<u32, SegmentCursor> = resume
                .into_iter()
                .filter(|(s, _)| targets.contains(s))
                .collect();
            let mut handles = Vec::new();
            for &segment in &targets {
                let cursor = cursors.get(&segment).cloned();
                if cursor == Some(SegmentCursor::Done) {
                    continue;
                }
                let sem = semaphore.clone();
                let client = self.client.clone();
                let config = self.config.clone();
                let exprs = exprs.clone();
                let tx = tx.clone();
                handles.push(tokio::spawn(async move {
                    let _permit = sem.acquire_owned().await.expect("semaphore closed");
                    run_segment(client, config, exprs, segment, total, cursor, tx).await;
                }));
            }
            drop(tx);

            let bookmark = |cursors: &BTreeMap<u32, SegmentCursor>| {
                emit_cursors.then(|| {
                    ScanBookmark { total_segments: total, segments: cursors.clone() }.to_value()
                })
            };
            let mut buffer: Vec<Value> = Vec::new();
            let mut total_items = 0usize;
            let mut failure = None;
            while let Some(event) = rx.recv().await {
                match event {
                    ScanEvent::Page { segment, items, next } => {
                        for item in items {
                            let record = match &snapshot {
                                Some(keys) => {
                                    let key = key_of(&item, keys);
                                    snapshot_envelope(item, key, &self.config.table_name)
                                }
                                None => item,
                            };
                            buffer.push(record);
                            total_items += 1;
                            if buffer.len() >= chunk {
                                yield StreamPage {
                                    records: std::mem::take(&mut buffer),
                                    bookmark: bookmark(&cursors),
                                };
                            }
                        }
                        cursors.insert(segment, next);
                    }
                    ScanEvent::Failed { segment, error } => {
                        tracing::error!(segment, error = %error, "dynamodb: segment failed");
                        failure = Some(error);
                        break;
                    }
                }
            }
            for h in handles {
                h.abort();
            }
            if let Some(error) = failure {
                Err(error)?;
            }
            tracing::info!(table = %self.config.table_name, items = total_items,
                segments = targets.len(), total_segments = total, "dynamodb scan complete");
            yield StreamPage { records: buffer, bookmark: Some(final_bookmark) };
        })
    }

    fn cdc_stream<'a>(&'a self) -> PageStream<'a> {
        let chunk = chunk_size(self.config.batch_size);
        Box::pin(async_stream::try_stream! {
            let arn = self.resolve_stream_arn().await?;
            let described = describe_shards(&self.streams, &arn).await?;
            let mut bm = self
                .start_bookmark()
                .map(|v| StreamBookmark::from_value(&v))
                .unwrap_or_default();

            let mut gaps = detect_gaps(&bm, &arn, &described);
            if gaps.is_empty() {
                let (ids, _) = ids_and_parents(&described);
                for (id, seq) in bm.shards.iter().filter(|(id, s)| !s.is_empty() && ids.contains(*id)) {
                    let start = crate::lineage::StartAt::After(seq.clone());
                    if let IteratorOutcome::Trimmed =
                        acquire_iterator(&self.streams, &arn, id, &start).await?
                    {
                        gaps.push(format!("shard {id} was trimmed past sequence {seq}"));
                    }
                }
            }
            if !gaps.is_empty() {
                let reasons = gaps.join("; ");
                match self.config.on_gap {
                    OnGap::Fail => Err(FaucetError::Source(format!(
                        "dynamodb streams: cannot resume table '{}' without losing changes: \
                         {reasons}. Set on_gap: resnapshot to re-read the table, or reset the \
                         pipeline state",
                        self.config.table_name
                    )))?,
                    OnGap::Resnapshot => {
                        tracing::warn!(table = %self.config.table_name, reasons = %reasons,
                            "dynamodb streams: bookmark is past the trim horizon; resnapshotting");
                        let keys = key_schema(&self.describe_table().await?)?;
                        let capture = capture_bookmark(&arn, &described);
                        let mut snap = self.scan_stream(
                            vec![0], 1, BTreeMap::new(), Some(keys), false, capture.to_value(),
                        );
                        while let Some(page) = futures::StreamExt::next(&mut snap).await {
                            yield page?;
                        }
                        bm = capture;
                    }
                }
            }
            bm.stream_arn = Some(arn.clone());
            let (ids, parents) = ids_and_parents(&described);
            bm.prune_finished(&ids, &parents);
            let mut planner = Planner::new(described, &bm.finished);

            let (tx, mut rx) = tokio::sync::mpsc::channel::<ShardEvent>(16);
            let semaphore = Arc::new(tokio::sync::Semaphore::new(self.config.shard_concurrency));
            let mut handles = Vec::new();
            let mut active = 0usize;
            let mut to_spawn = planner.ready();
            let idle = self.config.idle_termination_secs.map(Duration::from_secs);
            let max_messages = self.config.max_messages;
            let mut buffer: Vec<Value> = Vec::new();
            let mut total = 0usize;
            let mut failure: Option<FaucetError> = None;

            'consume: loop {
                for id in std::mem::take(&mut to_spawn) {
                    let start = start_for(
                        bm.shards.get(&id).map(String::as_str),
                        planner.parent_known(&id),
                        self.config.start_position,
                    );
                    planner.start(&id);
                    bm.open(&id);
                    active += 1;
                    let sem = semaphore.clone();
                    let client = self.streams.clone();
                    let config = self.config.clone();
                    let arn = arn.clone();
                    let tx = tx.clone();
                    handles.push(tokio::spawn(async move {
                        let _permit = sem.acquire_owned().await.expect("semaphore closed");
                        run_shard(client, config, arn, id, start, tx).await;
                    }));
                }
                if active == 0 {
                    break 'consume;
                }
                let event = match idle {
                    Some(window) => match tokio::time::timeout(window, rx.recv()).await {
                        Ok(ev) => ev,
                        Err(_) => {
                            tracing::info!(table = %self.config.table_name,
                                idle_secs = window.as_secs(), "dynamodb streams: idle termination");
                            break 'consume;
                        }
                    },
                    None => rx.recv().await,
                };
                match event {
                    Some(ShardEvent::Records { shard_id, records }) => {
                        for (sequence, record) in records {
                            buffer.push(record);
                            total += 1;
                            bm.advance(&shard_id, &sequence);
                            if buffer.len() >= chunk {
                                yield StreamPage {
                                    records: std::mem::take(&mut buffer),
                                    bookmark: Some(bm.to_value()),
                                };
                            }
                            if let Some(max) = max_messages
                                && total >= max
                            {
                                tracing::info!(table = %self.config.table_name, max,
                                    "dynamodb streams: max_messages reached");
                                break 'consume;
                            }
                        }
                    }
                    Some(ShardEvent::Done { shard_id }) => {
                        active -= 1;
                        bm.finish(&shard_id);
                        planner.finish(&shard_id);
                        if !planner.has_children(&shard_id) {
                            match describe_shards(&self.streams, &arn).await {
                                Ok(fresh) => planner.merge(fresh),
                                Err(e) => tracing::warn!(error = %e,
                                    "dynamodb streams: shard refresh failed"),
                            }
                        }
                        to_spawn = planner.ready();
                    }
                    Some(ShardEvent::Failed { shard_id, error }) => {
                        tracing::error!(shard = %shard_id, error = %error,
                            "dynamodb streams: shard worker failed");
                        failure = Some(error);
                        break 'consume;
                    }
                    None => break 'consume,
                }
            }
            rx.close();
            for h in handles {
                h.abort();
            }
            if let Some(error) = failure {
                Err(error)?;
            }
            tracing::info!(table = %self.config.table_name, records = total,
                "dynamodb streams source complete");
            yield StreamPage { records: buffer, bookmark: Some(bm.to_value()) };
        })
    }
}

#[faucet_core::async_trait]
impl faucet_core::Source for DynamoDbSource {
    async fn fetch_with_context(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        use futures::StreamExt;
        let mut pages = self.stream_pages(context, self.config.batch_size);
        let mut all = Vec::new();
        while let Some(page) = pages.next().await {
            all.extend(page?.records);
        }
        Ok(all)
    }

    fn stream_pages<'a>(
        &'a self,
        _context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> PageStream<'a> {
        if self.config.mode == ReadMode::Streams {
            return self.cdc_stream();
        }
        let (targets, total) = self.scan_targets();
        let resume = self
            .start_bookmark()
            .map(|v| ScanBookmark::from_value(&v).for_total(total))
            .unwrap_or_default();
        self.scan_stream(
            targets,
            total,
            resume,
            None,
            true,
            ScanBookmark::default().to_value(),
        )
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(DynamoDbSourceConfig))
            .expect("schema serialization")
    }

    fn state_key(&self) -> Option<String> {
        Some(state_key(self.config.mode, &self.config.table_name))
    }

    async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
        *self.start_bookmark.lock().expect("bookmark mutex poisoned") = Some(bookmark);
        Ok(())
    }

    /// Streams mode: every current shard opened at its trim horizon, so a
    /// CDC run started after a snapshot replays the retained window and
    /// converges. Other modes: `None`.
    async fn capture_resume_position(&self) -> Result<Option<Value>, FaucetError> {
        if self.config.mode != ReadMode::Streams {
            return Ok(None);
        }
        let arn = self.resolve_stream_arn().await?;
        let described = describe_shards(&self.streams, &arn).await?;
        Ok(Some(capture_bookmark(&arn, &described).to_value()))
    }

    fn is_shardable(&self) -> bool {
        self.config.mode == ReadMode::Scan
    }

    /// One shard per scan segment: `segments` when configured above 1,
    /// otherwise `target`.
    async fn enumerate_shards(&self, target: usize) -> Result<Vec<ShardSpec>, FaucetError> {
        if self.config.mode != ReadMode::Scan {
            return Ok(vec![ShardSpec::whole()]);
        }
        let total = if self.config.segments > 1 {
            self.config.segments
        } else {
            u32::try_from(target)
                .unwrap_or(crate::config::MAX_SEGMENTS)
                .clamp(1, crate::config::MAX_SEGMENTS)
        };
        Ok(plan_segment_shards(total))
    }

    async fn apply_shard(&self, shard: &ShardSpec) -> Result<(), FaucetError> {
        if shard.is_whole() {
            return Ok(());
        }
        if self.config.mode != ReadMode::Scan {
            return Err(FaucetError::Source(format!(
                "dynamodb: mode {} is not shardable",
                self.config.mode.as_str()
            )));
        }
        let parsed = parse_segment_shard(shard)?;
        *self.shard.lock().expect("shard mutex poisoned") = Some(parsed);
        Ok(())
    }

    fn supports_discover(&self) -> bool {
        true
    }

    /// `ListTables` + `DescribeTable` per table (metadata only).
    async fn discover(&self) -> Result<Vec<faucet_core::DatasetDescriptor>, FaucetError> {
        let mut names = Vec::new();
        let mut start: Option<String> = None;
        loop {
            let out = self
                .client
                .list_tables()
                .set_exclusive_start_table_name(start.clone())
                .send()
                .await
                .map_err(|e| {
                    FaucetError::Source(format!(
                        "dynamodb: ListTables failed: {}",
                        sdk_error_parts(&e).1
                    ))
                })?;
            names.extend(out.table_names().iter().cloned());
            start = out.last_evaluated_table_name().map(str::to_string);
            if start.is_none() {
                break;
            }
        }
        let mut out = Vec::with_capacity(names.len());
        for name in names {
            let desc = self
                .client
                .describe_table()
                .table_name(&name)
                .send()
                .await
                .map_err(|e| {
                    FaucetError::Source(format!(
                        "dynamodb: DescribeTable '{name}' failed: {}",
                        sdk_error_parts(&e).1
                    ))
                })?;
            if let Some(d) = desc
                .table()
                .and_then(crate::discover::descriptor_from_table)
            {
                out.push(d);
            }
        }
        Ok(out)
    }

    fn connector_name(&self) -> &'static str {
        "dynamodb"
    }

    fn dataset_uri(&self) -> String {
        format!(
            "dynamodb://{}/{}",
            self.config.region.as_deref().unwrap_or("default"),
            self.config.table_name
        )
    }

    /// Side-effect-free probe: `DescribeTable`, plus `DescribeStream` in
    /// streams mode (no records read, no iterator opened).
    async fn check(
        &self,
        ctx: &faucet_core::CheckContext,
    ) -> Result<faucet_core::CheckReport, FaucetError> {
        use faucet_core::{CheckReport, Probe};
        let start = std::time::Instant::now();
        let mut report = CheckReport::default();
        let table = tokio::time::timeout(ctx.timeout, self.describe_table()).await;
        let stream_arn = match table {
            Err(_) => {
                report
                    .probes
                    .push(Probe::fail("describe_table", start.elapsed(), "timed out"));
                return Ok(report);
            }
            Ok(Err(e)) => {
                report.probes.push(Probe::fail(
                    "describe_table",
                    start.elapsed(),
                    e.to_string(),
                ));
                return Ok(report);
            }
            Ok(Ok(desc)) => {
                report
                    .probes
                    .push(Probe::pass("describe_table", start.elapsed()));
                self.config
                    .stream_arn
                    .clone()
                    .or_else(|| desc.latest_stream_arn().map(str::to_string))
            }
        };
        if self.config.mode == ReadMode::Streams {
            let start = std::time::Instant::now();
            let probe = match stream_arn {
                None => Probe::fail(
                    "describe_stream",
                    start.elapsed(),
                    "table has no stream — enable DynamoDB Streams (NEW_AND_OLD_IMAGES)",
                ),
                Some(arn) => {
                    let fut = self
                        .streams
                        .describe_stream()
                        .stream_arn(arn)
                        .limit(1)
                        .send();
                    match tokio::time::timeout(ctx.timeout, fut).await {
                        Err(_) => Probe::fail("describe_stream", start.elapsed(), "timed out"),
                        Ok(Ok(_)) => Probe::pass("describe_stream", start.elapsed()),
                        Ok(Err(e)) => {
                            Probe::fail("describe_stream", start.elapsed(), sdk_error_parts(&e).1)
                        }
                    }
                }
            };
            report.probes.push(probe);
        }
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::Source as _;

    async fn offline(mut config: DynamoDbSourceConfig) -> DynamoDbSource {
        config.endpoint_url = Some("http://127.0.0.1:1".into());
        config.region = Some("us-east-1".into());
        config.credentials = faucet_common_dynamodb::DynamoDbCredentials::AccessKey {
            access_key_id: "test".into(),
            secret_access_key: "test".into(),
            session_token: None,
        };
        config.retry.max_retries = 0;
        DynamoDbSource::new(config).await.expect("source builds")
    }

    fn streams_config() -> DynamoDbSourceConfig {
        let mut c = DynamoDbSourceConfig::new("t");
        c.mode = ReadMode::Streams;
        c.max_messages = Some(1);
        c
    }

    #[test]
    fn segment_shards_plan_and_parse() {
        let shards = plan_segment_shards(3);
        assert_eq!(shards.len(), 3);
        assert_eq!(shards[2].id, "2");
        assert_eq!(parse_segment_shard(&shards[1]).unwrap(), (1, 3));
        for bad in [
            json!({"segment": 3, "total_segments": 3}),
            json!({"segment": 0, "total_segments": 0}),
            json!({"segment": 0}),
            json!(null),
        ] {
            assert!(parse_segment_shard(&ShardSpec::new("x", bad)).is_err());
        }
        assert_eq!(chunk_size(0), usize::MAX);
        assert_eq!(chunk_size(5), 5);
    }

    #[tokio::test]
    async fn new_validates() {
        assert!(
            DynamoDbSource::new(DynamoDbSourceConfig::new(""))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn identity_and_shard_hooks() {
        let mut cfg = DynamoDbSourceConfig::new("orders");
        cfg.segments = 4;
        let source = offline(cfg).await;
        assert_eq!(source.connector_name(), "dynamodb");
        assert_eq!(source.dataset_uri(), "dynamodb://us-east-1/orders");
        assert_eq!(source.state_key().as_deref(), Some("dynamodb:orders"));
        assert!(!source.supports_exactly_once());
        assert!(source.supports_discover());
        assert!(source.is_shardable());
        assert!(source.config_schema()["properties"]["table_name"].is_object());
        assert_eq!(source.scan_targets(), (vec![0, 1, 2, 3], 4));
        assert_eq!(source.enumerate_shards(16).await.unwrap().len(), 4);
        source.apply_shard(&ShardSpec::whole()).await.unwrap();
        assert_eq!(source.scan_targets().1, 4);
        source
            .apply_shard(&plan_segment_shards(4)[2])
            .await
            .unwrap();
        assert_eq!(source.scan_targets(), (vec![2], 4));
        assert!(source.capture_resume_position().await.unwrap().is_none());

        let single = offline(DynamoDbSourceConfig::new("orders")).await;
        assert_eq!(single.enumerate_shards(6).await.unwrap().len(), 6);
        assert_eq!(single.enumerate_shards(0).await.unwrap().len(), 1);

        let mut q = DynamoDbSourceConfig::new("orders");
        q.mode = ReadMode::Query;
        q.key_condition_expression = Some("pk = :p".into());
        let q = offline(q).await;
        assert_eq!(q.scan_targets(), (vec![0], 1));

        let s = offline(streams_config()).await;
        assert!(!s.is_shardable());
        assert_eq!(s.state_key().as_deref(), Some("dynamodb-streams:t"));
        assert!(s.enumerate_shards(4).await.unwrap()[0].is_whole());
        assert!(s.apply_shard(&plan_segment_shards(2)[0]).await.is_err());
    }

    #[tokio::test]
    async fn bookmark_is_held() {
        let source = offline(DynamoDbSourceConfig::new("t")).await;
        source.apply_start_bookmark(json!({"a": 1})).await.unwrap();
        assert_eq!(source.start_bookmark(), Some(json!({"a": 1})));
    }

    #[tokio::test]
    async fn offline_errors_are_typed() {
        use futures::StreamExt;
        let source = offline(DynamoDbSourceConfig::new("t")).await;
        let err = source.fetch_all().await.unwrap_err();
        assert!(err.to_string().contains("scan of 't'"), "{err}");
        assert!(
            source
                .discover()
                .await
                .unwrap_err()
                .to_string()
                .contains("ListTables")
        );
        let report = source
            .check(&faucet_core::CheckContext {
                timeout: Duration::from_secs(30),
            })
            .await
            .unwrap();
        assert_eq!(report.failed_count(), 1);

        let s = offline(streams_config()).await;
        let ctx = HashMap::new();
        let first = s.stream_pages(&ctx, 10).next().await.unwrap();
        assert!(first.unwrap_err().to_string().contains("DescribeTable"));
        assert!(s.capture_resume_position().await.is_err());

        let mut with_arn = streams_config();
        with_arn.stream_arn = Some("arn:aws:dynamodb:us-east-1:0:table/t/stream/x".into());
        let s = offline(with_arn).await;
        let first = s.stream_pages(&ctx, 10).next().await.unwrap();
        assert!(first.unwrap_err().to_string().contains("DescribeStream"));
    }

    #[tokio::test]
    async fn check_times_out_cleanly() {
        let source = offline(streams_config()).await;
        let report = source
            .check(&faucet_core::CheckContext {
                timeout: Duration::from_millis(1),
            })
            .await
            .unwrap();
        assert_eq!(report.failed_count(), 1);
    }

    use crate::test_support::{dynamo, err, ok, on, streams};
    use wiremock::MockServer;

    fn mock_source(uri: &str, config: DynamoDbSourceConfig) -> DynamoDbSource {
        DynamoDbSource {
            config,
            client: dynamo(uri),
            streams: streams(uri),
            start_bookmark: Mutex::new(None),
            shard: Mutex::new(None),
        }
    }

    #[tokio::test]
    async fn table_and_stream_resolution_errors() {
        let server = MockServer::start().await;
        on(&server, "DynamoDB_20120810.DescribeTable", ok(json!({})), 1).await;
        on(
            &server,
            "DynamoDB_20120810.DescribeTable",
            ok(json!({"Table": {"TableName": "t"}})),
            1,
        )
        .await;
        let source = mock_source(&server.uri(), streams_config());
        let e = source
            .capture_resume_position()
            .await
            .unwrap_err()
            .to_string();
        assert!(e.contains("returned no table"), "{e}");
        let e = source
            .capture_resume_position()
            .await
            .unwrap_err()
            .to_string();
        assert!(e.contains("has no stream"), "{e}");

        on(
            &server,
            "DynamoDB_20120810.ListTables",
            ok(json!({"TableNames": ["t"]})),
            1,
        )
        .await;
        on(
            &server,
            "DynamoDB_20120810.DescribeTable",
            err(400, "ResourceNotFoundException"),
            1,
        )
        .await;
        let e = source.discover().await.unwrap_err().to_string();
        assert!(e.contains("DescribeTable 't' failed"), "{e}");
    }

    #[tokio::test]
    async fn trimmed_bookmark_is_a_gap() {
        let server = MockServer::start().await;
        on(
            &server,
            "DynamoDBStreams_20120810.DescribeStream",
            ok(json!({"StreamDescription": {"Shards": [{"ShardId": "s1"}]}})),
            1,
        )
        .await;
        on(
            &server,
            "DynamoDBStreams_20120810.GetShardIterator",
            err(400, "TrimmedDataAccessException"),
            1,
        )
        .await;
        let mut cfg = streams_config();
        cfg.stream_arn = Some("arn".into());
        let source = mock_source(&server.uri(), cfg);
        source
            .apply_start_bookmark(json!({"stream_arn": "arn", "shards": {"s1": "5"}}))
            .await
            .unwrap();
        let e = source.fetch_all().await.unwrap_err().to_string();
        assert!(e.contains("trimmed past sequence 5"), "{e}");
    }
}
