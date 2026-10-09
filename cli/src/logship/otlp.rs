//! Export a batch of buffered lines as an OTLP `ExportLogsServiceRequest`
//! (#806), over gRPC or HTTP/protobuf. A batch counts as delivered only on a
//! successful response; the caller advances its watermark after that.

use crate::logship::ShipLine;
use crate::logship::record::severity_number;
use faucet_core::{OtelConfig, OtelProtocol};
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_client::LogsServiceClient;
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::common::v1::{
    AnyValue, InstrumentationScope, KeyValue, any_value,
};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost_otlp::Message as _;
use std::collections::BTreeMap;
use std::time::Duration;

/// Lines per export request.
pub const BATCH_LINES: usize = 512;
/// Approximate bytes per export request.
pub const BATCH_BYTES: u64 = 1024 * 1024;

fn kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_string())),
        }),
    }
}

fn kv_int(key: &str, value: i64) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::IntValue(value)),
        }),
    }
}

fn hex_bytes(s: &str, len: usize) -> Vec<u8> {
    if s.len() != len * 2 {
        return Vec::new();
    }
    (0..len)
        .map(|i| u8::from_str_radix(&s[i * 2..i * 2 + 2], 16))
        .collect::<Result<Vec<u8>, _>>()
        .unwrap_or_default()
}

fn unix_nanos(ts: &str) -> u64 {
    chrono::DateTime::parse_from_rfc3339(ts)
        .ok()
        .and_then(|t| t.timestamp_nanos_opt())
        .map(|n| n.max(0) as u64)
        .unwrap_or(0)
}

/// Build the request for one run's batch. `run_attrs` (run id, pipeline,
/// tenant) go on every record; the batch's sequence range goes on the scope
/// so a backend can recognise a re-sent batch.
pub fn build_request(
    service_name: &str,
    run_attrs: &BTreeMap<String, String>,
    lines: &[ShipLine],
) -> ExportLogsServiceRequest {
    let observed = chrono::Utc::now()
        .timestamp_nanos_opt()
        .map(|n| n.max(0) as u64)
        .unwrap_or(0);
    let records = lines
        .iter()
        .map(|l| {
            let mut attributes: Vec<KeyValue> = run_attrs
                .iter()
                .filter(|(k, _)| !l.attrs.contains_key(*k))
                .map(|(k, v)| kv(k, v))
                .collect();
            attributes.extend(
                l.attrs
                    .iter()
                    .filter(|(k, _)| !matches!(k.as_str(), "trace_id" | "span_id" | "log_run_id"))
                    .map(|(k, v)| kv(k, v)),
            );
            attributes.push(kv_int("faucet.seq", l.seq as i64));
            LogRecord {
                time_unix_nano: unix_nanos(&l.ts),
                observed_time_unix_nano: observed,
                severity_number: severity_number(&l.level),
                severity_text: l.level.clone(),
                body: Some(AnyValue {
                    value: Some(any_value::Value::StringValue(l.body.clone())),
                }),
                attributes,
                dropped_attributes_count: 0,
                flags: 0,
                trace_id: l
                    .attrs
                    .get("trace_id")
                    .map(|t| hex_bytes(t, 16))
                    .unwrap_or_default(),
                span_id: l
                    .attrs
                    .get("span_id")
                    .map(|s| hex_bytes(s, 8))
                    .unwrap_or_default(),
                event_name: String::new(),
            }
        })
        .collect();
    let mut scope_attrs = Vec::new();
    if let (Some(first), Some(last)) = (lines.first(), lines.last()) {
        scope_attrs.push(kv_int("faucet.batch.first_seq", first.seq as i64));
        scope_attrs.push(kv_int("faucet.batch.last_seq", last.seq as i64));
    }
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![kv("service.name", service_name)],
                dropped_attributes_count: 0,
                entity_refs: Vec::new(),
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: "faucet".to_string(),
                    version: env!("CARGO_PKG_VERSION").to_string(),
                    attributes: scope_attrs,
                    dropped_attributes_count: 0,
                }),
                log_records: records,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

/// The OTLP logs client for one collector.
#[derive(Clone)]
pub struct OtlpLogExporter {
    service_name: String,
    timeout: Duration,
    transport: Transport,
    headers: Vec<(String, String)>,
}

#[derive(Clone)]
enum Transport {
    Grpc(LogsServiceClient<tonic_otlp::transport::Channel>),
    Http {
        client: reqwest::Client,
        url: String,
    },
}

/// `<base>/v1/logs` unless the endpoint already names the path.
pub fn http_logs_endpoint(base: &str) -> String {
    let trimmed = base.trim_end_matches('/');
    if trimmed.ends_with("/v1/logs") {
        trimmed.to_string()
    } else {
        format!("{trimmed}/v1/logs")
    }
}

impl OtlpLogExporter {
    /// A client for `cfg`'s endpoint. Connects lazily, so a collector that is
    /// down at startup is retried on every export.
    pub fn new(cfg: &OtelConfig) -> Result<Self, String> {
        let endpoint = cfg.resolve_endpoint();
        let timeout = Duration::from_secs(cfg.timeout_secs.max(1));
        let transport = match cfg.protocol {
            OtelProtocol::Grpc => {
                let channel = tonic_otlp::transport::Endpoint::from_shared(endpoint.clone())
                    .map_err(|e| format!("otel.endpoint {endpoint}: {e}"))?
                    .connect_timeout(timeout)
                    .timeout(timeout)
                    .connect_lazy();
                Transport::Grpc(LogsServiceClient::new(channel))
            }
            OtelProtocol::Http => Transport::Http {
                client: reqwest::Client::builder()
                    .timeout(timeout)
                    .build()
                    .map_err(|e| format!("building the OTLP HTTP client: {e}"))?,
                url: http_logs_endpoint(&endpoint),
            },
        };
        for (k, v) in &cfg.headers {
            tonic_otlp::metadata::MetadataKey::<tonic_otlp::metadata::Ascii>::from_bytes(
                k.to_ascii_lowercase().as_bytes(),
            )
            .map_err(|e| format!("otel.headers: invalid header name '{k}': {e}"))?;
            tonic_otlp::metadata::MetadataValue::<tonic_otlp::metadata::Ascii>::try_from(
                v.as_str(),
            )
            .map_err(|e| format!("otel.headers: invalid value for header '{k}': {e}"))?;
        }
        Ok(Self {
            service_name: cfg.service_name.clone(),
            timeout,
            transport,
            headers: cfg
                .headers
                .iter()
                .map(|(k, v)| (k.to_ascii_lowercase(), v.clone()))
                .collect(),
        })
    }

    /// Export `lines` (one run's batch). `Ok` means the collector acknowledged
    /// it; the number of records it rejected outright is returned.
    pub async fn export(
        &self,
        run_attrs: &BTreeMap<String, String>,
        lines: &[ShipLine],
    ) -> Result<u64, String> {
        if lines.is_empty() {
            return Ok(0);
        }
        let req = build_request(&self.service_name, run_attrs, lines);
        let resp = match &self.transport {
            Transport::Grpc(client) => {
                let mut client = client.clone();
                let mut request = tonic_otlp::Request::new(req);
                request.set_timeout(self.timeout);
                for (k, v) in &self.headers {
                    if let (Ok(key), Ok(val)) = (
                        tonic_otlp::metadata::MetadataKey::from_bytes(k.as_bytes()),
                        tonic_otlp::metadata::MetadataValue::try_from(v.as_str()),
                    ) {
                        request.metadata_mut().insert(key, val);
                    }
                }
                client
                    .export(request)
                    .await
                    .map_err(|s| format!("OTLP gRPC export failed: {} ({})", s.message(), s.code()))?
                    .into_inner()
            }
            Transport::Http { client, url } => {
                let mut rb = client
                    .post(url)
                    .header("content-type", "application/x-protobuf")
                    .body(req.encode_to_vec());
                for (k, v) in &self.headers {
                    rb = rb.header(k.as_str(), v.as_str());
                }
                let r = rb
                    .send()
                    .await
                    .map_err(|e| format!("OTLP HTTP export to {url} failed: {e}"))?;
                let status = r.status();
                let body = r.bytes().await.unwrap_or_default();
                if !status.is_success() {
                    return Err(format!(
                        "OTLP HTTP export to {url} returned {status}: {}",
                        String::from_utf8_lossy(&body[..body.len().min(256)])
                    ));
                }
                ExportLogsServiceResponse::decode(body.as_ref()).unwrap_or_default()
            }
        };
        let rejected = resp
            .partial_success
            .map(|p| p.rejected_log_records.max(0) as u64)
            .unwrap_or(0);
        if rejected > 0 {
            crate::logship::metrics::export_error();
        }
        Ok(rejected)
    }
}

/// Split `lines` into export batches by count and size.
pub fn batches(lines: Vec<ShipLine>) -> Vec<Vec<ShipLine>> {
    let mut out = Vec::new();
    let mut cur = Vec::new();
    let mut bytes = 0u64;
    for l in lines {
        let sz = l.size();
        if !cur.is_empty() && (cur.len() >= BATCH_LINES || bytes + sz > BATCH_BYTES) {
            out.push(std::mem::take(&mut cur));
            bytes = 0;
        }
        bytes += sz;
        cur.push(l);
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line(seq: u64, attrs: &[(&str, &str)]) -> ShipLine {
        ShipLine {
            seq,
            ts: "2026-01-01T00:00:00.000Z".into(),
            level: "INFO".into(),
            body: format!("line {seq}"),
            attrs: attrs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    #[test]
    fn request_carries_attributes_trace_context_and_seq_range() {
        let run: BTreeMap<String, String> = [("run_id", "r1"), ("pipeline", "p"), ("row", "outer")]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let req = build_request(
            "svc",
            &run,
            &[
                line(
                    1,
                    &[
                        ("row", "inner"),
                        ("trace_id", "0123456789abcdef0123456789abcdef"),
                        ("span_id", "0123456789abcdef"),
                        ("log_run_id", "x"),
                    ],
                ),
                line(2, &[("trace_id", "bad")]),
            ],
        );
        let rl = &req.resource_logs[0];
        assert_eq!(
            rl.resource.as_ref().unwrap().attributes[0].key,
            "service.name"
        );
        let sl = &rl.scope_logs[0];
        let scope = sl.scope.as_ref().unwrap();
        assert_eq!(scope.attributes[0].key, "faucet.batch.first_seq");
        assert_eq!(scope.attributes[1].key, "faucet.batch.last_seq");
        let r0 = &sl.log_records[0];
        assert_eq!(r0.severity_number, 9);
        assert_eq!(r0.trace_id.len(), 16);
        assert_eq!(r0.span_id.len(), 8);
        assert!(r0.time_unix_nano > 0);
        let keys: Vec<&str> = r0.attributes.iter().map(|a| a.key.as_str()).collect();
        assert!(keys.contains(&"run_id") && keys.contains(&"pipeline"));
        assert!(!keys.contains(&"trace_id") && !keys.contains(&"log_run_id"));
        let rows: Vec<_> = r0.attributes.iter().filter(|a| a.key == "row").collect();
        assert_eq!(rows.len(), 1, "line attrs win over run attrs");
        assert!(sl.log_records[1].trace_id.is_empty());
        assert!(build_request("svc", &run, &[]).resource_logs[0].scope_logs[0]
            .scope
            .as_ref()
            .unwrap()
            .attributes
            .is_empty());
        assert_eq!(unix_nanos("garbage"), 0);
        assert!(hex_bytes("zz", 1).is_empty());
    }

    #[test]
    fn batching_splits_by_count_and_size() {
        let lines: Vec<ShipLine> = (0..(BATCH_LINES as u64 * 2 + 1)).map(|i| line(i, &[])).collect();
        let b = batches(lines);
        assert_eq!(b.len(), 3);
        assert_eq!(b[0].len(), BATCH_LINES);
        let mut big = line(1, &[]);
        big.body = "x".repeat(BATCH_BYTES as usize);
        let b = batches(vec![big.clone(), big]);
        assert_eq!(b.len(), 2);
        assert!(batches(Vec::new()).is_empty());
    }

    #[test]
    fn endpoints_and_header_validation() {
        assert_eq!(http_logs_endpoint("http://c:4318"), "http://c:4318/v1/logs");
        assert_eq!(http_logs_endpoint("http://c:4318/v1/logs/"), "http://c:4318/v1/logs");
        let mut cfg = OtelConfig {
            protocol: OtelProtocol::Http,
            ..Default::default()
        };
        cfg.headers.insert("bad header".into(), "v".into());
        assert!(OtlpLogExporter::new(&cfg).is_err());
        cfg.headers.clear();
        cfg.headers.insert("x-ok".into(), "line\nbreak".into());
        assert!(OtlpLogExporter::new(&cfg).is_err());
        let grpc = OtelConfig {
            endpoint: "not a uri".into(),
            ..Default::default()
        };
        assert!(OtlpLogExporter::new(&grpc).is_err());
    }

    #[tokio::test]
    async fn an_empty_batch_is_a_no_op_and_a_dead_collector_fails() {
        let cfg = OtelConfig {
            endpoint: "http://127.0.0.1:1".into(),
            protocol: OtelProtocol::Http,
            timeout_secs: 1,
            ..Default::default()
        };
        let ex = OtlpLogExporter::new(&cfg).unwrap();
        assert_eq!(ex.export(&BTreeMap::new(), &[]).await.unwrap(), 0);
        assert!(ex.export(&BTreeMap::new(), &[line(1, &[])]).await.is_err());
        let grpc = OtlpLogExporter::new(&OtelConfig {
            endpoint: "http://127.0.0.1:1".into(),
            timeout_secs: 1,
            ..Default::default()
        })
        .unwrap();
        assert!(grpc.export(&BTreeMap::new(), &[line(1, &[])]).await.is_err());
    }
}
