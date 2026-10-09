//! A mock OTLP logs collector for the log-shipping tests (#806): gRPC (tonic)
//! and HTTP/protobuf (wiremock). Records every acknowledged request; `down`
//! makes it refuse exports (gRPC `UNAVAILABLE`, HTTP 503) to simulate an outage.

#![allow(dead_code)]

use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::{
    LogsService, LogsServiceServer,
};
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::common::v1::any_value;
use prost_otlp::Message as _;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// One received log record, flattened.
#[derive(Debug, Clone)]
pub struct Record {
    pub service: String,
    pub body: String,
    pub severity_text: String,
    pub severity_number: i32,
    pub attrs: BTreeMap<String, String>,
    pub trace_id: Vec<u8>,
    pub span_id: Vec<u8>,
    pub scope_attrs: BTreeMap<String, String>,
}

impl Record {
    pub fn attr(&self, k: &str) -> Option<&str> {
        self.attrs.get(k).map(String::as_str)
    }
}

#[derive(Clone, Default)]
pub struct Collector {
    pub requests: Arc<Mutex<Vec<ExportLogsServiceRequest>>>,
    pub down: Arc<AtomicBool>,
    /// Acknowledge, but report one rejected record (OTLP partial success).
    pub reject: Arc<AtomicBool>,
}

fn value(v: &Option<opentelemetry_proto::tonic::common::v1::AnyValue>) -> String {
    match v.as_ref().and_then(|v| v.value.as_ref()) {
        Some(any_value::Value::StringValue(s)) => s.clone(),
        Some(any_value::Value::IntValue(i)) => i.to_string(),
        Some(other) => format!("{other:?}"),
        None => String::new(),
    }
}

impl Collector {
    pub fn set_down(&self, down: bool) {
        self.down.store(down, Ordering::SeqCst);
    }

    pub fn response(&self) -> ExportLogsServiceResponse {
        let mut r = ExportLogsServiceResponse::default();
        if self.reject.load(Ordering::SeqCst) {
            r.partial_success = Some(
                opentelemetry_proto::tonic::collector::logs::v1::ExportLogsPartialSuccess {
                    rejected_log_records: 1,
                    error_message: "rejected".into(),
                },
            );
        }
        r
    }

    pub fn is_down(&self) -> bool {
        self.down.load(Ordering::SeqCst)
    }

    pub fn accept(&self, req: ExportLogsServiceRequest) {
        self.requests.lock().unwrap().push(req);
    }

    pub fn request_count(&self) -> usize {
        self.requests.lock().unwrap().len()
    }

    pub fn records(&self) -> Vec<Record> {
        let mut out = Vec::new();
        for req in self.requests.lock().unwrap().iter() {
            for rl in &req.resource_logs {
                let service = rl
                    .resource
                    .as_ref()
                    .and_then(|r| r.attributes.iter().find(|a| a.key == "service.name"))
                    .map(|a| value(&a.value))
                    .unwrap_or_default();
                for sl in &rl.scope_logs {
                    let scope_attrs: BTreeMap<String, String> = sl
                        .scope
                        .as_ref()
                        .map(|s| {
                            s.attributes
                                .iter()
                                .map(|a| (a.key.clone(), value(&a.value)))
                                .collect()
                        })
                        .unwrap_or_default();
                    for r in &sl.log_records {
                        out.push(Record {
                            service: service.clone(),
                            body: value(&r.body),
                            severity_text: r.severity_text.clone(),
                            severity_number: r.severity_number,
                            attrs: r
                                .attributes
                                .iter()
                                .map(|a| (a.key.clone(), value(&a.value)))
                                .collect(),
                            trace_id: r.trace_id.clone(),
                            span_id: r.span_id.clone(),
                            scope_attrs: scope_attrs.clone(),
                        });
                    }
                }
            }
        }
        out
    }

    /// Every received record whose `attr` equals `value`.
    pub fn records_where(&self, attr: &str, value: &str) -> Vec<Record> {
        self.records()
            .into_iter()
            .filter(|r| r.attr(attr) == Some(value))
            .collect()
    }

    /// The raw bytes of every request, for "this never left the process" checks.
    pub fn raw(&self) -> String {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .map(|r| format!("{r:?}"))
            .collect()
    }
}

struct GrpcSvc(Collector);

#[async_trait::async_trait]
impl LogsService for GrpcSvc {
    async fn export(
        &self,
        request: tonic_otlp::Request<ExportLogsServiceRequest>,
    ) -> Result<tonic_otlp::Response<ExportLogsServiceResponse>, tonic_otlp::Status> {
        if self.0.is_down() {
            return Err(tonic_otlp::Status::unavailable("collector down"));
        }
        self.0.accept(request.into_inner());
        Ok(tonic_otlp::Response::new(self.0.response()))
    }
}

/// A gRPC collector on a free port; returns it and its `http://` endpoint.
pub async fn start_grpc() -> (Collector, String) {
    let c = Collector::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let svc = LogsServiceServer::new(GrpcSvc(c.clone()));
    let incoming = async_stream::stream! {
        loop {
            yield listener.accept().await.map(|(s, _)| s);
        }
    };
    tokio::spawn(async move {
        let _ = tonic_otlp::transport::Server::builder()
            .add_service(svc)
            .serve_with_incoming(incoming)
            .await;
    });
    (c, format!("http://{addr}"))
}

struct HttpResponder(Collector);

impl wiremock::Respond for HttpResponder {
    fn respond(&self, req: &wiremock::Request) -> wiremock::ResponseTemplate {
        if self.0.is_down() {
            return wiremock::ResponseTemplate::new(503).set_body_string("down");
        }
        match ExportLogsServiceRequest::decode(req.body.as_slice()) {
            Ok(r) => {
                self.0.accept(r);
                wiremock::ResponseTemplate::new(200)
                    .set_body_bytes(self.0.response().encode_to_vec())
            }
            Err(_) => wiremock::ResponseTemplate::new(400),
        }
    }
}

/// An HTTP/protobuf collector; returns it, its base endpoint, and the server
/// (keep it alive for the test).
pub async fn start_http() -> (Collector, String, wiremock::MockServer) {
    let c = Collector::default();
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .and(wiremock::matchers::path("/v1/logs"))
        .respond_with(HttpResponder(c.clone()))
        .mount(&server)
        .await;
    let uri = server.uri();
    (c, uri, server)
}

/// Poll `cond` for up to `secs` seconds.
pub async fn wait_for(secs: u64, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(secs);
    while std::time::Instant::now() < deadline {
        if cond() {
            return true;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    cond()
}
