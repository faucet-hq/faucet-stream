//! Turn a `tracing` event into a buffered line: the rendered message, the
//! attributes of the spans it happened in, and the trace context. Shared by the
//! serve run-log layer and the spool layer of `faucet run` / `schedule`.

use std::collections::BTreeMap;
use tracing::field::{Field, Visit};
use tracing::span::Attributes;
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;

/// Span field → line attribute. `run_id` on a pipeline span is that
/// invocation's id, so it becomes `invocation_id`.
const SPAN_FIELDS: &[(&str, &str)] = &[
    ("pipeline", "pipeline"),
    ("row", "row"),
    ("connector", "connector"),
    ("shard", "shard"),
    ("tenant", "tenant"),
    ("run_id", "invocation_id"),
    ("serve_run_id", "serve_run_id"),
    ("log_run_id", "log_run_id"),
];

/// The span fields kept for log attributes, stored as a span extension.
#[derive(Clone, Debug, Default)]
pub struct SpanFields(pub BTreeMap<&'static str, String>);

#[derive(Default)]
struct FieldVisitor(BTreeMap<&'static str, String>);

impl FieldVisitor {
    fn put(&mut self, field: &Field, value: String) {
        if let Some((_, attr)) = SPAN_FIELDS.iter().find(|(f, _)| *f == field.name()) {
            self.0.insert(attr, value);
        }
    }
}

impl Visit for FieldVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.put(field, value.to_string());
    }
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.put(field, format!("{value:?}"));
    }
}

/// Remember the attribute-bearing fields of a new span.
pub fn on_new_span<S>(attrs: &Attributes<'_>, id: &tracing::span::Id, ctx: &Context<'_, S>)
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    let mut v = FieldVisitor::default();
    attrs.record(&mut v);
    if v.0.is_empty() {
        return;
    }
    if let Some(span) = ctx.span(id) {
        let mut ext = span.extensions_mut();
        if ext.get_mut::<SpanFields>().is_none() {
            ext.insert(SpanFields(v.0));
        }
    }
}

/// Remember attribute-bearing fields recorded on a span after it was created
/// (`span.record("log_run_id", …)`).
pub fn on_record<S>(
    id: &tracing::span::Id,
    values: &tracing::span::Record<'_>,
    ctx: &Context<'_, S>,
) where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    let mut v = FieldVisitor::default();
    values.record(&mut v);
    if v.0.is_empty() {
        return;
    }
    if let Some(span) = ctx.span(id) {
        let mut ext = span.extensions_mut();
        match ext.get_mut::<SpanFields>() {
            Some(f) => f.0.extend(v.0),
            None => ext.insert(SpanFields(v.0)),
        }
    }
}

/// Formats an event's fields: the `message`, then the other `key=value` fields.
#[derive(Default)]
struct EventLineVisitor {
    message: String,
    fields: String,
}

impl Visit for EventLineVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        use std::fmt::Write;
        if field.name() == "message" {
            let _ = write!(self.message, "{value:?}");
        } else {
            let _ = write!(self.fields, " {}={:?}", field.name(), value);
        }
    }
}

/// A captured event.
#[derive(Debug, Clone)]
pub struct CapturedEvent {
    pub ts: String,
    pub level: &'static str,
    pub target: String,
    /// Message + fields, redacted and size-capped.
    pub body: String,
    /// Span attributes + `target` + trace context, values redacted.
    pub attrs: BTreeMap<String, String>,
}

impl CapturedEvent {
    /// `<ts> <LEVEL> <target>: <body>` — the serve log store's line format.
    pub fn rendered(&self) -> String {
        format!("{} {} {}: {}", self.ts, self.level, self.target, self.body)
    }

    /// A span attribute, if the event happened inside a span that set it.
    pub fn attr(&self, key: &str) -> Option<&str> {
        self.attrs.get(key).map(String::as_str)
    }
}

/// Capture `event`: render it and gather the attributes of its span scope
/// (inner spans win).
pub fn capture<S>(event: &Event<'_>, ctx: &Context<'_, S>) -> CapturedEvent
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    let mut v = EventLineVisitor::default();
    event.record(&mut v);
    let meta = event.metadata();
    let body = if v.fields.is_empty() {
        v.message
    } else {
        format!("{}{}", v.message, v.fields)
    };
    let body = crate::logship::truncate_line(crate::secrets::registry::redact(&body).into_owned());
    let mut attrs = BTreeMap::new();
    if let Some(scope) = ctx.event_scope(event) {
        for span in scope.from_root() {
            let ext = span.extensions();
            if let Some(f) = ext.get::<SpanFields>() {
                for (k, val) in &f.0 {
                    attrs.insert(
                        (*k).to_string(),
                        crate::secrets::registry::redact(val).into_owned(),
                    );
                }
            }
            trace_context(&ext, &mut attrs);
        }
    }
    attrs.insert("target".to_string(), meta.target().to_string());
    CapturedEvent {
        ts: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        level: meta.level().as_str(),
        target: meta.target().to_string(),
        body,
        attrs,
    }
}

#[cfg(feature = "otel")]
fn trace_context(
    ext: &tracing_subscriber::registry::Extensions<'_>,
    attrs: &mut BTreeMap<String, String>,
) {
    let valid = |s: &str| s.bytes().any(|b| b != b'0');
    if let Some(d) = ext.get::<tracing_opentelemetry::OtelData>() {
        if let Some(t) = d.trace_id().map(|t| t.to_string())
            && valid(&t)
        {
            attrs.insert("trace_id".to_string(), t);
        }
        if let Some(s) = d.span_id().map(|s| s.to_string())
            && valid(&s)
        {
            attrs.insert("span_id".to_string(), s);
        }
    }
}

#[cfg(not(feature = "otel"))]
fn trace_context(
    _ext: &tracing_subscriber::registry::Extensions<'_>,
    _attrs: &mut BTreeMap<String, String>,
) {
}

/// Targets whose events are never captured: the shipper and its transports
/// would otherwise log about shipping into the logs being shipped.
pub fn is_shipping_noise(target: &str) -> bool {
    const NOISE: &[&str] = &[
        "faucet_cli::logship",
        "faucet_cli::serve::log_export",
        "h2",
        "hyper",
        "hyper_util",
        "tonic",
        "tower",
        "reqwest",
        "opentelemetry",
        "rustls",
    ];
    NOISE
        .iter()
        .any(|n| target == *n || target.starts_with(&format!("{n}::")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tracing_subscriber::Layer;
    use tracing_subscriber::layer::SubscriberExt;

    struct Grab(Arc<Mutex<Vec<CapturedEvent>>>);

    impl<S> Layer<S> for Grab
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        fn on_new_span(&self, a: &Attributes<'_>, id: &tracing::span::Id, ctx: Context<'_, S>) {
            on_new_span(a, id, &ctx);
        }
        fn on_record(
            &self,
            id: &tracing::span::Id,
            v: &tracing::span::Record<'_>,
            ctx: Context<'_, S>,
        ) {
            on_record(id, v, &ctx);
        }
        fn on_event(&self, e: &Event<'_>, ctx: Context<'_, S>) {
            self.0.lock().unwrap().push(capture(e, &ctx));
        }
    }

    #[test]
    fn span_fields_become_attributes_inner_wins() {
        let got = Arc::new(Mutex::new(Vec::new()));
        let sub = tracing_subscriber::registry().with(Grab(got.clone()));
        tracing::subscriber::with_default(sub, || {
            let outer = tracing::info_span!(
                "o",
                pipeline = "pipe-capture-a",
                row = "row-capture-1",
                run_id = "inv-capture-1"
            );
            let _o = outer.enter();
            let inner = tracing::info_span!(
                "i",
                row = "row-capture-2",
                connector = %"conn-capture",
                other = 1
            );
            let _i = inner.enter();
            tracing::warn!(nnn = 31337, "hello-capture");
        });
        let e = got.lock().unwrap().pop().unwrap();
        // Other tests register secrets process-wide; compare against the
        // redacted form so a short registered value cannot flake this.
        let r = |v: &str| crate::secrets::registry::redact(v).into_owned();
        assert_eq!(e.level, "WARN");
        assert_eq!(e.body, r("hello-capture nnn=31337"));
        assert_eq!(e.attr("pipeline"), Some(r("pipe-capture-a").as_str()));
        assert_eq!(e.attr("row"), Some(r("row-capture-2").as_str()));
        assert_eq!(e.attr("connector"), Some(r("conn-capture").as_str()));
        assert_eq!(e.attr("invocation_id"), Some(r("inv-capture-1").as_str()));
        assert!(e.attr("other").is_none());
        assert_eq!(e.attr("target"), Some(e.target.as_str()));
        assert!(e.rendered().contains(" WARN "));
        assert!(e.rendered().ends_with(&r("hello-capture nnn=31337")));
    }

    #[cfg(feature = "otel")]
    #[test]
    fn trace_context_rides_on_the_line() {
        use opentelemetry::trace::TracerProvider as _;
        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder().build();
        let tracer = provider.tracer("t");
        let got = Arc::new(Mutex::new(Vec::new()));
        let sub = tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(tracer))
            .with(Grab(got.clone()));
        tracing::subscriber::with_default(sub, || {
            let s = tracing::info_span!("traced", pipeline = "p");
            let _g = s.enter();
            tracing::info!("inside a trace");
        });
        let e = got.lock().unwrap().pop().unwrap();
        assert_eq!(e.attr("trace_id").map(str::len), Some(32), "{:?}", e.attrs);
        assert_eq!(e.attr("span_id").map(str::len), Some(16));
    }

    #[test]
    fn fields_recorded_later_are_kept() {
        let got = Arc::new(Mutex::new(Vec::new()));
        let sub = tracing_subscriber::registry().with(Grab(got.clone()));
        tracing::subscriber::with_default(sub, || {
            let s = tracing::info_span!(
                "tick",
                log_run_id = tracing::field::Empty,
                shard = tracing::field::Empty
            );
            s.record("log_run_id", "tick-capture-1");
            s.record("shard", "shard-capture-2");
            let plain = tracing::info_span!("plain", other = tracing::field::Empty);
            plain.record("other", 1);
            let _p = plain.enter();
            let _g = s.enter();
            tracing::info!("in the tick");
        });
        let e = got.lock().unwrap().pop().unwrap();
        let r = |v: &str| crate::secrets::registry::redact(v).into_owned();
        assert_eq!(e.attr("log_run_id"), Some(r("tick-capture-1").as_str()));
        assert_eq!(e.attr("shard"), Some(r("shard-capture-2").as_str()));
    }

    #[test]
    fn recording_extends_and_ignores_unrelated_fields() {
        let got = Arc::new(Mutex::new(Vec::new()));
        let sub = tracing_subscriber::registry().with(Grab(got.clone()));
        tracing::subscriber::with_default(sub, || {
            let s = tracing::info_span!(
                "s",
                pipeline = "pipe-rec-x",
                row = tracing::field::Empty,
                other = tracing::field::Empty
            );
            s.record("other", 3);
            s.record("row", "row-rec-x");
            let _g = s.enter();
            tracing::info!("x");
        });
        let e = got.lock().unwrap().pop().unwrap();
        let r = |v: &str| crate::secrets::registry::redact(v).into_owned();
        assert_eq!(e.attr("pipeline"), Some(r("pipe-rec-x").as_str()));
        assert_eq!(e.attr("row"), Some(r("row-rec-x").as_str()));
        assert!(e.attr("other").is_none());
    }

    #[test]
    fn registered_secrets_never_reach_a_captured_line() {
        crate::secrets::registry::register("s3cr3t-capture-value");
        let got = Arc::new(Mutex::new(Vec::new()));
        let sub = tracing_subscriber::registry().with(Grab(got.clone()));
        tracing::subscriber::with_default(sub, || {
            let s = tracing::info_span!("o", pipeline = "s3cr3t-capture-value");
            let _g = s.enter();
            tracing::info!(token = "s3cr3t-capture-value", "using s3cr3t-capture-value");
        });
        let e = got.lock().unwrap().pop().unwrap();
        assert!(!e.body.contains("s3cr3t-capture-value"), "{}", e.body);
        assert!(!e.attr("pipeline").unwrap().contains("s3cr3t-capture-value"));
    }

    #[test]
    fn plain_message_and_noise_filter() {
        let got = Arc::new(Mutex::new(Vec::new()));
        let sub = tracing_subscriber::registry().with(Grab(got.clone()));
        tracing::subscriber::with_default(sub, || tracing::info!("bare"));
        assert_eq!(got.lock().unwrap()[0].body, "bare");
        assert!(is_shipping_noise("h2::proto"));
        assert!(is_shipping_noise("tonic"));
        assert!(is_shipping_noise("faucet_cli::logship::session"));
        assert!(!is_shipping_noise("h2o"));
        assert!(!is_shipping_noise("faucet_core::pipeline"));
    }
}
