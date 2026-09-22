//! Conversion utilities for WASI OpenTelemetry types.

use opentelemetry::logs::{AnyValue, LogRecord as OtelLogRecord};
use opentelemetry::trace::{
    Event, Link, SpanContext, SpanId, SpanKind, Status, TraceFlags, TraceId, TraceState,
};
use opentelemetry::{Array, InstrumentationScope, Key, KeyValue, Value};
use opentelemetry_sdk::trace::{SpanData, SpanEvents, SpanLinks};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::bindings::wasi::otel::logs::LogRecord as WasiLogRecord;
use super::bindings::wasi::otel::metrics as wasi_metrics;
use super::bindings::wasi::otel::tracing as wasi_tracing;
use super::bindings::wasi::otel::tracing::{
    SpanContext as WitSpanContext, TraceFlags as WitTraceFlags,
};

#[derive(Debug, PartialEq)]
pub enum SpanConversionError {
    InvalidField(&'static str),
    UnsupportedAttribute(String),
}

impl std::fmt::Display for SpanConversionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidField(field) => write!(formatter, "invalid {field}"),
            Self::UnsupportedAttribute(key) => {
                write!(formatter, "unsupported attribute value for {key}")
            }
        }
    }
}

pub fn otel_span_context_to_wit(ctx: &SpanContext) -> WitSpanContext {
    WitSpanContext {
        trace_id: format!("{:032x}", ctx.trace_id()),
        span_id: format!("{:016x}", ctx.span_id()),
        trace_flags: if ctx.is_sampled() {
            WitTraceFlags::SAMPLED
        } else {
            WitTraceFlags::empty()
        },
        is_remote: ctx.is_remote(),
        trace_state: ctx
            .trace_state()
            .header()
            .split(',')
            .filter_map(|entry| entry.split_once('='))
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect(),
    }
}

pub fn convert_wasi_log_record<R: OtelLogRecord>(
    wasi_record: WasiLogRecord,
    otel_record: &mut R,
    service_name: impl Into<String>,
) {
    use opentelemetry::logs::Severity;
    otel_record.add_attribute(
        Key::new("resource.service.name"),
        AnyValue::String(service_name.into().into()),
    );
    if let Some(ts) = wasi_record
        .timestamp
        .and_then(|ts| UNIX_EPOCH.checked_add(Duration::new(ts.seconds, ts.nanoseconds)))
    {
        otel_record.set_timestamp(ts);
    }
    if let Some(ts) = wasi_record
        .observed_timestamp
        .and_then(|ts| UNIX_EPOCH.checked_add(Duration::new(ts.seconds, ts.nanoseconds)))
    {
        otel_record.set_observed_timestamp(ts);
    }
    if let Some(number) = wasi_record.severity_number {
        otel_record.set_severity_number(match number {
            1..=4 => Severity::Trace,
            5..=8 => Severity::Debug,
            9..=12 => Severity::Info,
            13..=16 => Severity::Warn,
            17..=20 => Severity::Error,
            21..=24 => Severity::Fatal,
            _ => Severity::Info,
        });
    }
    if let Some(body) = wasi_record.body {
        otel_record.set_body(AnyValue::String(body.into()));
    }
    if let Some(attributes) = wasi_record.attributes {
        for kv in attributes {
            otel_record.add_attribute(Key::new(kv.key), AnyValue::String(kv.value.into()));
        }
    }
    if wasi_record.trace_id.is_some() || wasi_record.span_id.is_some() {
        let trace_id = wasi_record
            .trace_id
            .as_deref()
            .and_then(|id| TraceId::from_hex(id).ok())
            .unwrap_or(TraceId::INVALID);
        let span_id = wasi_record
            .span_id
            .as_deref()
            .and_then(|id| SpanId::from_hex(id).ok())
            .unwrap_or(SpanId::INVALID);
        let flags = wasi_record
            .trace_flags
            .filter(|flags| flags.contains(WitTraceFlags::SAMPLED))
            .map(|_| TraceFlags::SAMPLED)
            .unwrap_or_default();
        otel_record.set_trace_context(trace_id, span_id, Some(flags));
    }
}

pub struct MetricsSummary {
    pub total_scopes: usize,
    pub total_metrics: usize,
    pub metric_names: Vec<String>,
}
fn metric_number_to_f64(n: &wasi_metrics::MetricNumber) -> f64 {
    match n {
        wasi_metrics::MetricNumber::F64(v) => *v,
        wasi_metrics::MetricNumber::S64(v) => *v as f64,
        wasi_metrics::MetricNumber::U64(v) => *v as f64,
    }
}
fn metric_data_summary(data: &wasi_metrics::MetricData) -> String {
    match data {
        wasi_metrics::MetricData::F64Gauge(g)
        | wasi_metrics::MetricData::U64Gauge(g)
        | wasi_metrics::MetricData::S64Gauge(g) => format!("gauge({} points)", g.data_points.len()),
        wasi_metrics::MetricData::F64Sum(s)
        | wasi_metrics::MetricData::U64Sum(s)
        | wasi_metrics::MetricData::S64Sum(s) => format!(
            "sum({} points, {})",
            s.data_points.len(),
            if s.is_monotonic {
                "monotonic"
            } else {
                "non-monotonic"
            }
        ),
        wasi_metrics::MetricData::F64Histogram(h)
        | wasi_metrics::MetricData::U64Histogram(h)
        | wasi_metrics::MetricData::S64Histogram(h) => {
            format!("histogram({} points)", h.data_points.len())
        }
        wasi_metrics::MetricData::F64ExponentialHistogram(h)
        | wasi_metrics::MetricData::U64ExponentialHistogram(h)
        | wasi_metrics::MetricData::S64ExponentialHistogram(h) => {
            format!("exp_histogram({} points)", h.data_points.len())
        }
    }
}
pub fn summarize_resource_metrics(metrics: &wasi_metrics::ResourceMetrics) -> MetricsSummary {
    let metric_names = metrics
        .scope_metrics
        .iter()
        .flat_map(|scope| scope.metrics.iter())
        .map(|metric| format!("{}[{}]", metric.name, metric_data_summary(&metric.data)))
        .collect::<Vec<_>>();
    MetricsSummary {
        total_scopes: metrics.scope_metrics.len(),
        total_metrics: metric_names.len(),
        metric_names,
    }
}
type GaugeValue = (String, f64, Vec<(String, String)>);
pub fn extract_gauge_values(metrics: &wasi_metrics::ResourceMetrics) -> Vec<GaugeValue> {
    metrics
        .scope_metrics
        .iter()
        .flat_map(|s| &s.metrics)
        .filter_map(|m| match &m.data {
            wasi_metrics::MetricData::F64Gauge(g)
            | wasi_metrics::MetricData::U64Gauge(g)
            | wasi_metrics::MetricData::S64Gauge(g) => Some(
                g.data_points
                    .iter()
                    .map(|p| {
                        (
                            m.name.clone(),
                            metric_number_to_f64(&p.value),
                            p.attributes
                                .iter()
                                .map(|kv| (kv.key.clone(), kv.value.clone()))
                                .collect(),
                        )
                    })
                    .collect::<Vec<_>>(),
            ),
            _ => None,
        })
        .flatten()
        .collect()
}
type CounterValue = (String, f64, bool, Vec<(String, String)>);
pub fn extract_counter_values(metrics: &wasi_metrics::ResourceMetrics) -> Vec<CounterValue> {
    metrics
        .scope_metrics
        .iter()
        .flat_map(|s| &s.metrics)
        .filter_map(|m| match &m.data {
            wasi_metrics::MetricData::F64Sum(g)
            | wasi_metrics::MetricData::U64Sum(g)
            | wasi_metrics::MetricData::S64Sum(g) => Some(
                g.data_points
                    .iter()
                    .map(|p| {
                        (
                            m.name.clone(),
                            metric_number_to_f64(&p.value),
                            g.is_monotonic,
                            p.attributes
                                .iter()
                                .map(|kv| (kv.key.clone(), kv.value.clone()))
                                .collect(),
                        )
                    })
                    .collect::<Vec<_>>(),
            ),
            _ => None,
        })
        .flatten()
        .collect()
}

fn datetime(
    value: &super::bindings::wasi::clocks::wall_clock::Datetime,
    field: &'static str,
) -> Result<SystemTime, SpanConversionError> {
    if value.nanoseconds >= 1_000_000_000 {
        return Err(SpanConversionError::InvalidField(field));
    }
    UNIX_EPOCH
        .checked_add(Duration::new(value.seconds, value.nanoseconds))
        .ok_or(SpanConversionError::InvalidField(field))
}
fn span_context(value: &WitSpanContext) -> Result<SpanContext, SpanConversionError> {
    if value.trace_id.len() != 32 {
        return Err(SpanConversionError::InvalidField("trace_id"));
    }
    if value.span_id.len() != 16 {
        return Err(SpanConversionError::InvalidField("span_id"));
    }
    let trace_id = TraceId::from_hex(&value.trace_id)
        .map_err(|_| SpanConversionError::InvalidField("trace_id"))?;
    let span_id = SpanId::from_hex(&value.span_id)
        .map_err(|_| SpanConversionError::InvalidField("span_id"))?;
    if trace_id == TraceId::INVALID {
        return Err(SpanConversionError::InvalidField("trace_id"));
    }
    if span_id == SpanId::INVALID {
        return Err(SpanConversionError::InvalidField("span_id"));
    }
    let state = TraceState::from_key_value(
        value
            .trace_state
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str())),
    )
    .map_err(|_| SpanConversionError::InvalidField("trace_state"))?;
    Ok(SpanContext::new(
        trace_id,
        span_id,
        if value.trace_flags.contains(WitTraceFlags::SAMPLED) {
            TraceFlags::SAMPLED
        } else {
            TraceFlags::default()
        },
        value.is_remote,
        state,
    ))
}
fn json_value(key: &str, raw: &str) -> Result<Value, SpanConversionError> {
    let value: serde_json::Value = serde_json::from_str(raw)
        .map_err(|_| SpanConversionError::UnsupportedAttribute(key.to_string()))?;
    match value {
        serde_json::Value::String(v) => Ok(v.into()),
        serde_json::Value::Bool(v) => Ok(v.into()),
        serde_json::Value::Number(v) if v.is_i64() => Ok(v.as_i64().unwrap().into()),
        serde_json::Value::Number(v) if v.is_f64() => Ok(v.as_f64().unwrap().into()),
        serde_json::Value::Array(v) if v.iter().all(serde_json::Value::is_string) => {
            Ok(Value::Array(Array::String(
                v.into_iter()
                    .map(|v| v.as_str().unwrap().to_string().into())
                    .collect(),
            )))
        }
        serde_json::Value::Array(v) if v.iter().all(serde_json::Value::is_boolean) => {
            Ok(Value::Array(Array::Bool(
                v.into_iter().map(|v| v.as_bool().unwrap()).collect(),
            )))
        }
        serde_json::Value::Array(v) if v.iter().all(serde_json::Value::is_i64) => Ok(Value::Array(
            Array::I64(v.into_iter().map(|v| v.as_i64().unwrap()).collect()),
        )),
        serde_json::Value::Array(v) if v.iter().all(serde_json::Value::is_f64) => Ok(Value::Array(
            Array::F64(v.into_iter().map(|v| v.as_f64().unwrap()).collect()),
        )),
        _ => Err(SpanConversionError::UnsupportedAttribute(key.to_string())),
    }
}
fn key_values(
    values: Vec<super::bindings::wasi::otel::types::KeyValue>,
) -> Result<Vec<KeyValue>, SpanConversionError> {
    values
        .into_iter()
        .map(|kv| {
            Ok(KeyValue::new(
                kv.key.clone(),
                json_value(&kv.key, &kv.value)?,
            ))
        })
        .collect()
}
pub fn convert_span_kind(kind: wasi_tracing::SpanKind) -> SpanKind {
    match kind {
        wasi_tracing::SpanKind::Client => SpanKind::Client,
        wasi_tracing::SpanKind::Server => SpanKind::Server,
        wasi_tracing::SpanKind::Producer => SpanKind::Producer,
        wasi_tracing::SpanKind::Consumer => SpanKind::Consumer,
        wasi_tracing::SpanKind::Internal => SpanKind::Internal,
    }
}
pub fn convert_status(status: &wasi_tracing::Status) -> Status {
    match status {
        wasi_tracing::Status::Unset => Status::Unset,
        wasi_tracing::Status::Ok => Status::Ok,
        wasi_tracing::Status::Error(message) => Status::error(message.clone()),
    }
}

pub fn try_into_sdk_span_data(
    span: wasi_tracing::SpanData,
    outer_context: Option<&SpanContext>,
) -> Result<SpanData, SpanConversionError> {
    let context = span_context(&span.span_context)?;
    let parent_span_id = if span.parent_span_id.is_empty() {
        SpanId::INVALID
    } else {
        if span.parent_span_id.len() != 16 {
            return Err(SpanConversionError::InvalidField("parent_span_id"));
        }
        let id = SpanId::from_hex(&span.parent_span_id)
            .map_err(|_| SpanConversionError::InvalidField("parent_span_id"))?;
        if id == SpanId::INVALID {
            return Err(SpanConversionError::InvalidField("parent_span_id"));
        }
        id
    };
    let start_time = datetime(&span.start_time, "start_time")?;
    let end_time = datetime(&span.end_time, "end_time")?;
    if end_time < start_time {
        return Err(SpanConversionError::InvalidField("end_time"));
    }
    let mut events = SpanEvents::default();
    events.events = span
        .events
        .into_iter()
        .map(|event| {
            Ok(Event::new(
                event.name,
                datetime(&event.time, "event_time")?,
                key_values(event.attributes)?,
                0,
            ))
        })
        .collect::<Result<_, SpanConversionError>>()?;
    events.dropped_count = span.dropped_events;
    let mut links = SpanLinks::default();
    links.links = span
        .links
        .into_iter()
        .map(|link| {
            Ok(Link::new(
                span_context(&link.span_context)?,
                key_values(link.attributes)?,
                0,
            ))
        })
        .collect::<Result<_, SpanConversionError>>()?;
    links.dropped_count = span.dropped_links;
    let mut scope = InstrumentationScope::builder(span.instrumentation_scope.name);
    if let Some(version) = span.instrumentation_scope.version {
        scope = scope.with_version(version);
    }
    if let Some(schema_url) = span.instrumentation_scope.schema_url {
        scope = scope.with_schema_url(schema_url);
    }
    scope = scope.with_attributes(key_values(span.instrumentation_scope.attributes)?);
    let parent_span_is_remote = outer_context
        .filter(|outer| {
            outer.is_valid()
                && outer.trace_id() == context.trace_id()
                && outer.span_id() == parent_span_id
        })
        .is_some();
    Ok(SpanData {
        span_context: context,
        parent_span_id,
        parent_span_is_remote,
        span_kind: convert_span_kind(span.span_kind),
        name: span.name.into(),
        start_time,
        end_time,
        attributes: key_values(span.attributes)?,
        dropped_attributes_count: span.dropped_attributes,
        events,
        links,
        status: convert_status(&span.status),
        instrumentation_scope: scope.build(),
    })
}

pub struct SpanSummary {
    pub name: String,
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: String,
    pub kind: String,
    pub attribute_count: usize,
    pub event_count: usize,
    pub link_count: usize,
    pub status: String,
}
pub fn summarize_span_data(span: &wasi_tracing::SpanData) -> SpanSummary {
    SpanSummary {
        name: span.name.clone(),
        trace_id: span.span_context.trace_id.clone(),
        span_id: span.span_context.span_id.clone(),
        parent_span_id: span.parent_span_id.clone(),
        kind: format!("{:?}", span.span_kind),
        attribute_count: span.attributes.len(),
        event_count: span.events.len(),
        link_count: span.links.len(),
        status: format!("{:?}", span.status),
    }
}

#[cfg(test)]
mod tests {
    use super::super::bindings::wasi::clocks::wall_clock::Datetime;
    use super::super::bindings::wasi::otel::types::{InstrumentationScope, KeyValue};
    use super::*;

    fn fixture() -> wasi_tracing::SpanData {
        wasi_tracing::SpanData {
            span_context: WitSpanContext {
                trace_id: "00112233445566778899aabbccddeeff".into(),
                span_id: "0102030405060708".into(),
                trace_flags: WitTraceFlags::SAMPLED,
                is_remote: false,
                trace_state: vec![("vendor".into(), "value".into())],
            },
            parent_span_id: "1112131415161718".into(),
            span_kind: wasi_tracing::SpanKind::Internal,
            name: "guest-child".into(),
            start_time: Datetime {
                seconds: 10,
                nanoseconds: 20,
            },
            end_time: Datetime {
                seconds: 11,
                nanoseconds: 30,
            },
            attributes: vec![KeyValue {
                key: "count".into(),
                value: "42".into(),
            }],
            events: vec![wasi_tracing::Event {
                name: "event".into(),
                time: Datetime {
                    seconds: 10,
                    nanoseconds: 25,
                },
                attributes: vec![],
            }],
            links: vec![wasi_tracing::Link {
                span_context: WitSpanContext {
                    trace_id: "ffeeddccbbaa99887766554433221100".into(),
                    span_id: "1817161514131211".into(),
                    trace_flags: WitTraceFlags::empty(),
                    is_remote: true,
                    trace_state: vec![],
                },
                attributes: vec![],
            }],
            status: wasi_tracing::Status::Error("failed".into()),
            instrumentation_scope: InstrumentationScope {
                name: "guest-sdk".into(),
                version: Some("1.2.3".into()),
                schema_url: Some("https://example.test/schema".into()),
                attributes: vec![],
            },
            dropped_attributes: 2,
            dropped_events: 3,
            dropped_links: 4,
        }
    }

    #[test]
    fn existing_wit_payload_converts_without_replacing_graph_data() {
        let converted = try_into_sdk_span_data(fixture(), None).unwrap();
        assert_eq!(
            converted.span_context.trace_id().to_string(),
            "00112233445566778899aabbccddeeff"
        );
        assert_eq!(
            converted.span_context.span_id().to_string(),
            "0102030405060708"
        );
        assert_eq!(converted.parent_span_id.to_string(), "1112131415161718");
        assert!(converted.span_context.is_sampled());
        assert_eq!(
            converted.span_context.trace_state().header(),
            "vendor=value"
        );
        assert_eq!(
            converted.events[0].timestamp,
            UNIX_EPOCH + Duration::new(10, 25)
        );
        assert_eq!(
            converted.links[0].span_context.span_id().to_string(),
            "1817161514131211"
        );
        assert_eq!(converted.dropped_attributes_count, 2);
        assert_eq!(converted.events.dropped_count, 3);
        assert_eq!(converted.links.dropped_count, 4);
        assert_eq!(converted.instrumentation_scope.name(), "guest-sdk");
    }

    #[test]
    fn invalid_ids_are_rejected() {
        for (field, value, expected) in [
            ("trace", "0011", "trace_id"),
            ("trace", "00112233445566778899aabbccddeeff00", "trace_id"),
            ("trace", "00112233445566778899aabbccddeefg", "trace_id"),
            ("trace", "00000000000000000000000000000000", "trace_id"),
            ("span", "0102", "span_id"),
            ("span", "010203040506070800", "span_id"),
            ("span", "010203040506070g", "span_id"),
            ("span", "0000000000000000", "span_id"),
            ("parent", "1112", "parent_span_id"),
            ("parent", "111213141516171800", "parent_span_id"),
            ("parent", "111213141516171g", "parent_span_id"),
            ("parent", "0000000000000000", "parent_span_id"),
        ] {
            let mut span = fixture();
            match field {
                "trace" => span.span_context.trace_id = value.into(),
                "span" => span.span_context.span_id = value.into(),
                "parent" => span.parent_span_id = value.into(),
                _ => unreachable!(),
            }
            assert_eq!(
                try_into_sdk_span_data(span, None).unwrap_err(),
                SpanConversionError::InvalidField(expected),
                "{field} value {value}"
            );
        }
    }

    #[test]
    fn invalid_timestamps_are_rejected() {
        let mut span = fixture();
        span.start_time.nanoseconds = 1_000_000_000;
        assert_eq!(
            try_into_sdk_span_data(span, None).unwrap_err(),
            SpanConversionError::InvalidField("start_time")
        );
    }

    #[test]
    fn parent_is_remote_only_when_it_matches_the_outer_host_span() {
        let matching = SpanContext::new(
            TraceId::from_hex("00112233445566778899aabbccddeeff").unwrap(),
            SpanId::from_hex("1112131415161718").unwrap(),
            TraceFlags::SAMPLED,
            false,
            TraceState::default(),
        );
        assert!(
            try_into_sdk_span_data(fixture(), Some(&matching))
                .unwrap()
                .parent_span_is_remote
        );

        let different_span = SpanContext::new(
            matching.trace_id(),
            SpanId::from_hex("9999999999999999").unwrap(),
            TraceFlags::SAMPLED,
            true,
            TraceState::default(),
        );
        assert!(
            !try_into_sdk_span_data(fixture(), Some(&different_span))
                .unwrap()
                .parent_span_is_remote
        );
        let different_trace = SpanContext::new(
            TraceId::from_hex("ffeeddccbbaa99887766554433221100").unwrap(),
            matching.span_id(),
            TraceFlags::SAMPLED,
            true,
            TraceState::default(),
        );
        assert!(
            !try_into_sdk_span_data(fixture(), Some(&different_trace))
                .unwrap()
                .parent_span_is_remote
        );
        assert!(
            !try_into_sdk_span_data(fixture(), None)
                .unwrap()
                .parent_span_is_remote
        );
    }
}
