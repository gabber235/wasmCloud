//! Trace context at the NATS transport boundary.

use tracing_opentelemetry::OpenTelemetrySpanExt as _;

pub(super) fn parent(headers: &async_nats::HeaderMap) -> Option<opentelemetry::Context> {
    let value = crate::observability::PropagationContext {
        traceparent: headers.get("traceparent")?.as_str().to_owned(),
        tracestate: headers.get("tracestate").map(|v| v.as_str().to_owned()),
    };
    match crate::observability::context_from_propagation(&value) {
        Ok(context) => Some(context),
        Err(error) => {
            tracing::warn!(%error, "invalid NATS trace context");
            None
        }
    }
}

pub(super) fn set_parent(span: &tracing::Span, parent: Option<opentelemetry::Context>) {
    if let Some(parent) = parent {
        if let Err(error) = span.set_parent(parent) {
            tracing::warn!(%error, "cannot attach NATS trace context");
        }
    }
}

pub(super) fn producer(
    operation: &'static str,
    subject: &str,
    headers: &mut Option<async_nats::HeaderMap>,
) -> tracing::Span {
    let span = tracing::info_span!(
        "nats_send",
        otel.kind = "producer",
        messaging.operation.name = operation,
        messaging.destination.name = subject
    );
    set_parent(&span, headers.as_ref().and_then(parent));
    let context = crate::observability::inject_context(&span.context());
    if !context.traceparent.is_empty() {
        let headers = headers.get_or_insert_with(async_nats::HeaderMap::new);
        headers.insert("traceparent", context.traceparent.as_str());
        headers.remove("tracestate");
        if let Some(state) = context.tracestate {
            headers.insert("tracestate", state.as_str());
        }
    }
    span
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::trace::TraceContextExt as _;

    #[test]
    fn extracts_remote_context_and_rejects_malformed_headers() {
        let mut headers = async_nats::HeaderMap::new();
        assert!(parent(&headers).is_none());
        headers.insert(
            "traceparent",
            "00-11111111111111111111111111111111-2222222222222222-01",
        );
        headers.insert("tracestate", "vendor=value");
        let context = parent(&headers).expect("valid context");
        let span = context.span();
        assert_eq!(
            span.span_context().trace_id().to_string(),
            "11111111111111111111111111111111"
        );
        assert!(span.span_context().is_remote());
        assert_eq!(span.span_context().trace_state().header(), "vendor=value");
        headers.insert("tracestate", "invalid");
        assert!(parent(&headers).is_none());
        headers.insert("traceparent", "invalid");
        assert!(parent(&headers).is_none());
    }
}
