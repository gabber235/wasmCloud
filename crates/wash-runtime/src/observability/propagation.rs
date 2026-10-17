use anyhow::Context as _;
use opentelemetry::Context as OtelContext;
use opentelemetry::propagation::{Extractor, Injector, TextMapPropagator};
use opentelemetry::trace::TraceContextExt;
use opentelemetry_sdk::propagation::TraceContextPropagator;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PropagationContext {
    pub traceparent: String,
    pub tracestate: Option<String>,
}

#[derive(Debug)]
struct PropagationCarrier(PropagationContext);

impl Extractor for PropagationCarrier {
    fn get(&self, key: &str) -> Option<&str> {
        match key {
            "traceparent" => Some(&self.0.traceparent),
            "tracestate" => self.0.tracestate.as_deref(),
            _ => None,
        }
    }

    fn keys(&self) -> Vec<&str> {
        vec!["traceparent", "tracestate"]
    }
}

impl Injector for PropagationCarrier {
    fn set(&mut self, key: &str, value: String) {
        match key {
            "traceparent" => self.0.traceparent = value,
            "tracestate" => self.0.tracestate = Some(value),
            _ => {}
        }
    }
}

pub fn context_from_propagation(value: &PropagationContext) -> anyhow::Result<OtelContext> {
    if let Some(state) = &value.tracestate {
        let _: opentelemetry::trace::TraceState =
            state.parse().context("invalid W3C tracestate")?;
    }
    let context = TraceContextPropagator::new().extract(&PropagationCarrier(value.clone()));
    anyhow::ensure!(
        context.span().span_context().is_valid(),
        "invalid W3C trace context"
    );
    Ok(context)
}

pub fn inject_context(context: &OtelContext) -> PropagationContext {
    let mut carrier = PropagationCarrier(PropagationContext {
        traceparent: String::new(),
        tracestate: None,
    });
    TraceContextPropagator::new().inject_context(context, &mut carrier);
    carrier.0
}
