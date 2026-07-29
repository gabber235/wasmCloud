mod in_memory;
#[cfg(feature = "wasm_component_model_implements")]
mod multiplexed;
mod nats;

pub use in_memory::{
    HostMessage, InMemoryMessaging, InMemoryMessagingDriver, MessageOrigin, MessagingError,
    ObservationReceiver, ObservedMessage, ObservedOperation, ResponderReceiver, ResponderRequest,
    TraceContext as InMemoryTraceContext,
};
#[cfg(feature = "wasm_component_model_implements")]
pub use multiplexed::{
    BrokerMessage, InMemoryMsgBackend, InMemoryMsgProvider, MsgBackend, MsgId, MsgProvider,
    MultiplexedMessaging, NatsMsgBackend, NatsMsgProvider, TraceContext,
};
pub use nats::NatsMessaging;

pub(crate) fn record_messaging_error(span: &tracing::Span, slug: &'static str, message: &str) {
    let message: String = message
        .chars()
        .filter(|character| !character.is_control() || *character == ' ')
        .take(1024)
        .collect();
    span.record("otel.status_code", "ERROR");
    span.record("error", true);
    span.record("error.type", slug);
    span.record("exception.message", message);
    span.record("exception.slug", slug);
    span.record("messaging.operation.outcome", "failure");
}

/// Returns `true` if the world exports the `wasmcloud:messaging/handler`
/// interface at any version. Matches via [`WitInterface::contains`] rather
/// than set equality, so an exported `handler@0.2.x` is recognized no matter
/// which exact version the component was built against.
pub(crate) fn exports_messaging_handler(world: &crate::wit::WitWorld) -> bool {
    let handler = crate::wit::WitInterface::from("wasmcloud:messaging/handler");
    world.exports.iter().any(|e| e.contains(&handler))
}

/// Parses a comma-separated `subscriptions` config value into trimmed,
/// non-empty subjects. Shared by the in-memory and NATS backends so they
/// agree on how a configured subscription string maps to subjects.
pub(crate) fn parse_subscriptions(raw: Option<&str>) -> Vec<String> {
    raw.map(|s| {
        s.split(',')
            .map(|t| t.trim().to_string())
            .filter(|t| !t.is_empty())
            .collect()
    })
    .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::{exports_messaging_handler, parse_subscriptions, record_messaging_error};
    use crate::wit::{WitInterface, WitWorld};
    use std::collections::{HashMap, HashSet};
    use std::sync::{Arc, Mutex};
    use tracing::field::{Field, Visit};
    use tracing_subscriber::layer::{Context, SubscriberExt};
    use tracing_subscriber::{Layer, Registry};

    #[derive(Default)]
    struct FieldVisitor(HashMap<String, String>);

    impl Visit for FieldVisitor {
        fn record_bool(&mut self, field: &Field, value: bool) {
            self.0.insert(field.name().into(), value.to_string());
        }

        fn record_str(&mut self, field: &Field, value: &str) {
            self.0.insert(field.name().into(), value.into());
        }

        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            self.0.insert(field.name().into(), format!("{value:?}"));
        }
    }

    struct RecordLayer(Arc<Mutex<HashMap<String, String>>>);

    impl<S: tracing::Subscriber> Layer<S> for RecordLayer {
        fn on_record(
            &self,
            _id: &tracing::span::Id,
            values: &tracing::span::Record<'_>,
            _ctx: Context<'_, S>,
        ) {
            let mut visitor = FieldVisitor::default();
            values.record(&mut visitor);
            self.0.lock().unwrap().extend(visitor.0);
        }
    }

    #[test]
    fn timeout_records_error_status_and_semantic_slug() {
        let fields = Arc::new(Mutex::new(HashMap::new()));
        let subscriber = Registry::default().with(RecordLayer(fields.clone()));
        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!(
                "request",
                otel.status_code = tracing::field::Empty,
                error = tracing::field::Empty,
                error.type = tracing::field::Empty,
                exception.message = tracing::field::Empty,
                exception.slug = tracing::field::Empty,
                messaging.operation.outcome = tracing::field::Empty,
            );
            record_messaging_error(
                &span,
                "messaging-request-timeout",
                "request timed out after 10ms",
            );
        });

        let fields = fields.lock().unwrap();
        assert_eq!(fields["otel.status_code"], "ERROR");
        assert_eq!(fields["error"], "true");
        assert_eq!(fields["error.type"], "messaging-request-timeout");
        assert_eq!(fields["exception.slug"], "messaging-request-timeout");
        assert_eq!(fields["messaging.operation.outcome"], "failure");
    }

    #[test]
    fn recognizes_exported_handler_at_any_version() {
        for export in [
            "wasmcloud:messaging/handler",
            "wasmcloud:messaging/handler@0.2.0",
            "wasmcloud:messaging/handler@0.2.2",
        ] {
            let world = WitWorld {
                imports: HashSet::new(),
                exports: HashSet::from([WitInterface::from(export)]),
            };
            assert!(exports_messaging_handler(&world), "should match {export}");
        }
    }

    #[test]
    fn ignores_non_handler_worlds() {
        // Importing the handler is not exporting it
        let importer = WitWorld {
            imports: HashSet::from([WitInterface::from("wasmcloud:messaging/handler@0.2.0")]),
            exports: HashSet::new(),
        };
        assert!(!exports_messaging_handler(&importer));

        // Exporting other messaging interfaces does not count
        let consumer = WitWorld {
            imports: HashSet::new(),
            exports: HashSet::from([WitInterface::from("wasmcloud:messaging/consumer,types")]),
        };
        assert!(!exports_messaging_handler(&consumer));
    }

    #[test]
    fn parses_single_subject() {
        assert_eq!(
            parse_subscriptions(Some("tasks.task-worker")),
            vec!["tasks.task-worker".to_string()]
        );
    }

    #[test]
    fn parses_multiple_subjects() {
        assert_eq!(
            parse_subscriptions(Some("a,b,c")),
            vec!["a".to_string(), "b".to_string(), "c".to_string()]
        );
    }

    #[test]
    fn trims_surrounding_whitespace_and_drops_empties() {
        assert_eq!(
            parse_subscriptions(Some(" tasks.leet , tasks.reverse ,, ")),
            vec!["tasks.leet".to_string(), "tasks.reverse".to_string()]
        );
        assert!(parse_subscriptions(None).is_empty());
    }
}
