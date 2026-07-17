use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use crate::engine::ctx::{ActiveCtx, SharedCtx, extract_active_ctx};
use crate::engine::workload::{ResolvedWorkload, UnresolvedWorkload, WorkloadItem};
use crate::observability::{Meters, PropagationContext, context_from_propagation, inject_context};
use crate::plugin::{HostPlugin, WitInterfaces};
use crate::wit::{WitInterface, WitWorld};
use anyhow::Context;
use opentelemetry::KeyValue;
use tokio::sync::{Notify, RwLock, oneshot};
use tracing::{Instrument, debug, instrument, trace, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use wasmtime::component::Accessor;

const PLUGIN_MESSAGING_MEMORY_ID: &str = "wasmcloud-messaging-memory";
const MAX_QUEUE_SIZE: usize = 10000;

fn propagation_context(
    value: bindings::wasmcloud::observability::propagation::TraceContext,
) -> PropagationContext {
    PropagationContext {
        traceparent: value.traceparent,
        tracestate: value.tracestate,
    }
}

fn producer_span(
    operation: &'static str,
    destination: &str,
    payload_size: usize,
    parent: Option<bindings::wasmcloud::observability::propagation::TraceContext>,
) -> tracing::Span {
    let span = tracing::info_span!("wasmcloud.messaging.produce", otel.kind = "producer", messaging.system = "in-memory", messaging.operation = operation, messaging.destination.name = destination, messaging.message.body.size = payload_size, messaging.operation.outcome = tracing::field::Empty, error.type = tracing::field::Empty, otel.propagation.error = tracing::field::Empty, exception.slug = tracing::field::Empty, exception.message = tracing::field::Empty, otel.status_code = tracing::field::Empty, error = tracing::field::Empty);
    if let Some(parent) = parent {
        match context_from_propagation(&propagation_context(parent)) {
            Ok(parent) => {
                let _ = span.set_parent(parent);
            }
            Err(_) => {
                span.record("otel.propagation.error", true);
                span.record("exception.slug", "messaging-invalid-trace-context");
            }
        }
    }
    span
}

fn routed_message(message: types::BrokerMessage, span: &tracing::Span) -> RoutedMessage {
    let propagation = inject_context(&span.context());
    RoutedMessage {
        message,
        propagation: (!propagation.traceparent.is_empty()).then_some(propagation),
    }
}

/// A component's message inbox, shared between the publisher side
/// (`route_to_subscribers`) and the component's processing task.
#[derive(Clone)]
struct RoutedMessage {
    message: types::BrokerMessage,
    propagation: Option<PropagationContext>,
}

type Inbox = Arc<RwLock<VecDeque<RoutedMessage>>>;

mod bindings {
    crate::wasmtime::component::bindgen!({
        world: "messaging",
        imports: { default: store | async | trappable | tracing },
        exports: { default: async | tracing },
    });
}

use bindings::wasmcloud::messaging::consumer::{Host, HostWithStore};
use bindings::wasmcloud::messaging::types;

use crate::plugin::WorkloadTracker;

/// Per-workload tracking data. Holds the reply-routing table shared by every
/// component in the workload; message delivery itself is per-component (see
/// [`ComponentData`]).
#[derive(Default)]
struct WorkloadData {
    pending_requests: Arc<RwLock<HashMap<String, oneshot::Sender<RoutedMessage>>>>,
}

type PendingRequests = Arc<RwLock<HashMap<String, oneshot::Sender<RoutedMessage>>>>;

struct PendingRequestGuard {
    key: String,
    pending_requests: PendingRequests,
}

impl PendingRequestGuard {
    fn new(key: String, pending_requests: PendingRequests) -> Self {
        Self {
            key,
            pending_requests,
        }
    }
}

impl Drop for PendingRequestGuard {
    fn drop(&mut self) {
        if let Ok(mut pending_requests) = self.pending_requests.try_write() {
            pending_requests.remove(&self.key);
            return;
        }

        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let key = self.key.clone();
        let pending_requests = self.pending_requests.clone();
        runtime.spawn(async move {
            pending_requests.write().await.remove(&key);
        });
    }
}

/// Per-component tracking data. Each handler component has its own subject
/// subscriptions and inbox queue, so a published message is delivered only to
/// the components whose subscriptions match its subject.
struct ComponentData {
    cancel_token: tokio_util::sync::CancellationToken,
    task_handle: Option<tokio::task::JoinHandle<()>>,
    /// Subjects this component subscribes to (NATS tokens: `*` one token,
    /// `>` one or more trailing tokens). Empty means "receive everything",
    /// preserving the single-handler behavior of earlier versions.
    subscriptions: Vec<String>,
    /// This component's inbox. `publish`/`request` push matching messages
    /// here; the component's processing task drains it.
    inbox: Inbox,
    notify: Arc<Notify>,
}

/// Returns whether `subject` matches NATS subscription `pattern`, where `*`
/// matches exactly one token and `>` matches one or more trailing tokens.
fn subject_matches(pattern: &str, subject: &str) -> bool {
    let mut subject_tokens = subject.split('.');
    let mut pattern_tokens = pattern.split('.').peekable();
    while let Some(pat) = pattern_tokens.next() {
        if pat == ">" {
            // `>` is only valid as the final token and matches one or more
            // remaining subject tokens.
            return pattern_tokens.peek().is_none() && subject_tokens.next().is_some();
        }
        match subject_tokens.next() {
            Some(sub) if pat == "*" || pat == sub => continue,
            _ => return false,
        }
    }
    // Every pattern token matched; the subject must be fully consumed too.
    subject_tokens.next().is_none()
}

/// Whether any of a component's `subscriptions` match `subject`. An empty
/// subscription list matches everything (single-handler back-compat).
fn subscriptions_match(subscriptions: &[String], subject: &str) -> bool {
    subscriptions.is_empty() || subscriptions.iter().any(|s| subject_matches(s, subject))
}

/// Pushes `msg` onto the inbox of every component in `workload_id` whose
/// subscriptions match its subject, waking each one. Returns an error only if
/// the workload is untracked or a target inbox is full.
async fn route_to_subscribers(
    plugin: &InMemoryMessaging,
    workload_id: &str,
    msg: &RoutedMessage,
) -> Result<(), String> {
    let targets: Vec<(Inbox, Arc<Notify>)> = {
        let lock = plugin.tracker.read().await;
        let Some(item) = lock.workloads.get(workload_id) else {
            return Err("workload state not found".to_string());
        };
        item.components
            .values()
            .filter(|c| subscriptions_match(&c.subscriptions, &msg.message.subject))
            .map(|c| (c.inbox.clone(), c.notify.clone()))
            .collect()
    };

    for (inbox, notify) in targets {
        {
            let mut queue = inbox.write().await;
            if queue.len() >= MAX_QUEUE_SIZE {
                return Err("message queue full".to_string());
            }
            queue.push_back(msg.clone());
        }
        notify.notify_one();
    }
    Ok(())
}

/// In-memory messaging plugin for wash dev and mocking scenarios.
///
/// Messages published by a workload are only handled within that same workload
/// (per-workload isolation). This is useful for testing and development where
/// a full NATS server is not needed.
#[derive(Clone)]
pub struct InMemoryMessaging {
    tracker: Arc<RwLock<WorkloadTracker<WorkloadData, ComponentData>>>,
    meters: Arc<RwLock<Meters>>,
}

impl InMemoryMessaging {
    pub fn new() -> Self {
        Self {
            tracker: Arc::new(RwLock::new(WorkloadTracker::default())),
            meters: Default::default(),
        }
    }

    /// Route a message to a workload's subscribers, exactly as an inbound publish
    /// would: it is enqueued to every component whose subscriptions match
    /// `subject`, then processed by that component's receive loop. Lets a host or
    /// test inject a message without a component-side `consumer.publish`.
    pub async fn publish(
        &self,
        workload_id: &str,
        subject: &str,
        body: Vec<u8>,
    ) -> Result<(), String> {
        route_to_subscribers(
            self,
            workload_id,
            &types::BrokerMessage {
                subject: subject.to_string(),
                reply_to: None,
                body,
            },
        )
        .await
    }
}

impl Default for InMemoryMessaging {
    fn default() -> Self {
        Self::new()
    }
}

fn plugin_and_workload<T>(
    store: &Accessor<T, SharedCtx>,
) -> wasmtime::Result<(Arc<InMemoryMessaging>, String)> {
    store.with(|mut access| {
        let view = access.get();
        Ok((
            view.try_get_plugin::<InMemoryMessaging>(PLUGIN_MESSAGING_MEMORY_ID)?,
            view.workload_id.to_string(),
        ))
    })
}

impl Host for ActiveCtx<'_> {}

impl<T> HostWithStore<T> for SharedCtx {
    #[instrument(name = "wasmcloud.messaging.request", skip_all, fields(subject = %subject, timeout_ms))]
    async fn request(
        store: &Accessor<T, Self>,
        subject: String,
        body: Vec<u8>,
        timeout_ms: u32,
        parent_context: Option<bindings::wasmcloud::observability::propagation::TraceContext>,
    ) -> wasmtime::Result<Result<types::BrokerMessage, String>> {
        let (plugin, workload_id) = plugin_and_workload(store)?;
        let span = producer_span("request", &subject, body.len(), parent_context);

        let pending_requests = {
            let lock = plugin.tracker.read().await;
            match lock.get_workload_data(&workload_id) {
                Some(data) => data.pending_requests.clone(),
                None => wasmtime::bail!("workload state not found"),
            }
        };

        // Generate a unique reply-to subject
        let reply_to = format!("_INBOX.{}", uuid::Uuid::new_v4());

        // Create a oneshot channel for the response
        let (tx, rx) = oneshot::channel();

        // Register the pending request
        {
            let mut lock = pending_requests.write().await;
            lock.insert(reply_to.clone(), tx);
        }
        let _pending_request_guard =
            PendingRequestGuard::new(reply_to.clone(), pending_requests.clone());

        // Create the request message with reply_to set
        let msg = routed_message(
            types::BrokerMessage {
                subject,
                reply_to: Some(reply_to.clone()),
                body,
            },
            &span,
        );

        debug!(subject = %msg.message.subject, reply_to = %msg.message.reply_to.as_deref().unwrap_or("<none>"), "Sending request");
        // Route the request to subscribers of its subject.
        if let Err(error) = route_to_subscribers(&plugin, &workload_id, &msg).await {
            super::record_messaging_error(&span, "messaging-broker-failed", &error);
            return Ok(Err(error));
        }

        // Wait for the response with timeout
        let timeout_duration = std::time::Duration::from_millis(timeout_ms as u64);
        match tokio::time::timeout(timeout_duration, rx).await {
            Ok(Ok(response)) => {
                span.record("messaging.operation.outcome", "success");
                Ok(Ok(response.message))
            }
            Ok(Err(_)) => {
                // Channel was dropped without sending
                warn!("request channel closed without response");
                super::record_messaging_error(
                    &span,
                    "messaging-broker-failed",
                    "request channel closed without response",
                );
                Ok(Err("request channel closed without response".to_string()))
            }
            Err(_) => {
                warn!("request timed out after {timeout_ms}ms");
                super::record_messaging_error(
                    &span,
                    "messaging-request-timeout",
                    &format!("request timed out after {timeout_ms}ms"),
                );
                Ok(Err(format!("request timed out after {timeout_ms}ms")))
            }
        }
    }

    #[instrument(name = "wasmcloud.messaging.publish", skip_all, fields(subject = %msg.subject, reply_to = %msg.reply_to.as_deref().unwrap_or("<none>")))]
    async fn publish(
        store: &Accessor<T, Self>,
        msg: types::BrokerMessage,
        parent_context: Option<bindings::wasmcloud::observability::propagation::TraceContext>,
    ) -> wasmtime::Result<Result<(), String>> {
        let (plugin, workload_id) = plugin_and_workload(store)?;
        let span = producer_span("publish", &msg.subject, msg.body.len(), parent_context);
        let msg = routed_message(msg, &span);
        let pending_requests = {
            let lock = plugin.tracker.read().await;
            match lock.get_workload_data(&workload_id) {
                Some(data) => data.pending_requests.clone(),
                None => wasmtime::bail!("workload state not found"),
            }
        };

        {
            let mut lock = pending_requests.write().await;
            // Check if this is a reply to a pending request. Reply subjects
            // (`_INBOX.*`) are routed here, not to subscribers.
            if let Some(sender) = lock.remove(&msg.message.subject) {
                debug!(subject = %msg.message.subject, reply_to = %msg.message.reply_to.as_deref().unwrap_or("<none>"), "Responding message");
                // This is a response to a request - send it via the oneshot channel
                let _ = sender.send(msg);
                span.record("messaging.operation.outcome", "success");
                return Ok(Ok(()));
            }
        }

        debug!(subject = %msg.message.subject, reply_to = %msg.message.reply_to.as_deref().unwrap_or("<none>"), "Publishing message");

        // Regular publish - deliver to every subscriber of this subject.
        match route_to_subscribers(&plugin, &workload_id, &msg).await {
            Ok(()) => {
                span.record("messaging.operation.outcome", "success");
                Ok(Ok(()))
            }
            Err(error) => {
                super::record_messaging_error(&span, "messaging-broker-failed", &error);
                Ok(Err(error))
            }
        }
    }
}

impl<'a> types::Host for ActiveCtx<'a> {}

#[async_trait::async_trait]
impl HostPlugin for InMemoryMessaging {
    fn id(&self) -> &'static str {
        PLUGIN_MESSAGING_MEMORY_ID
    }

    fn world(&self) -> WitWorld {
        WitWorld {
            imports: HashSet::from([WitInterface::from(
                "wasmcloud:messaging/consumer,types@0.4.0",
            )]),
            exports: HashSet::from([WitInterface::from("wasmcloud:messaging/handler@0.4.0")]),
        }
    }

    async fn inject_meters(&self, meters: &Meters) {
        *self.meters.write().await = meters.clone();
    }

    async fn on_workload_bind(
        &self,
        workload: &UnresolvedWorkload,
        interfaces: WitInterfaces<'_>,
    ) -> anyhow::Result<()> {
        if !interfaces.contains("wasmcloud", "messaging", &[]) {
            return Ok(());
        }

        self.tracker
            .write()
            .await
            .add_unresolved_workload(workload, WorkloadData::default());
        Ok(())
    }

    async fn on_workload_item_bind<'a>(
        &self,
        component_handle: &mut WorkloadItem<'a>,
        interfaces: WitInterfaces<'_>,
    ) -> anyhow::Result<()> {
        if !interfaces.contains("wasmcloud", "messaging", &[]) {
            return Ok(());
        }

        bindings::wasmcloud::messaging::types::add_to_linker::<_, SharedCtx>(
            component_handle.linker(),
            extract_active_ctx,
        )?;
        bindings::wasmcloud::messaging::consumer::add_to_linker::<_, SharedCtx>(
            component_handle.linker(),
            extract_active_ctx,
        )?;

        // Per-component subscriptions come from this component's
        // `LocalResources.config` (set via `dev.components[].config` or a
        // WorkloadDeployment), so workers in one workload can subscribe to
        // different subjects.
        let subscriptions = super::parse_subscriptions(
            component_handle
                .local_resources()
                .config
                .get("subscriptions")
                .map(String::as_str),
        );

        // Track a handler component OR a long-lived handler service:
        // `WorkloadItem` derefs to the underlying metadata for both, so the
        // subscriber loop is set up either way (and its receive loop delivers to
        // the running service when one is registered).
        if super::exports_messaging_handler(&component_handle.world()) {
            debug!(?subscriptions, "Tracking component in in-memory messaging");
            self.tracker.write().await.add_component(
                component_handle,
                ComponentData {
                    cancel_token: tokio_util::sync::CancellationToken::new(),
                    task_handle: None,
                    subscriptions,
                    inbox: Arc::default(),
                    notify: Arc::new(Notify::new()),
                },
            );
        }

        Ok(())
    }

    async fn on_workload_resolved(
        &self,
        workload: &ResolvedWorkload,
        component_id: &str,
    ) -> anyhow::Result<()> {
        let (inbox, notify, cancel_token) = {
            let lock = self.tracker.read().await;
            match lock.get_component_data(component_id) {
                Some(data) => (
                    data.inbox.clone(),
                    data.notify.clone(),
                    data.cancel_token.clone(),
                ),
                None => return Ok(()),
            }
        };

        // A long-lived handler service has no per-component instance to
        // pre-instantiate; its receive loop delivers to the running service
        // instead. Only components get a `MessagingPre` for per-message work.
        let pre = match workload.instantiate_pre(component_id).await {
            Ok(instance_pre) => Some(
                bindings::MessagingPre::new(instance_pre)
                    .map_err(anyhow::Error::from)
                    .context("failed to instantiate messaging pre")?,
            ),
            Err(e) => {
                trace!(component_id, error = %e, "no per-message instance (long-lived service); messages delivered to the service");
                None
            }
        };

        let workload = workload.clone();
        let component_id = component_id.to_string();

        debug!("Spawning messaging processor for component {component_id}");

        // Spawn the message processing task
        let task_component_id = component_id.clone();
        let fuel_meter = self.meters.read().await.fuel_consumption.clone();

        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        break;
                    }
                    _ = notify.notified() => {
                        // Drain every message queued since the last wakeup, so a
                        // coalesced notification can't strand a message.
                        loop {
                        let msg = inbox.write().await.pop_front();

                        let Some(msg) = msg else {
                            break;
                        };

                        let parent = msg.propagation.as_ref().and_then(|context| context_from_propagation(context).ok());
                        let invalid_parent = msg.propagation.as_ref().is_some_and(|context| context_from_propagation(context).is_err());
                        let msg = msg.message;
                        debug!(subject = %msg.subject, reply_to = %msg.reply_to.as_deref().unwrap_or("<none>"), "Processing message");

                        let span = tracing::info_span!(
                            "wasmcloud.messaging.consume",
                            otel.kind = "consumer",
                            messaging.system = "in-memory",
                            messaging.operation = "process",
                            messaging.destination.name = %msg.subject,
                            messaging.message.body.size = msg.body.len(),
                            messaging.operation.outcome = tracing::field::Empty,
                            error.type = tracing::field::Empty,
                            otel.propagation.error = invalid_parent,
                            exception.slug = tracing::field::Empty,
                            exception.message = tracing::field::Empty,
                            otel.status_code = tracing::field::Empty,
                            error = tracing::field::Empty,
                        );
                        if let Some(parent) = parent {
                            let _ = span.set_parent(parent);
                        }
                        if invalid_parent {
                            span.record("exception.slug", "messaging-invalid-trace-context");
                        }

                        // If this workload runs a long-lived trigger service for
                        // messaging, deliver to it (preserving its in-memory
                        // state) rather than instantiating a component per message.
                        if workload
                            .http_handler()
                            .has_trigger_service_messaging(workload.id())
                            .await
                        {
                            let broker = crate::host::trigger_service::BrokerMessage {
                                subject: msg.subject.clone(),
                                body: msg.body.clone(),
                                reply_to: msg.reply_to.clone(),
                            };
                            let result = workload
                                .http_handler()
                                .deliver_trigger_service_message(workload.id(), broker)
                                .instrument(span.clone())
                                .await;
                            match result {
                                Ok(Ok(())) => {
                                    span.record("messaging.operation.outcome", "success");
                                    debug!(subject = %msg.subject, "trigger service handled message");
                                }
                                Ok(Err(e)) => {
                                    super::record_messaging_error(&span, "messaging-handler-rejected", &e);
                                    warn!(subject = %msg.subject, error = %e, "trigger service message handler returned error")
                                }
                                Err(e) => {
                                    super::record_messaging_error(&span, "messaging-consumer-delivery-failed", &e.to_string());
                                    warn!(subject = %msg.subject, error = %e, "failed to deliver message to trigger service")
                                }
                            }
                            continue;
                        }

                        let Some(pre) = &pre else {
                            warn!(
                                subject = %msg.subject,
                                component_id = %component_id,
                                "no trigger service registered and no per-message instance; dropping message"
                            );
                            continue;
                        };
                        let mut store = match workload.new_store(&component_id).await {
                            Err(error) => {
                                super::record_messaging_error(&span, "messaging-consumer-setup-failed", &error.to_string());
                                warn!("failed to create store for component {component_id}: {error}");
                                continue;
                            }
                            Ok(store) => store,
                        };

                        let proxy = match pre.instantiate_async(&mut store).await {
                            Err(error) => {
                                super::record_messaging_error(&span, "messaging-consumer-setup-failed", &error.to_string());
                                warn!("failed to instantiate component {component_id}: {error}");
                                continue;
                            }
                            Ok(proxy) => proxy,
                        };

                        let fuel_meter = fuel_meter.clone();

                        tokio::spawn(async move {
                            let handler_span = span.clone();
                            let result = fuel_meter.observe(
                                &[
                                    KeyValue::new("plugin", PLUGIN_MESSAGING_MEMORY_ID),
                                    KeyValue::new("subject", msg.subject.to_string()),
                                ],
                                &mut store,
                                async move |store| {
                                    let call = store
                                        .run_concurrent(async move |accessor| {
                                            proxy
                                                .wasmcloud_messaging_handler()
                                                .call_handle_message(accessor, msg)
                                                .await
                                        })
                                        .instrument(handler_span)
                                        .await
                                        .map_err(anyhow::Error::from)?;

                                    call.map_err(anyhow::Error::from)
                                }
                            ).await;

                            match result {
                                Ok(Ok(())) => { span.record("messaging.operation.outcome", "success"); debug!("message handled successfully"); },
                                Ok(Err(message)) => {
                                    super::record_messaging_error(&span, "messaging-handler-rejected", &message);
                                    warn!(error = %message, "handler rejected message");
                                }
                                Err(error) => {
                                    super::record_messaging_error(&span, "messaging-handler-rejected", &error.to_string());
                                    warn!(error = %error, "handler invocation failed");
                                }
                            };
                        });
                        }
                    }
                }
            }
        });

        // Store the task handle for tracking panics and cleanup
        {
            let mut lock = self.tracker.write().await;
            if let Some(data) = lock.get_component_data_mut(&task_component_id) {
                data.task_handle = Some(handle);
            }
        }

        Ok(())
    }

    async fn on_workload_unbind(
        &self,
        workload_id: &str,
        _interfaces: WitInterfaces<'_>,
    ) -> anyhow::Result<()> {
        // Clean up tracker
        let workload_cleanup = |_| async {};
        let component_cleanup = |component_data: ComponentData| async move {
            component_data.cancel_token.cancel();
            if let Some(handle) = component_data.task_handle {
                handle.abort();
            }
        };

        self.tracker
            .write()
            .await
            .remove_workload_with_cleanup(workload_id, workload_cleanup, component_cleanup)
            .await;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc, time::Duration};

    use opentelemetry::trace::{
        SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState,
    };
    use tokio::sync::{RwLock, oneshot};

    use super::{PendingRequestGuard, RoutedMessage, subject_matches, subscriptions_match, types};
    use crate::observability::{PropagationContext, context_from_propagation, inject_context};

    fn context(trace: u128, span: u64) -> opentelemetry::Context {
        opentelemetry::Context::new().with_remote_span_context(SpanContext::new(
            TraceId::from(trace),
            SpanId::from(span),
            TraceFlags::SAMPLED,
            true,
            TraceState::default(),
        ))
    }

    #[tokio::test]
    async fn concurrent_propagation_contexts_do_not_leak() {
        let tasks = (1..=256_u64).map(|id| {
            tokio::spawn(async move {
                let expected_trace = TraceId::from(id as u128);
                let expected_span = SpanId::from(id);
                let propagation = inject_context(&context(id as u128, id));
                tokio::task::yield_now().await;
                let extracted = context_from_propagation(&propagation).unwrap();
                let actual = extracted.span().span_context().clone();
                (expected_trace, expected_span, actual)
            })
        });

        for task in tasks {
            let (trace_id, span_id, actual) = task.await.unwrap();
            assert_eq!(actual.trace_id(), trace_id);
            assert_eq!(actual.span_id(), span_id);
        }
    }

    #[test]
    fn routed_message_envelope_preserves_and_validates_context() {
        let message = types::BrokerMessage {
            subject: "orders.created".into(),
            reply_to: Some("_INBOX.reply".into()),
            body: vec![1, 2, 3],
        };
        let propagation = inject_context(&context(0x1234, 0x5678));
        let routed = RoutedMessage {
            message: message.clone(),
            propagation: Some(propagation),
        };

        assert_eq!(routed.message.subject, message.subject);
        assert_eq!(routed.message.reply_to, message.reply_to);
        assert_eq!(routed.message.body, message.body);
        let extracted = context_from_propagation(routed.propagation.as_ref().unwrap()).unwrap();
        assert_eq!(
            extracted.span().span_context().trace_id(),
            TraceId::from(0x1234)
        );
        assert_eq!(
            extracted.span().span_context().span_id(),
            SpanId::from(0x5678)
        );

        let missing = RoutedMessage {
            message: message.clone(),
            propagation: None,
        };
        assert!(missing.propagation.is_none());
        let malformed = RoutedMessage {
            message,
            propagation: Some(PropagationContext {
                traceparent: "malformed".into(),
                tracestate: None,
            }),
        };
        assert!(context_from_propagation(malformed.propagation.as_ref().unwrap()).is_err());
    }

    #[tokio::test]
    async fn dropped_pending_request_is_removed() {
        let pending_requests = Arc::new(RwLock::new(HashMap::new()));
        let (sender, _receiver) = oneshot::channel();
        pending_requests
            .write()
            .await
            .insert("_INBOX.cancelled".to_string(), sender);

        let guard =
            PendingRequestGuard::new("_INBOX.cancelled".to_string(), pending_requests.clone());
        let write_lock = pending_requests.write().await;
        drop(guard);
        drop(write_lock);

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if pending_requests.read().await.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("pending request cleanup timed out");
    }

    #[test]
    fn exact_and_literal_tokens() {
        assert!(subject_matches("tasks.leet", "tasks.leet"));
        assert!(!subject_matches("tasks.leet", "tasks.reverse"));
        // Token counts must match for a literal pattern.
        assert!(!subject_matches("tasks.leet", "tasks.leet.extra"));
        assert!(!subject_matches("tasks.leet.extra", "tasks.leet"));
    }

    #[test]
    fn single_token_wildcard() {
        assert!(subject_matches("tasks.*", "tasks.leet"));
        assert!(subject_matches("tasks.*", "tasks.reverse"));
        // `*` matches exactly one token, not zero and not many.
        assert!(!subject_matches("tasks.*", "tasks"));
        assert!(!subject_matches("tasks.*", "tasks.leet.v2"));
    }

    #[test]
    fn multi_token_wildcard() {
        assert!(subject_matches("tasks.>", "tasks.leet"));
        assert!(subject_matches("tasks.>", "tasks.leet.v2"));
        // `>` requires at least one trailing token.
        assert!(!subject_matches("tasks.>", "tasks"));
    }

    #[test]
    fn empty_subscriptions_match_everything() {
        // Back-compat: a handler with no configured subscriptions receives
        // every subject, preserving single-handler behavior.
        assert!(subscriptions_match(&[], "anything.at.all"));
    }

    #[test]
    fn non_empty_subscriptions_match_only_listed_subjects() {
        let subs = vec!["tasks.leet".to_string()];
        assert!(subscriptions_match(&subs, "tasks.leet"));
        assert!(!subscriptions_match(&subs, "tasks.reverse"));
    }
}
