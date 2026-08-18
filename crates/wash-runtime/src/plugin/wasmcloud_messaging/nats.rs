use std::collections::HashSet;
use std::sync::Arc;

use async_nats::Subscriber;
use futures::stream::StreamExt;
use opentelemetry::KeyValue;
use tokio::sync::RwLock;
use tracing::{Instrument, debug, instrument, trace, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use wasmtime::component::Accessor;
use wasmtime::error::Context as _;
mod bindings {
    crate::wasmtime::component::bindgen!({
        world: "messaging",
        imports: { default: store | async | trappable | tracing },
        exports: { default: async | tracing },
    });
}

use bindings::wasmcloud::messaging::consumer::{Host, HostWithStore};
use bindings::wasmcloud::messaging::types;

use crate::engine::ctx::{ActiveCtx, SharedCtx, extract_active_ctx};
use crate::engine::workload::{ResolvedWorkload, WorkloadItem};
use crate::observability::{Meters, PropagationContext, context_from_propagation, inject_context};
use crate::plugin::{HostPlugin, WitInterfaces, WorkloadTracker};
use crate::wit::{WitInterface, WitWorld};

const PLUGIN_MESSAGING_ID: &str = "wasmcloud-messaging";
const CONSUMER_GROUP_CONFIG: &str = "consumer_group";
const BROADCAST_CONSUMER_GROUP: &str = "broadcast";
const DEFAULT_CONSUMER_GROUP_PREFIX: &str = "wasmcloud";
const MAX_DEFAULT_CONSUMER_GROUP_LEN: usize = 128;

fn propagation_context(
    value: bindings::wasmcloud::observability::propagation::TraceContext,
) -> PropagationContext {
    PropagationContext {
        traceparent: value.traceparent,
        tracestate: value.tracestate,
    }
}

pub(super) fn producer_span_with_parent(
    operation: &'static str,
    destination: &str,
    payload_size: usize,
    parent: Option<PropagationContext>,
) -> tracing::Span {
    let span = tracing::info_span!(
        "wasmcloud.messaging.produce",
        otel.kind = "producer",
        messaging.system = "nats",
        messaging.operation = operation,
        messaging.destination.name = destination,
        messaging.message.body.size = payload_size,
        messaging.operation.outcome = tracing::field::Empty,
        error.type = tracing::field::Empty,
        otel.propagation.error = tracing::field::Empty,
        exception.slug = tracing::field::Empty,
        exception.message = tracing::field::Empty,
        otel.status_code = tracing::field::Empty,
        error = tracing::field::Empty,
    );
    if let Some(parent) = parent {
        match context_from_propagation(&parent) {
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

pub(super) fn headers_for_span(
    span: &tracing::Span,
    parent: Option<PropagationContext>,
) -> async_nats::HeaderMap {
    let mut propagation = inject_context(&span.context());
    if propagation.traceparent.is_empty()
        && let Some(parent) = parent
        && let Ok(context) = context_from_propagation(&parent)
    {
        propagation = inject_context(&context);
    }
    let mut headers = async_nats::HeaderMap::new();
    if !propagation.traceparent.is_empty() {
        headers.insert("traceparent", propagation.traceparent);
    }
    if let Some(tracestate) = propagation.tracestate {
        headers.insert("tracestate", tracestate);
    }
    headers
}

fn message_parent(msg: &async_nats::Message) -> (Option<opentelemetry::Context>, bool) {
    let Some(headers) = &msg.headers else {
        return (None, false);
    };
    let Some(traceparent) = headers.get("traceparent") else {
        return (None, headers.get("tracestate").is_some());
    };
    let propagation = PropagationContext {
        traceparent: traceparent.as_str().to_string(),
        tracestate: headers
            .get("tracestate")
            .map(|value| value.as_str().to_string()),
    };
    match context_from_propagation(&propagation) {
        Ok(context) => (Some(context), false),
        Err(_) => (None, true),
    }
}

pub struct ComponentData {
    subscriptions: Vec<String>,
    consumer_group: ConsumerGroup,
    cancel_token: tokio_util::sync::CancellationToken,
    task_handle: Option<tokio::task::JoinHandle<()>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ConsumerGroup {
    Grouped(String),
    Broadcast,
}

impl ConsumerGroup {
    fn resolve(
        configured: Option<&str>,
        workload_namespace: &str,
        workload_name: &str,
        component_name: &str,
    ) -> anyhow::Result<Self> {
        match configured {
            None => Ok(Self::Grouped(default_consumer_group(
                workload_namespace,
                workload_name,
                component_name,
            ))),
            Some(value) if value == BROADCAST_CONSUMER_GROUP => Ok(Self::Broadcast),
            Some(value) => {
                validate_consumer_group(value)?;
                Ok(Self::Grouped(value.to_string()))
            }
        }
    }

    fn name(&self) -> Option<&str> {
        match self {
            Self::Grouped(name) => Some(name),
            Self::Broadcast => None,
        }
    }
}

#[derive(Clone)]
pub struct NatsMessaging {
    tracker: Arc<RwLock<WorkloadTracker<(), ComponentData>>>,
    client: Arc<async_nats::Client>,
    meters: Arc<RwLock<Meters>>,
}

impl NatsMessaging {
    pub fn new(client: Arc<async_nats::Client>) -> Self {
        Self {
            client,
            tracker: Arc::new(RwLock::new(WorkloadTracker::default())),
            meters: Default::default(),
        }
    }
}

fn plugin<T>(store: &Accessor<T, SharedCtx>) -> wasmtime::Result<Arc<NatsMessaging>> {
    store.with(|mut access| {
        access
            .get()
            .try_get_plugin::<NatsMessaging>(PLUGIN_MESSAGING_ID)
    })
}

impl Host for ActiveCtx<'_> {}

impl<T> HostWithStore<T> for SharedCtx {
    async fn request(
        store: &Accessor<T, Self>,
        subject: String,
        body: Vec<u8>,
        timeout_ms: u32,
        parent_context: Option<bindings::wasmcloud::observability::propagation::TraceContext>,
    ) -> wasmtime::Result<Result<types::BrokerMessage, String>> {
        let plugin = plugin(store)?;
        let parent = parent_context.map(propagation_context);
        let span = producer_span_with_parent("request", &subject, body.len(), parent.clone());
        let headers = headers_for_span(&span, parent);
        let result = async {
            match tokio::time::timeout(
                std::time::Duration::from_millis(timeout_ms as u64),
                plugin
                    .client
                    .request_with_headers(subject, headers, body.into()),
            )
            .await
            {
                Ok(Ok(msg)) => Ok(types::BrokerMessage {
                    subject: msg.subject.to_string(),
                    reply_to: msg.reply.as_ref().map(ToString::to_string),
                    body: msg.payload.into(),
                }),
                Ok(Err(error)) => Err(format!("failed to send request: {error}")),
                Err(_) => Err(format!("request timed out after {timeout_ms}ms")),
            }
        }
        .instrument(span.clone())
        .await;
        match &result {
            Ok(_) => {
                span.record("messaging.operation.outcome", "success");
            }
            Err(error) => {
                let slug = if error.starts_with("request timed out") {
                    "messaging-request-timeout"
                } else {
                    "messaging-broker-failed"
                };
                super::record_messaging_error(&span, slug, error);
            }
        }
        Ok(result)
    }

    async fn publish(
        store: &Accessor<T, Self>,
        msg: types::BrokerMessage,
        parent_context: Option<bindings::wasmcloud::observability::propagation::TraceContext>,
    ) -> wasmtime::Result<Result<(), String>> {
        let plugin = plugin(store)?;
        let parent = parent_context.map(propagation_context);
        let span =
            producer_span_with_parent("publish", &msg.subject, msg.body.len(), parent.clone());
        let headers = headers_for_span(&span, parent);
        let result = async {
            if let Some(reply_to) = msg.reply_to {
                plugin
                    .client
                    .publish_with_reply_and_headers(msg.subject, reply_to, headers, msg.body.into())
                    .await
            } else {
                plugin
                    .client
                    .publish_with_headers(msg.subject, headers, msg.body.into())
                    .await
            }
            .map_err(|error| format!("failed to send message: {error}"))
        }
        .instrument(span.clone())
        .await;
        match &result {
            Ok(_) => {
                span.record("messaging.operation.outcome", "success");
            }
            Err(error) => {
                super::record_messaging_error(&span, "messaging-broker-failed", error);
            }
        }
        Ok(result)
    }
}

impl<'a> types::Host for ActiveCtx<'a> {}

#[async_trait::async_trait]
impl HostPlugin for NatsMessaging {
    fn id(&self) -> &'static str {
        PLUGIN_MESSAGING_ID
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

    async fn on_workload_item_bind<'a>(
        &self,
        component_handle: &mut WorkloadItem<'a>,
        interfaces: WitInterfaces<'_>,
    ) -> anyhow::Result<()> {
        let Some(interface) = interfaces.get("wasmcloud", "messaging", &[]) else {
            return Ok(());
        };

        // Subscriptions come from this component's own `LocalResources.config`
        // (so workers in one workload can subscribe to different subjects),
        // falling back to the workload-scoped host interface config. Capture
        // the host-interface fallback before borrowing the component.
        let interface_subscriptions = interface.config.get("subscriptions").cloned();
        let interface_consumer_group = interface.config.get(CONSUMER_GROUP_CONFIG).cloned();

        bindings::wasmcloud::messaging::types::add_to_linker::<_, SharedCtx>(
            component_handle.linker(),
            extract_active_ctx,
        )?;
        bindings::wasmcloud::messaging::consumer::add_to_linker::<_, SharedCtx>(
            component_handle.linker(),
            extract_active_ctx,
        )?;

        let local_subscriptions = component_handle
            .local_resources()
            .config
            .get("subscriptions")
            .cloned();
        let local_consumer_group = component_handle
            .local_resources()
            .config
            .get(CONSUMER_GROUP_CONFIG)
            .cloned();

        // Track a handler component OR a long-lived handler service:
        // `WorkloadItem` derefs to the underlying metadata for both, so the
        // subscriber loop is set up either way (and its receive loop delivers to
        // the running service when one is registered). Works whether or not the
        // workload declares a `wasmcloud:messaging` host interface entry, and
        // matches the handler export version-tolerantly.
        if super::exports_messaging_handler(&component_handle.world()) {
            let raw = local_subscriptions.or(interface_subscriptions);
            let raw_subscriptions = super::parse_subscriptions(raw.as_deref());
            let component_name = match component_handle {
                WorkloadItem::Component(component) => component.name().to_string(),
                WorkloadItem::Service(_) => "service".to_string(),
            };
            let consumer_group = ConsumerGroup::resolve(
                local_consumer_group
                    .as_deref()
                    .or(interface_consumer_group.as_deref()),
                component_handle.workload_namespace(),
                component_handle.workload_name(),
                &component_name,
            )?;

            debug!(
                component_id = component_handle.id(),
                subscriptions = ?raw_subscriptions,
                consumer_group = consumer_group.name().unwrap_or(BROADCAST_CONSUMER_GROUP),
                "tracking handler component for NATS messaging"
            );
            self.tracker.write().await.add_component(
                component_handle,
                ComponentData {
                    cancel_token: tokio_util::sync::CancellationToken::new(),
                    subscriptions: raw_subscriptions,
                    consumer_group,
                    task_handle: None,
                },
            );
        }

        Ok(())
    }

    #[instrument(name = "wasmcloud.messaging.on_workload_resolved", skip_all, fields(component_id = %component_id, workload.id = %workload.id()))]
    async fn on_workload_resolved(
        &self,
        workload: &ResolvedWorkload,
        component_id: &str,
    ) -> anyhow::Result<()> {
        debug!("on_workload_resolved entered for NATS messaging");

        let (cancel_token, subjects, consumer_group) = {
            let lock = self.tracker.read().await;
            match lock.get_component_data(component_id) {
                Some(data) => (
                    data.cancel_token.clone(),
                    data.subscriptions.clone(),
                    data.consumer_group.clone(),
                ),
                None => {
                    debug!("no tracker entry for component, skipping subscription setup");
                    return Ok(());
                }
            }
        };

        debug!(?subjects, "loaded subscriptions from tracker");

        if subjects.is_empty() {
            debug!("no subscriptions configured, skipping subscription setup");
            return Ok(());
        }

        // A long-lived handler service has no per-component instance to
        // pre-instantiate; its receive loop delivers to the running service
        // instead. Only components get a `MessagingPre` for per-message work.
        let pre = match workload.instantiate_pre(component_id).await {
            Ok(instance_pre) => Some(
                bindings::MessagingPre::new(instance_pre)
                    .context("failed to instantiate messaging pre")?,
            ),
            Err(e) => {
                trace!(component_id, error = %e, "no per-message instance (long-lived service); messages delivered to the service");
                None
            }
        };

        let workload = workload.clone();
        let component_id = component_id.to_string();
        let tracker_component_id = component_id.clone();

        let mut subscriptions = Vec::<Subscriber>::new();
        for subject in &subjects {
            debug!(
                %subject,
                consumer_group = consumer_group.name().unwrap_or(BROADCAST_CONSUMER_GROUP),
                "subscribing to NATS subject"
            );
            let result = match &consumer_group {
                ConsumerGroup::Grouped(group) => {
                    self.client
                        .queue_subscribe(subject.clone(), group.clone())
                        .await
                }
                ConsumerGroup::Broadcast => self.client.subscribe(subject.clone()).await,
            };
            let sub = match result {
                Ok(sub) => sub,
                Err(e) => {
                    for sub in subscriptions {
                        drop(sub);
                    }
                    return Err(
                        anyhow::anyhow!(e).context(format!("failed to subscribe to {subject}"))
                    );
                }
            };
            debug!(
                %subject,
                consumer_group = consumer_group.name().unwrap_or(BROADCAST_CONSUMER_GROUP),
                "successfully subscribed"
            );

            subscriptions.push(sub);
        }

        // Make sure NATS has actually processed all the subscriptions above
        // before we let `on_workload_resolved` return Ok. `client.flush()`
        // only flushes the local TCP write buffer — NATS may not have seen
        // the SUB protocol messages yet by the time it returns, so a
        // request to the subscribed subject fired immediately after can
        // race ahead and get "no responders". To force a true server-side
        // round-trip we subscribe to a fresh inbox subject, publish a single
        // sentinel byte to it, and wait for the message to come back. NATS
        // processes commands per-connection in order, so by the time the
        // sentinel arrives, every SUB queued earlier on this connection has
        // also been processed. See https://github.com/wasmCloud/wasmCloud/issues/5074.
        if let Err(e) = sync_with_server(&self.client).await {
            warn!(error = ?e, "failed to sync subscriptions with NATS server");
        }

        let mut messages = futures::stream::select_all(subscriptions);
        let fuel_meter = self.meters.read().await.fuel_consumption.clone();

        let span = tracing::Span::current();
        let handle = tokio::spawn(async move {
            debug!(
                parent: &span,
                subjects = ?subjects,
                "NATS subscriber loop started"
            );
            loop {
                tokio::select! {
                    maybe_msg = messages.next() => {
                        let msg = match maybe_msg {
                            None => {
                                warn!(
                                    parent: &span,
                                    component_id = %component_id,
                                    "NATS subscriber stream closed unexpectedly; handler will stop receiving messages"
                                );
                                break;
                            }
                            Some(msg) => {
                                msg
                            }
                        };

                        let (message_parent, invalid_parent) = message_parent(&msg);
                        let subject = msg.subject.to_string();
                        let reply_to = msg.reply.as_ref().map(|r| r.to_string());
                        let payload_size = msg.payload.len();
                        let body: Vec<u8> = msg.payload.into();
                        let span = tracing::info_span!(
                            "wasmcloud.messaging.consume",
                            otel.kind = "consumer",
                            messaging.system = "nats",
                            messaging.operation = "process",
                            messaging.destination.name = %subject,
                            messaging.message.body.size = payload_size,
                            messaging.operation.outcome = tracing::field::Empty,
                            error.type = tracing::field::Empty,
                            otel.propagation.error = invalid_parent,
                            exception.slug = tracing::field::Empty,
                            exception.message = tracing::field::Empty,
                            otel.status_code = tracing::field::Empty,
                            error = tracing::field::Empty,
                        );
                        if let Some(parent) = message_parent {
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
                                subject: subject.clone(),
                                body,
                                reply_to,
                            };
                            let result = workload
                                .http_handler()
                                .deliver_trigger_service_message(workload.id(), broker)
                                .instrument(span.clone())
                                .await;
                            match result {
                                Ok(Ok(())) => {
                                    span.record("messaging.operation.outcome", "success");
                                    debug!(%subject, "trigger service handled message");
                                }
                                Ok(Err(e)) => {
                                    super::record_messaging_error(&span, "messaging-handler-rejected", &e);
                                    warn!(%subject, error = %e, "trigger service message handler returned error")
                                }
                                Err(e) => {
                                    super::record_messaging_error(&span, "messaging-consumer-delivery-failed", &e.to_string());
                                    warn!(%subject, error = %e, "failed to deliver message to trigger service")
                                }
                            }
                            continue;
                        }

                        let Some(pre) = &pre else {
                            warn!(
                                %subject,
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
                        let msg = types::BrokerMessage {
                            subject,
                            reply_to,
                            body,
                        };

                        let fuel_meter = fuel_meter.clone();

                        tokio::spawn(async move {
                            let handler_span = span.clone();
                            let result = fuel_meter.observe(
                                &[
                                    KeyValue::new("plugin", PLUGIN_MESSAGING_ID),
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
                    _ = cancel_token.cancelled() => {
                        debug!(
                            parent: &span,
                            component_id = %component_id,
                            "NATS subscriber loop cancelled"
                        );
                        break;
                    }
                }
            }
        });

        {
            let mut lock = self.tracker.write().await;
            if let Some(data) = lock.get_component_data_mut(&tracker_component_id) {
                data.task_handle = Some(handle);
            } else {
                warn!(
                    component_id = %tracker_component_id,
                    "tracker entry vanished before task handle could be stored"
                );
            }
        }

        Ok(())
    }

    async fn on_workload_unbind(
        &self,
        workload_id: &str,
        _interfaces: WitInterfaces<'_>,
    ) -> anyhow::Result<()> {
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

fn validate_consumer_group(value: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !value.is_empty(),
        "`{CONSUMER_GROUP_CONFIG}` cannot be empty; omit it for the default group or set it to `{BROADCAST_CONSUMER_GROUP}` for broadcast delivery"
    );
    anyhow::ensure!(
        !value
            .chars()
            .any(|c| c.is_whitespace() || c == '*' || c == '>'),
        "invalid `{CONSUMER_GROUP_CONFIG}` `{value}`: NATS consumer groups cannot contain whitespace, `*`, or `>`"
    );
    Ok(())
}

/// Return a stable, NATS-safe queue name for every replica of a logical
/// component. The readable prefix helps operators identify the consumer while
/// the FNV-1a suffix preserves distinctions lost through sanitization or
/// truncation without adding a hashing dependency to the runtime.
fn default_consumer_group(namespace: &str, workload: &str, component: &str) -> String {
    let identity = format!("{namespace}\0{workload}\0{component}");
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in identity.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }

    let readable = [namespace, workload, component]
        .into_iter()
        .map(|part| {
            part.chars()
                .map(|c| {
                    if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                        c
                    } else {
                        '-'
                    }
                })
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join(".");
    let suffix = format!(".{hash:016x}");
    let max_readable_len = MAX_DEFAULT_CONSUMER_GROUP_LEN
        .saturating_sub(DEFAULT_CONSUMER_GROUP_PREFIX.len() + 1 + suffix.len());
    let readable = &readable[..readable.floor_char_boundary(max_readable_len)];
    format!("{DEFAULT_CONSUMER_GROUP_PREFIX}.{readable}{suffix}")
}

/// Server-side synchronization barrier for the NATS client.
///
/// `client.flush()` in `async_nats` only flushes the local TCP write buffer
/// — it does not wait for the server to acknowledge that prior SUBs have
/// been registered. After flush returns, NATS may not yet have processed
/// our subscriptions, so an immediate request on a subscribed subject can
/// race ahead and hit "no responders" (this is exactly the failure mode
/// of #5074 in environments where the data path is slow enough to widen
/// the race window — kubernetes with TLS).
///
/// To bound the race, this helper subscribes to a fresh inbox, publishes a
/// single byte to it, and awaits the round-tripped message. NATS processes
/// per-connection commands in order, so once we receive the sentinel back
/// every earlier SUB on this connection is guaranteed to be active.
async fn sync_with_server(client: &async_nats::Client) -> anyhow::Result<()> {
    use futures::stream::StreamExt;

    let inbox = client.new_inbox();
    let mut sentinel = client
        .subscribe(inbox.clone())
        .await
        .context("failed to subscribe to sync inbox")?;
    client
        .publish(inbox, bytes::Bytes::from_static(&[0]))
        .await
        .context("failed to publish sync message")?;
    // Tight bound — if NATS is genuinely unreachable we'll bail; otherwise
    // the round trip is sub-millisecond locally, low single-digit ms in
    // kubernetes.
    match tokio::time::timeout(std::time::Duration::from_secs(5), sentinel.next()).await {
        Ok(Some(_)) => Ok(()),
        Ok(None) => anyhow::bail!("sync inbox subscription closed before sentinel arrived"),
        Err(_) => anyhow::bail!("sync with NATS timed out after 5s"),
    }
}

#[cfg(test)]
mod tests {
    //! Locks in the plugin's state-machine invariants without Docker /
    //! NATS / wasmtime: the seam between the plugin and its tracker, and
    //! pure-data parsing. Anything that requires a real
    //! `WorkloadComponent` / `ResolvedWorkload` is exercised by the
    //! integration suite instead.
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::plugin::WorkloadTrackerItem;
    use opentelemetry::trace::{
        SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState,
    };
    use std::time::Duration;

    fn remote_context(trace_id: &str, span_id: &str) -> opentelemetry::Context {
        opentelemetry::Context::new().with_remote_span_context(SpanContext::new(
            TraceId::from_hex(trace_id).unwrap(),
            SpanId::from_hex(span_id).unwrap(),
            TraceFlags::SAMPLED,
            true,
            TraceState::default(),
        ))
    }

    fn message_with_context(context: &opentelemetry::Context) -> async_nats::Message {
        let propagation = inject_context(context);
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("traceparent", propagation.traceparent);
        if let Some(tracestate) = propagation.tracestate {
            headers.insert("tracestate", tracestate);
        }
        nats_message(Some(headers))
    }

    #[test]
    fn producer_header_consumer_and_reply_producer_keep_exact_graph_ids() {
        const TRACE_ID: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
        const REQUEST_PRODUCER_ID: &str = "00f067aa0ba902b7";
        const CONSUMER_ID: &str = "1111111111111111";
        const REPLY_PRODUCER_ID: &str = "2222222222222222";

        let request_producer = remote_context(TRACE_ID, REQUEST_PRODUCER_ID);
        let (consumer_parent, invalid) = message_parent(&message_with_context(&request_producer));
        assert!(!invalid);
        let consumer_parent = consumer_parent.unwrap();
        assert_eq!(
            consumer_parent.span().span_context().trace_id().to_string(),
            TRACE_ID
        );
        assert_eq!(
            consumer_parent.span().span_context().span_id().to_string(),
            REQUEST_PRODUCER_ID
        );

        let consumer = remote_context(TRACE_ID, CONSUMER_ID);
        let (reply_parent, invalid) = message_parent(&message_with_context(&consumer));
        assert!(!invalid);
        let reply_parent = reply_parent.unwrap();
        assert_eq!(
            reply_parent.span().span_context().trace_id().to_string(),
            TRACE_ID
        );
        assert_eq!(
            reply_parent.span().span_context().span_id().to_string(),
            CONSUMER_ID
        );

        let reply_producer = remote_context(TRACE_ID, REPLY_PRODUCER_ID);
        let (reply_consumer_parent, invalid) =
            message_parent(&message_with_context(&reply_producer));
        assert!(!invalid);
        let reply_consumer_parent = reply_consumer_parent.unwrap();
        assert_eq!(
            reply_consumer_parent
                .span()
                .span_context()
                .trace_id()
                .to_string(),
            TRACE_ID
        );
        assert_eq!(
            reply_consumer_parent
                .span()
                .span_context()
                .span_id()
                .to_string(),
            REPLY_PRODUCER_ID
        );
    }

    /// Tracker round-trip: stored subscriptions and a stored cancellation
    /// token are retrievable by the same component_id; cleanup cancels the
    /// stored token. Does not exercise the NATS client at all — the goal is
    /// to lock in the contract `on_workload_resolved` depends on.
    fn nats_message(headers: Option<async_nats::HeaderMap>) -> async_nats::Message {
        async_nats::Message {
            subject: "subject".into(),
            reply: None,
            payload: bytes::Bytes::new(),
            headers,
            status: None,
            description: None,
            length: 0,
        }
    }

    #[test]
    fn extracts_unsampled_nats_parent() {
        let mut headers = async_nats::HeaderMap::new();
        headers.insert(
            "traceparent",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00",
        );
        headers.insert("tracestate", "vendor=value");
        let (parent, invalid) = message_parent(&nats_message(Some(headers)));
        let parent = parent.expect("valid unsampled context");
        let span = opentelemetry::trace::TraceContextExt::span(&parent);
        let span_context = span.span_context();
        assert!(span_context.is_valid());
        assert!(!span_context.is_sampled());
        assert!(!invalid);
    }

    #[test]
    fn accepts_missing_and_rejects_malformed_nats_parent() {
        let (parent, invalid) = message_parent(&nats_message(None));
        assert!(parent.is_none());
        assert!(!invalid);

        let mut headers = async_nats::HeaderMap::new();
        headers.insert("traceparent", "malformed");
        let (parent, invalid) = message_parent(&nats_message(Some(headers)));
        assert!(parent.is_none());
        assert!(invalid);

        let mut headers = async_nats::HeaderMap::new();
        headers.insert("tracestate", "vendor=value");
        let (parent, invalid) = message_parent(&nats_message(Some(headers)));
        assert!(parent.is_none());
        assert!(invalid);
    }

    #[tokio::test]
    async fn tracker_round_trip_with_component_data() {
        use crate::plugin::WorkloadTracker;

        let mut tracker: WorkloadTracker<(), ComponentData> = WorkloadTracker::default();
        // We can't construct a real WorkloadComponent here without the
        // engine, so we simulate `add_component`'s effect directly via the
        // public maps. This documents the invariant the plugin relies on.
        let workload_id = "wl-1".to_string();
        let component_id = "c-1".to_string();
        let cancel_token = tokio_util::sync::CancellationToken::new();
        let cancel_token_clone = cancel_token.clone();

        tracker
            .workloads
            .entry(workload_id.clone())
            .or_insert_with(|| WorkloadTrackerItem {
                workload_data: None,
                components: std::collections::HashMap::new(),
            })
            .components
            .insert(
                component_id.clone(),
                ComponentData {
                    cancel_token,
                    subscriptions: vec!["tasks.x".to_string()],
                    consumer_group: ConsumerGroup::Grouped("workers".to_string()),
                    task_handle: None,
                },
            );
        tracker
            .components
            .insert(component_id.clone(), workload_id.clone());

        let data = tracker
            .get_component_data(&component_id)
            .expect("component should be retrievable");
        assert_eq!(data.subscriptions, vec!["tasks.x".to_string()]);
        assert_eq!(data.consumer_group.name(), Some("workers"));
        assert!(!cancel_token_clone.is_cancelled());

        // Simulate on_workload_unbind's cleanup closure.
        tracker
            .remove_workload_with_cleanup(
                &workload_id,
                |_| async {},
                |cd: ComponentData| async move {
                    cd.cancel_token.cancel();
                },
            )
            .await;

        assert!(
            cancel_token_clone.is_cancelled(),
            "cleanup must propagate cancellation to the clone the spawn loop holds"
        );
        assert!(tracker.get_component_data(&component_id).is_none());
    }

    /// The cancel-token clone the spawn loop holds and the original in the
    /// tracker share state, so cancelling either one wakes the other.
    /// Catches anyone replacing `Clone` with `Copy`-style semantics that
    /// break the unbind→loop-exit signal.
    #[tokio::test]
    async fn cancel_token_clone_shares_state() {
        let original = tokio_util::sync::CancellationToken::new();
        let clone = original.clone();
        original.cancel();
        // cancelled() on the clone resolves immediately because the inner
        // state is shared.
        tokio::time::timeout(Duration::from_millis(50), clone.cancelled())
            .await
            .expect("cloned cancel token should observe the cancellation");
    }

    #[test]
    fn default_group_is_stable_for_component_replicas() {
        let first = default_consumer_group("orders", "processor", "worker");
        let second = default_consumer_group("orders", "processor", "worker");

        assert_eq!(first, second);
        assert!(first.starts_with("wasmcloud.orders.processor.worker."));
    }

    #[test]
    fn default_group_is_isolated_by_logical_component_identity() {
        let base = default_consumer_group("orders", "processor", "worker");

        assert_ne!(base, default_consumer_group("other", "processor", "worker"));
        assert_ne!(base, default_consumer_group("orders", "other", "worker"));
        assert_ne!(base, default_consumer_group("orders", "processor", "other"));
    }

    #[test]
    fn default_group_is_nats_safe_and_bounded() {
        let group = default_consumer_group(
            "namespace with spaces.*",
            &"workload".repeat(40),
            "handler.>",
        );

        assert!(group.len() <= MAX_DEFAULT_CONSUMER_GROUP_LEN);
        assert!(
            !group
                .chars()
                .any(|c| c.is_whitespace() || c == '*' || c == '>')
        );
        assert_eq!(
            group,
            default_consumer_group(
                "namespace with spaces.*",
                &"workload".repeat(40),
                "handler.>",
            )
        );
    }

    #[test]
    fn consumer_group_configuration_selects_default_explicit_or_broadcast() {
        let default = ConsumerGroup::resolve(None, "ns", "workload", "component").unwrap();
        assert_eq!(
            default,
            ConsumerGroup::Grouped(default_consumer_group("ns", "workload", "component"))
        );
        assert_eq!(
            ConsumerGroup::resolve(Some("shared-workers"), "ns", "workload", "component").unwrap(),
            ConsumerGroup::Grouped("shared-workers".to_string())
        );
        assert_eq!(
            ConsumerGroup::resolve(
                Some(BROADCAST_CONSUMER_GROUP),
                "ns",
                "workload",
                "component"
            )
            .unwrap(),
            ConsumerGroup::Broadcast
        );
    }

    #[test]
    fn consumer_group_configuration_rejects_invalid_values() {
        for value in ["", "two groups", "workers.*", "workers.>"] {
            assert!(
                ConsumerGroup::resolve(Some(value), "ns", "workload", "component").is_err(),
                "expected `{value}` to be rejected"
            );
        }
    }
}
