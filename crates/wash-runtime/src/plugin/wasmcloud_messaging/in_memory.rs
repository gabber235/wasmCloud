use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::engine::ctx::{ActiveCtx, SharedCtx, extract_active_ctx};
use crate::engine::workload::{ResolvedWorkload, UnresolvedWorkload, WorkloadItem};
use crate::observability::{Meters, PropagationContext, context_from_propagation};
use crate::plugin::{HostPlugin, WitInterfaces};
use crate::wit::{WitInterface, WitWorld};
use anyhow::Context;
use opentelemetry::KeyValue;
use tokio::sync::{Notify, RwLock, mpsc, oneshot};
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

/// A component's message inbox, shared between the publisher side
/// (`route_to_subscribers`) and the component's processing task.
struct RoutedMessage {
    message: types::BrokerMessage,
    propagation: Option<PropagationContext>,
    _activity: Option<Activity>,
}

type InboxSender = mpsc::Sender<RoutedMessage>;

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostMessage {
    pub subject: String,
    pub reply_to: Option<String>,
    pub body: Vec<u8>,
    pub trace_context: Option<TraceContext>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceContext {
    pub traceparent: String,
    pub tracestate: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MessageOrigin {
    Guest,
    HostInjected,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObservedOperation {
    Publish,
    Request,
    Reply,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObservedMessage {
    pub operation: ObservedOperation,
    pub origin: MessageOrigin,
    pub message: HostMessage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessagingError {
    AlreadyReserved,
    NotBound,
    Closed,
    QueueFull,
    Timeout,
    AlreadyReplied,
}
impl std::fmt::Display for MessagingError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyReserved => "workload is already reserved",
            Self::NotBound => "workload is not bound",
            Self::Closed => "workload is closed",
            Self::QueueFull => "message queue is full",
            Self::Timeout => "request timed out",
            Self::AlreadyReplied => "reply was already sent",
        })
    }
}
impl std::error::Error for MessagingError {}

pub struct ObservationReceiver(mpsc::Receiver<ObservedMessage>);
impl ObservationReceiver {
    pub async fn recv(&mut self) -> Option<ObservedMessage> {
        self.0.recv().await
    }
}
pub struct ResponderReceiver(mpsc::Receiver<ResponderRequest>);
impl ResponderReceiver {
    pub async fn recv(&mut self) -> Option<ResponderRequest> {
        self.0.recv().await
    }
}
pub struct ResponderRequest {
    pub message: HostMessage,
    reply: Option<oneshot::Sender<HostMessage>>,
}
impl ResponderRequest {
    pub fn reply(mut self, message: HostMessage) -> Result<(), MessagingError> {
        self.reply
            .take()
            .ok_or(MessagingError::AlreadyReplied)?
            .send(message)
            .map_err(|_| MessagingError::Closed)
    }
}

const RESERVED: u8 = 0;
const BOUND: u8 = 1;
const CLOSING: u8 = 2;
const CLOSED: u8 = 3;

const MAX_TOMBSTONES: usize = 1024;

struct Tombstones {
    subjects: HashSet<String>,
    order: VecDeque<String>,
}

impl Tombstones {
    fn insert(&mut self, subject: String) {
        if !self.subjects.insert(subject.clone()) {
            return;
        }
        self.order.push_back(subject);
        while self.order.len() > MAX_TOMBSTONES {
            if let Some(expired) = self.order.pop_front() {
                self.subjects.remove(&expired);
            }
        }
    }

    fn contains(&self, subject: &str) -> bool {
        self.subjects.contains(subject)
    }
}

struct WorkloadData {
    lifecycle: AtomicU8,
    pending_requests: Arc<Mutex<HashMap<String, oneshot::Sender<RoutedMessage>>>>,
    tombstones: Mutex<Tombstones>,
    observers: RwLock<Vec<mpsc::Sender<ObservedMessage>>>,
    responders: RwLock<Vec<(String, mpsc::Sender<ResponderRequest>)>>,
    activity: AtomicUsize,
    activity_changed: Notify,
}

impl WorkloadData {
    fn new(state: u8) -> Self {
        Self {
            lifecycle: AtomicU8::new(state),
            pending_requests: Arc::default(),
            tombstones: Mutex::new(Tombstones {
                subjects: HashSet::new(),
                order: VecDeque::new(),
            }),
            observers: RwLock::new(Vec::new()),
            responders: RwLock::new(Vec::new()),
            activity: AtomicUsize::new(0),
            activity_changed: Notify::new(),
        }
    }
    fn require_bound(&self) -> Result<(), MessagingError> {
        match self.lifecycle.load(Ordering::Acquire) {
            BOUND => Ok(()),
            CLOSING | CLOSED => Err(MessagingError::Closed),
            _ => Err(MessagingError::NotBound),
        }
    }
    fn begin(self: &Arc<Self>) -> Activity {
        self.activity.fetch_add(1, Ordering::AcqRel);
        Activity(self.clone())
    }
}
impl Default for WorkloadData {
    fn default() -> Self {
        Self::new(BOUND)
    }
}
struct PendingRequestGuard {
    inbox: String,
    pending: Arc<Mutex<HashMap<String, oneshot::Sender<RoutedMessage>>>>,
}

impl Drop for PendingRequestGuard {
    fn drop(&mut self) {
        self.pending.lock().unwrap().remove(&self.inbox);
    }
}

struct Activity(Arc<WorkloadData>);
impl Drop for Activity {
    fn drop(&mut self) {
        if self.0.activity.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.activity_changed.notify_waiters();
        }
    }
}

struct ComponentData {
    cancel_token: tokio_util::sync::CancellationToken,
    task_handle: Option<tokio::task::JoinHandle<()>>,
    subscriptions: Vec<String>,
    inbox: InboxSender,
    receiver: Option<mpsc::Receiver<RoutedMessage>>,
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
    data: &Arc<WorkloadData>,
    msg: &RoutedMessage,
) -> Result<(), MessagingError> {
    let targets = {
        let tracker = plugin.tracker.read().await;
        let item = tracker
            .workloads
            .get(workload_id)
            .ok_or(MessagingError::Closed)?;
        item.components
            .values()
            .filter(|component| subscriptions_match(&component.subscriptions, &msg.message.subject))
            .map(|component| component.inbox.clone())
            .collect::<Vec<_>>()
    };
    let mut permits = Vec::with_capacity(targets.len());
    for target in targets {
        permits.push(target.try_reserve_owned().map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => MessagingError::QueueFull,
            mpsc::error::TrySendError::Closed(_) => MessagingError::Closed,
        })?);
    }
    for permit in permits {
        permit.send(RoutedMessage {
            message: msg.message.clone(),
            propagation: msg.propagation.clone(),
            _activity: Some(data.begin()),
        });
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
    tracker: Arc<RwLock<WorkloadTracker<Arc<WorkloadData>, ComponentData>>>,
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
        self.publish_core(
            workload_id,
            HostMessage {
                subject: subject.to_string(),
                reply_to: None,
                body,
                trace_context: None,
            },
            MessageOrigin::HostInjected,
        )
        .await
        .map_err(|error| error.to_string())
    }
}

impl Default for InMemoryMessaging {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub struct InMemoryMessagingDriver {
    plugin: InMemoryMessaging,
    workload_id: Arc<str>,
}

impl InMemoryMessaging {
    pub async fn reserve_workload(
        &self,
        workload_id: impl Into<String>,
    ) -> Result<InMemoryMessagingDriver, MessagingError> {
        let workload_id = workload_id.into();
        let mut tracker = self.tracker.write().await;
        if tracker.workloads.contains_key(&workload_id) {
            return Err(MessagingError::AlreadyReserved);
        }
        tracker.workloads.insert(
            workload_id.clone(),
            crate::plugin::WorkloadTrackerItem {
                workload_data: Some(Arc::new(WorkloadData::new(RESERVED))),
                components: HashMap::new(),
            },
        );
        Ok(InMemoryMessagingDriver {
            plugin: self.clone(),
            workload_id: workload_id.into(),
        })
    }

    pub fn driver(&self, workload_id: impl Into<Arc<str>>) -> InMemoryMessagingDriver {
        InMemoryMessagingDriver {
            plugin: self.clone(),
            workload_id: workload_id.into(),
        }
    }
}

impl InMemoryMessagingDriver {
    async fn data(&self) -> Result<Arc<WorkloadData>, MessagingError> {
        let tracker = self.plugin.tracker.read().await;
        let data = tracker
            .get_workload_data(&self.workload_id)
            .cloned()
            .ok_or(MessagingError::NotBound)?;
        data.require_bound()?;
        Ok(data)
    }

    pub async fn observe(&self, capacity: usize) -> Result<ObservationReceiver, MessagingError> {
        let data = self.data().await?;
        let (sender, receiver) = mpsc::channel(capacity.max(1));
        let mut observers = data.observers.write().await;
        data.require_bound()?;
        observers.push(sender);
        Ok(ObservationReceiver(receiver))
    }

    pub async fn register_responder(
        &self,
        subject: impl Into<String>,
        capacity: usize,
    ) -> Result<ResponderReceiver, MessagingError> {
        let data = self.data().await?;
        let (sender, receiver) = mpsc::channel(capacity.max(1));
        let mut responders = data.responders.write().await;
        data.require_bound()?;
        responders.push((subject.into(), sender));
        Ok(ResponderReceiver(receiver))
    }

    pub async fn publish(&self, message: HostMessage) -> Result<(), MessagingError> {
        self.plugin
            .publish_core(&self.workload_id, message, MessageOrigin::HostInjected)
            .await
    }

    pub async fn request(
        &self,
        message: HostMessage,
        timeout: Duration,
    ) -> Result<HostMessage, MessagingError> {
        self.plugin
            .request_core(
                &self.workload_id,
                message,
                timeout,
                MessageOrigin::HostInjected,
            )
            .await
    }

    pub async fn wait_idle(&self) -> Result<(), MessagingError> {
        let data = self.data().await?;
        loop {
            let notified = data.activity_changed.notified();
            if data.activity.load(Ordering::Acquire) == 0 {
                return Ok(());
            }
            notified.await;
        }
    }

    pub async fn close(&self) -> Result<(), MessagingError> {
        self.plugin.close_workload(&self.workload_id).await
    }
}

fn to_routed(message: HostMessage) -> RoutedMessage {
    RoutedMessage {
        message: types::BrokerMessage {
            subject: message.subject,
            reply_to: message.reply_to,
            body: message.body,
        },
        propagation: message.trace_context.map(|context| PropagationContext {
            traceparent: context.traceparent,
            tracestate: context.tracestate,
        }),
        _activity: None,
    }
}

fn to_host(message: &RoutedMessage) -> HostMessage {
    HostMessage {
        subject: message.message.subject.clone(),
        reply_to: message.message.reply_to.clone(),
        body: message.message.body.clone(),
        trace_context: message.propagation.as_ref().map(|context| TraceContext {
            traceparent: context.traceparent.clone(),
            tracestate: context.tracestate.clone(),
        }),
    }
}

impl InMemoryMessaging {
    async fn observe_message(
        data: &WorkloadData,
        operation: ObservedOperation,
        message: HostMessage,
    ) {
        let mut senders = data.observers.write().await;
        senders.retain(|sender| {
            match sender.try_send(ObservedMessage {
                operation,
                origin: MessageOrigin::Guest,
                message: message.clone(),
            }) {
                Ok(()) | Err(mpsc::error::TrySendError::Full(_)) => true,
                Err(mpsc::error::TrySendError::Closed(_)) => false,
            }
        });
    }

    async fn publish_core(
        &self,
        workload_id: &str,
        message: HostMessage,
        origin: MessageOrigin,
    ) -> Result<(), MessagingError> {
        let data = self
            .tracker
            .read()
            .await
            .get_workload_data(workload_id)
            .cloned()
            .ok_or(MessagingError::NotBound)?;
        data.require_bound()?;
        let _operation = data.begin();
        let routed = to_routed(message.clone());
        let sender = data
            .pending_requests
            .lock()
            .unwrap()
            .remove(&message.subject);
        let is_reply =
            sender.is_some() || data.tombstones.lock().unwrap().contains(&message.subject);
        if is_reply {
            data.tombstones
                .lock()
                .unwrap()
                .insert(message.subject.clone());
            if origin == MessageOrigin::Guest {
                Self::observe_message(&data, ObservedOperation::Reply, message).await;
            }
            if let Some(sender) = sender {
                let _ = sender.send(routed);
            }
            return Ok(());
        }
        if origin == MessageOrigin::Guest {
            Self::observe_message(&data, ObservedOperation::Publish, message).await;
        }
        route_to_subscribers(self, workload_id, &data, &routed).await
    }

    async fn request_core(
        &self,
        workload_id: &str,
        mut message: HostMessage,
        timeout: Duration,
        origin: MessageOrigin,
    ) -> Result<HostMessage, MessagingError> {
        let data = self
            .tracker
            .read()
            .await
            .get_workload_data(workload_id)
            .cloned()
            .ok_or(MessagingError::NotBound)?;
        data.require_bound()?;
        let _request_activity = data.begin();
        let deadline = tokio::time::Instant::now() + timeout;
        let inbox = format!("_INBOX.{}", uuid::Uuid::new_v4());
        message.reply_to = Some(inbox.clone());
        let (sender, mut receiver) = oneshot::channel();
        data.pending_requests
            .lock()
            .unwrap()
            .insert(inbox.clone(), sender);
        let _pending_guard = PendingRequestGuard {
            inbox: inbox.clone(),
            pending: Arc::clone(&data.pending_requests),
        };
        data.tombstones.lock().unwrap().insert(inbox.clone());
        let mut responder_replies = tokio::task::JoinSet::new();
        if origin == MessageOrigin::Guest {
            Self::observe_message(&data, ObservedOperation::Request, message.clone()).await;
            let mut responders = data.responders.write().await;
            responders.retain(|(pattern, responder)| {
                if !subject_matches(pattern, &message.subject) {
                    return !responder.is_closed();
                }
                let (reply_sender, reply_receiver) = oneshot::channel();
                match responder.try_send(ResponderRequest {
                    message: message.clone(),
                    reply: Some(reply_sender),
                }) {
                    Ok(()) => {
                        responder_replies.spawn(async move { reply_receiver.await.ok() });
                        true
                    }
                    Err(mpsc::error::TrySendError::Full(_)) => true,
                    Err(mpsc::error::TrySendError::Closed(_)) => false,
                }
            });
        }
        let routed = to_routed(message);
        if let Err(error) = route_to_subscribers(self, workload_id, &data, &routed).await {
            responder_replies.abort_all();
            while responder_replies.join_next().await.is_some() {}
            return Err(error);
        }
        let result = loop {
            tokio::select! {
                reply = &mut receiver => break reply.map_err(|_| MessagingError::Closed).map(|reply| to_host(&reply)),
                responder = responder_replies.join_next(), if !responder_replies.is_empty() => {
                    if let Some(Ok(Some(reply))) = responder {
                        self.publish_core(workload_id, HostMessage { subject: inbox.clone(), ..reply }, MessageOrigin::Guest).await?;
                    }
                }
                _ = tokio::time::sleep_until(deadline) => break Err(MessagingError::Timeout),
            }
        };
        responder_replies.abort_all();
        while responder_replies.join_next().await.is_some() {}
        result
    }

    async fn close_workload(&self, workload_id: &str) -> Result<(), MessagingError> {
        let data = self
            .tracker
            .read()
            .await
            .get_workload_data(workload_id)
            .cloned()
            .ok_or(MessagingError::Closed)?;
        let previous = data.lifecycle.swap(CLOSING, Ordering::AcqRel);
        if previous == CLOSED {
            return Ok(());
        }
        data.pending_requests.lock().unwrap().clear();
        data.observers.write().await.clear();
        data.responders.write().await.clear();
        data.lifecycle.store(CLOSED, Ordering::Release);
        data.activity_changed.notify_waiters();
        Ok(())
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
        let span = producer_span("request", &subject, body.len(), parent_context.clone());
        let message = HostMessage {
            subject,
            reply_to: None,
            body,
            trace_context: parent_context.map(|context| TraceContext {
                traceparent: context.traceparent,
                tracestate: context.tracestate,
            }),
        };
        match plugin
            .request_core(
                &workload_id,
                message,
                Duration::from_millis(timeout_ms.into()),
                MessageOrigin::Guest,
            )
            .await
        {
            Ok(message) => {
                span.record("messaging.operation.outcome", "success");
                Ok(Ok(types::BrokerMessage {
                    subject: message.subject,
                    reply_to: message.reply_to,
                    body: message.body,
                }))
            }
            Err(error) => {
                super::record_messaging_error(&span, "messaging-broker-failed", &error.to_string());
                Ok(Err(error.to_string()))
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
        let span = producer_span(
            "publish",
            &msg.subject,
            msg.body.len(),
            parent_context.clone(),
        );
        let message = HostMessage {
            subject: msg.subject,
            reply_to: msg.reply_to,
            body: msg.body,
            trace_context: parent_context.map(|context| TraceContext {
                traceparent: context.traceparent,
                tracestate: context.tracestate,
            }),
        };
        match plugin
            .publish_core(&workload_id, message, MessageOrigin::Guest)
            .await
        {
            Ok(()) => {
                span.record("messaging.operation.outcome", "success");
                Ok(Ok(()))
            }
            Err(error) => {
                super::record_messaging_error(&span, "messaging-broker-failed", &error.to_string());
                Ok(Err(error.to_string()))
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

        let mut tracker = self.tracker.write().await;
        if let Some(data) = tracker.get_workload_data(workload.id()) {
            data.lifecycle
                .compare_exchange(RESERVED, BOUND, Ordering::AcqRel, Ordering::Acquire)
                .map_err(|state| {
                    anyhow::anyhow!("workload cannot be bound from lifecycle state {state}")
                })?;
            return Ok(());
        }
        tracker.add_unresolved_workload(workload, Arc::new(WorkloadData::new(BOUND)));
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
            let (inbox, receiver) = mpsc::channel(MAX_QUEUE_SIZE);
            self.tracker.write().await.add_component(
                component_handle,
                ComponentData {
                    cancel_token: tokio_util::sync::CancellationToken::new(),
                    task_handle: None,
                    subscriptions,
                    inbox,
                    receiver: Some(receiver),
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
        let (mut inbox, cancel_token) = {
            let mut tracker = self.tracker.write().await;
            let Some(data) = tracker.get_component_data_mut(component_id) else {
                return Ok(());
            };
            let Some(receiver) = data.receiver.take() else {
                anyhow::bail!("messaging processor already started");
            };
            (receiver, data.cancel_token.clone())
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
            let mut handlers = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        handlers.abort_all();
                        while handlers.join_next().await.is_some() {}
                        break;
                    }
                    _ = handlers.join_next(), if !handlers.is_empty() => {}
                    message = inbox.recv() => {
                        let Some(msg) = message else {
                            while handlers.join_next().await.is_some() {}
                            break;
                        };
                        let workload = workload.clone();
                        let component_id = component_id.clone();
                        let pre = pre.clone();
                        let fuel_meter = fuel_meter.clone();
                        handlers.spawn(async move {
                        let parent = msg.propagation.as_ref().and_then(|context| context_from_propagation(context).ok());
                        let invalid_parent = msg.propagation.as_ref().is_some_and(|context| context_from_propagation(context).is_err());
                        let _activity_guard = msg._activity;
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
                            return;
                        }

                        let Some(pre) = &pre else {
                            warn!(
                                subject = %msg.subject,
                                component_id = %component_id,
                                "no trigger service registered and no per-message instance; dropping message"
                            );
                            return;
                        };
                        let mut store = match workload.new_store(&component_id).await {
                            Err(error) => {
                                super::record_messaging_error(&span, "messaging-consumer-setup-failed", &error.to_string());
                                warn!("failed to create store for component {component_id}: {error}");
                                return;
                            }
                            Ok(store) => store,
                        };

                        let proxy = match pre.instantiate_async(&mut store).await {
                            Err(error) => {
                                super::record_messaging_error(&span, "messaging-consumer-setup-failed", &error.to_string());
                                warn!("failed to instantiate component {component_id}: {error}");
                                return;
                            }
                            Ok(proxy) => proxy,
                        };

                        let fuel_meter = fuel_meter.clone();

                        {
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
                        }
                        });
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
        let item = {
            let mut tracker = self.tracker.write().await;
            let item = tracker.workloads.remove(workload_id);
            if let Some(item) = &item {
                for component_id in item.components.keys() {
                    tracker.components.remove(component_id);
                }
            }
            item
        };
        let Some(item) = item else {
            return Ok(());
        };
        let data = item.workload_data;
        if let Some(data) = &data {
            data.lifecycle.store(CLOSING, Ordering::Release);
        }
        let shutdown = tokio::spawn(async move {
            if let Some(data) = &data {
                data.pending_requests.lock().unwrap().clear();
            }
            for component in item.components.into_values() {
                component.cancel_token.cancel();
                if let Some(handle) = component.task_handle {
                    let _ = handle.await;
                }
            }
            if let Some(data) = data {
                data.lifecycle.store(CLOSED, Ordering::Release);
                data.activity_changed.notify_waiters();
            }
        });
        shutdown.await.context("messaging shutdown task failed")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BOUND, HostMessage, InMemoryMessaging, InMemoryMessagingDriver, MessageOrigin,
        MessagingError, ObservedOperation, RoutedMessage, subject_matches, subscriptions_match,
        types,
    };
    use crate::observability::{PropagationContext, context_from_propagation, inject_context};
    use opentelemetry::trace::{
        SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState,
    };
    use std::sync::atomic::Ordering;
    use std::time::Duration;

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
            _activity: None,
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
            _activity: None,
        };
        assert!(missing.propagation.is_none());
        let malformed = RoutedMessage {
            message,
            propagation: Some(PropagationContext {
                traceparent: "malformed".into(),
                tracestate: None,
            }),
            _activity: None,
        };
        assert!(context_from_propagation(malformed.propagation.as_ref().unwrap()).is_err());
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

    async fn bound_driver() -> (InMemoryMessaging, InMemoryMessagingDriver) {
        let plugin = InMemoryMessaging::new();
        let driver = plugin.reserve_workload("test").await.unwrap();
        let data = plugin
            .tracker
            .read()
            .await
            .get_workload_data("test")
            .cloned()
            .unwrap();
        data.lifecycle.store(BOUND, Ordering::Release);
        (plugin, driver)
    }

    fn message(subject: &str, body: &[u8]) -> HostMessage {
        HostMessage {
            subject: subject.to_string(),
            reply_to: None,
            body: body.to_vec(),
            trace_context: None,
        }
    }

    #[tokio::test]
    async fn host_injection_is_not_observed_but_guest_publish_is() {
        let (plugin, driver) = bound_driver().await;
        let mut observations = driver.observe(4).await.unwrap();
        driver.publish(message("events", b"host")).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), observations.recv())
                .await
                .is_err()
        );
        plugin
            .publish_core("test", message("events", b"guest"), MessageOrigin::Guest)
            .await
            .unwrap();
        let observed = tokio::time::timeout(Duration::from_secs(1), observations.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(observed.operation, ObservedOperation::Publish);
        assert_eq!(observed.message.body, b"guest");
    }

    #[tokio::test]
    async fn explicit_responder_replies_to_guest_request() {
        let (plugin, driver) = bound_driver().await;
        let mut responder = driver.register_responder("rpc.*", 1).await.unwrap();
        let response = tokio::spawn(async move {
            let request = responder.recv().await.unwrap();
            request.reply(message("ignored", b"reply")).unwrap();
        });
        let reply = plugin
            .request_core(
                "test",
                message("rpc.echo", b"request"),
                Duration::from_secs(1),
                MessageOrigin::Guest,
            )
            .await
            .unwrap();
        response.await.unwrap();
        assert_eq!(reply.body, b"reply");
    }

    #[tokio::test]
    async fn request_timeout_cleans_pending_and_idle_activity() {
        let (plugin, driver) = bound_driver().await;
        let result = driver
            .request(message("nobody", b"request"), Duration::from_millis(10))
            .await;
        assert_eq!(result, Err(MessagingError::Timeout));
        driver.wait_idle().await.unwrap();
        let data = plugin
            .tracker
            .read()
            .await
            .get_workload_data("test")
            .cloned()
            .unwrap();
        assert!(data.pending_requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn dropped_request_cleans_pending_synchronously() {
        let (plugin, driver) = bound_driver().await;
        let request = tokio::spawn(async move {
            driver
                .request(message("nobody", b"request"), Duration::from_secs(60))
                .await
        });
        tokio::task::yield_now().await;
        request.abort();
        let _ = request.await;
        let data = plugin
            .tracker
            .read()
            .await
            .get_workload_data("test")
            .cloned()
            .unwrap();
        assert!(data.pending_requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn full_observer_and_responder_do_not_delay_request_deadline() {
        let (plugin, driver) = bound_driver().await;
        let _observer = driver.observe(1).await.unwrap();
        plugin
            .publish_core("test", message("event", b"first"), MessageOrigin::Guest)
            .await
            .unwrap();
        plugin
            .publish_core("test", message("event", b"second"), MessageOrigin::Guest)
            .await
            .unwrap();

        let _responder = driver.register_responder("rpc", 1).await.unwrap();
        let started = tokio::time::Instant::now();
        let result = plugin
            .request_core(
                "test",
                message("rpc", b"request"),
                Duration::from_millis(20),
                MessageOrigin::Guest,
            )
            .await;
        assert_eq!(result, Err(MessagingError::Timeout));
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn tombstones_evict_oldest_at_cap() {
        let mut tombstones = super::Tombstones {
            subjects: Default::default(),
            order: Default::default(),
        };
        for index in 0..=super::MAX_TOMBSTONES {
            tombstones.insert(index.to_string());
        }
        assert_eq!(tombstones.order.len(), super::MAX_TOMBSTONES);
        assert!(!tombstones.contains("0"));
        assert!(tombstones.contains(&super::MAX_TOMBSTONES.to_string()));
    }

    #[tokio::test]
    async fn registration_blocked_during_close_is_rejected() {
        let (plugin, driver) = bound_driver().await;
        let data = plugin
            .tracker
            .read()
            .await
            .get_workload_data("test")
            .cloned()
            .unwrap();
        let observers = data.observers.write().await;
        let registering = tokio::spawn({
            let driver = driver.clone();
            async move { driver.observe(1).await }
        });
        tokio::task::yield_now().await;
        let closing = tokio::spawn({
            let driver = driver.clone();
            async move { driver.close().await }
        });
        while data.lifecycle.load(Ordering::Acquire) != super::CLOSING {
            tokio::task::yield_now().await;
        }
        drop(observers);
        closing.await.unwrap().unwrap();
        assert!(matches!(
            registering.await.unwrap(),
            Err(MessagingError::Closed)
        ));
    }

    #[tokio::test]
    async fn reservation_prebind_duplicate_and_close() {
        let plugin = InMemoryMessaging::new();
        let driver = plugin.reserve_workload("test").await.unwrap();
        assert_eq!(
            driver.publish(message("event", b"body")).await,
            Err(MessagingError::NotBound)
        );
        assert!(matches!(
            plugin.reserve_workload("test").await,
            Err(MessagingError::AlreadyReserved)
        ));
        let data = plugin
            .tracker
            .read()
            .await
            .get_workload_data("test")
            .cloned()
            .unwrap();
        data.lifecycle.store(BOUND, Ordering::Release);
        let mut observer = driver.observe(1).await.unwrap();
        let mut responder = driver.register_responder("event", 1).await.unwrap();
        driver.close().await.unwrap();
        assert_eq!(observer.recv().await, None);
        assert!(responder.recv().await.is_none());
        assert_eq!(
            driver.publish(message("event", b"body")).await,
            Err(MessagingError::Closed)
        );
        assert!(matches!(
            driver.observe(1).await,
            Err(MessagingError::Closed)
        ));
        assert!(matches!(
            driver.register_responder("event", 1).await,
            Err(MessagingError::Closed)
        ));
    }
}
