//! NATS backend for the multiplexed `wasmcloud:messaging` plugin.

use std::collections::HashMap;
use std::sync::Arc;

use crate::observability::PropagationContext;
use crate::plugin::multiplex::BackendProvider;
use tracing::Instrument;

use super::{BrokerMessage, MsgBackend, MsgId, TraceContext};

pub struct NatsMsgBackend {
    client: Arc<async_nats::Client>,
}

fn parent_context(value: TraceContext) -> PropagationContext {
    PropagationContext {
        traceparent: value.traceparent,
        tracestate: value.tracestate,
    }
}

#[async_trait::async_trait]
impl MsgBackend for NatsMsgBackend {
    async fn request(
        &self,
        subject: String,
        body: Vec<u8>,
        timeout_ms: u32,
        parent: Option<TraceContext>,
    ) -> Result<BrokerMessage, String> {
        let parent = parent.map(parent_context);
        let span = super::super::nats::producer_span_with_parent(
            "request",
            &subject,
            body.len(),
            parent.clone(),
        );
        let headers = super::super::nats::headers_for_span(&span, parent);
        let timeout = std::time::Duration::from_millis(timeout_ms as u64);
        let result = async {
            match tokio::time::timeout(
                timeout,
                self.client
                    .request_with_headers(subject, headers, body.into()),
            )
            .await
            {
                Ok(Ok(msg)) => Ok(BrokerMessage {
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
                super::super::record_messaging_error(&span, slug, error);
            }
        }
        result
    }

    async fn publish(
        &self,
        msg: BrokerMessage,
        parent: Option<TraceContext>,
    ) -> Result<(), String> {
        let parent = parent.map(parent_context);
        let span = super::super::nats::producer_span_with_parent(
            "publish",
            &msg.subject,
            msg.body.len(),
            parent.clone(),
        );
        let headers = super::super::nats::headers_for_span(&span, parent);
        let result = async {
            if let Some(reply_to) = msg.reply_to {
                self.client
                    .publish_with_reply_and_headers(msg.subject, reply_to, headers, msg.body.into())
                    .await
            } else {
                self.client
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
                super::super::record_messaging_error(&span, "messaging-broker-failed", error)
            }
        }
        result
    }
}

#[derive(Default)]
pub struct NatsMsgProvider;

#[async_trait::async_trait]
impl BackendProvider<MsgId> for NatsMsgProvider {
    fn pool_key(&self, config: &HashMap<String, String>) -> Option<String> {
        config.get("url").cloned()
    }

    fn backend_type(&self) -> &'static str {
        "nats"
    }

    async fn instantiate(&self, config: &HashMap<String, String>) -> anyhow::Result<MsgId> {
        let url = config
            .get("url")
            .ok_or_else(|| anyhow::anyhow!("nats messaging backend requires a 'url' config"))?;
        let client = async_nats::connect(url).await?;
        Ok(Arc::new(NatsMsgBackend {
            client: Arc::new(client),
        }))
    }
}
