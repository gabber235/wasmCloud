//! # WASI OpenTelemetry Plugin
//! This module implements an OpenTelemetry plugin for the wasmCloud runtime,
//! providing the `wasi:otel@0.2.0-rc.2` interfaces.

mod convert;

pub use convert::otel_span_context_to_wit;
use convert::{
    convert_wasi_log_record, extract_counter_values, extract_gauge_values,
    summarize_resource_metrics, summarize_span_data, try_into_sdk_span_data,
};

use anyhow::bail;
use opentelemetry::KeyValue;
use opentelemetry::logs::{Logger, LoggerProvider};
use opentelemetry::trace::TraceContextExt;
use opentelemetry_sdk::logs::{BatchLogProcessor, SdkLoggerProvider};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::trace::SpanData;
use opentelemetry_sdk::trace::{BatchSpanProcessor, SpanProcessor};
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use opentelemetry_otlp::{LogExporter, MetricExporter, SpanExporter};

use crate::engine::ctx::{ActiveCtx, SharedCtx, extract_active_ctx};
use crate::plugin::{HostPlugin, WitInterfaces, WorkloadItem, WorkloadTracker};
use crate::wit::{WitInterface, WitWorld};

const WASI_OTEL_ID: &str = "wasi-otel";
const SPAN_SUBMISSION_QUEUE_CAPACITY: usize = 2_048;
const QUEUE_FULL_DIAGNOSTIC_INTERVAL: Duration = Duration::from_secs(30);

/// OTel gRPC default per the OTLP/gRPC spec. Matches what
/// `opentelemetry_otlp::SpanExporter::builder().with_tonic()` falls back to
/// when no `OTEL_EXPORTER_OTLP_*_ENDPOINT` is set; duplicated here only so
/// the log line at plugin start reflects what the exporter actually used.
const DEFAULT_OTLP_GRPC_ENDPOINT: &str = "http://localhost:4317";

mod bindings {
    wasmtime::component::bindgen!({
        world: "otel",
        imports: { default: async | trappable },
    });
}

use bindings::wasi::otel::tracing::{SpanContext as WitSpanContext, TraceFlags as WitTraceFlags};

/// Configuration for the [`WasiOtel`] plugin.
///
/// Construct via [`WasiOtelConfig::builder`] (or [`Default::default`]) to
/// stay forward-compatible with new fields.
///
/// # Examples
///
/// ```
/// use wash_runtime::plugin::wasi_otel::WasiOtelConfig;
///
/// let cfg = WasiOtelConfig::builder()
///     .service_name("my-service")
///     .build();
/// assert_eq!(cfg.service_name, "my-service");
/// ```
#[derive(Clone, Debug, bon::Builder)]
#[non_exhaustive]
pub struct WasiOtelConfig {
    /// `service.name` resource attribute attached to all exported spans,
    /// metrics, and logs. Defaults to the plugin id (`wasi-otel`).
    #[builder(default = WASI_OTEL_ID.to_string(), into)]
    pub service_name: String,
}

impl Default for WasiOtelConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

enum SpanSubmission {
    Span(SpanData),
    Shutdown,
}

struct ComponentContext {
    component_id: Arc<str>,
    submissions: mpsc::SyncSender<SpanSubmission>,
    dropped_spans: Arc<AtomicU64>,
    last_queue_full_diagnostic: AtomicU64,
    worker: JoinHandle<()>,
}

impl ComponentContext {
    fn submit(&self, span: SpanData) {
        if self
            .submissions
            .try_send(SpanSubmission::Span(span))
            .is_ok()
        {
            return;
        }

        self.record_queue_rejection();
    }

    fn record_queue_rejection(&self) {
        let dropped = self.dropped_spans.fetch_add(1, Ordering::Relaxed) + 1;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let previous = self.last_queue_full_diagnostic.load(Ordering::Relaxed);
        if now.saturating_sub(previous) < QUEUE_FULL_DIAGNOSTIC_INTERVAL.as_secs()
            || self
                .last_queue_full_diagnostic
                .compare_exchange(previous, now, Ordering::Relaxed, Ordering::Relaxed)
                .is_err()
        {
            return;
        }
        tracing::warn!(
            component_id = %self.component_id,
            wasi_otel_export_queue_dropped_spans = dropped,
            exception.slug = "wasi-otel-export-queue-full",
            "wasi-otel-export-queue-full"
        );
    }

    fn shutdown(self) {
        let _ = self.submissions.send(SpanSubmission::Shutdown);
        if self.worker.join().is_err() {
            tracing::warn!(
                exception.slug = "wasi-otel-component-shutdown-failed",
                "Failed to join component span exporter"
            );
        }
    }
}

fn component_context(
    component_id: Arc<str>,
    span_processor: BatchSpanProcessor,
    capacity: usize,
) -> ComponentContext {
    let (submissions, receiver) = mpsc::sync_channel(capacity);
    let dropped_spans = Arc::new(AtomicU64::new(0));
    let worker = std::thread::spawn(move || {
        while let Ok(SpanSubmission::Span(span)) = receiver.recv() {
            span_processor.on_end(span);
        }
        if let Err(error) = span_processor.force_flush() {
            tracing::warn!(error = %error, exception.slug = "wasi-otel-component-flush-failed", "Failed to flush component spans");
        }
        if let Err(error) = span_processor.shutdown() {
            tracing::warn!(error = %error, exception.slug = "wasi-otel-component-shutdown-failed", "Failed to shut down component span processor");
        }
    });
    ComponentContext {
        component_id,
        submissions,
        dropped_spans,
        last_queue_full_diagnostic: AtomicU64::new(0),
        worker,
    }
}

fn component_resource(
    config: &WasiOtelConfig,
    component_id: &str,
    workload_id: &str,
    workload_name: &str,
    workload_namespace: &str,
) -> opentelemetry_sdk::Resource {
    let configured_name = config.service_name.trim();
    let service_name = if configured_name.is_empty() || configured_name == WASI_OTEL_ID {
        workload_name
    } else {
        configured_name
    };

    opentelemetry_sdk::Resource::builder_empty()
        .with_attributes([
            KeyValue::new("service.name", service_name.to_string()),
            KeyValue::new("service.namespace", workload_namespace.to_string()),
            KeyValue::new("service.instance.id", workload_id.to_string()),
            KeyValue::new("wasmcloud.workload.name", workload_name.to_string()),
            KeyValue::new("wasmcloud.component.id", component_id.to_string()),
        ])
        .build()
}

/// WASI OpenTelemetry Plugin
pub struct WasiOtel {
    config: WasiOtelConfig,
    tracker: Arc<RwLock<WorkloadTracker<(), ComponentContext>>>,
    /// Meter provider for metrics export
    meter_provider: Arc<RwLock<Option<SdkMeterProvider>>>,
    logger_provider: Arc<RwLock<Option<SdkLoggerProvider>>>,
}

impl Default for WasiOtel {
    fn default() -> Self {
        Self {
            config: WasiOtelConfig::default(),
            tracker: Arc::new(RwLock::new(WorkloadTracker::default())),
            meter_provider: Arc::new(RwLock::new(None)),
            logger_provider: Arc::new(RwLock::new(None)),
        }
    }
}

#[async_trait::async_trait]
impl HostPlugin for WasiOtel {
    fn id(&self) -> &'static str {
        WASI_OTEL_ID
    }

    fn world(&self) -> WitWorld {
        WitWorld {
            imports: HashSet::from([WitInterface::from(
                "wasi:otel/types,tracing,metrics,logs@0.2.0-rc.2",
            )]),
            ..Default::default()
        }
    }

    async fn start(&self) -> anyhow::Result<()> {
        // The exporter resolves its endpoint from `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`,
        // then `OTEL_EXPORTER_OTLP_ENDPOINT`, falling back to the OTel gRPC default
        // ([`DEFAULT_OTLP_GRPC_ENDPOINT`]). Protocol is fixed to gRPC because we use
        // `with_tonic()` below; tracking richer per-target endpoint configuration as a
        // follow-up to this PR (see TODO below).
        let endpoint = std::env::var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
            .or_else(|_| std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT"))
            .unwrap_or_else(|_| DEFAULT_OTLP_GRPC_ENDPOINT.to_string());
        tracing::info!(
            endpoint = %endpoint,
            protocol = "grpc",
            "Starting WASI OTel plugin"
        );

        // TODO: thread per-target endpoints (host vs workload) through `WasiOtelConfig`
        // so platform telemetry and application telemetry can ship to different backends.

        // set up the grpc log exporter
        let log_exporter = LogExporter::builder()
            .with_tonic()
            //.with_endpoint("http://localhost:5318")
            //.with_protocol(opentelemetry_otlp::Protocol::Grpc)
            .build()?;

        // set up metric exporter
        let metric_exporter = MetricExporter::builder()
            .with_tonic()
            //.with_endpoint("http://localhost:5318")
            //.with_protocol(opentelemetry_otlp::Protocol::Grpc)
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to create metric exporter: {e}"))?;

        let processor = BatchLogProcessor::builder(log_exporter).build();
        let logger_provider = opentelemetry_sdk::logs::LoggerProviderBuilder::default()
            .with_log_processor(processor)
            .with_resource(
                opentelemetry_sdk::Resource::builder_empty()
                    .with_attributes([KeyValue::new(
                        "service.name",
                        self.config.service_name.clone(),
                    )])
                    .build(),
            )
            .build();
        let meter_provider = SdkMeterProvider::builder()
            .with_periodic_exporter(metric_exporter)
            .with_resource(
                opentelemetry_sdk::Resource::builder_empty()
                    .with_attributes([KeyValue::new(
                        "service.name",
                        self.config.service_name.clone(),
                    )])
                    .build(),
            )
            .build();

        *self.logger_provider.write().await = Some(logger_provider);
        *self.meter_provider.write().await = Some(meter_provider);

        tracing::info!("WASI OTel plugin started");
        Ok(())
    }

    async fn on_workload_item_bind<'a>(
        &self,
        component_handle: &mut WorkloadItem<'a>,
        _interfaces: WitInterfaces<'_>,
    ) -> anyhow::Result<()> {
        // Add all wasi:otel interfaces to linker
        bindings::wasi::otel::types::add_to_linker::<_, SharedCtx>(
            component_handle.linker(),
            extract_active_ctx,
        )?;
        bindings::wasi::otel::tracing::add_to_linker::<_, SharedCtx>(
            component_handle.linker(),
            extract_active_ctx,
        )?;
        bindings::wasi::otel::metrics::add_to_linker::<_, SharedCtx>(
            component_handle.linker(),
            extract_active_ctx,
        )?;
        bindings::wasi::otel::logs::add_to_linker::<_, SharedCtx>(
            component_handle.linker(),
            extract_active_ctx,
        )?;

        let WorkloadItem::Component(component_handle) = component_handle else {
            bail!("Service can not be tracked");
        };

        let span_exporter = SpanExporter::builder()
            .with_tonic()
            .build()
            .map_err(|error| {
                anyhow::anyhow!("Failed to create component span exporter: {error}")
            })?;
        let mut span_processor = BatchSpanProcessor::builder(span_exporter).build();
        span_processor.set_resource(&component_resource(
            &self.config,
            component_handle.id(),
            component_handle.workload_id(),
            component_handle.workload_name(),
            component_handle.workload_namespace(),
        ));

        let context = component_context(
            Arc::from(component_handle.id()),
            span_processor,
            SPAN_SUBMISSION_QUEUE_CAPACITY,
        );
        self.tracker
            .write()
            .await
            .add_component(component_handle, context);

        tracing::info!(
            component_id = component_handle.id(),
            "WASI OTel interfaces bound to component"
        );
        Ok(())
    }

    async fn on_workload_unbind(
        &self,
        workload_id: &str,
        _interfaces: WitInterfaces<'_>,
    ) -> anyhow::Result<()> {
        self.tracker
            .write()
            .await
            .remove_workload_with_cleanup(
                workload_id,
                |_| async {},
                |ctx| async move {
                    ctx.shutdown();
                },
            )
            .await;
        tracing::info!(workload_id, "WASI OTel unbound from workload");
        Ok(())
    }

    async fn stop(&self) -> anyhow::Result<()> {
        tracing::info!("Stopping WASI OTel plugin");

        let mut tracker = self.tracker.write().await;
        let workload_ids = tracker.workloads.keys().cloned().collect::<Vec<_>>();
        for workload_id in workload_ids {
            tracker
                .remove_workload_with_cleanup(
                    &workload_id,
                    |_| async {},
                    |ctx| async move {
                        ctx.shutdown();
                    },
                )
                .await;
        }
        drop(tracker);

        // Flush and shutdown all providers
        if let Some(provider) = self.logger_provider.write().await.take() {
            let _ = provider.shutdown();
        }
        if let Some(provider) = self.meter_provider.write().await.take() {
            let _ = provider.shutdown();
        }

        tracing::info!("WASI OTel plugin stopped");
        Ok(())
    }
}

// OTel Logs
impl<'a> bindings::wasi::otel::logs::Host for ActiveCtx<'a> {
    async fn on_emit(
        &mut self,
        data: bindings::wasi::otel::logs::LogRecord,
    ) -> wasmtime::Result<()> {
        tracing::info!(?data, "emitting log record");
        if let Ok(plugin) = self.ctx.try_get_plugin::<WasiOtel>(WASI_OTEL_ID) {
            let service_name = plugin.config.service_name.clone();
            let provider = plugin.logger_provider.read().await;

            if let Some(ref provider) = *provider {
                let logger = provider.logger(service_name.clone());
                let mut otel_record = logger.create_log_record();
                convert_wasi_log_record(data, &mut otel_record, service_name.clone());
                logger.emit(otel_record);
            }
        }
        Ok(())
    }
}

// OTel Metrics
impl<'a> bindings::wasi::otel::metrics::Host for ActiveCtx<'a> {
    async fn export(
        &mut self,
        resource_metrics: bindings::wasi::otel::metrics::ResourceMetrics,
    ) -> wasmtime::Result<Result<(), bindings::wasi::otel::metrics::Error>> {
        if let Ok(plugin) = self.ctx.try_get_plugin::<WasiOtel>(WASI_OTEL_ID) {
            // Summarize incoming metrics for logging
            let summary = summarize_resource_metrics(&resource_metrics);
            tracing::info!(
                total_scopes = summary.total_scopes,
                total_metrics = summary.total_metrics,
                metric_names = ?summary.metric_names,
                "Processing WASI resource metrics"
            );

            // Get the meter provider to record values
            let provider_guard = plugin.meter_provider.read().await;
            if let Some(ref provider) = *provider_guard {
                use opentelemetry::metrics::MeterProvider;
                let meter = provider.meter("wasi-otel");

                // Record gauge values
                for (name, value, attrs) in extract_gauge_values(&resource_metrics) {
                    let gauge = meter.f64_gauge(name).build();
                    let kv_attrs: Vec<KeyValue> = attrs
                        .into_iter()
                        .map(|(k, v)| KeyValue::new(k, v))
                        .collect();
                    gauge.record(value, &kv_attrs);
                }

                // Record counter values
                for (name, value, is_monotonic, attrs) in extract_counter_values(&resource_metrics)
                {
                    let kv_attrs: Vec<KeyValue> = attrs
                        .into_iter()
                        .map(|(k, v)| KeyValue::new(k, v))
                        .collect();
                    if is_monotonic {
                        let counter = meter.f64_counter(name).build();
                        counter.add(value, &kv_attrs);
                    } else {
                        let up_down = meter.f64_up_down_counter(name).build();
                        up_down.add(value, &kv_attrs);
                    }
                }

                // Force flush to export recorded metrics
                if let Err(e) = provider.force_flush() {
                    tracing::warn!(error = %e, "Failed to flush metrics");
                    return Ok(Err(format!("Failed to flush metrics: {e}")));
                }

                tracing::info!(
                    total_metrics = summary.total_metrics,
                    "Successfully processed WASI metrics"
                );
            } else {
                tracing::warn!("Meter provider not initialized");
                return Ok(Err("Meter provider not initialized".to_string()));
            }
        }

        Ok(Ok(()))
    }
}

// OTel Tracing
impl<'a> bindings::wasi::otel::tracing::Host for ActiveCtx<'a> {
    async fn on_start(
        &mut self,
        span_context: bindings::wasi::otel::tracing::SpanContext,
    ) -> wasmtime::Result<()> {
        // Log the span start - the actual span is managed by the guest
        tracing::info!(
            trace_id = %span_context.trace_id,
            span_id = %span_context.span_id,
            is_remote = span_context.is_remote,
            "WASI span started"
        );
        Ok(())
    }

    async fn on_end(
        &mut self,
        span_data: bindings::wasi::otel::tracing::SpanData,
    ) -> wasmtime::Result<()> {
        let outer_context = tracing::Span::current().context();
        let outer_span_context = outer_context.span().span_context().clone();
        if let Ok(plugin) = self.ctx.try_get_plugin::<WasiOtel>(WASI_OTEL_ID) {
            let summary = summarize_span_data(&span_data);
            tracing::info!(
                name = %summary.name,
                trace_id = %summary.trace_id,
                span_id = %summary.span_id,
                parent_span_id = %summary.parent_span_id,
                kind = %summary.kind,
                status = %summary.status,
                attribute_count = summary.attribute_count,
                event_count = summary.event_count,
                link_count = summary.link_count,
                "Processing WASI span end"
            );

            let tracker = plugin.tracker.read().await;
            if let Some(component) = tracker.get_component_data(&self.component_id.to_string()) {
                match try_into_sdk_span_data(span_data, Some(&outer_span_context)) {
                    Ok(span) => component.submit(span),
                    Err(error) => tracing::warn!(
                        error = %error,
                        exception.slug = "wasi-otel-invalid-span-data",
                        "Dropping invalid WASI span data"
                    ),
                }
            } else {
                tracing::warn!(
                    exception.slug = "wasi-otel-component-not-bound",
                    "Dropping span for unbound component"
                );
            }
        }
        Ok(())
    }

    async fn outer_span_context(&mut self) -> wasmtime::Result<WitSpanContext> {
        // Try to get the current span context from the OpenTelemetry context
        use opentelemetry::trace::TraceContextExt;
        let current_context = opentelemetry::Context::current();
        let span_context = current_context.span().span_context().clone();

        if span_context.is_valid() {
            tracing::info!(
                trace_id = %format!("{:032x}", span_context.trace_id()),
                span_id = %format!("{:016x}", span_context.span_id()),
                "Returning outer span context"
            );
            Ok(otel_span_context_to_wit(&span_context))
        } else {
            tracing::info!("No valid outer span context available");
            Ok(WitSpanContext {
                trace_id: String::new(),
                span_id: String::new(),
                trace_flags: WitTraceFlags::empty(),
                is_remote: false,
                trace_state: vec![],
            })
        }
    }
}

impl<'a> bindings::wasi::otel::types::Host for ActiveCtx<'a> {}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::sync::Mutex;

    use opentelemetry::InstrumentationScope;
    use opentelemetry::trace::{
        SpanContext, SpanId, SpanKind, Status, TraceFlags, TraceId, TraceState,
    };
    use opentelemetry_sdk::error::OTelSdkResult;
    use opentelemetry_sdk::trace::{SpanEvents, SpanExporter, SpanLinks};

    use super::*;

    #[derive(Clone, Debug, Default)]
    struct TestExporter {
        spans: Arc<Mutex<Vec<SpanData>>>,
        resource: Arc<Mutex<Option<opentelemetry_sdk::Resource>>>,
    }

    impl SpanExporter for TestExporter {
        async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
            self.spans.lock().unwrap().extend(batch);
            Ok(())
        }

        fn set_resource(&mut self, resource: &opentelemetry_sdk::Resource) {
            *self.resource.lock().unwrap() = Some(resource.clone());
        }
    }

    fn span(trace_id: u128, span_id: u64, parent_id: u64, name: &'static str) -> SpanData {
        SpanData {
            span_context: SpanContext::new(
                TraceId::from(trace_id),
                SpanId::from(span_id),
                TraceFlags::SAMPLED,
                false,
                TraceState::default(),
            ),
            parent_span_id: SpanId::from(parent_id),
            parent_span_is_remote: false,
            span_kind: SpanKind::Internal,
            name: Cow::Borrowed(name),
            start_time: SystemTime::now(),
            end_time: SystemTime::now(),
            attributes: vec![],
            dropped_attributes_count: 0,
            events: SpanEvents::default(),
            links: SpanLinks::default(),
            status: Status::Ok,
            instrumentation_scope: InstrumentationScope::builder("guest-test").build(),
        }
    }

    fn test_component(
        component_id: &str,
        resource: opentelemetry_sdk::Resource,
    ) -> (ComponentContext, TestExporter) {
        let exporter = TestExporter::default();
        let mut processor = BatchSpanProcessor::builder(exporter.clone()).build();
        processor.set_resource(&resource);
        (
            component_context(Arc::from(component_id), processor, 16),
            exporter,
        )
    }

    fn attribute(resource: &opentelemetry_sdk::Resource, key: &str) -> String {
        resource
            .iter()
            .find(|(candidate, _)| candidate.as_str() == key)
            .map(|(_, value)| value.to_string())
            .unwrap()
    }

    #[test]
    fn component_resources_have_distinct_namespace_and_name() {
        let config = WasiOtelConfig::default();
        let first = component_resource(&config, "random-a", "instance-a", "orders", "shop");
        let second = component_resource(&config, "random-b", "instance-b", "billing", "finance");

        assert_eq!(attribute(&first, "service.name"), "orders");
        assert_eq!(attribute(&first, "service.namespace"), "shop");
        assert_eq!(attribute(&second, "service.name"), "billing");
        assert_eq!(attribute(&second, "service.namespace"), "finance");
        assert_ne!(
            attribute(&first, "service.name"),
            attribute(&first, "service.namespace")
        );
    }

    #[test]
    fn component_resource_name_is_stable_across_random_instance_ids() {
        let config = WasiOtelConfig::builder().service_name("   ").build();
        let first = component_resource(&config, "random-a", "instance-a", "orders", "shop");
        let second = component_resource(&config, "random-b", "instance-b", "orders", "shop");

        assert_eq!(attribute(&first, "service.name"), "orders");
        assert_eq!(attribute(&second, "service.name"), "orders");
        assert_ne!(
            attribute(&first, "service.instance.id"),
            attribute(&second, "service.instance.id")
        );
    }

    #[test]
    fn validated_configured_service_name_takes_precedence() {
        let config = WasiOtelConfig::builder().service_name("orders").build();
        let resource = component_resource(&config, "component-id", "instance", "rollout", "shop");

        assert_eq!(attribute(&resource, "service.name"), "orders");
    }

    #[test]
    fn guest_spans_keep_exact_ids_resources_and_component_isolation_on_unbind() {
        let config = WasiOtelConfig::default();
        let first_resource =
            component_resource(&config, "component-a", "instance-a", "orders", "shop");
        let second_resource =
            component_resource(&config, "component-b", "instance-b", "billing", "finance");
        let (first, first_exporter) = test_component("component-a", first_resource);
        let (second, second_exporter) = test_component("component-b", second_resource);

        first.submit(span(0x1111, 0xaaaa, 0x1010, "first"));
        second.submit(span(0x2222, 0xbbbb, 0x2020, "second"));
        first.shutdown();
        second.shutdown();

        let first_spans = first_exporter.spans.lock().unwrap();
        let second_spans = second_exporter.spans.lock().unwrap();
        assert_eq!(first_spans.len(), 1);
        assert_eq!(second_spans.len(), 1);
        assert_eq!(
            first_spans[0].span_context.trace_id(),
            TraceId::from(0x1111)
        );
        assert_eq!(first_spans[0].span_context.span_id(), SpanId::from(0xaaaa));
        assert_eq!(first_spans[0].parent_span_id, SpanId::from(0x1010));
        assert_eq!(
            second_spans[0].span_context.trace_id(),
            TraceId::from(0x2222)
        );
        assert_eq!(second_spans[0].name, "second");
        drop(first_spans);
        drop(second_spans);

        let first_resource = first_exporter.resource.lock().unwrap();
        let second_resource = second_exporter.resource.lock().unwrap();
        assert_eq!(
            attribute(first_resource.as_ref().unwrap(), "service.name"),
            "orders"
        );
        assert_eq!(
            attribute(first_resource.as_ref().unwrap(), "service.namespace"),
            "shop"
        );
        assert_eq!(
            attribute(first_resource.as_ref().unwrap(), "wasmcloud.component.id"),
            "component-a"
        );
        assert_eq!(
            attribute(second_resource.as_ref().unwrap(), "service.name"),
            "billing"
        );
        assert_eq!(
            attribute(second_resource.as_ref().unwrap(), "wasmcloud.component.id"),
            "component-b"
        );
    }

    #[test]
    fn plugin_stop_style_shutdown_drains_every_component() {
        let resource = opentelemetry_sdk::Resource::builder_empty().build();
        let components = (0..2)
            .map(|id| {
                let (component, exporter) =
                    test_component(&format!("component-{id}"), resource.clone());
                component.submit(span(id + 1, id as u64 + 1, 9, "queued"));
                (component, exporter)
            })
            .collect::<Vec<_>>();
        let exporters = components
            .iter()
            .map(|(_, exporter)| exporter.clone())
            .collect::<Vec<_>>();

        for (component, _) in components {
            component.shutdown();
        }

        assert!(
            exporters
                .iter()
                .all(|exporter| exporter.spans.lock().unwrap().len() == 1)
        );
    }

    #[test]
    fn queue_rejections_are_counted_per_component() {
        let (submissions, _receiver) = mpsc::sync_channel(1);
        let dropped_spans = Arc::new(AtomicU64::new(0));
        let context = ComponentContext {
            component_id: Arc::from("component-a"),
            submissions,
            dropped_spans: dropped_spans.clone(),
            last_queue_full_diagnostic: AtomicU64::new(0),
            worker: std::thread::spawn(|| {}),
        };

        context.record_queue_rejection();
        context.record_queue_rejection();
        assert_eq!(dropped_spans.load(Ordering::Relaxed), 2);
        context.shutdown();
    }
}
