//! # WASI Logging Plugin
//!
//! This module routes logging calls from WASI components to the host's tracing
//! system. It implements the `wasi:logging/logging` interface, allowing
//! components to log messages at various levels (trace, debug, info, warn,
//! error, critical).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::engine::ctx::{ActiveCtx, SharedCtx, extract_active_ctx};
use crate::engine::workload::WorkloadItem;
use crate::plugin::{HostPlugin, WitInterfaces};
use crate::wit::{WitInterface, WitWorld};
use tracing::instrument;

pub(crate) const PLUGIN_LOGGING_ID: &str = "wasi-logging";

mod bindings {
    crate::wasmtime::component::bindgen!({
        world: "logging",
        imports: { default: async | trappable | tracing },
    });
}

pub use bindings::wasi::logging::logging::Level;
use tokio::sync::RwLock;

type ComponentMap = Arc<RwLock<HashMap<String, ComponentInfo>>>;

/// A log event emitted by a workload component.
#[derive(Clone, Debug)]
pub struct LogRecord {
    pub level: Level,
    pub context: String,
    pub message: String,
    pub workload_name: String,
    pub workload_namespace: String,
    pub component_id: String,
}

/// A synchronous destination for component log events.
pub trait LogSink: Send + Sync + 'static {
    fn log(&self, record: LogRecord);
}

impl<F> LogSink for F
where
    F: Fn(LogRecord) + Send + Sync + 'static,
{
    fn log(&self, record: LogRecord) {
        self(record);
    }
}

#[derive(Default)]
pub struct TracingLogger {
    components: ComponentMap,
    sink: Option<Arc<dyn LogSink>>,
}

impl TracingLogger {
    pub fn with_sink(sink: impl LogSink) -> Self {
        Self {
            components: ComponentMap::default(),
            sink: Some(Arc::new(sink)),
        }
    }
}

#[derive(Clone)]
struct ComponentInfo {
    workload_id: String,
    workload_name: String,
    workload_namespace: String,
}

impl<'a> bindings::wasi::logging::logging::Host for ActiveCtx<'a> {
    #[instrument(name = "wasi.logging.log", skip(self, message))]
    async fn log(
        &mut self,
        level: Level,
        context: String,
        message: String,
    ) -> wasmtime::Result<()> {
        let plugin = self.try_get_plugin::<TracingLogger>(PLUGIN_LOGGING_ID)?;

        // A call still running when its workload unbinds finds no entry; its
        // message is still worth keeping, just without the names.
        let info = plugin
            .components
            .read()
            .await
            .get(&*self.component_id)
            .cloned();
        let workload_name = info
            .as_ref()
            .map_or_else(String::new, |i| i.workload_name.clone());
        let workload_namespace = info
            .as_ref()
            .map_or_else(String::new, |i| i.workload_namespace.clone());
        let component_id = self.component_id.to_string();
        match level {
            Level::Trace => {
                tracing::trace!(
                    workload.component_id = %component_id,
                    workload.name = %workload_name,
                    workload.namespace = %workload_namespace,
                    context,
                    "{message}"
                )
            }
            Level::Debug => {
                tracing::debug!(
                    workload.component_id = %component_id,
                    workload.name = %workload_name,
                    workload.namespace = %workload_namespace,
                    context,
                    "{message}"
                )
            }
            Level::Info => {
                tracing::info!(
                    workload.component_id = %component_id,
                    workload.name = %workload_name,
                    workload.namespace = %workload_namespace,
                    context,
                    "{message}"
                )
            }
            Level::Warn => {
                tracing::warn!(
                    workload.component_id = %component_id,
                    workload.name = %workload_name,
                    workload.namespace = %workload_namespace,
                    context,
                    "{message}"
                )
            }
            Level::Error => {
                tracing::error!(
                    workload.component_id = %component_id,
                    workload.name = %workload_name,
                    workload.namespace = %workload_namespace,
                    context,
                    "{message}"
                )
            }
            Level::Critical => {
                tracing::error!(
                    workload.component_id = %component_id,
                    workload.name = %workload_name,
                    workload.namespace = %workload_namespace,
                    context,
                    "{message}"
                )
            }
        };

        if let Some(sink) = &plugin.sink {
            sink.log(LogRecord {
                level,
                context,
                message,
                workload_name,
                workload_namespace,
                component_id,
            });
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl HostPlugin for TracingLogger {
    fn id(&self) -> &'static str {
        PLUGIN_LOGGING_ID
    }

    fn world(&self) -> WitWorld {
        WitWorld {
            imports: HashSet::from([WitInterface::from("wasi:logging/logging")]),
            ..Default::default()
        }
    }

    async fn on_workload_item_bind<'a>(
        &self,
        component_handle: &mut WorkloadItem<'a>,
        interfaces: WitInterfaces<'_>,
    ) -> anyhow::Result<()> {
        // Ensure exactly one interface: "wasi:logging/logging"
        if !interfaces.contains("wasi", "logging", &[]) {
            tracing::warn!(
                "TracingLogger plugin requested for non-wasi:logging interface(s): {:?}",
                interfaces
            );
            return Ok(());
        }

        // Add `wasi:logging/logging` to the workload's linker
        bindings::wasi::logging::logging::add_to_linker::<_, SharedCtx>(
            component_handle.linker(),
            extract_active_ctx,
        )?;

        self.components.write().await.insert(
            component_handle.id().to_string(),
            ComponentInfo {
                workload_id: component_handle.workload_id().to_string(),
                workload_name: component_handle.workload_name().to_string(),
                workload_namespace: component_handle.workload_namespace().to_string(),
            },
        );

        Ok(())
    }

    async fn on_workload_unbind(
        &self,
        workload_id: &str,
        _interfaces: WitInterfaces<'_>,
    ) -> anyhow::Result<()> {
        self.components
            .write()
            .await
            .retain(|_, info| info.workload_id != workload_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info(workload_id: &str) -> ComponentInfo {
        ComponentInfo {
            workload_id: workload_id.to_string(),
            workload_name: "name".to_string(),
            workload_namespace: "default".to_string(),
        }
    }

    #[tokio::test]
    async fn unbind_forgets_only_that_workloads_components() {
        let logger = TracingLogger::default();
        {
            let mut components = logger.components.write().await;
            components.insert("a-1".to_string(), info("workload-a"));
            components.insert("a-2".to_string(), info("workload-a"));
            components.insert("b-1".to_string(), info("workload-b"));
        }

        let empty = HashSet::new();
        logger
            .on_workload_unbind("workload-a", WitInterfaces::new(&empty))
            .await
            .expect("unbind should succeed");

        let components = logger.components.read().await;
        assert_eq!(components.keys().collect::<Vec<_>>(), ["b-1"]);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::{Level, LogRecord, TracingLogger};

    #[test]
    fn sink_receives_original_record_without_a_subscriber() {
        let records = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&records);
        let logger = TracingLogger::with_sink(move |record| {
            if let Ok(mut records) = captured.lock() {
                records.push(record);
            }
        });
        let record = LogRecord {
            level: Level::Warn,
            context: "request-42".to_string(),
            message: "careful".to_string(),
            workload_name: "worker".to_string(),
            workload_namespace: "default".to_string(),
            component_id: "logger".to_string(),
        };
        if let Some(sink) = &logger.sink {
            sink.log(record);
        }

        let records = records.lock().unwrap_or_else(|error| error.into_inner());
        assert_eq!(records.len(), 1);
        assert!(matches!(records[0].level, Level::Warn));
        assert_eq!(records[0].context, "request-42");
        assert_eq!(records[0].message, "careful");
        assert_eq!(records[0].workload_name, "worker");
        assert_eq!(records[0].workload_namespace, "default");
        assert_eq!(records[0].component_id, "logger");
    }
}
