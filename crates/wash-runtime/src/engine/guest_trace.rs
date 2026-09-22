//! Trace ownership follows guest tasks across the concurrent Wasmtime scheduler.

use super::ctx::SharedCtx;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use wasmtime::component::{Accessor, ComponentNamedList, GuestTaskId, Lift, Lower, TypedFunc};
#[cfg(feature = "wasi-otel")]
use wasmtime::{AsContextMut, component::Access};

#[derive(Clone, Default)]
pub(crate) struct GuestTraces(Arc<Mutex<HashMap<GuestTaskId, tracing::Span>>>);

struct Registration {
    traces: GuestTraces,
    task: GuestTaskId,
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.traces
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.task);
    }
}

/// Associate the ingress span with the actual guest task, not its waiting host future.
pub(crate) async fn call<Params, Return>(
    accessor: &Accessor<SharedCtx>,
    function: TypedFunc<Params, Return>,
    params: Params,
) -> wasmtime::Result<Return>
where
    Params: ComponentNamedList + Lower + 'static,
    Return: ComponentNamedList + Lift + 'static,
{
    let span = tracing::Span::current();
    let (pending, _registration) = accessor.with(|mut access| -> wasmtime::Result<_> {
        let pending = function.start_call_concurrent(&mut access, params)?;
        let traces = access.get().guest_traces.clone();
        let task = pending.task();
        traces
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(task, span);
        wasmtime::Result::Ok((pending, Registration { traces, task }))
    })?;
    function.finish_call_concurrent(accessor, pending).await
}

/// Resolve the nearest registered caller, retaining isolation when tasks interleave.
#[cfg(feature = "wasi-otel")]
pub(crate) fn current<T>(access: &mut Access<'_, T, SharedCtx>) -> tracing::Span {
    let tasks = access
        .as_context_mut()
        .async_call_stack()
        .map(|stack| stack.collect::<Vec<_>>())
        .unwrap_or_default();
    let traces = access
        .get()
        .guest_traces
        .0
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    tasks
        .into_iter()
        .find_map(|task| traces.get(&task).cloned())
        .unwrap_or_else(tracing::Span::current)
}

#[cfg(all(test, feature = "wasi-otel"))]
mod tests {
    use super::*;
    use crate::engine::ctx::{Ctx, extract_active_ctx};
    use opentelemetry::trace::{TraceContextExt, TracerProvider};
    use tracing::{Instrument, instrument::WithSubscriber};
    use tracing_opentelemetry::OpenTelemetrySpanExt;
    use tracing_subscriber::layer::SubscriberExt;

    #[tokio::test]
    async fn concurrent_guest_imports_keep_their_ingress_and_release_registrations() {
        crate::init_crypto();
        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder().build();
        let subscriber = tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("guest-task-test")));
        async {
            let mut config = wasmtime::Config::new();
            config
                .wasm_component_model(true)
                .wasm_component_model_async(true)
                .concurrency_support(true);
            let engine = wasmtime::Engine::new(&config).unwrap();
            let component = wasmtime::component::Component::new(
                &engine,
                wat::parse_str(
                    r#"
                (component
                    (import "observe" (func $observe (param "id" u32)))
                    (core func $observe (canon lower (func $observe)))
                    (core module $module
                        (import "host" "observe" (func $observe (param i32)))
                        (func (export "run") (param i32)
                            local.get 0 call $observe))
                    (core instance $module (instantiate $module
                        (with "host" (instance (export "observe" (func $observe))))))
                    (func (export "run") (param "id" u32)
                        (canon lift (core func $module "run"))))
            "#,
                )
                .unwrap(),
            )
            .unwrap();
            let observed = Arc::new(Mutex::new(HashMap::new()));
            let capture = Arc::clone(&observed);

            let mut linker = wasmtime::component::Linker::<SharedCtx>::new(&engine);
            linker
                .root()
                .func_wrap_async("observe", move |store, (id,): (u32,)| {
                    let capture = Arc::clone(&capture);

                    Box::new(async move {
                        let mut access =
                            Access::<SharedCtx, SharedCtx>::new(store, extract_active_ctx);
                        let span = current(&mut access);
                        let context = span.context().span().span_context().clone();
                        if id == 2 {
                            wasmtime::bail!("intentional guest trap");
                        }
                        tokio::task::yield_now().await;
                        capture.lock().unwrap().insert(id, context);
                        Ok(())
                    })
                })
                .unwrap();
            let mut store = wasmtime::Store::new(
                &engine,
                SharedCtx::new(Ctx::builder("workload", "component").build()),
            );
            let first = linker
                .instantiate_async(&mut store, &component)
                .await
                .unwrap()
                .get_typed_func::<(u32,), ()>(&mut store, "run")
                .unwrap();
            let second = linker
                .instantiate_async(&mut store, &component)
                .await
                .unwrap()
                .get_typed_func::<(u32,), ()>(&mut store, "run")
                .unwrap();
            let first_span = tracing::info_span!("first-ingress");
            let second_span = tracing::info_span!("second-ingress");
            let first_context = first_span.context().span().span_context().clone();
            let second_context = second_span.context().span().span_context().clone();
            store
                .run_concurrent(async |accessor| {
                    tokio::try_join!(
                        call(accessor, first, (0,)).instrument(first_span),
                        call(accessor, second, (1,)).instrument(second_span),
                    )
                })
                .await
                .unwrap()
                .unwrap();
            {
                let actual = observed.lock().unwrap();
                assert!(first_context.is_valid());
                assert_ne!(first_context.trace_id(), second_context.trace_id());
                assert_eq!(actual[&0], first_context);
                assert_eq!(actual[&1], second_context);
            }
            assert!(store.data().guest_traces.0.lock().unwrap().is_empty());
            let failed = store
                .run_concurrent(async |accessor| call(accessor, first, (2,)).await)
                .await;
            assert!(failed.is_err() || failed.unwrap().is_err());
            assert!(store.data().guest_traces.0.lock().unwrap().is_empty());
        }
        .with_subscriber(subscriber)
        .await;
        provider.shutdown().unwrap();
    }
}
