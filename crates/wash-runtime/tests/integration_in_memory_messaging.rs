use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use tokio::time::timeout;
use wash_runtime::{
    engine::Engine,
    host::{
        HostApi, HostBuilder,
        http::{DevRouter, HttpServer},
    },
    plugin::wasmcloud_messaging::InMemoryMessaging,
    types::{Component, LocalResources, Workload, WorkloadStartRequest, WorkloadState},
    wit::WitInterface,
};

const REQUESTER: &[u8] = include_bytes!("wasm/messaging_requester.wasm");
const ECHO: &[u8] = include_bytes!("wasm/messaging_echo.wasm");

fn component(name: &str, wasm: &'static [u8]) -> Component {
    let mut local_resources = LocalResources::default();
    if name == "echo" {
        local_resources
            .config
            .insert("subscriptions".to_string(), "async.echo".to_string());
    }
    Component {
        name: name.to_string(),
        digest: None,
        bytes: bytes::Bytes::from_static(wasm),
        local_resources,
        pool_size: 1,
        max_invocations: 100,
    }
}

#[tokio::test]
async fn async_request_replies_between_components() -> Result<()> {
    let engine = Engine::builder().build()?;
    let http = HttpServer::new(DevRouter::default(), "127.0.0.1:0".parse()?).await?;
    let addr = http.addr();
    let host = HostBuilder::new()
        .with_engine(engine)
        .with_http_handler(Arc::new(http))
        .with_plugin(Arc::new(InMemoryMessaging::default()))?
        .build()?
        .start()
        .await?;

    let messaging_consumer = WitInterface::from("wasmcloud:messaging/consumer,types@0.3.0");
    let messaging_handler = WitInterface::from("wasmcloud:messaging/handler@0.3.0");
    let http = WitInterface {
        namespace: "wasi".to_string(),
        package: "http".to_string(),
        interfaces: ["handler".to_string()].into_iter().collect(),
        version: Some(semver::Version::new(0, 3, 0)),
        config: HashMap::from([("host".to_string(), "requester".to_string())]),
        name: None,
    };
    let req = WorkloadStartRequest {
        workload_id: uuid::Uuid::new_v4().to_string(),
        workload: Workload {
            namespace: "test".to_string(),
            name: "in-memory-async-messaging".to_string(),
            annotations: HashMap::new(),
            service: None,
            components: vec![component("requester", REQUESTER), component("echo", ECHO)],
            host_interfaces: vec![http, messaging_consumer, messaging_handler],
            volumes: vec![],
        },
    };
    let started = host.workload_start(req).await?;
    assert_eq!(
        started.workload_status.workload_state,
        WorkloadState::Running,
        "{}",
        started.workload_status.message
    );

    let payload = format!("in-memory-async-round-trip-{}", uuid::Uuid::new_v4());
    let response = timeout(
        Duration::from_secs(10),
        reqwest::Client::new()
            .post(format!("http://{addr}/"))
            .header("HOST", "requester")
            .body(payload.clone())
            .send(),
    )
    .await
    .context("HTTP request timed out")??;
    let status = response.status();
    let body = response.bytes().await?;
    assert!(
        status.is_success(),
        "expected success, got {status}: {body:?}"
    );
    assert_eq!(body.as_ref(), payload.as_bytes());
    Ok(())
}
