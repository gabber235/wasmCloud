mod bindings {
    wit_bindgen::generate!({
        path: "../wit",
        world: "http-api",
        generate_all,
        async: [
            "import:wasmcloud:messaging/consumer@0.3.0#request",
            "import:wasmcloud:messaging/consumer@0.3.0#publish",
            "export:wasi:http/handler@0.3.0#handle",
        ],
    });
}

use bindings::exports::wasi::http::handler::Guest as Handler;
use bindings::wasi::http::types::{ErrorCode, Fields, Request, Response};
use bindings::wasmcloud::messaging::consumer;
use serde::Deserialize;

static UI_HTML: &str = include_str!("../ui.html");

struct Component;

#[derive(Deserialize)]
struct TaskRequest {
    worker: Option<String>,
    payload: String,
}

fn respond(status: u16, content_type: Option<&str>, body: Vec<u8>) -> Response {
    let fields = Fields::new();
    if let Some(content_type) = content_type {
        let _ = fields.append("content-type", content_type.as_bytes());
    }

    let (mut body_tx, body_rx) = bindings::wit_stream::new();
    let (trailers_tx, trailers_rx) = bindings::wit_future::new(|| Ok(None));
    wit_bindgen::spawn_local(async move {
        if !body.is_empty() {
            body_tx.write_all(body).await;
        }
        drop(body_tx);
        let _ = trailers_tx.write(Ok(None)).await;
    });

    let (response, _result) = Response::new(fields, Some(body_rx), trailers_rx);
    let _ = response.set_status_code(status);
    response
}

async fn create_task(request: Request) -> Result<Response, ErrorCode> {
    let (body_result_tx, body_result_rx) = bindings::wit_future::new(|| Ok(()));
    let (body, _trailers) = Request::consume_body(request, body_result_rx);
    let body = body.collect().await;
    drop(body_result_tx);

    let task_request: TaskRequest = serde_json::from_slice(&body)
        .map_err(|error| ErrorCode::InternalError(Some(error.to_string())))?;
    let subject = format!(
        "tasks.{}",
        task_request.worker.unwrap_or_else(|| "leet".to_string())
    );

    match consumer::request(subject, task_request.payload.into_bytes(), 5_000).await {
        Ok(response) => Ok(respond(200, None, response.body)),
        Err(error) => Ok(respond(502, None, error.into_bytes())),
    }
}

impl Handler for Component {
    async fn handle(request: Request) -> Result<Response, ErrorCode> {
        let path = request.get_path_with_query().unwrap_or_default();
        match path.split('?').next().unwrap_or_default() {
            "/" => Ok(respond(200, Some("text/html"), UI_HTML.as_bytes().to_vec())),
            "/task" => create_task(request).await,
            _ => Ok(respond(404, None, b"Not found\n".to_vec())),
        }
    }
}

#[allow(unsafe_code)]
mod export {
    use super::{Component, bindings};
    bindings::export!(Component with_types_in bindings);
}
