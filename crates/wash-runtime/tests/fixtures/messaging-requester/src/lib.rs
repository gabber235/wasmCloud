mod bindings {
    wit_bindgen::generate!({
        world: "messaging-requester",
        generate_all,
    });
}

use bindings::exports::wasi::http::handler::Guest as Handler;
use bindings::wasi::http::types::{ErrorCode, Fields, Request, Response};
use bindings::wasmcloud::messaging::consumer;

struct Component;

const SUBJECT: &str = "async.echo";

fn internal(message: String) -> ErrorCode {
    ErrorCode::InternalError(Some(message))
}

fn respond(status: u16, body: Vec<u8>) -> Result<Response, ErrorCode> {
    let headers = Fields::new();
    let (mut body_tx, body_rx) = bindings::wit_stream::new();
    let (trailers_tx, trailers_rx) = bindings::wit_future::new(|| todo!());

    wit_bindgen::spawn_local(async move {
        body_tx.write_all(body).await;
        drop(body_tx);
        let _ = trailers_tx.write(Ok(None)).await;
    });

    let (response, _result) = Response::new(headers, Some(body_rx), trailers_rx);
    response
        .set_status_code(status)
        .map_err(|()| internal("failed to set status".into()))?;
    Ok(response)
}

impl Handler for Component {
    async fn handle(request: Request) -> Result<Response, ErrorCode> {
        let (body_result_tx, body_result_rx) = bindings::wit_future::new(|| Ok(()));
        let (body, _trailers) = Request::consume_body(request, body_result_rx);
        let body = body.collect().await;
        drop(body_result_tx);

        match consumer::request(SUBJECT.to_string(), body, 5_000, None).await {
            Ok(reply) => respond(200, reply.body),
            Err(message) => respond(502, message.into_bytes()),
        }
    }
}

bindings::export!(Component with_types_in bindings);
