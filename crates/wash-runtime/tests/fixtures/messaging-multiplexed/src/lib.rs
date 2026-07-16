mod bindings {
    wit_bindgen::generate!({
        world: "messaging-multiplexed",
        generate_all,
    });
}

use bindings::exports::wasi::http::handler::Guest as Handler;
use bindings::wasi::http::types::{ErrorCode, Fields, Request, Response};
use bindings::wasmcloud::messaging0_4_0::types::BrokerMessage;

struct Component;

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

async fn run() -> Result<Vec<u8>, String> {
    bindings::team_a::publish(
        BrokerMessage {
            subject: "team.events".to_string(),
            body: b"from-a".to_vec(),
            reply_to: None,
        },
        None,
    )
    .await?;
    bindings::team_b::publish(
        BrokerMessage {
            subject: "team.events".to_string(),
            body: b"from-b".to_vec(),
            reply_to: None,
        },
        None,
    )
    .await?;
    let reply = bindings::team_a::request(
        "team-a.rpc".to_string(),
        b"ping".to_vec(),
        5_000,
        None,
    )
    .await?;
    Ok(reply.body)
}

impl Handler for Component {
    async fn handle(_request: Request) -> Result<Response, ErrorCode> {
        match run().await {
            Ok(body) => respond(200, body),
            Err(message) => respond(502, message.into_bytes()),
        }
    }
}

bindings::export!(Component with_types_in bindings);
