use crate::bindings::wasmcloud::messaging0_4_0::{consumer, types::BrokerMessage};

mod bindings {
    use crate::Component;

    wit_bindgen::generate!({
        world: "echo",
        generate_all
    });

    export!(Component);
}

struct Component;

impl bindings::exports::wasmcloud::messaging0_4_0::handler::Guest for Component {
    async fn handle_message(msg: BrokerMessage) -> Result<(), String> {
        if let Some(reply_to) = msg.reply_to {
            let reply = BrokerMessage {
                subject: reply_to,
                body: msg.body,
                reply_to: None,
            };
            consumer::publish(reply, None).await?;
        }
        Ok(())
    }
}
