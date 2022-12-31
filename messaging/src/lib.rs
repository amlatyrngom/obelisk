mod client;
mod echo;
mod handler;
mod receiver;
mod rescaler;
pub use client::MessagingClient;
pub use echo::Echo;
pub use handler::MessagingHandler;
pub use receiver::Receiver;
pub use rescaler::MessagingRescaler;

const SUBSYSTEM_NAME: &str = "messaging";

fn send_queue_name(namespace: &str, name: &str) -> String {
    format!("obk__msg_send__{namespace}__{name}")
}

fn recv_queue_name(namespace: &str, name: &str) -> String {
    format!("obk__msg_recv__{namespace}__{name}")
}

pub async fn prepare_deployment() -> Vec<String> {
    // Return spec
    let spec = include_str!("deployment.toml");
    vec![spec.into()]
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
