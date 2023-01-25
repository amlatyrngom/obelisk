mod client;
mod echo;
mod handler;
mod rescaler;
pub use client::MessagingClient;
pub use echo::Echo;
pub use handler::MessagingHandler;
pub use rescaler::MessagingRescaler;

const SUBSYSTEM_NAME: &str = "messaging";

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
