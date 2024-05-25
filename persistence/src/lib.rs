pub mod database;
pub mod echo;
pub mod log_replica;
pub mod persistent_log;
pub mod rescaler;

pub use echo::Echo;
pub use log_replica::LogReplica;
pub use persistent_log::PersistentLog;
pub use rescaler::WalRescaler;

const SUBSYSTEM_NAME: &str = "wal";

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
