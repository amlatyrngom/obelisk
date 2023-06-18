pub mod database;
pub mod log_replica;
pub mod persistent_log;
pub mod rescaler;
pub mod echo;

pub use log_replica::LogReplica;
pub use persistent_log::PersistentLog;
pub use rescaler::WalRescaler;
pub use echo::Echo;

use serde::{Deserialize, Serialize};

const NUM_DB_RETRIES: usize = 20;
const SUBSYSTEM_NAME: &str = "wal";

#[derive(Serialize, Deserialize, Debug)]
enum PersistenceReqMeta {
    Log {
        lo_lsn: usize,
        hi_lsn: usize,
        owner_id: usize,
        persisted_lsn: usize,
        replica_id: String,
    },
    Drain {
        owner_id: usize,
        persisted_lsn: usize,
        replica_id: String,
    },
}

#[derive(Serialize, Deserialize, Debug)]
enum RPersistenceReqMeta {
    Log {
        owner_id: usize,
        persisted_lsn: usize,
        replica_id: String,
    },
    Drain {
        owner_id: usize,
        persisted_lsn: usize,
        replica_id: String,
    },
}

#[derive(Serialize, Deserialize, Debug)]
enum PersistenceRespMeta {
    Ok,
    Outdated,
    WrongReplica,
    Terminating,
    Err(String),
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
