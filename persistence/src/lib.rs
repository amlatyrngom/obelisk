pub mod database;
pub mod log_replica;
pub mod persistent_log;
pub mod rescaler;

pub use log_replica::LogReplica;
pub use persistent_log::PersistentLog;
pub use rescaler::LogRescaler;

use serde::{Deserialize, Serialize};

const NUM_DB_RETRIES: usize = 20;
const SUBSYSTEM_NAME: &str = "persistence";
const ECS_MODE: &str = "messaging_ecs";

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

async fn fast_serialize(entries: &[(usize, Vec<u8>)], entries_size: usize) -> Vec<u8> {
    // Output format: (len_1: usize, lsn_1: u64, entry_1: Vec<u8>) ..., (len_n, lsn_n, entry_n).
    let total_size = 2 * entries.len() * 8 + entries_size;
    let mut output: Vec<u8> = Vec::new();
    output.resize(total_size, 0);
    let mut curr_offset = 0;
    for (lsn, entry) in entries {
        let len_lo = curr_offset;
        let len_hi = len_lo + 8;
        let lsn_lo = len_hi;
        let lsn_hi = lsn_lo + 8;
        let entry_lo = lsn_hi;
        let entry_hi = entry_lo + entry.len();
        let len = entry.len().to_be_bytes();
        output[len_lo..len_hi].copy_from_slice(&len);
        let lsn = lsn.to_be_bytes();
        output[lsn_lo..lsn_hi].copy_from_slice(&lsn);
        output[entry_lo..entry_hi].copy_from_slice(entry);
        curr_offset = entry_hi;
    }
    output
}

/// Deserialize in a predictable manner.
pub fn fast_deserialize(entries: &[u8]) -> Vec<(usize, Vec<u8>)> {
    let total_size = entries.len();
    let mut curr_offset = 0;
    let mut output = Vec::new();
    while curr_offset < total_size {
        let len_lo = curr_offset;
        let len_hi = len_lo + 8;
        let lsn_lo = len_hi;
        let lsn_hi = lsn_lo + 8;
        let len = &entries[len_lo..len_hi];
        let len = usize::from_be_bytes(len.try_into().unwrap());
        let lsn = &entries[lsn_lo..lsn_hi];
        let lsn = usize::from_be_bytes(lsn.try_into().unwrap());
        let entry_lo = lsn_hi;
        let entry_hi = entry_lo + len;
        let entry = &entries[entry_lo..entry_hi];
        output.push((lsn, entry.to_vec()));
        curr_offset = entry_hi;
    }

    output
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
