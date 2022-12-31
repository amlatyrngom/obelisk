pub mod database;
pub mod persistent_log;
pub mod log_replica;
pub mod rdatabase;
pub mod rpersistent_log;

// pub use persistent_log::PersistentLog;
use serde::{Deserialize, Serialize};
use common::clean_die;

const NUM_DB_RETRIES: usize = 20;
const SUBSYSTEM_NAME: &str = "persistence";

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

fn sync_clean_die(handle: &tokio::runtime::Handle, msg: &str) -> ! {
    handle.block_on(async move {
        clean_die(msg).await
    })
}

fn get_shared_log_db(namespace: &str, name: &str) -> rdatabase::Database {
    let prefix = common::shared_storage_prefix();
    let shared_dir = format!("{prefix}/{SUBSYSTEM_NAME}/{namespace}/{name}/shared");
    rdatabase::Database::new(&shared_dir, true)
}

// fn get_shared_log_connection(namespace: &str, name: &str, create: bool) -> rusqlite::Connection {
//     let prefix = common::shared_storage_prefix();
//     let shared_file = format!("{prefix}/{SUBSYSTEM_NAME}/{namespace}/{name}/shared.db");
//     for _ in 0..NUM_DB_RETRIES {
//         // The default mode is locking_mode=none, journal_mode=delete.
//         // So multiprocess writes are possible.
//         let conn = match rusqlite::Connection::open(&shared_file) {
//             Ok(conn) => conn,
//             x => {
//                 println!("{x:?}");
//                 continue;
//             }
//         };
//         match conn.busy_timeout(std::time::Duration::from_secs(1)) {
//             Ok(_) => {},
//             x => {
//                 println!("{x:?}");
//                 continue;
//             }
//         }
//         if create {
//             match conn.execute("CREATE TABLE IF NOT EXISTS system__shared_ownership(unique_row INTEGER PRIMARY KEY, owner_id BIGINT)", []) {
//                 Ok(_) => {},
//                 x => {
//                     println!("{x:?}");
//                     continue;
//                 }
//             }
//             match conn.execute("CREATE TABLE IF NOT EXISTS system__shared_logs(lo_lsn INTEGER PRIMARY KEY, hi_lsn BIGINT, entries BLOB)", []) {
//                 Ok(_) => {},
//                 x => {
//                     println!("{x:?}");
//                     continue;
//                 }
//             }
//         }
//         return conn;
//     }
//     eprintln!("Cannot connect to shared db. Fast exiting...");
//     std::process::exit(1);  
// }

fn fast_serialize(entries: &[(usize, Vec<u8>)], entries_size: usize) -> Vec<u8> {
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
        output[entry_lo..entry_hi].copy_from_slice(&entry);
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


#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
