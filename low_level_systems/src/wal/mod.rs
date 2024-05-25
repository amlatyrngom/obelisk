use super::LLError;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::sync::{mpsc, Arc, Mutex};

pub mod manager;
pub use manager::Manager;
mod partition;
pub mod replica;
mod test;

/// Size of a slab in KB.
const SLAB_SIKE_KB: usize = 256;
/// Size of a slab in bytes.
const SLAB_SIZE: usize = SLAB_SIKE_KB * 1024;

/// Returned after global recovery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalRecoveryState {
    /// The lsn of the last persisted entry.
    pub persist_lsn: i64,
}

/// Returned after partition recovery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionRecoveryState {
    /// The (lo, hi, prev_hi) of the last persisted batch and the batch before that.
    pub persist_lsns: (i64, i64, i64),
}

/// A handle passed to the partition replay function to allow efficient resuming from previous point.
pub struct PartitionReplayHandle {
    /// The file handle.
    /// It is different from the file handle in the partition, to allow concurrent persists along the replay.
    file: fs::File,
    /// Slab offsets read from the metadata.
    /// This is to avoid contending on the metadata file at each step.
    cached_offsets: Vec<u64>,
    /// The next lsn to read when the cached offsets are exhausted.
    next_from_lsn: i64,
}

/// A handle passed to the manager replay function to allow efficient resuming from previous point.
pub struct ManagerReplayHandle {
    /// Current entries in sorted order.
    /// These are returned by the partitions, and must be contiguous.
    entries: BTreeMap<i64, Vec<u8>>,
    /// Replay handle of each partition.
    partition_handles: Vec<Option<PartitionReplayHandle>>,
    /// The last returned lsn.
    /// Will guarantee continuity between the returned entries.
    last_returned_lsn: i64,
    /// The persist lsn at the beginning of the replay.
    persist_lsn: i64,
    /// The truncate lsn at the beginning of the replay.
    truncate_lsn: i64,
}

/// Abstraction for a partition of the WAL.
pub trait WALPartition: Send + Sync {
    /// Persist entries.
    fn persist(
        &self,
        logs: Vec<(Vec<u8>, i64)>,
        replicas: Option<(String, String)>,
        inject_disk_failure: bool,
    ) -> Result<(), LLError>;

    /// Replay log.
    /// At the first call, the handle is None.
    /// At subsequent calls, the handle is the one returned by the previous call.
    fn replay(
        &self,
        handle: Option<PartitionReplayHandle>,
    ) -> Result<(Vec<(Vec<u8>, i64)>, Option<PartitionReplayHandle>), LLError>;

    /// Get recovery state.
    fn get_recovery_state(&self) -> Result<PartitionRecoveryState, LLError>;

    /// Apply global recovery.
    fn apply_global_recovery_state(&self, global_state: GlobalRecoveryState)
        -> Result<(), LLError>;

    /// Get replica lock. This lock should be held in read-mode whenever interacting with the partition.
    fn get_replica_lock(&self) -> Result<Arc<Mutex<()>>, LLError>;
}

pub trait AlternativePersister: Send + Sync {
    /// Persist entries.
    /// Use incernation number and persist lsn for coordination and cleanup.
    fn persist(
        &self,
        incarnation_number: i64,
        persist_lsn: i64,
        logs: Vec<(Vec<u8>, i64)>,
    ) -> Result<mpsc::Receiver<()>, LLError>;

    /// Recover entries.
    /// Optionally specify a starting lsn and number of entries to recover.
    fn recover_entries(
        &self,
        from_lsn: Option<i64>,
        num_entries: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, i64)>, LLError>;
}

/// File for the log.
pub fn log_filename(storage_dir: &str, name: &str) -> String {
    format!("{storage_dir}/{name}.log")
}

/// File for the log metadata.
pub fn metadata_filename(storage_dir: &str, name: &str) -> String {
    format!("{storage_dir}/{name}.log.db")
}

/// Remove all files associated with this WAL.
pub fn reset_wal(storage_dir: &str, name: &str) {
    let filename = log_filename(storage_dir, name);
    let _ = fs::remove_file(filename);
    let filename = metadata_filename(storage_dir, name);
    let _ = fs::remove_file(filename);
}

pub fn hello() {
    println!("[wal::hello] Hello, world!");
    partition::hello();
}
