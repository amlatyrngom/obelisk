use crate::{wal, LLError};
use std::sync::Arc;

pub trait WalProvider: Send + Sync {
    fn enqueue(&self, entry: Vec<u8>) -> Result<i64, LLError>;
    fn persist(&self, lsn: Option<i64>) -> Result<(), LLError>;
    fn persist_lsn(&self) -> i64;
    fn replay_all(&self) -> Result<Vec<(Vec<u8>, i64)>, LLError>;
    fn truncate(&self, lsn: i64) -> Result<(), LLError>;
}

pub struct BasicWalProvider {
    wal: Arc<wal::Manager>,
}

impl BasicWalProvider {
    pub fn new(storage_dir: &str, name: &str, parallelism: usize) -> Self {
        let wal = Arc::new(wal::Manager::new(storage_dir, name, parallelism).unwrap());
        BasicWalProvider { wal }
    }
}

impl WalProvider for BasicWalProvider {
    fn enqueue(&self, entry: Vec<u8>) -> Result<i64, LLError> {
        self.wal.enqueue(entry)
    }

    fn persist(&self, lsn: Option<i64>) -> Result<(), LLError> {
        self.wal.persist(lsn)
    }

    fn replay_all(&self) -> Result<Vec<(Vec<u8>, i64)>, LLError> {
        let mut replay_handle = None;
        let mut entries = vec![];
        loop {
            let (new_handle, new_entries) = self.wal.replay(replay_handle)?;
            replay_handle = new_handle;
            entries.extend(new_entries);
            if replay_handle.is_none() {
                break;
            }
        }
        Ok(entries)
    }

    fn truncate(&self, lsn: i64) -> Result<(), LLError> {
        self.wal.truncate(lsn)
    }

    fn persist_lsn(&self) -> i64 {
        self.wal.persist_lsn()
    }
}
