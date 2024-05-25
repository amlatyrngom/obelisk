pub mod bpm;
pub mod btree;
pub mod file_lease;
pub mod wal;

/// Low-level error.
#[derive(Debug, Clone)]
pub enum LLError {
    WALError(String),
    BPMError(String),
    BTreeError(String),
    LeaseError(String),
}

pub fn hello() {
    wal::hello();
    bpm::hello();
    btree::hello();
}
