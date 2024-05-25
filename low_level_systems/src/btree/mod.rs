use super::LLError;
use crate::{bpm, wal};

pub mod manager;
pub mod page;
mod test;
pub mod wal_provider;

pub use manager::Manager;

/// Max key size
const MAX_KEY_SIZE: usize = 1024;

/// Remove all files associated with this BTree.
pub fn reset_btree(storage_dir: &str, name: &str) {
    bpm::reset_bpm(storage_dir, name);
    wal::reset_wal(storage_dir, name);
}

/// Hello.
pub fn hello() {
    println!("[btree::hello] Hello, world!");
}
