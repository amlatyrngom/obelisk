use super::LLError;
pub use manager::Manager;
pub use page::{ExclusivePage, Page, SharedPage};
use std::fs;

pub mod journal;
pub mod manager;
pub mod page;
mod test;

/// Metadata is composed of: (checksum: i64, id: i64, version: i64)
pub const METADATA_ITEMS: usize = 3;
/// Metadata size in bytes.
pub const METADATA_SIZE: usize = METADATA_ITEMS * 8;
/// Max size, to prevent overallocation due to bugs.
pub const MAX_SIZE_GB: usize = 100;
pub const MAX_SIZE: usize = MAX_SIZE_GB * 1024 * 1024 * 1024;
/// Page size in bytes.
pub const DEFAULT_PAGE_SIZE: usize = 4096;

/// Journal file name.
pub fn journal_filename(storage_dir: &str, name: &str) -> String {
    format!("{storage_dir}/{name}.pages.journal")
}

/// Metadata file name.
pub fn metadata_filename(storage_dir: &str, name: &str) -> String {
    format!("{storage_dir}/{name}.pages.db")
}

/// Data file name.
pub fn data_filename(storage_dir: &str, name: &str) -> String {
    format!("{storage_dir}/{name}.pages")
}

/// Remove all files associated with this BPM.
pub fn reset_bpm(storage_dir: &str, name: &str) {
    let filename = journal_filename(storage_dir, name);
    let _ = fs::remove_file(filename);
    let filename = metadata_filename(storage_dir, name);
    let _ = fs::remove_file(filename);
    let filename = data_filename(storage_dir, name);
    let _ = fs::remove_file(filename);
}

/// Hello.
pub fn hello() {
    println!("[bpm::hello] Hello, world!");
}
