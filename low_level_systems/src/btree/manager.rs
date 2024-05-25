use std::{
    borrow::BorrowMut,
    collections::{BTreeMap, BTreeSet},
    sync::atomic,
};

use super::wal_provider::{BasicWalProvider, WalProvider};
use super::{page::BTreePage, LLError};
use crate::bpm::{self, SharedPage};
use serde::{Deserialize, Serialize};
use std::sync::{mpsc, Arc, RwLock};

/// Page id and height of root.
type RootInfo = (i64, usize);
/// Exclusive root lock.
type ExclusiveRoot = guardian::ArcRwLockWriteGuardian<RootInfo>;
/// Shared root lock.
type SharedRoot = guardian::ArcRwLockReadGuardian<RootInfo>;
/// Delete Version
/// Set to max to indicate that any write to this page need not be redone.
const DELETE_VERSION: i64 = i64::MAX;
/// WAL Parallelism
const WAL_PARALLELISM: usize = 8;

/// BTree manager.
#[derive(Clone)]
pub struct Manager {
    pub(crate) bpm: Arc<bpm::Manager>,
    root: Arc<RwLock<RootInfo>>,
    pub wal: Arc<dyn WalProvider>,
    setup: Arc<RwLock<Setup>>,
    completion_info: Arc<RwLock<CompletionInfo>>,
    access_stats: AccessStats,
}

#[derive(Clone, Debug)]
pub struct Setup {
    pub enable_early_return: bool,
    pub enable_completion: bool,
    pub enable_keep_pages: bool,
}

struct CompletionInfo {
    pending_lsns: BTreeSet<i64>,
    completion_lsns: BTreeMap<i64, i64>,
}

/// WAL entry for non-atomic operations.
#[derive(Serialize, Deserialize, Debug, Clone)]
enum WalEntry {
    Insert {
        page_id: i64,
        page_version: i64,
        parent_ids: Vec<i64>,
        key: Vec<u8>,
        val: Vec<u8>,
    },
    Delete {
        page_id: i64,
        page_version: i64,
        parent_ids: Vec<i64>,
        key: Vec<u8>,
    },
    Checkpoint {
        root_id: i64,
        root_height: usize,
        init_lsn: Option<i64>,
    },
    Completion {
        lsn: i64,
    },
    PageDelete {
        page_id: i64,
    },
    Initialize {},
}

/// Type of element for printing.
#[derive(Debug, Clone, Copy)]
pub enum ElemType {
    USIZE,
    Str,
    Bytes(Option<usize>),
}

/// Recovery info.
#[derive(Debug, Clone, Copy)]
struct RecoveryInfo {
    lsn: i64,
    version: i64,
}

/// Access Stats
#[derive(Debug, Clone)]
struct AccessStats {
    pub optimistic_writes: Arc<atomic::AtomicUsize>,
    pub pessimistic_writes: Arc<atomic::AtomicUsize>,
    pub leaf_lock_time: Arc<atomic::AtomicU64>,
    pub lock_sub_time: Arc<atomic::AtomicU64>,
    pub log_time: Arc<atomic::AtomicU64>,
}

impl Manager {
    /// Create a new BTree or recover an existing one.
    pub fn new(
        parallelism: usize,
        page_size: usize,
        storage_dir: &str,
        name: &str,
    ) -> Result<Self, LLError> {
        let wal = BasicWalProvider::new(storage_dir, name, WAL_PARALLELISM);
        Self::new_with_provider(parallelism, page_size, storage_dir, name, Arc::new(wal))
    }

    pub fn new_with_provider(
        parallelism: usize,
        page_size: usize,
        storage_dir: &str,
        name: &str,
        wal: Arc<dyn WalProvider>,
    ) -> Result<Self, LLError> {
        // Make BPM and WAL.
        let bpm = Arc::new(bpm::Manager::new(
            parallelism,
            page_size,
            storage_dir,
            name,
        )?);
        assert!(
            parallelism <= WAL_PARALLELISM,
            "WAL parallelism must be greater than BTREE parallelism."
        );
        // Start recovery: get root and entries.
        let (root_info, entries) = Self::start_recovery(&bpm, wal.as_ref())?;
        log::info!("[BTREE] Made/Recovered Root: {root_info:?}");
        let root = Arc::new(RwLock::new(root_info));
        let setup = Arc::new(RwLock::new(Setup {
            enable_early_return: true,
            enable_completion: true,
            enable_keep_pages: true,
        }));
        let completion_info = Arc::new(RwLock::new(CompletionInfo {
            pending_lsns: BTreeSet::new(),
            completion_lsns: BTreeMap::new(),
        }));
        let access_stats = AccessStats {
            optimistic_writes: Arc::new(atomic::AtomicUsize::new(0)),
            pessimistic_writes: Arc::new(atomic::AtomicUsize::new(0)),
            leaf_lock_time: Arc::new(atomic::AtomicU64::new(0)),
            log_time: Arc::new(atomic::AtomicU64::new(0)),
            lock_sub_time: Arc::new(atomic::AtomicU64::new(0)),
        };
        // Make actual recovery.
        let btree = Self {
            bpm,
            root,
            wal,
            setup,
            completion_info,
            access_stats,
        };
        btree.perform_recovery(entries)?;
        Ok(btree)
    }

    /// Set early return.
    pub fn change_setup(&self, new_setup: Setup) {
        let mut setup = self.setup.write().unwrap();
        *setup = new_setup;
        self.bpm.set_keep(setup.enable_keep_pages);
    }

    /// Get the height of the root.
    pub fn root_height(&self) -> usize {
        let root = self.root.read().unwrap();
        root.1
    }

    /// Show an element.
    pub fn show_elem(elem: &[u8], elem_type: ElemType) -> String {
        if elem.is_empty() {
            return "[]".into();
        }
        match elem_type {
            ElemType::USIZE => {
                let elem = usize::from_be_bytes(elem.try_into().unwrap());
                format!("{elem}")
            }
            ElemType::Str => {
                let elem = std::str::from_utf8(elem).unwrap();
                format!("{elem:?}")
            }
            ElemType::Bytes(max_size) => {
                let max_size = if let Some(max_size) = max_size {
                    std::cmp::min(max_size, elem.len())
                } else {
                    elem.len()
                };
                let elem = &elem[..max_size];
                format!("{elem:?}")
            }
        }
    }

    /// Perform traversal of the BTree.
    pub fn show_structure(&self, key_type: ElemType, val_type: ElemType) -> BTreeMap<i64, i64> {
        // Collect pages by level.
        let mut versions = BTreeMap::<i64, i64>::new();
        let mut queue = std::collections::VecDeque::new();
        let root = self.root.read().unwrap();
        queue.push_back(root.0);
        let max_height = root.1;
        while !queue.is_empty() {
            let page_id = queue.pop_front().unwrap();
            let page = self.bpm.pin(page_id).unwrap();
            let page = bpm::SharedPage::from(page);
            let height = BTreePage::height(&page);
            let spaces = "  ".repeat(max_height - height);
            let is_root = if page_id == root.0 { "Root " } else { "" };
            let version = page.page_version();
            versions.insert(page_id, version);
            println!("{spaces}{is_root}Page: {page_id}. Height: {height}. Version: {version}");
            let num_entries = BTreePage::num_entries(&page);
            for (key, value) in BTreePage::scan(&page, 0, num_entries) {
                if height == 0 {
                    // Leaf.
                    let key = Self::show_elem(key, key_type);
                    let value = Self::show_elem(value, val_type);
                    println!("{spaces} Leaf Entry {key}: {value}");
                } else {
                    let key = Self::show_elem(key, key_type);
                    let child_id = i64::from_be_bytes(value[..8].try_into().unwrap());
                    println!("{spaces} Inner Entry {key}: {child_id}");
                    queue.push_back(child_id);
                }
            }
            self.bpm.unpin(page_id).unwrap();
        }
        versions
    }

    pub fn reset_stats(&self) {
        self.access_stats
            .optimistic_writes
            .store(0, atomic::Ordering::Release);
        self.access_stats
            .pessimistic_writes
            .store(0, atomic::Ordering::Release);
        self.access_stats
            .leaf_lock_time
            .store(0, atomic::Ordering::Release);
        self.access_stats
            .log_time
            .store(0, atomic::Ordering::Release);
        self.access_stats
            .lock_sub_time
            .store(0, atomic::Ordering::Release);
    }

    pub fn show_stats(&self) {
        let optimistic_writes = self
            .access_stats
            .optimistic_writes
            .load(atomic::Ordering::Acquire);
        let pessimistic_writes = self
            .access_stats
            .pessimistic_writes
            .load(atomic::Ordering::Acquire);
        let leaf_lock_time = self
            .access_stats
            .leaf_lock_time
            .load(atomic::Ordering::Acquire);
        let leaf_lock_time = std::time::Duration::from_micros(leaf_lock_time);
        let log_time = self.access_stats.log_time.load(atomic::Ordering::Acquire);
        let log_time = std::time::Duration::from_micros(log_time);
        let lock_sub_time = self
            .access_stats
            .lock_sub_time
            .load(atomic::Ordering::Acquire);
        let lock_sub_time = std::time::Duration::from_micros(lock_sub_time);
        println!("[BTREE] Optimistic Writes: {optimistic_writes}. Pessimistic Writes: {pessimistic_writes}. Leaf Lock Time: {leaf_lock_time:?}. Log Time: {log_time:?}. Lock Sub Time: {lock_sub_time:?}");
    }

    /// Recover the BTree.
    /// TODO: Implement full recovery logic.
    fn start_recovery(
        bpm: &bpm::Manager,
        wal: &dyn WalProvider,
    ) -> Result<(RootInfo, BTreeMap<i64, WalEntry>), LLError> {
        let entries = wal.replay_all()?;
        let mut root_info = None;
        let mut ordered_entries = BTreeMap::<i64, _>::new();
        let mut existing_init_lsn = None;
        // Collect checkpoint and completed entries.
        for (entry, lsn) in &entries {
            let entry: WalEntry = bincode::deserialize(entry).unwrap();
            match entry {
                WalEntry::Checkpoint {
                    root_id,
                    root_height,
                    init_lsn,
                } => {
                    root_info = Some((root_id, root_height));
                    if let Some(init_lsn) = init_lsn {
                        ordered_entries.remove(&init_lsn);
                    }
                }
                WalEntry::Completion { lsn } => {
                    ordered_entries.remove(&lsn);
                }
                WalEntry::Initialize {} => {
                    if root_info.is_some() {
                        // Ignore: already initialized.
                        continue;
                    }
                    existing_init_lsn = Some(*lsn);
                }
                _ => {
                    ordered_entries.insert(*lsn, entry);
                }
            }
        }
        if let Some(root_info) = root_info {
            Ok((root_info, ordered_entries))
        } else {
            Ok((
                Self::initialize_root(bpm, wal, existing_init_lsn)?,
                ordered_entries,
            ))
        }
    }

    /// Perform recovery.
    fn perform_recovery(&self, entries: BTreeMap<i64, WalEntry>) -> Result<(), LLError> {
        for (lsn, wal_entry) in entries {
            match wal_entry {
                WalEntry::Checkpoint {
                    root_id: _,
                    root_height: _,
                    init_lsn: _,
                } => {
                    // Ignore
                    assert!(false);
                }
                WalEntry::Completion { lsn: _ } => {
                    // Ignore
                    assert!(false);
                }
                WalEntry::Initialize {} => {
                    // Ignore
                    assert!(false);
                }
                WalEntry::PageDelete { page_id } => {
                    self.complete_page_delete(page_id, lsn, true)?;
                }
                WalEntry::Insert {
                    page_id,
                    page_version,
                    parent_ids,
                    key,
                    val,
                } => {
                    log::info!("[BTREE] Recovering insert for key: {:?}", key);
                    let mut root = {
                        let root_info = ExclusiveRoot::from(self.root.clone());
                        let root_id = root_info.0;
                        if parent_ids.contains(&root_id) {
                            Some(root_info)
                        } else {
                            None
                        }
                    };
                    let mut parents = parent_ids
                        .iter()
                        .map(|id| {
                            let parent = self.bpm.pin(*id).unwrap();
                            bpm::ExclusivePage::from(parent)
                        })
                        .collect();
                    let page = self.bpm.pin(page_id).unwrap();
                    let mut page = bpm::ExclusivePage::from(page);
                    let recovery_info = RecoveryInfo {
                        lsn,
                        version: page_version,
                    };
                    self.perform_insert(
                        &mut page,
                        &mut parents,
                        &mut root,
                        &key,
                        &val,
                        None,
                        Some(recovery_info),
                    )?;
                    self.bpm.unpin(page_id).unwrap();
                    self.free_parents(&mut parents, &mut root);
                }
                WalEntry::Delete {
                    page_id,
                    page_version,
                    parent_ids,
                    key,
                } => {
                    log::info!("[BTREE] Recovering delete for key: {:?}", key);
                    let mut root = {
                        let root_info = ExclusiveRoot::from(self.root.clone());
                        let root_id = root_info.0;
                        if parent_ids.contains(&root_id) {
                            Some(root_info)
                        } else {
                            None
                        }
                    };
                    let mut parents = parent_ids
                        .iter()
                        .map(|id| {
                            let parent = self.bpm.pin(*id).unwrap();
                            bpm::ExclusivePage::from(parent)
                        })
                        .collect();
                    let page = self.bpm.pin(page_id).unwrap();
                    let mut page = bpm::ExclusivePage::from(page);
                    let recovery_info = RecoveryInfo {
                        lsn,
                        version: page_version,
                    };
                    self.perform_delete(
                        &mut page,
                        &mut parents,
                        &mut root,
                        &key,
                        None,
                        Some(recovery_info),
                    )?;
                    self.bpm.unpin(page_id).unwrap();
                    self.free_parents(&mut parents, &mut root);
                }
            }
        }
        let checkpoint = WalEntry::Checkpoint {
            root_id: self.root.read().unwrap().0,
            root_height: self.root.read().unwrap().1,
            init_lsn: None,
        };
        let checkpoint = bincode::serialize(&checkpoint).unwrap();
        let checkpoint_lsn = self.wal.enqueue(checkpoint)?;
        self.wal.persist(Some(checkpoint_lsn))?;
        self.wal.truncate(checkpoint_lsn - 1)?;
        Ok(())
    }

    pub fn perform_checkpoint(&self) {
        let truncate_lsn = {
            let mut completion_info = self.completion_info.write().unwrap();
            let persist_lsn = self.wal.persist_lsn();
            let mut written_completions = vec![];
            for (completion_lsn, corresponding_pending_lsn) in
                completion_info.completion_lsns.iter()
            {
                if *completion_lsn <= persist_lsn {
                    written_completions.push((*completion_lsn, *corresponding_pending_lsn));
                } else {
                    break;
                }
            }
            for (completion_lsn, corresponding_pending_lsn) in written_completions {
                completion_info.completion_lsns.remove(&completion_lsn);
                completion_info
                    .pending_lsns
                    .remove(&corresponding_pending_lsn);
            }
            if let Some(lsn) = completion_info.pending_lsns.first() {
                *lsn - 1
            } else {
                persist_lsn
            }
        };
        let checkpoint_lsn = {
            // IMPORTANT: Hold root lock while enqueing to prevent loss of concurrent root change.
            let root = self.root.read().unwrap();
            let checkpoint_entry = WalEntry::Checkpoint {
                root_id: root.0,
                root_height: root.1,
                init_lsn: None,
            };
            let checkpoint_entry = bincode::serialize(&checkpoint_entry).unwrap();
            let checkpoint_lsn = self.wal.enqueue(checkpoint_entry).unwrap();
            checkpoint_lsn
        };
        self.wal.persist(Some(checkpoint_lsn)).unwrap();
        self.wal.truncate(truncate_lsn).unwrap();
    }

    /// Initialize the root.
    fn initialize_root(
        bpm: &bpm::Manager,
        wal: &dyn WalProvider,
        with_lsn: Option<i64>,
    ) -> Result<RootInfo, LLError> {
        let (init_lsn, root_page) = if let Some(with_lsn) = with_lsn {
            let root_page = if let Some(root_id) = bpm.recover_physical_id(with_lsn)? {
                bpm.pin(root_id)?
            } else {
                bpm.create_page_with_logical_id(with_lsn)?
            };
            (with_lsn, root_page)
        } else {
            let log_entry = WalEntry::Initialize {};
            let log_entry = bincode::serialize(&log_entry).unwrap();
            let lsn = wal.enqueue(log_entry)?;
            wal.persist(Some(lsn))?;
            let root_page = bpm.create_page_with_logical_id(lsn)?;
            (lsn, root_page)
        };
        let root_id = {
            let mut root_page = bpm::ExclusivePage::from(root_page);
            BTreePage::initialize(&mut root_page);
            BTreePage::initialize_is_leaf(&mut root_page, true);
            BTreePage::initialize_height(&mut root_page, 0);
            BTreePage::set_next_id(&mut root_page, -1);
            BTreePage::set_prev_id(&mut root_page, -1);
            root_page.update_version(1);
            bpm.write_back(&mut root_page).unwrap();
            root_page.page_id()
        };
        bpm.unpin(root_id).unwrap();
        // Checkpoint.
        let log_entry = WalEntry::Checkpoint {
            root_id,
            root_height: 0,
            init_lsn: Some(init_lsn),
        };
        let log_entry = bincode::serialize(&log_entry).unwrap();
        let lsn = wal.enqueue(log_entry)?;
        wal.persist(Some(lsn))?;
        Ok((root_id, 0))
    }

    /// Get a value by key.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, LLError> {
        let (leaf, mut root_lock) = self.start_scan(key)?;
        let found = BTreePage::get(&leaf, key);
        self.free_page(&leaf, &mut root_lock);
        Ok(found.map(|v| v.into()))
    }

    /// Scan the BTree.
    /// TODO: Not yet tested.
    pub fn scan(
        &self,
        lo_key: Option<(&[u8], bool)>,
        hi_key: Option<(&[u8], bool)>,
        count: Option<usize>,
        forward: bool,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, LLError> {
        let (curr_scan_key, mut curr_leaf, mut root_lock): (
            Option<&[u8]>,
            bpm::SharedPage,
            Option<SharedRoot>,
        ) = if forward {
            if let Some((key, _)) = &lo_key {
                let (curr_leaf, root_lock) = self.start_scan(key)?;
                (Some(key), curr_leaf, root_lock)
            } else {
                let (curr_leaf, root_lock) = self.start_scan(&[])?;
                (None, curr_leaf, root_lock)
            }
        } else {
            if let Some((key, _)) = &hi_key {
                let (curr_leaf, root_lock) = self.start_scan(key)?;
                (Some(key), curr_leaf, root_lock)
            } else {
                let (curr_leaf, root_lock) = self.start_scan_from_rightmost()?;
                (None, curr_leaf, root_lock)
            }
        };
        let mut results = vec![];
        loop {
            let num_entries = BTreePage::num_entries(&curr_leaf);
            let (lo, hi) = if let Some(key) = &curr_scan_key {
                if let Some((bound, _, _)) = BTreePage::find_closest_elem(&curr_leaf, key) {
                    if forward {
                        (bound, num_entries)
                    } else {
                        (0, bound)
                    }
                } else {
                    (0, num_entries)
                }
            } else if forward {
                (0, num_entries)
            } else {
                (0, num_entries)
            };

            let iter: Vec<usize> = if forward {
                (lo..hi).collect()
            } else {
                (lo..hi).rev().collect()
            };
            let mut reached_end = false;
            for idx in iter {
                let (key, val) = BTreePage::get_item(&curr_leaf, idx);
                let mut included = if let Some((lo_key, lo_inclusive)) = &lo_key {
                    if *lo_inclusive && key < lo_key {
                        reached_end = !forward;
                        false
                    } else if !*lo_inclusive && key <= lo_key {
                        reached_end = !forward;
                        false
                    } else {
                        true
                    }
                } else {
                    true
                };
                included = included
                    && if let Some((hi_key, hi_inclusive)) = &hi_key {
                        if *hi_inclusive && key > hi_key {
                            reached_end = forward;
                            false
                        } else if !*hi_inclusive && key >= hi_key {
                            reached_end = forward;
                            false
                        } else {
                            true
                        }
                    } else {
                        true
                    };
                if included {
                    results.push((key.to_vec(), val.to_vec()));
                    if let Some(count) = count {
                        if results.len() >= count {
                            reached_end = true;
                            break;
                        }
                    }
                }
                if reached_end {
                    break;
                }
            }
            let next_leaf_id = if forward {
                BTreePage::next_id(&curr_leaf)
            } else {
                BTreePage::prev_id(&curr_leaf)
            };
            if next_leaf_id == -1 {
                reached_end = true;
            }
            if reached_end {
                self.free_page(&curr_leaf, &mut root_lock);
                break;
            }
            let next_leaf = self.try_lock_leaf(next_leaf_id, 10);
            if next_leaf.is_none() {
                self.free_page(&curr_leaf, &mut root_lock);
                return Err(LLError::BTreeError(
                    "Sibling leaf locked. Returning to avoid deadlock!".into(),
                ));
            }
            self.free_page(&curr_leaf, &mut root_lock);
            curr_leaf = next_leaf.unwrap();
        }
        Ok(results)
    }

    /// Insert a key-value pair.
    pub fn insert(&self, key: &[u8], val: &[u8]) -> Result<(), LLError> {
        let early_return = {
            let setup = self.setup.read().unwrap();
            setup.enable_early_return
        };
        if early_return {
            let (early_return_tx, early_return_rx) = mpsc::sync_channel(1);
            let this = self.clone();
            let (key, val) = (key.to_vec(), val.to_vec());
            {
                std::thread::spawn(move || {
                    let (key, val) = (&key, &val);
                    let inserted = this
                        .optimistic_write(key, Some(val), Some(early_return_tx.clone()))
                        .unwrap();
                    if !inserted {
                        this.pessimistic_write(key, Some(val), Some(early_return_tx))
                            .unwrap();
                        this.access_stats
                            .pessimistic_writes
                            .fetch_add(1, atomic::Ordering::AcqRel);
                    } else {
                        this.access_stats
                            .optimistic_writes
                            .fetch_add(1, atomic::Ordering::AcqRel);
                    }
                });
            }
            early_return_rx.recv().unwrap();
        } else {
            let inserted = self.optimistic_write(key, Some(val), None)?;
            if !inserted {
                self.pessimistic_write(key, Some(val), None)?;
                self.access_stats
                    .pessimistic_writes
                    .fetch_add(1, atomic::Ordering::AcqRel);
            } else {
                self.access_stats
                    .optimistic_writes
                    .fetch_add(1, atomic::Ordering::AcqRel);
            }
        }
        Ok(())
    }

    /// Delete a key.
    pub fn delete(&self, key: &[u8]) -> Result<(), LLError> {
        let early_return = {
            let setup = self.setup.read().unwrap();
            setup.enable_early_return
        };
        if early_return {
            let (early_return_tx, early_return_rx) = mpsc::sync_channel(1);
            let key = key.to_vec();
            let this = self.clone();
            {
                std::thread::spawn(move || {
                    let key = &key;
                    let deleted = this
                        .optimistic_write(key, None, Some(early_return_tx.clone()))
                        .unwrap();
                    if !deleted {
                        this.pessimistic_write(key, None, Some(early_return_tx))
                            .unwrap();
                    }
                });
            }
            early_return_rx.recv().unwrap();
        } else {
            let deleted = self.optimistic_write(key, None, None)?;
            if !deleted {
                self.pessimistic_write(key, None, None)?;
            }
        }
        Ok(())
    }

    /// Try to lock a leaf when scanning. Will return None to prevent deadlock.
    fn try_lock_leaf(&self, leaf_id: i64, retry_count: usize) -> Option<bpm::SharedPage> {
        let leaf = self.bpm.pin(leaf_id).unwrap();
        let mut locked = SharedPage::try_from(leaf.clone());
        for _ in 0..retry_count {
            if locked.is_ok() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_micros(10));
            locked = SharedPage::try_from(leaf.clone());
        }
        if locked.is_err() {
            self.bpm.unpin(leaf_id).unwrap();
            return None;
        }
        Some(locked.unwrap())
    }

    /// Free parents.
    fn free_parents(
        &self,
        parents: &mut Vec<bpm::ExclusivePage>,
        root: &mut Option<ExclusiveRoot>,
    ) {
        for parent in parents.iter() {
            self.bpm.unpin(parent.page_id()).unwrap();
        }
        parents.clear();
        *root = None;
    }

    /// Free predecessor.
    fn free_page(&self, page: &bpm::Page, root: &mut Option<SharedRoot>) {
        self.bpm.unpin(page.page_id()).unwrap();
        *root = None;
    }

    /// Find the child page to traverse to.
    fn find_child(&self, parent: &bpm::Page, key: &[u8]) -> i64 {
        let found = BTreePage::find_closest_elem(parent, key);
        let page_id = if let Some((_, _, val)) = found {
            val
        } else {
            let (_, val) = BTreePage::get_item(parent, 0);
            val
        };
        i64::from_be_bytes(page_id.try_into().unwrap())
    }

    /// Start scan from leaf containing the key.
    fn start_scan(&self, key: &[u8]) -> Result<(bpm::SharedPage, Option<SharedRoot>), LLError> {
        // Start traversal.
        let start_time = std::time::Instant::now();
        let (mut parent, mut root) = {
            let root = SharedRoot::from(self.root.clone());
            let (root_id, root_height) = *root;
            // Check if root is the only node.
            if root_height == 0 {
                let root_page = self.bpm.pin(root_id).unwrap();
                let root_page = bpm::SharedPage::from(root_page);
                return Ok((root_page, Some(root)));
            }
            // Regular traversal.
            let root_page = self.bpm.pin(root_id).unwrap();
            let root_page = bpm::SharedPage::from(root_page);
            (root_page, Some(root))
        };
        loop {
            // Get next page.
            let next_page_id = self.find_child(&parent, key);
            let next_page = self.bpm.pin(next_page_id).unwrap();
            let next_page = bpm::SharedPage::from(next_page);
            // Release previous one.
            self.free_page(&parent, &mut root);
            // Return if leaf.
            if BTreePage::is_leaf(&next_page) {
                let lock_time = start_time.elapsed().as_micros();
                self.access_stats
                    .leaf_lock_time
                    .fetch_add(lock_time as u64, atomic::Ordering::AcqRel);
                return Ok((next_page, root));
            }
            // Continue iteration
            parent = next_page;
        }
    }

    /// Start scan from rightmost leaf.
    fn start_scan_from_rightmost(&self) -> Result<(bpm::SharedPage, Option<SharedRoot>), LLError> {
        // Start traversal.
        let (mut parent, mut root) = {
            let root = SharedRoot::from(self.root.clone());
            let (root_id, root_height) = *root;
            // Check if root is the only node.
            if root_height == 0 {
                let root_page = self.bpm.pin(root_id).unwrap();
                let root_page = bpm::SharedPage::from(root_page);
                return Ok((root_page, Some(root)));
            }
            // Regular traversal.
            let root_page = self.bpm.pin(root_id).unwrap();
            let root_page = bpm::SharedPage::from(root_page);
            (root_page, Some(root))
        };
        loop {
            // Get next page.
            let num_entries = BTreePage::num_entries(&parent);
            let (_, next_page_id) = BTreePage::get_item(&parent, num_entries - 1);
            let next_page_id = i64::from_be_bytes(next_page_id.try_into().unwrap());
            let next_page = self.bpm.pin(next_page_id).unwrap();
            let next_page = bpm::SharedPage::from(next_page);
            // Release previous one.
            self.free_page(&parent, &mut root);
            // Return if leaf.
            if BTreePage::is_leaf(&next_page) {
                return Ok((next_page, root));
            }
            // Continue iteration
            parent = next_page;
        }
    }

    /// Perform an optimistic write (i.e. only leaf is write-locked).
    fn optimistic_write(
        &self,
        key: &[u8],
        val: Option<&[u8]>,
        early_return_tx: Option<mpsc::SyncSender<()>>,
    ) -> Result<bool, LLError> {
        let mut written = false;
        // Start traversal.
        let traversal_start_time = std::time::Instant::now();
        let (mut parent, mut root) = {
            let root = SharedRoot::from(self.root.clone());
            let (root_id, root_height) = *root;
            // Check if root is the only node.
            if root_height == 0 {
                let root_page = self.bpm.pin(root_id).unwrap();
                let mut root_page = bpm::ExclusivePage::from(root_page);
                // Attempt write.
                if let Some(val) = val {
                    // Try insert is enough space.
                    if BTreePage::has_space(&root_page, key, val) {
                        self.perform_insert(
                            &mut root_page,
                            &mut vec![],
                            &mut None,
                            key,
                            val,
                            early_return_tx,
                            None,
                        )?;
                        written = true;
                    }
                } else {
                    // Root delete is always possible.
                    self.perform_delete(
                        &mut root_page,
                        &mut vec![],
                        &mut None,
                        key,
                        early_return_tx,
                        None,
                    )?;
                    written = true;
                }
                // Unpin and return.
                self.bpm.unpin(root_id).unwrap();
                return Ok(written);
            }
            // Regular traversal.
            let root_page = self.bpm.pin(root_id).unwrap();
            let root_page = bpm::SharedPage::from(root_page);
            (root_page, Some(root))
        };
        loop {
            let next_page_id = self.find_child(&parent, key);
            let parent_height = BTreePage::height(&parent);
            let next_page = self.bpm.pin(next_page_id).unwrap();
            let p = next_page.clone();
            if parent_height > 1 {
                // Next page is node.
                // Lock next page.
                let next_page = bpm::SharedPage::from(next_page);
                // Release previous one.
                self.free_page(&parent, &mut root);
                // Keep next page locked.
                parent = next_page;
            } else {
                // Next page is leaf.
                // Lock leaf.
                let mut next_page = bpm::ExclusivePage::from(p);
                let lock_time = traversal_start_time.elapsed().as_micros();
                self.access_stats
                    .leaf_lock_time
                    .fetch_add(lock_time as u64, atomic::Ordering::AcqRel);
                // Release previous one.
                self.free_page(&parent, &mut root);
                // Write if possible.
                if let Some(val) = val {
                    if BTreePage::has_space(&next_page, key, val) {
                        self.perform_insert(
                            &mut next_page,
                            &mut vec![],
                            &mut None,
                            key,
                            val,
                            early_return_tx,
                            None,
                        )
                        .unwrap();
                        written = true;
                    }
                } else {
                    let del_size =
                        BTreePage::get(&next_page, key).map_or(0, |v| BTreePage::total_len(key, v));
                    if !BTreePage::too_empty(&next_page, del_size) {
                        self.perform_delete(
                            &mut next_page,
                            &mut vec![],
                            &mut None,
                            key,
                            early_return_tx,
                            None,
                        )
                        .unwrap();
                        written = true;
                    }
                }
                self.bpm.unpin(next_page_id).unwrap();
                break;
            }
        }
        Ok(written)
    }

    /// Perform a pessimistic write (i.e. all non-safe nodes are write-locked).
    fn pessimistic_write(
        &self,
        key: &[u8],
        val: Option<&[u8]>,
        early_return_tx: Option<mpsc::SyncSender<()>>,
    ) -> Result<(), LLError> {
        // Start traveral.
        let (mut parents, mut root) = {
            let root = ExclusiveRoot::from(self.root.clone());
            let (root_id, root_height) = *root;
            // Check if root is the only node.
            if root_height == 0 {
                let root_page = self.bpm.pin(root_id).unwrap();
                let mut root_page = bpm::ExclusivePage::from(root_page);
                if let Some(val) = val {
                    self.perform_insert(
                        &mut root_page,
                        &mut vec![],
                        &mut Some(root),
                        key,
                        val,
                        early_return_tx,
                        None,
                    )?;
                } else {
                    self.perform_delete(
                        &mut root_page,
                        &mut vec![],
                        &mut Some(root),
                        key,
                        early_return_tx,
                        None,
                    )?;
                }
                self.bpm.unpin(root_id).unwrap();
                return Ok(());
            }
            // Regular traversal.
            let root_page = self.bpm.pin(root_id).unwrap();
            let root_page = bpm::ExclusivePage::from(root_page);
            (vec![root_page], Some(root))
        };
        loop {
            let next_page_id = self.find_child(parents.last().unwrap(), key);
            let next_page = self.bpm.pin(next_page_id).unwrap();
            let mut next_page = bpm::ExclusivePage::from(next_page);
            // Check if should free parents.
            let free_parents = if BTreePage::is_leaf(&next_page) {
                if let Some(val) = val {
                    // For insert
                    BTreePage::has_space(&next_page, key, val)
                } else {
                    // For delete
                    let del_size =
                        BTreePage::get(&next_page, key).map_or(0, |v| BTreePage::total_len(key, v));
                    !BTreePage::too_empty(&next_page, del_size)
                }
            } else {
                if let Some(_) = val {
                    // For insert
                    BTreePage::is_inner_safe_for_insert(&next_page)
                } else {
                    // For delete. Do not yet know the size of the delete. So assume it is 40 (small key/val of size 8).
                    let del_size = 40;
                    !BTreePage::too_empty(&next_page, del_size)
                }
            };
            if free_parents {
                self.free_parents(&mut parents, &mut root);
            }

            if BTreePage::is_leaf(&next_page) {
                if let Some(val) = val {
                    self.perform_insert(
                        &mut next_page,
                        &mut parents,
                        &mut root,
                        key,
                        val,
                        early_return_tx,
                        None,
                    )?;
                } else {
                    self.perform_delete(
                        &mut next_page,
                        &mut parents,
                        &mut root,
                        key,
                        early_return_tx,
                        None,
                    )?;
                }
                self.bpm.unpin(next_page_id).unwrap();
                self.free_parents(&mut parents, &mut root);
                break;
            } else {
                parents.push(next_page);
            }
        }
        Ok(())
    }

    /// Perform insert operation.
    fn perform_insert(
        &self,
        page: &mut bpm::ExclusivePage,
        parents: &mut Vec<bpm::ExclusivePage>,
        root: &mut Option<ExclusiveRoot>,
        key: &[u8],
        val: &[u8],
        early_return_tx: Option<mpsc::SyncSender<()>>,
        recovery_info: Option<RecoveryInfo>,
    ) -> Result<(), LLError> {
        let sub_start_time = std::time::Instant::now();
        let recovering = recovery_info.is_some();
        let fully_written = recovering && (page.page_version() != recovery_info.unwrap().version);
        let wal_entry = WalEntry::Insert {
            page_id: page.page_id(),
            page_version: page.page_version(),
            parent_ids: parents.iter().map(|p| p.page_id()).collect(),
            key: key.into(),
            val: val.into(),
        };
        let start_time = std::time::Instant::now();
        let lsn = self.write_log_entry(wal_entry, &recovery_info)?;
        let log_time = start_time.elapsed().as_micros();
        self.access_stats
            .log_time
            .fetch_add(log_time as u64, atomic::Ordering::AcqRel);
        if fully_written {
            self.write_completion(lsn, true)?;
            return Ok(());
        }
        if BTreePage::has_space(page, key, val) {
            if let Some(early_return_tx) = early_return_tx {
                early_return_tx.send(()).unwrap();
                let sub_time = sub_start_time.elapsed().as_micros();
                self.access_stats
                    .lock_sub_time
                    .fetch_add(sub_time as u64, atomic::Ordering::AcqRel);
            }
            BTreePage::insert(page, key, val);
            self.bpm.write_back(page).unwrap();
            self.write_completion(lsn, false)?;
            return Ok(());
        }
        log::info!(
            "[BTREE] Splitting. Page={}. Version={}. Space={}.",
            page.page_id(),
            page.page_version(),
            BTreePage::occupied_space(page)
        );
        if let Some(early_return_tx) = early_return_tx {
            early_return_tx.send(()).unwrap();
        }
        let new_page = self.recover_or_create_page(lsn, recovering)?;
        let (new_page_id, split_key) = {
            let mut new_page = bpm::ExclusivePage::from(new_page);
            let should_write_back_new_page = !recovering || new_page.page_version() <= 0;
            // Move keys to new page.
            // IMPORTANT: Even when new page already written, this is necessary to remove the keys from the current page.
            let needed_space = BTreePage::total_len(key, val);
            let mut split_key = BTreePage::split(page, &mut new_page, needed_space).to_vec();
            if key < &split_key {
                BTreePage::insert(page, key, val);
            } else {
                BTreePage::insert(&mut new_page, key, val);
            }
            // IMPORTANT: Write back new page first to prevent move kvs from being lost.
            if should_write_back_new_page {
                self.bpm.write_back(&mut new_page).unwrap();
            } else {
                // Was already split. So this is the key to pass up.
                split_key = BTreePage::get_item(&new_page, 0).0.to_vec();
            }
            (new_page.page_id(), split_key)
        };
        log::info!(
            "[BTREE] Split Key: {:?}. New Page: {}",
            split_key,
            new_page_id
        );
        self.bpm.unpin(new_page_id).unwrap();
        let split_val = new_page_id.to_be_bytes().to_vec();
        // Recurse
        if !parents.is_empty() {
            // Inner node split.
            if !recovering {
                // Recurse only if not recovering. Otherwise recovery logic should take care of it.
                let mut parent = parents.pop().unwrap();
                self.perform_insert(
                    &mut parent,
                    parents,
                    root,
                    &split_key,
                    &split_val,
                    None,
                    None,
                )?;
                parents.push(parent);
            }
        } else {
            // Root split.
            // TODO: Get better logical ID. I think this works though.
            let new_root = self.recover_or_create_page(-lsn, recovering)?;
            // Root lock must be held.
            debug_assert!(root.is_some());
            let root_info = root.as_mut().unwrap().borrow_mut();
            let new_root_id = {
                let mut new_root = bpm::ExclusivePage::from(new_root);
                let new_root_id = new_root.page_id();
                if !recovering || new_root.page_version() <= 0 {
                    log::info!(
                        "[BTREE] Splitting root. Old Root: {}. New Root: {}",
                        root_info.0,
                        new_root_id
                    );
                    BTreePage::initialize(&mut new_root);
                    BTreePage::initialize_is_leaf(&mut new_root, false);
                    BTreePage::initialize_height(&mut new_root, root_info.1 + 1);
                    BTreePage::insert(&mut new_root, &split_key, &split_val);
                    BTreePage::insert(&mut new_root, &[], page.page_id().to_be_bytes().as_ref());
                    self.bpm.write_back(&mut new_root).unwrap();
                }
                root_info.0 = new_root_id;
                root_info.1 += 1;
                self.write_root_change(root_info)?;
                new_root_id
            };
            self.bpm.free_kept();
            self.bpm.unpin(new_root_id).unwrap();
        }
        log::info!(
            "[BTREE] Split complete. Page: {}. Version: {}",
            page.page_id(),
            page.page_version()
        );
        self.bpm.write_back(page).unwrap();
        self.write_completion(lsn, false)?;
        Ok(())
    }

    fn perform_delete(
        &self,
        page: &mut bpm::ExclusivePage,
        parents: &mut Vec<bpm::ExclusivePage>,
        root: &mut Option<ExclusiveRoot>,
        key: &[u8],
        early_return_tx: Option<mpsc::SyncSender<()>>,
        recovery_info: Option<RecoveryInfo>,
    ) -> Result<(), LLError> {
        let recovering = recovery_info.is_some();
        let fully_written = recovering && (page.page_version() != recovery_info.unwrap().version);
        // Do the actual delete.
        let wal_entry = WalEntry::Delete {
            page_id: page.page_id(),
            page_version: page.page_version(),
            parent_ids: parents.iter().map(|p| p.page_id()).collect(),
            key: key.into(),
        };
        let lsn = self.write_log_entry(wal_entry, &recovery_info)?;
        if let Some(early_return_tx) = early_return_tx {
            early_return_tx.send(()).unwrap();
        }
        if fully_written {
            self.write_completion(lsn, true)?;
            return Ok(());
        }
        // IMPORTANT: Actual write back should occur towards the end before completion.
        BTreePage::delete(page, key);
        // Special case for root with one child. Assumes num_entries=1 && parents.is_empty && !is_leaf ==> root.
        if parents.is_empty() && !BTreePage::is_leaf(page) && BTreePage::num_entries(page) == 1 {
            // Set child as new root.
            let (_, child_id) = BTreePage::get_item(page, 0);
            let child_id = i64::from_be_bytes(child_id.try_into().unwrap());
            log::info!(
                "[BTREE] Setting child as new root. Parent={parent_id}, Child={child_id}",
                parent_id = page.page_id()
            );
            debug_assert!(root.is_some());
            // Update root info.
            let root_info = root.as_mut().unwrap().borrow_mut();
            root_info.0 = child_id;
            debug_assert!(root_info.1 > 0);
            root_info.1 -= 1;
            // Mark as deleted.
            let delete_lsn = self.mark_page_delete(page)?;
            // Done. IMPORTANT: Anything that involves a root change should wait for completion.
            self.write_root_change(root_info)?;
            self.bpm.write_back(page).unwrap();
            self.write_completion(lsn, false)?;
            // Complete page deletion.
            self.complete_page_delete(page.page_id(), delete_lsn, recovering)?;
            self.bpm.free_kept();
            return Ok(());
        }
        // If not too empty, or root with more than one child.
        if !BTreePage::too_empty(page, 0) || parents.is_empty() {
            self.bpm.write_back(page).unwrap();
            self.write_completion(lsn, false)?;
            return Ok(());
        }
        // Get sibling.
        let mut parent = parents.pop().unwrap();
        // Key must map to this page.
        let page_idx = if let Some((idx, _, _)) = BTreePage::find_closest_elem(&parent, &key) {
            idx
        } else {
            0
        };
        // Find right sibling.
        // Only support right sibling for now to prevent deadlocks.
        let sibling_idx = if page_idx == BTreePage::num_entries(&parent) - 1 {
            self.bpm.write_back(page).unwrap();
            self.write_completion(lsn, false)?;
            parents.push(parent);
            return Ok(());
        } else {
            page_idx + 1
        };
        let (sibling_key, sibling_id) = BTreePage::get_item(&parent, sibling_idx);
        let (sibling_key, sibling_id) = (sibling_key.to_vec(), sibling_id.to_vec());
        let sibling_id = i64::from_be_bytes(sibling_id.try_into().unwrap());
        let sibling_page = self.bpm.pin(sibling_id)?;
        log::info!("[BTREE] Trying to merge. Page=({page_idx}, {page_id}). Sibling=({sibling_idx}, {sibling_id})", page_id=page.page_id());
        let mut sibling_page = bpm::ExclusivePage::from(sibling_page.clone());
        // Check if can merge.
        let page_delete_lsn = if BTreePage::occupied_space(page)
            < BTreePage::free_space(&sibling_page)
        {
            log::info!("[BTREE] Space is sufficient: {} < {}. Page=({page_idx}, {page_id}). Sibling=({sibling_idx}, {sibling_id})", BTreePage::occupied_space(page), BTreePage::free_space(&sibling_page), page_id=page.page_id());
            if !recovering {
                BTreePage::merge(page, &mut sibling_page);
                // Set sibling's next's prev to current.
                let next_sibling_id = BTreePage::next_id(&sibling_page);
                if next_sibling_id != -1 {
                    // Might deadlock? Probably not since delete is only implemented in one direction.
                    let next_sibling = self.bpm.pin(next_sibling_id)?;
                    let mut next_sibling = bpm::ExclusivePage::from(next_sibling);
                    BTreePage::set_prev_id(&mut next_sibling, page.page_id());
                    self.bpm.write_back(&mut next_sibling).unwrap();
                    BTreePage::set_next_id(page, next_sibling_id);
                    self.bpm.unpin(next_sibling_id).unwrap();
                }
            }
            // Mark sibling as deleted for recovery.
            let delete_lsn = self.mark_page_delete(&mut sibling_page)?;
            // Recurse.
            if !recovering {
                self.perform_delete(&mut parent, parents, root, &sibling_key, None, None)?;
            }
            Some(delete_lsn)
        } else {
            // TODO: Implement redistribution.
            self.bpm.unpin(sibling_id).unwrap();
            None
        };
        // Unpin if not deleted (can happen in root delete).
        if parent.page_version() != DELETE_VERSION {
            parents.push(parent);
        }
        self.bpm.write_back(page).unwrap();
        self.write_completion(lsn, false)?;
        if let Some(delete_lsn) = page_delete_lsn {
            self.complete_page_delete(sibling_id, delete_lsn, recovering)?;
            log::info!("[BTREE] Completed Page Delete. Page=({page_idx}, {page_id}). Sibling=({sibling_idx}, {sibling_id})", page_id=page.page_id());
        }
        Ok(())
    }

    /// Mark page as deleted.
    /// No need to wait for persist, since this occurs before the completion of actual kv delete.
    fn mark_page_delete(&self, page: &mut bpm::ExclusivePage) -> Result<i64, LLError> {
        page.update_version(DELETE_VERSION);
        self.bpm.write_back(page)?;
        let wal_entry = WalEntry::PageDelete {
            page_id: page.page_id(),
        };
        let wal_entry = bincode::serialize(&wal_entry).unwrap();
        let lsn = self.wal.enqueue(wal_entry)?;
        Ok(lsn)
    }

    /// Complete page deletion.
    /// No need to wait for persist, since delete is idempotent.
    fn complete_page_delete(
        &self,
        page_id: i64,
        lsn: i64,
        recovering: bool,
    ) -> Result<(), LLError> {
        self.bpm.delete_page(page_id, recovering)?;
        self.write_completion(lsn, false)?;
        Ok(())
    }

    /// Write completion entry. No need to wait persist.
    fn write_completion(&self, lsn: i64, force_completion: bool) -> Result<(), LLError> {
        let enable_completion = {
            let setup = self.setup.read().unwrap();
            setup.enable_completion
        };
        // Ops that need to wait for persist
        if !enable_completion && !force_completion {
            return Ok(());
        }
        let wal_entry = WalEntry::Completion { lsn };
        let wal_entry = bincode::serialize(&wal_entry).unwrap();
        let mut completion_info = self.completion_info.write().unwrap();
        let completion_lsn: i64 = self.wal.enqueue(wal_entry).unwrap();
        completion_info.completion_lsns.insert(completion_lsn, lsn);
        Ok(())
    }

    /// Write log entry.
    /// Return written lsn or recovery lsn.
    fn write_log_entry(
        &self,
        entry: WalEntry,
        recovery_info: &Option<RecoveryInfo>,
    ) -> Result<i64, LLError> {
        if let Some(RecoveryInfo { lsn, version: _ }) = recovery_info {
            // Ignore if recovering.
            return Ok(*lsn);
        }
        let wal_entry = bincode::serialize(&entry).unwrap();
        let lsn = {
            let mut completion_info = self.completion_info.write().unwrap();
            let lsn = self.wal.enqueue(wal_entry)?;
            completion_info.pending_lsns.insert(lsn);
            lsn
        };
        self.wal.persist(Some(lsn))?;
        Ok(lsn)
    }

    /// Recover or create a new page.
    fn recover_or_create_page(
        &self,
        lsn: i64,
        recovering: bool,
    ) -> Result<Arc<RwLock<bpm::Page>>, LLError> {
        if !recovering {
            Ok(self.bpm.create_page_with_logical_id(lsn)?)
        } else if let Some(page_id) = self.bpm.recover_physical_id(lsn)? {
            let page = self.bpm.pin(page_id)?;
            {
                let mut page = bpm::ExclusivePage::from(page.clone());
                if page.page_version() == DELETE_VERSION {
                    page.update_version(0);
                }
            }
            Ok(page)
        } else {
            Ok(self.bpm.create_page_with_logical_id(lsn)?)
        }
    }

    /// Write root change
    fn write_root_change(&self, root: &mut RootInfo) -> Result<(), LLError> {
        let checkpoint = WalEntry::Checkpoint {
            root_id: root.0,
            root_height: root.1,
            init_lsn: None,
        };
        let checkpoint = bincode::serialize(&checkpoint).unwrap();
        let checkpoint_lsn = self.wal.enqueue(checkpoint)?;
        self.wal.persist(Some(checkpoint_lsn))?;
        Ok(())
    }
}
