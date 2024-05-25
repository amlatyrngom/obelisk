use super::partition::Partition;
use super::{
    GlobalRecoveryState, LLError, ManagerReplayHandle, PartitionRecoveryState, WALPartition,
};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::params;
use std::collections::{BTreeMap, HashMap};
use std::sync::{mpsc, Arc, RwLock};

/// When injecting failures, sleep for this many seconds.
const FAILURE_INJECTION_SLEEP_SECS: u64 = 1;

/// Partition lock state.
/// Used to guarantee that only one thread is persisting to a partition at a time.
#[derive(Debug, Clone)]
enum PartitionLockState {
    /// Unlocked.
    Unlocked,
    /// Locked.
    Locked,
    /// Waiting on a channel.
    Waiting(mpsc::SyncSender<()>),
}

struct FlushInfo {
    /// Partition idx.
    partition_idx: usize,
    /// Entries to be persisted.
    entries: Vec<(Vec<u8>, i64)>,
    /// Signals to wait for ongoing flushes.
    signals: Vec<mpsc::Receiver<bool>>,
    /// Replicas
    replicas: Option<(String, String)>,
}

/// WAL Manager.
#[derive(Clone)]
pub struct Manager {
    /// Mutable stuff.
    inner: Arc<RwLock<ManagerInner>>,
    /// WAL partitions.
    partitions: Vec<Arc<dyn WALPartition>>,
    /// Next partition idx.
    next_partition_idx: Arc<RwLock<usize>>,
    /// Partition locks.
    partition_locks: Vec<Arc<RwLock<PartitionLockState>>>,
    /// Persist lock. Hold in read mode when persist. Hold in write mode when changing replicas.
    persist_lock: Arc<RwLock<()>>,
    /// Metadata DB.
    pool: Pool<SqliteConnectionManager>,
    /// Parallelism.
    parallelism: usize,
}

/// WAL Manager inner mutable stuff.
pub struct ManagerInner {
    /// Entries to be persisted.
    entries: Vec<(Vec<u8>, i64)>,
    /// The lsn of the next entry to be enqueued.
    curr_lsn: i64,
    /// The lsn of the last persisted entry.
    persist_lsn: i64,
    /// The lsn up to which the log has been truncated.
    truncate_lsn: i64,
    /// Signal end of ongoing flushes.
    flush_signals: HashMap<usize, Vec<mpsc::SyncSender<bool>>>,
    /// Replica addresses.
    replicas: Option<(String, String)>,
}

impl Manager {
    /// Create a new WAL manager.
    pub fn new(storage_dir: &str, name: &str, parallelism: usize) -> Result<Self, LLError> {
        Self::new_with_incarnation(storage_dir, name, parallelism, 0)
    }

    pub fn new_with_incarnation(
        storage_dir: &str,
        name: &str,
        parallelism: usize,
        incarnation: i64,
    ) -> Result<Self, LLError> {
        log::info!("[Manager::new] Creating manager for log {}.", name);
        let pool = Self::make_metadata_pool(storage_dir, name)?;
        log::debug!("[Manager::new] Recovering metadata.");
        let (truncate_lsn, replicas) = Self::recover_from_metadata(&pool)?;
        log::info!("[Manager::new] Recovered: truncate_lsn={truncate_lsn}. Replicas={replicas:?}.");
        let partitions = Self::make_wal_partitions(
            storage_dir,
            name,
            parallelism,
            &pool,
            replicas.clone(),
            incarnation,
        )?;
        let inner = ManagerInner {
            entries: Vec::new(),
            curr_lsn: 0,     // Will be recovered.
            persist_lsn: -1, // Will be recovered.
            truncate_lsn,
            flush_signals: HashMap::new(),
            replicas: None,
        };
        let inner = Arc::new(RwLock::new(inner));
        let manager = Self {
            inner: inner.clone(),
            partitions,
            next_partition_idx: Arc::new(RwLock::new(0)), // Will be recovered.
            partition_locks: (0..parallelism)
                .map(|_| Arc::new(RwLock::new(PartitionLockState::Unlocked)))
                .collect(),
            pool,
            parallelism,
            persist_lock: Arc::new(RwLock::new(())),
        };
        manager.recover_from_partitions()?;
        manager.set_replicas(None)?;
        Ok(manager)
    }

    pub fn start_lsn(&self) -> i64 {
        self.truncate_lsn()
    }

    /// Get the truncate lsn.
    pub fn truncate_lsn(&self) -> i64 {
        let inner = self.inner.read().unwrap();
        inner.truncate_lsn
    }

    /// Get the persist lsn.
    pub fn persist_lsn(&self) -> i64 {
        let inner = self.inner.read().unwrap();
        inner.persist_lsn
    }

    /// Get the curr lsn.
    pub fn curr_lsn(&self) -> i64 {
        let inner = self.inner.read().unwrap();
        inner.curr_lsn
    }

    /// Enqueue entry to be persisted, and return its lsn.
    pub fn enqueue(&self, entry: Vec<u8>) -> Result<i64, LLError> {
        let mut inner = self.inner.write().unwrap();
        let lsn = inner.curr_lsn;
        inner.curr_lsn += 1;
        inner.entries.push((entry, lsn));
        Ok(lsn)
    }

    /// Get flush info (partition_idx, ongoing_signals, entries).
    /// Return None and avoids incrementing next_partition_idx if there are no entries to flush.
    /// The next partition idx is chosen in order, making sure that only one thread is persisting to a partition at a time.
    fn get_flush_info(&self, at_lsn: Option<i64>) -> Option<FlushInfo> {
        // Check early exit before anything else. Note this is not a perfect check as entries may be flushed by another thread after this check.
        {
            let inner = self.inner.read().unwrap();
            if inner.entries.is_empty() {
                // Early exit.
                log::debug!("[Manager::get_flush_info] No entries to flush, returning early.");
                return None;
            }
            if let Some(at_lsn) = at_lsn {
                if inner.persist_lsn >= at_lsn {
                    return None;
                }
            }
        }
        // Get next partition idx.
        // For the sake of simplicity, hold next_partition_idx lock until getting flush info.
        // This is only slow-ish if there are more than PARALLELISM calls at the same time.
        // Even then, only extra calls will be blocked, and the ones that can be served will be served.
        let mut next_partition_idx = self.next_partition_idx.write().unwrap();
        let partition_idx = *next_partition_idx;
        // Increment next_partition_idx.
        *next_partition_idx = (*next_partition_idx + 1) % self.parallelism;
        // Lock partition; wait if necessary.
        let partition_lock = self.partition_locks[partition_idx].clone();
        let mut partition_lock = partition_lock.write().unwrap();
        match *partition_lock {
            PartitionLockState::Unlocked => {
                // Just lock.
                *partition_lock = PartitionLockState::Locked;
            }
            PartitionLockState::Locked => {
                // Mark as waiting.
                let (tx, rx) = mpsc::sync_channel(1);
                *partition_lock = PartitionLockState::Waiting(tx);
                // Manually release to allow current holder to unlock.
                std::mem::drop(partition_lock);
                // Wait for current holder to unlock.
                rx.recv().unwrap();
                // Now lock.
                let partition_lock = self.partition_locks[partition_idx].clone();
                let mut partition_lock = partition_lock.write().unwrap();
                // Must be currently unlocked.
                debug_assert!(matches!(*partition_lock, PartitionLockState::Unlocked));
                // Lock.
                *partition_lock = PartitionLockState::Locked;
            }
            PartitionLockState::Waiting(_) => {
                // Impossible. Would mean that two callers crossed the next partition idx lock.
                log::error!("PartitionLockState::Waiting should not be possible here, since next_partition_idx lock is held.");
                std::process::exit(1);
            }
        };
        // Get entries to persist, and signals to wait for ongoing flushes (which logically precede this one, but may finish persisting later).
        // Exit early if necessary.
        let (entries, signals, replicas) = {
            let mut inner = self.inner.write().unwrap();
            // Drain entries.
            let entries = inner.entries.drain(..).collect::<Vec<_>>();
            // Add self to signals to be notified when ongoing flushes are complete.
            let mut signals = Vec::new();
            for ongoing in inner.flush_signals.values_mut() {
                let (tx, rx) = mpsc::sync_channel(1);
                ongoing.push(tx);
                signals.push(rx);
            }
            // Add self to ongoing flushes.
            // Since we are holding the partition lock, this assert must be true.
            debug_assert!(inner.flush_signals.get(&partition_idx).is_none());
            inner.flush_signals.insert(partition_idx, Vec::new());
            (entries, signals, inner.replicas.clone())
        };
        // Done.
        Some(FlushInfo {
            partition_idx,
            entries,
            signals,
            replicas,
        })
    }

    /// Release partition lock.
    /// If there are any waiting threads, notify them.
    pub fn release_partition_lock(&self, partition_idx: usize) {
        let partition_lock = self.partition_locks[partition_idx].clone();
        let mut partition_lock = partition_lock.write().unwrap();
        match partition_lock.clone() {
            PartitionLockState::Unlocked => {
                panic!("PartitionLockState::Unlocked should not be possible here, since must have taken the lock.");
            }
            PartitionLockState::Locked => {
                *partition_lock = PartitionLockState::Unlocked;
            }
            PartitionLockState::Waiting(tx) => {
                *partition_lock = PartitionLockState::Unlocked;
                tx.send(()).unwrap();
            }
        };
    }

    /// Persist entries. Optionally specify an lsn for early return in case it is already persisted.
    pub fn persist(&self, at_lsn: Option<i64>) -> Result<(), LLError> {
        match self.controlled_persist(at_lsn, false, false, None) {
            Ok(_) => Ok(()),
            Err(x) => {
                log::error!(
                    "[Manager::persist] Error persisting entries: {err:?}",
                    err = x
                );
                std::process::exit(1);
            }
        }
    }

    /// Variant of persist that allows injecting failures.
    /// Note that the testing code is responsible correctness when using this variant.
    pub fn controlled_persist(
        &self,
        at_lsn: Option<i64>,
        inject_persist_failure: bool,
        inject_disk_failure: bool,
        extra_delay_secs: Option<f64>,
    ) -> Result<(), LLError> {
        // Take read lock for persist.
        let _persist_lock = self.persist_lock.read().unwrap();
        // Get flush info.
        let FlushInfo {
            partition_idx,
            entries,
            signals,
            replicas,
        } = match self.get_flush_info(at_lsn) {
            Some(flush_info) => flush_info,
            None => return Ok(()),
        };
        let partition = self.partitions[partition_idx].clone();
        // Persist logic goes here.
        let hi = if !entries.is_empty() {
            let hi = entries[entries.len() - 1].1;
            let lo = entries[0].1;
            log::debug!(
                "[Manager::persist {partition_idx}] Persisting entries. lo={lo}, hi={hi}.",
                partition_idx = partition_idx,
                lo = lo,
                hi = hi,
            );
            if !inject_persist_failure {
                if let Some(sleep_time) = extra_delay_secs {
                    log::debug!(
                        "[Manager::persist {partition_idx}] Sleeping for {sleep_time} seconds.",
                        sleep_time = sleep_time
                    );
                    std::thread::sleep(std::time::Duration::from_secs_f64(sleep_time));
                }
                match partition.persist(entries, replicas, inject_disk_failure) {
                    Ok(()) => {}
                    Err(err) => {
                        log::error!(
                            "[Manager::persist {partition_idx}] Error persisting entries: {err:?}"
                        );
                        std::process::exit(1);
                    }
                };
            } else {
                // Inject failure.
                log::info!("[Manager::persist {partition_idx}] Injecting failure.");
                std::thread::sleep(std::time::Duration::from_secs(FAILURE_INJECTION_SLEEP_SECS));
            }
            Some(hi)
        } else {
            None
        };

        // Now wait for ongoing flushes to complete.
        // all_ok will be false only when injecting failures in this thread or in another thread.
        let mut all_ok = !inject_persist_failure;
        for signal in signals {
            let ok = signal.recv().unwrap();
            if !ok {
                log::info!("[Manager::persist {partition_idx}] Previous ongoing persist failed!");
            }
            all_ok = all_ok && ok;
        }
        // Finishing logic.
        {
            let mut inner = self.inner.write().unwrap();
            // Update persist lsn.
            if all_ok && hi.is_some() {
                // Since waiting flushes are signaled after this, this assert must be true.
                // It means that flushes logically happen in order.
                let hi = hi.unwrap();
                if !(inner.persist_lsn < hi) {
                    log::error!(
                        "[Manager::persist {partition_idx}] Persist lsn must be less than hi: {persist_lsn} < {hi}.",
                        persist_lsn = inner.persist_lsn,
                        hi = hi,
                    );
                    std::process::exit(1);
                }
                debug_assert!(inner.persist_lsn < hi);
                inner.persist_lsn = hi;
            }

            // Remove self from ongoing flushes, and signal all waiting flushes.
            let waiting = inner.flush_signals.remove(&partition_idx).unwrap();
            if !waiting.is_empty() {
                log::debug!("[Manager::persist {partition_idx}] Signaling waiting flushes.");
                std::thread::spawn(move || {
                    for tx in waiting {
                        tx.send(all_ok).unwrap();
                    }
                });
            }
        }
        // Release partition lock.
        self.release_partition_lock(partition_idx);
        // Done.
        if all_ok {
            log::debug!(
                "[Manager::persist {partition_idx}] Successfully persisted entries.",
                partition_idx = partition_idx,
            );
            Ok(())
        } else {
            Err(LLError::WALError("Could not persist".into()))
        }
    }

    /// Replay log.
    /// At the first call, the handle is None.
    /// At subsequent calls, the handle is the one returned by the previous call.
    pub fn replay(
        &self,
        manager_handle: Option<ManagerReplayHandle>,
    ) -> Result<(Option<ManagerReplayHandle>, Vec<(Vec<u8>, i64)>), LLError> {
        // Start replay if necessary.
        let manager_handle = match manager_handle {
            Some(manager_handle) => manager_handle,
            None => match self.first_replay_step()? {
                Some(manager_handle) => manager_handle,
                None => return Ok((None, vec![])),
            },
        };
        log::debug!(
            "[Manager::replay] Replaying from last returned lsn: {:?}",
            manager_handle.last_returned_lsn,
        );
        // Establish continuity if necessary.
        let mut manager_handle =
            match self.reestablish_continuity_if_discontinous(manager_handle)? {
                Some(manager_handle) => manager_handle,
                None => return Ok((None, vec![])),
            };
        // Sanity check: The new entries must be contiguous with the old ones.
        debug_assert!(manager_handle.entries.len() > 0);
        let first_lsn = *manager_handle.entries.iter().next().unwrap().0;
        if first_lsn != manager_handle.last_returned_lsn + 1 {
            log::error!(
                "[Manager::replay] First lsn must be contiguous with last returned lsn: {first_lsn} == {last_returned_lsn} + 1.",
                first_lsn = first_lsn,
                last_returned_lsn = manager_handle.last_returned_lsn,
            );
            std::process::exit(1);
        }
        debug_assert!(first_lsn == manager_handle.last_returned_lsn + 1);
        log::debug!(
            "[Manager::replay] Reestablished continuity: {last_returned_lsn} --> {first_lsn}",
            last_returned_lsn = manager_handle.last_returned_lsn,
        );
        // Get next entries.
        let replayed_entries = self.get_next_entries(&mut manager_handle)?;
        Ok((Some(manager_handle), replayed_entries))
    }

    /// Get the first replay step.
    fn first_replay_step(&self) -> Result<Option<ManagerReplayHandle>, LLError> {
        let (persist_lsn, truncate_lsn) = {
            let inner = self.inner.read().unwrap();
            (inner.persist_lsn, inner.truncate_lsn)
        };
        let mut entries = BTreeMap::new();
        let mut partition_handles = Vec::new();
        // Call replay on each partition in parallel.
        let mut threads = Vec::new();
        for partition in &self.partitions {
            let partition = partition.clone();
            threads.push(std::thread::spawn(move || {
                let (entries, handle) = partition.replay(None)?;
                Ok::<_, LLError>((entries, handle))
            }));
        }
        // Collect results.
        // Set any value less than -1.
        let mut last_returned_lsn = (-1) - 1;
        for thread in threads {
            let (partition_entries, partition_handle) = thread.join().unwrap()?;
            for (entry, lsn) in partition_entries {
                if lsn <= truncate_lsn || lsn > persist_lsn {
                    // Skip truncated entries or persisted entries (may occur when persist called in parallel to replay).
                    continue;
                }
                if last_returned_lsn < -1 || lsn <= last_returned_lsn {
                    // Set to before smallest seen so far (or to first when ==-1).
                    last_returned_lsn = lsn - 1;
                }
                // Add to sorted entries.
                entries.insert(lsn, entry);
            }
            partition_handles.push(partition_handle);
        }
        if entries.is_empty() {
            // No entries in any partition.
            log::info!(
                "[Manager::first_replay_step] No entries in any partition, returning early."
            );
            return Ok(None);
        }
        let handle = ManagerReplayHandle {
            entries,
            partition_handles,
            last_returned_lsn,
            persist_lsn,
            truncate_lsn,
        };
        Ok(Some(handle))
    }

    /// Called to fetch new entries when the entries in the handle are exhausted or not contiguous.
    fn reestablish_continuity_if_discontinous(
        &self,
        mut manager_handle: ManagerReplayHandle,
    ) -> Result<Option<ManagerReplayHandle>, LLError> {
        // If already contiguous, return immediately.
        if manager_handle
            .entries
            .contains_key(&(manager_handle.last_returned_lsn + 1))
        {
            return Ok(Some(manager_handle));
        }
        // Read from each partition to reestablish continuity, or to confirm that there are no more entries.
        let old_partition_handles = manager_handle
            .partition_handles
            .drain(..)
            .collect::<Vec<_>>();
        let mut threads = Vec::new();
        for (partition, partition_handle) in self
            .partitions
            .iter()
            .zip(old_partition_handles.into_iter())
        {
            let partition = partition.clone();
            threads.push(std::thread::spawn(move || {
                if partition_handle.is_none() {
                    // Already done replaying on this partition.
                    return Ok::<_, LLError>((Vec::new(), None));
                }
                let (entries, handle) = partition.replay(partition_handle)?;
                Ok::<_, LLError>((entries, handle))
            }));
        }
        let mut new_partition_handles = Vec::new();
        let mut all_done_replaying = true;
        for thread in threads {
            let (partition_entries, partition_handle) = thread.join().unwrap()?;
            if partition_handle.is_some() {
                // Still replaying on this partition.
                all_done_replaying = false;
            }
            new_partition_handles.push(partition_handle);
            for (entry, lsn) in partition_entries {
                if lsn <= manager_handle.truncate_lsn || lsn > manager_handle.persist_lsn {
                    // Skip truncated entries or persisted entries (may occur when persist called in parallel to replay).
                    continue;
                }
                // Add to sorted entries.
                manager_handle.entries.insert(lsn, entry);
            }
        }
        if all_done_replaying {
            // All partitions are done replaying. Must have found all entries.
            // println!("All partitions are done replaying. Must have found all entries. {}", manager_handle.entries.len());
            debug_assert!(manager_handle.entries.is_empty());
            return Ok(None);
        }
        // Return new handle.
        manager_handle.partition_handles = new_partition_handles;
        Ok(Some(manager_handle))
    }

    /// Advance the replay step.
    fn get_next_entries(
        &self,
        manager_handle: &mut ManagerReplayHandle,
    ) -> Result<Vec<(Vec<u8>, i64)>, LLError> {
        // Return entries contiguous with last_returned_lsn.
        let mut to_return = Vec::new();
        for lsn in manager_handle.entries.keys() {
            if *lsn == manager_handle.last_returned_lsn + 1 {
                // Contiguous.
                manager_handle.last_returned_lsn = *lsn;
                to_return.push(*lsn);
            } else {
                // Break at first discontinuity.
                log::debug!(
                    "Breaking at lsn {lsn}. Discontinuous with {}",
                    manager_handle.last_returned_lsn
                );
                break;
            }
        }
        // Delete contiguous entries from manager_handle.
        let replayed_entries = to_return
            .iter()
            .map(|lsn| (manager_handle.entries.remove(lsn).unwrap(), *lsn))
            .collect::<Vec<_>>();
        // Some entries must have been found because of the logic in reestablish_continuity_if_discontinous.
        Ok(replayed_entries)
    }

    /// Recover the state of the manager from the partitions.
    fn recover_from_partitions(&self) -> Result<(), LLError> {
        // Get all states.
        let states = self.recover_states_from_partitions()?;
        // The global persist lsn is the first persist lsn discontinuous with its successor.
        // If there is no such persist lsn, then the global persist lsn is the largest persist lsn.
        let mut persist_lsns = states
            .iter()
            .enumerate()
            .map(|(idx, state)| {
                (
                    idx,
                    state.persist_lsns.2,
                    state.persist_lsns.0,
                    state.persist_lsns.1,
                )
            })
            .collect::<Vec<_>>();
        persist_lsns.sort_by_key(|(_, _, _, hi)| *hi);
        let mut next_partition_idx = 0;
        let (_, _, _, mut global_persist_lsn) = persist_lsns[0].clone();
        let mut was_discontinuous = false;
        for (idx, _prev_hi, lo, hi) in persist_lsns[1..].iter() {
            // Note that at the beginning, some los or his may be -1. This is fine, since the first valid lo will be 0 = (-1) + 1.
            if *lo > global_persist_lsn + 1 {
                // Discontinuous.
                was_discontinuous = true;
                break;
            }
            next_partition_idx = (*idx + 1) % self.parallelism;
            global_persist_lsn = *hi;
        }
        if was_discontinuous {
            // When discountinuous, the global persist lsn could be a previous hi lsn.
            let (max_prev_idx, max_prev_hi) = persist_lsns
                .iter()
                .map(|(idx, prev_hi, _, _)| (idx, prev_hi))
                .max_by_key(|(_, prev_hi)| *prev_hi)
                .unwrap();
            if *max_prev_hi > global_persist_lsn {
                global_persist_lsn = *max_prev_hi;
                next_partition_idx = (*max_prev_idx + 1) % self.parallelism;
            }
        }
        // Apply global recovery state to all partitions.
        let global_recovery_state = GlobalRecoveryState {
            persist_lsn: global_persist_lsn,
        };
        self.apply_global_recovery_state(global_recovery_state)?;
        // Set the manager state.
        {
            let mut inner = self.inner.write().unwrap();
            inner.persist_lsn = global_persist_lsn;
            inner.curr_lsn = global_persist_lsn + 1;
            log::info!("[Manager::recover_from_partitions] Found global persist lsn: {global_persist_lsn}.");
        }
        {
            let mut next_idx = self.next_partition_idx.write().unwrap();
            *next_idx = next_partition_idx;
        }
        Ok(())
    }

    /// Recover states from partitions.
    fn recover_states_from_partitions(&self) -> Result<Vec<PartitionRecoveryState>, LLError> {
        let mut states = Vec::new();
        for partition in &self.partitions {
            let partition = partition.clone();
            let state = partition.get_recovery_state()?;
            states.push(state);
        }
        Ok(states)
    }

    /// Apply global recovery state to partitions in parallel.
    fn apply_global_recovery_state(
        &self,
        global_recovery_state: GlobalRecoveryState,
    ) -> Result<(), LLError> {
        let mut apply_futures = Vec::new();
        for partition in &self.partitions {
            let partition = partition.clone();
            let global_recovery_state = global_recovery_state.clone();
            let apply_future = std::thread::spawn(move || {
                partition.apply_global_recovery_state(global_recovery_state)?;
                Ok::<_, LLError>(())
            });
            apply_futures.push(apply_future);
        }
        for apply_future in apply_futures {
            apply_future.join().unwrap()?;
        }
        Ok(())
    }

    /// Truncate log up to and including lsn.
    pub fn truncate(&self, lsn: i64) -> Result<(), LLError> {
        log::info!("[Manager::truncate] Truncating log up to: {lsn}.");
        let mut conn = self.pool.get().unwrap();
        let txn = conn.transaction().unwrap();
        // Set truncate_lsn in the global info table.
        let stmt = "UPDATE global_info SET truncate_lsn = ?";
        txn.execute(stmt, params![lsn]).unwrap();
        // Move all slabs with highest_persist_lsn <= lsn to free_slabs.
        let stmt = "INSERT INTO free_slabs (slab_id, slab_offset) SELECT slab_id, slab_offset FROM busy_slabs WHERE highest_persist_lsn >= 0 AND highest_persist_lsn <= ?";
        txn.execute(stmt, params![lsn]).unwrap();
        // Delete from busy_slabs.
        let stmt = "DELETE FROM busy_slabs WHERE highest_persist_lsn <= ?";
        txn.execute(stmt, params![lsn]).unwrap();
        // Commit
        txn.commit().unwrap();
        // Update state.
        let mut inner = self.inner.write().unwrap();
        inner.truncate_lsn = lsn;
        // Done.
        Ok(())
    }

    /// Recover state from metadata.
    fn recover_from_metadata(
        pool: &Pool<SqliteConnectionManager>,
    ) -> Result<(i64, Option<(String, String)>), LLError> {
        // Read truncate_lsn from global_info.
        let conn = pool.get().unwrap();
        let stmt = "SELECT truncate_lsn, replicas FROM global_info WHERE unique_row=0";
        let res = conn.query_row(stmt, [], |r| {
            let truncate_lsn: i64 = r.get(0).unwrap();
            let replicas: Option<String> = r.get(1).unwrap();
            Ok((truncate_lsn, replicas))
        });
        // If not exist, initialize.
        match res {
            Ok((truncate_lsn, replicas)) => {
                let replicas = match replicas {
                    Some(replicas) => serde_json::from_str(&replicas).unwrap(),
                    None => None,
                };
                Ok((truncate_lsn, replicas))
            }
            Err(e) => Err(LLError::WALError(e.to_string())),
        }
    }

    /// Create the metadata pool.
    fn make_metadata_pool(
        storage_dir: &str,
        name: &str,
    ) -> Result<Pool<SqliteConnectionManager>, LLError> {
        let metadata_filename = super::metadata_filename(storage_dir, name);
        // Open flags with timeout set to 1s.
        let manager = SqliteConnectionManager::file(&metadata_filename);
        log::debug!("[Manager::make_metadata_pool] Creating metadata pool.");
        let pool = r2d2::Builder::new()
            .max_size(1)
            .build(manager)
            .map_err(|e| LLError::WALError(format!("Failed to create pool: {}", e)))?;
        let conn = pool.get().unwrap();
        // Set timeout.
        conn.busy_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        // {
        //     // Set locking mode to EXCLUSIVE; journal mode to WAL; synchronous to FULL.
        //     conn.execute_batch("PRAGMA locking_mode=EXCLUSIVE; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;").unwrap();
        // }
        // Create schema.
        loop {
            match conn.execute_batch(include_str!("schema.sql")) {
                Ok(_) => break,
                Err(e) => {
                    eprintln!("[wal::Manager::make_metadata_pool] Error creating schema: {e:?}. Retrying!");
                    std::thread::sleep(std::time::Duration::from_secs(1));
                }
            }
        }
        // Done.
        Ok(pool)
    }

    /// Make WAL partitions in parallel.
    fn make_wal_partitions(
        storage_dir: &str,
        name: &str,
        parallelism: usize,
        pool: &Pool<SqliteConnectionManager>,
        replicas: Option<(String, String)>,
        incarnation: i64,
    ) -> Result<Vec<Arc<dyn WALPartition>>, LLError> {
        // Now open many handles to the same file in parallel.
        let mut partitions: Vec<Arc<dyn WALPartition>> = Vec::new();
        let mut js = Vec::new();
        for partition_idx in 0..parallelism {
            let pool = pool.clone();
            let name = name.to_string();
            let storage_dir = storage_dir.to_string();
            let replicas = replicas.clone();
            js.push(std::thread::spawn(move || {
                let partition = Partition::new(
                    &storage_dir,
                    &name,
                    partition_idx,
                    pool,
                    replicas,
                    incarnation,
                )?;
                Ok(Arc::new(partition))
            }));
        }
        for j in js {
            let partition = j.join().unwrap()?;
            partitions.push(partition);
        }
        Ok(partitions)
    }

    /// Set replicas
    pub fn set_replicas(&self, replicas: Option<(String, String)>) -> Result<(), LLError> {
        // Check if changed.
        {
            let inner = self.inner.read().unwrap();
            if inner.replicas == replicas {
                return Ok(());
            }
        }
        // Coordinate replicas change.
        let _persist_lock = self.persist_lock.write().unwrap();
        let mut _locks = vec![];
        for partition in &self.partitions {
            let lock = partition.get_replica_lock().unwrap();
            let lock = guardian::ArcMutexGuardian::from(lock);
            _locks.push(lock);
        }
        // Change global state.
        let conn = self.pool.get().unwrap();
        let replica_info = serde_json::to_string(&replicas).unwrap();
        let stmt = "UPDATE global_info SET replicas=?";
        conn.execute(stmt, params![replica_info]).unwrap();
        let mut inner = self.inner.write().unwrap();
        inner.replicas = replicas;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_pool_parallelism() {
        let metadata_filename = format!("essai.db");
        let _ = fs::remove_file(&metadata_filename);
        // Open flags with timeout set to 1s.
        let manager = SqliteConnectionManager::file(&metadata_filename);
        let pool = r2d2::Builder::new()
            .max_size(10)
            .build(manager)
            .map_err(|e| LLError::WALError(format!("Failed to create pool: {}", e)))
            .unwrap();
        let pool = Arc::new(pool);
        // Run schema file.
        {
            let conn = pool.get().unwrap();
            conn.execute_batch(include_str!("schema.sql")).unwrap();
        }
        let mut open_threads = Vec::new();
        let start_time = std::time::Instant::now();
        for i in 0..8 {
            let pool = pool.clone();
            let start_time = start_time.clone();
            open_threads.push(std::thread::spawn(move || {
                let pre_conn_time = start_time.elapsed();
                log::debug!("Thread {} getting connection at {:?}", i, pre_conn_time);
                let conn = pool.get().unwrap();
                let conn_time = start_time.elapsed();
                log::debug!("Thread {} got connection in {:?}", i, conn_time);
                conn.execute(
                    "INSERT INTO free_slabs (slab_id, file_offset) VALUES (?, ?)",
                    [i, 0],
                )
                .unwrap();
                std::thread::sleep(std::time::Duration::from_secs(3));
                let end_time = start_time.elapsed();
                log::debug!("Thread {} finished in {:?}", i, end_time);
            }));
        }
        for t in open_threads {
            t.join().unwrap();
        }
    }
}
