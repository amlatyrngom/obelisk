use super::replica::ReplicaReq;
use super::{
    replica, GlobalRecoveryState, LLError, PartitionRecoveryState, PartitionReplayHandle,
    WALPartition, SLAB_SIZE,
};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::params;
use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, Write};
use std::ops::DerefMut;
use std::os::unix::fs::OpenOptionsExt;
use std::sync::{mpsc, Arc, Condvar, Mutex, RwLock};

/// Number of slabs to read in replay steps.
pub const REPLAY_SLAB_COUNT: usize = 10;
/// Maximum lsn. Marks an ongoing slab.
pub const MAX_LSN: i64 = i64::MAX - 1;
/// Whether to `sync_all` or `flush` after writing a log buffer.
#[cfg(target_os = "linux")]
const USE_SYNC_ALL: bool = false;
#[cfg(target_os = "macos")]
const USE_SYNC_ALL: bool = false;

pub struct Partition {
    pub partition_idx: usize,
    log_filename: String,
    file: Arc<RwLock<fs::File>>,
    inner: Arc<RwLock<PartititionInner>>,
    pool: Pool<SqliteConnectionManager>,
    recovery_lsns: (i64, i64, i64),
    incarnation: i64,
    client: ureq::Agent,
    pub replica_lock: Arc<Mutex<()>>,
    ongoing_lock: Arc<Mutex<usize>>,
    ongoing_condvar: Arc<Condvar>,
}

struct PartititionInner {
    curr_slab: SlabInfo,
    write_offset: usize,
    prev_written_lsn: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SlabInfo {
    slab_id: i64,
    owner_partition_idx: usize,
    slab_offset: usize,
    highest_persist_lsn: i64,
}

impl WALPartition for Partition {
    fn persist(
        &self,
        logs: Vec<(Vec<u8>, i64)>,
        replicas: Option<(String, String)>,
        inject_disk_failure: bool,
    ) -> Result<(), LLError> {
        // Serialize logs.
        let batch = bincode::serialize(&logs).unwrap();
        // Compute checksum.
        let checksum = crc32fast::hash(&batch);
        // Make write buffer.
        let mut buf: Vec<u8> = Vec::new();
        // Write length usize.
        let len = batch.len();
        buf.extend_from_slice(&len.to_be_bytes());
        // Write batch.
        buf.extend_from_slice(&batch);
        // Write checksum.
        buf.extend_from_slice(&checksum.to_be_bytes());
        // Write.
        let hi_lsn = logs[logs.len() - 1].1;
        if let Some((url1, url2)) = replicas {
            // Replica request.
            let replica_req = ReplicaReq::Primary {
                incarnation: self.incarnation,
                partition_idx: self.partition_idx,
                secondary_url: url2,
                entries: logs.clone(),
            };
            let payload = bincode::serialize(&replica_req).unwrap();
            // I must hold the locks before spawning the thread until the log is written to the file AND sent to the replicas.
            // This simplifies things since another flush will not be started until the previous one is FULLY done.
            // This complex logic ensure that, since I cannot take a lock outside the thread and free inside (rust nonsense).
            let (done_tx, done_rx) = mpsc::sync_channel(2);
            let mut ongoing = self.ongoing_lock.lock().unwrap();
            while *ongoing != 0 {
                // println!("Waiting for ongoing to finish {ongoing}.");
                ongoing = self.ongoing_condvar.wait(ongoing).unwrap();
            }
            *ongoing = 2;
            self.write_and_replicate(buf, hi_lsn, url1, payload, done_tx, inject_disk_failure)
                .unwrap();
            // Wait for either to successfully finish.
            let done = done_rx.recv().unwrap();
            if !done {
                let done = done_rx.recv().unwrap();
                assert!(done);
            }
        } else {
            self.write_buffer(buf, hi_lsn)?;
        };
        // Done.
        Ok(())
    }

    fn replay(
        &self,
        handle: Option<PartitionReplayHandle>,
    ) -> Result<(Vec<(Vec<u8>, i64)>, Option<PartitionReplayHandle>), LLError> {
        // Allocate first if necessary.
        let handle = match handle {
            Some(handle) => handle,
            None => match self.first_replay_step()? {
                Some(handle) => handle,
                None => return Ok((Vec::new(), None)),
            },
        };
        // Cache next slabs if necessary.
        let mut handle = match self.cache_next_slabs_if_empty(handle)? {
            Some(handle) => handle,
            None => return Ok((Vec::new(), None)),
        };
        // Previous call would have returned None is the cache is empty.
        debug_assert!(!handle.cached_offsets.is_empty());
        // Remove first offset.
        let replay_offset = handle.cached_offsets.remove(0);
        // Read slab from file.
        log::debug!(
            "[Partition::replay {}] Replaying slab offset {replay_offset}.",
            self.partition_idx
        );
        let entries = Self::replay_slab(&mut handle, replay_offset)?;
        // log::debug!(
        //     "[Partition::replay {}] Found entries lsns: {:?}.",
        //     self.partition_idx,
        //     entries
        //         .iter()
        //         .map(|e| e.1)
        //         .collect::<Vec<_>>()
        // );
        // Done.
        Ok((entries, Some(handle)))
    }

    fn get_recovery_state(&self) -> Result<PartitionRecoveryState, LLError> {
        Ok(PartitionRecoveryState {
            persist_lsns: self.recovery_lsns,
        })
    }

    fn apply_global_recovery_state(
        &self,
        global_state: GlobalRecoveryState,
    ) -> Result<(), LLError> {
        let persist_lsn = global_state.persist_lsn;
        self.remove_extra_persist(persist_lsn)?;
        // Important for subsequent persists.
        self.reset_file_seek()?;
        Ok(())
    }

    fn get_replica_lock(&self) -> Result<Arc<Mutex<()>>, LLError> {
        Ok(self.replica_lock.clone())
    }
}

impl Partition {
    /// Open log file for MAC.
    #[cfg(target_os = "macos")]
    fn open_log_file(storage_dir: &str, name: &str) -> Result<fs::File, LLError> {
        let log_filename = super::log_filename(storage_dir, name);
        log::info!("[Partition::open_log_file] Opening file {log_filename}");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .custom_flags(libc::O_SYNC)
            .open(&log_filename)
            .map_err(|e| LLError::WALError(e.to_string()))?;
        Ok(file)
    }

    /// Open log file for Linux.
    #[cfg(target_os = "linux")]
    fn open_log_file(storage_dir: &str, name: &str) -> Result<fs::File, LLError> {
        let log_filename = super::log_filename(storage_dir, name);
        log::info!("[Partition::open_log_file] Opening file {log_filename}");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .custom_flags(libc::O_SYNC)
            .open(&log_filename)
            .map_err(|e| LLError::WALError(e.to_string()))?;
        Ok(file)
    }

    pub fn new(
        storage_dir: &str,
        name: &str,
        partition_idx: usize,
        pool: Pool<SqliteConnectionManager>,
        replicas: Option<(String, String)>,
        incarnation: i64,
    ) -> Result<Self, LLError> {
        let client = ureq::AgentBuilder::new()
            .timeout_connect(std::time::Duration::from_secs(1))
            .timeout_read(std::time::Duration::from_secs(1))
            .timeout_write(std::time::Duration::from_secs(1))
            .build();
        let log_filename = super::log_filename(storage_dir, name);
        log::info!("[Partition::new {partition_idx}] Opening file {log_filename}");
        let mut file = Self::open_log_file(storage_dir, name)?;
        let curr_slab =
            Self::recover_slab_or_allocate_first(pool.clone(), &mut file, partition_idx)?;
        println!("Calling recover_from_replicas().");
        let replica_entries =
            Self::recover_from_replicas(&client, partition_idx, replicas, incarnation)?;
        log::info!("[Partition::new {partition_idx}] Recovered current slab {curr_slab:?}");
        if replica_entries.is_empty() {
            log::info!("[Partition::new {partition_idx}] No replica entries found.");
        } else {
            log::info!(
                "[Partition::new {partition_idx}] Recovered replica entries ({lo}, {hi})",
                lo = replica_entries[0].1,
                hi = replica_entries[replica_entries.len() - 1].1
            );
        }
        let (lo_lsn, hi_lsn, prev_hi_lsn, write_offset) = Self::recover_partition_persist_state(
            pool.clone(),
            &mut file,
            partition_idx,
            curr_slab.clone(),
            replica_entries,
        )?;
        let recovery_lsns = (lo_lsn, hi_lsn, prev_hi_lsn);
        log::info!("[Partition::new {partition_idx}] Recovered last persist {recovery_lsns:?}");
        let inner = Arc::new(RwLock::new(PartititionInner {
            curr_slab,
            write_offset,
            prev_written_lsn: hi_lsn,
        }));
        let file = Arc::new(RwLock::new(file));
        let replica_lock = Arc::new(Mutex::new(()));
        Ok(Self {
            partition_idx,
            log_filename,
            inner,
            pool,
            file,
            recovery_lsns,
            replica_lock,
            incarnation,
            client,
            ongoing_lock: Arc::new(Mutex::new(0)),
            ongoing_condvar: Arc::new(Condvar::new()),
        })
    }

    fn fail_io(e: std::io::Error) -> () {
        eprintln!("Failed IO: {e:?}!");
        std::process::exit(1);
    }

    /// Write and replicate
    fn write_and_replicate(
        &self,
        buf: Vec<u8>,
        hi_lsn: i64,
        primary_url: String,
        payload: Vec<u8>,
        done_tx: mpsc::SyncSender<bool>,
        inject_disk_failure: bool,
    ) -> Result<(), LLError> {
        let buf_len = buf.len();
        let should_allocate_next = {
            let inner = self.inner.read().unwrap();
            inner.write_offset + buf_len > SLAB_SIZE
        };
        if should_allocate_next {
            self.allocate_next_slab()?;
            self.reset_file_seek()?;
        }
        let file = self.file.clone();
        let inner = self.inner.clone();
        let file_tx = done_tx.clone();
        let ongoing = self.ongoing_lock.clone();
        let condvar = self.ongoing_condvar.clone();
        let partition_idx = self.partition_idx;
        std::thread::spawn(move || {
            let mut file = file.write().unwrap();
            if inject_disk_failure {
                log::info!("Injecting disk failure!");
                std::thread::sleep(std::time::Duration::from_secs_f64(0.5));
                std::process::exit(1);
            }
            {
                let mut buf_writer = std::io::BufWriter::new(file.deref_mut());
                buf_writer.write_all(&buf).unwrap_or_else(Self::fail_io);
                buf_writer.flush().unwrap_or_else(Self::fail_io);
            }
            if USE_SYNC_ALL {
                file.sync_all().unwrap_or_else(Self::fail_io);
            } else {
                file.flush().unwrap_or_else(Self::fail_io);
            }
            let mut inner = inner.write().unwrap();
            log::debug!(
                "[Partition::write_and_replicate {partition_idx}] Wrote buffer of len {buf_len} at offset {write_offset}. HiLsn={hi_lsn}.",
                write_offset = inner.write_offset,
            );
            inner.write_offset += buf_len;
            inner.prev_written_lsn = hi_lsn;
            // Can fail when replication finishes first.
            let _ = file_tx.send(true);
            {
                let mut ongoing = ongoing.lock().unwrap();
                *ongoing -= 1;
            }
            condvar.notify_one();
        });
        let client = self.client.clone();
        let replica_lock = self.replica_lock.clone();
        let ongoing = self.ongoing_lock.clone();
        let condvar = self.ongoing_condvar.clone();
        let incarnation = self.incarnation;
        std::thread::spawn(move || {
            let _replica_lock = replica_lock.lock().unwrap();
            let done = if !primary_url.is_empty() {
                // Send with very short timeout.
                let response = client
                    .post(&primary_url)
                    .set("obelisk-meta", "")
                    .set("Content-Type", "application/octet-stream")
                    .set("Content-Length", &payload.len().to_string())
                    .timeout(std::time::Duration::from_millis(10))
                    .send_bytes(&payload);
                if let Ok(resp) = response {
                    if resp.status() == 200 {
                        let mut body = vec![];
                        resp.into_reader().read_to_end(&mut body).unwrap();
                        let resp: replica::ReplicaResp = bincode::deserialize(&body).unwrap();
                        if resp.highest_seen_incarnation != incarnation {
                            eprintln!("Higher incarnation seen! Exiting.");
                            std::process::exit(1);
                        }
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            } else {
                // For testing. Just sleep for a millisecond.
                std::thread::sleep(std::time::Duration::from_millis(1));
                true
            };
            // Can fail when writing finishes first.
            let _ = done_tx.send(done);
            {
                let mut ongoing = ongoing.lock().unwrap();
                *ongoing -= 1;
            }
            condvar.notify_one();
        });

        // Done.
        Ok(())
    }

    /// Write buffer to current slab or allocate new slab if necessary.
    fn write_buffer(&self, buf: Vec<u8>, hi_lsn: i64) -> Result<(), LLError> {
        let buf_len = buf.len();
        let should_allocate_next = {
            let inner = self.inner.read().unwrap();
            inner.write_offset + buf_len > SLAB_SIZE
        };
        if should_allocate_next {
            self.allocate_next_slab()?;
            self.reset_file_seek()?;
        }
        // Write to file, assuming seek is correctly set beforehand.
        let mut file = self.file.write().unwrap();
        {
            let mut buf_writer = std::io::BufWriter::new(file.deref_mut());
            buf_writer.write_all(&buf).unwrap();
            buf_writer.flush().unwrap();
        }
        if USE_SYNC_ALL {
            file.sync_all().unwrap();
        } else {
            file.flush().unwrap();
        }
        // Update write offset and prev seen lsn.
        let mut inner = self.inner.write().unwrap();
        log::debug!(
            "[Partition::write_buffer {partition_idx}] Wrote buffer of len {buf_len} at offset {write_offset}.",
            partition_idx = self.partition_idx,
            buf_len = buf_len,
            write_offset = inner.write_offset
        );
        inner.write_offset += buf_len;
        inner.prev_written_lsn = hi_lsn;
        // Done.
        Ok(())
    }

    /// Allocate next slab.
    fn allocate_next_slab(&self) -> Result<(), LLError> {
        let mut conn = self.pool.get().unwrap();
        let txn = conn.transaction().unwrap();
        let (curr_slab, prev_written_lsn) = {
            let inner = self.inner.read().unwrap();
            (inner.curr_slab.clone(), inner.prev_written_lsn)
        };
        // First try free slabs.
        // Should try to find earlier slab, to make truncation more likely later on.
        let stmt = "SELECT slab_id, slab_offset FROM free_slabs ORDER BY slab_id ASC LIMIT 1";
        let result = txn.query_row(stmt, [], |r| {
            let slab_id: i64 = r.get(0).unwrap();
            let slab_offset: usize = r.get(1).unwrap();
            Ok((slab_id, slab_offset))
        });
        let (slab_id, slab_offset) = match result {
            Ok((slab_id, slab_offset)) => {
                // Remove from free_slabs.
                let stmt = "DELETE FROM free_slabs WHERE slab_id = ?";
                txn.execute(stmt, params![slab_id]).unwrap();
                (slab_id, slab_offset)
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                // Allocate new slab.
                let stmt = "SELECT file_size, next_slab_id FROM global_info WHERE unique_row=0";
                let (file_size, next_slab_id): (usize, i64) = txn
                    .query_row(stmt, [], |r| {
                        let file_size: usize = r.get(0).unwrap();
                        let next_slab_id: i64 = r.get(1).unwrap();
                        Ok((file_size, next_slab_id))
                    })
                    .unwrap();
                // Increment each.
                let stmt =
                    "UPDATE global_info SET file_size = ?, next_slab_id=?  WHERE unique_row=0";
                txn.execute(stmt, params![file_size + SLAB_SIZE, next_slab_id + 1])
                    .unwrap();
                (next_slab_id, file_size)
            }
            Err(e) => return Err(LLError::WALError(e.to_string())),
        };
        // Mark the new slab as allocated.
        let stmt = "INSERT INTO busy_slabs (slab_id, owner_partition_idx, slab_offset, highest_persist_lsn)\
        VALUES (?, ?, ?, ?)";
        txn.execute(
            stmt,
            params![slab_id, self.partition_idx, slab_offset, MAX_LSN,],
        )
        .unwrap();
        // Mark the current slab as being done with.
        let stmt = "UPDATE busy_slabs SET highest_persist_lsn=? WHERE slab_id = ?";
        txn.execute(stmt, params![prev_written_lsn, curr_slab.slab_id])
            .unwrap();
        // Extend file if necessary. Zero out otherwise.
        // NOTE: This relies on pool acting as a mutex.
        {
            let mut file = self.file.write().unwrap();
            let file_size = file.metadata().unwrap().len() as usize;
            if file_size < slab_offset + SLAB_SIZE {
                let desired_len = slab_offset + 10 * SLAB_SIZE;
                file.set_len(desired_len as u64).unwrap();
            } else {
                // Zero out the slab.
                file.seek(std::io::SeekFrom::Start(slab_offset as u64))
                    .unwrap();
                file.write_all(&vec![0; SLAB_SIZE]).unwrap();
                if USE_SYNC_ALL {
                    file.sync_all().unwrap();
                } else {
                    file.flush().unwrap();
                }
            }
        }
        // Commit changes.
        txn.commit().unwrap();
        // Update inner.
        let mut inner = self.inner.write().unwrap();
        inner.curr_slab = SlabInfo {
            slab_id,
            owner_partition_idx: self.partition_idx,
            slab_offset,
            highest_persist_lsn: -1,
        };
        inner.write_offset = 0;
        log::debug!(
            "[Partition::allocate_next_slab {partition_idx}] Allocated next slab {slab:?}",
            partition_idx = self.partition_idx,
            slab = inner.curr_slab
        );
        // Done.
        Ok(())
    }

    /// Reset file seek to correct index.
    fn reset_file_seek(&self) -> Result<(), LLError> {
        let inner = self.inner.read().unwrap();
        let mut file = self.file.write().unwrap();
        let seek_idx = inner.curr_slab.slab_offset + inner.write_offset;
        file.seek(std::io::SeekFrom::Start(seek_idx as u64))
            .unwrap();
        Ok(())
    }

    /// Remove superfluous persist.
    /// Needed when a parallel persist finishes before its logical predecessor.
    fn remove_extra_persist(&self, global_persist_lsn: i64) -> Result<(), LLError> {
        let (_, hi_lsn, prev_hi_lsn) = self.recovery_lsns;
        debug_assert!(prev_hi_lsn <= global_persist_lsn);
        if hi_lsn <= global_persist_lsn {
            log::info!(
                "[Partition::remove_extra_persist {partition_idx}] No extra persist to remove.",
                partition_idx = self.partition_idx
            );
            // Nothing to do.
            return Ok(());
        }
        // The last persist is superfluous.
        // NOTE: This relies on the fact a new slab is allocated only if persist cannot succeed on the current slab.
        // This means that any superfluous persist MUST be on the current slab.
        // So we cannot asyncronously change the current slab when it is full after a call to persist.
        let mut buf: Vec<u8> = vec![0; SLAB_SIZE];
        let mut inner = self.inner.write().unwrap();
        let mut file = self.file.write().unwrap();
        file.seek(std::io::SeekFrom::Start(inner.curr_slab.slab_offset as u64))
            .unwrap();
        file.read_exact(&mut buf).unwrap();
        // Read until second to last persist.
        let mut prev_offset: usize = 0;
        let last_offset = inner.write_offset;
        loop {
            let mut curr_offset = prev_offset;
            // Read length usize.
            let len_bytes = &buf[curr_offset..curr_offset + 8];
            let len = usize::from_be_bytes(len_bytes.try_into().unwrap());
            curr_offset += 8;
            // Skip batch.
            curr_offset += len;
            // Skip checksum.
            curr_offset += 4;
            // If we are at the last persist, break.
            if curr_offset == last_offset {
                break;
            }
            // Otherwise, update prev_offset.
            prev_offset = curr_offset;
        }
        inner.write_offset = prev_offset;
        inner.prev_written_lsn = prev_hi_lsn;
        log::info!(
            "[Partition::remove_extra_persist {partition_idx}] Removed extra persist {recovery_lsns:?}; set back to {prev_written_lsn}.",
            partition_idx = self.partition_idx,
            recovery_lsns = self.recovery_lsns,
            prev_written_lsn = inner.prev_written_lsn,
        );
        // IMPORTANT: Zero out the rest of the slab.
        // This is necessary because the last lsn is now invalid.
        // We may not know this if a crash occurs shortly after recovery.
        let zero_offset = inner.curr_slab.slab_offset + inner.write_offset;
        let zero_len = last_offset - prev_offset;
        file.seek(std::io::SeekFrom::Start(zero_offset as u64))
            .unwrap();
        file.write_all(&vec![0; zero_len]).unwrap();
        if USE_SYNC_ALL {
            file.sync_all().unwrap();
        } else {
            file.flush().unwrap();
        }
        Ok(())
    }

    /// Recover state of last persist in the slabs.
    /// Returns (lo_lsn, hi_lsn, curr_slab_offset).
    fn recover_partition_persist_state(
        pool: Pool<SqliteConnectionManager>,
        file: &mut fs::File,
        partition_idx: usize,
        slab_info: SlabInfo,
        replica_entries: Vec<(Vec<u8>, i64)>,
    ) -> Result<(i64, i64, i64, usize), LLError> {
        // Read slab from file.
        let mut buf: Vec<u8> = vec![0; SLAB_SIZE];
        // Seek.
        // NOTE: cannot possibly be EOF thanks of the `set_len`` logic in `recover_slab_or_allocate_first`
        file.seek(std::io::SeekFrom::Start(slab_info.slab_offset as u64))
            .unwrap();
        file.read_exact(&mut buf).unwrap();
        // Scan to find the last valid log entry.
        let mut curr_offset: usize = 0;
        let mut recovered = None;
        while curr_offset < SLAB_SIZE {
            // Read length usize.
            if curr_offset + 8 > SLAB_SIZE {
                break;
            }
            let len_bytes = &buf[curr_offset..curr_offset + 8];
            let len = usize::from_be_bytes(len_bytes.try_into().unwrap());
            if len == 0 || curr_offset + len > SLAB_SIZE {
                // Nothing written here. Or garbage.
                log::debug!(
                    "[Partition::recover_partition_persist_state {partition_idx}] Found content len = {len} at offset {curr_offset}.",
                    partition_idx = partition_idx,
                    len = len,
                    curr_offset = curr_offset
                );
                break;
            }
            curr_offset += 8;
            // Read batch.
            let batch_bytes = &buf[curr_offset..curr_offset + len];
            let computed_checksum = crc32fast::hash(batch_bytes);
            curr_offset += len;
            // Read u32 checksum.
            let found_checksum =
                u32::from_be_bytes(buf[curr_offset..curr_offset + 4].try_into().unwrap());
            curr_offset += 4;
            // Verify checksum.
            if found_checksum != computed_checksum {
                // Incomplete write.
                break;
            }
            let batch: Vec<(Vec<u8>, i64)> = bincode::deserialize(batch_bytes).unwrap();
            let lo_lsn = batch[0].1;
            let hi_lsn = batch[batch.len() - 1].1;
            recovered = match recovered {
                None => Some((lo_lsn, hi_lsn, None, curr_offset)),
                Some((_, prev_hi_lsn, _, _)) => {
                    Some((lo_lsn, hi_lsn, Some(prev_hi_lsn), curr_offset))
                }
            };
        }
        if let Some((lo_lsn, hi_lsn, _, curr_offset)) = recovered {
            log::info!(
                "[Partition::recover_partition_persist_state {partition_idx}] Recovered persist state ({lo_lsn}, {hi_lsn}), {curr_offset}.",
            );
        } else {
            log::info!(
                "[Partition::recover_partition_persist_state {partition_idx}] No persist state found.",
                partition_idx = partition_idx
            );
        }
        let last_valid_offset = recovered.map_or(0, |(_, _, _, curr_offset)| curr_offset);
        let (recovered, changed) =
            Self::reconcile_replica_entries(recovered, replica_entries, &mut buf);
        if changed {
            // Write to file.
            let start_offset = last_valid_offset;
            let end_offset = recovered.map_or(0, |(_, _, _, curr_offset)| curr_offset);
            log::info!(
                "[Partition::recover_partition_persist_state {partition_idx}] Writing replicated entries to file from {start_offset} to {end_offset}.",
                partition_idx = partition_idx,
                start_offset = start_offset,
                end_offset = end_offset
            );
            file.seek(std::io::SeekFrom::Start(
                (slab_info.slab_offset + start_offset) as u64,
            ))
            .unwrap();
            file.write_all(&buf[start_offset..end_offset]).unwrap();
            if USE_SYNC_ALL {
                file.sync_all().unwrap();
            } else {
                file.flush().unwrap();
            }
        }
        // Check if successfully recovered.
        if let Some((lo_lsn, hi_lsn, prev_hi_lsn, curr_offset)) = recovered {
            let prev_hi_lsn = if let Some(prev_hi_lsn) = prev_hi_lsn {
                prev_hi_lsn
            } else if let Some(prev_slab_info) =
                Self::get_prev_slab_info(pool.clone(), partition_idx, &slab_info)?
            {
                prev_slab_info.highest_persist_lsn
            } else {
                -1
            };
            return Ok((lo_lsn, hi_lsn, prev_hi_lsn, curr_offset));
        }
        // Must check previous slab if it exists.
        if let Some(prev_slab_info) =
            Self::get_prev_slab_info(pool.clone(), partition_idx, &slab_info)?
        {
            return Self::recover_partition_persist_state(
                pool,
                file,
                partition_idx,
                prev_slab_info,
                vec![],
            );
        }
        // Never persisted anything.
        Ok((-1, -1, -1, 0))
    }

    fn reconcile_replica_entries(
        recovered: Option<(i64, i64, Option<i64>, usize)>,
        replica_entries: Vec<(Vec<u8>, i64)>,
        buf: &mut [u8],
    ) -> (Option<(i64, i64, Option<i64>, usize)>, bool) {
        let (should_write, write_offset) = if replica_entries.is_empty() {
            (false, usize::MAX)
        } else if let Some((_, hi_lsn, _, last_valid_offset)) = recovered {
            let replica_lsn = replica_entries[0].1;
            if replica_lsn <= hi_lsn {
                // Already persisted.
                (false, usize::MAX)
            } else {
                // Write replica entries.
                (true, last_valid_offset)
            }
        } else {
            (true, 0)
        };
        if !should_write {
            return (recovered, false);
        }
        // Write replica entries.
        let lo_lsn = replica_entries[0].1;
        let hi_lsn = replica_entries[replica_entries.len() - 1].1;
        let prev_hi_lsn = if let Some((_, hi_lsn, _, _)) = recovered {
            Some(hi_lsn)
        } else {
            None
        };
        let entries = bincode::serialize(&replica_entries).unwrap();
        let len = entries.len();
        let computed_checksum = crc32fast::hash(&entries);
        let mut curr_offset = write_offset;
        debug_assert!(curr_offset + 8 + len + 4 <= SLAB_SIZE);
        // Write length usize.
        let len_bytes = len.to_be_bytes();
        buf[curr_offset..curr_offset + 8].copy_from_slice(&len_bytes);
        curr_offset += 8;
        // Write batch.
        buf[curr_offset..curr_offset + len].copy_from_slice(&entries);
        curr_offset += len;
        // Write checksum.
        let checksum_bytes = computed_checksum.to_be_bytes();
        buf[curr_offset..curr_offset + 4].copy_from_slice(&checksum_bytes);
        curr_offset += 4;
        (Some((lo_lsn, hi_lsn, prev_hi_lsn, curr_offset)), true)
    }

    /// Recover current slab and previous slab or allocate first slab.
    fn recover_slab_or_allocate_first(
        pool: Pool<SqliteConnectionManager>,
        file: &mut fs::File,
        partition_idx: usize,
    ) -> Result<SlabInfo, LLError> {
        // Get and increase file size from DB.
        let mut conn = pool.get().unwrap();
        let txn = conn.transaction().unwrap();
        // Select that slab belonging to this partition that has the highest persist lsn.
        let stmt = "SELECT slab_id, slab_offset, highest_persist_lsn FROM busy_slabs WHERE owner_partition_idx = ? ORDER BY highest_persist_lsn DESC LIMIT 1";
        let result = txn.query_row(stmt, [partition_idx], |r| {
            let slab_id: i64 = r.get(0).unwrap();
            let slab_offset: usize = r.get(1).unwrap();
            let highest_persist_lsn: i64 = r.get(2).unwrap();
            Ok((slab_id, slab_offset, highest_persist_lsn))
        });
        match result {
            Ok((slab_id, slab_offset, highest_persist_lsn)) => {
                txn.commit().unwrap();
                // Extend file by SLAB_SIZE if necessary.
                // NOTE: This relies on pool acting as a mutex.
                let file_size = file.metadata().unwrap().len() as usize;
                if file_size < slab_offset + SLAB_SIZE {
                    file.set_len((slab_offset + SLAB_SIZE) as u64).unwrap();
                }
                return Ok(SlabInfo {
                    slab_id,
                    owner_partition_idx: partition_idx,
                    slab_offset,
                    highest_persist_lsn,
                });
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => {}
            Err(e) => return Err(LLError::WALError(e.to_string())),
        };

        // Get current file size and slab id.
        let stmt = "SELECT file_size, next_slab_id FROM global_info WHERE unique_row=0";
        let (file_size, next_slab_id): (usize, i64) = txn
            .query_row(stmt, [], |r| {
                let file_size: usize = r.get(0).unwrap();
                let next_slab_id: i64 = r.get(1).unwrap();
                Ok((file_size, next_slab_id))
            })
            .unwrap();
        // Increment each.
        let stmt = "UPDATE global_info SET file_size=?, next_slab_id=? WHERE unique_row=0";
        txn.execute(stmt, params![file_size + SLAB_SIZE, next_slab_id + 1])
            .unwrap();
        // Mark the new slab as allocated.
        let stmt = "INSERT INTO busy_slabs (slab_id, owner_partition_idx, slab_offset, highest_persist_lsn)\
        VALUES (?, ?, ?, ?)";
        txn.execute(
            stmt,
            params![next_slab_id, partition_idx, file_size, MAX_LSN],
        )
        .unwrap();
        txn.commit().unwrap();
        // Extend the file by SLAB_SIZE.
        // NOTE: This relies on pool acting as a mutex.
        let actual_file_size = file.metadata().unwrap().len() as usize;
        if actual_file_size < file_size + SLAB_SIZE {
            file.set_len((file_size + SLAB_SIZE) as u64).unwrap();
        }
        // Return the new slab.
        return Ok(SlabInfo {
            slab_id: next_slab_id,
            owner_partition_idx: partition_idx,
            slab_offset: file_size,
            highest_persist_lsn: MAX_LSN,
        });
    }

    /// Return the slab preceeding this one.
    fn get_prev_slab_info(
        pool: Pool<SqliteConnectionManager>,
        partition_idx: usize,
        slab_info: &SlabInfo,
    ) -> Result<Option<SlabInfo>, LLError> {
        let conn = pool.get().unwrap();
        let stmt = "SELECT slab_id, slab_offset, highest_persist_lsn FROM busy_slabs WHERE owner_partition_idx = ? AND highest_persist_lsn < ? ORDER BY highest_persist_lsn DESC LIMIT 1";
        let result = conn.query_row(
            stmt,
            params![partition_idx, slab_info.highest_persist_lsn],
            |r| {
                let slab_id: i64 = r.get(0).unwrap();
                let slab_offset: usize = r.get(1).unwrap();
                let highest_persist_lsn: i64 = r.get(2).unwrap();
                Ok((slab_id, slab_offset, highest_persist_lsn))
            },
        );
        match result {
            Ok((slab_id, slab_offset, highest_persist_lsn)) => Ok(Some(SlabInfo {
                slab_id,
                owner_partition_idx: partition_idx,
                slab_offset,
                highest_persist_lsn,
            })),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => return Err(LLError::WALError(e.to_string())),
        }
    }

    /// Replay slab.
    fn replay_slab(
        handle: &mut PartitionReplayHandle,
        slab_offset: u64,
    ) -> Result<Vec<(Vec<u8>, i64)>, LLError> {
        handle
            .file
            .seek(std::io::SeekFrom::Start(slab_offset))
            .unwrap();
        let mut buf: Vec<u8> = vec![0; SLAB_SIZE];
        handle.file.read_exact(&mut buf).unwrap();
        let mut entries = Vec::new();
        let mut curr_offset: usize = 0;
        while curr_offset < SLAB_SIZE {
            if curr_offset + 8 > SLAB_SIZE {
                break;
            }
            // Read length usize.
            let len_bytes = &buf[curr_offset..curr_offset + 8];
            let len = usize::from_be_bytes(len_bytes.try_into().unwrap());
            if len == 0 || curr_offset + len > SLAB_SIZE {
                // Nothing written here. Or garbage.
                break;
            }
            curr_offset += 8;
            // Read batch.
            let batch_bytes = &buf[curr_offset..curr_offset + len];
            let computed_checksum = crc32fast::hash(batch_bytes);
            curr_offset += len;
            // Read u32 checksum.
            let found_checksum =
                u32::from_be_bytes(buf[curr_offset..curr_offset + 4].try_into().unwrap());
            curr_offset += 4;
            // Verify checksum.
            if found_checksum != computed_checksum {
                // Incomplete write.
                break;
            }
            let batch: Vec<(Vec<u8>, i64)> = bincode::deserialize(batch_bytes).unwrap();
            entries.extend(batch);
        }
        // Done.
        Ok(entries)
    }

    /// Get first replay step.
    fn first_replay_step(&self) -> Result<Option<PartitionReplayHandle>, LLError> {
        let slabs = self.find_next_slabs(REPLAY_SLAB_COUNT, 0)?;
        if slabs.is_empty() {
            return Ok(None);
        }
        let offsets = slabs.iter().map(|s| s.slab_offset as u64).collect();
        let next_from_lsn = slabs.last().unwrap().highest_persist_lsn + 1;
        let replay_file = fs::File::open(&self.log_filename).unwrap();
        let new_handle = PartitionReplayHandle {
            file: replay_file,
            cached_offsets: offsets,
            next_from_lsn,
        };
        Ok(Some(new_handle))
    }

    /// Cache next slabs to replay.
    fn cache_next_slabs_if_empty(
        &self,
        handle: PartitionReplayHandle,
    ) -> Result<Option<PartitionReplayHandle>, LLError> {
        if !handle.cached_offsets.is_empty() {
            return Ok(Some(handle));
        }
        let slabs = self.find_next_slabs(10, handle.next_from_lsn)?;
        if slabs.is_empty() {
            return Ok(None);
        }
        let offsets = slabs.iter().map(|s| s.slab_offset as u64).collect();
        let next_from_lsn = slabs.last().unwrap().highest_persist_lsn + 1;
        let new_handle = PartitionReplayHandle {
            file: handle.file,
            cached_offsets: offsets,
            next_from_lsn,
        };
        Ok(Some(new_handle))
    }

    /// Find next slabs containing lsns >= from_lsn.
    fn find_next_slabs(&self, slab_count: usize, from_lsn: i64) -> Result<Vec<SlabInfo>, LLError> {
        let conn = self.pool.get().unwrap();
        let stmt = "SELECT slab_id, owner_partition_idx, slab_offset, highest_persist_lsn FROM busy_slabs WHERE owner_partition_idx=? AND highest_persist_lsn >= ? ORDER BY highest_persist_lsn ASC NULLS LAST LIMIT ?";
        let mut stmt = conn.prepare(stmt).unwrap();
        let mut rows = stmt
            .query(params![self.partition_idx, from_lsn, slab_count])
            .unwrap();
        let mut slabs = Vec::new();
        while let Some(row) = rows.next().unwrap() {
            let slab_id: i64 = row.get(0).unwrap();
            let owner_partition_idx: usize = row.get(1).unwrap();
            let slab_offset: usize = row.get(2).unwrap();
            let highest_persist_lsn: i64 = row.get(3).unwrap();
            slabs.push(SlabInfo {
                slab_id,
                owner_partition_idx,
                slab_offset,
                highest_persist_lsn,
            });
        }
        log::debug!(
            "[Partition::find_next_slabs {partition_idx}] Found Next Slabs: {slabs:?}",
            partition_idx = self.partition_idx
        );

        Ok(slabs)
    }

    fn recover_from_replicas(
        client: &ureq::Agent,
        partition_idx: usize,
        replicas: Option<(String, String)>,
        incarnation: i64,
    ) -> Result<Vec<(Vec<u8>, i64)>, LLError> {
        println!("Starting replica recovery!");
        let (primary, secondary) = match replicas {
            Some(replicas) => replicas,
            None => return Ok(vec![]),
        };
        if primary.is_empty() && secondary.is_empty() {
            // Just for testing.
            return Ok(vec![]);
        }
        // First try to contact primary.
        println!("Making primary request!");
        let req = replica::ReplicaReq::RecoverPrimary {
            incarnation,
            partition_idx,
            secondary_url: secondary.clone(),
        };
        let req = bincode::serialize(&req).unwrap();
        let mut body = vec![];
        println!("Calling primary!");
        let resp = client
            .post(&primary)
            .set("obelisk-meta", "")
            .set("Content-Type", "application/octet-stream")
            .set("Content-Length", &req.len().to_string())
            .send_bytes(&req)
            .map_err(|e| LLError::WALError(e.to_string()));
        if let Ok(resp) = resp {
            if resp.status() == 200 {
                resp.into_reader().read_to_end(&mut body).unwrap();
            }
        }
        if body.is_empty() {
            // Try secondary.
            println!("Recovering from secondary!");
            let req = replica::ReplicaReq::RecoverSecondary {
                partition_idx,
                incarnation,
            };
            let req = bincode::serialize(&req).unwrap();
            let resp = client
                .post(&secondary)
                .timeout(std::time::Duration::from_secs(1))
                .send_bytes(&req)
                .map_err(|e| LLError::WALError(e.to_string()));
            if let Ok(resp) = resp {
                if resp.status() == 200 {
                    resp.into_reader().read_to_end(&mut body).unwrap();
                }
            }
        }
        if body.is_empty() {
            return Ok(vec![]);
        }
        let resp: replica::ReplicaResp = bincode::deserialize(&body).unwrap();
        if resp.highest_seen_incarnation > incarnation {
            eprintln!("Saw higher incarnation!");
            std::process::exit(1);
        }
        let recovered_entries = resp.recovered_entries;
        if recovered_entries.is_empty() {
            return Ok(vec![]);
        }
        Ok(recovered_entries)
    }
}

pub fn hello() {
    log::debug!("[wal::partition::hello] Hello, world!");
}
