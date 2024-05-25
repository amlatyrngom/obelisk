use std::collections::HashMap;
use std::fs;
use std::io::{Read, Seek, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::sync::{mpsc, Arc, Mutex, RwLock};

use super::page::Page;
use super::LLError;
use threadpool::ThreadPool;

/// State of the journal writers.
#[derive(Clone)]
enum WritersState {
    Free(Vec<Arc<RwLock<JournalWriter>>>),
    Waiting(Vec<mpsc::SyncSender<()>>),
}

/// Ongoing writes.
type OngoingWrites = HashMap<i64, Vec<mpsc::SyncSender<()>>>;

/// Journal. Implement staged writing.
#[derive(Clone)]
pub struct Journal {
    parallelism: usize,
    page_size: usize,
    writers_state: Arc<RwLock<WritersState>>,
    ongoing_writes: Arc<RwLock<OngoingWrites>>,
    extension_lock: Arc<RwLock<()>>,
    thread_pool: Arc<Mutex<ThreadPool>>,
}

/// Single instance of a journal writer.
struct JournalWriter {
    write_idx: usize,
    journal_file: fs::File,
    data_file: fs::File,
}

impl Journal {
    /// Create a new journal.
    pub fn new(
        parallelism: usize,
        page_size: usize,
        storage_dir: &str,
        name: &str,
    ) -> Result<Self, LLError> {
        let writers = Self::make_journal_writers(parallelism, page_size, storage_dir, name)?;
        let writers_state = WritersState::Free(writers);
        let writers_state = Arc::new(RwLock::new(writers_state));
        let extension_lock = Arc::new(RwLock::new(()));
        let ongoing_writes = Arc::new(RwLock::new(HashMap::new()));
        let thread_pool = Arc::new(Mutex::new(ThreadPool::new(parallelism)));
        let journal = Self {
            parallelism,
            page_size,
            writers_state,
            ongoing_writes,
            extension_lock,
            thread_pool,
        };
        journal.recover_all()?;
        Ok(journal)
    }

    pub fn num_free_writers(&self) -> usize {
        let writers_state = self.writers_state.read().unwrap();
        match writers_state.clone() {
            WritersState::Free(writers) => writers.len(),
            WritersState::Waiting(_) => 0,
        }
    }

    /// Prevent concurrent writes to the same page.
    /// Without this, after staging, writes to the same page can be interleaved.
    fn lock_page_write(&self, page_id: i64) {
        let mut ongoing_writes = self.ongoing_writes.write().unwrap();
        if !ongoing_writes.contains_key(&page_id) {
            ongoing_writes.insert(page_id, Vec::new());
            return;
        }
        let waiting = ongoing_writes.get_mut(&page_id).unwrap();
        let (tx, rx) = mpsc::sync_channel(1);
        waiting.push(tx);
        drop(ongoing_writes);
        // Wait for the previous write to finish.
        rx.recv().unwrap();
        // Try again.
        self.lock_page_write(page_id);
    }

    /// Reallow writes to the page.
    fn unlock_page_write(&self, page_id: i64) {
        let mut ongoing_writes = self.ongoing_writes.write().unwrap();
        let waiting = ongoing_writes.remove(&page_id).unwrap();
        for waiter in waiting {
            waiter.send(()).unwrap();
        }
    }

    /// Get a free writer. If none is available, wait for one to be available.
    fn get_writer(&self) -> Result<Arc<RwLock<JournalWriter>>, LLError> {
        let mut writer_states = self.writers_state.write().unwrap();
        match writer_states.clone() {
            WritersState::Free(mut writers) => {
                let writer = writers.pop().unwrap();
                *writer_states = if writers.is_empty() {
                    WritersState::Waiting(Vec::new())
                } else {
                    WritersState::Free(writers)
                };
                Ok(writer)
            }
            WritersState::Waiting(mut waiters) => {
                let (tx, rx) = mpsc::sync_channel(1);
                waiters.push(tx);
                *writer_states = WritersState::Waiting(waiters);
                // Release the lock before blocking.
                drop(writer_states);
                // Wait for a writer to be available.
                rx.recv().unwrap();
                // Try again.
                self.get_writer()
            }
        }
    }

    /// Release a writer.
    fn release_writer(&self, writer: Arc<RwLock<JournalWriter>>) -> Result<(), LLError> {
        // Reset seek first.
        {
            let mut writer = writer.write().unwrap();
            let write_idx = writer.write_idx;
            writer
                .journal_file
                .seek(std::io::SeekFrom::Start(write_idx as u64))
                .map_err(|e| LLError::BPMError(e.to_string()))?;
        }
        let mut writer_states = self.writers_state.write().unwrap();
        match writer_states.clone() {
            WritersState::Free(mut writers) => {
                writers.push(writer);
                *writer_states = WritersState::Free(writers);
            }
            WritersState::Waiting(waiters) => {
                // Notify all waiters.
                for waiter in waiters {
                    waiter.send(()).unwrap();
                }
                *writer_states = WritersState::Free(vec![writer]);
            }
        }
        Ok(())
    }

    /// Stage data to the journal.
    fn stage_data(
        &self,
        writer: &RwLock<JournalWriter>,
        data: &[u8],
        partial_stage: bool,
    ) -> Result<(), LLError> {
        let mut writer = writer.write().unwrap();
        {
            let mut buf_writer = std::io::BufWriter::new(&mut writer.journal_file);
            if partial_stage {
                let partial_len = data.len() / 2;
                buf_writer.write_all(&data[..partial_len]).unwrap();
                buf_writer.flush().unwrap();
            } else {
                buf_writer.write_all(data).unwrap();
                buf_writer.flush().unwrap();
            }
        }
        writer.journal_file.flush().unwrap();
        Ok(())
    }

    /// Write data to the data file.
    fn write_page(
        &self,
        writer: &RwLock<JournalWriter>,
        page_data: &[u8],
        page_id: i64,
        _page_version: i64,
        partial_write: bool,
    ) -> Result<(), LLError> {
        let mut writer = writer.write().unwrap();
        let page_offset = page_id as usize * self.page_size;
        {
            let _extension_lock = self.extension_lock.write().unwrap();
            if page_offset + self.page_size > writer.data_file.metadata().unwrap().len() as usize {
                let desired_len = page_offset + self.parallelism * self.page_size;
                if desired_len > super::MAX_SIZE {
                    panic!("Cannot extend data file beyond {}GB.", super::MAX_SIZE_GB)
                }
                writer
                    .data_file
                    .set_len(desired_len as u64)
                    .map_err(|e| LLError::BPMError(e.to_string()))?;
            }
        }
        writer
            .data_file
            .seek(std::io::SeekFrom::Start(page_offset as u64))
            .map_err(|e| LLError::BPMError(e.to_string()))?;
        if partial_write {
            let partial_len = page_data.len() / 2;
            writer
                .data_file
                .write_all(&page_data[..partial_len])
                .map_err(|e| LLError::BPMError(e.to_string()))?;
        } else {
            writer
                .data_file
                .write_all(page_data)
                .map_err(|e| LLError::BPMError(e.to_string()))?;
        }
        writer
            .data_file
            .flush()
            .map_err(|e| LLError::BPMError(e.to_string()))?;
        // println!("Finished Write PageId={page_id}. Version={_page_version} at Offset {page_offset}.");
        Ok(())
    }

    /// Write data in the journal.
    pub fn write(&self, page: &Page) -> Result<(), LLError> {
        self.controlled_write(page, false, false, None)
    }

    /// Controlled journal write.
    /// This is used for testing.
    pub fn controlled_write(
        &self,
        page: &Page,
        partial_stage: bool,
        partial_write: bool,
        fail_after: Option<f64>,
    ) -> Result<(), LLError> {
        let page_data = page.data.clone();
        let page_id = page.page_id();
        let page_version: i64 = page.page_version();
        self.lock_page_write(page_id);
        let writer = self.get_writer()?;
        self.stage_data(&writer, &page_data, partial_stage).unwrap();
        if partial_stage {
            let page_metadata = page.read_metadata();
            log::info!("[Journal::controlled_write] Done partial stage: {page_metadata:?}.");
            std::thread::sleep(std::time::Duration::from_secs_f64(
                fail_after.unwrap_or(0.25),
            ));
            self.unlock_page_write(page_id);
            self.release_writer(writer).unwrap();
            return Err(LLError::BPMError("Partial stage.".to_string()));
        }
        let this = self.clone();
        // ThreadPool::execute does not block, so this is fine.
        let thread_pool = self.thread_pool.lock().unwrap();
        thread_pool.execute(move || {
            // Fail after partially writing.
            if partial_write {
                log::info!("[Journal::controlled_write] Doing partial write.");
                this.write_page(&writer, &page_data, page_id, page_version, true)
                    .unwrap();
                std::thread::sleep(std::time::Duration::from_secs_f64(
                    fail_after.unwrap_or(1.0),
                ));
                this.unlock_page_write(page_id);
                this.release_writer(writer).unwrap();
                return;
            }
            // Fail without writing.
            if let Some(fail_after) = fail_after {
                log::info!("[Journal::controlled_write] Doing failure after stage.");
                std::thread::sleep(std::time::Duration::from_secs_f64(fail_after));
                this.unlock_page_write(page_id);
                this.release_writer(writer).unwrap();
                return;
            }
            // Normal case.
            this.write_page(&writer, &page_data, page_id, page_version, false)
                .unwrap();
            this.unlock_page_write(page_id);
            this.release_writer(writer).unwrap();
        });
        Ok(())
    }

    /// Read a page from the data file.
    pub fn read_page(&self, page_id: i64) -> Result<Page, LLError> {
        let read_offset = page_id as usize * self.page_size;
        let mut buf = vec![0; self.page_size];
        self.lock_page_write(page_id);
        let writer = self.get_writer()?;
        {
            let mut writer = writer.write().unwrap();
            {
                let _extension_lock = self.extension_lock.write().unwrap();
                if read_offset + self.page_size
                    > writer.data_file.metadata().unwrap().len() as usize
                {
                    let desired_len = read_offset + self.parallelism * self.page_size;
                    if desired_len > super::MAX_SIZE {
                        panic!("Cannot extend data file beyond {}GB.", super::MAX_SIZE_GB)
                    }
                    writer
                        .data_file
                        .set_len(desired_len as u64)
                        .map_err(|e| LLError::BPMError(e.to_string()))
                        .unwrap();
                }
            }
            writer
                .data_file
                .seek(std::io::SeekFrom::Start(read_offset as u64))
                .map_err(|e| LLError::BPMError(e.to_string()))
                .unwrap();
            writer.data_file.read_exact(&mut buf).unwrap();
        }
        self.release_writer(writer).unwrap();
        self.unlock_page_write(page_id);
        let page = Page::new(buf);
        Ok(page)
    }

    /// Make journal writers.
    fn make_journal_writers(
        parallelism: usize,
        page_size: usize,
        storage_dir: &str,
        name: &str,
    ) -> Result<Vec<Arc<RwLock<JournalWriter>>>, LLError> {
        let _ = fs::create_dir_all(storage_dir);
        let mut writers = Vec::new();
        let journal_filename = super::journal_filename(storage_dir, name);
        let data_filename = super::data_filename(storage_dir, name);
        let file_length = parallelism * page_size;
        let mut ts = Vec::new();
        for i in 0..parallelism {
            let journal_filename = journal_filename.clone();
            let data_filename = data_filename.clone();
            ts.push(std::thread::spawn(move || {
                let mut journal_file = fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .custom_flags(libc::O_SYNC)
                    .open(&journal_filename)
                    .map_err(|e| LLError::BPMError(e.to_string()))?;
                journal_file
                    .set_len(file_length as u64)
                    .map_err(|e| LLError::BPMError(e.to_string()))?;
                let write_idx = i * page_size;
                journal_file
                    .seek(std::io::SeekFrom::Start(write_idx as u64))
                    .map_err(|e| LLError::BPMError(e.to_string()))?;
                let data_file = fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .custom_flags(libc::O_SYNC)
                    .open(&data_filename)
                    .map_err(|e| LLError::BPMError(e.to_string()))?;
                let writer = Arc::new(RwLock::new(JournalWriter {
                    journal_file,
                    data_file,
                    write_idx,
                }));
                Ok(writer)
            }));
        }
        for t in ts {
            writers.push(t.join().unwrap()?);
        }
        Ok(writers)
    }

    /// Recover everything the journal.
    pub fn recover_all(&self) -> Result<(), LLError> {
        log::info!("[Journal::recover_all] Recovering journal.");
        let writers_state = self.writers_state.read().unwrap();
        let writers = match writers_state.clone() {
            WritersState::Free(writers) => {
                assert!(writers.len() == self.parallelism);
                writers
            }
            WritersState::Waiting(_) => panic!("Cannot recover while writers are waiting."),
        };
        let mut ts = Vec::new();
        for writer in writers {
            let page_size = self.page_size;
            let extension_lock = self.extension_lock.clone();
            let parallelism = self.parallelism;
            ts.push(std::thread::spawn(move || {
                let mut writer = writer.write().unwrap();
                let mut buf = vec![0; page_size];
                let write_idx = writer.write_idx as u64;
                writer
                    .journal_file
                    .read_exact(&mut buf)
                    .map_err(|e| LLError::BPMError(e.to_string()))?;
                writer
                    .journal_file
                    .seek(std::io::SeekFrom::Start(write_idx))
                    .map_err(|e| LLError::BPMError(e.to_string()))?;
                let journaled_page = Page::new(buf);
                if !journaled_page.is_complete() {
                    log::info!(
                        "[Journal::recover_all {}] Journal page not complete. Id={}",
                        writer.write_idx,
                        journaled_page.page_id()
                    );
                    return Ok(());
                }
                let journaled_page_metadata = journaled_page.read_metadata();
                let page_offset = journaled_page_metadata.id as usize * page_size;
                // Extend first if needed.
                {
                    let _l = extension_lock.write().unwrap();
                    if page_offset + page_size > writer.data_file.metadata().unwrap().len() as usize
                    {
                        let desired_len = page_offset + parallelism * page_size;
                        if desired_len > super::MAX_SIZE {
                            panic!("Cannot extend data file beyond {}GB.", super::MAX_SIZE_GB)
                        }
                        writer
                            .data_file
                            .set_len(desired_len as u64)
                            .map_err(|e| LLError::BPMError(e.to_string()))?;
                    }
                }
                writer
                    .data_file
                    .seek(std::io::SeekFrom::Start(page_offset as u64))
                    .map_err(|e| LLError::BPMError(e.to_string()))?;
                let mut buf = vec![0; page_size];
                writer
                    .data_file
                    .read_exact(&mut buf)
                    .map_err(|e| LLError::BPMError(e.to_string()))?;
                let stored_page = Page::new(buf);
                let overwrite = if !stored_page.is_complete() {
                    log::info!(
                        "[Journal::recover_all] Page {} is not complete.",
                        journaled_page_metadata.id
                    );
                    true
                } else {
                    let stored_page_metadata = stored_page.read_metadata();
                    stored_page_metadata.version < journaled_page_metadata.version
                };
                if overwrite {
                    writer
                        .data_file
                        .seek(std::io::SeekFrom::Start(page_offset as u64))
                        .map_err(|e| LLError::BPMError(e.to_string()))?;
                    writer
                        .data_file
                        .write_all(&journaled_page.data)
                        .map_err(|e| LLError::BPMError(e.to_string()))?;
                    writer
                        .data_file
                        .flush()
                        .map_err(|e| LLError::BPMError(e.to_string()))?;
                }

                Ok(())
            }));
        }
        for t in ts {
            let _ = t.join().unwrap()?;
        }
        Ok(())
    }
}
