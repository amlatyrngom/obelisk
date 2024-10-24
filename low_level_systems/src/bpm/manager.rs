use super::journal::Journal;
use super::page::Page;
use super::LLError;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::params;
use std::collections::HashMap;
use std::sync::{mpsc, Arc, RwLock};

/// A manager for the BPM.
pub struct Manager {
    page_size: usize,
    pub journal: Arc<Journal>,
    pool: Pool<SqliteConnectionManager>,
    inner: Arc<RwLock<Inner>>,
    db_coordination: Arc<RwLock<DbCoordination>>,
}

/// Modifyable state of the manager.
struct Inner {
    pin_counts: HashMap<i64, i64>,
    pinned: HashMap<i64, Arc<RwLock<Page>>>,
    enable_keep: bool,
    kept: lru::LruCache<i64, Arc<RwLock<Page>>>,
    fetching: HashMap<i64, Vec<mpsc::SyncSender<()>>>,
}

/// A database operation. Allows batching.
enum DbOp {
    Create(i64),
    Delete(i64),
}

/// Coordinates database operations. Allows batching.
struct DbCoordination {
    op_seq_num: i64,
    waiting_creates: HashMap<i64, i64>,
    waiting_deletes: HashMap<i64, i64>,
    finished_creates: HashMap<i64, i64>,
}

impl Manager {
    /// Create a new manager.
    pub fn new(
        parallelism: usize,
        page_size: usize,
        storage_dir: &str,
        name: &str,
    ) -> Result<Self, LLError> {
        let journal = Journal::new(2 * parallelism, page_size, storage_dir, name)?;
        let journal = Arc::new(journal);
        let mem_mb: usize = if let Ok(mem) = std::env::var("OBK_MEMORY") {
            mem.parse().unwrap()
        } else {
            1024
        };
        let num_pages = (mem_mb * 1024 * 1024 / 2) / page_size;
        println!("Making LRU with {num_pages} page cap.");
        let kept = lru::LruCache::new(std::num::NonZeroUsize::new(num_pages as usize).unwrap());
        println!("Made LRU with {num_pages} page cap.");
        let inner = Arc::new(RwLock::new(Inner {
            pin_counts: HashMap::new(),
            pinned: HashMap::new(),
            fetching: HashMap::new(),
            kept,
            enable_keep: true,
        }));
        let db_coordination = Arc::new(RwLock::new(DbCoordination {
            op_seq_num: 0,
            waiting_creates: HashMap::new(),
            waiting_deletes: HashMap::new(),
            finished_creates: HashMap::new(),
        }));
        let manager = Self {
            journal,
            page_size,
            inner,
            db_coordination,
            pool: Self::make_metadata_pool(storage_dir, name)?,
        };
        Ok(manager)
    }

    pub fn set_keep(&self, enable_keep: bool) {
        let mut inner = self.inner.write().unwrap();
        inner.enable_keep = enable_keep;
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
            .map_err(|e| LLError::BPMError(format!("Failed to create pool: {}", e)))?;
        let conn = pool.get().unwrap();
        // Set timeout.
        conn.busy_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        loop {
            match conn.execute_batch(include_str!("schema.sql")) {
                Ok(_) => break,
                Err(e) => {
                    eprintln!("[bpm::Manager::make_metadata_pool] Error creating schema: {e:?}. Retrying!");
                    std::thread::sleep(std::time::Duration::from_secs(1));
                }
            }
        }
        // Done.
        Ok(pool)
    }

    /// Pin a page to memory.
    pub fn pin(&self, block_id: i64) -> Result<Arc<RwLock<Page>>, LLError> {
        let mut inner = self.inner.write().unwrap();
        if inner.pin_counts.contains_key(&block_id) {
            // Increase pincount.
            let pincount = inner.pin_counts.get_mut(&block_id).unwrap();
            *pincount += 1;
            let page = inner.pinned.get(&block_id).unwrap();
            Ok(page.clone())
        } else if inner.kept.contains(&block_id) {
            // Move from kept to pinned.
            let page = inner.kept.pop(&block_id).unwrap();
            inner.pinned.insert(block_id, page.clone());
            inner.pin_counts.insert(block_id, 1);
            Ok(page)
        } else if inner.fetching.contains_key(&block_id) {
            let (tx, rx) = mpsc::sync_channel(1);
            inner.fetching.get_mut(&block_id).unwrap().push(tx);
            drop(inner);
            rx.recv().unwrap();
            self.pin(block_id)
        } else {
            inner.fetching.insert(block_id, Vec::new());
            drop(inner);
            let page = self.journal.read_page(block_id)?;
            let mut inner = self.inner.write().unwrap();
            let waiting = inner.fetching.remove(&block_id).unwrap();
            for tx in waiting {
                tx.send(()).unwrap();
            }
            let page = Arc::new(RwLock::new(page));
            inner.pinned.insert(block_id, page.clone());
            inner.pin_counts.insert(block_id, 1);
            inner.kept.pop(&block_id);
            Ok(page)
        }
    }

    /// Write back a page.
    pub fn write_back(&self, page: &mut Page) -> Result<(), LLError> {
        page.rechecksum();
        self.journal.write(page)?;
        Ok(())
    }

    /// Controlled write back for testing.
    pub fn controlled_write_back(
        &self,
        page: &mut Page,
        partial_stage: bool,
        partial_write: bool,
        fail_after: Option<f64>,
    ) -> Result<(), LLError> {
        self.journal
            .controlled_write(page, partial_stage, partial_write, fail_after)?;
        Ok(())
    }

    /// Unpin a page.
    pub fn unpin(&self, page_id: i64) -> Result<(), LLError> {
        let mut inner = self.inner.write().unwrap();
        let pincount = inner
            .pin_counts
            .get_mut(&page_id)
            .ok_or(LLError::BPMError(format!("Page {} is not pinned", page_id)))?;
        *pincount -= 1;
        if *pincount == 0 {
            let page = inner.pinned.remove(&page_id).unwrap();
            inner.pin_counts.remove(&page_id);
            if inner.enable_keep {
                inner.kept.push(page_id, page);
            }
        }
        Ok(())
    }

    pub fn free_kept(&self) {
        let mut inner = self.inner.write().unwrap();
        inner.kept.clear();
    }

    /// Check that all pages are unpinned. For testing.
    pub fn check_all_unpinned(&self) -> Result<(), LLError> {
        let inner = self.inner.read().unwrap();
        if !inner.pinned.is_empty() {
            println!("Pinned: {:?}", inner.pinned.keys());
            return Err(LLError::BPMError(format!(
                "{} pages are pinned",
                inner.pinned.len()
            )));
        }
        if !inner.pin_counts.is_empty() {
            return Err(LLError::BPMError(format!(
                "{} pages have pincounts",
                inner.pin_counts.len()
            )));
        }
        Ok(())
    }

    /// Count the number of active and free pages.
    pub fn page_statistics(&self) -> Result<(usize, usize), LLError> {
        let conn = self.pool.get().unwrap();
        let active: i64 = conn
            .query_row("SELECT COUNT(*) FROM busy_pages", params![], |row| {
                row.get(0)
            })
            .unwrap();
        let free: i64 = conn
            .query_row("SELECT COUNT(*) FROM free_pages", params![], |row| {
                row.get(0)
            })
            .unwrap();
        Ok((active as usize, free as usize))
    }

    /// Perform a db operation in batches.
    fn perform_db_op(&self, op: DbOp) -> Result<Option<i64>, LLError> {
        // Add self to queue first.
        // Allows queuing before connection lock is taken, which might take a while.
        let (is_create, my_op_seq_num) = {
            let mut db_coord = self.db_coordination.write().unwrap();
            let op_seq_num = db_coord.op_seq_num;
            db_coord.op_seq_num += 1;
            let is_create = match op {
                DbOp::Create(logical_id) => {
                    db_coord.waiting_creates.insert(op_seq_num, logical_id);
                    true
                }
                DbOp::Delete(page_id) => {
                    db_coord.waiting_deletes.insert(op_seq_num, page_id);
                    false
                }
            };
            (is_create, op_seq_num)
        };
        // Take connection lock.
        let mut conn = self.pool.get().unwrap();
        let (waiting_creates, waiting_deletes) = {
            // After connection lock is taken, check if queue was handled by another thread.
            // If so, return.
            let mut db_coord = self.db_coordination.write().unwrap();
            if is_create && !db_coord.waiting_creates.contains_key(&my_op_seq_num) {
                drop(conn);
                let page_id = db_coord.finished_creates.remove(&my_op_seq_num).unwrap();
                return Ok(Some(page_id));
            } else if !is_create && !db_coord.waiting_deletes.contains_key(&my_op_seq_num) {
                drop(conn);
                return Ok(None);
            }
            // Otherwise, take the queues.
            let waiting_creates = db_coord.waiting_creates.drain().collect::<Vec<_>>();
            let waiting_deletes = db_coord.waiting_deletes.drain().collect::<Vec<_>>();
            (waiting_creates, waiting_deletes)
        };
        // Perform the operations.
        let txn = conn.transaction().unwrap();
        let mut finished_creates = HashMap::new();
        // Creates.
        for (op_seq_num, logical_id) in waiting_creates {
            // Check free pages.
            let stmt = "SELECT MIN(id) FROM free_pages";
            let page_id: Result<Option<i64>, _> = txn.query_row(stmt, params![], |row| row.get(0));
            let page_id = match page_id {
                Ok(Some(page_id)) => {
                    // Remove from free pages.
                    let stmt = "DELETE FROM free_pages WHERE id = ?";
                    txn.execute(stmt, params![page_id]).unwrap();
                    // Insert into busy pages.
                    let stmt = "INSERT INTO busy_pages (id) VALUES (?)";
                    txn.execute(stmt, params![page_id]).unwrap();
                    // Add to finished creates.
                    finished_creates.insert(op_seq_num, page_id);
                    page_id
                }
                Err(rusqlite::Error::QueryReturnedNoRows) | Ok(None) => {
                    // Get next page id
                    let stmt = "SELECT next_page_id FROM global_info";
                    let page_id: i64 = txn.query_row(stmt, params![], |row| row.get(0)).unwrap();
                    // Update next page id
                    let stmt = "UPDATE global_info SET next_page_id = ?";
                    txn.execute(stmt, params![page_id + 1]).unwrap();
                    // Insert into busy pages.
                    let stmt = "INSERT INTO busy_pages (id) VALUES (?)";
                    txn.execute(stmt, params![page_id]).unwrap();
                    // Add to finished creates.
                    finished_creates.insert(op_seq_num, page_id);
                    page_id
                }
                Err(e) => {
                    return Err(LLError::BPMError(format!("Failed to get free page: {}", e)));
                }
            };
            // Mark logical id.
            if logical_id >= 0 {
                let stmt = "REPLACE INTO logical_ids (logical_id, page_id) VALUES (?, ?)";
                txn.execute(stmt, rusqlite::params![logical_id, page_id])
                    .unwrap();
            }
        }
        // Deletes
        for (_, page_id) in waiting_deletes {
            let stmt = "DELETE FROM busy_pages WHERE id = ?";
            let deleted = txn.execute(stmt, params![page_id]).unwrap();
            // Prevent repeated delete during recovery.
            if deleted > 0 {
                let stmt = "INSERT INTO free_pages (id) VALUES (?)";
                txn.execute(stmt, params![page_id]).unwrap();
                let stmt = "DELETE FROM logical_ids WHERE page_id = ?";
                txn.execute(stmt, params![page_id]).unwrap();
            }
        }
        // Commit.
        txn.commit().unwrap();
        // Update finished creates.
        let mut db_coord = self.db_coordination.write().unwrap();
        let mut my_page_id = None;
        for (op_seq_num, page_id) in finished_creates {
            if op_seq_num != my_op_seq_num {
                db_coord.finished_creates.insert(op_seq_num, page_id as i64);
            } else {
                my_page_id = Some(page_id as i64);
            }
        }
        // Done.
        Ok(my_page_id)
    }

    /// Get the physical id for a logical id.
    /// Should only be called during recovery.
    pub fn recover_physical_id(&self, logical_id: i64) -> Result<Option<i64>, LLError> {
        let conn = self.pool.get().unwrap();
        let stmt = "SELECT page_id FROM logical_ids WHERE logical_id = ?";
        let page_id: Result<i64, _> = conn.query_row(stmt, params![logical_id], |row| row.get(0));
        match page_id {
            Ok(page_id) => Ok(Some(page_id)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(LLError::BPMError(format!(
                "Failed to get physical id: {}",
                e
            ))),
        }
    }

    /// Create a new page.
    /// Pins it to memory. Must call write_back() to write it.
    pub fn create_page(&self) -> Result<Arc<RwLock<Page>>, LLError> {
        self.create_page_with_logical_id(-1)
    }

    /// Like create page, but with a logical id.
    pub fn create_page_with_logical_id(
        &self,
        logical_id: i64,
    ) -> Result<Arc<RwLock<Page>>, LLError> {
        let db_op = DbOp::Create(logical_id);
        let page_id = self.perform_db_op(db_op)?.unwrap();
        let page = Page::create(self.page_size, page_id);
        let page = Arc::new(RwLock::new(page));
        let mut inner = self.inner.write().unwrap();
        inner.pinned.insert(page_id, page.clone());
        inner.pin_counts.insert(page_id, 1);
        Ok(page)
    }

    /// Delete a page.
    pub fn delete_page(&self, page_id: i64, recovering: bool) -> Result<(), LLError> {
        let res = self.unpin(page_id);
        if !recovering {
            res?;
        }
        let db_op = DbOp::Delete(page_id);
        self.perform_db_op(db_op)?;
        Ok(())
    }
}
