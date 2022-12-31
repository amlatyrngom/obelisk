// use dashmap::mapref::entry::Entry;
// use dashmap::DashMap;
// use moka::sync::Cache;
// use std::collections::{HashMap, HashSet};
// use std::sync::{mpsc, Arc, RwLock};
// use crate::database::Database;
// use crate::persistent_log::PersistentLog;
// use crate::{SUBSYSTEM_NAME, ACTOR_ECS_MODE};
// use concurrent_queue::ConcurrentQueue;


// /// Create a new lock manager.
// pub struct LockManager {
//     locks: DashMap<String, ()>,
// }

// /// Create a new in-memory cache.
// pub struct CacheManager {
//     cache: Cache<String, (String, usize)>,
// }

// /// Anything in here has been enqueued, but not yet written.
// pub struct MemState {
//     cache_manager: CacheManager,
//     lock_manager: LockManager,
//     putting: DashMap<String, (String, usize)>,
//     deleting: DashMap<String, usize>,
//     inner: RwLock<MemStateInner>,
// }

// struct MemStateInner {
//     write_queue: ConcurrentQueue<(DBOp, usize)>,
//     flush_lsn: usize,
//     seq_num: usize,
// }


// impl LockManager {
//     /// Create a new lock manager.
//     pub fn new() -> Self {
//         LockManager {
//             locks: DashMap::new(),
//         }
//     }
    
//     /// Lock a key
//     pub fn lock(&self, key: &str) -> bool {
//         let entry = self.locks.entry(key.into());
//         let locked = match &entry {
//             Entry::Vacant(_) => true,
//             _ => false,
//         };
//         entry.or_insert(());
//         locked
//     }

//     /// Unlock a key
//     pub fn unlock(&self, key: &str) {
//         self.locks.remove(key).expect(&format!("Unlocking unowned key: {key}"));
//     }
// }

// impl CacheManager {
//     /// Create new cache.
//     pub fn new() -> Self {
//         CacheManager {
//             cache: Cache::<String, (String, usize)>::builder()
//                 .weigher(|k, (v, _)| (k.len() + v.len()) as u32)
//                 .max_capacity(8 << 30)
//                 .build(),
//         }
//     }

//     /// Store a new key-value if lsn is highest.
//     pub fn store(&self, key: &str, value: &str, lsn: usize) {
//         self.cache.get_with_if(
//             key.into(),
//             || (value.into(), lsn),
//             |(_, curr_lsn)| lsn >= *curr_lsn
//         );
//     }

//     /// Invalidate a key.
//     pub fn invalidate(&self, key: &str, lsn: usize) {
//         self.cache.invalidate(key);
//     }

//     /// Read a key-value pair from cache.
//     pub fn read(&self, key: &str) -> Option<(String, usize)> {
//         self.cache.get(key)
//     }
// }

// impl MemState {
//     /// Get a value.
//     pub fn get(&self, key: &str, consistent: bool) -> Option<(String, usize)> {
//         // Read based on consistency level.
//         let val = {
//             if consistent {
//                 // Try recent writes if consistent read.
//                 if let Some(val) = self.putting.get(key) {
//                     Some((val.0, val.1))
//                 } else if let Some(val) = self.deleting.get(key) {
//                     Some((String::new(), 0))
//                 } else if let Some((val, lsn)) = self.cache_manager.read(key) {
//                     Some((val, lsn))
//                 } else {
//                     None
//                 }
//             } else if let Some((val, lsn)) = self.cache_manager.read(key) {
//                 Some((val, lsn))
//             } else {
//                 None
//             }
//         };
//         // Wait for read entry to make it to log.
//         if let Some((_, lsn)) = &val {
//             self.wait_for_flush(*lsn, 0);
//         }
//         val   
//     }


//     /// Lock a key.
//     /// Must be called before enqueuing in the log.
//     pub fn lock(&self, key: &str) -> bool {
//         let locked = self.lock_manager.lock(key);
//         locked
//     }

//     /// Unlock a key.
//     pub fn unlock(&self, key: &str) {
//         self.lock_manager.unlock(key);
//     }

//     /// Wait for an lsn to be flushed.
//     pub fn wait_for_flush(&self, at_lsn: usize, seq_num: usize) {
//         // TODO: Make less hacky. Should use condition variables. Probably works just fine though.
//         let sleep_duration = std::time::Duration::from_micros(500);
//         loop {
//             let inner = self.inner.read().unwrap();
//             if inner.flush_lsn >= at_lsn && inner.seq_num > seq_num {
//                 return;
//             }
//             std::thread::sleep(sleep_duration);
//         }
//     }

//     /// Progress state after a flush.
//     pub fn handle_flush(&self, flush_lsn: usize) -> Vec<(DBOp, usize)> {
//         let mut inner = self.inner.write().unwrap();
//         inner.seq_num += 1;
//         if inner.flush_lsn < flush_lsn {
//             inner.flush_lsn = flush_lsn;
//         }
//         let mut writes: Vec<(DBOp, usize)> = Vec::new();
//         while inner.write_queue.is_empty() {
//             writes.push(inner.write_queue.pop().unwrap());
//         }
//         writes
//     }

//     /// Call after locking and logging to perform a put.
//     pub fn advance_put(&self, db_op: DBOp, key: &str, value: &str, lsn: usize) {
//         self.putting.insert(key.into(), (value.into(), lsn));
//         self.deleting.remove(key);
//         let seq_num = {
//             let inner = self.inner.read().unwrap();
//             inner.write_queue.push((db_op, lsn));
//             inner.seq_num
//         };
//         self.unlock(key);
//         self.wait_for_flush(lsn, seq_num);
//     }

//     /// Call after writing to db to remove from put set.
//     pub fn complete_put(&self, key: &str, value: &str, lsn: usize) {
//         if let Some((_, (_, lsn))) = self.putting.remove_if(key, |_, (_, curr_lsn)| {
//             lsn == *curr_lsn
//         }) {
//             self.cache_manager.store(key, value, lsn)
//         }
//         self.deleting.remove_if(key, |_, curr_lsn| {
//             lsn == *curr_lsn
//         });
//     }


//     /// Call after locking and logging to perform delete.
//     pub fn advance_delete(&self, db_op: DBOp, key: &str, lsn: usize) {
//         self.deleting.insert(key.into(), lsn);
//         self.putting.remove(key);
//         let seq_num = {
//             let inner = self.inner.read().unwrap();
//             inner.write_queue.push((db_op, lsn));
//             inner.flush_lsn
//         };
//         self.unlock(key);
//         self.wait_for_flush(lsn, seq_num);
//     }

//     /// Call after deleting from persistent store.
//     pub fn complete_delete(&self, key: &str, lsn: usize) {
//         let deleted_val = self.deleting.remove_if(key, |_, curr_lsn| {
//             lsn == *curr_lsn
//         });
//         if let Some((_, deleted_lsn)) = deleted_val {
//             if deleted_lsn == lsn {
//                 self.cache_manager.invalidate(key, lsn);
//             }
//         }
//     }
// }

// struct PersistentDB {
//     lock_manager: LockManager,
//     cache: MemCache,
//     db: Database,
//     wal: PersistentLog,
//     write_queue: ConcurrentQueue<DBOp>,

//     flush_lock: Mutex<()>,
// }

// struct TxnPrepare {
//     lock_set: HashMap<String, usize>,
//     read_set: HashSet<String>,
//     delete_set: HashSet<String>,
//     write_set: HashSet<String>,
// }

// #[derive(Clone, Debug)]
// enum DBOp {
//     Read{key: String},
//     Write{key: String, value: String},
//     Delete{key: String},
//     Prepare{txn_id: u128, },
//     Commit{txn_id: u128},
// }

// impl PersistentDB {
//     pub async fn new(namespace: &str, name: &str) -> PersistentDB {
//         let wal = PersistentLog::new(namespace, name).await;
//         let db = tokio::task::block_in_place(move || {
//             let mode = std::env::var("EXECUTION_MODE").unwrap_or("".into());
//             let storage_dir = common::shared_storage_prefix();
//             let storage_dir = format!("{storage_dir}/{SUBSYSTEM_NAME}/{namespace}/{name}");
//             let is_ecs = mode == ACTOR_ECS_MODE;
//             let db = Database::new(&storage_dir, "exclusive", is_ecs);
//             db
//         });

//         PersistentDB {
//             wal,
//             db,
//         }
//     }

//     pub fn read(&self, key: &str) -> Option<(String, usize)> {
//         // First try cache.
//         let val = self.cache.read(key);
//         if val.is_some() {
//             return val;
//         }
//         // Then try db
//         let val = loop {
//             let conn = match self.db.pool.get() {
//                 Ok(conn) => conn,
//                 Err(x) => {
//                     println!("{x:?}");
//                     continue;
//                 }
//             };
//             let res = conn.query_row(
//                 "SELECT value, lsn FROM data_store WHERE key=?",
//                 [key],
//                 |r| r.get(0).map(|v: String| {
//                     let lsn: usize = r.get(1).unwrap();
//                     (v, lsn)
//                 })
//             );
//             match res {
//                 Ok((value, lsn)) => break Some((value, lsn)),
//                 Err(rusqlite::Error::QueryReturnedNoRows) => break None,
//                 Err(x) => {
//                     println!("{x:?}");
//                     continue;
//                 }
//             }
//         };
//         // Try cache.
//         if let Some((value, lsn)) = &val {
//             self.cache.store(key, value, *lsn);
//         }
//         // Return.
//         val
//     }

//     pub fn put(&self, req_vec: Vec<u8>, req: DBOp, key: &str, value: &str) -> Option<usize> {
//         let written = tokio::task::block_in_place(move || {
//             let locked = self.lock_manager.lock(key);
//             if !locked {
//                 return None;
//             }
//             let lsn = self.wal.enqueue_sync(req_vec);
//             // Early unlock. Must be pushed into the write queue first to prevent concurrency bugs.
//             self.write_queue.push(req).unwrap();
//             self.lock_manager.unlock(key);
//             loop {
//                 let flush_lsn = self.wal.get_flush_lsn_sync();
//                 if flush_lsn >= lsn {
//                     break;
//                 }
//                 std::thread::sleep(std::time::Duration::from_millis(1));
//             }
//             Some(lsn)
//         });
//         written
//     }

//     pub fn delete(&self, req_vec: Vec<u8>, req: &DBOp, key: &str) -> Option<usize> {
//         let written = tokio::task::block_in_place(move || {
//             // Lock.
//             let locked = self.lock_manager.lock(key);
//             if !locked {
//                 return None;
//             }
//             let lsn = self.wal.enqueue_sync(req_vec);
//             // Early unlock.
//             self.lock_manager.unlock(key);
//             loop {
//                 let flush_lsn = self.wal.get_flush_lsn_sync();
//                 if flush_lsn >= lsn {
//                     return Some(lsn);
//                 }
//                 std::thread::sleep(std::time::Duration::from_millis(1));
//             }
//         });
//         if let Some(lsn) = &written {
//             self.cache.invalidate(key);
//         }
//         written
//     }
// }