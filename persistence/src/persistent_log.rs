use super::{
    fast_deserialize, fast_serialize, get_shared_log_connection, sync_clean_die, ECS_MODE,
    NUM_DB_RETRIES, SUBSYSTEM_NAME,
};
use bytes::Bytes;
use common::adaptation::frontend::AdapterFrontend;
use common::adaptation::ServerfulInstance;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::atomic;
use std::sync::{mpsc, Arc, Mutex};
use tokio::runtime::Handle;
use tokio::sync::oneshot;

use crate::database::Database;
use crate::{PersistenceReqMeta, PersistenceRespMeta};
const MAX_REPLICATION_FAILURES: usize = 500;

/// A lockful implementation of persistent log.
/// Probably should not have more than two concurrent flushers.
#[derive(Clone)]
pub struct PersistentLog {
    inner: Arc<Mutex<PersistentLogInner>>,
    instance_ids_lock: Arc<Mutex<HashSet<String>>>,
    flush_lock: Arc<Mutex<()>>,
    failed_replications: Arc<atomic::AtomicUsize>,
    owner_id: usize,
    direct_client: reqwest::blocking::Client,
    front_end: Arc<AdapterFrontend>,
    pub db: Database,
    try_replicate: bool,
    handle: tokio::runtime::Handle,
}

/// Modifyable internal state.
struct PersistentLogInner {
    curr_lsn: usize,                    // curr_lsn+1 is the lsn of the next enqueue.
    flush_lsn: usize,                   // Highest lsn known to be persisted or replicated.
    persisted_lsn: usize,               // Highest lsn known to be persisted.
    start_lsn: usize,                   // Starting point of the logs given all the truncates.
    new_entries: Vec<(usize, Vec<u8>)>, // Enqueued entries.
    new_entries_size: usize,            // Total size of enqueued entries.
    instances: Vec<ServerfulInstance>,  // Instances to replicate to.
}

#[derive(Debug)]
enum PersistedMode {
    Database,
    Replicas(bool),
}

impl PersistentLog {
    /// Create a new persistent log.
    pub async fn new(namespace: &str, name: &str) -> Self {
        let handle = Handle::current();
        tokio::task::block_in_place(move || Self::new_sync(handle, namespace, name))
    }

    /// Sync version of new.
    fn new_sync(handle: Handle, namespace: &str, name: &str) -> Self {
        println!("Creating persistent log!");
        let mode = std::env::var("EXECUTION_MODE").unwrap_or_else(|_| ECS_MODE.into());
        let storage_dir = common::shared_storage_prefix();
        let storage_dir = format!("{storage_dir}/{SUBSYSTEM_NAME}/{namespace}/{name}");
        let is_ecs = mode == ECS_MODE;
        let db = Database::new(&storage_dir, "main", is_ecs);
        // First, use db to retrieve and update ownership and quorum information
        println!("Setting up persistent log!");
        let mut conn = db.pool.get().unwrap();
        let txn = conn.transaction().unwrap();
        // Create table of logs.
        let res = txn.execute(
            "CREATE TABLE IF NOT EXISTS system__logs (lo_lsn INTEGER PRIMARY KEY, hi_lsn BIGINT, entries BLOB)"
            ,[]
        );
        // Right after opening db, it is safe to assume conflicts are rare.
        // Because db opening takes an exclusive (advisory) lock.
        match res {
            Ok(_) => {}
            Err(x) => {
                eprintln!("Could not access db. Pid={}. {x:?}", std::process::id());
                std::process::exit(1);
            }
        }
        // Create table of log ownership.
        let res = txn.execute(
            "CREATE TABLE IF NOT EXISTS system__logs_ownership (unique_row INTEGER PRIMARY KEY, owner_id BIGINT, new_owner_id BIGINT, start_lsn BIGINT, instances TEXT)"
            , []
        );
        match res {
            Ok(_) => {}
            Err(x) => {
                eprintln!("Could not access db. Pid={}. {x:?}", std::process::id());
                std::process::exit(1);
            }
        }
        // Read state of old owner.
        let res = txn.query_row(
            "SELECT owner_id, new_owner_id, start_lsn, instances FROM system__logs_ownership",
            [],
            |r| {
                r.get(0).map(|owner_id: usize| {
                    let new_owner_id: usize = r.get(1).unwrap();
                    let start_lsn: usize = r.get(2).unwrap();
                    let instances: String = r.get(3).unwrap();
                    println!("INSTANCES: {instances}");
                    (owner_id, new_owner_id, start_lsn, instances)
                })
            },
        );
        // Note that start_lsn starts from 1. This is because null is conceptually at index 0.
        let (owner_id, new_owner_id, start_lsn, old_instances): (usize, usize, usize, String) = res
            .unwrap_or({
                let v: Vec<ServerfulInstance> = vec![];
                (0, 0, 1, serde_json::to_string(&v).unwrap())
            });
        let my_owner_id = new_owner_id + 1;
        // Update new owner id to prevent old owner from updating instances to replicate to.
        let res = txn.execute(
            "REPLACE INTO system__logs_ownership(unique_row, owner_id, new_owner_id, start_lsn, instances) VALUES (0, ?, ?, ?, ?)",
            rusqlite::params![owner_id, my_owner_id, start_lsn, &old_instances]
        );
        match res {
            Ok(_) => {}
            Err(x) => {
                eprintln!("Could not access db. Pid={}. {x:?}", std::process::id());
                std::process::exit(1);
            }
        }
        // Commit.
        let res = txn.commit();
        match res {
            Ok(_) => {}
            Err(x) => {
                eprintln!("Could not access db. Pid={}. {x:?}", std::process::id());
                std::process::exit(1);
            }
        }
        // At this point, the old owner cannot change the instances to replicate to.
        // However, those instances may have pending writes that were acked.
        // We need to make sure those instances are drained:
        // 1. They will accept no new writes from the old owner.
        // 2. Every write they have received should make it to shared disk.
        // We also need to recover flush_lsn and curr_lsn.
        // The initialize function() does all of this.
        let inner = Arc::new(Mutex::new(PersistentLogInner {
            curr_lsn: 0,      // Will be set in initialize().
            flush_lsn: 0,     // Will be set in initialize().
            persisted_lsn: 0, // Will be set in initialize().
            start_lsn,
            new_entries_size: 0,
            new_entries: Vec::new(),
            instances: Vec::new(), // Will be set in initialize().
        }));
        println!("Creating frontend");
        let front_end = handle
            .block_on(async move { AdapterFrontend::new(SUBSYSTEM_NAME, namespace, name).await });
        let plog = PersistentLog {
            inner,
            db,
            owner_id: my_owner_id,
            flush_lock: Arc::new(Mutex::new(())),
            instance_ids_lock: Arc::new(Mutex::new(HashSet::new())),
            direct_client: reqwest::blocking::ClientBuilder::new()
                .connect_timeout(std::time::Duration::from_millis(500))
                .timeout(std::time::Duration::from_millis(500))
                .build()
                .unwrap(),
            handle,
            front_end: Arc::new(front_end),
            failed_replications: Arc::new(atomic::AtomicUsize::new(0)),
            try_replicate: is_ecs,
        };
        let old_instances = serde_json::from_str(&old_instances).unwrap();
        println!("Initializing persistent log.");
        plog.initialize(namespace, name, old_instances);
        plog
    }

    /// Get current flush lsn.
    pub async fn get_flush_lsn(&self) -> usize {
        tokio::task::block_in_place(move || self.get_flush_lsn_sync())
    }

    /// Sync version of get_flush_lsn.
    pub fn get_flush_lsn_sync(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.flush_lsn
    }

    /// Enqueue one new element in the log.
    pub async fn enqueue(&self, content: Vec<u8>) -> usize {
        tokio::task::block_in_place(move || self.enqueue_sync(content))
    }

    /// Sync version of enqueue
    pub fn enqueue_sync(&self, content: Vec<u8>) -> usize {
        let mut inner = self.inner.lock().unwrap();
        // First log entry should be 1. So increment beforehand.
        // The 0th log entry is the null entry.
        inner.curr_lsn += 1;
        let lsn = inner.curr_lsn;
        inner.new_entries_size += content.len();
        inner.new_entries.push((lsn, content));
        lsn
    }

    /// Flush all enqued logs.
    /// Allows concurrent flushes.
    /// You should use spawn blocking within an async context.
    pub async fn flush(&self, ch: Option<oneshot::Sender<()>>) -> usize {
        tokio::task::block_in_place(move || self.flush_sync(ch))
    }

    /// Sync version of flush.
    pub fn flush_sync(&self, ch: Option<oneshot::Sender<()>>) -> usize {
        // Only allow one flush at a time, but also allow enqueues while flushing.
        // Using the channel, caller can know that flushing has started and keep enqueuing.
        let _l = self.flush_lock.lock().unwrap();
        let (entries, entries_size, curr_lsn, persisted_lsn, instances) = {
            let mut inner = self.inner.lock().unwrap();
            // Get and reset size.
            let entries_size = inner.new_entries_size;
            inner.new_entries_size = 0;
            // Get and reset entries.
            let entries: Vec<(usize, Vec<u8>)> = inner.new_entries.drain(..).collect();
            (
                entries,
                entries_size,
                inner.curr_lsn,
                inner.persisted_lsn,
                inner.instances.clone(),
            )
        };
        // Allow new enqueues.
        if let Some(ch) = ch {
            let _ = ch.send(());
        }
        self.handle.block_on(async {
            self.front_end.collect_metric(serde_json::Value::Null).await;
        });
        self.flush_entries(entries, entries_size, persisted_lsn, instances);
        {
            let mut inner = self.inner.lock().unwrap();
            inner.flush_lsn = curr_lsn;
        }
        let failed_count = self.failed_replications.load(atomic::Ordering::Relaxed);
        if failed_count > 500 {
            self.change_instances(false);
        }
        curr_lsn
    }

    /// Deserialize in a predictable manner.
    pub fn fast_deserialize(&self, entries: Vec<u8>) -> Vec<(usize, Vec<u8>)> {
        let total_size = entries.len();
        let mut curr_offset = 0;
        let mut output = Vec::new();
        while curr_offset < total_size {
            let len_lo = curr_offset;
            let len_hi = len_lo + 8;
            let lsn_lo = len_hi;
            let lsn_hi = lsn_lo + 8;
            let len = &entries[len_lo..len_hi];
            let len = usize::from_be_bytes(len.try_into().unwrap());
            let lsn = &entries[lsn_lo..lsn_hi];
            let lsn = usize::from_be_bytes(lsn.try_into().unwrap());
            let entry_lo = lsn_hi;
            let entry_hi = entry_lo + len;
            let entry = &entries[entry_lo..entry_hi];
            output.push((lsn, entry.to_vec()));
            curr_offset = entry_hi;
        }

        output
    }

    fn flush_entries(
        &self,
        entries: Vec<(usize, Vec<u8>)>,
        entries_size: usize,
        persisted_lsn: usize,
        instances: Vec<ServerfulInstance>,
    ) {
        if entries.is_empty() {
            return;
        }
        // Get bounds.
        let lo_lsn = entries.first().unwrap().0;
        let hi_lsn = entries.last().unwrap().0;
        // Serialize
        let entries = fast_serialize(&entries, entries_size);
        let entries = bytes::Bytes::from(entries);
        // Send to database and to replicas.
        let (tx, rx) = mpsc::channel();
        {
            let tx = tx.clone();
            let entries = entries.clone();
            self.persist_on_db(tx, entries, lo_lsn, hi_lsn);
        };
        {
            let entries = entries;
            self.persist_on_replicas(tx, entries, lo_lsn, hi_lsn, persisted_lsn, instances);
        };
        // Wait for persistence.
        let persisted = rx.recv().unwrap();
        match persisted {
            PersistedMode::Database => {
                // On db, persistence is guaranteed. So async recv remaining.
                std::thread::spawn(move || {
                    let _persisted = rx.recv().unwrap();
                });
            }
            PersistedMode::Replicas(replicated) => {
                if replicated {
                    // When replicated on a quorum, persistence is guaranteed. So async drain.
                    std::thread::spawn(move || {
                        let _persisted = rx.recv().unwrap();
                    });
                } else {
                    // Without a quorum, we have to wait for database.
                    let _persisted = rx.recv().unwrap();
                    // println!("{_persisted:?}");
                }
            }
        }
    }

    /// Return total size of log. Should not be called frequently.
    /// You should generally prevent the log from growing past 1GB.
    pub async fn log_size(&self) -> usize {
        tokio::task::block_in_place(move || {
            let conn = self.db.pool.get().unwrap();
            for _ in 0..NUM_DB_RETRIES {
                match conn.busy_timeout(std::time::Duration::from_secs(1)) {
                    Ok(_) => {}
                    _ => continue,
                }
                match conn.query_row(
                    "SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size()",
                    [],
                    |row| {
                        row.get(0)
                }) {
                    Ok(total_size) => {return total_size;}
                    _ => continue,
                }
            }
            sync_clean_die(&self.handle, "Cannot access db. Fast exiting...");
        })
    }

    /// Truncate up to (and including) at most the given lsn.
    pub async fn truncate(&self, lsn: usize) {
        tokio::task::block_in_place(move || self.truncate_sync(lsn))
    }

    /// Sync version of truncate.
    pub fn truncate_sync(&self, lsn: usize) {
        let curr_start_lsn = {
            let inner = self.inner.lock().unwrap();
            inner.start_lsn
        };
        println!("CurrStartLsn, lsn = {curr_start_lsn}, {lsn}");
        if curr_start_lsn >= lsn {
            return;
        }

        let mut conn = self.db.pool.get().unwrap();
        for _ in 0..NUM_DB_RETRIES {
            let txn = match conn.transaction() {
                Ok(txn) => txn,
                _ => continue,
            };
            match txn.execute(
                "UPDATE system__logs_ownership SET start_lsn=? WHERE new_owner_id=?",
                [lsn, self.owner_id],
            ) {
                Ok(executed) if executed > 0 => {}
                Ok(executed) if executed == 0 => {
                    eprintln!("Truncating node does not own db. Fast exiting...");
                    std::process::exit(1);
                }
                _ => {
                    continue;
                }
            }
            match txn.execute("DELETE FROM system__logs WHERE hi_lsn <= ?", [lsn]) {
                Ok(_) => {}
                _ => continue,
            }
            match txn.commit() {
                Ok(_) => {}
                _ => continue,
            }
            {
                let mut inner = self.inner.lock().unwrap();
                // Concurrent truncate may change value.
                if inner.start_lsn < lsn {
                    inner.start_lsn = lsn;
                }
                return;
            };
        }
        eprintln!("Cannot access db. Fast exiting...");
        std::process::exit(1);
    }

    /// Replay log entries, possibly starting from a given lsn.
    /// Will return log entries in flush batches.
    /// So use start_lsn to get values starting from a given lsn (exclusive).
    /// When called concurrently with flush(), may miss some last few entries
    /// Returns Vec<(lsn, log)>.
    pub async fn replay(&self, exclusive_start_lsn: usize) -> Vec<(usize, Vec<u8>)> {
        tokio::task::block_in_place(move || self.replay_sync(exclusive_start_lsn))
    }

    /// Sync version of replay.
    pub fn replay_sync(&self, exclusive_start_lsn: usize) -> Vec<(usize, Vec<u8>)> {
        let exclusive_start_lsn = {
            let inner = self.inner.lock().unwrap();
            if inner.start_lsn > exclusive_start_lsn {
                inner.start_lsn
            } else {
                exclusive_start_lsn
            }
        };
        // println!("Exclusive start lsn: {exclusive_start_lsn}");
        for _ in 0..NUM_DB_RETRIES {
            let conn = match self.db.pool.get() {
                Ok(conn) => conn,
                Err(x) => {
                    println!("{x:?}");
                    continue;
                }
            };
            let mut stmt = match conn.prepare(
                "SELECT entries FROM system__logs WHERE lo_lsn > ? ORDER BY lo_lsn LIMIT 2",
            ) {
                Ok(stmt) => stmt,
                Err(x) => {
                    println!("{x:?}");
                    continue;
                }
            };
            let mut rows = match stmt.query([exclusive_start_lsn]) {
                Ok(rows) => rows,
                Err(x) => {
                    println!("{x:?}");
                    continue;
                }
            };
            let mut entries: Vec<u8> = Vec::new();
            let (entries, ok) = loop {
                match rows.next() {
                    Ok(Some(row)) => {
                        let mut row: Vec<u8> = row.get(0).unwrap();
                        entries.append(&mut row);
                    }
                    Ok(None) => {
                        break (entries, true);
                    }
                    Err(x) => {
                        println!("{x:?}");
                        break (vec![], false);
                    }
                }
            };
            if !ok {
                continue;
            }
            let entries = fast_deserialize(&entries);
            let entries = entries
                .into_iter()
                .filter(|(lsn, _)| *lsn > exclusive_start_lsn)
                .collect();
            return entries;
        }
        eprintln!("Cannot access db. Fast exiting...");
        std::process::exit(1);
    }

    /// Call this to allow faster recovery.
    /// Will block every other operation.
    pub async fn terminate(&self) {
        tokio::task::block_in_place(move || {
            let _flush_lock = self.flush_lock.lock();
            let _instance_lock = self.instance_ids_lock.lock();
            let inner = self.inner.lock().unwrap();
            if inner.instances.is_empty() {
                return;
            }
            let persisted_lsn = self.fetch_persisted_lsn();
            self.drain_instances(&inner.instances, persisted_lsn);
            let new_instances: Vec<ServerfulInstance> = Vec::new();
            let new_instances_str = serde_json::to_string(&new_instances).unwrap();
            loop {
                // Can only execute if no new owner since startup.
                let conn = match self.db.pool.get() {
                    Ok(conn) => conn,
                    Err(x) => {
                        println!("{x:?}");
                        continue;
                    }
                };
                let executed = conn.execute(
                    "UPDATE system__logs_ownership SET instances=? WHERE new_owner_id=?",
                    rusqlite::params![new_instances_str, self.owner_id],
                );
                match executed {
                    Ok(x) if x > 0 => {
                        return;
                    }
                    Ok(_) => {
                        eprintln!("New owner changed underneath! Exiting...");
                        std::process::exit(1);
                    }
                    Err(_err) => continue,
                }
            }
        });
    }

    /// Persist entries in DB.
    fn persist_on_db(
        &self,
        tx: mpsc::Sender<PersistedMode>,
        entries: Bytes,
        lo_lsn: usize,
        hi_lsn: usize,
    ) {
        let db = self.db.clone();
        let owner_id = self.owner_id;
        let inner = self.inner.clone(); // To modify persisted lsn.
        std::thread::spawn(move || {
            let conn = db.pool.get().unwrap();
            let entries: &[u8] = &entries;

            for _ in 0..NUM_DB_RETRIES {
                // let start_time = std::time::Instant::now();
                let executed = conn.execute(
                    "REPLACE INTO system__logs(lo_lsn, hi_lsn, entries) \
                    SELECT ?, ?, ? FROM system__logs_ownership WHERE owner_id=?",
                    rusqlite::params![lo_lsn, hi_lsn, entries, owner_id],
                );
                // let end_time = std::time::Instant::now();
                // let duration = end_time.duration_since(start_time);
                // println!("DB Write took: {duration:?}");
                match executed {
                    Ok(executed) if executed > 0 => {
                        tx.send(PersistedMode::Database).unwrap();
                        let mut inner = inner.lock().unwrap();
                        if hi_lsn > inner.persisted_lsn {
                            inner.persisted_lsn = hi_lsn;
                        }
                        return;
                    }
                    Ok(_) => {
                        eprintln!("Node {owner_id} cannot persist due to ownership change. Fast exiting...");
                        std::process::exit(1);
                    }
                    Err(err) => {
                        let err = format!("{err:?}");
                        if !(err.contains("busy") || err.contains("locked")) {
                            eprintln!("Unhandled sqlite error: {err:?}");
                            std::process::exit(1);
                        }
                    }
                }
            }
            eprintln!("Cannot access db. Fast exiting...");
            std::process::exit(1);
        });
    }

    /// Persist entries in a quorum of replicas.
    fn persist_on_replicas(
        &self,
        tx: mpsc::Sender<PersistedMode>,
        entries: Bytes,
        lo_lsn: usize,
        hi_lsn: usize,
        persisted_lsn: usize,
        instances: Vec<ServerfulInstance>,
    ) {
        // Check if quorum can be achieved.
        // changes_instances ensures that num_total is 0 when quorum is not safe.
        let num_total = instances.len();
        let failed_replications = self.failed_replications.clone();
        if num_total == 0 {
            failed_replications.fetch_add(1, atomic::Ordering::Relaxed);
            tx.send(PersistedMode::Replicas(false)).unwrap();
            return;
        }
        let quorum_size: usize = num_total / 2 + 1;
        let client = self.direct_client.clone();
        let owner_id = self.owner_id;
        std::thread::spawn(move || {
            let mut num_completed: usize = 0;
            let mut num_valid: usize = 0;
            // Send to each replica.
            let (replica_tx, replica_rx) = mpsc::channel();
            for instance in instances {
                let entries = entries.clone();
                let client = client.clone();
                let replica_tx = replica_tx.clone();
                let failed_replications = failed_replications.clone();
                std::thread::spawn(move || {
                    let meta = PersistenceReqMeta::Log {
                        lo_lsn,
                        hi_lsn,
                        persisted_lsn,
                        owner_id,
                        replica_id: instance.id.clone(),
                    };
                    let meta = serde_json::to_string(&meta).unwrap();

                    let resp = client
                        .post(instance.url)
                        .header("obelisk-meta", meta)
                        .body(entries)
                        .send();
                    if resp.is_err() {
                        failed_replications.fetch_add(1, atomic::Ordering::Relaxed);
                        replica_tx.send(false).unwrap();
                        return;
                    }
                    let resp = resp.unwrap();
                    let meta = resp.headers().get("obelisk-meta");
                    if meta.is_none() {
                        failed_replications.fetch_add(1, atomic::Ordering::Relaxed);
                        replica_tx.send(false).unwrap();
                        return;
                    }
                    let meta = meta.unwrap().to_str().unwrap();
                    let meta: PersistenceRespMeta = serde_json::from_str(meta).unwrap();
                    match meta {
                        PersistenceRespMeta::Ok => {
                            failed_replications.store(0, atomic::Ordering::Relaxed);
                            replica_tx.send(true).unwrap();
                            
                        }
                        PersistenceRespMeta::Outdated => {
                            eprintln!("A new owner has contacted replicas. Fast exiting...");
                            std::process::exit(1);
                        }
                        _ => {
                            eprintln!("Impossible response type. Exiting...");
                            std::process::exit(1);
                        }
                    }
                });
            }
            while num_completed < num_total && num_valid < quorum_size {
                let valid = replica_rx.recv().unwrap();
                num_completed += 1;
                if valid {
                    num_valid += 1;
                }
            }
            if num_valid >= quorum_size {
                tx.send(PersistedMode::Replicas(true)).unwrap();
            } else {
                tx.send(PersistedMode::Replicas(false)).unwrap();
            }
            while num_completed < num_total {
                let _ = replica_rx.recv().unwrap();
                num_completed += 1;
            }
        });
    }

    /// See the comments inside new_sync() for what this function does.
    fn initialize(&self, namespace: &str, name: &str, old_instances: Vec<ServerfulInstance>) {
        // Assuming no more than AZ+1 uncontolled failures, when drain_instances is done,
        // All accepted writes have made it to disk.
        println!("Old Instances: {old_instances:?}");
        if !old_instances.is_empty() {
            println!("Draining instances");
            let persisted_lsn = self.fetch_persisted_lsn();
            self.drain_instances(&old_instances, persisted_lsn);
        }
        // When update shared_ownership is done, we can assume that even single replicas
        // Stuck in a partition with old owner cannot write to shared storage.
        println!("Update shared ownership!");
        self.update_shared_ownership(namespace, name);
        // It's safe to take the shared logs.
        println!("Take shared logs!");
        let shared_entries = self.take_shared_logs(namespace, name);
        println!("Apply shared logs: {:?}!", shared_entries.len());
        self.apply_shared_logs(shared_entries);
        // It's safe to take ownership with possibly new list of replicas.
        println!("Change lsns!");
        self.change_instances(true);
        // Update lsns.
        println!("Fetch lsns!");
        let persisted_lsn = self.fetch_persisted_lsn();
        let mut inner = self.inner.lock().unwrap();
        inner.flush_lsn = persisted_lsn;
        inner.curr_lsn = persisted_lsn;
        inner.persisted_lsn = persisted_lsn;
    }

    /// Fetch current flush_lsn and curr_lsn.
    fn fetch_persisted_lsn(&self) -> usize {
        for _ in 0..NUM_DB_RETRIES {
            let conn = self.db.pool.get().unwrap();
            let persisted_lsn =
                conn.query_row("SELECT MAX(hi_lsn) FROM system__logs", [], |r| r.get(0));
            println!("Persisted lsn: {persisted_lsn:?}");
            match persisted_lsn {
                Ok(persisted_lsn) => {
                    return persisted_lsn;
                }
                Err(rusqlite::Error::InvalidColumnType(_, _, rusqlite::types::Type::Null)) => {
                    let inner = self.inner.lock().unwrap();
                    return inner.start_lsn;
                }
                _ => {
                    continue;
                }
            }
        }
        eprintln!("Cannot access db. Fast exiting...");
        std::process::exit(1);
    }

    /// Change instances to replicate to.
    /// When set_ownership is set, will also set self as owner.
    fn change_instances(&self, set_ownership: bool) {
        // Prevent concurrent changes.
        let mut curr_instance_ids = self.instance_ids_lock.lock().unwrap();
        if !set_ownership {
            let counter = self.failed_replications.load(atomic::Ordering::Relaxed);
            println!("Counter: {counter}");
            if counter < MAX_REPLICATION_FAILURES {
                return;
            }
            self.failed_replications.store(0, atomic::Ordering::Relaxed);
        }
        let mut new_instances = Vec::new();
        let mut all_instances = self
            .handle
            .block_on(async { self.front_end.serverful_instances().await });
        // Order join_time. This to maximize reuse.
        all_instances.sort_by(|x1, x2| {
            match x1.join_time.cmp(&x2.join_time) {
                Ordering::Equal => {
                    // Just to handle unlikely same join times.
                    x1.id.cmp(&x2.id)
                }
                o => o,
            }
        });
        println!("All Instances: {all_instances:?}");
        let mut az_counts: HashMap<String, u64> = all_instances
            .iter()
            .map(|instance| (instance.az.clone(), 0))
            .collect();
        // We want to tolerate AZ + 1 failures, so we need at least three AZs.
        // We need 6 nodes across 3 AZs, or 5 nodes across 5 AZs.
        let mut num_azs = 0;
        let mut is_safe = false;
        for instance in all_instances {
            let az_count = az_counts.get_mut(&instance.az).unwrap();
            // Have no more than two nodes per az.
            if *az_count < 2 {
                // If this az is not yet considered, add it to the list.
                if *az_count == 0 {
                    num_azs += 1;
                }
                *az_count += 1;
                new_instances.push(instance);
            }
            // Check if replication is safe for AZ+1 failure.
            if new_instances.len() == 6 || num_azs == 5 {
                is_safe = true;
            }
        }
        // If amount of replication is not safe, empty instances.
        if !is_safe || !self.try_replicate {
            println!("NOT REPLICATE.");
            new_instances.clear();
        }
        let new_instance_ids: HashSet<String> = new_instances
            .iter()
            .map(|instance| instance.id.clone())
            .collect();
        // If instances have not changed, return unless we are trying to reset ownership.
        if !set_ownership && curr_instance_ids.eq(&new_instance_ids) {
            return;
        }
        // If instances have changed, or we are resetting ownership, write to db.
        let new_instances_str = serde_json::to_string(&new_instances).unwrap();
        let conn = self.db.pool.get().unwrap();
        for _ in 0..NUM_DB_RETRIES {
            // Can only execute if no new owner since startup.
            println!("Writing {new_instances_str}");
            let executed = conn.execute(
                "UPDATE system__logs_ownership SET instances=?, owner_id=? WHERE new_owner_id=?",
                rusqlite::params![new_instances_str, self.owner_id, self.owner_id],
            );
            match executed {
                Ok(x) if x > 0 => {
                    let mut inner = self.inner.lock().unwrap();
                    *curr_instance_ids = new_instance_ids;
                    inner.instances = new_instances;
                    println!("Updated ownership");
                    return;
                }
                Ok(_) => {
                    eprintln!("New owner changed underneath! Exiting...");
                    std::process::exit(1);
                }
                Err(_err) => continue,
            }
        }
        eprintln!("Cannot access db. Fast exiting...");
        std::process::exit(1);
    }

    /// Assuming at most AZ+1 uncontrolled crashes, this function makes sure that
    /// all acked writes will make to shared storage and that no previously active node can replicate new stuff.
    fn drain_instances(&self, instances: &[ServerfulInstance], persisted_lsn: usize) {
        if instances.is_empty() {
            return;
        }
        let mut reached_instances: HashSet<String> = HashSet::new();
        let owner_id = self.owner_id;
        let has_external_access = self.front_end.has_external_access;
        loop {
            let curr_instances = self
                .handle
                .block_on(async { self.front_end.force_read_instances().await.peers });
            let mut remaining_instances: Vec<&ServerfulInstance> = Vec::new();
            for instance in instances {
                if curr_instances.contains_key(&instance.id)
                    && !reached_instances.contains(&instance.id)
                {
                    remaining_instances.push(instance);
                }
            }
            if remaining_instances.is_empty() {
                return;
            }
            // Contact all remaining instances.
            let (tx, rx) = mpsc::channel();
            for instance in remaining_instances.iter() {
                let client = self.direct_client.clone();
                let instance_id = instance.id.clone();
                let url = instance.url.clone();
                let persisted_lsn = persisted_lsn;
                let drain_owner = if let Some(drain_owner) = instance.custom_info.as_u64() {
                    drain_owner as usize
                } else {
                    0
                };
                let tx = tx.clone();
                std::thread::spawn(move || {
                    if drain_owner >= owner_id {
                        tx.send((instance_id, true)).unwrap();
                        return;
                    }
                    // If no external access, only indirect communication is possible.
                    if !has_external_access {
                        tx.send((instance_id, false)).unwrap();
                        return;
                    }
                    let req_body: Vec<u8> = Vec::new();
                    let req_meta = PersistenceReqMeta::Drain {
                        owner_id,
                        persisted_lsn,
                        replica_id: instance_id.clone(),
                    };
                    let req_meta = serde_json::to_string(&req_meta).unwrap();
                    let resp = client
                        .post(url)
                        .header("obelisk-meta", req_meta)
                        .body(req_body)
                        .send();
                    if resp.is_err() {
                        tx.send((instance_id, false)).unwrap();
                        return;
                    }
                    let resp = resp.unwrap();
                    let meta = resp.headers().get("obelisk-meta");
                    if meta.is_none() {
                        tx.send((instance_id, false)).unwrap();
                        return;
                    }
                    let meta = meta.unwrap().to_str().unwrap();
                    let meta: PersistenceRespMeta = serde_json::from_str(meta).unwrap();
                    println!("Resp: {meta:?}");
                    match meta {
                        PersistenceRespMeta::Ok => {
                            tx.send((instance_id, true)).unwrap();
                            
                        }
                        PersistenceRespMeta::Outdated => {
                            eprintln!("A new owner has contacted replicas. Fast exiting...");
                            std::process::exit(1);
                        }
                        _ => {
                            eprintln!("Impossible response type. Exiting...");
                            std::process::exit(1);
                        }
                    }
                });
            }
            let mut num_received = 0;
            let mut all_ok = true;
            while num_received < remaining_instances.len() {
                // Reqwest client has a timeout, so this (hopefully) won't block too long.
                let (instance_id, ok) = rx.recv().unwrap();
                if ok {
                    reached_instances.insert(instance_id);
                }
                all_ok = all_ok && ok;
                num_received += 1;
            }
            if !all_ok {
                // Avoid putting too much pressure on dynamo and network.
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
        }
    }

    fn apply_shared_logs(&self, shared_entries: Vec<(usize, usize, Vec<u8>)>) {
        println!("{shared_entries:?}");
        for _ in 0..NUM_DB_RETRIES {
            println!("Apply");
            let mut conn = match self.db.pool.get() {
                Ok(conn) => conn,
                Err(x) => {
                    println!("Pool: {x:?}");
                    continue;
                }
            };
            match conn.busy_timeout(std::time::Duration::from_secs(1)) {
                Ok(_) => {}
                Err(x) => {
                    println!("Timeout: {x:?}");
                    continue;
                }
            }
            let txn = match conn.transaction() {
                Ok(txn) => txn,
                Err(x) => {
                    println!("Txn: {x:?}");
                    continue;
                }
            };
            match txn.query_row("SELECT new_owner_id FROM system__logs_ownership", [], |r| {
                let res: usize = r.get(0).unwrap();
                Ok(res)
            }) {
                Ok(new_owner_id) => {
                    if new_owner_id > self.owner_id {
                        eprintln!("Shared db is higher id owner. Fast exiting...");
                        std::process::exit(1);
                    }
                }
                Err(rusqlite::Error::QueryReturnedNoRows) => {}
                Err(x) => {
                    println!("Query: {x:?}");
                    continue;
                }
            }
            let ok = {
                let mut stmt = match txn
                    .prepare("REPLACE INTO system__logs(lo_lsn, hi_lsn, entries) VALUES (?, ?, ?)")
                {
                    Ok(stmt) => stmt,
                    Err(x) => {
                        println!("Stmt: {x:?}");
                        continue;
                    }
                };
                let mut ok = true;
                for (lo_lsn, hi_lsn, entries) in &shared_entries {
                    match stmt.execute(rusqlite::params![lo_lsn, hi_lsn, entries]) {
                        Ok(_) => {}
                        _ => {
                            ok = false;
                            break;
                        }
                    }
                }
                ok
            };
            if !ok {
                continue;
            }
            match txn.commit() {
                Ok(_) => return,
                Err(x) => {
                    println!("Commit: {x:?}");
                    continue;
                }
            }
        }
        eprintln!("Cannot access shared db. Fast exiting...");
        std::process::exit(1);
    }

    fn take_shared_logs(&self, namespace: &str, name: &str) -> Vec<(usize, usize, Vec<u8>)> {
        for _ in 0..NUM_DB_RETRIES {
            let conn = get_shared_log_connection(namespace, name, true);
            let mut prepared =
                conn.prepare("SELECT lo_lsn, hi_lsn, entries FROM system__shared_logs");
            let mut rows = match &mut prepared {
                Ok(stmt) => match stmt.query([]) {
                    Ok(rows) => rows,
                    Err(x) => {
                        println!("{x:?}");
                        continue;
                    }
                },
                x => {
                    println!("{x:?}");
                    continue;
                }
            };
            let mut res: Vec<(usize, usize, Vec<u8>)> = Vec::new();
            loop {
                let row = rows.next();
                if let Ok(row) = row {
                    if let Some(row) = row {
                        res.push((
                            row.get(0).unwrap(),
                            row.get(1).unwrap(),
                            row.get(2).unwrap(),
                        ))
                    } else {
                        return res;
                    }
                } else {
                    break;
                }
            }
        }
        eprintln!("Take shared logs. Cannot access shared db. Fast exiting...");
        std::process::exit(1);
    }

    fn update_shared_ownership(&self, namespace: &str, name: &str) {
        for _ in 0..NUM_DB_RETRIES {
            let mut conn = get_shared_log_connection(namespace, name, true);
            let txn = match conn.transaction() {
                Ok(txn) => txn,
                x => {
                    println!("{x:?}");
                    continue;
                }
            };
            // We want to prevent old owners from
            match txn.query_row("SELECT owner_id FROM system__shared_ownership", [], |r| {
                let res: usize = r.get(0).unwrap();
                Ok(res)
            }) {
                Ok(curr_owner_id) => {
                    if curr_owner_id > self.owner_id {
                        eprintln!("Shared db is higher id owner. Fast exiting...");
                        std::process::exit(1);
                    }
                }
                Err(rusqlite::Error::QueryReturnedNoRows) => {}
                x => {
                    println!("{x:?}");
                    continue;
                }
            }
            match txn.execute(
                "REPLACE INTO system__shared_ownership(unique_row, owner_id) VALUES (0, ?)",
                [self.owner_id],
            ) {
                Ok(_) => {}
                x => {
                    println!("{x:?}");
                    continue;
                }
            }
            match txn.commit() {
                Ok(_) => return,
                x => {
                    println!("{x:?}");
                    continue;
                }
            }
        }
        eprintln!("Update shared ownership. Cannot access shared db. Fast exiting...");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::PersistentLog;
    use std::sync::Arc;

    // async fn bench_log(instances: Vec<String>) {
    //     {
    //         let mut inner = log.inner.lock().unwrap();
    //         inner.instances = instances;
    //     }
    //     let num_threads: i32 = 2;
    //     let batch_size: i32 = 512;
    //     let num_elems: i32 = 512 * 200;
    //     // 1KB of data.
    //     let content: Vec<u8> = (0..1024).map(|i| (i % 256) as u8).collect();
    //     let start_time = std::time::Instant::now();
    //     let mut ts = Vec::new();
    //     for tidx in 0..num_threads {
    //         let content = content.clone();
    //         let num_elems: i32 = num_elems.clone();
    //         let log = log.clone();
    //         let _tidx = tidx;
    //         ts.push(tokio::task::spawn(async move {
    //             for i in 0..num_elems {
    //                 log.enqueue(content.clone()).await;
    //                 if (i + 1)%batch_size == 0 {
    //                     log.flush().await;
    //                 }
    //             }
    //         }));
    //     }
    //     for t in ts {
    //         t.await.unwrap();
    //     }

    //     let end_time = std::time::Instant::now();
    //     let duration = end_time.duration_since(start_time);
    //     {
    //         let inner = log.inner.lock().unwrap();
    //         println!("Logs {} took: {duration:?}. Total enqueues: {}", inner.new_entries.len(), num_threads * num_elems);
    //     }
    // }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn test_bench_log() {
        let plog = Arc::new(PersistentLog::new("test", "test").await);
        // 1KB of data.
        let content: Vec<u8> = (0..1024).map(|i| (i % 256) as u8).collect();
        let num_writes = 50_000;
        let start_time = std::time::Instant::now();
        for i in 0..num_writes {
            plog.enqueue(content.clone()).await;
            if (i + 1) % 100 == 0 {
                plog.flush(None).await;
            }
        }
        let end_time = std::time::Instant::now();
        let duration = end_time.duration_since(start_time);
        println!("Logging took: {duration:?}");
        // tokio::time::sleep(std::time::Duration::from_secs(20)).await;
    }

    async fn run_content_test() {
        let plog = Arc::new(PersistentLog::new("messaging", "echo").await);
        let flush_lsn = plog.get_flush_lsn().await;
        println!("Flush Lsn: {flush_lsn}");
        plog.truncate(flush_lsn).await;
        let mut num_entries = 0;
        let mut expected_entries = vec![];
        for i in 0..10 {
            for j in 0..10 {
                num_entries += 1;
                expected_entries.push(vec![i, j]);
                plog.enqueue(vec![i, j]).await;
            }
            plog.flush(None).await;
        }
        println!("Num entries: {num_entries}");
        let mut num_found_entries = 0;
        let mut curr_last_lsn = flush_lsn;
        loop {
            let entries = plog.replay(curr_last_lsn).await;
            if entries.is_empty() {
                break;
            }
            for (lsn, entry) in entries {
                assert!(entry == expected_entries[num_found_entries]);
                assert!(lsn == curr_last_lsn + 1);
                num_found_entries += 1;
                curr_last_lsn = lsn;
            }
        }
        println!("Num found entries: {num_found_entries}");
        assert!(num_found_entries == num_entries);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn content_test() {
        run_content_test().await;
    }

    async fn run_scaling_test() {
        let plog = Arc::new(PersistentLog::new("messaging", "echo").await);
        // Make 100 empty flushes per seconds for 100 seconds to trigger a scale up.
        // Note that the code is written to push metrics even for empty flushes.
        for i in 0..10 {
            let round = i + 1;
            println!("Round {round}/100");
            let failed_count = plog
                .failed_replications
                .load(std::sync::atomic::Ordering::Relaxed);
            println!("FailedCount={failed_count}");
            let scaling_state = plog.front_end.scaling_state().await;
            if let Some(scaling_state) = scaling_state {
                let deployment = scaling_state.deployment;
                let count = scaling_state.current_scale;
                let scaling_info = scaling_state.scaling_info;
                let peers = scaling_state.peers;
                tokio::task::block_in_place(|| {
                    plog.change_instances(false);
                    let inner = plog.inner.lock().unwrap();
                    println!("Instances={:?}", inner.instances);
                });
                println!("Deployment={deployment:?}");
                println!("Count={count:?}");
                println!("ScalingInfo={scaling_info:?}");
                println!("PeersLen={:?}", peers.len());
            } else {
                println!("No scaling state yet!");
            }
            let start_time = std::time::Instant::now();
            for _ in 0..2000 {
                plog.enqueue(vec![1, 2, 3]).await;
                plog.flush(None).await;
            }
            let end_time = std::time::Instant::now();
            let duration = end_time.duration_since(start_time);
            println!("Flushes took: {duration:?}");
            tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
        }
        {
            let inner = plog.inner.lock().unwrap();
            println!("Instances: {:?}", inner.instances);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn scaling_test() {
        run_scaling_test().await;
    }
}
