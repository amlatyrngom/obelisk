use super::{fast_deserialize, fast_serialize, ECS_MODE, NUM_DB_RETRIES, SUBSYSTEM_NAME};
use bytes::Bytes;
use common::adaptation::frontend::AdapterFrontend;
use common::adaptation::ServerfulInstance;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, Mutex, RwLock};

use crate::database::Database;
use crate::log_replica::PendingLog;
use crate::{PersistenceReqMeta, PersistenceRespMeta};
const MAX_REPLICATION_FAILURES: usize = 100;

/// A lockful implementation of persistent log.
/// Probably should not have more than two concurrent flushers.
#[derive(Clone)]
pub struct PersistentLog {
    inner: Arc<Mutex<PersistentLogInner>>,
    replication_lock: Arc<RwLock<()>>,
    flush_lock: Arc<Mutex<()>>,
    failed_replications: Arc<atomic::AtomicUsize>,
    owner_id: usize,
    direct_client: reqwest::Client,
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
    terminating: bool,                  // Actor terminating.
}

#[derive(Debug)]
enum PersistedMode {
    Database,
    Replicas(bool),
}

impl PersistentLog {
    // Pause lambda lock to be able to acquire lock.
    // Should be called very rarely (e.g. when first acquiring lock).
    pub async fn pause_lambda_lock(namespace: &str, name: &str) {
        let actor_function_name = common::full_messaging_name(namespace, name);
        tokio::spawn(async move {
            let shared_config = aws_config::load_from_env().await;
            let lambda_client = aws_sdk_lambda::Client::new(&shared_config);
            let arg = serde_json::json!("ReleaseLock");
            let arg = serde_json::to_vec(&arg).unwrap();
            let arg = aws_smithy_types::Blob::new(arg);
            let mut num_remaining: i64 = 10;
            while num_remaining > 0 {
                let resp = lambda_client
                    .invoke()
                    .function_name(&actor_function_name)
                    .payload(arg.clone())
                    .send()
                    .await;
                if let Ok(resp) = &resp {
                    if let Some(err) = resp.function_error() {
                        if !err.contains("TooManyRequestsException") {
                            num_remaining -= 1;
                            println!("Unlock error {actor_function_name}: {err:?}");
                            continue;
                        }
                    }
                }
                num_remaining -= 1;
                println!("Unlock resp: {resp:?}");
            }
        });
    }

    /// Create a new persistent log.
    pub async fn new(namespace: &str, name: &str) -> Self {
        let mode = std::env::var("EXECUTION_MODE").unwrap_or_else(|_| "".into());
        if mode == ECS_MODE {
            // Remove potential lambda's lock.
            println!("Removing lambda's lock!");
            Self::pause_lambda_lock(namespace, name).await;
        }
        let handle = Handle::current();
        tokio::task::block_in_place(move || Self::new_sync(handle, namespace, name))
    }

    /// Sync version of new.
    fn new_sync(handle: Handle, namespace: &str, name: &str) -> Self {
        println!("Creating persistent log!");
        let mode = std::env::var("EXECUTION_MODE").unwrap_or_else(|_| ECS_MODE.into());
        let shared_dir = common::shared_storage_prefix();
        let storage_dir = format!("{shared_dir}/{SUBSYSTEM_NAME}/{namespace}/{name}");
        let is_ecs = mode == ECS_MODE;
        // TODO: Remove this.
        // println!("Cleanup up shared dir. TODO: Remove me. I am only for testing.");
        // if namespace == "sbtree" && name == "manager" && !is_ecs {
        //     std::fs::remove_dir_all(format!("{shared_dir}/{SUBSYSTEM_NAME}")).unwrap();
        //     std::fs::remove_dir_all(format!("{shared_dir}/sless_btree")).unwrap();
        //     std::fs::remove_dir_all(format!("{shared_dir}/sbtree")).unwrap();
        // }

        let db = Database::new(&storage_dir, "main", true);
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
        std::mem::drop(conn); // Drop handle for initialize().
        let inner = Arc::new(Mutex::new(PersistentLogInner {
            curr_lsn: 0,      // Will be set in initialize().
            flush_lsn: 0,     // Will be set in initialize().
            persisted_lsn: 0, // Will be set in initialize().
            start_lsn,
            new_entries_size: 0,
            new_entries: Vec::new(),
            instances: Vec::new(), // Will be set in initialize().
            terminating: false,
        }));
        println!("Creating frontend");
        let front_end = handle
            .block_on(async move { AdapterFrontend::new(SUBSYSTEM_NAME, namespace, name).await });
        let plog = PersistentLog {
            inner,
            db,
            owner_id: my_owner_id,
            flush_lock: Arc::new(Mutex::new(())),
            replication_lock: Arc::new(RwLock::new(())),
            direct_client: reqwest::ClientBuilder::new()
                .connect_timeout(std::time::Duration::from_secs(2))
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .unwrap(),
            handle,
            front_end: Arc::new(front_end),
            failed_replications: Arc::new(atomic::AtomicUsize::new(0)),
            try_replicate: is_ecs,
        };
        let old_instances = serde_json::from_str(&old_instances).unwrap();
        println!("Initializing persistent log.");
        plog.handle.block_on(async {
            plog.initialize(old_instances).await;
        });
        plog
    }

    /// Get current flush lsn.
    pub async fn get_flush_lsn(&self) -> usize {
        let inner = self.inner.lock().await;
        inner.flush_lsn
    }

    /// Get current flush lsn.
    pub async fn get_start_lsn(&self) -> usize {
        let inner = self.inner.lock().await;
        inner.start_lsn
    }

    /// Enqueue one new element in the log.
    pub async fn enqueue(&self, content: Vec<u8>) -> usize {
        let mut inner = self.inner.lock().await;
        // First log entry should be 1. So increment beforehand.
        // The 0th log entry is the null entry.
        inner.curr_lsn += 1;
        let lsn = inner.curr_lsn;
        inner.new_entries_size += content.len();
        inner.new_entries.push((lsn, content));
        lsn
    }

    /// Flush log entries.
    pub async fn flush(&self) -> usize {
        // Only allow one flush at a time, but also allow enqueues while flushing.
        let _l = self.flush_lock.lock().await;
        let (entries, entries_size, curr_lsn, persisted_lsn, instances) = {
            let mut inner = self.inner.lock().await;
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
        // Log metrics if some entries are being flushed.
        if !entries.is_empty() {
            self.front_end.collect_metric(serde_json::Value::Null).await;
        }

        self.flush_entries(entries, entries_size, persisted_lsn, instances)
            .await;
        {
            let mut inner = self.inner.lock().await;
            inner.flush_lsn = curr_lsn;
        }
        let failed_count = self.failed_replications.load(atomic::Ordering::Relaxed);
        if failed_count > MAX_REPLICATION_FAILURES {
            let this = self.clone();
            tokio::spawn(async move {
                this.change_instances(false).await;
            });
        }
        curr_lsn
    }

    /// Deserialize in a predictable manner.
    pub async fn fast_deserialize(&self, entries: Vec<u8>) -> Vec<(usize, Vec<u8>)> {
        tokio::task::block_in_place(move || {
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
        })
    }

    async fn flush_entries(
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
        let entries = fast_serialize(&entries, entries_size).await;
        let entries = bytes::Bytes::from(entries);
        // Send to database and to replicas.
        let (tx, mut rx) = mpsc::channel(2);
        {
            let tx = tx.clone();
            let entries = entries.clone();
            self.persist_on_db(tx, entries, lo_lsn, hi_lsn).await;
        };
        {
            let tx = tx;
            let entries = entries;
            self.persist_on_replicas(tx, entries, lo_lsn, hi_lsn, persisted_lsn, instances)
                .await;
        };
        // Wait for persistence.
        let persisted = rx.recv().await.unwrap();
        match persisted {
            PersistedMode::Database => {
                // On db, persistence is guaranteed. So async recv remaining.
                tokio::spawn(async move {
                    let _persisted = rx.recv().await;
                });
            }
            PersistedMode::Replicas(replicated) => {
                if replicated {
                    // When replicated on a quorum, persistence is guaranteed. So async drain.
                    tokio::spawn(async move {
                        let _persisted = rx.recv().await;
                    });
                } else {
                    // Without a quorum, we have to wait for database.
                    let _persisted = rx.recv().await;
                }
            }
        }
    }

    /// Truncate up to (and including) at most the given lsn.
    /// TODO: Might have to vaccum.
    pub async fn truncate(&self, lsn: usize) -> Result<(), String> {
        let curr_start_lsn = {
            let inner = self.inner.lock().await;
            inner.start_lsn
        };
        if curr_start_lsn >= lsn {
            return Ok(());
        }
        // Update DB.
        let resp = tokio::task::block_in_place(move || {
            for _ in 0..NUM_DB_RETRIES {
                let mut conn = self.db.pool.get().unwrap();
                let txn = match conn.transaction() {
                    Ok(txn) => txn,
                    Err(x) => {
                        println!("Truncate 1: {x:?}");
                        continue;
                    }
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
                    x => {
                        println!("Truncate 2: {x:?}");
                        continue;
                    }
                }
                match txn.execute("DELETE FROM system__logs WHERE hi_lsn <= ?", [lsn]) {
                    Ok(_) => {}
                    _ => continue,
                }
                match txn.commit() {
                    Ok(_) => return Ok(()),
                    _ => continue,
                }
            }
            return Err("Cannot access db".into());
        });
        if resp.is_ok() {
            let mut inner = self.inner.lock().await;
            // Concurrent truncate may change value.
            if inner.start_lsn < lsn {
                inner.start_lsn = lsn;
            }
        }
        resp
    }

    /// Replay log entries, possibly starting from a given lsn.
    /// Will return log entries in flush batches.
    /// So use start_lsn to get values starting from a given lsn (exclusive).
    /// When called concurrently with flush(), may miss some last few entries
    /// Returns Vec<(lsn, log)>.
    pub async fn replay(
        &self,
        exclusive_start_lsn: usize,
    ) -> Result<Vec<(usize, Vec<u8>)>, String> {
        let exclusive_start_lsn = {
            let inner = self.inner.lock().await;
            if inner.start_lsn > exclusive_start_lsn {
                inner.start_lsn
            } else {
                exclusive_start_lsn
            }
        };
        tokio::task::block_in_place(move || {
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
                        println!("Replay 1: {x:?}");
                        continue;
                    }
                };
                let mut rows = match stmt.query([exclusive_start_lsn]) {
                    Ok(rows) => rows,
                    Err(x) => {
                        println!("Replay 2: {x:?}");
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
                return Ok(entries);
            }
            return Err("Cannot access db".into());
        })
    }

    /// Call this to allow faster recovery.
    /// Will block every other operation.
    pub async fn terminate(&self) {
        // Prevent new replications.
        let _replication_lock = self.replication_lock.write().await;
        let curr_instances = {
            let mut inner = self.inner.lock().await;
            inner.terminating = true;
            if inner.instances.is_empty() {
                return;
            }
            inner.instances.clone()
        };
        // Drain instances until there are no pending logs.
        loop {
            let persisted_lsn = {
                let inner = self.inner.lock().await;
                inner.persisted_lsn
            };
            let pending_logs = self.drain_instances(&curr_instances, persisted_lsn).await;
            if pending_logs.is_empty() {
                break;
            } else {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
        // Write empty instances.
        let new_instances: Vec<ServerfulInstance> = Vec::new();
        let new_instances_str = serde_json::to_string(&new_instances).unwrap();

        tokio::task::block_in_place(move || loop {
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
        });
        // Update inner.
        let mut inner = self.inner.lock().await;
        inner.instances = vec![];
    }

    /// Persist entries in DB.
    async fn persist_on_db(
        &self,
        tx: mpsc::Sender<PersistedMode>,
        entries: Bytes,
        lo_lsn: usize,
        hi_lsn: usize,
    ) {
        let db = self.db.clone();
        let owner_id = self.owner_id;
        let inner = self.inner.clone(); // To modify persisted lsn.
        tokio::spawn(async move {
            // Write in DB.
            tokio::task::block_in_place(move || {
                let entries: &[u8] = &entries;
                for _ in 0..NUM_DB_RETRIES {
                    // let start_time = std::time::Instant::now();
                    let conn = db.pool.get().unwrap();
                    let executed = conn.execute(
                        "REPLACE INTO system__logs(lo_lsn, hi_lsn, entries) \
                        SELECT ?, ?, ? FROM system__logs_ownership WHERE new_owner_id=?",
                        rusqlite::params![lo_lsn, hi_lsn, entries, owner_id],
                    );
                    // let end_time = std::time::Instant::now();
                    // let duration = end_time.duration_since(start_time);
                    // println!("DB Write took: {duration:?}");
                    match executed {
                        Ok(executed) if executed > 0 => {
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
            // Respond.
            tx.send(PersistedMode::Database).await.unwrap();
            // Update persisted lsn.
            let mut inner = inner.lock().await;
            if hi_lsn > inner.persisted_lsn {
                inner.persisted_lsn = hi_lsn;
            }
        });
    }

    /// Persist entries in a quorum of replicas.
    async fn persist_on_replicas(
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
            tx.send(PersistedMode::Replicas(false)).await.unwrap();
            return;
        }
        // Prevent concurrent replica changes.
        let replication_lock = self.replication_lock.clone().try_read_owned();
        if replication_lock.is_err() {
            // Return if replicas changing.
            tx.send(PersistedMode::Replicas(false)).await.unwrap();
            return;
        }

        let quorum_size: usize = num_total / 2 + 1;
        let client = self.direct_client.clone();
        let owner_id = self.owner_id;
        tokio::spawn(async move {
            // Prevent concurrent replica changes.
            let _replication_lock = replication_lock.unwrap();
            let mut num_completed: usize = 0;
            let mut num_valid: usize = 0;
            // Send to each replica.
            let (replica_tx, mut replica_rx) = mpsc::channel(6);
            for instance in instances {
                let entries = entries.clone();
                let client = client.clone();
                let replica_tx = replica_tx.clone();
                tokio::spawn(async move {
                    // Send request.
                    let meta = PersistenceReqMeta::Log {
                        lo_lsn,
                        hi_lsn,
                        persisted_lsn,
                        owner_id,
                        replica_id: instance.id.clone(),
                    };
                    let meta = serde_json::to_string(&meta).unwrap();
                    let resp = client
                        .post(&instance.url)
                        .header("obelisk-meta", meta)
                        .header("content-length", entries.len())
                        .body(entries)
                        .send()
                        .await;
                    // Parse resp.
                    if resp.is_err() {
                        println!("Resp err: {resp:?}");
                        replica_tx.send(false).await.unwrap();
                        return;
                    }
                    let resp = resp.unwrap();
                    let meta = resp.headers().get("obelisk-meta");
                    if meta.is_none() {
                        replica_tx.send(false).await.unwrap();
                        return;
                    }
                    let meta = meta.unwrap().to_str().unwrap();
                    let meta: PersistenceRespMeta = serde_json::from_str(meta).unwrap();
                    match meta {
                        PersistenceRespMeta::Ok => {
                            replica_tx.send(true).await.unwrap();
                            return;
                        }
                        PersistenceRespMeta::Terminating => {
                            replica_tx.send(false).await.unwrap();
                            return;
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
                let valid = replica_rx.recv().await.unwrap();
                num_completed += 1;
                if valid {
                    num_valid += 1;
                }
            }
            if num_valid >= quorum_size {
                failed_replications.store(0, atomic::Ordering::Relaxed);
                tx.send(PersistedMode::Replicas(true)).await.unwrap();
            } else {
                failed_replications.fetch_add(1, atomic::Ordering::Relaxed);
                tx.send(PersistedMode::Replicas(false)).await.unwrap();
            }
            while num_completed < num_total {
                let _ = replica_rx.recv().await.unwrap();
                num_completed += 1;
            }
        });
    }

    /// See the comments inside new_sync() for what this function does.
    async fn initialize(&self, old_instances: Vec<ServerfulInstance>) {
        // Assuming no more than AZ+1 uncontolled failures, when drain_instances is done,
        // All accepted writes have made it to disk.
        println!("Old Instances: {old_instances:?}");
        let mut persisted_lsn = self.fetch_persisted_lsn().await;
        let replicated_logs = self.drain_instances(&old_instances, persisted_lsn).await;
        if !replicated_logs.is_empty() {
            // Recover and refetch persistent lsn.
            self.recover_replicated_logs(replicated_logs).await;
            persisted_lsn = self.fetch_persisted_lsn().await;
            // Hacky way to remove replicated log entries.
            self.drain_instances(&old_instances, persisted_lsn).await;
        }
        // Set persisted lsn.
        {
            let mut inner = self.inner.lock().await;
            inner.flush_lsn = persisted_lsn;
            inner.curr_lsn = persisted_lsn;
            inner.persisted_lsn = persisted_lsn;
        }
        // Reset ownership of instances.
        self.change_instances(true).await;
    }

    /// Fetch current flush_lsn and curr_lsn.
    async fn fetch_persisted_lsn(&self) -> usize {
        // Read from database.
        let persisted_lsn: Option<usize> = tokio::task::block_in_place(move || {
            for _ in 0..NUM_DB_RETRIES {
                let conn = self.db.pool.get().unwrap();
                let persisted_lsn =
                    conn.query_row("SELECT MAX(hi_lsn) FROM system__logs", [], |r| r.get(0));
                println!("Persisted lsn: {persisted_lsn:?}");
                match persisted_lsn {
                    Ok(persisted_lsn) => {
                        return Some(persisted_lsn);
                    }
                    Err(rusqlite::Error::InvalidColumnType(_, _, rusqlite::types::Type::Null)) => {
                        return None;
                    }
                    _ => {
                        continue;
                    }
                }
            }
            eprintln!("Cannot access db. Fast exiting...");
            std::process::exit(1);
        });
        // Return.
        if let Some(persisted_lsn) = persisted_lsn {
            persisted_lsn
        } else {
            let inner = self.inner.lock().await;
            inner.start_lsn
        }
    }

    /// Change instances to replicate to.
    /// When set_ownership is set, will also set self as owner.
    async fn change_instances(&self, set_ownership: bool) {
        // Prevent concurrent changes.
        let mut _replication_lock = self.replication_lock.write().await;
        let curr_instances = {
            // No change if terminating.
            let inner = self.inner.lock().await;
            if inner.terminating {
                return;
            }
            inner.instances.clone()
        };
        let curr_instance_ids: HashSet<String> =
            curr_instances.iter().map(|s| s.id.clone()).collect();
        // If not setting ownership, only change ownership after multiple replication failures.
        if !set_ownership {
            let counter = self.failed_replications.load(atomic::Ordering::Relaxed);
            if counter < MAX_REPLICATION_FAILURES {
                return;
            }
            self.failed_replications.store(0, atomic::Ordering::Relaxed);
        }
        // Read instances.
        let mut new_instances = Vec::new();
        let mut all_instances = self.front_end.serverful_instances().await;
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
        // Drain instances until there are no pending logs.
        loop {
            let persisted_lsn = {
                let inner = self.inner.lock().await;
                inner.persisted_lsn
            };
            let pending_logs = self.drain_instances(&curr_instances, persisted_lsn).await;
            if pending_logs.is_empty() {
                break;
            } else {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
        // Write to db.
        let new_instances_str = serde_json::to_string(&new_instances).unwrap();
        tokio::task::block_in_place(move || {
            for _ in 0..NUM_DB_RETRIES {
                let conn = self.db.pool.get().unwrap();
                // Can only execute if no new owner since startup.
                let executed = conn.execute(
                    "UPDATE system__logs_ownership SET instances=?, owner_id=? WHERE new_owner_id=?",
                    rusqlite::params![new_instances_str, self.owner_id, self.owner_id],
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
            eprintln!("Cannot access db. Fast exiting...");
            std::process::exit(1);
        });
        let mut inner = self.inner.lock().await;
        inner.instances = new_instances;
        println!("Updated ownership");
    }

    /// Assuming at most AZ+1 uncontrolled crashes, this function makes sure that
    /// all acked writes will make to shared storage and that no previously active node can replicate new stuff.
    async fn drain_instances(
        &self,
        instances: &[ServerfulInstance],
        persisted_lsn: usize,
    ) -> Vec<(usize, usize, Vec<u8>)> {
        if instances.is_empty() {
            return vec![];
        }
        let mut reached_instances: HashSet<String> = HashSet::new();
        let mut all_entries: BTreeMap<usize, (usize, Vec<u8>)> = BTreeMap::new();
        let owner_id = self.owner_id;
        loop {
            let mut curr_instances = self.front_end.force_read_instances().await;
            common::adaptation::cleanup_instances(&mut curr_instances);
            let curr_instances = curr_instances.peers;
            println!("Draining Current: {curr_instances:?}");
            let mut remaining_instances: Vec<&ServerfulInstance> = Vec::new();
            for instance in instances {
                if curr_instances.contains_key(&instance.id)
                    && !reached_instances.contains(&instance.id)
                {
                    remaining_instances.push(instance);
                }
            }
            if remaining_instances.is_empty() {
                return all_entries
                    .into_iter()
                    .map(|(lo_lsn, (hi_lsn, entries))| (lo_lsn, hi_lsn, entries))
                    .collect();
            }
            // Contact all remaining instances.
            let (tx, mut rx) = mpsc::channel(remaining_instances.len());
            for instance in remaining_instances.iter() {
                let client = self.direct_client.clone();
                let instance_id = instance.id.clone();
                let url = if common::has_external_access() {
                    instance.url.clone()
                } else {
                    instance.private_url.clone()
                };
                let persisted_lsn = persisted_lsn;
                let tx = tx.clone();
                tokio::spawn(async move {
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
                        .header("content-length", req_body.len())
                        .body(req_body)
                        .send()
                        .await;
                    if resp.is_err() {
                        println!("Drain err: {resp:?}");
                        tx.send((instance_id, None)).await.unwrap();
                        return;
                    }
                    let resp = resp.unwrap();
                    let meta = resp.headers().get("obelisk-meta");
                    if meta.is_none() {
                        tx.send((instance_id, None)).await.unwrap();
                        return;
                    }
                    let meta = meta.unwrap().to_str().unwrap();
                    let meta: PersistenceRespMeta = serde_json::from_str(meta).unwrap();
                    println!("Resp: {meta:?}");
                    match meta {
                        PersistenceRespMeta::Ok => {
                            let resp_body = resp.bytes().await.unwrap().to_vec();
                            let pending_logs: Vec<PendingLog> =
                                bincode::deserialize(&resp_body).unwrap();
                            tx.send((instance_id, Some(pending_logs))).await.unwrap();
                        }
                        PersistenceRespMeta::Terminating => {
                            tx.send((instance_id, None)).await.unwrap();
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
                let (instance_id, pending_logs) = rx.recv().await.unwrap();
                let ok = if let Some(pending_logs) = pending_logs {
                    let pending_logs: Vec<PendingLog> = pending_logs;
                    for pending_log in pending_logs {
                        let PendingLog {
                            lo_lsn,
                            hi_lsn,
                            entries,
                        } = pending_log;
                        all_entries.insert(lo_lsn, (hi_lsn, entries));
                    }
                    true
                } else {
                    false
                };
                if ok {
                    reached_instances.insert(instance_id);
                }
                all_ok = all_ok && ok;
                num_received += 1;
            }
            if !all_ok {
                // Avoid putting too much pressure on dynamo and network.
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }

    async fn recover_replicated_logs(&self, replicated_entries: Vec<(usize, usize, Vec<u8>)>) {
        println!("Recover replicated logs: {replicated_entries:?}");
        if replicated_entries.is_empty() {
            return;
        }
        tokio::task::block_in_place(move || {
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
                    let mut stmt = match txn.prepare(
                        "REPLACE INTO system__logs(lo_lsn, hi_lsn, entries) VALUES (?, ?, ?)",
                    ) {
                        Ok(stmt) => stmt,
                        Err(x) => {
                            println!("Stmt: {x:?}");
                            continue;
                        }
                    };
                    let mut ok = true;
                    for (lo_lsn, hi_lsn, entries) in &replicated_entries {
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
        });
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
                plog.flush().await;
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
        plog.truncate(flush_lsn).await.unwrap();
        let mut num_entries = 0;
        let mut expected_entries = vec![];
        for i in 0..10 {
            for j in 0..10 {
                num_entries += 1;
                expected_entries.push(vec![i, j]);
                plog.enqueue(vec![i, j]).await;
            }
            plog.flush().await;
        }
        println!("Num entries: {num_entries}");
        let mut num_found_entries = 0;
        let mut curr_last_lsn = flush_lsn;
        loop {
            let entries = plog.replay(curr_last_lsn).await.unwrap();
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
        for i in 0..100 {
            let round = i + 1;
            println!("Round {round}/100");
            let failed_count = plog
                .failed_replications
                .load(std::sync::atomic::Ordering::Relaxed);
            println!("FailedCount={failed_count}");
            let start_time = std::time::Instant::now();
            for _ in 0..1000 {
                plog.enqueue(vec![1, 2, 3]).await;
                plog.flush().await;
            }
            let end_time = std::time::Instant::now();
            let duration = end_time.duration_since(start_time);
            println!("Flushes took: {duration:?}");
            tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
        }
        {
            let inner = plog.inner.lock().await;
            println!("Instances: {:?}", inner.instances);
        }
        plog.terminate().await;
        {
            let inner = plog.inner.lock().await;
            println!("Instances: {:?}", inner.instances);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn scaling_test() {
        run_scaling_test().await;
    }
}
