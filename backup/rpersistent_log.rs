use common::adaptation::frontend::AdapterFrontend;
use common::adaptation::ServerfulInstance;
use rocksdb::{WriteBatchWithTransaction, IteratorMode, ReadOptions};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::atomic;
use std::sync::{mpsc, Arc, Mutex};
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use super::{SUBSYSTEM_NAME, fast_serialize};

use crate::rdatabase::Database;
use crate::{RPersistenceReqMeta, PersistenceRespMeta, get_shared_log_db};
const ACTOR_LAMBDA_MODE: &str = "actor_function";
const MAX_REPLICATION_FAILURES: usize = 500;

/// A lockful implementation of persistent log.
/// Probably should not have more than two concurrent flushers.
#[derive(Clone)]
pub struct PersistentLog {
    inner: Arc<Mutex<PersistentLogInner>>,
    instance_ids_lock: Arc<Mutex<HashSet<String>>>,
    flush_lock: Arc<Mutex<()>>,
    failed_replications: Arc<atomic::AtomicUsize>,
    direct_client: reqwest::blocking::Client,
    front_end: Arc<AdapterFrontend>,
    pub db: Arc<rocksdb::OptimisticTransactionDB>,
    owner_id: usize,
    handle: tokio::runtime::Handle,
}

/// Modifyable internal state.
struct PersistentLogInner {
    curr_lsn: usize, // curr_lsn+1 is the lsn of the next enqueue.
    flush_lsn: usize, // Highest lsn known to be persisted or replicated.
    persisted_lsn: usize, // Highest lsn known to be persisted.
    start_lsn: usize, // Starting point of the logs given all the truncates.
    new_entries: Vec<(usize, Vec<u8>)>, // Enqueued entries.
    new_entries_size: usize, // Total size of enqueued entries.
    instances: Vec<ServerfulInstance>, // Instances to replicate to.
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
        // Open database.
        let mode = std::env::var("EXECUTION_MODE").unwrap_or("".into());
        let storage_dir = common::shared_storage_prefix();
        let storage_dir = format!("{storage_dir}/{SUBSYSTEM_NAME}/{namespace}/{name}/main");
        let db = Database::new(&storage_dir, mode != ACTOR_LAMBDA_MODE).db;
        // Read state of old owner.
        let txn = db.transaction();
        let owner_id = match txn.get(b"/system/owner_id") {
            Ok(Some(owner_id)) => {
                let owner_id = &owner_id[..];
                usize::from_be_bytes(owner_id.try_into().unwrap()) + 1
            },
            Ok(None) => {
                0
            },
            Err(x) => {
                println!("{x:?}. Exiting");
                std::process::exit(1);
            }
        };
        match txn.put(b"/system/owner_id", owner_id.to_be_bytes().to_vec()) {
            Ok(_) => {},
            Err(x) => {
                println!("{x:?}. Exiting");
                std::process::exit(1);
            }
        }
        let start_lsn = match txn.get(b"/system/start_lsn") {
            Ok(Some(start_lsn)) => {
                let start_lsn = &start_lsn[..];
                usize::from_be_bytes(start_lsn.try_into().unwrap()) + 1
            },
            Ok(None) => {
                0
            },
            Err(x) => {
                println!("{x:?}. Exiting");
                std::process::exit(1);
            }
        };
        let persisted_lsn = match txn.get(b"/system/persisted_lsn") {
            Ok(Some(persisted_lsn)) => {
                let persisted_lsn = &persisted_lsn[..];
                usize::from_be_bytes(persisted_lsn.try_into().unwrap()) + 1
            },
            Ok(None) => {
                0
            },
            Err(x) => {
                println!("{x:?}. Exiting");
                std::process::exit(1);
            }
        };
        let old_instances: Vec<ServerfulInstance> = match txn.get(b"/system/instances") {
            Ok(Some(instances)) => {
                serde_json::from_slice(&instances).unwrap()
            },
            Ok(None) => {
                vec![]
            },
            Err(x) => {
                println!("{x:?}. Exiting");
                std::process::exit(1);
            }
        };
        match txn.commit() {
            Ok(_) => {},
            Err(x) => {
                println!("{x:?}. Exiting");
                std::process::exit(1);
            }
        }
        println!("StartLsn={}; PersistedLsn={}", start_lsn, persisted_lsn);
        
        // At this point, the old owner cannot change the instances to replicate to.
        // However, those instances may have pending writes that were acked.
        // We need to make sure those instances are drained:
        // 1. They will accept no new writes from the old owner.
        // 2. Every write they have acked should make it to shared disk.
        // We also need to recover flush_lsn and curr_lsn.
        // The initialize function() does all of this.
        let inner = Arc::new(Mutex::new(PersistentLogInner {
            curr_lsn: persisted_lsn,
            flush_lsn: persisted_lsn,
            persisted_lsn,
            start_lsn,
            new_entries_size: 0,
            new_entries: Vec::new(),
            instances: Vec::new(),
        }));
        let front_end = handle.block_on(async move {
            AdapterFrontend::new(SUBSYSTEM_NAME, namespace, name).await
        });
        let plog = PersistentLog {
            inner,
            db,
            owner_id,
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
        };
        plog.initialize(namespace, name, persisted_lsn, old_instances);
        plog
    }

    /// Get current flush lsn.
    pub async fn get_flush_lsn(&self) -> usize {
        tokio::task::block_in_place(move || self.get_flush_lsn_sync())
    }

    /// Sync version of get_flush_lsn.
    fn get_flush_lsn_sync(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.flush_lsn
    }

    /// Enqueue one new element in the log.
    pub async fn enqueue(&self, content: Vec<u8>) -> usize {
        tokio::task::block_in_place(move || self.enqueue_sync(content))
    }

    /// Sync version of enqueue
    fn enqueue_sync(&self, content: Vec<u8>) -> usize {
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
    pub async fn flush(&self, ch: Option<oneshot::Sender<()>>) {
        tokio::task::block_in_place(move || {
            self.flush_sync(ch);
        })
    }

    /// Sync version of flush.
    fn flush_sync(&self, ch: Option<oneshot::Sender<()>>) {
        // Only allow one flush at a time, but also allow enqueues while flushing.
        // Using the channel, caller can know that flushing has started and keep enqueuing.
        let _ = self.flush_lock.lock().unwrap();
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
        self.flush_entries(entries, entries_size, persisted_lsn, instances);
        {
            let mut inner = self.inner.lock().unwrap();
            inner.flush_lsn = curr_lsn;
        }
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
        let entries = Arc::new(entries);
        // Send to database and to replicas.
        let (tx, rx) = mpsc::channel();
        {
            let tx = tx.clone();
            let entries = entries.clone();
            self.persist_on_db(tx, entries.clone());
        };
        {
            let tx = tx.clone();
            let entries = entries.clone();
            self.persist_on_replicas(tx, entries, entries_size, persisted_lsn, instances);
        };
        // Wait for persistence.
        let persisted = rx.recv().unwrap();
        let failed_counter = self.failed_replications.clone();
        match persisted {
            PersistedMode::Database => {
                // On db, persistence is guaranteed. So async recv remaining.
                std::thread::spawn(move || {
                    let persisted = rx.recv().unwrap();
                    if let PersistedMode::Replicas(valid) = persisted {
                        if valid {
                            failed_counter.store(0, atomic::Ordering::Relaxed);
                        } else {
                            failed_counter.fetch_add(1, atomic::Ordering::Relaxed);
                        }
                    }
                });
            }
            PersistedMode::Replicas(replicated) => {
                if replicated {
                    failed_counter.store(0, atomic::Ordering::Relaxed);
                    // When replicated on a quorum, persistence is guaranteed. So async drain.
                    std::thread::spawn(move || {
                        let _persisted = rx.recv().unwrap();
                    });
                } else {
                    // Without a quorum, we have to wait for database.
                    failed_counter.fetch_add(1, atomic::Ordering::Relaxed);
                    let _persisted = rx.recv().unwrap();
                    // println!("{_persisted:?}");
                }
            }
        }
    }

    /// Truncate up to (and including) at most the given lsn.
    /// Only call this once absolutely certain 
    pub async fn truncate(&self, lsn: usize) {
        tokio::task::block_in_place(move || self.truncate_sync(lsn))
    }

    /// Sync version of truncate.
    pub fn truncate_sync(&self, lsn: usize) {
        let curr_start_lsn = {
            let inner = self.inner.lock().unwrap();
            inner.start_lsn
        };
        if curr_start_lsn <= lsn {
            return;
        }
        let txn = self.db.transaction();
        let is_owner = match txn.get(b"/system/owner_id") {
            Ok(Some(owner_id)) => {
                let owner_id = &owner_id[..];
                let owner_id = usize::from_be_bytes(owner_id.try_into().unwrap());
                owner_id == self.owner_id
            },
            _ => {
                false
            }
        };
        if !is_owner {
            println!("Not owner anymore! Exiting.");
            std::process::exit(1);
        }
        match txn.put(b"/system/start_lsn", lsn.to_be_bytes().to_vec()) {
            Ok(_) => {},
            Err(x) => {
                println!("{x:?}. Exiting.");
               std::process::exit(1);
            }
        }
        

        let lsn_prefix = b"/system/log/".to_vec();
        let mut start_lsn_key = lsn_prefix.clone();
        start_lsn_key.extend(lsn.to_be_bytes().into_iter());
        let mode = IteratorMode::From(&start_lsn_key, rocksdb::Direction::Reverse);
        let it = self.db.iterator(mode);
        for kv in it {
            if let Ok((lsn_key, _)) = kv {
                match txn.delete(lsn_key.to_vec()) {
                    Ok(_) => {},
                    Err(x) => {
                        println!("{x:?}. Exiting.");
                        std::process::exit(1);
                    }
                }
            } else {
                eprintln!("Cannot access db: {kv:?}");
                std::process::exit(1);
            }
        }
        match txn.commit() {
            Ok(_) => {},
            Err(x) => {
                eprintln!("Cannot access db: {x:?}");
                std::process::exit(1);
            }
        }
        let mut inner = self.inner.lock().unwrap();
        // Concurrent truncate may change value.
        if inner.start_lsn < lsn {
            inner.start_lsn = lsn;
        }
    }

    /// Replay log entries, possibly starting from a given lsn.
    /// Will return log entries in flush batches. 
    /// So use exclusive_start_lsn to get values starting from a given lsn (can be 0).
    /// When called concurrently with flush(), may miss some last few entries
    /// Returns Vec<(lsn, log)>.
    pub async fn replay(&self, exclusive_start_lsn: usize, max_num_records: usize) -> Vec<(usize, Vec<u8>)> {
        tokio::task::block_in_place(move || self.replay_sync(exclusive_start_lsn, max_num_records))
    }

    /// Sync version of replay.
    pub fn replay_sync(&self, exclusive_start_lsn: usize, max_num_records: usize) -> Vec<(usize, Vec<u8>)> {
        let exclusive_start_lsn = {
            let inner = self.inner.lock().unwrap();
            if inner.start_lsn > exclusive_start_lsn {
                inner.start_lsn
            } else {
                exclusive_start_lsn
            }
        };
        let lsn_prefix = b"/system/log/".to_vec();
        let mut start_lsn_key = lsn_prefix.clone();
        start_lsn_key.extend(exclusive_start_lsn.to_be_bytes().into_iter());
        let mode = IteratorMode::From(&start_lsn_key, rocksdb::Direction::Forward);
        let mut res = Vec::new();
        let it = self.db.iterator(mode);
        for kv in it {
            if let Ok((lsn_key, entry)) = kv {
                let lsn = &lsn_key[lsn_prefix.len()..];
                let lsn = usize::from_be_bytes(lsn.try_into().unwrap());
                if lsn > exclusive_start_lsn {
                    let entries = entry.to_vec();
                    res.push((lsn, entries));
                    if res.len() >= max_num_records {
                        return res;
                    }
                }
            } else {
                eprintln!("Cannot access db: {kv:?}");
                std::process::exit(1);
            }
        }
        return res;
    }

    /// Call this to allow faster recovery.
    /// Will block every other operation.
    pub async fn terminate(&self) {
        tokio::task::block_in_place(move || {
            let _ = self.flush_lock.lock();
            let _ = self.instance_ids_lock.lock();
            let inner = self.inner.lock().unwrap();
            if inner.instances.is_empty() {
                return;
            }
            let persisted_lsn = inner.persisted_lsn;
            self.drain_instances(&inner.instances, persisted_lsn);
            let new_instances: Vec<ServerfulInstance> = Vec::new();
            let new_instances_str = serde_json::to_string(&new_instances).unwrap();
            let txn = self.db.transaction();
            let is_owner = match txn.get(b"/system/owner_id") {
                Ok(Some(owner_id)) => {
                    let owner_id = &owner_id[..];
                    let owner_id = usize::from_be_bytes(owner_id.try_into().unwrap());
                    owner_id == self.owner_id
                },
                _ => {
                    false
                }
            };
            if !is_owner {
                println!("Not owner anymore! Exiting.");
                std::process::exit(1);
            }
            match txn.put(b"/system/instances", new_instances_str.as_bytes()) {
                Ok(_) => {},
                Err(x) => {
                    println!("{x:?}");
                    std::process::exit(1);
                }
            }
            match txn.commit() {
                Ok(_) => {},
                Err(x) => {
                    println!("{x:?}");
                    std::process::exit(1);
                }
            }
        });
    }

    /// Persist entries in DB.
    fn persist_on_db(
        &self,
        tx: mpsc::Sender<PersistedMode>,
        entries: Arc<Vec<(usize, Vec<u8>)>>,
    ) {
        let db = self.db.clone();
        let my_owner_id = self.owner_id;
        let inner = self.inner.clone(); // To modify persisted lsn.
        std::thread::spawn(move || {
            let txn = db.transaction();
            let is_owner = match txn.get(b"/system/owner_id") {
                Ok(Some(owner_id)) => {
                    let owner_id = &owner_id[..];
                    let owner_id = usize::from_be_bytes(owner_id.try_into().unwrap());
                    owner_id == my_owner_id
                },
                _ => {
                    false
                }
            };
            if !is_owner {
                println!("Not owner anymore! Exiting.");
                std::process::exit(1);
            }
            let lsn_prefix = b"/system/log/".to_vec();
            for (lsn, entry) in entries.iter() {
                let mut lsn_key = lsn_prefix.clone();
                lsn_key.extend(lsn.to_be_bytes().into_iter());
                match txn.put(lsn_key, entry) {
                    Ok(_) => {},
                    Err(x) => {
                        println!("{x:?}");
                        std::process::exit(1);
                    }
                }
            }
            let last_lsn = entries.last().unwrap().0;
            match txn.put(b"/system/persisted_lsn", last_lsn.to_be_bytes().to_vec()) {
                Ok(_) => {},
                Err(x) => {
                    println!("{x:?}");
                    std::process::exit(1);
                }
            }
            match txn.commit() {
                Ok(_) => {},
                Err(x) => {
                    println!("{x:?}");
                    std::process::exit(1);
                }
            }
            {
                let mut inner = inner.lock().unwrap();
                inner.persisted_lsn = last_lsn;
            }
            tx.send(PersistedMode::Database).unwrap();
        });
    }

    /// Persist entries in a quorum of replicas.
    fn persist_on_replicas(
        &self,
        tx: mpsc::Sender<PersistedMode>,
        entries: Arc<Vec<(usize, Vec<u8>)>>,
        entries_size: usize,
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
            // Serialize
            let entries = fast_serialize(&entries, entries_size);
            let entries = bytes::Bytes::from(entries);
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
                    let meta = RPersistenceReqMeta::Log {
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
                    let meta: PersistenceRespMeta = serde_json::from_str(&meta).unwrap();
                    match meta {
                        PersistenceRespMeta::Ok => {
                            failed_replications.store(0, atomic::Ordering::Relaxed);
                            replica_tx.send(true).unwrap();
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
    fn initialize(&self, namespace: &str, name: &str, persisted_lsn: usize, old_instances: Vec<ServerfulInstance>) {
        self.initialize_shared_ownership(namespace, name);
        // Assuming no more than AZ+1 uncontolled failures, when drain_instances is done,
        // All accepted writes have made it to disk.
        if !old_instances.is_empty() {
            self.drain_instances(&old_instances, persisted_lsn);
        }
        // When update shared_ownership is done, we can assume that even single replicas
        // Stuck in a partition with old owner cannot write to shared storage.
        println!("Update shared ownership!");
        self.update_shared_ownership(namespace, name);
        // It's safe to take the shared logs.
        println!("Take shared logs!");
        let shared_entries = self.take_shared_logs(namespace, name, persisted_lsn);
        println!("Apply shared logs!");
        self.apply_shared_logs(shared_entries);
        // It's safe to take ownership with possibly new list of replicas.
        println!("Change lsns!");
        self.change_instances(true);
    }

    /// Change instances to replicate to.
    /// When set_ownership is set, will also set self as owner.
    fn change_instances(&self, set_ownership: bool) {
        // Prevent concurrent changes.
        let mut curr_instance_ids = self.instance_ids_lock.lock().unwrap();
        if !set_ownership {
            let counter = self.failed_replications.load(atomic::Ordering::Relaxed);
            if counter < MAX_REPLICATION_FAILURES {
                return;
            }
            self.failed_replications.store(0, atomic::Ordering::Relaxed);
        }
        let mut new_instances = Vec::new();
        let mut all_instances = self.handle.block_on(async {
            self.front_end.serverful_instances().await
        });
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
        // In the worst case, we may settle with 3 nodes across 3 AZs.
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
        if !is_safe {
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
        let txn = self.db.transaction();
        let owner_id = match txn.get(b"/system/owner_id") {
            Ok(owner_id) => {
                let owner_id = &owner_id.unwrap()[..];
                usize::from_be_bytes(owner_id.try_into().unwrap())
            },
            Err(x) => {
                println!("{x:?}. Exiting.");
                std::process::exit(1);
            }
        };
        if owner_id != self.owner_id {
            println!("Log owner changed. Exiting.");
            std::process::exit(1);
        }
        match txn.put(b"/system/instances", new_instances_str.as_bytes()) {
            Ok(_) => {},
            Err(x) => {
                println!("{x:?}. Exiting.");
                std::process::exit(1);
            }
        };
        match txn.commit() {
            Ok(_) => {},
            Err(x) => {
                println!("{x:?}. Exiting");
                std::process::exit(1);
            }
        }
        let mut inner = self.inner.lock().unwrap();
        *curr_instance_ids = new_instance_ids;
        inner.instances = new_instances;
        println!("Updated ownership");
    }


    /// Assuming at most AZ+1 uncontrolled crashes, this function makes sure that
    /// all acked writes will make to shared storage and that no previously active node can replicate new stuff.
    fn drain_instances(&self, instances: &[ServerfulInstance], persisted_lsn: usize) {
        if instances.is_empty() {
            return;
        }
        let mut reached_instances: HashSet<String> = HashSet::new();
        let owner_id = self.owner_id;
        loop {
            let curr_instances = self.handle.block_on(async {
                self.front_end.force_read_instances().await.peers
            });
            let mut remaining_instances: Vec<&ServerfulInstance> = Vec::new();
            for instance in instances {
                if curr_instances.contains_key(&instance.id) && !reached_instances.contains(&instance.id) {
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
                let tx = tx.clone();
                std::thread::spawn(move || {
                    let req_body: Vec<u8> = Vec::new();
                    let req_meta = RPersistenceReqMeta::Drain {
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
                    let meta: PersistenceRespMeta = serde_json::from_str(&meta).unwrap();
                    match meta {
                        PersistenceRespMeta::Ok => {
                            tx.send((instance_id, true)).unwrap();
                        },
                        PersistenceRespMeta::Outdated => {
                            eprintln!("A new owner has contacted replicas. Fast exiting...");
                            std::process::exit(1);
                        },
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

    fn apply_shared_logs(&self, shared_entries: Vec<(usize, Vec<u8>, Vec<u8>)>) {
        let mut batch = WriteBatchWithTransaction::<true>::default();
        let mut max_lsn: usize = 0;
        for (lsn, lsn_key, entry) in shared_entries {
            batch.put(lsn_key, entry);
            max_lsn = lsn;
        }
        match self.db.write(batch) {
            Ok(_) => {},
            Err(x) => {
                println!("{x:?}");
                std::process::exit(1);
            }
        }
        let mut inner = self.inner.lock().unwrap();
        if max_lsn > inner.persisted_lsn {
            inner.persisted_lsn = max_lsn;
            inner.curr_lsn = max_lsn;
            inner.flush_lsn = max_lsn;
        }
    }

    fn take_shared_logs(&self, namespace: &str, name: &str, persisted_lsn: usize) -> Vec<(usize, Vec<u8>, Vec<u8>)> {
        let db = get_shared_log_db(namespace, name).db;
        let prefix = b"/system/log/".to_vec();
        let mode = IteratorMode::From(&prefix, rocksdb::Direction::Forward);
        let mut ropt = ReadOptions::default();
        ropt.set_prefix_same_as_start(true);

        let it = db.iterator_opt(mode, ropt);
        let mut res = Vec::new();
        for elem in it {
            if let Ok((lsn_key, entry)) = elem {
                if lsn_key.starts_with(&prefix) {
                    println!("LSN KEY: {:?}", String::from_utf8(lsn_key.to_vec()).unwrap());
                    let lsn = &lsn_key[prefix.len()..];
                    let lsn = usize::from_be_bytes(lsn.try_into().unwrap());
                    if lsn > persisted_lsn {
                        let entries = entry.to_vec();
                        res.push((lsn, lsn_key.to_vec(), entries));
                    }    
                }
            } else {
                println!("Unexpected error while reading shared log!");
                std::process::exit(1);
            }
        }
        res
    }

    fn initialize_shared_ownership(&self, namespace: &str, name: &str) {
        let db = get_shared_log_db(namespace, name).db;
        let txn = db.transaction();
        let owner_id = match txn.get(b"/system/owner_id") {
            Ok(Some(owner_id)) => {
                owner_id
            },
            Ok(None) => {
                vec![]
            },
            Err(x) => {
                println!("{x:?}. Exiting");
                std::process::exit(1);
            }
        };
        if owner_id.is_empty() {
            let _ = match txn.put(b"/system/owner_id", (0 as usize).to_be_bytes().to_vec()) {
                Ok(_) => {},
                Err(x) => {
                    println!("{x:?}. Exiting");
                    std::process::exit(1);
                }
            };
        }
        match txn.commit() {
            Ok(_) => {},
            Err(x) => {
                println!("{x:?}. Exiting");
                std::process::exit(1);
            }
        }
        
    }

    fn update_shared_ownership(&self, namespace: &str, name: &str) {
        let db = get_shared_log_db(namespace, name).db;
        let txn = db.transaction();
        let owner_id = match txn.get(b"/system/owner_id") {
            Ok(Some(owner_id)) => {
                let owner_id = &owner_id[..];
                usize::from_be_bytes(owner_id.try_into().unwrap()) + 1
            },
            Ok(None) => {
                0
            },
            Err(x) => {
                println!("{x:?}. Exiting");
                std::process::exit(1);
            }
        }; 
        if owner_id > self.owner_id {
            println!("Newer owner {owner_id} > {}. Exiting...", self.owner_id);
            std::process::exit(1);
        }
        let _ = match txn.put(b"/system/owner_id", self.owner_id.to_be_bytes().to_vec()) {
            Ok(_) => {},
            Err(x) => {
                println!("{x:?}. Exiting");
                std::process::exit(1);
            }
        };
        match txn.commit() {
            Ok(_) => {},
            Err(x) => {
                println!("{x:?}. Exiting");
                std::process::exit(1);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::oneshot;
    use std::sync::Arc;
    use super::PersistentLog;

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
        let num_writes = 100_000;
        let start_time = std::time::Instant::now();
        for i in 0..num_writes {
            plog.enqueue(content.clone()).await;
            if (i + 1) % 1000 == 0 {
                plog.flush(None).await;
            }
        }
        let end_time = std::time::Instant::now();
        let duration = end_time.duration_since(start_time);
        println!("Logging took: {duration:?}");
    }
}
