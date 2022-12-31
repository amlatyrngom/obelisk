use fslock::LockFile;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use std::sync::{Arc, Mutex};

/// Time between retries.
/// There is already timeout of ~1s for connecting, so this is additive.
const RETRY_INTERVAL_MS: u64 = 500;
/// Maximum number of retries.
const MAX_NUM_RETRIES: u64 = 30;

#[derive(Clone)]
pub struct Database {
    pub pool: Pool<SqliteConnectionManager>,
    pub db_id: usize,
    pub lock_file_path: String,
    lock: Arc<Mutex<Option<LockFile>>>,
}

impl Database {
    /// Opening a database, or exits if the lock is already owned.
    /// If the the retry flag is set, will retry for a while and then exit.
    pub fn new(storage_dir: &str, filename: &str, with_retry: bool) -> Database {
        let create_dir_resp = std::fs::create_dir_all(storage_dir);
        println!("Create Dir ({storage_dir}) Resp: {create_dir_resp:?}");
        let db_file = format!("{storage_dir}/{filename}.db");
        let lock_file_path = format!("{storage_dir}/{filename}.lock");
        // Lock.
        let max_num_tries = if with_retry { MAX_NUM_RETRIES } else { 1 };
        for n in 0..max_num_tries {
            if n > 0 {
                std::thread::sleep(std::time::Duration::from_millis(RETRY_INTERVAL_MS));
            }
            let mut lock_file = match LockFile::open(&lock_file_path) {
                Ok(lock_file) => lock_file,
                Err(x) => {
                    println!("{x:?}");
                    continue;
                }
            };
            let has_lock = match lock_file.try_lock() {
                Ok(has_lock) => has_lock,
                Err(x) => {
                    println!("{x:?}");
                    continue;
                }
            };
            if !has_lock {
                println!("Process {}. File lock not acquired.!", std::process::id());
                continue;
            }
            println!("Process {}. File lock acquired.!", std::process::id());
            let manager = r2d2_sqlite::SqliteConnectionManager::file(&db_file);
            let pool = match r2d2::Pool::builder().max_size(10).build(manager) {
                Ok(pool) => pool,
                Err(x) => {
                    println!("{x:?}");
                    continue;
                }
            };
            let mut conn = match pool.get() {
                Ok(conn) => conn,
                Err(x) => {
                    println!("{x:?}");
                    continue;
                }
            };
            match conn.busy_timeout(std::time::Duration::from_secs(1)) {
                Ok(_) => {}
                Err(x) => {
                    println!("{x:?}");
                    continue;
                }
            }
            let txn = match conn.transaction() {
                Ok(txn) => txn,
                Err(x) => {
                    println!("{x:?}");
                    continue;
                }
            };
            match txn.execute("CREATE TABLE IF NOT EXISTS system__ownership (unique_row INTEGER PRIMARY KEY, db_id INT)", ()) {
                Ok(_) => {},
                Err(x) => {
                    println!("{x:?}");
                    continue;
                }
            }
            match txn.execute("CREATE TABLE IF NOT EXISTS system__ownership (unique_row INTEGER PRIMARY KEY, db_id INT)", ()) {
                Ok(_) => {},
                Err(x) => {
                    println!("{x:?}");
                    continue;
                }
            }
            let db_id = txn.query_row("SELECT db_id FROM system__ownership", [], |r| r.get(0));
            let db_id = db_id.unwrap_or(0 as usize) + 1;
            match txn.execute(
                "REPLACE INTO system__ownership (unique_row, db_id) VALUES (0, ?)",
                [&db_id],
            ) {
                Ok(_) => {}
                Err(x) => {
                    println!("{x:?}");
                    continue;
                }
            };
            match txn.commit() {
                Ok(_) => {
                    println!("Successfully opened database!");
                    return Database {
                        pool,
                        db_id,
                        lock_file_path,
                        lock: Arc::new(Mutex::new(Some(lock_file))),
                    };
                }
                _ => {
                    continue;
                }
            }
        }
        eprintln!(
            "Could not acquire database lock. Pid {}.",
            std::process::id()
        );
        std::process::exit(1);
    }

    /// Use to release lambda's lock on database.
    pub async fn release(&self) {
        let mut lock = self.lock.lock().unwrap();
        *lock = None;
    }

    /// Use to acquire lambda's lock.
    pub async fn acquire(&self) -> bool {
        let mut lock = self.lock.lock().unwrap();
        if lock.is_some() {
            return true;
        }
        let mut lock_file = match LockFile::open(&self.lock_file_path) {
            Ok(lock_file) => lock_file,
            Err(x) => {
                println!("{x:?}");
                return false;
            }
        };
        let has_lock = match lock_file.try_lock() {
            Ok(has_lock) => has_lock,
            Err(x) => {
                println!("{x:?}");
                return false;
            }
        };
        if !has_lock {
            false
        } else {
            *lock = Some(lock_file);
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Database;
    use fork::{fork, Fork};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::RwLock;

    #[test]
    fn bench_map() {
        let mut kvs: HashMap<String, String> = HashMap::new();
        let num_ops = 1000000;
        let start_time = std::time::Instant::now();
        for i in 0..num_ops {
            let k = format!("key{i}");
            let v = format!("val{i}");
            kvs.insert(k, v);
        }
        let end_time = std::time::Instant::now();
        let duration = end_time.duration_since(start_time);
        println!("Writes took: {duration:?}");
        let start_time = std::time::Instant::now();
        for i in 0..num_ops {
            let k = format!("key{i}");
            let _ = kvs.get(&k).unwrap();
        }
        let end_time = std::time::Instant::now();
        let duration = end_time.duration_since(start_time);
        println!("Reads took: {duration:?}");
        let kvs: HashMap<String, String> = HashMap::new();
        let kvs = Arc::new(RwLock::new(kvs));
        let num_threads = 4;
        let num_ops = num_ops / num_threads;
        let mut ts = Vec::new();
        let start_time = std::time::Instant::now();
        for n in 0..num_threads {
            let kvs = kvs.clone();
            ts.push(std::thread::spawn(move || {
                let lo = n * num_ops;
                let hi = lo + num_ops;
                println!("{}", lo - hi);
                for i in lo..hi {
                    let mut kvs = kvs.write().unwrap();
                    let k = format!("key{i}");
                    let v = format!("val{i}");
                    kvs.insert(k, v);
                }
            }));
        }
        for t in ts {
            t.join().unwrap();
        }
        let end_time = std::time::Instant::now();
        let duration = end_time.duration_since(start_time);
        println!("MT Writes took: {duration:?}");
        let (tx, rx) = std::sync::mpsc::channel();
        let mut kvs: HashMap<String, String> = HashMap::new();
        std::thread::spawn(move || {
            for (key, value, ch) in rx {
                let ch: std::sync::mpsc::Sender<()> = ch;
                kvs.insert(key, value);
                ch.send(()).unwrap();
            }
            println!("Main thread exiting")
        });
        let (resp_tx, resp_rx) = std::sync::mpsc::channel();
        let start_time = std::time::Instant::now();
        for i in 0..num_ops {
            let k = format!("key{i}");
            let v = format!("val{i}");
            tx.send((k, v, resp_tx.clone())).unwrap();
            let _: () = resp_rx.recv().unwrap();
        }
        let end_time = std::time::Instant::now();
        let duration = end_time.duration_since(start_time);
        println!("Channel Writes took: {duration:?}");
    }

    #[test]
    fn test_database() {
        match fork() {
            Ok(Fork::Parent(child1)) => {
                match fork() {
                    Ok(Fork::Parent(child2)) => {
                        {
                            // Open a database and write a few values.
                            let db = Database::new(&common::shared_storage_prefix(), "test", false);
                            println!("Parent: {}", db.db_id);
                            // Wait for child1 to crash.
                            std::thread::sleep(std::time::Duration::from_secs(2));
                            // Child1 must have exited with a status of 1.
                            let child_status = nix::sys::wait::waitpid(
                                Some(nix::unistd::Pid::from_raw(child1)),
                                None,
                            )
                            .unwrap();
                            assert!(
                                matches!(child_status, nix::sys::wait::WaitStatus::Exited(_, i) if i == 1)
                            );
                        }
                        // Wait for child2 to open and cleanly exit.
                        std::thread::sleep(std::time::Duration::from_secs(2));
                        let child_status =
                            nix::sys::wait::waitpid(Some(nix::unistd::Pid::from_raw(child2)), None)
                                .unwrap();
                        assert!(
                            matches!(child_status, nix::sys::wait::WaitStatus::Exited(_, i) if i == 0)
                        );
                    }
                    Ok(Fork::Child) => {
                        // Wait for parent to open db.
                        std::thread::sleep(std::time::Duration::from_secs(1));
                        // Unlike the other child, retrying ensures this does not crash.
                        let db = Database::new(&common::shared_storage_prefix(), "test", true);
                        println!("Child 2: {}", db.db_id); // Should not be reached.
                        std::process::exit(0);
                    }
                    Err(_) => println!("Fork failed"),
                }
            }
            Ok(Fork::Child) => {
                // Wait for parent to open db.
                std::thread::sleep(std::time::Duration::from_secs(1));
                // Should crash trying to open database.
                let db = Database::new(&common::shared_storage_prefix(), "test", false);
                println!("Child 1: {}", db.db_id); // Should not be reached.
            }
            Err(_) => println!("Fork failed"),
        }
    }
}
