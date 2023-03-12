use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use std::sync::{atomic, Arc};

/// Time between retries.
const MAX_NUM_RETRIES: usize = 100;
const MAX_NUM_RETRIES_LAMBDA: usize = 1;
const BUSY_TIMEOUT_MS: u64 = 300;

#[derive(Clone)]
pub struct Database {
    pub pool: Pool<SqliteConnectionManager>,
    pub db_id: usize,
    pub acquired: Arc<atomic::AtomicBool>,
}

impl Database {
    /// Opening a database, or exits if the lock is already owned.
    /// If the the retry flag is set, will retry for a while and then exit.
    pub fn new(storage_dir: &str, filename: &str, with_retry: bool) -> Result<Self, String> {
        let create_dir_resp = std::fs::create_dir_all(storage_dir);
        println!("Create Dir ({storage_dir}) Resp: {create_dir_resp:?}");
        let db_file = format!("{storage_dir}/{filename}.db");
        // Lock.
        for n in 0..Self::get_num_retries() {
            if n > 0 && !with_retry {
                break;
            }
            let manager = r2d2_sqlite::SqliteConnectionManager::file(db_file.clone());
            let pool = match r2d2::Pool::builder().max_size(1).build(manager) {
                Ok(pool) => pool,
                Err(x) => {
                    println!("Builder: {x:?}");
                    continue;
                }
            };
            let mut conn = match pool.get_timeout(std::time::Duration::from_secs(1)) {
                Ok(conn) => conn,
                Err(x) => {
                    println!("Conn: {x:?}");
                    continue;
                }
            };
            match conn.busy_timeout(std::time::Duration::from_millis(BUSY_TIMEOUT_MS)) {
                Ok(_) => {}
                Err(x) => {
                    println!("Setting Timeout: {x:?}");
                    continue;
                }
            }
            match conn.pragma_update(None, "locking_mode", "exclusive") {
                Ok(_) => {}
                Err(x) => {
                    println!("Pragma 1: {x:?}");
                    continue;
                }
            }
            match conn.pragma_update(None, "journal_mode", "wal") {
                Ok(_) => {}
                Err(x) => {
                    println!("Pragma 2: {x:?}");
                    continue;
                }
            }
            let txn = match conn.transaction() {
                Ok(txn) => txn,
                Err(x) => {
                    println!("Txn 1: {x:?}");
                    continue;
                }
            };
            match txn.execute("CREATE TABLE IF NOT EXISTS system__ownership (unique_row INTEGER PRIMARY KEY, db_id INT)", ()) {
                Ok(_) => {},
                Err(x) => {
                    println!("Txn 2: {x:?}");
                    continue;
                }
            }
            match txn.execute("CREATE TABLE IF NOT EXISTS system__ownership (unique_row INTEGER PRIMARY KEY, db_id INT)", ()) {
                Ok(_) => {},
                Err(x) => {
                    println!("Txn 3: {x:?}");
                    continue;
                }
            }
            let db_id = txn.query_row("SELECT db_id FROM system__ownership", [], |r| r.get(0));
            let db_id = db_id.unwrap_or(0_usize) + 1;
            match txn.execute(
                "REPLACE INTO system__ownership (unique_row, db_id) VALUES (0, ?)",
                [&db_id],
            ) {
                Ok(_) => {}
                Err(x) => {
                    println!("Txn 4: {x:?}");
                    continue;
                }
            };
            match txn.commit() {
                Ok(_) => {
                    println!("Successfully opened database!");
                    let acquired = Arc::new(atomic::AtomicBool::new(true));
                    return Ok(Database {
                        pool,
                        db_id,
                        acquired,
                    });
                }
                _ => {
                    continue;
                }
            }
        }
        Err(format!(
            "Could not acquire database lock. Pid {}.",
            std::process::id()
        ))
    }

    /// Return number of attempts to make.
    fn get_num_retries() -> usize {
        let mode = std::env::var("EXECUTION_MODE").unwrap_or_else(|_| "messaging_lambda".into());
        if mode == "messaging_lambda" {
            MAX_NUM_RETRIES_LAMBDA
        } else {
            MAX_NUM_RETRIES
        }
    }

    /// Sync version of release.
    pub fn release_sync(&self) {
        self.acquired.store(false, atomic::Ordering::Release);
        let conn = loop {
            match self.pool.get() {
                Ok(conn) => break conn,
                Err(x) => {
                    println!("{x:?}");
                    continue;
                }
            };
        };

        match conn.pragma_update(None, "journal_mode", "delete") {
            Ok(_) => {}
            Err(x) => {
                println!("Cannot release: {x:?}");
                std::process::exit(1);
            }
        }
        match conn.pragma_update(None, "locking_mode", "normal") {
            Ok(_) => {}
            Err(x) => {
                println!("Cannot release: {x:?}");
                std::process::exit(1);
            }
        }
        // Actually release lock in sqlite.
        match conn.query_row("SELECT unique_row FROM system__ownership", [], |r| r.get(0)) {
            Ok(res) => {
                let _res: usize = res;
            }
            Err(x) => {
                println!("Cannot release: {x:?}");
                std::process::exit(1);
            }
        }
    }

    /// Use to release lambda's lock on database.
    pub async fn release(&self) {
        tokio::task::block_in_place(move || self.release_sync())
    }

    /// Sync version of acquire.
    pub fn acquire_sync(&self, retry: bool) -> bool {
        let conn = loop {
            match self.pool.get() {
                Ok(conn) => break conn,
                Err(x) => {
                    println!("{x:?}");
                    continue;
                }
            };
        };
        for n in 0..Self::get_num_retries() {
            if n > 0 && !retry {
                return false;
            }
            match conn.pragma_update(None, "locking_mode", "exclusive") {
                Ok(_) => {}
                Err(x) => {
                    println!("Cannot acquire yet: {x:?}");
                    continue;
                }
            }
            match conn.pragma_update(None, "journal_mode", "wal") {
                Ok(_) => {}
                Err(x) => {
                    println!("Cannot acquire yet: {x:?}");
                    continue;
                }
            }

            // Actually acquire lock in sqlite.
            match conn.execute("UPDATE system__ownership SET unique_row=0", []) {
                Ok(res) => {
                    let _res: usize = res;
                    self.acquired.store(true, atomic::Ordering::Release);
                    return true;
                }
                Err(x) => {
                    println!("Cannot acquire yet: {x:?}");
                    continue;
                }
            }
        }
        println!("Could not acquire lock!");
        false
    }

    /// Use to acquire lambda's lock.
    pub async fn acquire(&self, retry: bool) -> bool {
        tokio::task::block_in_place(move || self.acquire_sync(retry))
    }

    /// Check if db is acquired for background tasks.
    pub async fn is_acquired(&self) -> bool {
        self.acquired.load(atomic::Ordering::Acquire)
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
        let test_release = true;
        match fork() {
            Ok(Fork::Parent(child1)) => {
                match fork() {
                    Ok(Fork::Parent(child2)) => {
                        let _parent_db: Option<Database> = {
                            // Open a database and write a few values.
                            let db = Database::new(&common::shared_storage_prefix(), "test", false);
                            assert!(db.is_ok());
                            let db = db.unwrap();
                            println!("Parent: {}", db.db_id);
                            if test_release {
                                // Fake release and reaquire.
                                db.release_sync();
                                db.acquire_sync(true);
                            }
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
                            if test_release {
                                // Test releasing the database.
                                db.release_sync();
                                Some(db)
                            } else {
                                // Test dropping the connection.
                                None
                            }
                        };
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
                        // Wait for parent and child1 to open db.
                        std::thread::sleep(std::time::Duration::from_secs(2));
                        // Unlike the other child, retrying ensures this does not crash.
                        let db = Database::new(&common::shared_storage_prefix(), "test", true);
                        assert!(db.is_ok());
                        println!("Child 2: Err({:?}, should be None)", db.clone().err());
                        std::process::exit(db.is_err() as i32);
                    }
                    Err(_) => println!("Fork failed"),
                }
            }
            Ok(Fork::Child) => {
                // Wait for parent to open db.
                std::thread::sleep(std::time::Duration::from_secs(1));
                // Should crash trying to open database.
                let db = Database::new(&common::shared_storage_prefix(), "test", false);
                assert!(db.is_err());
                println!("Child 1: Err({:?}, should be Some)", db.clone().err());
                std::process::exit(db.is_err() as i32);
            }
            Err(_) => println!("Fork failed"),
        }
    }
}
