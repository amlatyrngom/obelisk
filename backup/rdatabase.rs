use std::sync::Arc;

/// Time between retries.
/// There is already timeout of ~1s for connecting, so this is additive.
const RETRY_INTERVAL_MS: u64 = 500;
/// Maximum number of retries.
const NUM_RETRIES: usize = 30;

#[derive(Clone)]
pub struct Database {
    pub db: Arc<rocksdb::OptimisticTransactionDB>,
}

impl Database {
    /// Opening a database, or exits if the lock is already owned.
    /// If the the retry flag is set, will retry for a while and then exit.
    pub fn new(storage_dir: &str, with_retry: bool) -> Database {
        let num_retries = if with_retry {
            NUM_RETRIES
        } else {
            1
        };
        for _ in 0..num_retries {
            let _ = std::fs::create_dir_all(storage_dir);
            let db = match rocksdb::OptimisticTransactionDB::open_default(storage_dir) {
                Ok(db) => db,
                Err(x) => {
                    std::thread::sleep(std::time::Duration::from_millis(RETRY_INTERVAL_MS));
                    println!("{x:?}");
                    continue;
                }
            };
            return Database {
                db: Arc::new(db),
            };
        }
        eprintln!(
            "Could not acquire database lock. Exiting process with pid {}.",
            std::process::id()
        );
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::Database;
    use fork::{fork, Fork};
    use std::sync::Arc;
    use std::sync::RwLock;
    use std::collections::HashMap;

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
            let _  = kvs.get(&k).unwrap();
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
                println!("{}", lo-hi);
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
                            let db = Database::new(&common::shared_storage_prefix(), false);
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
                        let db = Database::new(&common::shared_storage_prefix(), true);
                        println!("Child 2."); // Should not be reached.
                        std::process::exit(0);
                    }
                    Err(_) => println!("Fork failed"),
                }
            }
            Ok(Fork::Child) => {
                // Wait for parent to open db.
                std::thread::sleep(std::time::Duration::from_secs(1));
                // Should crash trying to open database.
                let db = Database::new(&common::shared_storage_prefix(), false);
                println!("Child 1."); // Should not be reached.
            }
            Err(_) => println!("Fork failed"),
        }
    }
}
