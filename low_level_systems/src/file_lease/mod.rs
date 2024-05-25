use super::LLError;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::params;
use std::sync::{Arc, Mutex};

/// Lease duration in seconds.
const LEASE_DURATION: i64 = 10;
/// Lease renewal in seconds.
const LEASE_RENEWAL: i64 = 5;
/// DB Retries.
const DB_RETRIES: i64 = 20;

/// Lease.
#[derive(Debug, Clone)]
pub struct FileLease {
    pub incarnation: i64,
    pool: Pool<SqliteConnectionManager>,
    lease_duration: i64,
    curr_expiry: Arc<Mutex<i64>>,
}

/// Lease configuration.
#[derive(Debug, Clone)]
pub struct LeaseConfig {
    pub lease_duration: i64,
    pub renewal: i64,
}

/// Get the filename of the lease.
fn lease_filename(storage_dir: &str, name: &str) -> String {
    format!("{storage_dir}/{name}.lease.db")
}

/// Remove all files associated with this lease.
pub fn reset_lease(storage_dir: &str, name: &str) {
    let filename = lease_filename(storage_dir, name);
    let _ = std::fs::remove_file(filename);
    let _ = std::fs::create_dir_all(storage_dir);
}

impl FileLease {
    /// Create a new lease with default configuration.
    pub fn new(storage_dir: &str, name: &str, exclusive: bool) -> FileLease {
        FileLease::new_with_config(
            storage_dir,
            name,
            exclusive,
            LeaseConfig {
                lease_duration: LEASE_DURATION,
                renewal: LEASE_RENEWAL,
            },
        )
    }

    /// Create a new lease with the given configuration.
    pub fn new_with_config(
        storage_dir: &str,
        name: &str,
        exclusive: bool,
        config: LeaseConfig,
    ) -> FileLease {
        let pool = FileLease::make_pool(storage_dir, name, exclusive).unwrap();
        let (mut incarnation, mut expiry) = (-1, -1);
        for n in 0..DB_RETRIES {
            let r = FileLease::init_lease(&pool, config.lease_duration, exclusive, n);
            match r {
                Ok((i, e)) => {
                    incarnation = i;
                    expiry = e;
                    break;
                }
                Err(e) => {
                    eprintln!("[Lease] Failed to acquire lease: {e:?}. {n}/{DB_RETRIES}.");
                    std::thread::sleep(std::time::Duration::from_secs(1));
                }
            }
        }
        if incarnation < 0 {
            eprintln!("[Lease] Failed to acquire lease.");
            std::process::exit(1);
        }
        let lease = FileLease {
            incarnation,
            curr_expiry: Arc::new(Mutex::new(expiry)),
            pool,
            lease_duration: config.lease_duration,
        };
        let this = lease.clone();
        std::thread::spawn(move || loop {
            std::thread::sleep(std::time::Duration::from_secs(config.renewal as u64));
            let mut done = false;
            for _ in 0..DB_RETRIES {
                if this.renew_lease().is_ok() {
                    done = true;
                    break;
                }
            }
            if !done {
                eprintln!("[Lease] Failed to renew lease. Exiting.");
                std::process::exit(1);
            }
        });
        lease
    }

    /// Create the pool.
    fn make_pool(
        storage_dir: &str,
        name: &str,
        _exclusive: bool,
    ) -> Result<Pool<SqliteConnectionManager>, LLError> {
        // Create pool.
        let lease_filename = lease_filename(storage_dir, name);
        // Open flags with timeout set to 1s.
        let manager = SqliteConnectionManager::file(&lease_filename);
        println!("[Manager::make_pool] Creating pool.");
        let pool = r2d2::Builder::new()
            .max_size(1)
            .build(manager)
            .map_err(|e| LLError::LeaseError(format!("Failed to create pool: {}", e)))?;
        println!("[Manager::make_pool] Pool created.");
        let conn = pool.get().unwrap();
        // Set timeout to 1s.
        conn.busy_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        // if exclusive {
        //     // Set locking mode to EXCLUSIVE; journal mode to WAL; synchronous to FULL.
        //     conn.execute_batch("PRAGMA locking_mode=EXCLUSIVE; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;").unwrap();
        // } else {
        //     // Set locking mode to NORMAL; journal mode to DELETE; synchronous to FULL.
        //     conn.execute_batch("PRAGMA journal_mode=DELETE; PRAGMA locking_mode=NORMAL; PRAGMA synchronous=FULL;").unwrap();
        // }
        // For now, make shared. Easier logic.
        // conn.execute_batch(
        //     "PRAGMA journal_mode=DELETE; PRAGMA locking_mode=NORMAL; PRAGMA synchronous=FULL;",
        // )
        // .unwrap();
        // Create schema.
        loop {
            match conn.execute_batch(include_str!("schema.sql")) {
                Ok(_) => break,
                Err(e) => {
                    eprintln!("[Lease::make_pool] Failed to create schema: {e:?}. Retrying.");
                    std::thread::sleep(std::time::Duration::from_secs(1));
                }
            }
        }
        // Done.
        Ok(pool)
    }

    /// Check lease owned.
    pub fn check_lease(&self) -> bool {
        let now = chrono::Utc::now().timestamp();
        let expiry = *self.curr_expiry.lock().unwrap();
        let slack = expiry - (self.lease_duration / 2);
        if now < slack {
            true
        } else {
            for n in 0..DB_RETRIES {
                match self.renew_lease() {
                    Ok(_) => return true,
                    Err(_) => {
                        eprintln!("[Lease::check_lease] Failed to renew lease. {n}/{DB_RETRIES}.");
                        std::thread::sleep(std::time::Duration::from_secs(1));
                    }
                }
            }
            eprintln!("[Lease::check_lease] Failed to renew lease. Exiting.");
            std::process::exit(1);
        }
    }

    /// Renew the lease.
    pub fn renew_lease(&self) -> Result<(), LLError> {
        let _now = chrono::Utc::now().timestamp();
        let mut conn = self.pool.get().unwrap();
        conn.busy_timeout(std::time::Duration::from_millis(500))
            .map_err(|e| LLError::LeaseError(format!("{e:?}")))?;
        let txn = conn
            .transaction()
            .map_err(|e| LLError::LeaseError(format!("{e:?}")))?;
        let stmt = "SELECT incarnation_num, waiting, expiry FROM lease WHERE unique_row=0";
        let (incarnation, waiting, _expiry): (i64, i64, i64) = txn
            .query_row(stmt, params![], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .map_err(|e| LLError::LeaseError(format!("{e:?}")))?;
        if incarnation != self.incarnation {
            eprintln!(
                "[Lease::renew_lease] Lease {} lost. Value is now {incarnation}.",
                self.incarnation
            );
            std::process::exit(1);
        }
        // // Hack for better stability when redeploying.
        // if expiry < now {
        //     eprintln!("[Lease::renew_lease] Lease expired (likely Lambda freeze). Exiting.");
        //     std::process::exit(1);
        // }

        // Someone is waiting for this lease to end. Let it expire.
        let (expiry, exit) = if waiting == incarnation {
            // Reduce expiry to now+1.
            let expiry = chrono::Utc::now().timestamp() + 1;
            (expiry, true)
        } else {
            let expiry = chrono::Utc::now().timestamp() + self.lease_duration;
            (expiry, false)
        };
        let stmt = "UPDATE lease SET expiry=? WHERE unique_row=0";
        txn.execute(stmt, params![expiry])
            .map_err(|e| LLError::LeaseError(format!("{e:?}")))?;
        txn.commit()
            .map_err(|e| LLError::LeaseError(format!("{e:?}")))?;
        if exit {
            eprintln!("[Lease::renew_lease] Handing over lease to waiting process.");
            std::process::exit(1);
        }
        println!("Lease renewed to {expiry}. Incarnation={incarnation}.");
        *self.curr_expiry.lock().unwrap() = expiry;
        Ok(())
    }

    /// Release the file.
    pub fn revoke_lease(&self) -> Result<(), LLError> {
        let conn = self.pool.get().unwrap();
        let expiry = chrono::Utc::now().timestamp() + 1;
        let stmt = "UPDATE lease SET expiry=? WHERE unique_row=0";
        conn.execute(stmt, params![expiry]).unwrap();
        Ok(())
    }

    /// Acquire the file.
    pub fn init_lease(
        pool: &Pool<SqliteConnectionManager>,
        lease_duration: i64,
        exclusive: bool,
        retries: i64,
    ) -> Result<(i64, i64), LLError> {
        let mut conn = pool.get().unwrap();
        let txn = conn
            .transaction()
            .map_err(|e| LLError::LeaseError(format!("{e:?}")))?;
        let stmt = "SELECT incarnation_num, expiry, waiting FROM lease WHERE unique_row=0";
        let (incarnation, expiry, waiting): (i64, i64, i64) = txn
            .query_row(stmt, params![], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .map_err(|e| LLError::LeaseError(format!("{e:?}")))?;
        if retries == 0 && !exclusive && waiting == incarnation {
            let _ = txn.rollback();
            eprintln!("Some other thread is trying to acquire lease. Waiting.");
            std::thread::sleep(std::time::Duration::from_secs_f64(1.0));
            return Err(LLError::LeaseError("DB is busy.".to_string()));
        }
        let now = chrono::Utc::now().timestamp();
        if now < expiry && !exclusive {
            eprintln!(
                "Lease is still valid. Incarnation: {incarnation}, Expiry: {expiry}, Now: {now}"
            );
            std::process::exit(1);
        } else if now < expiry && exclusive {
            println!("Waiting for availability of lease. Waiting={incarnation}.");
            let stmt = "UPDATE lease SET waiting=? WHERE unique_row=0";
            txn.execute(stmt, params![incarnation])
                .map_err(|e| LLError::LeaseError(format!("{e:?}")))?;
            txn.commit()
                .map_err(|e| LLError::LeaseError(format!("{e:?}")))?;
            std::thread::sleep(std::time::Duration::from_secs_f64(0.5));
            // Forcibly drop connection.
            std::mem::drop(conn);
            return Err(LLError::LeaseError("DB is busy.".to_string()));
        }
        let incarnation = incarnation + 1;
        let expiry = now + lease_duration;
        let stmt = "UPDATE lease SET incarnation_num=?, expiry=? WHERE unique_row=0";
        txn.execute(stmt, params![incarnation, expiry])
            .map_err(|e| LLError::LeaseError(format!("{e:?}")))?;
        txn.commit()
            .map_err(|e| LLError::LeaseError(format!("{e:?}")))?;
        println!("Lease expiry set to {expiry}. Incarnation={incarnation}.");
        Ok((incarnation, expiry))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nix::sys::wait;
    use nix::unistd;

    #[test]
    fn test_basic_shared_lease() {
        let storage_dir = "storage";
        let name = "essai";
        reset_lease(storage_dir, name);
        let config = LeaseConfig {
            lease_duration: 5,
            renewal: 1,
        };
        match unsafe { unistd::fork() } {
            Ok(unistd::ForkResult::Parent { child, .. }) => {
                let child1 = child;
                match unsafe { unistd::fork() } {
                    Ok(unistd::ForkResult::Parent { child, .. }) => {
                        let child2 = child;
                        match unsafe { unistd::fork() } {
                            Ok(unistd::ForkResult::Parent { child, .. }) => {
                                let child3 = child;
                                // Child 1: First to acquire lease, should succeed.
                                let child1_status = wait::waitpid(child1, None).unwrap();
                                assert!(matches!(child1_status, wait::WaitStatus::Exited(_, 0)));
                                // Child 2: Immediately after child 1, should fail.
                                let child2_status = wait::waitpid(child2, None).unwrap();
                                assert!(matches!(child2_status, wait::WaitStatus::Exited(_, 1)));
                                // Child 3: Waits for lease duration to end without renewal, should succeed.
                                let child3_status = wait::waitpid(child3, None).unwrap();
                                assert!(matches!(child3_status, wait::WaitStatus::Exited(_, 0)));
                            }
                            Ok(unistd::ForkResult::Child) => {
                                // Child 3.
                                std::thread::sleep(std::time::Duration::from_secs(
                                    1 + config.lease_duration as u64,
                                ));
                                let lease =
                                    FileLease::new_with_config(storage_dir, name, false, config);
                                println!("Child 3 incarnation: {}", lease.incarnation);
                                assert_eq!(lease.incarnation, 2);
                                std::process::exit(0);
                            }
                            Err(_) => {
                                panic!("Failed to fork.");
                            }
                        }
                    }
                    Ok(unistd::ForkResult::Child) => {
                        // Child 2.
                        std::thread::sleep(std::time::Duration::from_secs(1));
                        let _lease = FileLease::new_with_config(storage_dir, name, false, config);
                        // Should not reach here.
                        std::process::exit(0);
                    }
                    Err(_) => {
                        panic!("Failed to fork.");
                    }
                }
            }
            Ok(unistd::ForkResult::Child) => {
                // Child 1.
                println!("Child 1 trying to acquire lease.");
                let lease = FileLease::new_with_config(storage_dir, name, false, config);
                println!("Child 1 incarnation: {}", lease.incarnation);
                assert_eq!(lease.incarnation, 1);
                std::process::exit(0);
            }
            Err(_) => {
                panic!("Failed to fork.");
            }
        }
    }

    #[test]
    fn test_shared_lease_renewal() {
        let storage_dir = "storage";
        let name = "essai";
        reset_lease(storage_dir, name);
        let config = LeaseConfig {
            lease_duration: 5,
            renewal: 3,
        };
        match unsafe { unistd::fork() } {
            Ok(unistd::ForkResult::Parent { child, .. }) => {
                let child1 = child;
                match unsafe { unistd::fork() } {
                    Ok(unistd::ForkResult::Parent { child, .. }) => {
                        let child2 = child;
                        match unsafe { unistd::fork() } {
                            Ok(unistd::ForkResult::Parent { child, .. }) => {
                                let child3 = child;
                                // Child 1: First to acquire lease, should succeed.
                                let child1_status = wait::waitpid(child1, None).unwrap();
                                assert!(matches!(child1_status, wait::WaitStatus::Exited(_, 0)));
                                // Child 2: Immediately after child 1, should fail.
                                let child2_status = wait::waitpid(child2, None).unwrap();
                                assert!(matches!(child2_status, wait::WaitStatus::Exited(_, 1)));
                                // Child 3: Waits for lease duration. Should still fail because now there is a renewal.
                                let child3_status = wait::waitpid(child3, None).unwrap();
                                assert!(matches!(child3_status, wait::WaitStatus::Exited(_, 1)));
                            }
                            Ok(unistd::ForkResult::Child) => {
                                // Child 3.
                                std::thread::sleep(std::time::Duration::from_secs(
                                    1 + config.lease_duration as u64,
                                ));
                                let lease =
                                    FileLease::new_with_config(storage_dir, name, false, config);
                                println!("Child 3 incarnation: {}", lease.incarnation);
                                assert_eq!(lease.incarnation, 2);
                                std::process::exit(0);
                            }
                            Err(_) => {
                                panic!("Failed to fork.");
                            }
                        }
                    }
                    Ok(unistd::ForkResult::Child) => {
                        // Child 2.
                        std::thread::sleep(std::time::Duration::from_secs(1));
                        let _lease = FileLease::new_with_config(storage_dir, name, false, config);
                        // Should not reach here.
                        std::process::exit(0);
                    }
                    Err(_) => {
                        panic!("Failed to fork.");
                    }
                }
            }
            Ok(unistd::ForkResult::Child) => {
                // Child 1.
                let lease = FileLease::new_with_config(storage_dir, name, false, config.clone());
                println!("Child 1 incarnation: {}", lease.incarnation);
                assert_eq!(lease.incarnation, 1);
                // Wait for renewal thread before exiting.
                std::thread::sleep(std::time::Duration::from_secs(2 * config.renewal as u64));
                std::process::exit(0);
            }
            Err(_) => {
                panic!("Failed to fork.");
            }
        }
    }

    #[test]
    fn test_exclusive_handover() {
        let storage_dir = "storage";
        let name = "essai";
        reset_lease(storage_dir, name);
        let config = LeaseConfig {
            lease_duration: 5,
            renewal: 1,
        };
        match unsafe { unistd::fork() } {
            Ok(unistd::ForkResult::Parent { child, .. }) => {
                let child1 = child;
                match unsafe { unistd::fork() } {
                    Ok(unistd::ForkResult::Parent { child, .. }) => {
                        let child2 = child;
                        match unsafe { unistd::fork() } {
                            Ok(unistd::ForkResult::Parent { child, .. }) => {
                                let child3 = child;
                                // Child 1: First to acquire lease. But is then taken by child 2. Should fail.
                                let child1_status = wait::waitpid(child1, None).unwrap();
                                assert!(matches!(child1_status, wait::WaitStatus::Exited(_, 1)));
                                // Child 2: Exclusive. But is then taken by child 3. Should fail.
                                let child2_status = wait::waitpid(child2, None).unwrap();
                                assert!(matches!(child2_status, wait::WaitStatus::Exited(_, 1)));
                                // Child 3: Last exclusive. Should succeed.
                                let child3_status = wait::waitpid(child3, None).unwrap();
                                assert!(matches!(child3_status, wait::WaitStatus::Exited(_, 0)));
                            }
                            Ok(unistd::ForkResult::Child) => {
                                // Child 3.
                                // Wait for child 2 to start.
                                std::thread::sleep(std::time::Duration::from_secs(2));
                                // Wait for child 2 to acquire lease in exlusive mode.
                                std::thread::sleep(std::time::Duration::from_secs(
                                    2 * config.renewal as u64,
                                ));
                                // Take lease.
                                println!("Child 3 trying to acquire lease.");
                                let lease =
                                    FileLease::new_with_config(storage_dir, name, true, config);
                                println!("Child 3 incarnation: {}", lease.incarnation);
                                assert_eq!(lease.incarnation, 3);
                                std::process::exit(0);
                            }
                            Err(_) => {
                                panic!("Failed to fork.");
                            }
                        }
                    }
                    Ok(unistd::ForkResult::Child) => {
                        // Child 2.
                        // Wait child 1 to start.
                        std::thread::sleep(std::time::Duration::from_secs(1));
                        println!("Child 2 trying to acquire lease.");
                        let lease =
                            FileLease::new_with_config(storage_dir, name, true, config.clone());
                        println!("Child 2 incarnation: {}", lease.incarnation);
                        // Should acquire lease, but then handover to child 3.
                        assert_eq!(lease.incarnation, 2);
                        // Wait for child 3 to take over. Should self-exit.
                        std::thread::sleep(std::time::Duration::from_secs(
                            3 * config.lease_duration as u64,
                        ));
                        // Should not reach here.
                        std::process::exit(0);
                    }
                    Err(_) => {
                        panic!("Failed to fork.");
                    }
                }
            }
            Ok(unistd::ForkResult::Child) => {
                // Child 1.
                let lease = FileLease::new_with_config(storage_dir, name, false, config.clone());
                println!("Child 1 incarnation: {}", lease.incarnation);
                assert_eq!(lease.incarnation, 1);
                // Wait sufficient time for child 2 to acquire lease in exlusive mode.
                // Should lead to self-exit.
                std::thread::sleep(std::time::Duration::from_secs(
                    2 * config.lease_duration as u64,
                ));
                // Should not reach here.
                std::process::exit(0);
            }
            Err(_) => {
                panic!("Failed to fork.");
            }
        }
    }
}
