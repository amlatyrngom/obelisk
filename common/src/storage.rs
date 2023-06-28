use crate::{debug_format, InstanceInfo};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use std::sync::Arc;

/// Serverless Storage.
#[derive(Clone, Debug)]
pub struct ServerlessStorage {
    /// Sequence number.
    pub seq_num: usize,
    /// File shared by all instances with this identifier.
    pub shared_pool: Pool<SqliteConnectionManager>,
    /// File owned by a unique instance. Only present if unique=true.
    pub exclusive_pool: Option<Pool<SqliteConnectionManager>>,
}

impl ServerlessStorage {
    /// Try opening lock file.
    pub fn try_exclusive_file(
        storage_dir: &str,
        max_num_tries: i32,
    ) -> Result<Pool<SqliteConnectionManager>, String> {
        println!("Opening Exclusive File!");
        let db_file = format!("{storage_dir}/lockfile.db");
        let manager = r2d2_sqlite::SqliteConnectionManager::file(db_file.clone());
        let pool = r2d2::Pool::builder()
            .max_size(1)
            .build(manager)
            .map_err(debug_format!())?;
        let mut num_tries = max_num_tries;
        loop {
            // Sleep since the conn timeout seems not enough.
            if num_tries < max_num_tries {
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
            let conn = pool
                .get_timeout(std::time::Duration::from_secs(1))
                .map_err(debug_format!())?;
            conn.busy_timeout(std::time::Duration::from_secs(1))
                .map_err(debug_format!())?;
            // Limit number of retries.
            if num_tries == 0 {
                return Err("Could not lock in time".into());
            }
            num_tries -= 1;
            // For the lock file, set the right pragmas to hold the lock.
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
            break;
        }
        Ok(pool)
    }

    /// Try opening shared file.
    pub fn try_shared_file(
        storage_dir: &str,
    ) -> Result<(Pool<SqliteConnectionManager>, usize), String> {
        println!("Opening Shared File!");
        let db_file = format!("{storage_dir}/notif.db");
        let manager = r2d2_sqlite::SqliteConnectionManager::file(db_file.clone());
        let pool = r2d2::Pool::builder()
            .max_size(1)
            .build(manager)
            .map_err(debug_format!())?;
        let mut conn = pool
            .get_timeout(std::time::Duration::from_secs(1))
            .map_err(debug_format!())?;
        conn.busy_timeout(std::time::Duration::from_secs(1))
            .map_err(debug_format!())?;
        // For the notification file, increase the sequence number.
        let txn = conn.transaction().map_err(debug_format!())?;
        txn.execute("CREATE TABLE IF NOT EXISTS system__ownership (unique_row INTEGER PRIMARY KEY, seq_num INT)", ()).map_err(debug_format!())?;
        let seq_num = txn.query_row("SELECT seq_num FROM system__ownership", [], |r| r.get(0));
        let seq_num: usize = match seq_num {
            Ok(seq_num) => {
                let seq_num: usize = seq_num;
                seq_num + 1
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => 1,
            e => {
                return Err(format!("{e:?}"));
            }
        };
        txn.execute(
            "REPLACE INTO system__ownership (unique_row, seq_num) VALUES (0, ?)",
            [&seq_num],
        )
        .map_err(debug_format!())?;
        txn.commit().map_err(debug_format!())?;
        Ok((pool, seq_num))
    }

    pub fn get_storage_dir(namespace: &str, identifier: &str, is_svc: bool) -> String {
        let shared_dir = crate::shared_storage_prefix();
        let s = if is_svc { "service" } else { "handler" };
        format!("{shared_dir}/{s}/{namespace}/{identifier}/")
    }

    /// New shared storage.
    pub async fn new_shared(
        namespace: &str,
        identifier: &str,
        is_svc: bool,
    ) -> Result<Self, String> {
        let storage_dir = Self::get_storage_dir(namespace, identifier, is_svc);
        println!("Storage Dir: {storage_dir}");
        let _ = std::fs::create_dir_all(&storage_dir);
        let dir = storage_dir.clone();
        let (shared_pool, seq_num) = tokio::task::block_in_place(move || {
            let resp = Self::try_shared_file(&dir);
            resp
        })?;
        Ok(ServerlessStorage {
            seq_num,
            shared_pool,
            exclusive_pool: None,
        })
    }

    /// New exclusive storage.
    pub async fn new_exclusive(
        namespace: &str,
        identifier: &str,
        is_svc: bool,
    ) -> Result<Self, String> {
        let exec_mode = std::env::var("OBK_EXECUTION_MODE").unwrap();
        let storage_dir = Self::get_storage_dir(namespace, identifier, is_svc);
        println!("Storage Dir: {storage_dir}");
        let _ = std::fs::create_dir_all(&storage_dir);
        let dir = storage_dir.clone();
        let exclusive_pool = tokio::task::block_in_place(move || {
            let small_num_tries = 1;
            Self::try_exclusive_file(&dir, small_num_tries)
        });
        if exclusive_pool.is_err() && exec_mode.contains("lambda") {
            // In Lambda mode, first acquisition attempt should be successful.
            let e = Err(format!(
                "Lambda could not acquire lock file first: {:?}!",
                exclusive_pool.err()
            ));
            println!("{e:?}!");
            return e;
        }
        // Get seq num and notify current owner.
        let dir = storage_dir.clone();
        let (shared_pool, seq_num) = tokio::task::block_in_place(move || {
            let resp = Self::try_shared_file(&dir);
            resp
        })?;
        let exclusive_pool = if let Ok(exclusive_pool) = exclusive_pool {
            exclusive_pool
        } else {
            let dir = storage_dir.clone();
            tokio::task::block_in_place(move || {
                let large_num_tries = 30;
                Self::try_exclusive_file(&dir, large_num_tries)
            })?
        };
        Ok(ServerlessStorage {
            seq_num,
            shared_pool,
            exclusive_pool: Some(exclusive_pool),
        })
    }

    /// Create object.
    pub async fn new(
        namespace: &str,
        identifier: &str,
        persistent: bool,
        unique: bool,
        is_svc: bool,
    ) -> Result<Option<Self>, String> {
        if !persistent && !unique {
            return Ok(None);
        }
        if !unique {
            let st = Self::new_shared(namespace, identifier, is_svc).await?;
            Ok(Some(st))
        } else {
            let st = Self::new_exclusive(namespace, identifier, is_svc).await?;
            Ok(Some(st))
        }
    }

    /// Create new serverless storage.
    pub async fn new_from_info(
        instance_info: Arc<InstanceInfo>,
    ) -> Result<Option<Arc<Self>>, String> {
        let (actual_namespace, is_svc) = if instance_info.service_name.is_some() {
            (instance_info.subsystem.clone(), true)
        } else {
            (instance_info.namespace.clone(), false)
        };
        let st = Self::new(
            &actual_namespace,
            &instance_info.identifier,
            instance_info.persistent,
            instance_info.unique,
            is_svc,
        )
        .await?;
        Ok(st.map(|st| Arc::new(st)))
    }

    /// Check current incarnation in notification file.
    pub async fn check_incarnation(&self) -> Result<(), String> {
        let conn = self
            .shared_pool
            .get_timeout(std::time::Duration::from_secs(1))
            .map_err(debug_format!())?;
        let seq_num = conn.query_row("SELECT seq_num FROM system__ownership", [], |r| r.get(0));
        let seq_num: usize = seq_num.map_err(debug_format!())?;
        if self.seq_num == seq_num {
            Ok(())
        } else {
            Err("Lost incarnation!".into())
        }
    }

    pub fn release_exclusive_file(&self, num_tries: i32) -> Result<(), String> {
        let pool = match self.exclusive_pool.clone() {
            Some(pool) => pool,
            None => return Ok(()),
        };
        let mut num_tries = num_tries;
        loop {
            if num_tries == 0 {
                return Err("Could not release!".into());
            }
            num_tries -= 1;
            let conn = match pool.get_timeout(std::time::Duration::from_secs(1)) {
                Ok(conn) => conn,
                Err(e) => {
                    println!("Release Conn Err: {e:?}");
                    continue;
                }
            };
            match conn.pragma_update(None, "journal_mode", "delete") {
                Ok(_) => {}
                Err(x) => {
                    println!("Cannot release: {x:?}");
                    continue;
                }
            }
            match conn.pragma_update(None, "locking_mode", "normal") {
                Ok(_) => {}
                Err(x) => {
                    println!("Cannot release: {x:?}");
                    continue;
                }
            }
            // Actually release lock in sqlite.
            match conn.query_row("SELECT COUNT(*) FROM sqlite_master LIMIT 1", [], |r| {
                r.get(0)
            }) {
                Ok(res) => {
                    let _res: usize = res;
                }
                Err(rusqlite::Error::QueryReturnedNoRows) => {}
                Err(rusqlite::Error::InvalidColumnType(_, _, _)) => {}
                Err(x) => {
                    println!("Cannot release: {x:?}");
                    continue;
                }
            }
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ServerlessStorage;

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_incarnation() {
        std::env::set_var("OBK_EXECUTION_MODE", "local_lambda");
        println!("First incarnation!");
        let incarnation = ServerlessStorage::new("test", "test0", true, true, false).await;
        assert!(matches!(incarnation, Ok(Some(_))));
        let incarnation = incarnation.unwrap().unwrap();
        println!("Second incarnation!");
        let incarnation2 = ServerlessStorage::new("test", "test0", true, true, false).await;
        assert!(matches!(incarnation2, Err(_)));
        // Fake an ecs instance. Should take away the other incarnation.
        std::env::set_var("OBK_EXECUTION_MODE", "local_ecs");
        let checking_thread = tokio::spawn(async move {
            // Check that ownership is lost and drop object to release locks.
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            let has_lock = incarnation.check_incarnation().await;
            println!("Has Lock: {has_lock:?}");
            assert!(matches!(has_lock, Err(_)));
            drop(incarnation)
        });
        let incarnation2 = ServerlessStorage::new("test", "test0", true, true, false).await;
        assert!(matches!(incarnation2, Ok(Some(_))));
        let _ = checking_thread.await;
        // Fake an ecs instance without dropping locks. Should wait for a long time before giving up.
        let incarnation3 = ServerlessStorage::new("test", "test0", true, true, false).await;
        assert!(matches!(incarnation3, Err(_)));
        // Now release incarnation 2 and retry.
        let incarnation2 = incarnation2.unwrap().unwrap();
        let ok = incarnation2.release_exclusive_file(1);
        assert!(matches!(ok, Ok(_)));
        let incarnation3 = ServerlessStorage::new("test", "test0", true, true, false).await;
        assert!(matches!(incarnation3, Ok(Some(_))));
    }
}
