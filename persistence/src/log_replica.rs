use super::{PersistenceReqMeta, PersistenceRespMeta};
use common::{HandlerKit, InstanceInfo, ScalingState, ServerlessHandler, ServerlessStorage};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;

/// A log replica.
#[derive(Clone)]
pub struct LogReplica {
    inner: Arc<Mutex<LogReplicaInner>>,
    instance_info: Arc<InstanceInfo>,
}

struct LogReplicaInner {
    pending_logs: Vec<PendingLog>,
    persisted_lsn: usize,
    max_seen_owner_id: usize,
    terminating: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PendingLog {
    pub lo_lsn: usize,
    pub hi_lsn: usize,
    pub entries: Vec<u8>,
}

#[async_trait::async_trait]
impl ServerlessHandler for LogReplica {
    async fn handle(&self, meta: String, arg: Vec<u8>) -> (String, Vec<u8>) {
        let req: PersistenceReqMeta = serde_json::from_str(&meta).unwrap();
        let (resp, payload) = self.handle_request(req, arg).await;
        (serde_json::to_string(&resp).unwrap(), payload)
    }

    async fn checkpoint(&self, _scaling_state: &ScalingState, terminating: bool) {
        if terminating {
            self.handle_termination().await;
        }
    }
}

impl LogReplica {
    /// Create replica.
    pub async fn new(kit: HandlerKit) -> Self {
        // Create shared db.
        println!("Replica: {:?}", kit.instance_info);
        let inner = Arc::new(Mutex::new(LogReplicaInner {
            pending_logs: vec![],
            persisted_lsn: 0,
            max_seen_owner_id: 0,
            terminating: false,
        }));
        let instance_info = kit.instance_info.clone();

        let replica = LogReplica {
            instance_info,
            inner,
        };
        replica
    }

    /// Handle a new request
    async fn handle_request(
        &self,
        req: PersistenceReqMeta,
        data: Vec<u8>,
    ) -> (PersistenceRespMeta, Vec<u8>) {
        // Try grabbing lease.
        match req {
            PersistenceReqMeta::Log {
                lo_lsn,
                hi_lsn,
                owner_id,
                persisted_lsn,
                replica_id,
            } => {
                let resp = if replica_id != self.instance_info.peer_id {
                    PersistenceRespMeta::WrongReplica
                } else {
                    self.handle_log(lo_lsn, hi_lsn, persisted_lsn, owner_id, data)
                        .await
                };
                (resp, vec![])
            }
            PersistenceReqMeta::Drain {
                owner_id,
                persisted_lsn,
                replica_id,
            } => {
                if replica_id != self.instance_info.peer_id {
                    (PersistenceRespMeta::WrongReplica, vec![])
                } else {
                    self.handle_drain(owner_id, persisted_lsn).await
                }
            }
        }
    }

    /// Handle log entry.
    async fn handle_log(
        &self,
        lo_lsn: usize,
        hi_lsn: usize,
        persisted_lsn: usize,
        owner_id: usize,
        entries: Vec<u8>,
    ) -> PersistenceRespMeta {
        println!("Handle Log: lo_lsn={lo_lsn}, hi_lsn={hi_lsn}, owner_id={owner_id}");
        let mut inner = self.inner.lock().await;
        println!("Handle Log: Locked.");
        if persisted_lsn > inner.persisted_lsn {
            inner.persisted_lsn = persisted_lsn;
            inner.pending_logs.retain(|p| p.hi_lsn > persisted_lsn);
        }
        if owner_id < inner.max_seen_owner_id {
            return PersistenceRespMeta::Outdated;
        } else {
            inner.max_seen_owner_id = owner_id;
        }
        if inner.terminating {
            return PersistenceRespMeta::Terminating;
        }
        if !entries.is_empty() {
            // May send empty entries just to update lsns.
            inner.pending_logs.push(PendingLog {
                lo_lsn,
                hi_lsn,
                entries,
            });
        }

        PersistenceRespMeta::Ok
    }

    /// Handle call to drain.
    async fn handle_drain(
        &self,
        owner_id: usize,
        persisted_lsn: usize,
    ) -> (PersistenceRespMeta, Vec<u8>) {
        let mut inner = self.inner.lock().await;
        println!("Drain: owner_id={owner_id:?}, persisted_lsn={persisted_lsn:?}");
        println!(
            "Replica: persisted_lsn={}, pending_size={}.",
            inner.persisted_lsn,
            inner.pending_logs.len(),
        );
        // Check perisisted lsn.
        if persisted_lsn > inner.persisted_lsn {
            inner.persisted_lsn = persisted_lsn;
            inner.pending_logs.retain(|p| p.hi_lsn > persisted_lsn);
        }
        // Check incarnation number.
        if owner_id < inner.max_seen_owner_id {
            return (PersistenceRespMeta::Outdated, vec![]);
        } else {
            inner.max_seen_owner_id = owner_id;
        }
        // Respond with still pending logs.
        let payload =
            tokio::task::block_in_place(move || bincode::serialize(&inner.pending_logs).unwrap());
        (PersistenceRespMeta::Ok, payload)
    }

    /// Handle call to terminate.
    async fn handle_termination(&self) {
        println!("Called log termination!");
        // Stop accepting new requests.
        {
            let mut inner = self.inner.lock().await;
            inner.terminating = true;
        }
        // Write all log entries.
        // Let drain happen.
        let exec_mode = std::env::var("OBK_EXECUTION_MODE").unwrap_or("local".into());
        let sleep_time_secs = if exec_mode.contains("local") {
            5
        } else {
            60
        };
        println!("Termination waiting: {sleep_time_secs}secs!");
        tokio::time::sleep(std::time::Duration::from_secs(sleep_time_secs)).await;
        loop {
            // Sleep for a while to allow to the handle to pass in persistent_lsn.
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            // If there are still pending logs after this, the handler is likely crashed. Write them down.
            // Hold lock during loop to avoid race conditions involving drain.
            // TODO: I still have to truly think about the correctness of this logic.
            let inner = self.inner.lock().await;
            println!("Termination pending: {:?}!", inner.pending_logs.len());
            let storage_dir = ServerlessStorage::get_storage_dir(
                &self.instance_info.namespace,
                &self.instance_info.identifier,
                false,
            );
            let ok = tokio::task::block_in_place(move || {
                let pool = ServerlessStorage::try_exclusive_file(&storage_dir, 1);
                let pool = match pool {
                    Ok(pool) => pool,
                    Err(e) => {
                        println!("Open Err: {e:?}");
                        drop(inner);
                        return false;
                    }
                };
                let mut conn = pool.get().unwrap();
                let txn = conn.transaction().unwrap();
                // TODO: Recheck if blind write (without checking start and persistent lsn) really is correct.
                // I think it should be, but I am not sure.
                let start_lsn = txn
                    .query_row("SELECT start_lsn FROM system__logs_ownership", [], |r| {
                        r.get::<usize, usize>(0)
                    })
                    .unwrap();
                let highest_lsn = txn.query_row("SELECT MAX(hi_lsn) FROM system__logs", [], |r| {
                    r.get::<usize, usize>(0)
                });
                let highest_lsn = match highest_lsn {
                    Ok(l) => l,
                    Err(rusqlite::Error::QueryReturnedNoRows) => start_lsn,
                    Err(rusqlite::Error::InvalidColumnType(_, _, rusqlite::types::Type::Null)) => {
                        start_lsn
                    },
                    Err(e) => {
                        println!("{e:?}");
                        return false;
                    }
                };
                for pending_log in &inner.pending_logs {
                    if pending_log.hi_lsn <= highest_lsn {
                        // Already persisted somehow.
                        continue;
                    }
                    txn.execute(
                        "REPLACE INTO system__logs(lo_lsn, hi_lsn, entries) VALUES (?, ?, ?)",
                        rusqlite::params![
                            pending_log.lo_lsn,
                            pending_log.hi_lsn,
                            &pending_log.entries
                        ],
                    )
                    .unwrap();
                }
                txn.commit().unwrap();
                return true;
            });
            if ok {
                return;
            } else {
                continue;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common::{HandlerKit, InstanceInfo, ServerlessStorage};

    use crate::{log_replica::PendingLog, LogReplica, PersistenceRespMeta, PersistentLog};

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn basic_replica_test() {
        run_basic_replica_test().await;
    }

    fn make_test_replica_kit() -> HandlerKit {
        let instance_info = InstanceInfo {
            peer_id: "111-222-333".into(),
            az: Some("af-sn-1".into()),
            mem: 1024,
            cpus: 256,
            public_url: Some("chezmoi.com".into()),
            private_url: Some("chezmoi.com".into()),
            service_name: Some("replica".into()),
            handler_name: None,
            subsystem: "wal".into(),
            namespace: "wal".into(),
            identifier: "echolog0".into(),
            unique: true,
            persistent: true,
        };

        HandlerKit {
            instance_info: Arc::new(instance_info),
            serverless_storage: None, // Does not matter.
        }
    }

    async fn make_test_echo_kit() -> HandlerKit {
        let instance_info = Arc::new(InstanceInfo {
            peer_id: "444-555-666".into(),
            az: None,
            mem: 1024,
            cpus: 512,
            public_url: Some("chezmoi.com".into()),
            private_url: Some("chezmoi.com".into()),
            service_name: None,
            handler_name: Some("echolog".into()),
            subsystem: "wal".into(),
            namespace: "wal".into(),
            identifier: "echolog0".into(),
            unique: true,
            persistent: true,
        });

        let serverless_storage = ServerlessStorage::new_from_info(instance_info.clone())
            .await
            .unwrap();

        HandlerKit {
            instance_info,
            serverless_storage, // Does not matter.
        }
    }

    async fn run_basic_replica_test() {
        let dir = common::shared_storage_prefix();
        let r = std::fs::remove_dir_all(&dir);
        println!("RM: {r:?}");
        std::env::set_var("OBK_EXECUTION_MODE", "local_ecs");
        let echo_kit = make_test_echo_kit().await;
        let echolog = PersistentLog::new(
            echo_kit.instance_info.clone(),
            echo_kit.serverless_storage.unwrap(),
        )
        .await
        .unwrap();
        let replica_kit = make_test_replica_kit();
        let replica = LogReplica::new(replica_kit).await;
        // Send lsns[0, 1], persist=0, owner=1.
        let resp = replica.handle_log(0, 1, 0, 1, vec![37]).await;
        assert!(matches!(resp, PersistenceRespMeta::Ok));
        // Try with owner = 0, persist=1. Should reject, and also clean up previous entries.
        let resp = replica.handle_log(2, 3, 1, 0, vec![1]).await;
        assert!(matches!(resp, PersistenceRespMeta::Outdated));
        {
            let inner = replica.inner.lock().await;
            assert!(inner.pending_logs.is_empty());
        }
        // Now send three requests:
        // owner=1, lsns[3, 7], persist=1.
        let resp = replica.handle_log(3, 7, 1, 1, vec![73, 74]).await;
        assert!(matches!(resp, PersistenceRespMeta::Ok));
        // owner=1, lsns[8, 10], persist=1.
        let resp = replica.handle_log(8, 10, 1, 1, vec![75, 76]).await;
        assert!(matches!(resp, PersistenceRespMeta::Ok));
        // owner=1, lsns[11, 15], persist=1.
        let resp = replica.handle_log(11, 15, 1, 2, vec![77, 78]).await;
        assert!(matches!(resp, PersistenceRespMeta::Ok));
        // Drain with owner=2, persist=7. Should return lsns[8, 10], lsns[11, 15].
        let (resp, payload) = replica.handle_drain(2, 7).await;
        assert!(matches!(resp, PersistenceRespMeta::Ok));
        let logs: Vec<PendingLog> = bincode::deserialize(&payload).unwrap();
        assert_eq!(logs.len(), 2);
        let logs1 = &logs[0];
        let logs2 = &logs[1];
        assert_eq!(logs1.lo_lsn, 8);
        assert_eq!(logs1.hi_lsn, 10);
        assert_eq!(logs2.lo_lsn, 11);
        assert_eq!(logs2.hi_lsn, 15);
        assert_eq!(logs1.entries, vec![75, 76]);
        assert_eq!(logs2.entries, vec![77, 78]);
        // Send empty drain to push persisted lsn.
        // owner=2, persist=10.
        let (resp, payload) = replica.handle_drain(2, 10).await;
        assert!(matches!(resp, PersistenceRespMeta::Ok));
        let logs: Vec<PendingLog> = bincode::deserialize(&payload).unwrap();
        assert_eq!(logs.len(), 1);
        // Drop log, and terminate. Should write last log entry[11, 15] to log.
        {
            // Drop
            drop(echolog);
        }
        replica.handle_termination().await;
        // Check log file.
        let storage_dir = ServerlessStorage::get_storage_dir("wal", "echolog0", false);

        let pool = ServerlessStorage::try_exclusive_file(&storage_dir, 5).unwrap();
        let conn = pool.get().unwrap();
        let (count, hi_lsn, lo_lsn) = conn
            .query_row(
                "SELECT COUNT(*), MAX(hi_lsn), MAX(lo_lsn) FROM system__logs",
                [],
                |r| {
                    let count = r.get::<usize, usize>(0).unwrap();
                    let hi_lsn = r.get::<usize, usize>(1).unwrap();
                    let lo_lsn = r.get::<usize, usize>(2).unwrap();
                    Ok((count, hi_lsn, lo_lsn))
                },
            )
            .unwrap();
        assert_eq!(count, 1);
        assert_eq!(lo_lsn, 11);
        assert_eq!(hi_lsn, 15);
    }
}
