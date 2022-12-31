use super::{get_shared_log_connection, PersistenceReqMeta, PersistenceRespMeta};
use common::adaptation::{backend::ServiceInfo, ServerfulScalingState};
use common::time_service::TimeService;
use common::ServiceInstance;
use std::sync::{Arc, Mutex};

/// Number of seconds between calls to drain.
const DRAINING_INTERVAL: i64 = 2;

/// A log replica.
#[derive(Clone)]
pub struct LogReplica {
    inner: Arc<Mutex<LogReplicaInner>>,
    svc_info: ServiceInfo,
    drain_lock: Arc<Mutex<()>>,
    time_service: TimeService,
}

struct LogReplicaInner {
    pending_logs: Vec<PendingLog>,
    persisted_lsn: usize,
    max_seen_owner_id: usize,
    terminating: bool,
    last_drain: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug)]
struct PendingLog {
    lo_lsn: usize,
    hi_lsn: usize,
    owner_id: usize,
    entries: Vec<u8>,
    timestamp: chrono::DateTime<chrono::Utc>,
}

#[async_trait::async_trait]
impl ServiceInstance for LogReplica {
    async fn call(&self, meta: String, arg: Vec<u8>) -> (String, Vec<u8>) {
        let req: PersistenceReqMeta = serde_json::from_str(&meta).unwrap();
        let resp = self.handle_request(req, arg).await;
        (serde_json::to_string(&resp).unwrap(), vec![])
    }

    async fn custom_info(&self, _scaling_state: &ServerfulScalingState) -> serde_json::Value {
        serde_json::Value::Null
    }

    async fn terminate(&self) {
        self.handle_termination().await;
    }
}

impl LogReplica {
    pub async fn new(svc_info: Arc<ServiceInfo>) -> Self {
        // Create shared db.
        let namespace = svc_info.namespace.clone();
        let name = svc_info.name.clone();
        tokio::task::block_in_place(move || {
            let _ = get_shared_log_connection(&namespace, &name, true);
        });
        let time_service = TimeService::new().await;
        let inner = Arc::new(Mutex::new(LogReplicaInner {
            pending_logs: vec![],
            persisted_lsn: 0,
            max_seen_owner_id: 0,
            terminating: false,
            last_drain: time_service.current_time().await,
        }));

        let replica = LogReplica {
            svc_info: svc_info.as_ref().clone(),
            time_service,
            drain_lock: Arc::new(Mutex::new(())),
            inner,
        };
        {
            let replica = replica.clone();
            tokio::spawn(async move {
                replica.bookkeeping_thread().await;
            });
        }
        replica
    }

    /// Handle a new request
    async fn handle_request(&self, req: PersistenceReqMeta, data: Vec<u8>) -> PersistenceRespMeta {
        // Try grabbing lease.
        match req {
            PersistenceReqMeta::Log {
                lo_lsn,
                hi_lsn,
                owner_id,
                persisted_lsn,
                replica_id,
            } => {
                if replica_id != self.svc_info.id {
                    PersistenceRespMeta::WrongReplica
                } else {
                    self.handle_log(lo_lsn, hi_lsn, persisted_lsn, owner_id, data)
                        .await
                }
            }
            PersistenceReqMeta::Drain {
                owner_id,
                persisted_lsn,
                replica_id,
            } => {
                if replica_id != self.svc_info.id {
                    PersistenceRespMeta::WrongReplica
                } else {
                    self.handle_drain(Some(owner_id), Some(persisted_lsn), false)
                        .await
                }
            }
        }
    }

    async fn handle_log(
        &self,
        lo_lsn: usize,
        hi_lsn: usize,
        persisted_lsn: usize,
        owner_id: usize,
        entries: Vec<u8>,
    ) -> PersistenceRespMeta {
        let timestamp = self.time_service.current_time().await;
        tokio::task::block_in_place(move || {
            let mut inner = self.inner.lock().unwrap();
            if owner_id < inner.max_seen_owner_id {
                return PersistenceRespMeta::Outdated;
            } else {
                inner.max_seen_owner_id = owner_id;
            }
            if inner.terminating {
                return PersistenceRespMeta::Terminating;
            }
            if persisted_lsn > inner.persisted_lsn {
                inner.persisted_lsn = persisted_lsn;
                inner.pending_logs.retain(|p| p.hi_lsn > persisted_lsn);
            }
            inner.pending_logs.push(PendingLog {
                lo_lsn,
                hi_lsn,
                owner_id,
                entries,
                timestamp,
            });
            PersistenceRespMeta::Ok
        })
    }

    async fn handle_drain(
        &self,
        owner_id: Option<usize>,
        persisted_lsn: Option<usize>,
        check_recency: bool,
    ) -> PersistenceRespMeta {
        println!("Drain: owner_id={owner_id:?}, persisted_lsn={persisted_lsn:?}, check_recency={check_recency:?}");
        let _l = self.drain_lock.lock().unwrap();
        let handle = tokio::runtime::Handle::current();
        tokio::task::block_in_place(move || {
            let (pending_logs, persisted_lsn) = {
                let mut inner = self.inner.lock().unwrap();
                println!(
                    "Replica: persisted_lsn={}, pending_size={}.",
                    inner.persisted_lsn,
                    inner.pending_logs.len(),
                );
                if let Some(owner_id) = owner_id {
                    if owner_id < inner.max_seen_owner_id {
                        return PersistenceRespMeta::Outdated;
                    } else {
                        inner.max_seen_owner_id = owner_id;
                    }
                }

                let timestamp = handle.block_on(async { self.time_service.current_time().await });
                if timestamp > inner.last_drain {
                    inner.last_drain = timestamp;
                }
                if let Some(persisted_lsn) = persisted_lsn {
                    if persisted_lsn > inner.persisted_lsn {
                        inner.persisted_lsn = persisted_lsn;
                        inner.pending_logs.retain(|p| p.hi_lsn > persisted_lsn);
                    }
                }

                let pending_logs = if check_recency {
                    let recency_threshold = chrono::Duration::seconds(DRAINING_INTERVAL);
                    let pending_logs: Vec<PendingLog> = inner.pending_logs.drain(..).collect();
                    let (recent, old): (Vec<_>, Vec<_>) =
                        pending_logs.into_iter().partition(|pending_log| {
                            inner
                                .last_drain
                                .signed_duration_since(pending_log.timestamp)
                                < recency_threshold
                        });
                    inner.pending_logs = recent;
                    old
                } else {
                    inner.pending_logs.drain(..).collect()
                };
                (pending_logs, inner.persisted_lsn)
            };

            // Remove already persisted lsns to prevent unnecessary writes.
            let pending_logs: Vec<PendingLog> = pending_logs
                .into_iter()
                .filter(|p| p.hi_lsn > persisted_lsn)
                .collect();
            // Write to shared log if owner_id is still same.
            // Assuming no more than AZ+1 uncontrolled failures,
            // this guarantees that all acked writes are persisted.
            // Checking owner_id prevents post-ownership change unacked writes from being peristed.
            for PendingLog {
                lo_lsn,
                hi_lsn,
                owner_id,
                entries,
                timestamp: _,
            } in pending_logs
            {
                // Just retry until success.
                loop {
                    let conn = get_shared_log_connection(
                        &self.svc_info.namespace,
                        &self.svc_info.name,
                        false,
                    );
                    let executed = conn.execute(
                        "REPLACE INTO system__shared_logs(lo_lsn, hi_lsn, entries) \
                        SELECT ?, ?, ? FROM system__shared_ownership WHERE owner_id=?",
                        rusqlite::params![lo_lsn, hi_lsn, entries, owner_id],
                    );
                    match executed {
                        Ok(_) => {
                            break;
                        }
                        Err(x) => {
                            println!("{x:?}");
                            continue;
                        }
                    }
                }
            }
            loop {
                let conn =
                    get_shared_log_connection(&self.svc_info.namespace, &self.svc_info.name, false);
                let executed = conn.execute(
                    "DELETE FROM system__shared_logs WHERE hi_lsn <= ?",
                    rusqlite::params![persisted_lsn],
                );
                match executed {
                    Ok(_) => {
                        break;
                    }
                    Err(x) => {
                        println!("{x:?}");
                        continue;
                    }
                }
            }
            PersistenceRespMeta::Ok
        })
    }

    async fn bookkeeping_thread(&self) {
        let mut drain_interval =
            tokio::time::interval(std::time::Duration::from_secs(DRAINING_INTERVAL as u64));
        drain_interval.tick().await;
        loop {
            drain_interval.tick().await;
            tokio::task::block_in_place(move || {
                let inner = self.inner.lock().unwrap();
                if inner.terminating {
                    return;
                }
            });
            self.handle_drain(None, None, true).await;
        }
    }

    async fn handle_termination(&self) {
        println!("Called log termination!");
        tokio::task::block_in_place(move || {
            let mut inner = self.inner.lock().unwrap();
            inner.terminating = true;
        });
        self.handle_drain(None, None, false).await;
    }
}
