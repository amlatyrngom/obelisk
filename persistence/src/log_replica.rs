use super::{PersistenceReqMeta, PersistenceRespMeta};
use common::adaptation::frontend::AdapterFrontend;
use common::adaptation::{backend::ServiceInfo, ServerfulScalingState};
use common::ServiceInstance;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;

/// A log replica.
#[derive(Clone)]
pub struct LogReplica {
    inner: Arc<Mutex<LogReplicaInner>>,
    svc_info: ServiceInfo,
}

struct LogReplicaInner {
    pending_logs: Vec<PendingLog>,
    persisted_lsn: usize,
    max_seen_owner_id: usize,
    terminating: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PendingLog {
    pub lo_lsn: usize,
    pub hi_lsn: usize,
    pub entries: Vec<u8>,
}

#[async_trait::async_trait]
impl ServiceInstance for LogReplica {
    async fn call(&self, meta: String, arg: Vec<u8>) -> (String, Vec<u8>) {
        let req: PersistenceReqMeta = serde_json::from_str(&meta).unwrap();
        let (resp, payload) = self.handle_request(req, arg).await;
        (serde_json::to_string(&resp).unwrap(), payload)
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
        println!("Replica: {svc_info:?}");
        let inner = Arc::new(Mutex::new(LogReplicaInner {
            pending_logs: vec![],
            persisted_lsn: 0,
            max_seen_owner_id: 0,
            terminating: false,
        }));

        let replica = LogReplica {
            svc_info: svc_info.as_ref().clone(),
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
                let resp = if replica_id != self.svc_info.id {
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
                if replica_id != self.svc_info.id {
                    (PersistenceRespMeta::WrongReplica, vec![])
                } else {
                    self.handle_drain(owner_id, persisted_lsn).await
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
        println!("Handle Log: lo_lsn={lo_lsn}, hi_lsn={hi_lsn}, owner_id={owner_id}");
        let mut inner = self.inner.lock().await;
        println!("Handle Log: Locked.");
        if owner_id < inner.max_seen_owner_id {
            return PersistenceRespMeta::Outdated;
        } else {
            inner.max_seen_owner_id = owner_id;
        }
        if persisted_lsn > inner.persisted_lsn {
            inner.persisted_lsn = persisted_lsn;
            inner.pending_logs.retain(|p| p.hi_lsn > persisted_lsn);
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
        // Check incarnation number.
        if owner_id < inner.max_seen_owner_id {
            return (PersistenceRespMeta::Outdated, vec![]);
        } else {
            inner.max_seen_owner_id = owner_id;
        }
        // Check perisisted lsn.
        if persisted_lsn > inner.persisted_lsn {
            inner.persisted_lsn = persisted_lsn;
            inner.pending_logs.retain(|p| p.hi_lsn > persisted_lsn);
        }
        // Respond with still pending logs.
        let payload =
            tokio::task::block_in_place(move || bincode::serialize(&inner.pending_logs).unwrap());
        (PersistenceRespMeta::Ok, payload)
    }

    async fn handle_termination(&self) {
        println!("Called log termination!");
        // Stop accepting new requests.
        {
            let mut inner = self.inner.lock().await;
            inner.terminating = true;
        }
        // Make sure to spin a new instance.
        let messaging_frontend = Arc::new(
            AdapterFrontend::new("messaging", &self.svc_info.namespace, &self.svc_info.name).await,
        );
        loop {
            let no_pending_logs = {
                let inner = self.inner.lock().await;
                inner.pending_logs.is_empty()
            };
            if no_pending_logs {
                return;
            }
            // Hacky way to signal spin up.
            let duration = 0.0;
            messaging_frontend
                .collect_metric(serde_json::to_value(duration).unwrap())
                .await;
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    }
}
