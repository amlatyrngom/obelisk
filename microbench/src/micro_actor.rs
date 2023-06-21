use common::{HandlerKit, ScalingState, ServerlessHandler};
use persistence::PersistentLog;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Request.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum MicroActorReq {
    Increment(i64),
    Retrieve,
}

/// Response.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MicroActorResp {
    pub val: i64,
}

/// Actor.
pub struct MicroActor {
    plog: Arc<PersistentLog>,
    inner: Arc<RwLock<MicroActorInner>>,
}

/// Modifiable.
pub struct MicroActorInner {
    curr_value: i64,
}

/// Implementation.
impl MicroActor {
    /// Create actor.
    pub async fn new(kit: HandlerKit) -> Self {
        let HandlerKit {
            instance_info,
            serverless_storage,
        } = kit;
        let serverless_storage = serverless_storage.unwrap();
        println!("Making micro bench actor: {}!", instance_info.identifier);
        let plog = Arc::new(
            PersistentLog::new(instance_info.clone(), serverless_storage)
                .await
                .unwrap(),
        );
        let inner = Arc::new(RwLock::new(MicroActorInner { curr_value: 0 }));

        let actor = MicroActor { plog, inner };
        // Recover first.
        actor.recover().await;
        actor
    }

    /// Recover last flushed value.
    async fn recover(&self) {
        let flush_lsn = self.plog.get_flush_lsn().await;
        let start_lsn = self.plog.get_start_lsn().await;
        if flush_lsn < start_lsn {
            // Nothing appended yet.
            return;
        }
        println!("Flush LSN: {flush_lsn}");
        println!("Start LSN: {start_lsn}");
        let entries = self.plog.replay(flush_lsn - 1).await.unwrap();
        assert_eq!(entries.len(), 1);
        let (lsn, entry) = entries[0].clone();
        assert_eq!(lsn, flush_lsn);
        let curr_value: i64 = serde_json::from_slice(&entry).unwrap();
        {
            let mut inner = self.inner.write().await;
            inner.curr_value = curr_value;
        }
        self.plog.truncate(flush_lsn - 1).await.unwrap();
    }

    /// Increment.
    async fn handle_increment(&self, v: i64) -> MicroActorResp {
        // Update and enqueue in log.
        let (new_value, lsn) = {
            let mut inner = self.inner.write().await;
            inner.curr_value += v;
            let entry: Vec<u8> = serde_json::to_vec(&inner.curr_value).unwrap();
            let lsn = self.plog.enqueue(entry, Some(512)).await;
            (inner.curr_value, lsn)
        };
        // Wait for flush and return.
        self.plog.flush_at(Some(lsn)).await;
        MicroActorResp { val: new_value }
    }

    /// Retrieve.
    async fn handle_retrieve(&self) -> MicroActorResp {
        // Just read the value.
        let val = {
            let inner = self.inner.read().await;
            inner.curr_value
        };
        MicroActorResp { val }
    }
}

#[async_trait::async_trait]
impl ServerlessHandler for MicroActor {
    /// Handle a message.
    async fn handle(&self, msg: String, _payload: Vec<u8>) -> (String, Vec<u8>) {
        let req: MicroActorReq = serde_json::from_str(&msg).unwrap();
        let resp: MicroActorResp = match req {
            MicroActorReq::Increment(v) => self.handle_increment(v).await,
            MicroActorReq::Retrieve => self.handle_retrieve().await,
        };
        let resp: String = serde_json::to_string(&resp).unwrap();
        (resp, vec![])
    }

    /// Checkpoint. Just truncate the log up to and excluding the last entry.
    async fn checkpoint(&self, _scaling_state: &ScalingState, terminating: bool) {
        println!("Checkpoint:  =({terminating})");
        let flush_lsn = self.plog.get_flush_lsn().await;
        self.plog.truncate(flush_lsn - 1).await.unwrap();
    }
}
