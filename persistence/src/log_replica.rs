use common::{HandlerKit, ScalingState, ServerlessHandler};
use low_level_systems::wal::replica::{Replica, ReplicaReq};
use std::sync::Arc;

/// A log replica.
#[derive(Clone)]
pub struct LogReplica {
    replica: Arc<Replica>,
}

#[async_trait::async_trait]
impl ServerlessHandler for LogReplica {
    async fn handle(&self, _meta: String, arg: Vec<u8>) -> (String, Vec<u8>) {
        tokio::task::block_in_place(|| {
            let req: ReplicaReq = bincode::deserialize(&arg).unwrap();
            let resp = self.replica.process_req(req);
            let resp = bincode::serialize(&resp).unwrap();
            (String::new(), resp)
        })
    }

    async fn checkpoint(&self, _scaling_state: &ScalingState, _terminating: bool) {
        // Do nothing.
    }
}

impl LogReplica {
    /// Create a new log replica.
    pub async fn new(_kit: HandlerKit) -> Self {
        let replica = Arc::new(Replica::new());
        LogReplica { replica }
    }
}
