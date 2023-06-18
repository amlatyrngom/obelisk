use common::{HandlerKit, InstanceInfo, ScalingState, ServerlessHandler, ServerlessStorage};
use std::sync::Arc;

pub struct Echo {
    instance_info: Arc<InstanceInfo>,
    serverless_storage: Arc<ServerlessStorage>,
}

impl Echo {
    /// Create echo handler.
    pub async fn new(kit: HandlerKit) -> Self {
        println!("Creating echo function: {:?}", kit.instance_info);
        Echo {
            instance_info: kit.instance_info,
            serverless_storage: kit.serverless_storage.unwrap(),
        }
    }
}

#[async_trait::async_trait]
impl ServerlessHandler for Echo {
    /// Handle message.
    async fn handle(&self, meta: String, payload: Vec<u8>) -> (String, Vec<u8>) {
        println!("Echo Handler: {:?}. Meta={meta}. St={}", self.instance_info, self.serverless_storage.exclusive_pool.is_some());
        (meta, payload)
    }

    /// Do checkpointing.
    async fn checkpoint(&self, scaling_state: &ScalingState, terminating: bool) {
        println!("Echo Handler Checkpointing: {scaling_state:?}");
        if terminating {
            println!("Echo Handler Terminating.");
        }
    }
}
