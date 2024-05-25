use common::{HandlerKit, InstanceInfo, ScalingState, ServerlessHandler};
use std::sync::Arc;

pub struct Echo {
    instance_info: Arc<InstanceInfo>,
    _incarnation: i64,
}

impl Echo {
    /// Create echo handler.
    pub async fn new(kit: HandlerKit) -> Self {
        println!("Creating echo function: {:?}", kit.instance_info);
        Echo {
            instance_info: kit.instance_info,
            _incarnation: kit.incarnation,
        }
    }
}

#[async_trait::async_trait]
impl ServerlessHandler for Echo {
    /// Handle message.
    async fn handle(&self, meta: String, payload: Vec<u8>) -> (String, Vec<u8>) {
        println!("Echo Handler: {:?}. Meta={meta}.", self.instance_info,);
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
