use common::{HandlerKit, ScalingState, ServerlessHandler};

pub struct MicroFunction {}

impl MicroFunction {
    pub async fn new(_kit: HandlerKit) -> Self {
        MicroFunction {}
    }
}

#[async_trait::async_trait]
impl ServerlessHandler for MicroFunction {
    async fn handle(&self, meta: String, payload: Vec<u8>) -> (String, Vec<u8>) {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
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
