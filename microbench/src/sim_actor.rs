use std::time::Duration;

use common::{HandlerKit, ScalingState, ServerlessHandler};

pub struct SimActor {
    sleep_time: Duration,
    metadata: Vec<u8>,
}

impl SimActor {
    pub async fn new(kit: HandlerKit) -> Self {
        let memory = kit.instance_info.mem;
        let sleep_time = if memory <= 2048 {
            30.0
        } else if memory <= 4096 {
            20.0
        } else {
            10.0
        };
        let is_lambda = kit.instance_info.private_url.is_none();
        let metadata = (memory, is_lambda);
        let metadata = serde_json::to_vec(&metadata).unwrap();

        let sleep_time = Duration::from_secs_f64(sleep_time / 1000.0);
        SimActor {
            sleep_time,
            metadata,
        }
    }
}

#[async_trait::async_trait]
impl ServerlessHandler for SimActor {
    async fn handle(&self, meta: String, _payload: Vec<u8>) -> (String, Vec<u8>) {
        // Simulate work.
        tokio::time::sleep(self.sleep_time.clone()).await;
        (meta, self.metadata.clone())
    }

    /// Do checkpointing.
    async fn checkpoint(&self, scaling_state: &ScalingState, terminating: bool) {
        println!("Handler Checkpointing: {scaling_state:?}");
        if terminating {
            println!("Handler Terminating.");
        }
    }
}
