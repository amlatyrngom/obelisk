use async_trait::async_trait;
use common::{HandlerKit, ScalingState, ServerlessHandler};

/// Base sleep time in seconds: 20 ms.
const BASE_SLEEP_TIME_S: f64 = 0.020;


pub struct MicroFunction {
    // Metadata to facilitate benchmarking.
    metadata: Vec<u8>,
    speedup: f64,
}

impl MicroFunction {
    pub async fn new(kit: HandlerKit) -> Self {
        let is_lambda = kit.instance_info.private_url.is_none();
        let memory = kit.instance_info.mem;
        let cpu = kit.instance_info.cpus;
        let speedup = cpu as f64 / 1024.0;
        let metadata = (memory, is_lambda);
        let metadata = serde_json::to_vec(&metadata).unwrap();
        MicroFunction { metadata, speedup }
    }
}

#[async_trait::async_trait]
impl ServerlessHandler for MicroFunction {
    async fn handle(&self, meta: String, _payload: Vec<u8>) -> (String, Vec<u8>) {
        // Simulate work.
        let sleep_time: f64 = BASE_SLEEP_TIME_S / self.speedup;
        tokio::time::sleep(std::time::Duration::from_secs_f64(sleep_time)).await;
        (meta, self.metadata.clone())
    }

    /// Do checkpointing.
    async fn checkpoint(&self, scaling_state: &ScalingState, terminating: bool) {
        println!("Echo Handler Checkpointing: {scaling_state:?}");
        if terminating {
            println!("Echo Handler Terminating.");
        }
    }
}

pub struct EchoFn {}

impl EchoFn {
    pub async fn new(_kit: HandlerKit) -> Self {
        EchoFn {}
    }
}

#[async_trait]
impl ServerlessHandler for EchoFn {
    async fn handle(&self, meta: String, payload: Vec<u8>) -> (String, Vec<u8>) {
        (meta, payload)
    }

    async fn checkpoint(&self, _scaling_state: &ScalingState, _terminating: bool) {}
}
