use crate::micro_actor::{MicroActorReq, MicroActorResp};
use common::{HandlerKit, ScalingState, ServerlessHandler};
use functional::FunctionalClient;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Use to run microbenchmarks.
pub struct BenchRunner {
    actor_client: Arc<FunctionalClient>,
    fn_client: Arc<FunctionalClient>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RunnerReq {
    Function(u64),
    Actor(u64),
}

impl BenchRunner {
    /// Create.
    pub async fn new(_kit: HandlerKit) -> Self {
        let fn_client =
            Arc::new(FunctionalClient::new("microbench", "microfn", None, Some(512)).await);
        let actor_client =
            Arc::new(FunctionalClient::new("microbench", "microactor", Some(0), Some(512)).await);
        BenchRunner {
            fn_client,
            actor_client,
        }
    }
}

#[async_trait::async_trait]
impl ServerlessHandler for BenchRunner {
    /// Call the benchmark function.
    async fn handle(&self, meta: String, _payload: Vec<u8>) -> (String, Vec<u8>) {
        let req: RunnerReq = serde_json::from_str(&meta).unwrap();
        println!("Bench mode: {req:?}");
        match req {
            RunnerReq::Function(count) => self.do_invoke_bench(count).await,
            RunnerReq::Actor(count) => self.do_messaging_bench(count).await,
        }
    }

    /// Do checkpointing.
    async fn checkpoint(&self, scaling_state: &ScalingState, terminating: bool) {
        println!("Echo Handler Checkpointing: {scaling_state:?}");
        if terminating {
            println!("Echo Handler Terminating.");
        }
    }
}

impl BenchRunner {
    /// Do stateless microbench.
    async fn do_invoke_bench(&self, count: u64) -> (String, Vec<u8>) {
        let mut durations: Vec<std::time::Duration> = Vec::new();
        for _ in 0..count {
            let start_time = std::time::Instant::now();
            let resp = self.fn_client.invoke("foo", &[]).await;
            if let Ok(_resp) = resp {
                let end_time = std::time::Instant::now();
                let duration = end_time.duration_since(start_time);
                durations.push(duration);
            }
        }
        (serde_json::to_string(&durations).unwrap(), vec![])
    }

    /// Do actor microbench.
    async fn do_messaging_bench(&self, count: u64) -> (String, Vec<u8>) {
        let mut retrieve_times = Vec::new();
        let mut increment_times = Vec::new();
        let mut vals: Vec<i64> = Vec::new();
        for _ in 0..count {
            // Retrieve.
            let req = MicroActorReq::Retrieve;
            let req = serde_json::to_string(&req).unwrap();
            let start_time = std::time::Instant::now();
            let resp = self.actor_client.invoke(&req, &[]).await;
            let end_time = std::time::Instant::now();
            let duration = end_time.duration_since(start_time);
            if let Ok((resp, _)) = resp {
                let resp: MicroActorResp = serde_json::from_str(&resp).unwrap();
                vals.push(resp.val);
                retrieve_times.push(duration);
            }
            // Increment
            let req = MicroActorReq::Increment(10);
            let req = serde_json::to_string(&req).unwrap();
            let start_time = std::time::Instant::now();
            let resp = self.actor_client.invoke(&req, &[]).await;
            let end_time = std::time::Instant::now();
            let duration = end_time.duration_since(start_time);
            if let Ok((resp, _)) = resp {
                let resp: MicroActorResp = serde_json::from_str(&resp).unwrap();
                vals.push(resp.val);
                increment_times.push(duration);
            }
        }
        let resp = (retrieve_times, increment_times, vals);
        (serde_json::to_string(&resp).unwrap(), vec![])
    }
}
