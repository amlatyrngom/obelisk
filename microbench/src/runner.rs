use crate::micro_actor::MicroActorReq;
use common::{HandlerKit, ScalingState, ServerlessHandler};
use functional::FunctionalClient;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Use to run microbenchmarks.
pub struct BenchRunner {
    actor_client: Arc<FunctionalClient>,
    sim_client: Arc<FunctionalClient>,
    fn_client: Arc<FunctionalClient>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RunnerReq {
    Function(u64),
    Actor(u64),
    Sim(u64),
}

impl BenchRunner {
    /// Create.
    pub async fn new(_kit: HandlerKit) -> Self {
        let fn_client =
            Arc::new(FunctionalClient::new("microbench", "microfn", None, Some(512)).await);
        let actor_client =
            Arc::new(FunctionalClient::new("microbench", "microactor", Some(0), Some(512)).await);
        let sim_client =
            Arc::new(FunctionalClient::new("microbench", "simactor", Some(0), Some(512)).await);

        // Hack to prevent benchmark timeouts.
        actor_client.set_indirect_lambda_retry(false);
        sim_client.set_indirect_lambda_retry(false);
        BenchRunner {
            fn_client,
            actor_client,
            sim_client,
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
            RunnerReq::Sim(count) => self.do_sim_bench(count).await,
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
        let mut durations = Vec::new();
        for _ in 0..count {
            let start_time = std::time::Instant::now();
            let resp = self.fn_client.invoke("foo", &[]).await;
            if let Ok((_, metadata)) = resp {
                let end_time = std::time::Instant::now();
                let duration = end_time.duration_since(start_time);
                durations.push((duration, metadata));
            }
        }
        (serde_json::to_string(&durations).unwrap(), vec![])
    }

    /// Do actor microbench.
    async fn do_messaging_bench(&self, count: u64) -> (String, Vec<u8>) {
        let mut durations = Vec::new();
        for _ in 0..count {
            // Increment
            let req = MicroActorReq::Increment(1);
            let req = serde_json::to_string(&req).unwrap();
            let start_time = std::time::Instant::now();
            // TODO: Should probably retry until success.
            let resp = self.actor_client.invoke(&req, &[]).await;
            let end_time = std::time::Instant::now();
            let duration = end_time.duration_since(start_time);
            if let Ok((_, metadata)) = resp {
                // let resp: MicroActorResp = serde_json::from_str(&resp).unwrap();
                // vals.push(resp.val);
                durations.push((duration, metadata));
            }
        }
        // let resp = (retrieve_times, increment_times, vals);
        (serde_json::to_string(&durations).unwrap(), vec![])
    }

    /// Do actor microbench.
    async fn do_sim_bench(&self, count: u64) -> (String, Vec<u8>) {
        let mut durations = Vec::new();
        for _ in 0..count {
            let start_time = std::time::Instant::now();
            // TODO: Should probably retry until success.
            let resp = self.sim_client.invoke("", &[]).await;
            let end_time = std::time::Instant::now();
            let duration = end_time.duration_since(start_time);
            if let Ok((_, metadata)) = resp {
                durations.push((duration, metadata));
            }
        }
        (serde_json::to_string(&durations).unwrap(), vec![])
    }
}
