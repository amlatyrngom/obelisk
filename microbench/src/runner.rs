use crate::micro_actor::MicroActorReq;
use crate::micro_kv::{MicroKVReq, MicroKVResp};
use common::{HandlerKit, ScalingState, ServerlessHandler};
use functional::FunctionalClient;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Use to run microbenchmarks.
pub struct BenchRunner {
    actor_client: Arc<FunctionalClient>,
    sim_client: Arc<FunctionalClient>,
    fn_client: Arc<FunctionalClient>,
    kv_client: Arc<FunctionalClient>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RunnerReq {
    Function(u64),
    Actor(u64),
    Sim(u64),
    KV(Vec<MicroKVReq>),
}

impl BenchRunner {
    /// Create.
    pub async fn new(_kit: HandlerKit) -> Self {
        let fn_client = FunctionalClient::new("microbench", "microfn", None, Some(512));
        let actor_client = FunctionalClient::new("microbench", "microactor", Some(0), Some(512));
        let sim_client = FunctionalClient::new("microbench", "simactor", Some(0), Some(512));
        let kv_client = FunctionalClient::new("microbench", "microkv", Some(0), Some(512));
        let (fn_client, actor_client, sim_client, kv_client) =
            tokio::join!(fn_client, actor_client, sim_client, kv_client);

        // Hack to prevent benchmark timeouts.
        actor_client.set_indirect_lambda_retry(false);
        sim_client.set_indirect_lambda_retry(false);
        kv_client.set_indirect_lambda_retry(false);
        BenchRunner {
            fn_client: Arc::new(fn_client),
            actor_client: Arc::new(actor_client),
            sim_client: Arc::new(sim_client),
            kv_client: Arc::new(kv_client),
        }
    }
}

#[async_trait::async_trait]
impl ServerlessHandler for BenchRunner {
    /// Call the benchmark function.
    async fn handle(&self, meta: String, payload: Vec<u8>) -> (String, Vec<u8>) {
        let req: RunnerReq = if let Ok(req) = serde_json::from_str(&meta) {
            req
        } else {
            bincode::deserialize(&payload).unwrap()
        };
        println!("Bench mode: {req:?}");
        match req {
            RunnerReq::Function(count) => self.do_invoke_bench(count).await,
            RunnerReq::Actor(count) => self.do_messaging_bench(count).await,
            RunnerReq::Sim(count) => self.do_sim_bench(count).await,
            RunnerReq::KV(reqs) => self.do_kv_bench(reqs).await,
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
            let req = MicroActorReq::Increment(1, 512);
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

    /// Do kv microbench.
    async fn do_kv_bench(&self, reqs: Vec<MicroKVReq>) -> (String, Vec<u8>) {
        let mut results = Vec::new();
        for req in reqs {
            let start_time = std::time::Instant::now();
            let resp = self
                .kv_client
                .invoke("", &bincode::serialize(&req).unwrap())
                .await;
            let end_time = std::time::Instant::now();
            let duration = end_time.duration_since(start_time);
            if let Ok((metadata, result)) = resp {
                let result = bincode::deserialize::<MicroKVResp>(&result).unwrap();
                let (memory, is_lambda): (i32, bool) = serde_json::from_str(&metadata).unwrap();
                results.push((duration, memory, is_lambda, result));
            }
        }
        (String::new(), bincode::serialize(&results).unwrap())
    }
}
