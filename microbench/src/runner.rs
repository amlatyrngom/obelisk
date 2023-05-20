use crate::micro_actor::{MicroActorReq, MicroActorResp};
use common::FunctionInstance;
use functional::FunctionalClient;
use messaging::MessagingClient;
use serde_json::Value;
use std::sync::Arc;

/// Use to run microbenchmarks.
pub struct BenchRunner {
    mc: Arc<MessagingClient>,
    fc: Arc<FunctionalClient>,
}

impl BenchRunner {
    /// Create.
    pub async fn new() -> Self {
        let fc = Arc::new(FunctionalClient::new("microbench").await);
        let mc = Arc::new(MessagingClient::new("microbench", "main").await);
        BenchRunner { fc, mc }
    }
}

#[async_trait::async_trait]
impl FunctionInstance for BenchRunner {
    /// Call the benchmark function.
    async fn invoke(&self, arg: Value) -> Value {
        let bench: String = serde_json::from_value(arg).unwrap();
        println!("Bench mode: {bench}");
        if bench == "invoke" {
            self.do_invoke_bench().await
        } else if bench == "message" {
            self.do_messaging_bench().await
        } else {
            let err = format!("Unknown mode, {bench}");
            serde_json::json!({
                "error": err,
            })
        }
    }
}

impl BenchRunner {
    /// Do stateless microbench.
    async fn do_invoke_bench(&self) -> Value {
        let arg = serde_json::to_vec("foo").unwrap();
        let start_time = std::time::Instant::now();
        let mut num_direct = 0;
        let mut num_indirect = 0;
        for _ in 0..10 {
            let resp = self.fc.invoke_internal(&arg).await;
            if let Ok((_, is_direct)) = resp {
                if is_direct {
                    num_direct += 1;
                } else {
                    num_indirect += 1;
                }
            }
        }
        let end_time = std::time::Instant::now();
        let duration = end_time.duration_since(start_time);
        let duration_ms = duration.as_millis() as u64;
        let resp = (duration_ms, num_direct, num_indirect);
        serde_json::to_value(resp).unwrap()
    }

    /// Do actor microbench.
    async fn do_messaging_bench(&self) -> Value {
        let mut retrieve_times = Vec::new();
        let mut increment_times = Vec::new();
        let mut vals: Vec<i64> = Vec::new();
        for _ in 0..1 {
            // Retrieve.
            let req = MicroActorReq::Retrieve;
            let req = serde_json::to_string(&req).unwrap();
            let start_time = std::time::Instant::now();
            let resp = self.mc.send_message(&req, &[]).await;
            let end_time = std::time::Instant::now();
            let duration = end_time.duration_since(start_time);
            if let Some((resp, _)) = resp {
                let resp: MicroActorResp = serde_json::from_str(&resp).unwrap();
                vals.push(resp.val);
                retrieve_times.push(duration);
            }
            // Increment
            let req = MicroActorReq::Increment(10);
            let req = serde_json::to_string(&req).unwrap();
            let start_time = std::time::Instant::now();
            let resp = self.mc.send_message(&req, &[]).await;
            let end_time = std::time::Instant::now();
            let duration = end_time.duration_since(start_time);
            if let Some((resp, _)) = resp {
                let resp: MicroActorResp = serde_json::from_str(&resp).unwrap();
                vals.push(resp.val);
                increment_times.push(duration);
            }
        }
        let resp = (retrieve_times, increment_times, vals);
        serde_json::to_value(resp).unwrap()
    }
}
