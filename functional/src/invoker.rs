use common::adaptation::backend::ServiceInfo;
use common::adaptation::ServerfulScalingState;
use common::ServiceInstance;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Responsible for handling invokations in ECS.
#[derive(Clone)]
pub struct Invoker {
    inner: Arc<Mutex<InvokerInner>>,
    http_client: reqwest::Client,
}

/// Inner modification.
struct InvokerInner {
    workers: HashMap<i32, WorkerInfo>,
    terminated: bool,
}

/// Info about worker.
struct WorkerInfo {
    avail_concurrency: i32,
}

#[async_trait::async_trait]
impl ServiceInstance for Invoker {
    /// Pass the call to a worker if possible.
    /// Otherwise return an error.
    async fn call(&self, _meta: String, arg: Vec<u8>) -> (String, Vec<u8>) {
        // Find worker with available concurrency.
        let idx = {
            let mut inner = self.inner.lock().await;
            if inner.terminated {
                // Panic to signal WARP to return an error.
                panic!("Terminated.");
            };
            let mut idx = None;
            for (worker_idx, worker) in inner.workers.iter_mut() {
                if worker.avail_concurrency > 0 {
                    worker.avail_concurrency -= 1;
                    idx = Some(*worker_idx);
                    break;
                }
            }
            if let Some(idx) = idx {
                idx
            } else {
                return (false.to_string(), vec![]);
            }
        };
        // Directly invoke worker.
        let port = 37001 + idx;
        let url = format!("http://localhost:{port}/invoke");
        println!("Url: {url}");
        let resp = self
            .http_client
            .post(url)
            .header("Content-Type", "application/json")
            .body(arg)
            .send()
            .await;
        {
            // Free concurrency after invocation is over.
            // Do this BEFORE unwrap().
            let mut inner = self.inner.lock().await;
            inner.workers.get_mut(&idx).unwrap().avail_concurrency += 1;
        }
        let resp = resp.unwrap();
        let body = {
            let body = resp.bytes().await.unwrap();
            body.to_vec()
        };
        (true.to_string(), body)
    }

    // Just mark as terminated
    async fn terminate(&self) {
        let mut inner = self.inner.lock().await;
        inner.terminated = true;
    }

    async fn custom_info(&self, _scaling_state: &ServerfulScalingState) -> Value {
        Value::Null
    }
}

impl Invoker {
    /// Make invoker.
    pub async fn new(svc_info: Arc<ServiceInfo>) -> Self {
        // Print svc info.
        println!("Invoker info: {svc_info:?}");
        // Read env variables.
        let num_workers = std::env::var("NUM_WORKERS").unwrap().parse().unwrap();
        let concurrency = std::env::var("CONCURRENCY").unwrap().parse().unwrap();
        let timeout = std::env::var("TIMEOUT").unwrap().parse().unwrap();
        // Make workers.
        let mut workers = HashMap::new();
        for i in 0..num_workers {
            workers.insert(
                i,
                WorkerInfo {
                    avail_concurrency: concurrency,
                },
            );
        }
        let inner = Arc::new(Mutex::new(InvokerInner {
            workers,
            terminated: false,
        }));
        // Make client.
        let http_client = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(1))
            .timeout(std::time::Duration::from_secs(timeout))
            .build()
            .unwrap();
        // Finalize.
        Invoker { http_client, inner }
    }
}
