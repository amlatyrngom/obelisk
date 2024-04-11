use base64::{engine::general_purpose, Engine as _};
use common::deployment::lambda;
use common::{
    HandlerKit, HandlingResp, InstanceInfo, ScalingState, ServerlessHandler, WrapperMessage,
};
use std::{collections::HashMap, sync::atomic, sync::Arc};
use tokio::sync::Mutex;

/// Responsible for handling invokations in ECS.
#[derive(Clone)]
pub struct Invoker {
    inner: Arc<Mutex<InvokerInner>>,
    direct_client: reqwest::Client,
    lambda_client: aws_sdk_lambda::Client,
    round_robin_counter: Arc<atomic::AtomicUsize>,
    namespace: String,
    name: String,
}

/// Inner modification.
struct InvokerInner {
    workers: HashMap<String, WorkerInfo>,
}

/// Info about worker.
#[derive(Clone, Debug)]
struct WorkerInfo {
    avail_concurrency: i32,
    join_time: chrono::DateTime<chrono::Utc>,
    info: InstanceInfo,
}

#[async_trait::async_trait]
impl ServerlessHandler for Invoker {
    /// Pass the call to a worker if possible.
    /// Otherwise return an error.
    async fn handle(&self, meta: String, payload: Vec<u8>) -> (String, Vec<u8>) {
        match self.route_request(&meta, &payload).await {
            Ok((resp_meta, resp_payload)) => (resp_meta, resp_payload),
            Err(e) => {
                println!("Routing error: {e:?}");
                match self.invoke_lambda(&meta, &payload).await {
                    Ok((resp_meta, resp_payload)) => (resp_meta, resp_payload),
                    Err(e) => {
                        println!("Lambda invoke error: {e:?}");
                        (String::new(), vec![])
                    }
                }
            }
        }
    }

    /// Do checkpointing.
    async fn checkpoint(&self, scaling_state: &ScalingState, terminating: bool) {
        if terminating {
            println!("Invoker Terminating.");
            return;
        }
        println!("Invoker Checkpointing: {scaling_state:?}");
        let mut new_workers = HashMap::new();
        let handler_state = scaling_state.handler_state.as_ref().unwrap();
        let mut inner = self.inner.lock().await;
        for (peer_id, handler) in &handler_state.peers {
            let avail_concurrency = if let Some(peer) = inner.workers.get(peer_id) {
                peer.avail_concurrency
            } else {
                handler_state.handler_spec.concurrency
            };

            new_workers.insert(
                peer_id.clone(),
                WorkerInfo {
                    avail_concurrency,
                    join_time: handler.join_time,
                    info: handler.instance_info.clone(),
                },
            );
        }
        inner.workers = new_workers;
    }
}

impl Invoker {
    /// Make invoker.
    pub async fn new(kit: HandlerKit) -> Self {
        // Print svc info.
        let instance_info = kit.instance_info;
        println!("Invoker info: {instance_info:?}");
        let inner = Arc::new(Mutex::new(InvokerInner {
            workers: HashMap::new(),
        }));
        // Make lambda client.
        let shared_config = aws_config::load_from_env().await;
        let lambda_client = aws_sdk_lambda::Client::new(&shared_config);
        // Make direct client.
        let direct_client = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_millis(10)) // Same region, so should be small.
            .timeout(std::time::Duration::from_secs(1))
            .build()
            .unwrap();
        // Finalize.
        let round_robin_counter = Arc::new(atomic::AtomicUsize::new(0));
        let namespace = instance_info.namespace.clone();
        let name = instance_info.identifier.clone();
        Invoker {
            direct_client,
            lambda_client,
            inner,
            round_robin_counter,
            namespace,
            name,
        }
    }

    /// Try direct invocation.
    pub async fn invoke_direct(
        &self,
        url: &str,
        meta: &str,
        payload: &[u8],
    ) -> Result<(String, Vec<u8>), String> {
        println!("Invoker Invoke Direct: {url}");
        // Send request.
        let resp = self
            .direct_client
            .post(url)
            .timeout(std::time::Duration::from_secs(1)) // TODO: Set to correct value
            .header("Content-Type", "application/octect-stream")
            .header("content-length", payload.len())
            .header("obelisk-meta", meta)
            .body(payload.to_vec())
            .send()
            .await
            .map_err(common::debug_format!())?;
        // Resp of invoker.
        let resp_meta = resp.headers().get("obelisk-meta").unwrap();
        let resp_meta = resp_meta.to_str().unwrap().to_string();
        let resp_payload = resp.bytes().await.unwrap().to_vec();
        Ok((resp_meta, resp_payload))
    }

    /// Route to instance.
    async fn route_request(&self, meta: &str, payload: &[u8]) -> Result<(String, Vec<u8>), String> {
        // Attempt to balance load (not perfect).
        let counter = self
            .round_robin_counter
            .fetch_add(1, atomic::Ordering::AcqRel);
        // Find available worker.
        let worker = {
            let now = chrono::Utc::now();
            let mut inner = self.inner.lock().await;
            {
                // Just for debugging.
                let workers: Vec<_> = inner
                    .workers
                    .iter()
                    .map(|(x, w)| (x.clone(), w.avail_concurrency))
                    .collect();
                println!("All Workers: {workers:?}");
            }
            let mut avail_workers = inner
                .workers
                .iter_mut()
                .filter(|(_, w)| {
                    w.avail_concurrency > 0
                        && now.signed_duration_since(w.join_time).num_seconds() > 30
                })
                .map(|(_, w)| w)
                .collect::<Vec<_>>();
            avail_workers.sort_by_key(|k| k.join_time);
            let l = avail_workers.len();
            if l == 0 {
                None
            } else {
                let worker = avail_workers.get_mut(counter % l).unwrap();
                worker.avail_concurrency -= 1;
                Some(worker.clone())
            }
        };
        // Direct invoke if worker found.
        if let Some(worker) = worker {
            // TODO: Some kind of infrequent bug here? Sometimes it seems like the worker is never freed.
            println!("Found Worker: {}", worker.info.peer_id);
            let url = worker.info.private_url.clone().unwrap();
            // let url = if let Some(url) = &worker.info.public_url {
            //     // Try using public url to facilitate tests from outside AWS.
            //     url
            // } else {
            //     worker.info.private_url.as_ref().unwrap()
            // };
            let start_time = std::time::Instant::now();
            let resp = self.invoke_direct(&url, meta, payload).await;
            let end_time = std::time::Instant::now();
            let duration = end_time.duration_since(start_time);
            println!(
                "Worker {} request duration: {duration:?}",
                worker.info.peer_id
            );
            {
                // Free worker.
                let mut inner = self.inner.lock().await;
                let worker = inner.workers.get_mut(&worker.info.peer_id);
                if let Some(worker) = worker {
                    worker.avail_concurrency += 1;
                }
            }
            resp
        } else {
            Err("No Workers Available".into())
        }
    }

    /// Invoke a lambda.
    pub async fn invoke_lambda(
        &self,
        meta: &str,
        payload: &[u8],
    ) -> Result<(String, Vec<u8>), String> {
        let fn_name = lambda::LambdaDeployment::handler_function_name(&self.namespace, &self.name);
        println!("Invoker::invoke_lambda: {fn_name}.");
        let payload = general_purpose::STANDARD_NO_PAD.encode(payload);
        let meta = serde_json::from_str(meta).unwrap();
        let arg: (WrapperMessage, String) = (meta, payload);
        let arg = serde_json::to_vec(&arg).unwrap();
        let arg = aws_smithy_types::Blob::new(arg);
        let resp = self
            .lambda_client
            .invoke()
            .function_name(&fn_name)
            .payload(arg.clone())
            .send()
            .await
            .map_err(|x| format!("{x:?}"))?;
        if let Some(x) = resp.function_error() {
            if !x.is_empty() {
                return Err(format!("lambda {fn_name} invoke error: {x:?}"));
            }
        }
        let resp: Vec<u8> = resp.payload().unwrap().clone().into_inner();
        let (resp_meta, resp_payload): (HandlingResp, String) =
            serde_json::from_slice(&resp).unwrap();
        let resp_payload = general_purpose::STANDARD_NO_PAD
            .decode(resp_payload)
            .unwrap();
        let resp_meta = serde_json::to_string(&resp_meta).unwrap();
        Ok((resp_meta, resp_payload))
    }
}
