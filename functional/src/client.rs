use super::rescaler::FunctionalMetric;
use base64::{engine::general_purpose, Engine as _};
use common::deployment::lambda;
use common::scaling_state::ScalingStateManager;
use common::wrapper::WrapperMessage;
use common::{HandlingResp, MetricsManager};
use std::sync::{Arc, Mutex};

const NUM_INDIRECT_RETRIES: u64 = 50;
const INDIRECT_WAIT_TIME_SECS: f64 = 0.02;

/// FunctionalClient.
#[derive(Clone)]
pub struct FunctionalClient {
    namespace: String,
    identifier: String,
    id: Option<usize>,
    lambda_client: aws_sdk_lambda::Client,
    s3_client: aws_sdk_s3::Client,
    direct_client: reqwest::Client,
    metrics_manager: Arc<MetricsManager>,
    scaling_manager: Arc<ScalingStateManager>,
    caller_mem: i32,
    last_force_refresh: Arc<Mutex<std::time::Instant>>,
}

impl FunctionalClient {
    /// Create client.
    pub async fn new(
        namespace: &str,
        name: &str,
        id: Option<usize>,
        caller_mem: Option<i32>,
    ) -> Self {
        let direct_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10)) // TODO: set to correct value.
            .connect_timeout(std::time::Duration::from_millis(100)) // Short on purpose.
            .build()
            .unwrap();
        let shared_config = aws_config::load_from_env().await;
        // Make Lambda.
        let lambda_config = aws_sdk_lambda::config::Builder::from(&shared_config)
            .retry_config(
                aws_sdk_lambda::config::retry::RetryConfig::standard()
                    .with_initial_backoff(std::time::Duration::from_millis(10)) // On avg: 25ms, 50ms, 100ms, ....
                    .with_max_attempts(10),
            )
            .build();
        let lambda_client = aws_sdk_lambda::Client::from_conf(lambda_config);
        // Make S3 client.
        let s3_client = aws_sdk_s3::Client::new(&shared_config);
        // Choose correct handler.
        let identifier = id.map(|i| format!("{name}{i}")).unwrap_or(name.into());
        if identifier != name {
            lambda::LambdaDeployment::duplicate_handler_lambda(
                &lambda_client,
                namespace,
                name,
                id.unwrap(),
                Some(1),
            )
            .await;
        }
        let metrics_manager = MetricsManager::new("functional", namespace, &identifier).await;
        let scaling_manager = ScalingStateManager::new("functional", namespace, &identifier).await;
        scaling_manager.start_refresh_thread().await;
        scaling_manager.start_rescaling_thread().await;
        metrics_manager.start_metrics_pushing_thread().await;

        FunctionalClient {
            namespace: namespace.into(),
            identifier,
            id,
            direct_client,
            lambda_client,
            s3_client,
            metrics_manager: Arc::new(metrics_manager),
            scaling_manager: Arc::new(scaling_manager),
            caller_mem: caller_mem.unwrap_or(512),
            last_force_refresh: Arc::new(Mutex::new(std::time::Instant::now())),
        }
    }

    /// Log a metric with a given probability.
    async fn log_metrics(&self, resp: &HandlingResp) {
        // println!("Resp Duration: {:?}.", resp.duration);
        let metric = FunctionalMetric {
            duration: resp.duration,
            mem_size_mb: resp.mem_size_mb,
            caller_mem: self.caller_mem,
            cpu_usage: resp.cpu_usage,
            mem_usage: resp.mem_usage,
        };
        let metric = bincode::serialize(&metric).unwrap();
        self.metrics_manager.accumulate_metric(metric).await;
    }

    /// Get invoker url.
    pub async fn get_invoker_url(&self) -> Option<String> {
        let scaling_state = self.scaling_manager.current_scaling_state().await;
        // println!("FunctionalClient::get_invoker_url: {scaling_state:?}");
        if let Some(peers) = scaling_state.subsys_state.peers.get("invoker") {
            let mut instances = peers.values().collect::<Vec<_>>();
            instances.sort_by_key(|k| k.join_time);
            let instance = instances.last();
            if let Some(instance) = instance {
                instance.instance_info.public_url.clone()
            } else {
                None
            }
        } else {
            None
        }
    }

    pub async fn get_handler_url(&self) -> Option<String> {
        let scaling_state = self.scaling_manager.current_scaling_state().await;
        // println!("FunctionalClient::get_invoker_url: {scaling_state:?}");
        if let Some(peers) = scaling_state.handler_state.as_ref().map(|s| &s.peers) {
            let mut instances = peers.values().collect::<Vec<_>>();
            instances.sort_by_key(|k| k.join_time);
            let instance = instances.last();
            if let Some(instance) = instance {
                instance.instance_info.public_url.clone()
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Invoke a lambda.
    pub async fn invoke_lambda_handler(
        &self,
        meta: &str,
        payload: &[u8],
    ) -> Result<(HandlingResp, Vec<u8>), String> {
        let fn_name =
            lambda::LambdaDeployment::handler_function_name(&self.namespace, &self.identifier);
        // println!("FunctionalClient::invoke_lambda: {fn_name}.");
        let payload = general_purpose::STANDARD_NO_PAD.encode(payload);
        let meta = WrapperMessage::HandlerMessage { meta: meta.into() };
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
        let (resp, payload): (HandlingResp, String) = serde_json::from_slice(&resp).unwrap();
        let payload = general_purpose::STANDARD_NO_PAD.decode(payload).unwrap();
        Ok((resp, payload))
    }

    /// Wake the lambda function up.
    pub async fn wake_lambda(&self) -> Result<(), String> {
        let fn_name =
            lambda::LambdaDeployment::handler_function_name(&self.namespace, &self.identifier);
        let arg: (WrapperMessage, String) = (WrapperMessage::IndirectMessage, String::new());
        let arg = serde_json::to_vec(&arg).unwrap();
        let arg = aws_smithy_types::Blob::new(arg);
        let lambda_client = self.lambda_client.clone();
        tokio::spawn(async move {
            let _resp = lambda_client
                .invoke()
                .function_name(&fn_name)
                .payload(arg.clone())
                .send()
                .await
                .map_err(|x| format!("{x:?}"));
        });
        Ok(())
    }

    /// Try direct invocation.
    pub async fn invoke_http_invoker(
        &self,
        meta: &str,
        payload: &[u8],
    ) -> Option<(HandlingResp, Vec<u8>)> {
        // Find invoker url.
        let url = self.get_invoker_url().await;
        if url.is_none() {
            return None;
        }
        let url = url.unwrap();
        // println!("FunctionalClient::invoke_direct. Url={url:?}");
        // Double wrap (client -> invoker -> handler).
        let meta = WrapperMessage::HandlerMessage { meta: meta.into() };
        let meta = serde_json::to_string(&meta).unwrap();
        let meta = WrapperMessage::HandlerMessage { meta };
        let meta = serde_json::to_string(&meta).unwrap();
        let resp = self
            .direct_client
            .post(&url)
            .header("Content-Type", "application/octect-stream")
            .header("content-length", payload.len())
            .header("obelisk-meta", meta)
            .body(payload.to_vec())
            .send()
            .await;
        // Return in case of success.
        if let Ok(resp) = resp {
            if resp.status().as_u16() != 200 {
                println!("{resp:?}");
                return None;
            }
            // Resp of invoker.
            let resp_meta = resp.headers().get("obelisk-meta").unwrap();
            let resp_meta: HandlingResp =
                serde_json::from_str(resp_meta.to_str().unwrap()).unwrap();
            // Resp of function.
            if resp_meta.meta.is_empty() {
                // Invalid.
                None
            } else {
                // Valid.
                let resp_meta: HandlingResp = serde_json::from_str(&resp_meta.meta).unwrap();
                let body = resp.bytes().await.unwrap().to_vec();
                Some((resp_meta, body))
            }
        } else {
            println!("HTTP error: {resp:?}");
            None
        }
    }

    /// Directly invoke handler.
    pub async fn invoke_http_handler(
        &self,
        meta: &str,
        payload: &[u8],
    ) -> Result<(HandlingResp, Vec<u8>), String> {
        // Find url.
        let url = self.get_handler_url().await;
        if url.is_none() {
            return Err("unavailable".into());
        }
        let url = url.unwrap();
        // Wrap message.
        let meta = WrapperMessage::HandlerMessage { meta: meta.into() };
        let meta = serde_json::to_string(&meta).unwrap();
        // Send.
        let resp = self
            .direct_client
            .post(&url)
            .header("Content-Type", "application/octect-stream")
            .header("content-length", payload.len())
            .header("obelisk-meta", meta)
            .body(payload.to_vec())
            .send()
            .await;
        // Return in case of success.
        if let Ok(resp) = resp {
            // Check code.
            if resp.status().as_u16() != 200 {
                let err = common::debug_format!()(resp);
                println!("HTTP Err: {err:?}");
                return Err(err);
            }
            // Resp of invoker.
            let resp_meta = resp.headers().get("obelisk-meta").unwrap();
            let resp_meta: HandlingResp =
                serde_json::from_str(resp_meta.to_str().unwrap()).unwrap();
            let body = resp.bytes().await.unwrap().to_vec();
            Ok((resp_meta, body))
        } else {
            println!("HTTP error: {resp:?}");
            Err("httperr".into())
        }
    }

    /// Indirect invokation.
    pub async fn invoke_indirect(
        &self,
        meta: &str,
        payload: &[u8],
    ) -> Result<(HandlingResp, Vec<u8>), String> {
        let msg_id: String = uuid::Uuid::new_v4().to_string();
        // Wake up messaging function.
        let _ = self.wake_lambda().await;
        // Write message to S3.
        let (send_prefix, recv_prefix) =
            common::ServerlessWrapper::messaging_prefix(&self.namespace, &self.identifier);
        let s3_send_key = format!("{send_prefix}/{msg_id}");
        println!("Sending to S3: {s3_send_key:?}");
        let s3_recv_key = format!("{recv_prefix}/{msg_id}");
        let body: (String, String, Vec<u8>) = (msg_id.clone(), meta.into(), payload.into());
        let body = tokio::task::block_in_place(move || bincode::serialize(&body).unwrap());
        let body = aws_sdk_s3::primitives::ByteStream::from(body);
        let _resp = self
            .s3_client
            .put_object()
            .bucket(&common::bucket_name())
            .key(&s3_send_key)
            .body(body)
            .send()
            .await
            .map_err(common::debug_format!())?;
        println!("Sent to S3: {s3_send_key:?}");

        // Repeatedly try reading response and waking up messaging function if response not found.
        for _n in 0..NUM_INDIRECT_RETRIES {
            // Wait for processing.
            tokio::time::sleep(std::time::Duration::from_secs_f64(INDIRECT_WAIT_TIME_SECS)).await;
            // Try reading.
            let resp = self
                .s3_client
                .get_object()
                .bucket(&common::bucket_name())
                .key(&s3_recv_key)
                .send()
                .await;
            match resp {
                Ok(resp) => {
                    // Delete response and return it.
                    let s3_client = self.s3_client.clone();
                    tokio::spawn(async move {
                        let _ = s3_client
                            .delete_object()
                            .bucket(&common::bucket_name())
                            .key(&s3_recv_key)
                            .send()
                            .await;
                    });
                    let body: Vec<u8> = resp.body.collect().await.unwrap().into_bytes().to_vec();
                    let (recv_msg_id, meta, payload): (String, HandlingResp, Vec<u8>) =
                        bincode::deserialize(&body).unwrap();
                    assert_eq!(recv_msg_id, msg_id);
                    return Ok((meta, payload));
                }
                Err(_) => {
                    // Wake up messaging function in case it is not active.
                    let _ = self.wake_lambda().await;
                }
            }
        }
        Err("timeout after many retries".into())
    }

    /// Internal invoke. Also returns whether a direct call as made.
    pub async fn invoke_function_internal(
        &self,
        meta: &str,
        payload: &[u8],
    ) -> Result<(String, Vec<u8>, bool), String> {
        // Try direct call.
        let resp = self.invoke_http_invoker(meta, payload).await;
        // If direct not success, try lambda.
        let (resp, payload, direct_call) = if let Some((resp, payload)) = resp {
            (resp, payload, true)
        } else {
            let (resp, payload) = self.invoke_lambda_handler(meta, payload).await?;
            (resp, payload, false)
        };
        // println!("FunctionalClient::invoke_function_internal. Resp: {resp:?}");
        // Log metrics.
        self.log_metrics(&resp).await;
        // Respond
        Ok((resp.meta, payload, direct_call))
    }

    /// Invoke an actor.
    pub async fn invoke_actor_internal(
        &self,
        meta: &str,
        payload: &[u8],
    ) -> Result<(String, Vec<u8>, bool, bool), String> {
        // Try fast path with direct http message.
        let (resp, was_http) = match self.invoke_http_handler(meta, payload).await {
            Ok(resp) => (Some(resp), true),
            Err(e) => {
                // If no container running, So try direct lambda invokation.
                if e.contains("avail") {
                    let resp = self.invoke_lambda_handler(meta, payload).await;
                    match resp {
                        Ok(resp) => (Some(resp), false),
                        Err(e) => {
                            println!("{e:?}");
                            // Check if should refresh scaling state.
                            let refresh = {
                                let mut last_force_refresh =
                                    self.last_force_refresh.lock().unwrap();
                                let now = std::time::Instant::now();
                                if now.duration_since(*last_force_refresh).as_secs() > 1 {
                                    *last_force_refresh = now;
                                    true
                                } else {
                                    false
                                }
                            };
                            if refresh {
                                let scaling_manager = self.scaling_manager.clone();
                                tokio::spawn(async move {
                                    let _ = scaling_manager.retrieve_scaling_state().await;
                                });
                            }
                            (None, false)
                        }
                    }
                } else {
                    (None, false)
                }
            }
        };
        // If direct failed, try indirect.
        let (resp, payload, was_direct) = if let Some((resp, payload)) = resp {
            (resp, payload, true)
        } else {
            let (resp, payload) = self.invoke_indirect(meta, payload).await?;
            (resp, payload, false)
        };
        // Log metrics.
        self.log_metrics(&resp).await;
        // Respond.
        Ok((resp.meta, payload, was_http, was_direct))
    }

    /// Call a function.
    pub async fn invoke(&self, meta: &str, payload: &[u8]) -> Result<(String, Vec<u8>), String> {
        if self.id.is_none() {
            let (resp, payload, _) = self.invoke_function_internal(meta, payload).await?;
            Ok((resp, payload))
        } else {
            let (resp, payload, _, _) = self.invoke_actor_internal(meta, payload).await?;
            Ok((resp, payload))
        }
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn simple_fn_invoke_test() {
        run_simple_invoke_test("echofn", None).await;
    }

    async fn run_simple_invoke_test(name: &str, id: Option<usize>) {
        let fc = super::FunctionalClient::new("functional", name, id, Some(1024)).await;
        let resp = fc.invoke("0.5", b"Ngom").await;
        println!("Resp: {resp:?}");
        // Try invoking rescaler.
        // let resp = fc.scaling_manager.invoke_rescaler(&fc.lambda_client).await;
        // println!("Resp: {resp:?}");
    }

    #[tokio::test]
    async fn simple_fn_rescale_test() {
        run_simple_rescale_test("echofn", None).await;
    }

    async fn run_simple_rescale_test(name: &str, id: Option<usize>) {
        let is_fn = id.is_none();
        let fc = super::FunctionalClient::new("functional", name, id, Some(1024)).await;
        // Do the following for 100 seconds.
        let sleep_time_secs = 1.0;
        let max_scaling_duration = 100.0;
        let num_rounds = (max_scaling_duration / sleep_time_secs) as i32;
        println!("Calling enough times to scale up.");
        for _ in 0..num_rounds {
            let start_time = std::time::Instant::now();
            let resp = fc.invoke(&sleep_time_secs.to_string(), b"Ngom").await;
            let end_time = std::time::Instant::now();
            let duration = end_time.duration_since(start_time);
            println!("Resp: {resp:?}. Duration: {duration:?}.");
        }
        // Now try again. This time, count the direct calls.
        let scaling_state = fc.scaling_manager.current_scaling_state().await;
        if is_fn {
            let invoker_scale = *scaling_state
                .subsys_state
                .service_scales
                .get("invoker")
                .unwrap();
            assert_eq!(invoker_scale, 1);
        }
        let mut num_directs = 0;
        println!("Trying to count direct calls.");
        for _ in 0..num_rounds {
            let start_time = std::time::Instant::now();
            let resp = if is_fn {
                let resp = fc
                    .invoke_function_internal(&sleep_time_secs.to_string(), b"Ngom")
                    .await;
                if let Ok((meta, _, is_direct)) = resp {
                    num_directs += is_direct as i32;
                    Some(meta)
                } else {
                    None
                }
            } else {
                let resp = fc
                    .invoke_actor_internal(&sleep_time_secs.to_string(), b"Ngom")
                    .await;
                if let Ok((meta, _, was_http, _)) = resp {
                    num_directs += was_http as i32;
                    Some(meta)
                } else {
                    None
                }
            };
            let end_time = std::time::Instant::now();
            let duration = end_time.duration_since(start_time);
            println!("Resp: {resp:?}. Duration: {duration:?}.");
        }
        // There should be enough direct calls.
        assert!(num_directs > num_rounds / 2);
        // Now do nothing for a while.
        println!("Sleeping long enough to scale down");
        tokio::time::sleep(std::time::Duration::from_secs_f64(max_scaling_duration)).await;
        let scaling_state = fc.scaling_manager.current_scaling_state().await;
        if is_fn {
            let invoker_scale = *scaling_state
                .subsys_state
                .service_scales
                .get("invoker")
                .unwrap();
            assert_eq!(invoker_scale, 0);
        }
    }

    #[tokio::test]
    async fn concurrent_fn_rescale_test() {
        run_concurrent_rescale_test("echofn", None).await;
    }

    /// Assumes echofn's concurrency=2.
    async fn run_concurrent_rescale_test(name: &str, id: Option<usize>) {
        let is_fn = id.is_none();
        let fc = super::FunctionalClient::new("functional", name, id, Some(1024)).await;
        // Do the following for 100 seconds.
        let sleep_time_secs = 1.0;
        let sleep_time = sleep_time_secs.to_string();
        let max_scaling_duration = 100.0;
        let num_rounds = (max_scaling_duration / sleep_time_secs) as i32;
        // Calling at a rate of 1*concurrency. Should scale up to 1.
        for _ in 0..num_rounds {
            let start_time = std::time::Instant::now();
            let resp = if is_fn {
                let resp1 = fc.invoke(&sleep_time, b"Ngom");
                let resp2 = fc.invoke(&sleep_time, b"Ngom");
                let resp = tokio::join!(resp1, resp2);
                resp.0
            } else {
                fc.invoke(&sleep_time, b"Ngom").await
            };
            let end_time = std::time::Instant::now();
            let duration = end_time.duration_since(start_time);
            println!("Resp: {resp:?}. Duration: {duration:?}.");
        }

        let scaling_state = fc.scaling_manager.current_scaling_state().await;
        let scaling_info = &scaling_state.handler_state.as_ref().unwrap().scaling_info;
        println!("Scaling Info: {scaling_info:?}");
        if is_fn {
            let invoker_scale = *scaling_state
                .subsys_state
                .service_scales
                .get("invoker")
                .unwrap();
            assert_eq!(invoker_scale, 1);
        }
        let handler_scale = *scaling_state
            .handler_state
            .as_ref()
            .unwrap()
            .handler_scales
            .get(&512)
            .unwrap();
        assert_eq!(handler_scale, 1);
        // Calling at a rate of 1.5*concurrency. Should scale up to 2.
        for _ in 0..num_rounds {
            let start_time = std::time::Instant::now();
            let resp1 = fc.invoke(&sleep_time, b"Ngom");
            let resp2 = fc.invoke(&sleep_time, b"Ngom");
            let resp3 = fc.invoke(&sleep_time, b"Ngom");
            let resp4 = fc.invoke(&sleep_time, b"Ngom");
            let resp = tokio::join!(resp1, resp2, resp3, resp4);
            let end_time = std::time::Instant::now();
            let duration = end_time.duration_since(start_time);
            println!("Resp: {resp:?}. Duration: {duration:?}.");
        }
        let scaling_state = fc.scaling_manager.current_scaling_state().await;
        let scaling_info = &scaling_state.handler_state.as_ref().unwrap().scaling_info;
        println!("Scaling Info: {scaling_info:?}");
        let handler_scale = *scaling_state
            .handler_state
            .as_ref()
            .unwrap()
            .handler_scales
            .get(&512)
            .unwrap();
        if is_fn {
            assert_eq!(handler_scale, 2);
        } else {
            assert_eq!(handler_scale, 1);
        }
        // Calling back at a rate of 1*concurrency. Should scale down to 1.
        for _ in 0..num_rounds {
            let start_time = std::time::Instant::now();
            let resp1 = fc.invoke(&sleep_time, b"Ngom");
            let resp2 = fc.invoke(&sleep_time, b"Ngom");
            let resp = tokio::join!(resp1, resp2);
            let end_time = std::time::Instant::now();
            let duration = end_time.duration_since(start_time);
            println!("Resp: {resp:?}. Duration: {duration:?}.");
        }
        let scaling_state = fc.scaling_manager.current_scaling_state().await;

        let handler_scale = *scaling_state
            .handler_state
            .as_ref()
            .unwrap()
            .handler_scales
            .get(&512)
            .unwrap();
        assert_eq!(handler_scale, 1);
    }

    #[tokio::test]
    async fn simple_fn_compute_bound_test() {
        run_simple_compute_bound_test("echofn", None).await;
    }

    /// Simple compute test.
    async fn run_simple_compute_bound_test(name: &str, id: Option<usize>) {
        let fc = super::FunctionalClient::new("functional", name, id, Some(1024)).await;
        // Do the following for 100 seconds.
        let sleep_time_secs = 0.001;
        let compute_time_secs = 1.0 - sleep_time_secs;
        let fn_arg = (sleep_time_secs, compute_time_secs);
        let fn_arg = serde_json::to_string(&fn_arg).unwrap();
        let max_scaling_duration = 100.0 * 5.0;
        let num_rounds = (max_scaling_duration / (sleep_time_secs + compute_time_secs)) as i32;
        // Calling at a rate of 1*concurrency. Should scale up to 1.
        for _ in 0..num_rounds {
            let start_time = std::time::Instant::now();
            let resp = fc.invoke(&fn_arg, b"Ngom").await;
            let end_time = std::time::Instant::now();
            let duration = end_time.duration_since(start_time);
            println!("Resp: {resp:?}. Duration: {duration:?}.");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn simple_actor_invoke_test() {
        run_simple_invoke_test("echoactor", Some(0)).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn simple_actor_rescale_test() {
        run_simple_rescale_test("echoactor", Some(0)).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn concurrent_actor_rescale_test() {
        run_concurrent_rescale_test("echoactor", Some(0)).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn simple_actor_compute_bound_test() {
        run_simple_compute_bound_test("echoactor", Some(0)).await;
    }
}
