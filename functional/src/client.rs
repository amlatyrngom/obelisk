use super::rescaler::FunctionalMetric;
use base64::{engine::general_purpose, Engine as _};
use common::deployment::lambda;
use common::scaling_state::ScalingStateManager;
use common::wrapper::WrapperMessage;
use common::{HandlingResp, MetricsManager};
use std::sync::Arc;

/// FunctionalClient.
#[derive(Clone)]
pub struct FunctionalClient {
    namespace: String,
    name: String,
    lambda_client: aws_sdk_lambda::Client,
    direct_client: reqwest::Client,
    metrics_manager: Arc<MetricsManager>,
    scaling_manager: Arc<ScalingStateManager>,
    caller_mem: i32,
}

impl FunctionalClient {
    /// Create client.
    pub async fn new(namespace: &str, name: &str, caller_mem: Option<i32>) -> Self {
        let direct_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10)) // TODO: set to correct value.
            .connect_timeout(std::time::Duration::from_millis(100)) // Short on purpose.
            .build()
            .unwrap();
        let shared_config = aws_config::load_from_env().await;
        let lambda_client = aws_sdk_lambda::Client::new(&shared_config);
        let metrics_manager = MetricsManager::new("functional", namespace, name).await;
        let scaling_manager = ScalingStateManager::new("functional", namespace, name).await;
        scaling_manager.start_refresh_thread().await;
        scaling_manager.start_rescaling_thread().await;
        metrics_manager.start_metrics_pushing_thread().await;

        FunctionalClient {
            namespace: namespace.into(),
            name: name.into(),
            direct_client,
            lambda_client,
            metrics_manager: Arc::new(metrics_manager),
            scaling_manager: Arc::new(scaling_manager),
            caller_mem: caller_mem.unwrap_or(512),
        }
    }

    /// Log a metric with a given probability.
    async fn log_metrics(&self, resp: &HandlingResp) {
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
        if let Some(peer) = scaling_state.subsys_state.peers.get("invoker") {
            let instances = peer.values().collect::<Vec<_>>();
            let instance = instances.first();
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
    pub async fn invoke_lambda(
        &self,
        meta: &str,
        payload: &[u8],
    ) -> Result<(HandlingResp, Vec<u8>), String> {
        let fn_name = lambda::LambdaDeployment::handler_function_name(&self.namespace, &self.name);
        println!("FunctionalClient::invoke_lambda: {fn_name}.");
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

    /// Try direct invocation.
    pub async fn invoke_direct(
        &self,
        meta: &str,
        payload: &[u8],
    ) -> Option<(HandlingResp, Vec<u8>)> {
        // Find random url. TODO: Better load balancing.
        let url = self.get_invoker_url().await;
        if url.is_none() {
            return None;
        }
        let url = url.unwrap();
        println!("FunctionalClient::invoke_direct. Url={url:?}");
        // Double wrap (client -> invoker -> handler).
        let meta = WrapperMessage::HandlerMessage { meta: meta.into() };
        let meta = serde_json::to_string(&meta).unwrap();
        let meta = WrapperMessage::HandlerMessage { meta };
        let meta = serde_json::to_string(&meta).unwrap();
        let resp = self
            .direct_client
            .post(&url)
            .header("Content-Type", "application/octect-stream")
            .header("obelisk-meta", meta)
            .body(payload.to_vec())
            .send()
            .await;
        // Return in case of success.
        if let Ok(resp) = resp {
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

    /// Internal invoke. Also returns whether a direct call as made.
    pub async fn invoke_internal(
        &self,
        meta: &str,
        payload: &[u8],
    ) -> Result<(String, Vec<u8>, bool), String> {
        // Try direct call.
        let resp = self.invoke_direct(meta, payload).await;
        // If direct not success, try lambda.
        let (resp, payload, direct_call) = if let Some((resp, payload)) = resp {
            (resp, payload, true)
        } else {
            let (resp, payload) = self.invoke_lambda(meta, payload).await?;
            (resp, payload, false)
        };
        // println!("FunctionalClient::invoke_internal. Resp: {resp:?}");
        // Log metrics.
        self.log_metrics(&resp).await;
        // Respond
        Ok((resp.meta, payload, direct_call))
    }

    /// Call a function.
    pub async fn invoke(&self, meta: &str, payload: &[u8]) -> Result<(String, Vec<u8>), String> {
        let (resp, payload, _) = self.invoke_internal(meta, payload).await?;
        Ok((resp, payload))
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn simple_invoke_test() {
        run_simple_invoke_test().await;
    }

    async fn run_simple_invoke_test() {
        let fc = super::FunctionalClient::new("functional", "echofn", Some(1024)).await;
        let resp = fc.invoke("0.5", b"Ngom").await;
        println!("Resp: {resp:?}");
        // Try invoking rescaler.
        let resp = fc.scaling_manager.invoke_rescaler(&fc.lambda_client).await;
        println!("Resp: {resp:?}");
    }

    #[tokio::test]
    async fn simple_rescale_test() {
        run_simple_rescale_test().await;
    }

    async fn run_simple_rescale_test() {
        let fc = super::FunctionalClient::new("functional", "echofn", Some(1024)).await;
        // Do the following for 100 seconds.
        let sleep_time_secs = 1.0;
        let max_scaling_duration = 100.0;
        let num_rounds = (max_scaling_duration / sleep_time_secs) as i32;
        println!("Calling enough times to scale up.");
        for _ in 0..num_rounds {
            let start_time = std::time::Instant::now();
            let resp = fc
                .invoke_internal(&sleep_time_secs.to_string(), b"Ngom")
                .await;
            let end_time = std::time::Instant::now();
            let duration = end_time.duration_since(start_time);
            println!("Resp: {resp:?}. Duration: {duration:?}.");
        }
        // Now try again. This time, count the direct calls.
        let scaling_state = fc.scaling_manager.current_scaling_state().await;
        let invoker_scale = *scaling_state
            .subsys_state
            .service_scales
            .get("invoker")
            .unwrap();
        assert_eq!(invoker_scale, 1);
        let mut num_directs = 0;
        println!("Trying to count direct calls.");
        for _ in 0..num_rounds {
            let start_time = std::time::Instant::now();
            let resp = fc
                .invoke_internal(&sleep_time_secs.to_string(), b"Ngom")
                .await;
            let end_time = std::time::Instant::now();
            let duration = end_time.duration_since(start_time);
            println!("Resp: {resp:?}. Duration: {duration:?}.");
            if let Ok((_, _, is_direct)) = resp {
                num_directs += is_direct as i32;
            }
        }
        // There should be enough direct calls.
        assert!(num_directs > num_rounds / 2);
        // Now do nothing for a while.
        println!("Sleeping long enough to scale down");
        tokio::time::sleep(std::time::Duration::from_secs_f64(max_scaling_duration)).await;
        let scaling_state = fc.scaling_manager.current_scaling_state().await;
        let invoker_scale = *scaling_state
            .subsys_state
            .service_scales
            .get("invoker")
            .unwrap();
        assert_eq!(invoker_scale, 0);
    }

    #[tokio::test]
    async fn concurrent_rescale_test() {
        run_concurrent_rescale_test().await;
    }

    /// Assumes echofn's concurrency=2.
    async fn run_concurrent_rescale_test() {
        let fc = super::FunctionalClient::new("functional", "echofn", Some(1024)).await;
        // Do the following for 100 seconds.
        let sleep_time_secs = 1.0;
        let sleep_time = sleep_time_secs.to_string();
        let max_scaling_duration = 100.0;
        let num_rounds = (max_scaling_duration / sleep_time_secs) as i32;
        // Calling at a rate of 1*concurrency. Should scale up to 1.
        for _ in 0..num_rounds {
            let start_time = std::time::Instant::now();
            let resp1 = fc.invoke_internal(&sleep_time, b"Ngom");
            let resp2 = fc.invoke_internal(&sleep_time, b"Ngom");
            let resp = tokio::join!(resp1, resp2);
            let end_time = std::time::Instant::now();
            let duration = end_time.duration_since(start_time);
            println!("Resp: {resp:?}. Duration: {duration:?}.");
        }

        let scaling_state = fc.scaling_manager.current_scaling_state().await;
        let scaling_info = &scaling_state.handler_state.as_ref().unwrap().scaling_info;
        println!("Scaling Info: {scaling_info:?}");
        let invoker_scale = *scaling_state
            .subsys_state
            .service_scales
            .get("invoker")
            .unwrap();
        assert_eq!(invoker_scale, 1);
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
            let resp1 = fc.invoke_internal(&sleep_time, b"Ngom");
            let resp2 = fc.invoke_internal(&sleep_time, b"Ngom");
            let resp3 = fc.invoke_internal(&sleep_time, b"Ngom");
            let resp4 = fc.invoke_internal(&sleep_time, b"Ngom");
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
        assert_eq!(handler_scale, 2);
        // Calling back at a rate of 1*concurrency. Should scale down to 1.
        for _ in 0..num_rounds {
            let start_time = std::time::Instant::now();
            let resp1 = fc.invoke_internal(&sleep_time, b"Ngom");
            let resp2 = fc.invoke_internal(&sleep_time, b"Ngom");
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
    async fn simple_compute_test() {
        run_simple_compute_test().await;
    }

    /// Simple compute test.
    async fn run_simple_compute_test() {
        let fc = super::FunctionalClient::new("functional", "echofn", Some(1024)).await;
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
            let resp = fc.invoke_internal(&fn_arg, b"Ngom").await;
            let end_time = std::time::Instant::now();
            let duration = end_time.duration_since(start_time);
            println!("Resp: {resp:?}. Duration: {duration:?}.");
        }
    }
}
