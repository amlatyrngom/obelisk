use super::rescaler::FunctionalMetric;
use common::adaptation::frontend::AdapterFrontend;
use common::{full_function_name, FunctionResp};
use rand::seq::SliceRandom;
use serde_json::Value;
use std::sync::Arc;

const NUM_DIRECT_RETRIES: u64 = 2;

/// FunctionalClient.
#[derive(Clone)]
pub struct FunctionalClient {
    direct_client: reqwest::Client,
    front_end: Arc<AdapterFrontend>,
}

impl FunctionalClient {
    /// Create client.
    pub async fn new(namespace: &str) -> Self {
        let direct_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10)) // TODO: set to correct value.
            .connect_timeout(std::time::Duration::from_millis(100)) // Short on purpose.
            .build()
            .unwrap();
        let front_end = Arc::new(AdapterFrontend::new("functional", namespace, "fn").await);

        FunctionalClient {
            direct_client,
            front_end,
        }
    }

    /// Log a metric with a given probability.
    async fn log_metrics(&self, duration_ms: u64) {
        let metric = FunctionalMetric {
            duration: std::time::Duration::from_millis(duration_ms),
        };
        let metric = serde_json::to_value(&metric).unwrap();
        self.front_end.collect_metric(metric).await;
    }

    /// Pick a random url for client side load balancing.
    pub async fn pick_random_url(&self) -> Option<String> {
        // TODO: Have more sophisticated client side load balancing.
        let serverful_instances = self.front_end.serverful_instances().await;
        let now = chrono::Utc::now();
        let instance = serverful_instances.choose(&mut rand::thread_rng());
        if let Some(instance) = instance {
            let url = instance.url.clone();
            let since = now.signed_duration_since(instance.join_time);
            if since.num_seconds() < 10 {
                // Just try connecting.
                let direct_client = self.direct_client.clone();
                tokio::spawn(async move {
                    let _ = direct_client.get(url).send().await;
                });
                None
            } else {
                Some(url)
            }
        } else {
            None
        }
    }

    /// Invoke a lambda.
    pub async fn invoke_lambda(&self, arg: &[u8]) -> Result<Vec<u8>, String> {
        let fn_name = full_function_name(&self.front_end.info.namespace);
        let arg = aws_smithy_types::Blob::new(arg);
        let lambda_client = self.front_end.clients.clone().unwrap().lambda_client;
        let resp = lambda_client
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
        Ok(resp)
    }

    /// Try direct invocation.
    pub async fn invoke_direct(&self, arg: &[u8]) -> Option<FunctionResp> {
        for _ in 0..NUM_DIRECT_RETRIES {
            // Find random url. TODO: Better load balancing.
            let url = self.pick_random_url().await;
            if url.is_none() {
                break;
            }
            let url = url.unwrap();
            // Send request.
            let resp = self
                .direct_client
                .post(&url)
                .header("Content-Type", "application/json")
                .header("obelisk-meta", "foo")
                .body(arg.to_vec())
                .send()
                .await;
            // Return in case of success.
            if let Ok(resp) = resp {
                let valid = resp.headers().get("obelisk-meta").unwrap();
                let valid: bool = valid.to_str().unwrap().parse().unwrap();
                if valid {
                    let body = {
                        let body = resp.bytes().await.unwrap();
                        body.to_vec()
                    };
                    return Some(serde_json::from_slice(&body).unwrap());
                } else {
                    println!("Contacted full invoker");
                    continue;
                }
            } else {
                println!("HTTP error: {resp:?}");
                break;
            }
        }
        None
    }

    /// Internal invoke. Also returns whether a direct call as made.
    pub async fn invoke_internal(&self, arg: &[u8]) -> Result<(Value, bool), String> {
        // Try direct call.
        let resp = self.invoke_direct(arg).await;
        // If direct not success, try lambda.
        let (resp, direct_call) = if let Some(resp) = resp {
            (resp, true)
        } else {
            let resp = self.invoke_lambda(arg).await?;
            let resp = serde_json::from_slice(&resp).unwrap();
            (resp, false)
        };

        // Get response.
        let FunctionResp {
            resp,
            duration_ms,
            mem_size_mb: _,
        } = resp;
        let duration_ms: u64 = if duration_ms <= 0 {
            1
        } else {
            duration_ms as u64
        };
        // Log metrics.
        {
            let this = self.clone();
            tokio::spawn(async move {
                this.log_metrics(duration_ms).await;
            });
        }
        // Respond
        Ok((resp, direct_call))
    }

    /// Call a function.
    /// Input must be valid json.
    pub async fn invoke(&self, arg: &[u8]) -> Result<Value, String> {
        let (resp, _) = self.invoke_internal(arg).await?;
        Ok(resp)
    }
}

#[cfg(test)]
mod tests {
    async fn simple_invoke() {
        let cl = super::FunctionalClient::new("functional").await;
        let mut direct_invokes = 0;
        let mut total_invokes = 0;
        let num_rounds = 50;
        let calls_per_round = 10;
        for i in 0..num_rounds {
            let mut resps = Vec::new();
            let start_time = std::time::Instant::now();
            for _ in 0..calls_per_round {
                let cl = cl.clone();
                let resp = tokio::spawn(async move {
                    let duration_ms: u64 = 2000;
                    let duration_ms = serde_json::to_vec(&duration_ms).unwrap();
                    cl.invoke_internal(&duration_ms).await.unwrap()
                });
                resps.push(resp);
            }
            total_invokes += calls_per_round;
            for resp in resps {
                let (_, direct) = resp.await.unwrap();
                if direct {
                    direct_invokes += 1;
                }
            }
            let end_time = std::time::Instant::now();
            let duration = end_time.duration_since(start_time);
            println!(
                "Round {i}. DirectInvokes={direct_invokes}/{total_invokes}. Duration={duration:?}"
            );
        }
        // The last few rounds should mostly use direct invokes.
        assert!(direct_invokes >= (num_rounds / 10) * calls_per_round);
    }

    #[tokio::test]
    async fn simple_invoke_test() {
        simple_invoke().await;
    }
}
