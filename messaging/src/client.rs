use super::SUBSYSTEM_NAME;
use common::adaptation::frontend::AdapterFrontend;
use common::{full_messaging_name, has_external_access};

const DIRECT_CONNECT_TIMEOUT_MS: u64 = 100; // Short on purpose.
const DIRECT_REQUEST_TIMEOUT_MS: u64 = 5000; // Short on purpose. Will switch to lambda.
const NUM_INDIRECT_RETRIES: u32 = 100; // ~100x100ms. TODO: Read timeout from function config.
const INDIRECT_RETRY_MS: u64 = 100; // Timeout to wait to poll S3 and retry waking up handler.
const LAMBDA_BACKOFF_MS: u64 = 10; // Lambda's exponential backoff initial time.
const LAMBDA_AUTO_RETRIES: u32 = 6; // Lambda's retries. With a 10ms initial backoff, the total timeout seems to to ~300-400ms.
const AVG_LAMBDA_OVERHEAD: f64 = 0.015;

#[derive(Clone)]
pub struct MessagingClient {
    direct_client: reqwest::Client,
    lambda_client: aws_sdk_lambda::Client,
    s3_client: aws_sdk_s3::Client,
    frontend: AdapterFrontend,
    actor_function_name: String,
}

impl MessagingClient {
    /// Create a new actor client.
    pub async fn new(namespace: &str, name: &str) -> Self {
        if !has_external_access() {
            panic!("Lambda actor {namespace}/{name}. Attempting to creating a client without external access");
        }
        let shared_config = aws_config::load_from_env().await;
        // Lambda.
        let lambda_config = aws_sdk_lambda::config::Builder::from(&shared_config)
            .retry_config(
                aws_sdk_lambda::config::retry::RetryConfig::standard()
                    .with_initial_backoff(std::time::Duration::from_millis(LAMBDA_BACKOFF_MS)) // On avg: 25ms, 50ms, 100ms, ....
                    .with_max_attempts(LAMBDA_AUTO_RETRIES),
            )
            .build();
        let lambda_client = aws_sdk_lambda::Client::from_conf(lambda_config);
        // S3.
        let s3_config = aws_sdk_s3::config::Builder::from(&shared_config)
            .timeout_config(
                aws_sdk_s3::config::timeout::TimeoutConfig::builder()
                    .connect_timeout(std::time::Duration::from_secs(1))
                    .operation_timeout(std::time::Duration::from_secs(1))
                    .build(),
            )
            .build();
        let s3_client = aws_sdk_s3::Client::from_conf(s3_config);
        // Frontend.
        let frontend = AdapterFrontend::new(SUBSYSTEM_NAME, namespace, name).await;
        // Forcibly refresh serverful information.
        frontend.invoke_scaler().await;
        // Forcibly warm up s3 client.
        let _ = s3_client
            .head_bucket()
            .bucket(common::bucket_name())
            .send()
            .await;
        // Try make messaging function.
        let actor_function_name = full_messaging_name(namespace, name);
        let actor_template_name = common::full_messaging_template_name(namespace);
        Self::try_make_function(
            &lambda_client,
            &actor_function_name,
            &actor_template_name,
            name,
        )
        .await;
        // TODO(Amadou): Should probably spawn a lambda warmer before returning.
        MessagingClient {
            s3_client,
            lambda_client,
            frontend,
            actor_function_name,
            direct_client: reqwest::Client::builder()
                .connect_timeout(std::time::Duration::from_millis(DIRECT_CONNECT_TIMEOUT_MS))
                .timeout(std::time::Duration::from_millis(DIRECT_REQUEST_TIMEOUT_MS))
                .build()
                .unwrap(),
        }
    }

    pub async fn delete(&self) {
        let _ = self
            .lambda_client
            .delete_function()
            .function_name(&self.actor_function_name)
            .send()
            .await;
        // TODO: delete actor service.
    }

    async fn try_make_function(
        lambda_client: &aws_sdk_lambda::Client,
        lambda_name: &str,
        template_name: &str,
        name: &str,
    ) {
        loop {
            let actor_function = lambda_client
                .get_function()
                .function_name(lambda_name)
                .send()
                .await;
            match actor_function {
                Ok(actor_function) => {
                    let st = actor_function.configuration().unwrap().state().unwrap();
                    match st {
                        aws_sdk_lambda::model::State::Inactive => {
                            return;
                        }
                        aws_sdk_lambda::model::State::Active => {
                            return;
                        }
                        _ => {
                            println!("Waiting for function availability. {st:?}...");
                            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                            continue;
                        }
                    }
                }
                Err(x) => {
                    println!("{x:?}. Creating...");
                    let mut create_function =
                        lambda_client.create_function().function_name(lambda_name);
                    println!("Template name: {template_name}");
                    println!("Lambda name: {lambda_name}");
                    let template = lambda_client
                        .get_function()
                        .function_name(template_name)
                        .send()
                        .await
                        .unwrap();
                    let config = template.configuration().unwrap();
                    let mem_size = config.memory_size().unwrap();
                    let timeout = config.timeout().unwrap();
                    create_function = create_function.memory_size(mem_size).timeout(timeout);
                    if let Some(efs_config) = config.file_system_configs() {
                        create_function = create_function
                            .file_system_configs(efs_config.first().unwrap().clone());
                    }
                    if let Some(vpc_config) = config.vpc_config() {
                        create_function = create_function.vpc_config(
                            aws_sdk_lambda::model::VpcConfig::builder()
                                .security_group_ids(
                                    vpc_config.security_group_ids().unwrap().first().unwrap(),
                                )
                                .subnet_ids(vpc_config.subnet_ids().unwrap().first().unwrap())
                                .build(),
                        );
                    }
                    let role = config.role().unwrap();
                    create_function = create_function.role(role);
                    let mut env_vars = aws_sdk_lambda::model::Environment::builder();
                    for (var_name, var_value) in config.environment().unwrap().variables().unwrap()
                    {
                        env_vars = env_vars.variables(var_name, var_value);
                    }
                    env_vars = env_vars.variables("NAME", name);
                    create_function = create_function.environment(env_vars.build());
                    create_function =
                        create_function.package_type(config.package_type().unwrap().clone());
                    let function_code = aws_sdk_lambda::model::FunctionCode::builder()
                        .image_uri(template.code().unwrap().image_uri().unwrap())
                        .build();
                    create_function = create_function.code(function_code);
                    let _ = create_function.send().await;
                    let _ = lambda_client
                        .put_function_concurrency()
                        .function_name(lambda_name)
                        .reserved_concurrent_executions(1)
                        .send()
                        .await;
                }
            }
        }
    }

    /// Directly call lambda.
    async fn lambda_message(
        lambda_client: aws_sdk_lambda::Client,
        fn_name: &str,
        msg: &str,
        payload: &[u8],
    ) -> Option<(String, Vec<u8>)> {
        // Can be very slow with large payloads. So just go for indirect call when payload > 2MB.
        if payload.len() > (2 << 20) {
            return None;
        }
        let arg = super::handler::HandlerReq::LambdaMsg(msg.into(), payload.into());
        let arg = tokio::task::block_in_place(move || serde_json::to_vec(&arg).unwrap());
        let arg = aws_smithy_types::Blob::new(arg);
        let resp = lambda_client
            .invoke()
            .function_name(fn_name)
            .payload(arg.clone())
            .send()
            .await;
        match resp {
            Ok(resp) => {
                if let Some(x) = resp.function_error() {
                    // Here, we have a routing error (req not sent to main lambda). So use S3.
                    if !x.is_empty() {
                        println!("Lambda Routing Error: {x:?}");
                        return None;
                    }
                }
                let resp: Vec<u8> = resp.payload().unwrap().clone().into_inner();
                let resp: super::handler::HandlerResp =
                    tokio::task::block_in_place(move || serde_json::from_slice(&resp).unwrap());
                return if let super::handler::HandlerResp::LambdaMsg(meta, payload) = resp {
                    Some((meta, payload))
                } else {
                    panic!("Impossible response type");
                };
            }
            Err(x) => {
                // Here, we have a throttling error. The lambda client has already done retries. So just return.
                println!("Lambda Throttling Error: {x:?}");
                return None;
            }
        };
    }

    /// Send message to url using http.
    async fn message_with_http(
        direct_client: reqwest::Client,
        url: &str,
        msg: &str,
        payload: &[u8],
    ) -> Option<(String, Vec<u8>)> {
        let msg = super::handler::HandlerReq::DirectMsg(msg.into());
        let msg = serde_json::to_string(&msg).unwrap();
        let resp = direct_client
            .post(url)
            .header("obelisk-meta", msg)
            .header("content-length", payload.len())
            .body(payload.to_vec())
            .send()
            .await;
        match resp {
            Ok(resp) => {
                let content: String = resp
                    .headers()
                    .get("obelisk-meta")
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .into();
                let payload = resp.bytes().await.unwrap().to_vec();
                // println!(
                // "Did message with http: {content:?}, payload_len={}",
                // payload.len()
                // );
                Some((content, payload))
            }
            Err(err) => {
                println!("Direct Message Error: {err:?}");
                None
            }
        }
    }

    /// Wake up the messaging function.
    async fn wake_up_messaging_handler(
        lambda_client: aws_sdk_lambda::Client,
        direct_client: reqwest::Client,
        fn_name: String,
        actor_url: Option<String>,
    ) {
        tokio::spawn(async move {
            let msg = super::handler::HandlerReq::IndirectMsg;
            let msg = serde_json::to_vec(&msg).unwrap();
            let fn_arg = aws_smithy_types::Blob::new(msg);
            let _ = lambda_client
                .invoke()
                .function_name(&fn_name)
                .payload(fn_arg)
                .send()
                .await;
        });
        if let Some(url) = actor_url {
            tokio::spawn(async move {
                let msg = super::handler::HandlerReq::IndirectMsg;
                let msg = bincode::serialize(&msg).unwrap();
                let payload: Vec<u8> = vec![0];
                let _ = direct_client
                    .post(url)
                    .header("obelisk-meta", msg)
                    .header("content-length", payload.len())
                    .body(payload.to_vec())
                    .send()
                    .await;
            });
        }
    }

    /// Indirect message.
    async fn indirect_message(
        &self,
        msg: &str,
        payload: &[u8],
        actor_url: Option<String>,
    ) -> Option<(String, Vec<u8>)> {
        let msg_id = uuid::Uuid::new_v4().to_string();
        // Wake up messaging function.
        let lambda_client = self.lambda_client.clone();
        let lambda_name = self.actor_function_name.clone();
        let direct_client = self.direct_client.clone();
        Self::wake_up_messaging_handler(
            lambda_client,
            direct_client,
            lambda_name,
            actor_url.clone(),
        )
        .await;
        // Write message to S3.
        let s3_send_key = format!("{}/send/{msg_id}", common::tmp_s3_dir());
        println!("Sending to S3: {s3_send_key:?}");
        let s3_recv_key = format!("{}/recv/{msg_id}", common::tmp_s3_dir());
        let body = tokio::task::block_in_place(move || {
            let body: (String, String, Vec<u8>) = (msg_id.into(), msg.into(), payload.into());
            bincode::serialize(&body).unwrap()
        });
        let body = aws_sdk_s3::types::ByteStream::from(body);
        let resp = self
            .s3_client
            .put_object()
            .bucket(&common::bucket_name())
            .key(&s3_send_key)
            .body(body)
            .send()
            .await;
        println!("Sent to S3: {s3_send_key:?}");
        match resp {
            Ok(_) => {}
            Err(x) => {
                println!("{x:?}");
                return None;
            }
        };

        // Repeatedly try reading response and waking up messaging function if response not found.
        for _n in 0..NUM_INDIRECT_RETRIES {
            // Wait for processing.
            tokio::time::sleep(std::time::Duration::from_millis(INDIRECT_RETRY_MS)).await;
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
                    let (_, msg, payload): (String, String, Vec<u8>) =
                        bincode::deserialize(&body).unwrap();
                    return Some((msg, payload));
                }
                Err(_) => {
                    // Wake up messaging function in case it is not active.
                    let lambda_client = self.lambda_client.clone();
                    let lambda_name = self.actor_function_name.clone();
                    let direct_client = self.direct_client.clone();
                    Self::wake_up_messaging_handler(
                        lambda_client,
                        direct_client,
                        lambda_name,
                        actor_url.clone(),
                    )
                    .await;
                }
            }
        }
        None
    }

    /// Get actor's url.
    async fn get_actor_url(&self) -> Option<String> {
        // Find latest actor to successfully join.
        let instances = self.frontend.serverful_instances().await;
        let join_time = instances.into_iter().fold(None, |curr, next| match curr {
            None => Some((next.join_time, next.url)),
            Some((curr_join_time, curr_url)) => {
                if curr_join_time > next.join_time {
                    Some((curr_join_time, curr_url))
                } else {
                    Some((next.join_time, next.url))
                }
            }
        });
        join_time.map(|(_, url)| url)
    }

    /// Send message.
    pub async fn send_message_internal(
        &self,
        msg: &str,
        payload: &[u8],
    ) -> (Option<(String, Vec<u8>)>, bool) {
        let start_time = std::time::Instant::now();
        // Try fast path with direct message.
        let actor_url = self.get_actor_url().await;
        let resp = match &actor_url {
            None => None,
            Some(url) => {
                match Self::message_with_http(self.direct_client.clone(), url, msg, payload).await {
                    Some(resp) => Some(resp),
                    None => None,
                }
            }
        };
        // Try lambda call otherwise.
        let (resp, is_http) = match resp {
            Some(resp) => {
                // Return resp if direct message possible.
                (Some(resp), true)
            }
            None => {
                // Try lambda call.
                let lambda_client = self.lambda_client.clone();
                let fn_name = self.actor_function_name.clone();
                let resp = Self::lambda_message(lambda_client, &fn_name, &msg, &payload).await;
                (resp, false)
            }
        };

        // Try indirect call.
        let (resp, is_http, is_indirect) = match resp {
            Some(resp) => {
                // Return resp if direct message possible.
                (Some(resp), is_http, false)
            }
            None => {
                // Try s3 call.
                let resp = match self.indirect_message(msg, payload, actor_url).await {
                    Some(resp) => Some(resp),
                    None => None,
                };
                (resp, false, true)
            }
        };

        // Collect metrics.
        if resp.is_some() {
            let end_time = std::time::Instant::now();
            let duration = end_time.duration_since(start_time);
            println!("Duration: {duration:?}. Is HTTP: {is_http}. Is Indirect: {is_indirect}");
            let mut duration = duration.as_secs_f64();
            // Treat all calls like lambda for metrics.
            // If we don't do this, the rescaling logic will think usage decreases when calls get faster.
            if is_http {
                // Add overhead of an slow call.
                duration += AVG_LAMBDA_OVERHEAD;
            } else if is_indirect {
                // Remove overhead of S3.
                duration -= 0.150;
                // Have a lower bound in case S3 calls were exceptionally fast.
                if duration < AVG_LAMBDA_OVERHEAD {
                    duration = AVG_LAMBDA_OVERHEAD;
                }
            }

            self.frontend
                .collect_metric(serde_json::to_value(duration).unwrap())
                .await;
            return (resp, is_http);
        }
        (None, false)
    }

    /// Send message to an actor and get response back.
    pub async fn send_message(&self, msg: &str, payload: &[u8]) -> Option<(String, Vec<u8>)> {
        let (resp, _) = self.send_message_internal(msg, payload).await;
        resp
    }

    /// Forcibly spin up.
    /// Returns true if already spun up.
    pub async fn spin_up(&self) {
        loop {
            // Hacky way to signal spin up.
            let duration = 0.0;
            self.frontend
                .collect_metric(serde_json::to_value(duration).unwrap())
                .await;
            // Try fast path with direct message.
            let actor_url = self.get_actor_url().await;
            match actor_url {
                None => {}
                Some(_url) => return,
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::MessagingClient;
    use std::sync::Arc;

    async fn run_basic_test() {
        let mc = MessagingClient::new("messaging", "echo").await;
        let req_msg = "essai";
        let req_payload = vec![];
        let (resp_msg, resp_payload) = mc.send_message(req_msg, &req_payload).await.unwrap();
        println!("Resp: ({resp_msg:?}, {resp_payload:?})");
        assert!(req_msg == resp_msg);
        assert!(req_payload == resp_payload);
        let req_payload = vec![1, 2, 3, 4, 5];
        let (resp_msg, resp_payload) = mc.send_message(req_msg, &req_payload).await.unwrap();
        assert!(req_msg == resp_msg);
        assert!(req_payload == resp_payload);
        println!("Resp: ({resp_msg:?}, {resp_payload:?})");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn basic_test() {
        run_basic_test().await;
    }

    async fn run_scaling_test() {
        let mc = MessagingClient::new("messaging", "echo").await;
        // Send messages without payloads.
        let mut num_direct_calls = 0;
        for _ in 0..1000 {
            let (_resp, was_direct) = mc.send_message_internal("essai", &[]).await;
            // println!("Round {}/100. Resp: {resp:?}", i + 1);
            if was_direct {
                num_direct_calls += 1;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        // The last ten rounds should almost certainly be direct calls.
        // Most likely, the last >= 500 rounds are direct calls.
        println!("Number of direct calls: {num_direct_calls}.");
        assert!(num_direct_calls >= 10);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn scaling_test() {
        run_scaling_test().await;
    }

    async fn run_parallel_scaling_test(mc: Arc<MessagingClient>) {
        let req_msg = "essai";
        let req_payload = vec![];
        // Prevent Cold start.
        let _ = mc.send_message(req_msg, &req_payload).await;
        // Do requests in parallel.
        let resp1 = mc.send_message(req_msg, &req_payload);
        let resp2 = mc.send_message(req_msg, &req_payload);
        let resp3 = mc.send_message(req_msg, &req_payload);
        let resp4 = mc.send_message(req_msg, &req_payload);
        let resp5 = mc.send_message(req_msg, &req_payload);
        let resp6 = mc.send_message(req_msg, &req_payload);
        let resp7 = mc.send_message(req_msg, &req_payload);
        let resp8 = mc.send_message(req_msg, &req_payload);
        let resp9 = mc.send_message(req_msg, &req_payload);
        let resp10 = mc.send_message(req_msg, &req_payload);
        let resp11 = mc.send_message(req_msg, &req_payload);
        let resp12 = mc.send_message(req_msg, &req_payload);
        let resp13 = mc.send_message(req_msg, &req_payload);
        let resp14 = mc.send_message(req_msg, &req_payload);

        let _resp = tokio::join!(
            resp1, resp2, resp3, resp4, resp5, resp6, resp7, resp8, resp9, resp10, resp11, resp12,
            resp13, resp14
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn parallel_scaling_test() {
        let mc = Arc::new(MessagingClient::new("messaging", "echo").await);
        // Cold starts.
        run_parallel_scaling_test(mc.clone()).await;
        // Warm starts.
        run_parallel_scaling_test(mc.clone()).await;
    }
}
