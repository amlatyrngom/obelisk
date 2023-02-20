use super::handler::{LambdaReq, LambdaResp};
use super::SUBSYSTEM_NAME;
use common::adaptation::frontend::AdapterFrontend;
use common::{full_messaging_name, has_external_access};

const NUM_RECEIVE_RETRIES: i32 = 50; // ~3 seconds.
const NUM_INDIRECT_RETRIES: i32 = 5; // ~500ms.

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
        let lambda_client = aws_sdk_lambda::Client::new(&shared_config);
        let s3_client = aws_sdk_s3::Client::new(&shared_config);
        let frontend = AdapterFrontend::new(SUBSYSTEM_NAME, namespace, name).await;
        // Forcibly refresh serverful information.
        frontend.invoke_scaler().await;
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
        MessagingClient {
            s3_client,
            lambda_client,
            frontend,
            actor_function_name,
            direct_client: reqwest::Client::builder()
                .connect_timeout(std::time::Duration::from_millis(500))
                .timeout(std::time::Duration::from_millis(5000))
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
    async fn lambda_message(&self, msg: &str, payload: &[u8]) -> Option<(String, Vec<u8>)> {
        let arg = LambdaReq::Msg(msg.into(), payload.into());
        let arg = serde_json::to_vec(&arg).unwrap(); // Inefficent with large payload.
        let arg = aws_smithy_types::Blob::new(arg);
        let resp = self
            .lambda_client
            .invoke()
            .function_name(&self.actor_function_name)
            .payload(arg.clone())
            .send()
            .await;
        match resp {
            Ok(resp) => {
                if let Some(x) = resp.function_error() {
                    if !x.is_empty() {
                        return None;
                    }
                }
                let resp: Vec<u8> = resp.payload().unwrap().clone().into_inner();
                let resp: LambdaResp = serde_json::from_slice(&resp).unwrap();
                return if let LambdaResp::Msg(resp) = resp {
                    resp
                } else {
                    panic!("Impossible response type");
                };
            }
            Err(x) => {
                println!("{x:?}");
                return None;
            }
        };
    }

    /// Send message to url using http.
    async fn message_with_http(
        &self,
        url: &str,
        msg: &str,
        payload: &[u8],
    ) -> Option<(String, Vec<u8>)> {
        let resp = self
            .direct_client
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
    async fn wake_up_messaging_function(lambda_client: aws_sdk_lambda::Client, fn_name: String) {
        let msg_id = super::handler::LambdaReq::IndirectMsg;
        let msg_id = serde_json::to_vec(&msg_id).unwrap();
        let fn_arg = aws_smithy_types::Blob::new(msg_id);
        let _ = lambda_client
            .invoke()
            .function_name(&fn_name)
            .payload(fn_arg)
            .send()
            .await;
    }

    /// Indirect message.
    async fn indirect_message(&self, msg: &str, payload: &[u8]) -> Option<(String, Vec<u8>)> {
        let msg_id = uuid::Uuid::new_v4().to_string();
        {
            // Wake up messaging function.
            let lambda_client = self.lambda_client.clone();
            let lambda_name = self.actor_function_name.clone();
            tokio::spawn(async move {
                Self::wake_up_messaging_function(lambda_client, lambda_name).await;
            });
        }

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
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
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
                    Self::wake_up_messaging_function(lambda_client, lambda_name).await;
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
        for n in 0..NUM_RECEIVE_RETRIES {
            if n > 0 {
                // Sleep random amount for client-side congestion control.
                let f = rand::random::<f64>(); // Number between 0-1.
                let duration_ms = (f * 20.0).ceil() as u64; // Number between 1-20ms.
                tokio::time::sleep(std::time::Duration::from_millis(duration_ms)).await;
            }
            // Try fast path with direct message.
            let actor_url = self.get_actor_url().await;
            let resp = match &actor_url {
                None => None,
                Some(url) => match self.message_with_http(url, msg, payload).await {
                    Some(resp) => Some(resp),
                    None => self.indirect_message(msg, payload).await,
                },
            };

            let (resp, is_direct) = match resp {
                Some(resp) => {
                    // Return resp if direct message possible.
                    (Some(resp), true)
                }
                None => {
                    // // Async update serverful target if unreachable.
                    // if actor_url.is_some() {
                    //     let frontend = self.frontend.clone();
                    //     tokio::spawn(async move {
                    //         frontend.force_read_instances().await;
                    //     });
                    // }
                    // Try slow path.
                    let resp = match self.lambda_message(msg, payload).await {
                        Some(resp) => Some(resp),
                        None => self.indirect_message(msg, payload).await,
                    };
                    (resp, false)
                }
            };

            // Return resp.
            if resp.is_some() {
                let end_time = std::time::Instant::now();
                let duration = end_time.duration_since(start_time);
                println!("Duration: {duration:?}. Is Direct: {is_direct}");
                let mut duration = duration.as_secs_f64();
                if is_direct {
                    // Add overhead of a lambda call.
                    // This is to prevent unecessary scale downs followed by scale up.
                    duration += 0.012;
                }
                self.frontend
                    .collect_metric(serde_json::to_value(duration).unwrap())
                    .await;
                return (resp, is_direct);
            }
        }
        (None, false)
    }

    /// Send message to an actor and get response back.
    pub async fn send_message(&self, msg: &str, payload: &[u8]) -> Option<(String, Vec<u8>)> {
        let (resp, _) = self.send_message_internal(msg, payload).await;
        if resp.is_none() {
            // When response is none, forcibly spin up.
            let this = self.clone();
            tokio::spawn(async move {
                this.spin_up().await;
            });
        }
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
        println!("Number of direct calls: {num_direct_calls}.");
        assert!(num_direct_calls >= 10);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn scaling_test() {
        run_scaling_test().await;
    }

    async fn run_parallel_scaling_test() {
        let mc = MessagingClient::new("messaging", "echo").await;
        let req_msg = "essai";
        let req_payload = vec![];
        let resp1 = mc.send_message(req_msg, &req_payload);
        let resp2 = mc.send_message(req_msg, &req_payload);
        let resp3 = mc.send_message(req_msg, &req_payload);
        let resp4 = mc.send_message(req_msg, &req_payload);
        let resp5 = mc.send_message(req_msg, &req_payload);
        let resp6 = mc.send_message(req_msg, &req_payload);
        let resp7 = mc.send_message(req_msg, &req_payload);
        let resp8 = mc.send_message(req_msg, &req_payload);

        let resp = tokio::join!(resp1, resp2, resp3, resp4, resp5, resp6, resp7, resp8);
        println!("Resp: {resp:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn parallel_scaling_test() {
        run_parallel_scaling_test().await;
    }
}
