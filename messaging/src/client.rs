use super::SUBSYSTEM_NAME;
use common::adaptation::frontend::AdapterFrontend;
use common::{full_messaging_name, has_external_access};

const NUM_RECEIVE_RETRIES: i32 = 20; // ~2 seconds.

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
            lambda_client,
            s3_client,
            frontend,
            actor_function_name,
            direct_client: reqwest::Client::builder()
                .connect_timeout(std::time::Duration::from_secs(1))
                .timeout(std::time::Duration::from_secs(5))
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

    /// Send message to message queue.
    /// Use get_message_from_function or get_message_from_http to receive message.
    async fn indirect_message(
        &self,
        msg_id: &str,
        msg: &str,
        payload: &[u8],
    ) -> Option<(String, Vec<u8>)> {
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
        for _ in 0..NUM_RECEIVE_RETRIES {
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

    /// Wake up the messaging function.
    async fn wake_up_messaging_function(lambda_client: aws_sdk_lambda::Client, fn_name: String) {
        let empty_json = serde_json::Value::Null.to_string();
        let fn_arg = aws_smithy_types::Blob::new(empty_json.as_bytes());
        // Async invokes have weird behavior (they keep being retried until long after).
        // So do fake async.
        tokio::spawn(async move {
            let _ = lambda_client
                .invoke()
                .function_name(&fn_name)
                .payload(fn_arg)
                .send()
                .await;
        });
    }

    /// Send message to url using http.
    async fn message_with_http(
        &self,
        url: &str,
        msg_id: &str,
        msg: &str,
        payload: &[u8],
    ) -> Option<(String, Vec<u8>)> {
        let msg_meta = serde_json::to_string(&(msg_id, msg)).unwrap();
        let resp = self
            .direct_client
            .post(url)
            .header("obelisk-meta", msg_meta)
            .body(payload.to_vec())
            .send()
            .await;
        if let Ok(resp) = resp {
            let content: String = resp
                .headers()
                .get("obelisk-meta")
                .unwrap()
                .to_str()
                .unwrap()
                .into();
            let payload = resp.bytes().await.unwrap().to_vec();
            println!(
                "Did message with http: {content:?}, payload_len={}",
                payload.len()
            );
            Some((content, payload))
        } else {
            None
        }
    }

    /// Get actor's url.
    async fn get_actor_url(&self) -> Option<String> {
        let instances = self.frontend.serverful_instances().await;
        let join_time = instances.into_iter().fold(None, |curr, next| match curr {
            None => Some((next.join_time, next.url)),
            Some((curr_join_time, curr_url)) => {
                if curr_join_time < next.join_time {
                    Some((curr_join_time, curr_url))
                } else {
                    Some((next.join_time, next.url))
                }
            }
        });
        join_time.map(|(_, url)| url)
    }

    pub async fn send_message_internal(
        &self,
        msg: &str,
        payload: &[u8],
    ) -> (Option<(String, Vec<u8>)>, bool) {
        let msg_id: String = uuid::Uuid::new_v4().to_string();
        // Add metric. For now let it be empty, since we only need number of invocations.
        // TODO: Add duration here.
        let start_time = std::time::Instant::now();
        // Try fast path with direct message.
        let actor_url = self.get_actor_url().await;
        let resp = match actor_url {
            None => None,
            Some(url) => self.message_with_http(&url, &msg_id, msg, payload).await,
        };
        // Return resp if direct message possible.
        match resp {
            Some(resp) => {
                let end_time = std::time::Instant::now();
                let duration = end_time.duration_since(start_time).as_secs_f64();
                println!("Duration: {duration:?}");
                self.frontend
                    .collect_metric(serde_json::to_value(duration).unwrap())
                    .await;
                return (Some(resp), true);
            }
            None => {
                // Update serverful target.
                let frontend = self.frontend.clone();
                tokio::spawn(async move {
                    frontend.force_read_instances().await;
                });
            }
        };
        // Try slow path with message queue.
        let resp = (self.indirect_message(&msg_id, msg, payload).await, false);
        let end_time = std::time::Instant::now();
        let duration = end_time.duration_since(start_time);
        println!("Duration: {duration:?}");
        let duration = duration.as_secs_f64();
        self.frontend
            .collect_metric(serde_json::to_value(duration).unwrap())
            .await;
        resp
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
            // Try fast path with direct message.
            let actor_url = self.get_actor_url().await;
            match actor_url {
                None => {}
                Some(_url) => return,
            }
            // Hacky way to signal spin up.
            let duration = 0.0;
            self.frontend
                .collect_metric(serde_json::to_value(duration).unwrap())
                .await;
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
        for i in 0..100 {
            let (resp, was_direct) = mc.send_message_internal("essai", &[]).await;
            println!("Round {}/100. Resp: {resp:?}", i + 1);
            if was_direct {
                num_direct_calls += 1;
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
        // The last ten rounds should almost certainly be direct calls.
        println!("Number of direct calls: {num_direct_calls}.");
        assert!(num_direct_calls >= 10);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn scaling_test() {
        run_scaling_test().await;
    }

    async fn run_basic_counter_test() {
        let mc = MessagingClient::new("persistence", "counter").await;
        // Send enough messages to trigger an actor scale up.
        let req_msg = "essai";
        let req_payload = vec![];
        for _ in 0..100 {
            let resp = mc.send_message(req_msg, &req_payload).await;
            if let Some((resp_msg, resp_payload)) = resp {
                println!("Resp: ({resp_msg:?}, {resp_payload:?})");
                let duration_ms: u64 = resp_msg.parse().unwrap();
                let duration = std::time::Duration::from_millis(duration_ms);
                println!("Duration: {duration:?}");
                tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
            }
        }
        // // Send enough messages to trigger a log scale up.
        // for _ in 0..100 {
        //     let (resp_msg, resp_payload) = mc.send_message(req_msg, &req_payload).await.unwrap();
        //     println!("Resp: ({resp_msg:?}, {resp_payload:?})");
        //     let duration_ms: u64 = resp_msg.parse().unwrap();
        //     let duration = std::time::Duration::from_millis(duration_ms);
        //     println!("Duration: {duration:?}");
        //     tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        // }
    }

    #[tokio::test]
    async fn basic_counter_test() {
        run_basic_counter_test().await;
    }
}
