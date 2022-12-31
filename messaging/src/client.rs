use super::{recv_queue_name, send_queue_name, SUBSYSTEM_NAME};
use aws_sdk_sqs::model::QueueAttributeName;
use common::adaptation::frontend::AdapterFrontend;
use common::{full_messaging_name, full_receiving_name};
use rand::Rng;

const NUM_RECEIVE_RETRIES: i32 = 20; // ~2 seconds.

pub struct MessagingClient {
    direct_client: reqwest::Client,
    lambda_client: aws_sdk_lambda::Client,
    s3_client: aws_sdk_s3::Client,
    sqs_client: aws_sdk_sqs::Client,
    frontend: AdapterFrontend,
    send_queue_url: String,
    recv_queue_url: String,
    actor_function_name: String,
    receiver_name: String,
}

impl MessagingClient {
    /// Create a new actor client.
    pub async fn new(namespace: &str, name: &str) -> Self {
        let shared_config = aws_config::load_from_env().await;
        let sqs_client = aws_sdk_sqs::Client::new(&shared_config);
        let lambda_client = aws_sdk_lambda::Client::new(&shared_config);
        let s3_client = aws_sdk_s3::Client::new(&shared_config);
        let frontend = AdapterFrontend::new(SUBSYSTEM_NAME, namespace, name).await;
        // Get or make queues.
        let send_queue_name = send_queue_name(namespace, name);
        let send_queue_url = Self::get_or_make_queue(&sqs_client, &send_queue_name).await;
        let recv_queue_name = recv_queue_name(namespace, name);
        let recv_queue_url = Self::get_or_make_queue(&sqs_client, &recv_queue_name).await;
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
        // Try make receiver.
        let receiver_name = full_receiving_name(namespace, name);
        let receiver_template_name = common::full_receiving_template_name(namespace);
        Self::try_make_function(
            &lambda_client,
            &receiver_name,
            &receiver_template_name,
            name,
        )
        .await;

        MessagingClient {
            sqs_client,
            lambda_client,
            s3_client,
            frontend,
            receiver_name,
            actor_function_name,
            send_queue_url,
            recv_queue_url,
            direct_client: reqwest::Client::builder()
                .connect_timeout(std::time::Duration::from_secs(1))
                .timeout(std::time::Duration::from_secs(5))
                .build()
                .unwrap(),
        }
    }

    pub async fn delete(&self) {
        let _ = self
            .sqs_client
            .delete_queue()
            .queue_url(&self.recv_queue_url)
            .send()
            .await;
        let _ = self
            .sqs_client
            .delete_queue()
            .queue_url(&self.send_queue_url)
            .send()
            .await;
        let _ = self
            .lambda_client
            .delete_function()
            .function_name(&self.receiver_name)
            .send()
            .await;
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
                    create_function = create_function.memory_size(mem_size);
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

    async fn get_or_make_queue(sqs_client: &aws_sdk_sqs::Client, queue_name: &str) -> String {
        // Get or make send queue.
        let queue_url = sqs_client
            .get_queue_url()
            .queue_name(queue_name)
            .send()
            .await;
        let queue_url: String = match queue_url {
            Ok(queue_url) => queue_url.queue_url().unwrap().into(),
            Err(_) => {
                let _ = sqs_client
                    .create_queue()
                    .queue_name(queue_name)
                    .attributes(QueueAttributeName::MessageRetentionPeriod, "60")
                    .attributes(QueueAttributeName::VisibilityTimeout, "10")
                    .send()
                    .await;
                let queue_url = sqs_client
                    .get_queue_url()
                    .queue_name(queue_name)
                    .send()
                    .await
                    .unwrap();
                queue_url.queue_url().unwrap().into()
            }
        };
        queue_url
    }

    /// Send message to message queue.
    /// Use get_message_from_function or get_message_from_http to receive message.
    async fn message_with_queue(
        &self,
        msg_id: &str,
        msg: &str,
        payload: &[u8],
    ) -> Option<(String, Vec<u8>)> {
        let msg_meta = serde_json::to_string(&(msg_id, msg, payload.len() > 0)).unwrap();
        if payload.len() > 0 {
            let s3_key = format!("{}/send/{msg_id}", common::messaging_s3_dir());
            let body = aws_sdk_s3::types::ByteStream::from(payload.to_vec());
            let resp = self
                .s3_client
                .put_object()
                .bucket(&common::bucket_name())
                .key(s3_key)
                .body(body)
                .send()
                .await;
            match resp {
                Ok(_) => {}
                Err(x) => {
                    println!("{x:?}");
                    return None;
                }
            }
        }
        self.sqs_client
            .send_message()
            .queue_url(&self.send_queue_url)
            .message_body(&msg_meta)
            .send()
            .await
            .unwrap();
        let fn_arg = aws_smithy_types::Blob::new(serde_json::to_vec(&msg_id).unwrap());
        for _ in 0..NUM_RECEIVE_RETRIES {
            // Wait for response.
            let sleep_duration = {
                // Putting rng inside of block because it cannot be held across async.
                let mut rng = rand::thread_rng();
                let sleep_duration = rng.gen_range(50..150); // Wait an average of 100ms.
                sleep_duration
            };
            tokio::time::sleep(std::time::Duration::from_millis(sleep_duration)).await;
            // Invoke.
            loop {
                let resp = self
                    .lambda_client
                    .invoke()
                    .function_name(&self.receiver_name)
                    .payload(fn_arg.clone())
                    .send()
                    .await;
                if let Ok(resp) = resp {
                    if resp.status_code() == 200 {
                        let resp = resp.payload().unwrap().clone().into_inner();
                        let resp: Option<(String, bool)> = serde_json::from_slice(&resp).unwrap();
                        if resp.is_none() {
                            // Wake up messaging function. Then do random sleep.
                            self.wake_up_messaging_function().await;
                            break;
                        }

                        let (content, has_payload): (String, bool) = resp.unwrap();
                        return if has_payload {
                            let s3_key = format!("{}/recv/{msg_id}", common::messaging_s3_dir());
                            let resp = self
                                .s3_client
                                .get_object()
                                .bucket(&common::bucket_name())
                                .key(&s3_key)
                                .send()
                                .await;
                            let s3_client = self.s3_client.clone();
                            // Asynchronous delete.
                            let _ = tokio::spawn(async move {
                                let _ = s3_client
                                    .delete_object()
                                    .bucket(&common::bucket_name())
                                    .key(&s3_key)
                                    .send()
                                    .await;
                            });
                            if let Ok(resp) = resp {
                                let payload: Vec<u8> =
                                    resp.body.collect().await.unwrap().into_bytes().to_vec();
                                Some((content, payload))
                            } else {
                                None
                            }
                        } else {
                            Some((content, vec![]))
                        };
                    }
                }
            }
        }
        None
    }

    /// Wake up the messaging function.
    async fn wake_up_messaging_function(&self) {
        let empty_json = serde_json::Value::Null.to_string();
        let fn_arg = aws_sdk_lambda::types::ByteStream::from(empty_json.as_bytes().to_vec());
        let _ = self
            .lambda_client
            .invoke_async()
            .function_name(&self.actor_function_name)
            .invoke_args(fn_arg)
            .send()
            .await;
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
                self.frontend
                    .collect_metric(serde_json::to_value(&duration).unwrap())
                    .await;
                return (Some(resp), true);
            }
            None => {}
        };
        // Try slow path with message queue.
        let resp = (self.message_with_queue(&msg_id, msg, payload).await, false);
        let end_time = std::time::Instant::now();
        let mut duration = end_time.duration_since(start_time).as_secs_f64();
        // Subtract average queue overhead time.
        duration -= 0.2;
        // Bound by a minimum.
        if duration < 0.01 {
            duration = 0.01;
        }
        self.frontend
            .collect_metric(serde_json::to_value(&duration).unwrap())
            .await;
        resp
    }

    /// Send message to an actor and get response back.
    pub async fn send_message(&self, msg: &str, payload: &[u8]) -> Option<(String, Vec<u8>)> {
        let (resp, _) = self.send_message_internal(msg, payload).await;
        resp
    }
}

#[cfg(test)]
mod tests {
    use super::MessagingClient;

    async fn run_basic_test() {
        let mc = MessagingClient::new("messaging", "echo").await;
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        let req_msg = "essai";
        let req_payload = vec![];
        let (resp_msg, resp_payload) = mc.send_message(req_msg, &req_payload).await.unwrap();
        println!("Resp: ({resp_msg:?}, {resp_payload:?})");
        assert!(req_msg == resp_msg);
        assert!(resp_payload == resp_payload);
        let req_payload = vec![1, 2, 3, 4, 5];
        let (resp_msg, resp_payload) = mc.send_message(req_msg, &req_payload).await.unwrap();
        assert!(req_msg == resp_msg);
        assert!(resp_payload == resp_payload);
        println!("Resp: ({resp_msg:?}, {resp_payload:?})");
    }

    #[tokio::test]
    async fn basic_test() {
        run_basic_test().await;
    }

    async fn run_scaling_test() {
        let mc = MessagingClient::new("messaging", "echo").await;
        // Send messages without payloads.
        let mut num_direct_calls = 0;
        for i in 0..100 {
            let (resp, was_direct) = mc.send_message_internal("essai", &vec![]).await;
            println!("Round {}/100. Resp: {resp:?}", i + 1);
            if was_direct {
                num_direct_calls += 1;
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
        // The last ten rounds should almost certainly be direct calls.
        println!("Number of direct calls: {num_direct_calls}.");
        assert!(num_direct_calls >= 10);
        // Send messages with payloads.
        let mut num_direct_calls = 0;
        for i in 0..100 {
            let (resp, was_direct) = mc
                .send_message_internal("essai", &vec![1, 2, 3, 4, 5])
                .await;
            println!("Round {}/100. Resp: {resp:?}", i + 1);
            if was_direct {
                num_direct_calls += 1;
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
        // Most calls should be direct calls since last phase already scaled up ecs.
        println!("Number of direct calls: {num_direct_calls}.");
        assert!(num_direct_calls >= 50);
    }

    #[tokio::test]
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
