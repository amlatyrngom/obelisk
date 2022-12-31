use common::adaptation::ServerfulScalingState;
use common::{ActorInstance, FunctionInstance, ServiceInstance};
use persistence::PersistentLog;
use serde_json::Value;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct MessagingHandler {
    sqs_client: aws_sdk_sqs::Client,
    s3_client: aws_sdk_s3::Client,
    actor_instance: Arc<dyn ActorInstance>,
    plog: Arc<PersistentLog>,
    send_queue_url: String,
    recv_queue_url: String,
    terminating: Arc<Mutex<bool>>,
}

#[async_trait::async_trait]
impl FunctionInstance for MessagingHandler {
    /// Invoke should only be called to wake up sleeping lambda.
    /// Use this to process messages in batch then go to sleep.
    async fn invoke(&self, _arg: Value) -> Value {
        // Try acquiring db lock to decrease likelihood of conflict.
        println!("Invoke called");
        let acquired = self.plog.db.acquire().await;
        if !acquired {
            println!("Lock not acquired");
            return Value::Null;
        }
        println!("Lock acquired");
        // Handle 10 batches.
        self.handle_message_batches(1).await;
        // Release db lock.
        self.plog.db.release().await;
        Value::Null
    }
}

#[async_trait::async_trait]
impl ServiceInstance for MessagingHandler {
    async fn call(&self, meta: String, arg: Vec<u8>) -> (String, Vec<u8>) {
        let (_msg_id, msg): (String, String) = serde_json::from_str(&meta).unwrap();
        self.actor_instance.message(msg, arg).await
    }

    async fn custom_info(&self, _scaling_state: &ServerfulScalingState) -> Value {
        Value::Null
    }

    async fn terminate(&self) {
        self.actor_instance.checkpoint(true).await;
        self.plog.terminate().await;
        let mut terminating = self.terminating.lock().unwrap();
        *terminating = true;
    }
}

impl MessagingHandler {
    pub async fn new(
        plog: Arc<PersistentLog>,
        actor_instance: Arc<dyn ActorInstance>,
        namespace: &str,
        name: &str,
    ) -> Self {
        let mode = std::env::var("EXECUTION_MODE").unwrap_or("messaging_lambda".into());
        let shared_config = aws_config::load_from_env().await;
        let sqs_client = aws_sdk_sqs::Client::new(&shared_config);
        let s3_client = aws_sdk_s3::Client::new(&shared_config);
        let send_queue_name = super::send_queue_name(&namespace, &name);
        let send_queue_url: String = sqs_client
            .get_queue_url()
            .queue_name(&send_queue_name)
            .send()
            .await
            .unwrap()
            .queue_url()
            .unwrap()
            .into();
        let recv_queue_name = super::recv_queue_name(&namespace, &name);
        let recv_queue_url: String = sqs_client
            .get_queue_url()
            .queue_name(&recv_queue_name)
            .send()
            .await
            .unwrap()
            .queue_url()
            .unwrap()
            .into();

        let handler = MessagingHandler {
            plog,
            actor_instance,
            sqs_client,
            s3_client,
            send_queue_url,
            recv_queue_url,
            terminating: Arc::new(Mutex::new(false)),
        };
        if mode != "messaging_lambda" {
            let handler = handler.clone();
            tokio::spawn(async move {
                handler.message_queue_thread().await;
            });
        }
        handler
    }

    async fn message_queue_thread(&self) {
        loop {
            let terminating: bool = {
                let terminating = self.terminating.lock().unwrap();
                *terminating
            };
            if terminating {
                return;
            }
            self.handle_message_batches(10).await;
        }
    }

    async fn handle_message_batches(&self, batch_count: usize) {
        // Handle 10 batches.
        let mut ts = Vec::new();
        let this = Arc::new(self.clone());
        for _ in 0..batch_count {
            let this = this.clone();
            ts.push(tokio::spawn(async move {
                this.handle_message_batch().await;
            }));
        }
        for t in ts {
            let _ = t.await.unwrap();
        }
        self.actor_instance.checkpoint(false).await;
    }

    /// Handle a batch of messages.
    async fn handle_message_batch(&self) -> usize {
        let batch = self.take_message_batch().await;
        let batch_len = batch.len();
        if batch_len == 0 {
            return 0;
        }
        let mut ts = Vec::new();
        for (msg_id, content, payload, had_payload, receipt) in batch {
            let actor_instance = self.actor_instance.clone();
            ts.push(tokio::spawn(async move {
                let (content, payload) = actor_instance.message(content, payload).await;
                let content: (String, String, bool) =
                    (msg_id.clone(), content, !payload.is_empty());
                let content = serde_json::to_string(&content).unwrap();
                (msg_id, content, payload, had_payload, receipt)
            }));
        }
        let mut results = Vec::new();
        for t in ts {
            results.push(t.await.unwrap());
        }
        self.finish_message_batch(results).await;
        batch_len
    }

    /// Return vector of message id, content, payload, receipt.
    async fn take_message_batch(&self) -> Vec<(String, String, Vec<u8>, bool, String)> {
        let resp = self
            .sqs_client
            .receive_message()
            .queue_url(&self.send_queue_url)
            .max_number_of_messages(10)
            .wait_time_seconds(1)
            .send()
            .await;
        let msgs: Vec<(String, String, bool, String)> = resp.map_or(vec![], |resp| {
            resp.messages().map_or(vec![], |msgs| {
                msgs.into_iter()
                    .map(|msg| {
                        let (msg_id, content, has_payload): (String, String, bool) =
                            serde_json::from_str(msg.body().unwrap()).unwrap();
                        let receipt: String = msg.receipt_handle().unwrap().into();
                        (msg_id, content, has_payload, receipt)
                    })
                    .collect()
            })
        });
        println!("Read msgs from send queue: {:?}", msgs.len());
        let mut payload_reads = Vec::new();
        for (msg_id, content, has_payload, receipt) in msgs {
            let s3_client = self.s3_client.clone();
            payload_reads.push(tokio::spawn(async move {
                if has_payload {
                    println!("Reading payload of {msg_id}");
                    let s3_key = format!("{}/send/{msg_id}", common::messaging_s3_dir());
                    let resp = s3_client
                        .get_object()
                        .bucket(&common::bucket_name())
                        .key(s3_key)
                        .send()
                        .await;
                    println!("Called S3!");
                    if let Ok(resp) = resp {
                        let payload: Vec<u8> =
                            resp.body.collect().await.unwrap().into_bytes().to_vec();
                        println!("Got payload {msg_id}: {payload:?}");
                        Some((msg_id, content, payload, has_payload, receipt))
                    } else {
                        println!("Payload error for {msg_id}.");
                        None
                    }
                } else {
                    println!("No payload for {msg_id}.");
                    Some((msg_id, content, Vec::new(), has_payload, receipt))
                }
            }));
        }
        let mut msgs: Vec<(String, String, Vec<u8>, bool, String)> = Vec::new();
        for t in payload_reads {
            let t = t.await.unwrap();
            if let Some(msg) = t {
                msgs.push(msg);
            }
        }
        msgs
    }

    /// Put response in recv queue + s3, and delete from send queue + s3.
    async fn finish_message_batch(&self, resps: Vec<(String, String, Vec<u8>, bool, String)>) {
        println!("Finishing batch: {:?}", resps.len());
        let mut send_request = self
            .sqs_client
            .send_message_batch()
            .queue_url(&self.recv_queue_url);
        let mut delete_request = self
            .sqs_client
            .delete_message_batch()
            .queue_url(&self.send_queue_url);
        let mut s3_keys_to_delete = Vec::new();
        let mut s3_puts = Vec::new();
        // Construct requests.
        for (msg_id, content, payload, had_payload, receipt) in resps {
            send_request = send_request.entries(
                aws_sdk_sqs::model::SendMessageBatchRequestEntry::builder()
                    .id(&msg_id)
                    .message_body(content)
                    .build(),
            );
            delete_request = delete_request.entries(
                aws_sdk_sqs::model::DeleteMessageBatchRequestEntry::builder()
                    .id(&msg_id)
                    .receipt_handle(receipt)
                    .build(),
            );
            // Mark keys to delete after sending response.
            if had_payload {
                let s3_key = format!("{}/send/{msg_id}", common::messaging_s3_dir());
                s3_keys_to_delete.push(s3_key);
            }
            // Make s3 put.
            if payload.len() > 0 {
                let s3_client = self.s3_client.clone();
                let s3_key = format!("{}/recv/{msg_id}", common::messaging_s3_dir());
                s3_puts.push(tokio::spawn(async move {
                    let body = aws_sdk_s3::types::ByteStream::from(payload.clone());
                    let resp = s3_client
                        .put_object()
                        .bucket(&common::bucket_name())
                        .key(&s3_key)
                        .body(body)
                        .send()
                        .await;
                    match resp {
                        Ok(_) => {}
                        Err(x) => {
                            println!("{x:?}");
                        }
                    }
                }));
            }
        }
        // Wait for s3 puts to finish.
        for t in s3_puts {
            let _ = t.await.unwrap();
        }
        // Wait for queue puts to finish.
        let resp = send_request.clone().send().await;
        match resp {
            Ok(_) => {}
            Err(x) => {
                println!("Send request");
                println!("{x:?}");
            }
        }
        // Delete input messages.
        let resp = delete_request.send().await;
        match resp {
            Ok(_) => {
                let s3_client = self.s3_client.clone();
                let _ = tokio::spawn(async move {
                    for s3_key in s3_keys_to_delete {
                        let delete_resp = s3_client
                            .delete_object()
                            .bucket(&common::bucket_name())
                            .key(&s3_key)
                            .send()
                            .await;
                        println!("Delete resp: {delete_resp:?}")
                    }
                });
            }
            Err(x) => {
                // TODO: Do nothing? Message will be garbage collected anyway.
                println!("{x:?}")
            }
        }
    }
}
