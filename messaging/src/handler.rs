use common::adaptation::ServerfulScalingState;
use common::{ActorInstance, FunctionInstance, ServiceInstance};
use persistence::PersistentLog;
use serde_json::Value;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct MessagingHandler {
    s3_client: aws_sdk_s3::Client,
    actor_instance: Arc<dyn ActorInstance>,
    plog: Arc<PersistentLog>,
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
        // Handle batch.
        let handled = self.handle_message_batch().await;
        if !handled {
            // Tentatively wait for message to be available.
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            self.handle_message_batch().await;
        }
        self.actor_instance.checkpoint(false).await;
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
        _namespace: &str,
        _name: &str,
    ) -> Self {
        let mode = std::env::var("EXECUTION_MODE").unwrap_or("messaging_lambda".into());
        let shared_config = aws_config::load_from_env().await;
        let s3_client = aws_sdk_s3::Client::new(&shared_config);
        let handler = MessagingHandler {
            plog,
            actor_instance,
            s3_client,
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
        let mut should_wait = true;
        let mut last_checkpoint = std::time::Instant::now();
        loop {
            let terminating: bool = {
                let terminating = self.terminating.lock().unwrap();
                *terminating
            };
            if terminating {
                return;
            }
            if should_wait {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
            let processed = self.handle_message_batch().await;
            should_wait = !processed;
            // Try checkpoint every 10 seconds.
            let now = std::time::Instant::now();
            if now.duration_since(last_checkpoint).as_secs() >= 10 {
                last_checkpoint = now;
                let actor_instance = self.actor_instance.clone();
                tokio::spawn(async move {
                    actor_instance.checkpoint(false).await;
                });
            }
        }
    }

    /// Handle a batch of messages.
    async fn handle_message_batch(&self) -> bool {
        let batch = self.take_indirect_messages().await;
        if batch.is_empty() {
            return false;
        }
        let mut ts = Vec::new();
        for (msg_id, msg, payload) in batch {
            let actor_instance = self.actor_instance.clone();
            ts.push(tokio::spawn(async move {
                let (msg, payload) = actor_instance.message(msg, payload).await;
                (msg_id, msg, payload)
            }));
        }
        let mut results = Vec::new();
        for t in ts {
            results.push(t.await.unwrap());
        }
        self.respond_to_indirect_messages(results).await;
        true
    }

    async fn take_indirect_messages(&self) -> Vec<(String, String, Vec<u8>)> {
        let s3_prefix = format!("{}/send/", common::tmp_s3_dir());
        let resp = self
            .s3_client
            .list_objects_v2()
            .bucket(&common::bucket_name())
            .max_keys(20)
            .prefix(s3_prefix)
            .send()
            .await;
        let keys: Vec<String> = match resp {
            Ok(resp) => {
                let empty = vec![];
                let objs = resp.contents().unwrap_or(&empty);
                objs.iter()
                    .map(|obj| obj.key().unwrap().to_string())
                    .collect()
            }
            Err(x) => {
                println!("Err: {x:?}");
                return vec![];
            }
        };
        let mut s3_reads = Vec::new();
        for key in keys {
            let s3_client = self.s3_client.clone();
            s3_reads.push(tokio::spawn(async move {
                let resp = s3_client
                    .get_object()
                    .bucket(&common::bucket_name())
                    .key(&key)
                    .send()
                    .await;
                match resp {
                    Ok(resp) => {
                        let body: Vec<u8> =
                            resp.body.collect().await.unwrap().into_bytes().to_vec();
                        tokio::task::block_in_place(move || {
                            let (msg_id, msg, payload): (String, String, Vec<u8>) =
                                bincode::deserialize(&body).unwrap();
                            Some((msg_id, msg, payload))
                        })
                    }
                    Err(x) => {
                        println!("Err: {x:?}");
                        return None;
                    }
                }
            }));
        }
        let mut msgs = Vec::new();
        for s3_read in s3_reads {
            let resp = s3_read.await.unwrap();
            if let Some(resp) = resp {
                msgs.push(resp);
            }
        }
        msgs
    }

    async fn respond_to_indirect_messages(&self, msgs: Vec<(String, String, Vec<u8>)>) {
        // Write responses.
        let mut s3_writes = Vec::new();
        for (msg_id, msg, payload) in msgs {
            let s3_client = self.s3_client.clone();
            let s3_recv_key = format!("{}/recv/{msg_id}", common::tmp_s3_dir());
            let s3_send_key = format!("{}/send/{msg_id}", common::tmp_s3_dir());
            s3_writes.push(tokio::spawn(async move {
                let body = tokio::task::block_in_place(move || {
                    let body: (String, String, Vec<u8>) = (msg_id, msg, payload);
                    bincode::serialize(&body).unwrap()
                });
                let body = aws_sdk_s3::types::ByteStream::from(body);
                let resp = s3_client
                    .put_object()
                    .bucket(&common::bucket_name())
                    .key(&s3_recv_key)
                    .body(body)
                    .send()
                    .await;
                resp.map_or(None, |_| Some(s3_send_key))
            }));
        }
        // Wait for writes, then delete.
        let mut to_delete: Vec<String> = Vec::new();
        for t in s3_writes {
            let resp = t.await.unwrap();
            if let Some(delete_key) = resp {
                to_delete.push(delete_key);
            }
        }
        // Delete messages.
        let mut s3_deletes = Vec::new();
        for key in to_delete {
            let s3_client = self.s3_client.clone();
            s3_deletes.push(tokio::spawn(async move {
                let _ = s3_client
                    .delete_object()
                    .bucket(&common::bucket_name())
                    .key(&key)
                    .send()
                    .await;
            }));
        }
        for t in s3_deletes {
            let _ = t.await;
        }
    }
}
