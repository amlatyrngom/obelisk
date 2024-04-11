use common::adaptation::ServerfulScalingState;
use common::{ActorInstance, FunctionInstance, ServiceInstance};
use persistence::PersistentLog;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

const UNLOCK_SLEEP_TIME_MS: u64 = 100;
const INDIRECT_WAIT_TIME_MS: u64 = 40;

#[derive(Clone)]
pub struct MessagingHandler {
    s3_client: aws_sdk_s3::Client,
    actor_instance: Arc<dyn ActorInstance>,
    plog: Arc<PersistentLog>,
    terminating: Arc<Mutex<bool>>,
    db_lock: Arc<RwLock<usize>>,
    // namespace: String,
    // name: String,
}

#[derive(Serialize, Deserialize)]
pub enum HandlerReq {
    ReleaseLock,
    DirectMsg(String), // Payload is encoded in HTTP body.
    LambdaMsg(String, Vec<u8>),
    IndirectMsg,
}

#[derive(Serialize, Deserialize)]
pub enum HandlerResp {
    ReleaseLock,
    IndirectMsg,
    LambdaMsg(String, Vec<u8>),
}

#[async_trait::async_trait]
impl FunctionInstance for MessagingHandler {
    /// Invoke should only be called to wake up sleeping lambda.
    /// Use this to process messages in batch then go to sleep.
    async fn invoke(&self, arg: Value) -> Value {
        // Parse message.
        let lambda_msg: HandlerReq = serde_json::from_value(arg).unwrap();
        let resp = match lambda_msg {
            HandlerReq::ReleaseLock => {
                // Release lock.
                println!("Releasing lock.");
                let db_lock = self.db_lock.clone().write_owned().await;
                tokio::spawn(async move {
                    // Die after responding.
                    let _db_lock = db_lock;
                    tokio::time::sleep(std::time::Duration::from_millis(UNLOCK_SLEEP_TIME_MS))
                        .await;
                    std::process::exit(1);
                });
                self.plog.db.release().await;
                println!("Lock released. Sleeping...");
                HandlerResp::ReleaseLock
            }
            HandlerReq::LambdaMsg(msg, payload) => {
                let mut db_lock = self.db_lock.clone().write_owned().await;
                println!("DBLock: {}", *db_lock);
                let has_lock = if *db_lock == 0 {
                    self.plog.db.acquire(false).await
                } else {
                    true
                };
                if has_lock {
                    *db_lock += 1;
                    let (meta, payload) = self.actor_instance.message(msg, payload).await;
                    HandlerResp::LambdaMsg(meta, payload)
                } else {
                    println!("Lost lock ownership. Dying...");
                    std::process::exit(1);
                }
            }
            HandlerReq::IndirectMsg => {
                let mut db_lock = self.db_lock.clone().write_owned().await;
                println!("DBLock: {}", *db_lock);
                let has_lock = if *db_lock == 0 {
                    self.plog.db.acquire(false).await
                } else {
                    true
                };
                if has_lock {
                    *db_lock += 1;
                    let handled = self.handle_message_batch().await;
                    if !handled {
                        // Tentatively wait for message to be available.
                        tokio::time::sleep(std::time::Duration::from_millis(INDIRECT_WAIT_TIME_MS))
                            .await;
                        self.handle_message_batch().await;
                    }
                } else {
                    println!("Lost lock ownership. Dying...");
                    std::process::exit(1);
                }
                HandlerResp::IndirectMsg
            }
            _ => {
                panic!("Impossible Message for Lambda");
            }
        };
        // Return
        serde_json::to_value(resp).unwrap()
    }
}

#[async_trait::async_trait]
impl ServiceInstance for MessagingHandler {
    /// Direct calls.
    async fn call(&self, meta: String, arg: Vec<u8>) -> (String, Vec<u8>) {
        // // On first call, acquire db lock.
        // let already_locked = {
        //     let db_lock = self.db_lock.read().await;
        //     *db_lock > 0
        // };
        // if !already_locked {
        //     let mut db_lock = self.db_lock.write().await;
        //     if *db_lock == 0 {
        //         *db_lock = 1;
        //         PersistentLog::pause_lambda_lock(&self.namespace, &self.name).await;
        //         let acquired = self.plog.db.acquire(true).await;
        //         if !acquired {
        //             std::process::exit(1);
        //         }
        //     }
        // }
        let req: HandlerReq = serde_json::from_str(&meta).unwrap();
        match req {
            HandlerReq::IndirectMsg => {
                println!("Received request for indirect message!");
                let handled = self.handle_message_batch().await;
                if !handled {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    self.handle_message_batch().await;
                }
                ("".into(), vec![])
            }
            HandlerReq::DirectMsg(meta) => {
                // Process request.
                self.actor_instance.message(meta, arg).await
            }
            _ => {
                panic!("Impossible message for container!");
            }
        }
    }

    async fn custom_info(&self, _scaling_state: &ServerfulScalingState) -> Value {
        println!("Custom Info: {_scaling_state:?}");
        Value::Null
    }

    async fn terminate(&self) {
        self.actor_instance.checkpoint(true).await;
        let mut terminating = self.terminating.lock().await;
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
        let mode = std::env::var("EXECUTION_MODE").unwrap_or_else(|_| "messaging_lambda".into());
        let shared_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let s3_client = aws_sdk_s3::Client::new(&shared_config);
        let handler = MessagingHandler {
            plog,
            s3_client,
            actor_instance,
            terminating: Arc::new(Mutex::new(false)),
            db_lock: Arc::new(RwLock::new(0)),
            // namespace: namespace.into(),
            // name: name.into(),
        };
        if mode != "messaging_lambda" {
            // ECS takes ~10s before becoming available. So do indirect messaging.
            let handler = handler.clone();
            tokio::spawn(async move {
                handler.message_queue_thread().await;
            });
        }
        handler
    }

    async fn message_queue_thread(&self) {
        println!("Running message queue thread for 5 minutes!");
        let starting_time = std::time::Instant::now();
        loop {
            let current_time = std::time::Instant::now();
            let since = current_time.duration_since(starting_time);
            if since > std::time::Duration::from_secs(60 * 5) {
                println!("Exiting message queue thread after");
                return;
            }
            let terminating: bool = {
                let terminating = self.terminating.lock().await;
                *terminating
            };
            if terminating {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            // Handle all available messages.
            let mut handled = self.handle_message_batch().await;
            while handled {
                // While last batch not empty.
                handled = self.handle_message_batch().await;
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
                // Delete
                tokio::spawn(async move {
                    let _ = s3_client
                        .delete_object()
                        .bucket(&common::bucket_name())
                        .key(&key)
                        .send()
                        .await;
                });
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
                        None
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
                let body = aws_sdk_s3::primitives::ByteStream::from(body);
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
        // Wait for writes.
        for t in s3_writes {
            let _ = t.await.unwrap();
        }
    }
}
