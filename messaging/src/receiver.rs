use common::FunctionInstance;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct Receiver {
    msgs_handles: Arc<Mutex<HashMap<String, (String, bool, String)>>>,
    received_msgs: Arc<Mutex<Vec<String>>>,
    sqs_client: aws_sdk_sqs::Client,
    recv_queue_url: String,
}

#[async_trait::async_trait]
impl FunctionInstance for Receiver {
    /// Invoke should only be called to wake up sleeping lambda.
    /// Use this to process messages in batch then go to sleep.
    async fn invoke(&self, arg: Value) -> Value {
        let msg_id: String = serde_json::from_value(arg).unwrap();
        let content = self.receive_msg(&msg_id).await;
        serde_json::to_value(content).unwrap()
    }
}

impl Receiver {
    pub async fn new() -> Self {
        let shared_config = aws_config::load_from_env().await;
        let sqs_client = aws_sdk_sqs::Client::new(&shared_config);
        let namespace = std::env::var("NAMESPACE").unwrap();
        let name = std::env::var("NAME").unwrap();
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
        Receiver {
            sqs_client,
            recv_queue_url,
            received_msgs: Arc::new(Mutex::new(Vec::new())),
            msgs_handles: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn receive_msg(&self, msg_id: &str) -> Option<(String, bool)> {
        let mut found = false;
        let mut first_try = true;
        loop {
            if !first_try && !found {
                return None;
            }
            first_try = false;
            let content_receipt = {
                let mut msgs_handles = self.msgs_handles.lock().await;
                msgs_handles.remove(msg_id)
            };
            if let Some((content, has_payload, receipt)) = content_receipt {
                let mut received_msgs = self.received_msgs.lock().await;
                received_msgs.push(receipt);
                if received_msgs.len() == 10 {
                    let mut delete_request = self
                        .sqs_client
                        .delete_message_batch()
                        .queue_url(&self.recv_queue_url);
                    for (id, receipt) in received_msgs.iter().enumerate() {
                        delete_request = delete_request.entries(
                            aws_sdk_sqs::model::DeleteMessageBatchRequestEntry::builder()
                                .id(id.to_string())
                                .receipt_handle(receipt)
                                .build(),
                        )
                    }
                    let _ = delete_request.send().await;
                    received_msgs.clear();
                }
                return Some((content, has_payload));
            } else {
                // Read up to 100 messages.
                for _ in 0..10 {
                    let resp = self
                        .sqs_client
                        .receive_message()
                        .queue_url(&self.recv_queue_url)
                        .max_number_of_messages(10)
                        .send()
                        .await;
                    let resp = match resp {
                        Err(_) => return None,
                        Ok(resp) => resp,
                    };
                    let new_msgs = match resp.messages() {
                        None => return None,
                        Some(new_msgs) => new_msgs,
                    };

                    let mut msgs_handles = self.msgs_handles.lock().await;
                    for new_msg in new_msgs {
                        let body = new_msg.body().unwrap();
                        println!("BODY: {body}");
                        let (new_msg_id, content, has_payload): (String, String, bool) =
                            serde_json::from_str(body).unwrap();
                        let receipt: String = new_msg.receipt_handle().unwrap().into();
                        if new_msg_id == msg_id {
                            found = true;
                        }
                        msgs_handles.insert(new_msg_id, (content, has_payload, receipt));
                    }
                    if found {
                        break;
                    }
                }
            }
        }
    }
}
