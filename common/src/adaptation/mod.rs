pub mod backend;
pub mod deploy;
pub mod frontend;

use crate::{clean_die, leasing::Leaser};
use aws_sdk_dynamodb::model::{AttributeAction, AttributeValue, AttributeValueUpdate};
use aws_sdk_sqs::model::DeleteMessageBatchRequestEntry;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

use super::{full_scaling_queue_name, scaling_table_name};
const NUM_RETRIES: u64 = 20;
const RETRY_INTERVAL: u64 = 1;

#[async_trait::async_trait]
pub trait Rescaler: Send + Sync {
    /// Return the new scale and the new scaling info.
    async fn rescale(
        &self,
        scaling_state: &ServerfulScalingState,
        curr_timestamp: chrono::DateTime<chrono::Utc>,
        metrics: Vec<Value>,
    ) -> (u64, Value);
}

pub const RESCALING_INTERVAL: u64 = 10;
pub const SERVERFUL_REFRESH_TIME: u64 = 15;
pub const SERVERFUL_INACTIVE_TIME: u64 = 25;

/// Information about a serverful instance.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ServerfulInstance {
    pub url: String,
    pub private_url: String,
    pub active_time: chrono::DateTime<chrono::Utc>,
    pub join_time: chrono::DateTime<chrono::Utc>,
    pub az: String,
    pub id: String,
    pub custom_info: Value,
}

/// Information about a particular service.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ServerfulScalingState {
    pub peers: HashMap<String, ServerfulInstance>,
    pub namespace: String,
    pub name: String,
    pub state_id: String,
    pub initialized: bool,
    pub revision: String,
    pub current_scale: u64,
    pub last_rescale: chrono::DateTime<chrono::Utc>,
    pub deployment: Value,
    pub scaling_info: Option<Value>,
}

impl ServerfulScalingState {
    pub fn new(
        namespace: &str,
        name: &str,
        revision: &str,
        deployment: Value,
        timestamp: chrono::DateTime<chrono::Utc>,
    ) -> Self {
        ServerfulScalingState {
            peers: HashMap::new(),
            namespace: namespace.into(),
            name: name.into(),
            state_id: uuid::Uuid::new_v4().to_string(),
            initialized: false,
            current_scale: 0,
            last_rescale: timestamp,
            scaling_info: None,
            deployment,
            revision: revision.into(),
        }
    }
}

/// Request to a scaler.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ScalerReq {
    pub namespace: String,
    pub name: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Scaling part of the adapter.
#[derive(Clone)]
pub struct AdapterScaling {
    subsystem: String,
    scaling_table: String,
    rescaler: Option<Arc<dyn Rescaler>>,
    sqs_client: aws_sdk_sqs::Client,
    dynamo_client: aws_sdk_dynamodb::Client,
    leaser: Leaser,
    deployer: deploy::AdapterDeployment,
}

impl AdapterScaling {
    pub async fn new(rescaler: Option<Arc<dyn Rescaler>>, subsystem: &str) -> AdapterScaling {
        let shared_config = aws_config::load_from_env().await;
        let sqs_client = aws_sdk_sqs::Client::new(&shared_config);
        let dynamo_client = aws_sdk_dynamodb::Client::new(&shared_config);
        let scaling_table = scaling_table_name(subsystem);
        let leaser = Leaser::new(&scaling_table).await;
        let deployer = deploy::AdapterDeployment::new().await;
        AdapterScaling {
            subsystem: subsystem.into(),
            scaling_table,
            rescaler,
            sqs_client,
            dynamo_client,
            leaser,
            deployer,
        }
    }

    pub async fn invoke(&self, req: Value) -> Value {
        println!("Rescaler req: {req}");
        let req: ScalerReq = serde_json::from_value(req).unwrap();
        let resp = self.handle_request(&req, true).await;
        serde_json::to_value(resp).unwrap()
    }

    pub async fn handle_request(&self, req: &ScalerReq, scale_ecs: bool) -> ServerfulScalingState {
        let queue_name = full_scaling_queue_name(&self.subsystem, &req.namespace, &req.name);
        // Get scaling state.
        let mut scaling_state = self.get_scaling_state(req).await;
        // Try to initialize.
        if !scaling_state.initialized {
            println!("Initializing!");
            self.deployer
                .create_service(&self.subsystem, &req.namespace, &req.name)
                .await;
            self.write_scaling_state(&mut scaling_state, "").await;
            return scaling_state;
        }
        // Check rescaling interval.
        let since_last_scaling = req
            .timestamp
            .signed_duration_since(scaling_state.last_rescale);
        if since_last_scaling < chrono::Duration::seconds(RESCALING_INTERVAL as i64) {
            println!("Since last scaling: {since_last_scaling:?}!");
            return scaling_state;
        }
        // Check if only running scaler.
        println!("Has lease: {}!", self.leaser.lease_id());
        let has_lease = self.leaser.renew(&queue_name, false).await;
        if !has_lease {
            println!("No lease: {}!", self.leaser.lease_id());
            return scaling_state;
        }
        // Read and apply metrics.
        let old_id = scaling_state.state_id.clone();
        let (metrics, receipts, queue_url) = self.scan_metrics(req).await;
        self.update_scaling_state(&mut scaling_state, req, metrics)
            .await;
        if scale_ecs {
            self.deployer
                .scale_service(
                    &self.subsystem,
                    &req.namespace,
                    &req.name,
                    scaling_state.current_scale as i32,
                )
                .await;
        }
        loop {
            cleanup_instances(&mut scaling_state);
            let updated = self.write_scaling_state(&mut scaling_state, &old_id).await;
            if updated {
                println!("Wrote scaling state: {scaling_state:?}");
                self.delete_metrics(&receipts, &queue_url).await;
                return scaling_state;
            } else {
                let new_scaling_state = self.get_scaling_state(req).await;
                // Failed write is not due to metrics update, but to peers update.
                if new_scaling_state.last_rescale == scaling_state.last_rescale {
                    scaling_state.peers = new_scaling_state.peers;
                    scaling_state.state_id = new_scaling_state.state_id;
                    continue;
                } else {
                    // Conflicting metrics update should almost never happed. But it's okay.
                    return new_scaling_state;
                }
            }
        }
    }

    /// Scan metrics by name.
    async fn scan_metrics(&self, req: &ScalerReq) -> (Vec<Value>, Vec<String>, String) {
        let queue_name = full_scaling_queue_name(&self.subsystem, &req.namespace, &req.name);
        let mut queue_url = String::new();
        for _ in 0..NUM_RETRIES {
            let resp = self
                .sqs_client
                .get_queue_url()
                .queue_name(&queue_name)
                .send()
                .await;
            let resp = match resp {
                Ok(resp) => resp,
                Err(x) => {
                    println!("{x:?}");
                    continue;
                }
            };
            queue_url = resp.queue_url().unwrap().into();
            break;
        }

        let mut metrics = Vec::new();
        let mut receipts = Vec::new();
        let max_num_batches = 50; // just reasonable amount (>10, but < 100)
        for _ in 0..max_num_batches {
            let msgs = self
                .sqs_client
                .receive_message()
                .queue_url(&queue_url)
                .max_number_of_messages(10)
                .send()
                .await;
            let msgs = match msgs {
                Ok(msgs) => msgs,
                _ => continue,
            };
            let msgs = if let Some(msgs) = msgs.messages() {
                if msgs.is_empty() {
                    break;
                }
                msgs
            } else {
                break;
            };

            for msg in msgs {
                receipts.push(msg.receipt_handle().unwrap().to_string());
                let new_metrics: Vec<Value> = serde_json::from_str(msg.body().unwrap()).unwrap();
                for metric in new_metrics {
                    metrics.push(metric);
                }
            }
        }

        (metrics, receipts, queue_url)
    }

    /// Write new scaling if old one has not changed.
    async fn write_scaling_state(
        &self,
        scaling_state: &mut ServerfulScalingState,
        old_id: &str,
    ) -> bool {
        let first = if !scaling_state.initialized {
            scaling_state.initialized = true;
            true
        } else {
            false
        };
        let ss = serde_json::to_string(&scaling_state).unwrap();
        let gc_ttl = scaling_state
            .last_rescale
            .checked_add_signed(chrono::Duration::minutes(10));
        let gc_ttl = gc_ttl.unwrap().timestamp().to_string();
        println!("Writing scaling state: {scaling_state:?}!");
        let mut put = self
            .dynamo_client
            .put_item()
            .table_name(&self.scaling_table)
            .item(
                "namespace",
                AttributeValue::S(scaling_state.namespace.clone()),
            )
            .item("name", AttributeValue::S(scaling_state.name.clone()))
            .item("state", AttributeValue::S(ss))
            .item(
                "state_id",
                AttributeValue::S(scaling_state.state_id.clone()),
            )
            .item("gc_ttl", AttributeValue::N(gc_ttl));
        if !first {
            put = put
                .condition_expression("attribute_not_exists(#namespace) OR #state_id = :old_id")
                .expression_attribute_names("#namespace", "namespace")
                .expression_attribute_names("#state_id", "state_id")
                .expression_attribute_values(":old_id", AttributeValue::S(old_id.into()));
        }
        let resp = put.send().await;
        if resp.is_err() {
            let err = format!("{resp:?}");
            println!("Dynamodb err: {err}");
            if !err.contains("ConditionalCheckFailedException") {
                return clean_die(&format!("Unhandled dynamo error: {err}")).await;
            }
            false
        } else {
            true
        }
    }

    /// Delete a list of metrics using receipts.
    async fn delete_metrics(&self, receipts: &[String], queue_url: &str) {
        let mut i = 0;
        while i < receipts.len() {
            let lo = i;
            let hi = if i + 10 < receipts.len() {
                i + 10
            } else {
                receipts.len()
            };
            let batch = &receipts[lo..hi];
            i = hi;
            let mut request = self.sqs_client.delete_message_batch().queue_url(queue_url);
            for (j, handle) in batch.iter().enumerate() {
                request = request.entries(
                    DeleteMessageBatchRequestEntry::builder()
                        .id(format!("entry{j}"))
                        .receipt_handle(handle)
                        .build(),
                );
            }
            let _ = request.send().await;
        }
    }

    /// Forcibly reset scaling state.
    /// Only called in tests, so ok to unwrap() without retries.
    pub async fn reset_revision(&self, namespace: &str) {
        self.dynamo_client
            .update_item()
            .table_name(&self.scaling_table)
            .key("namespace", AttributeValue::S("system".into()))
            .key("name", AttributeValue::S(namespace.into()))
            .attribute_updates(
                "revision",
                AttributeValueUpdate::builder()
                    .action(AttributeAction::Put)
                    .value(AttributeValue::S(uuid::Uuid::new_v4().to_string()))
                    .build(),
            )
            .send()
            .await
            .unwrap();
    }

    /// Read the current deployment.
    async fn read_deployment(&self, namespace: &str) -> (String, Value) {
        for _ in 0..NUM_RETRIES {
            let item = self
                .dynamo_client
                .get_item()
                .table_name(&self.scaling_table)
                .consistent_read(false)
                .key("namespace", AttributeValue::S("system".into()))
                .key("name", AttributeValue::S(namespace.into()))
                .send()
                .await;
            let item = match item {
                Ok(item) => item,
                Err(x) => {
                    println!("{x:?}");
                    tokio::time::sleep(std::time::Duration::from_secs(RETRY_INTERVAL)).await;
                    continue;
                }
            };
            if let Some(item) = item.item() {
                let v = item.get("revision").unwrap();
                let revision = v.as_s().unwrap();
                let v = item.get("deployment").unwrap();
                let deployment = v.as_s().unwrap();
                return (revision.into(), serde_json::from_str(deployment).unwrap());
            } else {
                return ("".into(), Value::Null);
            }
        }
        clean_die("scaler cannot read deployment.").await
    }

    /// Read scaling state.
    async fn get_scaling_state(&self, req: &ScalerReq) -> ServerfulScalingState {
        for _ in 0..NUM_RETRIES {
            let (revision, deployment) = self.read_deployment(&req.namespace).await;
            let item = self
                .dynamo_client
                .get_item()
                .table_name(&self.scaling_table)
                .consistent_read(false)
                .key("namespace", AttributeValue::S(req.namespace.clone()))
                .key("name", AttributeValue::S(req.name.clone()))
                .send()
                .await;
            let item = match item {
                Ok(item) => item,
                Err(x) => {
                    println!("{x:?}");
                    tokio::time::sleep(std::time::Duration::from_secs(RETRY_INTERVAL)).await;
                    continue;
                }
            };
            if let Some(item) = item.item() {
                let scaling_state = item.get("state").unwrap().as_s().unwrap();
                let scaling_state: ServerfulScalingState =
                    serde_json::from_str(scaling_state).unwrap();
                if scaling_state.revision == revision {
                    return scaling_state;
                } else {
                    return ServerfulScalingState::new(
                        &req.namespace,
                        &req.name,
                        &revision,
                        deployment,
                        req.timestamp,
                    );
                }
            } else {
                return ServerfulScalingState::new(
                    &req.namespace,
                    &req.name,
                    &revision,
                    deployment,
                    req.timestamp,
                );
            }
        }
        clean_die("scaler cannot get scaling state!").await
    }

    /// Accumulate and update metrics.
    async fn update_scaling_state(
        &self,
        scaling_state: &mut ServerfulScalingState,
        req: &ScalerReq,
        metrics: Vec<Value>,
    ) {
        // Compute new scaling info.
        let rescaler = match self.rescaler.as_ref() {
            Some(rescaler) => rescaler,
            _ => {
                return clean_die("rescaler not provided").await;
            }
        };
        let (curr_scale, scaling_info) = rescaler
            .rescale(scaling_state, req.timestamp, metrics)
            .await;
        scaling_state.current_scale = curr_scale;
        println!("Setting scaling info: {scaling_info}");
        scaling_state.scaling_info = Some(scaling_info);
        // Advance state.
        scaling_state.last_rescale = req.timestamp;
        scaling_state.state_id = uuid::Uuid::new_v4().to_string();
    }
}

/// Cleanup stale instances.
pub fn cleanup_instances(scaling_state: &mut ServerfulScalingState) {
    let mut to_delete = Vec::new();
    let grace_period = chrono::Duration::seconds(SERVERFUL_INACTIVE_TIME as i64);
    for (id, info) in scaling_state.peers.iter() {
        if scaling_state
            .last_rescale
            .signed_duration_since(info.active_time)
            > grace_period
        {
            to_delete.push(id.clone());
        }
    }
    for id in to_delete {
        scaling_state.peers.remove(&id);
    }
}
