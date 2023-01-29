use super::{
    cleanup_instances, ScalerReq, ServerfulInstance, ServerfulScalingState, RESCALING_INTERVAL,
};
use crate::time_service::TimeService;
use crate::{clean_die, full_scaler_name, full_scaling_queue_name, scaling_table_name};
use aws_sdk_dynamodb::model::AttributeValue;
use aws_sdk_sqs::model::QueueAttributeName;
use serde_json::Value;
use std::sync::Arc;
use tokio::signal::unix;
use tokio::sync::RwLock;

pub const MAX_SQS_SIZE_KB: usize = 256 - 64; // Subtract 64 to give user some leeway.
pub const MAX_METRIC_SIZE_KB: usize = 4;
pub const MAX_METRIC_BUFFER: usize = MAX_SQS_SIZE_KB / MAX_METRIC_SIZE_KB;
pub const MAX_METRIC_PUSH_INTERVAL: u64 = 5;
const NUM_RETRIES: u64 = 30;
const RETRY_INTERVAL: u64 = 1;

/// Frontend of an adapter.
#[derive(Clone)]
pub struct AdapterFrontend {
    pub sqs_client: aws_sdk_sqs::Client,
    pub lambda_client: aws_sdk_lambda::Client,
    pub dynamo_client: aws_sdk_dynamodb::Client,
    pub time_service: TimeService,
    pub info: Arc<FrontendInfo>,
    pub has_external_access: bool,
    inner: Arc<RwLock<AdapterFrontendInner>>,
}

#[derive(Clone, Debug)]
pub struct FrontendInfo {
    pub subsystem: String,
    pub namespace: String,
    pub name: String,
    pub scaler_name: String,
    pub queue_url: String,
    pub scaling_table: String,
}

struct AdapterFrontendInner {
    last_push: std::time::Instant,
    curr_metrics: Vec<Value>,
    scaling_state: Option<ServerfulScalingState>,
    peers: Vec<ServerfulInstance>,
}

impl AdapterFrontend {
    /// Return the frontend of the adapter.
    pub async fn new(subsystem: &str, namespace: &str, name: &str) -> Self {
        println!("Creating FrontEnd object!");
        let shared_config = aws_config::load_from_env().await;
        let sqs_client = aws_sdk_sqs::Client::new(&shared_config);
        let lambda_client = aws_sdk_lambda::Client::new(&shared_config);
        let dynamo_client = aws_sdk_dynamodb::Client::new(&shared_config);
        let inner = Arc::new(RwLock::new(AdapterFrontendInner {
            last_push: std::time::Instant::now(),
            scaling_state: None,
            curr_metrics: vec![],
            peers: vec![],
        }));
        let queue_name = full_scaling_queue_name(subsystem, namespace, name);
        let scaler_name = full_scaler_name(subsystem);
        let scaling_table = scaling_table_name(subsystem);
        // Special case: lambdas inside VPCs have no internet access.
        let execution_mode = std::env::var("EXECUTION_MODE").unwrap_or_default();
        let has_external_access = execution_mode != "messaging_lambda" && execution_mode != "local";

        for _ in 0..NUM_RETRIES {
            let queue_url: String = if has_external_access {
                let queue_url = sqs_client
                    .get_queue_url()
                    .queue_name(&queue_name)
                    .send()
                    .await;
                let queue_url = match queue_url {
                    Ok(queue_url) => queue_url,
                    Err(x) => {
                        println!("GetQueueURl ({queue_name}) error: {x:?}");
                        let resp = sqs_client
                            .create_queue()
                            .queue_name(&queue_name)
                            .attributes(QueueAttributeName::MessageRetentionPeriod, "60")
                            .attributes(QueueAttributeName::VisibilityTimeout, "10")
                            .send()
                            .await;
                        println!("Create queue resp: {resp:?}");
                        tokio::time::sleep(std::time::Duration::from_secs(RETRY_INTERVAL)).await;
                        continue;
                    }
                };
                queue_url.queue_url().unwrap().into()
            } else {
                "".into()
            };

            let time_service = TimeService::new().await;
            println!("Made front end object");
            let info = Arc::new(FrontendInfo {
                queue_url,
                scaler_name,
                scaling_table,
                subsystem: subsystem.into(),
                namespace: namespace.into(),
                name: name.into(),
            });
            let frontend = AdapterFrontend {
                sqs_client,
                lambda_client,
                dynamo_client,
                time_service,
                info,
                inner,
                has_external_access,
            };

            {
                let frontend = frontend.clone();
                tokio::spawn(async move {
                    frontend.bookkeeping().await;
                });
            }

            return frontend;
        }
        clean_die("front end cannot be created!").await
    }

    /// Read the current deployment.
    async fn force_read_deployment(&self) -> (String, Value) {
        for _ in 0..NUM_RETRIES {
            let item = self
                .dynamo_client
                .get_item()
                .table_name(&self.info.scaling_table)
                .consistent_read(false)
                .key("namespace", AttributeValue::S("system".into()))
                .key("name", AttributeValue::S(self.info.namespace.clone()))
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
                let revision = uuid::Uuid::new_v4().to_string();
                let deployment = Value::Null;
                let _ = self
                    .dynamo_client
                    .put_item()
                    .table_name(&self.info.scaling_table)
                    .item("namespace", AttributeValue::S("system".into()))
                    .item("name", AttributeValue::S(self.info.namespace.clone()))
                    .item("revision", AttributeValue::S(revision.clone()))
                    .item("deployment", AttributeValue::S(deployment.to_string()))
                    .send()
                    .await
                    .unwrap();
                return (revision, Value::Null);
            }
        }
        clean_die("front end cannot read deployment.").await
    }

    pub async fn current_time(&self) -> chrono::DateTime<chrono::Utc> {
        if self.has_external_access {
            self.time_service.current_time().await
        } else {
            chrono::Utc::now()
        }
    }

    pub async fn force_read_instances(&self) -> ServerfulScalingState {
        for _ in 0..NUM_RETRIES {
            let (revision, deployment) = self.force_read_deployment().await;
            let item = self
                .dynamo_client
                .get_item()
                .table_name(&self.info.scaling_table)
                .consistent_read(false)
                .key("namespace", AttributeValue::S(self.info.namespace.clone()))
                .key("name", AttributeValue::S(self.info.name.clone()))
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
            let scaling_state = if let Some(item) = item.item() {
                let scaling_state = item.get("state").unwrap().as_s().unwrap();
                let mut scaling_state: ServerfulScalingState =
                    serde_json::from_str(scaling_state).unwrap();
                if scaling_state.revision == revision {
                    cleanup_instances(&mut scaling_state);
                    scaling_state
                } else {
                    ServerfulScalingState::new(
                        &self.info.namespace,
                        &self.info.name,
                        &revision,
                        deployment,
                        self.current_time().await,
                    )
                }
            } else {
                ServerfulScalingState::new(
                    &self.info.namespace,
                    &self.info.name,
                    &revision,
                    deployment,
                    self.current_time().await,
                )
            };
            {
                let mut inner = self.inner.write().await;
                inner.peers = scaling_state.peers.values().cloned().collect();
                inner.scaling_state = Some(scaling_state.clone());
            }
            return scaling_state;
        }
        clean_die("front end read instances!").await
    }

    pub async fn scaling_state(&self) -> Option<ServerfulScalingState> {
        let inner = self.inner.read().await;
        inner.scaling_state.clone()
    }

    /// Get list of serverful instances.
    pub async fn serverful_instances(&self) -> Vec<ServerfulInstance> {
        let inner = self.inner.read().await;
        inner.peers.clone()
    }

    /// Call to push metrics.
    pub async fn push_collected_metrics(&self, force: bool) {
        let to_push = {
            let mut inner = self.inner.write().await;
            let should_push = if force {
                true
            } else {
                let now = std::time::Instant::now();
                let since_last_push = now.duration_since(inner.last_push);
                since_last_push >= std::time::Duration::from_secs(MAX_METRIC_PUSH_INTERVAL)
            };
            if should_push {
                inner.last_push = std::time::Instant::now();
                let metrics = inner.curr_metrics.clone();
                inner.curr_metrics = vec![];
                metrics
            } else {
                vec![]
            }
        };
        self.push_metrics(&to_push).await;
    }

    /// Push metrics to SQS.
    pub async fn push_metrics(&self, metrics: &[Value]) {
        if !self.has_external_access {
            return;
        }

        for _ in 0..NUM_RETRIES {
            if metrics.is_empty() {
                return;
            }
            let metrics = serde_json::to_string(metrics).unwrap();
            let resp = self
                .sqs_client
                .send_message()
                .queue_url(&self.info.queue_url)
                .message_body(metrics)
                .send()
                .await;
            match resp {
                Ok(_) => return,
                Err(x) => {
                    println!("{x:?}");
                    tokio::time::sleep(std::time::Duration::from_secs(RETRY_INTERVAL)).await;
                    continue;
                }
            }
        }
        return clean_die("front ent cannot push metric!").await;
    }

    /// Invoke scaler.
    pub async fn invoke_scaler(&self) {
        if !self.has_external_access {
            self.force_read_instances().await;
            return;
        }

        for _ in 0..NUM_RETRIES {
            let req = ScalerReq {
                namespace: self.info.namespace.clone(),
                name: self.info.name.clone(),
                timestamp: self.time_service.current_time().await,
            };
            let req = serde_json::to_vec(&req).unwrap();
            let req = aws_smithy_types::Blob::new(req);
            let resp = self
                .lambda_client
                .invoke()
                .function_name(&self.info.scaler_name)
                .payload(req)
                .send()
                .await;
            let resp = match resp {
                Ok(resp) => resp,
                Err(x) => {
                    println!("{x:?}");
                    tokio::time::sleep(std::time::Duration::from_secs(RETRY_INTERVAL)).await;
                    continue;
                }
            };
            if let Some(x) = resp.function_error() {
                if !x.is_empty() {
                    println!("{x:?}");
                    tokio::time::sleep(std::time::Duration::from_secs(RETRY_INTERVAL)).await;
                    continue;
                }
            }
            let resp: Vec<u8> = resp.payload().unwrap().clone().into_inner();
            let serverful_service: ServerfulScalingState = serde_json::from_slice(&resp).unwrap();
            {
                let mut inner = self.inner.write().await;
                inner.peers = serverful_service.peers.values().cloned().collect();
                inner.scaling_state = Some(serverful_service);
            }
            return;
        }
        return clean_die("front end cannot be created!").await;
    }

    /// The metric cannot exceed 4KB serialized size.
    /// If you want something larger, write to S3/EFS first, then pass in a reference.
    /// Or better yet, featurize it first, then push it.
    pub async fn collect_metric(&self, metric: Value) {
        let to_push = {
            let mut inner = self.inner.write().await;
            inner.curr_metrics.push(metric);
            if inner.curr_metrics.len() >= MAX_METRIC_BUFFER {
                inner.last_push = std::time::Instant::now();
                let metrics = inner.curr_metrics.clone();
                inner.curr_metrics = vec![];
                metrics
            } else {
                vec![]
            }
        };
        if to_push.is_empty() {
            return;
        }
        let this = self.clone();
        tokio::spawn(async move {
            this.push_metrics(&to_push).await;
        });
    }

    /// Perform bookkeeping tasks like pushing metrics and invoking rescaler.
    async fn bookkeeping(&self) {
        let mut push_interval =
            tokio::time::interval(std::time::Duration::from_secs(MAX_METRIC_PUSH_INTERVAL));
        push_interval.tick().await;
        let mut refresh_interval =
            tokio::time::interval(std::time::Duration::from_secs(RESCALING_INTERVAL));
        refresh_interval.tick().await;
        loop {
            let terminate_signal = unix::SignalKind::terminate();
            let mut sigterm = unix::signal(terminate_signal).unwrap();
            let sigint = tokio::signal::ctrl_c();
            tokio::select! {
                _ = push_interval.tick() => {
                    self.push_collected_metrics(false).await;
                },
                _ = refresh_interval.tick() => {
                    self.invoke_scaler().await;
                },
                _ = sigint => {
                    self.push_collected_metrics(true).await;
                    self.invoke_scaler().await;
                    return;
                }
                _ = sigterm.recv() => {
                    self.push_collected_metrics(true).await;
                    self.invoke_scaler().await;
                    return;
                }
            }
        }
    }
}
