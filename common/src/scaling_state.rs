use crate::debug_format;
use crate::deployment::{container, lambda};
use crate::wrapper::{InstanceInfo, InstanceStats};
use crate::{
    deployment::{self, HandlerSpec, ServiceSpec},
    leasing::Leaser,
};
use aws_sdk_dynamodb::types::AttributeValue;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::signal::unix;
use tokio::sync::Mutex;

/// Amount of time after which to consider to consider an instance inactive.
pub const SERVERFUL_INACTIVE_TIME_SECS: u64 = 30;
/// Amount of time between rescales.
pub const RESCALING_INTERVAL_SECS: u64 = 10;

/// Information about a serverful instance.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ServerfulInstance {
    pub join_time: chrono::DateTime<chrono::Utc>,
    pub active_time: chrono::DateTime<chrono::Utc>,
    pub instance_info: InstanceInfo,
    pub instance_stats: InstanceStats,
}

/// Information about a particular service.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ScalingState {
    /// Subsystem scaling state.
    pub subsys_state: SubsystemScalingState,
    /// Handler state.
    pub handler_state: Option<HandlerScalingState>,
    /// Time since last rescale.
    pub last_rescale: chrono::DateTime<chrono::Utc>,
    /// Whether the state has been initialized or not.
    pub initialized: bool,
    /// Used to prevent race conditions.
    pub state_id: String,
    /// Used to reset after redeploying.
    pub revision: String,
}

/// Information about a particular subsystem.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SubsystemScalingState {
    /// Subsystem.
    pub subsystem: String,
    /// Namespace of the user of this service.
    pub namespace: String,
    /// Identifier of the user of this service.
    pub identifier: String,
    /// Every serverful instance for each service.
    pub peers: HashMap<String, HashMap<String, ServerfulInstance>>,
    /// Current scaling info <service_name -> scale>.
    pub service_scales: HashMap<String, u64>,
    /// Custom information for rescaling.
    pub scaling_info: Option<String>,
    /// Service specs.
    pub service_specs: HashMap<String, ServiceSpec>,
}

/// Information about a particular subsystem.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct HandlerScalingState {
    /// Subsystem taking care of the scaling.
    pub subsystem: String,
    /// Namespace of the handler.
    pub namespace: String,
    /// Identifier of the handler,
    pub identifier: String,
    /// Serverful peers.
    pub peers: HashMap<String, ServerfulInstance>,
    /// Scales: mem -> count.
    pub handler_scales: HashMap<i32, u64>,
    /// Custom information for rescaling.
    pub scaling_info: Option<String>,
    /// Handler specification.
    pub handler_spec: HandlerSpec,
}

/// Manages scaling state.
#[derive(Clone)]
pub struct ScalingStateManager {
    pub subsystem: String,
    pub namespace: String,
    pub identifier: String,
    pub scaling_table: String,
    pub dynamo_client: aws_sdk_dynamodb::Client,
    pub leaser: Leaser,
    inner: Arc<Mutex<ScalingStateManagerInner>>,
}

struct ScalingStateManagerInner {
    curr_scaling_state: Arc<ScalingState>,
}

impl ScalingState {
    /// Create new scaling state.
    pub fn new(
        subsystem: &str,
        namespace: &str,
        identifier: &str,
        timestamp: chrono::DateTime<chrono::Utc>,
        service_specs: HashMap<String, ServiceSpec>,
        handler_spec: Option<HandlerSpec>,
        revision: &str,
    ) -> Self {
        let service_scales = service_specs
            .iter()
            .map(|(service_name, _)| (service_name.clone(), 0))
            .collect();
        let peers = service_specs
            .iter()
            .map(|(service_name, _)| (service_name.clone(), HashMap::new()))
            .collect();
        let subsys_state = SubsystemScalingState {
            subsystem: subsystem.into(),
            namespace: namespace.into(),
            identifier: identifier.into(),
            peers,
            service_scales,
            scaling_info: None,
            service_specs,
        };
        let handler_state = if let Some(handler_spec) = handler_spec {
            let mems = container::ContainerDeployment::all_avail_mems(
                handler_spec.default_mem,
                handler_spec.scaleup,
            );
            let handler_scales = mems.into_iter().map(|m| (m, 0)).collect();
            Some(HandlerScalingState {
                subsystem: subsystem.into(),
                namespace: namespace.into(),
                identifier: identifier.into(),
                peers: HashMap::new(),
                handler_scales,
                scaling_info: None,
                handler_spec,
            })
        } else {
            None
        };

        ScalingState {
            subsys_state,
            handler_state,
            state_id: uuid::Uuid::new_v4().to_string(),
            initialized: false,
            last_rescale: timestamp,
            revision: revision.into(),
        }
    }
}

impl ScalingStateManager {
    /// Start rescaling thread
    pub async fn start_rescaling_thread(&self) {
        let this = self.clone();
        tokio::spawn(async move {
            this.rescaling_thread().await;
        });
    }

    /// Periodically call the rescaler.
    async fn rescaling_thread(&self) {
        let shared_config = aws_config::load_from_env().await;
        let lambda_client = aws_sdk_lambda::Client::new(&shared_config);
        let duration = std::time::Duration::from_secs(RESCALING_INTERVAL_SECS);
        let mut interval = tokio::time::interval(duration);
        interval.tick().await;
        let terminate_signal = unix::SignalKind::terminate();
        let interrupt_signal = unix::SignalKind::interrupt();
        let mut sigterm = unix::signal(terminate_signal).unwrap();
        let mut sigint = unix::signal(interrupt_signal).unwrap();
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let _ = self.invoke_rescaler(&lambda_client).await;
                },
                _ = sigint.recv() => {
                    let _ = self.invoke_rescaler(&lambda_client).await;
                    return;
                }
                _ = sigterm.recv() => {
                    let _ = self.invoke_rescaler(&lambda_client).await;
                    return;
                }
            }
        }
    }

    /// Start the refresh state.
    /// `as_peer` indicates that the caller is one of the peers in the scaling state.
    pub async fn start_refresh_thread(&self) {
        let this = self.clone();
        tokio::spawn(async move {
            this.refresh_thread().await;
        });
    }

    /// Periodically refresh scaling state.
    async fn refresh_thread(&self) {
        let duration = std::time::Duration::from_secs(RESCALING_INTERVAL_SECS);
        let mut interval = tokio::time::interval(duration);
        let terminate_signal = unix::SignalKind::terminate();
        let interrupt_signal = unix::SignalKind::interrupt();
        let mut sigterm = unix::signal(terminate_signal).unwrap();
        let mut sigint = unix::signal(interrupt_signal).unwrap();
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let _ = self.retrieve_scaling_state().await;
                },
                _ = sigint.recv() => {
                    return;
                }
                _ = sigterm.recv() => {
                    return;
                }
            }
        }
    }

    /// Invoke the rescaler.
    pub async fn invoke_rescaler(&self, client: &aws_sdk_lambda::Client) -> Result<String, String> {
        if !crate::has_external_access() {
            return Err("No external access!".into());
        }
        let req = (self.namespace.clone(), self.identifier.clone());
        let req = serde_json::to_vec(&req).unwrap();
        let req = aws_sdk_lambda::primitives::Blob::new(req);
        let fn_name = lambda::LambdaDeployment::rescaler_name(&self.subsystem);
        println!("ScalingStateManager::invoke_rescaler. Name={fn_name}");
        let resp = client
            .invoke()
            .function_name(&fn_name)
            .payload(req)
            .send()
            .await
            .map_err(debug_format!())?;
        if let Some(x) = resp.function_error() {
            if !x.is_empty() {
                return Err(format!("lambda {fn_name} invoke error: {x:?}"));
            }
        }
        if let Some(err) = resp.function_error() {
            Err(err.into())
        } else {
            let resp: Vec<u8> = resp.payload().unwrap().clone().into_inner();
            let resp: String = serde_json::from_slice(&resp).unwrap();
            println!("Resp: {resp:?}.");
            Ok(resp)
        }
    }

    pub fn extract_name(identifier: &str) -> String {
        let mut curr_index = identifier.len();
        loop {
            let curr_char = identifier.as_bytes()[curr_index - 1] as char;
            if curr_char.is_numeric() {
                curr_index -= 1;
            } else {
                break;
            }
        }
        let name = identifier[0..curr_index].to_string();
        name
    }

    /// Create.
    pub async fn new(subsystem: &str, namespace: &str, identifier: &str) -> Self {
        let shared_config = aws_config::load_from_env().await;
        let dynamo_client = aws_sdk_dynamodb::Client::new(&shared_config);
        let scaling_table = deployment::dynamo::DynamoDeployment::scaling_table_name(namespace);
        let leaser: Leaser = Leaser::new(&scaling_table).await;
        let current_time = leaser.current_time().await;
        let scaling_state = Self::make_initial_scaling_state(
            &dynamo_client,
            subsystem,
            namespace,
            identifier,
            current_time,
        )
        .await;
        let inner = Arc::new(Mutex::new(ScalingStateManagerInner {
            curr_scaling_state: Arc::new(scaling_state),
        }));
        let mgr = ScalingStateManager {
            subsystem: subsystem.into(),
            namespace: namespace.into(),
            identifier: identifier.into(),
            scaling_table,
            dynamo_client,
            leaser,
            inner,
        };
        mgr.retrieve_scaling_state().await.unwrap();
        mgr
    }

    /// Get cached scaling state.
    pub async fn current_scaling_state(&self) -> Arc<ScalingState> {
        let inner = self.inner.lock().await;
        inner.curr_scaling_state.clone()
    }

    /// Make initial scaling state
    pub async fn make_initial_scaling_state(
        client: &aws_sdk_dynamodb::Client,
        subsystem: &str,
        namespace: &str,
        identifier: &str,
        current_time: chrono::DateTime<chrono::Utc>,
    ) -> ScalingState {
        let (ns_spec, revision) =
            deployment::dynamo::DynamoDeployment::fetch_namespace_spec(&client, namespace)
                .await
                .unwrap();
        let (subsys_ns_spec, _subsys_revision) =
            deployment::dynamo::DynamoDeployment::fetch_namespace_spec(&client, subsystem)
                .await
                .unwrap();
        let name = Self::extract_name(identifier);
        let handler_spec = ns_spec.handler_specs.get(&name).cloned();
        let handler_spec = if let Some(handler_spec) = handler_spec {
            if handler_spec.subsystem == subsystem {
                Some(handler_spec)
            } else {
                None
            }
        } else {
            None
        };
        let service_specs = subsys_ns_spec.subsystem_spec.unwrap().service_specs;
        let scaling_state = ScalingState::new(
            subsystem,
            namespace,
            identifier,
            current_time,
            service_specs,
            handler_spec,
            &revision,
        );
        scaling_state
    }

    /// Retrieve the current scaling state from DynamoDB.
    pub async fn retrieve_scaling_state(&self) -> Result<ScalingState, String> {
        // Read scaling state.
        let item = self
            .dynamo_client
            .get_item()
            .table_name(&self.scaling_table)
            .consistent_read(true)
            .key("subsystem", AttributeValue::S(self.subsystem.clone()))
            .key("identifier", AttributeValue::S(self.identifier.clone()))
            .send()
            .await
            .map_err(|e| format!("{e:?}"))?;
        let (_ns_spec, revision) = deployment::dynamo::DynamoDeployment::fetch_namespace_spec(
            &self.dynamo_client,
            &self.namespace,
        )
        .await
        .unwrap();
        let mut scaling_state = if let Some(item) = item.item() {
            let scaling_state = item.get("state").unwrap().as_s().unwrap();
            let scaling_state: ScalingState = serde_json::from_str(scaling_state).unwrap();
            if scaling_state.revision == revision {
                scaling_state
            } else {
                let timestamp = self.leaser.current_time().await;
                Self::make_initial_scaling_state(
                    &self.dynamo_client,
                    &self.subsystem,
                    &self.namespace,
                    &self.identifier,
                    timestamp,
                )
                .await
            }
        } else {
            let timestamp = self.leaser.current_time().await;
            Self::make_initial_scaling_state(
                &self.dynamo_client,
                &self.subsystem,
                &self.namespace,
                &self.identifier,
                timestamp,
            )
            .await
        };
        let mut inner = self.inner.lock().await;
        self.cleanup_instances(&mut scaling_state).await;
        inner.curr_scaling_state = Arc::new(scaling_state.clone());
        Ok(scaling_state)
    }

    /// Cleanup stale instances.
    pub async fn cleanup_instances(&self, scaling_state: &mut ScalingState) {
        let curr_time = self.leaser.current_time().await;
        let grace_period = chrono::Duration::seconds(SERVERFUL_INACTIVE_TIME_SECS as i64);
        // Delete state handlers.
        if let Some(handler_state) = &mut scaling_state.handler_state {
            let mut to_delete = Vec::new();
            for (id, info) in handler_state.peers.iter() {
                if scaling_state
                    .last_rescale
                    .signed_duration_since(info.active_time)
                    > grace_period
                {
                    to_delete.push(id.clone());
                }
            }
            for id in to_delete {
                handler_state.peers.remove(&id);
            }
        }
        // Delete state services.
        for (_svc_name, svc_peers) in scaling_state.subsys_state.peers.iter_mut() {
            let mut to_delete = Vec::new();
            for (id, info) in svc_peers.iter() {
                if curr_time.signed_duration_since(info.active_time) > grace_period {
                    to_delete.push(id.clone());
                }
            }
            for id in to_delete {
                svc_peers.remove(&id);
            }
        }
    }

    /// Write new scaling if old one has not changed.
    pub async fn write_scaling_state(
        &self,
        scaling_state: &mut ScalingState,
        old_id: &str,
    ) -> Result<bool, String> {
        let first = if !scaling_state.initialized {
            scaling_state.initialized = true;
            true
        } else {
            false
        };
        let ss = serde_json::to_string(&scaling_state).unwrap();
        let gc_ttl = self
            .leaser
            .current_time()
            .await
            .checked_add_signed(chrono::Duration::minutes(10));
        let gc_ttl = gc_ttl.unwrap().timestamp().to_string();
        let mut put = self
            .dynamo_client
            .put_item()
            .table_name(&self.scaling_table)
            .item("subsystem", AttributeValue::S(self.subsystem.clone()))
            .item("identifier", AttributeValue::S(self.identifier.clone()))
            .item("state", AttributeValue::S(ss))
            .item(
                "state_id",
                AttributeValue::S(scaling_state.state_id.clone()),
            )
            .item("gc_ttl", AttributeValue::N(gc_ttl));
        if !first {
            put = put
                .condition_expression("attribute_not_exists(#subsystem) OR #state_id = :old_id")
                .expression_attribute_names("#subsystem", "subsystem")
                .expression_attribute_names("#state_id", "state_id")
                .expression_attribute_values(":old_id", AttributeValue::S(old_id.into()));
        }
        let resp = put.send().await.map_err(|e| format!("{e:?}"));
        match resp {
            Ok(_) => {
                let mut inner = self.inner.lock().await;
                inner.curr_scaling_state = Arc::new(scaling_state.clone());
                Ok(true)
            }
            Err(e) => {
                println!("Write err: {e:?}");
                if e.contains("ConditionalCheckFailedException") {
                    Ok(false)
                } else {
                    Err(e)
                }
            }
        }
    }

    /// Accumulate and update metrics.
    pub async fn update_scaling_state(
        &self,
        rescaler: &dyn crate::rescaler::Rescaler,
        scaling_state: &mut ScalingState,
        metrics: Vec<Vec<u8>>,
    ) {
        // Compute new scaling info.
        let curr_time = self.leaser.current_time().await;
        let since_last_rescale = curr_time
            .signed_duration_since(scaling_state.last_rescale.clone())
            .to_std()
            .unwrap();
        let rescaling_results = rescaler
            .rescale(
                &scaling_state.subsys_state,
                scaling_state.handler_state.as_ref(),
                since_last_rescale,
                metrics,
            )
            .await;
        // Update state.
        scaling_state.subsys_state.service_scales = rescaling_results.services_scales;
        scaling_state.subsys_state.scaling_info = rescaling_results.services_scaling_info;
        if let Some(handler_state) = &mut scaling_state.handler_state {
            if let Some(handler_scales) = rescaling_results.handler_scales {
                handler_state.handler_scales = handler_scales;
            }
            handler_state.scaling_info = rescaling_results.handler_scaling_info;
        }
        // Advance state.
        scaling_state.last_rescale = curr_time;
        scaling_state.state_id = uuid::Uuid::new_v4().to_string();
        scaling_state.initialized = true;
    }

    /// Update state of a peer.
    pub async fn update_peer(
        &self,
        instance_info: Arc<InstanceInfo>,
        join_time: chrono::DateTime<chrono::Utc>,
        terminating: bool,
    ) -> Result<(), String> {
        let active_time = self.leaser.current_time().await;
        let peer_id = instance_info.peer_id.clone();
        let instance_stats = instance_info.read_stats().await?;
        let peer_info = ServerfulInstance {
            join_time,
            active_time,
            instance_info: instance_info.as_ref().clone(),
            instance_stats,
        };
        loop {
            // Get scaling state.
            let mut scaling_state = self.retrieve_scaling_state().await?;
            if !scaling_state.initialized {
                // Instance will soon shutdown anyway.
                return Ok(());
            }
            self.cleanup_instances(&mut scaling_state).await;
            let old_id = scaling_state.state_id.clone();
            scaling_state.state_id = uuid::Uuid::new_v4().to_string();

            let peers = if let Some(service_name) = &instance_info.service_name {
                if !scaling_state.subsys_state.peers.contains_key(service_name) {
                    scaling_state
                        .subsys_state
                        .peers
                        .insert(service_name.clone(), HashMap::new());
                }
                scaling_state
                    .subsys_state
                    .peers
                    .get_mut(service_name)
                    .unwrap()
            } else {
                &mut scaling_state.handler_state.as_mut().unwrap().peers
            };
            if terminating {
                peers.remove(&peer_id);
            } else {
                peers.insert(peer_id.clone(), peer_info.clone());
            }
            let updated = self
                .write_scaling_state(&mut scaling_state, &old_id)
                .await?;
            if updated {
                break;
            }
        }
        Ok(())
    }
}
