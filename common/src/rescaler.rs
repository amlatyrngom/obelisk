use crate::deployment::{container, networking};
use crate::metrics::MetricsManager;
use crate::scaling_state::{
    HandlerScalingState, ScalingStateManager, SubsystemScalingState, RESCALING_INTERVAL_SECS,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Result of rescaling.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RescalingResult {
    pub services_scales: HashMap<String, u64>,
    pub services_scaling_info: Option<String>,
    pub handler_scales: Option<HashMap<i32, u64>>,
    pub handler_scaling_info: Option<String>,
}

#[async_trait::async_trait]
pub trait Rescaler: Send + Sync {
    /// Return the new scale and the new scaling info.
    async fn rescale(
        &self,
        subsystem_scaling_state: &SubsystemScalingState,
        handler_scaling_state: Option<&HandlerScalingState>,
        since_last_rescale: std::time::Duration,
        metrics: Vec<Vec<u8>>,
    ) -> RescalingResult;
}

/// Rescaler.
pub struct ScalingStateRescaler {
    subsystem: String,
    rescaler: Arc<dyn Rescaler>,
    ecs_client: aws_sdk_ecs::Client,
    sg_id: String,
    subnet_ids: Vec<String>,
    inner: Arc<Mutex<ScalingStateRescalerInner>>,
}

/// Modifyable.
struct ScalingStateRescalerInner {
    state_managers: HashMap<(String, String), Arc<ScalingStateManager>>,
    metrics_managers: HashMap<(String, String), Arc<MetricsManager>>,
}

impl ScalingStateRescaler {
    /// Create new rescaler.
    pub async fn new(subsystem: &str, rescaler: Arc<dyn Rescaler>) -> Self {
        let ntwk = networking::NetworkingDeployment::new().await;
        let shared_config = aws_config::load_from_env().await;
        let ecs_client = aws_sdk_ecs::Client::new(&shared_config);
        ScalingStateRescaler {
            subsystem: subsystem.into(),
            rescaler,
            sg_id: ntwk.sg_id,
            subnet_ids: ntwk.subnet_ids,
            ecs_client,
            inner: Arc::new(Mutex::new(ScalingStateRescalerInner {
                state_managers: HashMap::new(),
                metrics_managers: HashMap::new(),
            })),
        }
    }

    pub async fn handle_lambda_request(
        &self,
        event: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let (namespace, identifier): (String, String) = serde_json::from_value(event).unwrap();
        let resp = self.handle_request(&namespace, &identifier).await?;
        Ok(serde_json::to_value(&resp).unwrap())
    }

    /// Handler a rescale request.
    pub async fn handle_request(
        &self,
        namespace: &str,
        identifier: &str,
    ) -> Result<String, String> {
        // Get state and metrics manager.
        let (state_manager, metrics_manager) = {
            let mut inner = self.inner.lock().await;
            let key = (namespace.to_string(), identifier.to_string());
            if !inner.state_managers.contains_key(&key) {
                let state_manager =
                    ScalingStateManager::new(&self.subsystem, namespace, identifier).await;
                let metrics_manager =
                    MetricsManager::new(&self.subsystem, namespace, identifier).await;
                inner
                    .state_managers
                    .insert(key.clone(), Arc::new(state_manager));
                inner
                    .metrics_managers
                    .insert(key.clone(), Arc::new(metrics_manager));
            }
            let state_manager = inner.state_managers.get(&key).unwrap().clone();
            let metric_manager = inner.metrics_managers.get(&key).unwrap().clone();
            (state_manager, metric_manager)
        };
        // Get scaling state.
        let mut scaling_state = state_manager.retrieve_scaling_state().await?;
        let mut old_id = scaling_state.state_id.clone();
        // If uninitiliazed, initialize.
        if !scaling_state.initialized {
            if let Some(handler_state) = &scaling_state.handler_state {
                self.rescale_handler_tasks(handler_state, true).await?;
            }
            self.rescale_service_tasks(&scaling_state.subsys_state, true)
                .await?;
            state_manager
                .write_scaling_state(&mut scaling_state, "")
                .await?;
            return Ok("Initializing".into());
        }
        // Check rescaling interval.
        let curr_time = state_manager.leaser.current_time().await;
        let old_last_rescale = scaling_state.last_rescale;
        let since_last_scaling = curr_time.signed_duration_since(old_last_rescale);
        if since_last_scaling < chrono::Duration::seconds(RESCALING_INTERVAL_SECS as i64) {
            return Ok("Recent Rescale".into());
        }
        // Check lease.
        let lease_name = format!("{}_{namespace}_{identifier}", self.subsystem);
        let has_lease = state_manager.leaser.renew(&lease_name, false).await;
        if !has_lease {
            return Ok("No Lease".into());
        }
        // Read metrics.
        let (metrics_keys, metrics) = metrics_manager.retrieve_metrics().await?;
        state_manager
            .update_scaling_state(self.rescaler.as_ref(), &mut scaling_state, metrics)
            .await;
        if let Some(handler_state) = &scaling_state.handler_state {
            self.rescale_handler_tasks(handler_state, false).await?;
        }
        self.rescale_service_tasks(&scaling_state.subsys_state, false)
            .await?;
        // self.deployer.scale_service(
        //     &self.subsystem,
        //     &req.namespace,
        //     &req.name,
        //     scaling_state.current_scale as i32,
        // ).await;
        for _ in 0..5 {
            state_manager.cleanup_instances(&mut scaling_state).await;
            let updated = state_manager
                .write_scaling_state(&mut scaling_state, &old_id)
                .await?;
            if updated {
                println!("Wrote scaling state: {scaling_state:?}");
                let _resp = metrics_manager.delete_metrics(&metrics_keys).await?;
                return Ok("All Good".into());
            } else {
                let new_scaling_state = state_manager.retrieve_scaling_state().await?;
                if new_scaling_state.last_rescale == old_last_rescale {
                    // Failed write is not due to rescale conflict, but to peers update.
                    scaling_state.subsys_state.peers = new_scaling_state.subsys_state.peers;
                    if let Some(handler_state) = &mut scaling_state.handler_state {
                        handler_state.peers = new_scaling_state.handler_state.unwrap().peers;
                    }
                    old_id = new_scaling_state.state_id.clone();
                    // scaling_state.state_id = new_scaling_state.state_id;
                    continue;
                } else {
                    // Rescale conflict: should almost never happed thanks to lease.
                    return Ok("Some other rescaling".into());
                }
            }
        }
        return Ok("Concurrent Ops".into());
    }

    /// Rescale service tasks.
    async fn rescale_handler_tasks(
        &self,
        new_state: &HandlerScalingState,
        initializing: bool,
    ) -> Result<(), String> {
        let mems = container::ContainerDeployment::all_avail_mems(
            new_state.handler_spec.default_mem,
            new_state.handler_spec.scaleup,
        );
        for mem in mems {
            let count = new_state.handler_scales.get(&mem).cloned().unwrap_or(0) as i32;
            if initializing {
                container::ContainerDeployment::create_handler_task(
                    &self.ecs_client,
                    &new_state.handler_spec,
                    &new_state.identifier,
                    &self.subnet_ids,
                    &self.sg_id,
                    mem,
                )
                .await;
            } else {
                container::ContainerDeployment::scale_handler_task(
                    &self.ecs_client,
                    &new_state.handler_spec,
                    &new_state.identifier,
                    mem,
                    count,
                )
                .await?;
            }
        }
        Ok(())
    }

    /// Rescale service tasks.
    async fn rescale_service_tasks(
        &self,
        new_state: &SubsystemScalingState,
        initializing: bool,
    ) -> Result<(), String> {
        for (service_name, count) in &new_state.service_scales {
            let count = *count as i32;
            let spec = new_state.service_specs.get(service_name).unwrap();
            let target_namespace = &new_state.namespace;
            let identifier = &new_state.identifier;
            if initializing {
                container::ContainerDeployment::create_service_task(
                    &self.ecs_client,
                    spec,
                    target_namespace,
                    identifier,
                    &self.subnet_ids,
                    &self.sg_id,
                )
                .await;
            } else {
                container::ContainerDeployment::scale_service_task(
                    &self.ecs_client,
                    spec,
                    target_namespace,
                    identifier,
                    count,
                )
                .await?;
            }
        }
        Ok(())
    }
}
