use crate::time_service::TimeService;
use crate::ServiceInstance;
use std::sync::Arc;
use tokio::signal::unix;
use tokio::sync::RwLock;

use super::{
    cleanup_instances, AdapterScaling, ScalerReq, ServerfulInstance, ServerfulScalingState,
    RESCALING_INTERVAL, SERVERFUL_REFRESH_TIME,
};
use crate::full_scaler_name;

#[derive(Clone, Debug)]
pub struct ServiceInfo {
    pub id: String,
    pub az: String,
    pub url: String,
    pub private_url: String,
    pub subsystem: String,
    pub namespace: String,
    pub name: String,
}

#[derive(Clone)]
pub struct AdapterBackend {
    join_time: chrono::DateTime<chrono::Utc>,
    time_service: TimeService,
    adapter_scaling: Arc<AdapterScaling>,
    inner: Arc<RwLock<AdapterBackerInner>>,
    svc_info: Arc<ServiceInfo>,
    lambda_client: aws_sdk_lambda::Client,
    svc: Arc<dyn ServiceInstance>,
}

pub struct AdapterBackerInner {
    scaling_state: Option<ServerfulScalingState>,
}

impl AdapterBackend {
    /// Create new adapter backend.
    pub async fn new(svc_info: Arc<ServiceInfo>, svc: Arc<dyn ServiceInstance>) -> Self {
        let time_service = TimeService::new().await;
        let join_time = time_service.current_time().await;
        let shared_config = aws_config::load_from_env().await;
        let lambda_client = aws_sdk_lambda::Client::new(&shared_config);
        let inner = Arc::new(RwLock::new(AdapterBackerInner {
            scaling_state: None,
        }));
        let adapter_scaling = Arc::new(AdapterScaling::new(None, &svc_info.subsystem).await);

        let backend = AdapterBackend {
            join_time,
            time_service,
            svc_info,
            inner,
            adapter_scaling,
            lambda_client,
            svc,
        };

        {
            let backend = backend.clone();
            tokio::spawn(async move {
                backend.bookkeeping().await;
            });
        }

        backend
    }

    /// Call service.
    pub async fn call_svc(&self, meta: String, arg: Vec<u8>) -> (String, Vec<u8>) {
        self.svc.call(meta, arg).await
    }

    /// Deregister self.
    async fn deregister(&self) {
        loop {
            let req = ScalerReq {
                namespace: self.svc_info.namespace.clone(),
                name: self.svc_info.name.clone(),
                timestamp: self.time_service.current_time().await,
            };
            let mut scaling_state = self.adapter_scaling.get_scaling_state(&req).await;
            cleanup_instances(&mut scaling_state);
            let old_id = scaling_state.state_id.clone();
            scaling_state.state_id = uuid::Uuid::new_v4().to_string();
            scaling_state.peers.remove(&self.svc_info.id);
            println!("Writing scaling state for deregister!");
            let updated = self
                .adapter_scaling
                .write_scaling_state(&mut scaling_state, &old_id)
                .await;
            if updated {
                break;
            }
        }
    }

    /// Invoke scaler.
    async fn invoke_scaler(&self) {
        let req = ScalerReq {
            namespace: self.svc_info.namespace.clone(),
            name: self.svc_info.name.clone(),
            timestamp: self.time_service.current_time().await,
        };
        let req = serde_json::to_vec(&req).unwrap();
        let req = aws_smithy_types::Blob::new(req);
        println!("Invoking scaler!");
        let resp = self
            .lambda_client
            .invoke()
            .function_name(full_scaler_name(&self.svc_info.subsystem))
            .payload(req)
            .send()
            .await;
        if let Ok(resp) = &resp {
            let resp: Vec<u8> = resp.payload().unwrap().clone().into_inner();
            let serverful_service: ServerfulScalingState = serde_json::from_slice(&resp).unwrap();
            {
                let mut inner = self.inner.write().await;
                inner.scaling_state = Some(serverful_service);
            }
        } else {
            println!("InvokeScaler Error: {resp:?}");
        }
    }

    /// Refresh self.
    pub async fn refresh(&self) {
        loop {
            let req = ScalerReq {
                namespace: self.svc_info.namespace.clone(),
                name: self.svc_info.name.clone(),
                timestamp: self.time_service.current_time().await,
            };
            let mut scaling_state = self.adapter_scaling.get_scaling_state(&req).await;
            cleanup_instances(&mut scaling_state);
            let old_id = scaling_state.state_id.clone();
            scaling_state.state_id = uuid::Uuid::new_v4().to_string();
            scaling_state.peers.insert(
                self.svc_info.id.clone(),
                ServerfulInstance {
                    id: self.svc_info.id.clone(),
                    az: self.svc_info.az.clone(),
                    url: self.svc_info.url.clone(),
                    private_url: self.svc_info.private_url.clone(),
                    active_time: req.timestamp,
                    join_time: self.join_time,
                    custom_info: self.svc.custom_info(&scaling_state).await,
                },
            );
            {
                let mut inner = self.inner.write().await;
                inner.scaling_state = Some(scaling_state.clone());
            };
            println!("Writing scaling state for refresh!");
            let updated = self
                .adapter_scaling
                .write_scaling_state(&mut scaling_state, &old_id)
                .await;
            if updated {
                break;
            }
        }
    }

    /// Book keeping thread.
    async fn bookkeeping(&self) {
        let mut refresh_interval =
            tokio::time::interval(std::time::Duration::from_secs(SERVERFUL_REFRESH_TIME));
        let mut rescale_interval =
            tokio::time::interval(std::time::Duration::from_secs(RESCALING_INTERVAL));
        let terminate_signal = unix::SignalKind::terminate();
        let mut sigterm = unix::signal(terminate_signal).unwrap();
        let int_signal = unix::SignalKind::interrupt();
        let mut sigint = unix::signal(int_signal).unwrap();
        let terminated = Arc::new(RwLock::new(false));
        loop {
            tokio::select! {
                _ = refresh_interval.tick() => {
                    {
                        // Check if terminated.
                        let terminated = terminated.read().await;
                        if *terminated {
                            self.deregister().await;
                            return
                        }
                    }
                    // Spawn to prevent blocked signal processing.
                    let this = self.clone();
                    tokio::spawn (async move {
                        this.refresh().await;
                    });
                },
                _ = rescale_interval.tick() => {
                    // Spawn to prevent blocked signal processing.
                    let this = self.clone();
                    tokio::spawn (async move {
                        this.invoke_scaler().await;
                    });
                },
                _ = sigint.recv() => {
                    let svc = self.svc.clone();
                    let terminated = terminated.clone();
                    tokio::spawn (async move {
                        svc.terminate().await;
                        let mut terminated = terminated.write().await;
                        *terminated = true;
                    });
                }
                _ = sigterm.recv() => {
                    let svc = self.svc.clone();
                    let terminated = terminated.clone();
                    tokio::spawn (async move {
                        svc.terminate().await;
                        let mut terminated = terminated.write().await;
                        *terminated = true;
                    });
                }
            }
        }
    }
}

impl ServiceInfo {
    /// Retrieve information about a service.
    pub async fn new() -> Result<ServiceInfo, String> {
        let shared_config = aws_config::load_from_env().await;
        let ecs_client = aws_sdk_ecs::Client::new(&shared_config);
        let service_url = crate::get_function_url().await?; // TODO: Get from metadata.
        let uri = std::env::var("ECS_CONTAINER_METADATA_URI_V4").map_err(|x| format!("{x:?}"))?;
        let uri = format!("{uri}/task");
        let resp = reqwest::get(uri).await.map_err(|x| format!("{x:?}"))?;
        let task: serde_json::Value = resp.json().await.map_err(|x| format!("{x:?}"))?;
        // Get AZ.
        let az = task.get("AvailabilityZone").unwrap().as_str().unwrap();
        println!("Got AZ {az:?}");
        // Get task arn.
        let task = task.get("TaskARN").unwrap().as_str().unwrap();
        println!("Got Task: {task}");
        // Describe task to get namespace and name.
        let task = ecs_client
            .describe_tasks()
            .tasks(task)
            .cluster(crate::cluster_name())
            .send()
            .await
            .map_err(|x| format!("{x:?}"))?;
        let task = task.tasks().unwrap().first().unwrap();
        // Get private url.
        let container = task.containers().unwrap().first().unwrap();
        let ni = container.network_interfaces().unwrap().first().unwrap();
        let private_address = ni.private_ipv4_address().unwrap();
        let port = crate::get_port().unwrap();
        let private_url = format!("http://{private_address}:{port}/invoke");
        println!("Private Url: {private_url}");
        // Get service namespace and name.
        let group = task.group().unwrap();
        println!("Got group: {group}");
        let service: String = group[8..].into();
        println!("Get service: {service}");
        let prefix = "obk__svc__";
        let (_, subsys_ns_name) = service.split_at(prefix.len());
        let (subsystem, ns_name) = subsys_ns_name.split_once("__").unwrap();
        let (namespace, name) = ns_name.split_once("__").unwrap();
        Ok(ServiceInfo {
            id: uuid::Uuid::new_v4().to_string(),
            url: service_url,
            az: az.into(),
            subsystem: subsystem.into(),
            namespace: namespace.into(),
            name: name.into(),
            private_url,
        })
    }
}
