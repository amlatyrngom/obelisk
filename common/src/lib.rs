// pub mod adaptation;
pub mod deployment;
pub mod leasing;
pub mod metrics;
pub mod rescaler;
pub mod scaling_state;
pub mod storage;
pub mod time_service;
pub mod wrapper;

pub use deployment::RescalerSpec;
pub use metrics::MetricsManager;
pub use rescaler::{Rescaler, RescalingResult, ScalingStateRescaler};
pub use scaling_state::{HandlerScalingState, ScalingState, SubsystemScalingState};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
pub use storage::ServerlessStorage;
pub use wrapper::{InstanceInfo, ServerlessWrapper, WrapperMessage};

/// Input to a handler.
pub struct HandlerKit {
    /// Contains information about the present instance.
    pub instance_info: Arc<InstanceInfo>,
    /// Allows access to shared storage.
    pub serverless_storage: Option<Arc<ServerlessStorage>>,
}

/// A serverless handler.
/// The class has to have a new function with the following signature: new(instance_info: Arc<InstanceInfo>)
#[async_trait::async_trait]
pub trait ServerlessHandler: Send + Sync {
    /// Call handler with the given metadata and payload.
    async fn handle(&self, meta: String, payload: Vec<u8>) -> (String, Vec<u8>);

    /// Periodic checkpoint.
    /// If terminating is set to true, this instance of the handler will terminate after this call finishes.
    async fn checkpoint(&self, scaling_state: &ScalingState, terminating: bool);
}

/// Raw function resp.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HandlingResp {
    pub meta: String,
    pub duration: std::time::Duration,
    pub mem_size_mb: i32,
    pub cpus: i32,
    pub mem_usage: Option<f64>,
    pub cpu_usage: Option<f64>,
    pub is_lambda: bool,
}

/// Macro to debug format.
#[macro_export]
macro_rules! debug_format {
    () => {
        |e| format!("{e:?}")
    };
}

/// Return the port of this node.
pub fn get_port() -> u16 {
    37000
}

/// Return the public url of this node.
pub async fn get_public_url() -> Result<String, String> {
    let port = get_port();
    let resp = reqwest::get("https://httpbin.org/ip")
        .await
        .map_err(|x| format!("{x:?}"))?
        .json::<HashMap<String, String>>()
        .await
        .map_err(|x| format!("{x:?}"))?;
    let host = resp.get("origin").unwrap().clone();
    Ok(format!("http://{host}:{port}/invoke"))
}

/// Cleanly die.
/// First propagate a sigterm.
/// Then wait 1 minute. If process hasn't exit, kill.
pub async fn clean_die(msg: &str) -> ! {
    eprintln!("obelisk clean die: {msg}");
    loop {
        let cmd = std::process::Command::new("kill")
            .arg("-s")
            .arg("TERM")
            .arg(std::process::id().to_string())
            .output();
        if cmd.is_ok() {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            std::process::exit(1);
        } else {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    }
}

/// Return directory for shared storage.
/// Actors and services are responsible for coordination internal directory structure.
pub fn shared_storage_prefix() -> String {
    if let Ok(v) = std::env::var("OBK_EXECUTION_MODE") {
        if !v.contains("local") {
            return persistence_mnt_path();
        }
    }
    // For local tests.
    "obelisk_shared_data".into()
}

pub fn persistence_mnt_path() -> String {
    "/mnt/obelisk_shared_data".into()
}

/// Name of s3 bucket. TODO: set to `obelisk` once existing bucket is deleted.
pub fn bucket_name() -> String {
    "obelisk1".into()
}

pub fn tmp_s3_dir() -> String {
    "/system/temporary".into()
}

pub fn has_external_access() -> bool {
    if let Ok(external_access) = std::env::var("OBK_EXTERNAL_ACCESS") {
        external_access.parse().unwrap()
    } else {
        true
    }
}

/// Full name of a function with namespace.
pub fn full_function_name(namespace: &str) -> String {
    format!("obk__fn__{namespace}")
}

/// Full template name.
pub fn full_messaging_template_name(namespace: &str) -> String {
    format!("obk__msg__{namespace}")
}

/// Full messaging name
pub fn full_messaging_name(namespace: &str, name: &str) -> String {
    format!("obk__msg__{namespace}__{name}")
}

/// Prefix of services for lookups.
pub fn messaging_name_prefix(namespace: &str) -> String {
    format!("obk__msg__{namespace}")
}

/// Name of a scaler function.
pub fn full_scaler_name(subsystem: &str) -> String {
    format!("obk__scl__{subsystem}")
}

/// Name of scaling table.
pub fn scaling_table_name(subsystem: &str) -> String {
    format!("obk__scl__{subsystem}")
}

/// Name of service definition.
pub fn full_service_definition_name(subsystem: &str) -> String {
    format!("obk__def__{subsystem}")
}

/// Name of scaling queue.
pub fn full_scaling_queue_name(subsystem: &str, namespace: &str, name: &str) -> String {
    format!("obk__scl__{subsystem}__{namespace}__{name}")
}

/// Prefix of scaling queues for lookups.
pub fn scaling_queue_prefix(subsystem: &str) -> String {
    format!("obk__scl__{subsystem}")
}

/// Name of service.
pub fn full_service_name(subsystem: &str, namespace: &str, name: &str) -> String {
    format!("obk__svc__{subsystem}__{namespace}__{name}")
}

/// Prefix of services for lookups.
pub fn service_name_prefix(subsystem: &str) -> String {
    format!("obk__svc__{subsystem}")
}
