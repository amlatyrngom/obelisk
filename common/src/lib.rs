pub mod adaptation;
pub mod leasing;
pub mod time_service;

use adaptation::ServerfulScalingState;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// Signature of a function.
#[async_trait::async_trait]
pub trait FunctionInstance: Send + Sync {
    /// Invoke.
    /// Input and output must be valid json.
    async fn invoke(&self, arg: Value) -> Value;
}

/// Signature of a service.
#[async_trait::async_trait]
pub trait ServiceInstance: Send + Sync {
    /// Call a service.
    /// Use `meta` to avoid encoding small metadata into `arg`.
    /// The number indicates an http code.
    async fn call(&self, meta: String, arg: Vec<u8>) -> (String, Vec<u8>);

    /// Use this to easily pass custom information to the front-end.
    async fn custom_info(&self, scaling_state: &ServerfulScalingState) -> Value;

    /// When called, the service will shutdown within the grace perior.
    async fn terminate(&self);
}

/// An actor not only has access to shared storage, but can also send direct messages.
/// Try having less than 10000 concurrently active actors. Beyond this, you may start seeing unrecoverable errors.
/// The actor should also implement a new(name: &str, Arc<PersistentLog>)->Self method.
#[async_trait::async_trait]
pub trait ActorInstance: Send + Sync {
    /// Input and output must be valid json.
    /// The msg itself has to be less than 8KB. The payload can be up to 64MB.
    /// Use `msg` to encode metadata or small items and avoid excessive (de)serialization.
    /// If payload is empty, delivery will be faster.
    async fn message(&self, msg: String, payload: Vec<u8>) -> (String, Vec<u8>);

    /// Trigger checkpoint to allow faster restart.
    /// Can be called by the runtime at any time to allow faster restarts.
    /// Roughly called every 100 messages, or every ~1 seconds when there are no messages.
    /// When terminating is set, it means the instance is about to terminate.
    async fn checkpoint(&self, terminating: bool);
}

/// Raw function resp.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FunctionResp {
    pub resp: Value,
    pub duration_ms: i64,
    pub mem_size_mb: i32,
}

/// Return the port of this node.
pub fn get_port() -> Option<u16> {
    let mode = std::env::var("EXECUTION_MODE").unwrap();
    if mode == "backend" {
        Some(37000)
    } else if mode == "lambda" {
        None
    } else if let Ok(worker_idx) = std::env::var("WORKER_INDEX") {
        let worker_idx: u16 = worker_idx.parse().unwrap();
        Some(37001 + worker_idx)
    } else {
        Some(37000)
    }
}

/// Return the url of this node.
pub async fn get_function_url() -> Result<String, String> {
    if let Some(port) = get_port() {
        let resp = reqwest::get("https://httpbin.org/ip")
            .await
            .map_err(|x| format!("{x:?}"))?
            .json::<HashMap<String, String>>()
            .await
            .map_err(|x| format!("{x:?}"))?;
        let host = resp.get("origin").unwrap().clone();
        Ok(format!("http://{host}:{port}/invoke"))
    } else {
        Err("not in fargate/ecs".into())
    }
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

/// Call service and get response back.
pub async fn call_service(
    client: &reqwest::Client,
    url: &str,
    meta: String,
    arg: Vec<u8>,
) -> Result<(String, Vec<u8>), String> {
    let url = format!("{url}/{meta}");
    let resp = client.post(url).body(arg).send().await;
    let resp = resp.map_err(|e| format!("{e:?}"))?;
    let meta = {
        let meta = resp.headers().get("obelisk-meta").unwrap();
        let meta = meta.to_str().unwrap();
        meta.into()
    };
    let body = {
        let body = resp.bytes().await.map_err(|e| format!("{e:?}"))?;
        body.to_vec()
    };
    Ok((meta, body))
}

/// Return directory for shared storage.
/// Actors and services are responsible for coordination internal directory structure.
pub fn shared_storage_prefix() -> String {
    if let Ok(mode) = std::env::var("EXECUTION_MODE") {
        if mode == "messaging_lambda" || mode == "messaging_ecs" || mode == "generic_ecs" {
            return messaging_mnt_path();
        }
    }
    // For local tests.
    "obelisk_shared_data".into()
}

pub fn messaging_mnt_path() -> String {
    "/mnt/obelisk_shared_data".into()
}

/// Name of the cluster.
pub fn cluster_name() -> String {
    "obelisk".into()
}

/// Name of s3 bucket. TODO: set to `obelisk` once existing bucket is deleted.
pub fn bucket_name() -> String {
    "obelisk1".into()
}

pub fn tmp_s3_dir() -> String {
    "/system/temporary".into()
}

pub fn has_external_access() -> bool {
    if let Ok(mode) = std::env::var("EXECUTION_MODE") {
        mode != "messaging_lambda"
    } else {
        true
    }
}

/// Name of file system.
pub fn filesystem_name(namespace: &str) -> String {
    format!("obk__{namespace}")
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

/// Full receiver template name.
pub fn full_receiving_template_name(namespace: &str) -> String {
    format!("obk__rcv__{namespace}")
}

/// Full messaging name
pub fn full_receiving_name(namespace: &str, name: &str) -> String {
    format!("obk__rcv__{namespace}__{name}")
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
