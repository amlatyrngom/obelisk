use crate::debug_format;
use crate::scaling_state::ScalingStateManager;
use crate::storage::ServerlessStorage;
use crate::{deployment, ServerlessHandler};
use crate::{time_service::TimeService, HandlingResp};
use base64::{engine::general_purpose, Engine as _};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};
use tokio::signal::unix;

/// Amount of time between checkpoints.
const CHECKPOINTING_INTERVAL_SECS: u64 = 10;

/// Serverless wrapper.
#[derive(Clone)]
pub struct ServerlessWrapper {
    s3_client: aws_sdk_s3::Client,
    join_time: chrono::DateTime<chrono::Utc>,
    pub instance_info: Arc<InstanceInfo>,
    state_manager: Arc<ScalingStateManager>,
    serverless_storage: Option<Arc<ServerlessStorage>>,
    handler: Arc<dyn ServerlessHandler>,
    instance_stats: Arc<RwLock<Option<InstanceStats>>>,
    initialized: bool,
}

/// Information about a running instance.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InstanceInfo {
    /// ID of this peer.
    pub peer_id: String,
    /// Availability Zone. Not present for a Lambda.
    pub az: Option<String>,
    /// Amount of mem.
    pub mem: i32,
    /// Amount of CPU.
    pub cpus: i32,
    /// Public URL: Not present for a Lambda or a private instance.
    pub public_url: Option<String>,
    /// Private URL. Not present for a Lambda.
    pub private_url: Option<String>,
    /// If this instance is for a service, name of that service.
    pub service_name: Option<String>,
    /// If this instance is for a handler, name of that handler.
    pub handler_name: Option<String>,
    /// Subsystem responsible for scaling this instance.
    pub subsystem: String,
    /// Namespace of the user of this instance.
    pub namespace: String,
    /// Identifier of user of this instance.
    pub identifier: String,
    /// Whether instance with the identifier should be the only deployed one.
    pub unique: bool,
    /// Whether this instance has access to persistent shared disk.
    pub persistent: bool,
}

/// Statistic about this instance.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InstanceStats {
    pub mem_usage: f64,
    pub cpu_usage: f64,
    // pub disk_reads: f64,
}

/// Message to send to handler.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WrapperMessage {
    HandlerMessage { meta: String },
    IndirectMessage,
    ForceShutdownMessage,
}

impl ServerlessWrapper {
    /// Prefixes to use when sending or receiving messages.
    pub fn messaging_prefix(namespace: &str, identifier: &str) -> (String, String) {
        let send_prefix = format!("{}/send/{namespace}/{identifier}", crate::tmp_s3_dir());
        let recv_prefix = format!("{}/recv/{namespace}/{identifier}", crate::tmp_s3_dir());
        (send_prefix, recv_prefix)
    }

    /// Create new adapter backend.
    pub async fn new(
        instance_info: Arc<InstanceInfo>,
        serverless_storage: Option<Arc<ServerlessStorage>>,
        handler: Arc<dyn ServerlessHandler>,
    ) -> Self {
        let shared_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let s3_client = aws_sdk_s3::Client::new(&shared_config);
        let time_service = TimeService::new().await;
        let join_time = time_service.current_time().await;
        let state_manager = ScalingStateManager::new(
            &instance_info.subsystem,
            &instance_info.namespace,
            &instance_info.identifier,
        )
        .await;
        // Make wrapper.
        let instance_stats = instance_info.read_stats().await.ok();
        let initialized = if let Some(ss) = &serverless_storage {
            ss.initialized
        } else {
            true
        };
        let wrapper = ServerlessWrapper {
            s3_client,
            join_time,
            instance_info,
            state_manager: Arc::new(state_manager),
            handler,
            serverless_storage,
            instance_stats: Arc::new(RwLock::new(instance_stats)),
            initialized,
        };
        if !wrapper.initialized {
            if wrapper.instance_info.private_url.is_some() {
                panic!("ServerlessWrapper::new. Serverless storage not initialized. Likely could not get lock!");
            } else {
                println!("Running Uninitialized Wrapper for Lambda!");
                return wrapper;
            }
        }
        // Start state manager threads.
        wrapper.state_manager.start_refresh_thread().await;
        wrapper.state_manager.start_rescaling_thread().await;
        // Start background threads.
        {
            let wrapper = wrapper.clone();
            tokio::spawn(async move {
                let t1 = wrapper.checkpointing_thread();
                let t2 = wrapper.message_queue_thread();
                let _ = tokio::join!(t1, t2);
            });
        }
        // If serverful, update peer list.
        if wrapper.instance_info.private_url.is_some() {
            let _ = wrapper
                .state_manager
                .update_peer(wrapper.instance_info.clone(), wrapper.join_time, false)
                .await;
        }
        wrapper
    }

    /// Wrap given response.
    fn wrap_response(&self, resp_meta: String, start_time: std::time::Instant) -> HandlingResp {
        let end_time = std::time::Instant::now();
        let mut duration = end_time.duration_since(start_time);
        if duration.as_millis() < 1 {
            duration = std::time::Duration::from_millis(1);
        }
        let (cpu_usage, mem_usage) = if self.instance_info.private_url.is_none() {
            (None, None)
        } else if chrono::Utc::now()
            .signed_duration_since(self.join_time)
            .num_minutes()
            < 1
        {
            (None, None)
        } else {
            let instance_stats = self.instance_stats.read().unwrap();
            (
                Some(instance_stats.as_ref().unwrap().cpu_usage),
                Some(instance_stats.as_ref().unwrap().mem_usage),
            )
        };
        HandlingResp {
            meta: resp_meta,
            duration,
            mem_size_mb: self.instance_info.mem as i32,
            cpus: self.instance_info.cpus as i32,
            is_lambda: self.instance_info.private_url.is_none(),
            cpu_usage,
            mem_usage,
        }
    }

    /// Handle ecs message.
    pub async fn handle_ecs_message(&self, meta: String, payload: Vec<u8>) -> (String, Vec<u8>) {
        let start_time = std::time::Instant::now();
        println!("Received Meta: {meta:?}");
        let meta: WrapperMessage = serde_json::from_str(&meta).unwrap();
        let (resp_meta, resp_payload) = match meta {
            WrapperMessage::HandlerMessage { meta } => {
                let (resp_meta, resp_payload) =
                    self.handle_direct_message(meta, payload, start_time).await;
                let resp_meta = serde_json::to_string(&resp_meta).unwrap();
                (resp_meta, resp_payload)
            }
            WrapperMessage::IndirectMessage => {
                let _ = self.handle_indirect_messages().await;
                (String::new(), vec![])
            },
            WrapperMessage::ForceShutdownMessage => {
                self.checkpoint_handler(true).await;
                (String::new(), vec![])
            }
        };
        (resp_meta, resp_payload)
    }

    /// Handle lambda message.
    pub async fn handle_lambda_message(&self, request: serde_json::Value) -> serde_json::Value {
        if !self.initialized {
            println!("Uninitialized Wrapper. Exiting!");
            std::process::exit(1);
        }
        let start_time = std::time::Instant::now();
        let (meta, payload): (WrapperMessage, String) = serde_json::from_value(request).unwrap();
        match meta {
            WrapperMessage::HandlerMessage { meta } => {
                let payload = general_purpose::STANDARD_NO_PAD.decode(payload).unwrap();
                let (resp_meta, resp_payload) =
                    self.handle_direct_message(meta, payload, start_time).await;
                let resp_payload = general_purpose::STANDARD_NO_PAD.encode(resp_payload);
                let response = (resp_meta, resp_payload);
                serde_json::to_value(response).unwrap()
            }
            WrapperMessage::IndirectMessage => {
                let _ = self.handle_indirect_messages().await;
                serde_json::Value::Null
            }
            WrapperMessage::ForceShutdownMessage => {
                self.checkpoint_handler(true).await;
                serde_json::Value::Null
            }
        }
    }

    /// Handle direct message message.
    async fn handle_direct_message(
        &self,
        meta: String,
        payload: Vec<u8>,
        start_time: std::time::Instant,
    ) -> (HandlingResp, Vec<u8>) {
        let (resp_meta, resp_payload) = self.handler.handle(meta, payload).await;
        let resp_meta = self.wrap_response(resp_meta, start_time);
        (resp_meta, resp_payload)
    }

    /// Perform checkpointing.
    async fn checkpoint_handler(&self, terminating: bool) {
        // Check unique.
        let lost_incarnation = if let Some(ss) = &self.serverless_storage {
            if ss.exclusive_pool.is_some() {
                let x = ss.check_incarnation().await;
                println!("Incarnation Check: {x:?}");
                x.is_err()
            } else {
                false
            }
        } else {
            false
        };
        let terminating = terminating || lost_incarnation;
        // Update stats.
        if let Ok(s) = self.instance_info.read_stats().await {
            let mut instance_stats = self.instance_stats.write().unwrap();
            *instance_stats = Some(s);
        }
        // TODO: Think about whether put this before or after deregistering.
        let scaling_state = self.state_manager.current_scaling_state().await;
        self.handler.checkpoint(&scaling_state, terminating).await;
        if self.instance_info.private_url.is_some() {
            // If serverful, update peer list.
            let _ = self
                .state_manager
                .update_peer(self.instance_info.clone(), self.join_time, terminating)
                .await;
        }
        if terminating {
            // Release exclusive file if held.
            if let Some(ss) = &self.serverless_storage {
                tokio::task::block_in_place(move || {
                    let _ = ss.release_exclusive_file(1);
                });
            }
            // If unique, wait to avoid spurrious ecs restarts.
            if self.instance_info.unique && self.instance_info.private_url.is_some() {
                tokio::time::sleep(std::time::Duration::from_secs(100)).await;
            }
            std::process::exit(0);
        } else {
            // Seems necessary to keep the lock active?
            if let Some(ss) = &self.serverless_storage {
                tokio::task::block_in_place(move || {
                    let _ = ss.dummy_write_to_exclusive();
                });
            }
        }
    }

    /// Periodically checkpoint.
    async fn checkpointing_thread(&self) -> Result<(), String> {
        let duration = std::time::Duration::from_secs(CHECKPOINTING_INTERVAL_SECS);
        let mut interval = tokio::time::interval(duration);
        let terminate_signal = unix::SignalKind::terminate();
        let interrupt_signal = unix::SignalKind::interrupt();
        let mut sigterm = unix::signal(terminate_signal).unwrap();
        let mut sigint = unix::signal(interrupt_signal).unwrap();
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    println!("Ticking!!");
                    self.checkpoint_handler(false).await;
                },
                _ = sigint.recv() => {
                    self.checkpoint_handler(true).await;
                }
                _ = sigterm.recv() => {
                    self.checkpoint_handler(true).await;
                }
            }
        }
    }

    async fn message_queue_thread(&self) {
        let mq_duration_minutes = 1;
        println!("Running message queue thread for {mq_duration_minutes}min!");
        let starting_time = std::time::Instant::now();
        loop {
            let current_time = std::time::Instant::now();
            let since = current_time.duration_since(starting_time);
            if since > std::time::Duration::from_secs(60 * mq_duration_minutes) {
                println!("Exiting message queue thread after {mq_duration_minutes}min!");
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            // Handle all available messages.
            let mut handled = self.handle_indirect_messages().await.unwrap_or(false);
            while handled {
                // While batch not empty.
                handled = self.handle_indirect_messages().await.unwrap_or(false);
            }
        }
    }

    /// Handle a specific indirect message.
    async fn handle_indirect_message(&self, key: String) -> Result<(), String> {
        println!("Handling indirect message: {key}.");
        let start_time = std::time::Instant::now();
        let resp = self
            .s3_client
            .get_object()
            .bucket(&crate::bucket_name())
            .key(&key)
            .send()
            .await
            .map_err(|e| format!("{e:?}"))?;
        // Delete
        {
            let s3_client = self.s3_client.clone();
            tokio::spawn(async move {
                let _ = s3_client
                    .delete_object()
                    .bucket(&crate::bucket_name())
                    .key(&key)
                    .send()
                    .await;
            });
        }
        // Deserialize.
        let body: Vec<u8> = resp.body.collect().await.unwrap().to_vec();
        let (msg_id, meta, payload): (String, String, Vec<u8>) =
            bincode::deserialize(&body).unwrap();
        // Handle.
        println!("Indirect Handling.");
        let (resp_meta, resp_payload) = self.handler.handle(meta, payload).await;
        println!("Indirect Handled.");
        let resp_meta = self.wrap_response(resp_meta, start_time);
        // Serialize response.
        let body: (String, HandlingResp, Vec<u8>) = (msg_id.clone(), resp_meta, resp_payload);
        let body = bincode::serialize(&body).unwrap();
        let body = aws_sdk_s3::primitives::ByteStream::from(body);
        // Write response.
        let (_, s3_recv_prefix) = Self::messaging_prefix(
            &self.instance_info.namespace,
            &self.instance_info.identifier,
        );
        let s3_recv_key = format!("{s3_recv_prefix}/{msg_id}");
        let _resp = self
            .s3_client
            .put_object()
            .bucket(&crate::bucket_name())
            .key(&s3_recv_key)
            .body(body)
            .send()
            .await
            .map_err(|e| format!("{e:?}"))?;
        Ok(())
    }

    /// Handle indirect messages from S3.
    pub async fn handle_indirect_messages(&self) -> Result<bool, String> {
        let (s3_prefix, _) = Self::messaging_prefix(
            &self.instance_info.namespace,
            &self.instance_info.identifier,
        );
        let resp = self
            .s3_client
            .list_objects_v2()
            .bucket(&crate::bucket_name())
            .max_keys(20)
            .prefix(s3_prefix)
            .send()
            .await
            .map_err(|e| format!("{e:?}"))?;
        let objs = resp.contents();
        let keys = objs
            .iter()
            .map(|obj| obj.key().unwrap().to_string())
            .collect::<Vec<_>>();
        let mut handled_messages = !keys.is_empty();
        let mut handling_threads = Vec::new();
        for key in keys {
            let this = self.clone();
            handling_threads.push(tokio::spawn(async move {
                this.handle_indirect_message(key).await
            }));
        }
        for t in handling_threads {
            let resp = t.await.unwrap();
            handled_messages &= resp.is_ok();
        }
        Ok(handled_messages)
    }
}

impl InstanceInfo {
    /// Retrieve information about a instance.
    pub async fn new() -> Result<InstanceInfo, String> {
        let mode = std::env::var("OBK_EXECUTION_MODE").unwrap();
        if mode.contains("lambda") {
            Self::function_new().await
        } else {
            Self::container_new().await
        }
    }

    /// Read statistics.
    pub async fn read_stats(&self) -> Result<InstanceStats, String> {
        let uri = std::env::var("ECS_CONTAINER_METADATA_URI_V4").map_err(|x| format!("{x:?}"))?;
        let uri = format!("{uri}/stats");
        let resp = reqwest::get(uri).await.map_err(|x| format!("{x:?}"))?;
        let task: serde_json::Value = resp.json().await.map_err(|x| format!("{x:?}"))?;
        let mem_stats = task.get("memory_stats").unwrap();
        let mem_usage = mem_stats.get("usage").unwrap().as_f64().unwrap() / 1e6;
        let mem_usage = mem_usage / (self.mem as f64);
        let cpu_stats = task.get("cpu_stats").unwrap();
        println!("CPU Stats: {cpu_stats:?}");
        let precpu_stats = task.get("precpu_stats").unwrap();
        let extract_cpu_info = |cpu_stats: &serde_json::Value| {
            let cpu_usage = cpu_stats.get("cpu_usage").unwrap();
            let total_usage = cpu_usage.get("total_usage");
            let total_usage = if let Some(total_usage) = total_usage {
                total_usage.as_f64().unwrap()
            } else {
                0.0
            };

            let sys_cpu = cpu_stats.get("system_cpu_usage");
            let sys_cpu = if let Some(sys_cpu) = sys_cpu {
                sys_cpu.as_f64().unwrap()
            } else {
                println!("InstanceInfo::read_stats. Missing sys cpu: {cpu_stats:?}");
                0.0
            };
            let online_cpus = cpu_stats.get("online_cpus");
            let online_cpus = if let Some(online_cpus) = online_cpus {
                online_cpus.as_f64().unwrap()
            } else {
                1.0
            };
            (total_usage, sys_cpu, online_cpus)
        };
        let (total_usage, sys_cpu, online_cpus) = extract_cpu_info(cpu_stats);
        let (pretotal_usage, presys_cpu, _) = extract_cpu_info(precpu_stats);
        let cpu_delta = total_usage - pretotal_usage;
        let sys_delta = (sys_cpu - presys_cpu) + 1e-5; // Prevent div by 0.
        let cpu_usage = cpu_delta / sys_delta;
        let cpu_usage = cpu_usage * online_cpus / (self.cpus as f64 / 1024.0);

        // let disk_stats = task.get("blkio_stats").unwrap();
        // let disk_ios = disk_stats.get("io_serviced_recursive").unwrap().as_array().unwrap();
        // let mut disk_reads = 0.0;
        // for disk_io in disk_ios {
        //     let op = disk_io.get("op").unwrap().as_str().unwrap();
        //     if op == "Read" {
        //         disk_reads += disk_io.get("value").unwrap().as_f64().unwrap();
        //     }
        // }
        println!("Mem usage: {mem_usage}");
        println!("CPU usage: {cpu_usage}. (Total, Sys) = ({total_usage}, {sys_cpu})");
        Ok(InstanceStats {
            mem_usage,
            cpu_usage,
        })
    }

    /// Get instance info for lambda functions.
    async fn function_new() -> Result<InstanceInfo, String> {
        // Read handler metadata
        let identifier = std::env::var("OBK_IDENTIFIER").unwrap();
        println!("Starting up function: {identifier}");
        // Always a handler, so this environment variable must be set.
        let spec = std::env::var("OBK_HANDLER_SPEC").unwrap();
        let spec: deployment::HandlerSpec = serde_json::from_str(&spec).unwrap();
        let mem = spec.default_mem;
        let cpus = mem / 2;
        let peer_id = uuid::Uuid::new_v4().to_string();
        Ok(InstanceInfo {
            peer_id,
            public_url: None,
            private_url: None,
            mem,
            cpus,
            az: None,
            subsystem: spec.subsystem,
            namespace: spec.namespace,
            persistent: spec.persistent,
            unique: spec.unique,
            service_name: None,
            handler_name: Some(spec.name),
            identifier,
        })
    }

    /// Get instance info for containers.
    async fn container_new() -> Result<InstanceInfo, String> {
        let shared_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let ecs_client = aws_sdk_ecs::Client::new(&shared_config);
        // Read mem and cpus.
        let mem: i32 = std::env::var("OBK_MEMORY").unwrap().parse().unwrap();
        let cpus: i32 = std::env::var("OBK_CPUS").unwrap().parse().unwrap();
        let (subsystem, namespace, service_name, handler_name, unique, persistent) =
            if let Ok(spec) = std::env::var("OBK_HANDLER_SPEC") {
                let spec: deployment::HandlerSpec = serde_json::from_str(&spec).unwrap();
                (
                    spec.subsystem,
                    Some(spec.namespace),
                    None,
                    Some(spec.name),
                    spec.unique,
                    spec.persistent,
                )
            } else {
                let spec = std::env::var("OBK_SERVICE_SPEC").unwrap();
                let spec: deployment::ServiceSpec = serde_json::from_str(&spec).unwrap();
                let subsystem = spec.namespace;
                (subsystem, None, Some(spec.name), None, spec.unique, true)
            };
        // Getting Task Metadata.
        println!("Getting Task Metadata!");
        let uri = std::env::var("ECS_CONTAINER_METADATA_URI_V4").map_err(|x| format!("{x:?}"))?;
        let uri = format!("{uri}/task");
        let resp = reqwest::get(uri).await.map_err(|x| format!("{x:?}"))?;
        let task: serde_json::Value = resp.json().await.map_err(|x| format!("{x:?}"))?;
        // Get task arn.
        let arn = task.get("TaskARN").unwrap().as_str().unwrap();
        println!("Got Task ARN: {arn}");
        // Get Cluster.
        let cluster = task.get("Cluster").unwrap().as_str().unwrap();
        println!("Got Cluter: {cluster:?}");
        // Get AZ.
        let az = task.get("AvailabilityZone").unwrap().as_str().unwrap();
        println!("Got AZ {az:?}");
        // Get network
        let containers = task.get("Containers").unwrap().as_array().unwrap();
        let container = containers.get(0).unwrap();
        let networks = container.get("Networks").unwrap().as_array().unwrap();
        let network = networks.get(0).unwrap();
        let addresses = network.get("IPv4Addresses").unwrap().as_array().unwrap();
        let private_address = addresses.get(0).unwrap();
        let private_address = private_address.as_str().unwrap();
        let port = crate::get_port();
        let private_url = Some(format!("http://{private_address}:{port}/invoke"));
        // Describe task to get tags.
        let task = ecs_client
            .describe_tasks()
            .tasks(arn)
            .cluster(cluster)
            .include(aws_sdk_ecs::types::TaskField::Tags)
            .send()
            .await
            .map_err(debug_format!())?;
        let task = task.tasks().first().unwrap();
        let tags = task.tags();
        println!("Found Tags: {tags:?}");
        let tag = tags
            .iter()
            .find(|tag| tag.key().unwrap() == "OBK_IDENTIFIER")
            .unwrap();
        let identifier = tag.value().unwrap().to_string();
        let namespace = if let Some(namespace) = namespace {
            namespace
        } else {
            let tag = tags
                .iter()
                .find(|tag| tag.key().unwrap() == "OBK_TARGET_NAMESPACE")
                .unwrap();
            let target_namespace = tag.value().unwrap().to_string();
            target_namespace
        };
        let peer_id = uuid::Uuid::new_v4().to_string();
        // Get public url.
        let is_public: bool = std::env::var("OBK_PUBLIC").unwrap().parse().unwrap();
        let public_url = if is_public {
            loop {
                let resp = crate::get_public_url().await;
                if resp.is_err() {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    println!("Retrying public URL: {resp:?}");
                    continue;
                } else {
                    break Some(resp.unwrap());
                }
            }
        } else {
            None
        };
        println!("Public Url: {public_url:?}");
        Ok(InstanceInfo {
            peer_id,
            public_url,
            private_url,
            mem,
            cpus,
            az: Some(az.into()),
            subsystem,
            namespace,
            identifier,
            persistent,
            unique,
            service_name,
            handler_name,
        })
    }
}
