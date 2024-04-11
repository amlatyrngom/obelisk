use std::collections::{HashMap, HashSet};

use crate::{bucket_name, tmp_s3_dir};

// use crate::{messaging_name_prefix, service_name_prefix};
use serde::{Deserialize, Serialize};

pub mod container;
pub mod dynamo;
mod filesystem;
pub mod lambda;
pub mod networking;
mod security;

#[derive(Clone)]
pub struct Deployment {
    subnet_ids: Vec<String>,
    sg_id: String,
    lambda_role_arn: String,
    ecs_role_arn: String,
    ecs_client: aws_sdk_ecs::Client,
    s3_client: aws_sdk_s3::Client,
    lambda_client: aws_sdk_lambda::Client,
    efs_client: aws_sdk_efs::Client,
    pub dynamo_client: aws_sdk_dynamodb::Client,
    region: String,
}

/// Specification for a handler.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespaceSpec {
    pub namespace: String,
    pub public_img_url: String,
    pub private_img_url: String,
    pub subsystem_spec: Option<SubsystemSpec>,
    pub handler_specs: HashMap<String, HandlerSpec>,
    pub dependencies: Vec<String>,
}

/// Specification for a handler.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubsystemSpec {
    pub rescaler_spec: RescalerSpec,
    pub service_specs: HashMap<String, ServiceSpec>,
}

/// Specification for a rescaler.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RescalerSpec {
    pub namespace: String,
    pub timeout: i32,
    pub mem: i32,
}

/// Specification for a handler.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceSpec {
    pub namespace: String,
    pub name: String,
    pub timeout: i32,
    pub mem: i32,
    pub cpus: i32,
    pub unique: bool,
}

/// Specification for a handler.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandlerSpec {
    pub subsystem: String,
    pub namespace: String,
    pub name: String,
    pub timeout: i32,
    pub default_mem: i32,
    pub concurrency: i32,
    pub ephemeral: i32,
    pub persistent: bool,
    pub unique: bool,
    pub scaleup: f64,
    pub spot: bool,
}

pub struct NamespaceDeployment {}

impl Deployment {
    pub fn make_identifier(name: &str, id: usize) -> String {
        format!("{name}{id}")
    }

    /// Create a deployer.
    pub async fn new() -> Self {
        let shared_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        // Security.
        let security_deployment = security::SecurityDeployment::new().await;
        let lambda_role_arn = security_deployment.lambda_role_arn;
        let ecs_role_arn = security_deployment.ecs_role_arn;
        // Networking.
        let networking_deployment = networking::NetworkingDeployment::new().await;
        let subnet_ids = networking_deployment.subnet_ids;
        let sg_id = networking_deployment.sg_id;
        let deployment = Deployment {
            lambda_client: aws_sdk_lambda::Client::new(&shared_config),
            dynamo_client: aws_sdk_dynamodb::Client::new(&shared_config),
            ecs_client: aws_sdk_ecs::Client::new(&shared_config),
            efs_client: aws_sdk_efs::Client::new(&shared_config),
            s3_client: aws_sdk_s3::Client::new(&shared_config),
            region: shared_config.region().unwrap().to_string(),
            subnet_ids,
            sg_id,
            lambda_role_arn,
            ecs_role_arn,
        };
        deployment
    }

    /// Setup a namespace deployment.
    pub async fn setup_namespace(&self, namespace_spec: &NamespaceSpec) -> Result<(), String> {
        // Create bucket.
        self.create_bucket().await;
        // Create deployment table.
        dynamo::DynamoDeployment::create_scaling_table(
            &self.dynamo_client,
            &namespace_spec.namespace,
        )
        .await?;
        dynamo::DynamoDeployment::write_namespace_spec(&self.dynamo_client, namespace_spec).await?;
        // Create filesystem.
        let fs_id = filesystem::FilesystemDeployment::get_file_system(
            &self.efs_client,
            &namespace_spec.namespace,
        )
        .await;
        let (ap_id, ap_arn) = filesystem::FilesystemDeployment::get_fs_access_point(
            &self.efs_client,
            &namespace_spec.namespace,
            &fs_id,
        )
        .await;
        filesystem::FilesystemDeployment::make_mount_target(
            &self.efs_client,
            &self.subnet_ids,
            &self.sg_id,
            &fs_id,
        )
        .await;
        // Create cluster.
        container::ContainerDeployment::create_cluster(&self.ecs_client, &namespace_spec.namespace)
            .await?;
        // Deploy rescaler, services and handlers.
        if let Some(subys_spec) = &namespace_spec.subsystem_spec {
            lambda::LambdaDeployment::create_rescaler_lambda(
                &self.lambda_client,
                &subys_spec.rescaler_spec,
                &namespace_spec.private_img_url,
                &self.lambda_role_arn,
            )
            .await;
            for (_, spec) in &subys_spec.service_specs {
                container::ContainerDeployment::register_service_task_definition(
                    &self.ecs_client,
                    spec,
                    &namespace_spec.public_img_url,
                    &self.ecs_role_arn,
                    &fs_id,
                    &ap_id,
                    None,
                )
                .await;
            }
        }
        for (_, spec) in &namespace_spec.handler_specs {
            container::ContainerDeployment::register_handler_task_definitions(
                &self.ecs_client,
                spec,
                &namespace_spec.public_img_url,
                &self.ecs_role_arn,
                &fs_id,
                &ap_id,
            )
            .await;
            lambda::LambdaDeployment::create_handler_lambda(
                &self.lambda_client,
                spec,
                &namespace_spec.private_img_url,
                &self.lambda_role_arn,
                &ap_arn,
                &self.subnet_ids,
                &self.sg_id,
            )
            .await;
        }
        Ok(())
    }

    /// Redeploy subsubsystem used in this namespace.
    pub async fn redeploy_subsystems(&self, namespace_spec: &NamespaceSpec) {
        let fs_id = filesystem::FilesystemDeployment::get_file_system(
            &self.efs_client,
            &namespace_spec.namespace,
        )
        .await;
        let (ap_id, _ap_arn) = filesystem::FilesystemDeployment::get_fs_access_point(
            &self.efs_client,
            &namespace_spec.namespace,
            &fs_id,
        )
        .await;
        let mut redeployed_subsystems = HashSet::<String>::new();
        let mut dependencies = namespace_spec.dependencies.clone();
        // Always include self.
        if namespace_spec.subsystem_spec.is_some() {
            dependencies.push(namespace_spec.namespace.clone());
        }
        for (_, handler_spec) in &namespace_spec.handler_specs {
            dependencies.push(handler_spec.subsystem.clone());
        }
        let target_namespace = Some(namespace_spec.namespace.clone());
        for dependency in dependencies {
            if !redeployed_subsystems.contains(&dependency) {
                let (subsys_ns_spec, _) = dynamo::DynamoDeployment::fetch_namespace_spec(
                    &self.dynamo_client,
                    &dependency,
                )
                .await
                .unwrap();
                for (_, spec) in &subsys_ns_spec.subsystem_spec.unwrap().service_specs {
                    container::ContainerDeployment::register_service_task_definition(
                        &self.ecs_client,
                        spec,
                        &subsys_ns_spec.public_img_url,
                        &self.ecs_role_arn,
                        &fs_id,
                        &ap_id,
                        target_namespace.clone(),
                    )
                    .await;
                }
                redeployed_subsystems.insert(dependency.clone());
            }
        }
    }

    // /// Delete a specific service.
    // pub async fn delete_service(&self, subsystem: &str, namespace: &str, name: &str) {
    //     let service_name = full_service_name(subsystem, namespace, name);
    //     let cluster_name = cluster_name();
    //     let res = self
    //         .ecs_client
    //         .delete_service()
    //         .cluster(&cluster_name)
    //         .service(&service_name)
    //         .force(true)
    //         .send()
    //         .await;
    //     if res.is_err() {
    //         let err = format!("{res:?}");
    //         if !err.contains("ClusterNotFoundException") {
    //             panic!("Unhandled ecs error: {err}");
    //         }
    //     }
    // }

    // /// Deregister ecs service.
    // pub async fn deregister_ecs_service_definition(&self, service_def_name: &str) {
    //     let defs = self
    //         .ecs_client
    //         .list_task_definitions()
    //         .family_prefix(service_def_name)
    //         .max_results(100)
    //         .send()
    //         .await
    //         .unwrap();
    //     for def_arn in defs.task_definition_arns().unwrap() {
    //         self.ecs_client
    //             .deregister_task_definition()
    //             .task_definition(def_arn)
    //             .send()
    //             .await
    //             .unwrap();
    //     }
    // }

    // /// Delete all previous services.
    // pub async fn delete_all_services(&self, prefix: &str) {
    //     // Iterate through all services
    //     let mut next_token: Option<String> = None;
    //     let mut first = true;
    //     let mut to_delete = Vec::new();
    //     loop {
    //         if next_token.is_none() && !first {
    //             break;
    //         }
    //         first = false;
    //         let mut svc_arns = self
    //             .ecs_client
    //             .list_services()
    //             .cluster(cluster_name())
    //             .max_results(10);
    //         if let Some(next_token) = &next_token {
    //             svc_arns = svc_arns.next_token(next_token.clone());
    //         }
    //         let svc_arns = svc_arns.send().await.unwrap();
    //         if let Some(tok) = svc_arns.next_token() {
    //             next_token = Some(tok.into());
    //         }
    //         let svc_arns = svc_arns.service_arns().unwrap();
    //         if svc_arns.is_empty() {
    //             break;
    //         }
    //         let mut svcs = self.ecs_client.describe_services().cluster(cluster_name());
    //         for svc_arn in svc_arns {
    //             svcs = svcs.services(svc_arn);
    //         }
    //         let svcs = svcs.send().await.unwrap();
    //         let svcs = svcs.services().unwrap();
    //         for svc in svcs {
    //             let name = svc.service_name().unwrap();
    //             println!("Deleting: {name}");
    //             if !name.starts_with(prefix) {
    //                 continue;
    //             }
    //             to_delete.push(name.to_string());
    //         }
    //     }
    //     for s in to_delete {
    //         let _ = self
    //             .ecs_client
    //             .delete_service()
    //             .cluster(cluster_name())
    //             .service(&s)
    //             .force(true)
    //             .send()
    //             .await;
    //     }
    // }

    pub async fn create_bucket(&self) {
        // let resp = self
        //     .s3_client
        //     .delete_bucket()
        //     .bucket(&bucket_name())
        //     .send()
        //     .await;
        // println!("Delete resp: {resp:?}");

        let resp = self
            .s3_client
            .create_bucket()
            .bucket(&bucket_name())
            .create_bucket_configuration(
                aws_sdk_s3::types::CreateBucketConfiguration::builder()
                    .location_constraint(aws_sdk_s3::types::BucketLocationConstraint::from(
                        self.region.as_str(),
                    ))
                    .build(),
            )
            .send()
            .await;
        println!("Resp: {resp:?}");
        let resp = self.s3_client.list_buckets().send().await.unwrap();
        println!("List resp: {resp:?}");

        let resp = self
            .s3_client
            .put_bucket_lifecycle_configuration()
            .bucket(&bucket_name())
            .lifecycle_configuration(
                aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
                    .rules(
                        aws_sdk_s3::types::LifecycleRule::builder()
                            .id("obelisk-expiration")
                            .expiration(
                                aws_sdk_s3::types::LifecycleExpiration::builder()
                                    .days(1)
                                    .build(),
                            )
                            .filter(aws_sdk_s3::types::LifecycleRuleFilter::Prefix(tmp_s3_dir()))
                            .status(aws_sdk_s3::types::ExpirationStatus::Enabled)
                            .build()
                            .unwrap(),
                    )
                    .build()
                    .unwrap(),
            )
            .send()
            .await;
        println!("Lifecycle resp: {resp:?}");
    }
}
