use crate::{
    bucket_name, clean_die, cluster_name, filesystem_name, full_function_name,
    full_messaging_template_name, full_scaler_name,
    full_scaling_queue_name, full_service_definition_name, full_service_name, scaling_queue_prefix,
    scaling_table_name, tmp_s3_dir,
};

// use crate::{messaging_name_prefix, service_name_prefix};

use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::types::{AttributeDefinition, ScalarAttributeType, TimeToLiveSpecification};
use aws_sdk_dynamodb::types::{BillingMode, KeySchemaElement, KeyType};
use aws_sdk_ecs::types::{ContainerDefinition, KeyValuePair, PortMapping};
use aws_sdk_efs::types::FileSystemDescription;
use aws_sdk_lambda::types::{FunctionCode, PackageType};
use aws_sdk_sqs::types::QueueAttributeName;
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct AdapterDeployment {
    ec2_client: aws_sdk_ec2::Client,
    ecs_client: aws_sdk_ecs::Client,
    s3_client: aws_sdk_s3::Client,
    lambda_client: aws_sdk_lambda::Client,
    sqs_client: aws_sdk_sqs::Client,
    efs_client: aws_sdk_efs::Client,
    pub dynamo_client: aws_sdk_dynamodb::Client,
    region: String,
}

pub struct FunctionalSpec {
    pub namespace: String,
    pub mem: i32,
    pub timeout: i32,
    pub concurrency: i32,
    pub ephemeral: i32,
    pub caller_mem: i32,
}

pub struct MessagingSpec {
    pub namespace: String,
    pub cpus: i32,
    pub mem: i32,
    pub fn_mem: i32,
    pub caller_mem: i32,
    pub timeout: i32,
}

pub struct SubsystemSpec {
    pub subsystem: String,
    pub svc_cpus: i32,
    pub svc_mem: i32,
    pub svc_grace: i32,
    pub scl_mem: i32,
    pub scl_timeout: i32,
}

#[derive(Serialize, Deserialize)]
pub struct FunctionalDeploymentInfo {
    pub invoker_cpu: i32,
    pub invoker_mem: i32,
    pub num_workers: i32,
    pub total_ecs_cpus: i32,
    pub total_ecs_mem: i32,
    pub function_mem: i32,
    pub function_cpu: i32,
    pub concurrency: i32,
    pub caller_mem: i32,
}

#[derive(Serialize, Deserialize)]
pub struct MessagingDeploymentInfo {
    pub cpus: i32,
    pub mem: i32,
    pub fn_mem: i32,
    pub caller_mem: i32,
    pub timeout: i32,
}

#[derive(Serialize, Deserialize)]
pub struct SubsystemDeploymentInfo {
    pub svc_cpus: i32,
    pub svc_mem: i32,
    pub svc_grace: i32,
    pub scl_mem: i32,
    pub scl_timeout: i32,
}

#[derive(Serialize, Deserialize)]
pub struct DeploymentInfo {
    pub subsys_info: Option<SubsystemDeploymentInfo>,
    pub fn_info: Option<FunctionalDeploymentInfo>,
    pub msg_info: Option<MessagingDeploymentInfo>,
}

pub fn make_function_info(
    function_mem: i32,
    concurrency: i32,
    caller_mem: i32,
) -> FunctionalDeploymentInfo {
    let invoker_mem = 256;
    let invoker_cpu = 256;
    let function_cpu = (((function_mem as f64) / 1769.0) * 1024.0) as i32;
    let min_num_cpus = invoker_cpu + function_cpu;
    let num_cpus = if min_num_cpus <= 512 {
        512
    } else if min_num_cpus <= 1024 {
        1024
    } else if min_num_cpus <= 2048 {
        2048
    } else if min_num_cpus <= 4096 {
        4096
    } else {
        8192
    };
    let total_mem = 2 * num_cpus;
    // Very small lambdas (like 128MB) can be multiplexed on the remaining compute.
    // TODO: Figure how to further fill up remaining compute.
    let mut num_workers =
        1 + (((num_cpus - min_num_cpus) as f32) / (function_cpu as f32)).floor() as i32;
    if num_workers > 9 {
        num_workers = 9;
    }

    FunctionalDeploymentInfo {
        invoker_cpu,
        invoker_mem,
        num_workers,
        total_ecs_cpus: num_cpus,
        total_ecs_mem: total_mem,
        function_mem,
        function_cpu,
        concurrency,
        caller_mem,
    }
}

impl AdapterDeployment {
    /// Create a deployer.
    pub async fn new() -> Self {
        let shared_config = aws_config::load_from_env().await;
        AdapterDeployment {
            lambda_client: aws_sdk_lambda::Client::new(&shared_config),
            dynamo_client: aws_sdk_dynamodb::Client::new(&shared_config),
            ecs_client: aws_sdk_ecs::Client::new(&shared_config),
            ec2_client: aws_sdk_ec2::Client::new(&shared_config),
            sqs_client: aws_sdk_sqs::Client::new(&shared_config),
            efs_client: aws_sdk_efs::Client::new(&shared_config),
            s3_client: aws_sdk_s3::Client::new(&shared_config),
            region: shared_config.region().unwrap().to_string(),
        }
    }

    async fn get_subnet(&self) -> (Vec<String>, String) {
        self.get_subnet_with_endpoints(false).await
    }

    /// Get default public subnet.
    pub async fn get_subnet_with_endpoints(&self, setup_endpoints: bool) -> (Vec<String>, String) {
        let _ = self.ec2_client.create_default_vpc();
        let _ = self.ec2_client.create_default_subnet();
        // Get vpc.
        let vpc = self
            .ec2_client
            .describe_vpcs()
            .filters(
                aws_sdk_ec2::types::Filter::builder()
                    .name("isDefault")
                    .values("true")
                    .build(),
            )
            .send()
            .await
            .unwrap();
        let vpc = vpc.vpcs().unwrap().first().unwrap();
        let vpc_id = vpc.vpc_id().unwrap();
        // Get subnet ids.
        let subnet = self
            .ec2_client
            .describe_subnets()
            .filters(
                aws_sdk_ec2::types::Filter::builder()
                    .name("defaultForAz")
                    .values("true")
                    .build(),
            )
            .send()
            .await
            .unwrap();
        println!("Subnets: {subnet:?}");
        let subnets = subnet.subnets().unwrap();
        let subnet_ids: Vec<String> = subnets
            .iter()
            .map(|s| s.subnet_id().unwrap().into())
            .collect();
        // Get sg ids.
        let sg = self
            .ec2_client
            .describe_security_groups()
            .filters(
                aws_sdk_ec2::types::Filter::builder()
                    .name("vpc-id")
                    .values(vpc_id)
                    .build(),
            )
            .filters(
                aws_sdk_ec2::types::Filter::builder()
                    .name("group-name")
                    .values("default")
                    .build(),
            )
            .send()
            .await
            .unwrap();
        let sg = sg.security_groups().unwrap().first().unwrap();
        let sg_id: String = sg.group_id().unwrap().into();
        // Get routing table id.
        let rt = self
            .ec2_client
            .describe_route_tables()
            .filters(
                aws_sdk_ec2::types::Filter::builder()
                    .name("vpc-id")
                    .values(vpc_id)
                    .build(),
            )
            .filters(
                aws_sdk_ec2::types::Filter::builder()
                    .name("association.main")
                    .values("true")
                    .build(),
            )
            .send()
            .await
            .unwrap();
        let rt = rt.route_tables().unwrap().first().unwrap();
        let rt_id: String = rt.route_table_id().unwrap().into();
        println!("Routing table ids: {rt_id}");
        // Create access endpoints.
        if setup_endpoints {
            let svc_names = vec![
                // (format!("com.amazonaws.{}.sqs", self.region), false),
                // (format!("com.amazonaws.{}.lambda", self.region), false),
                (format!("com.amazonaws.{}.s3", self.region), true),
                (format!("com.amazonaws.{}.dynamodb", self.region), true),
            ];
            for (svc_name, is_gateway) in svc_names {
                let resp = self
                    .ec2_client
                    .describe_vpc_endpoints()
                    .filters(
                        aws_sdk_ec2::types::Filter::builder()
                            .name("vpc-id")
                            .values(vpc_id)
                            .build(),
                    )
                    .filters(
                        aws_sdk_ec2::types::Filter::builder()
                            .name("service-name")
                            .values(&svc_name)
                            .build(),
                    )
                    .send()
                    .await
                    .unwrap();
                if let Some(resp) = resp.vpc_endpoints() {
                    if let Some(resp) = resp.first() {
                        println!("Found endpoint: {resp:?}");
                        continue;
                    }
                }
                let mut req = self
                    .ec2_client
                    .create_vpc_endpoint()
                    .vpc_id(vpc_id)
                    .service_name(&svc_name);
                if is_gateway {
                    req = req
                        .vpc_endpoint_type(aws_sdk_ec2::types::VpcEndpointType::Gateway)
                        .route_table_ids(&rt_id);
                } else {
                    req = req
                        .vpc_endpoint_type(aws_sdk_ec2::types::VpcEndpointType::Interface)
                        .security_group_ids(&sg_id);
                    for subnet_id in subnet_ids.iter() {
                        req = req.subnet_ids(subnet_id)
                    }
                }

                let resp = req.send().await.unwrap();
                println!("EndpointResp: {resp:?}");
            }
        }

        (subnet_ids, sg_id)
    }

    pub async fn teardown_vpc_endpoints(&self) {
        // Get vpc.
        let vpc = self
            .ec2_client
            .describe_vpcs()
            .filters(
                aws_sdk_ec2::types::Filter::builder()
                    .name("isDefault")
                    .values("true")
                    .build(),
            )
            .send()
            .await
            .unwrap();
        let vpc = vpc.vpcs().unwrap().first().unwrap();
        let vpc_id = vpc.vpc_id().unwrap();
        let svc_names = vec![
            (format!("com.amazonaws.{}.s3", self.region), true),
            (format!("com.amazonaws.{}.dynamodb", self.region), true),
        ];
        let mut delete_req = self.ec2_client.delete_vpc_endpoints();
        let mut found = false;
        for (svc_name, _) in svc_names {
            let resp = self
                .ec2_client
                .describe_vpc_endpoints()
                .filters(
                    aws_sdk_ec2::types::Filter::builder()
                        .name("vpc-id")
                        .values(vpc_id)
                        .build(),
                )
                .filters(
                    aws_sdk_ec2::types::Filter::builder()
                        .name("service-name")
                        .values(&svc_name)
                        .build(),
                )
                .send()
                .await
                .unwrap();
            if let Some(resp) = resp.vpc_endpoints() {
                if let Some(resp) = resp.first() {
                    found = true;
                    let endpoint_id = resp.vpc_endpoint_id().unwrap();
                    delete_req = delete_req.vpc_endpoint_ids(endpoint_id);
                }
            }
        }
        if found {
            let _ = delete_req.send().await.unwrap();
        }
    }

    // /// Delete a scaling subsystem.
    // pub async fn delete_scaling_subsystem(&self, subsystem: &str) {
    //     self.delete_all_queues(subsystem).await;
    //     self.delete_scaling_table(subsystem).await;
    //     self.delete_all_services(subsystem).await;
    //     self.delete_scaler_function(subsystem).await;
    //     self.deregister_ecs_service_definition(subsystem).await;
    // }

    // /// Create a scaling subsystem
    // pub async fn create_scaling_subsystem(&self, system_uri: &str, private_image_uri: &str, public_image_uri: &str, function_role: &str, service_role: &str, spec: &ServiceSpec) {
    //     self.create_scaling_table(&spec.subsystem).await;
    //     self.create_scaler_function(&spec.subsystem, private_image_uri, function_role).await;
    //     if spec.subsystem != "functional" {
    //         self.register_ecs_service_definition(&spec, public_image_uri, service_role).await;
    //     }
    // }

    // /// Delete a scaling item.
    // pub async fn delete_scaling_item(&self, subsystem: &str, namespace: &str, name: &str) {
    //     self.delete_scaling_queue(subsystem, namespace, name).await;
    //     self.delete_service(subsystem, namespace, name).await;
    // }

    // /// Delete one scaling item.
    // pub async fn create_scaling_item(&self, subsystem: &str, namespace: &str, name: &str) {
    //     self.create_scaling_queue(subsystem, namespace, name).await;
    //     self.create_service(subsystem, namespace, name).await;
    // }

    // pub async fn create_functional_scaling_item(&self, system_uri: &str, private_uri: &str, public_uri: &str, spec: &FunctionSpec) {

    // }

    /// Delete scaling table.
    pub async fn delete_scaling_table(&self, subsystem: &str) {
        let scaling_table = scaling_table_name(subsystem);
        let res = self
            .dynamo_client
            .delete_table()
            .table_name(&scaling_table)
            .send()
            .await;
        if res.is_err() {
            let err = format!("{res:?}");
            if !err.contains("ResourceNotFoundException") {
                panic!("Unhandled dynamo error: {err}");
            }
        }
    }

    /// Create table of scaling states.
    pub async fn create_scaling_table(&self, subsystem: &str) {
        let scaling_table = scaling_table_name(subsystem);
        let resp = self
            .dynamo_client
            .create_table()
            .table_name(&scaling_table)
            .billing_mode(BillingMode::PayPerRequest)
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("namespace")
                    .key_type(KeyType::Hash)
                    .build(),
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("name")
                    .key_type(KeyType::Range)
                    .build(),
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("namespace")
                    .attribute_type(ScalarAttributeType::S)
                    .build(),
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("name")
                    .attribute_type(ScalarAttributeType::S)
                    .build(),
            )
            .send()
            .await;
        if resp.is_err() {
            let err = format!("{resp:?}");
            if !err.contains("ResourceInUseException") {
                eprintln!("Dynamodb request error: {resp:?}");
                std::process::exit(1);
            }
        } else {
            println!("Creating {scaling_table} table...");
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        }
        let resp = self
            .dynamo_client
            .update_time_to_live()
            .table_name(&scaling_table)
            .time_to_live_specification(
                TimeToLiveSpecification::builder()
                    .attribute_name("gc_ttl")
                    .enabled(true)
                    .build(),
            )
            .send()
            .await;
        if resp.is_err() {
            let err = format!("{resp:?}");
            if !err.contains("TimeToLive is already enabled") {
                eprintln!("Dynamodb request error: {resp:?}");
                std::process::exit(1);
            }
        }
    }

    pub async fn finish_deployment(&self, namespace: &str, deployment_info: DeploymentInfo) {
        // Reuse scaling table to store deployment info.
        let mut subsystems = Vec::new();
        if deployment_info.fn_info.is_some() {
            subsystems.push(String::from("functional"));
        }
        if deployment_info.msg_info.is_some() {
            subsystems.push(String::from("messaging"));
            subsystems.push(String::from("persistence"));
        }
        if deployment_info.subsys_info.is_some() {
            subsystems.push(namespace.into());
        }
        let revision = uuid::Uuid::new_v4().to_string();
        for subsystem in subsystems {
            self.create_scaling_table(&subsystem).await;
            let scaling_table = scaling_table_name(&subsystem);
            println!("Resetting revision {scaling_table}: {revision}");
            let deployment_str = serde_json::to_string(&deployment_info).unwrap();
            self.dynamo_client
                .put_item()
                .table_name(&scaling_table)
                .item("namespace", AttributeValue::S("system".into()))
                .item("name", AttributeValue::S(namespace.into()))
                .item("revision", AttributeValue::S(revision.clone()))
                .item("deployment", AttributeValue::S(deployment_str))
                .send()
                .await
                .unwrap();
        }
    }

    /// Assumes no more than 1000 actors in a particular subsystem.
    pub async fn delete_all_queues(&self, subsystem: &str) {
        let prefix = scaling_queue_prefix(subsystem);
        let queues = self
            .sqs_client
            .list_queues()
            .queue_name_prefix(&prefix)
            .max_results(1000)
            .send()
            .await
            .unwrap();
        for q in queues.queue_urls().unwrap() {
            let _ = self.sqs_client.delete_queue().queue_url(q).send().await;
        }
    }

    /// Delete scaling queue.
    pub async fn delete_scaling_queue(&self, subsystem: &str, namespace: &str, name: &str) {
        let queue_name = full_scaling_queue_name(subsystem, namespace, name);
        let res = self
            .sqs_client
            .get_queue_url()
            .queue_name(&queue_name)
            .send()
            .await;
        if res.is_err() {
            let err = format!("{res:?}");
            if !err.contains("NonExistentQueue") {
                panic!("Unhandled sqs error: {err}");
            }
        }
        let res = res.unwrap();
        let queue_url = res.queue_url().unwrap();
        let _ = self
            .sqs_client
            .delete_queue()
            .queue_url(queue_url)
            .send()
            .await;
    }

    /// Create a scaling queue.
    pub async fn create_scaling_queue(&self, subsystem: &str, namespace: &str, name: &str) {
        let queue_name = full_scaling_queue_name(subsystem, namespace, name);
        let res = self
            .sqs_client
            .create_queue()
            .queue_name(&queue_name)
            .attributes(QueueAttributeName::MessageRetentionPeriod, "60")
            .attributes(QueueAttributeName::VisibilityTimeout, "10")
            .send()
            .await;
        if res.is_err() {
            let err = format!("{res:?}");
            if !err.contains("QueueAlreadyExists") {
                let msg = format!("unhandled sqs error: {err}");
                clean_die(&msg).await;
            }
        }
    }

    /// Delete scaler function.
    pub async fn delete_scaler_function(&self, subsystem: &str) {
        let scaler_name = full_scaler_name(subsystem);
        let _ = self
            .lambda_client
            .delete_function()
            .function_name(&scaler_name)
            .send()
            .await;
    }

    /// Create scaler function for a subsystem.
    pub async fn create_scaler_function(
        &self,
        private_image_uri: &str,
        role_arn: &str,
        spec: &SubsystemSpec,
    ) {
        // Delete scaler function if it exists.
        self.delete_scaler_function(&spec.subsystem).await;
        // Create scaler function.
        let scaler_name = full_scaler_name(&spec.subsystem);
        let code = FunctionCode::builder()
            .image_uri(format!("{private_image_uri}:latest"))
            .build();
        let environ = aws_sdk_lambda::types::Environment::builder()
            .variables("SUBSYSTEM", &spec.subsystem)
            .variables("EXECUTION_MODE", "rescaler")
            .variables("MEMORY", spec.scl_mem.to_string());
        let environ = environ.build();
        self.lambda_client
            .create_function()
            .function_name(&scaler_name)
            .code(code)
            .memory_size(spec.scl_mem)
            .package_type(PackageType::Image)
            .timeout(spec.scl_timeout)
            .role(role_arn)
            .environment(environ)
            .send()
            .await
            .unwrap();
        // self.lambda_client
        //     .put_function_concurrency()
        //     .function_name(&scaler_name)
        //     .reserved_concurrent_executions(1)
        //     .send()
        //     .await
        //     .unwrap();
    }

    /// Delete a specific service.
    pub async fn delete_service(&self, subsystem: &str, namespace: &str, name: &str) {
        let service_name = full_service_name(subsystem, namespace, name);
        let cluster_name = cluster_name();
        let res = self
            .ecs_client
            .delete_service()
            .cluster(&cluster_name)
            .service(&service_name)
            .force(true)
            .send()
            .await;
        if res.is_err() {
            let err = format!("{res:?}");
            if !err.contains("ClusterNotFoundException") {
                panic!("Unhandled ecs error: {err}");
            }
        }
    }

    /// Create a system-level service.
    /// Also marks the service as created with the given revision.
    pub async fn create_service(&self, subsystem: &str, namespace: &str, name: &str) {
        let (subnet_ids, sg_id) = self.get_subnet().await;
        let service_name = full_service_name(subsystem, namespace, name);
        let service_def_name = if subsystem == "functional" {
            full_function_name(namespace)
        } else if subsystem == "messaging" {
            full_messaging_template_name(namespace)
        } else {
            full_service_definition_name(subsystem)
        };
        let mut vpc_config = aws_sdk_ecs::types::AwsVpcConfiguration::builder()
            .assign_public_ip(aws_sdk_ecs::types::AssignPublicIp::Enabled)
            .security_groups(&sg_id);
        for subnet_id in subnet_ids {
            vpc_config = vpc_config.subnets(subnet_id);
        }
        let vpc_config = vpc_config.build();
        let service_def = self
            .ecs_client
            .create_service()
            .cluster(cluster_name())
            .service_name(&service_name)
            .task_definition(&service_def_name)
            .desired_count(0)
            .network_configuration(
                aws_sdk_ecs::types::NetworkConfiguration::builder()
                    .awsvpc_configuration(vpc_config.clone())
                    .build(),
            )
            .capacity_provider_strategy(
                aws_sdk_ecs::types::CapacityProviderStrategyItem::builder()
                    .capacity_provider("FARGATE_SPOT")
                    .weight(1)
                    .base(0)
                    .build(),
            );
        let created = service_def.clone().send().await;
        let created = if created.is_ok() {
            true
        } else {
            let err = format!("{created:?}");
            if !err.contains("idempotent") {
                panic!("Unhandled service creation error: {err}");
            }
            false
        };
        if !created {
            self.ecs_client
                .update_service()
                .cluster(cluster_name())
                .service(&service_name)
                .task_definition(&service_def_name)
                .force_new_deployment(true)
                .desired_count(0)
                .network_configuration(
                    aws_sdk_ecs::types::NetworkConfiguration::builder()
                        .awsvpc_configuration(vpc_config.clone())
                        .build(),
                )
                .send()
                .await
                .unwrap();
        }
    }

    /// Scale a service.
    pub async fn scale_service(&self, subsystem: &str, namespace: &str, name: &str, count: i32) {
        let service_name = full_service_name(subsystem, namespace, name);
        self.ecs_client
            .update_service()
            .desired_count(count)
            .cluster(cluster_name())
            .service(service_name)
            .send()
            .await
            .unwrap();
    }

    /// Deregister ecs service.
    pub async fn deregister_ecs_service_definition(&self, service_def_name: &str) {
        let defs = self
            .ecs_client
            .list_task_definitions()
            .family_prefix(service_def_name)
            .max_results(100)
            .send()
            .await
            .unwrap();
        for def_arn in defs.task_definition_arns().unwrap() {
            self.ecs_client
                .deregister_task_definition()
                .task_definition(def_arn)
                .send()
                .await
                .unwrap();
        }
    }

    /// Register ecs task def for service.
    pub async fn register_ecs_service_definition(
        &self,
        spec: &SubsystemSpec,
        public_image_uri: &str,
        role_arn: &str,
    ) {
        // Deregister existing definitions.
        // self.delete_all_services(&service_name_prefix(&spec.subsystem))
        //     .await;
        let service_def_name = full_service_definition_name(&spec.subsystem);
        self.deregister_ecs_service_definition(&service_def_name)
            .await;
        // Create new definition.
        let task_def = self
            .ecs_client
            .register_task_definition()
            .task_role_arn(role_arn)
            .execution_role_arn(role_arn)
            .cpu(format!("{}", spec.svc_cpus))
            .memory(format!("{}", spec.svc_mem))
            .requires_compatibilities(aws_sdk_ecs::types::Compatibility::Fargate)
            .network_mode(aws_sdk_ecs::types::NetworkMode::Awsvpc)
            .family(&service_def_name);

        let container_def = ContainerDefinition::builder()
            .cpu(spec.svc_cpus)
            .memory(spec.svc_mem)
            .image(format!("{public_image_uri}:latest"))
            .stop_timeout(spec.svc_grace)
            .port_mappings(
                PortMapping::builder()
                    .host_port(37000)
                    .container_port(37000)
                    .build(),
            )
            .name(&service_def_name)
            .log_configuration(
                aws_sdk_ecs::types::LogConfiguration::builder()
                    .log_driver(aws_sdk_ecs::types::LogDriver::Awslogs)
                    .options("awslogs-group", format!("service-{service_def_name}"))
                    .options("awslogs-create-group", "true")
                    .options("awslogs-region", &self.region)
                    .options("awslogs-stream-prefix", service_def_name)
                    .build(),
            )
            .environment(
                KeyValuePair::builder()
                    .name("SUBSYSTEM")
                    .value(&spec.subsystem)
                    .build(),
            )
            .environment(
                KeyValuePair::builder()
                    .name("MEMORY")
                    .value(spec.svc_mem.to_string())
                    .build(),
            )
            .environment(
                KeyValuePair::builder()
                    .name("EXECUTION_MODE")
                    .value("generic_ecs")
                    .build(),
            )
            .environment(
                KeyValuePair::builder()
                    .name("GRACE")
                    .value(spec.svc_grace.to_string())
                    .build(),
            )
            .build();
        let task_def = task_def.container_definitions(container_def);
        task_def.send().await.unwrap();
    }

    pub async fn create_functional_lambda(
        &self,
        private_image_uri: &str,
        role_arn: &str,
        spec: &FunctionalSpec,
    ) {
        let function_name = full_function_name(&spec.namespace);
        let code = FunctionCode::builder()
            .image_uri(format!("{private_image_uri}:latest"))
            .build();
        let environ = aws_sdk_lambda::types::Environment::builder()
            .variables("NAME", "fn")
            .variables("NAMESPACE", spec.namespace.clone())
            .variables("EXECUTION_MODE", "functional_lambda")
            .variables("MEMORY", spec.mem.to_string())
            .variables("TIMEOUT", spec.timeout.to_string())
            .variables("CONCURRENCY", spec.concurrency.to_string());
        let environ = environ.build();
        let _ = self
            .lambda_client
            .delete_function()
            .function_name(&function_name)
            .send()
            .await;
        self.lambda_client
            .create_function()
            .function_name(&function_name)
            .code(code)
            .memory_size(spec.mem)
            .ephemeral_storage(
                aws_sdk_lambda::types::EphemeralStorage::builder()
                    .size(spec.ephemeral)
                    .build(),
            )
            .package_type(PackageType::Image)
            .timeout(spec.timeout)
            .role(role_arn)
            .environment(environ)
            .send()
            .await
            .unwrap();
    }

    /// Register functional service.
    pub async fn register_functional_ecs_service_definition(
        &self,
        invoker_image_uri: &str,
        public_image_uri: &str,
        role_arn: &str,
        spec: &FunctionalSpec,
    ) -> FunctionalDeploymentInfo {
        // Create.
        let function_name = full_function_name(&spec.namespace);
        let deployment_info = make_function_info(spec.mem, spec.concurrency, spec.caller_mem);
        let FunctionalDeploymentInfo {
            invoker_cpu,
            invoker_mem,
            num_workers,
            total_ecs_cpus,
            total_ecs_mem,
            function_mem,
            function_cpu,
            concurrency,
            caller_mem: _,
        } = &deployment_info;
        let mut task_def = self
            .ecs_client
            .register_task_definition()
            .task_role_arn(role_arn)
            .execution_role_arn(role_arn)
            .cpu(format!("{total_ecs_cpus}"))
            .memory(format!("{total_ecs_mem}"))
            .requires_compatibilities(aws_sdk_ecs::types::Compatibility::Fargate)
            .network_mode(aws_sdk_ecs::types::NetworkMode::Awsvpc)
            .family(&function_name);
        let mut invoker_def = ContainerDefinition::builder()
            .cpu(*invoker_cpu)
            .memory(*invoker_mem)
            .image(format!("{invoker_image_uri}:latest"))
            .stop_timeout(spec.timeout)
            .port_mappings(
                PortMapping::builder()
                    .host_port(37000)
                    .container_port(37000)
                    .build(),
            )
            .name("invoker")
            .log_configuration(
                aws_sdk_ecs::types::LogConfiguration::builder()
                    .log_driver(aws_sdk_ecs::types::LogDriver::Awslogs)
                    .options("awslogs-group", format!("invoker-{function_name}"))
                    .options("awslogs-create-group", "true")
                    .options("awslogs-region", &self.region)
                    .options("awslogs-stream-prefix", &function_name)
                    .build(),
            )
            .environment(
                KeyValuePair::builder()
                    .name("MEMORY")
                    .value(invoker_mem.to_string())
                    .build(),
            )
            .environment(
                KeyValuePair::builder()
                    .name("EXECUTION_MODE")
                    .value("functional_backend")
                    .build(),
            )
            .environment(
                KeyValuePair::builder()
                    .name("NUM_WORKERS")
                    .value(num_workers.to_string())
                    .build(),
            )
            .environment(
                KeyValuePair::builder()
                    .name("SUBSYSTEM")
                    .value("functional")
                    .build(),
            )
            .environment(
                KeyValuePair::builder()
                    .name("FUNCTION_NAMESPACE")
                    .value(&spec.namespace)
                    .build(),
            )
            .environment(
                KeyValuePair::builder()
                    .name("CONCURRENCY")
                    .value(concurrency.to_string())
                    .build(),
            )
            .environment(
                KeyValuePair::builder()
                    .name("TIMEOUT")
                    .value(spec.timeout.to_string())
                    .build(),
            );

        for i in 0..*num_workers {
            let port = 37001 + i;
            let container_name = format!("worker{i}");
            task_def = task_def.container_definitions(
                ContainerDefinition::builder()
                    .cpu(*function_cpu)
                    .memory(spec.mem)
                    .image(format!("{public_image_uri}:latest"))
                    .stop_timeout(spec.timeout)
                    .port_mappings(
                        PortMapping::builder()
                            .host_port(port)
                            .container_port(port)
                            .build(),
                    )
                    .name(&container_name)
                    .log_configuration(
                        aws_sdk_ecs::types::LogConfiguration::builder()
                            .log_driver(aws_sdk_ecs::types::LogDriver::Awslogs)
                            .options("awslogs-group", format!("worker-{function_name}"))
                            .options("awslogs-create-group", "true")
                            .options("awslogs-region", &self.region)
                            .options("awslogs-stream-prefix", &function_name)
                            .build(),
                    )
                    .environment(
                        KeyValuePair::builder()
                            .name("NAME")
                            .value(&function_name)
                            .build(),
                    )
                    .environment(
                        KeyValuePair::builder()
                            .name("NAMESPACE")
                            .value(&spec.namespace)
                            .build(),
                    )
                    .environment(
                        KeyValuePair::builder()
                            .name("MEMORY")
                            .value(function_mem.to_string())
                            .build(),
                    )
                    .environment(
                        KeyValuePair::builder()
                            .name("EXECUTION_MODE")
                            .value("functional_ecs")
                            .build(),
                    )
                    .environment(
                        KeyValuePair::builder()
                            .name("WORKER_INDEX")
                            .value(i.to_string())
                            .build(),
                    )
                    .environment(
                        KeyValuePair::builder()
                            .name("CONCURRENCY")
                            .value(spec.concurrency.to_string())
                            .build(),
                    )
                    .environment(
                        KeyValuePair::builder()
                            .name("TIMEOUT")
                            .value(spec.timeout.to_string())
                            .build(),
                    )
                    .build(),
            );
            invoker_def = invoker_def.depends_on(
                aws_sdk_ecs::types::ContainerDependency::builder()
                    .container_name(&container_name)
                    .condition(aws_sdk_ecs::types::ContainerCondition::Start)
                    .build(),
            )
        }
        let task_def = task_def.container_definitions(invoker_def.build());
        task_def.send().await.unwrap();
        deployment_info
    }

    /// Delete all previous services.
    pub async fn delete_all_services(&self, prefix: &str) {
        // Iterate through all services
        let mut next_token: Option<String> = None;
        let mut first = true;
        let mut to_delete = Vec::new();
        loop {
            if next_token.is_none() && !first {
                break;
            }
            first = false;
            let mut svc_arns = self
                .ecs_client
                .list_services()
                .cluster(cluster_name())
                .max_results(10);
            if let Some(next_token) = &next_token {
                svc_arns = svc_arns.next_token(next_token.clone());
            }
            let svc_arns = svc_arns.send().await.unwrap();
            if let Some(tok) = svc_arns.next_token() {
                next_token = Some(tok.into());
            }
            let svc_arns = svc_arns.service_arns().unwrap();
            if svc_arns.is_empty() {
                break;
            }
            let mut svcs = self.ecs_client.describe_services().cluster(cluster_name());
            for svc_arn in svc_arns {
                svcs = svcs.services(svc_arn);
            }
            let svcs = svcs.send().await.unwrap();
            let svcs = svcs.services().unwrap();
            for svc in svcs {
                let name = svc.service_name().unwrap();
                println!("Deleting: {name}");
                if !name.starts_with(prefix) {
                    continue;
                }
                to_delete.push(name.to_string());
            }
        }
        for s in to_delete {
            let _ = self
                .ecs_client
                .delete_service()
                .cluster(cluster_name())
                .service(&s)
                .force(true)
                .send()
                .await;
        }
    }

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
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await;
        println!("Lifecycle resp: {resp:?}");
    }

    /// Create ECS cluster.
    pub async fn create_cluster(&self) {
        let _ = self
            .ecs_client
            .create_cluster()
            .cluster_name(&cluster_name())
            .send()
            .await
            .unwrap();
    }

    pub async fn get_mount_target(&self, fs_id: &str) {
        let (subnet_ids, sg_id) = self.get_subnet().await;
        let mut created = false;
        for subnet_id in subnet_ids {
            let mt = self
                .efs_client
                .create_mount_target()
                .security_groups(&sg_id)
                .subnet_id(&subnet_id)
                .file_system_id(fs_id)
                .send()
                .await;
            created = created
                || match mt {
                    Ok(_) => true,
                    Err(err) => {
                        let err = format!("{err:?}");
                        if !err.contains("already exists") {
                            panic!("Mount target error: {err}");
                        }
                        false
                    }
                };
        }
        if created {
            loop {
                let mts = self
                    .efs_client
                    .describe_mount_targets()
                    .file_system_id(fs_id)
                    .send()
                    .await
                    .unwrap();
                let mut avail = true;
                for mt in mts.mount_targets().unwrap() {
                    let lf = mt.life_cycle_state().unwrap();
                    match lf {
                        aws_sdk_efs::types::LifeCycleState::Available => {}
                        _ => {
                            avail = false;
                            break;
                        }
                    }
                }
                if !avail {
                    println!("Waiting for mount targets...");
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                } else {
                    break;
                }
            }
        }
    }

    pub async fn get_fs_access_point(&self, namespace: &str, fs_id: &str) -> (String, String) {
        let fs_name = filesystem_name(namespace);
        let ap_info = self
            .efs_client
            .describe_access_points()
            .file_system_id(fs_id)
            .max_results(1)
            .send()
            .await;

        if let Ok(ap_info) = ap_info {
            if let Some(ap_info) = ap_info.access_points() {
                if !ap_info.is_empty() {
                    let ap_id = ap_info.first().unwrap().access_point_id().unwrap();
                    let ap_arn = ap_info.first().unwrap().access_point_arn().unwrap();
                    return (ap_id.into(), ap_arn.into());
                }
            }
        }

        let _ = self
            .efs_client
            .create_access_point()
            .file_system_id(fs_id)
            .root_directory(
                aws_sdk_efs::types::RootDirectory::builder()
                    .path("/obelisk")
                    .creation_info(
                        aws_sdk_efs::types::CreationInfo::builder()
                            .owner_gid(1001)
                            .owner_uid(1001)
                            .permissions("755")
                            .build(),
                    )
                    .build(),
            )
            .posix_user(
                aws_sdk_efs::types::PosixUser::builder()
                    .uid(1001)
                    .gid(1001)
                    .build(),
            )
            .client_token(&fs_name)
            .send()
            .await
            .unwrap();
        loop {
            let ap_info = self
                .efs_client
                .describe_access_points()
                .file_system_id(fs_id)
                .max_results(1)
                .send()
                .await
                .unwrap()
                .access_points()
                .unwrap()
                .first()
                .unwrap()
                .clone();
            match ap_info.life_cycle_state().unwrap() {
                aws_sdk_efs::types::LifeCycleState::Available => {
                    let ap_id = ap_info.access_point_id().unwrap();
                    let ap_arn = ap_info.access_point_arn().unwrap();
                    return (ap_id.into(), ap_arn.into());
                }
                _ => {
                    println!("Waiting for access point...");
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    continue;
                }
            }
        }
    }

    /// Get or create file system.
    pub async fn get_file_system(&self, namespace: &str) -> String {
        let fs_name = filesystem_name(namespace);
        let mut marker: Option<String> = None;
        loop {
            let mut fs_info = self
                .efs_client
                .describe_file_systems()
                .creation_token(&fs_name)
                .max_items(100);
            if let Some(marker) = &marker {
                fs_info = fs_info.marker(marker);
            }
            let fs_info = fs_info.send().await.unwrap();
            marker = fs_info.next_marker().map(|x| x.to_string());
            let fs_info: Vec<FileSystemDescription> =
                fs_info.file_systems().map_or(vec![], |f| f.into());
            if fs_info.is_empty() {
                break;
            }
            for fs in fs_info {
                if fs.creation_token().unwrap_or("") == fs_name
                    && fs.name().unwrap_or("") == fs_name
                {
                    return fs.file_system_id().unwrap().into();
                }
            }
            if marker.is_none() {
                break;
            }
        }
        let _ = self
            .efs_client
            .create_file_system()
            .tags(
                aws_sdk_efs::types::Tag::builder()
                    .key("Name")
                    .value(&fs_name)
                    .build(),
            )
            .performance_mode(aws_sdk_efs::types::PerformanceMode::GeneralPurpose)
            .creation_token(&fs_name)
            .send()
            .await
            .unwrap();
        loop {
            let fs_info = self
                .efs_client
                .describe_file_systems()
                .creation_token(&fs_name)
                .max_items(1)
                .send()
                .await
                .unwrap()
                .file_systems()
                .unwrap()
                .first()
                .unwrap()
                .clone();
            let state = fs_info.life_cycle_state().unwrap();
            match state {
                aws_sdk_efs::types::LifeCycleState::Available => {
                    return fs_info.file_system_id().unwrap().into();
                }
                _ => {
                    println!("Waiting for EFS...");
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    continue;
                }
            }
        }
    }

    /// Create messaging lambda function.
    pub async fn create_messaging_lambda_template(
        &self,
        private_image_uri: &str,
        role_arn: &str,
        ap_arn: &str,
        spec: &MessagingSpec,
    ) {
        let function_name = full_messaging_template_name(&spec.namespace);
        let (subnet_ids, sg_id) = self.get_subnet().await;
        // Delete existiting functions. TODO: support more than 50.
        let mut marker: Option<String> = None;
        loop {
            let mut curr_fns = self.lambda_client.list_functions().max_items(50);
            if let Some(marker) = &marker {
                curr_fns = curr_fns.marker(marker);
            }
            let curr_fns = curr_fns.send().await.unwrap();
            marker = curr_fns.next_marker().map(|x| x.to_string());
            let curr_fns = curr_fns.functions().map_or(vec![], |f| {
                f.iter()
                    .map(|f| f.function_name().unwrap().to_string())
                    .collect()
            });
            if curr_fns.is_empty() {
                break;
            }
            for curr_fn in curr_fns {
                if curr_fn.contains(&function_name) {
                    println!("Deleting {curr_fn}.");
                    let _ = self
                        .lambda_client
                        .delete_function()
                        .function_name(&curr_fn)
                        .send()
                        .await;
                }
            }
            if marker.is_none() {
                break;
            }
        }

        let code = FunctionCode::builder()
            .image_uri(format!("{private_image_uri}:latest"))
            .build();
        let environ = aws_sdk_lambda::types::Environment::builder()
            .variables("NAME", "placeholder")
            .variables("NAMESPACE", &spec.namespace)
            .variables("EXECUTION_MODE", "messaging_lambda")
            .variables("MEMORY", spec.mem.to_string())
            .variables("TIMEOUT", spec.timeout.to_string());
        let environ = environ.build();
        let _ = self
            .lambda_client
            .delete_function()
            .function_name(&function_name)
            .send()
            .await;
        println!("Creating messaging function: {function_name}");
        let mut vpc_config = aws_sdk_lambda::types::VpcConfig::builder().security_group_ids(sg_id);
        for subnet_id in subnet_ids {
            vpc_config = vpc_config.subnet_ids(subnet_id);
        }
        let vpc_config = vpc_config.build();
        let resp = self
            .lambda_client
            .create_function()
            .function_name(&function_name)
            .code(code)
            .memory_size(spec.fn_mem)
            .package_type(PackageType::Image)
            .timeout(spec.timeout)
            .role(role_arn)
            .file_system_configs(
                aws_sdk_lambda::types::FileSystemConfig::builder()
                    .arn(ap_arn)
                    .local_mount_path(crate::messaging_mnt_path())
                    .build(),
            )
            .vpc_config(vpc_config)
            .environment(environ)
            .send()
            .await
            .unwrap();
        println!("Resp: {resp:?}")
    }

    /// Register ecs task def for messaging service.
    pub async fn register_messaging_service_definition(
        &self,
        spec: &MessagingSpec,
        public_image_uri: &str,
        role_arn: &str,
        fs_id: &str,
        ap_id: &str,
    ) {
        // Deregister existing definitions.
        // self.delete_all_services(&messaging_name_prefix(&spec.namespace))
        //     .await;
        let service_def_name = full_messaging_template_name(&spec.namespace);
        self.deregister_ecs_service_definition(&service_def_name)
            .await;
        // Create new definition.
        let task_def = self
            .ecs_client
            .register_task_definition()
            .task_role_arn(role_arn)
            .execution_role_arn(role_arn)
            .cpu(format!("{}", spec.cpus))
            .memory(format!("{}", spec.mem))
            .requires_compatibilities(aws_sdk_ecs::types::Compatibility::Fargate)
            .network_mode(aws_sdk_ecs::types::NetworkMode::Awsvpc)
            .family(&service_def_name)
            .volumes(
                aws_sdk_ecs::types::Volume::builder()
                    .name("obelisk_volume")
                    .efs_volume_configuration(
                        aws_sdk_ecs::types::EfsVolumeConfiguration::builder()
                            .file_system_id(fs_id)
                            .authorization_config(
                                aws_sdk_ecs::types::EfsAuthorizationConfig::builder()
                                    .access_point_id(ap_id)
                                    .iam(aws_sdk_ecs::types::EfsAuthorizationConfigIam::Enabled)
                                    .build(),
                            )
                            .transit_encryption(aws_sdk_ecs::types::EfsTransitEncryption::Enabled)
                            .build(),
                    )
                    .build(),
            );

        let container_def = ContainerDefinition::builder()
            .cpu(spec.cpus)
            .memory(spec.mem)
            .image(format!("{public_image_uri}:latest"))
            .stop_timeout(spec.timeout)
            .port_mappings(
                PortMapping::builder()
                    .host_port(37000)
                    .container_port(37000)
                    .build(),
            )
            .name(&service_def_name)
            .ulimits(
                aws_sdk_ecs::types::Ulimit::builder()
                    .name(aws_sdk_ecs::types::UlimitName::Nofile)
                    .soft_limit(10000)
                    .hard_limit(10000)
                    .build(),
            )
            .mount_points(
                aws_sdk_ecs::types::MountPoint::builder()
                    .container_path(crate::messaging_mnt_path())
                    .source_volume("obelisk_volume")
                    .build(),
            )
            .log_configuration(
                aws_sdk_ecs::types::LogConfiguration::builder()
                    .log_driver(aws_sdk_ecs::types::LogDriver::Awslogs)
                    .options("awslogs-group", format!("service-{service_def_name}"))
                    .options("awslogs-create-group", "true")
                    .options("awslogs-region", &self.region)
                    .options("awslogs-stream-prefix", service_def_name)
                    .build(),
            )
            .environment(
                KeyValuePair::builder()
                    .name("SUBSYSTEM")
                    .value("messaging")
                    .build(),
            )
            .environment(
                KeyValuePair::builder()
                    .name("NAMESPACE")
                    .value(&spec.namespace)
                    .build(),
            )
            .environment(
                KeyValuePair::builder()
                    .name("MEMORY")
                    .value(spec.mem.to_string())
                    .build(),
            )
            .environment(
                KeyValuePair::builder()
                    .name("EXECUTION_MODE")
                    .value("messaging_ecs")
                    .build(),
            )
            .environment(
                KeyValuePair::builder()
                    .name("TIMEOUT")
                    .value(spec.timeout.to_string())
                    .build(),
            )
            .build();
        let task_def = task_def.container_definitions(container_def);
        task_def.send().await.unwrap();
    }
}
