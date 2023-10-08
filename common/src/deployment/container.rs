use super::{HandlerSpec, ServiceSpec};
use crate::debug_format;
use aws_sdk_ecs::types::KeyValuePair;

pub struct ContainerDeployment {}

impl ContainerDeployment {
    /// Name of cluster for namespace.
    pub fn cluster_name(namespace: &str) -> String {
        format!("obk__{namespace}")
    }

    /// Name of task def for a service.
    pub fn service_task_def_name(subsystem: &str, name: &str, namespace: Option<String>) -> String {
        if let Some(namespace) = namespace {
            format!("obk__{subsystem}__{name}__{namespace}")
        } else {
            format!("obk__{subsystem}__{name}")
        }
    }

    /// Name of task for a service.
    pub fn service_task_name(
        subsystem: &str,
        name: &str,
        target_namespace: &str,
        identifier: &str,
    ) -> String {
        format!("obk__{subsystem}__{name}__{target_namespace}__{identifier}")
    }

    /// Parse target namespace and identifier from
    pub fn parse_service_task_name(s: &str) -> (String, String) {
        let elems = s.split("__").collect::<Vec<_>>();
        println!("Parsing Service Task Name: {elems:?}");
        let target_namespace = elems[3].to_string();
        let identifier = elems[4].to_string();
        (target_namespace, identifier)
    }

    /// Name of task for a handler.
    pub fn handler_task_name(namespace: &str, identifier: &str, mem: &str) -> String {
        format!("obk__{namespace}__{identifier}__{mem}")
    }

    /// Parse identifier from handler task name.
    pub fn parse_handler_task_name(s: &str) -> String {
        let mut elems = s.split("__");
        elems.nth(2).unwrap().to_string()
    }

    /// Name of task def for a handler.
    pub fn handler_task_def_name(namespace: &str, name: &str, mem: &str) -> String {
        format!("obk__{namespace}__{name}__{mem}")
    }

    /// Create cluster for a given namespace.
    pub async fn create_cluster(
        client: &aws_sdk_ecs::Client,
        namespace: &str,
    ) -> Result<(), String> {
        let _resp = client
            .create_cluster()
            .cluster_name(&Self::cluster_name(namespace))
            .send()
            .await
            .map_err(|e| format!("{e:?}"))?;
        Ok(())
    }

    /// Register service task definition.
    pub async fn register_service_task_definition(
        client: &aws_sdk_ecs::Client,
        spec: &ServiceSpec,
        public_image_uri: &str,
        role_arn: &str,
        fs_id: &str,
        ap_id: &str,
        target_namespace: Option<String>,
    ) {
        let task_def_name =
            Self::service_task_def_name(&spec.namespace, &spec.name, target_namespace.clone());
        let region = client.conf().region().unwrap().to_string();
        // Create new definition.
        let task_def = client
            .register_task_definition()
            .task_role_arn(role_arn)
            .execution_role_arn(role_arn)
            .cpu(format!("{}", spec.cpus))
            .memory(format!("{}", spec.mem))
            .requires_compatibilities(aws_sdk_ecs::types::Compatibility::Fargate)
            .network_mode(aws_sdk_ecs::types::NetworkMode::Awsvpc)
            .family(&task_def_name)
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
        let spec_str = serde_json::to_string(&spec).unwrap();
        let container_def = aws_sdk_ecs::types::ContainerDefinition::builder()
            .cpu(spec.cpus)
            .memory(spec.mem)
            .image(format!("{public_image_uri}:latest"))
            .stop_timeout(spec.timeout)
            .port_mappings(
                aws_sdk_ecs::types::PortMapping::builder()
                    .host_port(37000)
                    .container_port(37000)
                    .build(),
            )
            .name(&task_def_name)
            .ulimits(
                aws_sdk_ecs::types::Ulimit::builder()
                    .name(aws_sdk_ecs::types::UlimitName::Nofile)
                    .soft_limit(10000)
                    .hard_limit(10000)
                    .build(),
            )
            .mount_points(
                aws_sdk_ecs::types::MountPoint::builder()
                    .container_path(crate::persistence_mnt_path())
                    .source_volume("obelisk_volume")
                    .build(),
            )
            .log_configuration(
                aws_sdk_ecs::types::LogConfiguration::builder()
                    .log_driver(aws_sdk_ecs::types::LogDriver::Awslogs)
                    .options("awslogs-group", format!("service-{task_def_name}"))
                    .options("awslogs-create-group", "true")
                    .options("awslogs-region", &region)
                    .options("awslogs-stream-prefix", task_def_name)
                    .build(),
            )
            .environment(
                KeyValuePair::builder()
                    .name("OBK_SERVICE_SPEC")
                    .value(spec_str)
                    .build(),
            )
            .environment(
                KeyValuePair::builder()
                    .name("OBK_MEMORY")
                    .value(spec.mem.to_string())
                    .build(),
            )
            .environment(
                KeyValuePair::builder()
                    .name("OBK_CPUS")
                    .value(spec.cpus.to_string())
                    .build(),
            )
            .environment(
                KeyValuePair::builder()
                    .name("OBK_EXECUTION_MODE")
                    .value("service_ecs")
                    .build(),
            )
            .environment(
                KeyValuePair::builder()
                    .name("OBK_PUBLIC")
                    .value(true.to_string())
                    .build(),
            )
            .build();
        let task_def = task_def.container_definitions(container_def);
        loop {
            let to_send = task_def.clone();
            let resp = to_send.send().await;
            match resp {
                Ok(_) => return,
                Err(e) => {
                    let e = format!("{e:?}");
                    if e.contains("ThrottlingException") {
                        println!("Retrying due to request throtlling!");
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                        continue;
                    } else {
                        panic!("{e:?}");
                    }
                }
            }
        }
    }

    /// Register handler task definition.
    pub async fn register_handler_task_definition(
        client: &aws_sdk_ecs::Client,
        spec: &HandlerSpec,
        public_image_uri: &str,
        role_arn: &str,
        fs_id: &str,
        ap_id: &str,
        mem: i32,
        cpu: i32,
    ) {
        let mem_str = Self::mem_mb_to_str(mem);
        let task_def_name = Self::handler_task_def_name(&spec.namespace, &spec.name, &mem_str);
        let region = client.conf().region().unwrap().to_string();
        // Create new definition.
        let mut task_def = client
            .register_task_definition()
            .task_role_arn(role_arn)
            .execution_role_arn(role_arn)
            .cpu(format!("{}", cpu))
            .memory(format!("{}", mem_str))
            .requires_compatibilities(aws_sdk_ecs::types::Compatibility::Fargate)
            .network_mode(aws_sdk_ecs::types::NetworkMode::Awsvpc)
            .family(&task_def_name);
        if spec.persistent || spec.unique {
            task_def = task_def.volumes(
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
        }

        let spec_str = serde_json::to_string(&spec).unwrap();
        let mut container_def = aws_sdk_ecs::types::ContainerDefinition::builder()
            .cpu(cpu)
            .memory(mem)
            .image(format!("{public_image_uri}:latest"))
            .stop_timeout(spec.timeout)
            .port_mappings(
                aws_sdk_ecs::types::PortMapping::builder()
                    .host_port(37000)
                    .container_port(37000)
                    .build(),
            )
            .name(&task_def_name)
            .ulimits(
                aws_sdk_ecs::types::Ulimit::builder()
                    .name(aws_sdk_ecs::types::UlimitName::Nofile)
                    .soft_limit(10000)
                    .hard_limit(10000)
                    .build(),
            )
            .log_configuration(
                aws_sdk_ecs::types::LogConfiguration::builder()
                    .log_driver(aws_sdk_ecs::types::LogDriver::Awslogs)
                    .options("awslogs-group", format!("service-{task_def_name}"))
                    .options("awslogs-create-group", "true")
                    .options("awslogs-region", &region)
                    .options("awslogs-stream-prefix", task_def_name)
                    .build(),
            )
            .environment(
                KeyValuePair::builder()
                    .name("OBK_HANDLER_SPEC")
                    .value(spec_str)
                    .build(),
            )
            .environment(
                KeyValuePair::builder()
                    .name("OBK_MEMORY")
                    .value(mem.to_string())
                    .build(),
            )
            .environment(
                KeyValuePair::builder()
                    .name("OBK_CPUS")
                    .value(cpu.to_string())
                    .build(),
            )
            .environment(
                KeyValuePair::builder()
                    .name("OBK_EXECUTION_MODE")
                    .value("handler_ecs")
                    .build(),
            )
            .environment(
                KeyValuePair::builder()
                    .name("OBK_PUBLIC")
                    .value(true.to_string())
                    .build(),
            );
        if spec.persistent || spec.unique {
            container_def = container_def.mount_points(
                aws_sdk_ecs::types::MountPoint::builder()
                    .container_path(crate::persistence_mnt_path())
                    .source_volume("obelisk_volume")
                    .build(),
            )
        }
        let container_def = container_def.build();
        let task_def = task_def.container_definitions(container_def);
        loop {
            let to_send = task_def.clone();
            let resp = to_send.send().await;
            match resp {
                Ok(_) => return,
                Err(e) => {
                    let e = format!("{e:?}");
                    if e.contains("ThrottlingException") {
                        println!("Retrying due to request throtlling!");
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                        continue;
                    } else {
                        panic!("{e:?}");
                    }
                }
            }
        }
    }

    pub fn all_avail_mems(mem: i32, scaleup: f64) -> Vec<i32> {
        // Container mem based on lambda.
        let vcpus = (mem as f64) / 1769.0;
        let vcpus = if vcpus <= 0.25 {
            0.25
        } else if vcpus <= 0.5 {
            0.5
        } else if vcpus <= 1.0 {
            1.0
        } else if vcpus <= 2.0 {
            2.0
        } else if vcpus <= 4.0 {
            4.0
        } else {
            8.0
        };
        let mem = (vcpus * 2.0 * 1024.0) as i32;
        if scaleup <= 1e-4 {
            return vec![mem];
        }
        let mut res = Vec::<i32>::new();
        // if mem <= 64 * 1024 {
        //     res.push(64 * 1024);
        // }
        if mem <= 32 * 1024 {
            res.push(32 * 1024);
        }
        if mem <= 16 * 1024 {
            res.push(16 * 1024);
        }
        if mem <= 8 * 1024 {
            res.push(8 * 1024);
        }
        if mem <= 4 * 1024 {
            res.push(4 * 1024);
        }
        if mem <= 2 * 1024 {
            res.push(2 * 1024);
        }
        if mem <= 1024 {
            res.push(1024);
        }
        if mem <= 512 {
            res.push(512);
        }
        // Only keep up to 4x
        let res = res.into_iter().filter(|m| *m <= 4 * mem).collect();
        res
    }

    /// Convert mem amount to string.
    pub fn mem_mb_to_str(mem: i32) -> String {
        // For the optimized mems, cpus is 1/4 the memory.
        let str = if mem == 64 * 1024 {
            "64GB"
        } else if mem == 32 * 1024 {
            "32GB"
        } else if mem == 16 * 1024 {
            "16GB"
        } else if mem == 8 * 1024 {
            "8GB"
        } else if mem == 4 * 1024 {
            "4GB"
        } else if mem == 2 * 1024 {
            "2GB"
        } else if mem == 1024 {
            "1GB"
        } else {
            "512"
        };
        str.into()
    }

    /// Register handler task definition.
    pub async fn register_handler_task_definitions(
        client: &aws_sdk_ecs::Client,
        handler_spec: &HandlerSpec,
        public_img_url: &str,
        role_arn: &str,
        fs_id: &str,
        ap_id: &str,
    ) {
        let avail_mems = Self::all_avail_mems(handler_spec.default_mem, handler_spec.scaleup);
        println!("Avail Mems: {avail_mems:?}.");
        let mut mems = Vec::new();
        let mut cpus = Vec::new();
        for mem in avail_mems {
            mems.push(mem);
            cpus.push(mem / 2);
        }

        for (mem, cpu) in mems.into_iter().zip(cpus.into_iter()) {
            Self::register_handler_task_definition(
                client,
                handler_spec,
                public_img_url,
                role_arn,
                fs_id,
                ap_id,
                mem,
                cpu,
            )
            .await;
        }
    }

    /// Create service task.
    pub async fn create_service_task(
        client: &aws_sdk_ecs::Client,
        spec: &ServiceSpec,
        target_namespace: &str,
        identifier: &str,
        subnet_ids: &[String],
        sg_id: &str,
    ) {
        let task_name =
            Self::service_task_name(&spec.namespace, &spec.name, target_namespace, identifier);
        let task_def_name =
            Self::service_task_def_name(&spec.namespace, &spec.name, Some(target_namespace.into()));
        let mut vpc_config = aws_sdk_ecs::types::AwsVpcConfiguration::builder()
            .assign_public_ip(aws_sdk_ecs::types::AssignPublicIp::Enabled)
            .security_groups(sg_id);
        for subnet_id in subnet_ids {
            vpc_config = vpc_config.subnets(subnet_id);
        }
        let vpc_config = vpc_config.build();
        let identifier_tag = aws_sdk_ecs::types::Tag::builder()
            .key("OBK_IDENTIFIER")
            .value(identifier)
            .build();
        let ns_tag = aws_sdk_ecs::types::Tag::builder()
            .key("OBK_TARGET_NAMESPACE")
            .value(target_namespace)
            .build();
        let service_def = client
            .create_service()
            .cluster(Self::cluster_name(target_namespace))
            .service_name(&task_name)
            .task_definition(&task_def_name)
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
            )
            .tags(identifier_tag)
            .tags(ns_tag)
            .propagate_tags(aws_sdk_ecs::types::PropagateTags::Service);
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
            client
                .update_service()
                .cluster(Self::cluster_name(target_namespace))
                .service(&task_name)
                .task_definition(&task_def_name)
                .force_new_deployment(true)
                .desired_count(0)
                .network_configuration(
                    aws_sdk_ecs::types::NetworkConfiguration::builder()
                        .awsvpc_configuration(vpc_config.clone())
                        .build(),
                )
                .propagate_tags(aws_sdk_ecs::types::PropagateTags::Service)
                .send()
                .await
                .unwrap();
        }
    }

    /// Scale a service task.
    pub async fn scale_service_task(
        client: &aws_sdk_ecs::Client,
        spec: &ServiceSpec,
        target_namespace: &str,
        identifier: &str,
        count: i32,
    ) -> Result<(), String> {
        let task_name =
            Self::service_task_name(&spec.namespace, &spec.name, target_namespace, identifier);
        let _resp = client
            .update_service()
            .desired_count(count)
            .cluster(Self::cluster_name(target_namespace))
            .service(task_name)
            .propagate_tags(aws_sdk_ecs::types::PropagateTags::Service)
            .send()
            .await
            .map_err(debug_format!())?;
        Ok(())
    }

    /// Create handler task.
    pub async fn create_handler_task(
        client: &aws_sdk_ecs::Client,
        spec: &HandlerSpec,
        identifier: &str,
        subnet_ids: &[String],
        sg_id: &str,
        mem: i32,
    ) {
        let provider = if spec.spot { "FARGATE_SPOT" } else { "FARGATE" };
        let task_name =
            Self::handler_task_name(&spec.namespace, identifier, &Self::mem_mb_to_str(mem));
        let task_def_name =
            Self::handler_task_def_name(&spec.namespace, &spec.name, &Self::mem_mb_to_str(mem));
        let mut vpc_config = aws_sdk_ecs::types::AwsVpcConfiguration::builder()
            .assign_public_ip(aws_sdk_ecs::types::AssignPublicIp::Enabled)
            .security_groups(sg_id);
        for subnet_id in subnet_ids {
            vpc_config = vpc_config.subnets(subnet_id);
        }
        let vpc_config = vpc_config.build();
        let identifier_tag = aws_sdk_ecs::types::Tag::builder()
            .key("OBK_IDENTIFIER")
            .value(identifier)
            .build();
        let service_def = client
            .create_service()
            .cluster(Self::cluster_name(&spec.namespace))
            .service_name(&task_name)
            .task_definition(&task_def_name)
            .desired_count(0)
            .network_configuration(
                aws_sdk_ecs::types::NetworkConfiguration::builder()
                    .awsvpc_configuration(vpc_config.clone())
                    .build(),
            )
            .capacity_provider_strategy(
                aws_sdk_ecs::types::CapacityProviderStrategyItem::builder()
                    .capacity_provider(provider)
                    .weight(1)
                    .base(0)
                    .build(),
            )
            .tags(identifier_tag)
            .propagate_tags(aws_sdk_ecs::types::PropagateTags::Service);
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
            client
                .update_service()
                .cluster(Self::cluster_name(&spec.namespace))
                .service(&task_name)
                .task_definition(&task_def_name)
                .force_new_deployment(true)
                .desired_count(0)
                .network_configuration(
                    aws_sdk_ecs::types::NetworkConfiguration::builder()
                        .awsvpc_configuration(vpc_config.clone())
                        .build(),
                )
                .capacity_provider_strategy(
                    aws_sdk_ecs::types::CapacityProviderStrategyItem::builder()
                        .capacity_provider(provider)
                        .weight(1)
                        .base(0)
                        .build(),
                )
                .propagate_tags(aws_sdk_ecs::types::PropagateTags::Service)
                .send()
                .await
                .unwrap();
        }
    }

    /// Scale a handler task.
    pub async fn scale_handler_task(
        client: &aws_sdk_ecs::Client,
        spec: &HandlerSpec,
        identifier: &str,
        mem: i32,
        count: i32,
    ) -> Result<(), String> {
        let task_name =
            Self::handler_task_name(&spec.namespace, identifier, &Self::mem_mb_to_str(mem));
        let _resp = client
            .update_service()
            .desired_count(count)
            .cluster(Self::cluster_name(&spec.namespace))
            .service(task_name)
            .propagate_tags(aws_sdk_ecs::types::PropagateTags::Service)
            .send()
            .await
            .map_err(|e| format!("{e:?}"))?;
        Ok(())
    }
}
