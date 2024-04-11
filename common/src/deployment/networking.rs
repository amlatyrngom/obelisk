#[derive(Debug, Clone)]
pub struct NetworkingDeployment {
    pub subnet_ids: Vec<String>,
    pub sg_id: String,
}

impl NetworkingDeployment {
    /// Create networking stuff.
    pub async fn new() -> Self {
        let (subnet_ids, sg_id) = Self::setup_subnet_with_endpoints(true).await;
        NetworkingDeployment { subnet_ids, sg_id }
    }

    /// Get default public subnet.
    pub async fn setup_subnet_with_endpoints(setup_endpoints: bool) -> (Vec<String>, String) {
        let shared_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let region = shared_config.region().unwrap().to_string();
        let ec2_client = aws_sdk_ec2::Client::new(&shared_config);

        let _ = ec2_client.create_default_vpc();
        let _ = ec2_client.create_default_subnet();
        // Get vpc.
        let vpc = ec2_client
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
        let vpc = vpc.vpcs().first().unwrap();
        let vpc_id = vpc.vpc_id().unwrap();
        // Get subnet ids.
        let subnet = ec2_client
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
        let subnets = subnet.subnets();
        let subnet_ids: Vec<String> = subnets
            .iter()
            .map(|s| s.subnet_id().unwrap().into())
            .collect();
        // Get sg ids.
        let sg = ec2_client
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
        let sg = sg.security_groups().first().unwrap();
        let sg_id: String = sg.group_id().unwrap().into();
        // Get routing table id.
        let rt = ec2_client
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
        let rt = rt.route_tables().first().unwrap();
        let rt_id: String = rt.route_table_id().unwrap().into();
        println!("Routing table ids: {rt_id}");
        // Create access endpoints.
        if setup_endpoints {
            let svc_names = vec![
                // (format!("com.amazonaws.{}.sqs", region), false),
                // (format!("com.amazonaws.{}.lambda", region), false),
                (format!("com.amazonaws.{}.s3", region), true),
                (format!("com.amazonaws.{}.dynamodb", region), true),
            ];
            for (svc_name, is_gateway) in svc_names {
                let resp = ec2_client
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
                if let Some(resp) = resp.vpc_endpoints().first() {
                    println!("Found endpoint: {resp:?}");
                    continue;
                }
                let mut req = ec2_client
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

    pub async fn teardown_vpc_endpoints() {
        let shared_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let region = shared_config.region().unwrap().to_string();
        let ec2_client = aws_sdk_ec2::Client::new(&shared_config);
        // Get vpc.
        let vpc = ec2_client
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
        let vpc = vpc.vpcs().first().unwrap();
        let vpc_id = vpc.vpc_id().unwrap();
        let svc_names = vec![
            (format!("com.amazonaws.{}.s3", region), true),
            (format!("com.amazonaws.{}.dynamodb", region), true),
        ];
        let mut delete_req = ec2_client.delete_vpc_endpoints();
        let mut found = false;
        for (svc_name, _) in svc_names {
            let resp = ec2_client
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
            if let Some(resp) = resp.vpc_endpoints().first() {
                found = true;
                let endpoint_id = resp.vpc_endpoint_id().unwrap();
                delete_req = delete_req.vpc_endpoint_ids(endpoint_id);
            }
        }
        if found {
            let _ = delete_req.send().await.unwrap();
        }
    }
}
