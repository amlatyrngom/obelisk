use super::{HandlerSpec, RescalerSpec};
use aws_sdk_lambda::types::{FunctionCode, PackageType};

pub struct LambdaDeployment {}

impl LambdaDeployment {
    /// Name of the rescaler function.
    pub fn rescaler_name(namespace: &str) -> String {
        format!("obk__rescaler__{namespace}")
    }

    /// Handler function name.
    pub fn handler_function_name(namespace: &str, identifier: &str) -> String {
        format!("obk__{namespace}__{identifier}")
    }

    /// Create rescaler lambda.
    pub async fn create_rescaler_lambda(
        client: &aws_sdk_lambda::Client,
        spec: &RescalerSpec,
        private_image_uri: &str,
        role_arn: &str,
    ) {
        // Create scaler function.
        let scaler_name = Self::rescaler_name(&spec.namespace);
        let code = FunctionCode::builder()
            .image_uri(format!("{private_image_uri}:latest"))
            .build();
        let spec_str = serde_json::to_string(&spec).unwrap();
        let environ = aws_sdk_lambda::types::Environment::builder()
            .variables("OBK_RESCALER_SPEC", spec_str)
            .variables("OBK_EXECUTION_MODE", "rescaler_lambda")
            .variables("OBK_MEMORY", spec.mem.to_string());
        let environ = environ.build();
        let _ = client
            .delete_function()
            .function_name(&scaler_name)
            .send()
            .await;
        client
            .create_function()
            .function_name(&scaler_name)
            .code(code)
            .memory_size(spec.mem)
            .package_type(PackageType::Image)
            .timeout(spec.timeout)
            .role(role_arn)
            .environment(environ)
            .send()
            .await
            .unwrap();
    }

    /// Duplicate handler with a new identifier.
    pub async fn duplicate_handler_lambda(
        client: &aws_sdk_lambda::Client,
        namespace: &str,
        name: &str,
        identifier: usize,
        concurrency_limit: Option<i32>,
    ) {
        let identifier = format!("{name}{identifier}");
        let template_function_name = Self::handler_function_name(namespace, name);
        let function_name = Self::handler_function_name(namespace, &identifier);
        loop {
            let function = client
                .get_function()
                .function_name(&function_name)
                .send()
                .await;
            match function {
                Ok(function) => {
                    let st = function.configuration().unwrap().state().unwrap();
                    match st {
                        aws_sdk_lambda::types::State::Inactive => {
                            return;
                        }
                        aws_sdk_lambda::types::State::Active => {
                            return;
                        }
                        _ => {
                            println!("Waiting for function availability. {st:?}...");
                            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                            continue;
                        }
                    }
                }
                Err(x) => {
                    println!("{x:?}. Creating...");
                    let mut create_function =
                        client.create_function().function_name(&function_name);
                    println!("Template name: {template_function_name}");
                    println!("Lambda name: {function_name}");
                    let template = client
                        .get_function()
                        .function_name(&template_function_name)
                        .send()
                        .await
                        .unwrap();
                    let config = template.configuration().unwrap();
                    let mem_size = config.memory_size().unwrap();
                    let timeout = config.timeout().unwrap();
                    let ephemeral_storage = config.ephemeral_storage().cloned().unwrap_or(
                        aws_sdk_lambda::types::EphemeralStorage::builder()
                            .size(512)
                            .build(),
                    );
                    create_function = create_function.memory_size(mem_size).timeout(timeout);
                    create_function = create_function.ephemeral_storage(ephemeral_storage);
                    if let Some(efs_config) = config.file_system_configs() {
                        create_function = create_function
                            .file_system_configs(efs_config.first().unwrap().clone());
                    }
                    if let Some(vpc_config) = config.vpc_config() {
                        create_function = create_function.vpc_config(
                            aws_sdk_lambda::types::VpcConfig::builder()
                                .security_group_ids(
                                    vpc_config.security_group_ids().unwrap().first().unwrap(),
                                )
                                .subnet_ids(vpc_config.subnet_ids().unwrap().first().unwrap())
                                .build(),
                        );
                    }
                    let role = config.role().unwrap();
                    create_function = create_function.role(role);
                    let mut env_vars = aws_sdk_lambda::types::Environment::builder();
                    for (var_name, var_value) in config.environment().unwrap().variables().unwrap()
                    {
                        env_vars = env_vars.variables(var_name, var_value);
                    }
                    env_vars = env_vars.variables("OBK_IDENTIFIER", &identifier);
                    create_function = create_function.environment(env_vars.build());
                    create_function =
                        create_function.package_type(config.package_type().unwrap().clone());
                    let function_code = aws_sdk_lambda::types::FunctionCode::builder()
                        .image_uri(template.code().unwrap().image_uri().unwrap())
                        .build();
                    create_function = create_function.code(function_code);
                    let _ = create_function.send().await;
                    if let Some(concurrency_limit) = concurrency_limit {
                        let _ = client
                            .put_function_concurrency()
                            .function_name(&function_name)
                            .reserved_concurrent_executions(concurrency_limit)
                            .send()
                            .await;
                    }
                }
            }
        }
    }

    /// Create a lambda handler.
    pub async fn create_handler_lambda(
        client: &aws_sdk_lambda::Client,
        spec: &HandlerSpec,
        private_image_uri: &str,
        role_arn: &str,
        ap_arn: &str,
        subnet_ids: &[String],
        sg_id: &str,
    ) {
        let function_name = Self::handler_function_name(&spec.namespace, &spec.name);
        // Delete existiting functions. TODO: support more than 50.
        let mut marker: Option<String> = None;
        loop {
            let mut curr_fns = client.list_functions().max_items(50);
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
                    let _ = client
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
        let spec_str = serde_json::to_string(&spec).unwrap();
        let mut environ = aws_sdk_lambda::types::Environment::builder()
            .variables("OBK_IDENTIFIER", &spec.name)
            .variables("OBK_HANDLER_SPEC", spec_str)
            .variables("OBK_EXECUTION_MODE", "handler_lambda")
            .variables("OBK_MEMORY", spec.default_mem.to_string());
        if spec.persistent || spec.unique {
            environ = environ.variables("OBK_EXTERNAL_ACCESS", false.to_string());
        }
        let environ = environ.build();
        let _ = client
            .delete_function()
            .function_name(&function_name)
            .send()
            .await;
        println!("Creating messaging function: {function_name}");
        let mut create_function = client
            .create_function()
            .function_name(&function_name)
            .code(code)
            .memory_size(spec.default_mem)
            .package_type(PackageType::Image)
            .timeout(spec.timeout)
            .ephemeral_storage(
                aws_sdk_lambda::types::EphemeralStorage::builder()
                    .size(spec.ephemeral)
                    .build(),
            )
            .role(role_arn)
            .environment(environ);
        if spec.persistent || spec.unique {
            let mut vpc_config =
                aws_sdk_lambda::types::VpcConfig::builder().security_group_ids(sg_id);
            for subnet_id in subnet_ids {
                vpc_config = vpc_config.subnet_ids(subnet_id);
            }
            let vpc_config = vpc_config.build();
            create_function = create_function
                .file_system_configs(
                    aws_sdk_lambda::types::FileSystemConfig::builder()
                        .arn(ap_arn)
                        .local_mount_path(crate::persistence_mnt_path())
                        .build(),
                )
                .vpc_config(vpc_config);
        }
        let resp = create_function.send().await.unwrap();
        println!("Resp: {resp:?}")
    }
}
