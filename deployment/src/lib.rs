pub mod aws;
mod codegen;

const BUILD_DIR: &str = ".obelisk_build";

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Deserialize, Debug, Serialize)]
pub(crate) struct CargoConfig {
    workspace: Option<toml::Value>,
    dependencies: toml::Value,
    bin: Option<Vec<Binary>>,
    package: toml::Value,
}

#[derive(Deserialize, Debug, Serialize)]
pub(crate) struct Binary {
    name: String,
    path: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Deployment {
    namespace: String,
    functional: Option<Functional>,
    messaging: Option<Messaging>,
    subsystem: Option<Subsystem>,
}

/// Like deployment, but cannot contain services.
#[derive(Serialize, Deserialize, Clone, Debug)]
struct UserDeployment {
    namespace: String,
    // functions: Vec<Function>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Functional {
    path: String,
    mem_size: i32,
    timeout: i32,
    concurrency: i32,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Messaging {
    path: String,
    mem_size: i32,
    timeout: i32,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DeployedNamespace {
    pub revision: String,
    pub fns: HashMap<String, DeployedFunction>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DeployedFunction {
    pub lambda_mem: i32,
    pub ecs_cpu: i32,
    pub ecs_mem: i32,
    pub concurrency: i32,
    pub timeout: i32,
    pub workers: i32,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct Subsystem {
    service_path: String,
    rescaler_path: String,
    service_mem: i32,
    service_cpus: i32,
    service_grace: i32,
    rescaler_mem: i32,
    rescaler_timeout: i32,
}

fn exec_cmd(program: &str, args: Vec<&str>) {
    let mut o = std::process::Command::new(program)
        .args(&args)
        .spawn()
        .unwrap();
    let exit_status = o.wait().unwrap();
    assert!(exit_status.success());
}

fn build_image(for_system: bool) {
    let dockerfile = std::include_str!("Dockerfile");
    let name = if for_system {
        "obelisk_system"
    } else {
        "obelisk_user"
    };
    let file_path = format!("{BUILD_DIR}/Dockerfile.{name}");
    std::fs::write(&file_path, dockerfile).unwrap();

    exec_cmd("docker", vec!["build", "-t", name, "-f", &file_path, "."]);
}

fn pull_system_img(system_uri: &str) {
    let name = "obelisk_system";
    exec_cmd("docker", vec!["pull", system_uri]);
    exec_cmd("docker", vec!["tag", system_uri, name]);
}

fn push_images(private_uri: &str, public_uri: &str, for_system: bool) {
    let name = if for_system {
        "obelisk_system"
    } else {
        "obelisk_user"
    };
    exec_cmd("docker", vec!["tag", name, private_uri]);
    exec_cmd("docker", vec!["push", private_uri]);
    exec_cmd("docker", vec!["tag", name, public_uri]);
    exec_cmd("docker", vec!["push", public_uri]);
}

pub async fn build_system(deployments: &[String]) {
    // Gen main file.
    let deployments: Vec<Deployment> = deployments
        .iter()
        .map(|s| toml::from_str(s).unwrap())
        .collect();
    codegen::gen_main(&deployments, true);
    // Gen cargo file.
    let system_cargo = include_str!("SystemCargo.toml");
    let system_cargo: CargoConfig = toml::from_str(system_cargo).unwrap();
    codegen::gen_cargo(system_cargo);
    // Create repos and push image.
    let aws = aws::AWS::new().await;
    aws.deployer.create_cluster().await;
    aws.deployer.create_bucket().await;
    aws.deployer.get_subnet_with_endpoints(true).await;
    // Build image.
    build_image(true);
    let (private_uri, public_uri) = aws.create_repos("system").await;
    push_images(&private_uri, &public_uri, true);
    for deployment in deployments {
        aws.deploy(
            &public_uri,
            &private_uri,
            &private_uri,
            &public_uri,
            &deployment,
        )
        .await;
    }
}

pub async fn teardown_deployment() {
    let aws = aws::AWS::new().await;
    aws.deployer.teardown_vpc_endpoints().await;
}

pub async fn build_user_deployment(project_name: &str, system_img: &str, deployments: &[String]) {
    // Gen main file.
    let deployments: Vec<Deployment> = deployments
        .iter()
        .map(|s| toml::from_str(s).unwrap())
        .collect();
    codegen::gen_main(&deployments, false);
    // Gen cargo file.
    let user_cargo = std::fs::read_to_string("Cargo.toml").unwrap();
    let user_cargo: CargoConfig = toml::from_str(&user_cargo).unwrap();
    codegen::gen_cargo(user_cargo);
    // Create repos and push image.
    let aws = aws::AWS::new().await;
    aws.deployer.create_cluster().await;
    aws.deployer.create_bucket().await;
    aws.deployer.get_subnet_with_endpoints(true).await;
    // Make system repos.
    pull_system_img(system_img);
    let (system_private_uri, system_public_uri) = aws.create_repos("system").await;
    push_images(&system_private_uri, &system_public_uri, true);
    // Make user repos.
    build_image(false);
    let (private_uri, public_uri) = aws.create_repos(project_name).await;
    push_images(&private_uri, &public_uri, false);
    // Do deployment.
    for deployment in deployments {
        aws.deploy(
            &system_public_uri,
            &system_private_uri,
            &private_uri,
            &public_uri,
            &deployment,
        )
        .await;
    }
}
