pub mod aws;
mod codegen;

const BUILD_DIR: &str = ".obelisk_build";

use common::{
    deployment::{self, HandlerSpec, NamespaceSpec, ServiceSpec, SubsystemSpec},
    RescalerSpec,
};
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
    ephemeral: Option<i32>,
    caller_mem: Option<i32>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Messaging {
    path: String,
    mem_size: i32,
    fn_mem: Option<i32>,
    caller_mem: Option<i32>,
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

/// Service Rescaler.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct Rescaler {
    path: String,
    mem: i32,
    timeout: i32,
}

/// Service Rescaler.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct Service {
    name: String,
    path: String,
    mem: i32,
    cpus: i32,
    timeout: i32,
    unique: Option<bool>,
}

/// VISC handler.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct Handler {
    subsystem: Option<String>,
    name: String,
    path: String,
    mem: i32,
    timeout: i32,
    persistent: Option<bool>,
    concurrency: Option<i32>,
    caller: Option<i32>,
    unique: Option<bool>,
    ephemeral: Option<i32>,
    scaleup: Option<f64>,
    spot: Option<bool>,
}

/// A VISC subsystem.
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

/// A VISC subsystem.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct Subsystem1 {
    rescaler: Rescaler,
    services: Vec<Service>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Deployment1 {
    /// Everything within a namespace has the same filesystem, s3 bucket, ecs cluster, etc.
    namespace: String,
    /// Dependencies.
    dependencies: Option<Vec<String>>,
    /// A subsystem allows implementing VISC for an API.
    subsystem: Option<Subsystem1>,
    /// A subsystem allows implementing VISC
    handlers: Option<Vec<Handler>>,
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

fn make_specs(
    deployments: &[Deployment1],
    public_uri: &str,
    private_uri: &str,
) -> Vec<NamespaceSpec> {
    deployments
        .iter()
        .map(|deployment| {
            let subsystem_spec = deployment.subsystem.as_ref().map(|s| {
                let rescaler_spec = RescalerSpec {
                    namespace: deployment.namespace.clone(),
                    timeout: s.rescaler.timeout,
                    mem: s.rescaler.mem,
                };
                let service_specs = s
                    .services
                    .iter()
                    .map(|s| {
                        (
                            s.name.clone(),
                            ServiceSpec {
                                namespace: deployment.namespace.clone(),
                                name: s.name.clone(),
                                timeout: s.timeout,
                                mem: s.mem,
                                cpus: s.cpus,
                                unique: s.unique.unwrap_or(false),
                            },
                        )
                    })
                    .collect();
                SubsystemSpec {
                    rescaler_spec,
                    service_specs,
                }
            });
            let handler_specs = deployment
                .handlers
                .clone()
                .unwrap_or(vec![])
                .iter()
                .map(|h| {
                    (
                        h.name.clone(),
                        HandlerSpec {
                            subsystem: h.subsystem.clone().unwrap_or("functional".into()),
                            namespace: deployment.namespace.clone(),
                            name: h.name.clone(),
                            timeout: h.timeout,
                            default_mem: h.mem,
                            concurrency: h.concurrency.unwrap_or(1),
                            ephemeral: h.ephemeral.unwrap_or(512),
                            persistent: h.persistent.unwrap_or(false),
                            unique: h.unique.unwrap_or(false),
                            scaleup: h.scaleup.unwrap_or(0.0),
                            spot: h.spot.unwrap_or(true),
                        },
                    )
                })
                .collect();
            NamespaceSpec {
                namespace: deployment.namespace.clone(),
                public_img_url: public_uri.into(),
                private_img_url: private_uri.into(),
                dependencies: deployment.dependencies.clone().unwrap_or(vec![]),
                subsystem_spec,
                handler_specs,
            }
        })
        .collect()
}

pub async fn build_system(deployments: &[String]) {
    let deployments: Vec<Deployment1> = deployments
        .iter()
        .map(|s| toml::from_str(s).unwrap())
        .collect();
    println!("Deployments: {deployments:?}");
    // Gen cargo file.
    let system_cargo = include_str!("SystemCargo.toml");
    let system_cargo: CargoConfig = toml::from_str(system_cargo).unwrap();
    codegen::gen_cargo(system_cargo);
    // Gen main file.
    codegen::gen_main(&deployments, true);
    // Build image.
    let aws = aws::AWS::new().await;
    build_image(true);
    let (private_uri, public_uri) = aws.create_repos("system").await;
    println!("URLS: {private_uri}; {public_uri}");
    push_images(&private_uri, &public_uri, true);
    // Make specs.
    let namespace_specs = make_specs(&deployments, &public_uri, &private_uri);
    let deployer = deployment::Deployment::new().await;
    for spec in &namespace_specs {
        deployer.setup_namespace(spec).await.unwrap();
    }
    for spec in &namespace_specs {
        deployer.redeploy_subsystems(spec).await;
    }
}

// pub async fn teardown_deployment() {
//     let aws = aws::AWS::new().await;
//     aws.deployer.teardown_vpc_endpoints().await;
// }

pub async fn build_user_deployment(project_name: &str, deployments: &[String]) {
    // Gen main file.
    let deployments: Vec<Deployment1> = deployments
        .iter()
        .map(|s| toml::from_str(s).unwrap())
        .collect();
    codegen::gen_main(&deployments, false);
    // Gen cargo file.
    let user_cargo = std::fs::read_to_string("Cargo.toml").unwrap();
    let user_cargo: CargoConfig = toml::from_str(&user_cargo).unwrap();
    codegen::gen_cargo(user_cargo);
    // Build image.
    let aws = aws::AWS::new().await;
    build_image(false);
    let (private_uri, public_uri) = aws.create_repos(project_name).await;
    println!("URLS: {private_uri}; {public_uri}");
    push_images(&private_uri, &public_uri, false);
    // Make specs.
    let namespace_specs = make_specs(&deployments, &public_uri, &private_uri);
    let deployer = deployment::Deployment::new().await;
    for spec in &namespace_specs {
        deployer.setup_namespace(spec).await.unwrap();
    }
    for spec in &namespace_specs {
        deployer.redeploy_subsystems(spec).await;
    }
}
