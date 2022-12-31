use common::adaptation::deploy::{
    AdapterDeployment, DeploymentInfo, FunctionalSpec, MessagingDeploymentInfo, MessagingSpec,
    SubsystemDeploymentInfo, SubsystemSpec,
};

use super::Deployment;

const LAMBDA_ROLE: &str = "OBELISKLAMBDAROLE";
const ECS_ROLE: &str = "OBELISKECSROLE";
const POLICY: &str = "OBELISKPOLICY";
const IAM_PREFIX: &str = "/obelisk/";

#[derive(Clone)]
pub struct AWS {
    ecr_client: aws_sdk_ecr::Client,
    ecr_public_client: aws_sdk_ecrpublic::Client,
    iam_client: aws_sdk_iam::Client,
    pub deployer: AdapterDeployment,
}

fn full_image_name(namespace: &str) -> String {
    format!("obk-img-{namespace}")
}

impl AWS {
    /// Create.
    pub async fn new() -> Self {
        let shared_config = aws_config::load_from_env().await;
        let public_ecr_config = aws_sdk_ecrpublic::config::Builder::from(&shared_config)
            .region(aws_types::region::Region::new("us-east-1"))
            .build();

        let aws = AWS {
            ecr_client: aws_sdk_ecr::Client::new(&shared_config),
            ecr_public_client: aws_sdk_ecrpublic::Client::from_conf(public_ecr_config),
            iam_client: aws_sdk_iam::Client::new(&shared_config),
            deployer: AdapterDeployment::new().await,
        };
        aws
    }

    /// Get lambda role name and arn.
    async fn get_or_make_lambda_role(&self) -> (String, String) {
        let trust = include_str!("lambda_trust_policy.json");
        let roles = self
            .iam_client
            .list_roles()
            .path_prefix(IAM_PREFIX)
            .send()
            .await
            .unwrap();
        let mut role = if let Some(roles) = roles.roles() {
            let mut role = None;
            for r in roles.iter() {
                if r.role_name().unwrap() == LAMBDA_ROLE {
                    role = Some(r.clone());
                    break;
                }
            }
            role
        } else {
            None
        };
        if role.is_none() {
            println!("Making lambda role...");
            role = Some(
                self.iam_client
                    .create_role()
                    .role_name(LAMBDA_ROLE)
                    .path(IAM_PREFIX)
                    .assume_role_policy_document(trust)
                    .send()
                    .await
                    .unwrap()
                    .role()
                    .unwrap()
                    .clone(),
            );
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        }
        let role = role.unwrap();
        (role.role_name().unwrap().into(), role.arn().unwrap().into())
    }

    /// Get ecs role name and arn.
    async fn get_or_make_ecs_role(&self) -> (String, String) {
        let trust = include_str!("ecs_trust_policy.json");
        let roles = self
            .iam_client
            .list_roles()
            .path_prefix(IAM_PREFIX)
            .send()
            .await
            .unwrap();
        let mut role = if let Some(roles) = roles.roles() {
            let mut role = None;
            for r in roles.iter() {
                if r.role_name().unwrap() == ECS_ROLE {
                    role = Some(r.clone());
                    break;
                }
            }
            role
        } else {
            None
        };
        if role.is_none() {
            println!("Making ECS role...");
            role = Some(
                self.iam_client
                    .create_role()
                    .role_name(ECS_ROLE)
                    .path(IAM_PREFIX)
                    .assume_role_policy_document(trust)
                    .send()
                    .await
                    .unwrap()
                    .role()
                    .unwrap()
                    .clone(),
            );
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        }
        let role = role.unwrap();
        (role.role_name().unwrap().into(), role.arn().unwrap().into())
    }

    /// Get access policy.
    async fn get_or_make_policy(&self) -> (String, String) {
        let policy_document = include_str!("full_access_policy.json");
        let policies = self
            .iam_client
            .list_policies()
            .path_prefix(IAM_PREFIX)
            .send()
            .await
            .unwrap();
        let mut policy = if let Some(policies) = policies.policies() {
            let mut policy = None;
            for p in policies.iter() {
                if p.policy_name().unwrap() == POLICY {
                    policy = Some(p.clone());
                    break;
                }
            }
            policy
        } else {
            None
        };
        if policy.is_none() {
            println!("Making IAM policy...");
            policy = Some(
                self.iam_client
                    .create_policy()
                    .policy_name(POLICY)
                    .path(IAM_PREFIX)
                    .policy_document(policy_document)
                    .send()
                    .await
                    .unwrap()
                    .policy()
                    .unwrap()
                    .clone(),
            );
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        }
        let policy = policy.unwrap();
        (
            policy.policy_name().unwrap().into(),
            policy.arn().unwrap().into(),
        )
    }

    /// Get lambda and ecs roles.
    pub async fn get_role(&self) -> (String, String) {
        let (_, policy_arn) = self.get_or_make_policy().await;
        let (lambda_role_name, lambda_role_arn) = self.get_or_make_lambda_role().await;
        let (ecs_role_name, ecs_role_arn) = self.get_or_make_ecs_role().await;
        let _ = self
            .iam_client
            .attach_role_policy()
            .role_name(&lambda_role_name)
            .policy_arn(&policy_arn)
            .send()
            .await
            .unwrap();
        let _ = self
            .iam_client
            .attach_role_policy()
            .role_name(&ecs_role_name)
            .policy_arn(&policy_arn)
            .send()
            .await
            .unwrap();
        (lambda_role_arn, ecs_role_arn)
    }

    /// Make a deployment.
    pub(crate) async fn deploy(
        &self,
        system_public_image_uri: &str,
        system_private_image_uri: &str,
        private_image_uri: &str,
        public_image_uri: &str,
        deployment: &Deployment,
    ) {
        // Create roles for security bullshit.
        let (lambda_role_arn, ecs_role_arn) = self.get_role().await;
        let mut deployment_info = DeploymentInfo {
            subsys_info: None,
            fn_info: None,
            msg_info: None,
        };
        // Create scaling subsystem.
        if let Some(subsystem) = &deployment.subsystem {
            let subsystem_spec = SubsystemSpec {
                subsystem: deployment.namespace.clone(),
                svc_cpus: subsystem.service_cpus,
                svc_mem: subsystem.service_mem,
                svc_grace: subsystem.service_grace,
                scl_mem: subsystem.rescaler_mem,
                scl_timeout: subsystem.rescaler_timeout,
            };
            self.deployer
                .create_scaling_table(&subsystem_spec.subsystem)
                .await;
            self.deployer
                .create_scaler_function(private_image_uri, &lambda_role_arn, &subsystem_spec)
                .await;
            // Functional and messaging have definitions that depend on user provided cpu and memory.
            if deployment.namespace != "functional" && deployment.namespace != "messaging" {
                self.deployer
                    .register_ecs_service_definition(
                        &subsystem_spec,
                        public_image_uri,
                        &ecs_role_arn,
                    )
                    .await;
                deployment_info.subsys_info = Some(SubsystemDeploymentInfo {
                    scl_mem: subsystem_spec.scl_mem,
                    scl_timeout: subsystem_spec.scl_timeout,
                    svc_mem: subsystem_spec.svc_mem,
                    svc_cpus: subsystem_spec.svc_cpus,
                    svc_grace: subsystem_spec.svc_grace,
                });
            }
        }

        // Make messaging templates.
        if let Some(messaging) = &deployment.messaging {
            let messaging_spec = MessagingSpec {
                mem: messaging.mem_size,
                cpus: messaging.mem_size / 2,
                timeout: messaging.timeout,
                namespace: deployment.namespace.clone(),
            };
            let fs_id = self.deployer.get_file_system(&deployment.namespace).await;
            let (ap_id, ap_arn) = self
                .deployer
                .get_fs_access_point(&deployment.namespace, &fs_id)
                .await;
            self.deployer.get_mount_target(&fs_id).await;
            self.deployer
                .register_messaging_service_definition(
                    &messaging_spec,
                    public_image_uri,
                    &ecs_role_arn,
                    &fs_id,
                    &ap_id,
                )
                .await;
            self.deployer
                .create_messaging_lambda_template(
                    private_image_uri,
                    &lambda_role_arn,
                    &ap_arn,
                    &messaging_spec,
                )
                .await;
            self.deployer
                .create_receiving_lambda_template(
                    system_private_image_uri,
                    &lambda_role_arn,
                    &messaging_spec,
                )
                .await;
            deployment_info.msg_info = Some(MessagingDeploymentInfo {
                cpus: messaging_spec.cpus,
                mem: messaging_spec.mem,
                timeout: messaging_spec.timeout,
            });
        }

        // Create functions.
        if let Some(functional) = &deployment.functional {
            let functional_spec = FunctionalSpec {
                mem: functional.mem_size,
                timeout: functional.timeout,
                namespace: deployment.namespace.clone(),
                concurrency: functional.concurrency,
            };
            let deployed_fn = self
                .deployer
                .register_functional_ecs_service_definition(
                    system_public_image_uri,
                    public_image_uri,
                    &ecs_role_arn,
                    &functional_spec,
                )
                .await;
            self.deployer
                .create_service("functional", &functional_spec.namespace, "fn")
                .await;
            self.deployer
                .create_functional_lambda(private_image_uri, &lambda_role_arn, &functional_spec)
                .await;
            deployment_info.fn_info = Some(deployed_fn);
        }

        self.deployer
            .finish_deployment(&deployment.namespace, deployment_info)
            .await;
    }

    /// Create private and public ecr repos.
    pub(crate) async fn create_repos(&self, namespace: &str) -> (String, String) {
        let name = full_image_name(namespace);
        let res = self
            .ecr_client
            .create_repository()
            .repository_name(&name)
            .send()
            .await;
        let private_name: String = match res {
            Ok(res) => res.repository().unwrap().repository_uri().unwrap().into(),
            Err(_) => {
                let res = self
                    .ecr_client
                    .describe_repositories()
                    .repository_names(&name)
                    .send()
                    .await
                    .unwrap();
                res.repositories().unwrap()[0]
                    .repository_uri()
                    .unwrap()
                    .into()
            }
        };
        let res = self
            .ecr_public_client
            .create_repository()
            .repository_name(&name)
            .send()
            .await;
        let public_name: String = match res {
            Ok(res) => res.repository().unwrap().repository_uri().unwrap().into(),
            Err(_) => {
                let res = self
                    .ecr_public_client
                    .describe_repositories()
                    .repository_names(&name)
                    .send()
                    .await
                    .unwrap();
                res.repositories().unwrap()[0]
                    .repository_uri()
                    .unwrap()
                    .into()
            }
        };

        ({ private_name }, { public_name })
    }
}
