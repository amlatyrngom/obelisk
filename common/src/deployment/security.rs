const LAMBDA_ROLE: &str = "OBELISKLAMBDAROLE";
const ECS_ROLE: &str = "OBELISKECSROLE";
const POLICY: &str = "OBELISKPOLICY";
const IAM_PREFIX: &str = "/obelisk/";

pub struct SecurityDeployment {
    pub lambda_role_arn: String,
    pub ecs_role_arn: String,
}

impl SecurityDeployment {
    pub async fn new() -> Self {
        let shared_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let iam_client = aws_sdk_iam::Client::new(&shared_config);
        let (lambda_role_arn, ecs_role_arn) = Self::get_role(&iam_client).await;
        SecurityDeployment {
            lambda_role_arn,
            ecs_role_arn,
        }
    }

    /// Get lambda role name and arn.
    async fn get_or_make_lambda_role(iam_client: &aws_sdk_iam::Client) -> (String, String) {
        let trust = include_str!("lambda_trust_policy.json");
        let roles = iam_client
            .list_roles()
            .path_prefix(IAM_PREFIX)
            .send()
            .await
            .unwrap();
        let mut role = {
            let mut role = None;
            for r in roles.roles().iter() {
                if r.role_name() == LAMBDA_ROLE {
                    role = Some(r.clone());
                    break;
                }
            }
            role
        };
        if role.is_none() {
            println!("Making lambda role...");
            role = Some(
                iam_client
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
        (role.role_name().into(), role.arn().into())
    }

    /// Get ecs role name and arn.
    async fn get_or_make_ecs_role(iam_client: &aws_sdk_iam::Client) -> (String, String) {
        let trust = include_str!("ecs_trust_policy.json");
        let roles = iam_client
            .list_roles()
            .path_prefix(IAM_PREFIX)
            .send()
            .await
            .unwrap();
        let mut role = {
            let mut role = None;
            for r in roles.roles().iter() {
                if r.role_name() == ECS_ROLE {
                    role = Some(r.clone());
                    break;
                }
            }
            role
        };
        if role.is_none() {
            println!("Making ECS role...");
            role = Some(
                iam_client
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
        (role.role_name().into(), role.arn().into())
    }

    /// Get access policy.
    async fn get_or_make_policy(iam_client: &aws_sdk_iam::Client) -> (String, String) {
        let policy_document = include_str!("full_access_policy.json");
        let policies = iam_client
            .list_policies()
            .path_prefix(IAM_PREFIX)
            .send()
            .await
            .unwrap();
        let mut policy = {
            let mut policy = None;
            for p in policies.policies().iter() {
                if p.policy_name().unwrap() == POLICY {
                    policy = Some(p.clone());
                    break;
                }
            }
            policy
        };
        if policy.is_none() {
            println!("Making IAM policy...");
            policy = Some(
                iam_client
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
    pub async fn get_role(iam_client: &aws_sdk_iam::Client) -> (String, String) {
        let (_, policy_arn) = Self::get_or_make_policy(iam_client).await;
        let (lambda_role_name, lambda_role_arn) = Self::get_or_make_lambda_role(iam_client).await;
        let (ecs_role_name, ecs_role_arn) = Self::get_or_make_ecs_role(iam_client).await;
        let _ = iam_client
            .attach_role_policy()
            .role_name(&lambda_role_name)
            .policy_arn(&policy_arn)
            .send()
            .await
            .unwrap();
        let _ = iam_client
            .attach_role_policy()
            .role_name(&ecs_role_name)
            .policy_arn(&policy_arn)
            .send()
            .await
            .unwrap();
        (lambda_role_arn, ecs_role_arn)
    }
}
