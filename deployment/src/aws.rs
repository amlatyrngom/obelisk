#[derive(Clone)]
pub struct AWS {
    ecr_client: aws_sdk_ecr::Client,
    ecr_public_client: aws_sdk_ecrpublic::Client,
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
        };
        aws
    }

    /// Create private and public ecr repos.
    pub(crate) async fn create_repos(&self, project_name: &str) -> (String, String) {
        let name = full_image_name(project_name);
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
