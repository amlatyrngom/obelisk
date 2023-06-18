use aws_sdk_efs::types::FileSystemDescription;

pub struct FilesystemDeployment {}

impl FilesystemDeployment {
    /// Name of file system.
    pub fn filesystem_name(namespace: &str) -> String {
        format!("obk__{namespace}")
    }

    /// Make mount target.
    pub async fn make_mount_target(
        client: &aws_sdk_efs::Client,
        subnet_ids: &[String],
        sg_id: &str,
        fs_id: &str,
    ) {
        let mut created = false;
        for subnet_id in subnet_ids {
            let mt = client
                .create_mount_target()
                .security_groups(sg_id)
                .subnet_id(subnet_id)
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
                let mts = client
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

    /// Get access point.
    pub async fn get_fs_access_point(
        client: &aws_sdk_efs::Client,
        namespace: &str,
        fs_id: &str,
    ) -> (String, String) {
        let fs_name = Self::filesystem_name(namespace);
        let ap_info = client
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

        let _ = client
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
            let ap_info = client
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
    pub async fn get_file_system(client: &aws_sdk_efs::Client, namespace: &str) -> String {
        let fs_name = Self::filesystem_name(namespace);
        let mut marker: Option<String> = None;
        loop {
            let mut fs_info = client
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
        let _ = client
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
            let fs_info = client
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
}
