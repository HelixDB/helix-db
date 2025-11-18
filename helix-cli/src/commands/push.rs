use crate::commands::build::MetricsData;
use crate::commands::integrations::ecr::EcrManager;
use crate::commands::integrations::fly::FlyManager;
use crate::commands::integrations::helix::HelixManager;
use crate::config::{CloudConfig, InstanceInfo};
use crate::docker::DockerManager;
use crate::metrics_sender::MetricsSender;
use crate::project::ProjectContext;
use crate::utils::{print_status, print_success, spinner, helixc_utils::collect_hx_files};
use eyre::Result;
use std::time::Instant;

pub async fn run(instance_name: String, metrics_sender: &MetricsSender) -> Result<()> {
    let start_time = Instant::now();

    // Load project context
    let project = ProjectContext::find_and_load(None)?;

    // Get instance config
    let instance_config = project.config.get_instance(&instance_name)?;

    let deploy_result = if instance_config.is_local() {
        push_local_instance(&project, &instance_name, metrics_sender).await
    } else {
        push_cloud_instance(
            &project,
            &instance_name,
            instance_config.clone(),
            metrics_sender,
        )
        .await
    };

    // Send appropriate deploy metrics based on instance type and result
    let duration = start_time.elapsed().as_secs() as u32;
    let success = deploy_result.is_ok();
    let error_messages = deploy_result.as_ref().err().map(|e| e.to_string());

    // Get metrics data from the deploy result, or use defaults on error
    let default_metrics = MetricsData {
        queries_string: String::new(),
        num_of_queries: 0,
    };
    let metrics_data = deploy_result.as_ref().unwrap_or(&default_metrics);

    if instance_config.is_local() {
        // Check if this is a redeploy by seeing if container already exists
        let docker = DockerManager::new(&project);
        let is_redeploy = docker.instance_exists(&instance_name).unwrap_or(false);

        if is_redeploy {
            metrics_sender.send_redeploy_local_event(
                instance_name.clone(),
                metrics_data.queries_string.clone(),
                metrics_data.num_of_queries,
                duration,
                success,
                error_messages,
            );
        } else {
            metrics_sender.send_deploy_local_event(
                instance_name.clone(),
                metrics_data.queries_string.clone(),
                metrics_data.num_of_queries,
                duration,
                success,
                error_messages,
            );
        }
    } else {
        metrics_sender.send_deploy_cloud_event(
            instance_name.clone(),
            metrics_data.queries_string.clone(),
            metrics_data.num_of_queries,
            duration,
            success,
            error_messages,
        );
    }

    deploy_result.map(|_| ())
}

async fn push_local_instance(
    project: &ProjectContext,
    instance_name: &str,
    metrics_sender: &MetricsSender,
) -> Result<MetricsData> {
    print_status(
        "DEPLOY",
        &format!("Deploying local instance '{instance_name}'"),
    );

    let docker = DockerManager::new(project);

    // Check Docker availability
    DockerManager::check_docker_available()?;

    // Build the instance first (this ensures it's up to date) and get metrics data
    // build::run() has its own status messages, so no spinner needed
    let metrics_data = crate::commands::build::run(instance_name.to_string(), metrics_sender)
        .await?;

    // Start the instance (has its own status messages, so no spinner needed)
    docker.start_instance(instance_name)?;

    // Get the instance configuration to show connection info
    let instance_config = project.config.get_instance(instance_name)?;
    let port = instance_config.port().unwrap_or(6969);

    print_success(&format!("Instance '{instance_name}' is now running"));
    println!("  Local URL: http://localhost:{port}");
    let project_name = &project.config.project.name;
    println!("  Container: helix_{project_name}_{instance_name}");
    println!(
        "  Data volume: {}",
        project.instance_volume(instance_name).display()
    );

    Ok(metrics_data)
}

async fn push_cloud_instance(
    project: &ProjectContext,
    instance_name: &str,
    instance_config: InstanceInfo<'_>,
    metrics_sender: &MetricsSender,
) -> Result<MetricsData> {
    print_status(
        "CLOUD",
        &format!("Deploying to cloud instance '{instance_name}'"),
    );

    let cluster_id = instance_config
        .cluster_id()
        .ok_or_else(|| eyre::eyre!("Cloud instance '{instance_name}' must have a cluster_id"))?;

    let metrics_data = if instance_config.should_build_docker_image() {
        // Build happens, get metrics data from build
        // build::run() has its own status messages, so no spinner needed
        crate::commands::build::run(instance_name.to_string(), metrics_sender).await?
    } else {
        // No build, use lightweight parsing
        parse_queries_for_metrics(project)?
    };

    // TODO: Implement cloud deployment
    // This would involve:
    // 1. Reading compiled queries from the container directory
    // 2. Uploading them to the cloud cluster
    // 3. Triggering deployment on the cloud

    let config = project.config.cloud.get(instance_name).unwrap();
    match config {
        CloudConfig::FlyIo(config) => {
            spinner::with_spinner(
                "Deploying to Fly.io...",
                async {
                    let fly = FlyManager::new(project, config.auth_type.clone()).await?;
                    let docker = DockerManager::new(project);
                    // Get the correct image name from docker compose project name
                    let image_name = docker.image_name(instance_name, config.build_mode);

                    fly.deploy_image(&docker, config, instance_name, &image_name)
                        .await?;
                    Ok::<(), eyre::Error>(())
                },
            )
            .await?;
        }
        CloudConfig::Ecr(config) => {
            spinner::with_spinner(
                "Deploying to ECR...",
                async {
                    let ecr = EcrManager::new(project, config.auth_type.clone()).await?;
                    let docker = DockerManager::new(project);
                    // Get the correct image name from docker compose project name
                    let image_name = docker.image_name(instance_name, config.build_mode);

                    ecr.deploy_image(&docker, config, instance_name, &image_name)
                        .await?;
                    Ok::<(), eyre::Error>(())
                },
            )
            .await?;
        }
        CloudConfig::Helix(_config) => {
            spinner::with_spinner(
                "Deploying to Helix Cloud...",
                async {
                    let helix = HelixManager::new(project);
                    helix.deploy(None, instance_name.to_string()).await?;
                    Ok::<(), eyre::Error>(())
                },
            )
            .await?;
        }
    }

    print_status("UPLOAD", &format!("Uploading to cluster: {cluster_id}"));

    Ok(metrics_data)
}

/// Lightweight parsing for metrics when no compilation happens  
fn parse_queries_for_metrics(project: &ProjectContext) -> Result<MetricsData> {
    use crate::utils::helixc_utils::{generate_content, parse_content};

    // Collect .hx files using the configured queries path (consistent with build.rs)
    let dir_entries = collect_hx_files(&project.root, &project.config.project.queries)?;

    // Generate content from the files (same as build.rs)
    let content = generate_content(&dir_entries)?;

    // Parse the content
    let source = parse_content(&content)?;

    // Extract query names
    let all_queries: Vec<String> = source.queries.iter().map(|q| q.name.clone()).collect();

    Ok(MetricsData {
        queries_string: all_queries.join("\n"),
        num_of_queries: all_queries.len() as u32,
    })
}
