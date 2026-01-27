use crate::commands::auth::require_auth;
use crate::commands::build::MetricsData;
use crate::commands::integrations::ecr::EcrManager;
use crate::commands::integrations::fly::FlyManager;
use crate::commands::integrations::helix::HelixManager;
use crate::config::{BuildMode, CloudConfig, InstanceInfo};
use crate::docker::DockerManager;
use crate::metrics_sender::MetricsSender;
use crate::output::{Operation, Step, Verbosity};
use crate::port;
use crate::project::ProjectContext;
use crate::prompts;
use eyre::Result;
use std::time::Instant;

pub async fn run(
    instance_name: Option<String>,
    dev: bool,
    metrics_sender: &MetricsSender,
) -> Result<()> {
    let start_time = Instant::now();

    // Load project context
    let project = ProjectContext::find_and_load(None)?;

    // Get instance name - prompt if not provided
    let instance_name = match instance_name {
        Some(name) => name,
        None if prompts::is_interactive() => {
            let instances = project.config.list_instances_with_types();
            prompts::intro(
                "helix push",
                Some(
                    "This will build and redeploy your selected instance based on the configuration in helix.toml.",
                ),
            )?;
            prompts::select_instance(&instances)?
        }
        None => {
            let instances = project.config.list_instances();
            return Err(eyre::eyre!(
                "No instance specified. Available instances: {}",
                instances
                    .into_iter()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
    };

    // Get instance config
    let instance_config = project.config.get_instance(&instance_name)?;

    // Check auth early for Helix Cloud instances
    if let InstanceInfo::Helix(_) = &instance_config {
        require_auth().await?;
    }

    let deploy_result = if instance_config.is_local() {
        push_local_instance(&project, &instance_name, metrics_sender).await
    } else {
        push_cloud_instance(
            &project,
            &instance_name,
            instance_config.clone(),
            dev,
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
    let op = Operation::new("Deploying", instance_name);

    let docker = DockerManager::new(project);

    // Check Docker availability
    DockerManager::check_runtime_available(docker.runtime)?;

    // Check port availability before building
    let instance_config = project.config.get_instance(instance_name)?;
    let requested_port = instance_config.port().unwrap_or(port::DEFAULT_PORT);
    let (actual_port, port_changed) = port::ensure_port_available(requested_port)?;

    if port_changed {
        crate::output::warning(&format!(
            "Port {} is in use. Using port {} instead.",
            requested_port, actual_port
        ));
    }

    // Build the instance first (this ensures it's up to date) and get metrics data
    let metrics_data =
        crate::commands::build::run_build_steps(&op, project, instance_name, None, metrics_sender)
            .await?;

    // If port changed, regenerate docker-compose with new port
    if port_changed {
        let compose_content = docker.generate_docker_compose(
            instance_name,
            instance_config.clone(),
            Some(actual_port),
        )?;
        let compose_path = project.docker_compose_path(instance_name);
        std::fs::write(&compose_path, compose_content)?;
    }

    // Start the instance
    let mut start_step = Step::with_messages("Starting instance", "Instance started");
    start_step.start();
    docker.start_instance(instance_name)?;
    start_step.done();

    op.success();

    let project_name = &project.config.project.name;
    if Verbosity::current().show_normal() {
        Operation::print_details(&[
            ("Local URL", &format!("http://localhost:{actual_port}")),
            (
                "Container",
                &format!("helix_{project_name}_{instance_name}"),
            ),
            (
                "Data volume",
                &project.instance_volume(instance_name).display().to_string(),
            ),
        ]);
    }

    Ok(metrics_data)
}

async fn push_cloud_instance(
    project: &ProjectContext,
    instance_name: &str,
    instance_config: InstanceInfo<'_>,
    dev: bool,
    metrics_sender: &MetricsSender,
) -> Result<MetricsData> {
    let op = Operation::new("Deploying", instance_name);

    let cluster_id = instance_config
        .cluster_id()
        .ok_or_else(|| eyre::eyre!("Cloud instance '{instance_name}' must have a cluster_id"))?;

    // Check if cluster has been created
    if cluster_id == "YOUR_CLUSTER_ID" {
        op.failure();
        return Err(eyre::eyre!(
            "Cluster for instance '{instance_name}' has not been created yet.\nRun 'helix create-cluster {instance_name}' to create the cluster first."
        ));
    }

    let metrics_data = if instance_config.should_build_docker_image() {
        // Build happens, get metrics data from build
        crate::commands::build::run(Some(instance_name.to_string()), None, metrics_sender).await?
    } else {
        // No build, use lightweight parsing
        parse_queries_for_metrics(project)?
    };

    // Deploy to cloud
    let config = project.config.cloud.get(instance_name).unwrap();
    let mut deploy_step = Step::with_messages("Deploying to cloud", "Deployed to cloud");
    deploy_step.start();

    match config {
        CloudConfig::FlyIo(config) => {
            Step::verbose_substep("Deploying to Fly.io...");
            let fly = FlyManager::new(project, config.auth_type.clone()).await?;
            let docker = DockerManager::new(project);
            // Get the correct image name from docker compose project name
            let image_name = docker.image_name(instance_name, config.build_mode);

            fly.deploy_image(&docker, config, instance_name, &image_name)
                .await?;
        }
        CloudConfig::Ecr(config) => {
            Step::verbose_substep("Deploying to ECR...");
            let ecr = EcrManager::new(project, config.auth_type.clone()).await?;
            let docker = DockerManager::new(project);
            // Get the correct image name from docker compose project name
            let image_name = docker.image_name(instance_name, config.build_mode);

            ecr.deploy_image(&docker, config, instance_name, &image_name)
                .await?;
        }
        CloudConfig::Helix(config) => {
            let helix = HelixManager::new(project);
            // CLI --dev flag takes precedence, otherwise use build_mode from config
            let build_mode = if dev {
                BuildMode::Dev
            } else {
                config.build_mode
            };

            helix
                .deploy(None, instance_name.to_string(), build_mode)
                .await?;
        }
    }
    deploy_step.done_with_info(&format!("cluster: {cluster_id}"));

    op.success();

    Ok(metrics_data)
}

/// Lightweight parsing for metrics when no compilation happens
fn parse_queries_for_metrics(project: &ProjectContext) -> Result<MetricsData> {
    use helix_db::helixc::parser::{
        HelixParser,
        types::{Content, HxFile, Source},
    };
    use std::fs;

    // Collect .hx files in project root
    let dir_entries: Vec<_> = std::fs::read_dir(&project.root)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry.path().is_file() && entry.path().extension().map(|s| s == "hx").unwrap_or(false)
        })
        .collect();

    // Generate content from the files (similar to build.rs)
    let hx_files: Vec<HxFile> = dir_entries
        .iter()
        .map(|file| {
            let name = file.path().to_string_lossy().into_owned();
            let content = fs::read_to_string(file.path())
                .map_err(|e| eyre::eyre!("Failed to read file {}: {}", name, e))?;
            Ok(HxFile { name, content })
        })
        .collect::<Result<Vec<_>>>()?;

    let content_str = hx_files
        .iter()
        .map(|file| file.content.clone())
        .collect::<Vec<String>>()
        .join("\n");

    let content = Content {
        content: content_str,
        files: hx_files,
        source: Source::default(),
    };

    // Parse the content
    let source =
        HelixParser::parse_source(&content).map_err(|e| eyre::eyre!("Parse error: {}", e))?;

    // Extract query names
    let all_queries: Vec<String> = source.queries.iter().map(|q| q.name.clone()).collect();

    Ok(MetricsData {
        queries_string: all_queries.join("\n"),
        num_of_queries: all_queries.len() as u32,
    })
}
