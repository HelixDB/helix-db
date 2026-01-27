use crate::commands::integrations::fly::FlyManager;
use crate::config::CloudConfig;
use crate::docker::DockerManager;
use crate::output::{Operation, Step, Verbosity};
use crate::project::ProjectContext;
use crate::prompts;
use eyre::{OptionExt, Result};

pub async fn run(instance_name: Option<String>) -> Result<()> {
    // Load project context
    let project = ProjectContext::find_and_load(None)?;

    // Get instance name - prompt if not provided
    let instance_name = match instance_name {
        Some(name) => name,
        None if prompts::is_interactive() => {
            let instances = project.config.list_instances_with_types();
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

    if instance_config.is_local() {
        start_local_instance(&project, &instance_name).await
    } else {
        start_cloud_instance(&project, &instance_name, instance_config.into()).await
    }
}

async fn start_local_instance(project: &ProjectContext, instance_name: &str) -> Result<()> {
    let op = Operation::new("Starting", instance_name);

    let docker = DockerManager::new(project);

    // Check Docker availability
    DockerManager::check_runtime_available(docker.runtime)?;

    // Check if instance is built (has docker-compose.yml)
    let workspace = project.instance_workspace(instance_name);
    let compose_file = workspace.join("docker-compose.yml");

    if !compose_file.exists() {
        op.failure();
        let error = crate::errors::CliError::new(format!(
            "instance '{instance_name}' has not been built yet"
        ))
        .with_hint(format!(
            "run 'helix build {instance_name}' first to build the instance"
        ));
        return Err(eyre::eyre!("{}", error.render()));
    }

    // Start the instance
    let mut start_step = Step::with_messages("Starting container", "Container started");
    start_step.start();
    docker.start_instance(instance_name)?;
    start_step.done();

    // Get the instance configuration to show connection info
    let instance_config = project.config.get_instance(instance_name)?;
    let port = instance_config.port().unwrap_or(6969);

    op.success();

    let project_name = &project.config.project.name;
    if Verbosity::current().show_normal() {
        Operation::print_details(&[
            ("Local URL", &format!("http://localhost:{port}")),
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

    Ok(())
}

async fn start_cloud_instance(
    project: &ProjectContext,
    instance_name: &str,
    cloud_config: CloudConfig,
) -> Result<()> {
    let op = Operation::new("Starting", instance_name);

    let cluster_id = cloud_config
        .get_cluster_id()
        .ok_or_eyre("Cloud instance '{instance_name}' must have a cluster_id")?;

    let mut start_step = Step::with_messages("Starting cloud instance", "Cloud instance started");
    start_step.start();

    Step::verbose_substep(&format!("Starting instance on cluster: {cluster_id}"));

    match cloud_config {
        CloudConfig::FlyIo(config) => {
            let fly = FlyManager::new(project, config.auth_type.clone()).await?;
            fly.start_instance(instance_name).await?;
        }
        CloudConfig::Helix(_config) => {
            todo!()
        }
        CloudConfig::Ecr(_config) => {
            unimplemented!()
        }
    }

    start_step.done();
    op.success();

    Ok(())
}
