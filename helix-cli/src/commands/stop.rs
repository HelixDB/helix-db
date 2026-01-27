use crate::commands::integrations::fly::FlyManager;
use crate::config::CloudConfig;
use crate::docker::DockerManager;
use crate::output::{Operation, Step};
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
        stop_local_instance(&project, &instance_name).await
    } else {
        stop_cloud_instance(&project, &instance_name, instance_config.into()).await
    }
}

async fn stop_local_instance(project: &ProjectContext, instance_name: &str) -> Result<()> {
    let op = Operation::new("Stopping", instance_name);

    let docker = DockerManager::new(project);

    // Check Docker availability
    DockerManager::check_runtime_available(docker.runtime)?;

    // Stop the instance
    let mut stop_step = Step::with_messages("Stopping container", "Container stopped");
    stop_step.start();
    docker.stop_instance(instance_name)?;
    stop_step.done();

    op.success();

    Ok(())
}

async fn stop_cloud_instance(
    project: &ProjectContext,
    instance_name: &str,
    instance_config: CloudConfig,
) -> Result<()> {
    let op = Operation::new("Stopping", instance_name);

    let _cluster_id = instance_config
        .get_cluster_id()
        .ok_or_eyre("Cloud instance '{instance_name}' must have a cluster_id")?;

    let mut stop_step = Step::with_messages("Stopping cloud instance", "Cloud instance stopped");
    stop_step.start();

    match instance_config {
        CloudConfig::FlyIo(config) => {
            Step::verbose_substep("Stopping Fly.io instance...");
            let fly = FlyManager::new(project, config.auth_type.clone()).await?;
            fly.stop_instance(instance_name).await?;
        }
        CloudConfig::Helix(_config) => {
            todo!()
        }
        CloudConfig::Ecr(_config) => {
            unimplemented!()
        }
    }

    stop_step.done();
    op.success();

    Ok(())
}
