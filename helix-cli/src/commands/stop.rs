use crate::commands::integrations::fly::FlyManager;
use crate::config::InstanceInfo;
use crate::docker::DockerManager;
use crate::project::ProjectContext;
use crate::utils::{print_status, print_success};
use eyre::Result;

pub async fn run(instance_name: String) -> Result<()> {
    // Load project context
    let project = ProjectContext::find_and_load(None)?;

    // Get instance config
    let instance_config = project.config.get_instance(&instance_name)?;

    if instance_config.is_local() {
        stop_local_instance(&project, &instance_name).await
    } else {
        stop_cloud_instance(&project, &instance_name, instance_config).await
    }
}

async fn stop_local_instance(project: &ProjectContext, instance_name: &str) -> Result<()> {
    print_status(
        "STOP",
        &format!("Stopping local instance '{instance_name}'"),
    );

    let docker = DockerManager::new(project);

    // Check Docker availability
    DockerManager::check_docker_available()?;

    // Stop the instance
    docker.stop_instance(instance_name)?;

    print_success(&format!("Instance '{instance_name}' has been stopped"));

    Ok(())
}

async fn stop_cloud_instance(
    project: &ProjectContext,
    instance_name: &str,
    instance_config: InstanceInfo<'_>,
) -> Result<()> {
    print_status(
        "CLOUD",
        &format!("Stopping cloud instance '{instance_name}'"),
    );

    let _cluster_id = instance_config
        .cluster_id()
        .ok_or_else(|| eyre::eyre!("Cloud instance '{instance_name}' must have a cluster_id"))?;

    // TODO: Implement cloud instance stop
    // This would involve:
    // 1. Connecting to the cloud API
    // 2. Stopping the instance on the specified cluster
    // 3. Waiting for the instance to be fully stopped

    match instance_config {
        InstanceInfo::FlyIo(config) => {
            print_status(
                "FLY",
                &format!("Stopping Fly.io instance '{instance_name}'"),
            );
            let fly = FlyManager::new(project, config.auth_type.clone()).await?;
            fly.stop_instance(instance_name).await?;
        }
        InstanceInfo::HelixCloud(_config) => {
            todo!()
        }
        _ => {
            unimplemented!()
        }
    }

    Ok(())
}
