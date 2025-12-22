use crate::commands::auth::require_auth;
use crate::commands::integrations::ecr::EcrManager;
use crate::commands::integrations::fly::FlyManager;
use crate::config::InstanceInfo;
use crate::docker::DockerManager;
use crate::project::ProjectContext;
use crate::utils::{
    print_confirm, print_lines, print_newline, print_status, print_success, print_warning,
};
use eyre::Result;

pub async fn run(instance_name: String) -> Result<()> {
    // Load project context
    let project = ProjectContext::find_and_load(None)?;

    // Validate instance exists
    let instance_config = project.config.get_instance(&instance_name)?;

    // Check auth early for Helix Cloud instances
    if let InstanceInfo::Helix(_) = &instance_config {
        require_auth().await?;
    }

    print_warning(&format!(
        "This will permanently delete instance '{instance_name}' and ALL its data!"
    ));
    print_lines(&[
        "- Docker containers and images",
        "- Persistent volumes (databases, files)",
        "This action cannot be undone.",
    ]);
    print_newline();

    let confirmed = print_confirm(&format!(
        "Are you sure you want to delete instance '{instance_name}'?"
    ))?;

    if !confirmed {
        print_status("DELETE", "Deletion cancelled.");
        return Ok(());
    }

    print_status("DELETE", &format!("Deleting instance '{instance_name}'"));

    // Stop and remove Docker containers and volumes
    let runtime = project.config.project.container_runtime;
    if DockerManager::check_runtime_available(runtime).is_ok() {
        let docker = DockerManager::new(&project);

        // Remove containers and Docker volumes
        docker.prune_instance(&instance_name, true)?;

        // Remove Docker images
        docker.remove_instance_images(&instance_name)?;
    }

    // Remove instance workspace
    let workspace = project.instance_workspace(&instance_name);
    if workspace.exists() {
        std::fs::remove_dir_all(&workspace)?;
        print_status("DELETE", "Removed workspace directory");
    }

    // Remove instance volumes (permanent data loss)
    let volume = project.instance_volume(&instance_name);
    if volume.exists() {
        std::fs::remove_dir_all(&volume)?;
        print_status("DELETE", "Removed persistent volumes");
    }

    // if cloud instance, delete the app

    match instance_config {
        InstanceInfo::FlyIo(config) => {
            let fly = FlyManager::new(&project, config.auth_type.clone()).await?;
            fly.delete_app(&instance_name).await?;
        }
        InstanceInfo::Ecr(config) => {
            let ecr = EcrManager::new(&project, config.auth_type.clone()).await?;
            ecr.delete_repository(&instance_name).await?;
        }
        InstanceInfo::Helix(_config) => {
            todo!()
        }
        InstanceInfo::Local(_) => {
            // Local instances don't have cloud resources to delete
        }
    }

    print_success(&format!("Instance '{instance_name}' deleted successfully"));

    Ok(())
}
