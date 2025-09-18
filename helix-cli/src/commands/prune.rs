use crate::docker::DockerManager;
use crate::errors::project_error;
use crate::project::ProjectContext;
use crate::utils::{
    print_confirm, print_lines, print_newline, print_status, print_success, print_warning,
};
use eyre::Result;

pub async fn run(instance: Option<String>, all: bool) -> Result<()> {
    // Try to load project context
    match ProjectContext::find_and_load(None) {
        Ok(project) => {
            // Inside a Helix project
            if all {
                prune_all_instances(&project).await
            } else if let Some(instance_name) = instance {
                prune_instance(&project, &instance_name).await
            } else {
                prune_unused_resources(&project).await
            }
        }
        Err(_) => {
            // Outside a Helix project - offer system-wide clean
            if instance.is_some() || all {
                return Err(project_error("not in a Helix project directory")
                    .with_hint("use 'helix prune' without arguments for system-wide cleanup")
                    .into());
            }
            prune_system_wide().await
        }
    }
}

async fn prune_instance(project: &ProjectContext, instance_name: &str) -> Result<()> {
    print_status("PRUNE", &format!("Pruning instance '{instance_name}'"));

    // Validate instance exists
    let _instance_config = project.config.get_instance(instance_name)?;

    // Check Docker availability
    if DockerManager::check_docker_available().is_ok() {
        let docker = DockerManager::new(project);

        // Remove containers (but not volumes)
        let _ = docker.prune_instance(instance_name, false);

        // Remove Docker images
        let _ = docker.remove_instance_images(instance_name);
    }

    // Remove instance workspace directory
    let workspace = project.instance_workspace(instance_name);
    if workspace.exists() {
        std::fs::remove_dir_all(&workspace)?;
        print_status("PRUNE", &format!("Removed workspace for '{instance_name}'"));
    }

    print_success(&format!(
        "Instance '{instance_name}' pruned successfully (volumes preserved)"
    ));
    Ok(())
}

async fn prune_all_instances(project: &ProjectContext) -> Result<()> {
    print_status("PRUNE", "Pruning all instances in project");

    let instances = project.config.list_instances();

    if DockerManager::check_docker_available().is_ok() {
        let docker = DockerManager::new(project);

        for instance_name in &instances {
            print_status("PRUNE", &format!("Pruning instance '{instance_name}'"));

            // Remove containers (but not volumes)
            let _ = docker.prune_instance(instance_name, false);

            // Remove Docker images
            let _ = docker.remove_instance_images(instance_name);
        }
    }

    // Remove instance workspaces but keep volumes
    for instance_name in &instances {
        let workspace = project.instance_workspace(instance_name);
        if workspace.exists()
            && let Err(e) = std::fs::remove_dir_all(&workspace)
        {
            eprintln!("Warning: Failed to remove workspace for '{instance_name}': {e}");
        }
    }

    print_success("All instances pruned successfully (volumes preserved)");
    Ok(())
}

async fn prune_unused_resources(project: &ProjectContext) -> Result<()> {
    print_status("PRUNE", "Pruning unused Docker resources");

    // Check Docker availability
    DockerManager::check_docker_available()?;

    // Use centralized docker command
    let docker = DockerManager::new(project);
    let output = docker.run_docker_command(&["system", "prune", "-f"])?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(eyre::eyre!("Failed to prune Docker resources:\n{stderr}"));
    }

    print_success("Unused Docker resources pruned");
    Ok(())
}

async fn prune_system_wide() -> Result<()> {
    print_warning("You are not in a Helix project directory.");
    print_lines(&[
        "This will remove ALL Helix-related Docker images from your system.",
        "This action cannot be undone.",
    ]);
    print_newline();

    let confirmed = print_confirm("Are you sure you want to proceed?")?;

    if !confirmed {
        print_status("PRUNE", "Operation cancelled.");
        return Ok(());
    }

    print_status("PRUNE", "Pruning all Helix images from system");

    // Check Docker availability
    DockerManager::check_docker_available()?;

    // Remove all Helix images
    DockerManager::clean_all_helix_images()?;

    // Also clean unused Docker resources
    let output = std::process::Command::new("docker")
        .args(["system", "prune", "-f"])
        .output()
        .map_err(|e| eyre::eyre!("Failed to run docker system prune: {e}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(eyre::eyre!("Failed to prune Docker resources:\n{stderr}"));
    }

    print_success("System-wide Helix prune completed");
    Ok(())
}
