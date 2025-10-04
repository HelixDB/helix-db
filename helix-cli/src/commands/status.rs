use crate::docker::DockerManager;
use crate::project::ProjectContext;
use crate::utils::{print_error, print_field, print_header, print_newline};
use eyre::Result;

pub async fn run() -> Result<()> {
    // Load project context
    let project = match ProjectContext::find_and_load(None) {
        Ok(project) => project,
        Err(_) => {
            print_error("Not in a Helix project directory. Run 'helix init' to create one.");
            return Ok(());
        }
    };

    print_header("Helix Project Status");
    print_field("Project", &project.config.project.name);
    print_field("Root", &project.root.display().to_string());
    print_newline();

    // Show configured instances
    print_header("Configured Instances:");

    // Show local instances
    for (name, config) in &project.config.local {
        let port = config.port.unwrap_or(6969);
        print_field(&format!("{name} (Local)"), &format!("port {port}"));
    }

    // Show cloud instances
    let mut helix_cloud_instances = Vec::new();
    let mut flyio_instances = Vec::new();
    let mut ecr_instances = Vec::new();

    for (name, config) in &project.config.cloud {
        match config {
            crate::config::CloudConfig::Helix(helix_config) => {
                helix_cloud_instances.push((name, &helix_config.cluster_id));
            }
            crate::config::CloudConfig::FlyIo(_) => {
                flyio_instances.push((name, "flyio"));
            }
            crate::config::CloudConfig::Ecr(ecr_config) => {
                ecr_instances.push((name, &ecr_config.repository_name, &ecr_config.region));
            }
        }
    }

    for (name, cluster_id) in helix_cloud_instances {
        print_field(
            &format!("{name} (Helix Cloud)"),
            &format!("cluster {cluster_id}"),
        );
    }

    for (name, cluster_id) in flyio_instances {
        print_field(
            &format!("{name} (Fly.io)"),
            &format!("cluster {cluster_id}"),
        );
    }

    for (name, repository_name, region) in ecr_instances {
        print_field(
            &format!("{name} (AWS ECR)"),
            &format!("repository {repository_name} in {region}"),
        );
    }
    print_newline();

    // Show running containers (for local instances)
    show_container_status(&project).await?;

    Ok(())
}

async fn show_container_status(project: &ProjectContext) -> Result<()> {
    // Check if Docker is available
    if DockerManager::check_docker_available().is_err() {
        print_field("Docker Status", "Not available");
        return Ok(());
    }

    let docker = DockerManager::new(project);

    let statuses = match docker.get_project_status() {
        Ok(statuses) => statuses,
        Err(e) => {
            print_field("Container Status", &format!("Error getting status ({e})"));
            return Ok(());
        }
    };

    if statuses.is_empty() {
        print_field("Running Containers", "None");
        return Ok(());
    }

    print_header("Running Containers:");
    for status in statuses {
        let status_icon = if status.status.contains("Up") {
            "[UP]"
        } else {
            "[DOWN]"
        };

        let ports_info = if status.ports.is_empty() {
            "no ports".to_string()
        } else {
            status.ports.clone()
        };

        print_field(
            &format!("{status_icon} {}", status.instance_name),
            &format!("{} ({ports_info})", status.status),
        );
    }

    Ok(())
}
