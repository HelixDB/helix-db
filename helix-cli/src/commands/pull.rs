use crate::config::InstanceInfo;
use crate::project::ProjectContext;
use crate::utils::{print_status, print_warning};
use eyre::Result;

pub async fn run(instance_name: String) -> Result<()> {
    // Load project context
    let project = ProjectContext::find_and_load(None)?;

    // Get instance config
    let instance_config = project.config.get_instance(&instance_name)?;

    if instance_config.is_local() {
        pull_from_local_instance(&project, &instance_name).await
    } else {
        pull_from_cloud_instance(&project, &instance_name, instance_config).await
    }
}

async fn pull_from_local_instance(project: &ProjectContext, instance_name: &str) -> Result<()> {
    print_status(
        "PULL",
        &format!("Pulling queries from local instance '{instance_name}'"),
    );

    // For local instances, we'd need to extract the .hql files from the running container
    // or from the compiled workspace

    let workspace = project.instance_workspace(instance_name);
    let container_dir = workspace.join("helix-container");

    if !container_dir.exists() {
        return Err(eyre::eyre!(
            "Instance '{instance_name}' has not been built yet. Run 'helix build {instance_name}' first."
        ));
    }

    // TODO: Implement extraction of .hql files from compiled container
    // This would reverse-engineer the queries from the compiled Rust code
    // or maintain source files alongside compiled versions

    print_warning("Local instance query extraction not yet implemented");
    println!("  Local instances compile queries into Rust code.");
    println!("  Query extraction from compiled code is not currently supported.");

    Ok(())
}

async fn pull_from_cloud_instance(
    _project: &ProjectContext,
    instance_name: &str,
    instance_config: InstanceInfo<'_>,
) -> Result<()> {
    print_status(
        "CLOUD",
        &format!("Pulling queries from cloud instance '{instance_name}'"),
    );

    let cluster_id = instance_config
        .cluster_id()
        .ok_or_else(|| eyre::eyre!("Cloud instance '{instance_name}' must have a cluster_id"))?;

    print_status(
        "DOWNLOAD",
        &format!("Downloading from cluster: {cluster_id}"),
    );

    // TODO: Implement cloud query download
    // This would:
    // 1. Connect to the cloud cluster
    // 2. Download the current .hql files
    // 3. Update local schema.hx and queries.hx files

    print_warning("Cloud query pull not yet implemented");
    println!("  This will download the latest .hql files from cluster: {cluster_id}");
    println!("  and update your local schema.hx and queries.hx files.");

    Ok(())
}
