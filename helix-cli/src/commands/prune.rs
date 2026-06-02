use crate::local_runtime::Runtime;
use crate::output::Operation;
use crate::project::ProjectContext;
use crate::prompts::{self, PruneSelection};
use crate::utils::{print_confirm, print_warning};
use eyre::{Result, eyre};
use std::io::IsTerminal;

pub async fn run(instance: Option<String>, all: bool, yes: bool) -> Result<()> {
    let project = ProjectContext::find_and_load(None)?;
    if all {
        prune_all(&project, yes).await
    } else if let Some(instance) = instance {
        prune_one(&project, &instance).await
    } else if prompts::is_interactive() {
        match prompts::select_prune(&local_instances(&project))? {
            PruneSelection::All => prune_all(&project, yes).await,
            PruneSelection::Instance(instance) => prune_one(&project, &instance).await,
        }
    } else {
        Err(eyre!(
            "Specify a local instance to prune, or use --all to prune all local instances"
        ))
    }
}

async fn prune_one(project: &ProjectContext, instance: &str) -> Result<()> {
    let op = Operation::new("Pruning", instance);
    let removed_container = Runtime::for_project(project).prune(instance)?;
    let workspace = project.instance_workspace(instance);
    let removed_workspace = workspace.exists();
    if workspace.exists() {
        std::fs::remove_dir_all(workspace)?;
    }
    if removed_container || removed_workspace {
        op.success();
    } else {
        crate::output::info(&format!(
            "No local runtime resources found for '{instance}'"
        ));
    }
    Ok(())
}

fn local_instances(project: &ProjectContext) -> Vec<(String, String)> {
    let mut instances: Vec<(String, String)> = project
        .config
        .local
        .keys()
        .map(|name| (name.clone(), "local runtime resources".to_string()))
        .collect();
    instances.sort_by(|a, b| a.0.cmp(&b.0));
    instances
}

async fn prune_all(project: &ProjectContext, yes: bool) -> Result<()> {
    print_warning(
        "This will remove local v2 containers, workspaces, and on-disk storage volumes for all local instances.",
    );
    if !yes && !std::io::stdin().is_terminal() {
        return Err(eyre!(
            "Refusing to prune all instances non-interactively. Re-run with --yes to confirm."
        ));
    }
    if !yes && !print_confirm("Continue?")? {
        crate::output::info("Prune cancelled");
        return Ok(());
    }
    for instance in project.config.local.keys() {
        prune_one(project, instance).await?;
    }
    Ok(())
}
