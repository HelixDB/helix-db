use crate::local_runtime::LocalRuntime;
use crate::output::Operation;
use crate::project::ProjectContext;
use crate::prompts::{self, PruneSelection};
use crate::utils::{print_confirm, print_warning};
use eyre::{eyre, Result};
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
    // `instance` can come straight from the CLI arg (`helix prune <name>`), not just
    // from an already-validated `helix.toml` key — `local_instances`/`prune_all` only
    // iterate config keys, but the direct-name path below does not look the name up
    // in the config at all, by design (it also prunes leftover state for an instance
    // that was since renamed or removed from helix.toml). So a name containing `..`
    // or `/` must be rejected here, before it's joined onto `.helix/` and recursively
    // deleted.
    crate::config::validate_instance_name(instance).map_err(|message| eyre!(message))?;

    let op = Operation::new("Pruning", instance);
    let removed_container = LocalRuntime::new(project).prune_instance(instance)?;
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
        "This will remove local v2 containers, workspaces, and Helix-managed on-disk storage volumes for all local instances. Remote S3 object-store data is not deleted.",
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::HelixConfig;

    #[tokio::test]
    async fn prune_one_rejects_path_traversal_name_before_touching_the_filesystem() {
        let dir = tempfile::tempdir().unwrap();
        let helix_dir = dir.path().join(".helix");
        std::fs::create_dir_all(&helix_dir).unwrap();
        // A sibling directory that a `..`-escaping join would land on. If validation
        // didn't run first, `remove_dir_all` would delete this.
        let sentinel = dir.path().join("sentinel");
        std::fs::create_dir_all(&sentinel).unwrap();

        let project = ProjectContext {
            root: dir.path().to_path_buf(),
            config: HelixConfig::default_config("test-project"),
            helix_dir,
        };

        let result = prune_one(&project, "../sentinel").await;

        assert!(result.is_err());
        assert!(sentinel.exists(), "sentinel directory must survive");
    }

    #[tokio::test]
    async fn prune_one_rejects_empty_name() {
        let dir = tempfile::tempdir().unwrap();
        let project = ProjectContext {
            root: dir.path().to_path_buf(),
            config: HelixConfig::default_config("test-project"),
            helix_dir: dir.path().join(".helix"),
        };

        assert!(prune_one(&project, "").await.is_err());
    }
}
