use crate::local_runtime::LocalRuntime;
use crate::output::Operation;
use crate::project::ProjectContext;
use crate::prompts::{self, PruneSelection};
use crate::{errors::CliError, output};
use eyre::{eyre, Result};

/// Prune one instance or all of them, emitting one result either way:
/// `{"pruned": [{"instance": …, "removed": bool}, …]}`.
pub async fn run(instance: Option<String>, all: bool, yes: bool) -> Result<()> {
    let project = ProjectContext::find_and_load(None)?;
    let selection = match (all, instance) {
        (true, _) => PruneSelection::All,
        (false, Some(instance)) => PruneSelection::Instance(instance),
        (false, None) if prompts::is_interactive() => {
            prompts::select_prune(&local_instances(&project))?
        }
        (false, None) => {
            return Err(CliError::new("nothing to prune")
                .with_hint("pass a local instance name, or --all for every local instance")
                .into());
        }
    };
    let pruned = match selection {
        PruneSelection::All => prune_all(&project, yes).await?,
        PruneSelection::Instance(instance) => {
            let removed = prune_one(&project, &instance).await?;
            vec![(instance, removed)]
        }
    };
    output::emit(
        &serde_json::json!({
            "pruned": pruned
                .iter()
                .map(|(instance, removed)| serde_json::json!({"instance": instance, "removed": removed}))
                .collect::<Vec<_>>(),
        }),
        |_| Ok(()),
    )
}

/// Whether anything was removed.
async fn prune_one(project: &ProjectContext, instance: &str) -> Result<bool> {
    // `instance` can come straight from the CLI arg (`helix prune <name>`), not just
    // from an already-validated `helix.toml` key — `local_instances`/`prune_all` only
    // iterate config keys, but the direct-name path below does not look the name up
    // in the config at all, by design (it also prunes leftover state for an instance
    // that was since renamed or removed from helix.toml). So a name containing `..`
    // or `/` must be rejected here, before it's joined onto `.helix/` and recursively
    // deleted.
    crate::config::validate_instance_name(instance).map_err(|message| eyre!(message))?;
    // A valid instance name isn't enough on its own: `.helix` itself could be a
    // repository-tracked symlink to somewhere outside the project, in which case
    // even `.helix/dev` would resolve outside it. See `assert_safe_helix_dir`.
    project.assert_safe_helix_dir()?;

    let op = Operation::new("Pruning", instance);
    let removed_container = LocalRuntime::new(project).prune_instance(instance)?;
    let workspace = project.instance_workspace(instance);
    let removed_workspace = workspace.exists();
    if workspace.exists() {
        std::fs::remove_dir_all(workspace)?;
    }
    let removed = removed_container || removed_workspace;
    if removed {
        op.success();
    } else {
        output::outro(&format!("No local runtime resources found for {instance}"));
    }
    Ok(removed)
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

async fn prune_all(project: &ProjectContext, yes: bool) -> Result<Vec<(String, bool)>> {
    if !yes {
        if !prompts::is_interactive() {
            return Err(CliError::new(
                "refusing to prune every local instance without confirmation",
            )
            .with_hint("re-run with --yes to confirm")
            .into());
        }
        output::warning(
            "This removes local containers, workspaces, and Helix-managed on-disk storage volumes for every local instance. Remote S3 object-store data is not deleted.",
        );
        if !prompts::confirm("Prune every local instance?")? {
            output::info("Prune cancelled");
            return Ok(Vec::new());
        }
    }
    let mut instances: Vec<&String> = project.config.local.keys().collect();
    instances.sort();
    let mut pruned = Vec::with_capacity(instances.len());
    for instance in instances {
        pruned.push((instance.clone(), prune_one(project, instance).await?));
    }
    Ok(pruned)
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

    // A charset-valid instance name is not enough on its own: a repository can track
    // `.helix` itself as a symlink to a directory outside the project, in which case
    // `.helix/<valid name>` still resolves outside it. `symlink` is Unix-only in
    // `std::os`; the equivalent Windows primitive requires elevated privileges to
    // create, so this regression is covered on Unix only, matching how the rest of
    // the suite handles platform-specific filesystem primitives.
    #[cfg(unix)]
    #[tokio::test]
    async fn prune_one_rejects_a_symlinked_helix_dir_before_touching_the_filesystem() {
        let dir = tempfile::tempdir().unwrap();
        // The external directory a malicious `.helix` symlink points at. If the
        // symlink were followed, `remove_dir_all` would delete `external/dev`.
        let external = dir.path().join("external");
        std::fs::create_dir_all(external.join("dev")).unwrap();
        let helix_dir = dir.path().join(".helix");
        std::os::unix::fs::symlink(&external, &helix_dir).unwrap();

        let project = ProjectContext {
            root: dir.path().to_path_buf(),
            config: HelixConfig::default_config("test-project"),
            helix_dir,
        };

        let result = prune_one(&project, "dev").await;

        assert!(result.is_err());
        assert!(
            external.join("dev").exists(),
            "directory outside the project must survive"
        );
    }
}
