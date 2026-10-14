use crate::config::InstanceInfo;
use crate::local_runtime::LocalRuntime;
use crate::output::Operation;
use crate::project::ProjectContext;
use crate::{errors::CliError, output, prompts};
use eyre::{eyre, Result};

pub async fn run(instance: String, yes: bool) -> Result<()> {
    let mut project = ProjectContext::find_and_load(None)?;
    let info = project.config.get_instance(&instance)?;
    if project.config.local.len() + project.config.enterprise.len() == 1 {
        return Err(eyre!(
            "Cannot delete the final instance '{instance}'. Add a replacement instance first."
        ));
    }
    if !yes {
        if !prompts::is_interactive() {
            return Err(CliError::new(format!(
                "refusing to delete '{instance}' without confirmation"
            ))
            .with_hint("re-run with --yes to confirm")
            .into());
        }
        output::warning(&format!(
            "This removes '{instance}' from helix.toml and cleans its local runtime state, including Helix-managed on-disk storage volumes. Remote S3 object-store data is not deleted."
        ));
        if !prompts::confirm(&format!("Delete instance '{instance}'?"))? {
            output::info("Deletion cancelled");
            return Ok(());
        }
    }

    let op = Operation::new("Deleting", &instance);
    if matches!(info, InstanceInfo::Local(_)) {
        let _ = LocalRuntime::new(&project).prune_instance(&instance);
    }

    project.config.local.remove(&instance);
    project.config.enterprise.remove(&instance);
    project
        .config
        .save_to_file(&project.root.join("helix.toml"))?;

    project.assert_safe_helix_dir()?;
    let workspace = project.instance_workspace(&instance);
    if workspace.exists() {
        std::fs::remove_dir_all(workspace)?;
    }

    op.success();
    output::emit(&serde_json::json!({"deleted": instance}), |_| Ok(()))
}
