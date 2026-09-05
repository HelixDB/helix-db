use crate::config::InstanceInfo;
use crate::local_runtime::LocalRuntime;
use crate::output::Operation;
use crate::project::ProjectContext;
use crate::utils::{print_confirm, print_warning};
use eyre::{eyre, Result};
use std::io::IsTerminal;

pub async fn run(instance: String, yes: bool) -> Result<()> {
    let mut project = ProjectContext::find_and_load(None)?;
    let info = project.config.get_instance(&instance)?;
    if project.config.local.len() + project.config.enterprise.len() == 1 {
        return Err(eyre!(
            "Cannot delete the final instance '{instance}'. Add a replacement instance first."
        ));
    }
    print_warning(&format!(
        "This will remove instance '{instance}' from helix.toml and clean local runtime state, including Helix-managed on-disk storage volumes if present. Remote S3 object-store data is not deleted."
    ));
    if !yes && !std::io::stdin().is_terminal() {
        return Err(eyre!(
            "Refusing to delete '{instance}' non-interactively. Re-run with --yes to confirm."
        ));
    }
    if !yes && !print_confirm("Continue?")? {
        crate::output::info("Deletion cancelled");
        return Ok(());
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
    Ok(())
}
