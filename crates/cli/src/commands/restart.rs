use crate::local_runtime::LocalRuntime;
use crate::output::{self, Operation};
use crate::project::ProjectContext;
use eyre::Result;
use serde_json::json;

pub async fn run(instance: Option<String>) -> Result<()> {
    let project = ProjectContext::find_and_load(None)?;
    let _ = dotenvy::from_path(project.root.join(".env"));
    let instance = project.resolve_local_instance(instance, "Restart which local instance?")?;
    let op = Operation::new("Restarting", &instance);
    LocalRuntime::new(&project).restart(&instance)?;
    op.success();
    output::emit(&json!({"instance": instance}), |_| Ok(()))
}
