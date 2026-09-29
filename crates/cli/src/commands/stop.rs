use crate::local_runtime::LocalRuntime;
use crate::output::{self, Operation};
use crate::project::ProjectContext;
use eyre::Result;
use serde_json::json;

pub async fn run(instance: Option<String>) -> Result<()> {
    let project = ProjectContext::find_and_load(None)?;
    let instance = project.resolve_local_instance(instance, "Stop which local instance?")?;
    let op = Operation::new("Stopping", &instance);
    let was_running = LocalRuntime::new(&project).stop(&instance)?;
    if was_running {
        op.success();
    } else {
        output::outro(&format!("{instance} was not running"));
    }
    output::emit(
        &json!({"instance": instance, "wasRunning": was_running}),
        |_| Ok(()),
    )
}
