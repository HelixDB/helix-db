use crate::local_runtime::LocalRuntime;
use crate::output::{self, Operation};
use crate::project::ProjectContext;
use eyre::Result;
use serde_json::json;

pub async fn run(instance: Option<String>) -> Result<()> {
    let project = ProjectContext::find_and_load(None)?;
    let instance = project.resolve_local_instance(instance, "Stop which local instance?")?;
    let op = Operation::new("Stopping", &instance);
    let runtime = LocalRuntime::new(&project);
    let was_running = runtime.stop(&instance)?;
    // The Explorer only reads this instance, so it stops too. Best effort: a
    // failure to remove it never fails the stop.
    let explorer_stopped = runtime.remove_explorer(&instance).unwrap_or(false);
    if explorer_stopped {
        output::step(&format!("Stopped the {instance} Explorer"));
    }
    if was_running {
        op.success();
    } else {
        output::outro(&format!("{instance} was not running"));
    }
    output::emit(
        &json!({
            "instance": instance,
            "wasRunning": was_running,
            "explorerStopped": explorer_stopped,
        }),
        |_| Ok(()),
    )
}
