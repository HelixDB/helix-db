use crate::errors::CliError;
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
    // failure to remove it never fails the stop, but it is reported.
    let (explorer_stopped, explorer_error) = match runtime.remove_explorer(&instance) {
        Ok(stopped) => (stopped, None),
        Err(error) => {
            let cause = one_line(&CliError::from_report(&error).to_string());
            output::warning(&format!(
                "Could not stop the {instance} Explorer ({cause}); retry with \
                 `helix explorer {instance} --stop`"
            ));
            (false, Some(cause))
        }
    };
    if explorer_stopped {
        output::step(&format!("Stopped the {instance} Explorer"));
    }
    if was_running {
        op.success();
    } else {
        output::outro(&format!("{instance} was not running"));
    }
    let mut report = json!({
        "instance": instance,
        "wasRunning": was_running,
        "explorerStopped": explorer_stopped,
    });
    if let Some(error) = explorer_error {
        report["explorerError"] = error.into();
    }
    output::emit(&report, |_| Ok(()))
}

/// Runtime errors carry the runtime's multi-line stderr; a warning reads
/// best on one line.
fn one_line(message: &str) -> String {
    message.split_whitespace().collect::<Vec<_>>().join(" ")
}
