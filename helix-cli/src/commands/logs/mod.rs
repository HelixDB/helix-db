//! `helix logs` command for viewing instance logs.
//!
//! Supports two modes:
//! - CLI mode (with flags): Non-interactive log streaming/querying
//! - TUI mode (no flags): Interactive terminal UI with tabs and hotkeys

mod cli;
mod log_source;
mod tui;

use crate::commands::auth::require_auth;
use crate::config::InstanceInfo;
use crate::project::ProjectContext;
use crate::prompts;
use eyre::{Result, eyre};
use log_source::LogSource;

/// Run the logs command.
pub async fn run(
    instance: Option<String>,
    live: bool,
    range: bool,
    start: Option<String>,
    end: Option<String>,
) -> Result<()> {
    // Load project context
    let project = ProjectContext::find_and_load(None)?;

    // Get instance name - prompt if not provided
    let instance_name = match instance {
        Some(name) => name,
        None if prompts::is_interactive() => {
            let instances = project.config.list_instances_with_types();
            prompts::intro("helix logs", Some("View logs for your instance\n"))?;
            prompts::select_instance(&instances)?
        }
        None => {
            let instances = project.config.list_instances();
            return Err(eyre!(
                "No instance specified. Available instances: {}",
                instances
                    .into_iter()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
    };

    // Get instance config
    let instance_config = project.config.get_instance(&instance_name)?;

    // Check auth early for Helix Cloud instances
    let credentials = if let InstanceInfo::Helix(_) = &instance_config {
        Some(require_auth().await?)
    } else {
        None
    };

    // Create log source
    let log_source = LogSource::from_instance(&project, &instance_name, credentials.as_ref())?;

    // Route to appropriate mode
    if live {
        cli::stream_live(&log_source).await
    } else if range {
        cli::query_range(&log_source, start, end).await
    } else {
        // TUI mode (default when no flags)
        tui::run(log_source, instance_name).await
    }
}
