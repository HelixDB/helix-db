//! `helix skills` — manage the Helix agent skills installed via the `skills`
//! CLI (`npx skills`). `init`/`chef` install them on setup; this command group
//! lets users install, refresh, and inspect them afterwards.

use eyre::Result;
use std::env;

use crate::errors::CliError;
use crate::external_tools::{self, ExternalTool};
use crate::SkillsAction;

pub async fn run(action: SkillsAction) -> Result<()> {
    if !external_tools::available(ExternalTool::Npx) {
        return Err(CliError::new("npx not found")
            .with_hint(
                "The Helix skills are managed with the `skills` CLI, which needs Node.js/npm. \
                 Install Node.js, then re-run this command.",
            )
            .into());
    }

    // The `skills` CLI resolves global vs project scope itself; project scope is
    // relative to the current directory, so run from cwd.
    let project_dir = env::current_dir()?;

    match action {
        SkillsAction::Install { project } => {
            // Interactive install (no -y) so the user sees the skills CLI prompts,
            // matching `npx skills add HelixDB/skills` run by hand.
            crate::setup::install_skills(&project_dir, false, !project)?;
            crate::update::record_skills_refreshed();
        }
        SkillsAction::Update { project } => {
            // Forced, non-interactive refresh of every Helix skill from source.
            crate::setup::install_skills(&project_dir, true, !project)?;
            crate::update::record_skills_refreshed();
        }
        SkillsAction::List { project } => {
            crate::setup::list_skills(&project_dir, !project)?;
        }
    }

    Ok(())
}
