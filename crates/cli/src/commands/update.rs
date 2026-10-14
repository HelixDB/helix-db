use eyre::Result;
use self_update::cargo_crate_version;
use std::env;

use crate::errors::CliError;
use crate::output::{self, Step};

const V1_TARGET_VERSION: &str = "2.3.5";
const V1_TARGET_TAG: &str = "v2.3.5";

pub async fn run(force: bool, v1: bool) -> Result<()> {
    // We're using the self_update crate which is very handy but doesn't support async.
    // Still, this is good enough, but because it panics in an async context we must
    // do a spawn_blocking
    tokio::task::spawn_blocking(move || run_sync(force, v1)).await?
}

fn run_sync(force: bool, v1: bool) -> Result<()> {
    let Some(outcome) = crate::host_actions::test_update_outcome(force, v1)? else {
        return run_production_update(force, v1);
    };
    run_test_update(outcome)
}

fn run_production_update(force: bool, v1: bool) -> Result<()> {
    output::intro("Updating the Helix CLI");

    let mut check_step = Step::with_messages("Checking for updates", "Checked for updates");
    check_step.start();

    let mut update_builder = self_update::backends::github::Update::configure();
    update_builder
        .repo_owner("HelixDB")
        .repo_name("helix-db")
        .bin_name("helix")
        // The spinner below is the progress indicator; a second progress bar
        // would redraw underneath it.
        .show_download_progress(false)
        .show_output(false)
        .no_confirm(true)
        .current_version(cargo_crate_version!());

    if v1 {
        update_builder.target_version_tag(V1_TARGET_TAG);
    }

    let status = update_builder.build()?;

    let current_version = cargo_crate_version!();
    let latest_release = status.get_latest_release()?;

    if !force {
        let target_release = if v1 {
            status.get_release_version(V1_TARGET_TAG)?
        } else {
            status.get_latest_release()?
        };

        if target_release.version == current_version {
            check_step.done_with_info("already up to date");
            // Still refresh skills — `helix update` keeps the whole toolchain
            // current, even when the binary itself is already on latest.
            refresh_skills_if_installed();
            output::remark("Use --force to reinstall");
            output::outro(&format!("Already on v{current_version}"));
            return output::emit(
                &serde_json::json!({"version": current_version, "updated": false}),
                |_| Ok(()),
            );
        }

        check_step.done_with_info(&format!(
            "v{current_version} -> v{}",
            target_release.version
        ));
    } else if v1 {
        check_step.done_with_info(&format!("force update to v{V1_TARGET_VERSION}"));
    } else {
        check_step.done_with_info("force update");
    }

    if is_v3_update(current_version, &latest_release.version) {
        output::warning(
            "This updates to v3, a breaking change: existing v2 databases stop working.\nSee https://docs.helix-db.com before continuing.",
        );
    }

    let mut install_step =
        Step::with_messages("Downloading and installing", "Downloaded and installed");
    install_step.start();

    let version = match status.update() {
        Ok(updated) => {
            install_step.done();
            updated.version().to_owned()
        }
        Err(error) => {
            install_step.fail();
            output::outro_cancel("Update failed");
            return Err(CliError::new(format!("update failed: {error}"))
                .with_hint("check your internet connection and try again")
                .into());
        }
    };
    refresh_skills_if_installed();
    output::remark("Restart your terminal to use the new version");
    output::outro("Updated the Helix CLI");
    output::emit(
        &serde_json::json!({"version": version, "updated": true}),
        |_| Ok(()),
    )
}

/// Refresh the Helix agent skills as part of `helix update`, but only when they
/// were already installed (via `helix init`/`chef`/`helix skills`) and `npx` is
/// available. A skills-refresh failure degrades to a warning — it must never
/// fail the CLI self-update. Global scope only, matching how they're installed.
fn refresh_skills_if_installed() {
    if !crate::update::skills_installed()
        || !crate::external_tools::available(crate::external_tools::ExternalTool::Npx)
    {
        return;
    }

    let project_dir = env::current_dir().unwrap_or_else(|_| ".".into());
    match crate::setup::install_skills(&project_dir, true, true) {
        Ok(()) => crate::update::record_skills_refreshed(),
        Err(e) => output::warning(&format!("Skipping Helix skills refresh: {e}")),
    }
}

fn run_test_update(outcome: crate::host_actions::TestUpdateOutcome) -> Result<()> {
    match outcome {
        crate::host_actions::TestUpdateOutcome::Updated => {
            output::success("CLI updated successfully");
            output::emit(&serde_json::json!({"updated": true}), |_| Ok(()))
        }
        crate::host_actions::TestUpdateOutcome::Unchanged => {
            output::info("CLI is already up to date");
            output::emit(&serde_json::json!({"updated": false}), |_| Ok(()))
        }
        crate::host_actions::TestUpdateOutcome::Error => {
            Err(eyre::eyre!("simulated CLI update failure"))
        }
    }
}

fn is_v3_update(current_version: &str, latest_version: &str) -> bool {
    let current_version = current_version.trim_start_matches('v');
    let latest_version = latest_version.trim_start_matches('v');

    !current_version.starts_with("3.") && latest_version.starts_with("3.")
}
