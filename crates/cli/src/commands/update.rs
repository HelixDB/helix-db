use color_eyre::owo_colors::OwoColorize;
use eyre::Result;
use self_update::cargo_crate_version;
use std::env;

use crate::output::{Operation, Step, Verbosity};
use crate::utils::print_error_with_hint;

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
    let op = Operation::new("Updating", "CLI");

    let mut check_step = Step::with_messages("Checking for updates", "Checked for updates");
    check_step.start();

    let mut update_builder = self_update::backends::github::Update::configure();
    update_builder
        .repo_owner("HelixDB")
        .repo_name("helix-db")
        .bin_name("helix")
        .show_download_progress(true)
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
            op.success();
            println!("  Use --force to reinstall");
            // Still refresh skills — `helix update` keeps the whole toolchain
            // current, even when the binary itself is already on latest.
            refresh_skills_if_installed();
            return Ok(());
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
        print_v3_update_warning();
    }

    let mut install_step =
        Step::with_messages("Downloading and installing", "Downloaded and installed");
    install_step.start();

    match status.update() {
        Ok(_) => {
            install_step.done();
            op.success();
            if Verbosity::current().show_normal() {
                Operation::print_details(&[(
                    "Note",
                    "Please restart your terminal to use the new version",
                )]);
            }
        }
        Err(e) => {
            install_step.fail();
            op.failure();
            print_error_with_hint(
                &format!("Update failed: {e}"),
                "check your internet connection and try again",
            );
            return Err(e.into());
        }
    }

    refresh_skills_if_installed();

    Ok(())
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
        Err(e) => crate::output::warning(&format!("Skipping Helix skills refresh: {e}")),
    }
}

fn run_test_update(outcome: crate::host_actions::TestUpdateOutcome) -> Result<()> {
    match outcome {
        crate::host_actions::TestUpdateOutcome::Updated => {
            crate::output::success("CLI updated successfully");
            Ok(())
        }
        crate::host_actions::TestUpdateOutcome::Unchanged => {
            crate::output::info("CLI is already up to date");
            Ok(())
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

fn print_v3_update_warning() {
    if !Verbosity::current().show_normal() {
        return;
    }

    println!();
    println!(
        "{}",
        "WARNING: Updating from this version will update to v3, which is a breaking change."
            .yellow()
            .bold()
    );
    println!(
        "{}",
        "All existing databases will cease to work and only the Helix v2 DB will be available."
            .yellow()
            .bold()
    );
    println!(
        "{}",
        "For more information, see https://docs.helix-db.com"
            .yellow()
            .bold()
    );
}
