use eyre::Result;
use self_update::cargo_crate_version;

use crate::utils::{print_error_with_hint, print_status, print_success};

pub async fn run(force: bool) -> Result<()> {
    print_status("UPDATE", "Checking for updates...");

    let status = self_update::backends::github::Update::configure()
        .repo_owner("HelixDB")
        .repo_name("helix-db")
        .bin_name("helix")
        .show_download_progress(true)
        .current_version(cargo_crate_version!())
        .build()?;

    let current_version = cargo_crate_version!();

    if !force {
        let latest_release = status.get_latest_release()?;
        if latest_release.version == current_version {
            print_success(&format!("Already up to date! (v{current_version})"));
            print_status("UPDATE", "Use --force to reinstall");
            return Ok(());
        }

        print_status(
            "UPDATE",
            &format!(
                "Update available: v{current_version} → v{}",
                latest_release.version
            ),
        );
    } else {
        print_status("UPDATE", "Force update requested");
    }

    print_status("UPDATE", "Downloading and installing latest version...");

    match status.update() {
        Ok(_) => {
            print_success("Update completed successfully!");
            print_status(
                "UPDATE",
                "Please restart your terminal or run the command again to use the new version",
            );
        }
        Err(e) => {
            print_error_with_hint(
                &format!("Update failed: {e}"),
                "check your internet connection and try again",
            );
            return Err(e.into());
        }
    }

    Ok(())
}
