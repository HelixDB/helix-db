//! Host-level effects that must be recorded rather than executed in tests.

use eyre::{eyre, Result};
use serde::Serialize;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;

const TEST_HOST_ACTION_LOG_ENV: &str = "HELIX_TEST_HOST_ACTION_LOG";
const TEST_UPDATE_OUTCOME_ENV: &str = "HELIX_TEST_UPDATE_OUTCOME";
const TEST_CHEF_PERMISSION_MODE_ENV: &str = "HELIX_TEST_CHEF_PERMISSION_MODE";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum HostAction<'a> {
    OpenUrl { url: &'a str },
    Update { v1: bool, force: bool },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TestUpdateOutcome {
    Updated,
    Unchanged,
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TestChefPermissionMode {
    FullAuto,
    Scoped,
    Skip,
}

pub(crate) fn open_url(url: &str) -> Result<()> {
    if record_action(&HostAction::OpenUrl { url })? {
        return Ok(());
    }
    open::that(url)?;
    Ok(())
}

pub(crate) fn test_update_outcome(force: bool, v1: bool) -> Result<Option<TestUpdateOutcome>> {
    let Some(raw) = std::env::var_os(TEST_UPDATE_OUTCOME_ENV) else {
        return Ok(None);
    };
    record_action(&HostAction::Update { v1, force })?;
    match raw.to_string_lossy().as_ref() {
        "updated" => Ok(Some(TestUpdateOutcome::Updated)),
        "unchanged" => Ok(Some(TestUpdateOutcome::Unchanged)),
        "error" => Ok(Some(TestUpdateOutcome::Error)),
        value => Err(eyre!(
            "invalid {TEST_UPDATE_OUTCOME_ENV} value '{value}'; expected updated, unchanged, or error"
        )),
    }
}

pub(crate) fn test_chef_permission_mode() -> Result<Option<TestChefPermissionMode>> {
    let Some(raw) = std::env::var_os(TEST_CHEF_PERMISSION_MODE_ENV) else {
        return Ok(None);
    };
    match raw.to_string_lossy().as_ref() {
        "full_auto" => Ok(Some(TestChefPermissionMode::FullAuto)),
        "scoped" => Ok(Some(TestChefPermissionMode::Scoped)),
        "skip" => Ok(Some(TestChefPermissionMode::Skip)),
        value => Err(eyre!(
            "invalid {TEST_CHEF_PERMISSION_MODE_ENV} value '{value}'; expected full_auto, scoped, or skip"
        )),
    }
}

fn record_action(action: &HostAction<'_>) -> Result<bool> {
    let Some(path) = std::env::var_os(TEST_HOST_ACTION_LOG_ENV).map(PathBuf::from) else {
        return Ok(false);
    };
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    serde_json::to_writer(&mut file, action)?;
    writeln!(file)?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_action_json_is_structured() {
        let json = serde_json::to_value(HostAction::OpenUrl {
            url: "https://example.com",
        })
        .unwrap();
        assert_eq!(json["type"], "open_url");
        assert_eq!(json["url"], "https://example.com");
    }

    #[test]
    fn chef_permission_mode_is_absent_without_test_override() {
        assert_eq!(test_chef_permission_mode().unwrap(), None);
    }
}
