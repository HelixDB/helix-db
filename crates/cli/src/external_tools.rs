//! Typed construction of subprocesses used by the CLI.
//!
//! `HELIX_TEST_TOOL_DIR` is intentionally process-scoped: black-box tests set
//! it only on the spawned `helix` command, avoiding shared environment state.

use crate::utils::command_exists;
use std::ffi::OsString;
use std::path::PathBuf;
use std::process::Command;
use tokio::process::Command as TokioCommand;

const TEST_TOOL_DIR_ENV: &str = "HELIX_TEST_TOOL_DIR";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExternalTool {
    Cargo,
    Node,
    Npm,
    Npx,
    Curl,
    ClaudeCode,
    OpenAiCodex,
    OpenCode,
}

impl ExternalTool {
    pub(crate) const fn binary(self) -> &'static str {
        match self {
            Self::Cargo => "cargo",
            Self::Node => "node",
            Self::Npm => "npm",
            Self::Npx => "npx",
            Self::Curl => "curl",
            Self::ClaudeCode => "claude",
            Self::OpenAiCodex => "codex",
            Self::OpenCode => "opencode",
        }
    }

    fn test_path(self) -> Option<PathBuf> {
        let directory = std::env::var_os(TEST_TOOL_DIR_ENV).map(PathBuf::from)?;
        let mut name = self.binary().to_string();
        if cfg!(windows) {
            name.push_str(".cmd");
        }
        Some(directory.join(name))
    }
}

pub(crate) fn available(tool: ExternalTool) -> bool {
    match tool.test_path() {
        Some(path) => path.is_file(),
        None => command_exists(tool.binary()),
    }
}

pub(crate) fn command(tool: ExternalTool) -> Command {
    match tool.test_path() {
        Some(path) => command_from_test_path(path.into_os_string()),
        None => Command::new(tool.binary()),
    }
}

pub(crate) fn tokio_command(tool: ExternalTool) -> TokioCommand {
    match tool.test_path() {
        Some(path) => tokio_command_from_test_path(path.into_os_string()),
        None => TokioCommand::new(tool.binary()),
    }
}

#[cfg(windows)]
fn command_from_test_path(path: OsString) -> Command {
    let mut command = Command::new("cmd");
    command.arg("/C").arg("call").arg(path);
    command
}

#[cfg(not(windows))]
fn command_from_test_path(path: OsString) -> Command {
    Command::new(path)
}

#[cfg(windows)]
fn tokio_command_from_test_path(path: OsString) -> TokioCommand {
    let mut command = TokioCommand::new("cmd");
    command.arg("/C").arg("call").arg(path);
    command
}

#[cfg(not(windows))]
fn tokio_command_from_test_path(path: OsString) -> TokioCommand {
    TokioCommand::new(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_binary_names_are_stable() {
        assert_eq!(ExternalTool::Cargo.binary(), "cargo");
        assert_eq!(ExternalTool::Node.binary(), "node");
        assert_eq!(ExternalTool::Npm.binary(), "npm");
        assert_eq!(ExternalTool::Npx.binary(), "npx");
        assert_eq!(ExternalTool::Curl.binary(), "curl");
        assert_eq!(ExternalTool::ClaudeCode.binary(), "claude");
        assert_eq!(ExternalTool::OpenAiCodex.binary(), "codex");
        assert_eq!(ExternalTool::OpenCode.binary(), "opencode");
    }
}
