//! Shared project-setup helpers used by both `helix init` and `helix chef`:
//! installing the Helix agent skills + docs MCP via `npx`, and warning when no
//! container runtime is available.

use crate::config::ContainerRuntime;
use crate::external_tools::{self, ExternalTool};
use crate::local_runtime::LocalRuntime;
use crate::output::{Step, Verbosity};
use crate::utils::command_exists;
use eyre::{eyre, Result};
use std::path::Path;
use std::process::Stdio;

const HELIX_DOCS_MCP_URL: &str = "https://docs.helix-db.com/mcp";

// add-mcp errors out non-zero when an incompatible agent is detected. Claude Desktop
// only supports local stdio servers, so an http MCP install aborts the whole run.
// Pin the agent list to the http-capable subset of `add-mcp list-agents`.
const MCP_HTTP_COMPATIBLE_AGENTS: &[&str] = &[
    "antigravity",
    "claude-code",
    "cline",
    "cline-cli",
    "codex",
    "cursor",
    "gemini-cli",
    "github-copilot-cli",
    "goose",
    "mcporter",
    "opencode",
    "vscode",
    "zed",
];

/// Build the `npx skills add HelixDB/skills` argument list.
///
/// `automatic` adds the `-y`/`--skill *` flags so npx runs without prompting;
/// otherwise the user sees skills' own interactive prompts.
fn skills_install_args(automatic: bool, global: bool) -> Vec<&'static str> {
    let mut args = if automatic {
        vec![
            "-y",
            "skills",
            "add",
            "HelixDB/skills",
            "--skill",
            "*",
            "-y",
        ]
    } else {
        vec!["skills", "add", "HelixDB/skills"]
    };
    if global {
        args.push("-g");
    }
    args
}

/// Build the `npx skills list` argument list.
///
/// `skills list` defaults to project scope, so `-g` is what surfaces the
/// globally-installed Helix skills (the default for `helix init`/`chef`).
fn skills_list_args(global: bool) -> Vec<&'static str> {
    let mut args = vec!["-y", "skills", "list"];
    if global {
        args.push("-g");
    }
    args
}

/// Build the `npx add-mcp <docs url>` argument list.
fn mcp_install_args(automatic: bool, global: bool) -> Vec<&'static str> {
    let mut args = if automatic {
        let mut args = vec![
            "-y",
            "add-mcp",
            HELIX_DOCS_MCP_URL,
            "--name",
            "helixdb-docs",
            "-y",
        ];
        for agent in MCP_HTTP_COMPATIBLE_AGENTS {
            args.push("-a");
            args.push(agent);
        }
        args
    } else {
        vec!["add-mcp", HELIX_DOCS_MCP_URL, "--name", "helixdb-docs"]
    };
    if global {
        args.push("-g");
    }
    args
}

/// Install the Helix agent skills with `npx skills add HelixDB/skills`.
pub(crate) fn install_skills(project_dir: &Path, automatic: bool, global: bool) -> Result<()> {
    let args = skills_install_args(automatic, global);
    run_external_command(
        project_dir,
        "Installing Helix skills",
        "npx",
        &args,
        automatic,
    )
}

/// List installed agent skills with `npx skills list`.
pub(crate) fn list_skills(project_dir: &Path, global: bool) -> Result<()> {
    let args = skills_list_args(global);
    run_external_command(project_dir, "Listing skills", "npx", &args, false)
}

/// Install the Helix docs MCP with `npx add-mcp`.
pub(crate) fn install_mcp(project_dir: &Path, automatic: bool, global: bool) -> Result<()> {
    let args = mcp_install_args(automatic, global);
    run_external_command(
        project_dir,
        "Installing Helix docs MCP",
        "npx",
        &args,
        automatic,
    )
}

pub(crate) fn run_external_command(
    project_dir: &Path,
    description: &str,
    program: &str,
    args: &[&str],
    quiet: bool,
) -> Result<()> {
    let quiet = quiet && Verbosity::current() != Verbosity::Verbose;

    let tool = match program {
        "npx" => ExternalTool::Npx,
        _ => return Err(eyre!("unsupported external setup command: {program}")),
    };
    let mut step = Step::with_messages(description, description);
    step.start();

    if quiet {
        let output = external_tools::command(tool)
            .args(args)
            .current_dir(project_dir)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()?;
        if !output.status.success() {
            step.fail();
            if !output.stdout.is_empty() {
                eprintln!("{}", String::from_utf8_lossy(&output.stdout));
            }
            if !output.stderr.is_empty() {
                eprintln!("{}", String::from_utf8_lossy(&output.stderr));
            }
            return Err(eyre!("{description} failed with status {}", output.status));
        }
    } else {
        let status = external_tools::command(tool)
            .args(args)
            .current_dir(project_dir)
            .stdin(Stdio::inherit())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status()?;
        if !status.success() {
            step.fail();
            return Err(eyre!("{description} failed with status {status}"));
        }
    }

    step.done();
    Ok(())
}

/// Detect an installed container runtime, preferring Docker (which also covers
/// Docker-compatible daemons like OrbStack and Colima) over Podman.
pub(crate) fn detect_runtime() -> Result<ContainerRuntime> {
    if command_exists("docker") {
        Ok(ContainerRuntime::Docker)
    } else if command_exists("podman") {
        Ok(ContainerRuntime::Podman)
    } else {
        Err(eyre!("Docker or Podman is required"))
    }
}

/// Warn (never error) if no container runtime is installed or it isn't running.
///
/// Lets `init`/`chef` give an early heads-up without blocking project scaffolding —
/// users can install or start a runtime before `helix start`.
pub(crate) fn warn_if_container_runtime_unavailable() {
    match detect_runtime() {
        Err(_) => crate::output::warning(
            "No container runtime found. Install Docker, OrbStack, Podman, or Colima \
             to run local Helix instances with 'helix start'.",
        ),
        Ok(runtime) => {
            // Quick, non-blocking probe — must not trigger daemon auto-start
            // (which can block for up to 120s) during project scaffolding.
            if !LocalRuntime::is_running(runtime) {
                crate::output::warning(&format!(
                    "{} is installed but not running. Start it before 'helix start' — \
                     `open -a Docker` / `colima start` on macOS, `sudo systemctl start docker` \
                     (or `sudo dockerd &`) on Linux/headless.",
                    runtime.label()
                ));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skills_install_args_automatic_global() {
        let args = skills_install_args(true, true);
        assert_eq!(args[0], "-y");
        assert!(args.contains(&"skills"));
        assert!(args.contains(&"add"));
        assert!(args.contains(&"HelixDB/skills"));
        assert!(args.contains(&"--skill"));
        assert!(args.contains(&"*"));
        assert_eq!(args.last(), Some(&"-g"));
    }

    #[test]
    fn skills_install_args_automatic_project_local() {
        let args = skills_install_args(true, false);
        assert!(!args.contains(&"-g"));
        assert!(args.contains(&"HelixDB/skills"));
    }

    #[test]
    fn skills_install_args_manual_global() {
        let args = skills_install_args(false, true);
        // Manual mode skips the -y flags so the user sees CLI prompts.
        assert!(!args.contains(&"-y"));
        assert!(!args.contains(&"--skill"));
        assert!(args.contains(&"-g"));
    }

    #[test]
    fn skills_install_args_manual_project_local() {
        let args = skills_install_args(false, false);
        assert!(!args.contains(&"-g"));
        assert!(!args.contains(&"-y"));
    }

    #[test]
    fn skills_list_args_global() {
        let args = skills_list_args(true);
        assert_eq!(args[0], "-y");
        assert!(args.contains(&"skills"));
        assert!(args.contains(&"list"));
        assert_eq!(args.last(), Some(&"-g"));
    }

    #[test]
    fn skills_list_args_project() {
        let args = skills_list_args(false);
        assert!(args.contains(&"list"));
        assert!(!args.contains(&"-g"));
    }

    #[test]
    fn mcp_install_args_automatic_global() {
        let args = mcp_install_args(true, true);
        assert_eq!(args[0], "-y");
        assert!(args.contains(&"add-mcp"));
        assert!(args.contains(&HELIX_DOCS_MCP_URL));
        assert!(args.contains(&"helixdb-docs"));
        assert!(args.contains(&"-g"));
    }

    #[test]
    fn mcp_install_args_automatic_project_local() {
        let args = mcp_install_args(true, false);
        assert!(!args.contains(&"-g"));
        assert!(args.contains(&HELIX_DOCS_MCP_URL));
    }

    #[test]
    fn mcp_install_args_manual_global() {
        let args = mcp_install_args(false, true);
        assert!(!args.contains(&"-y"));
        assert!(args.contains(&"-g"));
    }

    #[test]
    fn mcp_install_args_manual_project_local() {
        let args = mcp_install_args(false, false);
        assert!(!args.contains(&"-g"));
        assert!(!args.contains(&"-y"));
    }
}
