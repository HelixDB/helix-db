use crate::config::DEFAULT_LOCAL_PORT;
use crate::{AddTarget, InitTarget};
use eyre::{eyre, Result};
use std::io::IsTerminal;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InstanceKind {
    Local,
    Enterprise,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ProjectDirectoryChoice {
    Current,
    Other(PathBuf),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatusSelection {
    All,
    Instance(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PruneSelection {
    All,
    Instance(String),
}

pub fn is_interactive() -> bool {
    std::io::stdin().is_terminal() && std::io::stdout().is_terminal()
}

/// Asks where a new project should be created.
///
/// The current directory is the default choice. Choosing another directory
/// accepts both relative and absolute paths; relative paths remain relative to
/// the process's current working directory.
pub fn select_init_project_dir(current_dir: &Path) -> Result<PathBuf> {
    let use_current_dir = cliclack::select("Where should the project be created?")
        .item(true, "Current directory", current_dir.display().to_string())
        .item(false, "Another directory", "Create or use a different path")
        .interact()?;

    let choice = if use_current_dir {
        ProjectDirectoryChoice::Current
    } else {
        let path: String = cliclack::input("Project directory")
            .placeholder("./my-helix-project")
            .validate(|input: &String| {
                if input.trim().is_empty() {
                    Err("path cannot be empty")
                } else {
                    Ok(())
                }
            })
            .interact()?;
        ProjectDirectoryChoice::Other(PathBuf::from(path.trim()))
    };

    Ok(resolve_project_directory(choice, current_dir))
}

fn resolve_project_directory(choice: ProjectDirectoryChoice, current_dir: &Path) -> PathBuf {
    match choice {
        ProjectDirectoryChoice::Current => current_dir.to_path_buf(),
        ProjectDirectoryChoice::Other(path) => path,
    }
}

pub fn confirm(message: &str) -> Result<bool> {
    Ok(cliclack::confirm(message).interact()?)
}

pub fn input_instance_name(default: &str) -> Result<String> {
    input_name("Instance name", default)
}

fn input_project_instance_name(default: &str) -> Result<String> {
    input_name("Instance name", default)
}

/// Prompts for a name, enforcing the exact same charset and length limit
/// [`crate::config::validate_instance_name`] enforces on a `--name` passed on the
/// command line and [`crate::config::HelixConfig::validate`] enforces on a name
/// loaded from `helix.toml`. All three sources go through one shared validator so
/// a name typed here can never be rejected later, after it's already been saved.
fn input_name(label: &str, default: &str) -> Result<String> {
    let name: String = cliclack::input(label)
        .default_input(default)
        .placeholder(default)
        .validate(|input: &String| crate::config::validate_instance_name(input))
        .interact()?;
    Ok(name)
}

pub fn input_port(default: u16) -> Result<u16> {
    let default = default.to_string();
    let port: String = cliclack::input("Local gateway port")
        .default_input(&default)
        .placeholder(&default)
        .validate(|input: &String| match input.parse::<u16>() {
            Ok(port) if port > 0 => Ok(()),
            _ => Err("please enter a valid TCP port"),
        })
        .interact()?;
    Ok(port.parse().unwrap_or(DEFAULT_LOCAL_PORT))
}

pub fn select_local_disk_mode() -> Result<bool> {
    Ok(cliclack::select("Local storage mode")
        .item(
            false,
            "In-memory",
            "Fast startup; data is wiped when the runtime stops or restarts",
        )
        .item(
            true,
            "On-disk",
            "Persists local data with a MinIO-backed disk volume",
        )
        .interact()?)
}

pub fn input_required(label: &str) -> Result<String> {
    let value: String = cliclack::input(label)
        .validate(|input: &String| {
            if input.trim().is_empty() {
                Err("value cannot be empty")
            } else {
                Ok(())
            }
        })
        .interact()?;
    Ok(value)
}

pub fn input_optional(label: &str) -> Result<Option<String>> {
    let value: String = cliclack::input(label)
        .placeholder("leave blank to skip")
        .interact()?;
    let value = value.trim();
    if value.is_empty() {
        Ok(None)
    } else {
        Ok(Some(value.to_string()))
    }
}

fn select_instance_kind(prompt: &str) -> Result<InstanceKind> {
    Ok(cliclack::select(prompt)
        .item(
            InstanceKind::Local,
            "Local",
            "Run a local v2 Enterprise dev instance",
        )
        .item(
            InstanceKind::Enterprise,
            "Enterprise Cloud",
            "Link an Enterprise Cloud runtime",
        )
        .interact()?)
}

pub fn select_init_target() -> Result<InitTarget> {
    match select_instance_kind("What kind of Helix instance should this project use?")? {
        InstanceKind::Local => {
            let name = input_project_instance_name("dev")?;
            let port = input_port(DEFAULT_LOCAL_PORT)?;
            let disk = select_local_disk_mode()?;
            Ok(InitTarget::Local {
                name,
                port,
                disk,
                s3: Default::default(),
                // Skills install is decided by a separate interactive prompt.
                skills: false,
                no_skills: false,
            })
        }
        InstanceKind::Enterprise => Ok(InitTarget::Enterprise {
            name: input_project_instance_name("production")?,
            database: None,
            project: None,
            workspace: None,
            skills: false,
            no_skills: false,
        }),
    }
}

pub fn select_add_target() -> Result<AddTarget> {
    match select_instance_kind("What kind of instance should be added?")? {
        InstanceKind::Local => {
            let name = input_instance_name("dev")?;
            let port = input_port(DEFAULT_LOCAL_PORT)?;
            let disk = select_local_disk_mode()?;
            Ok(AddTarget::Local {
                name,
                port,
                disk,
                s3: Default::default(),
            })
        }
        InstanceKind::Enterprise => Ok(AddTarget::Enterprise {
            name: input_instance_name("production")?,
            database: None,
            project: None,
            workspace: None,
        }),
    }
}

pub fn select_instance(instances: &[(String, String)], prompt: &str) -> Result<String> {
    if instances.is_empty() {
        return Err(eyre!("No instances found in helix.toml"));
    }
    if instances.len() == 1 {
        return Ok(instances[0].0.clone());
    }

    let mut select = cliclack::select(prompt);
    for (name, hint) in instances {
        select = select.item(name.clone(), name.as_str(), hint.as_str());
    }
    Ok(select.interact()?)
}

pub fn select_status(instances: &[(String, String)]) -> Result<StatusSelection> {
    if instances.is_empty() {
        return Ok(StatusSelection::All);
    }

    let all = "__all__".to_string();
    let mut select = cliclack::select("Show status for which instance?").item(
        all.clone(),
        "All instances",
        "Show every local and Enterprise instance",
    );
    for (name, hint) in instances {
        select = select.item(name.clone(), name.as_str(), hint.as_str());
    }
    let selected: String = select.interact()?;
    if selected == all {
        Ok(StatusSelection::All)
    } else {
        Ok(StatusSelection::Instance(selected))
    }
}

pub fn select_prune(local_instances: &[(String, String)]) -> Result<PruneSelection> {
    if local_instances.is_empty() {
        return Err(eyre!("No local instances found in helix.toml"));
    }

    let all = "__all__".to_string();
    let mut select = cliclack::select("Prune which local runtime resources?").item(
        all.clone(),
        "All local instances",
        "Remove containers and workspaces for every local instance",
    );
    for (name, hint) in local_instances {
        select = select.item(name.clone(), name.as_str(), hint.as_str());
    }
    let selected: String = select.interact()?;
    if selected == all {
        Ok(PruneSelection::All)
    } else {
        Ok(PruneSelection::Instance(selected))
    }
}

pub fn select_workspace(workspaces: &[(String, String, String)]) -> Result<String> {
    if workspaces.is_empty() {
        return Err(eyre!("No workspaces found"));
    }
    if workspaces.len() == 1 {
        return Ok(workspaces[0].0.clone());
    }

    let mut select = cliclack::select("Select a workspace");
    for (id, name, slug) in workspaces {
        select = select.item(id.clone(), name.as_str(), format!("slug: {slug}").as_str());
    }
    Ok(select.interact()?)
}

pub fn select_project(projects: &[(String, String)]) -> Result<String> {
    if projects.is_empty() {
        return Err(eyre!("No projects found in this workspace"));
    }
    if projects.len() == 1 {
        return Ok(projects[0].0.clone());
    }

    let mut select = cliclack::select("Select a project");
    for (id, name) in projects {
        let short_id = if id.len() > 8 { &id[..8] } else { id.as_str() };
        select = select.item(
            id.clone(),
            name.as_str(),
            format!("id: {short_id}").as_str(),
        );
    }
    Ok(select.interact()?)
}

pub fn select_cluster(clusters: &[(String, String, String)]) -> Result<String> {
    if clusters.is_empty() {
        return Err(eyre!("No Enterprise clusters found"));
    }
    if clusters.len() == 1 {
        return Ok(clusters[0].0.clone());
    }

    let mut select = cliclack::select("Select an Enterprise cluster");
    for (id, name, hint) in clusters {
        select = select.item(id.clone(), name.as_str(), hint.as_str());
    }
    Ok(select.interact()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_project_directory_choice_uses_current_directory() {
        let current_dir = Path::new("/work/existing-project");

        let selected = resolve_project_directory(ProjectDirectoryChoice::Current, current_dir);

        assert_eq!(selected, current_dir);
    }

    #[test]
    fn other_project_directory_choice_preserves_the_entered_path() {
        let current_dir = Path::new("/work");
        let entered = PathBuf::from("new-project");

        let selected =
            resolve_project_directory(ProjectDirectoryChoice::Other(entered.clone()), current_dir);

        assert_eq!(selected, entered);
    }
}
