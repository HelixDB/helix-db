use crate::config::DEFAULT_LOCAL_PORT;
use crate::{AddTarget, InitTarget};
use eyre::{eyre, Result};
use std::io::IsTerminal;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InstanceKind {
    Local,
    Enterprise,
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

pub fn confirm(message: &str) -> Result<bool> {
    Ok(cliclack::confirm(message).interact()?)
}

pub fn input_instance_name(default: &str) -> Result<String> {
    input_name("Instance name", default, 32)
}

fn input_project_instance_name(default: &str) -> Result<String> {
    input_name("Instance name", default, 32)
}

fn input_name(label: &str, default: &str, max_len: usize) -> Result<String> {
    let name: String = cliclack::input(label)
        .default_input(default)
        .placeholder(default)
        .validate(move |input: &String| {
            if input.trim().is_empty() {
                Err("name cannot be empty")
            } else if input.len() > max_len {
                Err("name is too long")
            } else if !input
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
            {
                Err("name can only contain letters, numbers, hyphens, and underscores")
            } else {
                Ok(())
            }
        })
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
            // Leave the cluster (and gateway URL) unset so the handler lists the
            // available clusters and lets the user pick one.
            cluster_id: None,
            gateway_url: None,
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
            // Leave the cluster (and gateway URL) unset so the handler lists the
            // available clusters and lets the user pick one.
            cluster_id: None,
            gateway_url: None,
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
