use crate::config::{HelixConfig, InstanceInfo};
use crate::errors::{Candidate, CliError, ProjectError};
use crate::{paths, prompts};
use eyre::Result;
use std::env;
use std::path::{Path, PathBuf};

pub struct ProjectContext {
    pub root: PathBuf,
    pub config: HelixConfig,
    pub helix_dir: PathBuf,
}

impl ProjectContext {
    pub fn find_and_load(start_dir: Option<&Path>) -> Result<Self, ProjectError> {
        Self::load_with(start_dir, true)
    }

    /// Like [`find_and_load`](Self::find_and_load), but tolerates a `helix.toml` that defines
    /// zero instances. Used by `helix add` so it can re-add the first instance after the last
    /// one was deleted.
    pub fn find_and_load_allow_no_instances(
        start_dir: Option<&Path>,
    ) -> Result<Self, ProjectError> {
        Self::load_with(start_dir, false)
    }

    fn load_with(start_dir: Option<&Path>, require_instances: bool) -> Result<Self, ProjectError> {
        let start = match start_dir {
            Some(dir) => dir.to_path_buf(),
            None => env::current_dir().map_err(|source| ProjectError::CurrentDir { source })?,
        };

        let root = find_project_root(&start)?;
        let config_path = root.join("helix.toml");
        let config = if require_instances {
            HelixConfig::from_file(&config_path)?
        } else {
            HelixConfig::from_file_allow_no_instances(&config_path)?
        };
        let helix_dir = root.join(".helix");

        Ok(Self {
            root,
            config,
            helix_dir,
        })
    }

    /// Pick the local instance a lifecycle command acts on: the explicit name,
    /// else `dev`, else the only local instance, else a prompt, else an error
    /// listing them. A Cloud instance is rejected by name.
    pub fn resolve_local_instance(&self, instance: Option<String>, prompt: &str) -> Result<String> {
        let mut names: Vec<&String> = self.config.local.keys().collect();
        names.sort();
        let name = match (instance, names.as_slice()) {
            (Some(name), _) => name,
            (None, _) if self.config.local.contains_key("dev") => "dev".to_owned(),
            (None, [only]) => (*only).clone(),
            (None, []) => {
                return Err(CliError::new("this project has no local instances")
                    .with_hint("add one with `helix add local --name dev`")
                    .into());
            }
            (None, _) if prompts::is_interactive() => prompts::select_instance(
                &names
                    .iter()
                    .map(|name| {
                        let port = self.config.local[*name].port;
                        ((*name).clone(), format!("http://localhost:{port}"))
                    })
                    .collect::<Vec<_>>(),
                prompt,
            )?,
            (None, _) => {
                return Err(CliError::new("no default local instance")
                    .with_hint("pass the instance name")
                    .with_candidates(
                        names
                            .iter()
                            .map(|name| Candidate {
                                id: (*name).clone(),
                                name: (*name).clone(),
                            })
                            .collect(),
                    )
                    .into());
            }
        };
        match self.config.get_instance(&name)? {
            InstanceInfo::Local(_) => Ok(name),
            InstanceInfo::Enterprise(_) => Err(CliError::new(format!(
                "'{name}' is not a local instance"
            ))
            .with_hint("start, stop, and restart manage local containers; Cloud databases need no lifecycle")
            .into()),
        }
    }

    pub fn instance_workspace(&self, instance_name: &str) -> PathBuf {
        self.helix_dir.join(instance_name)
    }

    /// Rejects a `.helix` that is a symlink or an existing non-directory, before any
    /// operation builds a path under it and creates or recursively deletes that path.
    ///
    /// A repository can track `.helix` as a symlink to a directory outside the
    /// project. Instance-name validation alone can't catch this: even a
    /// charset-valid name like `dev` joined onto a symlinked `.helix` resolves,
    /// via normal path resolution, to `<symlink target>/dev`, so
    /// `remove_dir_all`/`create_dir_all` would follow it and touch whatever the
    /// symlink points at. `symlink_metadata` (unlike `metadata`) reports on the
    /// link itself rather than following it, so a symlink is caught here instead
    /// of silently resolving through it.
    pub fn assert_safe_helix_dir(&self) -> Result<(), ProjectError> {
        match std::fs::symlink_metadata(&self.helix_dir) {
            Ok(metadata) if metadata.is_dir() => Ok(()),
            Ok(_) => Err(ProjectError::UnsafeHelixDir {
                path: self.helix_dir.clone(),
            }),
            Err(_) => Ok(()),
        }
    }

    pub fn ensure_instance_dir(&self, instance_name: &str) -> Result<(), ProjectError> {
        self.assert_safe_helix_dir()?;
        let workspace = self.instance_workspace(instance_name);
        std::fs::create_dir_all(&workspace).map_err(|source| ProjectError::CreateDir {
            path: workspace,
            source,
        })?;
        Ok(())
    }
}

fn find_project_root(start: &Path) -> Result<PathBuf, ProjectError> {
    let mut current = start.to_path_buf();
    loop {
        if current.join("helix.toml").exists() {
            return Ok(current);
        }

        match current.parent() {
            Some(parent) => current = parent.to_path_buf(),
            None => break,
        }
    }

    Err(ProjectError::ConfigNotFound {
        start: start.to_path_buf(),
    })
}

pub fn get_helix_cache_dir() -> Result<PathBuf> {
    let helix_dir = paths::helix_cache_dir()?;
    std::fs::create_dir_all(&helix_dir)?;
    Ok(helix_dir)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn project_root_is_found_from_nested_directory() {
        let directory = tempfile::tempdir().unwrap();
        let nested = directory.path().join("a/b/c");
        std::fs::create_dir_all(&nested).unwrap();
        let config = HelixConfig::default_config("demo");
        config
            .save_to_file(&directory.path().join("helix.toml"))
            .unwrap();

        let project = ProjectContext::find_and_load(Some(&nested)).unwrap();
        assert_eq!(project.root, directory.path());
        assert_eq!(
            project.instance_workspace("dev"),
            directory.path().join(".helix/dev")
        );
        project.ensure_instance_dir("dev").unwrap();
        assert!(directory.path().join(".helix/dev").is_dir());
    }

    #[test]
    fn assert_safe_helix_dir_accepts_a_missing_or_real_directory() {
        let directory = tempfile::tempdir().unwrap();
        let helix_dir = directory.path().join(".helix");
        let project = ProjectContext {
            root: directory.path().to_path_buf(),
            config: HelixConfig::default_config("demo"),
            helix_dir: helix_dir.clone(),
        };
        assert!(project.assert_safe_helix_dir().is_ok());

        std::fs::create_dir_all(&helix_dir).unwrap();
        assert!(project.assert_safe_helix_dir().is_ok());
    }

    #[test]
    fn assert_safe_helix_dir_rejects_an_existing_regular_file() {
        let directory = tempfile::tempdir().unwrap();
        let helix_dir = directory.path().join(".helix");
        std::fs::write(&helix_dir, b"not a directory").unwrap();
        let project = ProjectContext {
            root: directory.path().to_path_buf(),
            config: HelixConfig::default_config("demo"),
            helix_dir,
        };

        assert!(matches!(
            project.assert_safe_helix_dir(),
            Err(ProjectError::UnsafeHelixDir { .. })
        ));
    }

    #[cfg(unix)]
    #[test]
    fn assert_safe_helix_dir_rejects_a_symlink_even_to_a_real_directory() {
        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("elsewhere");
        std::fs::create_dir_all(&target).unwrap();
        let helix_dir = directory.path().join(".helix");
        std::os::unix::fs::symlink(&target, &helix_dir).unwrap();
        let project = ProjectContext {
            root: directory.path().to_path_buf(),
            config: HelixConfig::default_config("demo"),
            helix_dir,
        };

        assert!(matches!(
            project.assert_safe_helix_dir(),
            Err(ProjectError::UnsafeHelixDir { .. })
        ));
    }

    #[test]
    fn missing_project_reports_original_start_path() {
        let directory = tempfile::tempdir().unwrap();
        let error = find_project_root(directory.path()).unwrap_err();
        match error {
            ProjectError::ConfigNotFound { start } => assert_eq!(start, directory.path()),
            other => panic!("unexpected project error: {other}"),
        }
    }
}
