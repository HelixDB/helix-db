use crate::config::HelixConfig;
use eyre::{Result, eyre};
use std::env;
use std::path::{Path, PathBuf};

pub struct ProjectContext {
    /// The root directory of the project
    pub root: PathBuf,
    pub config: HelixConfig,
    // The path to the .helix directory
    pub helix_dir: PathBuf,
}

impl ProjectContext {
    /// Find and load the project context starting from the given directory
    pub fn find_and_load(start_dir: Option<&Path>) -> Result<Self> {
        let start = match start_dir {
            Some(dir) => dir.to_path_buf(),
            None => env::current_dir()?,
        };

        let root = find_project_root(&start)?;
        let config_path = root.join("helix.toml");
        let config = HelixConfig::from_file(&config_path)?;
        let helix_dir = root.join(".helix");

        Ok(ProjectContext {
            root,
            config,
            helix_dir,
        })
    }

    /// Get the workspace directory for a specific instance
    pub fn instance_workspace(&self, instance_name: &str) -> PathBuf {
        self.helix_dir.join(instance_name)
    }

    /// Get the volumes directory for persistent data
    pub fn volumes_dir(&self) -> PathBuf {
        self.helix_dir.join(".volumes")
    }

    /// Get the volume path for a specific instance
    pub fn instance_volume(&self, instance_name: &str) -> PathBuf {
        self.volumes_dir().join(instance_name)
    }

    /// Get the docker-compose file path for an instance
    pub fn docker_compose_path(&self, instance_name: &str) -> PathBuf {
        self.instance_workspace(instance_name)
            .join("docker-compose.yml")
    }

    /// Get the Dockerfile path for an instance
    pub fn dockerfile_path(&self, instance_name: &str) -> PathBuf {
        self.instance_workspace(instance_name).join("Dockerfile")
    }

    /// Get the compiled container directory for an instance
    pub fn container_dir(&self, instance_name: &str) -> PathBuf {
        self.instance_workspace(instance_name)
            .join("helix-container")
    }

    /// Ensure all necessary directories exist for an instance
    pub fn ensure_instance_dirs(&self, instance_name: &str) -> Result<()> {
        let workspace = self.instance_workspace(instance_name);
        let volume = self.instance_volume(instance_name);
        let container = self.container_dir(instance_name);

        std::fs::create_dir_all(&workspace)?;
        std::fs::create_dir_all(&volume)?;
        std::fs::create_dir_all(&container)?;

        Ok(())
    }
}

/// Find the project root by looking for helix.toml file
fn find_project_root(start: &Path) -> Result<PathBuf> {
    let mut current = start.to_path_buf();

    loop {
        let config_path = current.join("helix.toml");
        if config_path.exists() {
            return Ok(current);
        }

        match current.parent() {
            Some(parent) => current = parent.to_path_buf(),
            None => break,
        }
    }

    Err(eyre!(
        "Could not find helix.toml file in {} or any parent directory. \
        Run 'helix init' to create a new Helix project.",
        start.display()
    ))
}

pub fn get_helix_cache_dir() -> Result<PathBuf> {
    let home = dirs::home_dir().ok_or_else(|| eyre!("Cannot find home directory"))?;
    let helix_dir = home.join(".helix");
    std::fs::create_dir_all(&helix_dir)?;
    Ok(helix_dir)
}

pub fn get_helix_repo_cache() -> Result<PathBuf> {
    let helix_dir = get_helix_cache_dir()?;
    Ok(helix_dir.join("repo"))
}
