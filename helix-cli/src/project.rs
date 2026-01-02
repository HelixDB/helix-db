use crate::config::{HelixConfig, InstanceInfo};
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

    /// Get the data directory for a specific instance (respects custom data_dir for local instances)
    pub fn instance_data_dir(&self, instance_name: &str) -> Result<PathBuf> {
        let instance_config = self.config.get_instance(instance_name)?;
        let data_dir = instance_config.data_dir().cloned();

        let data_dir = match data_dir {
            Some(path) if path.is_absolute() => path,
            Some(path) => self.root.join(path),
            None => self.instance_volume(instance_name),
        };

        Ok(data_dir)
    }

    /// Get the LMDB user directory for a specific instance
    pub fn instance_user_dir(&self, instance_name: &str) -> Result<PathBuf> {
        Ok(self.instance_data_dir(instance_name)?.join("user"))
    }

    /// Get the LMDB data.mdb file path for a specific instance
    pub fn instance_data_file(&self, instance_name: &str) -> Result<PathBuf> {
        Ok(self.instance_user_dir(instance_name)?.join("data.mdb"))
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
        let volume = self
            .config
            .get_instance(instance_name)
            .ok()
            .and_then(|instance| match instance {
                InstanceInfo::Local(config) => config.data_dir.as_ref().cloned(),
                _ => None,
            })
            .map(|path| if path.is_absolute() { path } else { self.root.join(path) })
            .unwrap_or_else(|| self.instance_volume(instance_name));
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

        // Check for old v1 config.hx.json file
        let v1_config_path = current.join("config.hx.json");
        if v1_config_path.exists() {
            let error = crate::errors::config_error("found v1 project configuration")
                .with_file_path(v1_config_path.display().to_string())
                .with_context("This project uses the old v1 configuration format")
                .with_hint(format!("Run 'helix migrate --path \"{}\"' to migrate this project to v2 format", current.display()));
            return Err(eyre!("{}", error.render()));
        }

        match current.parent() {
            Some(parent) => current = parent.to_path_buf(),
            None => break,
        }
    }

    let error = crate::errors::config_error("project configuration not found")
        .with_file_path(start.display().to_string())
        .with_context(format!("searched from {} up to filesystem root", start.display()));
    Err(eyre!("{}", error.render()))
}

pub fn get_helix_cache_dir() -> Result<PathBuf> {
    if let Ok(cache_dir) = env::var("HELIX_CACHE_DIR") {
        let helix_dir = PathBuf::from(cache_dir);
        std::fs::create_dir_all(&helix_dir)?;
        return Ok(helix_dir);
    }

    let helix_dir = if cfg!(test) {
        let thread_id = format!("{:?}", std::thread::current().id());
        let pid = std::process::id();
        std::env::temp_dir()
            .join("helix-test-cache")
            .join(pid.to_string())
            .join(thread_id)
            .join(".helix")
    } else {
        let home = dirs::home_dir().ok_or_else(|| eyre!("Cannot find home directory"))?;
        home.join(".helix")
    };

    // Check if this is a fresh installation (no .helix directory exists)
    let is_fresh_install = !helix_dir.exists();

    std::fs::create_dir_all(&helix_dir)?;

    // For fresh installations, create .v2 marker to indicate this is a v2 helix directory
    if is_fresh_install {
        let v2_marker = helix_dir.join(".v2");
        std::fs::write(&v2_marker, "")?;
    }

    Ok(helix_dir)
}

pub fn get_helix_repo_cache() -> Result<PathBuf> {
    let helix_dir = get_helix_cache_dir()?;
    Ok(helix_dir.join("repo"))
}
