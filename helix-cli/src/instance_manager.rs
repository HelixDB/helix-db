use crate::types::BuildMode;

use super::utils::find_available_port;
use helix_db::utils::styled_string::StyledString;
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct InstanceInfo {
    pub short_id: u16,
    pub id: String,
    pub pid: u32,
    pub port: u16,
    pub started_at: String,
    pub available_endpoints: Vec<String>,
    pub binary_path: PathBuf,
    pub running: bool,
}

pub struct InstanceManager {
    instances_file: PathBuf,
    pub cache_dir: PathBuf,
    logs_dir: PathBuf,
}

impl InstanceManager {
    pub fn new() -> io::Result<Self> {
        let home_dir = dirs::home_dir().expect("Could not find home directory");
        let helix_dir = home_dir.join(".helix");
        let cache_dir = helix_dir.join("cached_builds");
        let logs_dir = helix_dir.join("logs");
        fs::create_dir_all(&helix_dir)?;
        fs::create_dir_all(&cache_dir)?;
        fs::create_dir_all(&logs_dir)?;

        Ok(Self {
            instances_file: helix_dir.join("instances.json"),
            cache_dir,
            logs_dir,
        })
    }

    fn id_from_short_id(&self, n: u16) -> Result<InstanceInfo, String> {
        let instances = self.list_instances()
            .map_err(|e| e.to_string())?;

        instances.into_iter()
            .find(|i| i.short_id == n)
            .ok_or_else(|| "No instance found".to_string())
    }

    pub fn init_start_instance(
        &self,
        source_binary: &Path,
        port: u16,
        endpoints: Vec<String>,
        openai_key: Option<String>,
    ) -> io::Result<InstanceInfo> {
        let instance_id = Uuid::new_v4().to_string();
        let cached_binary = self.cache_dir.join(&instance_id);
        fs::copy(source_binary, &cached_binary)?;

        // make sure data dir exists
        // make it .cached_builds/data/instance_id/
        let data_dir = self.cache_dir.join("data").join(&instance_id);
        fs::create_dir_all(&data_dir)?;

        let log_file = self.logs_dir.join(format!("instance_{instance_id}.log"));
        let log_file = OpenOptions::new()
            .create(true)
            
            .append(true)
            .open(log_file)?;
        let error_log_file = self
            .logs_dir
            .join(format!("instance_{instance_id}_error.log"));
        let error_log_file = OpenOptions::new()
            .create(true)
            
            .append(true)
            .open(error_log_file)?;

        let mut command = Command::new(&cached_binary);
        command.env("PORT", port.to_string());
        command
            .env("HELIX_DAEMON", "1")
            .env("HELIX_DATA_DIR", data_dir.to_str().unwrap())
            .env("HELIX_PORT", port.to_string())
            .env("OPENAI_API_KEY", openai_key.unwrap_or_default())
            .stdout(Stdio::from(log_file))
            .stderr(Stdio::from(error_log_file));

        let child = command.spawn()?;

        let instance = InstanceInfo {
            short_id: (self.list_instances()?.len() + 1) as u16,
            id: instance_id,
            pid: child.id(),
            port,
            started_at: chrono::Local::now().to_rfc3339(),
            available_endpoints: endpoints,
            binary_path: cached_binary,
            running: true,
        };

        let mut instances = self.list_instances()?;
        instances.push(instance.clone());
        let _ = self.save_instances(&instances);

        Ok(instance)
    }

    /// instance_id can either be u16 or uuid here (same for the others)
    pub fn start_instance(&self, instance_id: &str, endpoints: Option<Vec<String>>, openai_key: Option<String>, release_mode: BuildMode) -> Result<InstanceInfo, String> {
        let instance_id = match instance_id.parse() {
            Ok(n) => match self.id_from_short_id(n) {
                Ok(n) => n.id,
                Err(_) => return Err(format!("No instance found with id {}", &instance_id)),
            },
            Err(_) => instance_id.to_string(),
        };

        let mut instance = match self.get_instance(&instance_id) {
            Ok(instance) => {
                match instance {
                    Some(val) => val,
                    None => return Err(format!("No instance found with id {instance_id}")),
                }
            }
            Err(e) => return Err(format!("Error occured getting instance {e}")),
        };

        if !instance.binary_path.exists() {
            return Err(format!("Binary not found for instance {}: {:?}",
                        instance_id, instance.binary_path));
        }

        let data_dir = self.cache_dir.join("data").join(&instance_id);
        if !data_dir.exists() {
            fs::create_dir_all(&data_dir).map_err(|e| {
                format!("Failed to create data directory for {instance_id}: {e}")
            })?;
        }

        let log_file = self.logs_dir.join(format!("instance_{instance_id}.log"));
        let log_file = OpenOptions::new()
            
            .append(true)
            .create(true)
            .open(log_file)
            .map_err(|e| format!("Failed to open log file: {e}"))?;

        let port = match find_available_port(instance.port) {
            Some(port) => port,
            None => {
                return Err("Could not find an available port!".red().bold().to_string());
            }
        };
        instance.port = port;

        let mut command = Command::new(&instance.binary_path);
        command.env("PORT", instance.port.to_string());
        command
            .env("HELIX_DAEMON", "1")
            .env("HELIX_DATA_DIR", data_dir.to_str().unwrap())
            .env("HELIX_PORT", instance.port.to_string())
            .env("OPENAI_API_KEY", openai_key.unwrap_or_default())
            .stdout(Stdio::from(log_file.try_clone().map_err(|e| {
                format!("Failed to clone log file: {e}")
            })?))
        .stderr(Stdio::from(log_file));

        let child = command.spawn().map_err(|e| {
            format!("Failed to spawn process for {instance_id}: {e}")
        })?;

        instance.pid = child.id();
        instance.running = true;
        if let Some(endpoints) = endpoints {
            instance.available_endpoints = endpoints;
        }

        self.update_instance(&instance)?;

        Ok(instance)
    }

    pub fn get_instance(&self, instance_id: &str) -> io::Result<Option<InstanceInfo>> {
        let instance_id = match instance_id.parse() {
            Ok(n) => match self.id_from_short_id(n) {
                Ok(n) => n.id,
                Err(_) => return Err(
                    io::Error::other(
                        format!("No instance found with id {instance_id}"),
                    )),
            },
            Err(_) => instance_id.to_string(),
        };

        let instances = self.list_instances()?;
        Ok(instances.into_iter().find(|i| i.id == instance_id))
    }

    pub fn list_instances(&self) -> io::Result<Vec<InstanceInfo>> {
        if !self.instances_file.exists() {
            return Ok(Vec::new());
        }

        let mut file = File::open(&self.instances_file)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;

        if contents.is_empty() {
            return Ok(Vec::new());
        }

        let instances: Vec<InstanceInfo> = sonic_rs::from_str(&contents)?;
        Ok(instances)
    }

    pub fn stop_instance(&self, instance_id: &str) -> Result<bool, String> {
        let instance_id = match instance_id.parse() {
            Ok(n) => match self.id_from_short_id(n) {
                Ok(n) => n.id,
                Err(_) => return Err(format!("No instance found with id {}", &instance_id)),
            },
            Err(_) => instance_id.to_string(),
        };

        let mut instances = match self.list_instances() {
            Ok(val) => val,
            Err(e) => return Err(format!("Error occured stopping instnace! {e}")),
        };
        if let Some(pos) = instances.iter().position(|i| i.id == instance_id) {
            if !instances[pos].running {
                return Ok(false);
            }
            instances[pos].running = false;
            #[cfg(unix)]
            unsafe {
                libc::kill(instances[pos].pid as i32, libc::SIGTERM);
            }
            #[cfg(windows)]
            {
                use windows::Win32::System::Threading::{
                    OpenProcess, TerminateProcess, PROCESS_TERMINATE,
                };
                let handle = unsafe { OpenProcess(PROCESS_TERMINATE, false.into(), instances[pos].pid) };
                if let Ok(handle) = handle {
                    unsafe { TerminateProcess(handle, 0) };
                }
            }
            self.save_instances(&instances)?;
            return Ok(true);
        }
        Ok(false)
    }

    pub fn running_instances(&self) -> Result<bool, String> {
        let instances = match self.list_instances() {
            Ok(val) => val,
            Err(e) => return Err(format!("Error occured listing instnaces! {e}")),
        };
        for instance in instances {
            if instance.running {
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn save_instances(&self, instances: &[InstanceInfo]) -> Result<(), String> {
        let contents = match sonic_rs::to_string(instances) {
            Ok(s) => s,
            Err(e) => return Err(format!("Failed to serialize instances: {e}")),
        };
        let mut file = match OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.instances_file)
            {
                Ok(f) => f,
                Err(e) => return Err(format!("Failed to open file: {e}")),
            };
        match file.write_all(contents.as_bytes()) {
            Ok(_) => (),
            Err(e) => return Err(format!("Failed to write to file: {e}")),
        };
        Ok(())
    }

    fn update_instance(&self, updated_instance: &InstanceInfo) -> Result<(), String> {
        let mut instances = match self.list_instances() {
            Ok(val) => val,
            Err(e) => return Err(format!("Error occured stopping instnace! {e}")),
        };
        if let Some(pos) = instances.iter().position(|i| i.id == updated_instance.id) {
            instances[pos] = updated_instance.clone();
        } else {
            instances.push(updated_instance.clone());
        }

        self.save_instances(&instances)
    }

    pub fn delete_instance(&self, instance_id: &str) -> Result<bool, String> {
        let instance_id = match instance_id.parse() {
            Ok(n) => match self.id_from_short_id(n) {
                Ok(n) => n.id,
                Err(_) => return Err(format!("No instance found with id {}", &instance_id)),
            },
            Err(_) => instance_id.to_string(),
        };

        let mut instances = match self.list_instances() {
            Ok(val) => val,
            Err(e) => return Err(format!("Error occured stopping instnace! {e}")),
        };
        if let Some(pos) = instances.iter().position(|i| i.id == instance_id) {
            instances.remove(pos);
            self.save_instances(&instances)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

