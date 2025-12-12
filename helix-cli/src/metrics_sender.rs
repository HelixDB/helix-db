use chrono::{Local, NaiveDate};
use dirs::home_dir;
use eyre::{eyre, OptionExt, Result};
use flume::{Receiver, Sender, unbounded};
use helix_metrics::events::{
    CompileEvent, DeployCloudEvent, DeployLocalEvent, EventData, EventType, RawEvent,
    RedeployLocalEvent, TestEvent,
};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{BufWriter, Write},
    path::PathBuf,
    process::Command,
};
use tokio::task::JoinHandle;

const METRICS_URL: &str = "https://logs.helix-db.com/v2";

#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MetricsLevel {
    Full,
    #[default]
    Basic,
    Off,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MetricsConfig {
    pub level: MetricsLevel,
    pub user_id: Option<&'static str>,
    pub email: Option<&'static str>,
    pub name: Option<&'static str>,
    pub device_id: Option<&'static str>,
    pub last_updated: u64,
    pub install_event_sent: bool,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            level: MetricsLevel::default(),
            user_id: None,
            email: None,
            name: None,
            device_id: get_device_id(),
            last_updated: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            install_event_sent: false,
        }
    }
}
impl MetricsConfig {
    #[allow(unused)]
    pub fn new(user_id: Option<&'static str>) -> Self {
        Self {
            level: MetricsLevel::default(),
            user_id,
            email: None,
            name: None,
            device_id: get_device_id(),
            last_updated: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            install_event_sent: false,
        }
    }
}

pub struct MetricsSender {
    tx: Sender<MetricsMessage>,
    handle: JoinHandle<()>,
}

#[derive(Debug)]
enum MetricsMessage {
    Event(RawEvent<EventData>),
    Shutdown,
}

impl MetricsSender {
    pub fn new() -> Result<Self> {
        let (tx, rx) = unbounded();
        let handle = tokio::spawn(async move {
            if let Err(e) = metrics_task(rx).await {
                eprintln!("Metrics task error: {e}");
            }
        });

        Ok(Self { tx, handle })
    }

    pub fn send_event(&self, event: RawEvent<EventData>) {
        let _ = self.tx.send(MetricsMessage::Event(event));
    }

    pub async fn shutdown(self) -> Result<()> {
        let _ = self.tx.send(MetricsMessage::Shutdown);
        self.handle
            .await
            .map_err(|e| eyre!("Metrics task join error: {e}"))?;
        Ok(())
    }
}

async fn metrics_task(rx: Receiver<MetricsMessage>) -> Result<()> {
    let mut log_writer = None;

    let config = load_metrics_config().unwrap_or_default();

    if config.level != MetricsLevel::Off {
        let _ = upload_previous_logs().await;

        let metrics_dir = get_metrics_dir()?;
        let today = Local::now().format("%Y-%m-%d").to_string();
        let log_file_path = metrics_dir.join(format!("{today}.json"));

        log_writer = create_log_writer(&log_file_path).ok();
    }

    while let Ok(message) = rx.recv_async().await {
        match message {
            MetricsMessage::Event(event) => {
                if let Some(ref mut writer) = log_writer
                    && let Err(e) = write_event_to_log(writer, &event)
                {
                    eprintln!("Failed to write metrics event: {e}");
                }
            }
            MetricsMessage::Shutdown => {
                break;
            }
        }
    }

    if let Some(mut writer) = log_writer {
        let _ = writer.flush();
    }

    Ok(())
}

pub(crate) fn load_metrics_config() -> Result<MetricsConfig> {
    let config_path = get_metrics_config_path()?;

    if !config_path.exists() {
        return Ok(MetricsConfig::default());
    }

    let content: &'static str = fs::read_to_string(&config_path)?.leak();
    let config = toml::from_str(content)?;
    Ok(config)
}

pub(crate) fn save_metrics_config(config: &MetricsConfig) -> Result<()> {
    let config_path = get_metrics_config_path()?;
    let content = toml::to_string_pretty(config)?;
    fs::write(&config_path, content)?;
    Ok(())
}

pub(crate) fn get_metrics_config_path() -> Result<PathBuf> {
    let home = home_dir().ok_or_eyre("Cannot find home directory")?;
    let helix_dir = home.join(".helix");
    fs::create_dir_all(&helix_dir)?;
    Ok(helix_dir.join("metrics.toml"))
}

fn get_metrics_dir() -> Result<PathBuf> {
    let home = home_dir().ok_or_eyre("Cannot find home directory")?;
    let metrics_dir = home.join(".helix").join("metrics");
    fs::create_dir_all(&metrics_dir)?;
    Ok(metrics_dir)
}

fn create_log_writer(path: &PathBuf) -> Result<BufWriter<File>> {
    let file = OpenOptions::new().create(true).append(true).open(path)?;
    Ok(BufWriter::new(file))
}

fn write_event_to_log<W: Write>(writer: &mut W, event: &RawEvent<EventData>) -> Result<()> {
    let json = serde_json::to_string(event)?;
    writeln!(writer, "{json}")?;
    writer.flush()?;
    Ok(())
}

async fn upload_previous_logs() -> Result<()> {
    let metrics_dir = get_metrics_dir()?;
    let client = Client::new();
    let today = Local::now().date_naive();

    let entries = fs::read_dir(&metrics_dir)?;

    for entry in entries {
        let entry = entry?;
        let path = entry.path();

        if let Some(file_name) = path.file_name().and_then(|n| n.to_str())
            && let Some(date_str) = file_name.strip_suffix(".json")
            && let Ok(file_date) = NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
            && file_date < today
            && upload_log_file(&client, &path).await.is_ok()
        {
            let _ = fs::remove_file(&path);
        }
    }

    Ok(())
}

async fn upload_log_file(client: &Client, path: &PathBuf) -> Result<()> {
    let content = fs::read_to_string(path)?;

    if content.trim().is_empty() {
        return Ok(());
    }

    let response = client
        .post(METRICS_URL) // TODO: change to actual logs endpoint
        .header("Content-Type", "application/x-ndjson")
        .body(content)
        .send()
        .await?;

    if !response.status().is_success() {
        return Err(eyre!(
            "Failed to upload log file: HTTP {}",
            response.status()
        ));
    }

    Ok(())
}

// Helper functions for creating and sending events
impl MetricsSender {
    pub fn send_cli_install_event_if_first_time(&self) {
        let mut config = load_metrics_config().unwrap_or_default();

        if !config.install_event_sent {
            let event = RawEvent {
                os: get_os_string(),
                event_type: EventType::CliInstall,
                event_data: EventData::CliInstall,
                user_id: get_user_id(),
                email: get_email(),
                device_id: get_device_id(),
                timestamp: get_current_timestamp(),
            };
            self.send_event(event);

            // Mark install event as sent
            config.install_event_sent = true;
            let _ = save_metrics_config(&config);
        }
    }

    pub fn send_compile_event(
        &self,
        cluster_id: String,
        queries_string: String,
        num_of_queries: u32,
        time_taken_seconds: u32,
        success: bool,
        error_messages: Option<String>,
    ) {
        let event = RawEvent {
            os: get_os_string(),
            event_type: EventType::Compile,
            event_data: EventData::Compile(CompileEvent {
                cluster_id,
                queries_string,
                num_of_queries,
                time_taken_seconds,
                success,
                error_messages,
            }),
            user_id: get_user_id(),
            email: get_email(),
            device_id: get_device_id(),
            timestamp: get_current_timestamp(),
        };
        self.send_event(event);
    }

    pub fn send_deploy_local_event(
        &self,
        cluster_id: String,
        queries_string: String,
        num_of_queries: u32,
        time_taken_sec: u32,
        success: bool,
        error_messages: Option<String>,
    ) {
        let event = RawEvent {
            os: get_os_string(),
            event_type: EventType::DeployLocal,
            event_data: EventData::DeployLocal(DeployLocalEvent {
                cluster_id,
                queries_string,
                num_of_queries,
                time_taken_sec,
                success,
                error_messages,
            }),
            user_id: get_user_id(),
            email: get_email(),
            device_id: get_device_id(),
            timestamp: get_current_timestamp(),
        };
        self.send_event(event);
    }

    pub fn send_redeploy_local_event(
        &self,
        cluster_id: String,
        queries_string: String,
        num_of_queries: u32,
        time_taken_sec: u32,
        success: bool,
        error_messages: Option<String>,
    ) {
        let event = RawEvent {
            os: get_os_string(),
            event_type: EventType::RedeployLocal,
            event_data: EventData::RedeployLocal(RedeployLocalEvent {
                cluster_id,
                queries_string,
                num_of_queries,
                time_taken_sec,
                success,
                error_messages,
            }),
            user_id: get_user_id(),
            email: get_email(),
            device_id: get_device_id(),
            timestamp: get_current_timestamp(),
        };
        self.send_event(event);
    }

    pub fn send_deploy_cloud_event(
        &self,
        cluster_id: String,
        queries_string: String,
        num_of_queries: u32,
        time_taken_sec: u32,
        success: bool,
        error_messages: Option<String>,
    ) {
        let event = RawEvent {
            os: get_os_string(),
            event_type: EventType::DeployCloud,
            event_data: EventData::DeployCloud(DeployCloudEvent {
                cluster_id,
                queries_string,
                num_of_queries,
                time_taken_sec,
                success,
                error_messages,
            }),
            user_id: get_user_id(),
            email: get_email(),
            device_id: get_device_id(),
            timestamp: get_current_timestamp(),
        };
        self.send_event(event);
    }

    #[allow(unused)]
    pub fn send_test_event(
        &self,
        cluster_id: String,
        queries_string: String,
        num_of_queries: u32,
        time_taken_sec: u32,
        success: bool,
        error_messages: Option<String>,
    ) {
        let event = RawEvent {
            os: get_os_string(),
            event_type: EventType::Test,
            event_data: EventData::Test(TestEvent {
                cluster_id,
                queries_string,
                num_of_queries,
                time_taken_sec,
                success,
                error_messages,
            }),
            user_id: get_user_id(),
            email: get_email(),
            device_id: get_device_id(),
            timestamp: get_current_timestamp(),
        };
        self.send_event(event);
    }
}

fn get_os_string() -> &'static str {
    std::env::consts::OS
}

fn get_user_id() -> Option<&'static str> {
    load_metrics_config().ok().and_then(|config| config.user_id)
}

fn get_email() -> Option<&'static str> {
    load_metrics_config().ok().and_then(|config| config.email)
}

fn get_current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// Get a deterministic device ID derived from the machine's unique identifier.
/// This ID is stable across CLI reinstalls and file deletions.
pub fn get_device_id() -> Option<&'static str> {
    get_machine_id()
        .map(|id| hash_to_device_id(&id))
        .map(|s| -> &'static str { s.leak() })
}

/// Hash the machine ID to create a privacy-preserving device identifier.
fn hash_to_device_id(machine_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"helix-device-id:");
    hasher.update(machine_id.as_bytes());
    let result = hasher.finalize();
    // Use first 16 bytes (32 hex chars) for a shorter but still unique ID
    hex::encode(&result[..16])
}

/// Get the machine's unique identifier (platform-specific).
#[cfg(target_os = "macos")]
fn get_machine_id() -> Option<String> {
    // macOS: Use IOPlatformUUID from IOKit
    Command::new("ioreg")
        .args(["-rd1", "-c", "IOPlatformExpertDevice"])
        .output()
        .ok()
        .and_then(|output| {
            let stdout = String::from_utf8_lossy(&output.stdout);
            stdout
                .lines()
                .find(|line| line.contains("IOPlatformUUID"))
                .and_then(|line| {
                    line.split('"')
                        .nth(3)
                        .map(|s| s.to_string())
                })
        })
}

#[cfg(target_os = "linux")]
fn get_machine_id() -> Option<String> {
    // Linux: Read from /etc/machine-id or /var/lib/dbus/machine-id
    fs::read_to_string("/etc/machine-id")
        .or_else(|_| fs::read_to_string("/var/lib/dbus/machine-id"))
        .ok()
        .map(|s| s.trim().to_string())
}

#[cfg(target_os = "windows")]
fn get_machine_id() -> Option<String> {
    // Windows: Read MachineGuid from registry
    Command::new("reg")
        .args([
            "query",
            r"HKLM\SOFTWARE\Microsoft\Cryptography",
            "/v",
            "MachineGuid",
        ])
        .output()
        .ok()
        .and_then(|output| {
            let stdout = String::from_utf8_lossy(&output.stdout);
            stdout
                .lines()
                .find(|line| line.contains("MachineGuid"))
                .and_then(|line| line.split_whitespace().last())
                .map(|s| s.to_string())
        })
}

#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
fn get_machine_id() -> Option<String> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_machine_id_returns_value() {
        // Machine ID should be available on macOS, Linux, and Windows
        let machine_id = get_machine_id();

        #[cfg(any(target_os = "macos", target_os = "linux", target_os = "windows"))]
        {
            assert!(
                machine_id.is_some(),
                "Machine ID should be available on this platform"
            );
            let id = machine_id.unwrap();
            assert!(!id.is_empty(), "Machine ID should not be empty");
            println!("Machine ID: {}", id);
        }

        #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
        {
            // On unsupported platforms, it's expected to be None
            assert!(machine_id.is_none());
        }
    }

    #[test]
    fn test_hash_to_device_id_produces_consistent_hash() {
        let machine_id = "test-machine-id-12345";
        let hash1 = hash_to_device_id(machine_id);
        let hash2 = hash_to_device_id(machine_id);

        assert_eq!(hash1, hash2, "Same input should produce same hash");
        assert_eq!(hash1.len(), 32, "Hash should be 32 hex characters (16 bytes)");

        // Verify it's valid hex
        assert!(
            hash1.chars().all(|c| c.is_ascii_hexdigit()),
            "Hash should only contain hex digits"
        );
    }

    #[test]
    fn test_hash_to_device_id_different_inputs_produce_different_hashes() {
        let hash1 = hash_to_device_id("machine-id-1");
        let hash2 = hash_to_device_id("machine-id-2");

        assert_ne!(hash1, hash2, "Different inputs should produce different hashes");
    }

    #[test]
    fn test_get_device_id_is_deterministic() {
        // Get device ID twice - should be the same
        let device_id1 = get_device_id();
        let device_id2 = get_device_id();

        #[cfg(any(target_os = "macos", target_os = "linux", target_os = "windows"))]
        {
            assert!(device_id1.is_some(), "Device ID should be available");
            assert!(device_id2.is_some(), "Device ID should be available");
            assert_eq!(
                device_id1.unwrap(),
                device_id2.unwrap(),
                "Device ID should be deterministic"
            );
            println!("Device ID: {}", device_id1.unwrap());
        }
    }

    #[test]
    fn test_device_id_format() {
        let device_id = get_device_id();

        #[cfg(any(target_os = "macos", target_os = "linux", target_os = "windows"))]
        {
            let id = device_id.expect("Device ID should be available");
            assert_eq!(id.len(), 32, "Device ID should be 32 characters");
            assert!(
                id.chars().all(|c| c.is_ascii_hexdigit()),
                "Device ID should only contain hex digits"
            );
        }
    }

    #[test]
    fn test_hash_includes_salt() {
        // The hash function includes a salt "helix-device-id:"
        // This ensures different apps using machine ID get different hashes
        let machine_id = "same-machine-id";

        // Direct SHA256 of machine_id without salt would be different
        let mut hasher = Sha256::new();
        hasher.update(machine_id.as_bytes());
        let direct_hash = hex::encode(&hasher.finalize()[..16]);

        let salted_hash = hash_to_device_id(machine_id);

        assert_ne!(
            direct_hash, salted_hash,
            "Salted hash should differ from unsalted"
        );
    }
}
