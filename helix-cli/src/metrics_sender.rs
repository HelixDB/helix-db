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
use std::{
    fs::{self, File, OpenOptions},
    io::{BufWriter, Write},
    path::PathBuf,
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
