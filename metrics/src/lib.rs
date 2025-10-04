pub mod events;

use std::{env::consts::OS, fs, path::Path, sync::LazyLock};

use serde::Serialize;

pub static METRICS_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(reqwest::Client::new);

static CONFIG: LazyLock<String> = LazyLock::new(|| {
    let home_dir = std::env::var("HOME").unwrap_or("~/".to_string());
    let config_path = &format!("{home_dir}/.helix/credentials");
    let config_path = Path::new(config_path);
    fs::read_to_string(config_path).unwrap_or_default()
});

pub static HELIX_USER_ID: LazyLock<String> = LazyLock::new(|| {
    // read from credentials file
    for line in CONFIG.lines() {
        if let Some((key, value)) = line.split_once("=")
            && key.to_lowercase() == "helix_user_id"
        {
            return value.to_string();
        }
    }
    "".to_string()
});

pub static METRICS_ENABLED: LazyLock<bool> = LazyLock::new(|| {
    for line in CONFIG.lines() {
        if let Some((key, value)) = line.split_once("=")
            && key.to_lowercase().as_str() == "metrics"
        {
            return value.to_string().parse().unwrap_or(true);
        }
    }
    true
});

pub const METRICS_URL: &str = "https://logs.helix-db.com";

pub struct HelixMetricsClient {
    threads_tx: flume::Sender<tokio::task::JoinHandle<()>>,
    threads_rx: flume::Receiver<tokio::task::JoinHandle<()>>,
}

impl Default for HelixMetricsClient {
    fn default() -> Self {
        Self::new()
    }
}

impl HelixMetricsClient {
    pub fn new() -> Self {
        let (tx, rx) = flume::unbounded();
        Self {
            threads_tx: tx,
            threads_rx: rx,
        }
    }

    pub fn get_client(&self) -> &'static LazyLock<reqwest::Client> {
        &METRICS_CLIENT
    }

    pub async fn flush(&self) {
        for handle in self.threads_rx.try_iter().collect::<Vec<_>>() {
            let _ = handle.await;
        }
    }

    pub fn send_event<D: Serialize + std::fmt::Debug + Send + 'static>(
        &self,
        event_type: events::EventType,
        event_data: D,
    ) {
        if !*METRICS_ENABLED {
            return;
        }

        // get OS
        let os = OS.to_string();

        // get user id
        let user_id = Some(HELIX_USER_ID.as_str().to_string());

        let raw_event = events::RawEvent {
            os,
            user_id,
            event_type,
            event_data,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("Failed to get system time")
                .as_secs(),
            email: None,
        };

        // Spawn the request in the background for fire-and-forget behavior
        let handle = tokio::spawn(async move {
            let _ = METRICS_CLIENT
                .post(METRICS_URL)
                .header("Content-Type", "application/json")
                .body(sonic_rs::to_vec(&raw_event).expect("Failed to serialize event"))
                .send()
                .await;
        });
        let _ = self.threads_tx.send(handle);
    }
}

#[derive(Debug)]
pub struct MetricError(String);

impl std::fmt::Display for MetricError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for MetricError {}

impl From<sonic_rs::Error> for MetricError {
    fn from(e: sonic_rs::Error) -> Self {
        MetricError(e.to_string())
    }
}

impl From<reqwest::Error> for MetricError {
    fn from(e: reqwest::Error) -> Self {
        MetricError(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_send_event() {
        let client = HelixMetricsClient::new();
        client.send_event(events::EventType::Test, events::TestEvent::default());

        client.flush().await;

        assert!(false);
    }
}
