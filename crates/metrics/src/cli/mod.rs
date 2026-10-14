//! Durable anonymous JSON telemetry for the Helix CLI.

mod config;
mod spool;

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

use flume::TrySendError;
use serde::Serialize;
use thiserror::Error;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::telemetry;

pub use config::{
    load_metrics_config, load_query_metrics_settings, save_metrics_config, MetricsConfig,
    MetricsLevel, QueryMetricsSettings,
};

const EVENT_QUEUE_CAPACITY: usize = 1_000;
const FLUSH_INTERVAL: Duration = Duration::from_secs(1);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Error)]
pub enum CliMetricsError {
    #[error("cannot locate the Helix home directory")]
    HomeDirectory,
    #[error("{0} cannot be empty")]
    Empty(&'static str),
    #[error("installation_id must be a UUIDv4")]
    InvalidInstallationId,
    #[error("{0} is missing")]
    Missing(&'static str),
    #[error("{0} contains an unknown value")]
    InvalidEnum(&'static str),
    #[error("CLI metrics worker stopped before queued events were persisted")]
    WorkerStopped,
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    TomlDecode(#[from] toml::de::Error),
    #[error(transparent)]
    TomlEncode(#[from] toml::ser::Error),
    #[error(transparent)]
    Telemetry(#[from] telemetry::TelemetryError),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum EventOutcome {
    Succeeded,
    Failed {
        #[serde(skip_serializing_if = "Option::is_none")]
        stage: Option<String>,
        message: String,
    },
}

impl EventOutcome {
    /// Converts a command result into a correlated telemetry outcome.
    ///
    /// ```
    /// use helix_metrics::cli::EventOutcome;
    ///
    /// assert_eq!(EventOutcome::from_status(true, None), EventOutcome::Succeeded);
    /// assert!(matches!(
    ///     EventOutcome::from_status(false, Some("failed".to_owned())),
    ///     EventOutcome::Failed { .. }
    /// ));
    /// ```
    #[must_use]
    pub fn from_status(success: bool, message: Option<String>) -> Self {
        if success {
            Self::Succeeded
        } else {
            Self::Failed {
                stage: None,
                message: message.unwrap_or_else(|| "operation failed".to_owned()),
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct OperationEvent {
    pub cluster_id: String,
    pub queries_string: String,
    pub query_count: u32,
    pub duration_seconds: u64,
    pub outcome: EventOutcome,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChefPhase {
    Started,
    AuthFailed,
    UploadFailed,
    Completed,
}

impl TryFrom<&str> for ChefPhase {
    type Error = CliMetricsError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "started" => Ok(Self::Started),
            "auth_failed" => Ok(Self::AuthFailed),
            "upload_failed" => Ok(Self::UploadFailed),
            "completed" => Ok(Self::Completed),
            _ => Err(CliMetricsError::InvalidEnum("chef phase")),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChefSetupMode {
    Automatic,
    Manual,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ChefEvent {
    pub run_id: String,
    pub phase: ChefPhase,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_seconds: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub setup_mode: Option<ChefSetupMode>,
    pub has_custom_intent: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub overview_size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_snapshot_size_bytes: Option<u64>,
    pub outcome: EventOutcome,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Event {
    CliInstall,
    Compile(OperationEvent),
    DeployLocal(OperationEvent),
    DeployCloud(OperationEvent),
    RedeployLocal(OperationEvent),
    Chef(ChefEvent),
    Test(OperationEvent),
}

#[derive(Debug)]
enum MetricsMessage {
    Event(QueuedEvent),
    Shutdown(oneshot::Sender<Result<(), CliMetricsError>>),
}

#[derive(Clone, Debug)]
struct QueuedEvent {
    event_id: String,
    occurred_at: String,
    event: Event,
}

pub struct MetricsSender {
    tx: Option<flume::Sender<MetricsMessage>>,
    handle: Option<JoinHandle<()>>,
    install_event_queued: Option<Arc<AtomicBool>>,
}

impl MetricsSender {
    #[must_use]
    pub fn new() -> Self {
        let Ok(root) = config::metrics_root() else {
            return Self::disabled();
        };
        let Ok(config) = config::load_metrics_config_from(&root) else {
            return Self::disabled();
        };
        if config.level == MetricsLevel::Off {
            let _ = spool::cleanup_obsolete(&root);
            let _ = spool::apply_privacy(&root, config.level);
            return Self::disabled();
        }
        let Ok(endpoint) = metrics_endpoint() else {
            return Self::disabled();
        };
        Self::configured(root, endpoint, config)
    }

    fn configured(root: std::path::PathBuf, endpoint: String, config: MetricsConfig) -> Self {
        Self::configured_with_delivery_timeout(root, endpoint, config, spool::REQUEST_TIMEOUT)
    }

    fn configured_with_delivery_timeout(
        root: std::path::PathBuf,
        endpoint: String,
        config: MetricsConfig,
        delivery_timeout: Duration,
    ) -> Self {
        let Some(client) = telemetry_client(&config) else {
            return Self::disabled();
        };
        let (tx, rx) = flume::bounded(EVENT_QUEUE_CAPACITY);
        let install_event_queued = Arc::new(AtomicBool::new(config.install_event_sent));
        let handle = tokio::spawn(metrics_task(
            rx,
            root,
            endpoint,
            config,
            client,
            delivery_timeout,
        ));
        Self {
            tx: Some(tx),
            handle: Some(handle),
            install_event_queued: Some(install_event_queued),
        }
    }

    fn disabled() -> Self {
        Self {
            tx: None,
            handle: None,
            install_event_queued: None,
        }
    }

    #[must_use]
    pub fn send_event(&self, event: Event) -> bool {
        let Some(tx) = &self.tx else {
            return false;
        };
        let queued = QueuedEvent {
            event_id: Uuid::now_v7().to_string(),
            occurred_at: telemetry::timestamp_now(),
            event,
        };
        match tx.try_send(MetricsMessage::Event(queued)) {
            Ok(()) => true,
            Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => false,
        }
    }

    pub fn send_cli_install_event_if_first_time(&self) {
        let Some(install_event_queued) = &self.install_event_queued else {
            return;
        };
        if install_event_queued
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        if !self.send_event(Event::CliInstall) {
            install_event_queued.store(false, Ordering::Release);
        }
    }

    pub fn send_compile_event(
        &self,
        cluster_id: String,
        queries_string: String,
        query_count: u32,
        duration_seconds: u32,
        success: bool,
        error_message: Option<String>,
    ) {
        self.send_operation(
            OperationKind::Compile,
            cluster_id,
            queries_string,
            query_count,
            duration_seconds,
            success,
            error_message,
        );
    }

    pub fn send_deploy_local_event(
        &self,
        cluster_id: String,
        queries_string: String,
        query_count: u32,
        duration_seconds: u32,
        success: bool,
        error_message: Option<String>,
    ) {
        self.send_operation(
            OperationKind::DeployLocal,
            cluster_id,
            queries_string,
            query_count,
            duration_seconds,
            success,
            error_message,
        );
    }

    pub fn send_deploy_cloud_event(
        &self,
        cluster_id: String,
        queries_string: String,
        query_count: u32,
        duration_seconds: u32,
        success: bool,
        error_message: Option<String>,
    ) {
        self.send_operation(
            OperationKind::DeployCloud,
            cluster_id,
            queries_string,
            query_count,
            duration_seconds,
            success,
            error_message,
        );
    }

    pub fn send_redeploy_local_event(
        &self,
        cluster_id: String,
        queries_string: String,
        query_count: u32,
        duration_seconds: u32,
        success: bool,
        error_message: Option<String>,
    ) {
        self.send_operation(
            OperationKind::RedeployLocal,
            cluster_id,
            queries_string,
            query_count,
            duration_seconds,
            success,
            error_message,
        );
    }

    pub fn send_test_event(
        &self,
        cluster_id: String,
        queries_string: String,
        query_count: u32,
        duration_seconds: u32,
        success: bool,
        error_message: Option<String>,
    ) {
        self.send_operation(
            OperationKind::Test,
            cluster_id,
            queries_string,
            query_count,
            duration_seconds,
            success,
            error_message,
        );
    }

    pub fn send_chef_event(&self, event: ChefEvent) {
        let _ = self.send_event(Event::Chef(event));
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the public helpers preserve the established CLI call boundary"
    )]
    fn send_operation(
        &self,
        kind: OperationKind,
        cluster_id: String,
        queries_string: String,
        query_count: u32,
        duration_seconds: u32,
        success: bool,
        error_message: Option<String>,
    ) {
        let operation = OperationEvent {
            cluster_id,
            queries_string,
            query_count,
            duration_seconds: u64::from(duration_seconds),
            outcome: EventOutcome::from_status(success, error_message),
        };
        let event = match kind {
            OperationKind::Compile => Event::Compile(operation),
            OperationKind::DeployLocal => Event::DeployLocal(operation),
            OperationKind::DeployCloud => Event::DeployCloud(operation),
            OperationKind::RedeployLocal => Event::RedeployLocal(operation),
            OperationKind::Test => Event::Test(operation),
        };
        let _ = self.send_event(event);
    }

    pub async fn shutdown(mut self) -> Result<(), CliMetricsError> {
        let Some(tx) = self.tx.take() else {
            return Ok(());
        };
        let Some(mut handle) = self.handle.take() else {
            return Ok(());
        };

        let (durable_tx, durable_rx) = oneshot::channel();
        if tx
            .send_async(MetricsMessage::Shutdown(durable_tx))
            .await
            .is_err()
        {
            let _ = handle.await;
            return Err(CliMetricsError::WorkerStopped);
        }
        let result = match durable_rx.await {
            Ok(result) => result,
            Err(_) => {
                let _ = handle.await;
                return Err(CliMetricsError::WorkerStopped);
            }
        };
        if tokio::time::timeout(SHUTDOWN_TIMEOUT, &mut handle)
            .await
            .is_err()
        {
            handle.abort();
            let _ = handle.await;
        }
        result
    }
}

impl Default for MetricsSender {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy)]
enum OperationKind {
    Compile,
    DeployLocal,
    DeployCloud,
    RedeployLocal,
    Test,
}

async fn metrics_task(
    rx: flume::Receiver<MetricsMessage>,
    root: std::path::PathBuf,
    endpoint: String,
    mut config: MetricsConfig,
    client: telemetry::ClientInfo,
    delivery_timeout: Duration,
) {
    let _ = spool::cleanup_obsolete(&root);
    let _ = spool::apply_privacy(&root, config.level);
    let _ = spool::prune(&root);

    let (delivery_tx, delivery_rx) = flume::bounded(1);
    let delivery_root = root.clone();
    let delivery_endpoint = endpoint.clone();
    let mut delivery_handle = tokio::spawn(async move {
        while delivery_rx.recv_async().await.is_ok() {
            let _ = spool::deliver_pending(&delivery_root, &delivery_endpoint, 8, delivery_timeout)
                .await;
        }
    });
    let _ = delivery_tx.try_send(());

    let mut events = Vec::new();
    let mut interval =
        tokio::time::interval_at(tokio::time::Instant::now() + FLUSH_INTERVAL, FLUSH_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let (shutdown_ack, flush_result) = loop {
        tokio::select! {
            message = rx.recv_async() => {
                let Ok(message) = message else {
                    break (
                        None,
                        flush_events(
                            &root,
                            &client,
                            &mut config,
                            &delivery_tx,
                            &mut events,
                        ),
                    );
                };
                match message {
                    MetricsMessage::Event(event) => events.push(event),
                    MetricsMessage::Shutdown(ack) => {
                        break (
                            Some(ack),
                            flush_events(
                                &root,
                                &client,
                                &mut config,
                                &delivery_tx,
                                &mut events,
                            ),
                        );
                    }
                }
            }
            _ = interval.tick() => {
                let _ = flush_events(
                    &root,
                    &client,
                    &mut config,
                    &delivery_tx,
                    &mut events,
                );
            }
        }
    };

    drop(delivery_tx);
    if let Some(ack) = shutdown_ack {
        let _ = ack.send(flush_result);
    }
    if tokio::time::timeout(SHUTDOWN_TIMEOUT, &mut delivery_handle)
        .await
        .is_err()
    {
        delivery_handle.abort();
        let _ = delivery_handle.await;
    }
}

fn flush_events(
    root: &std::path::Path,
    client: &telemetry::ClientInfo,
    config: &mut MetricsConfig,
    delivery_tx: &flume::Sender<()>,
    events: &mut Vec<QueuedEvent>,
) -> Result<(), CliMetricsError> {
    if events.is_empty() {
        return Ok(());
    }

    let queued = std::mem::take(events);
    let mut persistable = Vec::with_capacity(queued.len());
    let telemetry_events = queued
        .into_iter()
        .filter_map(|event| {
            let telemetry = event.clone().into_telemetry().ok()?;
            persistable.push(event);
            Some(telemetry)
        })
        .collect();
    let contains_install = persistable
        .iter()
        .any(|event| matches!(event.event, Event::CliInstall));
    let result = spool::persist_events(root, client, telemetry_events);
    if let Err(error) = result {
        *events = persistable;
        return Err(error);
    }

    let _ = spool::prune(root);
    let _ = delivery_tx.try_send(());
    if contains_install && !config.install_event_sent {
        // A CLI command can change metrics preferences while this worker still
        // holds the startup snapshot. Reload before setting the install marker
        // so shutdown cannot restore stale privacy settings.
        let mut updated = config::load_metrics_config_from(root).unwrap_or_else(|_| config.clone());
        updated.install_event_sent = true;
        if config::save_metrics_config_to(root, &updated).is_ok() {
            *config = updated;
        }
    }
    Ok(())
}

impl QueuedEvent {
    fn into_telemetry(self) -> Result<telemetry::Event, CliMetricsError> {
        let (name, properties) = self.event.into_properties()?;
        Ok(telemetry::Event::with_identity(
            self.event_id,
            name,
            self.occurred_at,
            properties,
        )?)
    }
}

impl Event {
    fn into_properties(self) -> Result<(&'static str, serde_json::Value), CliMetricsError> {
        let (name, properties) = match self {
            Self::CliInstall => ("cli.install", serde_json::json!({})),
            Self::Compile(event) => ("cli.compile", operation_properties(event)?),
            Self::DeployLocal(event) => ("cli.deploy_local", operation_properties(event)?),
            Self::DeployCloud(event) => ("cli.deploy_cloud", operation_properties(event)?),
            Self::RedeployLocal(event) => ("cli.redeploy_local", operation_properties(event)?),
            Self::Chef(event) => {
                non_empty(&event.run_id, "chef run_id")?;
                validate_outcome(&event.outcome)?;
                ("cli.chef", serde_json::to_value(event)?)
            }
            Self::Test(event) => ("cli.test", operation_properties(event)?),
        };
        Ok((name, properties))
    }
}

fn operation_properties(event: OperationEvent) -> Result<serde_json::Value, CliMetricsError> {
    non_empty(&event.cluster_id, "cluster_id")?;
    validate_outcome(&event.outcome)?;
    Ok(serde_json::to_value(event)?)
}

fn validate_outcome(outcome: &EventOutcome) -> Result<(), CliMetricsError> {
    let EventOutcome::Failed { stage, message } = outcome else {
        return Ok(());
    };
    non_empty(message, "failure message")?;
    if stage
        .as_deref()
        .is_some_and(|stage| stage.trim().is_empty())
    {
        return Err(CliMetricsError::Empty("failure stage"));
    }
    Ok(())
}

fn telemetry_client(config: &MetricsConfig) -> Option<telemetry::ClientInfo> {
    let identity = config.query_identity().ok()??;
    identity.client_info().ok()
}

fn metrics_endpoint() -> Result<String, CliMetricsError> {
    let endpoint = std::env::var("HELIX_TELEMETRY_ENDPOINT")
        .unwrap_or_else(|_| telemetry::DEFAULT_TELEMETRY_ENDPOINT.to_owned());
    telemetry::validate_endpoint(&endpoint)?;
    Ok(endpoint)
}

fn non_empty(value: &str, name: &'static str) -> Result<(), CliMetricsError> {
    if value.trim().is_empty() {
        return Err(CliMetricsError::Empty(name));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::atomic::AtomicBool;

    use tokio::io::AsyncReadExt as _;
    use tokio::net::TcpListener;

    use super::*;

    fn queued(event: Event) -> QueuedEvent {
        QueuedEvent {
            event_id: Uuid::now_v7().to_string(),
            occurred_at: telemetry::timestamp_now(),
            event,
        }
    }

    fn operation() -> OperationEvent {
        OperationEvent {
            cluster_id: "cluster-1".to_owned(),
            queries_string: "QUERY FindUser() =>\n    user <- N<User>\n    RETURN user".to_owned(),
            query_count: 1,
            duration_seconds: 2,
            outcome: EventOutcome::Succeeded,
        }
    }

    async fn hanging_endpoint() -> (String, oneshot::Receiver<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let (observed_tx, observed_rx) = oneshot::channel();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut request = vec![0_u8; 64 * 1024];
            let _ = stream.read(&mut request).await;
            let _ = observed_tx.send(());
            std::future::pending::<()>().await;
        });
        (format!("http://{address}/v1/events"), observed_rx)
    }

    fn pending_envelopes(root: &std::path::Path) -> Vec<telemetry::Envelope> {
        fs::read_dir(root.join("metrics").join("spool"))
            .expect("read spool")
            .filter_map(Result::ok)
            .map(|entry| fs::read(entry.path()).expect("read envelope"))
            .map(|bytes| telemetry::Envelope::from_slice(&bytes).expect("valid envelope"))
            .collect()
    }

    #[test]
    fn exact_cli_envelope_never_contains_email() {
        let config = MetricsConfig {
            level: MetricsLevel::Full,
            user_id: Some("user-1".to_owned()),
            email: Some("user@example.com".to_owned()),
            ..MetricsConfig::default()
        };
        let event = QueuedEvent {
            event_id: "018f6f1e-0000-7000-8000-000000000001".to_owned(),
            occurred_at: "2026-07-28T12:00:00Z".to_owned(),
            event: Event::CliInstall,
        }
        .into_telemetry()
        .expect("CLI event");
        let envelope = telemetry::Envelope::with_sent_at(
            telemetry::Source::Cli,
            "2026-07-28T12:00:01Z",
            telemetry_client(&config).expect("client"),
            vec![event],
        )
        .expect("envelope");
        let encoded = String::from_utf8(envelope.to_vec().expect("encode")).expect("UTF-8");
        assert_eq!(
            encoded,
            format!(
                r#"{{"schema_version":1,"source":"helix-cli","sent_at":"2026-07-28T12:00:01Z","client":{{"version":"{}","os":"{}","arch":"{}","installation_id":"{}","user_id":"user-1"}},"events":[{{"event_id":"018f6f1e-0000-7000-8000-000000000001","name":"cli.install","occurred_at":"2026-07-28T12:00:00Z","properties":{{}}}}]}}"#,
                env!("CARGO_PKG_VERSION"),
                std::env::consts::OS,
                std::env::consts::ARCH,
                config.installation_id.as_deref().expect("installation id"),
            )
        );
        assert!(!encoded.contains("email"));
        assert!(!encoded.contains("user@example.com"));
    }

    #[tokio::test]
    async fn sender_is_non_blocking_when_disabled_or_disconnected() {
        let sender = MetricsSender::disabled();
        assert!(!sender.send_event(Event::CliInstall));
        sender.shutdown().await.expect("disabled shutdown");

        let (tx, rx) = flume::bounded(1);
        drop(rx);
        let install_event_queued = Arc::new(AtomicBool::new(false));
        let sender = MetricsSender {
            tx: Some(tx),
            handle: None,
            install_event_queued: Some(Arc::clone(&install_event_queued)),
        };
        assert!(!sender.send_event(Event::CliInstall));
        sender.send_cli_install_event_if_first_time();
        assert!(!install_event_queued.load(Ordering::Acquire));
    }

    #[test]
    fn sender_preserves_event_kinds_and_rejects_queue_pressure() {
        let (tx, rx) = flume::bounded(6);
        let sender = MetricsSender {
            tx: Some(tx),
            handle: None,
            install_event_queued: Some(Arc::new(AtomicBool::new(false))),
        };
        sender.send_compile_event(
            "cluster-1".to_owned(),
            "compile".to_owned(),
            1,
            1,
            true,
            None,
        );
        sender.send_deploy_local_event(
            "cluster-1".to_owned(),
            "deploy local".to_owned(),
            1,
            2,
            false,
            Some("failed".to_owned()),
        );
        sender.send_deploy_cloud_event(
            "cluster-1".to_owned(),
            "deploy cloud".to_owned(),
            1,
            3,
            true,
            None,
        );
        sender.send_redeploy_local_event(
            "cluster-1".to_owned(),
            "redeploy".to_owned(),
            1,
            4,
            true,
            None,
        );
        sender.send_test_event("cluster-1".to_owned(), "test".to_owned(), 1, 5, true, None);
        sender.send_chef_event(ChefEvent {
            run_id: "run-1".to_owned(),
            phase: ChefPhase::Completed,
            duration_seconds: Some(6),
            setup_mode: Some(ChefSetupMode::Automatic),
            has_custom_intent: true,
            agent: Some("codex".to_owned()),
            overview_size_bytes: Some(7),
            project_snapshot_size_bytes: Some(8),
            outcome: EventOutcome::Succeeded,
        });
        assert!(!sender.send_event(Event::CliInstall));

        let events = rx
            .try_iter()
            .map(|message| {
                let MetricsMessage::Event(event) = message else {
                    panic!("event message");
                };
                event.event
            })
            .collect::<Vec<_>>();
        assert!(matches!(events[0], Event::Compile(_)));
        assert!(matches!(events[1], Event::DeployLocal(_)));
        assert!(matches!(events[2], Event::DeployCloud(_)));
        assert!(matches!(events[3], Event::RedeployLocal(_)));
        assert!(matches!(events[4], Event::Test(_)));
        assert!(matches!(events[5], Event::Chef(_)));
    }

    #[test]
    fn failed_persistence_keeps_events_and_install_marker_retryable() {
        let parent = tempfile::tempdir().expect("parent");
        let root = parent.path().join("not-a-directory");
        fs::write(&root, b"file").expect("blocking file");
        let mut config = MetricsConfig::default();
        let client = telemetry_client(&config).expect("client");
        let (delivery_tx, delivery_rx) = flume::bounded(1);
        let mut events = vec![queued(Event::CliInstall)];

        assert!(flush_events(&root, &client, &mut config, &delivery_tx, &mut events).is_err());
        assert_eq!(events.len(), 1);
        assert!(!config.install_event_sent);

        fs::remove_file(&root).expect("remove blocking file");
        flush_events(&root, &client, &mut config, &delivery_tx, &mut events)
            .expect("retry persistence");
        assert!(events.is_empty());
        assert!(config.install_event_sent);
        assert!(delivery_rx.try_recv().is_ok());
        assert!(
            config::load_metrics_config_from(&root)
                .expect("saved config")
                .install_event_sent
        );
    }

    #[test]
    fn install_marker_preserves_preferences_changed_after_worker_start() {
        let root = tempfile::tempdir().expect("root");
        let mut worker_config = MetricsConfig::default();
        config::save_metrics_config_to(root.path(), &worker_config).expect("startup config");
        let updated = MetricsConfig {
            level: MetricsLevel::Full,
            email: Some("user@example.com".to_owned()),
            ..worker_config.clone()
        };
        config::save_metrics_config_to(root.path(), &updated).expect("updated preferences");
        let client = telemetry_client(&worker_config).expect("client");
        let (delivery_tx, _delivery_rx) = flume::bounded(1);
        let mut events = vec![queued(Event::CliInstall)];

        flush_events(
            root.path(),
            &client,
            &mut worker_config,
            &delivery_tx,
            &mut events,
        )
        .expect("flush install event");

        let saved = config::load_metrics_config_from(root.path()).expect("saved config");
        assert_eq!(saved.level, MetricsLevel::Full);
        assert_eq!(saved.email.as_deref(), Some("user@example.com"));
        assert!(saved.install_event_sent);
        assert_eq!(worker_config, saved);
    }

    #[test]
    fn invalid_events_are_discarded_without_blocking_valid_events() {
        let root = tempfile::tempdir().expect("root");
        let mut config = MetricsConfig::default();
        let client = telemetry_client(&config).expect("client");
        let (delivery_tx, _delivery_rx) = flume::bounded(1);
        let mut events = vec![
            queued(Event::Compile(OperationEvent {
                cluster_id: String::new(),
                ..operation()
            })),
            queued(Event::CliInstall),
        ];

        flush_events(root.path(), &client, &mut config, &delivery_tx, &mut events)
            .expect("flush valid event");
        let envelopes = pending_envelopes(root.path());
        assert_eq!(envelopes.len(), 1);
        assert_eq!(envelopes[0].events.len(), 1);
        assert_eq!(envelopes[0].events[0].name, "cli.install");
    }

    #[tokio::test]
    async fn hanging_delivery_never_blocks_durable_shutdown() {
        let root = tempfile::tempdir().expect("root");
        let config = MetricsConfig::default();
        let client = telemetry_client(&config).expect("client");
        spool::persist_events(
            root.path(),
            &client,
            vec![telemetry::Event::new("cli.test", serde_json::json!({})).expect("old event")],
        )
        .expect("old spool");
        let (endpoint, request_observed) = hanging_endpoint().await;
        let sender = MetricsSender::configured_with_delivery_timeout(
            root.path().to_path_buf(),
            endpoint,
            config,
            Duration::from_millis(100),
        );
        request_observed.await.expect("startup delivery request");

        sender.send_cli_install_event_if_first_time();
        sender.send_cli_install_event_if_first_time();
        assert!(sender.send_event(Event::Compile(operation())));
        sender.shutdown().await.expect("durable shutdown");

        let envelopes = pending_envelopes(root.path());
        let names = envelopes
            .iter()
            .flat_map(|envelope| envelope.events.iter())
            .map(|event| event.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            names.iter().filter(|name| **name == "cli.install").count(),
            1
        );
        assert!(names.contains(&"cli.compile"));
        assert!(
            config::load_metrics_config_from(root.path())
                .expect("config")
                .install_event_sent
        );
    }

    #[tokio::test]
    async fn shutdown_reports_a_stopped_worker() {
        let (tx, rx) = flume::bounded(1);
        drop(rx);
        let sender = MetricsSender {
            tx: Some(tx),
            handle: Some(tokio::spawn(async {})),
            install_event_queued: Some(Arc::new(AtomicBool::new(false))),
        };
        assert!(matches!(
            sender.shutdown().await,
            Err(CliMetricsError::WorkerStopped)
        ));
    }

    #[test]
    fn event_validation_covers_outcomes_and_chef_enums() {
        assert_eq!(
            ChefPhase::try_from("started").expect("started"),
            ChefPhase::Started
        );
        assert_eq!(
            ChefPhase::try_from("auth_failed").expect("auth failed"),
            ChefPhase::AuthFailed
        );
        assert_eq!(
            ChefPhase::try_from("upload_failed").expect("upload failed"),
            ChefPhase::UploadFailed
        );
        assert!(ChefPhase::try_from("unknown").is_err());
        assert!(validate_outcome(&EventOutcome::Succeeded).is_ok());
        assert!(validate_outcome(&EventOutcome::Failed {
            stage: Some(" ".to_owned()),
            message: "failed".to_owned(),
        })
        .is_err());
        assert!(validate_outcome(&EventOutcome::Failed {
            stage: None,
            message: String::new(),
        })
        .is_err());
    }
}
