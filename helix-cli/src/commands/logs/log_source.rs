//! Log source abstraction for local Docker and cloud instances.

use crate::commands::auth::Credentials;
use crate::commands::integrations::helix::CLOUD_AUTHORITY;
use crate::config::ContainerRuntime;
use crate::docker::DockerManager;
use crate::project::ProjectContext;
use crate::sse_client::{SseClient, SseEvent};
use chrono::{DateTime, Utc};
use eyre::{Result, eyre};
use serde::Deserialize;
use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};

/// Cloud range logs API response format.
#[derive(Debug, Deserialize)]
struct CloudLogsRangeResponse {
    logs: Vec<CloudDeploymentLog>,
}

/// Individual log entry from cloud range API.
#[derive(Debug, Deserialize)]
struct CloudDeploymentLog {
    message: String,
    #[allow(dead_code)]
    severity: Option<String>,
    #[allow(dead_code)]
    timestamp: String,
}

/// Represents where to fetch logs from.
#[derive(Clone)]
pub enum LogSource {
    /// Local Docker/Podman container
    Local {
        container_name: String,
        runtime: ContainerRuntime,
    },
    /// Helix Cloud instance
    Cloud {
        cluster_id: String,
        user_id: String,
        api_key: String,
    },
}

impl LogSource {
    /// Create a log source from project context and instance name.
    pub fn from_instance(
        project: &ProjectContext,
        instance_name: &str,
        credentials: Option<&Credentials>,
    ) -> Result<Self> {
        let instance_config = project.config.get_instance(instance_name)?;

        if instance_config.is_local() {
            let docker = DockerManager::new(project);
            let project_name = format!("helix-{}-{}", project.config.project.name, instance_name);
            let container_name = format!("{project_name}_app");

            Ok(LogSource::Local {
                container_name,
                runtime: docker.runtime,
            })
        } else {
            // Cloud instance
            let credentials = credentials
                .ok_or_else(|| eyre!("Authentication required for cloud instance logs"))?;
            let cluster_id = instance_config
                .cluster_id()
                .ok_or_else(|| eyre!("Cloud instance must have a cluster_id"))?
                .to_string();

            Ok(LogSource::Cloud {
                cluster_id,
                user_id: credentials.user_id.clone(),
                api_key: credentials.helix_admin_key.clone(),
            })
        }
    }

    /// Stream live logs. Calls the callback with each log line.
    /// Returns when the stream ends or an error occurs.
    pub async fn stream_live<F>(&self, mut on_line: F) -> Result<()>
    where
        F: FnMut(String),
    {
        match self {
            LogSource::Local {
                container_name,
                runtime,
            } => stream_local_logs(container_name, runtime, &mut on_line),
            LogSource::Cloud {
                cluster_id,
                user_id,
                api_key,
            } => stream_cloud_logs(cluster_id, user_id, api_key, &mut on_line).await,
        }
    }

    /// Query historical logs within a time range.
    pub async fn query_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<String>> {
        match self {
            LogSource::Local {
                container_name,
                runtime,
            } => query_local_logs(container_name, runtime, start, end),
            LogSource::Cloud {
                cluster_id,
                user_id,
                api_key,
            } => query_cloud_logs(cluster_id, user_id, api_key, start, end).await,
        }
    }
}

/// Stream logs from a local Docker/Podman container.
fn stream_local_logs<F>(
    container_name: &str,
    runtime: &ContainerRuntime,
    on_line: &mut F,
) -> Result<()>
where
    F: FnMut(String),
{
    let mut child = Command::new(runtime.binary())
        .args(["logs", "-f", "--tail", "100", container_name])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| eyre!("Failed to spawn {} logs command: {}", runtime.binary(), e))?;

    // Read from both stdout and stderr (docker logs outputs to both)
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    // Spawn a thread to read stderr
    let stderr_handle = if let Some(stderr) = stderr {
        let reader = BufReader::new(stderr);
        Some(std::thread::spawn(move || {
            reader.lines().map_while(Result::ok).collect::<Vec<_>>()
        }))
    } else {
        None
    };

    // Read stdout in main flow
    if let Some(stdout) = stdout {
        let reader = BufReader::new(stdout);
        for line in reader.lines() {
            match line {
                Ok(line) => on_line(line),
                Err(e) => return Err(eyre!("Error reading log line: {}", e)),
            }
        }
    }

    // Collect stderr lines
    if let Some(handle) = stderr_handle
        && let Ok(lines) = handle.join()
    {
        for line in lines {
            on_line(line);
        }
    }

    let status = child.wait()?;
    if !status.success() {
        return Err(eyre!("Docker logs command failed with status: {}", status));
    }

    Ok(())
}

/// Query historical logs from a local Docker/Podman container.
fn query_local_logs(
    container_name: &str,
    runtime: &ContainerRuntime,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Vec<String>> {
    let since = start.format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let until = end.format("%Y-%m-%dT%H:%M:%SZ").to_string();

    let output = Command::new(runtime.binary())
        .args(["logs", "--since", &since, "--until", &until, container_name])
        .output()
        .map_err(|e| eyre!("Failed to run {} logs command: {}", runtime.binary(), e))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(eyre!("Docker logs failed: {}", stderr));
    }

    // Combine stdout and stderr (docker logs outputs to both)
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    let mut lines: Vec<String> = stdout.lines().map(String::from).collect();
    lines.extend(stderr.lines().map(String::from));

    Ok(lines)
}

/// Stream live logs from Helix Cloud via SSE.
async fn stream_cloud_logs<F>(
    cluster_id: &str,
    user_id: &str,
    api_key: &str,
    on_line: &mut F,
) -> Result<()>
where
    F: FnMut(String),
{
    let url = format!("https://{}/logs/live", *CLOUD_AUTHORITY);

    let client = SseClient::new(url)
        .header("x-api-key", api_key)
        .header("x-cluster-id", cluster_id)
        .header("x-user-id", user_id);

    client
        .connect(|event| {
            match event {
                SseEvent::Log { message, .. } => on_line(message),
                SseEvent::BackfillComplete => {}
                SseEvent::Error { error } => {
                    return Err(eyre!("Log stream error from server: {}", error));
                }
                _ => {} // Ignore other event types
            }
            Ok(true)
        })
        .await
}

/// Query historical logs from Helix Cloud.
async fn query_cloud_logs(
    cluster_id: &str,
    user_id: &str,
    api_key: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Vec<String>> {
    // Cloud API expects Unix timestamps in seconds
    let start_ts = start.timestamp();
    let end_ts = end.timestamp();

    let url = format!(
        "https://{}/logs/range?start_time={}&end_time={}",
        *CLOUD_AUTHORITY, start_ts, end_ts
    );

    let client = reqwest::Client::new();
    let response = client
        .get(&url)
        .header("x-api-key", api_key)
        .header("x-cluster-id", cluster_id)
        .header("x-user-id", user_id)
        .send()
        .await?;

    if !response.status().is_success() {
        let error = response.text().await.unwrap_or_default();
        return Err(eyre!("Failed to fetch logs: {}", error));
    }

    let range_response: CloudLogsRangeResponse = response.json().await?;
    let logs = range_response.logs.into_iter().map(|l| l.message).collect();
    Ok(logs)
}
