use crate::commands::auth::require_auth;
use crate::config::InstanceInfo;
use crate::enterprise_cloud::cloud_base_url;
use crate::local_runtime::Runtime;
use crate::project::ProjectContext;
use crate::prompts;
use chrono::{DateTime, Duration, Utc};
use eyre::{Result, eyre};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct LogsRangeResponse {
    logs: Vec<LogEntry>,
}

#[derive(Debug, Deserialize)]
struct LogEntry {
    message: String,
}

pub async fn run(
    instance: Option<String>,
    follow: bool,
    range: bool,
    start: Option<String>,
    end: Option<String>,
) -> Result<()> {
    let project = ProjectContext::find_and_load(None)?;
    let instance = resolve_instance(&project, instance)?;
    match project.config.get_instance(&instance)? {
        InstanceInfo::Local(_) => {
            if range || start.is_some() || end.is_some() {
                return Err(eyre!(
                    "--range, --start, and --end are only supported for Enterprise logs; local logs use docker/podman logs"
                ));
            }
            Runtime::for_project(&project).logs(&instance, follow)?;
        }
        InstanceInfo::Enterprise(config) => {
            if follow {
                return Err(eyre!(
                    "live Enterprise logs are not supported yet; use --range instead"
                ));
            }
            let credentials = require_auth().await?;
            let (start, end) = parse_range(range, start, end)?;
            let logs =
                query_enterprise_logs(&config.cluster_id, &credentials.helix_admin_key, start, end)
                    .await?;
            for line in logs {
                println!("{line}");
            }
        }
    }
    Ok(())
}

fn resolve_instance(project: &ProjectContext, instance: Option<String>) -> Result<String> {
    if let Some(instance) = instance {
        return Ok(instance);
    }
    let instances = all_instances(project);
    if prompts::is_interactive() && instances.len() > 1 {
        return prompts::select_instance(&instances, "Show logs for which instance?");
    }
    if project.config.local.contains_key("dev") || project.config.enterprise.contains_key("dev") {
        return Ok("dev".to_string());
    }
    if instances.len() == 1 {
        return Ok(instances[0].0.clone());
    }
    Err(eyre!("No instance specified"))
}

fn all_instances(project: &ProjectContext) -> Vec<(String, String)> {
    project
        .config
        .list_instances_with_types()
        .into_iter()
        .map(|(name, kind)| (name.clone(), kind.to_string()))
        .collect()
}

fn parse_range(
    range: bool,
    start: Option<String>,
    end: Option<String>,
) -> Result<(DateTime<Utc>, DateTime<Utc>)> {
    let end = match end {
        Some(end) => DateTime::parse_from_rfc3339(&end)?.with_timezone(&Utc),
        None => Utc::now(),
    };
    let start = match start {
        Some(start) => DateTime::parse_from_rfc3339(&start)?.with_timezone(&Utc),
        None if range => end - Duration::hours(1),
        None => end - Duration::hours(1),
    };
    Ok((start, end))
}

async fn query_enterprise_logs(
    cluster_id: &str,
    api_key: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Vec<String>> {
    let url = format!(
        "{}/api/cli/enterprise-clusters/{}/logs/range?start_time={}&end_time={}",
        cloud_base_url(),
        cluster_id,
        start.timestamp(),
        end.timestamp()
    );
    let response = reqwest::Client::new()
        .get(url)
        .header("x-api-key", api_key)
        .send()
        .await?;
    if !response.status().is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(eyre!("Failed to fetch Enterprise logs: {body}"));
    }
    let payload: LogsRangeResponse = response.json().await?;
    Ok(payload.logs.into_iter().map(|log| log.message).collect())
}
