use crate::commands::auth::require_auth;
use crate::config::{DatabaseReference, InstanceInfo};
use crate::errors::CliError;
use crate::local_runtime::LocalRuntime;
use crate::output::{self, table, OutputMode};
use crate::project::ProjectContext;
use chrono::{DateTime, Duration, Utc};
use eyre::Result;
use serde::{Deserialize, Serialize};

/// One failed Cloud query.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryError {
    #[serde(default)]
    timestamp: String,
    #[serde(default)]
    query_name: String,
    #[serde(default)]
    output: String,
}

/// Local instances stream container logs; Cloud instances list recent query
/// errors (the last hour unless `--start`/`--end` say otherwise).
pub async fn run(
    instance: Option<String>,
    follow: bool,
    start: Option<String>,
    end: Option<String>,
) -> Result<()> {
    let project = ProjectContext::find_and_load(None)?;
    let instance = super::query::resolve_instance_name(&project, instance)?;
    match project.config.get_instance(&instance)? {
        InstanceInfo::Local(_) => {
            if start.is_some() || end.is_some() {
                return Err(CliError::new("--start and --end only apply to Cloud instances")
                    .with_hint("local logs come straight from docker/podman; use --follow to stream them")
                    .into());
            }
            if OutputMode::current().is_json() {
                return Err(CliError::new("local logs are plain text")
                    .with_hint("--json is only supported for Cloud query errors")
                    .into());
            }
            LocalRuntime::new(&project).logs(&instance, follow)?;
        }
        InstanceInfo::Enterprise(config) => {
            if follow {
                return Err(CliError::new("Cloud logs cannot be followed")
                    .with_hint("omit --follow to list recent query errors")
                    .into());
            }
            let (start, end) = parse_range(start, end)?;
            let path = match &config.database {
                DatabaseReference::Cluster(id) => format!("/v1/clusters/{id}/query-errors"),
                DatabaseReference::Tenant(id) => format!("/v1/tenants/{id}/query-errors"),
            };
            let (start_time, end_time) =
                (start.timestamp().to_string(), end.timestamp().to_string());
            let errors: Vec<QueryError> = require_auth()
                .await?
                .list(
                    &path,
                    &[("startTime", &start_time), ("endTime", &end_time)],
                    "errors",
                    "list Cloud query errors",
                )
                .await?;
            output::emit(&errors, |errors| {
                if errors.is_empty() {
                    output::info(&format!(
                        "No query errors on {instance} between {} and {}",
                        start.format("%Y-%m-%d %H:%M UTC"),
                        end.format("%Y-%m-%d %H:%M UTC")
                    ));
                    return Ok(());
                }
                let mut rows = table::Table::new(["TIME", "QUERY", "ERROR"]);
                for error in errors {
                    rows.row([
                        error.timestamp.clone(),
                        error.query_name.clone(),
                        error.output.lines().collect::<Vec<_>>().join(" "),
                    ]);
                }
                rows.print();
                Ok(())
            })?;
        }
    }
    Ok(())
}

fn parse_range(
    start: Option<String>,
    end: Option<String>,
) -> Result<(DateTime<Utc>, DateTime<Utc>)> {
    let parse = |flag: &str, value: &str| -> Result<DateTime<Utc>> {
        DateTime::parse_from_rfc3339(value)
            .map(|time| time.with_timezone(&Utc))
            .map_err(|error| {
                CliError::new(format!("--{flag} '{value}' is not an RFC 3339 time"))
                    .with_caused_by(error.to_string())
                    .with_hint("use a time like 2026-01-02T15:04:05Z")
                    .into()
            })
    };
    let end = match end {
        Some(end) => parse("end", &end)?,
        None => Utc::now(),
    };
    let start = match start {
        Some(start) => parse("start", &start)?,
        None => end - Duration::hours(1),
    };
    if start >= end {
        return Err(CliError::new("--start must be before --end").into());
    }
    Ok((start, end))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_log_range_is_parsed_in_utc() {
        let (start, end) = parse_range(
            Some("2026-01-01T00:00:00+01:00".to_string()),
            Some("2026-01-01T02:00:00+01:00".to_string()),
        )
        .unwrap();
        assert_eq!(start.timestamp(), 1_767_222_000);
        assert_eq!(end.timestamp(), 1_767_229_200);
    }

    #[test]
    fn missing_start_defaults_to_one_hour_before_end() {
        let (start, end) = parse_range(None, Some("2026-01-01T02:00:00Z".to_string())).unwrap();
        assert_eq!(end - start, Duration::hours(1));
    }

    #[test]
    fn malformed_or_inverted_ranges_are_rejected() {
        let error = parse_range(Some("yesterday".to_string()), None).unwrap_err();
        assert!(error.to_string().contains("--start 'yesterday'"));
        assert!(parse_range(None, Some("later".to_string())).is_err());
        assert!(parse_range(
            Some("2026-01-01T02:00:00Z".to_string()),
            Some("2026-01-01T01:00:00Z".to_string()),
        )
        .is_err());
    }
}
