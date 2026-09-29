use crate::cloud::CloudClient;
use crate::config::{DatabaseReference, InstanceInfo};
use crate::errors::{Candidate, CliError};
use crate::output::{self, Verbosity};
use crate::project::ProjectContext;
use crate::prompts;
use base64::Engine as _;
use console::style;
use eyre::{eyre, Report, Result};
use reqwest::header::CONTENT_TYPE;
use serde_json::Value;
use std::time::{Duration, Instant};

/// Where a query goes.
pub(crate) enum Target {
    /// A local instance from helix.toml, reached over plain HTTP.
    Local { name: String, port: u16 },
    /// A Cloud database, reached through the session-authenticated broker.
    Cloud {
        name: String,
        database: DatabaseReference,
    },
}

impl Target {
    /// Resolve the CLI's target argument. A typed `cluster:<id>`/`tenant:<id>`
    /// reference needs no helix.toml; anything else names an instance.
    pub(crate) fn resolve(instance: Option<String>) -> Result<Self> {
        let database = instance
            .as_deref()
            .and_then(|target| target.parse::<DatabaseReference>().ok());
        let Some(database) = database else {
            let project = ProjectContext::find_and_load(None)?;
            let name = resolve_instance_name(&project, instance)?;
            return Ok(match project.config.get_instance(&name)? {
                InstanceInfo::Local(config) => Self::Local {
                    port: config.port,
                    name,
                },
                InstanceInfo::Enterprise(config) => Self::Cloud {
                    database: config.database.clone(),
                    name,
                },
            });
        };
        Ok(Self::Cloud {
            name: database.to_string(),
            database,
        })
    }

    pub(crate) fn name(&self) -> &str {
        match self {
            Self::Local { name, .. } | Self::Cloud { name, .. } => name,
        }
    }
}

/// Overrides that only apply to local targets.
#[derive(Default)]
pub struct LocalOverrides {
    pub warm: bool,
    pub host: Option<String>,
    pub port: Option<u16>,
}

/// A completed query.
pub(crate) struct Outcome {
    status: reqwest::StatusCode,
    elapsed: Duration,
    /// The decoded response; `None` for an empty body (e.g. 204).
    body: Option<Value>,
}

pub async fn run(
    instance: Option<String>,
    file: Option<String>,
    body: Option<String>,
    ts: Option<String>,
    ts_file: Option<String>,
    overrides: LocalOverrides,
) -> Result<()> {
    let target = Target::resolve(instance)?;
    let request = parse_query_request(file, body, ts, ts_file)?;
    let outcome = execute(&target, request, &overrides).await?;
    print_outcome(&target, &outcome)
}

/// Pick the instance to query: the explicit name, else `dev`, else the only
/// instance, else a prompt, else an error listing the candidates.
pub(crate) fn resolve_instance_name(
    project: &ProjectContext,
    instance: Option<String>,
) -> Result<String> {
    let has_dev =
        project.config.local.contains_key("dev") || project.config.enterprise.contains_key("dev");
    let instances = project.config.list_instances_with_types();
    match (instance, instances.as_slice()) {
        (Some(instance), _) => Ok(instance),
        (None, _) if has_dev => Ok("dev".to_owned()),
        (None, [(only, _)]) => Ok((*only).clone()),
        _ if prompts::is_interactive() => prompts::select_instance(
            &instances
                .iter()
                .map(|(name, kind)| ((*name).clone(), (*kind).to_owned()))
                .collect::<Vec<_>>(),
            "Which instance should be queried?",
        ),
        _ => Err(CliError::new("no default query target")
            .with_hint("pass an instance name, or cluster:<id> / tenant:<id>")
            .with_candidates(
                instances
                    .iter()
                    .map(|(name, _)| Candidate {
                        id: (*name).clone(),
                        name: (*name).clone(),
                    })
                    .collect(),
            )
            .into()),
    }
}

pub(crate) async fn execute(
    target: &Target,
    request: Value,
    overrides: &LocalOverrides,
) -> Result<Outcome> {
    let request_type = validate_dynamic_request(&request, overrides.warm)?;
    let started = Instant::now();
    let (status, body) = match target {
        Target::Local { name, port } => {
            let host = overrides.host.as_deref().unwrap_or("localhost");
            let port = overrides.port.unwrap_or(*port);
            let endpoint = format!("http://{host}:{port}/v2/query");
            let mut http = reqwest::Client::new()
                .post(&endpoint)
                .header(CONTENT_TYPE, "application/json");
            if overrides.warm {
                http = http.header("X-Helix-Warm", "true");
            }
            let response = http
                .json(&request)
                .send()
                .await
                .map_err(|error| -> Report {
                    if error.is_connect() || error.is_timeout() {
                        connect_error(name, &endpoint, &error.to_string()).into()
                    } else {
                        error.into()
                    }
                })?;
            let status = response.status();
            (status, response.bytes().await?.to_vec())
        }
        Target::Cloud { database, .. } => {
            if overrides.host.is_some() || overrides.port.is_some() {
                return Err(eyre!(
                    "--host and --port are only valid for local instances"
                ));
            }
            if overrides.warm {
                return Err(eyre!("--warm is only supported for local queries"));
            }
            execute_cloud_query(database, request_type, &request).await?
        }
    };
    let elapsed = started.elapsed();
    if !status.is_success() {
        return Err(CliError::new(format!("query failed with HTTP {status}"))
            .with_caused_by(String::from_utf8_lossy(&body).trim())
            .into());
    }
    let body = (!body.iter().all(u8::is_ascii_whitespace)).then(|| {
        serde_json::from_slice(&body)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&body).into_owned()))
    });
    Ok(Outcome {
        status,
        elapsed,
        body,
    })
}

async fn execute_cloud_query(
    database: &DatabaseReference,
    request_type: &str,
    request_json: &Value,
) -> Result<(reqwest::StatusCode, Vec<u8>)> {
    let query_json = serde_json::to_vec(request_json)?;
    let payload = serde_json::json!({
        "database": database.query_request(),
        "queryJson": base64::engine::general_purpose::STANDARD.encode(query_json),
    });
    let path = match request_type {
        "read" => "/v1/databases:query-read",
        "write" => "/v1/databases:query-write",
        _ => unreachable!("validated request type"),
    };
    let response = CloudClient::new()?
        .post(path, payload, "execute Cloud query")
        .await?;
    let status = response
        .get("statusCode")
        .and_then(Value::as_u64)
        .and_then(|status| u16::try_from(status).ok())
        .and_then(|status| reqwest::StatusCode::from_u16(status).ok())
        .ok_or_else(|| eyre!("Cloud query response has no valid statusCode"))?;
    let encoded = response
        .get("responseJson")
        .and_then(Value::as_str)
        .ok_or_else(|| eyre!("Cloud query response has no responseJson"))?;
    let body = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|error| eyre!("Cloud query response is invalid: {error}"))?;
    Ok((status, body))
}

/// The result goes to stdout — highlighted pretty JSON for humans, compact
/// JSON under `--json` — and is still printed under `--quiet`. A dim footer
/// with status and latency goes to stderr.
pub(crate) fn print_outcome(target: &Target, outcome: &Outcome) -> Result<()> {
    outcome
        .body
        .as_ref()
        .map(|body| {
            output::emit(body, |body| {
                println!("{}", output::json::pretty(body, console::colors_enabled()));
                Ok(())
            })
        })
        .transpose()?;
    if Verbosity::current().show_normal() {
        eprintln!(
            "{}",
            style(format!(
                "{} · {} · {}",
                outcome.status,
                output::format_duration(outcome.elapsed),
                target.name()
            ))
            .dim()
            .for_stderr()
        );
    }
    Ok(())
}

fn connect_error(instance: &str, endpoint: &str, cause: &str) -> CliError {
    CliError::new(format!(
        "cannot reach Helix instance '{instance}' at {endpoint}"
    ))
    .with_context(cause.to_string())
    .with_hint(format!(
        "No Helix instance is listening there. Start it with `helix start {instance}` and check it with `helix status {instance}`. If it runs on another host/port, pass --host/--port."
    ))
}

fn parse_query_request(
    file: Option<String>,
    body: Option<String>,
    ts: Option<String>,
    ts_file: Option<String>,
) -> Result<Value> {
    let provided = [
        file.is_some(),
        body.is_some(),
        ts.is_some(),
        ts_file.is_some(),
    ]
    .into_iter()
    .filter(|present| *present)
    .count();
    if provided == 0 {
        return Err(eyre!(
            "Provide a query with --file <path>, --body '<json>', -e '<ts>', or --ts-file <path>"
        ));
    }
    if provided > 1 {
        return Err(eyre!(
            "--file, --body, -e/--ts, and --ts-file are mutually exclusive"
        ));
    }

    if let Some(file) = file {
        let request_text = std::fs::read_to_string(&file)
            .map_err(|e| eyre!("Failed to read query request file '{file}': {e}"))?;
        return serde_json::from_str(&request_text)
            .map_err(|e| eyre!("Failed to parse query request file '{file}': {e}"));
    }
    if let Some(body) = body {
        return serde_json::from_str(&body)
            .map_err(|e| eyre!("Failed to parse query request JSON: {e}"));
    }
    if let Some(ts) = ts {
        return crate::ts_query::build_request_from_ts(&ts);
    }
    let ts_file = ts_file.expect("exactly one query input is present");
    let snippet = std::fs::read_to_string(&ts_file)
        .map_err(|e| eyre!("Failed to read TypeScript query file '{ts_file}': {e}"))?;
    crate::ts_query::build_request_from_ts(&snippet)
}

fn validate_dynamic_request(request: &Value, warm: bool) -> Result<&str> {
    let request_type = request
        .get("request_type")
        .and_then(Value::as_str)
        .ok_or_else(|| eyre!("query request must include request_type"))?;
    if request_type != "read" && request_type != "write" {
        return Err(eyre!("request_type must be lowercase 'read' or 'write'"));
    }
    if warm && request_type != "read" {
        return Err(eyre!("--warm is only valid for read requests"));
    }
    if request.get("query").is_none() {
        return Err(eyre!("query request must include query"));
    }
    Ok(request_type)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_query_request_accepts_inline_json() {
        let request = parse_query_request(
            None,
            Some(r#"{"request_type":"read","query":{"queries":[]}}"#.to_string()),
            None,
            None,
        )
        .expect("inline JSON should parse");
        assert_eq!(request["request_type"], "read");
    }

    #[test]
    fn parse_query_request_rejects_missing_or_multiple_inputs() {
        assert!(parse_query_request(None, None, None, None)
            .unwrap_err()
            .to_string()
            .contains("--file <path>, --body"));
        assert!(
            parse_query_request(Some("request.json".into()), Some("{}".into()), None, None)
                .unwrap_err()
                .to_string()
                .contains("mutually exclusive")
        );
    }

    #[test]
    fn validates_request_type_and_warm_mode() {
        let read = serde_json::json!({"request_type":"read","query":{}});
        assert_eq!(validate_dynamic_request(&read, true).unwrap(), "read");
        let write = serde_json::json!({"request_type":"write","query":{}});
        assert!(validate_dynamic_request(&write, true).is_err());
        assert!(validate_dynamic_request(
            &serde_json::json!({"request_type":"READ","query":{}}),
            false
        )
        .is_err());
    }

    #[test]
    fn typed_database_targets_skip_helix_toml() {
        let Target::Cloud { name, database } = Target::resolve(Some("tenant:abc".into())).unwrap()
        else {
            panic!("typed reference must resolve to a Cloud target");
        };
        assert_eq!(name, "tenant:abc");
        assert_eq!(database, DatabaseReference::Tenant("abc".into()));
    }

    #[test]
    fn instance_name_defaults_to_dev_then_the_only_instance_then_errors() {
        use crate::config::{EnterpriseInstanceConfig, HelixConfig};
        let mut config = HelixConfig::default_config("project");
        let project = |config: HelixConfig| ProjectContext {
            root: std::path::PathBuf::from("/tmp"),
            helix_dir: std::path::PathBuf::from("/tmp/.helix"),
            config,
        };
        assert_eq!(
            resolve_instance_name(&project(config.clone()), Some("qa".into())).unwrap(),
            "qa"
        );
        assert_eq!(
            resolve_instance_name(&project(config.clone()), None).unwrap(),
            "dev"
        );

        let dev = config.local.remove("dev").unwrap();
        config.local.insert("preview".into(), dev);
        assert_eq!(
            resolve_instance_name(&project(config.clone()), None).unwrap(),
            "preview"
        );

        config.enterprise.insert(
            "production".into(),
            EnterpriseInstanceConfig {
                database: DatabaseReference::Tenant("t".into()),
                workspace_id: None,
                project_id: None,
            },
        );
        let error = resolve_instance_name(&project(config), None).unwrap_err();
        let error = error.downcast_ref::<CliError>().unwrap();
        let names: Vec<_> = error.candidates.iter().map(|c| c.id.as_str()).collect();
        assert_eq!(names, ["preview", "production"]);
    }

    #[test]
    fn connect_error_points_at_local_recovery() {
        let error = connect_error("dev", "http://localhost:8080/v2/query", "refused");
        assert!(error.hint.unwrap().contains("helix start dev"));
    }
}
