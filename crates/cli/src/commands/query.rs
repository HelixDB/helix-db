use crate::cloud::CloudClient;
use crate::config::{DatabaseReference, InstanceInfo};
use crate::errors::CliError;
use crate::project::ProjectContext;
use base64::Engine as _;
use eyre::{eyre, Report, Result};
use reqwest::header::CONTENT_TYPE;
use serde_json::Value;

#[allow(clippy::too_many_arguments)]
pub async fn run(
    instance: Option<String>,
    file: Option<String>,
    json: Option<String>,
    ts: Option<String>,
    ts_file: Option<String>,
    warm: bool,
    host: Option<String>,
    port: Option<u16>,
    compact: bool,
) -> Result<()> {
    let project = ProjectContext::find_and_load(None)?;
    let instance = resolve_instance_target(&project, instance)?;
    let request_json = parse_query_request(file, json, ts, ts_file)?;
    execute(&project, &instance, request_json, warm, host, port, compact).await
}

pub(crate) fn resolve_instance_target(
    project: &ProjectContext,
    instance: Option<String>,
) -> Result<String> {
    if let Some(instance) = instance {
        return Ok(instance);
    }
    if project.config.local.contains_key("dev") || project.config.enterprise.contains_key("dev") {
        return Ok("dev".to_owned());
    }
    let instances = project.config.list_instances();
    if instances.len() == 1 {
        return Ok(instances[0].clone());
    }
    let candidates = instances
        .into_iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join(", ");
    Err(eyre!(
        "Cannot derive an unambiguous query target. Pass an instance name or cluster:<id> / tenant:<id>. Candidates: {candidates}"
    ))
}

pub(crate) async fn execute(
    project: &ProjectContext,
    instance: &str,
    request_json: Value,
    warm: bool,
    host: Option<String>,
    port: Option<u16>,
    compact: bool,
) -> Result<()> {
    let request_type = validate_dynamic_request(&request_json, warm)?;
    let explicit_database = instance.parse::<DatabaseReference>().ok();
    let body = if let Some(database) = explicit_database {
        if host.is_some() || port.is_some() {
            return Err(eyre!(
                "--host and --port are only valid for local instances"
            ));
        }
        if warm {
            return Err(eyre!("--warm is only supported for local queries"));
        }
        execute_cloud_query(&database, request_type, &request_json).await?
    } else {
        match project.config.get_instance(instance)? {
            InstanceInfo::Local(config) => {
                let host = host.unwrap_or_else(|| "localhost".to_string());
                let port = port.unwrap_or(config.port);
                let endpoint = format!("http://{host}:{port}/v2/query");
                let mut request = reqwest::Client::new()
                    .post(&endpoint)
                    .header(CONTENT_TYPE, "application/json");
                if warm {
                    request = request.header("X-Helix-Warm", "true");
                }
                let response =
                    request
                        .json(&request_json)
                        .send()
                        .await
                        .map_err(|error| -> Report {
                            if error.is_connect() || error.is_timeout() {
                                connect_error(instance, &endpoint, &error.to_string()).into()
                            } else {
                                error.into()
                            }
                        })?;
                let status = response.status();
                if status == reqwest::StatusCode::NO_CONTENT {
                    return Ok(());
                }
                let body = response.bytes().await?.to_vec();
                if !status.is_success() {
                    return Err(eyre!(
                        "Query failed with HTTP {status}: {}",
                        String::from_utf8_lossy(&body)
                    ));
                }
                body
            }
            InstanceInfo::Enterprise(config) => {
                if host.is_some() || port.is_some() {
                    return Err(eyre!(
                        "--host and --port are only valid for local instances"
                    ));
                }
                if warm {
                    return Err(eyre!("--warm is only supported for local queries"));
                }
                execute_cloud_query(&config.database, request_type, &request_json).await?
            }
        }
    };

    print_response(&body, compact)
}

async fn execute_cloud_query(
    database: &DatabaseReference,
    request_type: &str,
    request_json: &Value,
) -> Result<Vec<u8>> {
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
        .and_then(Value::as_i64)
        .ok_or_else(|| eyre!("Cloud query response has no statusCode"))?;
    let encoded = response
        .get("responseJson")
        .and_then(Value::as_str)
        .ok_or_else(|| eyre!("Cloud query response has no responseJson"))?;
    let body = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|error| eyre!("Cloud query response is invalid: {error}"))?;
    if !(200..300).contains(&status) {
        return Err(eyre!(
            "Query failed with HTTP {status}: {}",
            String::from_utf8_lossy(&body)
        ));
    }
    Ok(body)
}

pub(super) fn print_response(body: &[u8], compact: bool) -> Result<()> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Ok(());
    }
    let value: Value = serde_json::from_slice(body)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(body).into_owned()));
    if crate::output::Verbosity::current().show_normal() {
        if compact {
            println!("{}", serde_json::to_string(&value)?);
        } else {
            println!("{}", serde_json::to_string_pretty(&value)?);
        }
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
    json: Option<String>,
    ts: Option<String>,
    ts_file: Option<String>,
) -> Result<Value> {
    let provided = [
        file.is_some(),
        json.is_some(),
        ts.is_some(),
        ts_file.is_some(),
    ]
    .into_iter()
    .filter(|present| *present)
    .count();
    if provided == 0 {
        return Err(eyre!(
            "Provide a query with --file <path>, --json '<json>', -e '<ts>', or --ts-file <path>"
        ));
    }
    if provided > 1 {
        return Err(eyre!(
            "--file, --json, -e/--ts, and --ts-file are mutually exclusive"
        ));
    }

    if let Some(file) = file {
        let request_text = std::fs::read_to_string(&file)
            .map_err(|e| eyre!("Failed to read query request file '{file}': {e}"))?;
        return serde_json::from_str(&request_text)
            .map_err(|e| eyre!("Failed to parse query request file '{file}': {e}"));
    }
    if let Some(json) = json {
        return serde_json::from_str(&json)
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
            .contains("--file <path>, --json"));
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
    fn connect_error_points_at_local_recovery() {
        let error = connect_error("dev", "http://localhost:8080/v2/query", "refused");
        assert!(error.hint.unwrap().contains("helix start dev"));
    }
}
