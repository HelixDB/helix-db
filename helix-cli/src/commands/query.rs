use crate::config::InstanceInfo;
use crate::errors::CliError;
use crate::project::ProjectContext;
use eyre::{Report, Result, eyre};
use reqwest::header::{CONTENT_TYPE, HeaderName, HeaderValue};
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
    dry_run: bool,
) -> Result<()> {
    let instance = instance.unwrap_or_else(|| "dev".to_string());
    let request_json = parse_query_request(file, json, ts, ts_file)?;

    validate_dynamic_request(&request_json, warm)?;
    if dry_run {
        if crate::output::Verbosity::current().show_normal() {
            println!("{}", format_query_request(&request_json, compact)?);
        }
        return Ok(());
    }

    let project = ProjectContext::find_and_load(None)?;
    // Load a project-root .env so Enterprise query auth can come from a file
    // instead of requiring the caller to export it in their shell.
    let _ = dotenvy::from_path(project.root.join(".env"));
    let client = reqwest::Client::new();
    let (mut request, endpoint, is_local) = match project.config.get_instance(&instance)? {
        InstanceInfo::Local(config) => {
            let host = host.unwrap_or_else(|| "localhost".to_string());
            let port = port.unwrap_or(config.port);
            let endpoint = format!("http://{host}:{port}/v1/query");
            (client.post(&endpoint), endpoint, true)
        }
        InstanceInfo::Enterprise(config) => {
            let gateway_url = config.gateway_url.as_deref().ok_or_else(|| {
                eyre!(
                    "Enterprise gateway URL is not configured for '{instance}'. Run 'helix sync {instance}' or set gateway_url in helix.toml."
                )
            })?;
            let auth_value = std::env::var(&config.query_auth_env).map_err(|_| -> Report {
                CliError::new(format!(
                    "environment variable {} is required for Enterprise query auth",
                    config.query_auth_env
                ))
                .with_hint(format!(
                    "set {} in a .env file in your project root, or export it in your shell",
                    config.query_auth_env
                ))
                .into()
            })?;
            let header_name = HeaderName::from_bytes(config.query_auth_header.as_bytes())?;
            let endpoint = format!("{}/v1/query", gateway_url.trim_end_matches('/'));
            (
                client
                    .post(&endpoint)
                    .header(header_name, HeaderValue::from_str(&auth_value)?),
                endpoint,
                false,
            )
        }
    };

    request = request.header(CONTENT_TYPE, "application/json");
    if warm {
        request = request.header("X-Helix-Warm", "true");
    }

    let response = request
        .json(&request_json)
        .send()
        .await
        .map_err(|e| -> Report {
            if e.is_connect() || e.is_timeout() {
                connect_error(&instance, &endpoint, is_local, &e.to_string()).into()
            } else {
                e.into()
            }
        })?;
    let status = response.status();
    if status == reqwest::StatusCode::NO_CONTENT {
        return Ok(());
    }
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(eyre!("Query failed with HTTP {status}: {body}"));
    }

    if body.trim().is_empty() {
        return Ok(());
    }
    let value: Value = serde_json::from_str(&body).unwrap_or(Value::String(body));
    if crate::output::Verbosity::current().show_normal() {
        if compact {
            println!("{}", serde_json::to_string(&value)?);
        } else {
            println!("{}", serde_json::to_string_pretty(&value)?);
        }
    }
    Ok(())
}

/// Error for a query that never reached a Helix instance (connection refused,
/// DNS failure, timeout). The raw reqwest error doesn't tell an agent or user
/// what to do next, so spell out the recovery path for each instance kind.
fn connect_error(instance: &str, endpoint: &str, is_local: bool, cause: &str) -> CliError {
    let hint = if is_local {
        format!(
            "No Helix instance is listening there. Start it with `helix start {instance}` and \
             check it with `helix status {instance}`. If it runs on another host/port, pass \
             --host/--port."
        )
    } else {
        format!(
            "Check the gateway_url for '{instance}' in helix.toml and your network connection. \
             `helix sync {instance}` refreshes the gateway metadata from Helix Cloud."
        )
    };
    CliError::new(format!(
        "cannot reach Helix instance '{instance}' at {endpoint}"
    ))
    .with_context(cause.to_string())
    .with_hint(hint)
}

fn format_query_request(request: &Value, compact: bool) -> Result<String> {
    if compact {
        Ok(serde_json::to_string(request)?)
    } else {
        Ok(serde_json::to_string_pretty(request)?)
    }
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

fn validate_dynamic_request(request: &Value, warm: bool) -> Result<()> {
    let request_type = request
        .get("request_type")
        .and_then(Value::as_str)
        .ok_or_else(|| eyre!("dynamic query request must include request_type"))?;
    if request_type != "read" && request_type != "write" {
        return Err(eyre!("request_type must be lowercase 'read' or 'write'"));
    }
    if warm && request_type != "read" {
        return Err(eyre!("--warm is only valid for read requests"));
    }
    if request.get("query").is_none() {
        return Err(eyre!("dynamic query request must include query"));
    }
    Ok(())
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
    fn parse_query_request_rejects_missing_input() {
        let error = parse_query_request(None, None, None, None)
            .unwrap_err()
            .to_string();

        assert!(error.contains("--file <path>, --json"));
    }

    #[test]
    fn parse_query_request_rejects_both_inputs() {
        let error = parse_query_request(
            Some("request.json".to_string()),
            Some("{}".to_string()),
            None,
            None,
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("mutually exclusive"));
    }

    #[test]
    fn connect_error_local_points_at_helix_start() {
        let err = connect_error("dev", "http://localhost:8080/v1/query", true, "refused");

        assert!(err.message.contains("'dev'"));
        assert!(err.message.contains("http://localhost:8080/v1/query"));
        let hint = err.hint.expect("hint should be set");
        assert!(hint.contains("helix start dev"));
    }

    #[test]
    fn connect_error_enterprise_points_at_gateway_config() {
        let err = connect_error("prod", "https://gw.example.com/v1/query", false, "timeout");

        let hint = err.hint.expect("hint should be set");
        assert!(hint.contains("gateway_url"));
        assert!(hint.contains("helix sync prod"));
    }

    #[test]
    fn parse_query_request_rejects_json_and_ts_together() {
        let error = parse_query_request(
            None,
            Some("{}".to_string()),
            Some("readBatch()".to_string()),
            None,
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("mutually exclusive"));
    }

    #[test]
    fn format_query_request_pretty_prints_dry_run_json() {
        let request: Value =
            serde_json::from_str(r#"{"request_type":"read","query":{"queries":[]}}"#).unwrap();

        let output = format_query_request(&request, false).unwrap();

        assert!(output.contains('\n'));
        assert!(output.contains("\"request_type\": \"read\""));
        assert!(output.contains("\n  \"query\": {"));
    }

    #[test]
    fn format_query_request_compacts_dry_run_json() {
        let request: Value =
            serde_json::from_str(r#"{"request_type":"read","query":{"queries":[]}}"#).unwrap();

        let output = format_query_request(&request, true).unwrap();

        assert_eq!(output, r#"{"query":{"queries":[]},"request_type":"read"}"#);
    }
}
