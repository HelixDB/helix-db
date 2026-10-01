use crate::commands::auth::require_auth;
use crate::output;
use crate::CloudApiAction;
use eyre::{eyre, Result};
use serde_json::Value;

/// Call any `/v1/...` WFE path with the active session. The response prints as
/// highlighted JSON, or compact JSON under `--json`.
pub async fn run(action: CloudApiAction) -> Result<()> {
    let client = require_auth().await?;
    let value = match action {
        CloudApiAction::Get { path } => {
            client
                .get(validate_api_path(&path)?, "call Cloud API")
                .await?
        }
        CloudApiAction::Post { path, body } => {
            client
                .post(
                    validate_api_path(&path)?,
                    parse_body(&body)?,
                    "call Cloud API",
                )
                .await?
        }
        CloudApiAction::Patch { path, body } => {
            client
                .patch(
                    validate_api_path(&path)?,
                    parse_body(&body)?,
                    "call Cloud API",
                )
                .await?
        }
        CloudApiAction::Delete { path } => {
            client
                .delete(validate_api_path(&path)?, "call Cloud API")
                .await?
        }
    };
    output::emit(&value, |value| {
        println!("{}", output::json::pretty(value, console::colors_enabled()));
        Ok(())
    })
}

fn validate_api_path(path: &str) -> Result<&str> {
    if !path.starts_with("/v1/")
        || path.contains("://")
        || path.contains('\n')
        || path.contains('\r')
    {
        return Err(eyre!("Cloud API path must be an absolute /v1/... path"));
    }
    Ok(path)
}

fn parse_body(body: &str) -> Result<Value> {
    serde_json::from_str(body).map_err(|error| eyre!("invalid JSON body: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generic_api_rejects_absolute_urls_and_non_v1_paths() {
        assert!(validate_api_path("https://gateway.example/v1/query").is_err());
        assert!(validate_api_path("/v2/query").is_err());
        assert_eq!(
            validate_api_path("/v1/workspaces").unwrap(),
            "/v1/workspaces"
        );
    }
}
