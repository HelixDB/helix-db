use crate::cloud::model::{Named, ServiceCredential, Workspace};
use crate::cloud::resolve::{Kind, Scope};
use crate::errors::CliError;
use crate::output::{self, table};
use crate::ServiceCredentialAction;
use eyre::{eyre, Result};
use serde_json::{json, Map, Value};
use std::collections::HashSet;

pub async fn run(action: Option<ServiceCredentialAction>) -> Result<()> {
    let scope = Scope::load().await?;
    match action.unwrap_or(ServiceCredentialAction::List { workspace: None }) {
        ServiceCredentialAction::Create {
            name,
            grants,
            expires_at,
            workspace,
        } => {
            let grants = parse_grants(&grants)?;
            let workspace = scope.workspace(workspace.as_deref()).await?;
            let response = scope
                .client()
                .post(
                    &collection(&workspace),
                    json!({
                        "workspaceId": workspace.id,
                        "name": name,
                        "grants": grants,
                        "expiresAt": expires_at,
                    }),
                    "create service credential",
                )
                .await?;
            let token = super::one_time_token(&response, "credential")?;
            output::success(&format!(
                "Created service credential {name} in {}",
                workspace.label()
            ));
            output::emit(&response, |_| {
                output::warning("This token is shown once. The CLI did not store it.");
                println!("{token}");
                Ok(())
            })
        }
        ServiceCredentialAction::List { workspace } => {
            let workspace = scope.workspace(workspace.as_deref()).await?;
            let credentials = list(&scope, &workspace).await?;
            output::emit(&credentials, |credentials| {
                if credentials.is_empty() {
                    output::info(&format!("{} has no service credentials", workspace.label()));
                    return Ok(());
                }
                let mut rows = table::Table::new(["NAME", "GRANTS", "EXPIRES", "ID"]);
                for credential in credentials {
                    rows.row([
                        credential.label().to_owned(),
                        match credential.grants.as_deref().map_or(0, <[Value]>::len) {
                            1 => "1 project".to_owned(),
                            count => format!("{count} projects"),
                        },
                        credential
                            .expires_at
                            .clone()
                            .unwrap_or_else(|| "never".to_owned()),
                        credential.id.clone(),
                    ]);
                }
                rows.print();
                Ok(())
            })
        }
        ServiceCredentialAction::Get {
            credential,
            workspace,
        } => {
            let workspace = scope.workspace(workspace.as_deref()).await?;
            let credential = scope.pick(
                Kind::ServiceCredential,
                &credential,
                list(&scope, &workspace).await?,
            )?;
            output::emit(&credential, |credential| {
                print!(
                    "{}",
                    table::key_values(&[
                        ("Name", credential.label().to_owned()),
                        ("ID", credential.id.clone()),
                        ("Workspace", workspace.label().to_owned()),
                        (
                            "Expires",
                            credential
                                .expires_at
                                .clone()
                                .unwrap_or_else(|| "never".to_owned()),
                        ),
                    ])
                );
                let Some(grants) = credential
                    .grants
                    .clone()
                    .filter(|grants| !grants.is_empty())
                else {
                    return Ok(());
                };
                println!(
                    "{}",
                    output::json::pretty(&Value::Array(grants), console::colors_enabled())
                );
                Ok(())
            })
        }
        ServiceCredentialAction::Update {
            credential,
            name,
            grants,
            expires_at,
            clear_expiry,
            workspace,
        } => {
            let replace_grants = !grants.is_empty();
            let grants = parse_grants(&grants)?;
            if name.is_none() && !replace_grants && expires_at.is_none() && !clear_expiry {
                return Err(CliError::new("nothing to update")
                    .with_hint("pass --name, --grant, --expires-at, or --clear-expiry")
                    .into());
            }
            let workspace = scope.workspace(workspace.as_deref()).await?;
            let credential = scope.pick(
                Kind::ServiceCredential,
                &credential,
                list(&scope, &workspace).await?,
            )?;
            let mut body = Map::new();
            body.insert("workspaceId".into(), Value::String(workspace.id.clone()));
            body.insert("id".into(), Value::String(credential.id.clone()));
            body.insert("replaceGrants".into(), Value::Bool(replace_grants));
            if replace_grants {
                body.insert("grants".into(), Value::Array(grants));
            }
            body.extend(name.map(|name| ("name".to_owned(), Value::String(name))));
            if expires_at.is_some() || clear_expiry {
                body.insert("replaceExpiry".into(), Value::Bool(true));
                body.extend(
                    expires_at
                        .map(|expires_at| ("expiresAt".to_owned(), Value::String(expires_at))),
                );
            }
            let response = scope
                .client()
                .patch(
                    &format!("{}/{}", collection(&workspace), credential.id),
                    Value::Object(body),
                    "update service credential",
                )
                .await?;
            output::success(&format!(
                "Updated service credential {}; its secret was not rotated",
                credential.label()
            ));
            output::emit(&response, |_| Ok(()))
        }
        ServiceCredentialAction::Revoke {
            credential,
            yes,
            workspace,
        } => {
            super::ensure_confirmable(yes)?;
            let workspace = scope.workspace(workspace.as_deref()).await?;
            let credential = scope.pick(
                Kind::ServiceCredential,
                &credential,
                list(&scope, &workspace).await?,
            )?;
            let question = format!(
                "Revoke service credential {} ({})? Automation using it stops working immediately.",
                credential.label(),
                credential.id
            );
            if !super::confirm(yes, &question)? {
                output::info("Cancelled");
                return Ok(());
            }
            scope
                .client()
                .delete(
                    &format!("{}/{}", collection(&workspace), credential.id),
                    "revoke service credential",
                )
                .await?;
            output::success(&format!(
                "Revoked service credential {}",
                credential.label()
            ));
            output::emit(
                &json!({"revoked": {"kind": "service-credential", "id": credential.id}}),
                |_| Ok(()),
            )
        }
    }
}

fn collection(workspace: &Workspace) -> String {
    format!("/v1/workspaces/{}/service-credentials", workspace.id)
}

async fn list(scope: &Scope, workspace: &Workspace) -> Result<Vec<ServiceCredential>> {
    scope
        .client()
        .list(
            &collection(workspace),
            &[],
            "credentials",
            "list service credentials",
        )
        .await
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum ServiceCredentialPermission {
    ProjectRead,
    ProjectWrite,
    QueryRead,
    QueryWrite,
}

impl ServiceCredentialPermission {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "project-read" => Ok(Self::ProjectRead),
            "project-write" => Ok(Self::ProjectWrite),
            "query-read" => Ok(Self::QueryRead),
            "query-write" => Ok(Self::QueryWrite),
            permission => Err(eyre!(
                "unknown service-credential permission '{permission}'"
            )),
        }
    }

    fn api_name(self) -> &'static str {
        match self {
            Self::ProjectRead => "SERVICE_CREDENTIAL_PERMISSION_PROJECT_READ",
            Self::ProjectWrite => "SERVICE_CREDENTIAL_PERMISSION_PROJECT_WRITE",
            Self::QueryRead => "SERVICE_CREDENTIAL_PERMISSION_DATABASE_QUERY_READ",
            Self::QueryWrite => "SERVICE_CREDENTIAL_PERMISSION_DATABASE_QUERY_WRITE",
        }
    }
}

fn parse_grants(grants: &[String]) -> Result<Vec<Value>> {
    let mut seen_projects = HashSet::with_capacity(grants.len());
    grants
        .iter()
        .map(|grant| {
            let (project, permissions) = grant.split_once('=').ok_or_else(|| {
                eyre!("grant must be PROJECT_ID=project-read,project-write,query-read,query-write")
            })?;
            let project = project.trim();
            if project.is_empty() {
                return Err(eyre!("grant project ID cannot be empty"));
            }
            if !seen_projects.insert(project) {
                return Err(eyre!("duplicate project grant '{project}'"));
            }
            if permissions.trim().is_empty() {
                return Err(eyre!("grant permissions cannot be empty"));
            }
            let permissions = permissions
                .split(',')
                .map(str::trim)
                .map(ServiceCredentialPermission::parse)
                .collect::<Result<Vec<_>>>()?;
            let unique_permissions = permissions.iter().copied().collect::<HashSet<_>>();
            if unique_permissions.len() != permissions.len() {
                return Err(eyre!("grant permissions cannot contain duplicates"));
            }
            if unique_permissions.contains(&ServiceCredentialPermission::ProjectWrite)
                && !unique_permissions.contains(&ServiceCredentialPermission::ProjectRead)
            {
                return Err(eyre!("project-write requires project-read"));
            }
            if unique_permissions.contains(&ServiceCredentialPermission::QueryWrite)
                && !unique_permissions.contains(&ServiceCredentialPermission::QueryRead)
            {
                return Err(eyre!("query-write requires query-read"));
            }
            Ok(json!({
                "projectId": project,
                "permissions": permissions
                    .into_iter()
                    .map(ServiceCredentialPermission::api_name)
                    .collect::<Vec<_>>(),
            }))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grants_are_project_scoped_and_map_every_permission() {
        let grants =
            parse_grants(&[" project-1 =project-read,project-write,query-read,query-write".into()])
                .unwrap();
        assert_eq!(grants[0]["projectId"], "project-1");
        assert_eq!(
            grants[0]["permissions"],
            json!([
                "SERVICE_CREDENTIAL_PERMISSION_PROJECT_READ",
                "SERVICE_CREDENTIAL_PERMISSION_PROJECT_WRITE",
                "SERVICE_CREDENTIAL_PERMISSION_DATABASE_QUERY_READ",
                "SERVICE_CREDENTIAL_PERMISSION_DATABASE_QUERY_WRITE",
            ])
        );
    }

    #[test]
    fn grants_require_matching_read_permissions() {
        assert!(parse_grants(&["project-1=project-write".into()]).is_err());
        assert!(parse_grants(&["project-1=query-write".into()]).is_err());
        assert!(
            parse_grants(&["project-1=project-read,project-write,query-write".into()]).is_err()
        );
    }

    #[test]
    fn grants_reject_duplicate_and_malformed_values() {
        assert!(parse_grants(&[]).unwrap().is_empty());
        assert!(parse_grants(&[
            "project-1=query-read".into(),
            "project-1=project-read".into(),
        ])
        .is_err());
        assert!(parse_grants(&["project-1=query-read,query-read".into()]).is_err());
        assert!(parse_grants(&["project-1=".into()]).is_err());
        assert!(parse_grants(&["=query-read".into()]).is_err());
        assert!(parse_grants(&["project-1".into()]).is_err());
        assert!(parse_grants(&["project-1=service-credentials-manage".into()]).is_err());
    }
}
