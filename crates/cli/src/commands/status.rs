use crate::cloud::model::{status_label, Cluster, Named as _, Tenant};
use crate::cloud::CloudClient;
use crate::commands::auth::require_auth;
use crate::config::{DatabaseReference, InstanceInfo};
use crate::errors::CliError;
use crate::local_runtime::LocalRuntime;
use crate::output::{self, table};
use crate::project::ProjectContext;
use console::style;
use eyre::Result;
use serde::Serialize;
use serde_json::Value;
use std::path::Path;

#[derive(Serialize)]
struct Report<'a> {
    project: &'a str,
    root: &'a Path,
    instances: Vec<InstanceStatus>,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
enum InstanceStatus {
    Local {
        name: String,
        state: String,
        url: String,
        storage: String,
        /// URL of the instance's running Explorer (`helix explorer`).
        #[serde(skip_serializing_if = "Option::is_none")]
        explorer: Option<String>,
    },
    Cloud {
        name: String,
        state: String,
        database: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        label: Option<String>,
        /// Why the state could not be read; one unreachable Cloud database
        /// never hides the status of the others.
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
}

/// Every instance's state (or just `instance`'s): local containers from the
/// runtime, Cloud databases from the API.
pub async fn run(instance: Option<String>) -> Result<()> {
    let project = ProjectContext::find_and_load(None)?;
    let names = match instance {
        Some(name) => {
            project.config.get_instance(&name)?;
            vec![name]
        }
        None => project
            .config
            .list_instances()
            .into_iter()
            .cloned()
            .collect(),
    };
    let runtime = LocalRuntime::new(&project);
    // Reading the local session costs nothing, so load it unconditionally;
    // it is only consulted for Cloud instances.
    let cloud = require_auth()
        .await
        .map_err(|error| CliError::from_report(&error).message);

    let mut instances = Vec::with_capacity(names.len());
    for name in names {
        instances.push(match project.config.get_instance(&name)? {
            InstanceInfo::Local(config) => {
                let status = runtime.status(&name)?;
                // An Explorer only runs beside an existing instance container,
                // so a missing one costs no extra runtime call.
                let explorer = status
                    .as_ref()
                    .and_then(|_| runtime.explorer_port(&name))
                    .map(super::explorer::explorer_url);
                InstanceStatus::Local {
                    state: status.map_or_else(|| "not created".to_owned(), |status| status.status),
                    url: format!("http://localhost:{}", config.port),
                    storage: config.storage.as_str().to_owned(),
                    explorer,
                    name,
                }
            }
            InstanceInfo::Enterprise(config) => match &cloud {
                Ok(client) => cloud_status(client, name, &config.database).await,
                Err(message) => InstanceStatus::Cloud {
                    name,
                    state: "unknown".to_owned(),
                    database: config.database.to_string(),
                    label: None,
                    error: Some(message.clone()),
                },
            },
        });
    }

    let report = Report {
        project: &project.config.project.name,
        root: &project.root,
        instances,
    };
    output::emit(&report, |report| {
        print!(
            "{}",
            table::key_values(&[
                ("Project", report.project.to_owned()),
                ("Root", report.root.display().to_string()),
            ])
        );
        println!();
        let mut rows = table::Table::new(["INSTANCE", "KIND", "STATUS", "ENDPOINT"]);
        for instance in &report.instances {
            match instance {
                InstanceStatus::Local {
                    name,
                    state,
                    url,
                    storage,
                    explorer,
                } => {
                    rows.row([
                        name.clone(),
                        "local".to_owned(),
                        table::state(state),
                        format!("{url} {}", style(format!("({storage})")).dim()),
                    ]);
                    let Some(explorer) = explorer else {
                        continue;
                    };
                    rows.row([
                        name.clone(),
                        "explorer".to_owned(),
                        table::state("running"),
                        explorer.clone(),
                    ]);
                }
                InstanceStatus::Cloud {
                    name,
                    state,
                    database,
                    label,
                    ..
                } => rows.row([
                    name.clone(),
                    "cloud".to_owned(),
                    table::state(state),
                    match label {
                        Some(label) => format!("{label} {}", style(format!("({database})")).dim()),
                        None => database.clone(),
                    },
                ]),
            }
        }
        rows.print();
        for instance in &report.instances {
            let InstanceStatus::Cloud {
                name,
                error: Some(error),
                ..
            } = instance
            else {
                continue;
            };
            output::warning(&format!("{name}: {error}"));
        }
        Ok(())
    })
}

async fn cloud_status(
    client: &CloudClient,
    name: String,
    database: &DatabaseReference,
) -> InstanceStatus {
    let state = match database {
        DatabaseReference::Tenant(id) => client
            .fetch::<Tenant>(&format!("/v1/tenants/{id}"), "get Cloud tenant status")
            .await
            .map(|tenant| {
                (
                    tenant.label().to_owned(),
                    status_label(tenant.status.as_deref()),
                )
            }),
        DatabaseReference::Cluster(id) => {
            let cluster = client
                .fetch::<Cluster>(&format!("/v1/clusters/{id}"), "get Cloud cluster status")
                .await;
            match cluster {
                Ok(cluster) => client
                    .get(
                        &format!("/v1/clusters/{id}/topology"),
                        "get Cloud cluster topology",
                    )
                    .await
                    .map(|topology| {
                        let state = ["phase", "status", "state"]
                            .into_iter()
                            .find_map(|field| topology.get(field).and_then(Value::as_str))
                            .or(cluster.status.as_deref());
                        (cluster.label().to_owned(), status_label(state))
                    }),
                Err(error) => Err(error),
            }
        }
    };
    match state {
        Ok((label, state)) => InstanceStatus::Cloud {
            name,
            state,
            database: database.to_string(),
            label: Some(label),
            error: None,
        },
        Err(error) => InstanceStatus::Cloud {
            name,
            state: "unreachable".to_owned(),
            database: database.to_string(),
            label: None,
            error: Some(CliError::from_report(&error).to_string()),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statuses_serialize_with_their_kind() {
        let local = serde_json::to_value(InstanceStatus::Local {
            name: "dev".into(),
            state: "not created".into(),
            url: "http://localhost:6969".into(),
            storage: "memory".into(),
            explorer: None,
        })
        .unwrap();
        assert_eq!(local["kind"], "local");
        let cloud = serde_json::to_value(InstanceStatus::Cloud {
            name: "prod".into(),
            state: "unreachable".into(),
            database: "tenant:t".into(),
            label: None,
            error: Some("HTTP 503".into()),
        })
        .unwrap();
        assert_eq!(cloud["kind"], "cloud");
        assert_eq!(cloud["error"], "HTTP 503");
        assert!(cloud.get("label").is_none());
    }
}
