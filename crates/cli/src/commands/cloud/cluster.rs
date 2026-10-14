use crate::cloud::model::{status_label, Cluster, Named};
use crate::cloud::resolve::Scope;
use crate::output::{self, table};
use crate::{ClusterAction, ScopeArgs};
use eyre::Result;

pub async fn run(action: Option<ClusterAction>) -> Result<()> {
    let scope = Scope::load().await?;
    match action.unwrap_or(ClusterAction::List {
        scope: ScopeArgs::default(),
    }) {
        ClusterAction::List { scope: args } => {
            // A project (explicit or linked) narrows the list; otherwise list
            // the whole workspace.
            let in_project = args.project.is_some()
                || (args.workspace.is_none() && scope.link().project.is_some());
            let clusters = if in_project {
                let project = scope.project(&args).await?;
                scope.clusters_in_project(&project).await?
            } else {
                let workspace = scope.workspace(args.workspace.as_deref()).await?;
                scope.clusters_in_workspace(&workspace).await?
            };
            output::emit(&clusters, |clusters| {
                if clusters.is_empty() {
                    output::info("No clusters found");
                    return Ok(());
                }
                let mut rows = table::Table::new(["NAME", "ACCESS", "STATUS", "ID"]);
                for cluster in clusters {
                    rows.row([
                        cluster.label().to_owned(),
                        cluster.access().label().to_owned(),
                        table::state(&status_label(cluster.status.as_deref())),
                        cluster.id.clone(),
                    ]);
                }
                rows.print();
                Ok(())
            })
        }
        ClusterAction::Get {
            cluster,
            scope: args,
        } => {
            let cluster = scope.cluster(cluster.as_deref(), &args).await?;
            output::emit(&cluster, |cluster| {
                print!("{}", details(cluster));
                Ok(())
            })
        }
        ClusterAction::Indexes {
            cluster,
            scope: args,
        } => {
            let cluster = scope.cluster(cluster.as_deref(), &args).await?;
            let indexes = scope
                .client()
                .get(
                    &format!("/v1/clusters/{}/indexes", cluster.id),
                    "list cluster indexes",
                )
                .await?;
            output::emit(&indexes, |indexes| {
                println!(
                    "{}",
                    output::json::pretty(indexes, console::colors_enabled())
                );
                Ok(())
            })
        }
    }
}

fn details(cluster: &Cluster) -> String {
    table::key_values(&[
        ("Name", cluster.label().to_owned()),
        ("Slug", cluster.slug.clone().unwrap_or_default()),
        ("ID", cluster.id.clone()),
        ("Access", cluster.access().label().to_owned()),
        ("Status", status_label(cluster.status.as_deref())),
        ("Project", cluster.project_id.clone().unwrap_or_default()),
        (
            "Workspace",
            cluster.workspace_id.clone().unwrap_or_default(),
        ),
    ])
}
