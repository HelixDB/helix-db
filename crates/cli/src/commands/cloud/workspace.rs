use crate::cloud::model::{status_label, Named, Workspace};
use crate::cloud::resolve::Scope;
use crate::output::{self, table};
use crate::WorkspaceAction;
use eyre::Result;

pub async fn run(action: Option<WorkspaceAction>) -> Result<()> {
    let scope = Scope::load().await?;
    match action.unwrap_or(WorkspaceAction::List) {
        WorkspaceAction::List => {
            let workspaces = scope.workspaces().await?;
            let linked = scope
                .link()
                .project
                .as_ref()
                .and_then(|link| link.workspace_id.as_deref());
            output::emit(&workspaces, |workspaces| {
                if workspaces.is_empty() {
                    output::info("You are not a member of any workspace");
                    return Ok(());
                }
                let name = format!("{}NAME", super::linked_prefix(linked.is_some(), false));
                let mut rows = table::Table::new([name.as_str(), "SLUG", "REGION", "ID"]);
                for workspace in workspaces {
                    rows.row([
                        format!(
                            "{}{}",
                            super::linked_prefix(
                                linked.is_some(),
                                linked == Some(workspace.id.as_str())
                            ),
                            workspace.label()
                        ),
                        workspace.slug.clone().unwrap_or_default(),
                        workspace.region.clone().unwrap_or_default(),
                        workspace.id.clone(),
                    ]);
                }
                rows.print();
                Ok(())
            })
        }
        WorkspaceAction::Get { workspace } => {
            let workspace = scope.workspace(workspace.as_deref()).await?;
            output::emit(&workspace, |workspace| {
                print!("{}", details(workspace));
                Ok(())
            })
        }
    }
}

fn details(workspace: &Workspace) -> String {
    table::key_values(&[
        ("Name", workspace.label().to_owned()),
        ("Slug", workspace.slug.clone().unwrap_or_default()),
        ("ID", workspace.id.clone()),
        ("Region", workspace.region.clone().unwrap_or_default()),
        ("Status", status_label(workspace.status.as_deref())),
    ])
}
