use crate::cloud::model::{Named, Project};
use crate::cloud::resolve::{Kind, ResolvedProject, Scope};
use crate::output::{self, table};
use crate::project::ProjectContext;
use crate::{ProjectAction, ScopeArgs};
use eyre::Result;
use serde_json::json;

pub async fn run(action: Option<ProjectAction>) -> Result<()> {
    let scope = Scope::load().await?;
    match action.unwrap_or(ProjectAction::List { workspace: None }) {
        ProjectAction::List { workspace } => {
            let workspace = scope.workspace(workspace.as_deref()).await?;
            let projects = scope.projects_in(&workspace).await?;
            let linked = scope
                .link()
                .project
                .as_ref()
                .map(|link| link.project_id.as_str());
            output::emit(&projects, |projects| {
                if projects.is_empty() {
                    output::info(&format!("No projects in {}", workspace.label()));
                    output::remark("Create one with `helix project create <name>`");
                    return Ok(());
                }
                let name = format!("{}NAME", super::linked_prefix(linked.is_some(), false));
                let mut rows = table::Table::new([name.as_str(), "SLUG", "ID"]);
                for project in projects {
                    rows.row([
                        format!(
                            "{}{}",
                            super::linked_prefix(
                                linked.is_some(),
                                linked == Some(project.id.as_str())
                            ),
                            project.label()
                        ),
                        project.slug.clone().unwrap_or_default(),
                        project.id.clone(),
                    ]);
                }
                rows.print();
                Ok(())
            })
        }
        ProjectAction::Get { project, workspace } => {
            let resolved = scope.project(&ScopeArgs { workspace, project }).await?;
            output::emit(&resolved.project, |_| {
                print!("{}", details(&resolved));
                Ok(())
            })
        }
        ProjectAction::Create {
            name,
            slug,
            workspace,
            link,
        } => {
            // --link needs a helix.toml; check before creating anything.
            if link {
                ProjectContext::find_and_load(None)?;
            }
            let workspace = scope.workspace(workspace.as_deref()).await?;
            let slug = match slug {
                Some(slug) => slug,
                None => super::slugify(&name)?,
            };
            let project: Project = serde_json::from_value(
                scope
                    .client()
                    .post(
                        "/v1/projects",
                        json!({"workspaceId": workspace.id, "slug": slug, "displayName": name}),
                        "create project",
                    )
                    .await?,
            )?;
            output::success(&format!(
                "Created project {} in {}",
                project.label(),
                workspace.label()
            ));
            let resolved = ResolvedProject {
                project,
                workspace_id: workspace.id.clone(),
            };
            if link {
                let config = write_link(&resolved)?;
                output::success(&format!("Linked {}", config.display()));
            }
            output::emit(&resolved.project, |_| {
                print!("{}", details(&resolved));
                Ok(())
            })
        }
        ProjectAction::Delete {
            project,
            workspace,
            yes,
        } => {
            super::ensure_confirmable(yes)?;
            let scope = scope.removing(Kind::Project);
            let project = scope
                .project(&ScopeArgs { workspace, project })
                .await?
                .project;
            let question = format!(
                "Delete project {} ({})? This cannot be undone.",
                project.label(),
                project.id
            );
            if !super::confirm(yes, &question)? {
                output::info("Cancelled");
                return Ok(());
            }
            scope
                .client()
                .delete(&format!("/v1/projects/{}", project.id), "delete project")
                .await?;
            output::success(&format!("Deleted project {}", project.label()));
            output::emit(
                &json!({"deleted": {"kind": "project", "id": project.id}}),
                |_| Ok(()),
            )
        }
        ProjectAction::Link { project, workspace } => {
            // Fail before any prompt when there is nothing to link.
            ProjectContext::find_and_load(None)?;
            // Ignore the current link so a linked directory can be relinked.
            let project = scope
                .unlinked()
                .project(&ScopeArgs { workspace, project })
                .await?;
            let config = write_link(&project)?;
            output::success(&format!(
                "Linked {} to project {}",
                config.display(),
                project.label()
            ));
            output::emit(
                &json!({"project": project.project, "config": config}),
                |_| Ok(()),
            )
        }
    }
}

/// Point this directory's helix.toml at `project`, returning its path. The
/// local project name is left alone: it names local containers.
fn write_link(project: &ResolvedProject) -> Result<std::path::PathBuf> {
    let mut context = ProjectContext::find_and_load(None)?;
    context.config.project.id = Some(project.project.id.clone());
    context.config.project.workspace_id = Some(project.workspace_id.clone());
    let path = context.root.join("helix.toml");
    context.config.save_to_file(&path)?;
    Ok(path)
}

fn details(resolved: &ResolvedProject) -> String {
    let project = &resolved.project;
    table::key_values(&[
        ("Name", project.label().to_owned()),
        ("Slug", project.slug.clone().unwrap_or_default()),
        ("ID", project.id.clone()),
        ("Workspace", resolved.workspace_id.clone()),
    ])
}
