use crate::config::InstanceInfo;
use crate::local_runtime::Runtime;
use crate::project::ProjectContext;
use crate::prompts::{self, StatusSelection};
use crate::utils::{print_field, print_header, print_newline};
use eyre::Result;

pub async fn run(instance: Option<String>) -> Result<()> {
    let project = ProjectContext::find_and_load(None)?;

    print_header("Helix Project Status");
    print_field("Project", &project.config.project.name);
    print_field("Root", &project.root.display().to_string());
    print_newline();

    let runtime = Runtime::for_project(&project);
    print_header("Instances");
    match resolve_status_selection(&project, instance)? {
        StatusSelection::All => {
            for name in project.config.list_instances() {
                print_instance(&project, &runtime, name)?;
            }
        }
        StatusSelection::Instance(instance) => {
            print_instance(&project, &runtime, &instance)?
        }
    }

    Ok(())
}

fn resolve_status_selection(
    project: &ProjectContext,
    instance: Option<String>,
) -> Result<StatusSelection> {
    if let Some(instance) = instance {
        return Ok(StatusSelection::Instance(instance));
    }
    let instances = all_instances(project);
    if prompts::is_interactive() && instances.len() > 1 {
        return prompts::select_status(&instances);
    }
    Ok(StatusSelection::All)
}

fn print_instance(project: &ProjectContext, runtime: &Runtime, name: &str) -> Result<()> {
    match project.config.get_instance(name)? {
        InstanceInfo::Local(config) => {
            let status = runtime.status(name)?;
            let state = status
                .as_ref()
                .map(|status| status.status.as_str())
                .unwrap_or("not created");
            print_field(
                &format!("{name} (local)"),
                &format!(
                    "http://localhost:{} - {state} - storage: {}",
                    config.port,
                    config.storage.as_str()
                ),
            );
        }
        InstanceInfo::Enterprise(config) => {
            let gateway = config
                .gateway_url
                .as_deref()
                .unwrap_or("gateway not configured");
            print_field(
                &format!("{name} (Enterprise)"),
                &format!("cluster {} - {gateway}", config.cluster_id),
            );
        }
    }
    Ok(())
}

fn all_instances(project: &ProjectContext) -> Vec<(String, String)> {
    project
        .config
        .list_instances_with_types()
        .into_iter()
        .map(|(name, kind)| (name.clone(), kind.to_string()))
        .collect()
}
