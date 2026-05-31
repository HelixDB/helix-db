use crate::config::InstanceInfo;
use crate::local_runtime::Runtime;
use crate::output::Operation;
use crate::project::ProjectContext;
use crate::prompts;
use eyre::{Result, eyre};

pub async fn run(instance: Option<String>) -> Result<()> {
    let project = ProjectContext::find_and_load(None)?;
    let instance = resolve_local_instance(&project, instance)?;
    if !matches!(
        project.config.get_instance(&instance)?,
        InstanceInfo::Local(_)
    ) {
        return Err(eyre!("'{instance}' is not a local v2 instance"));
    }
    let op = Operation::new("Stopping", &instance);
    if Runtime::for_project(&project).stop(&instance)? {
        op.success();
    } else {
        crate::output::info(&format!("Instance '{instance}' was not running"));
    }
    Ok(())
}

fn resolve_local_instance(project: &ProjectContext, instance: Option<String>) -> Result<String> {
    if let Some(instance) = instance {
        return Ok(instance);
    }
    if prompts::is_interactive() && project.config.local.len() > 1 {
        return prompts::select_instance(&local_instances(project), "Stop which local instance?");
    }
    if project.config.local.contains_key("dev") {
        return Ok("dev".to_string());
    }
    if project.config.local.len() == 1 {
        return Ok(project.config.local.keys().next().unwrap().clone());
    }
    Err(eyre!("No local instance specified"))
}

fn local_instances(project: &ProjectContext) -> Vec<(String, String)> {
    let mut instances: Vec<(String, String)> = project
        .config
        .local
        .iter()
        .map(|(name, config)| (name.clone(), format!("http://localhost:{}", config.port)))
        .collect();
    instances.sort_by(|a, b| a.0.cmp(&b.0));
    instances
}
