use crate::commands::enterprise_deploy::deploy_enterprise;
use crate::config::InstanceInfo;
use crate::errors::CliError;
use crate::metrics_sender::MetricsSender;
use crate::output::Operation;
use crate::project::ProjectContext;
use crate::prompts;
use eyre::{eyre, Result};
use std::time::Instant;

pub async fn run(
    instance_name: Option<String>,
    dev: bool,
    metrics_sender: &MetricsSender,
) -> Result<()> {
    let start_time = Instant::now();
    let project = ProjectContext::find_and_load(None)?;
    let instance_name = resolve_instance_name(instance_name, &project)?;

    if dev {
        crate::output::warning(
            "Ignoring --dev; Enterprise deploys always use the query project build profile.",
        );
    }

    let instance_config = project.config.get_instance(&instance_name)?;
    let InstanceInfo::Enterprise(config) = instance_config else {
        if instance_config.is_local() {
            return Err(eyre!(
                "Local instance '{instance_name}' uses the v2 runtime. Run 'helix start {instance_name}' instead."
            ));
        }
        return Err(eyre!(
            "Instance '{instance_name}' is not an Enterprise instance"
        ));
    };

    let op = Operation::new("Deploying", &instance_name);
    let deploy_result = deploy_enterprise(&project, &instance_name, config).await;
    let duration = start_time.elapsed().as_secs() as u32;
    let success = deploy_result.is_ok();
    let error_messages = deploy_result.as_ref().err().map(|error| error.to_string());

    metrics_sender.send_deploy_cloud_event(
        instance_name.clone(),
        String::new(),
        0,
        duration,
        success,
        error_messages,
    );

    match deploy_result {
        Ok(()) => {
            op.success();
            Ok(())
        }
        Err(error) => {
            op.failure();
            Err(CliError::new(format!(
                "Enterprise deploy of '{instance_name}' failed"
            ))
            .with_caused_by(error.to_string())
            .with_hint(
                "check the cluster with 'helix status', and re-authenticate with 'helix auth login' if needed",
            )
            .into())
        }
    }
}

fn resolve_instance_name(
    instance_name: Option<String>,
    project: &ProjectContext,
) -> Result<String> {
    if let Some(instance_name) = instance_name {
        return Ok(instance_name);
    }

    let enterprise_instances: Vec<(String, String)> = project
        .config
        .enterprise
        .keys()
        .map(|name| (name.clone(), "Enterprise".to_string()))
        .collect();

    if prompts::is_interactive() {
        return prompts::select_instance(
            &enterprise_instances,
            "Deploy which Enterprise instance?",
        );
    }

    let available = enterprise_instances
        .into_iter()
        .map(|(name, _)| name)
        .collect::<Vec<_>>()
        .join(", ");
    if available.is_empty() {
        Err(eyre!("No Enterprise instances found in helix.toml"))
    } else {
        Err(eyre!(
            "No Enterprise instance specified. Available Enterprise instances: {available}"
        ))
    }
}
