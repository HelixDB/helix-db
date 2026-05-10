use crate::cleanup::CleanupTracker;
use crate::output::{Operation, Step, Verbosity};
use crate::project::ProjectContext;
use crate::utils::{print_confirm, print_instructions, print_warning};
use crate::errors::project_error;
use eyre::Result;
use heed3::{CompactionOption, EnvFlags, EnvOpenOptions};
use std::fs;
use std::fs::create_dir_all;
use std::path::Path;

pub async fn run(
    source: String,
    branch_name: String,
    deploy: bool,
    port: Option<u16>,
) -> Result<()> {
    // Load project context
    let mut project = ProjectContext::find_and_load(None)?;

    // Start UI Operation
    let op = Operation::new("Branching", &source);
    
    // Validate that the target branch doesn't conflict with existing instances if we are deploying
    if deploy && project.config.get_instance(&branch_name).is_ok() {
        op.failure();
        return Err(project_error(format!("Cannot deploy branch '{branch_name}' because an instance with this name already exists in helix.toml"))
            .with_hint("Choose a different branch name, or remove the existing instance first")
            .into());
    }

    // Get source instance config
    let _instance_config = match project.config.get_instance(&source) {
        Ok(c) => c,
        Err(e) => {
            op.failure();
            return Err(e.into());
        }
    };

    // Get the instance volume
    let volumes_dir = project
        .root
        .join(".helix")
        .join(".volumes")
        .join(&source)
        .join("user");

    let data_file = volumes_dir.join("data.mdb");
    let env_path = Path::new(&volumes_dir);

    // Validate existence of environment
    if !env_path.exists() {
        op.failure();
        return Err(eyre::eyre!(
            "Source instance LMDB environment not found at {:?}",
            env_path
        ));
    }

    if !data_file.exists() {
        op.failure();
        return Err(eyre::eyre!(
            "Source instance data file not found at {:?}",
            data_file
        ));
    }

    // Prepare branch volume
    let branch_volumes_dir = project
        .root
        .join(".helix")
        .join(".volumes")
        .join(&branch_name)
        .join("user");

    let branch_data_file = branch_volumes_dir.join("data.mdb");

    // Check if the destination already exists
    if branch_data_file.exists() {
        print_warning(&format!("A branched database already exists at {:?}", branch_volumes_dir));
        let confirmed = print_confirm("Do you want to overwrite it?")?;
        if !confirmed {
            op.failure();
            crate::output::info("Branch aborted by user");
            return Ok(());
        }
    }

    create_dir_all(&branch_volumes_dir)?;

    let total_size = fs::metadata(&data_file)?.len();
    const TEN_GB: u64 = 10 * 1024 * 1024 * 1024;

    if total_size > TEN_GB {
        let size_gb = (total_size as f64) / (1024.0 * 1024.0 * 1024.0);
        print_warning(&format!(
            "Database size is {:.2} GB. Taking atomic snapshot… this may take time depending on DB size",
            size_gb
        ));
        let confirmed = print_confirm("Do you want to continue?")?;
        if !confirmed {
            op.failure();
            crate::output::info("Branch aborted by user");
            return Ok(());
        }
    }

    let mut copy_step = Step::with_messages(
        &format!("Cloning '{}' to '{}' (zero-downtime snapshot)", source, branch_name),
        &format!("Securely cloned database to branch '{}'", branch_name)
    );
    copy_step.start();

    let env = unsafe {
        EnvOpenOptions::new()
            .flags(EnvFlags::READ_ONLY)
            .max_dbs(200)
            .max_readers(200)
            .open(env_path)?
    };

    Step::verbose_substep(&format!("Copying {:?} → {:?}", &data_file, &branch_volumes_dir));

    // Note: LMDB environments consist of two files: data.mdb and lock.mdb.
    // heed3::Env::copy_to_path naturally copies the data file with an atomic snapshot 
    // and inherently skips lock.mdb since the destination doesn't need the source's lock state.
    // When the destination environment is subsequently opened, LMDB safely auto-generates 
    // a fresh lock.mdb matching the destination.
    env.copy_to_path(branch_data_file, CompactionOption::Disabled)?;

    copy_step.done();

    let mut deployed_port = None;

    if deploy {
        let mut cleanup_tracker = CleanupTracker::new();

        let mut config_step = Step::with_messages(
            &format!("Configuring new instance '{}'", branch_name), 
            "Instance configured successfully"
        );
        config_step.start();

        let config_path = project.root.join("helix.toml");
        cleanup_tracker.backup_config(&project.config, config_path.clone());

        let source_db_config = project.config.get_instance(&source)?.db_config().clone();
        
        let branch_port = port.or_else(|| {
            // Find an unused port starting from 6969 to avoid collision
            let mut p = 6969;
            let mut used_ports = std::collections::HashSet::new();
            for instance in project.config.local.values() {
                if let Some(port) = instance.port {
                    used_ports.insert(port);
                }
            }
            while used_ports.contains(&p) {
                p += 1;
            }
            Some(p)
        });
        
        deployed_port = branch_port;

        let mut new_config = project.config.clone();
        new_config.local.insert(
            branch_name.clone(),
            crate::config::LocalInstanceConfig {
                port: branch_port,
                build_mode: crate::config::BuildMode::Dev,
                db_config: source_db_config,
            },
        );

        project.config = new_config; // update project config
        project.config.save_to_file(&config_path)?;

        config_step.done();

        crate::output::info(&format!("Deploying branched instance '{}'...", branch_name));
        
        let metrics_sender = crate::metrics_sender::MetricsSender::new()?;
        
        // Build instance
        if let Err(e) = crate::commands::build::run(Some(branch_name.clone()), None, &metrics_sender).await {
            op.failure();
            cleanup_tracker.cleanup().log_summary();
            return Err(e);
        }
        
        // Start instance
        if let Err(e) = crate::commands::start::run(Some(branch_name.clone())).await {
            op.failure();
            cleanup_tracker.cleanup().log_summary();
            return Err(e);
        }
    }

    op.success();

    if Verbosity::current().show_normal() {
        let mut details = vec![
            ("Source instance", source.clone()),
            ("Branch name", branch_name.clone()),
            ("Branch path", branch_volumes_dir.display().to_string()),
        ];
        
        let port_str;
        if let Some(p) = deployed_port {
            port_str = format!("http://localhost:{}", p);
            details.push(("Branch URL", port_str.clone()));
        }
        
        // using lifetime hack for print_details since it requires &[(&str, &str)]
        let details_ref: Vec<(&str, &str)> = details.iter().map(|(k, v)| (*k, v.as_str())).collect();
        Operation::print_details(&details_ref);
    }

    // Give the user award-winning polish with instructions
    let mut next_steps = Vec::new();
    let port_str = deployed_port.unwrap_or(6969).to_string();
    if deploy {
        let url = format!("http://localhost:{}", port_str);
        next_steps.push(format!("Access your branched instance at {}", url));
        next_steps.push(format!("Run 'helix stop {}' when you are done to conserve resources", branch_name));
    } else {
        next_steps.push(format!("Run 'helix add local --name {}' to configure a local instance using this branch", branch_name));
        next_steps.push(format!("Run 'helix push {}' to deploy the branch when ready", branch_name));
    }
    
    let instructions: Vec<&str> = next_steps.iter().map(|s| s.as_str()).collect();
    print_instructions("Next steps:", &instructions);

    Ok(())
}
