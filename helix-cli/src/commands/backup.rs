use crate::output::{Operation, Step, Verbosity};
use crate::project::ProjectContext;
use crate::utils::{print_confirm, print_warning};
use eyre::Result;
use heed3::{CompactionOption, EnvFlags, EnvOpenOptions};
use std::fs;
use std::fs::create_dir_all;
use std::path::Path;
use std::path::PathBuf;

pub async fn run(output: Option<PathBuf>, instance_name: String) -> Result<()> {
    // Load project context
    let project = ProjectContext::find_and_load(None)?;

    // Get instance config
    let _instance_config = project.config.get_instance(&instance_name)?;

    let op = Operation::new("Backing up", &instance_name);

    // Get the instance volume
    let volumes_dir = project
        .root
        .join(".helix")
        .join(".volumes")
        .join(&instance_name)
        .join("user");

    let data_file = volumes_dir.join("data.mdb");

    let env_path = Path::new(&volumes_dir);

    // Validate existence of environment
    if !env_path.exists() {
        op.failure();
        return Err(eyre::eyre!(
            "Instance LMDB environment not found at {:?}",
            env_path
        ));
    }

    // Check existence of data_file before calling metadata()
    if !data_file.exists() {
        op.failure();
        return Err(eyre::eyre!(
            "instance data file not found at {:?}",
            data_file
        ));
    }

    // Get path to backup instance
    let backup_dir = match output {
        Some(path) => path,
        None => {
            let ts = chrono::Local::now()
                .format("backup-%Y%m%d-%H%M%S")
                .to_string();
            project.root.join("backups").join(ts)
        }
    };

    create_dir_all(&backup_dir)?;

    // Get the size of the data
    let total_size = fs::metadata(&data_file)?.len();

    const TEN_GB: u64 = 10 * 1024 * 1024 * 1024;

    // Check and warn if file is greater than 10 GB

    if total_size > TEN_GB {
        let size_gb = (total_size as f64) / (1024.0 * 1024.0 * 1024.0);
        print_warning(&format!(
            "Backup size is {:.2} GB. Taking atomic snapshot… this may take time depending on DB size",
            size_gb
        ));
        let confirmed = print_confirm("Do you want to continue?");
        if !confirmed? {
            crate::output::info("Backup aborted by user");
            return Ok(());
        }
    }

    // Open LMDB read-only snapshot environment
    let mut copy_step = Step::with_messages("Copying database", "Database copied");
    copy_step.start();

    let env = unsafe {
        EnvOpenOptions::new()
            .flags(EnvFlags::READ_ONLY)
            .max_dbs(200)
            .max_readers(200)
            .open(env_path)?
    };

    Step::verbose_substep(&format!("Copying {:?} → {:?}", &data_file, &backup_dir));

    // backup database to given database
    env.copy_to_path(backup_dir.join("data.mdb"), CompactionOption::Disabled)?;

    copy_step.done();
    op.success();

    if Verbosity::current().show_normal() {
        Operation::print_details(&[("Backup location", &backup_dir.display().to_string())]);
    }

    Ok(())
}
