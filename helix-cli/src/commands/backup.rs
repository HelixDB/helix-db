use crate::project::ProjectContext;
use crate::utils::{print_confirm, print_status, print_success, print_warning};
use eyre::Result;
use heed3::{CompactionOption, EnvFlags, EnvOpenOptions};
use std::fs;
use std::fs::create_dir_all;
use std::path::Path;
use std::path::PathBuf;

pub async fn run(output: Option<PathBuf>, instance_name: String) -> Result<()> {
    // Load project context
    let project = ProjectContext::find_and_load(None)?;

    print_status("BACKUP", &format!("Backing up instance '{instance_name}'"));

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

    let completed = backup_instance_to_dir(&project, &instance_name, &backup_dir)?;
    if !completed {
        return Ok(());
    }

    print_success(&format!(
        "Backup for '{instance_name}' created at {:?}",
        backup_dir
    ));

    Ok(())
}

pub(crate) fn backup_instance_to_dir(
    project: &ProjectContext,
    instance_name: &str,
    output_dir: &Path,
) -> Result<bool> {
    // Get instance config
    let instance_config = project.config.get_instance(instance_name)?;

    if !instance_config.is_local() {
        return Err(eyre::eyre!(
            "Backup is only supported for local instances"
        ));
    }

    // Get the instance volume
    let env_path = project.instance_user_dir(instance_name)?;
    let data_file = project.instance_data_file(instance_name)?;
    let env_path = Path::new(&env_path);

    // Validate existence of environment
    if !env_path.exists() {
        return Err(eyre::eyre!(
            "Instance LMDB environment not found at {:?}",
            env_path
        ));
    }

    // Check existence of data_file before calling metadata()
    if !data_file.exists() {
        return Err(eyre::eyre!(
            "instance data file not found at {:?}",
            data_file
        ));
    }

    create_dir_all(output_dir)?;

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
            print_status("CANCEL", "Backup aborted by user");
            return Ok(false);
        }
    }

    // Open LMDB read-only snapshot environment
    let env = unsafe {
        EnvOpenOptions::new()
            .flags(EnvFlags::READ_ONLY)
            .max_dbs(200)
            .max_readers(200)
            .open(env_path)?
    };

    println!("Copying {:?} → {:?}", &data_file, &output_dir);

    // backup database to given database
    env.copy_to_path(output_dir.join("data.mdb"), CompactionOption::Disabled)?;

    Ok(true)
}
