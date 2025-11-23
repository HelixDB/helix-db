use crate::project::ProjectContext;
use crate::utils::{print_confirm, print_error, print_status, print_success, print_warning};
use eyre::Result;
use std::fs;
use std::fs::create_dir_all;
use std::path::Path;
use std::path::PathBuf;

pub async fn run(output: Option<PathBuf>, instance_name: String) -> Result<()> {
    // Load project context
    let project = ProjectContext::find_and_load(None)?;

    // Get instance config
    let _instance_config = project.config.get_instance(&instance_name)?;

    print_status("BACKUP", &format!("Backing up instance '{instance_name}'"));

    // Get the instance volume
    let volumes_dir = project
        .root
        .join(".helix")
        .join(".volumes")
        .join(&instance_name)
        .join("user");

    let data_file = volumes_dir.join("data.mdb");
    let lock_file = volumes_dir.join("lock.mdb");

    // Check existence of data_file before calling metadata()
    if !data_file.exists() {
        return Err(eyre::eyre!(
            "instance data file not found at {:?}",
            data_file
        ));
    }

    // Check existence of lock_file before calling metadata()
    if !lock_file.exists() {
        return Err(eyre::eyre!(
            "instance lock file not found at {:?}",
            lock_file
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
    let total_size = fs::metadata(&data_file)?.len() + std::fs::metadata(&lock_file)?.len();

    const TEN_GB: u64 = 10 * 1024 * 1024 * 1024;

    // Check and warn if file is greater than 10 GB

    if total_size > TEN_GB {
        let size_gb = (total_size as f64) / (1024.0 * 1024.0 * 1024.0);
        print_warning(&format!(
            "Backup size is {:.2} GB — this may take a while.",
            size_gb
        ));
        let confirmed = print_confirm(&format!("Do you want to continue?"));
        if !confirmed? {
            print_status("CANCEL", "Backup aborted by user");
            return Ok(());
        }
    }

    // Check if the instance is readable or not
    if !check_read_write_permission(&data_file, &backup_dir.join("data.mdb"))? {
        print_status("CANCEL", "Backup aborted due to permission failure");
        return Ok(());
    }

    if !check_read_write_permission(&lock_file, &backup_dir.join("lock.mdb"))? {
        print_status("CANCEL", "Backup aborted due to permission failure");
        return Ok(());
    }

    println!("Copying {:?} → {:?}", &data_file, &backup_dir);

    // Copy the instance data
    fs::copy(&data_file, backup_dir.join("data.mdb"))?;
    fs::copy(&lock_file, backup_dir.join("lock.mdb"))?;

    print_success(&format!(
        "Backup for '{instance_name}' created at {:?}",
        backup_dir
    ));

    Ok(())
}

pub fn check_read_write_permission(src: &Path, dest: &Path) -> std::io::Result<bool> {
    // Check permission for src
    print_status("BACKUP", "Checking read permission for: src");
    if let Err(_e) = fs::File::open(src) {
        print_error("Not readable");
        return Ok(false);
    }
    print_status("BACKUP", "Readable ✔");

    // Check permission for dest
    print_status("BACKUP", "Checking write permission for: dest");
    if let Some(dir) = dest.parent() {
        let testfile = dir.join(".perm_test");

        if let Err(_e) = fs::File::create(&testfile) {
            print_error("Not writable");
            return Ok(false);
        }

        let _ = fs::remove_file(testfile);
        print_status("BACKUP", "Writable ✔");
    } else {
        print_error("Destination has no parent directory");
        return Ok(false);
    }

    Ok(true)
}
