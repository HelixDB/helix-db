use crate::{
    args::{CommandType, HelixCli},
    instance_manager::InstanceManager,
    types::*,
    utils::*,
};
use clap::Parser;
use helix_db::{helix_engine::traversal_core::config::Config, utils::styled_string::StyledString};
use helix_metrics::{
    HelixMetricsClient,
    events::{DeployLocalEvent, EventType, RedeployLocalEvent},
};
use serde::Deserialize;
use sonic_rs::json;
use spinners::{Spinner, Spinners};
use std::{
    fmt::Write,
    fs::{self, File, OpenOptions, read_to_string},
    io::{Read, Write as iWrite},
    path::{Path, PathBuf},
    process::{Command, ExitCode},
    sync::LazyLock,
    time::Instant,
};

mod args;
mod docker_dev_manager;
mod instance_manager;
mod types;
mod utils;

pub static HELIX_METRICS_CLIENT: LazyLock<HelixMetricsClient> =
    LazyLock::new(HelixMetricsClient::new);

#[tokio::main]
async fn main() -> ExitCode {
    let exit_code = run().await;
    HELIX_METRICS_CLIENT.flush().await;
    exit_code
}

async fn run() -> ExitCode {
    check_helix_version().await;

    let args = HelixCli::parse();
    match args.command {
        CommandType::Deploy(command) => {
            let start_time = Instant::now();
            match Command::new("cargo").output() {
                Ok(_) => {}
                Err(_) => {
                    println!("{}", "Cargo is not installed".red().bold());
                    return ExitCode::FAILURE;
                }
            };

            match check_helix_installation() {
                Some(_) => {}
                None => {
                    println!(
                        "{}",
                        "Helix is not installed. Please run `helix install` first."
                            .red()
                            .bold()
                    );
                    return ExitCode::FAILURE;
                }
            };

            if command.path.is_none()
                && !Path::new(&format!("./{DB_DIR}")).is_dir()
                && command.cluster.is_none()
            {
                println!("{}", "No path or instance specified!".red().bold());
                return ExitCode::FAILURE;
            }

            // -- helix start --
            if command.cluster.is_some()
                && command.path.is_none()
                && !Path::new(&format!("./{DB_DIR}")).is_dir()
            {
                let instance_manager = InstanceManager::new().unwrap();
                let mut sp = Spinner::new(Spinners::Dots9, "Starting Helix instance".into());
                let openai_key = get_openai_key();
                match instance_manager.start_instance(
                    &command.cluster.unwrap(),
                    None,
                    openai_key,
                    BuildMode::from_release(command.release),
                ) {
                    Ok(instance) => {
                        sp.stop_with_message(
                            "Successfully started Helix instance"
                                .green()
                                .bold()
                                .to_string(),
                        );
                        HELIX_METRICS_CLIENT.send_event(
                            EventType::DeployLocal,
                            DeployLocalEvent {
                                cluster_id: instance.id.clone(),
                                queries_string: "".to_string(),
                                num_of_queries: 0,
                                time_taken_sec: 0,
                                success: true,
                                error_messages: None,
                            },
                        );
                        print_instance(instance);
                    }
                    Err(e) => {
                        sp.stop_with_message("Failed to start instance".red().bold().to_string());
                        println!("└── {} {}", "Error:".red().bold(), e);
                        return ExitCode::FAILURE;
                    }
                }
                return ExitCode::SUCCESS;
            }

            let output = dirs::home_dir()
                .unwrap_or_else(|| PathBuf::from("./"))
                .join(".helix/repo/helix-db/helix-container");
            let start_port = command.port.unwrap_or(6969);

            let path = get_cfg_deploy_path(command.path.clone());
            let files = match check_and_read_files(&path) {
                Ok(files) if !files.is_empty() => files,
                Ok(_) => {
                    println!("{}", "No queries found, nothing to compile".red().bold());
                    return ExitCode::FAILURE;
                }
                Err(e) => {
                    println!("{} {}", "Error:".red().bold(), e);
                    return ExitCode::FAILURE;
                }
            };

            if !command.remote {
                let code = match compile_and_build_helix(
                    path,
                    &output,
                    files,
                    BuildMode::from_release(command.release),
                    command.dev,
                ) {
                    Ok(code) => code,
                    Err(_) => return ExitCode::FAILURE,
                };
                let queries_string = code
                    .source
                    .queries
                    .iter()
                    .map(|q| q.name.clone())
                    .collect::<Vec<String>>()
                    .join("\n");
                let num_of_queries = code.source.queries.len() as u32;
                let event = |instance_id: String, time_taken_sec: u32| DeployLocalEvent {
                    cluster_id: instance_id,
                    queries_string: queries_string.clone(),
                    num_of_queries,
                    time_taken_sec,
                    success: true,
                    error_messages: None,
                };
                let redeploy_event =
                    |instance_id: String, time_taken_sec: u32| RedeployLocalEvent {
                        cluster_id: instance_id,
                        queries_string: queries_string.clone(),
                        num_of_queries,
                        time_taken_sec,
                        success: true,
                        error_messages: None,
                    };

                if command.cluster.is_some()
                    && (command.path.is_some() || Path::new(&format!("./{DB_DIR}")).is_dir())
                {
                    println!(
                        "{} {}",
                        "Redeploying cluster".green().bold(),
                        command.cluster.clone().unwrap()
                    );
                    match redeploy_helix(
                        command.cluster.clone().unwrap(),
                        code,
                        BuildMode::from_release(command.release),
                    ) {
                        Ok(_) => {
                            HELIX_METRICS_CLIENT.send_event(
                                EventType::RedeployLocal,
                                redeploy_event(
                                    command.cluster.unwrap(),
                                    start_time.elapsed().as_secs() as u32,
                                ),
                            );
                        }
                        Err(_) => {
                            println!("{}", "Failed to deploy Helix queries".red().bold());
                            return ExitCode::FAILURE;
                        }
                    }
                    println!("{}", "Successfully deployed Helix queries".green().bold());
                    return ExitCode::SUCCESS;
                }

                // -- helix deploy --
                if command.cluster.is_none()
                    && (command.path.is_some() || Path::new(&format!("./{DB_DIR}")).is_dir())
                {
                    let port = match find_available_port(start_port) {
                        Some(port) => {
                            if port != start_port {
                                println!(
                                    "{} {} {} {} {}",
                                    "Port".yellow(),
                                    start_port,
                                    "is in use, using port".yellow(),
                                    port,
                                    "instead".yellow(),
                                );
                            }
                            port
                        }
                        None => {
                            println!(
                                "{} {}",
                                "No available ports found starting from".red().bold(),
                                start_port
                            );
                            return ExitCode::FAILURE;
                        }
                    };
                    match deploy_helix(port, code, None, BuildMode::from_release(command.release)) {
                        Ok(cluster_id) => {
                            HELIX_METRICS_CLIENT.send_event(
                                EventType::DeployLocal,
                                event(cluster_id, start_time.elapsed().as_secs() as u32),
                            );
                        }
                        Err(_) => {
                            println!("{}", "Failed to deploy Helix queries".red().bold());
                            return ExitCode::FAILURE;
                        }
                    }
                    println!("{}", "Successfully deployed Helix queries".green().bold());
                    return ExitCode::SUCCESS;
                }
            } else if let Some(cluster) = command.cluster {
                match redeploy_helix_remote(cluster.clone(), path, files).await {
                    Ok(_) => {
                        return ExitCode::SUCCESS;
                    }
                    Err(_) => {
                        println!("{}", "Failed to deploy Helix queries".red().bold());
                        return ExitCode::FAILURE;
                    }
                }
            } else {
                println!(
                    "{}",
                    "Need to pass in a cluster id when redeploying a remote instance!"
                        .red()
                        .bold()
                );
                return ExitCode::FAILURE;
            }
        }

        CommandType::Update => {
            match check_helix_installation() {
                Some(_) => {} // container path
                None => {
                    println!(
                        "{}",
                        "Helix is not installed. Please run `helix install` first."
                            .red()
                            .bold()
                    );
                    return ExitCode::FAILURE;
                }
            };

            let repo_path = {
                let home_dir = match dirs::home_dir() {
                    Some(dir) => dir,
                    None => {
                        println!("{}", "Could not determine home directory".red().bold());
                        return ExitCode::FAILURE;
                    }
                };
                home_dir.join(".helix/repo/helix-db/helix-db")
            };

            if !check_cargo_version() {
                match Command::new("rustup").arg("update").output() {
                    Ok(_) => println!("{}", "Updating cargo!".green().bold()),
                    Err(e) => println!("Error updating cargo! {e}"),
                }
            } else {
                println!("{}", "cargo up-to-date!".green().bold());
            }

            let local_cli_version = match get_cli_version() {
                Ok(val) => val,
                Err(e) => {
                    println!(
                        "{} {}",
                        "Failed fetching the local cli version".red().bold(),
                        e
                    );
                    return ExitCode::FAILURE;
                }
            };
            let local_helix_version = match get_crate_version(&repo_path) {
                Ok(val) => val,
                Err(e) => {
                    println!(
                        "{} {}",
                        "Failed fetching the local db version".red().bold(),
                        e
                    );
                    return ExitCode::FAILURE;
                }
            };
            let remote_helix_version = get_remote_helix_version().await.unwrap();
            println!(
                "local helix-cli version: {local_cli_version}, local helix-db version: {local_helix_version}, remote helix version: {remote_helix_version}",
            );

            if local_helix_version < remote_helix_version
                || local_cli_version < remote_helix_version
            {
                let mut runner = Command::new("git");
                runner.arg("reset");
                runner.arg("--hard");
                runner.current_dir(&repo_path);
                match runner.output() {
                    Ok(_) => {}
                    Err(e) => {
                        println!(
                            "{} {}",
                            "Error while reseting installed helix-db version:"
                                .red()
                                .bold(),
                            e
                        );
                        return ExitCode::FAILURE;
                    }
                }

                match Command::new("git")
                    .arg("pull")
                    .current_dir(&repo_path)
                    .output()
                {
                    Ok(_) => println!(
                        "{}",
                        "New helix-db version successfully pulled!".green().bold()
                    ),
                    Err(e) => {
                        println!(
                            "{} {}",
                            "Error while pulling new helix-db version:".red().bold(),
                            e
                        );
                        return ExitCode::FAILURE;
                    }
                }

                match get_n_helix_cli() {
                    Ok(_) => println!(
                        "{}",
                        "New helix-cli version successfully installed!"
                            .green()
                            .bold()
                    ),
                    Err(e) => {
                        println!(
                            "{} {}",
                            "Error while installing new helix-cli version:".red().bold(),
                            e
                        );
                        return ExitCode::FAILURE;
                    }
                }
            } else {
                println!("{}", "HelixDB is up to date!".green().bold());
            }
        }

        CommandType::Compile(command) => {
            let path = match get_path_or_cwd(command.path.as_ref()) {
                Ok(path) => path,
                Err(e) => {
                    println!("{}", "Error: failed to get path".red().bold());
                    println!("└── {e}");
                    return ExitCode::FAILURE;
                }
            };

            let output = match &command.output {
                Some(output) => output.to_owned(),
                None => ".".to_string(),
            };

            let mut sp = Spinner::new(Spinners::Dots9, "Compiling Helix queries".into());
            let files = match check_and_read_files(&path) {
                Ok(files) => files,
                Err(e) => {
                    sp.stop_with_message("Failed to read files".red().bold().to_string());
                    println!("└── {e}");
                    return ExitCode::FAILURE;
                }
            };

            if files.is_empty() {
                sp.stop_with_message(
                    "No queries found, nothing to compile"
                        .red()
                        .bold()
                        .to_string(),
                );
                return ExitCode::FAILURE;
            }

            let analyzed_source = match generate(&files, &path) {
                Ok((_, analyzed_source)) => analyzed_source,
                Err(e) => {
                    sp.stop_with_message(e.to_string().red().bold().to_string());
                    return ExitCode::FAILURE;
                }
            };

            if let Some(OutputLanguage::TypeScript) = command.r#gen {
                match gen_typescript(&analyzed_source, &output) {
                    Ok(_) => {}
                    Err(e) => {
                        println!("{} {}", "Failed to write typescript types".red().bold(), e);
                        println!("└── {} {}", "Error:".red().bold(), e);
                        return ExitCode::FAILURE;
                    }
                };
            }

            let file_path = PathBuf::from(&output).join("queries.rs");
            let mut generated_rust_code = String::new();
            match write!(&mut generated_rust_code, "{analyzed_source}") {
                Ok(_) => sp.stop_with_message(
                    "Successfully transpiled queries".green().bold().to_string(),
                ),
                Err(e) => {
                    println!("{}", "Failed to transpile queries".red().bold());
                    println!("└── {} {}", "Error:".red().bold(), e);
                    return ExitCode::FAILURE;
                }
            }

            match fs::write(file_path, generated_rust_code) {
                Ok(_) => println!(
                    "{} {}",
                    "Successfully compiled queries to".green().bold(),
                    output
                ),
                Err(e) => {
                    println!("{} {}", "Failed to write queries file".red().bold(), e);
                    println!("└── {} {}", "Error:".red().bold(), e);
                    return ExitCode::FAILURE;
                }
            }
        }

        CommandType::Check(command) => {
            let path = match get_path_or_cwd(command.path.as_ref()) {
                Ok(path) => path,
                Err(e) => {
                    println!("{}", "Error: failed to get path".red().bold());
                    println!("└── {e}");
                    return ExitCode::FAILURE;
                }
            };

            let mut sp = Spinner::new(Spinners::Dots9, "Checking Helix queries".into());

            let files = match check_and_read_files(&path) {
                Ok(files) => files,
                Err(e) => {
                    sp.stop_with_message("Error checking files".red().bold().to_string());
                    println!("└── {e}");
                    return ExitCode::FAILURE;
                }
            };

            if files.is_empty() {
                sp.stop_with_message(
                    "No queries found, nothing to compile"
                        .red()
                        .bold()
                        .to_string(),
                );
                return ExitCode::FAILURE;
            }

            match generate(&files, &path) {
                Ok(_) => {}
                Err(e) => {
                    sp.stop_with_message("Failed to generate queries".red().bold().to_string());
                    println!("└── {e}");
                    return ExitCode::FAILURE;
                }
            }

            sp.stop_with_message(
                "Helix-QL schema and queries validated successfully with zero errors"
                    .green()
                    .bold()
                    .to_string(),
            );
        }

        CommandType::Install(command) => {
            match Command::new("cargo").output() {
                Ok(_) => {}
                Err(_) => {
                    println!("{}", "Cargo is not installed".red().bold());
                    return ExitCode::FAILURE;
                }
            }

            if !check_cargo_version() {
                match Command::new("rustup").arg("update").output() {
                    Ok(_) => println!("{}", "Updating cargo!".green().bold()),
                    Err(e) => println!("Error updating cargo! {e}"),
                }
            } else {
                println!("{}", "cargo up-to-date!".green().bold());
            }

            match Command::new("git").arg("version").output() {
                Ok(_) => {}
                Err(_) => {
                    println!("{}", "Git is not installed".red().bold());
                    return ExitCode::FAILURE;
                }
            }

            let repo_path = {
                // check if helix repo exists
                let home_dir = match dirs::home_dir() {
                    Some(dir) => dir,
                    None => {
                        println!("{}", "Could not determine home directory".red().bold());
                        return ExitCode::FAILURE;
                    }
                };
                home_dir.join(".helix/repo")
            };

            if repo_path.clone().join("helix-db").exists()
                && repo_path.clone().join("helix-db").is_dir()
            {
                println!(
                    "{} {}",
                    "Helix repo already exists at".yellow().bold(),
                    repo_path
                        .join("helix-db")
                        .display()
                        .to_string()
                        .yellow()
                        .bold(),
                );
                return ExitCode::FAILURE;
            }

            match fs::create_dir_all(&repo_path) {
                Ok(_) => println!(
                    "{} {}",
                    "Created directory structure at".green().bold(),
                    repo_path.display()
                ),
                Err(e) => {
                    println!("{}", "Failed to create directory structure".red().bold());
                    println!("|");
                    println!("└── {e}");
                    return ExitCode::FAILURE;
                }
            }

            let mut runner = Command::new("git");
            runner.arg("clone");
            runner.arg("https://github.com/HelixDB/helix-db.git");
            if command.dev {
                runner.arg("--branch").arg("dev");
            }
            runner.current_dir(&repo_path);

            match runner.output() {
                Ok(_) => {
                    let home_dir = dirs::home_dir().unwrap();
                    println!(
                        "{} {}",
                        "Helix repo installed at".green().bold(),
                        home_dir.join(".helix/repo/").to_string_lossy()
                    );
                    println!("|");
                    println!("└── To get started, begin writing helix queries in your project.");
                    println!("|");
                    println!(
                        "└── Then run `helix check --path <path-to-project>` to check your queries."
                    );
                    println!("|");
                    println!(
                        "└── Then run `helix deploy --path <path-to-project>` to build your queries."
                    );
                }
                Err(e) => {
                    println!("{}", "Failed to install Helix repo".red().bold());
                    println!("|");
                    println!("└── {e}");
                    return ExitCode::FAILURE;
                }
            }
        }

        CommandType::Init(command) => {
            println!("{}", "Initialising Helix project...".bold());

            let path_str = match get_path_or_cwd(command.path.as_ref()) {
                Ok(path) => path,
                Err(e) => {
                    println!("{}", "Error: failed to get path".red().bold());
                    println!("└── {e}");
                    return ExitCode::FAILURE;
                }
            };

            let path = PathBuf::from(&path_str);

            match check_and_read_files(&path_str) {
                Ok(files) if !files.is_empty() => {
                    println!(
                        "{} {}",
                        "Queries already exist in".yellow().bold(),
                        path_str
                    );
                    return ExitCode::FAILURE;
                }
                Ok(_) => {}
                Err(_) => {}
            };

            fs::create_dir_all(&path).unwrap();

            let schema_path = path.join("schema.hx");
            fs::write(&schema_path, DEFAULT_SCHEMA).expect("could not write schema");

            let main_path = path.join("queries.hx");
            fs::write(main_path, DEFAULT_QUERIES).expect("could not write queries");

            let config_path = path.join("config.hx.json");
            fs::write(config_path, Config::init_config()).expect("could not write config");

            println!(
                "{} {}",
                "Helix project initialised at".green().bold(),
                path.display()
            );
        }

        CommandType::Metrics(command) => {
            // get metrics from ~/.helix/credentials
            let home_dir = std::env::var("HOME").unwrap_or("~/".to_string());
            let config_path = &format!("{home_dir}/.helix/credentials");
            let config_path = Path::new(config_path);
            let metrics = match fs::read_to_string(config_path) {
                Ok(config) => parse_credentials(&config).metrics.unwrap_or(true),
                Err(_) => {
                    // create file
                    fs::write(config_path, "helix_user_id=\nhelix_user_key=\nmetrics=true")
                        .unwrap();
                    true
                }
            };

            if command.off {
                if metrics {
                    // write to config_path
                    let mut file = File::open(config_path).unwrap();
                    let mut contents = String::new();
                    file.read_to_string(&mut contents).unwrap();
                    let new_contents = contents.replace("metrics=true", "metrics=false");
                    file.write_all(new_contents.as_bytes()).unwrap();
                    println!("{}", "Metrics disabled".yellow().bold());
                } else {
                    println!("{}", "Metrics already disabled".yellow().bold());
                }
            } else if command.on {
                if !metrics {
                    // write to config_path
                    let mut file = File::open(config_path).unwrap();
                    let mut contents = String::new();
                    file.read_to_string(&mut contents).unwrap();
                    let new_contents = contents.replace("metrics=false", "metrics=true");
                    file.write_all(new_contents.as_bytes()).unwrap();
                    println!("{}", "Metrics enabled".green().bold());
                } else {
                    println!("{}", "Metrics already enabled".green().bold());
                }
            }
        }

        CommandType::Status => {
            let instance_manager = InstanceManager::new().unwrap();
            match instance_manager.list_instances() {
                Ok(instances) => {
                    if instances.is_empty() {
                        println!("{}", "No running Helix instances".yellow().bold());
                        return ExitCode::FAILURE;
                    }
                    for instance in instances {
                        print_instance(instance);
                        println!();
                    }
                }
                Err(e) => println!("{} {}", "Failed to list instances:".red().bold(), e),
            }
        }

        CommandType::Stop(command) => {
            let instance_manager = InstanceManager::new().unwrap();
            match instance_manager.list_instances() {
                Ok(instances) => {
                    if !instance_manager.running_instances().unwrap() {
                        println!("{}", "No running Helix instances".bold());
                        return ExitCode::FAILURE;
                    }
                    if command.all {
                        println!("{}", "Stopping all running Helix instances".bold());
                        instances.iter().for_each(|instance| {
                            if instance.running {
                                match instance_manager.stop_instance(instance.id.as_str()) {
                                    Ok(_) => {
                                        println!(
                                            "└── {} {}",
                                            "ID:".yellow().bold(),
                                            instance.id.yellow().bold()
                                        );
                                    }
                                    Err(e) => {
                                        println!(
                                            "{} {}, {}",
                                            "Failed to stop instance:".red().bold(),
                                            instance.id.red().bold(),
                                            e,
                                        );
                                    }
                                }
                            }
                        });
                    } else if let Some(cluster_id) = command.cluster {
                        match instance_manager.stop_instance(&cluster_id) {
                            Ok(false) => {
                                println!(
                                    "{} {}",
                                    "Instance is not running".yellow().bold(),
                                    cluster_id
                                )
                            }
                            Ok(true) => {
                                println!("{} {}", "Stopped instance".green().bold(), cluster_id)
                            }
                            Err(e) => println!("{} {}", "Failed to stop instance:".red().bold(), e),
                        }
                    } else {
                        println!(
                            "{}",
                            "Please specify --all or provide an instance ID\n"
                                .yellow()
                                .bold()
                        );
                        println!("Available instances (green=running, yellow=stopped): ");
                        for instance in instances {
                            print_instance(instance);
                        }
                    }
                }
                Err(e) => {
                    println!("{} {}", "Failed to find instances:".red().bold(), e);
                }
            }
        }

        CommandType::Save(command) => {
            let instance_manager = InstanceManager::new().unwrap();
            let iid = &command.cluster;

            match instance_manager.get_instance(iid) {
                Ok(Some(_)) => println!("{}", "Helix instance found!".green().bold()),
                Ok(None) => {
                    println!(
                        "{} {}",
                        "No Helix instance found with id".red().bold(),
                        iid.red().bold()
                    );
                    return ExitCode::FAILURE;
                }
                Err(e) => {
                    println!("{} {}", "Error:".red().bold(), e);
                    return ExitCode::FAILURE;
                }
            }

            let output_path = match command.output {
                Some(output) => format!("{output}helix_instance_{iid}"),
                None => format!("helix_instance_{iid}"),
            };
            let home_dir = std::env::var("HOME").expect("Failed to get HOME environment variable");
            let instance_path = format!("{home_dir}/.helix/cached_builds/data/{iid}/user");

            let mut runner = Command::new("cp");
            runner.arg("-r");
            runner.arg(instance_path);
            runner.arg(&output_path);
            match runner.output() {
                Ok(_) => println!(
                    "{} {}",
                    "Saved Helix instance to".green().bold(),
                    output_path.green().bold()
                ),
                Err(e) => println!("{} {}", "Error while copying:".red().bold(), e),
            }
        }

        CommandType::Delete(command) => {
            let instance_manager = InstanceManager::new().unwrap();
            let iid = &command.cluster;

            if iid.is_none() && !command.all {
                println!(
                    "{}",
                    "Need to pass either an instance or `--all`!".red().bold()
                );
                return ExitCode::FAILURE;
            }

            if instance_manager.list_instances().unwrap().is_empty() {
                println!("{}", "No instances running!".yellow().bold());
                return ExitCode::FAILURE;
            }

            if !command.all {
                let iid = iid.as_ref().unwrap();
                match instance_manager.get_instance(iid) {
                    Ok(Some(_)) => println!("{}", "Helix instance found!".green().bold()),
                    Ok(None) => {
                        println!(
                            "{} {}",
                            "No Helix instance found with id".red().bold(),
                            iid.red().bold()
                        );
                        return ExitCode::FAILURE;
                    }
                    Err(e) => {
                        println!("{} {}", "Error:".red().bold(), e);
                        return ExitCode::FAILURE;
                    }
                }
            }

            let mut _del_prompt: bool = false;
            print!(
                "Are you sure you want to delete the specified instances and their data? (y/n): "
            );
            std::io::stdout().flush().unwrap();
            let mut input = String::new();
            std::io::stdin().read_line(&mut input).unwrap();
            _del_prompt = input.trim().to_lowercase() == "y";

            if _del_prompt {
                let mut to_delete: Vec<String> = vec![];

                if command.all {
                    let instances = instance_manager.list_instances().unwrap();
                    for inst in instances {
                        to_delete.push(inst.id.to_string());
                    }
                } else {
                    let iid = match iid {
                        Some(val) => val,
                        None => {
                            println!(
                                "{}",
                                "Need to pass either an instance or `--all`!".red().bold()
                            );
                            return ExitCode::FAILURE;
                        }
                    };
                    to_delete.push(iid.to_string());
                }

                for del_iid in to_delete {
                    match instance_manager.stop_instance(&del_iid) {
                        Ok(true) => println!(
                            "{} {}",
                            "Stopped instance".green().bold(),
                            del_iid.green().bold()
                        ),
                        Ok(false) => {}
                        Err(e) => {
                            println!("{} {}", "Error while stopping instance".red().bold(), e)
                        }
                    }

                    match instance_manager.delete_instance(&del_iid) {
                        Ok(_) => println!("{}", "Deleted Helix instance".green().bold()),
                        Err(e) => {
                            println!("{} {}", "Error while deleting instance".red().bold(), e)
                        }
                    }

                    let home_dir =
                        std::env::var("HOME").expect("Failed to get HOME environment variable");
                    let instance_path = format!("{home_dir}/.helix/cached_builds/data/{del_iid}");
                    let binary_path = format!("{home_dir}/.helix/cached_builds/{del_iid}");
                    let log_path = format!("{home_dir}/.helix/logs/instance_{del_iid}.log");
                    let error_log_path =
                        format!("{home_dir}/.helix/logs/instance_{del_iid}_error.log");

                    let mut runner = Command::new("rm");
                    runner.arg("-r");
                    runner.arg(instance_path);
                    runner.arg(binary_path);
                    runner.arg(log_path);
                    runner.arg(error_log_path);

                    match runner.output() {
                        Ok(_) => println!("{}", "Deleted Helix instance data".green().bold()),
                        Err(e) => println!("{} {}", "Error while deleting data:".red().bold(), e),
                    }
                }
            }
        }

        CommandType::Version => match check_helix_installation() {
            Some(_) => {
                let repo_path = {
                    let home_dir = match dirs::home_dir() {
                        Some(dir) => dir,
                        None => {
                            println!(
                                "{}",
                                "helix-db: not installed (could not determine home directory)"
                                    .red()
                                    .bold()
                            );
                            return ExitCode::FAILURE;
                        }
                    };
                    home_dir.join(".helix/repo/helix-db/helix-db")
                };

                match get_crate_version(repo_path) {
                    Ok(local_db_version) => {
                        let local_cli_version = match get_cli_version() {
                            Ok(val) => val,
                            Err(e) => {
                                println!(
                                    "{} {}",
                                    "Error while fetching the local cli version!".red().bold(),
                                    e
                                );
                                return ExitCode::FAILURE;
                            }
                        };
                        println!(
                            "helix-cli version: {local_cli_version}, helix-db version: {local_db_version}"
                        );
                    }
                    Err(_) => println!("helix-db: installed but version could not be determined"),
                }
            }
            None => println!("helix-db: not installed (run 'helix install' to install)"),
        },

        CommandType::Login => {
            let home_dir = std::env::var("HOME").unwrap_or("~/".to_string());
            let config_path = &format!("{home_dir}/.helix");
            let config_path = Path::new(config_path);
            if !config_path.exists() {
                fs::create_dir_all(config_path).unwrap();
            }

            let cred_path = config_path.join("credentials");

            if let Ok(contents) = read_to_string(&cred_path)
                && let Some(_key) = parse_key_from_creds(&contents)
            {
                println!(
                    "You have an existing key which may be valid, only continue if it doesn't work or you want to switch accounts. (Key checking is WIP)"
                );
            }

            let (key, user_id) = github_login().await.unwrap();
            println!("{}", "Successfully logged in!".green().bold());

            let mut cred_file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(cred_path)
                .unwrap();

            if let Err(e) = cred_file
                .write_all(&format!("helix_user_id={user_id}\nhelix_user_key={key}").into_bytes())
            {
                println!(
                    "Got error when writing key: {}\nYou're key is: {}",
                    e.to_string().red(),
                    key
                );
            }
        }

        CommandType::Logout => {
            let home_dir = std::env::var("HOME").unwrap_or("~/".to_string());
            let config_path = &format!("{home_dir}/.helix");
            let config_path = Path::new(config_path);
            if !config_path.exists() {
                fs::create_dir_all(config_path).unwrap();
            }

            let cred_path = config_path.join("credentials");
            if cred_path.exists() {
                fs::remove_file(cred_path).unwrap()
            }
        }

        CommandType::CreateKey { cluster } => {
            let home_dir = std::env::var("HOME").unwrap_or("~/".to_string());
            let config_path = &format!("{home_dir}/.helix");
            let config_path = Path::new(config_path);
            if !config_path.exists() {
                println!("Error: Can't find credentials, try helix login");
                return ExitCode::FAILURE;
            }

            let cred_path = config_path.join("credentials");

            let contents = match read_to_string(&cred_path) {
                Ok(c) => c,
                Err(e) => {
                    println!("Error: Can't read credentials, try helix login: \n{e:?}");
                    return ExitCode::FAILURE;
                }
            };

            let key = match parse_key_from_creds(&contents) {
                Some(k) => k,
                None => {
                    println!("Error: Can't parse credentials, try helix login");
                    return ExitCode::FAILURE;
                }
            };

            let client = reqwest::Client::new();

            let cloud_url = format!("http://{}/clusters/create_api_key", *CLOUD_AUTHORITY);

            let res = client
                .post(cloud_url)
                .bearer_auth(key)
                .header("x-cluster-id", &cluster)
                .json(&json!({
                    "name": "N/A",
                }))
                .send()
                .await;

            let res = match res {
                Ok(r) => r,
                Err(e) => {
                    println!("Error sending request: {e:?}");
                    return ExitCode::FAILURE;
                }
            };

            match res.error_for_status() {
                Ok(r) => match r.json::<CreateKeyResponse>().await {
                    Ok(key_info) => {
                        println!(
                            "Successfully created new key for cluster: {}",
                            cluster.bold()
                        );
                        println!(
                            "Your api key is: {}, please store it carefully (id:{})",
                            key_info.key.bold(),
                            key_info.key_id
                        )
                    }
                    Err(e) => {
                        println!("Couldn't decode response: {e:?}");
                        return ExitCode::FAILURE;
                    }
                },
                Err(e) => {
                    println!("Error creating key: {e:?}");
                    return ExitCode::FAILURE;
                }
            }
        }

        CommandType::DockerDev(command) => {
            use args::DockerDevSubcommand;
            use docker_dev_manager::DockerDevManager;

            let manager = match DockerDevManager::new() {
                Ok(m) => m,
                Err(e) => {
                    println!(
                        "{}",
                        format!("Error initializing Docker dev manager: {e}")
                            .red()
                            .bold()
                    );
                    return ExitCode::FAILURE;
                }
            };

            match command.subcommand {
                DockerDevSubcommand::Run(run_cmd) => {
                    if let Err(e) = manager.run(run_cmd.background, run_cmd.port) {
                        println!("{}", format!("Error: {e}").red().bold());
                        return ExitCode::FAILURE;
                    }
                }
                DockerDevSubcommand::Stop => {
                    if let Err(e) = manager.stop() {
                        println!("{}", format!("Error: {e}").red().bold());
                        return ExitCode::FAILURE;
                    }
                }
                DockerDevSubcommand::Restart(run_cmd) => {
                    println!(
                        "{}",
                        "Restarting Docker development instance...".blue().bold()
                    );

                    // Stop first (don't fail if it's not running)
                    if let Err(e) = manager.stop() {
                        println!("{}", format!("Warning: {e}").yellow());
                    }

                    // Wait a moment for cleanup
                    std::thread::sleep(std::time::Duration::from_secs(1));

                    // Start again
                    if let Err(e) = manager.run(run_cmd.background, run_cmd.port) {
                        println!("{}", format!("Error: {e}").red().bold());
                        return ExitCode::FAILURE;
                    }
                }
                DockerDevSubcommand::Delete => {
                    if let Err(e) = manager.delete() {
                        println!("{}", format!("Error: {e}").red().bold());
                        return ExitCode::FAILURE;
                    }
                }
                DockerDevSubcommand::Status => {
                    if let Err(e) = manager.status() {
                        println!("{}", format!("Error: {e}").red().bold());
                        return ExitCode::FAILURE;
                    }
                }
                DockerDevSubcommand::Logs(logs_cmd) => {
                    if let Err(e) = manager.logs(logs_cmd.follow, logs_cmd.lines) {
                        println!("{}", format!("Error: {e}").red().bold());
                        return ExitCode::FAILURE;
                    }
                }
                DockerDevSubcommand::Exec(exec_cmd) => {
                    if exec_cmd.command.is_empty() {
                        println!("{}", "Error: No command specified".red().bold());
                        return ExitCode::FAILURE;
                    }
                    let command_args: Vec<&str> =
                        exec_cmd.command.iter().map(|s| s.as_str()).collect();
                    if let Err(e) = manager.exec_command(&command_args) {
                        println!("{}", format!("Error: {e}").red().bold());
                        return ExitCode::FAILURE;
                    }
                }
                DockerDevSubcommand::Url => match manager.get_instance_url() {
                    Ok(url) => {
                        println!("{}", url.blue().bold());
                    }
                    Err(e) => {
                        println!("{}", format!("Error: {e}").red().bold());
                        return ExitCode::FAILURE;
                    }
                },
            }
        }
    }

    ExitCode::SUCCESS
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateKeyResponse {
    key: String,
    key_id: String,
}
