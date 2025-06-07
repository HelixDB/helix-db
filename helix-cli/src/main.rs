use crate::{
    args::{CommandType, HelixCLI},
    instance_manager::InstanceManager,
    styled_string::StyledString,
    utils::*,
    types::*,
};
use helixdb::{
    helix_engine::graph_core::config::Config,
    ingestion_engine::postgres_ingestion::PostgresIngestor,
};
use std::{
    fmt::Write,
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    io::Write as iWrite,
};
use spinners::{Spinner, Spinners};
use args::OutputLanguage;
use clap::Parser;

pub mod args;
mod instance_manager;
mod styled_string;
mod utils;
mod types;

fn main() {
    let args = HelixCLI::parse();

    match args.command {
        CommandType::Deploy(command) => {
            match Command::new("cargo").output() {
                Ok(_) => {}
                Err(_) => {
                    println!("{}", "Cargo is not installed".red().bold());
                    return;
                }
            }

            match check_helix_installation() {
                Ok(_) => {}
                Err(_) => {
                    println!(
                        "{}",
                        "Helix is not installed. Please run `helix install` first."
                            .red()
                            .bold()
                    );
                    return;
                }
            };

            let path = get_cfg_deploy_path(command.path).unwrap();

            let output = match command.output {
                Some(output) => output,
                None => dirs::home_dir()
                    .map(|path| {
                        path.join(".helix/repo/helix-db/helix-container")
                            .to_string_lossy()
                            .into_owned()
                    })
                    .unwrap_or_else(|| "./.helix/repo/helix-db/helix-container".to_string()),
            };

            let start_port = match command.port {
                Some(port) => port,
                None => 6969,
            };

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
                    return;
                }
            };

            let files = match check_and_read_files(&path) {
                Ok(files) if !files.is_empty() => files,
                Ok(_) => {
                    println!("{}", "No queries found, nothing to compile".red().bold());
                    return;
                }
                Err(e) => {
                    println!("{} {}", "Error:".red().bold(), e);
                    return;
                }
            };

            let mut sp = Spinner::new(Spinners::Dots9, "Compiling Helix queries".into());

            let num_files = files.len();

            let (code, analyzed_source) = match generate(&files) {
                Ok(code) => code,
                Err(e) => {
                    sp.stop_with_message(format!("{}", "Error compiling queries".red().bold()));
                    println!("└── {}", e);
                    return;
                }
            };

            sp.stop_with_message(format!(
                "{} {} {}",
                "Successfully compiled".green().bold(),
                num_files,
                "query files".green().bold()
            ));

            let cache_dir = PathBuf::from(&output);
            fs::create_dir_all(&cache_dir).unwrap();

            let file_path = PathBuf::from(&output).join("src/queries.rs");
            let mut generated_rust_code = String::new();
            match write!(&mut generated_rust_code, "{}", analyzed_source) {
                Ok(_) => {
                    println!("{}", "Successfully transpiled queries".green().bold());
                }
                Err(e) => {
                    println!("{}", "Failed to transpile queries".red().bold());
                    println!("└── {} {}", "Error:".red().bold(), e);
                    return;
                }
            }
            match fs::write(file_path, generated_rust_code) {
                Ok(_) => {
                    println!("{}", "Successfully wrote queries file".green().bold());
                }
                Err(e) => {
                    println!("{}", "Failed to write queries file".red().bold());
                    println!("└── {} {}", "Error:".red().bold(), e);
                    return;
                }
            }

            let mut sp = Spinner::new(Spinners::Dots9, "Building Helix".into());

            // copy config.hx.json to ~/.helix/repo/helix-db/helix-container/config.hx.json
            let config_path = PathBuf::from(&output).join("src/config.hx.json");
            fs::copy(PathBuf::from(path + "/config.hx.json"), config_path).unwrap();

            // check rust code
            let mut runner = Command::new("cargo");
            runner
                .arg("check")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .current_dir(PathBuf::from(&output));

            match runner.output() {
                Ok(_) => {}
                Err(e) => {
                    sp.stop_with_message(format!("{}", "Failed to check Rust code".red().bold()));
                    println!("└── {} {}", "Error:".red().bold(), e);
                    return;
                }
            }

            let mut runner = Command::new("cargo");
            runner
                .arg("build")
                .arg("--release")
                .current_dir(PathBuf::from(&output));

            match runner.output() {
                Ok(output) => {
                    if output.status.success() {
                        sp.stop_with_message(format!(
                            "{}",
                            "Successfully built Helix".green().bold()
                        ));
                    } else {
                        sp.stop_with_message(format!("{}", "Failed to build Helix".red().bold()));
                        let stderr = String::from_utf8_lossy(&output.stderr);
                        if !stderr.is_empty() {
                            println!("└── {} {}", "Error:\n".red().bold(), stderr);
                        }
                        return;
                    }
                }
                Err(e) => {
                    sp.stop_with_message(format!("{}", "Failed to build Helix".red().bold()));
                    println!("└── {} {}", "Error:".red().bold(), e);
                    return;
                }
            }

            let mut sp = Spinner::new(Spinners::Dots9, "Starting Helix instance".into());

            let instance_manager = InstanceManager::new().unwrap();

            let binary_path = dirs::home_dir()
                .map(|path| path.join(".helix/repo/helix-db/target/release/helix-container"))
                .unwrap();

            let endpoints: Vec<String> = code
                .source
                .queries
                .iter()
                .map(|q| to_snake_case(&q.name))
                .collect();

            match instance_manager.init_start_instance(&binary_path, port, endpoints) {
                Ok(instance) => {
                    sp.stop_with_message(format!(
                        "{}",
                        "Successfully started Helix instance".green().bold()
                    ));
                    print_instnace(&instance);
                }
                Err(e) => {
                    sp.stop_with_message(format!(
                        "{}",
                        "Failed to start Helix instance".red().bold()
                    ));
                    println!("└── {} {}", "Error:".red().bold(), e);
                    return;
                }
            }
        }

        CommandType::Update(_) => {
            match check_helix_installation() {
                Ok(_) => {}
                Err(_) => {
                    println!("{}",
                        "Helix is not installed. Please run `helix install` first.".red().bold()
                    );
                    return;
                }
            };

            let repo_path = {
                let home_dir = match dirs::home_dir() {
                    Some(dir) => dir,
                    None => {
                        println!("{}", "Could not determine home directory".red().bold());
                        return;
                    }
                };
                home_dir.join(".helix/repo/helix-db/helixdb")
            };

            let local_cli_version = Version::parse(&format!("v{}", env!("CARGO_PKG_VERSION"))).unwrap();
            let local_db_version = Version::parse(&format!("v{}", get_crate_version(&repo_path).unwrap())).unwrap();
            let remote_helix_version = get_remote_helix_version().unwrap();
            println!("helix-cli version: {}, helix-db version: {}, remote helix version: {}",
                local_cli_version, local_db_version, remote_helix_version);

            if local_db_version < remote_helix_version
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
                            "Error while reseting installed helix-db version:".red().bold(),
                            e
                        );
                        return;
                    }
                }

                let mut runner = Command::new("git");
                runner.arg("pull");
                runner.current_dir(&repo_path);
                match runner.output() {
                    Ok(_) => println!("{}", "New helix-db version successfully pulled!".green().bold()),
                    Err(e) => {
                        println!(
                            "{} {}",
                            "Error while pulling new helix-db version:".red().bold(),
                            e
                        );
                        return;
                    }
                }


                match get_n_helix_cli() {
                    Ok(_) => println!("{}", "New helix-cli version successfully installed!".green().bold()),
                    Err(e) => {
                        println!(
                            "{} {}",
                            "Error while installing new helix-cli version:".red().bold(),
                            e
                        );
                        return;
                    }
                }
            } else {
                println!("{}", "HelixDB is up to date!".green().bold());
                return;
            }
        }

        CommandType::Version(_) => {
            match check_helix_installation() {
                Ok(_) => {}
                Err(_) => {
                    println!(
                        "{}",
                        "Helix is not installed. Please run `helix install` first."
                            .red()
                            .bold()
                    );
                    return;
                }
            };

            let repo_path = {
                let home_dir = match dirs::home_dir() {
                    Some(dir) => dir,
                    None => {
                        println!("{}", "Could not determine home directory".red().bold());
                        return;
                    }
                };
                home_dir.join(".helix/repo/helix-db/helixdb")
            };

            let local_cli_version = Version::parse(
                &format!("v{}", env!("CARGO_PKG_VERSION"))
            ).unwrap();
            let local_db_version = Version::parse(
                &format!("v{}", get_crate_version(repo_path).unwrap())
            ).unwrap();

            println!(
                "{} {}, {} {}",
                "helix-cli version:",
                local_cli_version,
                "helix-db version:",
                local_db_version
            );
        }

        CommandType::Redeploy(command) => {
            match Command::new("cargo").output() {
                Ok(_) => {}
                Err(_) => {
                    println!("{}", "Cargo is not installed".red().bold());
                    return;
                }
            }

            match check_helix_installation() {
                Ok(_) => {}
                Err(_) => {
                    println!(
                        "{}",
                        "Helix is not installed. Please run `helix install` first."
                            .red()
                            .bold()
                    );
                    return;
                }
            };

            let instance_manager = InstanceManager::new().unwrap();
            let iid = &command.instance;

            match instance_manager.get_instance(iid) {
                Ok(Some(_)) => println!("{}", "Helix instance found!".green().bold()),
                Ok(None) => {
                    println!(
                        "{} {}",
                        "No Helix instance found with id".red().bold(),
                        iid.red().bold()
                    );
                    return;
                }
                Err(e) => {
                    println!("{} {}", "Error:".red().bold(), e);
                    return;
                }
            };

            let path = get_cfg_deploy_path(command.path).unwrap();

            let output = dirs::home_dir()
                .map(|path| {
                    path.join(".helix/repo/helix-db/helix-container")
                        .to_string_lossy()
                        .into_owned()
                })
                .unwrap_or_else(|| "./.helix/repo/helix-db/helix-container".to_string());

            let files = match check_and_read_files(&path) {
                Ok(files) if !files.is_empty() => files,
                Ok(_) => {
                    println!("{}", "No queries found, nothing to compile".red().bold());
                    return;
                }
                Err(e) => {
                    println!("{} {}", "Error:".red().bold(), e);
                    return;
                }
            };

            let mut sp = Spinner::new(Spinners::Dots9, "Compiling Helix queries".into());

            let num_files = files.len();

            let (code, analyzed_source) = match generate(&files) {
                Ok(code) => code,
                Err(e) => {
                    sp.stop_with_message(format!("{}", "Error compiling queries".red().bold()));
                    println!("└── {}", e);
                    return;
                }
            };

            sp.stop_with_message(format!(
                "{} {} {}",
                "Successfully compiled".green().bold(),
                num_files,
                "query files".green().bold()
            ));

            let cache_dir = PathBuf::from(&output);
            fs::create_dir_all(&cache_dir).unwrap();

            let file_path = PathBuf::from(&output).join("src/queries.rs");
            let mut generated_rust_code = String::new();
            match write!(&mut generated_rust_code, "{}", analyzed_source) {
                Ok(_) => {}
                Err(e) => {
                    println!("{}", "Failed to write queries file".red().bold());
                    println!("└── {} {}", "Error:".red().bold(), e);
                    return;
                }
            }
            match fs::write(file_path, generated_rust_code) {
                Ok(_) => {
                    println!("{}", "Successfully wrote queries file".green().bold());
                }
                Err(e) => {
                    println!("{}", "Failed to write queries file".red().bold());
                    println!("└── {} {}", "Error:".red().bold(), e);
                    return;
                }
            }

            let mut sp = Spinner::new(Spinners::Dots9, "Building Helix".into());

            // copy config.hx.json to ~/.helix/repo/helix-db/helix-container/config.hx.json
            let config_path = PathBuf::from(&output).join("src/config.hx.json");
            fs::copy(PathBuf::from(path + "/config.hx.json"), config_path).unwrap();

            // check rust code
            let mut runner = Command::new("cargo");
            runner
                .arg("check")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .current_dir(PathBuf::from(&output));

            match runner.output() {
                Ok(_) => {}
                Err(e) => {
                    sp.stop_with_message(format!("{}", "Failed to check Rust code".red().bold()));
                    println!("└── {} {}", "Error:".red().bold(), e);
                    return;
                }
            }

            let mut runner = Command::new("cargo");
            runner
                .arg("build")
                .arg("--release")
                .current_dir(PathBuf::from(&output));

            match runner.output() {
                Ok(output) => {
                    if output.status.success() {
                        sp.stop_with_message(format!(
                            "{}",
                            "Successfully built Helix".green().bold()
                        ));
                    } else {
                        sp.stop_with_message(format!("{}", "Failed to build Helix".red().bold()));
                        let stderr = String::from_utf8_lossy(&output.stderr);
                        if !stderr.is_empty() {
                            println!("└── {} {}", "Error:\n".red().bold(), stderr);
                        }
                        return;
                    }
                }
                Err(e) => {
                    sp.stop_with_message(format!("{}", "Failed to build Helix".red().bold()));
                    println!("└── {} {}", "Error:".red().bold(), e);
                    return;
                }
            }

            match instance_manager.stop_instance(iid) {
                Ok(_) => {}
                Err(e) => {
                    println!("{} {}", "Error while stopping instance:".red().bold(), e);
                    return;
                }
            }

            let mut sp = Spinner::new(Spinners::Dots9, "Starting Helix instance".into());

            let binary_path = dirs::home_dir()
                .map(|path| path.join(".helix/repo/helix-db/target/release/helix-container"))
                .unwrap();

            let endpoints: Vec<String> = code
                .source
                .queries
                .iter()
                .map(|q| to_snake_case(&q.name))
                .collect();

            let cached_binary = instance_manager.cache_dir.join(&iid);
            match fs::copy(binary_path, &cached_binary) {
                Ok(_) => {}
                Err(e) => {
                    println!("{} {}", "Error while copying binary:".red().bold(), e);
                    return;
                }
            }

            match instance_manager.start_instance(iid, Some(endpoints)) {
                Ok(instance) => {
                    sp.stop_with_message(format!(
                        "{}",
                        "Successfully started Helix instance".green().bold()
                    ));
                    print_instnace(&instance);
                }
                Err(e) => {
                    sp.stop_with_message(format!(
                        "{}",
                        "Failed to start Helix instance".red().bold()
                    ));
                    println!("└── {} {}", "Error:".red().bold(), e);
                    return;
                }
            }
        }

        CommandType::Instances(_) => {
            let instance_manager = InstanceManager::new().unwrap();
            match instance_manager.list_instances() {
                Ok(instances) => {
                    if instances.is_empty() {
                        println!("No running Helix instances");
                        return;
                    }
                    for instance in instances {
                        print_instnace(&instance);
                        println!();
                    }
                }
                Err(e) => {
                    println!("{} {}", "Failed to list instances:".red().bold(), e);
                }
            }
        }

        CommandType::Stop(command) => {
            let instance_manager = InstanceManager::new().unwrap();
            match instance_manager.list_instances() {
                Ok(instances) => {
                    if !instance_manager.running_instances().unwrap() {
                        println!("{}", "No running Helix instances".bold());
                        return;
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
                    } else if let Some(instance_id) = command.instance {
                        match instance_manager.stop_instance(&instance_id) {
                            Ok(false) => {
                                println!(
                                    "{} {}",
                                    "Instance is not running".yellow().bold(),
                                    instance_id
                                )
                            }
                            Ok(true) => {
                                println!("{} {}", "Stopped instance".green().bold(), instance_id)
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
                            print_instnace(&instance);
                        }
                    }
                }
                Err(e) => {
                    println!("{} {}", "Failed to find instances:".red().bold(), e);
                }
            }
        }

        CommandType::Start(command) => {
            let instance_manager = InstanceManager::new().unwrap();
            let mut sp = Spinner::new(Spinners::Dots9, "Starting Helix instance".into());

            match instance_manager.start_instance(&command.instance, None) {
                Ok(instance) => {
                    sp.stop_with_message(format!(
                        "{}",
                        "Successfully started Helix instance".green().bold()
                    ));
                    print_instnace(&instance);
                }
                Err(e) => {
                    sp.stop_with_message(format!("{}", "Failed to start instance".red().bold()));
                    println!("└── {} {}", "Error:".red().bold(), e);
                }
            }
        }

        CommandType::Label(command) => {
            let instance_manager = InstanceManager::new().unwrap();
            let instance_id = command.instance;
            let label = command.label;
            match instance_manager.set_label(&instance_id, &label) {
                Ok(false) => {
                    println!("{} {}", "Instance not found".red().bold(), instance_id)
                }
                Ok(true) => {
                    println!("{} {}", "Labeled".green().bold(), instance_id)
                }
                Err(e) => println!("{} {}", "Failed to stop instance:".red().bold(), e),
            }
        }

        CommandType::Compile(command) => {
            let path = if let Some(p) = &command.path {
                p
            } else {
                println!(
                    "{} '{}'",
                    "No path provided, defaulting to".yellow().bold(),
                    DB_DIR.yellow().bold()
                );
                DB_DIR
            };

            let output = match &command.output {
                Some(output) => output.to_owned(),
                None => ".".to_string(),
            };

            let mut sp = Spinner::new(Spinners::Dots9, "Compiling Helix queries".into());
            let files = match check_and_read_files(&path) {
                Ok(files) => files,
                Err(e) => {
                    sp.stop_with_message(format!("{}", "Failed to read files".red().bold()));
                    println!("└── {}", e);
                    return;
                }
            };

            if files.is_empty() {
                sp.stop_with_message(format!(
                    "{}",
                    "No queries found, nothing to compile".red().bold()
                ));
                return;
            }

            let (_, analyzed_source) = match generate(&files) {
                Ok((code, analyzed_source)) => (code, analyzed_source),
                Err(e) => {
                    sp.stop_with_message(format!("{}", e.to_string().red().bold()));
                    return;
                }
            };

            if OutputLanguage::TypeScript == command.gen {
                match gen_typescript(&analyzed_source, &output) {
                    Ok(_) => {}
                    Err(e) => {
                        println!("{} {}", "Failed to write typescript types".red().bold(), e);
                        println!("└── {} {}", "Error:".red().bold(), e);
                        return;
                    }
                };
            }

            let file_path = PathBuf::from(&output).join("queries.rs");
            let mut generated_rust_code = String::new();
            match write!(&mut generated_rust_code, "{}", analyzed_source) {
                Ok(_) => {
                    println!("{}", "Successfully transpiled queries".green().bold());
                }
                Err(e) => {
                    println!("{}", "Failed to transpile queries".red().bold());
                    println!("└── {} {}", "Error:".red().bold(), e);
                    return;
                }
            }
            match fs::write(file_path, generated_rust_code) {
                Ok(_) => {
                    println!(
                        "{} {}",
                        "Successfully compiled queries to".green().bold(),
                        output
                    );
                }
                Err(e) => {
                    println!("{} {}", "Failed to write queries file".red().bold(), e);
                    println!("└── {} {}", "Error:".red().bold(), e);
                    return;
                }
            }
        }

        CommandType::Check(command) => {
            let path = if let Some(p) = &command.path {
                p
            } else {
                println!(
                    "{} '{}'",
                    "No path provided, defaulting to".yellow().bold(),
                    DB_DIR.yellow().bold()
                );
                DB_DIR
            };

            let mut sp = Spinner::new(Spinners::Dots9, "Checking Helix queries".into());

            let files = match check_and_read_files(&path) {
                Ok(files) => files,
                Err(e) => {
                    sp.stop_with_message(format!("{}", "Error checking files".red().bold()));
                    println!("└── {}", e);
                    return;
                }
            };

            if files.is_empty() {
                sp.stop_with_message(format!(
                    "{}",
                    "No queries found, nothing to compile".red().bold()
                ));
                return;
            }

            match generate(&files) {
                Ok(_) => {}
                Err(e) => {
                    sp.stop_with_message(format!("{}", "Failed to generate queries".red().bold()));
                    println!("└── {}", e);
                    return;
                }
            }

            sp.stop_with_message(format!(
                "{}",
                "Helix-QL schema and queries validated successfully with zero errors"
                    .green()
                    .bold()
            ));
        }

        CommandType::Install(_) => {
            match Command::new("cargo").output() {
                Ok(_) => {}
                Err(_) => {
                    println!("{}", "Cargo is not installed".red().bold());
                    return;
                }
            }

            match Command::new("git").arg("version").output() {
                Ok(_) => {}
                Err(_) => {
                    println!("{}", "Git is not installed".red().bold());
                    return;
                }
            }

            let repo_path = {
                // check if helix repo exists
                let home_dir = match dirs::home_dir() {
                    Some(dir) => dir,
                    None => {
                        println!("{}", "Could not determine home directory".red().bold());
                        return;
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
                return;
            }

            // Create the directory structure if it doesn't exist
            match fs::create_dir_all(&repo_path) {
                Ok(_) => println!(
                    "{} {}",
                    "Created directory structure at".green().bold(),
                    repo_path.display()
                ),
                Err(e) => {
                    println!("{}", "Failed to create directory structure".red().bold());
                    println!("|");
                    println!("└── {}", e);
                    return;
                }
            }

            let mut runner = Command::new("git");
            runner.arg("clone");
            runner.arg("https://github.com/HelixDB/helix-db.git");
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
                    println!("└── Then run `helix check --path <path-to-project>` to check your queries.");
                    println!("|");
                    println!("└── Then run `helix deploy --path <path-to-project>` to build your queries.");
                }
                Err(e) => {
                    println!("{}", "Failed to install Helix repo".red().bold());
                    println!("|");
                    println!("└── {}", e);
                    return;
                }
            }
        }

        CommandType::Test(_command) => {
            unimplemented!("helix test coming soon!");
        }

        CommandType::Init(command) => {
            println!("Initialising Helix project...");
            let path = match command.path {
                Some(path) => PathBuf::from(path),
                None => PathBuf::from(DB_DIR),
            };
            let path_str = path.to_str().unwrap();

            match check_and_read_files(path_str) {
                Ok(files) if !files.is_empty() => {
                    println!(
                        "{} {}",
                        "Queries already exist in".yellow().bold(),
                        path_str
                    );
                    return;
                }
                Ok(_) => {}
                Err(_) => {}
            };

            fs::create_dir_all(&path).unwrap();

            let schema_path = path.join("schema.hx");
            fs::write(&schema_path, DEFAULT_SCHEMA).unwrap();

            let main_path = path.join("queries.hx");
            fs::write(main_path, DEFAULT_QUERIES).unwrap();

            let config_path = path.join("config.hx.json");
            fs::write(config_path, Config::init_config()).unwrap();

            println!(
                "{} {}",
                "Helix project initialised at".green().bold(),
                path.display()
            );
        }

        CommandType::Save(command) => {
            let instance_manager = InstanceManager::new().unwrap();
            let iid = &command.instance;

            match instance_manager.get_instance(iid) {
                Ok(Some(_)) => println!("{}", "Helix instance found!".green().bold()),
                Ok(None) => {
                    println!(
                        "{} {}",
                        "No Helix instance found with id".red().bold(),
                        iid.red().bold()
                    );
                    return;
                }
                Err(e) => {
                    println!("{} {}", "Error:".red().bold(), e);
                    return;
                }
            }

            let output_path = match command.output {
                Some(output) => format!("{}helix_instance_{}", output, iid),
                None => format!("helix_instance_{}", iid),
            };
            let home_dir = std::env::var("HOME").expect("Failed to get HOME environment variable");
            let instance_path = format!("{}/.helix/cached_builds/data/{}/user", home_dir, iid);

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
            let iid = &command.instance;

            match instance_manager.get_instance(iid) {
                Ok(Some(_)) => println!("{}", "Helix instance found!".green().bold()),
                Ok(None) => {
                    println!(
                        "{} {}",
                        "No Helix instance found with id".red().bold(),
                        iid.red().bold()
                    );
                    return;
                }
                Err(e) => {
                    println!("{} {}", "Error:".red().bold(), e);
                    return;
                }
            }

            match instance_manager.stop_instance(iid) {
                Ok(true) => println!(
                    "{} {}",
                    "Stopped instance".green().bold(),
                    iid.green().bold()
                ),
                Ok(false) => {}
                Err(e) => println!("{} {}", "Error while stopping instance".red().bold(), e),
            }

            print!("Are you sure you want to delete the instance and its data? (y/n): ");
            std::io::stdout().flush().unwrap();
            let mut input = String::new();
            std::io::stdin().read_line(&mut input).unwrap();
            let del_prompt = input.trim().to_lowercase() == "y";

            if del_prompt {
                match instance_manager.delete_instance(iid) {
                    Ok(_) => println!("{}", "Deleted Helix instance".green().bold()),
                    Err(e) => println!("{} {}", "Error while deleting instance".red().bold(), e),
                }

                let home_dir =
                    std::env::var("HOME").expect("Failed to get HOME environment variable");
                let instance_path = format!("{}/.helix/cached_builds/data/{}", home_dir, iid);
                let binary_path = format!("{}/.helix/cached_builds/{}", home_dir, iid);
                let log_path = format!("{}/.helix/logs/instance_{}.log", home_dir, iid);
                let error_log_path = format!("{}/.helix/logs/instance_{}_error.log", home_dir, iid);

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

        CommandType::Ingest(command) => {
            match command.db_type.as_str() {
                "sqlite" => {
                    let path_str = command.db_url; // Database path for SQLite
                    let instance = command.instance;

                    let path = Path::new(&path_str);
                    if !path.exists() {
                        println!(
                            "{} '{}' {}",
                            "The file".red().bold(),
                            path.display().to_string().red().bold(),
                            "does not exist".red().bold()
                        );
                        return;
                    }

                    let valid_extensions = [".sqlite", ".db", ".sqlite3"];
                    let is_valid_extension = path
                        .extension()
                        .and_then(|ext| ext.to_str())
                        .map(|ext| valid_extensions.iter().any(|&valid_ext| valid_ext == ext))
                        .unwrap_or(false);

                    if !is_valid_extension {
                        println!(
                            "{} '{}' {}",
                            "The file".red().bold(),
                            path.display().to_string().red().bold(),
                            "must have a .sqlite, .db, or .sqlite3 file extension"
                                .red()
                                .bold(),
                        );
                        return;
                    }

                    let instance_manager = InstanceManager::new().unwrap();
                    match instance_manager.list_instances() {
                        Ok(instances) => {
                            if instances.is_empty() {
                                println!(
                                    "{}",
                                    "There are no running Helix instances!".red().bold()
                                );
                                return;
                            }
                            let mut is_valid_instance = false;
                            for iter_instance in instances {
                                if iter_instance.id == instance {
                                    is_valid_instance = true;
                                    break;
                                }
                            }
                            if !is_valid_instance {
                                println!("No Helix instance found with id: '{}'!", instance);
                                return;
                            } else {
                                println!("Helix instance found with id: '{}'!", instance);
                            }
                        }
                        Err(e) => {
                            println!("Error while searching for Helix instances: {}", e);
                        }
                    }

                    // TODO: Add ingestion logic
                    // SqliteIngestor::new(&path_str, None, 5).unwrap();
                }
                "pg" | "postgres" => {
                    let mut sp = Spinner::new(
                        Spinners::Dots9,
                        "Connecting to PostgreSQL database...".into(),
                    );
                    // Create output directory if specified
                    let output_dir = command.output_dir.as_deref().unwrap_or("./");
                    if !Path::new(output_dir).exists() {
                        fs::create_dir_all(output_dir).unwrap_or_else(|e| {
                            sp.stop_with_message(format!(
                                "{}",
                                "Failed to create output directory".red().bold()
                            ));
                            println!("└── {}", e);
                            return;
                        });
                    }

                    // Run the PostgreSQL ingestion
                    let rt = tokio::runtime::Runtime::new().unwrap();
                    rt.block_on(async {
                        let ingestor = PostgresIngestor::new(&command.db_url, Some(command.instance.clone()), command.batch_size, command.use_ssl).await;

                        match ingestor {
                            Ok(mut ingestor) => {
                                sp.stop_with_message(format!("{}", "Connected to PostgreSQL database".red().bold()));

                                let mut sp = Spinner::new(Spinners::Dots9, "Dumping data to JSONL files".into());
                                match ingestor.dump_to_json(output_dir).await {
                                    Ok(_) => {
                                        sp.stop_with_message(format!("{}", "Successfully dumped data to JSONL files".red().bold()));

                                        // Create schema file
                                        let schema_path = Path::new(output_dir).join("schema.hx");
                                        println!("Schema file created at: {}", schema_path.display());

                                        println!("PostgreSQL ingestion completed successfully!");
                                        println!("Press ENTER to open the Helix dashboard in your browser...");
                                        let mut input = String::new();
                                        std::io::stdin().read_line(&mut input).unwrap();

                                        #[cfg(target_os = "macos")]
                                        {
                                            if let Err(_) = std::process::Command::new("open")
                                                .arg("https://helix-db.com/dashboard")
                                                .spawn()
                                            {
                                                println!("Failed to open url");
                                                println!("Please visit https://helix-db.com/dashboard"); // TODO: this is outdated
                                            }
                                        }

                                        #[cfg(not(target_os = "macos"))]
                                        {
                                            if let Err(e) = open::that("https://helix-db.com/dashboard") {
                                                println!("Failed to open url");
                                                println!("Please visit https://helix-db.com/dashboard");
                                            }
                                        }
                                    },
                                    Err(e) => {
                                        sp.stop_with_message(format!("{}", "Failed to dump data".red().bold()));
                                        println!("└── {}", e);
                                        return;
                                    }
                                }
                            },
                            Err(e) => {
                                sp.stop_with_message(format!("{}", "Failed to connect to PostgreSQL".red().bold()));
                                println!("└── {}", e);
                                return;
                            }
                        }
                    });
                }
                _ => {
                    println!(
                        "{}",
                        "Invalid database type. Must be either 'sqlite' or 'pg/postgres'"
                            .red()
                            .bold()
                    );
                    return;
                }
            }
        }
    }
}
