use crate::{
    args::{
        HelixCLI,
        CommandType,
    },
    instance_manager::InstanceManager,
    utils::*,
};
use clap::Parser;
use colored::*;
use helixdb::{
    helix_engine::graph_core::config::Config,
    ingestion_engine::{postgres_ingestion::PostgresIngestor, sql_ingestion::SqliteIngestor},
};
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use spinners::{Spinner, Spinners};

pub mod args;
mod instance_manager;
mod utils;

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

            let local = command.local;

            if !local {
                println!("{}", "Building for remote instance is not supported yet, use --local flag to build for local machine".yellow().bold());
                println!("└── Example: helix deploy --local");
                return;
            }

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

            let code = match generate(&files) {
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

            // if local overwrite queries file in ~/.helix/repo/helix-container/src/queries.rs
            if local {
                let file_path = PathBuf::from(&output).join("src/queries.rs");
                match fs::write(file_path, code.content) {
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
                        sp.stop_with_message(format!(
                            "{}",
                            "Failed to check Rust code".red().bold()
                        ));
                        println!("└── {} {}", "Error:".red().bold(), e);
                        return;
                    }
                }

                let mut runner = Command::new("cargo");
                runner
                    .arg("build")
                    .arg("--release")
                    .current_dir(PathBuf::from(&output)) // TODO: build only in helix-container/ dir
                    .env("RUSTFLAGS", "-Awarnings");

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

                match instance_manager.start_instance(&binary_path, port, endpoints) {
                    Ok(instance) => {
                        sp.stop_with_message(format!(
                            "{}",
                            "Successfully started Helix instance".green().bold()
                        ));
                        println!("└── Instance ID: {}", instance.id);
                        println!("└── Port: {}", instance.port);
                        println!("└── Available endpoints:");
                        for endpoint in instance.available_endpoints {
                            println!("    └── /{}", endpoint);
                        }
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
        }

        CommandType::Instances(_) => {
            let instance_manager = InstanceManager::new().unwrap();
            match instance_manager.list_instances() {
                Ok(instances) => {
                    if instances.is_empty() {
                        println!("No running Helix instances");
                        return;
                    }
                    println!("{}", "Running Helix instances".green().bold());
                    for instance in instances {
                        println!("└── Instance ID: {}", instance.id);
                        println!("└── Port: {}", instance.port);
                        println!("└── Available endpoints:");
                        for endpoint in instance.available_endpoints {
                            println!("    └── /{}", endpoint);
                        }
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
                    if instances.is_empty() {
                        println!("No running Helix instances");
                        return;
                    }
                    if command.all {
                        match instance_manager.stop_all_instances() {
                            Ok(_) => {
                                println!("{}", "Stopping all Helix instances".green().bold());
                                for instance in instances {
                                    println!("└── {} {}", "ID:".green().bold(), instance.id);
                                }
                            }
                            Err(e) => {
                                println!("{} {}", "Failed to stop instances:".red().bold(), e)
                            }
                        }
                    } else if let Some(instance_id) = command.instance_id {
                        match instance_manager.stop_instance(&instance_id) {
                            Ok(_) => {
                                println!("{} {}", "Stopped instance".green().bold(), instance_id)
                            }
                            Err(e) => println!("{} {}", "Failed to stop instance:".red().bold(), e),
                        }
                    } else {
                        println!("{}", "Please specify --all or provide an instance ID".yellow().bold());
                        println!("Running instances: ");
                        for instance in instances {
                            println!("└── {} {}", "ID:".green().bold(), instance.id);
                        }
                    }
                }
                Err(e) => {
                    println!("{} {}", "Failed to find instances:".red().bold(), e);
                }
            }
        }

        CommandType::Start(_command) => {
            unimplemented!("helix start has not been implemented yet!");
            /*
            let instance_manager = InstanceManager::new().unwrap();
            let mut sp = Spinner::new(Spinners::Dots9, "Starting Helix instance".into());

            match instance_manager.restart_instance(&command.instance_id) {
                Ok(Some(instance)) => {
                    sp.stop_with_message(format!(
                        "{}",
                        "Successfully restarted Helix instance".green().bold()
                    ));
                    println!("└── Instance ID: {}", instance.id);
                    println!("└── Port: {}", instance.port);
                    println!("└── Available endpoints:");
                    for endpoint in instance.available_endpoints {
                        println!("    └── /{}", endpoint);
                    }
                }
                Ok(None) => {
                    sp.stop_with_message(format!(
                        "{}",
                        "Instance not found or binary missing".red().bold()
                    ));
                    println!(
                        "└── Could not find instance with ID: {}",
                        command.instance_id
                    );
                }
                Err(e) => {
                    sp.stop_with_message(format!("{}", "Failed to restart instance".red().bold()));
                    println!("└── {} {}", "Error:".red().bold(), e);
                }
            }
            */
        }

        CommandType::Compile(command) => {
            let path = if let Some(p) = &command.path {
                p
            } else {
                println!("{} '{}'", "No path provided, defaulting to".yellow().bold(), DB_DIR.yellow().bold());
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
                sp.stop_with_message(format!("{}", "No queries found, nothing to compile".red().bold()));
                return;
            }

            let code = match generate(&files) {
                Ok(code) => code,
                Err(e) => {
                    sp.stop_with_message(format!("{}", e.to_string().red().bold()));
                    return;
                }
            };

            // write source to file
            let file_path = PathBuf::from(&output).join("queries.rs");
            fs::write(file_path, code.content).unwrap();
            sp.stop_with_message(format!(
                "{} {}",
                "Successfully compiled queries to".green().bold(),
                output
            ));
        }

        CommandType::Check(command) => {
            let path = if let Some(p) = &command.path {
                p
            } else {
                println!("{} '{}'", "No path provided, defaulting to".yellow().bold(), DB_DIR.yellow().bold());
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
                sp.stop_with_message(format!("{}", "No queries found, nothing to compile".red().bold()));
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

            sp.stop_with_message(
                format!(
                    "{}",
                    "Helix-QL schema and queries validated successfully with zero errors".green().bold()
                )
            );
        }

        CommandType::Install(command) => {
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

            let repo_path = match command.path {
                Some(path) => {
                    let path = PathBuf::from(path);
                    if !path.is_dir() {
                        println!("{}", "Path is not a directory".red().bold());
                        return;
                    }
                    if !path.exists() {
                        println!("{}", "Path does not exist".red().bold());
                        return;
                    }
                    path
                }
                None => {
                    // check if helix repo exists
                    let home_dir = match dirs::home_dir() {
                        Some(dir) => dir,
                        None => {
                            println!("{}", "Could not determine home directory".red().bold());
                            return;
                        }
                    };
                    home_dir.join(".helix/repo")
                }
            };

            if repo_path.clone().join("helix-db").exists()
                && repo_path.clone().join("helix-db").is_dir()
            {
                println!(
                    "{} {}",
                    "Helix repo already exists at".yellow().bold(),
                    repo_path.join("helix-db").display().to_string().yellow().bold(),
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
                    println!("└── Then run `helix deploy --path <path-to-project> --local` to build your queries.");
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

            let _ = match check_and_read_files(path_str) {
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
                            "must have a .sqlite, .db, or .sqlite3 file extension".red().bold(),
                        );
                        return;
                    }

                    let instance_manager = InstanceManager::new().unwrap();
                    match instance_manager.list_instances() {
                        Ok(instances) => {
                            if instances.is_empty() {
                                println!("{}", "There are no running Helix instances!".red().bold());
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

                    let ingestor = SqliteIngestor::new(&path_str, None, 5).unwrap();
                    // TODO: Add ingestion logic
                },
                "pg" | "postgres" => {
                    let mut sp = Spinner::new(Spinners::Dots9, "Connecting to PostgreSQL database...".into());
                    // Create output directory if specified
                    let output_dir = command.output_dir.as_deref().unwrap_or("./");
                    if !Path::new(output_dir).exists() {
                        fs::create_dir_all(output_dir).unwrap_or_else(|e| {
                            sp.stop_with_message(format!("{}", "Failed to create output directory".red().bold()));
                            println!("└── {}", e);
                            return;
                        });
                    }

                    // Run the PostgreSQL ingestion
                    let rt = tokio::runtime::Runtime::new().unwrap();
                    let result = rt.block_on(async {
                        let mut ingestor = PostgresIngestor::new(&command.db_url, Some(command.instance.clone()), command.batch_size, command.use_ssl).await;

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
                                            if let Err(e) = std::process::Command::new("open")
                                                .arg("https://helix-db.com/dashboard")
                                                .spawn()
                                            {
                                                println!("Failed to open url");
                                                println!("Please visit https://helix-db.com/dashboard");
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
                    println!("{}", "Invalid database type. Must be either 'sqlite' or 'pg/postgres'".red().bold());
                    return;
                }
            }
        }
    }
}
