use crate::{
    HELIX_METRICS_CLIENT,
    instance_manager::{InstanceInfo, InstanceManager},
    types::*,
};
use futures_util::StreamExt;
use helix_db::{
    helix_engine::traversal_core::config::Config,
    helixc::{
        analyzer::analyze,
        generator::{Source as GeneratedSource, tsdisplay::ToTypeScript},
        parser::{
            HelixParser,
            types::{Content, HxFile, Source},
        },
    },
    utils::styled_string::StyledString,
};
use helix_metrics::events::{DeployCloudEvent, EventType};
use reqwest::Client;
use serde::Deserialize;
use sonic_rs::{JsonValueTrait, Value as JsonValue, json};
use spinners::{Spinner, Spinners};
use std::{
    env,
    error::Error,
    fmt::Write,
    fs::{self, DirEntry, File},
    io::{self, ErrorKind, Write as iWrite},
    net::{SocketAddr, TcpListener},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::LazyLock,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        Message,
        protocol::{CloseFrame, frame::coding::CloseCode},
    },
};
use toml::Value;

const DEFAULT_CLOUD_AUTHORITY: &str = "ec2-184-72-27-116.us-west-1.compute.amazonaws.com:3000";

pub static CLOUD_AUTHORITY: LazyLock<String> = LazyLock::new(|| {
    env::var("HELIX_CLOUD_AUTHORITY").unwrap_or(DEFAULT_CLOUD_AUTHORITY.to_string())
});

pub const DEFAULT_SCHEMA: &str = r#"// Start building your schema here.
//
// The schema is used to to ensure a level of type safety in your queries.
//
// The schema is made up of Node types, denoted by N::,
// and Edge types, denoted by E::
//
// Under the Node types you can define fields that
// will be stored in the database.
//
// Under the Edge types you can define what type of node
// the edge will connect to and from, and also the
// properties that you want to store on the edge.
//
// Example:
//
// N::User {
//     Name: String,
//     Age: I32,
//     IsAdmin: Boolean,
// }
//
// E::Knows {
//     From: User,
//     To: User,
//     Properties: {
//         Since: Date,
//     }
// }
//
// For more information on how to write queries,
// see the documentation at https://docs.helix-db.com
// or checkout our GitHub at https://github.com/HelixDB/helix-db
"#;

pub const DEFAULT_QUERIES: &str = r#"// Start writing your queries here.
//
// You can use the schema to help you write your queries.
//
// Queries take the form:
//     QUERY {query name}({input name}: {input type}) =>
//         {variable} <- {traversal}
//         RETURN {variable}
//
// Example:
//     QUERY GetUserFriends(user_id: ID) =>
//         friends <- N<User>(user_id)::Out<Knows>
//         RETURN friends
//
//
// For more information on how to write queries,
// see the documentation at https://docs.helix-db.com
// or checkout our GitHub at https://github.com/HelixDB/helix-db
"#;

pub fn check_helix_installation() -> Option<PathBuf> {
    let home_dir = dirs::home_dir()?;
    let repo_path = home_dir.join(".helix/repo/helix-db");
    let container_path = repo_path.join("helix-container");
    let cargo_path = container_path.join("Cargo.toml");

    if !repo_path.exists()
        || !repo_path.is_dir()
        || !container_path.exists()
        || !container_path.is_dir()
        || !cargo_path.exists()
    {
        return None;
    }

    Some(container_path)
}

pub fn print_instance(mut instance: InstanceInfo) {
    let rg: bool = instance.running;
    println!(
        "{} {} {}{}",
        if rg {
            format!(
                "{}{}{}",
                "(".green().bold(),
                instance.short_id.to_string().green().bold(),
                ")".green().bold(),
            )
        } else {
            format!(
                "{}{}{}",
                "(".yellow().bold(),
                instance.short_id.to_string().green().bold(),
                ")".yellow().bold(),
            )
        },
        if rg {
            "Instance ID:".green().bold()
        } else {
            "Instance ID:".yellow().bold()
        },
        if rg {
            instance.id.green().bold()
        } else {
            instance.id.yellow().bold()
        },
        if rg {
            " (running)".to_string().green().bold()
        } else {
            " (not running)".to_string().yellow().bold()
        },
    );

    println!(
        "└── Short ID: {}",
        instance.short_id.to_string().underline()
    );
    println!("└── Port: {}", instance.port);
    println!("└── Available endpoints:");

    instance.available_endpoints.sort();
    instance
        .available_endpoints
        .iter()
        .for_each(|ep| println!("    └── /{ep}"));
}

pub fn get_cli_version() -> Result<Version, String> {
    Version::parse(&format!("v{}", env!("CARGO_PKG_VERSION")))
}

pub fn get_crate_version<P: AsRef<Path>>(path: P) -> Result<Version, String> {
    let cargo_toml_path = path.as_ref().join("Cargo.toml");
    if !cargo_toml_path.exists() {
        return Err("Not a Rust crate: Cargo.toml not found".to_string());
    }

    let contents = fs::read_to_string(&cargo_toml_path)
        .map_err(|e| format!("Failed to read Cargo.toml: {e}"))?;

    let parsed_toml = contents
        .parse::<Value>()
        .map_err(|e| format!("Failed to parse Cargo.toml: {e}"))?;

    let version = parsed_toml
        .get("package")
        .and_then(|pkg| pkg.get("version"))
        .and_then(|v| v.as_str())
        .ok_or("Version field not found in [package] section")?;

    let vers = Version::parse(version)?;
    Ok(vers)
}

pub async fn get_remote_helix_version() -> Result<Version, Box<dyn Error>> {
    let client = Client::new();

    let url = "https://api.github.com/repos/HelixDB/helix-db/releases/latest";

    // if response is not returned within timeout just return.
    // At the moment this is stopping cli be used offline
    let response = client
        .get(url)
        .header("User-Agent", "rust")
        .header("Accept", "application/vnd.github+json")
        .send()
        .await?
        .text()
        .await?;

    let json: JsonValue = sonic_rs::from_str(&response)?;
    let tag_name = json
        .get("tag_name")
        .and_then(|v| v.as_str())
        .ok_or("Failed to find tag_name in response")?
        .to_string();

    Ok(Version::parse(&tag_name)?)
}

pub async fn github_login() -> Result<(String, String), Box<dyn Error>> {
    let url = format!("ws://{}/login", *CLOUD_AUTHORITY);
    let (mut ws_stream, _) = connect_async(url).await?;

    let init_msg: UserCodeMsg = match ws_stream.next().await {
        Some(Ok(Message::Text(payload))) => sonic_rs::from_str(&payload)?,
        Some(Ok(m)) => return Err(format!("Unexpected message: {m:?}").into()),
        Some(Err(e)) => return Err(e.into()),
        None => return Err("Connection Closed Unexpectedly".into()),
    };

    println!(
        "To Login please go \x1b]8;;{}\x1b\\here\x1b]8;;\x1b\\({}),\nand enter the code: {}",
        init_msg.verification_uri,
        init_msg.verification_uri,
        init_msg.user_code.bold()
    );

    let msg: ApiKeyMsg = match ws_stream.next().await {
        Some(Ok(Message::Text(payload))) => sonic_rs::from_str(&payload)?,
        Some(Ok(Message::Close(Some(CloseFrame {
            code: CloseCode::Error,
            reason,
        })))) => return Err(format!("Error: {reason}").into()),
        Some(Ok(m)) => return Err(format!("Unexpected message: {m:?}").into()),
        Some(Err(e)) => return Err(e.into()),
        None => return Err("Connection Closed Unexpectedly".into()),
    };

    Ok((msg.key, msg.user_id))
}

#[derive(Deserialize)]
struct UserCodeMsg {
    user_code: String,
    verification_uri: String,
}

#[derive(Deserialize)]
struct ApiKeyMsg {
    user_id: String,
    key: String,
}

pub struct Credentials {
    pub key: Option<String>,
    pub user_id: Option<String>,
    pub metrics: Option<bool>,
}

pub fn parse_credentials(creds: &str) -> Credentials {
    let mut credentials = Credentials {
        key: None,
        user_id: None,
        metrics: None,
    };

    for line in creds.lines() {
        if let Some((key, value)) = line.split_once("=") {
            match key.to_lowercase().as_str() {
                "helix_user_key" => credentials.key = Some(value.to_string()),
                "helix_user_id" => credentials.user_id = Some(value.to_string()),
                "metrics" => credentials.metrics = Some(value.to_string().parse().unwrap()),
                _ => {}
            }
        }
    }

    credentials
}

/// tries to parse a credential file, returning the key, if any
pub fn parse_key_from_creds(creds: &str) -> Option<&str> {
    for line in creds.lines() {
        if let Some((key, value)) = line.split_once("=")
            && key.to_lowercase() == "helix_user_key"
        {
            return Some(value);
        }
    }
    None
}

pub async fn check_helix_version() {
    match check_helix_installation() {
        Some(_) => {}
        None => return,
    }

    let repo_path = {
        let home_dir = match dirs::home_dir() {
            Some(dir) => dir,
            None => return,
        };
        home_dir.join(".helix/repo/helix-db/helix-db")
    };

    let local_cli_version = match Version::parse(&format!("v{}", env!("CARGO_PKG_VERSION"))) {
        Ok(value) => value,
        Err(_) => return,
    };

    let crate_version = match get_crate_version(&repo_path) {
        Ok(value) => value,
        Err(_) => return,
    };

    let local_db_version = match Version::parse(&format!("v{crate_version}")) {
        Ok(value) => value,
        Err(_) => return,
    };

    let remote_helix_version = match get_remote_helix_version().await {
        Ok(value) => value,
        Err(_) => return,
    };

    if local_db_version < remote_helix_version || local_cli_version < remote_helix_version {
        println!(
            "{} {} {} {}",
            "New HelixDB version is available!".yellow().bold(),
            "Run".yellow().bold(),
            "helix update".white().bold(),
            "to install the newest version!".yellow().bold(),
        );
    }
}

pub fn check_cargo_version() -> bool {
    match Command::new("cargo").arg("--version").output() {
        Ok(output) => {
            let version_str = String::from_utf8_lossy(&output.stdout);
            let version = version_str
                .split_whitespace()
                .nth(1)
                .unwrap_or("0.0.0")
                .split('-')
                .next()
                .unwrap_or("0.0.0");
            let parts: Vec<u32> = version.split('.').filter_map(|s| s.parse().ok()).collect();
            if parts.len() >= 2 {
                parts[0] >= 1 && parts[1] >= 88
            } else {
                false
            }
        }
        Err(_) => false,
    }
}

pub fn get_n_helix_cli() -> Result<(), Box<dyn Error>> {
    // TODO: running this through rust doesn't identify GLIBC so has to compile from source
    let status = Command::new("sh")
        .arg("-c")
        .arg("curl -sSL 'https://install.helix-db.com' | bash")
        .env(
            "PATH",
            format!(
                "{}:{}",
                std::env::var("HOME")
                    .map(|h| format!("{h}/.cargo/bin"))
                    .unwrap_or_default(),
                std::env::var("PATH").unwrap_or_default()
            ),
        )
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()?;

    if !status.success() {
        return Err(format!("Command failed with status: {status}").into());
    }

    Ok(())
}

/// Returns the path or the current working directory if no path is provided
pub fn get_path_or_cwd(path: Option<&String>) -> Result<String, Box<dyn Error>> {
    match path {
        Some(p) => Ok(p.to_string()),
        None => {
            let cwd = env::current_dir()?;
            cwd.to_str()
                .map(|s| s.to_string())
                .ok_or_else(|| format!("Path contains invalid UTF-8 characters: {cwd:?}").into())
        }
    }
}

/// Checks if the path contains a schema.hx and config.hx.json file
/// Returns a vector of DirEntry objects for all .hx files in the path
pub fn check_and_read_files(path: &str) -> Result<Vec<DirEntry>, String> {
    if !fs::read_dir(path)
        .map_err(|e| format!("IO Error: {e}"))?
        .any(|file| file.ok().is_some_and(|f| f.file_name() == "schema.hx"))
    {
        return Err("No schema file found".to_string());
    }

    if !fs::read_dir(path)
        .map_err(|e| format!("IO Error: {e}"))?
        .any(|file| file.ok().is_some_and(|f| f.file_name() == "config.hx.json"))
    {
        return Err("No config.hx.json file found".to_string());
    }

    let files: Vec<DirEntry> = fs::read_dir(path)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|file| file.file_name().to_string_lossy().ends_with(".hx"))
        .collect();

    let has_queries = files.iter().any(|file| file.file_name() != "schema.hx");
    if !has_queries {
        return Err("No query files (.hx) found".to_string());
    }

    Ok(files)
}

/// Generates a Content object from a vector of DirEntry objects
/// Returns a Content object with the files and source
///
/// This essentially makes a full string of all of the files while having a separate vector of the individual files
///
/// This could be changed in the future but keeps the option open for being able to access the files separately or all at once
pub fn generate_content(files: &[DirEntry]) -> Result<Content, String> {
    let files: Vec<HxFile> = files
        .iter()
        .map(|file| {
            let name = file.path().to_string_lossy().into_owned();
            let content = fs::read_to_string(file.path()).unwrap();
            HxFile { name, content }
        })
        .collect();

    let content = files
        .clone()
        .iter()
        .map(|file| file.content.clone())
        .collect::<Vec<String>>()
        .join("\n");

    Ok(Content {
        content,
        files,
        source: Source::default(),
    })
}

/// Uses the helix parser to parse the content into a Source object
fn parse_content(content: &Content) -> Result<Source, String> {
    let source = match HelixParser::parse_source(content) {
        Ok(source) => source,
        Err(e) => {
            return Err(e.to_string());
        }
    };

    Ok(source)
}

/// Runs the static analyzer on the parsed source to catch errors and generate diagnostics if any.
/// Otherwise returns the generated source object which is an IR used to transpile the queries to rust.
fn analyze_source(source: Source) -> Result<GeneratedSource, String> {
    if source.schema.is_empty() {
        return Err("No schema definitions provided".to_string());
    }

    let (diagnostics, source) = analyze(&source);
    if !diagnostics.is_empty() {
        for diag in diagnostics {
            let filepath = diag.filepath.clone().unwrap_or("queries.hx".to_string());
            println!("{}", diag.render(&source.src, &filepath));
        }
        return Err("compilation failed!".to_string());
    }

    Ok(source)
}

pub fn generate(files: &[DirEntry], path: &str) -> Result<(Content, GeneratedSource), String> {
    let mut content = generate_content(files)?;
    content.source = parse_content(&content)?;
    let mut analyzed_source = analyze_source(content.source.clone())?;
    analyzed_source.config = read_config(path)?;
    Ok((content, analyzed_source))
}

pub fn read_config(path: &str) -> Result<Config, String> {
    let config_path = PathBuf::from(path).join("config.hx.json");
    let schema_path = PathBuf::from(path).join("schema.hx");
    let config = Config::from_files(config_path, schema_path).map_err(|e| e.to_string())?;
    Ok(config)
}

pub fn gen_typescript(source: &GeneratedSource, output_path: &str) -> Result<(), String> {
    let mut file = match File::create(PathBuf::from(output_path).join("interface.d.ts")) {
        Ok(file) => file,
        Err(e) => return Err(e.to_string()),
    };

    for node in &source.nodes {
        match write!(file, "{}", node.to_typescript()) {
            Ok(_) => {}
            Err(e) => return Err(e.to_string()),
        }
    }
    for edge in &source.edges {
        match write!(file, "{}", edge.to_typescript()) {
            Ok(_) => {}
            Err(e) => return Err(e.to_string()),
        }
    }
    for vector in &source.vectors {
        match write!(file, "{}", vector.to_typescript()) {
            Ok(_) => {}
            Err(e) => return Err(e.to_string()),
        }
    }

    Ok(())
}

pub fn find_available_port(start_port: u16) -> Option<u16> {
    let mut port = start_port;
    while port < 65535 {
        let addr = format!("0.0.0.0:{port}").parse::<SocketAddr>().unwrap();
        match TcpListener::bind(addr) {
            Ok(listener) => {
                drop(listener);
                let localhost = format!("127.0.0.1:{port}").parse::<SocketAddr>().unwrap();
                match TcpListener::bind(localhost) {
                    Ok(local_listener) => {
                        drop(local_listener);
                        return Some(port);
                    }
                    Err(e) => {
                        if e.kind() != ErrorKind::AddrInUse {
                            return None;
                        }
                        port += 1;
                        continue;
                    }
                }
            }
            Err(e) => {
                if e.kind() != ErrorKind::AddrInUse {
                    return None;
                }
                port += 1;
                continue;
            }
        }
    }
    None
}

pub fn compile_and_build_helix(
    path: String,
    output: &PathBuf,
    files: Vec<DirEntry>,
    release_mode: BuildMode,
    enable_dev_instance: bool,
) -> Result<Content, String> {
    let mut sp = Spinner::new(Spinners::Dots9, "Compiling Helix queries".into());

    let num_files = files.len();

    let (code, analyzed_source) = match generate(&files, &path) {
        Ok((code, analyzer_source)) => (code, analyzer_source),
        Err(e) => {
            sp.stop_with_message("Error compiling queries".red().bold().to_string());
            println!("└── {e}");
            return Err("Error compiling queries".to_string());
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

    match write!(&mut generated_rust_code, "{analyzed_source}") {
        Ok(_) => println!("{}", "Successfully transpiled queries".green().bold()),
        Err(e) => {
            println!("{}", "Failed to transpile queries".red().bold());
            println!("└── {} {}", "Error:".red().bold(), e);
            return Err("Failed to transpile queries".to_string());
        }
    }

    match fs::write(file_path, generated_rust_code) {
        Ok(_) => println!("{}", "Successfully wrote queries file".green().bold()),
        Err(e) => {
            println!("{}", "Failed to write queries file".red().bold());
            println!("└── {} {}", "Error:".red().bold(), e);
            return Err("Failed to write queries file".to_string());
        }
    }

    let mut sp = Spinner::new(Spinners::Dots9, "Building Helix".into());

    let config_path = PathBuf::from(&output).join("src/config.hx.json");
    fs::copy(
        PathBuf::from(path.to_string() + "/config.hx.json"),
        config_path,
    )
    .unwrap();
    let schema_path = PathBuf::from(&output).join("src/schema.hx");
    fs::copy(PathBuf::from(path.to_string() + "/schema.hx"), schema_path).unwrap();

    let mut runner = Command::new("cargo");
    runner
        .arg("check")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .current_dir(PathBuf::from(&output));

    match runner.output() {
        Ok(_) => {}
        Err(e) => {
            sp.stop_with_message("Failed to check Rust code".red().bold().to_string());
            println!("└── {} {}", "Error:".red().bold(), e);
            return Err("Error checking rust code".to_string());
        }
    }

    println!("building helix at: {}", output.display());
    let mut runner = Command::new("cargo");
    let mut args = vec!["build"];

    match release_mode {
        BuildMode::Dev => args.extend_from_slice(&["--profile", "dev"]),
        BuildMode::Release => args.push("--release"),
    }

    if enable_dev_instance {
        args.extend_from_slice(&["--features", "dev-instance"]);
    }

    runner
        .args(args)
        .current_dir(PathBuf::from(&output))
        .env("RUSTFLAGS", "-Awarnings");

    match runner.output() {
        Ok(output) => {
            if output.status.success() {
                sp.stop_with_message("Successfully built Helix".green().bold().to_string());
                Ok(code)
            } else {
                sp.stop_with_message("Failed to build Helix".red().bold().to_string());
                let stderr = String::from_utf8_lossy(&output.stderr);
                if !stderr.is_empty() {
                    println!("└── {} {}", "Error:\n".red().bold(), stderr);
                }
                Err("Error building helix".to_string())
            }
        }
        Err(e) => {
            sp.stop_with_message("Failed to build Helix".red().bold().to_string());
            println!("└── {} {}", "Error:".red().bold(), e);
            Err("Error building helix".to_string())
        }
    }
}

pub fn deploy_helix(
    port: u16,
    code: Content,
    instance_id: Option<String>,
    release_mode: BuildMode,
) -> Result<String, String> {
    let mut sp = Spinner::new(Spinners::Dots9, "Starting Helix instance".into());

    let instance_manager = InstanceManager::new().unwrap();

    let binary_path = dirs::home_dir()
        .map(|path| {
            path.join(format!(
                ".helix/repo/helix-db/target/{}/helix-container",
                release_mode.to_path()
            ))
        })
        .unwrap();

    let endpoints: Vec<String> = code.source.queries.iter().map(|q| q.name.clone()).collect();

    if let Some(iid) = instance_id {
        let cached_binary = instance_manager.cache_dir.join(&iid);
        match fs::copy(binary_path, &cached_binary) {
            Ok(_) => {}
            Err(e) => {
                println!("{} {}", "Error while copying binary:".red().bold(), e);
                return Err("".to_string());
            }
        }
        let openai_key = get_openai_key();
        match instance_manager.start_instance(&iid, Some(endpoints), openai_key, release_mode) {
            Ok(instance) => {
                sp.stop_with_message(
                    "Successfully started Helix instance"
                        .green()
                        .bold()
                        .to_string(),
                );
                let instance_id = instance.id.clone();
                print_instance(instance);
                Ok(instance_id)
            }
            Err(e) => {
                sp.stop_with_message("Failed to start Helix instance".red().bold().to_string());
                println!("└── {} {}", "Error:".red().bold(), e);
                Err("".to_string())
            }
        }
    } else {
        let openai_key = get_openai_key();
        match instance_manager.init_start_instance(&binary_path, port, endpoints, openai_key) {
            Ok(instance) => {
                sp.stop_with_message(
                    "Successfully started Helix instance"
                        .green()
                        .bold()
                        .to_string(),
                );
                let instance_id = instance.id.clone();
                print_instance(instance);
                Ok(instance_id)
            }
            Err(e) => {
                sp.stop_with_message("Failed to start Helix instance".red().bold().to_string());
                println!("└── {} {}", "Error:".red().bold(), e);
                Err("Failed to start Helix instance".to_string())
            }
        }
    }
}

pub fn redeploy_helix(
    instance: String,
    code: Content,
    release_mode: BuildMode,
) -> Result<(), String> {
    let instance_manager = InstanceManager::new().unwrap();
    let iid = instance;

    match instance_manager.get_instance(&iid) {
        Ok(Some(instance)) => {
            println!("{}", "Helix instance found!".green().bold());
            instance
        }
        Ok(None) => {
            println!(
                "{} {}",
                "No Helix instance found with id".red().bold(),
                iid.red().bold()
            );
            return Err("Error".to_string());
        }
        Err(e) => {
            println!("{} {}", "Error:".red().bold(), e);
            return Err("".to_string());
        }
    };

    match instance_manager.stop_instance(&iid) {
        Ok(_) => {}
        Err(e) => {
            println!("{} {}", "Error while stopping instance:".red().bold(), e);
            return Err("".to_string());
        }
    }

    match deploy_helix(0, code, Some(iid), release_mode) {
        Ok(_) => Ok(()),
        Err(e) => Err(e.to_string()),
    }
}

pub async fn redeploy_helix_remote(
    cluster: String,
    path: String,
    files: Vec<DirEntry>,
) -> Result<(), String> {
    let mut sp = Spinner::new(Spinners::Dots9, "Uploading queries to remote db".into());

    let content = match generate_content(&files) {
        Ok(content) => content,
        Err(e) => {
            sp.stop_with_message("Error generating content".red().bold().to_string());
            println!("└── {e}");
            return Err("".to_string());
        }
    };

    // get config from ~/.helix/credentials
    let home_dir = std::env::var("HOME").unwrap_or("~/".to_string());
    let config_path = &format!("{home_dir}/.helix");
    let config_path = Path::new(config_path);
    let config_path = config_path.join("credentials");
    if !config_path.exists() {
        sp.stop_with_message("No credentials found".yellow().bold().to_string());
        println!(
            "{}",
            "Please run `helix config` to set your credentials"
                .yellow()
                .bold()
        );
        return Err("".to_string());
    }

    // TODO: probable could make this more secure
    // reads credentials from ~/.helix/credentials
    let config = fs::read_to_string(config_path).unwrap();
    let user_id = config
        .split("helix_user_id=")
        .nth(1)
        .unwrap()
        .split("\n")
        .next()
        .unwrap();
    let user_key = config
        .split("helix_user_key=")
        .nth(1)
        .unwrap()
        .split("\n")
        .next()
        .unwrap();

    // read config.hx.json
    let config = match Config::from_files(
        PathBuf::from(path.clone()).join("config.hx.json"),
        PathBuf::from(path.clone()).join("schema.hx"),
    ) {
        Ok(config) => config,
        Err(e) => {
            println!("Error loading config: {e}");
            sp.stop_with_message("Error loading config".red().bold().to_string());
            return Err("".to_string());
        }
    };

    let deployment = DeployCloudEvent {
        cluster_id: cluster.clone(),
        queries_string: content
            .source
            .queries
            .iter()
            .map(|q| q.name.clone())
            .collect::<Vec<String>>()
            .join("\n"),
        num_of_queries: content.source.queries.len() as u32,
        time_taken_sec: 0,
        success: true,
        error_messages: None,
    };

    // upload queries to central server
    let payload = json!({
        "user_id": user_id,
        "queries": content.files,
        "cluster_id": cluster,
        "version": "0.1.0",
        "helix_config": config.to_json()
    });
    let client = reqwest::Client::new();

    let cloud_url = format!("http://{}/clusters/deploy-queries", *CLOUD_AUTHORITY);

    match client
        .post(cloud_url)
        .header("x-api-key", user_key) // used to verify user
        .header("x-cluster-id", &cluster) // used to verify instance with user
        .header("Content-Type", "application/json")
        .body(sonic_rs::to_string(&payload).unwrap())
        .send()
        .await
    {
        Ok(response) => {
            if response.status().is_success() {
                sp.stop_with_message("Queries uploaded to remote db".green().bold().to_string());
                HELIX_METRICS_CLIENT.send_event(EventType::DeployCloud, deployment);
            } else {
                sp.stop_with_message(
                    "Error uploading queries to remote db"
                        .red()
                        .bold()
                        .to_string(),
                );
                println!("└── {}", response.text().await.unwrap());
                return Err("".to_string());
            }
        }
        Err(e) => {
            sp.stop_with_message(
                "Error uploading queries to remote db"
                    .red()
                    .bold()
                    .to_string(),
            );
            println!("└── {e}");
            return Err("".to_string());
        }
    };
    Ok(())
}

pub fn get_openai_key() -> Option<String> {
    use dotenvy::dotenv;
    dotenv().ok();
    env::var("OPENAI_API_KEY").ok()
}

pub fn copy_repo_dir_for_build(src: &std::path::Path, dst: &std::path::Path) -> io::Result<()> {
    fs::create_dir_all(dst)?;

    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());

        // Skip copying unnecessary files and directories
        if let Some(file_name) = entry.file_name().to_str()
            && matches!(
                file_name,
                ".git" | ".gitignore" | ".github" | ".DS_Store" | "target" | "docs"
            )
        {
            continue;
        }

        if src_path.is_dir() {
            copy_repo_dir_for_build(&src_path, &dst_path)?;
        } else {
            fs::copy(&src_path, &dst_path)?;
        }
    }

    Ok(())
}
